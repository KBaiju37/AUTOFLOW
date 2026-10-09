"""The AutoFlow engine: the public Python API and the end-to-end reliability workflow."""
from __future__ import annotations

import functools
import hashlib
import json
import os
import re
import time
import uuid
from typing import Any, Callable, Dict, List, Optional, Sequence, Union

import pandas as pd

from .adapters import find_adapter, to_dataset
from .config import Config
from .contracts import (ConfigError, Dataset, Issue, Kind, Mode, MODES, QualityGateFailed, RecoveryProposal,
                        RunResult, RunStatus, Severity, SourceError, AutoFlowError)
from .plugins import DomainPlugin, resolve_plugins
from .profiling import make_baseline, profile_frame, schema_proposal
from .quality import run_checks
from .recovery import RecoveryPlanner, RecoveryRegistry
from .registry import registry as connector_registry
from .rules import RuleSet, load_structured
from .store import Store, StoreError
from .util import frame_fingerprint, jsonable, now_iso, redact_text


def _py(x: Any) -> Any:
    return x.item() if hasattr(x, "item") else x


class AutoFlow:
    def __init__(self, config: Union[Config, Dict[str, Any], str, None] = None,
                 rules: Union[RuleSet, Dict[str, Any], str, None] = None,
                 plugins: Optional[Sequence[Union[str, DomainPlugin]]] = None,
                 approver: Optional[Callable[[RecoveryProposal], bool]] = None,
                 recovery_registry: Optional[RecoveryRegistry] = None):
        self.config = config if isinstance(config, Config) else Config.load(config)
        self.rules = self._load_rules(rules)
        self.plugins = resolve_plugins(list(self.config["plugins"]) + list(plugins or []))
        self.approver = approver
        self.recovery_registry = recovery_registry or RecoveryRegistry()
        self._baselines: Dict[str, Dict[str, Any]] = {}
        self._audit: Optional[Store] = None
        self._out: Optional[Store] = None
        self._quar: Optional[Store] = None
        a, o, q = self.config["audit"], self.config["output"], self.config["quarantine"]
        stores: Dict[str, Store] = {}

        def store_for(path: str) -> Store:
            path = os.path.abspath(path)
            if path not in stores:
                stores[path] = Store(path)
            return stores[path]

        if a["enabled"]:
            self._audit = store_for(a["path"])
        if o["enabled"] and o["type"] == "sqlite_staging":
            self._out = store_for(o["path"])
        qpath = q.get("path") or (o["path"] if o["enabled"] and o["type"] == "sqlite_staging" else None)
        if q["enabled"] and qpath:
            self._quar = store_for(qpath)

    # ------------------------------------------------------------------ construction
    @classmethod
    def from_config(cls, path: Union[str, Dict[str, Any]], **kw: Any) -> "AutoFlow":
        return cls(Config.load(path), **kw)

    def _load_rules(self, rules: Any) -> RuleSet:
        if isinstance(rules, RuleSet):
            return rules
        if isinstance(rules, dict):
            return RuleSet.from_dict(rules)
        if isinstance(rules, str):
            return RuleSet.from_file(rules)
        if self.config["rules_file"]:
            return RuleSet.from_file(self.config["rules_file"])
        return RuleSet.from_dict(self.config["rules"]) if self.config["rules"] else RuleSet.empty()

    @property
    def audit_store(self) -> Optional[Store]:
        return self._audit

    # ------------------------------------------------------------------ public API
    def profile(self, data: Any, dataset_name: str = "dataset") -> Dict[str, Any]:
        ds = to_dataset(data, dataset_name)
        pc = self.config["profiling"]
        prof = profile_frame(ds.frame, dataset_name, pc["sample_size"], ds.total_rows_in_source, len(ds.malformed),
                             pc["mask_examples"], pc["outlier_iqr_multiplier"])
        prof["warnings"] = list(ds.warnings)
        prof["schema_proposal"] = schema_proposal(prof)
        return jsonable(prof)

    def validate(self, data: Any, dataset_name: str = "dataset", mode: str = Mode.GATE,
                 rules: Union[RuleSet, Dict[str, Any], str, None] = None,
                 reference_data: Optional[Dict[str, pd.DataFrame]] = None, run_id: Optional[str] = None) -> RunResult:
        if mode not in MODES:
            raise ConfigError(f"mode must be one of {list(MODES)}")
        user_rules = self._load_rules(rules) if rules is not None else self.rules
        try:
            ds = to_dataset(data, dataset_name)
        except SourceError as exc:
            return self._failed_before_start(dataset_name, mode, run_id, redact_text(str(exc)))
        return self._execute(ds, mode, user_rules, reference_data, run_id)

    def monitor(self, data: Any, dataset_name: str = "dataset", **kw: Any) -> RunResult:
        return self.validate(data, dataset_name, mode=Mode.MONITOR, **kw)

    def run(self, mode: Optional[str] = None, **kw: Any) -> RunResult:
        """Run the pipeline described by the config's ``source`` section."""
        src = dict(self.config["source"])
        if not src:
            raise ConfigError("No source configured")
        t = src.pop("type")
        conn = connector_registry.create(t, src)
        errs = conn.validate()
        name = self.config["autoflow"]["name"]
        if errs:
            return self._failed_before_start(name, mode or self.config["autoflow"]["mode"], None,
                                             "; ".join(errs))
        return self.validate(conn, name, mode=mode or self.config["autoflow"]["mode"], **kw)

    def approve_schema(self, data: Any, dataset_name: str = "dataset") -> Dict[str, Any]:
        """Record the current data's schema/statistics as the approved baseline for drift detection."""
        ds = to_dataset(data, dataset_name)
        pc = self.config["profiling"]
        base = make_baseline(profile_frame(ds.frame, dataset_name, pc["sample_size"], None, 0, True,
                                           pc["outlier_iqr_multiplier"]))
        self._baselines[dataset_name] = base
        if self._audit:
            self._audit.save_baseline(dataset_name, base)
        return base

    def report(self, run_id: str) -> Dict[str, Any]:
        if not self._audit:
            raise ConfigError("Audit store is not enabled")
        r = self._audit.get_run(run_id)
        if r is None:
            raise ConfigError(f"Unknown run_id '{run_id}'")
        return {"run": r, "events": self._audit.events(run_id), "recovery_log": self._audit.recovery_log(run_id)}

    def load_quarantine(self, run_id: str) -> pd.DataFrame:
        store = self._quar or self._audit
        if not store:
            raise ConfigError("No store configured for quarantine")
        key = self._audit.run_key_for(run_id) if self._audit else None
        return store.load_quarantine(run_id, run_key=key)

    def gate(self, dataset_name: Optional[str] = None, on_blocked: str = "raise", **validate_kw: Any):
        """Decorator: validate the DataFrame returned by an existing ETL step before it continues."""
        def deco(fn):
            @functools.wraps(fn)
            def wrapper(*a, **k):
                data = fn(*a, **k)
                r = self.validate(data, dataset_name or fn.__name__, **validate_kw)
                wrapper.last_result = r
                if not r.can_proceed:
                    if on_blocked == "raise":
                        raise QualityGateFailed(r)
                    return None
                return find_adapter(data).from_result(r, data)
            wrapper.last_result = None
            return wrapper
        return deco

    # ------------------------------------------------------------------ internals
    def _failed_before_start(self, name: str, mode: str, run_id: Optional[str], msg: str) -> RunResult:
        now = now_iso()
        res = RunResult(run_id or uuid.uuid4().hex, name, RunStatus.FAILED, mode, started_at=now, ended_at=now,
                        errors=[msg])
        res.diagnosis = [{"symptom": msg, "probable_cause": "Source unreachable, misconfigured, or malformed",
                          "recommendation": "Fix the source configuration, then re-run", "evidence": "observed"}]
        self._persist(res, [{"ts": now, "event": "source_failed", "detail": {"error": msg}}])
        return res

    def _config_hash(self, rules: RuleSet) -> str:
        c = self.config
        blob = json.dumps({"rules": rules.to_dict(), "validation": c["validation"],
                           "recovery": {k: c["recovery"][k] for k in ("enabled", "auto_apply_categories",
                                                                      "approved_rules", "disabled_rules")},
                           "registry": self.recovery_registry.signature(),
                           "plugins": sorted(p.name for p in self.plugins),
                           "approver": self.approver is not None}, sort_keys=True, default=str)
        return hashlib.sha256(blob.encode()).hexdigest()

    def _baseline_for(self, name: str) -> Optional[Dict[str, Any]]:
        if name in self._baselines:
            return self._baselines[name]
        return self._audit.load_baseline(name) if self._audit else None

    def _effective_rules(self, user: RuleSet, df: pd.DataFrame, name: str) -> RuleSet:
        base = RuleSet.empty()
        for p in self.plugins:
            frag = p.rules() if p.applies(df, name) else None
            if frag:
                base = RuleSet.from_dict(frag).merged_over(base)
        return user.merged_over(base)

    def _execute(self, ds: Dataset, mode: str, user_rules: RuleSet,
                 reference_data: Optional[Dict[str, pd.DataFrame]], run_id: Optional[str]) -> RunResult:
        c = self.config
        v = c["validation"]
        rec_cfg = dict(c["recovery"])
        t0 = time.time()
        frame, name = ds.frame, ds.name
        total = len(frame) + len(ds.malformed)
        res = RunResult(run_id or uuid.uuid4().hex, name, RunStatus.FAILED, mode, started_at=now_iso(),
                        rows_processed=total)
        events: List[Dict[str, Any]] = []

        def ev(event: str, **detail: Any) -> None:
            events.append({"ts": now_iso(), "event": event, "detail": jsonable(detail)})

        ev("run_started", mode=mode, source_type=ds.source_type, source_id=ds.source_id, rows=total,
           malformed=len(ds.malformed))
        try:
            self._run_pipeline(ds, mode, user_rules, reference_data, res, ev, rec_cfg, v, frame, name, total)
        except (StoreError, OSError) as exc:
            res.status, res.can_proceed, res.approved_data = RunStatus.FAILED, False, None
            res.errors.append(f"Output write failed: {redact_text(str(exc))}")
            res.output = {"committed": False, "error": redact_text(str(exc))}
            ev("output_failed", error=redact_text(str(exc)))
        except Exception as exc:  # never crash the host pipeline; report a FAILED result
            res.status, res.can_proceed, res.approved_data = RunStatus.FAILED, False, None
            res.errors.append(f"Internal error: {exc.__class__.__name__}: {redact_text(str(exc))}")
            ev("run_error", error=f"{exc.__class__.__name__}: {redact_text(str(exc))}")
        res.ended_at = now_iso()
        res.duration_s = round(time.time() - t0, 4)
        ev("run_finished", status=res.status, can_proceed=res.can_proceed)
        self._persist(res, events)
        return res

    def _run_pipeline(self, ds, mode, user_rules, reference_data, res, ev, rec_cfg, v, frame, name, total):
        c = self.config
        pc = c["profiling"]
        # ---- fingerprint / idempotency identity
        res.fingerprint = ds.fingerprint or frame_fingerprint(frame)
        rules = self._effective_rules(user_rules, frame, name)
        res.run_key = hashlib.sha256(f"{name}|{res.fingerprint}|{self._config_hash(rules)}".encode()).hexdigest()
        if self._audit:
            last = self._audit.last_fingerprint(name)
            res.source_changed_since_last_run = None if last is None else (last != res.fingerprint)

        # ---- profile + detect
        profile = profile_frame(frame, name, pc["sample_size"], ds.total_rows_in_source, len(ds.malformed),
                                pc["mask_examples"], pc["outlier_iqr_multiplier"])
        res.profile = profile
        ev("profiled", rows=len(frame), columns=len(frame.columns), exact=profile["exact"])
        out = run_checks(frame, rules, profile, v, self._baseline_for(name), reference_data, self.plugins,
                         ds.malformed, ds.duplicate_columns, name)
        issues = out.issues
        res.issues = issues
        res.issues_detected = len(issues)
        res.warnings = list(ds.warnings)
        ev("checks_completed", issues=len(issues), errors=sum(i.severity == Severity.ERROR for i in issues),
           warnings=sum(i.severity == Severity.WARNING for i in issues))

        err_viol = [x for x in out.violations if x.severity == Severity.ERROR]
        blocking = any(i.severity == Severity.ERROR and i.scope != "row" for i in issues)
        flagged = {x.row_id for x in err_viol}
        reasons: Dict[Any, List[str]] = {}
        for x in err_viol:
            reasons.setdefault(x.row_id, []).append(f"{x.check}:{x.column}" if x.column else x.check)

        # ---- propose / authorize
        can_apply = mode == Mode.GATE and bool(rec_cfg["enabled"]) and not blocking
        proposals: List[RecoveryProposal] = []
        repairable: Dict[Any, bool] = {}
        if err_viol and not blocking:
            planner = RecoveryPlanner(self.recovery_registry, rec_cfg, self.approver)
            proposals, repairable = planner.plan(err_viol, frame, rules, res.run_id[:8], can_apply)
            ev("recovery_planned", proposals=len([p for p in proposals if p.action != "none"]),
               unresolved=len([p for p in proposals if p.action == "none"]))
        res.proposals = proposals
        res.recovery_proposals = len([p for p in proposals if p.action != "none"])

        # ---- recover + revalidate
        work = frame
        dropped: set = set()
        failed_reval: set = set()
        if can_apply and any(repairable.values()):
            fixed = frame.copy()
            repair_rows = {rid for rid, ok in repairable.items() if ok}
            for p in proposals:
                if p.row_id in repair_rows and p.authorization == "authorized":
                    if p.action == "set_value":
                        if fixed[p.column].dtype != object:
                            fixed[p.column] = fixed[p.column].astype(object)
                        fixed.at[p.row_id, p.column] = p.proposed
                    elif p.action == "drop_row":
                        dropped.add(p.row_id)
                    p.status = "applied"
                    res.recovery_actions_applied += 1
            ev("recovery_applied", actions=res.recovery_actions_applied, rows=len(repair_rows))
            work = fixed.drop(index=list(dropped))
            out2 = run_checks(work, rules, profile, v, self._baseline_for(name), reference_data, self.plugins,
                              ds.malformed, ds.duplicate_columns, name)
            err2 = [x for x in out2.violations if x.severity == Severity.ERROR]
            flagged = {x.row_id for x in err2}
            reasons = {}
            for x in err2:
                reasons.setdefault(x.row_id, []).append(f"{x.check}:{x.column}" if x.column else x.check)
            repaired = repair_rows - dropped
            failed_reval = repaired & flagged
            res.revalidation_failed = len(failed_reval)
            res.revalidation_passed = len(repaired - flagged)
            for p in proposals:
                if p.status == "applied" and p.row_id in repaired:
                    if p.row_id in failed_reval:
                        p.status, p.result = "failed_revalidation", "row still violates rules after repair; quarantined with original values"
                    else:
                        p.status, p.result = "revalidated", "row passed revalidation"
                elif p.status == "applied" and p.row_id in dropped:
                    p.result = "row removed; preserved in quarantine as duplicate_removed"
            for rid in failed_reval:
                reasons.setdefault(rid, []).append("revalidation_failed")
            for rid in dropped:
                reasons[rid] = ["duplicate_removed"]
            flagged |= dropped
            ev("revalidated", passed=res.revalidation_passed, failed=res.revalidation_failed, dropped=len(dropped))

        # ---- route
        malformed = ds.malformed
        n_q = len(flagged) + len(malformed)
        approved_ids = [i for i in work.index if i not in flagged]
        approved_df = work.loc[approved_ids]
        quarantine_records = self._quarantine_records(frame, flagged, reasons, proposals, malformed)
        res.quarantine_summary = [{"row_id": _py(r["row_id"]), "reasons": r["reasons"]} for r in quarantine_records][:1000]
        res.quarantined_data = pd.DataFrame([r["original"] | {"_quarantine_reasons": "; ".join(r["reasons"])}
                                             for r in quarantine_records],
                                            index=pd.Index([r["row_id"] for r in quarantine_records], name="_row_id"))
        outcome_status, proceed, why = self._outcome(blocking, n_q, total, issues, v)
        ev("routed", would_approve=len(approved_ids), would_quarantine=n_q, blocking=blocking, outcome=outcome_status)
        if why:
            res.warnings.append(why)
        res.diagnosis = self._diagnose(issues, total)

        if mode == Mode.GATE:
            res.status, res.can_proceed = outcome_status, proceed
            res.rows_quarantined = n_q
            res.rows_blocked = total if blocking else 0
            res.rows_approved = len(approved_ids) if (proceed and not blocking) else 0
            if not proceed and not blocking:
                res.rows_blocked = len(approved_ids)
            res.approved_data = approved_df if proceed else None
            if blocking:
                res.rows_quarantined = 0
            self._write_output(res, ev, approved_df if proceed else None, quarantine_records if not blocking else [])
        else:
            res.rows_would_approve = 0 if blocking else len(approved_ids)
            res.rows_would_quarantine = 0 if blocking else n_q
            res.would_proceed = proceed
            if mode == Mode.DRY_RUN:
                res.status, res.can_proceed = RunStatus.DRY_RUN, False
            else:
                res.status, res.can_proceed = outcome_status, proceed
            res.output = {"committed": False, "reason": f"{mode} mode never writes output"}
        if mode == Mode.GATE:
            res.would_proceed = res.can_proceed

    @staticmethod
    def _outcome(blocking: bool, n_q: int, total: int, issues: List[Issue], v: Dict[str, Any]):
        if blocking:
            return RunStatus.FAILED, False, "Blocked by dataset-level error(s); see issues"
        if n_q:
            pct = 100.0 * n_q / max(total, 1)
            if v["allow_partial_success"] and pct <= v["max_quarantine_pct"]:
                return RunStatus.PARTIAL_SUCCESS, True, f"{n_q} row(s) quarantined; partial load permitted"
            return RunStatus.FAILED, False, (f"{n_q} row(s) quarantined and partial success is not permitted"
                                             if not v["allow_partial_success"]
                                             else f"{pct:.1f}% quarantined exceeds max_quarantine_pct={v['max_quarantine_pct']}")
        if any(i.severity == Severity.WARNING for i in issues):
            if v["warnings_block"]:
                return RunStatus.FAILED, False, "warnings block the pipeline (validation.warnings_block)"
            return RunStatus.SUCCESS_WITH_WARNINGS, True, ""
        return RunStatus.SUCCESS, True, ""

    @staticmethod
    def _quarantine_records(frame, flagged, reasons, proposals, malformed) -> List[Dict[str, Any]]:
        by_row: Dict[Any, List[Dict[str, Any]]] = {}
        for p in proposals:
            by_row.setdefault(p.row_id, []).append(p.to_dict())
        recs = []
        for rid in frame.index:
            if rid in flagged:
                recs.append({"row_id": _py(rid), "reasons": reasons.get(rid, ["unspecified"]),
                             "original": {str(k): jsonable(x) for k, x in frame.loc[rid].items()},
                             "recovery": by_row.get(rid, [])})
        for m in malformed:
            recs.append({"row_id": m["row_id"], "reasons": [f"malformed_record: {m['reason']}"],
                         "original": {"_raw": m.get("raw", "")}, "recovery": []})
        return recs

    @staticmethod
    def _diagnose(issues: List[Issue], total: int) -> List[Dict[str, Any]]:
        out = []
        for i in issues:
            if i.severity in (Severity.ERROR, Severity.WARNING):
                out.append({"symptom": i.message, "probable_cause": i.probable_cause,
                            "recommendation": i.recommendation,
                            "evidence": {"confirmed": "violates an explicit rule/structural fact",
                                         "suspected": "heuristic/statistical signal, not proof",
                                         "inferred": "recommendation from profiling"}[i.kind],
                            "severity": i.severity})
            if i.code == "type_mismatch_violation" and total and i.count / total >= 0.3:
                out.append({"symptom": f"{i.count} of {total} rows fail the type check on '{i.column}'",
                            "probable_cause": "Probable upstream format change for this column (not confirmed)",
                            "recommendation": "Compare with the previous extract before attempting repairs",
                            "evidence": "suspected", "severity": Severity.WARNING})
        return out

    def _write_output(self, res: RunResult, ev, approved: Optional[pd.DataFrame],
                      quarantine: List[Dict[str, Any]]) -> None:
        o, q = self.config["output"], self.config["quarantine"]
        name = res.dataset_name
        info: Dict[str, Any] = {"type": o["type"] if o["enabled"] else "none", "staged": False,
                                "rows_staged": 0, "quarantine_rows_written": 0}
        stage = approved if (o["enabled"] and approved is not None) else None
        write_q = quarantine if (q["enabled"] and quarantine) else None
        if self._out is not None and self._out is self._quar:
            r = self._out.commit_batch(res.run_key, name, res.run_id, stage, write_q)
            self._fold(info, r, stage, write_q)
        else:
            if self._out is not None and stage is not None:
                self._fold(info, self._out.commit_batch(res.run_key, name, res.run_id, stage, None), stage, None)
            if self._quar is not None and write_q:
                r = self._quar.commit_batch(res.run_key, name, res.run_id, None, write_q)
                info["quarantine_rows_written"] = len(write_q) if r.get("committed") else 0
                if not r.get("committed"):
                    info["quarantine_skipped"] = r.get("skipped")
        if o["enabled"] and o["type"] == "local_files" and (stage is not None or write_q):
            self._write_files(info, res, stage, write_q)
        if info.get("skipped") == "already_committed":
            res.idempotent_replay, res.replay_of = True, info.get("original_run_id")
        res.output = info
        ev("output", **info)

    @staticmethod
    def _fold(info, r, stage, write_q) -> None:
        if r.get("committed"):
            info.update(staged=stage is not None, rows_staged=0 if stage is None else len(stage),
                        quarantine_rows_written=len(write_q or []))
        else:
            info.update(skipped=r.get("skipped"), original_run_id=r.get("original_run_id"))

    def _write_files(self, info, res, stage: Optional[pd.DataFrame], write_q) -> None:
        """Write local-file outputs using a manifest as the batch commit marker.

        Files are replaced atomically one at a time. Consumers should treat a batch as
        committed only when its manifest exists; a retry can safely replace orphaned
        files left behind if a process stops before the manifest is published.
        """
        d = self.config["output"]["directory"]
        os.makedirs(d, exist_ok=True)
        # Dataset names are user-controlled config, so never allow them to become paths.
        safe_name = re.sub(r"[^A-Za-z0-9._-]+", "_", str(res.dataset_name)).strip("._-")
        safe_name = safe_name[:80] or "dataset"
        stem = f"{safe_name}_{res.run_key[:12]}"
        manifest_path = os.path.join(d, stem + "_manifest.json")
        if os.path.exists(manifest_path):
            info.update(skipped="already_committed", original_run_id=None, manifest=manifest_path)
            return

        written = {}
        if stage is not None:
            approved_path = os.path.join(d, stem + "_approved.csv")
            tmp = approved_path + ".tmp"
            try:
                stage.to_csv(tmp, index_label="_row_id")
                os.replace(tmp, approved_path)
            finally:
                if os.path.exists(tmp):
                    os.remove(tmp)
            written["approved"] = os.path.basename(approved_path)
            info.update(staged=True, rows_staged=len(stage), path=approved_path)

        if write_q:
            qp = os.path.join(d, stem + "_quarantine.csv")
            tmp_q = qp + ".tmp"
            try:
                pd.DataFrame([{"row_id": r["row_id"], "reasons": "; ".join(r["reasons"]),
                               "original": json.dumps(r["original"], default=str)} for r in write_q]).to_csv(
                                   tmp_q, index=False)
                os.replace(tmp_q, qp)
            finally:
                if os.path.exists(tmp_q):
                    os.remove(tmp_q)
            written["quarantine"] = os.path.basename(qp)
            info["quarantine_rows_written"] = len(write_q)

        # Publish last: downstream consumers can ignore batches without a manifest.
        manifest = {"run_id": res.run_id, "run_key": res.run_key, "dataset": res.dataset_name,
                    "status": res.status, "files": written, "rows_staged": 0 if stage is None else len(stage),
                    "rows_quarantined": len(write_q or [])}
        tmp_manifest = manifest_path + ".tmp"
        try:
            with open(tmp_manifest, "w", encoding="utf-8") as fh:
                json.dump(manifest, fh, indent=2, sort_keys=True)
            os.replace(tmp_manifest, manifest_path)
        finally:
            if os.path.exists(tmp_manifest):
                os.remove(tmp_manifest)
        info["manifest"] = manifest_path

    def _persist(self, res: RunResult, events: List[Dict[str, Any]]) -> None:
        if not self._audit:
            return
        redact = self.config["audit"]["redact_values"]
        d = res.to_dict()
        rec_rows = [p.to_dict() for p in res.proposals]
        if redact:
            for p in d["proposals"]:
                p["original"], p["proposed"] = "***", "***" if p["proposed"] is not None else None
        try:
            self._audit.save_run(d, events, rec_rows, redact=redact)
        except Exception as exc:  # audit failure must be visible, not silent
            res.warnings.append(f"Audit record could not be saved: {redact_text(str(exc))}")
