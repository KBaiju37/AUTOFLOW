"""Source-independent data quality checks.

Layers (see README): structural -> user rules -> generic suspicions -> baseline drift -> plugins.
Only *confirmed* violations with severity 'error' cause rows to be quarantined. Suspected and
inferred findings (including statistical outliers) are reported but never reject rows.
"""
from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Sequence

import numpy as np
import pandas as pd

from .contracts import Issue, Kind, RowViolation, Severity
from .rules import RuleSet
from .typeops import as_str, coerce_series, missing_mask
from .util import frame_str

CAUSES = {
    "type_mismatch": ("Upstream format change, free-text entry, or a serialization artifact",
                      "Inspect the sample rows; fix upstream, or enable a matching recovery rule"),
    "not_null": ("Value missing upstream, or an extraction/join dropped it",
                 "Check the producing step; supply a default only if the business allows it"),
    "range": ("Out-of-range measurement, unit change, or data-entry error",
              "Verify against the source system before changing values"),
    "length": ("Truncation or padding upstream", "Check column width limits in the producer"),
    "pattern": ("Format drift or free-text entry", "Check the producer's validation or normalise upstream"),
    "allowed_values": ("New category introduced upstream, casing/spelling variation, or bad mapping",
                       "Add the value to the allowed list if legitimate, otherwise fix upstream"),
    "duplicate_key": ("Replayed load, missing dedup step, or a key collision",
                      "Check for retries or overlapping extracts; decide which record is authoritative"),
    "compare": ("Inconsistent related fields, or one of them mis-parsed", "Inspect both columns on the flagged rows"),
    "reference": ("Orphan record or stale reference data", "Refresh the reference data or fix the foreign key"),
}


class CheckOutput:
    def __init__(self) -> None:
        self.issues: List[Issue] = []
        self.violations: List[RowViolation] = []

    def error_row_ids(self) -> set:
        return {v.row_id for v in self.violations if v.severity == Severity.ERROR}


def _ids(index: pd.Index, k: int = 5) -> List[Any]:
    return [x.item() if hasattr(x, "item") else x for x in list(index[:k])]


def column_value_checks(nonmiss: pd.Series, rule: Dict[str, Any]) -> Dict[str, pd.Series]:
    """Per-value invalid masks for the explicit column rule. Shared by validation and recovery."""
    out: Dict[str, pd.Series] = {}
    if len(nonmiss) == 0:
        return out
    typ = rule.get("type")
    valid = pd.Series(True, index=nonmiss.index)
    parsed = nonmiss
    if typ:
        parsed, valid = coerce_series(nonmiss, typ, rule.get("date_format"))
        if (~valid).any():
            out["type_mismatch"] = ~valid
    if ("min" in rule or "max" in rule) and typ in ("integer", "number", "datetime", "date"):
        bad = pd.Series(False, index=nonmiss.index)
        for key, cmp in (("min", lambda a, b: a < b), ("max", lambda a, b: a > b)):
            if key in rule:
                bound = rule[key]
                if typ in ("datetime", "date"):
                    bound = pd.Timestamp(bound)
                    bound = bound.tz_localize("UTC") if bound.tzinfo is None else bound.tz_convert("UTC")
                bad = bad | (valid & cmp(parsed, bound).fillna(False).astype(bool))
        if bad.any():
            out["range"] = bad
    strs = as_str(nonmiss)
    if "min_length" in rule or "max_length" in rule:
        ln = strs.str.len()
        bad = pd.Series(False, index=nonmiss.index)
        if "min_length" in rule:
            bad = bad | (ln < rule["min_length"])
        if "max_length" in rule:
            bad = bad | (ln > rule["max_length"])
        if bad.any():
            out["length"] = bad
    if "pattern" in rule:
        rx = re.compile(str(rule["pattern"]))
        bad = ~strs.map(lambda v: rx.fullmatch(v) is not None).astype(bool)
        if bad.any():
            out["pattern"] = bad
    if "allowed" in rule:
        allowed = {str(a) for a in rule["allowed"]}
        bad = ~strs.isin(allowed)
        if bad.any():
            out["allowed_values"] = bad
    return out


def cell_is_valid(value: Any, rule: Dict[str, Any]) -> bool:
    """Would a single value satisfy the explicit column rule?"""
    s = pd.Series([value], dtype=object)
    if missing_mask(s).iloc[0]:
        return bool(rule.get("nullable", True))
    return not column_value_checks(s, rule)


# --------------------------------------------------------------------------- helpers
class _Ctx:
    def __init__(self, df: pd.DataFrame, out: CheckOutput):
        self.df, self.out, self._fs = df, out, None

    @property
    def fs(self) -> pd.DataFrame:
        if self._fs is None:
            self._fs = frame_str(self.df)
        return self._fs

    def row_issue(self, code: str, check: str, column: Optional[str], bad: pd.Series, values: pd.Series,
                  severity: str, message: str, source: str = "user", detail: Optional[dict] = None,
                  kind: str = Kind.CONFIRMED) -> None:
        idx = bad[bad].index
        if len(idx) == 0:
            return
        cause, rec = CAUSES.get(check, (None, None))
        for rid in idx:
            self.out.violations.append(RowViolation(rid, column, check, values.get(rid), severity,
                                                    dict(detail or {}), source))
        self.out.issues.append(Issue(code, severity, kind, message.format(n=len(idx)), "row", column,
                                     int(len(idx)), _ids(idx), cause, rec, source))


def _unique_check(ctx: _Ctx, cols: List[str], severity: str, source: str) -> None:
    df = ctx.df
    mm = pd.concat([missing_mask(df[c]) for c in cols], axis=1).any(axis=1)
    kh = pd.util.hash_pandas_object(ctx.fs[cols], index=False)
    kh.index = df.index
    dup = kh.duplicated(keep=False) & ~mm
    if not dup.any():
        return
    rh = pd.util.hash_pandas_object(ctx.fs, index=False)
    rh.index = df.index
    d = pd.DataFrame({"kh": kh[dup], "rh": rh[dup]})
    exact = d.groupby("kh")["rh"].transform("nunique") == 1
    first = ~d.duplicated("kh", keep="first")
    flag = (~exact) | (exact & ~first)
    bad = pd.Series(False, index=df.index)
    bad.loc[d.index[flag]] = True
    key_label = ",".join(cols)
    exact_ids = set(d.index[exact])
    col0 = cols[0] if len(cols) == 1 else None
    vals = ctx.fs[cols].agg("|".join, axis=1) if len(cols) > 1 else ctx.fs[cols[0]]
    for rid in bad[bad].index:
        ctx.out.violations.append(RowViolation(rid, col0, "duplicate_key", vals.get(rid), severity,
                                               {"key": key_label, "exact_duplicate": rid in exact_ids}, source))
    cause, rec = CAUSES["duplicate_key"]
    ctx.out.issues.append(Issue("duplicate_key", severity, Kind.CONFIRMED,
                                f"{int(bad.sum())} row(s) violate uniqueness of ({key_label})", "row", col0,
                                int(bad.sum()), _ids(bad[bad].index), cause, rec, source))


def _parse_pair(a: pd.Series, b: Optional[pd.Series], const: Any):
    """Return (left_parsed, right_parsed, kind) or None if not safely comparable."""
    am = ~missing_mask(a)
    n_a = int(am.sum())
    if n_a == 0:
        return None
    for kind in ("number", "datetime"):
        la, va = coerce_series(a[am], kind)
        if va.mean() < 0.9:
            continue
        left = pd.Series(np.nan if kind == "number" else pd.NaT, index=a.index, dtype=la.dtype)
        left.loc[va[va].index] = la[va]
        if b is not None:
            bm = ~missing_mask(b)
            lb, vb = coerce_series(b[bm], kind)
            if len(vb) == 0 or vb.mean() < 0.9:
                continue
            right = pd.Series(np.nan if kind == "number" else pd.NaT, index=b.index, dtype=lb.dtype)
            right.loc[vb[vb].index] = lb[vb]
        else:
            cp, cv = coerce_series(pd.Series([const], dtype=object), kind)
            if not cv.iloc[0]:
                continue
            right = pd.Series(cp.iloc[0], index=a.index)
        return left, right, kind
    return None


_OPS = {"<": lambda a, b: a < b, "<=": lambda a, b: a <= b, ">": lambda a, b: a > b,
        ">=": lambda a, b: a >= b, "==": lambda a, b: a == b, "!=": lambda a, b: a != b}


def _compare_check(ctx: _Ctx, chk: Dict[str, Any]) -> None:
    df, sev = ctx.df, chk.get("severity", "error")
    left_name = chk["left"]
    right_name = chk.get("right")
    for c in (left_name, right_name):
        if c is not None and c not in df.columns:
            ctx.out.issues.append(Issue("check_skipped", Severity.WARNING, Kind.CONFIRMED,
                                        f"compare check skipped: column '{c}' not present", "dataset", c, source="user"))
            return
    pair = _parse_pair(df[left_name], df[right_name] if right_name else None, chk.get("value"))
    if pair is None:
        if chk["op"] in ("==", "!="):
            a = ctx.fs[left_name]
            b = ctx.fs[right_name] if right_name else pd.Series(str(chk.get("value")), index=df.index)
            ok = _OPS[chk["op"]](a, b)
            valid_rows = ~(missing_mask(df[left_name]))
        else:
            ctx.out.issues.append(Issue("check_skipped", Severity.WARNING, Kind.CONFIRMED,
                                        f"compare check on '{left_name}' skipped: values are not safely numeric or date-like",
                                        "dataset", left_name, source="user"))
            return
    else:
        left, right, _ = pair
        valid_rows = left.notna() & right.notna()
        ok = _OPS[chk["op"]](left, right)
    bad = valid_rows & ~ok.fillna(False).astype(bool)
    target = right_name or repr(chk.get("value"))
    ctx.row_issue("compare_violation", "compare", left_name, bad, ctx.fs[left_name], sev,
                  "{n} row(s) violate '" + f"{left_name} {chk['op']} {target}" + "'",
                  detail={"op": chk["op"], "target": target})


# --------------------------------------------------------------------------- main entry
def run_checks(df: pd.DataFrame, rules: RuleSet, profile: Dict[str, Any], cfg: Dict[str, Any],
               baseline: Optional[Dict[str, Any]] = None,
               reference_data: Optional[Dict[str, pd.DataFrame]] = None,
               plugins: Sequence[Any] = (), malformed: Sequence[Dict[str, Any]] = (),
               duplicate_columns: Sequence[str] = (), dataset_name: str = "") -> CheckOutput:
    out = CheckOutput()
    ctx = _Ctx(df, out)
    n = len(df)

    # ---- structural -------------------------------------------------------
    if duplicate_columns:
        out.issues.append(Issue("duplicate_column_names", Severity.ERROR, Kind.CONFIRMED,
                                f"Duplicate column names in source: {list(duplicate_columns)}", "dataset",
                                count=len(duplicate_columns),
                                probable_cause="Bad header or a join that did not rename columns",
                                recommendation="Fix the producer's column naming"))
    if malformed:
        out.issues.append(Issue("malformed_records", Severity.ERROR, Kind.CONFIRMED,
                                f"{len(malformed)} record(s) could not be parsed", "row", count=len(malformed),
                                sample_row_ids=[m["row_id"] for m in malformed[:5]],
                                probable_cause=malformed[0]["reason"],
                                recommendation="Inspect the raw lines in quarantine; fix the producer or delimiter/quoting settings"))
    if n == 0:
        out.issues.append(Issue("empty_input", Severity.ERROR, Kind.CONFIRMED,
                                "Dataset contains no valid rows" + (" (all records malformed)" if malformed else ""),
                                "dataset", probable_cause="Upstream extract returned nothing, or the file/query is wrong",
                                recommendation="Check the upstream job and the source path/query"))
        return out

    present = set(df.columns)
    missing_cols = [c for c in rules.required_columns() if c not in present]
    for c in missing_cols:
        sev = Severity.ERROR if cfg["fail_on_missing_required_columns"] else Severity.WARNING
        out.issues.append(Issue("missing_required_column", sev, Kind.CONFIRMED,
                                f"Required column '{c}' is missing", "column", c, source="user",
                                probable_cause="Upstream schema change or a renamed/dropped field",
                                recommendation="Compare with the previous schema; restore or map the column"))
    if rules.columns and not rules.allow_extra_columns:
        extra = sorted(present - set(rules.columns))
        if extra:
            out.issues.append(Issue("unexpected_columns", Severity.WARNING, Kind.CONFIRMED,
                                    f"Unexpected column(s): {extra}", "dataset", count=len(extra), source="user",
                                    probable_cause="Upstream added fields", recommendation="Add them to the rules or drop upstream"))

    # ---- user rules: columns ---------------------------------------------
    for col, rule in rules.columns.items():
        if col not in present:
            continue
        s = df[col]
        miss = missing_mask(s)
        sev = rule.get("severity", Severity.ERROR)
        null_pct = float(miss.mean())
        if "null_fail_pct" in rule and null_pct > rule["null_fail_pct"]:
            out.issues.append(Issue("null_rate_exceeded", Severity.ERROR, Kind.CONFIRMED,
                                    f"'{col}' is {null_pct:.1%} null (fail threshold {rule['null_fail_pct']:.1%})",
                                    "column", col, int(miss.sum()), source="user",
                                    probable_cause="Upstream stopped populating the field or a join failed",
                                    recommendation="Check the producing step for this column"))
        elif "null_warn_pct" in rule and null_pct > rule["null_warn_pct"]:
            out.issues.append(Issue("null_rate_high", Severity.WARNING, Kind.CONFIRMED,
                                    f"'{col}' is {null_pct:.1%} null (warn threshold {rule['null_warn_pct']:.1%})",
                                    "column", col, int(miss.sum()), source="user"))
        if rule.get("nullable", True) is False:
            ctx.row_issue("null_in_non_nullable", "not_null", col, miss, s, sev,
                          "{n} null/empty value(s) in non-nullable column '" + col + "'")
        nonmiss = s[~miss]
        for check, bad in column_value_checks(nonmiss, rule).items():
            label = {"type_mismatch": f"value(s) not valid {rule.get('type')}", "range": "value(s) out of range",
                     "length": "value(s) violate length limits", "pattern": "value(s) do not match the pattern",
                     "allowed_values": "value(s) not in the allowed set"}[check]
            full = pd.Series(False, index=df.index)
            full.loc[bad[bad].index] = True
            ctx.row_issue(f"{check}_violation", check, col, full, s, sev, "{n} " + label + f" in '{col}'")
        if rule.get("unique"):
            _unique_check(ctx, [col], sev, "user")

    # ---- user rules: dataset checks ------------------------------------
    for chk in rules.checks:
        t, sev = chk["type"], chk.get("severity", Severity.ERROR)
        cols = chk.get("columns", [])
        absent = [c for c in cols if c not in present]
        if absent and t in ("unique", "not_null"):
            out.issues.append(Issue("check_skipped", Severity.WARNING, Kind.CONFIRMED,
                                    f"{t} check skipped: column(s) {absent} not present", "dataset", source="user"))
            continue
        if t == "unique":
            _unique_check(ctx, cols, sev, "user")
        elif t == "not_null":
            for c in cols:
                ctx.row_issue("null_in_non_nullable", "not_null", c, missing_mask(df[c]), df[c], sev,
                              "{n} null/empty value(s) in '" + c + "'")
        elif t == "compare":
            _compare_check(ctx, chk)
        elif t == "row_count":
            lo, hi = chk.get("min"), chk.get("max")
            if (lo is not None and n < lo) or (hi is not None and n > hi):
                out.issues.append(Issue("row_count_out_of_bounds", sev, Kind.CONFIRMED,
                                        f"Row count {n} outside configured bounds [{lo}, {hi}]", "dataset", count=n,
                                        source="user", probable_cause="Partial extract, duplicated load, or upstream filter change",
                                        recommendation="Compare with the source system's row count"))
        elif t == "reference":
            col = chk["column"]
            if col not in present:
                continue
            if "values" in chk:
                allowed = {str(v) for v in chk["values"]}
            else:
                ref = (reference_data or {}).get(chk["dataset"])
                if ref is None or chk["ref_column"] not in ref.columns:
                    out.issues.append(Issue("reference_unavailable", Severity.ERROR, Kind.CONFIRMED,
                                            f"Reference data '{chk['dataset']}.{chk['ref_column']}' was not supplied",
                                            "dataset", col, source="user",
                                            recommendation="Pass reference_data={'name': DataFrame} to validate()"))
                    continue
                allowed = set(frame_str(ref[[chk["ref_column"]]])[chk["ref_column"]])
            miss = missing_mask(df[col])
            bad = ~miss & ~ctx.fs[col].isin(allowed)
            ctx.row_issue("reference_violation", "reference", col, bad, df[col], sev,
                          "{n} value(s) in '" + col + "' have no match in the reference set")

    # ---- generic, conservative suspicions ---------------------------------
    if cfg["use_generic_rules"]:
        _generic(ctx, rules, profile, cfg)

    # ---- baseline drift ---------------------------------------------------
    if baseline:
        _drift(out, df, profile, baseline, cfg)

    # ---- domain plugins ---------------------------------------------------
    for p in plugins:
        if p.applies(df, dataset_name):
            iss, viol = p.check(df)
            for i in iss:
                i.source = f"plugin:{p.name}"
            for v in viol:
                v.source = f"plugin:{p.name}"
            out.issues.extend(iss)
            out.violations.extend(viol)
    return out


def _generic(ctx: _Ctx, rules: RuleSet, profile: Dict[str, Any], cfg: Dict[str, Any]) -> None:
    df, out = ctx.df, ctx.out
    if profile.get("duplicate_rows", 0) > 0 and not profile.get("sampled"):
        n = profile["duplicate_rows"]
        out.issues.append(Issue("duplicate_rows", Severity.WARNING, Kind.SUSPECTED,
                                f"{n} fully duplicated row(s). Identical rows can be legitimate; not rejected.",
                                "dataset", count=n,
                                probable_cause="Replayed load or missing dedup step (or genuinely repeated events)",
                                recommendation="Add a unique-key rule so duplicates can be judged against business meaning"))
    for col, p in profile["columns"].items():
        declared = rules.columns.get(col, {})
        if p["null_pct"] >= cfg["generic_null_warn_pct"] and "null_warn_pct" not in declared and p["null_count"] > 0:
            out.issues.append(Issue("high_null_rate", Severity.WARNING, Kind.SUSPECTED,
                                    f"'{col}' is {p['null_pct']:.1%} null. Missing values are not assumed to be errors.",
                                    "column", col, p["null_count"],
                                    recommendation="Declare nullable/null thresholds in rules if this matters"))
        if p["whitespace_anomalies"]:
            out.issues.append(Issue("whitespace_anomalies", Severity.INFO, Kind.INFERRED,
                                    f"'{col}' has {p['whitespace_anomalies']} value(s) with leading/trailing whitespace",
                                    "column", col, p["whitespace_anomalies"],
                                    recommendation="Consider a trim rule for this column"))
        if p["is_constant"]:
            out.issues.append(Issue("constant_column", Severity.INFO, Kind.INFERRED,
                                    f"'{col}' holds a single value", "column", col, 0))
        if p.get("potential_outliers"):
            out.issues.append(Issue("potential_outliers", Severity.INFO, Kind.SUSPECTED,
                                    f"'{col}' has {p['potential_outliers']} statistical outlier(s). Not treated as invalid.",
                                    "column", col, p["potential_outliers"],
                                    recommendation="Review; large values may be legitimate business events"))
        if p.get("date_order") in ("ambiguous", "conflict"):
            out.issues.append(Issue("ambiguous_date_format", Severity.WARNING, Kind.SUSPECTED,
                                    f"'{col}' has non-ISO dates whose day/month order is {p['date_order']}",
                                    "column", col, 0,
                                    recommendation="Configure date_format for this column; ambiguous dates are never auto-fixed"))
        if "type" not in declared and p["inferred_type"] in ("integer", "number", "boolean") \
                and 0 < p["invalid_for_inferred_type"] and p["confidence"] >= 0.9 and col in df.columns:
            nonmiss = df[col][~missing_mask(df[col])]
            _, valid = coerce_series(nonmiss, p["inferred_type"])
            bad_idx = valid[~valid].index
            if len(bad_idx):
                cause, rec = CAUSES["type_mismatch"]
                out.issues.append(Issue("inferred_type_mismatch", Severity.WARNING, Kind.SUSPECTED,
                                        f"{len(bad_idx)} value(s) in '{col}' do not fit the inferred type "
                                        f"{p['inferred_type']} (inferred, not declared)", "column", col, len(bad_idx),
                                        _ids(bad_idx), cause, "Declare the type in rules to enforce it; " + rec))
        if p["possible_role"] == "identifier" and 0.9 <= p["uniqueness_ratio"] < 1.0 \
                and ID_LIKE.search(col) and not declared.get("unique"):
            out.issues.append(Issue("possible_key_duplicates", Severity.INFO, Kind.INFERRED,
                                    f"'{col}' looks like an identifier but is not fully unique "
                                    f"(ratio {p['uniqueness_ratio']:.3f}). 'id' columns are not assumed to be keys.",
                                    "column", col, 0, recommendation="Declare a unique rule if it is a key"))


ID_LIKE = re.compile(r"(^id$|_id$|^id_)", re.I)


def _drift(out: CheckOutput, df: pd.DataFrame, profile: Dict[str, Any], base: Dict[str, Any],
           cfg: Dict[str, Any]) -> None:
    bcols, cur = base.get("columns", {}), profile["columns"]
    removed = [c for c in bcols if c not in cur]
    added = [c for c in cur if c not in bcols]
    for c in removed:
        sev = Severity.ERROR if cfg["fail_on_schema_drift"] else Severity.WARNING
        out.issues.append(Issue("schema_drift_removed", sev, Kind.CONFIRMED,
                                f"Column '{c}' existed in the approved baseline but is gone", "column", c,
                                source="baseline", probable_cause="Upstream schema change (dropped or renamed field)",
                                recommendation="Confirm the change; update the baseline if intentional"))
    for c in added:
        out.issues.append(Issue("schema_drift_added", Severity.WARNING, Kind.CONFIRMED,
                                f"New column '{c}' not in the approved baseline", "column", c, source="baseline",
                                probable_cause="Upstream added a field", recommendation="Review and approve a new baseline"))
    for c, b in bcols.items():
        if c not in cur:
            continue
        p = cur[c]
        if p["inferred_type"] != b["type"]:
            out.issues.append(Issue("schema_drift_type", Severity.WARNING, Kind.SUSPECTED,
                                    f"'{c}' inferred type changed {b['type']} -> {p['inferred_type']}", "column", c,
                                    source="baseline", probable_cause="Format change or contamination with bad values",
                                    recommendation="Inspect recent values of this column"))
        if abs(p["null_pct"] - b["null_pct"]) >= cfg["null_rate_shift"]:
            out.issues.append(Issue("null_rate_shift", Severity.WARNING, Kind.SUSPECTED,
                                    f"'{c}' null rate moved {b['null_pct']:.1%} -> {p['null_pct']:.1%}", "column", c,
                                    source="baseline", probable_cause="Upstream population or join change",
                                    recommendation="Check the producer of this column"))
        ns = p.get("numeric_stats")
        if ns and "mean" in b and b.get("std"):
            if abs(ns["mean"] - b["mean"]) > cfg["drift_std_multiplier"] * b["std"]:
                out.issues.append(Issue("distribution_drift", Severity.WARNING, Kind.SUSPECTED,
                                        f"'{c}' mean moved {b['mean']:.4g} -> {ns['mean']:.4g} "
                                        f"(> {cfg['drift_std_multiplier']} baseline std)", "column", c, source="baseline",
                                        probable_cause="Unit change, mix shift, or genuine business change",
                                        recommendation="Treat as a prompt to investigate, not as proof of bad data"))
    brows = base.get("rows")
    if brows:
        change = abs(len(df) - brows) / brows
        if change > cfg["row_count_change_pct"]:
            out.issues.append(Issue("row_count_change", Severity.WARNING, Kind.SUSPECTED,
                                    f"Row count {brows} -> {len(df)} ({change:.0%} change)", "dataset", source="baseline",
                                    probable_cause="Partial extract, duplicated load, or upstream filter change",
                                    recommendation="Compare with the source system's counts"))
