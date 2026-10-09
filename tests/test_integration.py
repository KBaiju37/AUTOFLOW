import contextlib
import io
import json
import os
import sqlite3
import tempfile
import unittest
from unittest import mock

import pandas as pd

from autoflow import AutoFlow, QualityGateFailed
from autoflow.cli import main as cli_main
from autoflow.config import Config
from autoflow.contracts import ConfigError
from autoflow.store import Store, StoreError
from pathlib import Path

RULES = {"dataset": "orders", "columns": {
    "order_id": {"type": "string", "nullable": False, "unique": True},
    "amount": {"type": "number", "nullable": False, "min": 0},
    "status": {"allowed": ["new", "paid"]}}}


def orders(bad=0):
    rows = [{"order_id": f"{i:04d}", "amount": str(10 + i), "status": "paid"} for i in range(1, 11)]
    for i in range(bad):
        rows[i]["amount"] = "n/a"
    return pd.DataFrame(rows)


class Tmp(unittest.TestCase):
    def setUp(self):
        self._td = tempfile.TemporaryDirectory()
        self.dir = self._td.name
        self.addCleanup(self._td.cleanup)

    def p(self, *parts):
        return os.path.join(self.dir, *parts)

    def cfg(self, **over):
        base = {"audit": {"enabled": True, "path": self.p("af.db")},
                "output": {"type": "sqlite_staging", "enabled": True, "path": self.p("stage.db")},
                "validation": {"allow_partial_success": False}}
        for k, v in over.items():
            base[k] = {**base.get(k, {}), **v}
        return base


class PythonApiAndPipelineTests(Tmp):
    def etl(self, af, df):
        """A tiny 'existing pipeline': extract -> transform -> [AutoFlow gate] -> load."""
        loaded = []
        transformed = df.assign(amount=df["amount"])
        result = af.validate(transformed, "orders")
        if result.can_proceed:
            loaded.extend(result.approved_data.to_dict("records"))
        return result, loaded

    def test_pipeline_continues_after_successful_validation(self):
        result, loaded = self.etl(AutoFlow(rules=RULES), orders())
        self.assertTrue(result.can_proceed)
        self.assertEqual((result.status, len(loaded), result.rows_approved), ("SUCCESS", 10, 10))

    def test_pipeline_blocks_on_failure(self):
        result, loaded = self.etl(AutoFlow(rules=RULES), orders(bad=2))
        self.assertFalse(result.can_proceed)
        self.assertEqual(loaded, [])
        self.assertEqual(result.rows_quarantined, 2)

    def test_pipeline_partial_load_when_policy_allows(self):
        af = AutoFlow({"validation": {"allow_partial_success": True}}, rules=RULES)
        result, loaded = self.etl(af, orders(bad=2))
        self.assertEqual((result.status, len(loaded)), ("PARTIAL_SUCCESS", 8))

    def test_success_does_not_mean_safe_when_data_is_bad(self):
        r = AutoFlow(rules=RULES).validate(orders(bad=1), "orders")
        self.assertEqual(r.errors, [])                      # execution itself did not crash...
        self.assertFalse(r.can_proceed)                     # ...but the data may not proceed

    def test_gate_decorator(self):
        af = AutoFlow(rules=RULES)

        @af.gate("orders")
        def extract_good():
            return orders()

        @af.gate("orders")
        def extract_bad():
            return orders(bad=1)
        self.assertEqual(len(extract_good()), 10)
        with self.assertRaises(QualityGateFailed) as cm:
            extract_bad()
        self.assertEqual(cm.exception.result.rows_quarantined, 1)

    def test_records_callable_and_source_inputs(self):
        af = AutoFlow(rules=RULES)
        recs = orders().to_dict("records")
        self.assertTrue(af.validate(recs, "o").can_proceed)
        self.assertTrue(af.validate(lambda: orders(), "o").can_proceed)
        with self.assertRaises(ConfigError):
            af.validate(12345, "o")

    def test_result_contract_is_json_serialisable(self):
        r = AutoFlow(rules=RULES).validate(orders(bad=1), "orders")
        d = json.loads(json.dumps(r.to_dict()))
        for k in ("run_id", "status", "mode", "rows_processed", "rows_approved", "rows_quarantined", "issues_detected",
                  "recovery_proposals", "recovery_actions_applied", "revalidation_passed", "revalidation_failed",
                  "can_proceed", "errors", "warnings"):
            self.assertIn(k, d)

    def test_monitor_mode_changes_nothing(self):
        cfg = self.cfg(recovery={"enabled": True})
        af = AutoFlow(cfg, rules=RULES)
        df = orders(bad=1)
        df.loc[0, "status"] = " paid"
        before = df.copy()
        r = af.monitor(df, "orders")
        pd.testing.assert_frame_equal(df, before)
        self.assertEqual((r.recovery_actions_applied, r.rows_approved, r.rows_quarantined), (0, 0, 0))
        self.assertIsNone(r.approved_data)
        self.assertTrue(r.issues)
        counts = Store(self.p("stage.db")).counts()
        self.assertEqual((counts["staged_rows"], counts["quarantine_rows"], counts["batches"]), (0, 0, 0))
        self.assertEqual(counts["runs"], 0)                 # stage.db is a separate file from the audit db
        self.assertEqual(af.audit_store.counts()["runs"], 1)

    def test_engine_never_raises_into_host_pipeline(self):
        with mock.patch("autoflow.engine.run_checks", side_effect=RuntimeError("boom password=hunter2")):
            r = AutoFlow().validate(orders(), "o")
        self.assertEqual((r.status, r.can_proceed), ("FAILED", False))
        self.assertNotIn("hunter2", " ".join(r.errors))

    def test_source_failure_returns_failed_result(self):
        r = AutoFlow(self.cfg()).run() if False else None
        af = AutoFlow({"source": {"type": "csv", "path": self.p("missing.csv")}, **self.cfg()})
        r = af.run(mode="gate")
        self.assertEqual((r.status, r.can_proceed), ("FAILED", False))
        self.assertTrue(r.errors)


class ConfigTests(Tmp):
    def test_validation_messages(self):
        bad = [{"autoflow": {"mode": "yolo"}}, {"output": {"type": "sqlite_staging", "enabled": True}},
               {"output": {"type": "s3"}}, {"bogus": {}}, {"recovery": {"nonsense": 1}},
               {"source": {"type": "nosuch"}}, {"validation": {"max_quarantine_pct": 200}},
               {"output": {"enabled": True}}]
        for b in bad:
            with self.assertRaises(ConfigError, msg=str(b)):
                Config.load(b)

    def test_yaml_file_with_relative_paths(self):
        Path(self.p("d.csv")).write_text("order_id,amount,status\n1,5,new\n", encoding="utf-8")
        Path(self.p("rules.yaml")).write_text(
            "dataset: orders\ncolumns:\n  amount: {type: number, min: 0}\n", encoding="utf-8")
        Path(self.p("autoflow.yaml")).write_text(
            "autoflow: {name: orders, mode: gate}\nsource: {type: csv, path: d.csv}\nrules_file: rules.yaml\n"
            "audit: {enabled: true, path: audit.db}\n", encoding="utf-8")
        af = AutoFlow.from_config(self.p("autoflow.yaml"))
        r = af.run()
        self.assertEqual(r.status, "SUCCESS")
        self.assertTrue(os.path.exists(self.p("audit.db")))

    def test_corrupt_yaml(self):
        Path(self.p("x.yaml")).write_text("a: [unclosed", encoding="utf-8")
        with self.assertRaises(ConfigError):
            Config.load(self.p("x.yaml"))


class AuditIdempotencyTests(Tmp):
    def test_audit_trail_and_traceability(self):
        af = AutoFlow(self.cfg(recovery={"enabled": True}), rules=RULES)
        df = orders()
        df.loc[2, "amount"] = " 12 "
        df.loc[3, "amount"] = "zzz"
        r = af.validate(df, "orders")
        rep = af.report(r.run_id)
        events = [e["event"] for e in rep["events"]]
        for e in ("run_started", "profiled", "checks_completed", "recovery_planned", "recovery_applied",
                  "revalidated", "routed", "output", "run_finished"):
            self.assertIn(e, events)
        log = {x["row_id"]: x for x in rep["recovery_log"] if x["rule_id"] != "none"}
        self.assertEqual((log["2"]["original"], log["2"]["proposed"], log["2"]["status"]), (" 12 ", "12", "revalidated"))
        self.assertEqual(rep["run"]["status"], "FAILED")
        q = af.load_quarantine(r.run_id)
        self.assertEqual(list(q.index), ["3"])
        self.assertEqual(q.loc["3", "amount"], "zzz")              # original value, traceable by source row id
        self.assertIn("type_mismatch:amount", q.loc["3", "_quarantine_reasons"])

    def test_redacted_audit_values(self):
        af = AutoFlow(self.cfg(recovery={"enabled": True}, audit={"redact_values": True}), rules=RULES)
        df = orders()
        df.loc[0, "amount"] = " 77 "
        r = af.validate(df, "orders")
        row = next(x for x in af.report(r.run_id)["recovery_log"] if x["rule_id"] == "trim_whitespace")
        self.assertTrue(row["original"].startswith("sha256:"))
        self.assertNotIn("77", json.dumps(af.report(r.run_id)["run"]["proposals"]))

    def test_dry_run_writes_no_staging_but_is_audited(self):
        af = AutoFlow(self.cfg(), rules=RULES)
        r = af.validate(orders(bad=1), "orders", mode="dry_run")
        c = Store(self.p("stage.db")).counts()
        self.assertEqual((c["staged_rows"], c["quarantine_rows"], c["batches"]), (0, 0, 0))
        self.assertEqual(af.report(r.run_id)["run"]["status"], "DRY_RUN")

    def test_retry_same_input_does_not_duplicate_output(self):
        af = AutoFlow(self.cfg(), rules=RULES)
        r1 = af.validate(orders(), "orders")
        r2 = af.validate(orders(), "orders")                       # same logical input and config
        self.assertNotEqual(r1.run_id, r2.run_id)                   # audit rows are never overwritten
        self.assertEqual(r1.run_key, r2.run_key)
        self.assertTrue(r2.idempotent_replay)
        self.assertEqual(r2.replay_of, r1.run_id)
        self.assertEqual(r2.source_changed_since_last_run, False)
        c = Store(self.p("stage.db")).counts()
        self.assertEqual((c["staged_rows"], c["batches"]), (10, 1))
        self.assertEqual(af.audit_store.counts()["runs"], 2)

    def test_changed_source_is_a_new_batch(self):
        af = AutoFlow(self.cfg(), rules=RULES)
        af.validate(orders(), "orders")
        changed = orders()
        changed.loc[0, "amount"] = "999"
        r = af.validate(changed, "orders")
        self.assertFalse(r.idempotent_replay)
        self.assertTrue(r.source_changed_since_last_run)
        self.assertEqual(Store(self.p("stage.db")).counts()["batches"], 2)

    def test_changed_config_is_a_new_batch(self):
        af1 = AutoFlow(self.cfg(), rules=RULES)
        af2 = AutoFlow(self.cfg(validation={"allow_partial_success": True}), rules=RULES)
        a, b = af1.validate(orders(), "orders"), af2.validate(orders(), "orders")
        self.assertNotEqual(a.run_key, b.run_key)

    def test_identical_rows_in_input_are_not_collapsed(self):
        df = pd.DataFrame({"a": ["x", "x"], "b": ["1", "1"]})
        af = AutoFlow(self.cfg())
        r = af.validate(df, "d")
        self.assertEqual(r.rows_approved, 2)
        self.assertEqual(Store(self.p("stage.db")).counts()["staged_rows"], 2)

    def test_failed_output_write_rolls_back_and_retry_commits_once(self):
        af = AutoFlow(self.cfg(), rules=RULES)
        with mock.patch.object(Store, "_insert_staged", side_effect=sqlite3.OperationalError("disk full")):
            r = af.validate(orders(), "orders")
        self.assertEqual((r.status, r.can_proceed), ("FAILED", False))
        self.assertIsNone(r.approved_data)
        self.assertTrue(any("rolled back" in e for e in r.errors))
        c = Store(self.p("stage.db")).counts()
        self.assertEqual((c["batches"], c["staged_rows"], c["quarantine_rows"]), (0, 0, 0))
        self.assertEqual(af.report(r.run_id)["run"]["status"], "FAILED")      # failure itself is audited
        r2 = af.validate(orders(), "orders")                                    # retry
        self.assertTrue(r2.can_proceed)
        self.assertFalse(r2.idempotent_replay)
        c = Store(self.p("stage.db")).counts()
        self.assertEqual((c["batches"], c["staged_rows"]), (1, 10))

    def test_quarantine_written_atomically_with_staging(self):
        af = AutoFlow(self.cfg(validation={"allow_partial_success": True}), rules=RULES)
        r = af.validate(orders(bad=2), "orders")
        c = Store(self.p("stage.db")).counts()
        self.assertEqual((r.status, c["staged_rows"], c["quarantine_rows"]), ("PARTIAL_SUCCESS", 8, 2))
        staged = Store(self.p("stage.db")).load_staged(r.run_key)
        self.assertEqual(len(staged), 8)
        self.assertNotIn("1", set(staged.index))            # quarantined source rows are not in staging

    def test_blocked_run_stages_nothing_but_keeps_quarantine_for_inspection(self):
        af = AutoFlow(self.cfg(), rules=RULES)
        r = af.validate(orders(bad=2), "orders")
        c = Store(self.p("stage.db")).counts()
        self.assertEqual((r.can_proceed, c["staged_rows"], c["quarantine_rows"]), (False, 0, 2))

    def test_reprocessing_quarantined_rows(self):
        af = AutoFlow(self.cfg(), rules=RULES)
        r = af.validate(orders(bad=2), "orders")
        q = af.load_quarantine(r.run_id).drop(columns="_quarantine_reasons")
        q["amount"] = "5"                                       # upstream corrected the values
        again = af.validate(q, "orders_reprocessed")
        self.assertTrue(again.can_proceed)

    def test_local_files_output_idempotent(self):
        cfg = self.cfg(output={"type": "local_files", "enabled": True, "directory": self.p("out"), "path": None})
        af = AutoFlow(cfg, rules=RULES)
        r1 = af.validate(orders(), "orders")
        r2 = af.validate(orders(), "orders")
        files = sorted(os.listdir(self.p("out")))
        self.assertEqual(len(files), 2)  # approved CSV + manifest commit marker
        self.assertTrue(r1.output["staged"])
        self.assertTrue(os.path.exists(r1.output["manifest"]))
        self.assertTrue(r2.idempotent_replay)

    def test_local_files_writes_quarantine_even_when_gate_blocks(self):
        cfg = self.cfg(output={"type": "local_files", "enabled": True,
                               "directory": self.p("blocked_out"), "path": None})
        af = AutoFlow(cfg, rules=RULES)
        result = af.validate(orders(bad=2), "orders")
        self.assertFalse(result.can_proceed)
        files = os.listdir(self.p("blocked_out"))
        self.assertEqual(len(files), 2)  # quarantine CSV + manifest; no approved CSV
        self.assertTrue(any(name.endswith("_quarantine.csv") for name in files))
        self.assertFalse(any(name.endswith("_approved.csv") for name in files))
        self.assertTrue(any(name.endswith("_manifest.json") for name in files))

    def test_local_files_sanitizes_dataset_name_for_output_paths(self):
        cfg = self.cfg(output={"type": "local_files", "enabled": True,
                               "directory": self.p("safe_out"), "path": None})
        result = AutoFlow(cfg, rules=RULES).validate(orders(), "../../outside\\name")
        self.assertTrue(result.can_proceed)
        self.assertEqual(os.path.dirname(result.output["path"]), self.p("safe_out"))

    def test_output_disabled_by_default_writes_nothing(self):
        af = AutoFlow({"audit": {"enabled": True, "path": self.p("only_audit.db")}}, rules=RULES)
        r = af.validate(orders(), "orders")
        self.assertTrue(r.can_proceed)
        self.assertEqual(r.output["rows_staged"], 0)
        self.assertEqual(os.listdir(self.dir), ["only_audit.db"])

    def test_store_load_quarantine_unknown_run(self):
        with self.assertRaises(StoreError):
            Store(self.p("s.db")).load_quarantine("nope")


class CliTests(Tmp):
    def run_cli(self, *args):
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = cli_main(list(args))
        return code, out.getvalue(), err.getvalue()

    def setUp(self):
        super().setUp()
        pd.DataFrame({"order_id": ["0001", "0002", "0003"], "amount": ["5", "6", "x"], "status": ["new", "paid", "paid"]}
                     ).to_csv(self.p("d.csv"), index=False)
        pd.DataFrame({"order_id": ["0001"], "amount": ["5"], "status": ["new"]}).to_csv(self.p("ok.csv"), index=False)
        with open(self.p("rules.json"), "w") as fh:
            json.dump(RULES, fh)

    def test_profile(self):
        code, out, _ = self.run_cli("profile", "--input", self.p("d.csv"))
        self.assertEqual(code, 0)
        self.assertIn("order_id", out)
        code, out, _ = self.run_cli("profile", "--input", self.p("d.csv"), "--json")
        self.assertEqual(json.loads(out)["columns"]["order_id"]["inferred_type"], "string")

    def test_validate_exit_codes(self):
        self.assertEqual(self.run_cli("validate", "--input", self.p("ok.csv"), "--rules", self.p("rules.json"))[0], 0)
        code, out, _ = self.run_cli("validate", "--input", self.p("d.csv"), "--rules", self.p("rules.json"), "--json")
        self.assertEqual(code, 1)
        self.assertEqual(json.loads(out)["rows_quarantined"], 1)

    def test_usage_errors_exit_2(self):
        code, _, err = self.run_cli("validate", "--input", self.p("nope.csv"))
        self.assertEqual(code, 2)
        self.assertIn("not found", err)
        self.assertEqual(self.run_cli("validate", "--input", self.p("d.csv"), "--rules", self.p("nope.json"))[0], 2)

    def write_cfg(self, recovery=False):
        pd.DataFrame({"order_id": ["0001", "0002"], "amount": [" 5 ", "6"], "status": ["new", "paid"]}).to_csv(
            self.p("r.csv"), index=False)
        cfg = {"autoflow": {"name": "orders", "mode": "gate"}, "source": {"type": "csv", "path": self.p("r.csv")},
               "rules": RULES, "recovery": {"enabled": recovery},
               "output": {"type": "sqlite_staging", "enabled": True, "path": self.p("stage.db")},
               "audit": {"enabled": True, "path": self.p("af.db")}}
        with open(self.p("autoflow.json"), "w") as fh:
            json.dump(cfg, fh)
        return self.p("autoflow.json")

    def test_run_is_dry_by_default_and_commit_needs_flag(self):
        cfg = self.write_cfg()
        code, out, _ = self.run_cli("run", "--config", cfg, "--json")
        d = json.loads(out)
        self.assertEqual((d["status"], d["mode"]), ("DRY_RUN", "dry_run"))
        self.assertEqual(Store(self.p("stage.db")).counts()["staged_rows"], 0)
        code, out, _ = self.run_cli("run", "--config", cfg, "--commit", "--json")
        self.assertEqual(code, 1)                                   # " 5 " is invalid and recovery is not enabled
        self.assertEqual(Store(self.p("stage.db")).counts()["staged_rows"], 0)

    def test_run_with_recovery_then_report(self):
        cfg = self.write_cfg()
        code, out, _ = self.run_cli("run", "--config", cfg, "--enable-recovery", "--json")
        d = json.loads(out)
        self.assertEqual((code, d["status"], d["recovery_actions_applied"]), (0, "SUCCESS", 1))
        self.assertEqual(Store(self.p("stage.db")).counts()["staged_rows"], 2)
        code, out, _ = self.run_cli("report", "--run-id", d["run_id"], "--config", cfg)
        self.assertEqual(code, 0)
        self.assertIn("Audit events", out)
        self.assertEqual(self.run_cli("report", "--run-id", "missing", "--config", cfg)[0], 2)
        # re-running the identical logical input does not duplicate staged rows
        self.run_cli("run", "--config", cfg, "--enable-recovery", "--json")
        self.assertEqual(Store(self.p("stage.db")).counts()["staged_rows"], 2)

    def test_connectors_listing(self):
        code, out, _ = self.run_cli("connectors", "--json")
        types = {r["type"] for r in json.loads(out)}
        self.assertTrue({"csv", "json", "jsonl", "excel", "parquet", "sqlite", "sql", "rest_api"} <= types)


if __name__ == "__main__":
    unittest.main()
