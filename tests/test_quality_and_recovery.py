import unittest

import pandas as pd

from autoflow import AutoFlow
from autoflow.contracts import ConfigError, RecoveryProposal
from autoflow.plugins import DomainPlugin
from autoflow.recovery import Fix, RecoveryRegistry, RecoveryRule
from autoflow.rules import RuleSet


def codes(r):
    return {i.code for i in r.issues}


class QualityTests(unittest.TestCase):
    def test_missing_required_column_blocks(self):
        rules = {"columns": {"a": {"type": "string"}, "b": {"type": "string"}}}
        r = AutoFlow(rules=rules).validate(pd.DataFrame({"a": ["x"]}), "d")
        self.assertIn("missing_required_column", codes(r))
        self.assertEqual(r.status, "FAILED")
        self.assertFalse(r.can_proceed)
        self.assertIsNone(r.approved_data)
        self.assertEqual(r.rows_approved, 0)

    def test_optional_column_may_be_absent(self):
        rules = {"columns": {"a": {"type": "string"}, "b": {"type": "string", "required": False}}}
        r = AutoFlow(rules=rules).validate(pd.DataFrame({"a": ["x"]}), "d")
        self.assertTrue(r.can_proceed)

    def test_missing_required_column_can_be_downgraded(self):
        af = AutoFlow({"validation": {"fail_on_missing_required_columns": False}},
                      rules={"columns": {"b": {"type": "string"}}})
        r = af.validate(pd.DataFrame({"a": ["x"]}), "d")
        self.assertEqual(r.status, "SUCCESS_WITH_WARNINGS")

    def test_type_violation_quarantine_and_partial_policy(self):
        df = pd.DataFrame({"amt": ["1", "2", "oops", "4"]})
        rules = {"columns": {"amt": {"type": "number", "nullable": False}}}
        r = AutoFlow(rules=rules).validate(df, "d")
        self.assertEqual((r.status, r.can_proceed, r.rows_quarantined, r.rows_approved), ("FAILED", False, 1, 0))
        self.assertIsNone(r.approved_data)
        self.assertEqual(list(r.quarantined_data.index), [2])
        af = AutoFlow({"validation": {"allow_partial_success": True}}, rules=rules)
        r = af.validate(df, "d")
        self.assertEqual((r.status, r.can_proceed, r.rows_quarantined, r.rows_approved),
                         ("PARTIAL_SUCCESS", True, 1, 3))
        self.assertEqual(list(r.approved_data.index), [0, 1, 3])

    def test_partial_success_respects_max_quarantine_pct(self):
        df = pd.DataFrame({"amt": ["1", "x", "y", "4"]})
        af = AutoFlow({"validation": {"allow_partial_success": True, "max_quarantine_pct": 25}},
                      rules={"columns": {"amt": {"type": "number"}}})
        self.assertFalse(af.validate(df, "d").can_proceed)

    def test_null_thresholds_warn_vs_fail(self):
        df = pd.DataFrame({"a": ["x", None, None, "y"]})
        r = AutoFlow(rules={"columns": {"a": {"null_warn_pct": 0.3}}}).validate(df, "d")
        self.assertEqual(r.status, "SUCCESS_WITH_WARNINGS")
        r = AutoFlow(rules={"columns": {"a": {"null_fail_pct": 0.3}}}).validate(df, "d")
        self.assertEqual(r.status, "FAILED")
        self.assertIn("null_rate_exceeded", codes(r))

    def test_empty_strings_and_whitespace_are_missing(self):
        df = pd.DataFrame({"a": ["x", "", "   ", None]})
        r = AutoFlow({"validation": {"allow_partial_success": True}},
                     rules={"columns": {"a": {"nullable": False}}}).validate(df, "d")
        self.assertEqual(r.rows_quarantined, 3)

    def test_unique_composite_and_exact_vs_conflicting_duplicates(self):
        df = pd.DataFrame({"k1": ["a", "a", "b", "b", "c"], "k2": ["1", "1", "1", "1", "1"],
                           "v": ["x", "x", "p", "q", "z"]})
        rules = {"checks": [{"type": "unique", "columns": ["k1", "k2"]}]}
        r = AutoFlow({"validation": {"allow_partial_success": True}}, rules=rules).validate(df, "d")
        # exact duplicate: only the later row flagged; conflicting duplicates: both flagged
        self.assertEqual(sorted(r.quarantined_data.index), [1, 2, 3])

    def test_nulls_in_key_not_treated_as_duplicates(self):
        df = pd.DataFrame({"k": [None, None, "a"]})
        r = AutoFlow(rules={"checks": [{"type": "unique", "columns": ["k"]}]}).validate(df, "d")
        self.assertNotIn("duplicate_key", codes(r))

    def test_range_allowed_pattern_length(self):
        df = pd.DataFrame({"n": ["5", "-1", "500"], "c": ["A", "B", "Z"], "p": ["ab1", "xx", "ab2"], "l": ["ok", "toolong", "ok"]})
        rules = {"columns": {"n": {"type": "integer", "min": 0, "max": 100}, "c": {"allowed": ["A", "B"]},
                             "p": {"pattern": "ab\\d"}, "l": {"max_length": 3}}}
        r = AutoFlow({"validation": {"allow_partial_success": True}}, rules=rules).validate(df, "d")
        self.assertEqual({"range_violation", "allowed_values_violation", "pattern_violation", "length_violation"},
                         codes(r) & {"range_violation", "allowed_values_violation", "pattern_violation", "length_violation"})
        self.assertEqual(r.rows_quarantined, 2)

    def test_warning_severity_never_quarantines(self):
        df = pd.DataFrame({"n": ["5", "-1"]})
        r = AutoFlow(rules={"columns": {"n": {"type": "integer", "min": 0, "severity": "warning"}}}).validate(df, "d")
        self.assertEqual((r.status, r.rows_quarantined), ("SUCCESS_WITH_WARNINGS", 0))

    def test_cross_column_compare_and_date_order(self):
        df = pd.DataFrame({"start": ["2024-01-05", "2024-01-05"], "end": ["2024-01-01", "2024-02-01"],
                           "lo": ["1", "9"], "hi": ["5", "5"]})
        rules = {"checks": [{"type": "compare", "left": "start", "op": "<=", "right": "end"},
                            {"type": "compare", "left": "lo", "op": "<", "right": "hi"},
                            {"type": "compare", "left": "hi", "op": ">=", "value": 5}]}
        r = AutoFlow({"validation": {"allow_partial_success": True}}, rules=rules).validate(df, "d")
        self.assertEqual(sorted(r.quarantined_data.index), [0, 1])
        self.assertEqual(r.rows_quarantined, 2)

    def test_reference_and_row_count(self):
        df = pd.DataFrame({"cid": ["1", "2", "9"]})
        ref = {"customers": pd.DataFrame({"id": ["1", "2", "3"]})}
        rules = {"checks": [{"type": "reference", "column": "cid", "dataset": "customers", "ref_column": "id"},
                            {"type": "row_count", "min": 5, "severity": "warning"}]}
        r = AutoFlow({"validation": {"allow_partial_success": True}}, rules=rules).validate(df, "d", reference_data=ref)
        self.assertEqual(list(r.quarantined_data.index), [2])
        self.assertIn("row_count_out_of_bounds", codes(r))
        r2 = AutoFlow(rules=rules).validate(df, "d")
        self.assertIn("reference_unavailable", codes(r2))
        self.assertEqual(r2.status, "FAILED")

    def test_no_rules_conservative_generic_checks(self):
        df = pd.DataFrame({"amt": [-5.0, 10.0, 12.0, 11.0, 9.0, 10.5, 9.5, 10.2, 9.8, 100000.0] * 3,
                           "name": ["a"] * 30, "e": [None] * 30})
        r = AutoFlow().validate(df, "d")
        self.assertTrue(r.can_proceed)                       # negatives/outliers/nulls are not errors
        self.assertEqual(r.rows_quarantined, 0)
        c = codes(r)
        self.assertIn("potential_outliers", c)
        self.assertIn("high_null_rate", c)
        self.assertIn("duplicate_rows", c)
        kinds = {i.code: i.kind for i in r.issues}
        self.assertEqual(kinds["potential_outliers"], "suspected")
        self.assertTrue(all(i.kind != "confirmed" for i in r.issues))   # without rules nothing is a confirmed violation

    def test_inferred_type_mismatch_is_suspected_not_enforced(self):
        df = pd.DataFrame({"n": [str(i) for i in range(30)] + ["oops"]})
        r = AutoFlow().validate(df, "d")
        i = next(i for i in r.issues if i.code == "inferred_type_mismatch")
        self.assertEqual(i.kind, "suspected")
        self.assertEqual(r.rows_quarantined, 0)

    def test_user_rule_overrides_inference(self):
        df = pd.DataFrame({"n": [str(i) for i in range(30)]})
        r = AutoFlow(rules={"columns": {"n": {"type": "string", "pattern": "\\d+"}}}).validate(df, "d")
        self.assertEqual(r.status, "SUCCESS")

    def test_empty_and_malformed_inputs(self):
        r = AutoFlow().validate(pd.DataFrame({"a": []}), "d")
        self.assertEqual((r.status, r.can_proceed), ("FAILED", False))
        self.assertIn("empty_input", codes(r))

    def test_invalid_rules_rejected_with_helpful_message(self):
        for bad in ({"columns": {"a": {"type": "wat"}}}, {"columns": {"a": {"min": 1}}},
                    {"columns": {"a": {"pattern": "("}}}, {"checks": [{"type": "compare", "left": "a"}]},
                    {"checks": [{"type": "nope"}]}, {"columns": {"a": {"bogus": 1}}}):
            with self.assertRaises(ConfigError):
                RuleSet.from_dict(bad)

    def test_json_nested_values_do_not_crash(self):
        df = pd.DataFrame({"a": [{"x": 1}, [1, 2], "s"]}, dtype=object)
        r = AutoFlow().validate(df, "d")
        self.assertTrue(r.can_proceed)


class RecoveryTests(unittest.TestCase):
    RULES = {"columns": {"qty": {"type": "integer", "nullable": False},
                         "status": {"allowed": ["Active", "Closed"]}}}

    def df(self):
        return pd.DataFrame({"qty": ["1", " 2 ", "3", "1,234,567"], "status": ["Active", "Closed", "Active", "Closed"]})

    def af(self, **rec):
        return AutoFlow({"recovery": {"enabled": True, **rec}, "validation": {"allow_partial_success": True}},
                        rules=self.RULES)

    def test_recovery_disabled_proposes_but_does_not_apply(self):
        r = AutoFlow({"validation": {"allow_partial_success": True}}, rules=self.RULES).validate(self.df(), "d")
        self.assertEqual(r.recovery_actions_applied, 0)
        self.assertEqual(r.recovery_proposals, 2)
        self.assertEqual({p.authorization for p in r.proposals}, {"recovery_disabled"})
        self.assertEqual(sorted(r.quarantined_data.index), [1, 3])

    def test_safe_repairs_applied_revalidated_and_original_preserved(self):
        src = self.df()
        before = src.copy()
        r = self.af().validate(src, "d")
        self.assertEqual(r.rows_quarantined, 0)
        self.assertEqual(r.status, "SUCCESS")
        self.assertEqual(list(r.approved_data["qty"]), ["1", "2", "3", "1234567"])
        self.assertEqual((r.recovery_actions_applied, r.revalidation_passed, r.revalidation_failed), (2, 2, 0))
        trim = next(p for p in r.proposals if p.rule_id == "trim_whitespace")
        self.assertEqual((trim.original, trim.proposed, trim.category, trim.status), (" 2 ", "2", "A", "revalidated"))
        pd.testing.assert_frame_equal(src, before)           # caller's data is never mutated

    def test_dry_run_does_not_mutate_or_apply(self):
        src = self.df()
        r = self.af().validate(src, "d", mode="dry_run")
        self.assertEqual(r.status, "DRY_RUN")
        self.assertEqual((r.recovery_actions_applied, r.rows_approved, r.rows_quarantined), (0, 0, 0))
        self.assertIsNone(r.approved_data)
        self.assertFalse(r.can_proceed)
        self.assertTrue(r.would_proceed)
        self.assertEqual(r.recovery_proposals, 2)
        # a dry run assesses the data as it stands; proposals show what recovery *would* change
        self.assertEqual((r.rows_would_approve, r.rows_would_quarantine), (2, 2))
        self.assertEqual(src.loc[1, "qty"], " 2 ")

    def test_dry_run_assessment_counts_without_recovery(self):
        r = AutoFlow(rules=self.RULES).validate(self.df(), "d", mode="dry_run")
        self.assertEqual((r.rows_would_approve, r.rows_would_quarantine, r.rows_approved), (2, 2, 0))
        self.assertFalse(r.would_proceed)

    def test_category_b_requires_approval(self):
        df = pd.DataFrame({"qty": ["1"], "status": ["active"]})
        r = self.af().validate(df, "d")
        self.assertEqual(r.rows_quarantined, 1)
        p = r.proposals[0]
        self.assertEqual((p.rule_id, p.category, p.authorization, p.status),
                         ("match_allowed_case", "B", "needs_approval", "pending_approval"))
        self.assertEqual(r.recovery_actions_applied, 0)
        r = self.af(approved_rules=["match_allowed_case"]).validate(df, "d")
        self.assertEqual((r.rows_quarantined, r.recovery_actions_applied), (0, 1))
        self.assertEqual(r.approved_data.loc[0, "status"], "Active")

    def test_approver_callback_accept_and_reject(self):
        df = pd.DataFrame({"qty": ["1"], "status": ["active"]})
        seen = []

        def yes(p: RecoveryProposal):
            seen.append(p.rule_id)
            return True
        af = AutoFlow({"recovery": {"enabled": True}}, rules=self.RULES, approver=yes)
        self.assertEqual(af.validate(df, "d").status, "SUCCESS")
        self.assertEqual(seen, ["match_allowed_case"])
        af = AutoFlow({"recovery": {"enabled": True}}, rules=self.RULES, approver=lambda p: False)
        r = af.validate(df, "d")
        self.assertEqual((r.status, r.proposals[0].status), ("FAILED", "rejected"))
        # dry-run never calls the approver
        seen.clear()
        AutoFlow({"recovery": {"enabled": True}}, rules=self.RULES, approver=yes).validate(df, "d", mode="dry_run")
        self.assertEqual(seen, [])

    def test_fill_default_changes_meaning_and_needs_approval(self):
        rules = {"columns": {"qty": {"type": "integer", "nullable": False, "default": "0"}}}
        df = pd.DataFrame({"qty": ["1", None]})
        r = AutoFlow({"recovery": {"enabled": True}}, rules=rules).validate(df, "d")
        p = r.proposals[0]
        self.assertTrue(p.changes_business_meaning and p.requires_approval)
        self.assertEqual(r.status, "FAILED")
        r = AutoFlow({"recovery": {"enabled": True, "approved_rules": ["fill_default"]}}, rules=rules).validate(df, "d")
        self.assertEqual(list(r.approved_data["qty"]), ["1", "0"])

    def test_rows_repaired_atomically(self):
        df = pd.DataFrame({"qty": [" 2 "], "status": ["nope"]})       # trim fixes qty, status has no remedy
        r = self.af().validate(df, "d")
        self.assertEqual(r.recovery_actions_applied, 0)
        self.assertEqual(r.quarantined_data.loc[0, "qty"], " 2 ")      # original values preserved in quarantine

    def test_failed_revalidation_goes_to_quarantine_with_original(self):
        class Bad(RecoveryRule):
            id, category, checks = "bad_rule", "A", ("type_mismatch",)

            def propose(self, v, ctx):
                return Fix("still-not-an-int", "intentionally wrong")
        reg = RecoveryRegistry([Bad()])
        af = AutoFlow({"recovery": {"enabled": True}, "validation": {"allow_partial_success": True}},
                      rules={"columns": {"qty": {"type": "integer"}}}, recovery_registry=reg)
        r = af.validate(pd.DataFrame({"qty": ["1", "abc"]}), "d")
        self.assertEqual((r.revalidation_failed, r.revalidation_passed, r.rows_quarantined), (1, 0, 1))
        self.assertEqual(r.proposals[0].status, "failed_revalidation")
        self.assertEqual(r.quarantined_data.loc[1, "qty"], "abc")
        self.assertIn("revalidation_failed", r.quarantined_data.loc[1, "_quarantine_reasons"])
        self.assertEqual(list(r.approved_data.index), [0])

    def test_numeric_ambiguity_never_guessed(self):
        rules = {"columns": {"n": {"type": "number"}}}
        df = pd.DataFrame({"n": ["1,5", "1,234", "1,234.50", "3.0"]})
        r = AutoFlow({"recovery": {"enabled": True}, "validation": {"allow_partial_success": True}}, rules=rules).validate(df, "d")
        self.assertEqual(sorted(r.quarantined_data.index), [0, 1])
        self.assertEqual(r.approved_data.loc[2, "n"], "1234.50")
        unresolved = [p for p in r.proposals if p.category == "C"]
        self.assertEqual(len(unresolved), 2)

    def test_dates_ambiguous_vs_evidenced_vs_configured(self):
        base = {"recovery": {"enabled": True}, "validation": {"allow_partial_success": True}}
        amb = pd.DataFrame({"d": ["01/02/2024", "2024-03-01"]})
        r = AutoFlow(base, rules={"columns": {"d": {"type": "date"}}}).validate(amb, "d")
        self.assertEqual((r.rows_quarantined, r.proposals[0].category), (1, "C"))
        evid = pd.DataFrame({"d": ["25/02/2024", "03/04/2024", "2024-03-01"]})
        r = AutoFlow({**base, "recovery": {"enabled": True, "approved_rules": ["normalize_date"]}},
                     rules={"columns": {"d": {"type": "date"}}}).validate(evid, "d")
        self.assertEqual(list(r.approved_data["d"]), ["2024-02-25", "2024-04-03", "2024-03-01"])
        self.assertEqual({p.category for p in r.proposals}, {"B"})
        cfg = AutoFlow(base, rules={"columns": {"d": {"type": "date", "normalize_from": ["%d/%m/%Y"]}}})
        r = cfg.validate(amb, "d")
        self.assertEqual(r.approved_data.loc[0, "d"], "2024-02-01")
        self.assertEqual(r.proposals[0].category, "A")

    def test_drop_exact_duplicates_needs_approval_and_is_traceable(self):
        df = pd.DataFrame({"k": ["a", "a", "b"], "v": ["1", "1", "2"]})
        rules = {"columns": {"k": {"unique": True}}}
        r = AutoFlow({"recovery": {"enabled": True}}, rules=rules).validate(df, "d")
        self.assertEqual((r.status, r.rows_quarantined), ("FAILED", 1))
        r = AutoFlow({"recovery": {"enabled": True, "approved_rules": ["drop_exact_duplicate"]}}, rules=rules).validate(df, "d")
        # the duplicate is removed (kept in quarantine), but quarantined rows still block unless partial success is allowed
        self.assertEqual((r.status, r.rows_quarantined, r.recovery_actions_applied), ("FAILED", 1, 1))
        r = AutoFlow({"recovery": {"enabled": True, "approved_rules": ["drop_exact_duplicate"]},
                      "validation": {"allow_partial_success": True}}, rules=rules).validate(df, "d")
        self.assertEqual((r.status, r.rows_approved, r.rows_quarantined), ("PARTIAL_SUCCESS", 2, 1))
        self.assertEqual(r.quarantined_data.loc[1, "_quarantine_reasons"], "duplicate_removed")

    def test_conflicting_duplicates_are_never_auto_resolved(self):
        df = pd.DataFrame({"k": ["a", "a"], "v": ["1", "2"]})
        r = AutoFlow({"recovery": {"enabled": True, "approved_rules": ["drop_exact_duplicate"]}},
                     rules={"columns": {"k": {"unique": True}}}).validate(df, "d")
        self.assertEqual(r.rows_quarantined, 2)
        self.assertEqual(r.recovery_actions_applied, 0)

    def test_recovery_rule_not_applicable_to_dataset(self):
        # string column: trimming offers nothing because nothing is invalid
        r = AutoFlow({"recovery": {"enabled": True}}, rules={"columns": {"s": {"type": "string"}}}).validate(
            pd.DataFrame({"s": [" a "]}), "d")
        self.assertEqual((r.recovery_proposals, r.status), (0, "SUCCESS"))

    def test_disabled_rule_is_skipped(self):
        r = self.af(disabled_rules=["trim_whitespace"]).validate(self.df(), "d")
        self.assertEqual(r.recovery_actions_applied, 1)         # only the numeric normalisation
        self.assertEqual(r.rows_quarantined, 1)

    def test_config_cannot_disable_revalidation_or_auto_apply_b(self):
        with self.assertRaises(ConfigError):
            AutoFlow({"recovery": {"revalidate_after_recovery": False}})
        with self.assertRaises(ConfigError):
            AutoFlow({"recovery": {"auto_apply_categories": ["A", "B"]}})


class PluginTests(unittest.TestCase):
    def retail(self):
        return pd.DataFrame({"InvoiceNo": ["536365", "C536379", "536366", "536367"],
                             "Quantity": ["6", "-1", "-3", "2"], "UnitPrice": ["2.5", "9.9", "1.0", "-4"]})

    def test_plugin_is_opt_in_and_domain_specific(self):
        df = self.retail()
        r = AutoFlow().validate(df, "d")                      # generic engine: negative numbers are not invalid
        self.assertEqual(r.rows_quarantined, 0)
        af = AutoFlow({"plugins": ["retail_invoices"], "validation": {"allow_partial_success": True}})
        r = af.validate(df, "d")
        self.assertEqual(sorted(r.quarantined_data.index), [2, 3])
        self.assertIn("retail_cancellations", codes(r))

    def test_plugin_not_applicable_to_other_data(self):
        r = AutoFlow({"plugins": ["retail_invoices"]}).validate(pd.DataFrame({"x": [-1, -2]}), "d")
        self.assertEqual((r.status, r.rows_quarantined), ("SUCCESS", 0))

    def test_unknown_plugin_and_custom_plugin_with_rule_precedence(self):
        with self.assertRaises(ConfigError):
            AutoFlow({"plugins": ["nope"]})

        class P(DomainPlugin):
            name = "p"

            def rules(self):
                return {"columns": {"a": {"type": "integer"}}}
        af = AutoFlow(plugins=[P()], rules={"columns": {"a": {"type": "string"}}})
        self.assertEqual(af.validate(pd.DataFrame({"a": ["x"]}), "d").status, "SUCCESS")   # user rule wins
        self.assertEqual(AutoFlow(plugins=[P()]).validate(pd.DataFrame({"a": ["x"]}), "d").status, "FAILED")


if __name__ == "__main__":
    unittest.main()
