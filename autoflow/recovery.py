"""Deterministic, rule-based recovery.

Detect -> Classify -> Diagnose -> Propose -> Authorize -> Recover -> Revalidate -> Route -> Audit

Categories
  A  safe automatic candidate (unambiguous, reversible, meaning-preserving); applied only if recovery is enabled
  B  needs approval (listed in recovery.approved_rules, or an ``approver`` callback says yes)
  C  never applied: ambiguous or no safe remedy -> quarantine
Every applied change is verified by revalidation; originals are always kept in the audit record.
"""
from __future__ import annotations

import abc
import datetime as _dt
import re
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Sequence, Tuple, Union

import pandas as pd

from .contracts import RecoveryProposal, RowViolation
from .quality import cell_is_valid
from .rules import RuleSet
from .typeops import as_str, date_order_evidence, slash_to_iso, missing_mask


@dataclass
class Unresolved:
    reason: str


@dataclass
class Fix:
    value: Any
    reason: str
    category: Optional[str] = None      # overrides the rule's default category
    action: str = "set_value"            # or drop_row


class RecoveryContext:
    def __init__(self, df: pd.DataFrame, rules: RuleSet):
        self.rules = rules
        self.date_evidence: Dict[str, str] = {}
        for col, r in rules.columns.items():
            if col in df.columns and r.get("type") in ("date", "datetime"):
                nm = df[col][~missing_mask(df[col])]
                self.date_evidence[col] = date_order_evidence(as_str(nm))

    def col_rule(self, col: Optional[str]) -> Dict[str, Any]:
        return self.rules.columns.get(col or "", {})


class RecoveryRule(abc.ABC):
    id = "abstract"
    version = "1"
    category = "B"
    reversible = True
    changes_business_meaning = False
    checks: Tuple[str, ...] = ()
    description = ""

    @abc.abstractmethod
    def propose(self, v: RowViolation, ctx: RecoveryContext) -> Union[Fix, Unresolved, None]: ...


class TrimWhitespace(RecoveryRule):
    id, category = "trim_whitespace", "A"
    checks = ("type_mismatch", "pattern", "allowed_values", "length")
    description = "Trim leading/trailing whitespace when that alone makes the value valid."

    def propose(self, v, ctx):
        if not isinstance(v.value, str) or v.value == v.value.strip():
            return None
        new = v.value.strip()
        if new and cell_is_valid(new, ctx.col_rule(v.column)):
            return Fix(new, "surrounding whitespace removed; result satisfies the column rule")
        return None


class NormalizeNumericString(RecoveryRule):
    id, category = "normalize_numeric_string", "A"
    checks = ("type_mismatch",)
    description = "Remove unambiguous thousands separators / trailing '.0' from numeric strings."
    _MULTI = re.compile(r"[+-]?\d{1,3}(?:,\d{3}){2,}(?:\.\d+)?")
    _ONE_WITH_DEC = re.compile(r"[+-]?\d{1,3},\d{3}\.\d+")
    _AMBIG = re.compile(r"[+-]?\d+,\d+")
    _TRAIL0 = re.compile(r"[+-]?\d+\.0+")

    def propose(self, v, ctx):
        rule = ctx.col_rule(v.column)
        if rule.get("type") not in ("integer", "number") or not isinstance(v.value, str):
            return None
        s = v.value.strip()
        new = None
        if self._MULTI.fullmatch(s) or self._ONE_WITH_DEC.fullmatch(s):
            new = s.replace(",", "")
        elif rule["type"] == "integer" and self._TRAIL0.fullmatch(s):
            new = s.split(".")[0]
        elif self._AMBIG.fullmatch(s):
            return Unresolved("comma could be a thousands separator or a decimal separator")
        if new is not None and cell_is_valid(new, rule):
            return Fix(new, "unambiguous numeric formatting normalised")
        return None


class NormalizeDate(RecoveryRule):
    id = "normalize_date"
    category = "A"
    checks = ("type_mismatch",)
    description = ("Category A when the column lists explicit 'normalize_from' formats and exactly one parses; "
                   "category B when day/month order is evidenced by other values; ambiguous dates are never fixed.")

    @staticmethod
    def _render(dt: _dt.datetime, rule: Dict[str, Any]) -> str:
        if rule.get("date_format"):
            return dt.strftime(rule["date_format"])
        if rule.get("type") == "datetime" and (dt.hour or dt.minute or dt.second):
            return dt.isoformat()
        return dt.date().isoformat()

    def propose(self, v, ctx):
        rule = ctx.col_rule(v.column)
        if rule.get("type") not in ("date", "datetime") or not isinstance(v.value, str):
            return None
        s = v.value.strip()
        fmts = rule.get("normalize_from") or []
        if fmts:
            outs = set()
            for f in fmts:
                try:
                    outs.add(self._render(_dt.datetime.strptime(s, f), rule))
                except ValueError:
                    continue
            if len(outs) == 1:
                new = outs.pop()
                if cell_is_valid(new, rule):
                    return Fix(new, "date parsed with an explicitly configured source format", "A")
            if len(outs) > 1:
                return Unresolved("value parses differently under the configured source formats")
            return None
        order = ctx.date_evidence.get(v.column, "none")
        if re.fullmatch(r"\d{1,2}[/.\-]\d{1,2}[/.\-]\d{2,4}", s):
            if order in ("dmy", "mdy"):
                iso = slash_to_iso(s, order)
                if iso and cell_is_valid(iso, rule):
                    return Fix(iso, f"day/month order '{order}' inferred from other values in the column", "B")
                return Unresolved("date could not be converted safely (invalid date or 2-digit year)")
            return Unresolved(f"day/month order is {order}; set normalize_from to resolve")
        return None


class MatchAllowedCase(RecoveryRule):
    id, category = "match_allowed_case", "B"
    checks = ("allowed_values",)
    description = "Map a value to the single allowed value that matches it ignoring case/whitespace."

    def propose(self, v, ctx):
        allowed = [str(a) for a in ctx.col_rule(v.column).get("allowed", [])]
        if not isinstance(v.value, str):
            return None
        hits = [a for a in allowed if a.strip().lower() == v.value.strip().lower()]
        return Fix(hits[0], "matches one allowed value ignoring case/whitespace") if len(hits) == 1 else None


class FillDefault(RecoveryRule):
    id, category = "fill_default", "B"
    changes_business_meaning = True
    checks = ("not_null",)
    description = "Fill a missing value with the column's configured 'default'. Changes business meaning."

    def propose(self, v, ctx):
        rule = ctx.col_rule(v.column)
        if "default" not in rule or not cell_is_valid(rule["default"], rule):
            return None
        return Fix(rule["default"], "missing value replaced by the configured default")


class DropExactDuplicate(RecoveryRule):
    id, category = "drop_exact_duplicate", "B"
    checks = ("duplicate_key",)
    description = "Remove a later row identical in every column to an earlier one (kept in quarantine as 'duplicate_removed')."

    def propose(self, v, ctx):
        if v.detail.get("exact_duplicate"):
            return Fix(None, "row is identical to an earlier row in every column", action="drop_row")
        return None


class RecoveryRegistry:
    def __init__(self, rules: Optional[Sequence[RecoveryRule]] = None):
        self.rules: List[RecoveryRule] = list(rules if rules is not None else [
            TrimWhitespace(), NormalizeNumericString(), NormalizeDate(), MatchAllowedCase(), FillDefault(),
            DropExactDuplicate()])

    def register(self, rule: RecoveryRule) -> None:
        self.rules.append(rule)

    def signature(self) -> str:
        return ",".join(f"{r.id}@{r.version}" for r in sorted(self.rules, key=lambda r: r.id))


Approver = Callable[[RecoveryProposal], bool]


class RecoveryPlanner:
    def __init__(self, registry: RecoveryRegistry, cfg: Dict[str, Any], approver: Optional[Approver] = None):
        self.registry, self.cfg, self.approver = registry, cfg, approver

    def plan(self, violations: List[RowViolation], df: pd.DataFrame, rules: RuleSet, id_prefix: str,
             can_apply: bool) -> Tuple[List[RecoveryProposal], Dict[Any, bool]]:
        """Propose a remedy for every row-level error violation.

        Returns (proposals, repairable) where repairable[row_id] is True only if *every* violation
        on the row has an authorized, applicable proposal (rows are repaired atomically or not at all).
        """
        ctx = RecoveryContext(df, rules)
        disabled = set(self.cfg.get("disabled_rules", []))
        proposals: List[RecoveryProposal] = []
        by_row: Dict[Any, List[RecoveryProposal]] = {}
        for i, v in enumerate(violations):
            prop: Optional[RecoveryProposal] = None
            for r in self.registry.rules:
                if r.id in disabled or v.check not in r.checks:
                    continue
                res = r.propose(v, ctx)
                if res is None:
                    continue
                pid = f"{id_prefix}-{i + 1}"
                if isinstance(res, Unresolved):
                    prop = RecoveryProposal(pid, r.id, r.version, "C", v.row_id, v.column, v.value, None, "none",
                                            res.reason, r.reversible, r.changes_business_meaning, False,
                                            "unresolved", "unresolved")
                else:
                    cat = res.category or r.category
                    prop = RecoveryProposal(pid, r.id, r.version, cat, v.row_id, v.column, v.value, res.value,
                                            res.action, res.reason, r.reversible, r.changes_business_meaning,
                                            cat == "B")
                    self._authorize(prop, can_apply)
                break
            if prop is None:
                prop = RecoveryProposal(f"{id_prefix}-{i + 1}", "none", "0", "C", v.row_id, v.column, v.value, None,
                                        "none", f"no safe deterministic remedy for '{v.check}'", False, False, False,
                                        "unresolved", "unresolved")
            proposals.append(prop)
            by_row.setdefault(v.row_id, []).append(prop)
        repairable = {rid: all(p.authorization == "authorized" and p.action != "none" for p in ps)
                      for rid, ps in by_row.items()}
        return proposals, repairable

    def _authorize(self, p: RecoveryProposal, can_apply: bool) -> None:
        if not self.cfg.get("enabled"):
            p.authorization, p.status = "recovery_disabled", "proposed"
            return
        if p.category == "A" and "A" in self.cfg.get("auto_apply_categories", ["A"]):
            p.authorization = "authorized"
        elif p.category == "B":
            if p.rule_id in self.cfg.get("approved_rules", []):
                p.authorization = "authorized"
            elif self.approver is not None and can_apply:
                p.authorization = "authorized" if self.approver(p) else "rejected"
            else:
                p.authorization = "needs_approval"
        else:
            p.authorization = "unresolved"
        p.status = {"authorized": "proposed", "needs_approval": "pending_approval", "rejected": "rejected",
                    "unresolved": "unresolved"}.get(p.authorization, "proposed")
