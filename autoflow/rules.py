"""User-configured rules (YAML/JSON). Rules are explicit business truth; they never come from inference."""
from __future__ import annotations

import copy
import json
import re
from typing import Any, Dict, List, Optional

from .contracts import ConfigError
from .typeops import VALID_TYPES

COLUMN_KEYS = {
    "type", "nullable", "required", "min", "max", "min_length", "max_length", "pattern",
    "allowed", "unique", "null_warn_pct", "null_fail_pct", "severity", "date_format",
    "default", "description", "normalize_from",
}
CHECK_TYPES = {"unique", "not_null", "compare", "row_count", "reference"}
COMPARE_OPS = {"<", "<=", ">", ">=", "==", "!="}
SEVERITIES = {"error", "warning"}


def load_structured(path: str) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as fh:
            text = fh.read()
    except OSError as exc:
        raise ConfigError(f"Cannot read '{path}': {exc.strerror or exc}") from exc
    if path.lower().endswith(".json"):
        try:
            data = json.loads(text)
        except ValueError as exc:
            raise ConfigError(f"Invalid JSON in '{path}': {exc}") from exc
    else:
        try:
            import yaml
        except ImportError as exc:  # pragma: no cover
            raise ConfigError("PyYAML is required for YAML files (pip install pyyaml) or use JSON") from exc
        try:
            data = yaml.safe_load(text)
        except yaml.YAMLError as exc:
            raise ConfigError(f"Invalid YAML in '{path}': {exc}") from exc
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise ConfigError(f"'{path}' must contain a mapping at the top level")
    return data


class RuleSet:
    def __init__(self, dataset: Optional[str] = None, columns: Optional[Dict[str, Dict[str, Any]]] = None,
                 checks: Optional[List[Dict[str, Any]]] = None, allow_extra_columns: bool = True):
        self.dataset = dataset
        self.columns = columns or {}
        self.checks = checks or []
        self.allow_extra_columns = allow_extra_columns

    # ------------------------------------------------------------------ construction
    @classmethod
    def empty(cls) -> "RuleSet":
        return cls()

    @classmethod
    def from_dict(cls, raw: Optional[Dict[str, Any]]) -> "RuleSet":
        raw = copy.deepcopy(raw or {})
        rs = cls(
            dataset=raw.get("dataset"),
            columns=raw.get("columns") or {},
            checks=raw.get("checks") or [],
            allow_extra_columns=bool(raw.get("allow_extra_columns", True)),
        )
        unknown = set(raw) - {"dataset", "columns", "checks", "allow_extra_columns"}
        errors = [f"unknown top-level rule key '{k}'" for k in sorted(unknown)]
        errors += rs.problems()
        if errors:
            raise ConfigError("Invalid rules: " + "; ".join(errors))
        return rs

    @classmethod
    def from_file(cls, path: str) -> "RuleSet":
        return cls.from_dict(load_structured(path))

    # ------------------------------------------------------------------ validation
    def problems(self) -> List[str]:
        errs: List[str] = []
        if not isinstance(self.columns, dict):
            return ["'columns' must be a mapping"]
        for name, rule in self.columns.items():
            if not isinstance(rule, dict):
                errs.append(f"column '{name}': rule must be a mapping")
                continue
            for k in rule:
                if k not in COLUMN_KEYS:
                    errs.append(f"column '{name}': unknown key '{k}'")
            t = rule.get("type")
            if t is not None and t not in VALID_TYPES:
                errs.append(f"column '{name}': type '{t}' not in {list(VALID_TYPES)}")
            if rule.get("severity", "error") not in SEVERITIES:
                errs.append(f"column '{name}': severity must be one of {sorted(SEVERITIES)}")
            if ("min" in rule or "max" in rule) and t not in ("integer", "number", "datetime", "date"):
                errs.append(f"column '{name}': min/max require type integer, number, date or datetime")
            if "min" in rule and "max" in rule:
                try:
                    if rule["min"] > rule["max"]:
                        errs.append(f"column '{name}': min is greater than max")
                except TypeError:
                    pass
            if "pattern" in rule:
                try:
                    re.compile(str(rule["pattern"]))
                except re.error as exc:
                    errs.append(f"column '{name}': invalid pattern ({exc})")
            if "allowed" in rule and not isinstance(rule["allowed"], list):
                errs.append(f"column '{name}': 'allowed' must be a list")
            for k in ("null_warn_pct", "null_fail_pct"):
                if k in rule and not (isinstance(rule[k], (int, float)) and 0 <= rule[k] <= 1):
                    errs.append(f"column '{name}': {k} must be a fraction between 0 and 1")
        if not isinstance(self.checks, list):
            return errs + ["'checks' must be a list"]
        for i, chk in enumerate(self.checks):
            tag = f"check #{i + 1}"
            if not isinstance(chk, dict) or "type" not in chk:
                errs.append(f"{tag}: must be a mapping with a 'type'")
                continue
            ct = chk["type"]
            if ct not in CHECK_TYPES:
                errs.append(f"{tag}: unknown type '{ct}' (supported: {sorted(CHECK_TYPES)})")
                continue
            if chk.get("severity", "error") not in SEVERITIES:
                errs.append(f"{tag}: severity must be one of {sorted(SEVERITIES)}")
            if ct in ("unique", "not_null") and not (isinstance(chk.get("columns"), list) and chk["columns"]):
                errs.append(f"{tag} ({ct}): 'columns' must be a non-empty list")
            if ct == "compare":
                if chk.get("op") not in COMPARE_OPS:
                    errs.append(f"{tag} (compare): op must be one of {sorted(COMPARE_OPS)}")
                if "left" not in chk or ("right" not in chk and "value" not in chk):
                    errs.append(f"{tag} (compare): needs 'left' and either 'right' or 'value'")
            if ct == "row_count" and "min" not in chk and "max" not in chk:
                errs.append(f"{tag} (row_count): needs 'min' and/or 'max'")
            if ct == "reference":
                if "column" not in chk or not ("values" in chk or ("dataset" in chk and "ref_column" in chk)):
                    errs.append(f"{tag} (reference): needs 'column' and 'values' or 'dataset'+'ref_column'")
        return errs

    # ------------------------------------------------------------------ helpers
    def merged_over(self, base: "RuleSet") -> "RuleSet":
        """Return a RuleSet where *self* (user rules) takes precedence over *base* (e.g. plugin rules)."""
        cols = copy.deepcopy(base.columns)
        for name, rule in self.columns.items():
            cols.setdefault(name, {}).update(copy.deepcopy(rule))
        return RuleSet(self.dataset or base.dataset, cols, copy.deepcopy(base.checks + self.checks),
                       self.allow_extra_columns and base.allow_extra_columns)

    def to_dict(self) -> Dict[str, Any]:
        return {"dataset": self.dataset, "columns": self.columns, "checks": self.checks,
                "allow_extra_columns": self.allow_extra_columns}

    def is_empty(self) -> bool:
        return not self.columns and not self.checks

    def required_columns(self) -> List[str]:
        return [c for c, r in self.columns.items() if r.get("required", True)]
