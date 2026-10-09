"""Configuration loading and validation."""
from __future__ import annotations

import copy
import os
from typing import Any, Dict, List, Optional, Union

from .contracts import ConfigError, MODES
from .rules import load_structured

DEFAULTS: Dict[str, Any] = {
    "autoflow": {"name": "autoflow", "mode": "dry_run"},
    "source": {},
    "profiling": {"enabled": True, "sample_size": 10000, "max_rows": None,
                  "mask_examples": True, "outlier_iqr_multiplier": 3.0},
    "validation": {
        "use_generic_rules": True,
        "fail_on_missing_required_columns": True,
        "fail_on_schema_drift": True,
        "allow_partial_success": False,
        "max_quarantine_pct": 100.0,
        "warnings_block": False,
        "generic_null_warn_pct": 0.5,
        "null_rate_shift": 0.15,
        "row_count_change_pct": 0.5,
        "drift_std_multiplier": 3.0,
    },
    "rules": {},
    "rules_file": None,
    "recovery": {
        "enabled": False,
        "auto_apply_categories": ["A"],
        "approved_rules": [],
        "disabled_rules": [],
        "preserve_original_values": True,
        "revalidate_after_recovery": True,
    },
    "output": {"type": "none", "enabled": False, "path": None, "directory": None},
    "quarantine": {"enabled": True, "path": None},
    "audit": {"enabled": False, "path": None, "redact_values": False},
    "plugins": [],
}
OUTPUT_TYPES = ("none", "sqlite_staging", "local_files")
PATH_KEYS = [("source", "path"), ("output", "path"), ("output", "directory"),
             ("quarantine", "path"), ("audit", "path")]


def _merge(base: Dict[str, Any], over: Dict[str, Any]) -> Dict[str, Any]:
    out = copy.deepcopy(base)
    for k, v in (over or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict) and k not in ("rules", "source"):
            out[k] = _merge(out[k], v)
        else:
            out[k] = copy.deepcopy(v)
    return out


class Config:
    def __init__(self, data: Dict[str, Any], base_dir: Optional[str] = None):
        self.data = data
        self.base_dir = base_dir or os.getcwd()

    # ------------------------------------------------------------------ loading
    @classmethod
    def load(cls, src: Union[str, Dict[str, Any], None]) -> "Config":
        base_dir = None
        if src is None:
            raw: Dict[str, Any] = {}
        elif isinstance(src, dict):
            raw = src
        else:
            raw = load_structured(str(src))
            base_dir = os.path.dirname(os.path.abspath(str(src)))
        # accept both {"autoflow": {...}, ...} and flat; top-level 'autoflow' is metadata only
        unknown = [k for k in raw if k not in DEFAULTS]
        if unknown:
            raise ConfigError(f"Unknown config section(s): {', '.join(sorted(unknown))}")
        data = _merge(DEFAULTS, raw)
        cfg = cls(data, base_dir)
        cfg._resolve_paths()
        errors = cfg.problems()
        if errors:
            raise ConfigError("Invalid configuration: " + "; ".join(errors))
        return cfg

    def _resolve_paths(self) -> None:
        for section, key in PATH_KEYS:
            v = self.data[section].get(key)
            if v and not os.path.isabs(v):
                self.data[section][key] = os.path.normpath(os.path.join(self.base_dir, v))
        rf = self.data.get("rules_file")
        if rf and not os.path.isabs(rf):
            self.data["rules_file"] = os.path.normpath(os.path.join(self.base_dir, rf))
        if self.data["audit"]["enabled"] and not self.data["audit"].get("path"):
            self.data["audit"]["path"] = os.path.join(self.base_dir, ".autoflow", "autoflow.db")

    # ------------------------------------------------------------------ validation
    def problems(self) -> List[str]:
        d, errs = self.data, []
        for section, defaults in DEFAULTS.items():
            if isinstance(defaults, dict) and section not in ("rules", "source"):
                for k in d[section]:
                    if k not in defaults:
                        errs.append(f"unknown key '{section}.{k}'")
        if d["autoflow"]["mode"] not in MODES:
            errs.append(f"autoflow.mode must be one of {list(MODES)}")
        rec = d["recovery"]
        if not rec["preserve_original_values"]:
            errs.append("recovery.preserve_original_values cannot be disabled")
        if not rec["revalidate_after_recovery"]:
            errs.append("recovery.revalidate_after_recovery cannot be disabled (recovery must not bypass validation)")
        bad_cat = [c for c in rec["auto_apply_categories"] if c not in ("A", "B")]
        if bad_cat or "B" in rec["auto_apply_categories"]:
            errs.append("recovery.auto_apply_categories may only contain 'A' "
                        "(category B needs approved_rules or an approver; category C is never applied)")
        out = d["output"]
        if out["type"] not in OUTPUT_TYPES:
            errs.append(f"output.type must be one of {list(OUTPUT_TYPES)}")
        elif out["enabled"]:
            if out["type"] == "none":
                errs.append("output.enabled is true but output.type is 'none'")
            if out["type"] == "sqlite_staging" and not out.get("path"):
                errs.append("output.type 'sqlite_staging' requires output.path")
            if out["type"] == "local_files" and not out.get("directory"):
                errs.append("output.type 'local_files' requires output.directory")
        v = d["validation"]
        if not 0 <= v["max_quarantine_pct"] <= 100:
            errs.append("validation.max_quarantine_pct must be between 0 and 100")
        if d["profiling"]["sample_size"] is not None and d["profiling"]["sample_size"] < 1:
            errs.append("profiling.sample_size must be positive or null")
        src = d["source"]
        if src:
            from .registry import registry
            t = src.get("type")
            if not t:
                errs.append("source.type is required when a source is configured")
            elif t not in registry.types():
                errs.append(f"source.type '{t}' is not a registered connector (known: {registry.types()})")
        return errs

    # ------------------------------------------------------------------ access
    def __getitem__(self, k: str) -> Any:
        return self.data[k]

    def get(self, k: str, default: Any = None) -> Any:
        return self.data.get(k, default)
