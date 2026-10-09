"""Dataset profiling and schema *proposal*. Nothing here is business truth; it is evidence."""
from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from .typeops import (INT_RE, ISO_RE, NUM_RE, SLASH_RE, TEXT_BOOL_VALUES, as_str, date_order_evidence,
                      missing_mask)
from .util import frame_str, jsonable

SENSITIVE_NAME = re.compile(r"(email|phone|mobile|ssn|passw|secret|token|card|iban|dob|birth|address|name|salary)", re.I)
ID_NAME = re.compile(r"(^id$|_id$|^id_|id$|code$|key$|number$|no$)", re.I)
INFER_THRESHOLD = 0.9


def _frac(mask: pd.Series) -> float:
    return float(mask.mean()) if len(mask) else 0.0


def _match(st: pd.Series, rx: re.Pattern) -> pd.Series:
    return st.map(lambda v: bool(rx.fullmatch(v))).astype(bool)


def _label(conf: float) -> str:
    return "high" if conf >= 0.99 else "medium" if conf >= INFER_THRESHOLD else "low"


def infer_text_type(vals: pd.Series) -> Dict[str, Any]:
    """Infer a type from non-missing values (any dtype). Returns type/confidence/invalid_count/flags."""
    n = len(vals)
    if n == 0:
        return {"type": "string", "confidence": 0.0, "invalid": 0, "leading_zeros": False, "date_order": "none"}
    st = as_str(vals).str.strip()
    lz = bool(_match(st, re.compile(r"[+-]?0\d+")).any())
    long_digits = bool(_match(st, re.compile(r"\d{16,}")).any())
    out = {"leading_zeros": lz, "date_order": "none"}

    low = st.str.lower()
    textual_bool = low.isin(TEXT_BOOL_VALUES)
    if textual_bool.any() and low.isin(TEXT_BOOL_VALUES | {"0", "1"}).mean() >= INFER_THRESHOLD:
        c = _frac(low.isin(TEXT_BOOL_VALUES | {"0", "1"}))
        return {**out, "type": "boolean", "confidence": c, "invalid": int(n - low.isin(TEXT_BOOL_VALUES | {"0", "1"}).sum())}

    is_int = _match(st, INT_RE)
    if _frac(is_int) >= INFER_THRESHOLD:
        if lz or long_digits:
            return {**out, "type": "string", "confidence": 1.0, "invalid": 0, "role_hint": "identifier"}
        return {**out, "type": "integer", "confidence": _frac(is_int), "invalid": int(n - is_int.sum())}
    is_num = _match(st, NUM_RE)
    if _frac(is_num) >= INFER_THRESHOLD:
        return {**out, "type": "number", "confidence": _frac(is_num), "invalid": int(n - is_num.sum())}
    iso, slash = _match(st, ISO_RE), _match(st, SLASH_RE)
    if _frac(iso | slash) >= INFER_THRESHOLD:
        parsed = pd.to_datetime(st.where(iso), format="ISO8601", errors="coerce", utc=True)
        ok = (iso & parsed.notna()) | slash
        order = date_order_evidence(st[slash]) if slash.any() else "none"
        return {**out, "type": "datetime", "confidence": _frac(ok), "invalid": int(n - ok.sum()), "date_order": order}
    return {**out, "type": "string", "confidence": 1.0, "invalid": 0}


def profile_column(s: pd.Series, name: str, n_rows: int, mask_examples: bool, iqr_mult: float) -> Dict[str, Any]:
    miss = missing_mask(s)
    nonmiss = s[~miss]
    n_null = int(miss.sum())
    p: Dict[str, Any] = {
        "name": name,
        "normalized_name": re.sub(r"[^0-9a-z]+", "_", name.strip().lower()).strip("_") or name,
        "null_count": n_null,
        "null_pct": round(n_null / n_rows, 6) if n_rows else 0.0,
        "nullable": n_null > 0,
        "empty_string_count": int(s.map(lambda v: isinstance(v, str) and v == "").sum()) if len(s) and s.dtype == object else 0,
    }
    # --- type
    if pd.api.types.is_bool_dtype(s):
        inf = {"type": "boolean", "confidence": 1.0, "invalid": 0, "leading_zeros": False, "date_order": "none"}
    elif pd.api.types.is_integer_dtype(s):
        inf = {"type": "integer", "confidence": 1.0, "invalid": 0, "leading_zeros": False, "date_order": "none"}
    elif pd.api.types.is_float_dtype(s):
        whole = bool(((nonmiss % 1) == 0).all()) if len(nonmiss) else False
        inf = {"type": "integer" if whole and len(nonmiss) else "number", "confidence": 1.0, "invalid": 0,
               "leading_zeros": False, "date_order": "none"}
    elif pd.api.types.is_datetime64_any_dtype(s):
        inf = {"type": "datetime", "confidence": 1.0, "invalid": 0, "leading_zeros": False, "date_order": "none"}
    else:
        inf = infer_text_type(nonmiss)
    t = inf["type"]
    p.update(inferred_type=t, confidence=round(inf["confidence"], 4), confidence_label=_label(inf["confidence"]),
             invalid_for_inferred_type=int(inf["invalid"]), leading_zeros=inf["leading_zeros"])

    # --- cardinality
    strs = as_str(nonmiss)
    uniq = int(strs.nunique()) if len(strs) else 0
    p["unique_count"] = uniq
    p["uniqueness_ratio"] = round(uniq / len(nonmiss), 6) if len(nonmiss) else 0.0
    p["is_constant"] = bool(uniq == 1 and len(nonmiss) > 1)

    # --- whitespace
    ws = strs.map(lambda v: v != v.strip()) if len(strs) else pd.Series([], dtype=bool)
    p["whitespace_anomalies"] = int(ws.sum())

    # --- numeric stats / outliers
    if t in ("integer", "number") and len(nonmiss):
        num = pd.to_numeric(strs.str.strip(), errors="coerce").dropna() if nonmiss.dtype == object else nonmiss.astype(float)
        if len(num):
            q1, q3 = float(num.quantile(0.25)), float(num.quantile(0.75))
            iqr = q3 - q1
            outliers = int(((num < q1 - iqr_mult * iqr) | (num > q3 + iqr_mult * iqr)).sum()) if iqr > 0 else 0
            p["numeric_stats"] = {"min": float(num.min()), "max": float(num.max()), "mean": float(num.mean()),
                                  "median": float(num.median()),
                                  "std": float(num.std()) if len(num) > 1 else 0.0}
            p["potential_outliers"] = outliers
    # --- strings
    if t == "string" and len(strs):
        lens = strs.str.len()
        p["string_length"] = {"min": int(lens.min()), "max": int(lens.max())}
    # --- dates
    if t == "datetime" and len(strs):
        p["date_order"] = inf.get("date_order", "none")
        p["date_format_note"] = ("non-ISO dates present; day/month order " + p["date_order"]
                                 if p["date_order"] != "none" else "ISO-8601")
    # --- categorical frequencies
    if t in ("string", "boolean", "integer") and 0 < uniq <= max(20, int(0.05 * max(len(nonmiss), 1))):
        vc = strs.value_counts().head(10)
        p["top_values"] = {str(k): int(v) for k, v in vc.items()}
    # --- examples (masked for sensitive-looking names)
    if mask_examples and SENSITIVE_NAME.search(name):
        p["examples"] = ["***"] * min(3, len(strs))
    else:
        p["examples"] = [v[:24] for v in strs.drop_duplicates().head(3).tolist()]

    # --- role
    role = "text"
    if t == "datetime":
        role = "datetime"
    elif t == "boolean":
        role = "flag"
    elif t in ("integer", "number"):
        role = "identifier" if (ID_NAME.search(name) and p["uniqueness_ratio"] > 0.9) else "measure"
    elif inf.get("role_hint") == "identifier" or (ID_NAME.search(name) and p["uniqueness_ratio"] > 0.5):
        role = "identifier"
    elif p.get("top_values"):
        role = "categorical"
    p["possible_role"] = role

    # --- conversion safety & suggested rules (proposals only)
    p["safe_conversion"] = bool(t in ("integer", "number", "datetime", "boolean") and inf["invalid"] == 0
                                and not inf["leading_zeros"] and p.get("date_order", "none") in ("none",))
    p["needs_confirmation"] = bool(inf["invalid"] > 0 or inf["leading_zeros"]
                                   or p.get("date_order", "none") in ("ambiguous", "conflict", "dmy", "mdy")
                                   or inf["confidence"] < 0.99)
    sug: Dict[str, Any] = {"type": t}
    if n_null == 0 and n_rows >= 10:
        sug["nullable"] = False
    if p["uniqueness_ratio"] == 1.0 and n_null == 0 and n_rows >= 10 and role == "identifier":
        sug["unique"] = True
    p["suggested_rules"] = sug
    p["suggestion_status"] = "proposal - requires human confirmation"
    return p


def profile_frame(df: pd.DataFrame, name: str = "dataset", sample_size: Optional[int] = 10000,
                  total_rows: Optional[int] = None, malformed_count: int = 0, mask_examples: bool = True,
                  iqr_mult: float = 3.0, seed: int = 0) -> Dict[str, Any]:
    n_loaded = len(df)
    sampled = bool(sample_size) and n_loaded > sample_size
    work = df.sample(sample_size, random_state=seed).sort_index() if sampled else df
    exact = (not sampled) and (total_rows is None or total_rows == n_loaded + malformed_count)
    n = len(work)
    cols: Dict[str, Any] = {}
    for c in work.columns:
        cols[c] = profile_column(work[c], c, n, mask_examples, iqr_mult)
    dup = int(frame_str(work).duplicated().sum()) if n and len(work.columns) else 0
    keys = [c for c, p in cols.items()
            if n >= 10 and p["uniqueness_ratio"] == 1.0 and p["null_count"] == 0]
    return {
        "dataset": name,
        "rows_loaded": n_loaded,
        "rows_profiled": n,
        "malformed_records": malformed_count,
        "total_rows_in_source": total_rows,
        "columns_count": len(work.columns),
        "column_names": list(work.columns),
        "exact": bool(exact),
        "sampled": sampled,
        "note": ("Counts refer to a random sample; percentages are estimates." if sampled
                 else "Statistics are exact for the rows loaded."),
        "empty": n_loaded == 0,
        "duplicate_rows": dup,
        "candidate_keys": keys,
        "candidate_keys_note": "uniqueness observed, not guaranteed; confirm before relying on it",
        "columns": cols,
    }


def schema_proposal(profile: Dict[str, Any]) -> Dict[str, Any]:
    """A rules-file skeleton derived from a profile. A starting point for a human, not an approved schema."""
    return {"dataset": profile["dataset"], "status": "proposal",
            "columns": {c: dict(p["suggested_rules"]) for c, p in profile["columns"].items()}}


def make_baseline(profile: Dict[str, Any]) -> Dict[str, Any]:
    cols = {}
    for c, p in profile["columns"].items():
        e = {"type": p["inferred_type"], "null_pct": p["null_pct"]}
        if "numeric_stats" in p:
            e["mean"], e["std"] = p["numeric_stats"]["mean"], p["numeric_stats"]["std"]
        cols[c] = e
    return {"rows": profile["rows_loaded"], "columns": cols}
