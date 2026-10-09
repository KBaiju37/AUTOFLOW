"""Type coercion helpers shared by profiling, validation and recovery.

Values from text sources are kept as strings, so a type is only *proposed* by
profiling and only *enforced* when a rule declares it. Leading zeros survive.
"""
from __future__ import annotations

import datetime as _dt
import re
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

INT_RE = re.compile(r"[+-]?\d+")
NUM_RE = re.compile(r"[+-]?(?:\d+\.?\d*|\.\d+)(?:[eE][+-]?\d+)?")
ISO_RE = re.compile(
    r"\d{4}-\d{2}-\d{2}(?:[T ]\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?(?:Z|[+-]\d{2}:?\d{2})?)?"
)
SLASH_RE = re.compile(r"(\d{1,2})([/.\-])(\d{1,2})\2(\d{2}|\d{4})")
TRUE_VALUES = {"true", "yes", "y", "t", "1"}
FALSE_VALUES = {"false", "no", "n", "f", "0"}
TEXT_BOOL_VALUES = {"true", "false", "yes", "no", "y", "n", "t", "f"}
VALID_TYPES = ("string", "integer", "number", "boolean", "datetime", "date")


def normalize_frame(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    """Copy, stringify column names, de-duplicate names, make text columns plain object dtype."""
    out = df.copy()
    names = [str(c) for c in out.columns]
    seen: Dict[str, int] = {}
    dups: List[str] = []
    final: List[str] = []
    for n in names:
        seen[n] = seen.get(n, 0) + 1
        if seen[n] > 1:
            if n not in dups:
                dups.append(n)
            final.append(f"{n}__dup{seen[n]}")
        else:
            final.append(n)
    out.columns = final
    for c in out.columns:
        s = out[c]
        if s.dtype != object and (
            isinstance(s.dtype, pd.CategoricalDtype) or pd.api.types.is_string_dtype(s)
        ):
            out[c] = s.astype(object)
    return out, dups


def missing_mask(s: pd.Series) -> pd.Series:
    """True for None/NaN, and for empty or whitespace-only strings."""
    m = s.isna().astype(bool)
    if s.dtype == object and len(s):
        ws = s.map(lambda v: isinstance(v, str) and not v.strip()).astype(bool)
        m = m | ws
    return m


def as_str(s: pd.Series) -> pd.Series:
    return s.map(lambda v: v if isinstance(v, str) else ("" if v is None else str(v))).astype(object)


def _fullmatch(st: pd.Series, rx: re.Pattern) -> pd.Series:
    return st.map(lambda v: bool(rx.fullmatch(v))).astype(bool)


def coerce_series(s: pd.Series, typ: str, fmt: Optional[str] = None) -> Tuple[pd.Series, pd.Series]:
    """Coerce *non-missing* values to ``typ``. Returns (parsed, valid_mask)."""
    if len(s) == 0:
        return s.copy(), pd.Series([], dtype=bool, index=s.index)
    if typ == "string":
        return s, pd.Series(True, index=s.index)

    if typ in ("integer", "number"):
        if pd.api.types.is_numeric_dtype(s) and not pd.api.types.is_bool_dtype(s):
            vals = pd.to_numeric(s, errors="coerce").astype(float)
            valid = vals.notna() & np.isfinite(vals)
            if typ == "integer":
                valid = valid & ((vals % 1) == 0)
            return vals, valid.astype(bool)
        st = as_str(s)
        valid = _fullmatch(st, INT_RE if typ == "integer" else NUM_RE)
        vals = pd.to_numeric(st.where(valid), errors="coerce")
        return vals, valid

    if typ == "boolean":
        if pd.api.types.is_bool_dtype(s):
            return s, pd.Series(True, index=s.index)
        low = as_str(s).str.lower()
        valid = low.isin(TRUE_VALUES | FALSE_VALUES)
        return low.isin(TRUE_VALUES).where(valid), valid

    if typ in ("datetime", "date"):
        if pd.api.types.is_datetime64_any_dtype(s):
            vals = pd.to_datetime(s, utc=True)
            return vals, vals.notna()
        st = as_str(s)
        if fmt:
            vals = pd.to_datetime(st, format=fmt, errors="coerce", utc=True)
            return vals, vals.notna()
        iso = _fullmatch(st, ISO_RE)
        vals = pd.to_datetime(st.where(iso), format="ISO8601", errors="coerce", utc=True)
        return vals, (iso & vals.notna()).astype(bool)

    raise ValueError(f"unknown type '{typ}'")


def date_order_evidence(st: pd.Series) -> str:
    """For d/m/Y-style strings: 'dmy', 'mdy', 'ambiguous', 'conflict' or 'none'."""
    ex = st.str.extract("^" + SLASH_RE.pattern + "$")
    matched = ex[0].notna()
    if not matched.any():
        return "none"
    a = pd.to_numeric(ex[0], errors="coerce")
    b = pd.to_numeric(ex[2], errors="coerce")
    first_big = bool(((a > 12) & matched).any())
    second_big = bool(((b > 12) & matched).any())
    if first_big and second_big:
        return "conflict"
    if first_big:
        return "dmy"
    if second_big:
        return "mdy"
    return "ambiguous"


def slash_to_iso(value: str, order: str) -> Optional[str]:
    """Convert d/m/YYYY or m/d/YYYY to ISO date given a *resolved* order. 2-digit years are refused."""
    m = SLASH_RE.fullmatch(value.strip())
    if not m or order not in ("dmy", "mdy"):
        return None
    p1, p2, yr = int(m.group(1)), int(m.group(3)), m.group(4)
    if len(yr) != 4:
        return None
    day, month = (p1, p2) if order == "dmy" else (p2, p1)
    try:
        return _dt.date(int(yr), month, day).isoformat()
    except ValueError:
        return None
