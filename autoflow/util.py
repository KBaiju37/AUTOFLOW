from __future__ import annotations

import datetime as _dt
import hashlib
import importlib
import math
import os
import re
from typing import Any, Iterable

import numpy as np
import pandas as pd


def now_iso() -> str:
    return _dt.datetime.now(_dt.timezone.utc).isoformat(timespec="seconds")


def dependency_available(module: str) -> bool:
    """Return whether an optional dependency can actually be imported.

    ``find_spec`` only confirms that a module appears discoverable; it can
    still fail at import time (for example, due to missing native DLLs or
    incompatible installations). Importing here makes optional-feature checks
    reflect whether the dependency is usable by this process.
    """
    try:
        importlib.import_module(module)
        return True
    except Exception:
        # Optional dependencies may fail with ImportError, OSError (native
        # library/DLL problems), or other initialization errors.
        return False


def jsonable(v: Any) -> Any:
    """Convert numpy/pandas scalars and containers into JSON-safe values."""
    if v is None:
        return None
    if isinstance(v, (str, bool)):
        return v
    if isinstance(v, int):
        return v
    if isinstance(v, (float, np.floating)):
        f = float(v)
        return None if (math.isnan(f) or math.isinf(f)) else f
    if isinstance(v, np.integer):
        return int(v)
    if isinstance(v, np.bool_):
        return bool(v)
    if isinstance(v, (pd.Timestamp, _dt.datetime, _dt.date)):
        return v.isoformat()
    if isinstance(v, dict):
        return {str(k): jsonable(x) for k, x in v.items()}
    if isinstance(v, (list, tuple, set)):
        return [jsonable(x) for x in v]
    try:
        if pd.isna(v):
            return None
    except (TypeError, ValueError):
        pass
    return str(v)


def sha256_file(path: str, extra: str = "") -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(1 << 20), b""):
            h.update(block)
    h.update(extra.encode())
    return h.hexdigest()


def _cell_str(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, float) and v != v:
        return ""
    return v if isinstance(v, str) else str(v)


def frame_str(df: pd.DataFrame) -> pd.DataFrame:
    """String form of every cell (missing -> '')."""
    return df.astype(object).map(_cell_str)


def frame_fingerprint(df: pd.DataFrame) -> str:
    h = hashlib.sha256()
    h.update("\x1f".join(map(str, df.columns)).encode())
    if len(df):
        s = frame_str(df)
        h.update(pd.util.hash_pandas_object(s, index=False).values.tobytes())
        h.update(pd.util.hash_pandas_object(pd.Series([str(i) for i in df.index]), index=False).values.tobytes())
    h.update(str(len(df)).encode())
    return h.hexdigest()


_SECRET_PATTERNS = [
    (re.compile(r"(://[^:/@\s]+:)[^@\s]+(@)"), r"\1***\2"),
    (re.compile(r"(?i)(password|passwd|pwd|token|secret|api[_-]?key|apikey|authorization)=([^&\s]+)"), r"\1=***"),
    (re.compile(r"(?i)(bearer\s+)[A-Za-z0-9._\-]+"), r"\1***"),
]


def redact_text(text: str, secrets: Iterable[str] = ()) -> str:
    out = str(text)
    for s in secrets:
        if s and len(s) >= 4:
            out = out.replace(s, "***")
    for pat, rep in _SECRET_PATTERNS:
        out = pat.sub(rep, out)
    return out


def env_secret(name: str) -> str:
    val = os.environ.get(name)
    if val is None:
        from .contracts import ConfigError
        raise ConfigError(f"Environment variable '{name}' is not set")
    return val
