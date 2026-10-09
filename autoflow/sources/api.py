"""REST/JSON API connector with page, offset and cursor pagination (stdlib urllib; no extra dependency)."""
from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Callable, Dict, List, Optional

import pandas as pd

from ..contracts import Dataset, SourceConnector, SourceError
from ..registry import registry
from ..typeops import normalize_frame
from ..util import env_secret, frame_fingerprint, redact_text
from .files import _records_to_frame

Fetcher = Callable[[str, Dict[str, Any], Dict[str, str], float], Any]


def _dig(obj: Any, dotted: Optional[str]) -> Any:
    if not dotted:
        return obj
    for part in dotted.split("."):
        if isinstance(obj, dict) and part in obj:
            obj = obj[part]
        else:
            raise SourceError(f"path '{dotted}' not found in API response")
    return obj


@registry.register
class RestApiSource(SourceConnector):
    type = "rest_api"
    description = ("HTTP GET JSON API. pagination.type: none | page | offset | cursor. Secrets via 'headers_env'. "
                   "Tested with an injected fetcher (no live network calls).")
    capabilities = {"chunked": False, "sampling": True, "read_only": True, "stable_row_ids": False}

    def __init__(self, config: Dict[str, Any], fetcher: Optional[Fetcher] = None):
        super().__init__(config)
        self._fetcher = fetcher or self._default_fetch

    # ------------------------------------------------------------------ config
    def validate(self) -> List[str]:
        errs: List[str] = []
        url = self.config.get("url")
        if not url:
            return ["'url' is required"]
        if urllib.parse.urlparse(url).scheme not in ("http", "https"):
            errs.append("Only http and https URLs are supported")
        pag = self.config.get("pagination") or {"type": "none"}
        t = pag.get("type", "none")
        if t not in ("none", "page", "offset", "cursor"):
            errs.append(f"Unknown pagination type '{t}'")
        if t in ("page", "offset") and not pag.get("page_size"):
            errs.append(f"pagination.page_size is required for '{t}' pagination")
        if t == "cursor" and not (pag.get("cursor_param") and pag.get("next_cursor_path")):
            errs.append("cursor pagination needs cursor_param and next_cursor_path")
        return errs

    def _headers(self) -> Dict[str, str]:
        h = {str(k): str(v) for k, v in (self.config.get("headers") or {}).items()}
        for name, env in (self.config.get("headers_env") or {}).items():
            h[name] = env_secret(env)
        return h

    def _secrets(self) -> List[str]:
        return [v for k, v in self._headers().items() if k.lower() in ("authorization", "x-api-key") or "token" in k.lower()]

    # ------------------------------------------------------------------ fetching
    def _default_fetch(self, url: str, params: Dict[str, Any], headers: Dict[str, str], timeout: float) -> Any:
        full = url + (("&" if "?" in url else "?") + urllib.parse.urlencode(params) if params else "")
        req = urllib.request.Request(full, headers={"Accept": "application/json", **headers})
        max_bytes = int(self.config.get("max_response_bytes", 20_000_000))
        with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310 (scheme validated)
            raw = resp.read(max_bytes + 1)
        if len(raw) > max_bytes:
            raise SourceError("API response exceeds max_response_bytes")
        return json.loads(raw.decode("utf-8"))

    def _get(self, params: Dict[str, Any], page_no: int) -> Any:
        try:
            return self._fetcher(self.config["url"], params, self._headers(), float(self.config.get("timeout", 30)))
        except SourceError:
            raise
        except urllib.error.HTTPError as exc:
            raise SourceError(f"API request failed on page {page_no}: HTTP {exc.code}") from exc
        except Exception as exc:
            msg = redact_text(f"{exc.__class__.__name__}: {exc}", self._secrets())
            raise SourceError(f"API request failed on page {page_no}: {msg}") from exc

    def _records(self, payload: Any) -> List[Any]:
        recs = _dig(payload, self.config.get("records_path"))
        if not isinstance(recs, list):
            raise SourceError("API records must be a JSON list (set 'records_path' if nested)")
        return recs

    # ------------------------------------------------------------------ reading
    def read(self, limit: Optional[int] = None) -> Dataset:
        errs = self.validate()
        if errs:
            raise SourceError("; ".join(errs))
        pag = self.config.get("pagination") or {"type": "none"}
        t = pag.get("type", "none")
        base = dict(self.config.get("params") or {})
        max_pages = int(self.config.get("max_pages", 100))
        raw: List[Any] = []
        pages, finished = 0, False
        seen_cursors = set()
        cursor = None
        page = int(pag.get("start_page", 1))
        offset = 0
        size = pag.get("page_size")
        while pages < max_pages:
            params = dict(base)
            if t == "page":
                params[pag.get("page_param", "page")] = page
                params[pag.get("size_param", "page_size")] = size
            elif t == "offset":
                params[pag.get("offset_param", "offset")] = offset
                params[pag.get("limit_param", "limit")] = size
            elif t == "cursor" and cursor is not None:
                params[pag["cursor_param"]] = cursor
            payload = self._get(params, pages + 1)
            recs = self._records(payload)
            pages += 1
            raw.extend(recs)
            if t == "none" or not recs:
                finished = True
            elif t in ("page", "offset") and len(recs) < int(size):
                finished = True
            elif t == "cursor":
                nxt = _dig(payload, pag["next_cursor_path"]) if self._has(payload, pag["next_cursor_path"]) else None
                if not nxt:
                    finished = True
                elif nxt in seen_cursors:
                    raise SourceError("API pagination loop detected (cursor repeated)")
                else:
                    seen_cursors.add(nxt)
                    cursor = nxt
            page += 1
            offset += len(recs)
            if finished or (limit is not None and len(raw) >= limit):
                break
        truncated = (not finished) and (limit is None or len(raw) < limit)
        warns = [f"Stopped after max_pages={max_pages}; results are incomplete"] if truncated else []
        if limit is not None:
            raw = raw[:limit]
        records, ids, malformed = [], [], []
        for i, r in enumerate(raw, start=1):
            if isinstance(r, dict):
                records.append(r)
                ids.append(i)
            else:
                malformed.append({"row_id": i, "reason": f"record is {type(r).__name__}, expected object", "raw": str(r)[:200]})
        frame, dups = normalize_frame(_records_to_frame(records, ids))
        frame.index.name = "_row_id"
        return Dataset(frame, source_type="rest_api", source_id=urllib.parse.urlparse(self.config["url"]).netloc,
                       fingerprint=frame_fingerprint(frame), malformed=malformed, duplicate_columns=dups,
                       warnings=warns, metadata={"pages_fetched": pages, "incomplete": truncated},
                       total_rows_in_source=None if (limit or truncated) else len(raw))

    @staticmethod
    def _has(payload: Any, dotted: str) -> bool:
        try:
            _dig(payload, dotted)
            return True
        except SourceError:
            return False
