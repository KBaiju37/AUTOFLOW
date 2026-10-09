"""Local file connectors: CSV/delimited, JSON, JSON Lines, Excel, Parquet. All strictly read-only."""
from __future__ import annotations

import csv
import json
import os
from typing import Any, Dict, Iterator, List, Optional

import pandas as pd

from ..contracts import Dataset, DependencyMissing, SourceConnector, SourceError
from ..registry import registry
from ..typeops import normalize_frame
from ..util import dependency_available, sha256_file

ROW_ID = "_row_id"


class _FileSource(SourceConnector):
    def validate(self) -> List[str]:
        p = self.config.get("path")
        if not p:
            return ["'path' is required"]
        if not os.path.exists(p):
            return [f"File not found: {p}"]
        if not os.path.isfile(p):
            return [f"Not a regular file: {p}"]
        errs: List[str] = []
        roots = self.config.get("allowed_roots")
        if roots:
            real = os.path.realpath(p)
            ok = any(real == os.path.realpath(r) or real.startswith(os.path.realpath(r) + os.sep) for r in roots)
            if not ok:
                errs.append("Path is outside the allowed roots")
        mb = self.config.get("max_bytes")
        if mb and os.path.getsize(p) > mb:
            errs.append(f"File exceeds max_bytes ({os.path.getsize(p)} > {mb})")
        return errs

    def _require_valid(self) -> str:
        errs = self.validate()
        if errs:
            raise SourceError("; ".join(errs))
        return self.config["path"]

    def _empty(self, note: str = "file is empty") -> Dataset:
        return Dataset(pd.DataFrame(index=pd.Index([], name=ROW_ID)), source_type=self.type,
                       source_id=os.path.basename(self.config.get("path", "")),
                       metadata={"empty": True, "note": note})

    def _finish(self, frame: pd.DataFrame, malformed, dups, warns, total, extra=None) -> Dataset:
        frame.index.name = ROW_ID
        p = self.config["path"]
        meta = {"path_basename": os.path.basename(p), "size_bytes": os.path.getsize(p)}
        meta.update(extra or {})
        return Dataset(frame, source_type=self.type, source_id=os.path.basename(p),
                       fingerprint=self.fingerprint(), malformed=malformed, duplicate_columns=dups,
                       warnings=warns, metadata=meta, total_rows_in_source=total)

    def fingerprint(self) -> str:
        return sha256_file(self.config["path"], extra=f"{self.type}|{sorted(map(str, self.config.items()))}")


def _records_to_frame(records: List[Dict[str, Any]], ids: List[int]) -> pd.DataFrame:
    cols: List[str] = []
    seen = set()
    for r in records:
        for k in r:
            if k not in seen:
                seen.add(k)
                cols.append(k)
    data = {str(c): [r.get(c) for r in records] for c in cols}
    return pd.DataFrame(data, index=pd.Index(ids), dtype=object)


# --------------------------------------------------------------------------- CSV
@registry.register
class CsvSource(_FileSource):
    type = "csv"
    description = "CSV / delimited text (set 'delimiter' for TSV, pipe, etc.). Streams records; reports malformed rows."
    capabilities = {"chunked": True, "sampling": True, "read_only": True, "stable_row_ids": True}

    def _scan(self) -> Iterator[tuple]:
        p = self._require_valid()
        enc = self.config.get("encoding", "utf-8-sig")
        delim = self.config.get("delimiter", ",")
        if not isinstance(delim, str) or len(delim) != 1:
            raise SourceError("'delimiter' must be a single character")
        has_header = self.config.get("has_header", True)
        try:
            with open(p, newline="", encoding=enc) as fh:
                reader = csv.reader(fh, delimiter=delim)
                first = True
                rid = 0
                for row in reader:
                    if not row:
                        continue
                    if first:
                        first = False
                        if has_header:
                            yield ("header", row)
                            continue
                        yield ("header", [f"col_{i + 1}" for i in range(len(row))])
                    rid += 1
                    yield ("row", rid, row)
        except UnicodeDecodeError as exc:
            raise SourceError(f"Cannot decode file as {enc}: {exc.reason} at byte {exc.start}") from exc
        except csv.Error as exc:
            raise SourceError(f"CSV parse error: {exc}") from exc

    @staticmethod
    def _clean_header(header: List[str]):
        warns, names = [], []
        for i, h in enumerate(header):
            n = h.strip()
            if not n:
                n = f"unnamed_{i + 1}"
                warns.append(f"Column {i + 1} has an empty header; named '{n}'")
            names.append(n)
        return names, warns

    def read(self, limit: Optional[int] = None) -> Dataset:
        p = self._require_valid()
        if os.path.getsize(p) == 0:
            return self._empty()
        header: Optional[List[str]] = None
        rows, ids, malformed = [], [], []
        truncated = False
        for ev in self._scan():
            if ev[0] == "header":
                header = ev[1]
                continue
            _, rid, row = ev
            if len(row) != len(header):
                malformed.append({"row_id": rid, "reason": f"expected {len(header)} fields, found {len(row)}",
                                  "raw": self.config.get("delimiter", ",").join(row)[:200]})
                continue
            if limit is not None and len(rows) >= limit:
                truncated = True
                break
            rows.append(row)
            ids.append(rid)
        if header is None:
            return self._empty("no header row")
        names, warns = self._clean_header(header)
        frame = pd.DataFrame(rows, columns=names, index=pd.Index(ids), dtype=object) if rows else \
            pd.DataFrame({n: [] for n in names}, index=pd.Index([], dtype="int64"), dtype=object)
        frame, dups = normalize_frame(frame)
        if dups:
            warns.append(f"Duplicate column names in header: {dups}")
        total = None if truncated else len(rows) + len(malformed)
        return self._finish(frame, malformed, dups, warns, total, {"delimiter": self.config.get("delimiter", ",")})

    def iter_chunks(self, chunksize: int = 50_000) -> Iterator[pd.DataFrame]:
        header, buf, ids = None, [], []
        for ev in self._scan():
            if ev[0] == "header":
                header, _ = self._clean_header(ev[1])
                continue
            _, rid, row = ev
            if len(row) != len(header):
                continue  # malformed rows are reported by read(); chunk mode skips them
            buf.append(row)
            ids.append(rid)
            if len(buf) >= chunksize:
                yield self._chunk(buf, header, ids)
                buf, ids = [], []
        if buf:
            yield self._chunk(buf, header, ids)

    @staticmethod
    def _chunk(buf, header, ids) -> pd.DataFrame:
        df, _ = normalize_frame(pd.DataFrame(buf, columns=header, index=pd.Index(ids, name=ROW_ID), dtype=object))
        return df

    def count_rows(self) -> Optional[int]:
        n = 0
        for ev in self._scan():
            if ev[0] == "row":
                n += 1
        return n


# --------------------------------------------------------------------------- JSON
def _dig(obj: Any, dotted: str) -> Any:
    for part in dotted.split("."):
        if isinstance(obj, dict) and part in obj:
            obj = obj[part]
        else:
            raise SourceError(f"records_key '{dotted}' not found in JSON document")
    return obj


@registry.register
class JsonSource(_FileSource):
    type = "json"
    description = "JSON file: a list of objects, or an object with a list under 'records_key'. Loaded fully in memory."

    def read(self, limit: Optional[int] = None) -> Dataset:
        p = self._require_valid()
        if os.path.getsize(p) == 0:
            return self._empty()
        try:
            with open(p, encoding=self.config.get("encoding", "utf-8-sig")) as fh:
                doc = json.load(fh)
        except UnicodeDecodeError as exc:
            raise SourceError(f"Cannot decode JSON file: {exc.reason}") from exc
        except ValueError as exc:
            raise SourceError(f"Invalid JSON: {exc}") from exc
        if isinstance(doc, dict):
            key = self.config.get("records_key")
            if not key:
                raise SourceError("JSON root is an object; set 'records_key' to the list of records")
            doc = _dig(doc, key)
        if not isinstance(doc, list):
            raise SourceError("JSON records must be a list")
        records, ids, malformed = [], [], []
        for i, rec in enumerate(doc, start=1):
            if limit is not None and len(records) >= limit:
                break
            if isinstance(rec, dict):
                records.append(rec)
                ids.append(i)
            else:
                malformed.append({"row_id": i, "reason": f"record is {type(rec).__name__}, expected object",
                                  "raw": str(rec)[:200]})
        frame = _records_to_frame(records, ids)
        frame, dups = normalize_frame(frame)
        total = len(doc) if (limit is None or len(records) < limit) else None
        return self._finish(frame, malformed, dups, [], total)


@registry.register
class JsonlSource(_FileSource):
    type = "jsonl"
    description = "JSON Lines: one JSON object per line. Streams; bad lines are reported as malformed records."
    capabilities = {"chunked": True, "sampling": True, "read_only": True, "stable_row_ids": True}

    def _scan(self) -> Iterator[tuple]:
        p = self._require_valid()
        try:
            with open(p, encoding=self.config.get("encoding", "utf-8-sig")) as fh:
                for n, line in enumerate(fh, start=1):
                    if not line.strip():
                        continue
                    try:
                        rec = json.loads(line)
                    except ValueError as exc:
                        yield (n, None, f"invalid JSON ({exc.__class__.__name__})", line)
                        continue
                    if not isinstance(rec, dict):
                        yield (n, None, f"line is {type(rec).__name__}, expected object", line)
                        continue
                    yield (n, rec, None, None)
        except UnicodeDecodeError as exc:
            raise SourceError(f"Cannot decode JSONL file: {exc.reason}") from exc

    def read(self, limit: Optional[int] = None) -> Dataset:
        p = self._require_valid()
        if os.path.getsize(p) == 0:
            return self._empty()
        records, ids, malformed, truncated = [], [], [], False
        for n, rec, err, raw in self._scan():
            if err:
                malformed.append({"row_id": n, "reason": err, "raw": raw.strip()[:200]})
                continue
            if limit is not None and len(records) >= limit:
                truncated = True
                break
            records.append(rec)
            ids.append(n)
        frame, dups = normalize_frame(_records_to_frame(records, ids))
        total = None if truncated else len(records) + len(malformed)
        return self._finish(frame, malformed, dups, [], total)

    def iter_chunks(self, chunksize: int = 50_000) -> Iterator[pd.DataFrame]:
        recs, ids = [], []
        for n, rec, err, _ in self._scan():
            if err:
                continue
            recs.append(rec)
            ids.append(n)
            if len(recs) >= chunksize:
                yield normalize_frame(_records_to_frame(recs, ids))[0]
                recs, ids = [], []
        if recs:
            yield normalize_frame(_records_to_frame(recs, ids))[0]

    def count_rows(self) -> Optional[int]:
        return sum(1 for _, rec, err, _ in self._scan() if not err)


# --------------------------------------------------------------------------- Excel
@registry.register
class ExcelSource(_FileSource):
    type = "excel"
    description = "Excel .xlsx/.xlsm via openpyxl (first sheet unless 'sheet' is set). Loaded fully in memory."
    dependencies = ("openpyxl",)

    def validate(self) -> List[str]:
        errs = super().validate()
        if not dependency_available("openpyxl"):
            errs.append("openpyxl is not installed (pip install openpyxl)")
        return errs

    def read(self, limit: Optional[int] = None) -> Dataset:
        if not dependency_available("openpyxl"):
            raise DependencyMissing("The Excel connector requires openpyxl (pip install openpyxl)")
        p = self._require_valid()
        try:
            df = pd.read_excel(p, sheet_name=self.config.get("sheet", 0), dtype=object, nrows=limit)
        except Exception as exc:  # corrupt workbook, bad sheet name ...
            raise SourceError(f"Cannot read Excel file: {exc.__class__.__name__}: {exc}") from exc
        if df.empty and len(df.columns) == 0:
            return self._empty("sheet is empty")
        df.index = pd.Index(range(1, len(df) + 1))
        df, dups = normalize_frame(df)
        return self._finish(df, [], dups, [], None if limit else len(df), {"sheet": str(self.config.get("sheet", 0))})


# --------------------------------------------------------------------------- Parquet
@registry.register
class ParquetSource(_FileSource):
    type = "parquet"
    description = "Parquet via pyarrow. Implemented but NOT exercised by the bundled tests unless pyarrow is installed."
    dependencies = ("pyarrow",)
    capabilities = {"chunked": True, "sampling": True, "read_only": True, "stable_row_ids": True}

    def validate(self) -> List[str]:
        errs = super().validate()
        if not dependency_available("pyarrow"):
            errs.append("pyarrow is not installed (pip install pyarrow)")
        return errs

    def read(self, limit: Optional[int] = None) -> Dataset:
        if not dependency_available("pyarrow"):
            raise DependencyMissing("The Parquet connector requires pyarrow (pip install pyarrow)")
        p = self._require_valid()
        try:
            df = pd.read_parquet(p, engine="pyarrow")
        except Exception as exc:
            raise SourceError(f"Cannot read Parquet file: {exc.__class__.__name__}: {exc}") from exc
        if limit:
            df = df.head(limit)
        df.index = pd.Index(range(1, len(df) + 1))
        df, dups = normalize_frame(df)
        return self._finish(df, [], dups, [], None if limit else len(df))

    def iter_chunks(self, chunksize: int = 50_000) -> Iterator[pd.DataFrame]:
        if not dependency_available("pyarrow"):
            raise DependencyMissing("The Parquet connector requires pyarrow")
        import pyarrow.parquet as pq

        self._require_valid()
        start = 1
        for batch in pq.ParquetFile(self.config["path"]).iter_batches(batch_size=chunksize):
            df = batch.to_pandas()
            df.index = pd.Index(range(start, start + len(df)), name=ROW_ID)
            start += len(df)
            yield normalize_frame(df)[0]
