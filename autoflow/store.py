"""SQLite store for audit trail, staging output, quarantine and approved baselines.

Idempotency model (see README):
  * ``run_id``  - unique per execution; audit rows are append-only and never overwritten.
  * ``run_key`` - sha256(dataset, input fingerprint, config+rule fingerprint). Output (staged rows and
                  quarantine rows) is committed at most once per run_key, inside one transaction.
A retry or re-run on identical input and config therefore never duplicates committed output; a changed
input or config produces a new run_key and a new batch.
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from contextlib import contextmanager
from typing import Any, Dict, Iterator, List, Optional

import pandas as pd

from .contracts import AutoFlowError
from .util import jsonable, now_iso

SCHEMA = """
CREATE TABLE IF NOT EXISTS runs (
  run_id TEXT PRIMARY KEY, run_key TEXT, dataset TEXT, mode TEXT, status TEXT, can_proceed INTEGER,
  fingerprint TEXT, started_at TEXT, ended_at TEXT, replay_of TEXT, result_json TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS idx_runs_dataset ON runs(dataset, started_at);
CREATE TABLE IF NOT EXISTS audit_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT NOT NULL, ts TEXT NOT NULL, event TEXT NOT NULL, detail TEXT);
CREATE TABLE IF NOT EXISTS recovery_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT, run_id TEXT NOT NULL, proposal_id TEXT, rule_id TEXT, rule_version TEXT,
  category TEXT, row_id TEXT, col TEXT, original TEXT, proposed TEXT, action TEXT, authorization TEXT,
  status TEXT, result TEXT, reason TEXT);
CREATE TABLE IF NOT EXISTS batches (
  run_key TEXT PRIMARY KEY, dataset TEXT, run_id TEXT, committed_at TEXT, staged_rows INTEGER, quarantined_rows INTEGER);
CREATE TABLE IF NOT EXISTS staged_rows (
  run_key TEXT NOT NULL, row_id TEXT NOT NULL, payload TEXT NOT NULL, PRIMARY KEY (run_key, row_id));
CREATE TABLE IF NOT EXISTS quarantine_rows (
  run_key TEXT NOT NULL, row_id TEXT NOT NULL, reasons TEXT, original TEXT, recovery TEXT,
  PRIMARY KEY (run_key, row_id));
CREATE TABLE IF NOT EXISTS baselines (dataset TEXT PRIMARY KEY, baseline TEXT NOT NULL, updated_at TEXT);
"""


class StoreError(AutoFlowError):
    pass


def _row_json(row: pd.Series) -> str:
    return json.dumps({str(k): jsonable(v) for k, v in row.items()}, default=str)


class Store:
    def __init__(self, path: str):
        self.path = path
        d = os.path.dirname(os.path.abspath(path))
        os.makedirs(d, exist_ok=True)
        con = sqlite3.connect(self.path, timeout=30)
        try:
            con.executescript(SCHEMA)   # executescript commits implicitly, so it runs outside _tx()
        finally:
            con.close()

    # ------------------------------------------------------------------ plumbing
    @contextmanager
    def _tx(self) -> Iterator[sqlite3.Connection]:
        con = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        try:
            con.execute("BEGIN IMMEDIATE")
            yield con
            con.execute("COMMIT")
        except Exception:
            try:
                con.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            con.close()

    def _query(self, sql: str, args: tuple = ()) -> List[tuple]:
        con = sqlite3.connect(self.path, timeout=30)
        try:
            return con.execute(sql, args).fetchall()
        finally:
            con.close()

    # ------------------------------------------------------------------ audit
    def last_fingerprint(self, dataset: str) -> Optional[str]:
        rows = self._query("SELECT fingerprint FROM runs WHERE dataset=? ORDER BY started_at DESC, rowid DESC LIMIT 1",
                           (dataset,))
        return rows[0][0] if rows else None

    def save_run(self, result: Dict[str, Any], events: List[Dict[str, Any]], recovery_rows: List[Dict[str, Any]],
                 redact: bool = False) -> None:
        def val(v: Any) -> Optional[str]:
            if v is None:
                return None
            s = str(v)
            return "sha256:" + hashlib.sha256(s.encode()).hexdigest()[:16] if redact else s

        with self._tx() as con:
            con.execute(
                "INSERT INTO runs(run_id,run_key,dataset,mode,status,can_proceed,fingerprint,started_at,ended_at,replay_of,result_json)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                (result["run_id"], result["run_key"], result["dataset_name"], result["mode"], result["status"],
                 int(bool(result["can_proceed"])), result["fingerprint"], result["started_at"], result["ended_at"],
                 result.get("replay_of"), json.dumps(result)))
            con.executemany("INSERT INTO audit_events(run_id,ts,event,detail) VALUES (?,?,?,?)",
                            [(result["run_id"], e["ts"], e["event"], json.dumps(e["detail"])) for e in events])
            con.executemany(
                "INSERT INTO recovery_log(run_id,proposal_id,rule_id,rule_version,category,row_id,col,original,proposed,"
                "action,authorization,status,result,reason) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                [(result["run_id"], r["proposal_id"], r["rule_id"], r["rule_version"], r["category"], str(r["row_id"]),
                  r["column"], val(r["original"]), val(r["proposed"]), r["action"], r["authorization"], r["status"],
                  r["result"], r["reason"]) for r in recovery_rows])

    def get_run(self, run_id: str) -> Optional[Dict[str, Any]]:
        rows = self._query("SELECT result_json FROM runs WHERE run_id=?", (run_id,))
        return json.loads(rows[0][0]) if rows else None

    def list_runs(self, dataset: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
        sql = "SELECT run_id,dataset,mode,status,can_proceed,started_at FROM runs"
        args: tuple = ()
        if dataset:
            sql, args = sql + " WHERE dataset=?", (dataset,)
        rows = self._query(sql + " ORDER BY started_at DESC, rowid DESC LIMIT ?", args + (limit,))
        return [dict(zip(("run_id", "dataset", "mode", "status", "can_proceed", "started_at"), r)) for r in rows]

    def events(self, run_id: str) -> List[Dict[str, Any]]:
        return [{"ts": r[0], "event": r[1], "detail": json.loads(r[2] or "{}")} for r in
                self._query("SELECT ts,event,detail FROM audit_events WHERE run_id=? ORDER BY id", (run_id,))]

    def recovery_log(self, run_id: str) -> List[Dict[str, Any]]:
        cols = ("proposal_id", "rule_id", "rule_version", "category", "row_id", "column", "original", "proposed",
                "action", "authorization", "status", "result", "reason")
        rows = self._query("SELECT proposal_id,rule_id,rule_version,category,row_id,col,original,proposed,action,"
                           "authorization,status,result,reason FROM recovery_log WHERE run_id=? ORDER BY id", (run_id,))
        return [dict(zip(cols, r)) for r in rows]

    # ------------------------------------------------------------------ baselines
    def save_baseline(self, dataset: str, baseline: Dict[str, Any]) -> None:
        with self._tx() as con:
            con.execute("INSERT INTO baselines(dataset,baseline,updated_at) VALUES (?,?,?) "
                        "ON CONFLICT(dataset) DO UPDATE SET baseline=excluded.baseline, updated_at=excluded.updated_at",
                        (dataset, json.dumps(baseline), now_iso()))

    def load_baseline(self, dataset: str) -> Optional[Dict[str, Any]]:
        rows = self._query("SELECT baseline FROM baselines WHERE dataset=?", (dataset,))
        return json.loads(rows[0][0]) if rows else None

    # ------------------------------------------------------------------ output (staging + quarantine)
    def batch(self, run_key: str) -> Optional[Dict[str, Any]]:
        rows = self._query("SELECT run_id,committed_at,staged_rows,quarantined_rows FROM batches WHERE run_key=?",
                           (run_key,))
        return dict(zip(("run_id", "committed_at", "staged_rows", "quarantined_rows"), rows[0])) if rows else None

    def _insert_staged(self, con: sqlite3.Connection, run_key: str, approved: pd.DataFrame) -> None:
        con.executemany("INSERT INTO staged_rows(run_key,row_id,payload) VALUES (?,?,?)",
                        [(run_key, str(rid), _row_json(row)) for rid, row in approved.iterrows()])

    def _insert_quarantine(self, con: sqlite3.Connection, run_key: str, records: List[Dict[str, Any]]) -> None:
        con.executemany("INSERT INTO quarantine_rows(run_key,row_id,reasons,original,recovery) VALUES (?,?,?,?,?)",
                        [(run_key, str(r["row_id"]), json.dumps(r["reasons"]), json.dumps(r["original"], default=str),
                          json.dumps(r.get("recovery", []), default=str)) for r in records])

    def commit_batch(self, run_key: str, dataset: str, run_id: str, approved: Optional[pd.DataFrame],
                     quarantine: Optional[List[Dict[str, Any]]]) -> Dict[str, Any]:
        """Atomically commit staged rows + quarantine rows for ``run_key``; no-op if already committed."""
        try:
            with self._tx() as con:
                existing = con.execute("SELECT run_id FROM batches WHERE run_key=?", (run_key,)).fetchone()
                if existing:
                    return {"committed": False, "skipped": "already_committed", "original_run_id": existing[0]}
                n_s = 0 if approved is None else len(approved)
                n_q = 0 if not quarantine else len(quarantine)
                con.execute("INSERT INTO batches(run_key,dataset,run_id,committed_at,staged_rows,quarantined_rows) "
                            "VALUES (?,?,?,?,?,?)", (run_key, dataset, run_id, now_iso(), n_s, n_q))
                if approved is not None and n_s:
                    self._insert_staged(con, run_key, approved)
                if quarantine:
                    self._insert_quarantine(con, run_key, quarantine)
            return {"committed": True, "staged_rows": n_s, "quarantined_rows": n_q}
        except sqlite3.Error as exc:
            raise StoreError(f"Output write failed and was rolled back: {exc}") from exc
        except Exception as exc:
            raise StoreError(f"Output write failed and was rolled back: {exc.__class__.__name__}: {exc}") from exc

    def load_staged(self, run_key: str) -> pd.DataFrame:
        rows = self._query("SELECT row_id,payload FROM staged_rows WHERE run_key=? ORDER BY rowid", (run_key,))
        return pd.DataFrame([json.loads(p) for _, p in rows], index=pd.Index([r for r, _ in rows], name="_row_id"))

    def run_key_for(self, run_id: str) -> Optional[str]:
        rows = self._query("SELECT run_key FROM runs WHERE run_id=?", (run_id,))
        return rows[0][0] if rows else None

    def load_quarantine(self, run_id: str, run_key: Optional[str] = None) -> pd.DataFrame:
        """Quarantined rows (original values) for a run, plus reasons. Feed back into validate() to reprocess."""
        key = run_key or self.run_key_for(run_id)
        if not key:
            raise StoreError(f"Unknown run_id '{run_id}'")
        rows = self._query("SELECT row_id,reasons,original FROM quarantine_rows WHERE run_key=? ORDER BY rowid", (key,))
        data = []
        for rid, reasons, original in rows:
            d = json.loads(original)
            d["_quarantine_reasons"] = "; ".join(json.loads(reasons))
            data.append(d)
        return pd.DataFrame(data, index=pd.Index([r[0] for r in rows], name="_row_id"))

    def counts(self) -> Dict[str, int]:
        return {t: self._query(f"SELECT COUNT(*) FROM {t}")[0][0]
                for t in ("runs", "audit_events", "recovery_log", "batches", "staged_rows", "quarantine_rows")}
