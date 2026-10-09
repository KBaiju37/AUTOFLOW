"""SQL connectors.

* ``sqlite``  - stdlib only, opened read-only (``mode=ro``). Tested.
* ``sql``     - PostgreSQL / MySQL / anything SQLAlchemy supports. Implemented but NOT
                exercised by the bundled tests (SQLAlchemy and DB drivers are optional).

Safety: table identifiers are validated and quoted; custom queries require explicit
opt-in and are parsed as read-only query statements when SQLGlot is installed. Without
SQLGlot, the connector fails closed on WITH/CTE statements and rejects common
write-capable SELECT clauses. SQLite additionally opens in read-only mode and installs
a SQLite authorizer. Use read-only database credentials for SQLAlchemy connections.
"""
from __future__ import annotations

import os
import pathlib
import re
import sqlite3
from typing import Any, Dict, Iterator, List, Optional

import pandas as pd

from ..contracts import ConfigError, Dataset, DependencyMissing, SourceConnector, SourceError
from ..registry import registry
from ..typeops import normalize_frame
from ..util import dependency_available, env_secret, frame_fingerprint, redact_text

_QUERY_START = re.compile(r"^\s*(select|with)\b", re.I)
_SIMPLE_SELECT = re.compile(r"^\s*select\b", re.I)
_UNSAFE_SELECT_CLAUSE = re.compile(
    r"\b(into|outfile|dumpfile)\b|\bfor\s+(update|share)\b|"
    r"\block\s+in\s+share\s+mode\b",
    re.I,
)


def check_query(query: str) -> str:
    """Accept one read-only query; fail closed when a SQL AST parser is unavailable."""
    if not isinstance(query, str) or not query.strip():
        raise SourceError("A non-empty SQL query is required")
    q = query.strip()
    q = q[:-1].rstrip() if q.endswith(";") else q
    if ";" in q:
        raise SourceError("Only a single SQL statement is allowed")

    try:
        import sqlglot
        from sqlglot import exp
    except ImportError:
        sqlglot = None
        exp = None

    if sqlglot is not None:
        try:
            parsed = sqlglot.parse(q)
        except Exception as exc:
            raise SourceError("SQL query could not be parsed safely") from exc
        if len(parsed) != 1 or parsed[0] is None:
            raise SourceError("Only a single SQL statement is allowed")
        statement = parsed[0]
        if not isinstance(statement, exp.Query):
            raise SourceError("Only read-only SELECT queries are allowed")
        if statement.find(exp.Into) is not None:
            raise SourceError("SELECT INTO is not allowed in read-only queries")
        if statement.find(exp.Lock) is not None:
            raise SourceError("Locking clauses are not allowed in read-only queries")
        return q

    # Conservative fallback: reject WITH because a prefix check cannot safely
    # distinguish a SELECT CTE from a data-changing CTE across SQL dialects.
    if not _SIMPLE_SELECT.match(q):
        if _QUERY_START.match(q):
            raise SourceError("WITH queries require the optional 'sqlglot' dependency for safe parsing")
        raise SourceError("Only SELECT queries are allowed")
    if _UNSAFE_SELECT_CLAUSE.search(q):
        raise SourceError("Write-capable or locking SELECT clauses are not allowed")
    return q


def _quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


@registry.register
class SqliteSource(SourceConnector):
    type = "sqlite"
    description = "SQLite database (read-only). Use 'table', or 'query' with allow_custom_query: true."
    capabilities = {"chunked": True, "sampling": True, "read_only": True, "stable_row_ids": False}

    def validate(self) -> List[str]:
        p = self.config.get("path")
        errs: List[str] = []
        if not p:
            return ["'path' is required"]
        if not os.path.isfile(p):
            return [f"Database file not found: {p}"]
        has_t, has_q = bool(self.config.get("table")), bool(self.config.get("query"))
        if has_t == has_q:
            errs.append("Provide exactly one of 'table' or 'query'")
        if has_q and not self.config.get("allow_custom_query"):
            errs.append("Custom queries are disabled; set allow_custom_query: true to enable")
        return errs

    def _connect(self) -> sqlite3.Connection:
        uri = pathlib.Path(self.config["path"]).resolve().as_uri() + "?mode=ro"
        try:
            con = sqlite3.connect(uri, uri=True)
            # Defense in depth: SQLite's authorizer blocks write/schema changes
            # even if a future query-validation regression lets one through.
            write_actions = {
                sqlite3.SQLITE_INSERT, sqlite3.SQLITE_UPDATE, sqlite3.SQLITE_DELETE,
                sqlite3.SQLITE_CREATE_INDEX, sqlite3.SQLITE_CREATE_TABLE,
                sqlite3.SQLITE_CREATE_TEMP_INDEX, sqlite3.SQLITE_CREATE_TEMP_TABLE,
                sqlite3.SQLITE_CREATE_TEMP_TRIGGER, sqlite3.SQLITE_CREATE_TEMP_VIEW,
                sqlite3.SQLITE_CREATE_TRIGGER, sqlite3.SQLITE_CREATE_VIEW,
                sqlite3.SQLITE_DROP_INDEX, sqlite3.SQLITE_DROP_TABLE,
                sqlite3.SQLITE_DROP_TEMP_INDEX, sqlite3.SQLITE_DROP_TEMP_TABLE,
                sqlite3.SQLITE_DROP_TEMP_TRIGGER, sqlite3.SQLITE_DROP_TEMP_VIEW,
                sqlite3.SQLITE_DROP_TRIGGER, sqlite3.SQLITE_DROP_VIEW,
                sqlite3.SQLITE_ALTER_TABLE, sqlite3.SQLITE_ATTACH, sqlite3.SQLITE_DETACH,
                sqlite3.SQLITE_REINDEX, sqlite3.SQLITE_ANALYZE,
            }

            def authorize(action, arg1, arg2, db_name, trigger_name):
                if action in write_actions:
                    return sqlite3.SQLITE_DENY
                return sqlite3.SQLITE_OK

            con.set_authorizer(authorize)
            return con
        except sqlite3.Error as exc:
            raise SourceError(f"Cannot open SQLite database read-only: {exc}") from exc

    def _statement(self, con: sqlite3.Connection):
        errs = self.validate()
        if errs:
            raise SourceError("; ".join(errs))
        params: Any = self.config.get("params") or {}
        if self.config.get("query"):
            return check_query(self.config["query"]), params
        table = self.config["table"]
        row = con.execute("SELECT type FROM sqlite_master WHERE type IN ('table','view') AND name=?", (table,)).fetchone()
        if not row:
            raise SourceError(f"Table or view '{table}' not found")
        cols = "*"
        if self.config.get("columns"):
            existing = {r[1] for r in con.execute(f"PRAGMA table_info({_quote_ident(table)})")}
            missing = [c for c in self.config["columns"] if c not in existing]
            if missing:
                raise SourceError(f"Columns not found in '{table}': {missing}")
            cols = ", ".join(_quote_ident(c) for c in self.config["columns"])
        return f"SELECT {cols} FROM {_quote_ident(table)}", params

    def _frame(self, names, rows, start: int) -> pd.DataFrame:
        df = pd.DataFrame([list(r) for r in rows], columns=names, dtype=object,
                          index=pd.Index(range(start, start + len(rows)), name="_row_id"))
        return normalize_frame(df)[0]

    def read(self, limit: Optional[int] = None) -> Dataset:
        con = self._connect()
        try:
            sql, params = self._statement(con)
            try:
                cur = con.execute(sql, params)
            except sqlite3.Error as exc:
                raise SourceError(f"Query failed: {exc}") from exc
            names = [d[0] for d in cur.description]
            rows = cur.fetchmany(limit) if limit else cur.fetchall()
        finally:
            con.close()
        frame, dups = self._frame(names, rows, 1), []
        frame, dups = normalize_frame(frame)
        return Dataset(frame, source_type="sqlite", source_id=os.path.basename(self.config["path"]),
                       fingerprint=frame_fingerprint(frame), duplicate_columns=dups,
                       metadata={"table": self.config.get("table"), "note": "row ids are result order, not rowids"},
                       total_rows_in_source=None if limit else len(frame))

    def iter_chunks(self, chunksize: int = 50_000) -> Iterator[pd.DataFrame]:
        con = self._connect()
        try:
            sql, params = self._statement(con)
            cur = con.execute(sql, params)
            names = [d[0] for d in cur.description]
            start = 1
            while True:
                rows = cur.fetchmany(chunksize)
                if not rows:
                    break
                yield self._frame(names, rows, start)
                start += len(rows)
        finally:
            con.close()


@registry.register
class SqlAlchemySource(SourceConnector):
    type = "sql"
    description = ("PostgreSQL/MySQL/etc. through SQLAlchemy. Provide the URL via environment variable "
                   "('url_env'). IMPLEMENTED BUT UNTESTED in this repository's bundled tests.")
    dependencies = ("sqlalchemy",)
    status = "implemented-untested"
    capabilities = {"chunked": True, "sampling": True, "read_only": False, "stable_row_ids": False}

    def validate(self) -> List[str]:
        errs: List[str] = []
        if not dependency_available("sqlalchemy"):
            errs.append("sqlalchemy is not installed (pip install sqlalchemy plus a driver such as psycopg2-binary or pymysql)")
        if not self.config.get("url_env"):
            errs.append("'url_env' (name of the environment variable holding the connection URL) is required; "
                        "credentials must not be placed in config files")
        has_t, has_q = bool(self.config.get("table")), bool(self.config.get("query"))
        if has_t == has_q:
            errs.append("Provide exactly one of 'table' or 'query'")
        if has_q and not self.config.get("allow_custom_query"):
            errs.append("Custom queries are disabled; set allow_custom_query: true to enable")
        if has_q and not dependency_available("sqlglot"):
            errs.append("Custom SQL queries require sqlglot for safe statement parsing (pip install 'autoflow[sql]')")
        return errs

    def _engine(self):
        if not dependency_available("sqlalchemy"):
            raise DependencyMissing("The 'sql' connector requires sqlalchemy and a database driver")
        import sqlalchemy as sa

        url = env_secret(self.config["url_env"])
        try:
            return sa, sa.create_engine(url)
        except Exception as exc:
            raise SourceError(redact_text(f"Cannot create engine: {exc}", [url])) from exc

    def read(self, limit: Optional[int] = None) -> Dataset:
        errs = self.validate()
        if errs:
            raise SourceError("; ".join(errs))
        sa, engine = self._engine()
        url = os.environ.get(self.config["url_env"], "")
        try:
            if self.config.get("query"):
                stmt = sa.text(check_query(self.config["query"]))
                params = self.config.get("params") or {}
            else:
                if not sa.inspect(engine).has_table(self.config["table"]):
                    raise SourceError(f"Table '{self.config['table']}' not found")
                stmt = sa.select(sa.text("*")).select_from(sa.table(self.config["table"]))
                params = {}
                if limit:
                    stmt = stmt.limit(limit)
            with engine.connect() as con:
                df = pd.read_sql(stmt, con, params=params)
        except SourceError:
            raise
        except Exception as exc:
            raise SourceError(redact_text(f"Query failed: {exc.__class__.__name__}: {exc}", [url])) from exc
        if limit:
            df = df.head(limit)
        df.index = pd.Index(range(1, len(df) + 1), name="_row_id")
        df, dups = normalize_frame(df)
        return Dataset(df, source_type="sql", source_id=self.config.get("table") or "query",
                       fingerprint=frame_fingerprint(df), duplicate_columns=dups,
                       total_rows_in_source=None if limit else len(df))
