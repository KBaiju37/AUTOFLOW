import json
import os
import sqlite3
import tempfile
import unittest
from unittest import mock

import pandas as pd

from autoflow import AutoFlow
from autoflow.contracts import ConfigError, DependencyMissing, SourceError
from autoflow.registry import connector_for_path, registry
from autoflow.sources.api import RestApiSource
from autoflow.sources.files import CsvSource, ExcelSource, JsonlSource, JsonSource, ParquetSource
from autoflow.sources.sql import SqlAlchemySource, SqliteSource, check_query
from autoflow.util import dependency_available


class TmpCase(unittest.TestCase):
    def setUp(self):
        self._td = tempfile.TemporaryDirectory()
        self.dir = self._td.name
        self.addCleanup(self._td.cleanup)

    def write(self, name, text, mode="w", **kw):
        p = os.path.join(self.dir, name)
        with open(p, mode, **kw) as fh:
            fh.write(text)
        return p


class CsvTests(TmpCase):
    def test_valid_csv_preserves_leading_zeros_and_row_ids(self):
        p = self.write("a.csv", "id,amt\n00125,5\n00126,6\n")
        ds = CsvSource({"path": p}).read()
        self.assertEqual(list(ds.frame["id"]), ["00125", "00126"])
        self.assertEqual(list(ds.frame.index), [1, 2])
        self.assertEqual(ds.source_type, "csv")

    def test_malformed_rows_reported_not_dropped_silently(self):
        p = self.write("a.csv", "a,b\n1,2\n3\n4,5,6\n7,8\n")
        ds = CsvSource({"path": p}).read()
        self.assertEqual(len(ds.frame), 2)
        self.assertEqual([m["row_id"] for m in ds.malformed], [2, 3])

    def test_empty_and_header_only_files(self):
        p = self.write("e.csv", "")
        self.assertTrue(CsvSource({"path": p}).read().metadata["empty"])
        p2 = self.write("h.csv", "a,b\n")
        self.assertEqual(len(CsvSource({"path": p2}).read().frame), 0)

    def test_missing_file_and_bad_delimiter(self):
        with self.assertRaises(SourceError):
            CsvSource({"path": os.path.join(self.dir, "nope.csv")}).read()
        p = self.write("a.csv", "a\n1\n")
        with self.assertRaises(SourceError):
            CsvSource({"path": p, "delimiter": ";;"}).read()

    def test_duplicate_headers_flagged(self):
        p = self.write("d.csv", "a,a,b\n1,2,3\n")
        ds = CsvSource({"path": p}).read()
        self.assertEqual(ds.duplicate_columns, ["a"])
        self.assertEqual(len(set(ds.frame.columns)), 3)

    def test_tsv_and_chunking_and_count(self):
        p = self.write("a.tsv", "a\tb\n" + "".join(f"{i}\t{i}\n" for i in range(25)))
        c = connector_for_path(p)
        self.assertEqual(c.config["delimiter"], "\t")
        chunks = list(c.iter_chunks(10))
        self.assertEqual([len(x) for x in chunks], [10, 10, 5])
        self.assertEqual(c.count_rows(), 25)
        self.assertEqual(len(c.read(limit=7).frame), 7)

    def test_bad_encoding(self):
        p = self.write("l.csv", b"a\n\xff\xfe\n", mode="wb")
        with self.assertRaises(SourceError):
            CsvSource({"path": p}).read()

    def test_allowed_roots_and_max_bytes(self):
        p = self.write("a.csv", "a\n1\n")
        self.assertTrue(CsvSource({"path": p, "allowed_roots": ["/definitely/elsewhere"]}).validate())
        self.assertTrue(CsvSource({"path": p, "max_bytes": 1}).validate())

    def test_source_file_is_never_modified(self):
        p = self.write("a.csv", "a,b\n1,2\n")
        with open(p, "rb") as fh:
            before = fh.read()
        mtime = os.path.getmtime(p)
        AutoFlow().validate(CsvSource({"path": p}), "x")
        with open(p, "rb") as fh:
            self.assertEqual(fh.read(), before)
        self.assertEqual(os.path.getmtime(p), mtime)


class JsonTests(TmpCase):
    def test_json_list_and_records_key(self):
        p = self.write("a.json", json.dumps([{"a": 1, "b": "x"}, {"a": 2}]))
        ds = JsonSource({"path": p}).read()
        self.assertEqual(list(ds.frame.columns), ["a", "b"])
        p2 = self.write("b.json", json.dumps({"data": {"rows": [{"a": 1}]}}))
        self.assertEqual(len(JsonSource({"path": p2, "records_key": "data.rows"}).read().frame), 1)
        with self.assertRaises(SourceError):
            JsonSource({"path": p2}).read()

    def test_json_invalid_and_non_object_records(self):
        with self.assertRaises(SourceError):
            JsonSource({"path": self.write("bad.json", "{oops")}).read()
        ds = JsonSource({"path": self.write("m.json", json.dumps([{"a": 1}, 5, "x"]))}).read()
        self.assertEqual(len(ds.malformed), 2)

    def test_jsonl_with_bad_lines(self):
        p = self.write("a.jsonl", '{"a":1}\n\nnot json\n{"a":2}\n[1]\n')
        ds = JsonlSource({"path": p}).read()
        self.assertEqual(len(ds.frame), 2)
        self.assertEqual([m["row_id"] for m in ds.malformed], [3, 5])
        self.assertEqual(sum(len(c) for c in JsonlSource({"path": p}).iter_chunks(1)), 2)

    def test_integers_not_floated_when_keys_missing(self):
        p = self.write("a.jsonl", '{"a":1,"b":2}\n{"a":3}\n')
        ds = JsonlSource({"path": p}).read()
        self.assertEqual(ds.frame.loc[1, "a"], 1)
        self.assertIsInstance(ds.frame.loc[1, "a"], int)


class SqliteTests(TmpCase):
    def make_db(self):
        p = os.path.join(self.dir, "t.db")
        con = sqlite3.connect(p)
        con.execute("CREATE TABLE people (code TEXT, age INTEGER)")
        con.executemany("INSERT INTO people VALUES (?,?)", [("007", 30), ("008", None)])
        con.commit()
        con.close()
        return p

    def test_read_table_read_only(self):
        p = self.make_db()
        ds = SqliteSource({"path": p, "table": "people"}).read()
        self.assertEqual(list(ds.frame["code"]), ["007", "008"])
        with mock.patch.object(SqliteSource, "_statement", return_value=("DELETE FROM people", {})):
            with self.assertRaises(SourceError):
                SqliteSource({"path": p, "table": "people"}).read()
        con = sqlite3.connect(p)
        try:
            self.assertEqual(con.execute("SELECT COUNT(*) FROM people").fetchone()[0], 2)
        finally:
            con.close()

    def test_custom_query_controls(self):
        p = self.make_db()
        self.assertTrue(SqliteSource({"path": p, "query": "SELECT * FROM people"}).validate())
        ok = SqliteSource({"path": p, "query": "SELECT * FROM people WHERE code = :c", "params": {"c": "007"},
                           "allow_custom_query": True})
        self.assertEqual(len(ok.read().frame), 1)
        for bad in ("DROP TABLE people", "SELECT 1; DROP TABLE people"):
            with self.assertRaises(SourceError):
                SqliteSource({"path": p, "query": bad, "allow_custom_query": True}).read()

    def test_query_guard_rejects_data_changing_ctes_and_write_clauses(self):
        bad_queries = (
            "WITH x AS (SELECT 1) DELETE FROM people",
            "WITH x AS (SELECT 1) UPDATE people SET age = 0",
            "WITH x AS (SELECT 1) INSERT INTO people VALUES ('009', 20)",
            "SELECT * INTO new_people FROM people",
            "SELECT * FROM people FOR UPDATE",
            "DELETE FROM people",
            "SELECT 1; DELETE FROM people",
        )
        for query in bad_queries:
            with self.subTest(query=query):
                with self.assertRaises(SourceError):
                    check_query(query)

    def test_sqlalchemy_custom_query_requires_ast_parser(self):
        if dependency_available("sqlglot"):
            self.skipTest("sqlglot is installed; parser-required path is not applicable")
        src = SqlAlchemySource({
            "url_env": "AF_TEST_DATABASE_URL",
            "query": "WITH x AS (SELECT 1) DELETE FROM people",
            "allow_custom_query": True,
        })
        self.assertTrue(any("sqlglot" in err for err in src.validate()))

    def test_query_guard_allows_simple_select_and_sqlite_remains_unchanged(self):
        p = self.make_db()
        self.assertEqual(check_query("SELECT * FROM people WHERE code = :code"),
                         "SELECT * FROM people WHERE code = :code")
        source = SqliteSource({
            "path": p,
            "query": "WITH x AS (SELECT * FROM people) DELETE FROM people",
            "allow_custom_query": True,
        })
        with self.assertRaises(SourceError):
            source.read()
        con = sqlite3.connect(p)
        try:
            self.assertEqual(con.execute("SELECT COUNT(*) FROM people").fetchone()[0], 2)
        finally:
            con.close()

    def test_unknown_table_and_injection_in_name(self):
        p = self.make_db()
        for t in ("nope", 'people"; DROP TABLE people; --'):
            with self.assertRaises(SourceError):
                SqliteSource({"path": p, "table": t}).read()

    def test_chunks(self):
        p = self.make_db()
        self.assertEqual([len(c) for c in SqliteSource({"path": p, "table": "people"}).iter_chunks(1)], [1, 1])


class ApiTests(unittest.TestCase):
    def pages(self, data, size):
        calls = []

        def fetch(url, params, headers, timeout):
            calls.append((dict(params), dict(headers)))
            page = params["page"]
            return {"items": data[(page - 1) * size: page * size]}
        return fetch, calls

    def test_page_pagination(self):
        data = [{"i": i} for i in range(25)]
        fetch, calls = self.pages(data, 10)
        src = RestApiSource({"url": "https://x.test/api", "records_path": "items",
                             "pagination": {"type": "page", "page_size": 10}}, fetcher=fetch)
        ds = src.read()
        self.assertEqual(len(ds.frame), 25)
        self.assertEqual(len(calls), 3)

    def test_offset_and_cursor(self):
        data = [{"i": i} for i in range(5)]

        def off(url, params, headers, timeout):
            return data[params["offset"]: params["offset"] + params["limit"]]
        ds = RestApiSource({"url": "http://x.test", "pagination": {"type": "offset", "page_size": 2}}, fetcher=off).read()
        self.assertEqual(len(ds.frame), 5)

        def cur(url, params, headers, timeout):
            return {"r": [{"i": 1}], "next": None} if params.get("c") == "b" else {"r": [{"i": 0}], "next": "b"}
        ds = RestApiSource({"url": "http://x.test", "records_path": "r",
                            "pagination": {"type": "cursor", "cursor_param": "c", "next_cursor_path": "next"}},
                           fetcher=cur).read()
        self.assertEqual(len(ds.frame), 2)

    def test_cursor_loop_detected(self):
        def loop(url, params, headers, timeout):
            return {"r": [{"i": 1}], "next": "same"}
        with self.assertRaises(SourceError):
            RestApiSource({"url": "http://x.test", "records_path": "r", "max_pages": 10,
                           "pagination": {"type": "cursor", "cursor_param": "c", "next_cursor_path": "next"}},
                          fetcher=loop).read()

    def test_failure_midway_is_an_error_and_secrets_redacted(self):
        os.environ["AF_TEST_TOKEN"] = "supersecret-token-value"
        self.addCleanup(os.environ.pop, "AF_TEST_TOKEN", None)

        def fetch(url, params, headers, timeout):
            if params["page"] == 2:
                raise RuntimeError(f"boom {headers['Authorization']}")
            return [{"i": 1}, {"i": 2}]
        src = RestApiSource({"url": "https://x.test", "headers_env": {"Authorization": "AF_TEST_TOKEN"},
                             "pagination": {"type": "page", "page_size": 2}}, fetcher=fetch)
        with self.assertRaises(SourceError) as cm:
            src.read()
        self.assertIn("page 2", str(cm.exception))
        self.assertNotIn("supersecret", str(cm.exception))

    def test_url_can_come_from_environment(self):
        os.environ["AF_TEST_API_URL"] = "https://api.example.test/records"
        self.addCleanup(os.environ.pop, "AF_TEST_API_URL", None)
        seen = {}

        def fetch(url, params, headers, timeout):
            seen["url"] = url
            return [{"id": 1}]

        src = RestApiSource({"url_env": "AF_TEST_API_URL"}, fetcher=fetch)
        ds = src.read()
        self.assertEqual(seen["url"], "https://api.example.test/records")
        self.assertEqual(len(ds.frame), 1)

    def test_max_pages_marks_incomplete_and_validation(self):
        def fetch(url, params, headers, timeout):
            return [{"i": 1}, {"i": 2}]
        ds = RestApiSource({"url": "http://x.test", "max_pages": 2,
                            "pagination": {"type": "page", "page_size": 2}}, fetcher=fetch).read()
        self.assertTrue(ds.metadata["incomplete"])
        self.assertTrue(ds.warnings)
        self.assertTrue(RestApiSource({"url": "ftp://x"}).validate())
        self.assertTrue(RestApiSource({"url": "http://x", "pagination": {"type": "page"}}).validate())


class OptionalDependencyTests(TmpCase):
    def test_excel_roundtrip_when_available(self):
        if not dependency_available("openpyxl"):
            self.skipTest("openpyxl not installed")
        p = os.path.join(self.dir, "a.xlsx")
        pd.DataFrame({"a": [1, 2], "b": ["x", None]}).to_excel(p, index=False)
        ds = ExcelSource({"path": p}).read()
        self.assertEqual(len(ds.frame), 2)

    def test_excel_corrupt_file(self):
        if not dependency_available("openpyxl"):
            self.skipTest("openpyxl not installed")
        with self.assertRaises(SourceError):
            ExcelSource({"path": self.write("bad.xlsx", "not a workbook")}).read()

    def test_missing_dependency_behaviour(self):
        with mock.patch("autoflow.sources.files.dependency_available", return_value=False):
            with self.assertRaises(DependencyMissing):
                ParquetSource({"path": "x.parquet"}).read()
            with self.assertRaises(DependencyMissing):
                ExcelSource({"path": "x.xlsx"}).read()

    def test_parquet_roundtrip_when_available(self):
        if not dependency_available("pyarrow"):
            self.skipTest("pyarrow not installed")
        p = os.path.join(self.dir, "a.parquet")
        pd.DataFrame({"a": [1, 2]}).to_parquet(p)
        self.assertEqual(len(ParquetSource({"path": p}).read().frame), 2)

    def test_unsupported_extension_and_registry(self):
        with self.assertRaises(ConfigError):
            connector_for_path("x.weird")
        self.assertIn("csv", registry.types())
        status = {r["type"]: r["status"] for r in registry.describe()}
        self.assertEqual(status["sql"], "implemented-untested" if dependency_available("sqlalchemy") else "dependency-missing")


class ProfilingTests(TmpCase):
    def test_unknown_dataset_profile(self):
        n = 40
        df = pd.DataFrame({
            "cust_code": [f"{i:05d}" for i in range(n)],
            "amount": [str(i * 1.5) for i in range(n - 2)] + ["abc", "n/a"],
            "when": ["2024-01-%02d" % (i % 28 + 1) for i in range(n)],
            "const": ["x"] * n,
            "email": [f"u{i}@e.com" for i in range(n)],
            "note": [" a " if i == 0 else "b" for i in range(n)],
            "maybe": [None if i % 2 else "v" for i in range(n)],
        })
        pr = AutoFlow().profile(df, "unk")
        c = pr["columns"]
        self.assertEqual(c["cust_code"]["inferred_type"], "string")      # leading zeros not converted
        self.assertTrue(c["cust_code"]["leading_zeros"])
        self.assertEqual(c["cust_code"]["possible_role"], "identifier")
        self.assertEqual(c["amount"]["inferred_type"], "number")
        self.assertEqual(c["amount"]["invalid_for_inferred_type"], 2)
        self.assertTrue(c["amount"]["needs_confirmation"])
        self.assertEqual(c["when"]["inferred_type"], "datetime")
        self.assertTrue(c["const"]["is_constant"])
        self.assertEqual(c["email"]["examples"], ["***", "***", "***"])
        self.assertEqual(c["note"]["whitespace_anomalies"], 1)
        self.assertAlmostEqual(c["maybe"]["null_pct"], 0.5)
        self.assertEqual(pr["schema_proposal"]["status"], "proposal")
        self.assertIn("cust_code", pr["candidate_keys"])

    def test_id_column_not_assumed_unique(self):
        df = pd.DataFrame({"id": [1, 2, 2, 3] * 5})
        pr = AutoFlow().profile(df)
        self.assertNotIn("id", pr["candidate_keys"])
        self.assertNotIn("unique", pr["columns"]["id"]["suggested_rules"])

    def test_ambiguous_dates_flagged(self):
        df = pd.DataFrame({"d": ["01/02/2024", "03/04/2024", "05/06/2024"] * 4})
        c = AutoFlow().profile(df)["columns"]["d"]
        self.assertEqual(c["date_order"], "ambiguous")
        self.assertTrue(c["needs_confirmation"])
        df2 = pd.DataFrame({"d": ["25/02/2024", "03/04/2024"] * 6})
        self.assertEqual(AutoFlow().profile(df2)["columns"]["d"]["date_order"], "dmy")

    def test_empty_and_sampling(self):
        pr = AutoFlow().profile(pd.DataFrame({"a": []}))
        self.assertTrue(pr["empty"])
        big = pd.DataFrame({"a": range(5000)})
        pr = AutoFlow({"profiling": {"sample_size": 1000}}).profile(big)
        self.assertTrue(pr["sampled"])
        self.assertFalse(pr["exact"])
        self.assertEqual(pr["rows_profiled"], 1000)
        self.assertEqual(pr["rows_loaded"], 5000)
        self.assertTrue(AutoFlow().profile(pd.DataFrame({"a": range(10)}))["exact"])

    def test_mixed_native_dtypes_and_outliers(self):
        df = pd.DataFrame({"i": [1, 2, 3] * 10 + [10000], "f": [1.5] * 30 + [2.0], "b": [True, False] * 15 + [True],
                           "t": pd.date_range("2024-01-01", periods=31)})
        c = AutoFlow().profile(df)["columns"]
        self.assertEqual(c["i"]["inferred_type"], "integer")
        self.assertEqual(c["i"]["potential_outliers"], 1)
        self.assertEqual(c["f"]["inferred_type"], "number")
        self.assertEqual(c["b"]["inferred_type"], "boolean")
        self.assertEqual(c["t"]["inferred_type"], "datetime")

    def test_schema_drift_against_approved_baseline(self):
        af = AutoFlow()
        base_df = pd.DataFrame({"a": range(20), "b": ["x"] * 20, "c": [1.0] * 20})
        af.approve_schema(base_df, "ds")
        new = pd.DataFrame({"a": range(20), "c": [1.0] * 20, "z": [1] * 20})
        r = af.validate(new, "ds")
        codes = {i.code for i in r.issues}
        self.assertIn("schema_drift_removed", codes)
        self.assertIn("schema_drift_added", codes)
        self.assertFalse(r.can_proceed)
        self.assertEqual(r.status, "FAILED")


if __name__ == "__main__":
    unittest.main()
