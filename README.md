# AutoFlow — a reliability layer for existing data pipelines

AutoFlow sits **beside** a pipeline you already have. It inspects data from any supported source, finds quality
problems, proposes (and only when explicitly permitted, applies) conservative fixes, **revalidates**, routes bad rows to
**quarantine**, and records every decision in an **audit trail**. It is deterministic, local, free, and needs no LLM.

```
Source -> Extract -> Transform -> [ AutoFlow gate ] -> approved rows -> Load
                                        |-> quarantined rows (with reasons)  |-> audit trail
```

> **Status honesty.** This is a standalone package built from scratch. It was **not** integrated into an existing
> AutoFlow repository (none was available), so there is no existing API/dashboard/simulator/UCI-Retail code here, and
> nothing in this repo claims to preserve them. See *Known limitations*.

## What is implemented and verified

The bundled suite includes core, connector, integration, demo, and optional HTTP-service tests. Run the commands below in your environment; optional connector behavior varies with installed extras. CI is configured for Python 3.11–3.13 with the optional dependencies installed.

| Connector (`type`) | Status | Notes |
|---|---|---|
| `csv` (any single-char delimiter) | implemented, tested | streaming, chunked, malformed rows reported, leading zeros kept |
| `json`, `jsonl` | implemented, tested | JSONL streams; bad lines become quarantined "malformed records" |
| `excel` | implemented, tested | needs `openpyxl` |
| `sqlite` | implemented, tested | opened read-only (`mode=ro`) with a write-blocking authorizer; custom queries are opt-in |
| `rest_api` | implemented, tested with an injected fetcher | page / offset / cursor pagination, loop + max-page guards, secrets via env vars; **no live network test** |
| `parquet` | implemented, **not exercised** here | needs `pyarrow`; tested only for the missing-dependency path |
| `sql` (PostgreSQL / MySQL) | implemented, **untested** | needs `sqlalchemy`, `sqlglot` and a driver for custom-query parsing; URL comes from an env var; use read-only DB credentials |
| `dataframe`, `callable`, records, PyArrow table | implemented (DataFrame/records/callable tested; Arrow untested) | pipeline adapters |

Implemented in this release: optional FastAPI HTTP service for JSON records (`autoflow[api]`). Not implemented: dashboard UI, Airflow operator, Spark/warehouse/cloud connectors, UCI Retail
connector. `autoflow.plugins.RetailInvoicesPlugin` is only a small *example* of a domain plugin.

## Concepts

* **Profiling** (`AutoFlow.profile`): types with confidence, nulls, uniqueness, candidate keys, numeric stats, IQR outliers,
  whitespace anomalies, date-order evidence, categorical frequencies, masked examples. Large inputs are sampled
  (`profiling.sample_size`); a sampled profile says `exact: false`. Output is a **proposal** — leading-zero identifiers stay
  strings, `id` columns are not assumed to be keys, nulls are not assumed to be errors.
* **Layers of checks**: structural → user rules → generic suspicions → baseline drift → optional domain plugins.
  Every finding is `confirmed` (violates an explicit rule), `suspected` (heuristic) or `inferred` (recommendation).
  **Only confirmed `error` row violations quarantine rows.** Outliers, duplicate rows without a key rule, negative numbers,
  null-heavy columns are reported, never rejected.
* **Rules** (YAML/JSON, see `examples/orders_rules.yaml`): per-column `type`, `nullable`, `required`, `min/max`,
  `min_length/max_length`, `pattern`, `allowed`, `unique`, `null_warn_pct/null_fail_pct`, `severity`, `date_format`,
  `normalize_from`, `default`; dataset checks `unique` (composite), `not_null`, `compare` (no `eval`), `row_count`,
  `reference`. User rules override plugin rules.
* **Recovery** — `Detect → Classify → Diagnose → Propose → Authorize → Recover → Revalidate → Route → Audit`:
  * **A** safe automatic (applied only if `recovery.enabled`): `trim_whitespace`, `normalize_numeric_string`
    (`1,234,567`, `1,234.50`, `5.0`→`5` for integers), `normalize_date` with explicit `normalize_from` formats.
  * **B** needs approval (`recovery.approved_rules` or an `approver` callback): `match_allowed_case`, `fill_default`,
    `drop_exact_duplicate`, `normalize_date` from column-level day/month evidence.
  * **C** never applied → quarantine: ambiguous dates (`01/02/2024` with no evidence), `1,5` / `1,234` (decimal or thousands?),
    conflicting duplicates, anything with no rule.
  * A row is repaired **atomically** (all its violations fixable and authorised) or not at all. Every repaired row is
    revalidated against the whole rule set; failures go to quarantine with their **original** values. Caller data is never mutated.
  * `revalidate_after_recovery` and `preserve_original_values` cannot be switched off.
* **Modes**: `gate` (default for the Python API; in-memory, recovery/output only if configured), `dry_run` (assess + propose,
  nothing applied or written, `can_proceed` is always `False`, `would_proceed` shows the policy outcome, rows are reported as
  `rows_would_approve/quarantine`, never as approved), `monitor` (observe only; no recovery, no writes, no `approved_data`).
  The CLI `run` command is **dry-run by default**.
* **Result contract** (`RunResult`): `status` ∈ `SUCCESS`, `SUCCESS_WITH_WARNINGS`, `PARTIAL_SUCCESS`, `FAILED`, `DRY_RUN`;
  `can_proceed` reflects policy, not "did not crash". Quarantined rows block the pipeline unless
  `validation.allow_partial_success` (and `max_quarantine_pct`) permit a partial load. Dataset-level errors (missing required
  column, empty input, schema drift, null-rate failure, malformed header…) always block. `approved_data` is `None` whenever
  `can_proceed` is false.
* **Audit & idempotency**: SQLite store with append-only `runs`, `audit_events`, `recovery_log`. `run_id` is unique per
  execution; `run_key = sha256(dataset, input fingerprint, rules/policy/recovery-rule versions)`. Staged rows and quarantine
  rows are committed **once per `run_key`, in one transaction**. So: retrying the same input+config never duplicates output
  (the new run is audited with `idempotent_replay=true`); changed input or changed config creates a new batch;
  `source_changed_since_last_run` is reported. Identical rows inside one input are **not** collapsed. Failed writes roll back,
  the run is `FAILED`, and a later retry commits exactly once. Quarantined originals can be re-fed with `AutoFlow.load_quarantine(run_id)`.
  Atomicity is per SQLite file; staging and a *separate* quarantine file are **not** atomic with each other, and nothing here is
  atomic across external systems. `audit.redact_values: true` stores hashes instead of cell values in the recovery log.

## Use it from an existing Python pipeline

```python
from autoflow import AutoFlow

af = AutoFlow.from_config("autoflow.yaml")          # or AutoFlow(rules={...})
result = af.validate(df, dataset_name="customer_orders")   # df = your transformed DataFrame

if result.can_proceed:
    load(result.approved_data)                       # your existing load step, unchanged
else:
    handle_failure(result.status, result.diagnosis, result.quarantined_data)
```

Or wrap an existing step: `@af.gate("orders")` returns the approved DataFrame or raises `QualityGateFailed`.
Monitoring only: `af.monitor(df, "orders")`. Reference data for foreign-key checks: `validate(..., reference_data={"customers": df})`.
Drift baseline: `af.approve_schema(good_df, "orders")`, then later runs report added/removed columns, type changes, null-rate shifts,
row-count changes and mean shifts. Add your own source with `@registry.register` on a `SourceConnector` subclass, your own
pipeline type with `register_adapter`, your own domain rules with `DomainPlugin` + `register_plugin`, and your own recovery rules
with `RecoveryRule` + `RecoveryRegistry.register`.

**Airflow / orchestrators:** no operator is shipped. Call `af.validate(...)` inside a `PythonOperator` and raise on
`not result.can_proceed` (or use `@af.gate`).

## Optional HTTP service (for non-Python pipelines)

Install and run the service:

```bash
python -m pip install -e ".[api]"
uvicorn autoflow.service:create_app --factory --host 127.0.0.1 --port 8000
```

Endpoints:

- `GET /health` — liveness check.
- `GET /v1/connectors` — supported connector inventory and dependency status.
- `POST /v1/profile` — profile an unfamiliar JSON records batch.
- `POST /v1/validate` — validate records and return the policy decision, approved records (when permitted), and quarantined records.

Example request body: `{"dataset_name":"orders","records":[{"order_id":1,"total":12.5}]}`. Validation rules may be supplied as a JSON object in `rules`. Request record, column, and payload-size limits are configurable through `create_app`. The service intentionally does **not** accept filesystem paths, import paths, database URLs, or arbitrary connector configs over HTTP. It does not provide authentication itself: bind to localhost for development, and place it behind authenticated TLS/reverse-proxy controls before any shared or public deployment. Avoid sending sensitive data unless your deployment's retention and access policies permit it.

This service supports record-based integration from languages and tools that can make HTTP requests; it is not a universal remote connector proxy. For database/file ingestion, configure the source connector locally or register an application-specific connector.

## CLI

```
python -m autoflow.cli connectors
python -m autoflow.cli profile  --input data.csv [--json] [--sample-size N]
python -m autoflow.cli validate --input data.csv [--rules rules.yaml] [--json]     # exit 1 if it may not proceed
python -m autoflow.cli run --config autoflow.yaml                 # DRY RUN (default)
python -m autoflow.cli run --config autoflow.yaml --commit        # gate mode; writes configured output
python -m autoflow.cli run --config autoflow.yaml --enable-recovery
python -m autoflow.cli report --run-id RUN_ID --config autoflow.yaml
```
Exit codes: `0` may proceed, `1` blocked/failed, `2` usage or configuration error. See `examples/autoflow.yaml` for every option.
Credentials never go in config: use `url_env` / `headers_env` (environment variable names).

## Install & test (Windows)

The core is Python-based. Windows verification should be run locally using the commands below.

```bat
py -3.12 -m venv .venv
.venv\\Scripts\\activate
python -m pip install --upgrade pip
python -m pip install -e .
python -m pip install -e ".[sql]"
python -m pip install -e ".[api,test,excel,parquet,sql]"
python -m unittest discover -s tests -t .
python demo\\run_demo.py
```

The core install (`python -m pip install -e .`) installs required dependencies, including PyYAML. If tests fail with
`ModuleNotFoundError: No module named 'yaml'`, install the package into the same Python environment used to run the tests,
then rerun them. The YAML integration test is expected to run; do not skip it to hide the missing dependency.
(`pytest` also works if you install it.) Memory: the CSV/JSONL/SQLite readers can stream in chunks, but `validate()` currently
loads the full dataset into a DataFrame; use `profiling.sample_size` for profiling and expect roughly a few × the data size in RAM.

## Demos (`demo/run_demo.py`, synthetic data generated on the fly)

1. **CSV, unknown schema** (IoT sensor readings): profile → generic checks without rules → rules + category-A recovery.
2. **SQLite table** (shipments; different columns and problems): category-B repairs held until approved.
3. **An "existing ETL" function** that calls AutoFlow and loads only when `can_proceed`; plus dry-run semantics.

## Known limitations

* The optional HTTP API accepts JSON records; it is not a hosted universal source proxy. No dashboard, Airflow operator, or Spark/warehouse/cloud connectors are shipped. `sql`, `parquet` and PyArrow paths still need environment-specific integration testing.
* Whole-dataset validation is in-memory (see above). Uniqueness checks hash row text; extremely wide frames are slow.
* Row ids: files → 1-based record number; DataFrames → your index (reset if not unique); SQL → result order (not rowid).
* Excel/Parquet/SQL dtype fidelity depends on the reader; text sources are kept as strings so types are only enforced via rules.
* Recovery rules are intentionally few; `normalize_date` never guesses 2-digit years. Ambiguity ⇒ quarantine.
* Probable causes in `diagnosis` are heuristics labelled as such; AutoFlow does not claim to find true root causes.
* Tested against pandas 3.0.2 only; `pyproject` declares `pandas>=2.1` (needs `DataFrame.map`) but older versions were not run.
