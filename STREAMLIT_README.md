# AutoFlow Streamlit Data Reliability Studio

A visual interface for the existing AutoFlow reliability engine. Supports CSV ingestion, REST/JSON API ingestion, schema profiling, configurable validation rules, issue inspection, and report/data exports.

## Windows setup (VS Code terminal)

Open the extracted project folder, then run:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -e .
python -m pip install -r requirements.txt
streamlit run streamlit_app.py
```

If PowerShell blocks environment activation, use Command Prompt and run:

```bat
.venv\Scripts\activate.bat
python -m pip install -e .
python -m pip install -r requirements.txt
streamlit run streamlit_app.py
```

Streamlit opens a local browser tab (usually `http://localhost:8501`). Keep the terminal open while using the app.

## Dashboard tour

1. **Load data** — upload CSV/TSV/text data, fetch a REST API, or click **Load demo orders** for an offline walkthrough. The bundled sample demonstrates malformed numeric data, whitespace, and inconsistent category casing.
2. **Explore profile** — run AutoFlow's profiler to see inferred types, confidence, null rates, uniqueness, numeric statistics, outliers, duplicate rows and candidate keys. Download a proposed schema as JSON.
3. **Validate quality** — configure required columns, not-null and uniqueness rules, optional numeric range checks, and generic statistical checks. Review findings and gate status.
4. **Export & report** — download loaded data and the latest profile / validation report.
5. **Optional AI overview** — ask Google's hosted Gemini API to interpret the computed profile.
   This does not download a model locally. Only column names and aggregate statistics are sent;
   row samples, categorical values, and examples are excluded. A Gemini API key is required
   (enter it in the UI or set `GEMINI_API_KEY`). `gemini-3.8-flash` is the default model; check
   Google AI Studio for current free-tier availability and quotas. Google's free tier may use
   submitted content to improve its products, so review the current terms before sending data.

## What AutoFlow solves

Data that is malformed, incomplete, duplicated, or unexpectedly changed can silently flow from
an extract/transform step into a warehouse, report, or model. AutoFlow adds a reviewable quality
gate before that handoff: it profiles incoming data, checks it against explicit rules, identifies
rows that fail those rules, and reports what can proceed versus what needs attention. It is a
reliability layer beside an existing ETL/ELT pipeline, not a replacement for the pipeline or a
source of business definitions.

## Good presentation use cases

- **Pre-load checks:** reject or quarantine rows with missing required fields, invalid types, or out-of-range values before loading.
- **Partner/API feeds:** inspect unfamiliar CSV or JSON data, understand its structure, and catch formatting changes early.
- **Schema change review:** compare data against an approved baseline and investigate added, removed, or changed columns.
- **Data incident triage:** view findings, quarantine details, and export a profile or report for follow-up.
- **Safe rule design:** use profile suggestions as a starting point, then let a data owner confirm the actual business rules.

## Safety and limitations

- The app validates data in memory and exports files only when you click a download. It does not commit to a database or production destination.
- A schema proposal is an inference and must be reviewed; it is not automatically treated as business truth.
- API endpoints must be reachable from the computer running Streamlit. Only GET JSON APIs are supported by the current connector. Authentication can be provided with request headers; avoid exposing secrets in screenshots or shared machines.
- The API fetcher currently uses one request/page per run in the UI. The core connector supports more pagination modes through configuration, but the UI does not expose every pagination option yet.
- Do not upload sensitive personal or confidential production data to a shared or public deployment. This starter is intended for local demonstration and review.
- The AI overview is optional and requires explicit consent each run. Even without row values,
  column names and aggregate statistics can be sensitive; use it only when your policies permit
  sending that metadata to Google. Free-tier use may be subject to Google's data-use terms.
