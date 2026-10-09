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

1. **Ingest data** — upload CSV/TSV/text data or fetch a REST API. The default Open-Meteo geocoding preset is a public API demo; custom endpoints support JSON query parameters, dotted record paths, timeouts and headers.
2. **Profile & explore** — run AutoFlow's profiler to see inferred types, confidence, null rates, uniqueness, numeric statistics, outliers, duplicate rows and candidate keys. Download a proposed schema as JSON.
3. **Validate quality** — configure required columns, not-null and uniqueness rules, optional numeric range checks, and generic statistical checks. Review findings and gate status.
4. **Export & report** — download loaded data and the latest profile / validation report.

## Safety and limitations

- The app validates data in memory and exports files only when you click a download. It does not commit to a database or production destination.
- A schema proposal is an inference and must be reviewed; it is not automatically treated as business truth.
- API endpoints must be reachable from the computer running Streamlit. Only GET JSON APIs are supported by the current connector. Authentication can be provided with request headers; avoid exposing secrets in screenshots or shared machines.
- The API fetcher currently uses one request/page per run in the UI. The core connector supports more pagination modes through configuration, but the UI does not expose every pagination option yet.
- Do not upload sensitive personal or confidential production data to a shared or public deployment. This starter is intended for local demonstration and review.
