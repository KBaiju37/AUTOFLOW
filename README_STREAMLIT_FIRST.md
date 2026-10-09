# AutoFlow — Streamlit edition

## Launch the visual dashboard

### Fast path (Windows)
1. Extract the ZIP.
2. Open the extracted folder in VS Code.
3. Open Terminal → New Terminal and run:

```powershell
python -m pip install -e .
python -m pip install -r requirements.txt
streamlit run streamlit_app.py
```

Or double-click `run_streamlit.bat` after dependencies are installed.

## What is included
- CSV/TSV ingestion with delimiter and encoding choices.
- REST API ingestion button with public API presets or custom GET URL, JSON parameters, record path, timeout, and optional headers.
- Data preview and column diagnostics.
- AutoFlow profiling: type inference, confidence, null rates, uniqueness, duplicate rows, numeric statistics/outliers, candidate keys and schema proposal.
- Interactive quality rules: expected columns, non-null columns, unique columns, optional numeric ranges and generic checks.
- Gate result, issue table, detailed issue JSON, and downloads for CSV, JSON, profile, schema proposal, and validation report.

Read `STREAMLIT_README.md` for setup, dashboard details, and current limitations.
