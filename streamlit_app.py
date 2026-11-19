from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

from autoflow.engine import AutoFlow
from autoflow.profiling import schema_proposal
from autoflow.rules import RuleSet
from autoflow.sources.api import RestApiSource
from autoflow.contracts import Mode
from autoflow.gemini_analysis import DEFAULT_MODEL, generate_overview
from autoflow.util import safe_nunique

ROOT = Path(__file__).resolve().parent

st.set_page_config(page_title="AutoFlow | Data Quality Workbench", page_icon="A", layout="wide")

st.markdown("""
<style>
.block-container {padding-top: 1.4rem; padding-bottom: 2.5rem; max-width: 1360px;}
.hero {padding: 2.25rem 2rem 1.8rem; border: 1px solid #dce7ef; border-radius: 18px; background: linear-gradient(120deg,#10243a,#176258); color: #fff; margin-bottom: 1.25rem;}
.hero .eyebrow {display: block; color: #a7f3d0; font-size: .75rem; font-weight: 700; line-height: 1.25; letter-spacing: .14em; margin: .1rem 0 .6rem;}
.hero h1 {margin: 0; font-size: 2.35rem; line-height: 1.15; color: #fff;}
.hero p {max-width: 780px; margin: .6rem 0 0; color: #d9e8f1; font-size: 1.04rem;}
div[data-testid="stMetric"] {background: #f7fafc; padding: .85rem 1rem; border-radius: 12px; border: 1px solid #e3ebf2;}
div[data-testid="stTabs"] button {font-weight: 650;}
div[data-testid="stExpander"] {border-color: #e3ebf2; border-radius: 12px;}
</style>
<div class="hero">
  <p class="eyebrow">DATA QUALITY WORKBENCH</p>
  <h1>Make data safer to move.</h1>
  <p>Inspect incoming data, enforce your rules, and catch quality issues before they reach reports, models, or downstream systems.</p>
</div>
""", unsafe_allow_html=True)

if "df" not in st.session_state:
    st.session_state.df = None
if "source_label" not in st.session_state:
    st.session_state.source_label = ""
if "last_profile" not in st.session_state:
    st.session_state.last_profile = None
if "last_result" not in st.session_state:
    st.session_state.last_result = None
if "ai_overview" not in st.session_state:
    st.session_state.ai_overview = None

with st.sidebar:
    st.header("AutoFlow")
    st.caption("DATA RELIABILITY")
    st.divider()
    st.markdown("### Review before you load")
    st.caption("A quality gate for data already moving through your pipeline.")
    st.divider()
    st.markdown("**1** · Load a file or API")
    st.markdown("**2** · Profile and explore")
    st.markdown("**3** · Validate your rules")
    st.markdown("**4** · Review and export")
    st.divider()
    st.caption("Safe by default: checks run in memory. This dashboard does not write to a production destination.")

source_tab, profile_tab, validate_tab, export_tab = st.tabs(["Load data", "Explore profile", "Quality gate", "Export"])

with source_tab:
    st.subheader("Bring data into the review")
    st.caption("Start with a local file, a ready-to-run sample, or a REST/JSON endpoint.")
    left, right = st.columns([1, 1], gap="large")
    with left:
        st.markdown("#### Upload a file")
        uploaded = st.file_uploader("Choose CSV", type=["csv", "tsv", "txt"], key="csv_upload")
        delimiter_choice = st.selectbox("Delimiter", ["Auto-detect", "Comma ,", "Tab \\t", "Semicolon ;", "Pipe |"], index=0)
        encoding = st.selectbox("Text encoding", ["utf-8", "utf-8-sig", "latin-1"], index=0)
        load_col, detail_col = st.columns([1, 1.5], vertical_alignment="center")
        with load_col:
            load_uploaded = st.button("Load uploaded file", type="primary", disabled=uploaded is None, width="stretch")
        with detail_col:
            st.caption("CSV, TSV, and delimited text")
        if load_uploaded:
            try:
                sep_map = {"Auto-detect": None, "Comma ,": ",", "Tab \\t": "\t", "Semicolon ;": ";", "Pipe |": "|"}
                df = pd.read_csv(uploaded, sep=sep_map[delimiter_choice], engine="python" if sep_map[delimiter_choice] is None else "c", encoding=encoding, on_bad_lines="error")
                if df.columns.duplicated().any():
                    st.warning("Duplicate column names were detected. Rename duplicate headers before relying on column-level rules.")
                st.session_state.df = df
                st.session_state.source_label = uploaded.name
                st.session_state.last_profile = None
                st.session_state.last_result = None
                st.session_state.ai_overview = None
                st.success(f"Loaded {len(df):,} rows and {len(df.columns):,} columns from {uploaded.name}.")
            except Exception as exc:
                st.error(f"CSV ingestion failed: {exc}")
        st.divider()
        st.markdown("#### Want a quick demo?")
        st.caption("Load the included five-row orders sample. It has a malformed amount, extra whitespace, and inconsistent category casing.")
        if st.button("Load demo orders", width="stretch"):
            try:
                demo_path = ROOT / "examples" / "orders.csv"
                demo_df = pd.read_csv(demo_path, dtype={"order_id": "string"})
                st.session_state.df = demo_df
                st.session_state.source_label = "Demo orders sample"
                st.session_state.last_profile = None
                st.session_state.last_result = None
                st.session_state.ai_overview = None
                st.success("Demo orders loaded. Open Explore profile or Quality gate to continue.")
            except (OSError, pd.errors.ParserError) as exc:
                st.error(f"Could not load the bundled demo file: {exc}")
    with right:
        st.markdown("#### Connect to a REST / JSON API")
        st.caption("Fetch a JSON records endpoint. The bundled presets are public demo services.")
        api_presets = {
            "Open-Meteo geocoding (public demo)": ("https://geocoding-api.open-meteo.com/v1/search", {"name": "Pune", "count": 10, "language": "en", "format": "json"}, "results"),
            "JSONPlaceholder posts (public demo)": ("https://jsonplaceholder.typicode.com/posts", {}, ""),
            "Custom API endpoint": ("", {}, ""),
        }
        preset = st.selectbox("API preset", list(api_presets.keys()))
        default_url, default_params, default_path = api_presets[preset]
        api_url = st.text_input("GET endpoint URL", value=default_url, placeholder="https://api.example.com/v1/data")
        api_params_text = st.text_area("Query parameters (JSON object)", value=json.dumps(default_params, indent=2), height=105)
        records_path = st.text_input("Records path in JSON response", value=default_path, help="Use dotted path for nested JSON, e.g. data.items. Leave blank when the response itself is a list.")
        timeout = st.slider("Request timeout (seconds)", 3, 60, 15)
        max_records = st.number_input("Maximum records to load", min_value=1, max_value=10000, value=1000, step=100)
        with st.expander("Advanced request settings (optional)"):
            headers_text = st.text_area("HTTP headers (JSON object)", value="{}", height=80, help="Avoid pasting long-lived secrets into shared screenshots or source code.")
        if st.button("Fetch API data", type="primary", width="stretch"):
            try:
                params = json.loads(api_params_text or "{}")
                headers = json.loads(headers_text or "{}")
                if not isinstance(params, dict) or not isinstance(headers, dict):
                    raise ValueError("Query parameters and headers must both be JSON objects.")
                source_cfg: dict[str, Any] = {"url": api_url.strip(), "params": params, "timeout": timeout, "max_response_bytes": 5_000_000, "max_pages": 1, "pagination": {"type": "none"}}
                if records_path.strip():
                    source_cfg["records_path"] = records_path.strip()
                if headers:
                    source_cfg["headers"] = headers
                connector = RestApiSource(source_cfg)
                errors = connector.validate()
                if errors:
                    raise ValueError("; ".join(errors))
                with st.spinner("Requesting API and normalizing records…"):
                    dataset = connector.read(limit=int(max_records))
                st.session_state.df = dataset.frame.reset_index(drop=True)
                st.session_state.source_label = f"REST API · {api_url.strip()}"
                st.session_state.last_profile = None
                st.session_state.last_result = None
                st.session_state.ai_overview = None
                st.success(f"Fetched {len(dataset.frame):,} records from the API.")
                if dataset.warnings:
                    for warning in dataset.warnings:
                        st.warning(warning)
            except Exception as exc:
                st.error(f"API ingestion failed: {exc}")
    st.divider()
    st.subheader("Loaded dataset")
    if st.session_state.df is None:
        st.info("No data loaded yet. Use the demo sample for a quick presentation, or upload your own file.")
    else:
        df = st.session_state.df
        m1, m2, m3 = st.columns(3)
        m1.metric("Rows", f"{len(df):,}")
        m2.metric("Columns", f"{len(df.columns):,}")
        m3.metric("Source", "CSV" if not st.session_state.source_label.startswith("REST API") else "REST API")
        st.caption(f"Source: {st.session_state.source_label}")
        st.dataframe(df.head(100), width="stretch", height=320)
        with st.expander("Column names and inferred pandas types"):
            st.dataframe(pd.DataFrame({"column": df.columns, "pandas_type": [str(df[c].dtype) for c in df.columns], "nulls": [int(df[c].isna().sum()) for c in df.columns], "unique_values": [safe_nunique(df[c]) for c in df.columns]}), width="stretch")

with profile_tab:
    st.subheader("Understand the shape of your data")
    if st.session_state.df is None:
        st.info("Load a dataset first. For a quick walkthrough, use the demo orders sample in Load data.")
    else:
        df = st.session_state.df
        st.caption("Get evidence about types, missing values, uniqueness, distributions, and possible schema issues. Suggestions are never business rules by themselves.")
        profile_controls = st.columns([1.3, 1, 1])
        dataset_name = profile_controls[0].text_input("Dataset name", value="uploaded_dataset", key="profile_dataset_name")
        sample_size = profile_controls[1].number_input("Rows to profile (0 = all)", min_value=0, max_value=max(0, len(df)), value=min(len(df), 10000), step=max(1, min(1000, max(1, len(df)))), key="sample_size")
        profile_controls[2].markdown("<br>", unsafe_allow_html=True)
        if profile_controls[2].button("Build profile", type="primary", width="stretch"):
            try:
                cfg = {"profiling": {"sample_size": int(sample_size) if sample_size else None}}
                st.session_state.last_profile = AutoFlow(cfg).profile(df, dataset_name or "dataset")
                st.session_state.ai_overview = None
                st.success("Profile ready.")
            except Exception as exc:
                st.error(f"Profiling failed: {exc}")
        profile = st.session_state.last_profile
        if profile:
            with st.expander("Optional AI overview · Gemini API"):
                st.warning(
                    "This sends column names and aggregate profile statistics to Google Gemini. "
                    "Row samples and category/example values are excluded. Gemini's free tier "
                    "may use submitted content to improve Google products; review Google's terms "
                    "and do not send sensitive metadata unless your policies allow it. Free-tier "
                    "availability and quotas can change."
                )
                model_id = st.text_input(
                    "Gemini model ID",
                    value=DEFAULT_MODEL,
                    help="Default: gemini-3.8-flash. Check Google AI Studio for current model availability.",
                )
                gemini_api_key = st.text_input(
                    "Gemini API key",
                    value=os.environ.get("GEMINI_API_KEY", ""),
                    type="password",
                    help="Create a key in Google AI Studio, or set GEMINI_API_KEY in the environment.",
                )
                consent = st.checkbox(
                    "I agree to send the sanitized profile metadata to Google Gemini for analysis."
                )
                if st.button(
                    "Generate AI overview",
                    type="primary",
                    disabled=not consent or not gemini_api_key.strip(),
                ):
                    try:
                        with st.spinner("Requesting hosted model analysis…"):
                            st.session_state.ai_overview = generate_overview(
                                profile, gemini_api_key, model_id
                            )
                    except (ValueError, RuntimeError) as exc:
                        st.error(f"AI overview failed: {exc}")
                if st.session_state.ai_overview:
                    st.markdown(st.session_state.ai_overview)
            a, b, c, d = st.columns(4)
            a.metric("Rows profiled", f"{profile.get('rows_profiled', 0):,}")
            b.metric("Columns", profile.get("columns_count", 0))
            c.metric("Duplicate rows", profile.get("duplicate_rows", 0))
            d.metric("Candidate keys", len(profile.get("candidate_keys", [])))
            st.caption(profile.get("note", ""))
            col_rows = []
            for name, info in profile.get("columns", {}).items():
                stats = info.get("numeric_stats", {})
                col_rows.append({"Column": name, "Inferred type": info.get("inferred_type"), "Confidence": info.get("confidence_label"), "Nulls": info.get("null_count"), "Null %": round(info.get("null_pct", 0) * 100, 2), "Unique": info.get("unique_count"), "Role": info.get("possible_role"), "Outliers": info.get("potential_outliers", 0), "Min": stats.get("min"), "Mean": stats.get("mean"), "Max": stats.get("max"), "Needs confirmation": info.get("needs_confirmation")})
            st.dataframe(pd.DataFrame(col_rows), width="stretch")
            with st.expander("Inspect detailed profile per column"):
                selected_col = st.selectbox("Column", list(profile.get("columns", {}).keys()))
                st.json(profile["columns"][selected_col])
            with st.expander("Suggested schema rules (proposal only)"):
                st.warning("Inferred schema is a suggestion, not business truth. Review and approve it before using as production rules.")
                st.json(schema_proposal(profile))
                st.download_button("Download schema proposal (JSON)", json.dumps(schema_proposal(profile), indent=2), file_name="autoflow_schema_proposal.json", mime="application/json")

with validate_tab:
    st.subheader("Apply the rules your pipeline depends on")
    if st.session_state.df is None:
        st.info("Load a dataset first. For a quick walkthrough, use the demo orders sample in Load data.")
    else:
        df = st.session_state.df
        all_cols = list(df.columns)
        st.caption("Explicit rules can block bad rows. Generic checks add cautious warnings; they do not replace your business definitions.")
        with st.form("validation_rules_form"):
            st.markdown("Configure rules that represent your data contract. Unticked columns have no explicit column rule.")
            required_text = st.text_input("Required columns (comma-separated; must exist)", value="", placeholder="customer_id, order_date, amount", help="You can name columns that are missing from the current data to detect upstream schema changes.")
            nonnull_cols = st.multiselect("Columns that must not be null", all_cols, default=[])
            unique_cols = st.multiselect("Columns that must be unique", all_cols, default=[])
            numeric_candidates = [
                c for c in all_cols
                if pd.api.types.is_numeric_dtype(df[c])
                or (
                    df[c].notna().any()
                    and pd.to_numeric(df[c].astype(str).str.strip(), errors="coerce").notna().sum()
                    >= max(1, int(df[c].notna().sum() * 0.5))
                )
            ]
            range_options = ["(none)"] + numeric_candidates
            demo_range_index = (
                1 + numeric_candidates.index("amount")
                if st.session_state.source_label == "Demo orders sample" and "amount" in numeric_candidates
                else 0
            )
            range_col = st.selectbox("Optional numeric range rule", range_options, index=demo_range_index, help="Includes partly numeric text columns so a declared number rule can flag malformed values such as 'abc'.")
            rmin, rmax = st.columns(2)
            min_value = rmin.number_input("Minimum allowed value", value=0.0, format="%.6f")
            max_value = rmax.number_input("Maximum allowed value", value=100.0, format="%.6f")
            default_quarantine_pct = 50 if st.session_state.source_label == "Demo orders sample" else 25
            max_quarantine_pct = st.slider("Maximum quarantined row percentage before gate blocks", 0, 100, default_quarantine_pct)
            generic_checks = st.checkbox("Include AutoFlow's generic statistical checks", value=True, help="Adds conservative warnings for suspicious patterns; these are not a substitute for domain rules.")
            submitted = st.form_submit_button("Run validation", type="primary", width="stretch")
        if submitted:
            try:
                rule_columns: dict[str, Any] = {}
                for col in nonnull_cols:
                    rule_columns.setdefault(col, {})["nullable"] = False
                for col in unique_cols:
                    rule_columns.setdefault(col, {})["unique"] = True
                if range_col != "(none)":
                    if min_value > max_value:
                        st.error("Minimum must be less than or equal to maximum.")
                        st.stop()
                    rule_columns.setdefault(range_col, {}).update({"type": "integer" if pd.api.types.is_integer_dtype(df[range_col]) else "number", "min": float(min_value), "max": float(max_value)})
                required_cols = [c.strip() for c in required_text.split(",") if c.strip()]
                rules = {"dataset": st.session_state.source_label or "dataset", "columns": rule_columns, "allow_extra_columns": True}
                # Mark explicitly named required columns, including names absent from the current dataset.
                for col in required_cols:
                    rules["columns"].setdefault(col, {})["required"] = True
                cfg = {"validation": {"use_generic_rules": bool(generic_checks), "fail_on_missing_required_columns": True, "fail_on_schema_drift": False, "allow_partial_success": True, "max_quarantine_pct": float(max_quarantine_pct), "warnings_block": False}, "profiling": {"sample_size": 10000}, "recovery": {"enabled": False}, "quarantine": {"enabled": True}, "audit": {"enabled": False}, "output": {"enabled": False}}
                result = AutoFlow(cfg, rules=RuleSet.from_dict(rules)).validate(df, st.session_state.source_label or "dataset", mode=Mode.GATE)
                st.session_state.last_result = result.to_dict()
                st.success("Validation run completed. Review status and findings below.")
            except Exception as exc:
                st.error(f"Validation failed: {exc}")
        result = st.session_state.last_result
        if result:
            st.divider()
            status = result.get("status", "UNKNOWN")
            if result.get("can_proceed"):
                st.success(f"Gate result: {status} — data can proceed under the configured rules.")
            else:
                st.warning(f"Gate result: {status} — inspect the issues before using this data downstream.")
            a, b, c, d, e = st.columns(5)
            a.metric("Processed", result.get("rows_processed", 0))
            b.metric("Approved", result.get("rows_approved", 0))
            c.metric("Quarantined", result.get("rows_quarantined", 0))
            d.metric("Issues", result.get("issues_detected", len(result.get("issues", []))))
            e.metric("Recovery proposals", result.get("recovery_proposals", 0))
            issues = result.get("issues", [])
            if issues:
                st.markdown("#### Findings")
                issue_df = pd.DataFrame([{ "Severity": x.get("severity"), "Kind": x.get("kind"), "Code": x.get("code"), "Column": x.get("column") or "—", "Count": x.get("count", 0), "Finding": x.get("message"), "Recommendation": x.get("recommendation") or "—"} for x in issues])
                st.dataframe(issue_df, width="stretch", hide_index=True)
                with st.expander("Full issue details (JSON)"):
                    st.json(issues)
            else:
                st.success("No issues were reported by the configured checks.")
            quarantine = getattr(result, "quarantined_data", None)
            if quarantine is not None and not quarantine.empty:
                st.markdown("#### Quarantined rows")
                st.dataframe(quarantine, width="stretch")
            elif result.get("quarantine_summary"):
                st.markdown("#### Quarantine summary")
                st.dataframe(pd.DataFrame(result["quarantine_summary"]), width="stretch")
            if result.get("proposals"):
                with st.expander("Recovery proposals — not automatically applied"):
                    st.dataframe(pd.DataFrame(result["proposals"]), width="stretch")
                    st.info("This UI does not automatically apply recovery proposals or write to a destination.")

with export_tab:
    st.subheader("Take the review results with you")
    st.caption("Export the loaded data, profile, and validation report for further review or a downstream workflow.")
    if st.session_state.df is None:
        st.info("Load data first to enable exports.")
    else:
        df = st.session_state.df
        e1, e2 = st.columns(2)
        with e1:
            st.markdown("#### Dataset")
            st.download_button("Download loaded data as CSV", df.to_csv(index=False).encode("utf-8"), file_name="autoflow_loaded_data.csv", mime="text/csv", width="stretch")
            st.download_button("Download loaded data as JSON", df.to_json(orient="records", indent=2, force_ascii=False).encode("utf-8"), file_name="autoflow_loaded_data.json", mime="application/json", width="stretch")
        with e2:
            st.markdown("#### Run artifacts")
            if st.session_state.last_profile:
                st.download_button("Download profile JSON", json.dumps(st.session_state.last_profile, indent=2, default=str), file_name="autoflow_profile.json", mime="application/json", width="stretch")
            if st.session_state.last_result:
                st.download_button("Download validation report JSON", json.dumps(st.session_state.last_result, indent=2, default=str), file_name="autoflow_validation_report.json", mime="application/json", width="stretch")
            else:
                st.caption("Run validation to enable the report export.")
        st.divider()
        st.markdown("#### Pipeline overview")
        st.code("Source (CSV / REST API)\n        ↓\nNormalize to DataFrame\n        ↓\nProfile schema + statistics\n        ↓\nValidate explicit rules + generic checks\n        ↓\nReview issues / quarantine signals\n        ↓\nExport data and report", language="text")
        st.caption("The dashboard does not commit to a database or production destination. This is a review-first demo UI.")

st.divider()
st.caption("AutoFlow · Data quality review before downstream use · Human review remains part of the gate.")
