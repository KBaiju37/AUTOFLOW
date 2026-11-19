"""Optional Gemini analysis of sanitized dataset profiles."""
from __future__ import annotations

import json
from typing import Any, Dict
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen


GEMINI_API_URL = "https://generativelanguage.googleapis.com/v1beta/models"
DEFAULT_MODEL = "gemini-3.8-flash"
MAX_PROFILE_BYTES = 25_000


class GeminiAnalysisError(RuntimeError):
    """Raised when a Gemini profile analysis request fails."""


def sanitize_profile(profile: Dict[str, Any]) -> Dict[str, Any]:
    """Keep only column names and aggregate profile statistics; omit values/examples."""
    column_fields = (
        "inferred_type", "confidence", "confidence_label", "null_count", "null_pct",
        "empty_string_count", "unique_count", "uniqueness_ratio", "is_constant",
        "whitespace_anomalies", "numeric_stats", "potential_outliers", "string_length",
        "date_order", "possible_role", "needs_confirmation", "invalid_for_inferred_type",
    )
    columns = []
    for name, details in profile.get("columns", {}).items():
        column = {"name": str(name)}
        column.update({field: details[field] for field in column_fields if field in details})
        columns.append(column)
    return {
        "rows_loaded": profile.get("rows_loaded"),
        "rows_profiled": profile.get("rows_profiled"),
        "columns_count": profile.get("columns_count"),
        "exact": profile.get("exact"),
        "sampled": profile.get("sampled"),
        "duplicate_rows": profile.get("duplicate_rows"),
        "candidate_key_count": len(profile.get("candidate_keys", [])),
        "columns": columns,
    }


def generate_overview(profile: Dict[str, Any], api_key: str, model: str, timeout: int = 60) -> str:
    """Request a concise interpretation using the Gemini Developer API."""
    if not api_key.strip():
        raise ValueError("Enter a Gemini API key.")
    if not model.strip():
        raise ValueError("Enter a Gemini model ID.")

    sanitized = sanitize_profile(profile)
    profile_json = json.dumps(sanitized, ensure_ascii=False, default=str)
    if len(profile_json.encode("utf-8")) > MAX_PROFILE_BYTES:
        raise ValueError(
            f"The sanitized profile exceeds {MAX_PROFILE_BYTES:,} bytes. "
            "Reduce the number of columns before requesting an AI overview."
        )

    system_instruction = (
        "You are a careful data quality analyst. Interpret only the supplied "
        "aggregate profile. Separate measured facts from hypotheses, avoid "
        "claiming business meaning, and give a concise overview, notable risks, "
        "and a short list of follow-up checks. The profile contains no row samples."
    )
    request_body = {
        "systemInstruction": {"parts": [{"text": system_instruction}]},
        "contents": [{"role": "user", "parts": [{"text": f"Review this dataset profile:\n{profile_json}"}]}],
        "generationConfig": {"temperature": 0.2, "maxOutputTokens": 900},
    }
    url = f"{GEMINI_API_URL}/{quote(model.strip(), safe='-._')}:generateContent"
    request = Request(
        url,
        data=json.dumps(request_body).encode("utf-8"),
        headers={
            "x-goog-api-key": api_key.strip(),
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urlopen(request, timeout=timeout) as response:
            result = json.loads(response.read().decode("utf-8"))
    except HTTPError as exc:
        try:
            detail = exc.read().decode("utf-8", errors="replace")[:500]
        finally:
            exc.close()
        raise GeminiAnalysisError(
            f"Gemini API returned HTTP {exc.code}: {detail or exc.reason}"
        ) from exc
    except URLError as exc:
        raise GeminiAnalysisError(f"Could not reach the Gemini API: {exc.reason}") from exc
    except (TimeoutError, json.JSONDecodeError) as exc:
        raise GeminiAnalysisError(f"Gemini API response failed: {exc}") from exc

    try:
        parts = result["candidates"][0]["content"]["parts"]
    except (KeyError, IndexError, TypeError) as exc:
        raise GeminiAnalysisError("Gemini returned an unexpected generateContent response.") from exc
    content = "\n".join(part["text"] for part in parts if isinstance(part.get("text"), str))
    if not content.strip():
        raise GeminiAnalysisError("Gemini returned an empty or non-text overview.")
    return content.strip()
