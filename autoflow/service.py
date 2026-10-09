"""Optional HTTP interface for AutoFlow.

The service deliberately accepts records, not filesystem paths, Python import paths,
connection strings, or arbitrary connector configuration. Deploy behind authentication
and TLS when exposed outside a trusted network.
"""

from typing import Any, Dict, List, Optional
import json

import pandas as pd

from .contracts import AutoFlowError, ConfigError, Mode
from .engine import AutoFlow
from .registry import registry


def create_app(*, max_records: int = 10_000, max_columns: int = 500,
               max_payload_bytes: int = 5_000_000, api_title: str = "AutoFlow API"):
    """Create the optional FastAPI app. Install ``autoflow[api]`` to use this function."""
    try:
        from fastapi import FastAPI, HTTPException
        from pydantic import BaseModel, Field
    except ImportError as exc:  # pragma: no cover - exercised in minimal installs
        raise ImportError("HTTP service requires optional dependencies; install autoflow[api]") from exc

    if max_records < 1 or max_columns < 1 or max_payload_bytes < 1:
        raise ValueError("request limits must be positive")

    class RecordsRequest(BaseModel):
        dataset_name: str = Field(default="api_dataset", min_length=1, max_length=128)
        records: List[Dict[str, Any]]
        rules: Optional[Dict[str, Any]] = None

    app = FastAPI(title=api_title, version="0.1.0")

    @app.get("/health")
    def health():
        return {"status": "ok", "service": "autoflow"}

    @app.get("/v1/connectors")
    def connectors():
        return {"connectors": registry.describe()}

    def frame_from_request(req: RecordsRequest) -> pd.DataFrame:
        model_data = req.model_dump() if hasattr(req, "model_dump") else req.dict()
        payload_size = len(json.dumps(model_data, ensure_ascii=False, default=str).encode("utf-8"))
        if payload_size > max_payload_bytes:
            raise HTTPException(status_code=413, detail=f"Request body exceeds {max_payload_bytes} bytes")
        if len(req.records) > max_records:
            raise HTTPException(status_code=413, detail=f"Too many records; limit is {max_records}")
        columns = set()
        for record in req.records:
            columns.update(record.keys())
            if len(columns) > max_columns:
                raise HTTPException(status_code=413, detail=f"Too many columns; limit is {max_columns}")
        return pd.DataFrame(req.records, dtype=object)

    @app.post("/v1/profile")
    def profile(req: RecordsRequest):
        frame = frame_from_request(req)
        try:
            result = AutoFlow().profile(frame, req.dataset_name)
            return result
        except (AutoFlowError, ValueError, TypeError) as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    @app.post("/v1/validate")
    def validate(req: RecordsRequest):
        frame = frame_from_request(req)
        try:
            af = AutoFlow(rules=req.rules)
            result = af.validate(frame, req.dataset_name, mode=Mode.GATE)
            payload = result.to_dict(include_profile=True)
            payload["approved_records"] = (
                result.approved_data.reset_index(drop=True).to_dict(orient="records")
                if result.approved_data is not None else None
            )
            payload["quarantined_records"] = (
                result.quarantined_data.reset_index(drop=True).to_dict(orient="records")
                if result.quarantined_data is not None else None
            )
            return payload
        except (AutoFlowError, ValueError, TypeError) as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc

    return app
