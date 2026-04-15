"""
main.py — FastAPI ingestion service.

Endpoints:
    POST /ingest   — validate and forward a batch of transaction events
    GET  /health   — liveness probe

Run locally:
    uvicorn backend.main:app --reload --port 8000

Environment variables:
    LOCAL_MODE          true|false  (default: true)
    AWS_REGION          e.g. us-east-1
    KINESIS_STREAM_NAME e.g. event-stream
    DYNAMODB_TABLE      e.g. transactions
    S3_BUCKET           e.g. my-events-bucket
"""

from __future__ import annotations

import json
import logging
import os
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, AsyncGenerator

import boto3
import botocore.exceptions
from fastapi import FastAPI, Request, status
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from backend.schemas import (
    EventError,
    IngestRequest,
    IngestResponse,
    TransactionEvent,
)

# ──────────────────────────────────────────────
# Structured JSON logger
# ──────────────────────────────────────────────
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("ingestion")


def _log(level: str, msg: str, **extra: Any) -> None:
    record = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "level": level,
        "service": "ingestion-api",
        "msg": msg,
        **extra,
    }
    getattr(log, level.lower())(json.dumps(record))


# ──────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────
LOCAL_MODE: bool = os.getenv("LOCAL_MODE", "true").lower() == "true"
AWS_REGION: str = os.getenv("AWS_REGION", "us-east-1")
KINESIS_STREAM: str = os.getenv("KINESIS_STREAM_NAME", "event-stream")
DYNAMODB_TABLE: str = os.getenv("DYNAMODB_TABLE", "transactions")
S3_BUCKET: str = os.getenv("S3_BUCKET", "my-events-bucket")

# ──────────────────────────────────────────────
# Local fallback store  (replaces Kinesis / DynamoDB / S3)
# ──────────────────────────────────────────────
LOCAL_EVENT_STORE: list[dict[str, Any]] = []

# ──────────────────────────────────────────────
# AWS clients (lazy-init only in AWS mode)
# ──────────────────────────────────────────────
_kinesis_client: Any = None


def get_kinesis() -> Any:
    global _kinesis_client
    if _kinesis_client is None:
        _kinesis_client = boto3.client("kinesis", region_name=AWS_REGION)
    return _kinesis_client


# ──────────────────────────────────────────────
# Downstream forwarding
# ──────────────────────────────────────────────
def forward_to_local(events: list[TransactionEvent]) -> None:
    """Append enriched events to the in-process list (local fallback)."""
    for evt in events:
        enriched = evt.model_dump(mode="json")
        enriched["processing_timestamp"] = datetime.now(timezone.utc).isoformat()
        amount = enriched["amount"]
        enriched["transaction_category"] = (
            "low" if amount < 20 else "medium" if amount <= 100 else "high"
        )
        LOCAL_EVENT_STORE.append(enriched)

    _log("info", "Forwarded to local store", count=len(events), store_size=len(LOCAL_EVENT_STORE))


def forward_to_kinesis(events: list[TransactionEvent]) -> None:
    """Put records onto Kinesis — each event is its own record."""
    client = get_kinesis()
    records = [
        {
            "Data": json.dumps(evt.model_dump(mode="json")),
            "PartitionKey": str(evt.user_id),
        }
        for evt in events
    ]

    # Kinesis put_records accepts up to 500 records per call
    for i in range(0, len(records), 500):
        chunk = records[i : i + 500]
        try:
            resp = client.put_records(StreamName=KINESIS_STREAM, Records=chunk)
            failed = resp.get("FailedRecordCount", 0)
            if failed:
                _log("warning", "Kinesis partial failure", failed=failed, attempted=len(chunk))
            else:
                _log("info", "Kinesis put_records OK", count=len(chunk))
        except botocore.exceptions.ClientError as exc:
            _log("error", "Kinesis put_records error", error=str(exc))
            raise


def forward_events(events: list[TransactionEvent]) -> None:
    if LOCAL_MODE:
        forward_to_local(events)
    else:
        forward_to_kinesis(events)


# ──────────────────────────────────────────────
# App
# ──────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    mode = "LOCAL" if LOCAL_MODE else "AWS"
    _log("info", f"Ingestion service starting in {mode} mode")
    yield
    _log("info", "Ingestion service shutting down")


app = FastAPI(
    title="Event Ingestion API",
    description="High-throughput transaction event ingestion service",
    version="1.0.0",
    lifespan=lifespan,
)


# ──────────────────────────────────────────────
# Middleware — request timing
# ──────────────────────────────────────────────
@app.middleware("http")
async def log_requests(request: Request, call_next: Any) -> Any:
    t0 = time.perf_counter()
    response = await call_next(request)
    duration_ms = round((time.perf_counter() - t0) * 1000, 2)
    _log(
        "info",
        "Request",
        method=request.method,
        path=request.url.path,
        status=response.status_code,
        duration_ms=duration_ms,
    )
    return response


# ──────────────────────────────────────────────
# Routes
# ──────────────────────────────────────────────
@app.get("/health", tags=["ops"])
async def health() -> dict[str, str]:
    """Liveness probe."""
    return {"status": "ok", "mode": "local" if LOCAL_MODE else "aws"}


@app.post(
    "/ingest",
    response_model=IngestResponse,
    status_code=status.HTTP_200_OK,
    tags=["ingestion"],
)
async def ingest(request: Request) -> IngestResponse:
    """
    Accept a batch of raw transaction events.

    - Validates each event against the TransactionEvent schema.
    - Forwards valid events downstream (Kinesis or local queue).
    - Returns counts of accepted / rejected events with error details.
    """
    request_id = str(uuid.uuid4())

    # Parse raw body so we can gracefully report per-event errors
    try:
        raw_body = await request.json()
    except Exception:
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"detail": "Invalid JSON body"},
        )

    raw_events: list[Any] = raw_body.get("events", [])
    if not isinstance(raw_events, list) or len(raw_events) == 0:
        return JSONResponse(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            content={"detail": "Payload must contain a non-empty 'events' list"},
        )

    valid: list[TransactionEvent] = []
    errors: list[EventError] = []

    for idx, raw in enumerate(raw_events):
        try:
            event = TransactionEvent.model_validate(raw)
            valid.append(event)
        except (ValidationError, Exception) as exc:
            errors.append(EventError(index=idx, raw=raw if isinstance(raw, dict) else {}, reason=str(exc)))

    if valid:
        try:
            forward_events(valid)
        except Exception as exc:
            _log("error", "Forward failed", request_id=request_id, error=str(exc))
            return JSONResponse(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                content={"detail": f"Downstream unavailable: {exc}"},
            )

    _log(
        "info",
        "Batch processed",
        request_id=request_id,
        total=len(raw_events),
        accepted=len(valid),
        rejected=len(errors),
    )

    return IngestResponse(accepted=len(valid), rejected=len(errors), errors=errors)


# ──────────────────────────────────────────────
# Debug endpoint (local mode only)
# ──────────────────────────────────────────────
@app.get("/events", include_in_schema=False)
async def list_events(limit: int = 50) -> dict[str, Any]:
    """Returns the last `limit` locally-stored events (dev / local mode only)."""
    if not LOCAL_MODE:
        return JSONResponse(status_code=403, content={"detail": "Only available in local mode"})
    return {
        "total": len(LOCAL_EVENT_STORE),
        "events": LOCAL_EVENT_STORE[-limit:],
    }
