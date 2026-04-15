"""
schemas.py — Pydantic models for request / response validation.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import List
from uuid import UUID

from pydantic import BaseModel, Field, field_validator


class EventType(str, Enum):
    purchase = "purchase"
    refund = "refund"
    transfer = "transfer"
    subscription = "subscription"
    withdrawal = "withdrawal"


class TransactionEvent(BaseModel):
    """A single transaction event from a producer."""

    event_id: UUID = Field(..., description="Unique event identifier (UUIDv4)")
    user_id: int = Field(..., ge=1, description="Positive integer user identifier")
    amount: float = Field(..., gt=0, description="Transaction amount — must be positive")
    event_type: EventType = Field(..., description="Type of transaction")
    timestamp: datetime = Field(..., description="ISO-8601 event timestamp")

    @field_validator("amount")
    @classmethod
    def amount_precision(cls, v: float) -> float:
        """Round to 2 decimal places to avoid floating-point drift."""
        return round(v, 2)

    model_config = {"json_schema_extra": {"example": {
        "event_id": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
        "user_id": 42,
        "amount": 75.50,
        "event_type": "purchase",
        "timestamp": "2024-01-15T10:30:00Z",
    }}}


class IngestRequest(BaseModel):
    """Batch ingestion payload."""
    events: List[TransactionEvent] = Field(..., min_length=1, max_length=1000)


class EventError(BaseModel):
    """Details of a single rejected event."""
    index: int
    raw: dict
    reason: str


class IngestResponse(BaseModel):
    """Summary of a batch ingestion call."""
    accepted: int
    rejected: int
    errors: List[EventError] = []
