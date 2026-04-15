"""
processor.py — AWS Lambda function triggered by Kinesis.

Responsibilities:
  1. Decode and parse Kinesis records
  2. Enrich each event with transaction_category + processing_timestamp
  3. Write to DynamoDB (real-time lookup store)
  4. Write to S3 (analytics / Athena store) as NDJSON, partitioned by date

Environment variables (set in Lambda configuration):
    AWS_REGION          e.g. us-east-1
    DYNAMODB_TABLE      e.g. transactions
    S3_BUCKET           e.g. my-events-bucket
"""

from __future__ import annotations

import base64
import json
import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Any

import boto3
import botocore.exceptions

# ──────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────
log = logging.getLogger()
log.setLevel(logging.INFO)


def _log(level: str, msg: str, **extra: Any) -> None:
    record = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "level": level,
        "service": "lambda-processor",
        "msg": msg,
        **extra,
    }
    getattr(log, level.lower())(json.dumps(record))


# ──────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────
AWS_REGION: str = os.environ.get("AWS_REGION", "us-east-1")
DYNAMODB_TABLE: str = os.environ["DYNAMODB_TABLE"]
S3_BUCKET: str = os.environ["S3_BUCKET"]

# ──────────────────────────────────────────────
# AWS clients  (module-level = reused across warm invocations)
# ──────────────────────────────────────────────
dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
table = dynamodb.Table(DYNAMODB_TABLE)

s3 = boto3.client("s3", region_name=AWS_REGION)


# ──────────────────────────────────────────────
# Enrichment
# ──────────────────────────────────────────────
def categorise(amount: float) -> str:
    if amount < 20:
        return "low"
    elif amount <= 100:
        return "medium"
    return "high"


def enrich(event: dict[str, Any]) -> dict[str, Any]:
    enriched = dict(event)
    enriched["processing_timestamp"] = datetime.now(timezone.utc).isoformat()
    enriched["transaction_category"] = categorise(float(enriched.get("amount", 0)))
    return enriched


# ──────────────────────────────────────────────
# Storage helpers
# ──────────────────────────────────────────────
def write_to_dynamodb(event: dict[str, Any]) -> None:
    """
    Upsert an enriched event into DynamoDB.
    Partition key : user_id  (string)
    Sort key      : timestamp (ISO-8601 string)
    Idempotent: same event_id written twice is safe (last-write wins).
    """
    item = {
        "user_id": str(event["user_id"]),
        "timestamp": event["timestamp"],
        "event_id": event["event_id"],
        "amount": str(event["amount"]),          # DynamoDB stores Decimal; use string for portability
        "event_type": event["event_type"],
        "transaction_category": event["transaction_category"],
        "processing_timestamp": event["processing_timestamp"],
    }
    try:
        table.put_item(Item=item)
    except botocore.exceptions.ClientError as exc:
        _log("error", "DynamoDB write failed", event_id=event.get("event_id"), error=str(exc))
        raise


def write_to_s3(events: list[dict[str, Any]], partition_date: str) -> None:
    """
    Write a batch of events as NDJSON to S3.
    Key pattern: events/YYYY/MM/DD/<uuid>.ndjson
    This layout is optimal for Athena partition pruning.
    """
    if not events:
        return

    year, month, day = partition_date.split("-")
    key = f"events/{year}/{month}/{day}/{uuid.uuid4()}.ndjson"
    body = "\n".join(json.dumps(e) for e in events)

    try:
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=key,
            Body=body.encode("utf-8"),
            ContentType="application/x-ndjson",
        )
        _log("info", "S3 write OK", key=key, count=len(events))
    except botocore.exceptions.ClientError as exc:
        _log("error", "S3 write failed", key=key, error=str(exc))
        raise


# ──────────────────────────────────────────────
# Local fallback storage (used in unit tests / local mode)
# ──────────────────────────────────────────────
LOCAL_DB: dict[str, list[dict[str, Any]]] = {}
LOCAL_FS: list[dict[str, Any]] = []


def write_to_local_db(event: dict[str, Any]) -> None:
    key = str(event["user_id"])
    LOCAL_DB.setdefault(key, []).append(event)


def write_to_local_fs(events: list[dict[str, Any]]) -> None:
    LOCAL_FS.extend(events)


# ──────────────────────────────────────────────
# Lambda handler
# ──────────────────────────────────────────────
def handler(kinesis_event: dict[str, Any], context: Any) -> dict[str, Any]:
    """
    Lambda entry point.

    kinesis_event shape:
    {
      "Records": [
        {
          "kinesis": {
            "data": "<base64-encoded JSON>",
            ...
          },
          ...
        }
      ]
    }
    """
    records = kinesis_event.get("Records", [])
    _log("info", "Lambda invoked", record_count=len(records))

    processed: list[dict[str, Any]] = []
    errors: list[str] = []
    partition_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    for record in records:
        try:
            raw_data = base64.b64decode(record["kinesis"]["data"]).decode("utf-8")
            event = json.loads(raw_data)
        except (KeyError, ValueError, json.JSONDecodeError) as exc:
            _log("warning", "Skipping malformed Kinesis record", error=str(exc))
            errors.append(str(exc))
            continue

        try:
            enriched = enrich(event)
        except Exception as exc:
            _log("warning", "Enrichment failed", event_id=event.get("event_id"), error=str(exc))
            errors.append(str(exc))
            continue

        # Write to DynamoDB (per-event, idempotent)
        try:
            write_to_dynamodb(enriched)
        except Exception:
            errors.append(f"dynamo_fail:{enriched.get('event_id')}")
            continue

        processed.append(enriched)

    # Batch-write to S3
    if processed:
        try:
            write_to_s3(processed, partition_date)
        except Exception as exc:
            _log("error", "S3 batch write failed", error=str(exc))

    _log(
        "info",
        "Lambda complete",
        processed=len(processed),
        errors=len(errors),
    )

    return {
        "statusCode": 200,
        "processed": len(processed),
        "errors": len(errors),
    }
