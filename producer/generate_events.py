"""
Event Producer — generates synthetic transaction events and sends them
to the ingestion API (or a local mock queue in fallback mode).

Usage:
    # Local mode (default)
    python generate_events.py --rate 200 --duration 30

    # AWS mode (targets real FastAPI endpoint)
    INGEST_URL=http://<host>/ingest python generate_events.py --rate 500
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import queue
import random
import time
import uuid
from datetime import datetime, timezone
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ──────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='{"time": "%(asctime)s", "level": "%(levelname)s", "msg": "%(message)s"}',
)
log = logging.getLogger("producer")

# ──────────────────────────────────────────────
# Constants / config
# ──────────────────────────────────────────────
INGEST_URL: str = os.getenv("INGEST_URL", "http://localhost:8000/ingest")
BATCH_SIZE: int = int(os.getenv("BATCH_SIZE", "50"))
LOCAL_MODE: bool = os.getenv("LOCAL_MODE", "true").lower() == "true"

EVENT_TYPES: list[str] = ["purchase", "refund", "transfer", "subscription", "withdrawal"]
USER_POOL: list[int] = list(range(1, 501))          # 500 synthetic users

# ──────────────────────────────────────────────
# Local fallback queue (replaces Kinesis in local mode)
# ──────────────────────────────────────────────
LOCAL_QUEUE: queue.Queue[dict[str, Any]] = queue.Queue(maxsize=100_000)


# ──────────────────────────────────────────────
# HTTP session with retry
# ──────────────────────────────────────────────
def build_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=0.5,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["POST"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


SESSION = build_session()


# ──────────────────────────────────────────────
# Event generation
# ──────────────────────────────────────────────
def make_event() -> dict[str, Any]:
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": random.choice(USER_POOL),
        "amount": round(random.uniform(0.5, 500.0), 2),
        "event_type": random.choice(EVENT_TYPES),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def make_batch(size: int) -> list[dict[str, Any]]:
    return [make_event() for _ in range(size)]


# ──────────────────────────────────────────────
# Sending
# ──────────────────────────────────────────────
def send_batch_local(batch: list[dict[str, Any]]) -> None:
    """Push events into the local in-process queue."""
    for event in batch:
        try:
            LOCAL_QUEUE.put_nowait(event)
        except queue.Full:
            log.warning("Local queue full — dropping event %s", event["event_id"])
    log.info("Queued %d events locally (queue size=%d)", len(batch), LOCAL_QUEUE.qsize())


def send_batch_api(batch: list[dict[str, Any]]) -> None:
    """POST a batch to the FastAPI ingestion endpoint."""
    try:
        resp = SESSION.post(INGEST_URL, json={"events": batch}, timeout=10)
        resp.raise_for_status()
        body = resp.json()
        log.info(
            "Sent %d events — accepted=%d rejected=%d",
            len(batch),
            body.get("accepted", "?"),
            body.get("rejected", "?"),
        )
    except requests.exceptions.RequestException as exc:
        log.error("Failed to send batch: %s", exc)


def send_batch(batch: list[dict[str, Any]]) -> None:
    if LOCAL_MODE:
        send_batch_local(batch)
    else:
        send_batch_api(batch)


# ──────────────────────────────────────────────
# Main loop
# ──────────────────────────────────────────────
def run(rate: int, duration: int) -> None:
    """
    Produce events at `rate` events/sec for `duration` seconds.
    Events are accumulated into batches of BATCH_SIZE before sending.
    """
    log.info(
        "Producer starting | rate=%d/s duration=%ds batch=%d local_mode=%s",
        rate,
        duration,
        BATCH_SIZE,
        LOCAL_MODE,
    )

    interval: float = 1.0 / rate          # seconds between individual events
    deadline: float = time.monotonic() + duration
    buffer: list[dict[str, Any]] = []
    total_sent = 0

    while time.monotonic() < deadline:
        t0 = time.monotonic()

        buffer.append(make_event())

        if len(buffer) >= BATCH_SIZE:
            send_batch(buffer)
            total_sent += len(buffer)
            buffer = []

        elapsed = time.monotonic() - t0
        sleep_for = interval - elapsed
        if sleep_for > 0:
            time.sleep(sleep_for)

    # flush remaining
    if buffer:
        send_batch(buffer)
        total_sent += len(buffer)

    log.info("Producer finished — total events sent: %d", total_sent)


# ──────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Synthetic event producer")
    parser.add_argument("--rate", type=int, default=100, help="Events per second (100–1000)")
    parser.add_argument("--duration", type=int, default=60, help="Run duration in seconds")
    args = parser.parse_args()

    rate = max(1, min(args.rate, 1000))
    run(rate=rate, duration=args.duration)
