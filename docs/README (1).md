# event-platform

**A production-structured real-time event ingestion, streaming, and analytics pipeline**  
Suitable for SDE internship portfolios · Mirrors Amazon-scale distributed systems architecture

---

## Architecture Overview

```
┌─────────────────┐     HTTP POST /ingest      ┌───────────────────┐
│  Event Producer │ ──────────────────────────► │  FastAPI Backend  │
│  (Python)       │     batched JSON            │  (Pydantic valid) │
└─────────────────┘                             └─────────┬─────────┘
                                                          │
                                          LOCAL_MODE=true │ LOCAL_MODE=false
                                                          │
                              ┌───────────────────────────┤
                              │                           │
                    ┌─────────▼──────────┐    ┌──────────▼──────────┐
                    │  Python in-proc    │    │  Amazon Kinesis      │
                    │  Queue (fallback)  │    │  Data Streams        │
                    └─────────┬──────────┘    └──────────┬──────────┘
                              │                          │ triggers
                              │                ┌─────────▼──────────┐
                              │                │  AWS Lambda         │
                              │                │  processor.py       │
                              │                │  · enrich events    │
                              │                │  · categorise txn   │
                              │                └────┬──────────┬─────┘
                              │                     │          │
                    ┌─────────▼─────────┐  ┌────────▼──┐  ┌───▼──────────┐
                    │  Local dict +     │  │ DynamoDB  │  │ S3 (NDJSON)  │
                    │  filesystem       │  │ real-time │  │ analytics    │
                    └───────────────────┘  └───────────┘  └──────┬───────┘
                                                                  │
                                                         ┌────────▼────────┐
                                                         │ AWS Athena SQL  │
                                                         │ Power BI / viz  │
                                                         └─────────────────┘
```

---

## Project Structure

```
event-platform/
├── producer/
│   └── generate_events.py      # Synthetic event generator (configurable rate)
├── backend/
│   ├── main.py                 # FastAPI ingestion service
│   └── schemas.py              # Pydantic event models
├── lambda/
│   └── processor.py            # AWS Lambda enrichment + storage
├── infra/
│   └── aws_setup_notes.md      # Step-by-step AWS provisioning guide
├── analytics/
│   └── queries.sql             # Athena SQL analytics queries
└── README.md
```

---

## Quick Start — Local Mode

### 1 · Install dependencies

```bash
pip install fastapi uvicorn pydantic boto3 requests urllib3
```

### 2 · Start the ingestion API

```bash
# From the repo root
LOCAL_MODE=true uvicorn backend.main:app --reload --port 8000
```

### 3 · Run the event producer

```bash
# 200 events/sec for 30 seconds
python producer/generate_events.py --rate 200 --duration 30
```

### 4 · Check ingested events (dev endpoint)

```bash
curl http://localhost:8000/events?limit=10 | python -m json.tool
curl http://localhost:8000/health
```

---

## Quick Start — AWS Mode

See **`infra/aws_setup_notes.md`** for full step-by-step provisioning.

```bash
# After provisioning Kinesis, DynamoDB, S3, Lambda:

LOCAL_MODE=false \
AWS_REGION=us-east-1 \
KINESIS_STREAM_NAME=event-stream \
DYNAMODB_TABLE=transactions \
S3_BUCKET=my-events-bucket \
INGEST_URL=http://localhost:8000/ingest \
uvicorn backend.main:app --port 8000 &

python producer/generate_events.py --rate 500 --duration 60
```

---

## Environment Variables Reference

| Variable             | Default          | Description                          |
|----------------------|------------------|--------------------------------------|
| `LOCAL_MODE`         | `true`           | Use in-process queue instead of AWS  |
| `AWS_REGION`         | `us-east-1`      | AWS region                           |
| `KINESIS_STREAM_NAME`| `event-stream`   | Kinesis stream name                  |
| `DYNAMODB_TABLE`     | `transactions`   | DynamoDB table name                  |
| `S3_BUCKET`          | `my-events-bucket`| S3 bucket for analytics data        |
| `INGEST_URL`         | `http://localhost:8000/ingest` | Producer target URL    |
| `BATCH_SIZE`         | `50`             | Events per batch from producer       |

---

## Event Schema

```json
{
  "event_id":   "uuid4",
  "user_id":    123,
  "amount":     45.67,
  "event_type": "purchase | refund | transfer | subscription | withdrawal",
  "timestamp":  "2024-01-15T10:30:00Z"
}
```

Enriched (after Lambda / local processing):

```json
{
  "...all above fields...",
  "processing_timestamp":   "2024-01-15T10:30:01Z",
  "transaction_category":   "low | medium | high"
}
```

---

## Analytics Queries

See **`analytics/queries.sql`** for Athena queries covering:

- Total transaction volume per day
- Average transaction amount per user
- Top 10 users by spending
- Hourly transaction frequency
- Category and event-type breakdowns
- Power BI–ready CTAS daily summary table

---

## How This Project Mirrors Real Distributed Systems

### Scalability via decoupled services
Each component (producer, ingestion API, streaming layer, storage, analytics) is independently deployable and scalable. The FastAPI backend can run behind a load balancer with N replicas; Kinesis scales by adding shards; Lambda scales to thousands of concurrent executions automatically. No component is a bottleneck for another.

### Streaming-based architecture
Events flow through a durable, ordered stream (Kinesis) rather than being written directly to a database. This pattern — popularised by LinkedIn's Kafka and Amazon's internal Kinesis usage — decouples write throughput from read/processing throughput and enables multiple downstream consumers without touching the producer.

### Fault tolerance design
- **Retry logic** in the producer (urllib3 `Retry` with exponential backoff)
- **Idempotent Lambda processing**: DynamoDB `put_item` is safe to replay; duplicate `event_id` simply overwrites with the same data
- **Kinesis bisect-on-error**: bad batches are split in half and retried, preventing a single malformed record from blocking the shard
- **Graceful validation**: malformed events are rejected with structured error details rather than failing the entire batch

### Tradeoffs vs monolithic systems
| Concern        | Monolith               | This architecture             |
|----------------|------------------------|-------------------------------|
| Latency        | Lower (in-process)     | Higher (network hops)         |
| Scalability    | Vertical only          | Horizontal per component      |
| Fault isolation| Full process crash     | Component-level failure       |
| Observability  | Single log stream      | Per-service structured logs   |
| Deployment     | All-or-nothing         | Independent CI/CD per service |

The dual-write to DynamoDB (low-latency lookups) and S3/Athena (high-throughput analytics) follows the **Lambda Architecture** pattern used in Amazon's own order and inventory systems — hot path for real-time queries, cold path for batch analytics.

---

## Extending to Production

1. **Add authentication**: API key or AWS Cognito on `/ingest`
2. **Metrics**: Emit CloudWatch metrics from Lambda (invocations, latency, error rate)
3. **Dead-letter queue**: Route failed Kinesis records to an SQS DLQ for manual review
4. **Schema registry**: Use AWS Glue Schema Registry to enforce event schema across producers
5. **Power BI**: Connect via Athena ODBC driver using the `daily_user_summary` CTAS table
6. **CI/CD**: GitHub Actions → ECR → App Runner for the FastAPI service; SAM/CDK for Lambda

---

*Built as an Amazon-level internship portfolio project · Python 3.10+ · FastAPI · AWS Kinesis / Lambda / DynamoDB / S3 / Athena*
