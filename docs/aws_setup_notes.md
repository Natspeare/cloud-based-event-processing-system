# AWS Setup Notes — event-platform
## Free-tier budget guide  |  ~$0/month for dev workloads

---

## 0 · Prerequisites

```bash
pip install awscli boto3
aws configure          # enter Access Key, Secret, region (e.g. us-east-1)
```

---

## 1 · Kinesis Data Stream

```bash
aws kinesis create-stream \
    --stream-name event-stream \
    --shard-count 1          # 1 shard = 1 MB/s ingest, free tier eligible
```

Verify:
```bash
aws kinesis describe-stream-summary --stream-name event-stream
```

Cost note: 1 shard = $0.015/hr → ~$11/month. Use `PUT_RECORDS` sparingly during dev.
For pure free-tier use, keep the local Python queue fallback (`LOCAL_MODE=true`).

---

## 2 · DynamoDB Table

```bash
aws dynamodb create-table \
    --table-name transactions \
    --attribute-definitions \
        AttributeName=user_id,AttributeType=S \
        AttributeName=timestamp,AttributeType=S \
    --key-schema \
        AttributeName=user_id,KeyType=HASH \
        AttributeName=timestamp,KeyType=RANGE \
    --billing-mode PAY_PER_REQUEST   # on-demand — no cost if idle
```

Free tier: 25 GB storage + 200M requests/month always free.

---

## 3 · S3 Bucket

```bash
BUCKET=my-events-$(aws sts get-caller-identity --query Account --output text)

aws s3api create-bucket \
    --bucket $BUCKET \
    --region us-east-1               # omit CreateBucketConfiguration for us-east-1

aws s3api put-public-access-block \
    --bucket $BUCKET \
    --public-access-block-configuration \
        BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true
```

---

## 4 · Lambda Function

### 4a — Package the code

```bash
cd lambda
pip install boto3 -t package/
cp processor.py package/
cd package && zip -r ../function.zip . && cd ..
```

### 4b — Create IAM role for Lambda

```bash
aws iam create-role \
    --role-name lambda-event-processor \
    --assume-role-policy-document '{
        "Version":"2012-10-17",
        "Statement":[{
            "Effect":"Allow",
            "Principal":{"Service":"lambda.amazonaws.com"},
            "Action":"sts:AssumeRole"
        }]
    }'

# Attach managed policies
aws iam attach-role-policy --role-name lambda-event-processor \
    --policy-arn arn:aws:iam::aws:policy/service-role/AWSLambdaKinesisExecutionRole

aws iam attach-role-policy --role-name lambda-event-processor \
    --policy-arn arn:aws:iam::aws:policy/AmazonDynamoDBFullAccess

aws iam attach-role-policy --role-name lambda-event-processor \
    --policy-arn arn:aws:iam::aws:policy/AmazonS3FullAccess
```

### 4c — Deploy Lambda

```bash
ACCOUNT=$(aws sts get-caller-identity --query Account --output text)
REGION=us-east-1

aws lambda create-function \
    --function-name event-processor \
    --runtime python3.12 \
    --handler processor.handler \
    --role arn:aws:iam::${ACCOUNT}:role/lambda-event-processor \
    --zip-file fileb://function.zip \
    --environment "Variables={
        DYNAMODB_TABLE=transactions,
        S3_BUCKET=${BUCKET},
        AWS_REGION=${REGION}
    }" \
    --timeout 60 \
    --memory-size 256
```

### 4d — Add Kinesis trigger

```bash
STREAM_ARN=$(aws kinesis describe-stream \
    --stream-name event-stream \
    --query StreamDescription.StreamARN --output text)

aws lambda create-event-source-mapping \
    --event-source-arn $STREAM_ARN \
    --function-name event-processor \
    --starting-position LATEST \
    --batch-size 100 \
    --bisect-batch-on-function-error   # re-tries bad batches by splitting
```

---

## 5 · Deploy FastAPI Backend

Option A — Local (recommended for dev):
```bash
pip install fastapi uvicorn boto3 pydantic requests
LOCAL_MODE=false \
AWS_REGION=us-east-1 \
KINESIS_STREAM_NAME=event-stream \
uvicorn backend.main:app --host 0.0.0.0 --port 8000
```

Option B — AWS App Runner / EC2 (production):
- Package as a Docker image, push to ECR, deploy on App Runner (free tier eligible for small workloads).

---

## 6 · Athena Setup

```bash
# Create Athena workgroup (optional, uses default otherwise)
aws athena create-work-group \
    --name event-analytics \
    --configuration "ResultConfiguration={OutputLocation=s3://${BUCKET}/athena-results/}"

# Run table creation DDL from analytics/queries.sql in Athena console
# or via AWS CLI:
aws athena start-query-execution \
    --query-string file://analytics/queries.sql \
    --work-group event-analytics
```

---

## 7 · Teardown (avoid surprise charges)

```bash
aws kinesis delete-stream --stream-name event-stream
aws dynamodb delete-table --table-name transactions
aws s3 rm s3://$BUCKET --recursive
aws s3api delete-bucket --bucket $BUCKET
aws lambda delete-function --function-name event-processor
```

---

## Cost Summary (dev usage)

| Service    | Free Tier           | Est. Cost (dev)    |
|------------|---------------------|--------------------|
| Kinesis    | None (per shard-hr) | ~$11/mo (1 shard)  |
| DynamoDB   | 25 GB + 200M req    | $0                 |
| Lambda     | 1M invocations/mo   | $0                 |
| S3         | 5 GB storage        | ~$0.02/GB          |
| Athena     | $5 per TB scanned   | ~$0 for small data |

**Recommendation**: develop in `LOCAL_MODE=true`, switch to AWS only for integration testing.
