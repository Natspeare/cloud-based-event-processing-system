-- =============================================================
-- analytics/queries.sql
-- Athena queries for the event-platform analytics store
--
-- Prerequisite: Glue / Athena table pointing at
--   s3://<S3_BUCKET>/events/
-- with partition projection or MSCK REPAIR TABLE run.
--
-- Table DDL (run once in Athena console):
-- =============================================================

CREATE EXTERNAL TABLE IF NOT EXISTS transactions (
    event_id              STRING,
    user_id               INT,
    amount                DOUBLE,
    event_type            STRING,
    timestamp             STRING,
    processing_timestamp  STRING,
    transaction_category  STRING
)
PARTITIONED BY (year STRING, month STRING, day STRING)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
LOCATION 's3://<S3_BUCKET>/events/'
TBLPROPERTIES (
    'projection.enabled' = 'true',
    'projection.year.type'  = 'integer', 'projection.year.range'  = '2024,2030',
    'projection.month.type' = 'integer', 'projection.month.range' = '1,12',   'projection.month.digits' = '2',
    'projection.day.type'   = 'integer', 'projection.day.range'   = '1,31',   'projection.day.digits'   = '2',
    'storage.location.template' = 's3://<S3_BUCKET>/events/${year}/${month}/${day}'
);

-- =============================================================
-- Q1  Total transaction volume per day
-- =============================================================
SELECT
    year,
    month,
    day,
    COUNT(*)          AS total_transactions,
    SUM(amount)       AS total_volume,
    AVG(amount)       AS avg_amount
FROM transactions
GROUP BY year, month, day
ORDER BY year DESC, month DESC, day DESC;


-- =============================================================
-- Q2  Average transaction amount per user
-- =============================================================
SELECT
    user_id,
    COUNT(*)          AS transaction_count,
    ROUND(AVG(amount), 2) AS avg_amount,
    ROUND(SUM(amount), 2) AS total_spent
FROM transactions
GROUP BY user_id
ORDER BY avg_amount DESC;


-- =============================================================
-- Q3  Top 10 users by total spending
-- =============================================================
SELECT
    user_id,
    COUNT(*)              AS transaction_count,
    ROUND(SUM(amount), 2) AS total_spent
FROM transactions
GROUP BY user_id
ORDER BY total_spent DESC
LIMIT 10;


-- =============================================================
-- Q4  Hourly transaction frequency  (last 7 days)
-- =============================================================
SELECT
    DATE_TRUNC('hour', CAST(timestamp AS TIMESTAMP)) AS hour_bucket,
    COUNT(*)                                          AS transaction_count,
    ROUND(SUM(amount), 2)                             AS hourly_volume
FROM transactions
WHERE
    CAST(timestamp AS TIMESTAMP) >= NOW() - INTERVAL '7' DAY
GROUP BY 1
ORDER BY 1 DESC;


-- =============================================================
-- Q5  Transaction category breakdown
-- =============================================================
SELECT
    transaction_category,
    COUNT(*)              AS count,
    ROUND(SUM(amount), 2) AS total_amount,
    ROUND(AVG(amount), 2) AS avg_amount
FROM transactions
GROUP BY transaction_category
ORDER BY total_amount DESC;


-- =============================================================
-- Q6  Event type breakdown
-- =============================================================
SELECT
    event_type,
    COUNT(*)              AS count,
    ROUND(SUM(amount), 2) AS total_amount
FROM transactions
GROUP BY event_type
ORDER BY count DESC;


-- =============================================================
-- Q7  Power BI / dashboard-ready: daily summary per user
-- (export to S3 as a view or CTAS table for Power BI DirectQuery)
-- =============================================================
CREATE TABLE analytics.daily_user_summary
WITH (
    format = 'PARQUET',
    parquet_compression = 'SNAPPY',
    external_location = 's3://<S3_BUCKET>/analytics/daily_user_summary/'
) AS
SELECT
    user_id,
    year,
    month,
    day,
    COUNT(*)                  AS transactions,
    ROUND(SUM(amount), 2)     AS total_spent,
    ROUND(AVG(amount), 2)     AS avg_amount,
    MAX(amount)               AS max_amount,
    MIN(amount)               AS min_amount
FROM transactions
GROUP BY user_id, year, month, day;
