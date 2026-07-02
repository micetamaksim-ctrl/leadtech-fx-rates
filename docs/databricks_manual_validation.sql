-- Manual validation SQL for production Delta write pattern.
-- This is a follow-up validation script, not the current Airflow runtime code.
-- Catalog/schema/table names are example values and may vary by environment.

-- 1) Check uploaded rows.
SELECT COUNT(*) AS uploaded_rows
FROM workspace.default.fx_rates_pipeline_sample;

-- 2) Create staging table.
CREATE OR REPLACE TABLE workspace.default.fx_rates_staging (
  requested_date DATE,
  rate_date DATE,
  base_currency STRING,
  quote_currency STRING,
  rate DOUBLE,
  source STRING,
  ingested_at TIMESTAMP
) USING DELTA;

-- 3) Load staging from uploaded sample.
INSERT OVERWRITE workspace.default.fx_rates_staging
SELECT
  CAST(requested_date AS DATE) AS requested_date,
  CAST(rate_date AS DATE) AS rate_date,
  CAST(base_currency AS STRING) AS base_currency,
  CAST(quote_currency AS STRING) AS quote_currency,
  CAST(rate AS DOUBLE) AS rate,
  CAST(source AS STRING) AS source,
  CAST(ingested_at AS TIMESTAMP) AS ingested_at
FROM workspace.default.fx_rates_pipeline_sample;

-- 4) Create target table.
CREATE OR REPLACE TABLE workspace.default.fx_rates_target (
  requested_date DATE,
  rate_date DATE,
  base_currency STRING,
  quote_currency STRING,
  rate DOUBLE,
  source STRING,
  ingested_at TIMESTAMP
) USING DELTA;

-- 5) Insert EUR update test row (same natural key as staging row, wrong rate).
INSERT INTO workspace.default.fx_rates_target
VALUES (DATE '2024-01-01', DATE '2024-01-01', 'USD', 'EUR', -1.0, 'seed', current_timestamp());

-- 6) Insert ZZZ target-only test row (not present in staging).
INSERT INTO workspace.default.fx_rates_target
VALUES (DATE '2024-01-01', DATE '2024-01-01', 'USD', 'ZZZ', 999.0, 'seed', current_timestamp());

-- 7) Run MERGE from staging into target.
MERGE INTO workspace.default.fx_rates_target AS target
USING workspace.default.fx_rates_staging AS staging
ON  target.rate_date = staging.rate_date
AND target.base_currency = staging.base_currency
AND target.quote_currency = staging.quote_currency
WHEN MATCHED THEN UPDATE SET
  target.requested_date = staging.requested_date,
  target.rate = staging.rate,
  target.source = staging.source,
  target.ingested_at = staging.ingested_at
WHEN NOT MATCHED THEN INSERT (
  requested_date,
  rate_date,
  base_currency,
  quote_currency,
  rate,
  source,
  ingested_at
)
VALUES (
  staging.requested_date,
  staging.rate_date,
  staging.base_currency,
  staging.quote_currency,
  staging.rate,
  staging.source,
  staging.ingested_at
);

-- 8) Validate row count after first MERGE (expected 508).
SELECT COUNT(*) AS target_row_count_after_first_merge
FROM workspace.default.fx_rates_target;

-- 9) Validate EUR/ZZZ behavior.
SELECT
  rate_date,
  base_currency,
  quote_currency,
  rate,
  source
FROM workspace.default.fx_rates_target
WHERE rate_date = DATE '2024-01-01'
  AND base_currency = 'USD'
  AND quote_currency IN ('EUR', 'ZZZ')
ORDER BY quote_currency;

-- 10) Run MERGE a second time to validate idempotency.
MERGE INTO workspace.default.fx_rates_target AS target
USING workspace.default.fx_rates_staging AS staging
ON  target.rate_date = staging.rate_date
AND target.base_currency = staging.base_currency
AND target.quote_currency = staging.quote_currency
WHEN MATCHED THEN UPDATE SET
  target.requested_date = staging.requested_date,
  target.rate = staging.rate,
  target.source = staging.source,
  target.ingested_at = staging.ingested_at
WHEN NOT MATCHED THEN INSERT (
  requested_date,
  rate_date,
  base_currency,
  quote_currency,
  rate,
  source,
  ingested_at
)
VALUES (
  staging.requested_date,
  staging.rate_date,
  staging.base_currency,
  staging.quote_currency,
  staging.rate,
  staging.source,
  staging.ingested_at
);

-- 11) Validate row count after second MERGE (expected 508, no duplicates).
SELECT COUNT(*) AS target_row_count_after_second_merge
FROM workspace.default.fx_rates_target;

-- 12) Review Delta operation metrics.
DESCRIBE HISTORY workspace.default.fx_rates_target;
