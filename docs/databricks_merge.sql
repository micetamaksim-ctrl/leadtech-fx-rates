-- Production follow-up SQL sketch for Delta merge.
-- Catalog/schema/table names are environment-specific and should be configured
-- per workspace and deployment environment.

MERGE INTO analytics.fx_rates AS target
USING analytics_staging.fx_rates_staging AS staging
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
