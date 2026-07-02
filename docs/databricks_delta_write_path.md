# Databricks Delta Write Path (Production Follow-Up)

This take-home intentionally uses a mocked Databricks sink to keep the submission
reproducible, dependency-light, and aligned with the assessment scope. The prompt
explicitly allowed a mocked Databricks sink.

In a production deployment, `MockDatabricksSink` would be replaced by a real
Databricks/Delta write path while preserving the same logical contract: idempotent
upserts for FX rates and safe reprocessing.

Illustrative target naming in these follow-up docs uses
`analytics.fx_rates_daily`; exact catalog/schema/table names are environment-specific.

## Recommended Production Pattern

1. **Write transformed rows to a staging Delta table**
   - Batch-level landing area for the current run.
2. **Validate schema and required columns**
   - Verify presence/types of:
     - `requested_date`
     - `rate_date`
     - `base_currency`
     - `quote_currency`
     - `rate`
     - `source`
     - `ingested_at`
3. **Merge staging into target Delta table**
   - Use a natural key for FX rates:
     - `rate_date`
     - `base_currency`
     - `quote_currency`
4. **Clean up or overwrite staging**
   - Keep staging reusable for subsequent runs.

## MERGE vs Partition Overwrite

- **Use `MERGE` when corrections/reprocessing are possible**
  - Safer if existing rates can be updated for previously loaded dates.
  - Supports idempotent upsert semantics row-by-row.
- **Use partition overwrite for immutable full-snapshot partitions**
  - Simpler when each run fully regenerates an entire date partition.
  - Typical when source guarantees snapshot immutability for that partition.

## Partitioning and Layout

- Partition target data by `rate_date` or `requested_date` (workload dependent).
- Consider clustering or Z-ordering by currency dimensions (`base_currency`,
  `quote_currency`) for common query patterns.

## Idempotency and Schema Enforcement

- Enforce schema at write-time to prevent silent drift.
- Keep writes idempotent so reruns do not duplicate facts.
- Ensure each run can be retried safely without manual cleanup.
