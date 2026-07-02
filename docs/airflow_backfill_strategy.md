# Airflow Backfill Strategy (Production Follow-Up)

The current daily DAG is already backfill-friendly: each logical date run uses
`{{ ds }}` and processes one date at a time.

The operator also supports multi-day ranges, which is useful for controlled
historical loads. For larger historical backfills, a production setup should
prefer one task instance per date via dynamic task mapping.

## Why One Task Per Date for Large Backfills

- **Isolated retries**: retry only failed dates, not the entire range.
- **Clearer observability**: per-date success/failure is easy to inspect.
- **Better parallelism**: scheduler can distribute dates across workers.
- **Smaller blast radius**: one bad date does not rerun all dates.

## Production Follow-Up Pseudo-Code

The following is illustrative pseudo-code for a follow-up production DAG pattern
and is not the current executed DAG implementation:

```python
# Pseudo-code only (production follow-up idea)
daily_or_backfill_dates = build_date_list(...)

OpenExchangeRatesToDatabricksOperator.partial(
    task_id="open_exchange_rates_to_databricks_mapped",
    databricks_conn_id="databricks_default",
    target_table="analytics.fx_rates_daily",
    max_requests_per_run=1,
    throttle_seconds=0.2,
).expand(
    rate_start_date=daily_or_backfill_dates,
    rate_end_date=daily_or_backfill_dates,
)
```

This keeps each mapped task focused on a single logical date while preserving
the same operator semantics and idempotent sink behavior.
