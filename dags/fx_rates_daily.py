"""Daily DAG for loading FX rates from Open Exchange Rates to Databricks sink."""

from __future__ import annotations

import pendulum

from airflow.sdk import DAG  # pyright: ignore[reportMissingImports]

from leadtech_fx.operators.open_exchange_rates_to_databricks import (
    OpenExchangeRatesToDatabricksOperator,
)


with DAG(
    dag_id="fx_rates_daily",
    description="Daily OXR historical FX ingestion into mock Databricks sink.",
    schedule="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=True,
    max_active_runs=1,
    tags=["fx", "openexchangerates", "daily"],
) as dag:
    fx_rates_daily = OpenExchangeRatesToDatabricksOperator(
        task_id="open_exchange_rates_to_databricks_daily",
        rate_start_date="{{ ds }}",
        rate_end_date="{{ ds }}",
        databricks_conn_id="databricks_default",
        target_table="analytics.fx_rates_daily",
        max_requests_per_run=30,
        throttle_seconds=0.2,
    )

    # Example historical backfill task configuration (disabled by default):
    # fx_rates_backfill = OpenExchangeRatesToDatabricksOperator(
    #     task_id="open_exchange_rates_to_databricks_backfill",
    #     rate_start_date="2024-01-01",
    #     rate_end_date="2024-01-31",
    #     databricks_conn_id="databricks_default",
    #     target_table="analytics.fx_rates_daily",
    #     max_requests_per_run=31,
    #     throttle_seconds=0.2,
    # )
