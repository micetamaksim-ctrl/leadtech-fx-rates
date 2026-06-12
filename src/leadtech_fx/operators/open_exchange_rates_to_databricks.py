"""Custom operator to load OXR historical FX rates into Databricks sink."""

from __future__ import annotations

from datetime import UTC
from typing import Any

from airflow.sdk import BaseOperator, Variable  # pyright: ignore[reportMissingImports]

from leadtech_fx.clients.open_exchange_rates import OpenExchangeRatesClient
from leadtech_fx.date_utils import iter_inclusive_dates, parse_date, utc_now
from leadtech_fx.sinks.mock_databricks_sink import MockDatabricksSink
from leadtech_fx.transforms import transform_oxr_payload_to_rows


class OpenExchangeRatesToDatabricksOperator(BaseOperator):
    """Fetch OXR historical rates, transform, and write idempotently."""

    template_fields: tuple[str, ...] = ("rate_start_date", "rate_end_date", "target_table")

    def __init__(
        self,
        *,
        rate_start_date: str,
        rate_end_date: str,
        app_id: str | None = None,
        databricks_conn_id: str,
        target_table: str,
        max_requests_per_run: int = 30,
        throttle_seconds: float = 0.2,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.rate_start_date = rate_start_date
        self.rate_end_date = rate_end_date
        self.app_id = app_id
        if not databricks_conn_id or not databricks_conn_id.strip():
            raise ValueError("databricks_conn_id must be a non-empty string.")
        self.databricks_conn_id = databricks_conn_id
        self.target_table = target_table
        self.max_requests_per_run = max_requests_per_run
        self.throttle_seconds = throttle_seconds

    def execute(self, context: dict[str, Any]) -> dict[str, Any]:
        """Run the extract-transform-load flow for requested date range."""
        resolved_app_id = self._resolve_app_id()
        start_date = parse_date(self.rate_start_date)
        end_date = parse_date(self.rate_end_date)
        requested_dates = iter_inclusive_dates(start_date=start_date, end_date=end_date)

        if len(requested_dates) > self.max_requests_per_run:
            raise ValueError(
                "Requested date range would issue "
                f"{len(requested_dates)} requests, exceeding max_requests_per_run="
                f"{self.max_requests_per_run}."
            )

        self.log.info(
            "Running FX load for range=%s..%s target_table=%s requests=%s",
            start_date.isoformat(),
            end_date.isoformat(),
            self.target_table,
            len(requested_dates),
        )

        client = OpenExchangeRatesClient(
            app_id=resolved_app_id,
            throttle_seconds=self.throttle_seconds,
            max_requests_per_run=self.max_requests_per_run,
        )
        sink = MockDatabricksSink()
        ingested_at = utc_now().astimezone(UTC)
        payloads_by_date = client.fetch_historical_range(
            start_date=start_date,
            end_date=end_date,
        )

        all_rows = []
        for requested_date in requested_dates:
            payload = payloads_by_date[requested_date]
            rows = transform_oxr_payload_to_rows(
                payload=payload,
                requested_date=requested_date,
                ingested_at=ingested_at,
            )
            all_rows.extend(rows)

        rows_written = sink.write_idempotent(rows=all_rows, target_table=self.target_table)
        summary = {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "requests_made": len(requested_dates),
            "rows_transformed": len(all_rows),
            "rows_written": rows_written,
            "target_table": self.target_table,
            "run_at_utc": utc_now().isoformat(),
        }

        self.log.info(
            "Completed FX load target_table=%s requests=%s rows_transformed=%s rows_written=%s",
            self.target_table,
            summary["requests_made"],
            summary["rows_transformed"],
            summary["rows_written"],
        )
        return summary

    def _resolve_app_id(self) -> str:
        """Resolve app id from explicit parameter, then Airflow Variable."""
        if self.app_id:
            return self.app_id

        app_id = Variable.get("open_exchange_rates_app_id", default=None)
        if app_id:
            return app_id

        raise ValueError(
            "Open Exchange Rates app id is missing. Provide app_id explicitly or set "
            "Airflow Variable 'open_exchange_rates_app_id'."
        )
