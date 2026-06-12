"""Tests for sink idempotent write behavior."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

from leadtech_fx.models import FxRateRow
from leadtech_fx.sinks.mock_databricks_sink import MockDatabricksSink


def test_sink_idempotency_no_duplicates_on_repeated_partition_writes(tmp_path: Path) -> None:
    """Second write for same partition replaces prior file contents."""
    sink = MockDatabricksSink(root_path=tmp_path / "outputs" / "mock_delta")
    first_rows = [
        FxRateRow(
            requested_date=date(2024, 1, 1),
            rate_date=date(2024, 1, 1),
            base_currency="USD",
            quote_currency="EUR",
            rate=0.91,
            source="openexchangerates",
            ingested_at=datetime(2024, 1, 2, tzinfo=UTC),
        ),
    ]
    second_rows = [
        FxRateRow(
            requested_date=date(2024, 1, 1),
            rate_date=date(2024, 1, 1),
            base_currency="USD",
            quote_currency="EUR",
            rate=0.92,
            source="openexchangerates",
            ingested_at=datetime(2024, 1, 2, tzinfo=UTC),
        ),
    ]

    first_written = sink.write_idempotent(rows=first_rows, target_table="analytics.fx_rates")
    second_written = sink.write_idempotent(rows=second_rows, target_table="analytics.fx_rates")

    assert first_written == 1
    assert second_written == 1

    data_path = (
        tmp_path
        / "outputs"
        / "mock_delta"
        / "analytics"
        / "fx_rates"
        / "requested_date=2024-01-01"
        / "data.jsonl"
    )
    payloads = [json.loads(line) for line in data_path.read_text(encoding="utf-8").splitlines()]

    assert len(payloads) == 1
    assert payloads[0]["quote_currency"] == "EUR"
    assert payloads[0]["rate"] == 0.92
