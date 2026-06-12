"""Tests for transformation logic."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path

import pytest

from leadtech_fx.transforms import transform_oxr_payload_to_rows


def _load_sample_payload() -> dict[str, object]:
    sample_path = (
        Path(__file__).resolve().parents[1]
        / "sample_data"
        / "oxr_historical_2024-01-01.json"
    )
    return json.loads(sample_path.read_text(encoding="utf-8"))


def test_transform_sample_payload_to_expected_rows() -> None:
    """Sample OXR JSON becomes one row per quote currency."""
    payload = _load_sample_payload()

    rows = transform_oxr_payload_to_rows(
        payload=payload,
        requested_date=date(2024, 1, 1),
        ingested_at=datetime(2024, 1, 2, tzinfo=UTC),
    )

    assert len(rows) == 3
    assert {row.base_currency for row in rows} == {"USD"}
    assert rows[0].quote_currency == "EUR"
    assert {row.quote_currency for row in rows} == {"EUR", "GBP", "JPY"}
    assert {row.requested_date for row in rows} == {date(2024, 1, 1)}


def test_transform_raises_for_empty_quote_currency() -> None:
    """Empty quote currency keys are rejected clearly."""
    payload = {
        "base": "USD",
        "timestamp": 1704067200,
        "rates": {"": 1.23},
    }

    with pytest.raises(ValueError, match="Quote currency must be a non-empty string"):
        transform_oxr_payload_to_rows(
            payload=payload,
            requested_date=date(2024, 1, 1),
            ingested_at=datetime(2024, 1, 2, tzinfo=UTC),
        )


def test_transform_raises_for_non_string_quote_currency() -> None:
    """Non-string quote currency keys are rejected clearly."""
    payload = {
        "base": "USD",
        "timestamp": 1704067200,
        "rates": {1: 1.23},
    }

    with pytest.raises(ValueError, match="Quote currency must be a non-empty string"):
        transform_oxr_payload_to_rows(
            payload=payload,
            requested_date=date(2024, 1, 1),
            ingested_at=datetime(2024, 1, 2, tzinfo=UTC),
        )
