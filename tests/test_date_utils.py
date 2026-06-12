"""Tests for UTC date utility helpers."""

from __future__ import annotations

from datetime import UTC, date, datetime

import pytest

from leadtech_fx import date_utils


def test_iter_inclusive_dates_returns_all_dates() -> None:
    """Inclusive iteration includes both start and end dates."""
    result = date_utils.iter_inclusive_dates(date(2024, 1, 1), date(2024, 1, 3))
    assert result == [date(2024, 1, 1), date(2024, 1, 2), date(2024, 1, 3)]


def test_iter_inclusive_dates_raises_for_invalid_range() -> None:
    """Invalid date ranges are rejected clearly."""
    with pytest.raises(ValueError, match="must be <="):
        date_utils.iter_inclusive_dates(date(2024, 1, 3), date(2024, 1, 1))


def test_parse_date_raises_for_future_date(monkeypatch: pytest.MonkeyPatch) -> None:
    """Future dates are rejected using UTC assumptions."""
    monkeypatch.setattr(
        date_utils,
        "utc_now",
        lambda: datetime(2024, 1, 2, 0, 0, 0, tzinfo=UTC),
    )
    with pytest.raises(ValueError, match="future"):
        date_utils.parse_date("2024-01-03")
