"""UTC date helper utilities for inclusive date ranges."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta


def utc_now() -> datetime:
    """Return a timezone-aware UTC timestamp."""
    return datetime.now(tz=UTC)


def parse_date(value: str) -> date:
    """Parse YYYY-MM-DD and reject future dates."""
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(
            f"Invalid date '{value}'. Expected ISO format YYYY-MM-DD."
        ) from exc

    today_utc = utc_now().date()
    if parsed > today_utc:
        raise ValueError(
            f"Date '{value}' is in the future. Future dates are not allowed (UTC)."
        )
    return parsed


def iter_inclusive_dates(start_date: date, end_date: date) -> list[date]:
    """Return an inclusive list of UTC dates from start_date to end_date."""
    if start_date > end_date:
        raise ValueError(
            f"start_date ({start_date.isoformat()}) must be <= "
            f"end_date ({end_date.isoformat()})."
        )

    today_utc = utc_now().date()
    if start_date > today_utc or end_date > today_utc:
        raise ValueError(
            "Date range contains future dates. Future dates are not allowed (UTC)."
        )

    total_days = (end_date - start_date).days
    return [start_date + timedelta(days=day) for day in range(total_days + 1)]
