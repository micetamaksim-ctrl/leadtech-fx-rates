"""Data models for FX rate ingestion and transformed records."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime


@dataclass(frozen=True, slots=True)
class FxRateRow:
    """A single analytics-friendly FX rate observation."""

    requested_date: date
    rate_date: date
    base_currency: str
    quote_currency: str
    rate: float
    source: str
    ingested_at: datetime
