"""Transformations from Open Exchange Rates payloads to tabular records."""

from __future__ import annotations

import math
from datetime import UTC, date, datetime
from typing import Any

from leadtech_fx.models import FxRateRow


def transform_oxr_payload_to_rows(
    payload: dict[str, Any], requested_date: date, ingested_at: datetime
) -> list[FxRateRow]:
    """Flatten Open Exchange Rates payload into one row per quote currency."""
    if "timestamp" not in payload:
        raise ValueError("Payload missing required key: 'timestamp'.")
    if "base" not in payload:
        raise ValueError("Payload missing required key: 'base'.")
    if "rates" not in payload:
        raise ValueError("Payload missing required key: 'rates'.")

    base_currency = payload["base"]
    if not isinstance(base_currency, str) or not base_currency.strip():
        raise ValueError("Payload field 'base' must be a non-empty string.")
    base_currency_normalized = base_currency.upper()

    timestamp = payload["timestamp"]
    if isinstance(timestamp, bool) or not isinstance(timestamp, int):
        raise ValueError("Payload field 'timestamp' must be an integer.")

    rates = payload["rates"]
    if not isinstance(rates, dict):
        raise ValueError("Payload field 'rates' must be an object.")

    if ingested_at.tzinfo is None:
        ingested_at_utc = ingested_at.replace(tzinfo=UTC)
    else:
        ingested_at_utc = ingested_at.astimezone(UTC)

    rate_date = datetime.fromtimestamp(timestamp, tz=UTC).date()
    rows: list[FxRateRow] = []

    for quote_currency, raw_rate in sorted(rates.items(), key=lambda item: str(item[0])):
        if not isinstance(quote_currency, str) or not quote_currency.strip():
            raise ValueError(
                "Quote currency must be a non-empty string, "
                f"got {quote_currency!r}."
            )
        quote_currency_normalized = quote_currency.upper()

        try:
            rate = float(raw_rate)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "Invalid rate value for quote currency "
                f"'{quote_currency}': {raw_rate!r}."
            ) from exc

        if not math.isfinite(rate):
            raise ValueError(
                f"Rate value for quote currency '{quote_currency}' must be finite."
            )

        rows.append(
            FxRateRow(
                requested_date=requested_date,
                rate_date=rate_date,
                base_currency=base_currency_normalized,
                quote_currency=quote_currency_normalized,
                rate=rate,
                source="openexchangerates",
                ingested_at=ingested_at_utc,
            )
        )

    return rows
