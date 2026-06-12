"""Open Exchange Rates HTTP client abstractions."""

from __future__ import annotations

import logging
import time
from datetime import UTC, date, datetime
from typing import Any

import requests

from leadtech_fx.date_utils import iter_inclusive_dates

logger = logging.getLogger(__name__)


class OpenExchangeRatesClient:
    """Client for Open Exchange Rates historical endpoint."""

    BASE_URL = "https://openexchangerates.org/api/historical/{requested_date}.json"

    def __init__(
        self,
        app_id: str,
        session: requests.Session | None = None,
        timeout_seconds: float = 10.0,
        max_retries: int = 3,
        backoff_seconds: float = 0.5,
        throttle_seconds: float = 0.1,
        max_requests_per_run: int | None = None,
    ) -> None:
        if not app_id:
            raise ValueError("app_id is required and cannot be empty.")
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be > 0.")
        if max_retries < 0:
            raise ValueError("max_retries must be >= 0.")
        if backoff_seconds < 0:
            raise ValueError("backoff_seconds must be >= 0.")
        if throttle_seconds < 0:
            raise ValueError("throttle_seconds must be >= 0.")
        if max_requests_per_run is not None and max_requests_per_run <= 0:
            raise ValueError("max_requests_per_run must be > 0 when provided.")

        self._app_id = app_id
        self._session = session or requests.Session()
        self._timeout_seconds = timeout_seconds
        self._max_retries = max_retries
        self._backoff_seconds = backoff_seconds
        self._throttle_seconds = throttle_seconds
        self._max_requests_per_run = max_requests_per_run

    def fetch_historical_range(
        self, start_date: date, end_date: date
    ) -> dict[date, dict[str, Any]]:
        """Fetch payloads for each day in an inclusive range."""
        payloads: dict[date, dict[str, Any]] = {}
        requested_dates = iter_inclusive_dates(start_date=start_date, end_date=end_date)
        if (
            self._max_requests_per_run is not None
            and len(requested_dates) > self._max_requests_per_run
        ):
            raise ValueError(
                "Requested date range would issue "
                f"{len(requested_dates)} requests, exceeding max_requests_per_run="
                f"{self._max_requests_per_run}."
            )

        for index, requested_date in enumerate(requested_dates):
            payloads[requested_date] = self.fetch_historical(requested_date=requested_date)
            # Light throttle to avoid bursty traffic across many dates.
            if index < len(requested_dates) - 1 and self._throttle_seconds > 0:
                time.sleep(self._throttle_seconds)
        return payloads

    def fetch_historical(self, requested_date: date) -> dict[str, Any]:
        """Fetch and validate one historical payload."""
        today_utc = datetime.now(tz=UTC).date()
        if requested_date > today_utc:
            raise ValueError(
                f"requested_date {requested_date.isoformat()} is in the future (UTC)."
            )

        url = self.BASE_URL.format(requested_date=requested_date.isoformat())
        params = {"app_id": self._app_id}

        for attempt in range(self._max_retries + 1):
            logger.info(
                "Requesting Open Exchange Rates historical data for %s (attempt %s/%s).",
                requested_date.isoformat(),
                attempt + 1,
                self._max_retries + 1,
            )
            try:
                response = self._session.get(
                    url=url, params=params, timeout=self._timeout_seconds
                )
            except requests.RequestException as exc:
                if attempt < self._max_retries:
                    sleep_seconds = self._backoff_seconds * (2**attempt)
                    logger.warning(
                        "Retryable request exception for date=%s: %s. Backing off %.2fs.",
                        requested_date.isoformat(),
                        exc.__class__.__name__,
                        sleep_seconds,
                    )
                    if sleep_seconds > 0:
                        time.sleep(sleep_seconds)
                    continue
                raise RuntimeError(
                    "Open Exchange Rates request error for "
                    f"{requested_date.isoformat()} after retries: {exc}"
                ) from exc

            if response.status_code == 200:
                try:
                    payload: dict[str, Any] = response.json()
                except ValueError as exc:
                    raise ValueError(
                        "Open Exchange Rates returned non-JSON response for "
                        f"{requested_date.isoformat()}."
                    ) from exc
                self._validate_payload(payload=payload, requested_date=requested_date)
                return payload

            should_retry = response.status_code == 429 or 500 <= response.status_code <= 599
            if should_retry and attempt < self._max_retries:
                sleep_seconds = self._backoff_seconds * (2**attempt)
                logger.warning(
                    "Retryable response status=%s for date=%s. Backing off %.2fs.",
                    response.status_code,
                    requested_date.isoformat(),
                    sleep_seconds,
                )
                if sleep_seconds > 0:
                    time.sleep(sleep_seconds)
                continue

            # Do not retry normal 4xx errors (except 429 handled above).
            response_excerpt = response.text[:200].replace("\n", " ")
            raise ValueError(
                "Open Exchange Rates request failed for "
                f"{requested_date.isoformat()} with status={response.status_code}: "
                f"{response_excerpt}"
            )

        raise RuntimeError(
            f"Exceeded retries while requesting Open Exchange Rates for {requested_date.isoformat()}."
        )

    @staticmethod
    def _validate_payload(payload: dict[str, Any], requested_date: date) -> None:
        """Defensively validate required fields from OXR response."""
        missing_keys = [key for key in ("base", "timestamp", "rates") if key not in payload]
        if missing_keys:
            raise ValueError(
                "Open Exchange Rates payload missing required keys "
                f"{missing_keys} for requested_date={requested_date.isoformat()}."
            )

        if not isinstance(payload["base"], str) or not payload["base"]:
            raise ValueError("Open Exchange Rates payload field 'base' must be a non-empty string.")

        if isinstance(payload["timestamp"], bool) or not isinstance(payload["timestamp"], int):
            raise ValueError("Open Exchange Rates payload field 'timestamp' must be an integer.")

        if not isinstance(payload["rates"], dict):
            raise ValueError("Open Exchange Rates payload field 'rates' must be an object.")
