"""Tests for OpenExchangeRatesClient behavior."""

from __future__ import annotations

from datetime import date
from typing import Any

import pytest
import requests

from leadtech_fx.clients.open_exchange_rates import OpenExchangeRatesClient


class _FakeResponse:
    """Minimal fake response object for client tests."""

    def __init__(
        self,
        status_code: int,
        payload: dict[str, Any] | None = None,
        text: str = "",
    ) -> None:
        self.status_code = status_code
        self._payload = payload or {}
        self.text = text

    def json(self) -> dict[str, Any]:
        """Return mocked JSON payload."""
        return self._payload


class _FakeSession:
    """Minimal fake requests.Session-like object."""

    def __init__(self, outcomes: list[Any]) -> None:
        self._outcomes = outcomes
        self.calls = 0

    def get(self, **_: Any) -> _FakeResponse:
        """Return next outcome or raise next exception."""
        self.calls += 1
        outcome = self._outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def test_client_retries_network_exception_then_succeeds() -> None:
    """Retryable request exceptions are retried with same retry loop."""
    payload = {
        "base": "USD",
        "timestamp": 1704067200,
        "rates": {"EUR": 0.91},
    }
    session = _FakeSession(
        outcomes=[
            requests.Timeout("network timeout"),
            _FakeResponse(status_code=200, payload=payload),
        ]
    )
    client = OpenExchangeRatesClient(
        app_id="test-app-id",
        session=session,  # type: ignore[arg-type]
        max_retries=2,
        backoff_seconds=0,
        throttle_seconds=0,
    )

    result = client.fetch_historical(requested_date=date(2024, 1, 1))

    assert result == payload
    assert session.calls == 2


def test_client_enforces_max_requests_per_run_limit() -> None:
    """Date ranges larger than configured request limit are rejected early."""
    session = _FakeSession(outcomes=[])
    client = OpenExchangeRatesClient(
        app_id="test-app-id",
        session=session,  # type: ignore[arg-type]
        max_retries=0,
        backoff_seconds=0,
        throttle_seconds=0,
        max_requests_per_run=2,
    )

    with pytest.raises(ValueError, match="max_requests_per_run"):
        client.fetch_historical_range(
            start_date=date(2024, 1, 1),
            end_date=date(2024, 1, 3),
        )

    assert session.calls == 0


def test_client_retries_http_429_then_succeeds() -> None:
    """HTTP 429 responses are retried and can recover on next attempt."""
    payload = {
        "base": "USD",
        "timestamp": 1704067200,
        "rates": {"EUR": 0.91},
    }
    session = _FakeSession(
        outcomes=[
            _FakeResponse(status_code=429, text="rate limit"),
            _FakeResponse(status_code=200, payload=payload),
        ]
    )
    client = OpenExchangeRatesClient(
        app_id="test-app-id",
        session=session,  # type: ignore[arg-type]
        max_retries=2,
        backoff_seconds=0,
        throttle_seconds=0,
    )

    result = client.fetch_historical(requested_date=date(2024, 1, 1))

    assert result == payload
    assert session.calls == 2


def test_client_retries_http_500_then_succeeds() -> None:
    """HTTP 5xx responses are retried and can recover on next attempt."""
    payload = {
        "base": "USD",
        "timestamp": 1704067200,
        "rates": {"EUR": 0.91},
    }
    session = _FakeSession(
        outcomes=[
            _FakeResponse(status_code=500, text="server error"),
            _FakeResponse(status_code=200, payload=payload),
        ]
    )
    client = OpenExchangeRatesClient(
        app_id="test-app-id",
        session=session,  # type: ignore[arg-type]
        max_retries=2,
        backoff_seconds=0,
        throttle_seconds=0,
    )

    result = client.fetch_historical(requested_date=date(2024, 1, 1))

    assert result == payload
    assert session.calls == 2


@pytest.mark.parametrize("status_code", [401, 403])
def test_client_does_not_retry_normal_4xx(status_code: int) -> None:
    """Normal 4xx responses (non-429) fail fast without retries."""
    session = _FakeSession(
        outcomes=[
            _FakeResponse(status_code=status_code, text="auth/forbidden"),
            _FakeResponse(
                status_code=200,
                payload={"base": "USD", "timestamp": 1704067200, "rates": {"EUR": 0.91}},
            ),
        ]
    )
    client = OpenExchangeRatesClient(
        app_id="test-app-id",
        session=session,  # type: ignore[arg-type]
        max_retries=2,
        backoff_seconds=0,
        throttle_seconds=0,
    )

    with pytest.raises(ValueError, match=f"status={status_code}"):
        client.fetch_historical(requested_date=date(2024, 1, 1))

    assert session.calls == 1


def test_client_raises_clear_error_when_retries_exhausted() -> None:
    """Exhausted request-exception retries raise a clear sanitized error."""
    session = _FakeSession(
        outcomes=[
            requests.Timeout("first timeout"),
            requests.Timeout("second timeout"),
            requests.Timeout("third timeout"),
        ]
    )
    client = OpenExchangeRatesClient(
        app_id="test-app-id",
        session=session,  # type: ignore[arg-type]
        max_retries=2,
        backoff_seconds=0,
        throttle_seconds=0,
    )

    with pytest.raises(
        RuntimeError,
        match="Open Exchange Rates request error for 2024-01-01 after retries: Timeout",
    ):
        client.fetch_historical(requested_date=date(2024, 1, 1))

    assert session.calls == 3
