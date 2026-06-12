# Leadtech FX Rates

## Overview

Small, production-minded Airflow project that fetches daily historical FX rates from Open Exchange Rates (OXR), transforms them into tabular rows, and writes them into a local mock Databricks/Delta-style sink.

The goal is clarity and engineering judgment rather than heavy infrastructure.

## Project Goals

- Implement a custom Airflow operator: `OpenExchangeRatesToDatabricksOperator`.
- Keep logic modular (client, transforms, sink, operator).
- Support re-runs and backfills safely via idempotent partition overwrite.
- Keep solution reproducible without external platform dependencies.

## Architecture

Pipeline flow:

1. **Operator** parses and validates date inputs (UTC assumptions).
2. **Client** fetches one OXR historical payload per requested date with retries/backoff.
3. **Transform** flattens `rates` into analytics-friendly `FxRateRow` records.
4. **Sink** writes records into partitioned local files under `outputs/mock_delta/`.

Core modules:

- `clients/open_exchange_rates.py`: API integration with retry and throttling.
- `transforms.py`: payload-to-row normalization and validation.
- `sinks/mock_databricks_sink.py`: idempotent partition overwrite sink.
- `operators/open_exchange_rates_to_databricks.py`: orchestration layer for Airflow.

## Project Structure

```text
leadtech-fx-rates/
├── README.md
├── requirements.txt
├── .env.example
├── .gitignore
├── dags/
│   └── fx_rates_daily.py
├── src/
│   └── leadtech_fx/
│       ├── __init__.py
│       ├── models.py
│       ├── date_utils.py
│       ├── transforms.py
│       ├── clients/
│       │   ├── __init__.py
│       │   └── open_exchange_rates.py
│       ├── sinks/
│       │   ├── __init__.py
│       │   ├── base.py
│       │   └── mock_databricks_sink.py
│       └── operators/
│           ├── __init__.py
│           └── open_exchange_rates_to_databricks.py
├── tests/
│   ├── conftest.py
│   ├── test_open_exchange_rates_client.py
│   ├── test_transforms.py
│   ├── test_sink_idempotency.py
│   └── test_date_utils.py
└── sample_data/
    └── oxr_historical_2024-01-01.json
```

## Prerequisites

- Python **3.11**
- Airflow **3.x** target
- Open Exchange Rates app id (free tier is enough for this assessment)

## Setup

```bash
python -m venv .venv
# Windows PowerShell
.venv\Scripts\Activate.ps1

python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

For tests/imports, the repository uses `tests/conftest.py` to add `src/` to `PYTHONPATH`.

## Airflow Configuration

Configure OXR credentials at runtime:

1. Preferred for this project: Airflow Variable `open_exchange_rates_app_id`
2. Operator also supports explicit `app_id` parameter (useful for local/testing only)

Example:

```bash
airflow variables set open_exchange_rates_app_id "<your_app_id>"
```

`app_id` is never logged.

Production note: store credentials in Airflow Connection or Variable, not source code.

## Running the DAG

Daily DAG:

- DAG id: `fx_rates_daily`
- Schedule: `@daily`
- Catchup enabled (backfill-friendly)
- Daily task uses `{{ ds }}` for both `start_date` and `end_date`, so each run processes one logical UTC date

Example local Airflow commands:

```bash
export PYTHONPATH=$PWD/src
airflow db migrate
airflow dags list
airflow dags trigger fx_rates_daily
```

On Windows PowerShell:

```powershell
$env:PYTHONPATH = "$PWD/src"
airflow db migrate
airflow dags list
airflow dags trigger fx_rates_daily
```

The DAG file includes a commented example showing how to configure a historical range backfill task.

## Testing

Run tests:

```bash
python -m pytest -q
```

Tests are local-only and do not require a real API key.
Sample payload-based tests read `sample_data/oxr_historical_2024-01-01.json`.

Current test files:

- `tests/conftest.py`
- `tests/test_open_exchange_rates_client.py`
- `tests/test_date_utils.py`
- `tests/test_transforms.py`
- `tests/test_sink_idempotency.py`

## Design Decisions

- **Mock Databricks sink chosen intentionally**: I chose a mocked Databricks sink to keep the project reproducible and focused within the expected 4–8 hour effort.
- **Partition strategy**: the sink partitions by `requested_date`.
- **Idempotency strategy**: re-running the same date range overwrites the same logical partitions.
- **Production mapping**: a real Databricks implementation would use Delta `MERGE` or partition overwrite.
- **API scope**: I intentionally avoided paid Open Exchange Rates parameters such as custom base currency or symbols.

### Data model

`FxRateRow` contains:

- `requested_date`: logical date asked from API
- `rate_date`: effective date derived from API payload timestamp
- `base_currency`, `quote_currency`, `rate`
- `source`
- `ingested_at` (UTC)

## Assumptions and Limitations

- OXR free tier constraints apply:
  - base currency fixed to `USD`
  - daily historical granularity only (no intraday)
  - soft request quota (1,000/month)
- `max_requests_per_run` protects against accidental large backfills in one run.
- Current sink is filesystem-based, not a real Databricks target.
- Current implementation writes full partition files for requested dates (simple and clear for assessment scope).

### Backfill behavior

- Because the operator accepts date ranges and the DAG has catchup enabled, historical backfills are straightforward.
- Re-running backfill dates is safe due to partition overwrite idempotency.

## Next Improvements

- Replace mock sink with real Databricks Delta writer (`MERGE`/overwrite semantics).
- Add integration tests around operator execution with mocked Airflow context.
- Add metrics/observability hooks (latency, retries, row counts).
- Add configurable retry jitter and optional circuit-breaker behavior.
