"""Mock Databricks-like sink for local testing and idempotency checks."""

from __future__ import annotations

import json
import os
import tempfile
from collections import defaultdict
from dataclasses import asdict
from datetime import UTC, datetime
from pathlib import Path

from leadtech_fx.models import FxRateRow
from leadtech_fx.sinks.base import DatabricksSink


class MockDatabricksSink(DatabricksSink):
    """Local filesystem sink that mimics Delta partition overwrite behavior."""

    def __init__(self, root_path: str | Path = "outputs/mock_delta") -> None:
        self._root_path = Path(root_path)

    def write_idempotent(self, rows: list[FxRateRow], target_table: str) -> int:
        """Overwrite each requested_date partition and return rows written."""
        if not target_table or not target_table.strip():
            raise ValueError("target_table must be a non-empty string.")

        table_path = self._table_path(target_table=target_table)
        table_path.mkdir(parents=True, exist_ok=True)

        if not rows:
            self._write_run_summary(
                table_path=table_path,
                target_table=target_table,
                requested_dates=[],
                rows_written=0,
            )
            return 0

        rows_by_date: dict[str, list[FxRateRow]] = defaultdict(list)
        for row in rows:
            partition_key = row.requested_date.isoformat()
            rows_by_date[partition_key].append(row)

        total_rows_written = 0
        for partition_date, partition_rows in rows_by_date.items():
            partition_dir = table_path / f"requested_date={partition_date}"
            partition_dir.mkdir(parents=True, exist_ok=True)
            data_path = partition_dir / "data.jsonl"

            # Idempotency + safer write: write to temp file then atomically replace.
            self._write_partition_data_atomically(
                partition_rows=partition_rows,
                partition_dir=partition_dir,
                data_path=data_path,
            )
            total_rows_written += len(partition_rows)

        self._write_run_summary(
            table_path=table_path,
            target_table=target_table,
            requested_dates=sorted(rows_by_date.keys()),
            rows_written=total_rows_written,
        )
        return total_rows_written

    def _table_path(self, target_table: str) -> Path:
        table_parts = [part.strip() for part in target_table.split(".") if part.strip()]
        if not table_parts:
            raise ValueError("target_table must contain at least one valid name segment.")
        return self._root_path.joinpath(*table_parts)

    @staticmethod
    def _serialize_row(row: FxRateRow) -> str:
        payload = asdict(row)
        payload["requested_date"] = row.requested_date.isoformat()
        payload["rate_date"] = row.rate_date.isoformat()
        payload["ingested_at"] = row.ingested_at.astimezone(UTC).isoformat()
        return json.dumps(payload, sort_keys=True)

    def _write_partition_data_atomically(
        self,
        partition_rows: list[FxRateRow],
        partition_dir: Path,
        data_path: Path,
    ) -> None:
        """Write partition rows via temp file and atomic replace."""
        file_descriptor, temp_name = tempfile.mkstemp(
            dir=partition_dir,
            prefix="data_",
            suffix=".tmp",
        )
        temp_path = Path(temp_name)
        try:
            with os.fdopen(file_descriptor, "w", encoding="utf-8") as handle:
                for row in partition_rows:
                    handle.write(self._serialize_row(row) + "\n")
            temp_path.replace(data_path)
        finally:
            if temp_path.exists():
                temp_path.unlink()

    @staticmethod
    def _write_run_summary(
        table_path: Path,
        target_table: str,
        requested_dates: list[str],
        rows_written: int,
    ) -> None:
        summary_path = table_path / "run_summary.json"
        summary = {
            "target_table": target_table,
            "rows_written": rows_written,
            "requested_dates": requested_dates,
            "written_at_utc": datetime.now(tz=UTC).isoformat(),
        }
        with summary_path.open("w", encoding="utf-8") as handle:
            json.dump(summary, handle, indent=2, sort_keys=True)
