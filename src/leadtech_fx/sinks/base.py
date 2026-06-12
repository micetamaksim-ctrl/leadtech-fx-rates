"""Base sink contract for writing transformed FX records."""

from __future__ import annotations

from abc import ABC, abstractmethod

from leadtech_fx.models import FxRateRow


class DatabricksSink(ABC):
    """Contract for writing FX rows into a Databricks-like table sink."""

    @abstractmethod
    def write_idempotent(self, rows: list[FxRateRow], target_table: str) -> int:
        """Write rows idempotently and return number of rows written."""
