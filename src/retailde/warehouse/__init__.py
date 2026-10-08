"""DuckDB star-schema warehouse: DDL, loaders, and row-count introspection."""

from __future__ import annotations

from .load import WarehouseError, counts, load
from .schema import SCHEMA_STATEMENTS

__all__ = ["load", "counts", "WarehouseError", "SCHEMA_STATEMENTS"]
