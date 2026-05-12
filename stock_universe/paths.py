"""Project-wide filesystem paths for the stock-universe workspace."""

from __future__ import annotations

from pathlib import Path


CANONICAL_DB_PATH = Path("production_build/stock_universe.sqlite")


def canonical_db_text() -> str:
    return str(CANONICAL_DB_PATH)
