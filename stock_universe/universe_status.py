"""Read-only status reporting for the canonical stock universe database."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from stock_universe.paths import CANONICAL_DB_PATH
from stock_universe.storage.sqlite_access import (
    connect_readonly_sqlite,
    readonly_db_uri,
)
from stock_universe.storage.sqlite_repo import SCHEMA_VERSION


def universe_status(db_path: str | Path | None = None) -> dict[str, Any]:
    db = Path(db_path or CANONICAL_DB_PATH)
    canonical = CANONICAL_DB_PATH
    payload: dict[str, Any] = {
        "canonical_db": str(canonical),
        "db": str(db),
        "db_is_canonical": db.resolve() == canonical.resolve(),
        "db_exists": db.exists(),
        "schema_expected": SCHEMA_VERSION,
        "schema_version": "",
        "schema_current": False,
        "required_tables_present": False,
        "universe_populated": False,
        "reference_universe": {
            "row_count": 0,
            "distinct_tickers": 0,
            "distinct_series": 0,
            "latest_snapshot_as_of_date": "",
            "scopes": [],
            "security_types": [],
            "updates": [],
            "latest_update": None,
        },
        "execution": {},
    }
    if not db.exists():
        payload["ok"] = False
        payload["reason"] = "database_not_found"
        return payload
    try:
        with connect_readonly_sqlite(db) as conn:
            present_tables = _present_tables(conn)
            payload["required_tables_present"] = _required_tables().issubset(
                present_tables
            )
            payload["schema_version"] = _schema_version(conn)
            payload["schema_current"] = payload["schema_version"] == SCHEMA_VERSION
            if "reference_universe_snapshots" in present_tables:
                payload["reference_universe"] = _reference_universe_payload(
                    conn, present_tables
                )
            payload["execution"] = _execution_payload(conn, present_tables)
    except sqlite3.Error as exc:
        payload["ok"] = False
        payload["reason"] = "sqlite_read_failed"
        payload["error"] = str(exc)
        return payload
    payload["universe_populated"] = bool(payload["reference_universe"]["row_count"])
    payload["ok"] = bool(
        payload["schema_current"] and payload["required_tables_present"]
    )
    if not payload["universe_populated"]:
        payload["reason"] = "reference_universe_empty"
    return payload


def _reference_universe_payload(
    conn: sqlite3.Connection, present_tables: set[str]
) -> dict[str, Any]:
    counts = conn.execute(
        """
        SELECT
          COUNT(*) AS row_count,
          COUNT(DISTINCT ticker) AS distinct_tickers,
          COUNT(DISTINCT ohlcv_series_id) AS distinct_series,
          COALESCE(MAX(snapshot_as_of_date), '') AS latest_snapshot_as_of_date
        FROM reference_universe_snapshots
        """
    ).fetchone()
    payload = {
        "row_count": int(counts["row_count"]),
        "distinct_tickers": int(counts["distinct_tickers"]),
        "distinct_series": int(counts["distinct_series"]),
        "latest_snapshot_as_of_date": str(counts["latest_snapshot_as_of_date"] or ""),
        "scopes": [
            dict(row)
            for row in conn.execute(
                """
                SELECT
                  snapshot_as_of_date,
                  market,
                  primary_exchange,
                  active_flag,
                  COUNT(*) AS row_count
                FROM reference_universe_snapshots
                GROUP BY snapshot_as_of_date, market, primary_exchange, active_flag
                ORDER BY snapshot_as_of_date DESC, market, primary_exchange, active_flag
                """
            ).fetchall()
        ],
        "security_types": [
            dict(row)
            for row in conn.execute(
                """
                SELECT security_type, COUNT(*) AS row_count
                FROM reference_universe_snapshots
                GROUP BY security_type
                ORDER BY row_count DESC, security_type
                """
            ).fetchall()
        ],
        "updates": [],
    }
    if "reference_universe_updates" in present_tables:
        payload["updates"] = [
            _update_row_payload(row)
            for row in conn.execute(
                """
                SELECT
                  provider,
                  snapshot_as_of_date,
                  market,
                  exchange,
                  active_mode,
                  limit_value,
                  max_pages,
                  complete_flag,
                  fetched_count,
                  page_count,
                  pending_requests_json,
                  committed_at_utc
                FROM reference_universe_updates
                ORDER BY reference_update_id DESC
                LIMIT 10
                """
            ).fetchall()
        ]
        payload["latest_update"] = payload["updates"][0] if payload["updates"] else None
    return payload


def _update_row_payload(row: sqlite3.Row) -> dict[str, Any]:
    pending = _json_list(row["pending_requests_json"])
    return {
        "provider": row["provider"],
        "snapshot_as_of_date": row["snapshot_as_of_date"],
        "market": row["market"],
        "exchange": row["exchange"],
        "active_mode": row["active_mode"],
        "limit": row["limit_value"],
        "max_pages": row["max_pages"],
        "complete": bool(row["complete_flag"]),
        "fetched_count": row["fetched_count"],
        "page_count": row["page_count"],
        "pending_count": len(pending),
        "pending_requests": pending,
        "committed_at_utc": row["committed_at_utc"],
    }


def _execution_payload(
    conn: sqlite3.Connection, present_tables: set[str]
) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for table in (
        "data_sources",
        "ohlcv_series_id_lookup",
        "ohlcv_series",
        "ohlcv_bar_scopes",
        "ohlcv_bar_lineage",
        "ohlcv_bars_day",
        "ohlcv_bars_hour",
        "ohlcv_bars_minute",
        "backfill_plans",
        "execution_receipts",
        "execution_approvals",
    ):
        if table in present_tables:
            payload[table] = int(
                conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            )
    if {"ohlcv_bars_day", "ohlcv_bars_hour", "ohlcv_bars_minute"}.issubset(
        present_tables
    ):
        payload["ohlcv_bars"] = (
            int(payload.get("ohlcv_bars_day") or 0)
            + int(payload.get("ohlcv_bars_hour") or 0)
            + int(payload.get("ohlcv_bars_minute") or 0)
        )
    return payload


def _schema_version(conn: sqlite3.Connection) -> str:
    if "schema_metadata" not in _present_tables(conn):
        return ""
    row = conn.execute(
        "SELECT value FROM schema_metadata WHERE key = 'schema_version'"
    ).fetchone()
    return str(row[0]) if row else ""


def _present_tables(conn: sqlite3.Connection) -> set[str]:
    return {
        str(row[0])
        for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }


def _required_tables() -> set[str]:
    return {
        "schema_metadata",
        "data_sources",
        "ohlcv_series_id_lookup",
        "ohlcv_series",
        "ticker_aliases",
        "reference_universe_snapshots",
        "reference_universe_updates",
        "evidence_facts",
        "backfill_plans",
        "ohlcv_bar_scopes",
        "ohlcv_bar_lineage",
        "ohlcv_bars_day",
        "ohlcv_bars_hour",
        "ohlcv_bars_minute",
        "ohlcv_day_bar_quality_events",
        "ohlcv_hour_bar_quality_events",
        "ohlcv_minute_bar_quality_events",
        "execution_receipts",
        "execution_approvals",
    }


def _json_list(value: Any) -> list[Any]:
    try:
        parsed = json.loads(str(value or "[]"))
    except ValueError:
        return []
    return parsed if isinstance(parsed, list) else []


def _readonly_db_uri(path: Path) -> str:
    return readonly_db_uri(path)
