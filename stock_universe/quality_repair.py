"""Commit-gated data-quality repair helpers."""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path
from typing import Any

from stock_universe.paths import CANONICAL_DB_PATH
from stock_universe.storage import SQLiteStockUniverseRepository
from stock_universe.storage.sqlite_access import (
    connect_readonly_sqlite,
    readonly_db_uri,
)


def repair_missing_execution_receipts(
    db_path: str | Path | None = None,
    *,
    series_ids: tuple[int, ...] = (),
    limit: int = 50,
    commit: bool = False,
    reason: str = "quality audit repair for approval without durable execution receipt",
) -> dict[str, Any]:
    db = Path(db_path or CANONICAL_DB_PATH)
    rows = _missing_receipt_rows(db, series_ids=series_ids, limit=limit)
    repository = SQLiteStockUniverseRepository(db)
    repairs: list[dict[str, Any]] = []
    for row in rows:
        receipt = _repair_receipt(row, reason=reason)
        item = {
            "execution_approval_id": row["execution_approval_id"],
            "ohlcv_series_id": row["ohlcv_series_id"],
            "plan_id": row["plan_id"],
            "plan_status": row["plan_status"],
            "request_hash": row["request_hash"],
            "evidence_ledger_hash": row["evidence_ledger_hash"],
            "approved_by": row["approved_by"],
            "approved_at_utc": row["approved_at_utc"],
            "planned_segment_count": receipt["planned_segment_count"],
            "receipt_status": receipt["status"],
            "error_type": receipt["error_type"],
            "committed": False,
        }
        if commit:
            item["execution_receipt_id"] = repository.insert_execution_receipt(receipt)
            item["committed"] = True
        repairs.append(item)
    return {
        "db": str(db),
        "commit": commit,
        "selected_count": len(rows),
        "repaired_count": sum(1 for item in repairs if item["committed"]),
        "repairs": repairs,
    }


def _missing_receipt_rows(
    db: Path,
    *,
    series_ids: tuple[int, ...],
    limit: int,
) -> list[dict[str, Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    if series_ids:
        placeholders = ", ".join("?" for _ in series_ids)
        clauses.append(f"p.ohlcv_series_id IN ({placeholders})")
        params.extend(series_ids)
    where = f"AND {' AND '.join(clauses)}" if clauses else ""
    params.append(limit)
    with connect_readonly_sqlite(db) as conn:
        rows = conn.execute(
            f"""
            SELECT
              p.plan_id,
              p.request_hash,
              p.evidence_ledger_hash,
              p.ohlcv_series_id,
              p.status AS plan_status,
              p.plan_json,
              p.plan_hash,
              a.execution_approval_id,
              a.approved_by,
              a.approved_at_utc,
              a.approval_hash,
              a.inserted_at_utc
            FROM backfill_plans p
            JOIN execution_approvals a
              ON a.request_hash = p.request_hash
             AND a.evidence_ledger_hash = p.evidence_ledger_hash
             AND a.plan_hash = p.plan_hash
             AND a.ohlcv_series_id = p.ohlcv_series_id
            LEFT JOIN execution_receipts r
              ON r.request_hash = p.request_hash
             AND r.evidence_ledger_hash = p.evidence_ledger_hash
             AND r.ohlcv_series_id = p.ohlcv_series_id
            WHERE r.execution_receipt_id IS NULL
              {where}
            ORDER BY a.execution_approval_id
            LIMIT ?
            """,
            params,
        ).fetchall()
    return [dict(row) for row in rows]


def _repair_receipt(row: dict[str, Any], *, reason: str) -> dict[str, Any]:
    now = dt.datetime.now(dt.UTC).isoformat()
    try:
        plan = json.loads(str(row.get("plan_json") or "{}"))
    except ValueError:
        plan = {}
    planned_segment_count = len(plan.get("segments") or [])
    approved_at = str(row.get("approved_at_utc") or row.get("inserted_at_utc") or now)
    return {
        "ok": False,
        "status": "error",
        "request_hash": row["request_hash"],
        "evidence_ledger_hash": row["evidence_ledger_hash"],
        "ohlcv_series_id": int(row["ohlcv_series_id"]),
        "planned_segment_count": planned_segment_count,
        "fetched_bar_count": 0,
        "inserted_bar_count": 0,
        "started_at_utc": approved_at,
        "finished_at_utc": now,
        "request_log": [],
        "approved_by": row.get("approved_by") or "",
        "approval_hash": row.get("approval_hash") or "",
        "error_type": "MissingExecutionReceiptRepair",
        "error_message": (
            f"Inserted durable error receipt for execution approval {row['execution_approval_id']} because no "
            f"execution receipt was persisted. {reason}."
        ),
    }


def _readonly_db_uri(path: Path) -> str:
    return readonly_db_uri(path)
