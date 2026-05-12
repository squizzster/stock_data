#!/usr/bin/env python
"""Populate a new SQLite database from new approved plans and live bars."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from stock_universe.domain import BackfillPlan
from stock_universe.executors import ExecutionApproval, execute_live_bar_backfill
from stock_universe.paths import canonical_db_text
from stock_universe.storage import SQLiteStockUniverseRepository
from stock_universe.workflows import (
    live_dry_run_base_facts_from_legacy_plan,
    massive_live_dry_run_source_from_legacy_plan,
    run_backfill_source_dry_run_trace,
)


DEFAULT_PRESSURE_REPORT = Path(
    "/home/EdgarTools/stocks/stock_test/production_build/test_reports/backfill_plan_pressure_report.json"
)
DEFAULT_FIXTURE_DIR = REPO_ROOT / "tests" / "fixtures" / "legacy_plans"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", default=canonical_db_text())
    parser.add_argument(
        "--fixture",
        action="append",
        default=[],
        help="Legacy plan JSON path. May repeat.",
    )
    parser.add_argument(
        "--fixture-dir", default=None, help="Directory of legacy plan JSON files."
    )
    parser.add_argument("--pressure-report", default=str(DEFAULT_PRESSURE_REPORT))
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument(
        "--api-key", default=None, help="Massive API key. Defaults to MASSIVE_API_KEY."
    )
    parser.add_argument("--base-url", default="https://api.massive.com")
    parser.add_argument("--max-rounds", type=int, default=8)
    parser.add_argument(
        "--no-caution",
        action="store_true",
        help="Skip caution plans instead of approving them.",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Return nonzero when any instrument fails or skips.",
    )
    args = parser.parse_args(argv)

    api_key = args.api_key or os.environ.get("MASSIVE_API_KEY")
    if not api_key:
        parser.error("--api-key or MASSIVE_API_KEY is required")

    paths = _input_paths(args)
    repository = SQLiteStockUniverseRepository(args.db)
    repository.ensure_schema()
    results = []
    for path in paths[: args.limit]:
        results.append(_execute_one(path, args, api_key, repository))

    validation = repository.validate()
    counts = repository.counts()
    summary = {
        "ok": validation.ok and all(item["status"] == "ok" for item in results),
        "db": str(Path(args.db)),
        "attempted": len(results),
        "ok_count": sum(1 for item in results if item["status"] == "ok"),
        "skipped_count": sum(1 for item in results if item["status"] == "skipped"),
        "error_count": sum(1 for item in results if item["status"] == "error"),
        "counts": counts,
        "validation": {
            "ok": validation.ok,
            "checks": list(validation.checks),
            "failures": list(validation.failures),
        },
        "results": results,
    }
    print(json.dumps(summary, indent=2, sort_keys=True))
    if args.strict and not summary["ok"]:
        return 1
    return 0


def _execute_one(
    path: Path,
    args: argparse.Namespace,
    api_key: str,
    repository: SQLiteStockUniverseRepository,
) -> dict[str, Any]:
    try:
        legacy = json.loads(path.read_text(encoding="utf-8"))
        source, client = massive_live_dry_run_source_from_legacy_plan(
            legacy,
            api_key=api_key,
            base_url=args.base_url,
        )
        trace = run_backfill_source_dry_run_trace(source, max_rounds=args.max_rounds)
        result = trace.result
        if not isinstance(result, BackfillPlan):
            return {
                "fixture": str(path),
                "status": "skipped",
                "reason": "planner returned EvidenceNeeded",
                "requests": [request.to_legacy_dict() for request in result.requests],
                "rounds": _rounds(trace.rounds),
                "planning_request_count": len(client.request_log),
            }
        if result.status == "blocked":
            return {
                "fixture": str(path),
                "status": "skipped",
                "ohlcv_series_id": result.target.ohlcv_series_id,
                "plan_status": result.status,
                "reason": "blocked plans are not executable",
            }
        if result.status == "caution" and args.no_caution:
            return {
                "fixture": str(path),
                "status": "skipped",
                "ohlcv_series_id": result.target.ohlcv_series_id,
                "plan_status": result.status,
                "reason": "caution plan skipped by --no-caution",
            }
        approval = ExecutionApproval(
            request_hash=result.request.request_hash,
            allow_caution=result.status == "caution",
            approved_by="live_sqlite_backfill",
        )
        approval_record = repository.insert_execution_approval(
            result,
            approval,
            reason="fixture-seeded script backfill approval",
        )
        receipt = execute_live_bar_backfill(
            result,
            approval,
            client,
            repository,
            evidence_facts=(
                live_dry_run_base_facts_from_legacy_plan(legacy)
                + tuple(fact for item in trace.rounds for fact in item.collected_facts)
            ),
        )
        return {
            "fixture": str(path),
            "status": "ok",
            "ohlcv_series_id": result.target.ohlcv_series_id,
            "latest_ticker": result.target.latest_ticker,
            "plan_status": result.status,
            "segments": [segment.to_legacy_dict() for segment in result.segments],
            "fetched_bar_count": receipt.fetched_bar_count,
            "inserted_bar_count": receipt.inserted_bar_count,
            "approval_hash": approval_record["approval_hash"],
            "request_count": len(receipt.request_log),
            "planning_rounds": _rounds(trace.rounds),
        }
    except Exception as exc:
        return {
            "fixture": str(path),
            "status": "error",
            "error_type": exc.__class__.__name__,
            "error": str(exc),
        }


def _input_paths(args: argparse.Namespace) -> list[Path]:
    explicit = [Path(path) for path in args.fixture]
    if explicit:
        return explicit
    if args.fixture_dir:
        return sorted(Path(args.fixture_dir).glob("*.json"))
    pressure_paths = _pressure_report_paths(Path(args.pressure_report))
    if pressure_paths:
        return pressure_paths
    return sorted(DEFAULT_FIXTURE_DIR.glob("*.json"))


def _pressure_report_paths(path: Path) -> list[Path]:
    if not path.exists():
        return []
    data = json.loads(path.read_text(encoding="utf-8"))
    paths = []
    for result in data.get("results") or []:
        plan_files = (result.get("evidence") or {}).get("plan_files") or {}
        plan_path = plan_files.get("json")
        if plan_path:
            candidate = Path(plan_path)
            if not candidate.is_absolute():
                candidate = Path("/home/EdgarTools/stocks/stock_test") / candidate
            if candidate.exists():
                paths.append(candidate)
    return paths


def _rounds(rounds: tuple[Any, ...]) -> list[dict[str, Any]]:
    return [
        {
            "round_index": item.round_index,
            "ledger_hash": item.ledger_hash,
            "result_type": item.result.__class__.__name__,
            "collected_fact_count": len(item.collected_facts),
        }
        for item in rounds
    ]


if __name__ == "__main__":
    raise SystemExit(main())
