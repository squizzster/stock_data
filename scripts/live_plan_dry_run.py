#!/usr/bin/env python
"""Run a read-only live backfill planning dry-run.

This script is a boundary tool: it may read an API key and call live providers,
but the planner still receives only typed evidence facts.
"""

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
from stock_universe.reports import legacy_plan_dict, render_backfill_plan_markdown
from stock_universe.workflows import (
    massive_live_dry_run_source_from_legacy_plan,
    run_backfill_source_dry_run_trace,
)
from stock_universe.xctx import result_envelope


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Plan a backfill using live read-only evidence."
    )
    parser.add_argument(
        "--fixture",
        required=True,
        help="Legacy plan fixture/input used for request and target seed facts.",
    )
    parser.add_argument(
        "--api-key", default=None, help="Massive API key. Defaults to MASSIVE_API_KEY."
    )
    parser.add_argument(
        "--base-url", default="https://api.massive.com", help="Massive API base URL."
    )
    parser.add_argument(
        "--capture-dir", default=None, help="Optional raw capture directory."
    )
    parser.add_argument("--max-rounds", type=int, default=8)
    parser.add_argument(
        "--legacy-json-out",
        default=None,
        help="Optional path for legacy-compatible plan JSON.",
    )
    parser.add_argument(
        "--markdown-out", default=None, help="Optional path for Markdown plan report."
    )
    args = parser.parse_args(argv)

    api_key = args.api_key or os.environ.get("MASSIVE_API_KEY")
    if not api_key:
        parser.error("--api-key or MASSIVE_API_KEY is required")

    fixture_path = Path(args.fixture)
    plan_seed = json.loads(fixture_path.read_text(encoding="utf-8"))
    capture_dir = Path(args.capture_dir) if args.capture_dir else None
    source, client = massive_live_dry_run_source_from_legacy_plan(
        plan_seed,
        api_key=api_key,
        base_url=args.base_url,
        capture_dir=capture_dir,
    )
    trace = run_backfill_source_dry_run_trace(source, max_rounds=args.max_rounds)

    envelope = result_envelope("live-backfill-plan-dry-run", trace.result)
    envelope["effects"] = {
        "will_read": _planned_reads(args),
        "will_write": _planned_writes(args),
        "did_write": _captured_raw_files(args),
    }
    envelope["request_log"] = [
        {
            "endpoint": item.endpoint,
            "params_without_api_key": item.params_without_api_key,
            "http_code": item.http_code,
            "api_status": item.api_status,
            "elapsed_seconds": item.elapsed_seconds,
        }
        for item in client.request_log
    ]
    envelope["rounds"] = [
        {
            "round_index": item.round_index,
            "ledger_hash": item.ledger_hash,
            "result_type": item.result.__class__.__name__,
            "collected_fact_count": len(item.collected_facts),
        }
        for item in trace.rounds
    ]

    if isinstance(trace.result, BackfillPlan):
        _write_optional_plan_outputs(trace.result, args, envelope)

    print(json.dumps(envelope, indent=2, sort_keys=True))
    return 0


def _planned_writes(args: argparse.Namespace) -> list[str]:
    writes = []
    if args.capture_dir:
        writes.append(f"{args.capture_dir}/*.json")
    if args.legacy_json_out:
        writes.append(args.legacy_json_out)
    if args.markdown_out:
        writes.append(args.markdown_out)
    return writes


def _planned_reads(args: argparse.Namespace) -> list[str]:
    reads = [
        args.fixture,
        "massive.ticker_events",
        "massive.reference_boundary",
        "massive.bar_probe",
        "massive.identity_scan",
        "massive.ticker_replacement",
    ]
    if not args.api_key:
        reads.append("env:MASSIVE_API_KEY")
    return reads


def _captured_raw_files(args: argparse.Namespace) -> list[str]:
    if not args.capture_dir:
        return []
    capture_dir = Path(args.capture_dir)
    if not capture_dir.exists():
        return []
    return [str(path) for path in sorted(capture_dir.glob("*.json"))]


def _write_optional_plan_outputs(
    plan: BackfillPlan, args: argparse.Namespace, envelope: dict[str, Any]
) -> None:
    did_write = envelope["effects"]["did_write"]
    if args.legacy_json_out:
        path = Path(args.legacy_json_out)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(legacy_plan_dict(plan), indent=2, sort_keys=True),
            encoding="utf-8",
        )
        did_write.append(str(path))
    if args.markdown_out:
        path = Path(args.markdown_out)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(render_backfill_plan_markdown(plan), encoding="utf-8")
        did_write.append(str(path))


if __name__ == "__main__":
    raise SystemExit(main())
