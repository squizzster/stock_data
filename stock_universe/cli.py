"""Primary stock-universe command surface."""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import signal
import shutil
import sqlite3
import sys
import time
from pathlib import Path
from textwrap import dedent
from typing import Any

from stock_universe.cli_runtime import (
    emit_json,
    interrupted_exit,
    print_help_for_missing_command,
    silence_stdout,
)
from stock_universe.defaults import DEFAULT_MAX_ROUNDS
from stock_universe.domain import BackfillPlan, normalize_bar_grain
from stock_universe.executors import ExecutionApproval, execute_live_bar_backfill
from stock_universe.paths import canonical_db_text
from stock_universe.planner import plan_backfill
from stock_universe.providers import MassiveProviderConfig, MassiveReadOnlyClient
from stock_universe.progress import emit_cli_progress, emit_stderr_progress
from stock_universe.quality_audit import ISSUE_CATEGORIES, quality_audit
from stock_universe.quality_repair import repair_missing_execution_receipts
from stock_universe.reports import render_backfill_plan_markdown
from stock_universe.storage import (
    SQLiteStockUniverseRepository,
    connect_readonly_sqlite,
)
from stock_universe.storage.sqlite_repo import SCHEMA_VERSION
from stock_universe.universe_status import universe_status
from stock_universe.workflows import (
    CATCH_UP_STOP_MODES,
    DEFAULT_CATCH_UP_BATCH_SIZE,
    DEFAULT_CATCH_UP_STOP_MODE,
    DEFAULT_CATCH_UP_WORKERS,
    DEFAULT_RESOURCE_CHECK_SECONDS,
    DEFAULT_TICKER_SEED_FROM_DATE,
    MAX_CATCH_UP_WORKERS,
    ReferenceUniverseRequest,
    build_catch_up_plan,
    catch_up_plan_from_run_dir,
    execute_catch_up_plan,
    fetch_massive_reference_universe,
    live_identity_search,
    massive_live_source_from_ticker,
    massive_live_source_from_series_id,
    reconcile_catch_up_run,
    request_catch_up_stop,
    run_backfill_source_dry_run_trace,
    sqlite_identity_search,
)
from stock_universe.xctx import (
    PROTOCOL_VERSION,
    normalize_action_records,
    result_envelope,
)


def main(argv: list[str] | None = None) -> int:
    prog = "stock-universe"
    try:
        argv_was_none = argv is None
        argv = list(sys.argv[1:] if argv is None else argv)
        prog = _display_prog() if argv_was_none else "stock-universe"
        if argv and argv[0] == "xctx":
            from stock_universe.xctx.cli import main as xctx_main

            return xctx_main(argv[1:], prog=f"{prog} xctx")
        parser = _parser(prog=prog)
        if not argv:
            print_help_for_missing_command(parser)
        args = parser.parse_args(argv)
        try:
            payload = args.func(args, parser)
        except CliError as exc:
            parser.exit(exc.returncode, f"stock-universe: error: {exc}\n")
        payload = normalize_action_records(payload)
        if not emit_json(payload):
            return 0
        strict_exit = bool(
            getattr(args, "strict_exit", False) or getattr(args, "strict", False)
        )
        return 0 if payload.get("ok", False) or not strict_exit else 1
    except BrokenPipeError:
        silence_stdout()
        return 0
    except KeyboardInterrupt:
        return interrupted_exit(prog)


class CliError(ValueError):
    def __init__(self, message: str, *, returncode: int = 2) -> None:
        super().__init__(message)
        self.returncode = returncode


class CatchUpHardTargetError(RuntimeError):
    def __init__(self, *, series_id: int, error_type: str, error: str) -> None:
        super().__init__(
            f"hard target error: ohlcv_series_id={series_id} error_type={error_type} error={error}"
        )
        self.series_id = series_id
        self.error_type = error_type
        self.error = error


def _display_prog() -> str:
    invoked = sys.argv[0]
    name = Path(invoked).name
    if name in {"__main__.py", "cli.py"}:
        return "stock-universe"
    if name == "stock_universe.cli" and ("/" in invoked or "\\" in invoked):
        return invoked
    return name or "stock-universe"


def _parser(*, prog: str = "stock-universe") -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog=prog,
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=dedent(
            """\
            Agent / Executable Context:
              {prog} xctx --help
                  Start here when an agent needs to learn the command surface.

              {prog} xctx doctor
                  Check local readiness and safe next actions without mutation.

              {prog} xctx universe-status
                  Inspect canonical DB coverage and reference-universe completeness.

              {prog} xctx tree
                  Discover transitions, schemas, effects, recipes, and next actions.

              {prog} xctx schema --command "xctx dry-run"
                  Inspect structured inputs and argv binding maps.

              {prog} xctx examples
                  Get runnable examples for the xctx learning loop.

              {prog} xctx dry-run --ohlcv-series-id 1
                  Rehearse planning without mutation.

            Safety boundary:
              xctx commands are read-oriented protocol commands. Durable mutations stay
              on stock-universe commands and require explicit --commit or backfill.
            """
        ).format(prog=prog),
    )
    parser.set_defaults(strict_exit=False)
    subcommands = parser.add_subparsers(dest="command", required=True)

    subcommands.add_parser(
        "xctx",
        help="Executable Context protocol plane for agent discovery and workflow rehearsal.",
        description="Use xctx subcommands to discover, dry-run, inspect, audit, and compose workflows.",
    )

    identity_search = subcommands.add_parser(
        "identity-search",
        aliases=["search"],
        help="Resolve a ticker, name, CIK, FIGI, or stored OHLCV series ID into identity candidates and related issuer share classes.",
    )
    identity_search.add_argument(
        "query_arg",
        nargs="?",
        help="Ticker, company name, CIK, FIGI, or OHLCV series ID.",
    )
    identity_search.add_argument(
        "--query",
        dest="query_option",
        default=None,
        help="Ticker, company name, CIK, FIGI, or OHLCV series ID.",
    )
    identity_search.add_argument("--source", choices=("live", "db"), default="live")
    identity_search.add_argument(
        "--db",
        default=canonical_db_text(),
        help="SQLite database path for --source db. Defaults to the canonical universe DB.",
    )
    identity_search.add_argument(
        "--api-key", default=None, help="Massive API key. Defaults to MASSIVE_API_KEY."
    )
    identity_search.add_argument("--base-url", default="https://api.massive.com")
    identity_search.add_argument(
        "--as-of-date", default=None, help="Optional Massive reference date."
    )
    identity_search.add_argument("--limit", type=int, default=25)
    identity_search.add_argument("--capture-dir", default=None)
    identity_search.set_defaults(func=_identity_search)

    reference_universe = subcommands.add_parser(
        "update-reference-universe",
        help="Fetch a bounded live reference-universe snapshot and optionally persist it.",
    )
    reference_universe.add_argument(
        "--db",
        default=canonical_db_text(),
        help="SQLite database path. Defaults to the canonical universe DB.",
    )
    reference_universe.add_argument(
        "--api-key", default=None, help="Massive API key. Defaults to MASSIVE_API_KEY."
    )
    reference_universe.add_argument("--base-url", default="https://api.massive.com")
    reference_universe.add_argument("--market", default="stocks")
    reference_universe.add_argument(
        "--exchange",
        default="",
        help="Optional primary exchange MIC, for example XNAS.",
    )
    reference_universe.add_argument(
        "--as-of-date", default=None, help="Optional Massive reference date."
    )
    reference_universe.add_argument(
        "--active", choices=("active", "inactive", "all"), default="active"
    )
    reference_universe.add_argument("--limit", type=int, default=1000)
    reference_universe.add_argument("--max-pages", type=int, default=100)
    reference_universe.add_argument("--capture-dir", default=None)
    reference_universe.add_argument(
        "--commit", action="store_true", help="Persist fetched snapshots into SQLite."
    )
    _add_progress_args(reference_universe)
    reference_universe.set_defaults(func=_update_reference_universe)

    dry_run = subcommands.add_parser("dry-run", help="Run a live planning dry-run.")
    dry_run_input = dry_run.add_mutually_exclusive_group(required=True)
    dry_run_input.add_argument(
        "--ticker", help="Resolve a live Massive ticker into seed facts."
    )
    dry_run_input.add_argument(
        "--ohlcv-series-id",
        "--ohlcv_series_id",
        dest="ohlcv_series_id",
        type=int,
        help="Load a selected reference-universe OHLCV series ID from --db.",
    )
    dry_run.add_argument(
        "--db",
        default=canonical_db_text(),
        help="SQLite database path for --ohlcv-series-id. Defaults to the canonical universe DB.",
    )
    dry_run.add_argument(
        "--api-key", default=None, help="Massive API key. Defaults to MASSIVE_API_KEY."
    )
    dry_run.add_argument("--base-url", default="https://api.massive.com")
    dry_run.add_argument("--capture-dir", default=None)
    dry_run.add_argument(
        "--from-date",
        default=DEFAULT_TICKER_SEED_FROM_DATE,
        help="Start date. Defaults to the first session after the rolling 5-year boundary.",
    )
    dry_run.add_argument("--to-date", default=None)
    _add_bar_grain_arg(dry_run)
    dry_run.add_argument(
        "--identity-as-of-date",
        default=None,
        help="Use the latest DB reference snapshot on or before this date.",
    )
    dry_run.add_argument("--max-rounds", type=int, default=DEFAULT_MAX_ROUNDS)
    dry_run.add_argument("--markdown-out", default=None)
    dry_run.set_defaults(func=_dry_run)

    backfill = subcommands.add_parser(
        "backfill",
        help="Execute approved live bars for ticker or OHLCV-series selections.",
    )
    backfill.add_argument(
        "--db",
        default=canonical_db_text(),
        help="SQLite database path. Defaults to the canonical universe DB.",
    )
    backfill.add_argument(
        "--ticker",
        action="append",
        default=[],
        help="Resolve and execute a live Massive ticker. May repeat.",
    )
    backfill.add_argument(
        "--ohlcv-series-id",
        "--ohlcv_series_id",
        dest="ohlcv_series_id",
        action="append",
        type=int,
        default=[],
        help="Load and execute a selected reference-universe OHLCV series ID. May repeat.",
    )
    backfill.add_argument(
        "--api-key", default=None, help="Massive API key. Defaults to MASSIVE_API_KEY."
    )
    backfill.add_argument("--base-url", default="https://api.massive.com")
    backfill.add_argument(
        "--from-date",
        default=DEFAULT_TICKER_SEED_FROM_DATE,
        help="Start date. Defaults to the first session after the rolling 5-year boundary.",
    )
    backfill.add_argument("--to-date", default=None)
    _add_bar_grain_arg(backfill)
    backfill.add_argument(
        "--identity-as-of-date",
        default=None,
        help="Use the latest DB reference snapshot on or before this date.",
    )
    backfill.add_argument("--max-rounds", type=int, default=DEFAULT_MAX_ROUNDS)
    backfill.add_argument("--no-caution", action="store_true")
    backfill.add_argument(
        "--strict",
        action="store_true",
        help="Report ok=false if any input fails or skips.",
    )
    _add_progress_args(backfill)
    backfill.set_defaults(func=_backfill)

    reference_batch = subcommands.add_parser(
        "backfill-reference-batch",
        help="Enumerate persisted reference-universe snapshots and optionally backfill selected OHLCV series IDs.",
    )
    reference_batch.add_argument(
        "--db",
        default=canonical_db_text(),
        help="SQLite database path. Defaults to the canonical universe DB.",
    )
    reference_batch.add_argument(
        "--exchange",
        default="",
        help="Optional primary exchange MIC, for example XNAS.",
    )
    reference_batch.add_argument(
        "--market",
        default="",
        help="Optional persisted market filter, for example stocks.",
    )
    reference_batch.add_argument(
        "--security-type",
        action="append",
        default=[],
        help="Restrict to persisted Massive security type, e.g. CS, ETF, WARRANT. May repeat.",
    )
    reference_batch.add_argument(
        "--cs",
        dest="common_stock",
        action="store_true",
        help="Restrict to common stock (CS).",
    )
    reference_batch.add_argument(
        "--common-stock",
        dest="common_stock",
        action="store_true",
        help="Restrict to common stock (CS).",
    )
    reference_batch.add_argument("--etf", action="store_true", help="Restrict to ETFs.")
    reference_batch.add_argument(
        "--warrant", action="store_true", help="Restrict to warrants."
    )
    reference_batch.add_argument(
        "--unit", action="store_true", help="Restrict to units."
    )
    reference_batch.add_argument(
        "--adrc", action="store_true", help="Restrict to ADR common shares (ADRC)."
    )
    reference_batch.add_argument(
        "--right", action="store_true", help="Restrict to rights."
    )
    reference_batch.add_argument(
        "--preferred", action="store_true", help="Restrict to preferred shares (PFD)."
    )
    reference_batch.add_argument(
        "--fund", action="store_true", help="Restrict to funds."
    )
    reference_batch.add_argument(
        "--active", choices=("active", "inactive", "all"), default="active"
    )
    reference_batch.add_argument(
        "--ohlcv-series-id",
        "--ohlcv_series_id",
        dest="ohlcv_series_id",
        action="append",
        type=int,
        default=[],
        help="Restrict to selected OHLCV series IDs. May repeat.",
    )
    reference_batch.add_argument(
        "--identity-as-of-date",
        default=None,
        help="Use latest DB reference snapshots on or before this date.",
    )
    reference_batch.add_argument(
        "--limit",
        "--page-size",
        dest="limit",
        type=int,
        default=25,
        help=(
            "Bounded selection size. With --all-pages, this is the internal page size."
        ),
    )
    reference_batch.add_argument("--offset", type=int, default=0)
    reference_batch.add_argument(
        "--all-pages",
        action="store_true",
        help=(
            "Internally page through every matching persisted OHLCV series. "
            "--page-size/--limit is used as the page size."
        ),
    )
    reference_batch.add_argument(
        "--api-key",
        default=None,
        help="Massive API key for --commit. Defaults to MASSIVE_API_KEY.",
    )
    reference_batch.add_argument("--base-url", default="https://api.massive.com")
    reference_batch.add_argument(
        "--from-date",
        default=DEFAULT_TICKER_SEED_FROM_DATE,
        help="Start date. Defaults to the first session after the rolling 5-year boundary.",
    )
    reference_batch.add_argument("--to-date", default=None)
    _add_bar_grain_arg(reference_batch)
    reference_batch.add_argument("--max-rounds", type=int, default=DEFAULT_MAX_ROUNDS)
    reference_batch.add_argument("--no-caution", action="store_true")
    reference_batch.add_argument(
        "--commit",
        action="store_true",
        help="Execute selected OHLCV series IDs. Without this, emit a no-write manifest.",
    )
    reference_batch.add_argument(
        "--strict",
        action="store_true",
        help="Report ok=false if any selected OHLCV series ID fails or skips.",
    )
    _add_progress_args(reference_batch)
    reference_batch.set_defaults(func=_backfill_reference_batch)

    catch_up = subcommands.add_parser(
        "catch-up",
        help="Plan or execute a deterministic reference-universe database catch-up.",
    )
    catch_up.add_argument(
        "--db",
        default=canonical_db_text(),
        help="SQLite database path. Defaults to the canonical universe DB.",
    )
    catch_up.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_CATCH_UP_WORKERS,
        help=f"Concurrent workers. Default {DEFAULT_CATCH_UP_WORKERS}; max {MAX_CATCH_UP_WORKERS}.",
    )
    catch_up.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_CATCH_UP_BATCH_SIZE,
        help=f"Materialized targets per batch. Default {DEFAULT_CATCH_UP_BATCH_SIZE}.",
    )
    catch_up.add_argument(
        "--target-limit",
        type=int,
        default=0,
        help="Optional cap on materialized targets for rehearsal or bounded execution.",
    )
    catch_up.add_argument(
        "--stale-before",
        default=None,
        help="Override stale-date classification; defaults to DB global max bar date.",
    )
    catch_up.add_argument(
        "--category",
        action="append",
        choices=sorted(ISSUE_CATEGORIES),
        default=[],
        help="Filter quality categories before selecting executable catch-up targets. May repeat.",
    )
    catch_up.add_argument(
        "--exchange",
        action="append",
        default=[],
        help="Filter by primary exchange MIC. May repeat.",
    )
    catch_up.add_argument(
        "--security-type",
        action="append",
        default=[],
        help="Filter by Massive security type. May repeat.",
    )
    catch_up.add_argument(
        "--ohlcv-series-id",
        "--ohlcv_series_id",
        dest="ohlcv_series_id",
        action="append",
        type=int,
        default=[],
        help="Filter to selected OHLCV series IDs. May repeat.",
    )
    catch_up.add_argument(
        "--ticker",
        action="append",
        default=[],
        help="Filter to selected latest tickers. May repeat.",
    )
    catch_up.add_argument(
        "--from-date",
        default=None,
        help="Override all target start dates. By default stale targets resume from max_bar_date + 1.",
    )
    catch_up.add_argument("--to-date", default=None)
    _add_bar_grain_arg(catch_up)
    catch_up.add_argument(
        "--run-root", default=None, help="Root directory for committed run artifacts."
    )
    catch_up.add_argument(
        "--run-dir", default=None, help="Exact committed run artifact directory."
    )
    catch_up.add_argument(
        "--api-key",
        default=None,
        help="Massive API key for --commit. Defaults to MASSIVE_API_KEY.",
    )
    catch_up.add_argument("--base-url", default="https://api.massive.com")
    catch_up.add_argument("--max-rounds", type=int, default=DEFAULT_MAX_ROUNDS)
    catch_up.add_argument("--no-caution", action="store_true")
    catch_up.add_argument(
        "--commit",
        action="store_true",
        help="Execute the materialized target set. Without this, only emit the plan.",
    )
    catch_up.add_argument(
        "--strict",
        action="store_true",
        help="Return ok=false if any target fails or skips.",
    )
    catch_up.add_argument(
        "--fail-fast",
        action="store_true",
        help="Stop scheduling additional batches after the first target-level failure.",
    )
    catch_up.add_argument(
        "--resume",
        action="store_true",
        help="Reuse completed batch artifacts in --run-dir when the plan hash matches.",
    )
    catch_up.add_argument(
        "--heartbeat-seconds",
        type=int,
        default=60,
        help="Progress heartbeat cadence. Default 60 seconds.",
    )
    catch_up.add_argument(
        "--mini-summary-seconds",
        type=int,
        default=240,
        help="Progress mini-summary cadence. Default 240 seconds.",
    )
    catch_up.add_argument(
        "--summary-seconds",
        type=int,
        default=720,
        help="Progress summary cadence. Default 720 seconds.",
    )
    catch_up.add_argument(
        "--resource-check-seconds",
        type=int,
        default=DEFAULT_RESOURCE_CHECK_SECONDS,
        help="Disk/memory check cadence. Default 600 seconds.",
    )
    catch_up.set_defaults(func=_catch_up, strict_exit=True)

    catch_up_stop = subcommands.add_parser(
        "catch-up-stop",
        help="Request a committed catch-up run to drain in-flight batches and stop.",
    )
    catch_up_stop.add_argument(
        "--run-dir", required=True, help="Committed catch-up run artifact directory."
    )
    catch_up_stop.add_argument(
        "--reason",
        default="operator requested stop",
        help="Reason stored in stop_request.json.",
    )
    catch_up_stop.add_argument(
        "--requested-by",
        default="operator",
        help="Operator or automation identity stored in stop_request.json.",
    )
    catch_up_stop.add_argument(
        "--mode",
        choices=sorted(CATCH_UP_STOP_MODES),
        default=DEFAULT_CATCH_UP_STOP_MODE,
        help="Stop mode: drain finishes in-flight batches, quiesce stops between targets, abort stops before starting another target.",
    )
    catch_up_stop.set_defaults(func=_catch_up_stop)

    catch_up_reconcile = subcommands.add_parser(
        "catch-up-reconcile",
        help="Adopt DB-completed catch-up receipts into recovered artifacts before resume.",
    )
    catch_up_reconcile.add_argument(
        "--run-dir", required=True, help="Committed catch-up run artifact directory."
    )
    catch_up_reconcile.add_argument(
        "--commit",
        action="store_true",
        help="Write recovered artifacts. Without this, emit a no-write reconciliation plan.",
    )
    catch_up_reconcile.set_defaults(func=_catch_up_reconcile, strict_exit=True)

    validate_db = subcommands.add_parser(
        "validate-db", help="Validate SQLite output integrity."
    )
    validate_db.add_argument(
        "--db",
        default=canonical_db_text(),
        help="SQLite database path. Defaults to the canonical universe DB.",
    )
    validate_db.add_argument(
        "--heartbeat-seconds",
        type=int,
        default=60,
        help="Progress heartbeat cadence on stderr. Default 60 seconds.",
    )
    validate_db.add_argument(
        "--summary-seconds",
        type=int,
        default=180,
        help="Progress summary cadence on stderr. Default 180 seconds.",
    )
    validate_db.set_defaults(func=_validate_db)

    universe_status_command = subcommands.add_parser(
        "universe-status",
        help="Report canonical DB universe coverage and completeness.",
    )
    universe_status_command.add_argument(
        "--db",
        default=canonical_db_text(),
        help="SQLite database path. Defaults to the canonical universe DB.",
    )
    universe_status_command.set_defaults(func=_universe_status)

    quality = subcommands.add_parser(
        "quality-audit",
        help="Classify stale, missing-bar, and receipt/accounting issues read-only.",
    )
    _add_quality_audit_args(quality)
    quality.set_defaults(func=_quality_audit)

    repair_receipts = subcommands.add_parser(
        "repair-missing-receipts",
        help="Insert durable error receipts for approvals that have no receipt.",
    )
    repair_receipts.add_argument(
        "--db",
        default=canonical_db_text(),
        help="SQLite database path. Defaults to the canonical universe DB.",
    )
    repair_receipts.add_argument(
        "--ohlcv-series-id",
        "--ohlcv_series_id",
        dest="ohlcv_series_id",
        action="append",
        type=int,
        default=[],
        help="Restrict to selected OHLCV series IDs. May repeat.",
    )
    repair_receipts.add_argument("--limit", type=int, default=50)
    repair_receipts.add_argument(
        "--reason",
        default="quality audit repair for approval without durable execution receipt",
    )
    repair_receipts.add_argument(
        "--commit",
        action="store_true",
        help="Persist repair receipts. Without this, emit a no-write manifest.",
    )
    repair_receipts.set_defaults(func=_repair_missing_receipts)

    audit = subcommands.add_parser(
        "audit-executions", help="Inspect execution receipts and approval links."
    )
    audit.add_argument(
        "--db",
        default=canonical_db_text(),
        help="SQLite database path. Defaults to the canonical universe DB.",
    )
    audit.add_argument("--request-hash", default=None)
    audit.add_argument(
        "--ohlcv-series-id",
        "--ohlcv_series_id",
        dest="ohlcv_series_id",
        type=int,
        default=None,
    )
    audit.add_argument("--limit", type=int, default=20)
    audit.set_defaults(func=_audit_executions)

    doctor = subcommands.add_parser("doctor", help="Check local CLI prerequisites.")
    doctor.add_argument(
        "--db",
        default=canonical_db_text(),
        help="SQLite path to check. Defaults to the canonical universe DB.",
    )
    doctor.add_argument(
        "--api-key", default=None, help="Massive API key. Defaults to MASSIVE_API_KEY."
    )
    doctor.add_argument(
        "--require-entrypoint",
        action="store_true",
        help="Fail if stock-universe is not on PATH.",
    )
    doctor.set_defaults(func=_doctor, strict_exit=True)
    return parser


def _add_quality_audit_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--db",
        default=canonical_db_text(),
        help="SQLite database path. Defaults to the canonical universe DB.",
    )
    parser.add_argument(
        "--stale-before",
        default=None,
        help="Classify covered series with max bar before this date as stale. Defaults to DB global max bar date.",
    )
    _add_bar_grain_arg(parser)
    parser.add_argument(
        "--category",
        action="append",
        choices=sorted(ISSUE_CATEGORIES),
        default=[],
        help="Filter to a quality category. May repeat.",
    )
    parser.add_argument(
        "--exchange",
        action="append",
        default=[],
        help="Filter by primary exchange MIC. May repeat.",
    )
    parser.add_argument(
        "--security-type",
        action="append",
        default=[],
        help="Filter by Massive security type. May repeat.",
    )
    parser.add_argument(
        "--ohlcv-series-id",
        "--ohlcv_series_id",
        dest="ohlcv_series_id",
        action="append",
        type=int,
        default=[],
        help="Filter by OHLCV series ID. May repeat.",
    )
    parser.add_argument(
        "--ticker",
        action="append",
        default=[],
        help="Filter by latest reference ticker. May repeat.",
    )
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument(
        "--include-healthy",
        action="store_true",
        help="Include healthy/recent rows in output.",
    )


def _add_progress_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--heartbeat-seconds",
        type=int,
        default=60,
        help="Progress heartbeat cadence on stderr. Default 60 seconds.",
    )
    parser.add_argument(
        "--summary-seconds",
        type=int,
        default=180,
        help="Progress summary cadence on stderr. Default 180 seconds.",
    )


def _add_bar_grain_arg(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--bar-grain",
        default="1d",
        help="Aggregate bar grain to plan/read/write. Defaults to 1d. Supported values: 1d, 1m, 30m.",
    )


class _CliProgressReporter:
    def __init__(
        self,
        *,
        prefix: str,
        command: str,
        db: str = "",
        heartbeat_seconds: int = 60,
        summary_seconds: int = 180,
        total_inputs: int | None = None,
    ) -> None:
        self.prefix = prefix
        self.command = command
        self.db = db
        self.heartbeat_seconds = max(1, int(heartbeat_seconds))
        self.summary_seconds = max(self.heartbeat_seconds, int(summary_seconds))
        self.started_at = time.monotonic()
        self.total_inputs = total_inputs

    def emit(self, event_type: str, message: str, **fields: Any) -> None:
        base: dict[str, Any] = {}
        if self.db:
            base["db"] = self.db
        if self.total_inputs is not None:
            base["total_inputs"] = self.total_inputs
        base.update(fields)
        emit_cli_progress(
            self.prefix,
            command=self.command,
            event_type=event_type,
            message=message,
            started_at=self.started_at,
            **base,
        )

    def wait_for(
        self,
        future: concurrent.futures.Future[Any],
        *,
        heartbeat_message: str,
        summary_message: str,
        counts: Any,
    ) -> Any:
        next_heartbeat_at = time.monotonic() + self.heartbeat_seconds
        next_summary_at = time.monotonic() + self.summary_seconds
        while not future.done():
            now = time.monotonic()
            timeout = max(min(next_heartbeat_at, next_summary_at) - now, 0.05)
            try:
                return future.result(timeout=timeout)
            except concurrent.futures.TimeoutError:
                pass
            now = time.monotonic()
            if now >= next_heartbeat_at and not future.done():
                self.emit("heartbeat", heartbeat_message, counts=counts())
                next_heartbeat_at = now + self.heartbeat_seconds
            if now >= next_summary_at and not future.done():
                self.emit("summary", summary_message, counts=counts())
                next_summary_at = now + self.summary_seconds
        return future.result()


def _validate_progress_args(args: argparse.Namespace) -> None:
    if args.heartbeat_seconds < 1:
        raise CliError("--heartbeat-seconds must be positive")
    if args.summary_seconds < args.heartbeat_seconds:
        raise CliError(
            "--summary-seconds must be greater than or equal to --heartbeat-seconds"
        )


def _validate_bar_grain_arg(args: argparse.Namespace) -> None:
    try:
        normalize_bar_grain(getattr(args, "bar_grain", "1d"))
    except ValueError as exc:
        raise CliError(str(exc)) from exc


def _execute_progress_work_items(
    progress: _CliProgressReporter,
    work_items: list[tuple[str, str, Any]],
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    progress.emit(
        "started", f"{progress.command} started", counts=_result_counts(results)
    )
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        for index, (input_type, input_value, work) in enumerate(work_items, start=1):
            progress.emit(
                "input_started",
                "input execution started",
                current_input={
                    "index": index,
                    "type": input_type,
                    "value": input_value,
                },
                counts=_result_counts(results),
            )
            future = pool.submit(work)
            result = progress.wait_for(
                future,
                heartbeat_message="input execution still running",
                summary_message="input execution summary",
                counts=lambda: _result_counts(results),
            )
            results.append(result)
            progress.emit(
                "input_finished",
                "input execution finished",
                current_input={
                    "index": index,
                    "type": input_type,
                    "value": input_value,
                },
                result_status=result.get("status"),
                counts=_result_counts(results),
            )
    return results


def _result_counts(results: list[dict[str, Any]]) -> dict[str, int]:
    return {
        "attempted": len(results),
        "ok": sum(1 for item in results if item.get("status") == "ok"),
        "skipped": sum(1 for item in results if item.get("status") == "skipped"),
        "error": sum(1 for item in results if item.get("status") == "error"),
        "fetched_bars": sum(
            int(item.get("fetched_bar_count") or 0) for item in results
        ),
        "inserted_bars": sum(
            int(item.get("inserted_bar_count") or 0) for item in results
        ),
    }


def _identity_search(
    args: argparse.Namespace, parser: argparse.ArgumentParser
) -> dict[str, Any]:
    query = args.query_option or args.query_arg
    if not query:
        raise CliError("identity-search requires a query")
    if args.limit < 1:
        raise CliError("--limit must be positive")
    if args.source == "db":
        if not args.db:
            raise CliError("--db is required for --source db")
        try:
            result = sqlite_identity_search(args.db, query, limit=args.limit)
        except sqlite3.Error as exc:
            raise CliError(f"SQLite identity search failed: {exc}") from exc
        payload = result.to_dict()
        payload.update(
            {
                "ok": True,
                "command": "stock-universe identity-search",
                "effects": {
                    "will_read": [args.db],
                    "will_write": [],
                    "did_write": [],
                },
            }
        )
        return payload
    api_key = _api_key(args, parser)
    capture_dir = Path(args.capture_dir) if args.capture_dir else None
    client = MassiveReadOnlyClient(
        MassiveProviderConfig(api_key=api_key, base_url=args.base_url),
        raw_capture_dir=capture_dir,
    )
    result = live_identity_search(
        query, client=client, as_of_date=args.as_of_date, limit=args.limit
    )
    payload = result.to_dict()
    payload.update(
        {
            "ok": True,
            "command": "stock-universe identity-search",
            "effects": {
                "will_read": _identity_search_live_reads(args),
                "will_write": [f"{args.capture_dir}/*.json"]
                if args.capture_dir
                else [],
                "did_write": _captured_raw_files(capture_dir),
            },
            "request_log": _request_log(client),
        }
    )
    return payload


def _update_reference_universe(
    args: argparse.Namespace, parser: argparse.ArgumentParser
) -> dict[str, Any]:
    _validate_progress_args(args)
    api_key = _api_key(args, parser)
    capture_dir = Path(args.capture_dir) if args.capture_dir else None
    client = MassiveReadOnlyClient(
        MassiveProviderConfig(api_key=api_key, base_url=args.base_url),
        raw_capture_dir=capture_dir,
    )
    progress: _CliProgressReporter | None = None
    try:
        request = ReferenceUniverseRequest(
            market=args.market,
            exchange=args.exchange,
            as_of_date=args.as_of_date or "",
            active=_reference_active_value(args.active),
            limit=args.limit,
            max_pages=args.max_pages,
        )
        progress = _CliProgressReporter(
            prefix="update-reference-universe progress: ",
            command="stock-universe update-reference-universe",
            db=args.db,
            heartbeat_seconds=args.heartbeat_seconds,
            summary_seconds=args.summary_seconds,
        )
        progress.emit(
            "started",
            "reference-universe update started",
            commit=bool(args.commit),
            limit=args.limit,
            max_pages=args.max_pages,
        )
        fetch_counts = {"page_count": 0, "fetched_count": 0}

        def fetch_progress(event: dict[str, Any]) -> None:
            fetch_counts["page_count"] = int(
                event.get("page_count") or fetch_counts["page_count"]
            )
            fetch_counts["fetched_count"] = int(
                event.get("fetched_count") or fetch_counts["fetched_count"]
            )
            progress.emit(
                str(event.get("event_type") or "page_fetched"),
                str(event.get("message") or "reference page fetched"),
                **{
                    key: value
                    for key, value in event.items()
                    if key not in {"event_type", "message"}
                },
            )

        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(
                fetch_massive_reference_universe,
                client,
                request,
                progress_sink=fetch_progress,
            )
            update = progress.wait_for(
                future,
                heartbeat_message="reference-universe update still running",
                summary_message="reference-universe update summary",
                counts=lambda: dict(fetch_counts),
            )
    except ValueError as exc:
        if progress is not None:
            progress.emit(
                "error",
                "reference-universe update failed",
                error_type=exc.__class__.__name__,
                error=str(exc),
            )
        raise CliError(str(exc)) from exc

    repository = SQLiteStockUniverseRepository(args.db)
    validation = None
    upserted_count = 0
    did_write = _captured_raw_files(capture_dir)
    if args.commit:
        progress.emit(
            "summary",
            "persisting reference-universe update",
            fetched_count=len(update.snapshots),
            page_count=update.page_count,
        )
        repository.ensure_schema()
        upserted_count = repository.upsert_reference_snapshots(update.snapshots)
        reference_update_record = repository.insert_reference_universe_update(
            update, request_log=_request_log(client)
        )
        validation = repository.validate()
        did_write = [str(Path(args.db))] + did_write

    payload: dict[str, Any] = {
        "ok": validation.ok if validation is not None else True,
        "command": "stock-universe update-reference-universe",
        "db": str(Path(args.db)),
        "dry_run": not args.commit,
        "complete": update.complete,
        "fetched_count": len(update.snapshots),
        "page_count": update.page_count,
        "pending_requests": list(update.pending_requests),
        "request": update.request.to_dict(),
        "snapshot_preview": _reference_snapshot_preview(update.snapshots),
        "effects": {
            "will_read": _reference_universe_reads(args),
            "will_write": _reference_universe_writes(args),
            "did_write": did_write,
        },
        "request_log": _request_log(client),
    }
    if args.commit:
        payload["upserted_count"] = upserted_count
        payload["reference_update"] = reference_update_record
        payload["counts"] = repository.counts()
        payload["validation"] = _validation_payload(validation)
    progress.emit(
        "finished",
        "reference-universe update finished",
        commit=bool(args.commit),
        fetched_count=len(update.snapshots),
        page_count=update.page_count,
        upserted_count=upserted_count,
        ok=payload["ok"],
    )
    return payload


def _dry_run(
    args: argparse.Namespace, parser: argparse.ArgumentParser
) -> dict[str, Any]:
    _validate_bar_grain_arg(args)
    capture_dir = Path(args.capture_dir) if args.capture_dir else None
    client = None
    selected_identity = None
    if args.ticker:
        api_key = _api_key(args, parser)
        try:
            source, client = massive_live_source_from_ticker(
                args.ticker,
                api_key=api_key,
                base_url=args.base_url,
                db_path=args.db,
                require_existing_identity=True,
                from_date=args.from_date,
                to_date=args.to_date,
                bar_grain=args.bar_grain,
                capture_dir=capture_dir,
            )
        except ValueError as exc:
            raise CliError(str(exc)) from exc
    elif args.ohlcv_series_id is not None:
        if not args.db:
            raise CliError("--db is required for --ohlcv-series-id")
        api_key = _api_key(args, parser)
        try:
            source, client, snapshot = massive_live_source_from_series_id(
                args.db,
                args.ohlcv_series_id,
                api_key=api_key,
                base_url=args.base_url,
                from_date=args.from_date,
                to_date=args.to_date,
                bar_grain=args.bar_grain,
                as_of_date=args.identity_as_of_date,
                capture_dir=capture_dir,
            )
        except (sqlite3.Error, ValueError) as exc:
            raise CliError(str(exc)) from exc
        selected_identity = _reference_snapshot_identity_payload(snapshot)
    trace = run_backfill_source_dry_run_trace(source, max_rounds=args.max_rounds)
    envelope = result_envelope(
        f"stock-universe dry-run:{_dry_run_command_suffix(args)}",
        trace.result,
    )
    envelope["effects"] = {
        "will_read": _planned_reads(args),
        "will_write": _planned_writes(args),
        "did_write": _captured_raw_files(capture_dir),
    }
    envelope["rounds"] = _rounds(trace.rounds)
    if client is not None:
        envelope["request_log"] = _request_log(client)
    if selected_identity is not None:
        envelope["selected_identity"] = selected_identity
    if isinstance(trace.result, BackfillPlan):
        _write_optional_plan_outputs(trace.result, args, envelope)
    return envelope


def _dry_run_command_suffix(args: argparse.Namespace) -> str:
    if args.ticker:
        return "live-ticker"
    if args.ohlcv_series_id is not None:
        return "live-ohlcv-series-id"
    return "live"


def _backfill(
    args: argparse.Namespace, parser: argparse.ArgumentParser
) -> dict[str, Any]:
    if not args.ticker and not args.ohlcv_series_id:
        raise CliError(
            "backfill requires at least one --ticker or --ohlcv-series-id"
        )
    _validate_bar_grain_arg(args)
    _validate_progress_args(args)
    api_key = _api_key(args, parser)
    repository = SQLiteStockUniverseRepository(args.db)
    repository.ensure_schema()
    work_items: list[tuple[str, str, Any]] = []
    work_items.extend(
        (
            "ticker",
            str(ticker),
            lambda ticker=ticker: _execute_one_ticker(
                ticker, args, api_key, repository
            ),
        )
        for ticker in args.ticker
    )
    work_items.extend(
        (
            "ohlcv_series_id",
            str(series_id),
            lambda series_id=series_id: _execute_one_series_id(
                series_id, args, api_key, repository
            ),
        )
        for series_id in args.ohlcv_series_id
    )
    progress = _CliProgressReporter(
        prefix="backfill progress: ",
        command="stock-universe backfill",
        db=args.db,
        heartbeat_seconds=args.heartbeat_seconds,
        summary_seconds=args.summary_seconds,
        total_inputs=len(work_items),
    )
    results = _execute_progress_work_items(progress, work_items)
    progress.emit(
        "summary", "validating backfill database", counts=_result_counts(results)
    )
    validation = repository.validate()
    summary = {
        "ok": validation.ok and all(item["status"] == "ok" for item in results),
        "db": str(Path(args.db)),
        "attempted": len(results),
        "ok_count": sum(1 for item in results if item["status"] == "ok"),
        "skipped_count": sum(1 for item in results if item["status"] == "skipped"),
        "error_count": sum(1 for item in results if item["status"] == "error"),
        "counts": repository.counts(),
        "validation": _validation_payload(validation),
        "results": results,
    }
    if args.strict:
        summary["strict"] = True
    progress.emit(
        "finished",
        "backfill finished",
        counts=_result_counts(results),
        ok=summary["ok"],
    )
    return summary


def _backfill_reference_batch(
    args: argparse.Namespace, parser: argparse.ArgumentParser
) -> dict[str, Any]:
    _validate_progress_args(args)
    _validate_bar_grain_arg(args)
    if args.limit < 1 or args.limit > 1000:
        raise CliError("--limit must be between 1 and 1000")
    if args.offset < 0:
        raise CliError("--offset must be non-negative")
    active = _reference_active_value(args.active)
    security_types = _reference_batch_security_types(args)
    repository = SQLiteStockUniverseRepository(args.db)
    try:
        pages, total_available = _reference_batch_pages(
            repository=repository,
            args=args,
            security_types=security_types,
            active=active,
        )
    except ValueError as exc:
        raise CliError(str(exc)) from exc
    snapshots = tuple(snapshot for page in pages for snapshot in page["snapshots"])
    pending_count = (
        0 if args.all_pages else max(total_available - args.offset - len(snapshots), 0)
    )
    pending_preview: tuple[Any, ...] = ()
    if pending_count:
        pending_preview, _ = repository.reference_snapshots_for_batch(
            exchange=args.exchange,
            market=args.market,
            security_types=security_types,
            active=active,
            as_of_date=args.identity_as_of_date,
            series_ids=args.ohlcv_series_id,
            limit=min(args.limit, 20),
            offset=args.offset + len(snapshots),
        )
    results: list[dict[str, Any]] = []
    validation = None
    if args.commit and snapshots:
        api_key = _api_key(args, parser)
        repository.ensure_schema()
        progress = _CliProgressReporter(
            prefix="backfill-reference-batch progress: ",
            command="stock-universe backfill-reference-batch",
            db=args.db,
            heartbeat_seconds=args.heartbeat_seconds,
            summary_seconds=args.summary_seconds,
            total_inputs=len(snapshots),
        )
        if args.all_pages:
            results = _execute_reference_batch_pages(
                progress=progress,
                pages=pages,
                args=args,
                api_key=api_key,
                repository=repository,
            )
        else:
            work_items = [
                (
                    "ohlcv_series_id",
                    str(snapshot.ohlcv_series_id),
                    lambda snapshot=snapshot: _execute_one_series_id(
                        snapshot.ohlcv_series_id, args, api_key, repository
                    ),
                )
                for snapshot in snapshots
            ]
            results = _execute_progress_work_items(progress, work_items)
        progress.emit(
            "summary",
            "validating reference-batch database",
            counts=_result_counts(results),
            page_count=len(pages),
        )
        validation = repository.validate()
        final_counts = _result_counts(results)
        progress.emit(
            "finished",
            "reference-batch backfill finished",
            counts=final_counts,
            page_count=len(pages),
            ok=validation.ok
            and final_counts["skipped"] == 0
            and final_counts["error"] == 0,
        )
    elif args.commit:
        progress = _CliProgressReporter(
            prefix="backfill-reference-batch progress: ",
            command="stock-universe backfill-reference-batch",
            db=args.db,
            heartbeat_seconds=args.heartbeat_seconds,
            summary_seconds=args.summary_seconds,
            total_inputs=0,
        )
        progress.emit(
            "started",
            "reference-batch backfill started",
            counts=_result_counts(results),
            page_count=len(pages),
        )
        progress.emit(
            "finished",
            "reference-batch backfill finished",
            counts=_result_counts(results),
            page_count=len(pages),
            ok=False,
        )

    counts = {
        "total_available": total_available,
        "selected": len(snapshots),
        "pending": pending_count,
        "offset": args.offset,
        "limit": args.limit,
        "all_pages": bool(args.all_pages),
        "page_count": len(pages),
        "ok": sum(1 for item in results if item["status"] == "ok"),
        "skipped": sum(1 for item in results if item["status"] == "skipped"),
        "error": sum(1 for item in results if item["status"] == "error"),
    }
    ok = bool(snapshots)
    if args.commit:
        ok = (
            bool(snapshots)
            and validation is not None
            and validation.ok
            and counts["skipped"] == 0
            and counts["error"] == 0
        )
    payload: dict[str, Any] = {
        "ok": ok,
        "command": "stock-universe backfill-reference-batch",
        "schema_version": "stock_universe.reference_batch_manifest.v1",
        "db": str(Path(args.db)),
        "dry_run": not args.commit,
        "commit": bool(args.commit),
        "selection": _reference_batch_selection_payload(args),
        "counts": counts,
        "selected_ohlcv_series_ids": [
            snapshot.ohlcv_series_id for snapshot in snapshots
        ],
        "selected_snapshots": [
            _reference_snapshot_identity_payload(snapshot) for snapshot in snapshots
        ],
        "pages": _reference_batch_page_summaries(pages),
        "pending_items": [
            _reference_snapshot_identity_payload(snapshot)
            for snapshot in pending_preview
        ],
        "effects": {
            "will_read": _reference_batch_reads(args),
            "will_write": _reference_batch_writes(args),
            "did_write": [str(Path(args.db))] if args.commit and snapshots else [],
        },
        "next_action": _reference_batch_next_action(
            args, snapshots=snapshots, pending_count=pending_count, results=results
        ),
        "next_actions": _reference_batch_next_actions(
            args, snapshots=snapshots, pending_count=pending_count, results=results
        ),
        "repair_hints": _reference_batch_repair_hints(
            args, snapshots=snapshots, results=results
        ),
    }
    if args.commit:
        payload["results"] = results
    if validation is not None:
        payload["validation"] = _validation_payload(validation)
        payload["db_counts"] = repository.counts()
    return payload


def _reference_batch_pages(
    *,
    repository: SQLiteStockUniverseRepository,
    args: argparse.Namespace,
    security_types: tuple[str, ...],
    active: bool | None,
) -> tuple[list[dict[str, Any]], int]:
    pages: list[dict[str, Any]] = []
    offset = args.offset
    total_available = 0
    while True:
        snapshots, page_total = repository.reference_snapshots_for_batch(
            exchange=args.exchange,
            market=args.market,
            security_types=security_types,
            active=active,
            as_of_date=args.identity_as_of_date,
            series_ids=args.ohlcv_series_id,
            limit=args.limit,
            offset=offset,
        )
        if not pages:
            total_available = page_total
        if not snapshots:
            break
        pages.append(
            {
                "page_index": len(pages) + 1,
                "offset": offset,
                "limit": args.limit,
                "snapshots": tuple(snapshots),
            }
        )
        if not args.all_pages:
            break
        offset += len(snapshots)
        if offset >= page_total:
            break
    return pages, total_available


def _reference_batch_page_summary(
    page: dict[str, Any], *, include_ids: bool = True
) -> dict[str, Any]:
    snapshots = tuple(page["snapshots"])
    ohlcv_series_ids = [snapshot.ohlcv_series_id for snapshot in snapshots]
    tickers = [snapshot.ticker for snapshot in snapshots]
    summary = {
        "page_index": page["page_index"],
        "offset": page["offset"],
        "limit": page["limit"],
        "selected": len(snapshots),
        "tickers_preview": tickers[:10],
        "first_ohlcv_series_id": ohlcv_series_ids[0] if ohlcv_series_ids else None,
        "last_ohlcv_series_id": ohlcv_series_ids[-1] if ohlcv_series_ids else None,
    }
    if include_ids:
        summary["ohlcv_series_ids"] = ohlcv_series_ids
    else:
        summary["ohlcv_series_id_count"] = len(ohlcv_series_ids)
    return summary


def _reference_batch_page_summaries(
    pages: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    return [_reference_batch_page_summary(page) for page in pages]


def _execute_reference_batch_pages(
    *,
    progress: _CliProgressReporter,
    pages: list[dict[str, Any]],
    args: argparse.Namespace,
    api_key: str,
    repository: SQLiteStockUniverseRepository,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    progress.emit(
        "started",
        "reference-batch all-pages backfill started",
        counts=_result_counts(results),
        page_count=len(pages),
    )
    current_index = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        for page in pages:
            page_summary = _reference_batch_page_summary(page, include_ids=False)
            progress.emit(
                "page_started",
                "reference-batch page started",
                page=page_summary,
                counts=_result_counts(results),
            )
            for snapshot in page["snapshots"]:
                current_index += 1
                progress.emit(
                    "input_started",
                    "input execution started",
                    current_input={
                        "index": current_index,
                        "type": "ohlcv_series_id",
                        "value": str(snapshot.ohlcv_series_id),
                    },
                    page={
                        "page_index": page["page_index"],
                        "offset": page["offset"],
                        "limit": page["limit"],
                    },
                    counts=_result_counts(results),
                )
                future = pool.submit(
                    _execute_one_series_id,
                    snapshot.ohlcv_series_id,
                    args,
                    api_key,
                    repository,
                )
                result = progress.wait_for(
                    future,
                    heartbeat_message="input execution still running",
                    summary_message="input execution summary",
                    counts=lambda: _result_counts(results),
                )
                results.append(result)
                progress.emit(
                    "input_finished",
                    "input execution finished",
                    current_input={
                        "index": current_index,
                        "type": "ohlcv_series_id",
                        "value": str(snapshot.ohlcv_series_id),
                    },
                    page={
                        "page_index": page["page_index"],
                        "offset": page["offset"],
                        "limit": page["limit"],
                    },
                    result_status=result.get("status"),
                    counts=_result_counts(results),
                )
            progress.emit(
                "page_finished",
                "reference-batch page finished",
                page=page_summary,
                counts=_result_counts(results),
            )
    return results


_HARD_CATCH_UP_ERROR_TYPES = {
    "DatabaseError",
    "DataError",
    "IntegrityError",
    "InternalError",
    "NotSupportedError",
    "OperationalError",
    "PermissionError",
    "ProgrammingError",
}


def _catch_up(
    args: argparse.Namespace, parser: argparse.ArgumentParser
) -> dict[str, Any]:
    try:
        _validate_catch_up_args(args)
        if args.commit and args.run_dir and (Path(args.run_dir) / "plan.json").exists():
            plan = catch_up_plan_from_run_dir(args.run_dir)
        else:
            plan = build_catch_up_plan(
                args.db,
                workers=args.workers,
                batch_size=args.batch_size,
                stale_before=args.stale_before,
                categories=tuple(args.category),
                exchanges=tuple(args.exchange),
                security_types=tuple(args.security_type),
                series_ids=tuple(args.ohlcv_series_id),
                tickers=tuple(args.ticker),
                target_limit=args.target_limit,
                from_date=args.from_date,
                to_date=args.to_date,
                run_root=args.run_root,
                run_dir=args.run_dir,
                bar_grain=args.bar_grain,
            )
    except (OSError, sqlite3.Error, ValueError) as exc:
        raise CliError(str(exc)) from exc

    if not args.commit:
        payload = plan.to_dict()
        payload.update(
            {
                "ok": True,
                "command": "stock-universe catch-up",
                "result_type": "CatchUpPlan",
                "dry_run": True,
                "commit": False,
                "effects": {
                    "will_read": [args.db],
                    "will_write": [],
                    "did_write": [],
                },
            }
        )
        return payload

    api_key = _api_key(args, parser)

    def execute_target(target: Any) -> dict[str, Any]:
        target_args = argparse.Namespace(**vars(args))
        target_args.db = plan.db
        target_args.from_date = target.from_date
        target_args.to_date = args.to_date or plan.target_policy.get("to_date") or None
        target_args.bar_grain = target.bar_grain
        target_args.identity_as_of_date = target.snapshot_as_of_date
        repository = SQLiteStockUniverseRepository(plan.db)
        result = _execute_one_series_id(
            target.ohlcv_series_id, target_args, api_key, repository
        )
        if (
            result.get("status") == "error"
            and str(result.get("error_type") or "") in _HARD_CATCH_UP_ERROR_TYPES
        ):
            raise CatchUpHardTargetError(
                series_id=target.ohlcv_series_id,
                error_type=str(result.get("error_type") or ""),
                error=str(result.get("error") or ""),
            )
        return result

    signal_stop: dict[str, Any] = {"count": 0, "signum": 0}
    previous_signal_handlers = _install_catch_up_signal_handlers(signal_stop)
    try:
        return execute_catch_up_plan(
            plan,
            execute_target=execute_target,
            strict=args.strict,
            fail_fast=args.fail_fast,
            resume=args.resume,
            heartbeat_seconds=args.heartbeat_seconds,
            mini_summary_seconds=args.mini_summary_seconds,
            summary_seconds=args.summary_seconds,
            resource_check_seconds=args.resource_check_seconds,
            progress_sink=_catch_up_progress_sink,
            stop_probe=lambda active_plan: _catch_up_signal_stop_request(
                signal_stop, active_plan
            ),
        )
    except (sqlite3.Error, ValueError) as exc:
        return _catch_up_hard_error_payload(
            args, str(exc), error_type=exc.__class__.__name__
        )
    finally:
        _restore_signal_handlers(previous_signal_handlers)


def _catch_up_stop(
    args: argparse.Namespace, parser: argparse.ArgumentParser
) -> dict[str, Any]:
    try:
        stop_request = request_catch_up_stop(
            args.run_dir,
            reason=args.reason,
            requested_by=args.requested_by,
            mode=args.mode,
        )
    except (OSError, ValueError) as exc:
        raise CliError(str(exc)) from exc
    return {
        "ok": True,
        "command": "stock-universe catch-up-stop",
        "result_type": "CatchUpStopRequest",
        "run_dir": args.run_dir,
        "stop_request": stop_request,
        "effects": {
            "will_read": [args.run_dir],
            "will_write": [str(Path(args.run_dir) / "stop_request.json")],
            "did_write": [str(Path(args.run_dir) / "stop_request.json")],
        },
        "next_actions": [
            {
                "name": "inspect-catch-up-status",
                "kind": "command",
                "command": {
                    "name": "xctx catch-up-status",
                    "description": "Confirm the running catch-up observed the stop request and drained in-flight work.",
                    "args": {"run_dir": args.run_dir},
                    "reads": [args.run_dir],
                    "writes": [],
                },
                "effects": [
                    {
                        "kind": "read",
                        "target": args.run_dir,
                        "description": "Read catch-up stop/drain status.",
                    }
                ],
                "requires_approval": False,
                "reason": "The stop request is cooperative; the active runner records completion according to the requested mode.",
            }
        ],
    }


def _catch_up_reconcile(
    args: argparse.Namespace, parser: argparse.ArgumentParser
) -> dict[str, Any]:
    try:
        return reconcile_catch_up_run(args.run_dir, commit=args.commit)
    except (OSError, sqlite3.Error, ValueError) as exc:
        return {
            "ok": False,
            "command": "stock-universe catch-up-reconcile",
            "result_type": "RepairError",
            "run_dir": args.run_dir,
            "errors": [
                {
                    "code": "catch_up_reconcile_failed",
                    "what_failed": "catch-up reconciliation could not inspect or write artifacts",
                    "minimal_fix": "Inspect xctx catch-up-status, validate the DB, and retry with an existing run directory.",
                    "detail": str(exc),
                }
            ],
            "effects": {
                "will_read": [args.run_dir],
                "will_write": [args.run_dir] if args.commit else [],
                "did_write": [],
            },
        }


def _install_catch_up_signal_handlers(signal_stop: dict[str, Any]) -> dict[int, Any]:
    previous: dict[int, Any] = {}

    def request_stop(signum: int, frame: Any) -> None:
        signal_stop["count"] = int(signal_stop.get("count") or 0) + 1
        signal_stop["signum"] = int(signum)
        if int(signal_stop["count"]) > 1:
            raise KeyboardInterrupt

    for signum in (signal.SIGINT, signal.SIGTERM):
        try:
            previous[int(signum)] = signal.getsignal(signum)
            signal.signal(signum, request_stop)
        except (OSError, RuntimeError, ValueError):
            continue
    return previous


def _restore_signal_handlers(previous_signal_handlers: dict[int, Any]) -> None:
    for signum, handler in previous_signal_handlers.items():
        try:
            signal.signal(signum, handler)
        except (OSError, RuntimeError, ValueError):
            continue


def _catch_up_signal_stop_request(
    signal_stop: dict[str, Any], plan: Any
) -> dict[str, Any] | None:
    signum = int(signal_stop.get("signum") or 0)
    if signum == 0:
        return None
    try:
        signal_name = signal.Signals(signum).name
    except ValueError:
        signal_name = f"signal {signum}"
    return {
        "reason": f"{signal_name} received; drain in-flight catch-up batches and stop",
        "requested_by": "process_signal",
        "mode": DEFAULT_CATCH_UP_STOP_MODE,
    }


def _validate_catch_up_args(args: argparse.Namespace) -> None:
    normalize_bar_grain(args.bar_grain)
    if args.workers < 1 or args.workers > MAX_CATCH_UP_WORKERS:
        raise ValueError(f"--workers must be between 1 and {MAX_CATCH_UP_WORKERS}")
    if args.batch_size < 1 or args.batch_size > 1000:
        raise ValueError("--batch-size must be between 1 and 1000")
    if args.target_limit < 0:
        raise ValueError("--target-limit must be non-negative")
    if args.resume and not args.run_dir:
        raise ValueError("--resume requires --run-dir")
    if args.heartbeat_seconds < 1:
        raise ValueError("--heartbeat-seconds must be positive")
    if args.mini_summary_seconds < args.heartbeat_seconds:
        raise ValueError(
            "--mini-summary-seconds must be greater than or equal to --heartbeat-seconds"
        )
    if args.summary_seconds < args.mini_summary_seconds:
        raise ValueError(
            "--summary-seconds must be greater than or equal to --mini-summary-seconds"
        )
    if args.resource_check_seconds < 1:
        raise ValueError("--resource-check-seconds must be positive")


def _catch_up_progress_sink(event: dict[str, Any]) -> None:
    line = {
        "command": "stock-universe catch-up",
        "event_type": event.get("event_type"),
        "message": event.get("message"),
        "elapsed_seconds": event.get("elapsed_seconds"),
        "completed": (event.get("counts") or {}).get("completed"),
        "pending": (event.get("counts") or {}).get("pending"),
        "errors": (event.get("counts") or {}).get("error"),
        "run_dir": event.get("run_dir"),
    }
    if event.get("resource_check"):
        disk = (event.get("resource_check") or {}).get("disk") or {}
        memory = (event.get("resource_check") or {}).get("memory") or {}
        line["disk_status"] = disk.get("status")
        line["disk_free_gb"] = disk.get("min_free_gb")
        line["memory_available_gb"] = memory.get("available_gb")
    emit_stderr_progress("catch-up progress: ", line)


def _catch_up_hard_error_payload(
    args: argparse.Namespace, error: str, *, error_type: str
) -> dict[str, Any]:
    return {
        "ok": False,
        "command": "stock-universe catch-up",
        "result_type": "RepairError",
        "hard_error": {
            "error_type": error_type,
            "error": error,
            "run_dir": args.run_dir or "",
            "db": args.db,
        },
        "errors": [
            {
                "code": "catch_up_hard_error",
                "what_failed": "catch-up stopped before execution completed",
                "minimal_fix": "Inspect xctx catch-up-status for a run directory when available, validate the DB, then resume only after the cause is fixed.",
                "detail": error,
            }
        ],
        "effects": {
            "will_read": [args.db],
            "will_write": [args.db] if args.commit else [],
            "did_write": [],
        },
        "next_actions": [
            {
                "name": "validate-db",
                "kind": "command",
                "command": {
                    "name": "stock-universe validate-db",
                    "description": "Validate DB integrity before retrying catch-up.",
                    "args": _db_arg_if_override(args.db),
                    "reads": [args.db],
                    "writes": [args.db],
                },
                "effects": [
                    {
                        "kind": "read",
                        "target": args.db,
                        "description": "Read SQLite integrity state.",
                    }
                ],
                "requires_approval": True,
                "reason": error,
            }
        ],
    }


def _approved_execution_payload(
    result: BackfillPlan,
    repository: SQLiteStockUniverseRepository,
    *,
    reason: str,
) -> tuple[ExecutionApproval, dict[str, Any]]:
    approval = ExecutionApproval(
        request_hash=result.request.request_hash,
        allow_caution=result.status == "caution",
        approved_by="stock-universe backfill",
    )
    approval_record = repository.insert_execution_approval(
        result,
        approval,
        reason=reason,
    )
    return approval, approval_record


def _execute_one_ticker(
    ticker: str,
    args: argparse.Namespace,
    api_key: str,
    repository: SQLiteStockUniverseRepository,
) -> dict[str, Any]:
    try:
        source, client = massive_live_source_from_ticker(
            ticker,
            api_key=api_key,
            base_url=args.base_url,
            db_path=args.db,
            allocate_identity=True,
            from_date=args.from_date,
            to_date=args.to_date,
            bar_grain=args.bar_grain,
        )
        trace = run_backfill_source_dry_run_trace(source, max_rounds=args.max_rounds)
        result = trace.result
        if not isinstance(result, BackfillPlan):
            return {
                "ticker": ticker,
                "status": "skipped",
                "reason": "planner returned EvidenceNeeded",
                "requests": [request.to_payload() for request in result.requests],
                "rounds": _rounds(trace.rounds),
                "planning_request_count": len(client.request_log),
            }
        if result.status == "blocked":
            return {
                "ticker": ticker,
                "status": "skipped",
                "ohlcv_series_id": result.target.ohlcv_series_id,
                "plan_status": result.status,
                "reason": "blocked plans are not executable",
            }
        if result.status == "caution" and args.no_caution:
            return {
                "ticker": ticker,
                "status": "skipped",
                "ohlcv_series_id": result.target.ohlcv_series_id,
                "plan_status": result.status,
                "reason": "caution plan skipped by --no-caution",
            }
        approval, approval_record = _approved_execution_payload(
            result,
            repository,
            reason="ticker-seeded CLI backfill approval",
        )
        receipt = execute_live_bar_backfill(
            result,
            approval,
            client,
            repository,
            evidence_facts=source.base_facts
            + tuple(fact for item in trace.rounds for fact in item.collected_facts),
        )
        return _live_execution_result_payload(
            {
                "ticker": ticker,
            },
            result=result,
            receipt=receipt,
            approval_record=approval_record,
            planning_rounds=_rounds(trace.rounds),
        )
    except Exception as exc:
        return {
            "ticker": ticker,
            "status": "error",
            "error_type": exc.__class__.__name__,
            "error": str(exc),
        }


def _live_execution_result_payload(
    base_payload: dict[str, Any],
    *,
    result: BackfillPlan,
    receipt: Any,
    approval_record: dict[str, Any],
    planning_rounds: list[dict[str, Any]],
) -> dict[str, Any]:
    status = str(getattr(receipt, "status", "") or ("ok" if receipt.ok else "error"))
    payload = base_payload | {
        "status": status,
        "ohlcv_series_id": result.target.ohlcv_series_id,
        "latest_ticker": result.target.latest_ticker,
        "bar_grain": normalize_bar_grain(
            multiplier=result.request.multiplier, timespan=result.request.timespan
        ).bar_grain,
        "multiplier": result.request.multiplier,
        "timespan": result.request.timespan,
        "composite_figi": result.target.composite_figi,
        "share_class_figi": result.target.share_class_figi,
        "plan_status": result.status,
        "segments": [segment.to_payload() for segment in result.segments],
        "fetched_bar_count": receipt.fetched_bar_count,
        "inserted_bar_count": receipt.inserted_bar_count,
        "approval_hash": approval_record["approval_hash"],
        "request_count": len(receipt.request_log),
        "planning_rounds": planning_rounds,
    }
    if status == "skipped":
        payload["reason"] = (
            getattr(receipt, "skip_reason", "") or "provider skipped execution"
        )
        if getattr(receipt, "provider_status", ""):
            payload["provider_status"] = receipt.provider_status
        if getattr(receipt, "error_type", ""):
            payload["error_type"] = receipt.error_type
        if getattr(receipt, "error_message", ""):
            payload["message"] = receipt.error_message
    return payload


def _execute_one_series_id(
    series_id: int,
    args: argparse.Namespace,
    api_key: str,
    repository: SQLiteStockUniverseRepository,
) -> dict[str, Any]:
    try:
        source, client, snapshot = massive_live_source_from_series_id(
            args.db,
            series_id,
            api_key=api_key,
            base_url=args.base_url,
            from_date=args.from_date,
            to_date=args.to_date,
            bar_grain=args.bar_grain,
            as_of_date=args.identity_as_of_date,
        )
        trace = run_backfill_source_dry_run_trace(source, max_rounds=args.max_rounds)
        result = trace.result
        if not isinstance(result, BackfillPlan):
            return {
                "ohlcv_series_id": series_id,
                "status": "skipped",
                "selected_identity": _reference_snapshot_identity_payload(snapshot),
                "reason": "planner returned EvidenceNeeded",
                "requests": [request.to_payload() for request in result.requests],
                "rounds": _rounds(trace.rounds),
                "planning_request_count": len(client.request_log),
            }
        if result.status == "blocked":
            return {
                "ohlcv_series_id": result.target.ohlcv_series_id,
                "status": "skipped",
                "selected_identity": _reference_snapshot_identity_payload(snapshot),
                "plan_status": result.status,
                "reason": "blocked plans are not executable",
            }
        if result.status == "caution" and args.no_caution:
            return {
                "ohlcv_series_id": result.target.ohlcv_series_id,
                "status": "skipped",
                "selected_identity": _reference_snapshot_identity_payload(snapshot),
                "plan_status": result.status,
                "reason": "caution plan skipped by --no-caution",
            }
        approval, approval_record = _approved_execution_payload(
            result,
            repository,
            reason="ohlcv_series_id-seeded CLI backfill approval",
        )
        receipt = execute_live_bar_backfill(
            result,
            approval,
            client,
            repository,
            evidence_facts=source.base_facts
            + tuple(fact for item in trace.rounds for fact in item.collected_facts),
        )
        return _live_execution_result_payload(
            {
                "ohlcv_series_id": series_id,
                "selected_identity": _reference_snapshot_identity_payload(snapshot),
            },
            result=result,
            receipt=receipt,
            approval_record=approval_record,
            planning_rounds=_rounds(trace.rounds),
        )
    except Exception as exc:
        return {
            "ohlcv_series_id": series_id,
            "status": "error",
            "error_type": exc.__class__.__name__,
            "error": str(exc),
        }


def _validate_db(
    args: argparse.Namespace, parser: argparse.ArgumentParser
) -> dict[str, Any]:
    if args.heartbeat_seconds < 1:
        raise CliError("--heartbeat-seconds must be positive")
    if args.summary_seconds < args.heartbeat_seconds:
        raise CliError(
            "--summary-seconds must be greater than or equal to --heartbeat-seconds"
        )
    started_at = time.monotonic()
    _validate_db_progress_sink(
        "starting",
        "STARTING validate-db",
        db=args.db,
        started_at=started_at,
        polling_interval_seconds=args.heartbeat_seconds,
        user_update_interval_seconds=args.summary_seconds,
    )
    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(_validate_db_work, args.db)
        next_heartbeat_at = started_at + args.heartbeat_seconds
        next_summary_at = started_at + args.summary_seconds
        while not future.done():
            now = time.monotonic()
            timeout = max(min(next_heartbeat_at, next_summary_at) - now, 0.05)
            try:
                result = future.result(timeout=timeout)
                break
            except concurrent.futures.TimeoutError:
                pass
            now = time.monotonic()
            if now >= next_heartbeat_at and not future.done():
                _validate_db_progress_sink(
                    "heartbeat",
                    "validate-db still running",
                    db=args.db,
                    started_at=started_at,
                )
                next_heartbeat_at = now + args.heartbeat_seconds
            if now >= next_summary_at and not future.done():
                _validate_db_progress_sink(
                    "summary",
                    "validate-db summary",
                    db=args.db,
                    started_at=started_at,
                )
                next_summary_at = now + args.summary_seconds
        else:
            result = future.result()
    _validate_db_progress_sink(
        "finished",
        "validate-db finished",
        db=args.db,
        started_at=started_at,
    )
    return result


def _validate_db_work(db: str) -> dict[str, Any]:
    repository = SQLiteStockUniverseRepository(db)
    repository.ensure_schema()
    validation = repository.validate()
    return {
        "ok": validation.ok,
        "db": str(Path(db)),
        "counts": repository.counts(),
        "validation": _validation_payload(validation),
    }


def _validate_db_progress_sink(
    event_type: str,
    message: str,
    *,
    db: str,
    started_at: float,
    polling_interval_seconds: int | None = None,
    user_update_interval_seconds: int | None = None,
) -> None:
    emit_cli_progress(
        "",
        command="stock-universe validate-db",
        event_type=event_type,
        message=message,
        started_at=started_at,
        db=str(Path(db)),
        polling_interval_seconds=polling_interval_seconds,
        user_update_interval_seconds=user_update_interval_seconds,
    )


def _universe_status(
    args: argparse.Namespace, parser: argparse.ArgumentParser
) -> dict[str, Any]:
    return {
        "ok": True,
        "command": "stock-universe universe-status",
        "result_type": "UniverseStatus",
        "effects": {
            "will_read": [args.db],
            "will_write": [],
            "did_write": [],
        },
        "status": universe_status(args.db),
    }


def _quality_audit(
    args: argparse.Namespace, parser: argparse.ArgumentParser
) -> dict[str, Any]:
    try:
        _validate_bar_grain_arg(args)
        report = quality_audit(
            args.db,
            stale_before=args.stale_before,
            limit=args.limit,
            categories=tuple(args.category),
            exchanges=tuple(args.exchange),
            security_types=tuple(args.security_type),
            series_ids=tuple(args.ohlcv_series_id),
            tickers=tuple(args.ticker),
            include_healthy=args.include_healthy,
            bar_grain=args.bar_grain,
        )
    except sqlite3.Error as exc:
        return {
            "protocol_version": PROTOCOL_VERSION,
            "ok": False,
            "command": "stock-universe quality-audit",
            "result_type": "RepairError",
            "error": str(exc),
            "repairs": [
                {
                    "name": "provide-existing-sqlite-db",
                    "command": {
                        "name": "stock-universe validate-db",
                        "description": "Initialize or validate DB before auditing quality.",
                        "args": {"db": args.db},
                        "reads": [args.db],
                        "writes": [args.db],
                    },
                    "requires_approval": True,
                    "reason": "quality-audit reads an existing SQLite DB and does not initialize schema.",
                }
            ],
            "effects": {
                "will_read": [args.db],
                "will_write": [],
                "did_write": [],
            },
        }
    return {
        "ok": True,
        "command": "stock-universe quality-audit",
        "result_type": "QualityAudit",
        "effects": {
            "will_read": [args.db],
            "will_write": [],
            "did_write": [],
        },
        **report,
    }


def _repair_missing_receipts(
    args: argparse.Namespace, parser: argparse.ArgumentParser
) -> dict[str, Any]:
    report = repair_missing_execution_receipts(
        args.db,
        series_ids=tuple(args.ohlcv_series_id),
        limit=args.limit,
        commit=args.commit,
        reason=args.reason,
    )
    return {
        "ok": True,
        "command": "stock-universe repair-missing-receipts",
        "result_type": "MissingReceiptRepair",
        "effects": {
            "will_read": [args.db],
            "will_write": [args.db] if args.commit else [],
            "did_write": [args.db] if args.commit and report["repaired_count"] else [],
        },
        **report,
    }


def _audit_executions(
    args: argparse.Namespace, parser: argparse.ArgumentParser
) -> dict[str, Any]:
    repository = SQLiteStockUniverseRepository(args.db)
    rows = repository.execution_audit(
        request_hash=args.request_hash,
        series_id=args.ohlcv_series_id,
        limit=args.limit,
    )
    return {
        "ok": True,
        "db": str(Path(args.db)),
        "count": len(rows),
        "executions": rows,
    }


def _doctor(
    args: argparse.Namespace, parser: argparse.ArgumentParser
) -> dict[str, Any]:
    api_key = args.api_key or os.environ.get("MASSIVE_API_KEY")
    checks = {
        "python": sys.version.split()[0],
        "stock_universe_module": str(Path(__file__).resolve()),
        "massive_api_key_present": bool(api_key),
    }
    if args.require_entrypoint:
        entrypoint = shutil.which("stock-universe")
        checks.update(
            {
                "stock_universe_entrypoint": entrypoint or "",
                "stock_universe_entrypoint_present": bool(entrypoint),
            }
        )
    if args.db:
        db_path = Path(args.db).expanduser().resolve()
        parent = db_path.parent
        checks["db_path"] = str(db_path)
        checks["db_exists"] = db_path.exists()
        checks["db_parent"] = str(parent)
        checks["db_parent_exists"] = parent.exists()
        checks["db_parent_writable"] = parent.exists() and os.access(parent, os.W_OK)
        if db_path.exists():
            checks.update(_sqlite_schema_state(db_path))
    return {
        "ok": _doctor_ok(checks, require_entrypoint=args.require_entrypoint),
        "checks": checks,
    }


def _doctor_ok(checks: dict[str, Any], *, require_entrypoint: bool) -> bool:
    required = [
        bool(checks.get("massive_api_key_present")),
    ]
    if require_entrypoint:
        required.append(bool(checks.get("stock_universe_entrypoint_present")))
    if "db_parent_exists" in checks:
        required.extend(
            [
                bool(checks.get("db_parent_exists")),
                bool(checks.get("db_parent_writable")),
            ]
        )
    if checks.get("db_exists"):
        required.extend(
            [
                bool(checks.get("db_schema_current")),
                bool(checks.get("db_required_tables_present")),
            ]
        )
    return all(required)


def _sqlite_schema_state(path: Path) -> dict[str, Any]:
    required_tables = (
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
    )
    try:
        with connect_readonly_sqlite(path) as conn:
            row = conn.execute(
                "SELECT value FROM schema_metadata WHERE key = 'schema_version'"
            ).fetchone()
            actual_version = str(row[0]) if row else ""
            present_tables = {
                str(item[0])
                for item in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                ).fetchall()
            }
    except sqlite3.Error as exc:
        return {
            "db_schema_version": "",
            "db_schema_expected": SCHEMA_VERSION,
            "db_schema_current": False,
            "db_required_tables_present": False,
            "db_schema_error": str(exc),
        }
    missing_tables = [name for name in required_tables if name not in present_tables]
    return {
        "db_schema_version": actual_version,
        "db_schema_expected": SCHEMA_VERSION,
        "db_schema_current": actual_version == SCHEMA_VERSION,
        "db_required_tables_present": not missing_tables,
        "db_missing_tables": missing_tables,
    }


def _api_key(args: argparse.Namespace, parser: argparse.ArgumentParser) -> str:
    api_key = args.api_key or os.environ.get("MASSIVE_API_KEY")
    if not api_key:
        raise CliError("--api-key or MASSIVE_API_KEY is required")
    return api_key


def _identity_search_live_reads(args: argparse.Namespace) -> list[str]:
    reads = ["massive.reference_tickers"]
    if not args.api_key:
        reads.append("env:MASSIVE_API_KEY")
    return reads


def _reference_active_value(value: str) -> bool | None:
    if value == "active":
        return True
    if value == "inactive":
        return False
    return None


def _reference_universe_reads(args: argparse.Namespace) -> list[str]:
    reads = ["massive.reference_tickers"]
    if not args.api_key:
        reads.append("env:MASSIVE_API_KEY")
    return reads


def _reference_universe_writes(args: argparse.Namespace) -> list[str]:
    writes = []
    if args.commit:
        writes.append(args.db)
    if args.capture_dir:
        writes.append(f"{args.capture_dir}/*.json")
    return writes


def _reference_snapshot_preview(
    snapshots: tuple[Any, ...], *, limit: int = 20
) -> dict[str, Any]:
    return {
        "count": len(snapshots),
        "preview_count": min(len(snapshots), limit),
        "snapshots": [
            {
                "active": snapshot.active,
                "cik": snapshot.cik,
                "company_name": snapshot.company_name,
                "composite_figi": snapshot.composite_figi,
                "identity_status": snapshot.identity_status,
                "natural_key": snapshot.natural_key,
                "ohlcv_series_id": snapshot.ohlcv_series_id or None,
                "primary_exchange": snapshot.primary_exchange,
                "security_type": snapshot.security_type,
                "share_class_figi": snapshot.share_class_figi,
                "snapshot_as_of_date": snapshot.snapshot_as_of_date,
                "ticker": snapshot.ticker,
            }
            for snapshot in snapshots[:limit]
        ],
    }


def _reference_snapshot_identity_payload(snapshot: Any) -> dict[str, Any]:
    return {
        "active": snapshot.active,
        "cik": snapshot.cik,
        "company_name": snapshot.company_name,
        "composite_figi": snapshot.composite_figi,
        "identity_status": snapshot.identity_status,
        "natural_key": snapshot.natural_key,
        "ohlcv_series_id": snapshot.ohlcv_series_id or None,
        "primary_exchange": snapshot.primary_exchange,
        "security_type": snapshot.security_type,
        "share_class_figi": snapshot.share_class_figi,
        "snapshot_as_of_date": snapshot.snapshot_as_of_date,
        "source": snapshot.provider,
        "ticker": snapshot.ticker,
    }


def _reference_batch_selection_payload(args: argparse.Namespace) -> dict[str, Any]:
    grain = normalize_bar_grain(args.bar_grain)
    return {
        "active": _reference_active_value(args.active),
        "all_pages": bool(args.all_pages),
        "bar_grain": grain.bar_grain,
        "multiplier": grain.multiplier,
        "timespan": grain.timespan,
        "exchange": args.exchange,
        "identity_as_of_date": args.identity_as_of_date,
        "market": args.market,
        "offset": args.offset,
        "limit": args.limit,
        "security_types": list(_reference_batch_security_types(args)),
        "ohlcv_series_ids": list(args.ohlcv_series_id),
    }


def _reference_batch_security_types(args: argparse.Namespace) -> tuple[str, ...]:
    aliases = {
        "common_stock": "CS",
        "etf": "ETF",
        "warrant": "WARRANT",
        "unit": "UNIT",
        "adrc": "ADRC",
        "right": "RIGHT",
        "preferred": "PFD",
        "fund": "FUND",
    }
    values = [
        str(item).strip().upper()
        for item in getattr(args, "security_type", [])
        if str(item).strip()
    ]
    values.extend(code for flag, code in aliases.items() if getattr(args, flag, False))
    return tuple(dict.fromkeys(values))


def _reference_batch_reads(args: argparse.Namespace) -> list[str]:
    reads = [f"sqlite.reference_universe:{args.db}"]
    if args.commit:
        reads.extend(
            [
                "massive.ticker_events",
                "massive.reference_boundary",
                "massive.bar_probe",
                "massive.identity_scan",
                "massive.ticker_replacement",
            ]
        )
        if not args.api_key:
            reads.append("env:MASSIVE_API_KEY")
    return reads


def _reference_batch_writes(args: argparse.Namespace) -> list[str]:
    if args.commit:
        return [args.db]
    return []


def _reference_batch_next_action(
    args: argparse.Namespace,
    *,
    snapshots: tuple[Any, ...],
    pending_count: int,
    results: list[dict[str, Any]],
) -> str:
    if not snapshots:
        return "update_reference_universe"
    if not args.commit:
        if args.all_pages:
            return "commit_all_pages"
        return "commit_selected_batch"
    if any(item["status"] != "ok" for item in results):
        return "repair_failures"
    if pending_count and not args.all_pages:
        return "continue_batch"
    return "done"


def _reference_batch_next_actions(
    args: argparse.Namespace,
    *,
    snapshots: tuple[Any, ...],
    pending_count: int,
    results: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    actions: list[dict[str, Any]] = []
    if not snapshots:
        actions.append(_reference_batch_update_reference_action(args))
        return actions
    if not args.commit:
        action_name = (
            "commit-all-reference-pages"
            if args.all_pages
            else "commit-selected-reference-batch"
        )
        description = (
            "Execute every internally paged DB-backed OHLCV series ID in this selection."
            if args.all_pages
            else "Execute exactly the selected DB-backed OHLCV series IDs."
        )
        reason = (
            "The dry-run manifest enumerates every internally paged persisted OHLCV series ID; --commit is required to execute."
            if args.all_pages
            else "The dry-run manifest only enumerates selected persisted OHLCV series IDs; --commit is required to execute."
        )
        actions.append(
            {
                "name": action_name,
                "kind": "command",
                "command": {
                    "name": "stock-universe backfill-reference-batch",
                    "description": description,
                    "args": _reference_batch_command_args(args, commit=True),
                    "reads": _reference_batch_reads(_args_with_commit(args, True)),
                    "writes": [args.db],
                },
                "effects": [
                    {
                        "kind": "write",
                        "target": args.db,
                        "description": "Persist backfill receipts and bars for selected OHLCV series IDs.",
                    }
                ],
                "requires_approval": True,
                "reason": reason,
            }
        )
    for item in results:
        if item["status"] != "ok":
            actions.append(_reference_batch_failure_action(args, item))
    if pending_count and not args.all_pages:
        actions.append(
            {
                "name": "continue-reference-batch",
                "kind": "command",
                "command": {
                    "name": "stock-universe backfill-reference-batch",
                    "description": "Enumerate the next bounded slice of reference-universe OHLCV series IDs.",
                    "args": _reference_batch_command_args(
                        args, commit=False, offset=args.offset + len(snapshots)
                    ),
                    "reads": [args.db],
                    "writes": [],
                },
                "effects": [
                    {
                        "kind": "read",
                        "target": args.db,
                        "description": "Read the next reference snapshot slice.",
                    }
                ],
                "requires_approval": False,
                "reason": f"{pending_count} reference snapshot(s) remain after this bounded slice.",
            }
        )
    return actions


def _reference_batch_repair_hints(
    args: argparse.Namespace,
    *,
    snapshots: tuple[Any, ...],
    results: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    hints: list[dict[str, Any]] = []
    if not snapshots:
        hints.append(_reference_batch_update_reference_action(args))
    for item in results:
        if item["status"] != "ok":
            hints.append(_reference_batch_failure_action(args, item))
    return hints


def _reference_batch_update_reference_action(
    args: argparse.Namespace,
) -> dict[str, Any]:
    command_args = {
        **_db_arg_if_override(args.db),
        "limit": 1000,
        "max_pages": 100,
        "commit": True,
    }
    if args.exchange:
        command_args["exchange"] = args.exchange
    return {
        "name": "update-reference-universe",
        "kind": "command",
        "command": {
            "name": "stock-universe update-reference-universe",
            "description": "Populate the reference-universe snapshot table before selecting a batch.",
            "args": command_args,
            "reads": ["Massive API"],
            "writes": [args.db],
        },
        "effects": [
            {
                "kind": "write",
                "target": args.db,
                "description": "Persist reference-universe snapshots.",
            }
        ],
        "requires_approval": True,
        "reason": "No persisted reference snapshots matched this batch selection.",
    }


def _reference_batch_failure_action(
    args: argparse.Namespace, result: dict[str, Any]
) -> dict[str, Any]:
    series_id = result.get("ohlcv_series_id", "{ohlcv_series_id}")
    return {
        "name": "dry-run-failed-ohlcv-series-id",
        "kind": "command",
        "command": {
            "name": "stock-universe dry-run",
            "description": "Rehearse the failed selected OHLCV series ID and inspect planner requests or errors.",
            "args": {
                **_db_arg_if_override(args.db),
                "ohlcv_series_id": series_id,
                "max_rounds": args.max_rounds,
                "identity_as_of_date": args.identity_as_of_date,
                **_bar_grain_arg_if_override(args.bar_grain),
            },
            "reads": [args.db, "Massive API"],
            "writes": [],
        },
        "effects": [
            {
                "kind": "read",
                "target": args.db,
                "description": "Reload selected OHLCV series ID evidence.",
            }
        ],
        "requires_approval": False,
        "reason": result.get("reason")
        or result.get("error")
        or "Selected OHLCV series ID did not complete.",
    }


def _reference_batch_command_args(
    args: argparse.Namespace,
    *,
    commit: bool,
    offset: int | None = None,
) -> dict[str, Any]:
    command_args: dict[str, Any] = {
        **_db_arg_if_override(args.db),
        "limit": args.limit,
        "offset": args.offset if offset is None else offset,
        "active": args.active,
    }
    if args.exchange:
        command_args["exchange"] = args.exchange
    if args.market:
        command_args["market"] = args.market
    if _reference_batch_security_types(args):
        command_args["security_type"] = list(_reference_batch_security_types(args))
    if args.identity_as_of_date:
        command_args["identity_as_of_date"] = args.identity_as_of_date
    if args.ohlcv_series_id:
        command_args["ohlcv_series_id"] = list(args.ohlcv_series_id)
    if args.all_pages:
        command_args["all_pages"] = True
    command_args.update(_bar_grain_arg_if_override(args.bar_grain))
    if commit:
        command_args["commit"] = True
    return command_args


def _args_with_commit(args: argparse.Namespace, commit: bool) -> argparse.Namespace:
    values = vars(args).copy()
    values["commit"] = commit
    return argparse.Namespace(**values)


def _db_arg_if_override(db: str | None) -> dict[str, str]:
    target = str(db or canonical_db_text())
    try:
        if Path(target).resolve() == Path(canonical_db_text()).resolve():
            return {}
    except OSError:
        pass
    return {"db": target}


def _bar_grain_arg_if_override(bar_grain: str | None) -> dict[str, str]:
    grain = normalize_bar_grain(bar_grain or "1d")
    if grain.bar_grain == "1d":
        return {}
    return {"bar_grain": grain.bar_grain}


def _planned_reads(args: argparse.Namespace) -> list[str]:
    if getattr(args, "ohlcv_series_id", None) is not None:
        reads = [
            f"sqlite.reference_universe:{args.db}",
            "massive.ticker_events",
            "massive.reference_boundary",
            "massive.bar_probe",
            "massive.identity_scan",
            "massive.ticker_replacement",
        ]
        if not args.api_key:
            reads.append("env:MASSIVE_API_KEY")
        return reads
    if args.ticker:
        reads = [
            f"massive.reference_ticker:{args.ticker}",
            "massive.ticker_events",
            "massive.reference_boundary",
            "massive.bar_probe",
            "massive.identity_scan",
            "massive.ticker_replacement",
        ]
        if not args.api_key:
            reads.append("env:MASSIVE_API_KEY")
        return reads
    return []


def _planned_writes(args: argparse.Namespace) -> list[str]:
    writes = []
    if args.capture_dir:
        writes.append(f"{args.capture_dir}/*.json")
    if args.markdown_out:
        writes.append(args.markdown_out)
    return writes


def _captured_raw_files(capture_dir: Path | None) -> list[str]:
    if capture_dir is None or not capture_dir.exists():
        return []
    return [str(path) for path in sorted(capture_dir.glob("*.json"))]


def _write_optional_plan_outputs(
    plan: BackfillPlan, args: argparse.Namespace, envelope: dict[str, Any]
) -> None:
    did_write = envelope["effects"]["did_write"]
    if args.markdown_out:
        path = Path(args.markdown_out)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(render_backfill_plan_markdown(plan), encoding="utf-8")
        did_write.append(str(path))


def _plan_summary(plan: BackfillPlan) -> dict[str, Any]:
    return {
        "ohlcv_series_id": plan.target.ohlcv_series_id,
        "latest_ticker": plan.target.latest_ticker,
        "status": plan.status,
        "segment_count": len(plan.segments),
        "request_hash": plan.request.request_hash,
        "evidence_ledger_hash": plan.evidence_ledger_hash,
    }


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


def _request_log(client: Any) -> list[dict[str, Any]]:
    return [
        {
            "endpoint": item.endpoint,
            "params_without_api_key": item.params_without_api_key,
            "http_code": item.http_code,
            "api_status": item.api_status,
            "elapsed_seconds": item.elapsed_seconds,
        }
        for item in client.request_log
    ]


def _validation_payload(validation: Any) -> dict[str, Any]:
    return {
        "ok": validation.ok,
        "checks": list(validation.checks),
        "failures": list(validation.failures),
    }


if __name__ == "__main__":
    raise SystemExit(main())
