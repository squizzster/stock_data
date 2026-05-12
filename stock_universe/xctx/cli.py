"""Read-oriented xctx command surface for backfill planning."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import shutil
import sqlite3
import sys
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
from stock_universe.domain import normalize_bar_grain
from stock_universe.market_calendar import (
    classify_us_equity_session,
    first_us_equity_trading_date_on_or_after,
    is_us_equity_trading_date,
    next_us_equity_trading_date,
    previous_us_equity_trading_date,
)
from stock_universe.paths import canonical_db_text
from stock_universe.providers import MassiveProviderConfig, MassiveReadOnlyClient
from stock_universe.quality_audit import ISSUE_CATEGORIES, quality_audit
from stock_universe.storage import connect_readonly_sqlite
from stock_universe.storage.sqlite_repo import SCHEMA_VERSION
from stock_universe.universe_status import universe_status
from stock_universe.workflows import (
    DEFAULT_CATCH_UP_BATCH_SIZE,
    DEFAULT_CATCH_UP_WORKERS,
    DEFAULT_TICKER_SEED_FROM_DATE,
    MAX_CATCH_UP_WORKERS,
    build_catch_up_plan,
    catch_up_run_status,
    catch_up_runs,
    live_identity_search,
    massive_live_source_from_ticker,
    massive_live_source_from_series_id,
    run_backfill_source_dry_run_trace,
    sqlite_identity_search,
)
from stock_universe.workflows.catch_up import (
    EXECUTABLE_CATCH_UP_CATEGORIES,
    REVIEW_ONLY_CATCH_UP_CATEGORIES,
)
from stock_universe.xctx import (
    PROTOCOL_VERSION,
    normalize_action_records,
    result_envelope,
    result_envelope_schema,
    xctx_binding_maps,
    xctx_command_schemas,
    xctx_recipes,
    xctx_runnable_argv,
    xctx_tool_manifest,
)


class XctxCliError(ValueError):
    def __init__(self, payload: dict[str, Any]) -> None:
        super().__init__(
            str(payload.get("error") or payload.get("errors") or "xctx error")
        )
        self.payload = payload


def main(argv: list[str] | None = None, *, prog: str = "xctx") -> int:
    try:
        argv = list(sys.argv[1:] if argv is None else argv)
        parser = _parser(prog=prog)
        if not argv:
            print_help_for_missing_command(parser)
        args = parser.parse_args(argv)
        try:
            payload = args.func(args, parser)
        except XctxCliError as exc:
            payload = exc.payload
        payload = normalize_action_records(payload)
        if not emit_json(payload):
            return 0
        return 0
    except BrokenPipeError:
        silence_stdout()
        return 0
    except KeyboardInterrupt:
        return interrupted_exit(prog)


def _parser(*, prog: str = "xctx") -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog=prog,
        description="Executable Context protocol plane for stock-universe.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=dedent(
            """\
            Recommended agent loop:
              1. {prog} doctor
              2. {prog} universe-status
              3. {prog} tree
              4. {prog} schema --command "xctx dry-run"
              5. {prog} examples
              6. {prog} resolve-identity --source db --query NVDA
              7. {prog} dry-run --ohlcv-series-id <ohlcv_series_id>
              8. ./stock_universe.cli backfill --ohlcv-series-id <ohlcv_series_id> --strict
              9. {prog} observe

            Protocol:
              Start with doctor and tree, then follow recipe command fields or schema
              argv/source_checkout_argv fields. command.name and logical_command values
              are identifiers, not shell commands.

            Read-oriented by default:
              xctx commands expose schemas, binding maps, dry-runs, audits, and recipes.
              Write paths are explicit stock-universe commands and require --commit or
              backfill execution.

            Source checkout:
              ./stock_universe.cli xctx tree
              ./stock_universe.cli xctx doctor
              ./stock_universe.cli xctx universe-status
              ./stock_universe.cli xctx examples
            """
        ).format(prog=prog),
    )
    subcommands = parser.add_subparsers(dest="command", required=True)

    tree = subcommands.add_parser(
        "tree", help="Return the executable-context transition tree."
    )
    tree.add_argument(
        "--view",
        choices=("simple", "detail", "extra_detail"),
        default="simple",
        help="Choose compact discovery, schema detail, or full protocol internals.",
    )
    tree.set_defaults(func=_tree)

    capabilities = subcommands.add_parser(
        "capabilities", help="List supported xctx commands."
    )
    capabilities.set_defaults(func=_capabilities)

    doctor = subcommands.add_parser(
        "doctor", help="Check local readiness for xctx-guided workflows."
    )
    doctor.add_argument(
        "--db",
        default=canonical_db_text(),
        help="SQLite path to inspect in readiness checks. Defaults to the canonical universe DB.",
    )
    doctor.add_argument(
        "--api-key", default=None, help="Massive API key. Defaults to MASSIVE_API_KEY."
    )
    doctor.add_argument(
        "--require-entrypoint",
        action="store_true",
        help="Require installed stock-universe/xctx entrypoints.",
    )
    doctor.set_defaults(func=_doctor)

    examples = subcommands.add_parser(
        "examples", help="Return runnable xctx examples and structured inputs."
    )
    examples.add_argument(
        "--command",
        default=None,
        help='Optional command name, for example "xctx dry-run".',
    )
    examples.set_defaults(func=_examples)

    describe = subcommands.add_parser("describe", help="Describe a command protocol.")
    describe_subcommands = describe.add_subparsers(dest="topic", required=True)
    backfill = describe_subcommands.add_parser(
        "backfill-plan", help="Describe backfill planning envelopes."
    )
    backfill.set_defaults(func=_describe_backfill_plan)

    schema = subcommands.add_parser(
        "schema", help="Return command schemas and binding maps."
    )
    schema.add_argument(
        "--command", default=None, help="Optional command name to filter."
    )
    schema.set_defaults(func=_schema)

    dry_run = subcommands.add_parser(
        "dry-run", help="Run read-oriented adaptive planning."
    )
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
        "--api-key",
        default=None,
        help="Massive API key for --source live. Defaults to MASSIVE_API_KEY.",
    )
    dry_run.add_argument(
        "--base-url",
        default="https://api.massive.com",
        help="Massive API base URL for --source live.",
    )
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
    dry_run.set_defaults(func=_dry_run)

    resolve_identity = subcommands.add_parser(
        "resolve-identity",
        help="Resolve identity candidates and same-CIK issuer context with read-oriented providers.",
    )
    resolve_identity.add_argument(
        "query_arg",
        nargs="?",
        help="Ticker, company name, CIK, FIGI, or OHLCV series ID.",
    )
    resolve_identity.add_argument(
        "--query",
        dest="query_option",
        default=None,
        help="Ticker, company name, CIK, FIGI, or OHLCV series ID.",
    )
    resolve_identity.add_argument("--source", choices=("live", "db"), default="live")
    resolve_identity.add_argument(
        "--db",
        default=canonical_db_text(),
        help="SQLite database path for --source db. Defaults to the canonical universe DB.",
    )
    resolve_identity.add_argument(
        "--api-key",
        default=None,
        help="Massive API key for --source live. Defaults to MASSIVE_API_KEY.",
    )
    resolve_identity.add_argument("--base-url", default="https://api.massive.com")
    resolve_identity.add_argument(
        "--as-of-date", default=None, help="Optional Massive reference date."
    )
    resolve_identity.add_argument("--limit", type=int, default=25)
    resolve_identity.set_defaults(func=_resolve_identity)

    bars = subcommands.add_parser(
        "bars", help="Read canonical OHLCV bar observations from the stock system DB."
    )
    bars_input = bars.add_mutually_exclusive_group(required=True)
    bars_input.add_argument(
        "--ohlcv-series-id",
        "--ohlcv_series_id",
        dest="ohlcv_series_id",
        type=int,
        help="Selected canonical OHLCV series ID.",
    )
    bars_input.add_argument(
        "--query",
        help="DB-backed ticker, company name, CIK, FIGI, or OHLCV series ID query.",
    )
    bars.add_argument(
        "--db",
        default=canonical_db_text(),
        help="SQLite database path. Defaults to the canonical universe DB.",
    )
    bars.add_argument("--date", default=None, help="Single bar date to observe.")
    bars.add_argument(
        "--from-date",
        default=None,
        help="Inclusive start date for a date-range observation.",
    )
    bars.add_argument(
        "--to-date",
        default=None,
        help="Inclusive end date for a date-range observation.",
    )
    _add_bar_grain_arg(bars)
    bars.add_argument(
        "--ticker-label",
        default=None,
        help="Optional point-in-time ticker label filter; defaults to canonical series scope.",
    )
    bars.add_argument(
        "--limit",
        type=int,
        default=5,
        help="Maximum bars to return. Also controls latest rows when no date range is supplied.",
    )
    bars.add_argument(
        "--view",
        choices=("simple", "detail", "extra_detail"),
        default="simple",
        help="Use simple for the requested bar, detail for identity/calendar/quality, extra_detail for session/UTC/direct lineage/raw sidecar audit evidence.",
    )
    bars.set_defaults(func=_bars)

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
        help="Classify stale, missing-bar, and receipt/accounting issues with read-oriented checks.",
    )
    _add_quality_audit_args(quality)
    quality.set_defaults(func=_quality_audit)

    catch_up_plan = subcommands.add_parser(
        "catch-up-plan",
        help="Materialize a deterministic read-oriented database catch-up plan.",
    )
    _add_catch_up_plan_args(catch_up_plan)
    catch_up_plan.set_defaults(func=_catch_up_plan)

    catch_up_runs = subcommands.add_parser(
        "catch-up-runs", help="List recent catch-up run summaries."
    )
    catch_up_runs.add_argument(
        "--run-root",
        default=None,
        help="Root directory containing catch-up run artifacts.",
    )
    catch_up_runs.add_argument(
        "--limit", type=int, default=5, help="Maximum recent runs to return."
    )
    catch_up_runs.add_argument(
        "--view",
        choices=("simple", "detail", "extra_detail"),
        default="simple",
        help="Choose compact run summaries, action detail, or full run records.",
    )
    catch_up_runs.set_defaults(func=_catch_up_runs)

    catch_up_status = subcommands.add_parser(
        "catch-up-status", help="Read durable catch-up run artifacts."
    )
    catch_up_status_input = catch_up_status.add_mutually_exclusive_group(required=True)
    catch_up_status_input.add_argument(
        "--run-dir",
        help="Catch-up run directory containing plan/status/batch artifacts.",
    )
    catch_up_status_input.add_argument(
        "--latest",
        action="store_true",
        help="Read the most recent catch-up run under --run-root.",
    )
    catch_up_status.add_argument(
        "--run-root",
        default=None,
        help="Root directory used with --latest. Defaults to canonical catch_up_runs.",
    )
    catch_up_status.add_argument(
        "--view",
        choices=("simple", "detail", "extra_detail"),
        default="simple",
        help="Choose compact status, diagnostic detail, or full artifact detail.",
    )
    catch_up_status.set_defaults(func=_catch_up_status)

    observe = subcommands.add_parser(
        "observe", help="Observe persisted execution receipts and approval links."
    )
    observe.add_argument(
        "--db",
        default=canonical_db_text(),
        help="SQLite database path. Defaults to the canonical universe DB.",
    )
    observe.add_argument("--request-hash", default=None)
    observe.add_argument(
        "--ohlcv-series-id",
        "--ohlcv_series_id",
        dest="ohlcv_series_id",
        type=int,
        default=None,
    )
    observe.add_argument("--limit", type=int, default=20)
    observe.add_argument(
        "--view",
        choices=("simple", "detail", "extra_detail"),
        default="simple",
        help="Choose compact receipt summary, receipt rows, or full audit evidence.",
    )
    observe.set_defaults(func=_observe)

    compose = subcommands.add_parser(
        "compose", help="Return workflow recipes composed from xctx transitions."
    )
    compose.add_argument(
        "--recipe", default=None, help="Optional recipe name to filter."
    )
    compose.set_defaults(func=_compose)
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
    parser.add_argument(
        "--view",
        choices=("simple", "detail", "extra_detail"),
        default="simple",
        help="Choose compact status, bounded issue detail, or full issue rows.",
    )


def _add_catch_up_plan_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--db",
        default=canonical_db_text(),
        help="SQLite database path. Defaults to the canonical universe DB.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_CATCH_UP_WORKERS,
        help=f"Concurrent workers. Default {DEFAULT_CATCH_UP_WORKERS}; max {MAX_CATCH_UP_WORKERS}.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_CATCH_UP_BATCH_SIZE,
        help=f"Materialized targets per batch. Default {DEFAULT_CATCH_UP_BATCH_SIZE}.",
    )
    parser.add_argument(
        "--target-limit",
        type=int,
        default=0,
        help="Optional cap on materialized targets.",
    )
    parser.add_argument(
        "--stale-before",
        default=None,
        help="Override stale-date classification; defaults to DB global max bar date.",
    )
    parser.add_argument(
        "--category",
        action="append",
        choices=sorted(ISSUE_CATEGORIES),
        default=[],
        help="Filter quality categories before selecting executable targets. May repeat.",
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
        help="Filter to selected OHLCV series IDs. May repeat.",
    )
    parser.add_argument(
        "--ticker",
        action="append",
        default=[],
        help="Filter to selected latest tickers. May repeat.",
    )
    parser.add_argument(
        "--from-date", default=None, help="Override all target start dates."
    )
    parser.add_argument("--to-date", default=None)
    _add_bar_grain_arg(parser)
    parser.add_argument(
        "--run-root", default=None, help="Root directory for committed run artifacts."
    )
    parser.add_argument(
        "--run-dir", default=None, help="Exact committed run artifact directory."
    )
    parser.add_argument(
        "--view",
        choices=("simple", "detail", "extra_detail"),
        default="simple",
        help="Choose compact plan, bounded target detail, or full plan output.",
    )
    parser.add_argument(
        "--detail-limit",
        type=int,
        default=25,
        help="Maximum targets and batches included when --view detail is used.",
    )


def _add_bar_grain_arg(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--bar-grain",
        default="1d",
        help="Aggregate bar grain. Defaults to 1d. Supported values: 1d, 1m, 30m.",
    )


def _tree(args: argparse.Namespace, parser: argparse.ArgumentParser) -> dict[str, Any]:
    manifest = xctx_tool_manifest()
    manifest["ok"] = True
    manifest["command"] = "xctx tree"
    manifest["result_type"] = "ToolManifest"
    manifest["view"] = args.view
    manifest["current_invocation"] = parser.prog
    manifest["effects"] = _effects(reads=[], writes=[])
    if args.view == "extra_detail":
        manifest["command_schemas"] = xctx_command_schemas()
        manifest["binding_maps"] = xctx_binding_maps()
        manifest["recipes"] = xctx_recipes()
        return manifest
    schemas = xctx_command_schemas()
    manifest["commands"] = [
        _command_summary_record(name, schema) for name, schema in schemas.items()
    ]
    manifest["recipes"] = [_recipe_summary(recipe) for recipe in xctx_recipes()]
    if args.view == "detail":
        manifest["command_schemas"] = schemas
        manifest["views"] = {
            name: schema["views"]
            for name, schema in schemas.items()
            if schema.get("views")
        }
    manifest["next_actions"] = [
        {
            "name": "inspect-extra-detail-tree",
            "kind": "command",
            "command": {
                "name": "xctx tree",
                "description": "Return full schemas, binding maps, and recipe details.",
                "args": {"view": "extra_detail"},
                "reads": [],
                "writes": [],
            },
            "effects": [
                {
                    "kind": "none",
                    "target": "stdout",
                    "description": "Render the extra-detail ToolManifest.",
                }
            ],
            "requires_approval": False,
        },
        {
            "name": "inspect-stock-universe-health-check",
            "kind": "command",
            "command": {
                "name": "xctx compose",
                "description": "Return the compact stock status and catch-up assessment recipe.",
                "args": {"recipe": "stock-universe-health-check"},
                "reads": [],
                "writes": [],
            },
            "effects": [
                {
                    "kind": "none",
                    "target": "stdout",
                    "description": "Render recipe steps.",
                }
            ],
            "requires_approval": False,
        },
    ]
    return manifest


def _capabilities(
    args: argparse.Namespace, parser: argparse.ArgumentParser
) -> dict[str, Any]:
    schemas = xctx_command_schemas()
    return {
        "protocol_version": PROTOCOL_VERSION,
        "ok": True,
        "namespace": "xctx",
        "command": "xctx capabilities",
        "result_type": "CapabilityList",
        "commands": [_command_record(name, schema) for name, schema in schemas.items()],
        "transitions": xctx_tool_manifest()["transitions"],
        "effects": _effects(reads=[], writes=[]),
    }


def _doctor(
    args: argparse.Namespace, parser: argparse.ArgumentParser
) -> dict[str, Any]:
    api_key = args.api_key or os.environ.get("MASSIVE_API_KEY")
    wrapper = Path("stock_universe.cli")
    checks: dict[str, Any] = {
        "python": sys.version.split()[0],
        "protocol_version": PROTOCOL_VERSION,
        "current_invocation": parser.prog,
        "source_checkout_wrapper": str(wrapper.resolve()) if wrapper.exists() else "",
        "source_checkout_wrapper_present": wrapper.exists(),
        "source_checkout_wrapper_executable": wrapper.exists()
        and os.access(wrapper, os.X_OK),
        "massive_api_key_present": bool(api_key),
    }
    reads = ["env:MASSIVE_API_KEY"]
    if args.require_entrypoint:
        stock_universe_entrypoint = shutil.which("stock-universe")
        xctx_entrypoint = shutil.which("xctx")
        checks.update(
            {
                "stock_universe_entrypoint": stock_universe_entrypoint or "",
                "stock_universe_entrypoint_present": bool(stock_universe_entrypoint),
                "xctx_entrypoint": xctx_entrypoint or "",
                "xctx_entrypoint_present": bool(xctx_entrypoint),
            }
        )
        reads.append("filesystem:entrypoints")
    if args.db:
        db_path = Path(args.db).expanduser().resolve()
        parent = db_path.parent
        checks.update(
            {
                "db_path": str(db_path),
                "db_exists": db_path.exists(),
                "db_parent": str(parent),
                "db_parent_exists": parent.exists(),
                "db_parent_writable": parent.exists() and os.access(parent, os.W_OK),
            }
        )
        reads.append(str(db_path))
        if db_path.exists():
            checks.update(_sqlite_schema_state(db_path))
    ok = _doctor_ok(checks, require_entrypoint=args.require_entrypoint)
    return {
        "protocol_version": PROTOCOL_VERSION,
        "ok": ok,
        "command": "xctx doctor",
        "result_type": "DoctorReport",
        "checks": checks,
        "effects": _effects(reads=reads, writes=[]),
        "next_actions": _doctor_next_actions(checks),
    }


def _examples(
    args: argparse.Namespace, parser: argparse.ArgumentParser
) -> dict[str, Any]:
    catalog = _example_catalog(parser)
    examples = catalog
    if args.command:
        examples = [
            example for example in examples if example["command"] == args.command
        ]
    schemas = xctx_command_schemas()
    return {
        "protocol_version": PROTOCOL_VERSION,
        "ok": bool(examples),
        "command": "xctx examples",
        "result_type": "ExampleList",
        "examples": examples,
        "known_commands": sorted({example["command"] for example in catalog}),
        "known_example_commands": sorted({example["command"] for example in catalog}),
        "known_schema_commands": sorted(schemas),
        "known_aliases": sorted(_schema_aliases(schemas)),
        "effects": _effects(reads=[], writes=[]),
        "next_actions": [
            {
                "name": "start-with-tree",
                "kind": "command",
                "command": {
                    "name": "xctx tree",
                    "description": "Discover the transition graph, command schemas, binding maps, and recipes.",
                    "args": {},
                    "reads": [],
                    "writes": [],
                },
                "effects": [
                    {
                        "kind": "none",
                        "target": "stdout",
                        "description": "Render the ToolManifest.",
                    }
                ],
                "requires_approval": False,
            }
        ],
    }


def _describe_backfill_plan(
    args: argparse.Namespace, parser: argparse.ArgumentParser
) -> dict[str, Any]:
    return {
        "protocol_version": PROTOCOL_VERSION,
        "ok": True,
        "command": "xctx describe backfill-plan",
        "result_type": "CommandDescription",
        "name": "backfill-plan",
        "schema": result_envelope_schema(),
        "mutates": False,
        "effects": _effects(reads=[], writes=[]),
        "planner_contract": {
            "http": False,
            "sqlite": False,
            "filesystem": False,
            "clock": False,
        },
    }


def _schema(
    args: argparse.Namespace, parser: argparse.ArgumentParser
) -> dict[str, Any]:
    schemas = xctx_command_schemas()
    binding_maps = xctx_binding_maps()
    if args.command:
        requested_command = args.command
        schema_key = (
            requested_command
            if requested_command in schemas
            else _schema_alias_target(requested_command, schemas)
        )
        schema = schemas.get(schema_key or "")
        if schema is None:
            return {
                "protocol_version": PROTOCOL_VERSION,
                "ok": False,
                "command": "xctx schema",
                "result_type": "RepairError",
                "error": f"unknown command: {requested_command}",
                "known_commands": sorted(schemas),
                "known_aliases": sorted(_schema_aliases(schemas)),
                "effects": _effects(reads=[], writes=[]),
            }
        payload = {
            "protocol_version": PROTOCOL_VERSION,
            "ok": True,
            "command": "xctx schema",
            "result_type": "CommandSchema",
            "command_schema": {schema_key: schema},
            "binding_map": {schema_key: binding_maps.get(schema_key, {})},
            "effects": _effects(reads=[], writes=[]),
        }
        if schema_key != requested_command:
            payload["alias_of"] = schema_key
        return payload
    return {
        "protocol_version": PROTOCOL_VERSION,
        "ok": True,
        "command": "xctx schema",
        "result_type": "CommandSchema",
        "command_schemas": schemas,
        "binding_maps": binding_maps,
        "effects": _effects(reads=[], writes=[]),
    }


def _dry_run(
    args: argparse.Namespace, parser: argparse.ArgumentParser
) -> dict[str, Any]:
    grain_error = _bar_grain_input_error("xctx dry-run", args)
    if grain_error:
        return grain_error
    selected_identity = None
    if args.ticker:
        api_key = args.api_key or os.environ.get("MASSIVE_API_KEY")
        if not api_key:
            return _missing_api_key_error("xctx dry-run", purpose="live ticker dry-run")
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
            )
        except ValueError as exc:
            return _unresolved_ticker_lookup_error(args, str(exc))
        trace = run_backfill_source_dry_run_trace(source, max_rounds=args.max_rounds)
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
        request_log = _request_log(client)
    elif args.ohlcv_series_id is not None:
        if not args.db:
            return _input_repair_error(
                "xctx dry-run",
                code="db_required_for_ohlcv_series_id",
                what_failed="An OHLCV series ID was provided alongside an empty SQLite reference-universe DB path.",
                minimal_fix="Pass --db pointing to the SQLite DB that contains reference_universe_snapshots.",
                suggested_inputs=[
                    {"ohlcv_series_id": args.ohlcv_series_id, "db": "{db}"}
                ],
            )
        api_key = args.api_key or os.environ.get("MASSIVE_API_KEY")
        if not api_key:
            return _missing_api_key_error(
                "xctx dry-run", purpose="live OHLCV series ID dry-run"
            )
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
            )
        except (sqlite3.Error, ValueError) as exc:
            return _series_id_lookup_repair_error(args, str(exc))
        trace = run_backfill_source_dry_run_trace(source, max_rounds=args.max_rounds)
        reads = [
            args.db,
            "massive.ticker_events",
            "massive.reference_boundary",
            "massive.bar_probe",
            "massive.identity_scan",
            "massive.ticker_replacement",
        ]
        if not args.api_key:
            reads.append("env:MASSIVE_API_KEY")
        request_log = _request_log(client)
        selected_identity = _reference_snapshot_identity_payload(snapshot)
    envelope = result_envelope("xctx dry-run", trace.result)
    envelope["effects"] = _effects(reads=reads, writes=[])
    envelope["rounds"] = _rounds(trace.rounds)
    envelope["request_log"] = request_log
    if selected_identity is not None:
        envelope["selected_identity"] = selected_identity
    return envelope


def _resolve_identity(
    args: argparse.Namespace, parser: argparse.ArgumentParser
) -> dict[str, Any]:
    query = args.query_option or args.query_arg
    if not query:
        return _input_repair_error(
            "xctx resolve-identity",
            code="query_required",
            what_failed="Identity query was empty.",
            minimal_fix="Pass --query with a ticker, company name, CIK, FIGI, or OHLCV series ID.",
            suggested_inputs=[{"query": "Alphabet", "source": args.source}],
        )
    if args.limit < 1:
        return _input_repair_error(
            "xctx resolve-identity",
            code="limit_not_positive",
            what_failed="Identity search limit must be positive.",
            minimal_fix="Pass --limit 1 or greater.",
            suggested_inputs=[{"query": query, "limit": 25}],
        )
    if args.source == "db":
        if not args.db:
            return _input_repair_error(
                "xctx resolve-identity",
                code="db_required_for_db_source",
                what_failed="DB-backed identity search was requested alongside an empty --db value.",
                minimal_fix="Pass --db pointing to a SQLite DB with reference_universe_snapshots.",
                suggested_inputs=[{"query": query, "source": "db", "db": "{db}"}],
            )
        try:
            result = sqlite_identity_search(args.db, query, limit=args.limit)
        except sqlite3.Error as exc:
            return _identity_search_repair_error(str(exc), reads=[args.db], db=args.db)
        payload = result.to_dict()
        payload.update(
            {
                "protocol_version": PROTOCOL_VERSION,
                "ok": True,
                "command": "xctx resolve-identity",
                "result_type": "IdentityCandidateList",
                "effects": _effects(reads=[args.db], writes=[]),
                "next_actions": _identity_next_actions(
                    payload["candidates"], query=query, source=args.source, db=args.db
                ),
            }
        )
        return payload
    api_key = args.api_key or os.environ.get("MASSIVE_API_KEY")
    if not api_key:
        return _missing_api_key_error(
            "xctx resolve-identity", purpose="live identity search"
        )
    client = MassiveReadOnlyClient(
        MassiveProviderConfig(api_key=api_key, base_url=args.base_url)
    )
    result = live_identity_search(
        query, client=client, as_of_date=args.as_of_date, limit=args.limit
    )
    reads = ["massive.reference_tickers"]
    if not args.api_key:
        reads.append("env:MASSIVE_API_KEY")
    payload = result.to_dict()
    payload.update(
        {
            "protocol_version": PROTOCOL_VERSION,
            "ok": True,
            "command": "xctx resolve-identity",
            "result_type": "IdentityCandidateList",
            "effects": _effects(reads=reads, writes=[]),
            "request_log": _request_log(client),
            "next_actions": _identity_next_actions(
                payload["candidates"], query=query, source=args.source, db=args.db
            ),
        }
    )
    return payload


def _universe_status(
    args: argparse.Namespace, parser: argparse.ArgumentParser
) -> dict[str, Any]:
    status = universe_status(args.db)
    return {
        "protocol_version": PROTOCOL_VERSION,
        "ok": True,
        "command": "xctx universe-status",
        "result_type": "UniverseStatus",
        "status": status,
        "effects": _effects(reads=[args.db], writes=[]),
        "next_actions": _universe_status_next_actions(status),
    }


def _quality_audit(
    args: argparse.Namespace, parser: argparse.ArgumentParser
) -> dict[str, Any]:
    grain_error = _bar_grain_input_error("xctx quality-audit", args)
    if grain_error:
        return grain_error
    try:
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
        return _sqlite_db_repair_error(
            "xctx quality-audit",
            str(exc),
            db=args.db,
            purpose="quality audit",
        )
    if not report.get("next_actions") and report.get("active_reference_series") == 0:
        report["next_actions"] = _empty_quality_audit_next_actions(args.db)
    view_payload = _quality_audit_view(report, view=args.view, detail_limit=args.limit)
    payload = {
        "protocol_version": PROTOCOL_VERSION,
        "ok": True,
        "cognition_unit": "status",
        "command": "xctx quality-audit",
        "result_type": "QualityAudit",
        "view": args.view,
        **view_payload,
    }
    if args.view == "extra_detail":
        payload["effects"] = _effects(reads=[args.db], writes=[])
    return payload


def _catch_up_plan(
    args: argparse.Namespace, parser: argparse.ArgumentParser
) -> dict[str, Any]:
    grain_error = _bar_grain_input_error("xctx catch-up-plan", args)
    if grain_error:
        return grain_error
    if args.detail_limit < 0:
        return _input_repair_error(
            "xctx catch-up-plan",
            code="detail_limit_negative",
            what_failed="detail_limit must be non-negative.",
            minimal_fix="Pass --detail-limit 0 or greater.",
            reads=[args.db],
        )
    try:
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
    except sqlite3.Error as exc:
        return _sqlite_db_repair_error(
            "xctx catch-up-plan",
            str(exc),
            db=args.db,
            purpose="catch-up planning",
        )
    except ValueError as exc:
        return _input_repair_error(
            "xctx catch-up-plan",
            code="catch_up_plan_invalid_input",
            what_failed=str(exc),
            minimal_fix="Adjust worker, batch, target, or filter arguments and rerun catch-up-plan.",
            reads=[args.db],
        )
    payload = _catch_up_plan_view(plan, view=args.view, detail_limit=args.detail_limit)
    payload.update(
        {
            "protocol_version": PROTOCOL_VERSION,
            "ok": True,
            "cognition_unit": "plan",
            "command": "xctx catch-up-plan",
            "result_type": "CatchUpPlan",
            "view": args.view,
        }
    )
    if args.view == "extra_detail":
        payload["effects"] = _effects(reads=[args.db], writes=[])
    return payload


def _quality_audit_view(
    report: dict[str, Any], *, view: str, detail_limit: int
) -> dict[str, Any]:
    if view == "extra_detail":
        payload = dict(report)
        payload["views"] = _quality_audit_views()
        return payload
    payload = _quality_audit_simple_view(report)
    if view == "detail":
        issues = list(report.get("issues") or [])
        payload["issues"] = issues[:detail_limit]
        payload["detail_limit"] = detail_limit
        payload["omitted_issue_row_count"] = max(len(issues) - detail_limit, 0)
        payload["next_actions"] = list(report.get("next_actions") or [])
    return payload


def _quality_audit_simple_view(report: dict[str, Any]) -> dict[str, Any]:
    keys = (
        "db",
        "bar_grain",
        "multiplier",
        "timespan",
        "latest_reference_snapshot_as_of_date",
        "global_min_bar_date",
        "global_max_bar_date",
        "stale_before",
        "active_reference_series",
        "matched_series_count",
        "issue_count",
        "category_counts",
        "unfiltered_issue_count",
        "unfiltered_category_counts",
        "filters",
    )
    payload = {key: report[key] for key in keys if key in report}
    payload["omitted_issue_row_count"] = len(report.get("issues") or [])
    payload["next_moves"] = [
        _compact_action_record(action) for action in report.get("next_actions") or []
    ]
    return payload


def _catch_up_plan_view(plan: Any, *, view: str, detail_limit: int) -> dict[str, Any]:
    full = plan.to_dict()
    if view == "extra_detail":
        full["views"] = _catch_up_plan_views()
        return full
    payload = _catch_up_plan_summary_view(full)
    if view == "detail":
        payload.update(
            {
                "target_detail": full["targets"][:detail_limit],
                "batch_detail": full["batches"][:detail_limit],
                "detail_limit": detail_limit,
                "omitted_target_count": max(len(full["targets"]) - detail_limit, 0),
                "omitted_batch_count": max(len(full["batches"]) - detail_limit, 0),
                "next_actions": full.get("next_actions") or [],
            }
        )
    return payload


def _catch_up_plan_summary_view(full: dict[str, Any]) -> dict[str, Any]:
    quality = dict(full.get("quality_audit_summary") or {})
    category_counts = dict(quality.get("category_counts") or {})
    executable_counts = {
        key: category_counts.get(key, 0)
        for key in sorted(EXECUTABLE_CATCH_UP_CATEGORIES)
        if category_counts.get(key, 0)
    }
    review_only_counts = {
        key: category_counts.get(key, 0)
        for key in sorted(REVIEW_ONLY_CATCH_UP_CATEGORIES)
        if category_counts.get(key, 0)
    }
    return {
        "schema_version": full.get("schema_version"),
        "db": full.get("db"),
        "generated_at_utc": full.get("generated_at_utc"),
        "reference_snapshot_as_of_date": full.get("reference_snapshot_as_of_date"),
        "quality_audit_summary": quality,
        "target_policy": full.get("target_policy"),
        "target_count": full.get("target_count"),
        "batch_count": len(full.get("batches") or []),
        "worker_count": full.get("worker_count"),
        "batch_size": full.get("batch_size"),
        "run_dir": full.get("run_dir"),
        "category_counts": category_counts,
        "executable_category_counts": executable_counts,
        "review_only_category_counts": review_only_counts,
        "excluded_issue_count": max(
            int(quality.get("issue_count") or 0) - int(full.get("target_count") or 0), 0
        ),
        "commit_expected_reads": full.get("commit_expected_reads"),
        "commit_expected_writes": full.get("commit_expected_writes"),
        "next_moves": [
            _compact_action_record(action) for action in full.get("next_actions") or []
        ],
        "repair_hints": full.get("repair_hints") or [],
        "monitoring": _compact_monitoring(),
    }


def _quality_audit_views() -> dict[str, str]:
    return {
        "simple": "Counts, filters, category totals, and compact next moves while omitting issue rows.",
        "detail": "Simple status plus bounded issue rows and full next actions.",
        "extra_detail": "Full bounded issue rows controlled by --limit.",
    }


def _catch_up_plan_views() -> dict[str, str]:
    return {
        "simple": "Counts, category totals, run_dir, monitoring, and compact next moves.",
        "detail": "Simple plan plus bounded target_detail and batch_detail controlled by --detail-limit.",
        "extra_detail": "Complete materialized target and batch lists.",
    }


def _catch_up_status(
    args: argparse.Namespace, parser: argparse.ArgumentParser
) -> dict[str, Any]:
    run_dir = args.run_dir
    if args.latest:
        try:
            run_list = catch_up_runs(args.run_root, limit=1)
        except (OSError, ValueError, json.JSONDecodeError) as exc:
            return _catch_up_runs_repair_error(args.run_root, str(exc))
        runs = list(run_list.get("runs") or [])
        if not runs:
            return {
                "protocol_version": PROTOCOL_VERSION,
                "ok": False,
                "command": "xctx catch-up-status",
                "result_type": "RepairError",
                "error": "Catch-up run artifacts were absent.",
                "errors": [
                    {
                        "code": "catch_up_run_absent",
                        "what_failed": "Catch-up run artifacts were absent under the requested run root.",
                        "minimal_fix": "Use xctx catch-up-plan to prepare a run, or pass --run-dir for a known run directory.",
                    }
                ],
                "effects": _effects(
                    reads=[str(run_list.get("run_root") or args.run_root or "")],
                    writes=[],
                ),
                "next_actions": [_plan_catch_up_run_action()],
            }
        run_dir = str(runs[0]["run_dir"])
    try:
        status = catch_up_run_status(run_dir)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        return {
            "protocol_version": PROTOCOL_VERSION,
            "ok": False,
            "command": "xctx catch-up-status",
            "result_type": "RepairError",
            "error": str(exc),
            "errors": [
                {
                    "code": "catch_up_run_unreadable",
                    "what_failed": f"Catch-up run artifacts were unreadable at {run_dir}.",
                    "minimal_fix": "Pass --run-dir for a directory produced by stock-universe catch-up --commit.",
                    "detail": str(exc),
                }
            ],
            "effects": _effects(reads=[str(run_dir)], writes=[]),
            "next_actions": [_plan_catch_up_run_action()],
        }
    view_payload = _catch_up_status_view(status, view=args.view)
    view_payload.update(
        {
            "protocol_version": PROTOCOL_VERSION,
            "cognition_unit": "status",
            "command": "xctx catch-up-status",
            "view": args.view,
        }
    )
    if args.view == "extra_detail":
        view_payload["effects"] = _effects(reads=[str(run_dir)], writes=[])
    return view_payload


def _catch_up_status_view(status: dict[str, Any], *, view: str) -> dict[str, Any]:
    if view == "extra_detail":
        payload = dict(status)
        payload["views"] = _catch_up_status_views()
        return payload
    progress_events = list(status.get("progress_events") or [])
    last_progress = progress_events[-1] if progress_events else {}
    payload = {
        key: status[key]
        for key in (
            "schema_version",
            "ok",
            "result_type",
            "run_dir",
            "plan_hash",
            "db",
            "state",
            "persisted_state",
            "stale_running",
            "target_count",
            "batch_count",
            "completed_batch_count",
            "counts",
            "started_at_utc",
            "finished_at_utc",
            "hard_error",
            "resource_stop",
            "operator_stop",
            "plan_artifact",
            "status_artifact",
        )
        if key in status
    }
    failed_results = list(status.get("failed_results") or [])
    payload["failed_result_count"] = len(failed_results)
    if view == "detail":
        payload["failed_result_detail"] = [
            _compact_failed_result(item) for item in failed_results[:5]
        ]
        payload["db_reconciliation"] = status.get("db_reconciliation")
        payload["reconciliation_repair"] = status.get("reconciliation_repair")
        payload["last_resource_check"] = status.get("last_resource_check")
        payload["next_actions"] = list(
            status.get("repairs") or status.get("post_run_next_actions") or []
        )
    payload["post_run_next_actions"] = [
        _compact_action_record(action)
        for action in status.get("post_run_next_actions") or []
    ]
    payload["repairs"] = [
        _compact_action_record(action) for action in status.get("repairs") or []
    ]
    payload["progress_event_count"] = len(progress_events)
    payload["last_progress_event"] = last_progress
    payload["batch_artifact_count"] = len(status.get("batch_artifacts") or [])
    payload["monitoring"] = _compact_monitoring(status.get("agent_reporting"))
    return payload


def _catch_up_status_views() -> dict[str, str]:
    return {
        "simple": "Status, counts, problem flags, latest progress event, and compact monitoring.",
        "detail": "Simple status plus reconciliation, resources, failed-result detail, and full next actions.",
        "extra_detail": "Complete batch_artifacts and progress_events arrays.",
    }


def _compact_monitoring(
    agent_reporting: dict[str, Any] | None = None,
) -> dict[str, Any]:
    routine = dict((agent_reporting or {}).get("routine") or {})
    return {
        "poll_seconds": int(routine.get("system_poll_seconds") or 60),
        "first_update_seconds": int(routine.get("first_update_seconds") or 180),
        "routine_update_seconds": int(routine.get("default_update_seconds") or 300),
        "quiet_when_healthy": bool(routine.get("quiet_when_healthy", True)),
    }


def _compact_action_record(action: dict[str, Any]) -> dict[str, Any]:
    command = dict(action.get("command") or {})
    return {
        "name": str(action.get("name") or ""),
        "kind": str(action.get("kind") or ""),
        "command_name": str(command.get("name") or ""),
        "requires_approval": bool(action.get("requires_approval")),
        "reason": str(action.get("reason") or ""),
    }


def _compact_failed_result(result: dict[str, Any]) -> dict[str, Any]:
    reason = str(result.get("reason") or result.get("error") or "")
    if reason.startswith("blocked plans"):
        reason = "blocked plan"
    return {
        "ohlcv_series_id": int(result.get("ohlcv_series_id") or 0),
        "ticker": str((result.get("catch_up_target") or {}).get("ticker") or ""),
        "status": str(result.get("status") or ""),
        "plan_status": str(result.get("plan_status") or ""),
        "reason": reason,
    }


def _catch_up_runs(
    args: argparse.Namespace, parser: argparse.ArgumentParser
) -> dict[str, Any]:
    if args.limit < 1:
        return _input_repair_error(
            "xctx catch-up-runs",
            code="limit_not_positive",
            what_failed="Catch-up run listing limit must be positive.",
            minimal_fix="Pass --limit 1 or greater.",
            suggested_inputs=[{"limit": 5}],
        )
    try:
        payload = catch_up_runs(args.run_root, limit=args.limit)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        return _catch_up_runs_repair_error(args.run_root, str(exc))
    next_actions = [
        *_promoted_catch_up_run_actions(payload),
        _inspect_latest_catch_up_status_action(str(payload.get("run_root") or "")),
        _plan_catch_up_run_action(),
    ]
    return _catch_up_runs_view(
        payload,
        view=args.view,
        next_actions=next_actions,
        read_target=str(payload.get("run_root") or args.run_root or ""),
    )


def _catch_up_runs_view(
    payload: dict[str, Any],
    *,
    view: str,
    next_actions: list[dict[str, Any]],
    read_target: str,
) -> dict[str, Any]:
    base: dict[str, Any] = {
        "protocol_version": PROTOCOL_VERSION,
        "ok": bool(payload.get("ok", True)),
        "cognition_unit": "audit",
        "command": "xctx catch-up-runs",
        "result_type": payload.get("result_type", "CatchUpRunList"),
        "view": view,
        "run_root": payload.get("run_root"),
        "limit": payload.get("limit"),
        "run_count": payload.get("run_count"),
        "errors": payload.get("errors") or [],
        "runs": [_compact_catch_up_run(run) for run in payload.get("runs") or []],
        "next_moves": [_compact_action_record(action) for action in next_actions],
    }
    if view in {"detail", "extra_detail"}:
        base["next_actions"] = next_actions
    if view == "extra_detail":
        base["runs"] = list(payload.get("runs") or [])
        base["effects"] = _effects(reads=[read_target], writes=[])
    return base


def _compact_catch_up_run(run: dict[str, Any]) -> dict[str, Any]:
    return {
        "run_dir": run.get("run_dir"),
        "state": run.get("state"),
        "ok": run.get("ok"),
        "target_count": run.get("target_count"),
        "counts": run.get("counts"),
        "started_at_utc": run.get("started_at_utc"),
        "finished_at_utc": run.get("finished_at_utc"),
        "hard_error": run.get("hard_error"),
        "resource_stop": run.get("resource_stop"),
        "operator_stop": run.get("operator_stop"),
        "requires_reconciliation": run.get("requires_reconciliation"),
    }


def _promoted_catch_up_run_actions(payload: dict[str, Any]) -> list[dict[str, Any]]:
    runs = list(payload.get("runs") or [])
    if not runs:
        return []
    latest = dict(runs[0])
    counts = dict(latest.get("counts") or {})
    pending = int(counts.get("pending") or 0)
    run_dir = str(latest.get("run_dir") or "")
    if pending <= 0 or not run_dir:
        return []
    repair_names = {
        str(action.get("name") or "") for action in latest.get("repair_actions") or []
    }
    if "resume-catch-up" not in repair_names:
        return []
    return [
        {
            "name": "resume-latest-catch-up-run",
            "kind": "command",
            "command": {
                "name": "stock-universe catch-up",
                "description": "Resume the latest stopped catch-up run using completed batch artifacts.",
                "args": {
                    "run_dir": run_dir,
                    "commit": True,
                    "resume": True,
                    "fail_fast": True,
                },
                "reads": [run_dir],
                "writes": [run_dir],
            },
            "effects": [
                {
                    "kind": "write",
                    "target": run_dir,
                    "description": "Continue pending catch-up targets from the existing run.",
                }
            ],
            "requires_approval": True,
            "reason": f"Latest catch-up run is {latest.get('state') or 'stopped'} with {pending} pending targets.",
        }
    ]


def _inspect_latest_catch_up_status_action(run_root: str) -> dict[str, Any]:
    return {
        "name": "inspect-latest-catch-up-status",
        "kind": "command",
        "command": {
            "name": "xctx catch-up-status",
            "description": "Read the latest catch-up run status from this run root.",
            "args": {"latest": True, "run_root": run_root},
            "reads": [run_root],
            "writes": [],
        },
        "effects": [
            {
                "kind": "read",
                "target": run_root,
                "description": "Read catch-up run artifacts.",
            }
        ],
        "requires_approval": False,
    }


def _catch_up_runs_repair_error(run_root: str | None, detail: str) -> dict[str, Any]:
    root = str(run_root or "")
    return {
        "protocol_version": PROTOCOL_VERSION,
        "ok": False,
        "command": "xctx catch-up-runs",
        "result_type": "RepairError",
        "error": detail,
        "errors": [
            {
                "code": "catch_up_run_root_unreadable",
                "what_failed": "Catch-up run root was unreadable.",
                "minimal_fix": "Pass --run-root for a readable catch-up run artifact root.",
                "detail": detail,
            }
        ],
        "effects": _effects(reads=[root], writes=[]),
        "next_actions": [_plan_catch_up_run_action()],
    }


def _plan_catch_up_run_action() -> dict[str, Any]:
    return {
        "name": "plan-catch-up-run",
        "kind": "command",
        "command": {
            "name": "xctx catch-up-plan",
            "description": "Materialize a new read-oriented catch-up plan.",
            "args": {"view": "simple"},
            "reads": [canonical_db_text()],
            "writes": [],
        },
        "effects": [
            {
                "kind": "read",
                "target": canonical_db_text(),
                "description": "Read quality-audit state.",
            }
        ],
        "requires_approval": False,
    }


def _identity_search_repair_error(
    error: str, *, reads: list[str], db: str
) -> dict[str, Any]:
    db_args = _db_arg_if_override(db)
    return {
        "protocol_version": PROTOCOL_VERSION,
        "ok": False,
        "command": "xctx resolve-identity",
        "result_type": "RepairError",
        "error": error,
        "repairs": [
            {
                "name": "provide-existing-sqlite-db",
                "evidence_kind": "identity_search",
                "request": {"db": db},
                "effect": {
                    "kind": "read",
                    "target": db,
                    "description": "Provide an existing SQLite DB.",
                },
                "reason": "xctx resolve-identity uses an existing DB and leaves schema setup to validate-db.",
                "command": {
                    "name": "stock-universe validate-db",
                    "description": "Initialize or validate DB through the production CLI before searching persisted identities.",
                    "args": db_args,
                    "reads": [db],
                    "writes": [db],
                },
            }
        ],
        "effects": _effects(reads=reads, writes=[]),
    }


def _sqlite_db_repair_error(
    command: str, error: str, *, db: str, purpose: str
) -> dict[str, Any]:
    db_args = _db_arg_if_override(db)
    return {
        "protocol_version": PROTOCOL_VERSION,
        "ok": False,
        "command": command,
        "result_type": "RepairError",
        "error": error,
        "errors": [
            {
                "code": "sqlite_db_unreadable",
                "what_failed": f"SQLite DB was unavailable for {purpose}.",
                "minimal_fix": "Pass an existing initialized DB, or initialize one with stock-universe validate-db.",
                "suggested_inputs": [{"db": db}],
            }
        ],
        "repairs": [
            {
                "name": "provide-existing-sqlite-db",
                "kind": "repair",
                "command": {
                    "name": "stock-universe validate-db",
                    "description": "Initialize or validate DB through the production CLI.",
                    "args": db_args,
                    "reads": [db],
                    "writes": [db],
                },
                "effects": [
                    {
                        "kind": "read",
                        "target": db,
                        "description": "Read SQLite schema state.",
                    },
                    {
                        "kind": "write",
                        "target": db,
                        "description": "Create schema when the DB is missing.",
                    },
                ],
                "requires_approval": True,
                "reason": f"{command} uses an existing SQLite DB and leaves schema setup to validate-db.",
            }
        ],
        "effects": _effects(reads=[db], writes=[]),
    }


def _series_id_lookup_repair_error(
    args: argparse.Namespace, error: str
) -> dict[str, Any]:
    db_target = _db_target(args.db)
    return _input_repair_error(
        "xctx dry-run",
        code="ohlcv_series_id_not_found",
        what_failed=f"ohlcv_series_id {args.ohlcv_series_id} was unavailable in the reference universe.",
        minimal_fix="Select an existing DB-backed identity candidate, or refresh the reference universe and retry.",
        suggested_inputs=[{"ohlcv_series_id": args.ohlcv_series_id, "db": db_target}],
        reads=[db_target],
        detail=error,
        repairs=_reference_universe_repair_actions(
            db=db_target,
            dry_run_reason="The selected OHLCV series ID is outside the current DB reference universe.",
        ),
    )


def _unresolved_ticker_lookup_error(
    args: argparse.Namespace, error: str
) -> dict[str, Any]:
    db_target = _db_target(args.db)
    db_args = _db_arg_if_override(db_target)
    return _input_repair_error(
        "xctx dry-run",
        code="ohlcv_series_id_unresolved",
        what_failed=f"Ticker dry-run resolved zero existing ohlcv_series_id values for {args.ticker}.",
        minimal_fix="Run a committing workflow that allocates the natural key, then retry the read-oriented dry-run.",
        suggested_inputs=[{"ticker": args.ticker, "db": db_target}],
        reads=[db_target, f"massive.reference_ticker:{args.ticker}"],
        detail=error,
        repairs=[
            {
                "name": "commit-reference-universe-update",
                "kind": "command",
                "command": {
                    "name": "stock-universe update-reference-universe",
                    "description": "Populate the central ohlcv_series_id lookup from the reference universe.",
                    "args": {
                        **db_args,
                        "limit": 1000,
                        "max_pages": 100,
                        "commit": True,
                    },
                    "reads": ["Massive API"],
                    "writes": [db_target],
                },
                "effects": [
                    {
                        "kind": "read",
                        "target": "Massive API",
                        "description": "Fetch reference snapshot candidates.",
                    },
                    {
                        "kind": "write",
                        "target": db_target,
                        "description": "Allocate ohlcv_series_id rows and persist reference snapshots.",
                    },
                ],
                "requires_approval": True,
                "reason": "Read-oriented ticker dry-runs use central IDs that already exist.",
            },
            {
                "name": "execute-ticker-backfill",
                "kind": "command",
                "command": {
                    "name": "stock-universe backfill",
                    "description": "Execute a ticker-seeded backfill, allocating the central ohlcv_series_id as part of the mutation.",
                    "args": {**db_args, "ticker": [args.ticker], "strict": True},
                    "reads": ["Massive API"],
                    "writes": [db_target],
                },
                "effects": [
                    {
                        "kind": "read",
                        "target": "Massive API",
                        "description": "Resolve and execute the ticker seed.",
                    },
                    {
                        "kind": "write",
                        "target": db_target,
                        "description": "Allocate lookup ID and persist backfill outputs.",
                    },
                ],
                "requires_approval": True,
                "reason": "Backfill is a mutating command and may allocate a new ohlcv_series_id.",
            },
        ],
    )


def _identity_next_actions(
    candidates: list[dict[str, Any]],
    *,
    query: str,
    source: str,
    db: str | None = None,
) -> list[dict[str, Any]]:
    actions = [
        {
            "name": "review-identity-candidates",
            "kind": "inspection",
            "command": {
                "name": "xctx resolve-identity",
                "description": "Inspect candidate identities, match ranks, and match reasons.",
                "args": {"query": query, "source": source},
                "reads": [],
                "writes": [],
            },
            "effects": [
                {
                    "kind": "none",
                    "target": "stdout",
                    "description": "Render candidate evidence.",
                }
            ],
            "requires_approval": False,
        }
    ]
    if not candidates:
        if source == "db":
            actions.extend(_reference_universe_repair_actions(db=db))
        actions.append(
            {
                "name": "broaden-identity-query",
                "kind": "command",
                "command": {
                    "name": "xctx resolve-identity",
                    "description": "Search again with a broader ticker, company name, CIK, or FIGI.",
                    "args": {"query": query, "source": source},
                    "reads": ["Massive API or SQLite DB"],
                    "writes": [],
                },
                "effects": [
                    {
                        "kind": "read",
                        "target": "identity-source",
                        "description": "Read identity candidates.",
                    }
                ],
                "requires_approval": False,
            }
        )
        return actions
    actions.append(_selected_identity_bars_action(source=source, db=db))
    actions.append(_selected_identity_dry_run_action(source=source, db=db))
    if len(candidates) > 1:
        actions.append(
            {
                "name": "narrow-identity-query",
                "kind": "command",
                "command": {
                    "name": "xctx resolve-identity",
                    "description": "Search with a more precise ticker, CIK, FIGI, or company phrase.",
                    "args": {"query": query, "source": source},
                    "reads": ["Massive API or SQLite DB"],
                    "writes": [],
                },
                "effects": [
                    {
                        "kind": "read",
                        "target": "identity-source",
                        "description": "Read narrower identity candidates.",
                    }
                ],
                "requires_approval": False,
                "reason": "Multiple identity candidates matched the query.",
            }
        )
    return actions


def _selected_identity_bars_action(*, source: str, db: str | None) -> dict[str, Any]:
    db_target = _db_target(db)
    if source == "db":
        return {
            "name": "observe-selected-ohlcv-series-bars",
            "kind": "command",
            "command": {
                "name": "xctx bars",
                "description": "Read a compact canonical OHLCV frame for one explicitly selected DB OHLCV series ID.",
                "args": {
                    "ohlcv_series_id": "{selected_candidate.ohlcv_series_id}",
                    **_db_arg_if_override(db_target),
                    "date": "{date}",
                },
                "reads": [db_target],
                "writes": [],
            },
            "effects": [
                {
                    "kind": "read",
                    "target": db_target,
                    "description": "Read canonical OHLCV bars.",
                }
            ],
            "requires_approval": False,
            "requires_selection": True,
            "selection_fields": [
                "ohlcv_series_id",
                "ticker",
                "composite_figi",
                "share_class_figi",
                "cik",
            ],
            "reason": "After DB identity resolution, price questions should use xctx bars with ohlcv_series_id; ticker is only an alias.",
        }
    return {
        "name": "resolve-db-identity-before-bars",
        "kind": "command",
        "command": {
            "name": "xctx resolve-identity",
            "description": "Resolve the candidate against the persisted DB before reading canonical bars.",
            "args": {
                "query": "{selected_candidate.ticker}",
                "source": "db",
                **_db_arg_if_override(db_target),
            },
            "reads": [db_target],
            "writes": [],
        },
        "effects": [
            {
                "kind": "read",
                "target": db_target,
                "description": "Find the persisted OHLCV series ID.",
            }
        ],
        "requires_approval": False,
        "requires_selection": True,
        "selection_fields": ["ticker", "composite_figi", "share_class_figi", "cik"],
        "reason": "Canonical bar observation is DB-backed and requires a persisted ohlcv_series_id.",
    }


def _selected_identity_dry_run_action(*, source: str, db: str | None) -> dict[str, Any]:
    if source == "db":
        db_target = _db_target(db)
        return {
            "name": "dry-run-selected-ohlcv-series-id",
            "kind": "command",
            "command": {
                "name": "xctx dry-run",
                "description": "Run an xctx read-oriented backfill dry-run for one explicitly selected DB OHLCV series ID.",
                "args": {
                    "ohlcv_series_id": "{selected_candidate.ohlcv_series_id}",
                    **_db_arg_if_override(db_target),
                    "max_rounds": DEFAULT_MAX_ROUNDS,
                },
                "reads": [db_target, "Massive API"],
                "writes": [],
            },
            "effects": [
                {
                    "kind": "read",
                    "target": db_target,
                    "description": "Load selected reference-universe identity.",
                },
                {
                    "kind": "read",
                    "target": "Massive API",
                    "description": "Collect planning evidence for selected identity.",
                },
            ],
            "requires_approval": False,
            "requires_selection": True,
            "selection_fields": [
                "ohlcv_series_id",
                "ticker",
                "composite_figi",
                "share_class_figi",
                "cik",
            ],
            "reason": (
                "DB identity search lists persisted candidates. For OHLCV reporting, select and carry forward "
                "ohlcv_series_id; ticker is an alias and may not cover historical ticker labels."
            ),
        }
    return {
        "name": "dry-run-selected-ticker",
        "kind": "command",
        "command": {
            "name": "xctx dry-run",
            "description": "Run an xctx read-oriented backfill dry-run for one explicitly selected candidate ticker.",
            "args": {
                "ticker": "{selected_candidate.ticker}",
                "max_rounds": DEFAULT_MAX_ROUNDS,
            },
            "reads": ["Massive API"],
            "writes": [],
        },
        "effects": [
            {
                "kind": "read",
                "target": "Massive API",
                "description": "Collect planning evidence for selected ticker.",
            }
        ],
        "requires_approval": False,
        "requires_selection": True,
        "selection_fields": [
            "natural_key",
            "lookup_status",
            "ticker",
            "composite_figi",
            "share_class_figi",
            "cik",
        ],
        "reason": "Identity search lists candidates for operator selection among share classes, ETFs, or same-name securities.",
    }


def _reference_universe_repair_actions(
    *,
    db: str | None,
    dry_run_reason: str = "The DB identity catalog returned zero candidates for this query.",
) -> list[dict[str, Any]]:
    db_target = _db_target(db)
    db_args = _db_arg_if_override(db_target)
    return [
        {
            "name": "dry-run-reference-universe-update",
            "kind": "command",
            "command": {
                "name": "stock-universe update-reference-universe",
                "description": "Rehearse a bounded live reference snapshot update before mutating SQLite.",
                "args": {**db_args, "limit": 1000, "max_pages": 100},
                "reads": ["Massive API"],
                "writes": [],
            },
            "effects": [
                {
                    "kind": "read",
                    "target": "Massive API",
                    "description": "Fetch reference snapshot candidates.",
                }
            ],
            "requires_approval": False,
            "reason": dry_run_reason,
        },
        {
            "name": "commit-reference-universe-update",
            "kind": "command",
            "command": {
                "name": "stock-universe update-reference-universe",
                "description": "Persist fetched reference snapshots, then validate SQLite.",
                "args": {**db_args, "limit": 1000, "max_pages": 100, "commit": True},
                "reads": ["Massive API"],
                "writes": [db_target],
            },
            "effects": [
                {
                    "kind": "read",
                    "target": "Massive API",
                    "description": "Fetch reference snapshot candidates.",
                },
                {
                    "kind": "write",
                    "target": db_target,
                    "description": "Upsert reference_universe_snapshots rows.",
                },
            ],
            "requires_approval": True,
            "reason": "DB reference maintenance is a durable mutation and must be explicit.",
        },
    ]


def _bars(args: argparse.Namespace, parser: argparse.ArgumentParser) -> dict[str, Any]:
    grain_error = _bar_grain_input_error("xctx bars", args)
    if grain_error:
        return grain_error
    grain = normalize_bar_grain(args.bar_grain)
    if args.limit < 1:
        return _input_repair_error(
            "xctx bars",
            code="limit_not_positive",
            what_failed="Bar observation limit must be positive.",
            minimal_fix="Pass --limit 1 or greater.",
            suggested_inputs=[{"limit": 5}],
            reads=[args.db],
        )
    if args.date and (args.from_date or args.to_date):
        return _input_repair_error(
            "xctx bars",
            code="ambiguous_bar_date_scope",
            what_failed="A single --date was provided together with --from-date or --to-date.",
            minimal_fix="Use either --date for one bar or --from-date/--to-date for a range.",
            suggested_inputs=[
                {"date": args.date},
                {"from_date": args.from_date, "to_date": args.to_date},
            ],
            reads=[args.db],
        )
    date_error = _bar_date_input_error(args)
    if date_error:
        return date_error
    if args.from_date and args.to_date and args.from_date > args.to_date:
        return _input_repair_error(
            "xctx bars",
            code="invalid_bar_date_range",
            what_failed="The requested bar date range has from_date after to_date.",
            minimal_fix="Pass dates in inclusive ascending order.",
            suggested_inputs=[{"from_date": args.to_date, "to_date": args.from_date}],
            reads=[args.db],
        )
    try:
        selected_identity, candidates = _selected_bar_identity(args)
        if selected_identity is None:
            return _bar_identity_repair_error(args, candidates)
        series_id = int(selected_identity["ohlcv_series_id"])
        observations = _bar_observations_read_only(
            args.db,
            series_id=series_id,
            date=args.date,
            from_date=args.from_date,
            to_date=args.to_date,
            ticker_label=args.ticker_label,
            multiplier=grain.multiplier,
            timespan=grain.timespan,
            limit=args.limit,
            view=args.view,
        )
        identity = _bar_identity_snapshot(args.db, series_id) or selected_identity
    except sqlite3.Error as exc:
        return _sqlite_db_repair_error(
            "xctx bars", str(exc), db=args.db, purpose="bar observation"
        )
    scope = _bar_scope(args, series_id=series_id, grain=grain)
    actual_result = _bar_actual_result(args.db, observations, scope)
    return _bar_observation_payload(
        args,
        identity=identity,
        selected_identity=selected_identity,
        candidate_count=len(candidates),
        scope=scope,
        actual_result=actual_result,
        observations=observations,
    )


def _bar_date_input_error(args: argparse.Namespace) -> dict[str, Any] | None:
    invalid = []
    for attr in ("date", "from_date", "to_date"):
        value = getattr(args, attr, None)
        if not value:
            continue
        try:
            dt.date.fromisoformat(str(value))
        except ValueError:
            invalid.append({"field": attr, "value": value})
    if not invalid:
        return None
    return _input_repair_error(
        "xctx bars",
        code="invalid_bar_date",
        what_failed="One or more bar date inputs are not ISO calendar dates.",
        minimal_fix="Pass dates as YYYY-MM-DD.",
        suggested_inputs=[
            {"date": "2026-01-09"},
            {"from_date": "2026-01-01", "to_date": "2026-01-31"},
        ],
        reads=[args.db],
        detail=json.dumps({"invalid_dates": invalid}, sort_keys=True),
    )


def _bar_grain_input_error(
    command: str, args: argparse.Namespace
) -> dict[str, Any] | None:
    try:
        normalize_bar_grain(getattr(args, "bar_grain", "1d"))
    except ValueError as exc:
        return _input_repair_error(
            command,
            code="invalid_bar_grain",
            what_failed=str(exc),
            minimal_fix="Pass --bar-grain 1d, --bar-grain 1m, or --bar-grain 30m.",
            suggested_inputs=[
                {"bar_grain": "1d"},
                {"bar_grain": "1m"},
                {"bar_grain": "30m"},
            ],
            reads=[getattr(args, "db", "")],
        )
    return None


def _selected_bar_identity(
    args: argparse.Namespace,
) -> tuple[dict[str, Any] | None, list[dict[str, Any]]]:
    if args.ohlcv_series_id is not None:
        ohlcv_series_id = int(args.ohlcv_series_id)
        if ohlcv_series_id < 1:
            raise XctxCliError(
                _input_repair_error(
                    "xctx bars",
                    code="invalid_ohlcv_series_id",
                    what_failed="ohlcv_series_id must be a positive integer.",
                    minimal_fix="Pass --ohlcv-series-id with a positive canonical OHLCV series ID, or use --query for DB-backed identity resolution.",
                    suggested_inputs=[
                        {"ohlcv_series_id": "{selected_candidate.ohlcv_series_id}"},
                        {"query": args.query or "{query}"},
                    ],
                    reads=[args.db],
                )
            )
        if not _ohlcv_series_id_exists(args.db, ohlcv_series_id):
            raise XctxCliError(
                _input_repair_error(
                    "xctx bars",
                    code="ohlcv_series_id_not_found",
                    what_failed=f"ohlcv_series_id {ohlcv_series_id} was not found in the DB identity catalog.",
                    minimal_fix="Use xctx resolve-identity --source db, then rerun xctx bars with the selected ohlcv_series_id.",
                    suggested_inputs=[
                        {"query": args.query or "{query}", "source": "db"},
                        {"ohlcv_series_id": "{selected_candidate.ohlcv_series_id}"},
                    ],
                    reads=[args.db],
                )
            )
        return {"ohlcv_series_id": ohlcv_series_id, "lookup_status": "provided"}, []
    result = sqlite_identity_search(args.db, str(args.query), limit=max(args.limit, 5))
    candidates = list(result.to_dict().get("candidates") or [])
    selected = _select_bar_identity_candidate(str(args.query), candidates)
    if selected is None and candidates:
        raise XctxCliError(_ambiguous_bar_identity_repair_error(args, candidates))
    return selected, candidates


def _ohlcv_series_id_exists(db_path: str, ohlcv_series_id: int) -> bool:
    with connect_readonly_sqlite(db_path) as conn:
        row = conn.execute(
            "SELECT 1 FROM ohlcv_series_id_lookup WHERE ohlcv_series_id = ? LIMIT 1",
            (ohlcv_series_id,),
        ).fetchone()
    return row is not None


def _select_bar_identity_candidate(
    query: str, candidates: list[dict[str, Any]]
) -> dict[str, Any] | None:
    resolved = _dedupe_candidates_by_ohlcv_series_id(candidates)
    if not resolved:
        return None
    normalized_query = query.strip()
    if normalized_query.isdigit():
        exact_id = [
            candidate
            for candidate in resolved
            if int(candidate.get("ohlcv_series_id") or 0) == int(normalized_query)
        ]
        return exact_id[0] if len(exact_id) == 1 else None
    upper_query = normalized_query.upper()
    exact_ticker = _dedupe_candidates_by_ohlcv_series_id(
        [
            candidate
            for candidate in resolved
            if str(candidate.get("ticker") or "").upper() == upper_query
        ]
    )
    if len(exact_ticker) == 1:
        return exact_ticker[0]
    exact_common_stock = _dedupe_candidates_by_ohlcv_series_id(
        [
            candidate
            for candidate in exact_ticker
            if str(candidate.get("security_type") or "").upper() == "CS"
            and str(candidate.get("active", candidate.get("active_flag", 1))).lower()
            not in {"0", "false", "none"}
        ]
    )
    if len(exact_common_stock) == 1:
        return exact_common_stock[0]
    return resolved[0] if len(resolved) == 1 else None


def _dedupe_candidates_by_ohlcv_series_id(
    candidates: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    by_id: dict[int, dict[str, Any]] = {}
    for candidate in candidates:
        value = candidate.get("ohlcv_series_id")
        if value is None:
            continue
        try:
            ohlcv_series_id = int(value)
        except (TypeError, ValueError):
            continue
        by_id.setdefault(ohlcv_series_id, candidate)
    return list(by_id.values())


def _ambiguous_bar_identity_repair_error(
    args: argparse.Namespace, candidates: list[dict[str, Any]]
) -> dict[str, Any]:
    db_target = _db_target(args.db)
    candidate_preview = [
        {
            "ohlcv_series_id": candidate.get("ohlcv_series_id"),
            "ticker": candidate.get("ticker"),
            "company_name": candidate.get("company_name"),
            "cik": candidate.get("cik"),
            "composite_figi": candidate.get("composite_figi"),
            "share_class_figi": candidate.get("share_class_figi"),
            "security_type": candidate.get("security_type"),
            "match_reason": candidate.get("match_reason"),
        }
        for candidate in candidates[:10]
    ]
    return {
        "protocol_version": PROTOCOL_VERSION,
        "ok": False,
        "command": "xctx bars",
        "result_type": "RepairError",
        "error": "ambiguous DB identity for bar observation",
        "errors": [
            {
                "code": "ambiguous_ohlcv_identity",
                "what_failed": "The query matched multiple DB-backed OHLCV identities.",
                "minimal_fix": "Select one candidate and rerun xctx bars with --ohlcv-series-id.",
                "suggested_inputs": [
                    {
                        "ohlcv_series_id": "{selected_candidate.ohlcv_series_id}",
                        "date": args.date,
                    }
                ],
            }
        ],
        "candidate_count": len(candidates),
        "candidate_preview": candidate_preview,
        "effects": _effects(reads=[db_target], writes=[]),
        "next_actions": [
            {
                "name": "observe-explicit-ohlcv-series-id",
                "kind": "command",
                "command": {
                    "name": "xctx bars",
                    "description": "Read bars after explicitly selecting the canonical OHLCV series.",
                    "args": {
                        "ohlcv_series_id": "{selected_candidate.ohlcv_series_id}",
                        **_db_arg_if_override(db_target),
                        "date": args.date or "{date}",
                    },
                    "reads": [db_target],
                    "writes": [],
                },
                "effects": [
                    {
                        "kind": "read",
                        "target": db_target,
                        "description": "Read canonical OHLCV bars for the selected identity.",
                    }
                ],
                "requires_approval": False,
                "requires_selection": True,
                "selection_fields": [
                    "ohlcv_series_id",
                    "ticker",
                    "composite_figi",
                    "share_class_figi",
                    "cik",
                ],
                "reason": "Duplicate or multi-class matches must not be guessed for pricing questions.",
            }
        ],
    }


def _bar_identity_repair_error(
    args: argparse.Namespace, candidates: list[dict[str, Any]]
) -> dict[str, Any]:
    db_target = _db_target(args.db)
    repairs = [
        {
            "name": "resolve-db-identity",
            "kind": "command",
            "command": {
                "name": "xctx resolve-identity",
                "description": "Inspect DB-backed identity candidates before observing bars.",
                "args": {
                    "query": args.query or "{query}",
                    "source": "db",
                    **_db_arg_if_override(db_target),
                },
                "reads": [db_target],
                "writes": [],
            },
            "effects": [
                {
                    "kind": "read",
                    "target": db_target,
                    "description": "Read persisted identity candidates.",
                }
            ],
            "requires_approval": False,
            "reason": "xctx bars needs a selected canonical ohlcv_series_id.",
        }
    ]
    if not candidates:
        repairs.extend(_reference_universe_repair_actions(db=db_target))
    payload = _input_repair_error(
        "xctx bars",
        code="ohlcv_series_id_unresolved",
        what_failed="No DB-backed identity candidate with ohlcv_series_id was available for this bar observation.",
        minimal_fix="Resolve identity from the DB and rerun xctx bars with --ohlcv-series-id.",
        suggested_inputs=[
            {"query": args.query, "source": "db"},
            {"ohlcv_series_id": "{selected_candidate.ohlcv_series_id}"},
        ],
        reads=[db_target],
        repairs=repairs,
    )
    payload["succeeded"] = False
    payload["actual_result"] = "ticker_not_resolved"
    return payload


def _bar_scope(
    args: argparse.Namespace, *, series_id: int, grain: Any
) -> dict[str, Any]:
    scope = {
        "ohlcv_series_id": series_id,
        "query": args.query,
        "bar_grain": grain.bar_grain,
        "multiplier": grain.multiplier,
        "timespan": grain.timespan,
        "date": args.date,
        "from_date": args.from_date,
        "to_date": args.to_date,
        "ticker_label": args.ticker_label,
        "limit": args.limit,
        "canonical_scope": "ohlcv_series_id",
    }
    if not args.date and not args.from_date and not args.to_date:
        scope["latest"] = args.limit
    return scope


def _bar_actual_result(
    db_path: str, observations: list[dict[str, Any]], scope: dict[str, Any]
) -> str:
    if observations:
        return "bar_found"
    requested_date = scope.get("date")
    if requested_date:
        session = classify_us_equity_session(str(requested_date))
        if session != "trading_session":
            return session
    if (
        scope.get("ticker_label")
        and _bar_scope_unfiltered_bar_count(db_path, scope) > 0
    ):
        return "ticker_label_no_match"
    if not _bar_scope_has_trading_session(scope):
        return "this_is_not_a_trading_session"
    quality_category = _bar_series_quality_category(
        db_path, int(scope["ohlcv_series_id"]), scope
    )
    if quality_category == "data_not_loaded":
        return "data_not_loaded"
    inventory = _bar_series_inventory(db_path, int(scope["ohlcv_series_id"]), scope)
    if int(inventory.get("bar_count") or 0) == 0 and quality_category in {
        "provider_not_authorized",
        "provider_zero_bar_response_stale",
        "no_action_needed",
    }:
        return "series_not_covered"
    return "bar_expected_but_missing"


def _bar_scope_has_trading_session(scope: dict[str, Any]) -> bool:
    requested_date = scope.get("date")
    if requested_date:
        return is_us_equity_trading_date(str(requested_date))
    from_date = scope.get("from_date")
    to_date = scope.get("to_date")
    if from_date and to_date:
        return first_us_equity_trading_date_on_or_after(str(from_date)) <= str(to_date)
    return True


def _bar_scope_unfiltered_bar_count(db_path: str, scope: dict[str, Any]) -> int:
    clauses = ["b.ohlcv_series_id = ?", "b.multiplier = ?", "b.timespan = ?"]
    params: list[Any] = [
        int(scope["ohlcv_series_id"]),
        int(scope["multiplier"]),
        str(scope["timespan"]),
    ]
    if scope.get("date"):
        clauses.append("b.bar_date = ?")
        params.append(scope["date"])
    else:
        if scope.get("from_date"):
            clauses.append("b.bar_date >= ?")
            params.append(scope["from_date"])
        if scope.get("to_date"):
            clauses.append("b.bar_date <= ?")
            params.append(scope["to_date"])
    with connect_readonly_sqlite(db_path) as conn:
        row = conn.execute(
            f"SELECT COUNT(*) FROM v_ohlcv_bars_unified b WHERE {' AND '.join(clauses)}",
            params,
        ).fetchone()
    return int(row[0] if row else 0)


def _bar_series_inventory(
    db_path: str, series_id: int, scope: dict[str, Any]
) -> dict[str, Any]:
    with connect_readonly_sqlite(db_path) as conn:
        row = conn.execute(
            """
            SELECT
              COUNT(*) AS bar_count,
              COALESCE(MIN(bar_date), '') AS min_bar_date,
              COALESCE(MAX(bar_date), '') AS max_bar_date
            FROM v_ohlcv_bars_unified
            WHERE ohlcv_series_id = ?
              AND multiplier = ?
              AND timespan = ?
            """,
            (series_id, int(scope["multiplier"]), str(scope["timespan"])),
        ).fetchone()
    return (
        dict(row) if row else {"bar_count": 0, "min_bar_date": "", "max_bar_date": ""}
    )


def _bar_series_quality_category(
    db_path: str, series_id: int, scope: dict[str, Any]
) -> str:
    try:
        report = quality_audit(
            db_path,
            series_ids=(series_id,),
            include_healthy=True,
            limit=1,
            bar_grain=str(scope["bar_grain"]),
        )
    except sqlite3.Error:
        return ""
    issues = list(report.get("issues") or [])
    if issues:
        return str(issues[0].get("category") or "")
    counts = dict(report.get("category_counts") or {})
    if counts:
        return next(iter(counts))
    return ""


def _bar_observation_payload(
    args: argparse.Namespace,
    *,
    identity: dict[str, Any],
    selected_identity: dict[str, Any],
    candidate_count: int,
    scope: dict[str, Any],
    actual_result: str,
    observations: list[dict[str, Any]],
) -> dict[str, Any]:
    single_date = bool(scope.get("date"))
    ticker = str(identity.get("ticker") or selected_identity.get("ticker") or "")
    succeeded = actual_result == "bar_found"
    base: dict[str, Any] = {
        "succeeded": succeeded,
        "actual_result": actual_result,
        "ohlcv_series_id": int(scope["ohlcv_series_id"]),
        "ticker": ticker,
    }
    if scope.get("bar_grain") != "1d":
        base["bar_grain"] = scope.get("bar_grain")
    if single_date:
        base["date"] = scope.get("date")
        base["bar"] = observations[0] if observations else None
    else:
        base["bar_count"] = len(observations)
        base["bars"] = observations
    if args.view == "simple":
        return base

    if args.view in {"detail", "extra_detail"}:
        base.update(
            {
                "protocol_version": PROTOCOL_VERSION,
                "ok": True,
                "cognition_unit": "observation",
                "type": "bar_observation" if single_date else "bar_observation_list",
                "command": "xctx bars",
                "view": args.view,
                "result_type": "BarObservation"
                if single_date
                else "BarObservationList",
                "identity": identity,
                "identity_resolution": {
                    "query": args.query,
                    "selected_identity": selected_identity,
                    "candidate_count": candidate_count,
                },
                "scope": scope,
                "calendar": _bar_calendar_context(scope),
            }
        )
        actions = _bar_observation_next_actions(
            args.db, scope, actual_result=actual_result
        )
        if actions:
            base["next_actions"] = actions

    if args.view == "extra_detail":
        base["db"] = args.db
        base["effects"] = _effects(reads=[args.db], writes=[])
    return base


def _bar_calendar_context(scope: dict[str, Any]) -> dict[str, Any]:
    requested_date = scope.get("date")
    if not requested_date:
        return {"scope": "range_or_latest"}
    date_text = str(requested_date)
    session = classify_us_equity_session(date_text)
    return {
        "date": date_text,
        "calendar": "us_equity",
        "session": session,
        "is_trading_day": session == "trading_session",
        "previous_trading_date": previous_us_equity_trading_date(date_text),
        "next_trading_date": next_us_equity_trading_date(date_text),
    }


def _bar_identity_snapshot(db_path: str, series_id: int) -> dict[str, Any] | None:
    with connect_readonly_sqlite(db_path) as conn:
        row = conn.execute(
            """
            SELECT
              r.ohlcv_series_id,
              r.ticker,
              r.company_name,
              r.cik,
              r.composite_figi,
              r.share_class_figi,
              r.security_type,
              r.primary_exchange,
              r.market,
              r.locale,
              r.snapshot_as_of_date,
              r.identity_status
            FROM reference_universe_snapshots r
            WHERE r.ohlcv_series_id = ?
            ORDER BY r.snapshot_as_of_date DESC, r.active_flag DESC, r.reference_snapshot_id DESC
            LIMIT 1
            """,
            (series_id,),
        ).fetchone()
    return dict(row) if row else None


def _bar_observations_read_only(
    db_path: str,
    *,
    series_id: int,
    date: str | None,
    from_date: str | None,
    to_date: str | None,
    ticker_label: str | None,
    multiplier: int,
    timespan: str,
    limit: int,
    view: str,
) -> list[dict[str, Any]]:
    clauses = ["b.ohlcv_series_id = ?", "b.multiplier = ?", "b.timespan = ?"]
    params: list[Any] = [series_id, multiplier, timespan]
    latest_mode = not date and not from_date and not to_date
    if date:
        clauses.append("b.bar_date = ?")
        params.append(date)
    else:
        if from_date:
            clauses.append("b.bar_date >= ?")
            params.append(from_date)
        if to_date:
            clauses.append("b.bar_date <= ?")
            params.append(to_date)
    if ticker_label:
        clauses.append(
            """
            (
              b.ticker = ?
              OR EXISTS (
                SELECT 1
                FROM ticker_aliases ta
                WHERE ta.ohlcv_series_id = b.ohlcv_series_id
                  AND ta.ticker = ?
              )
            )
            """
        )
        params.append(ticker_label)
        params.append(ticker_label)
    params.append(limit)
    order = (
        "b.bar_date DESC, b.bar_start_ts DESC"
        if latest_mode
        else "b.bar_date ASC, b.bar_start_ts ASC"
    )
    with connect_readonly_sqlite(db_path) as conn:
        rows = conn.execute(
            f"""
            SELECT
              b.ohlcv_bar_scope_id AS ohlcv_bar_id,
              b.ohlcv_series_id,
              b.ticker,
              b.bar_date,
              b.session_date,
              b.calendar_id,
              b.timezone_name,
              b.market_session_id,
              b.session_start_time,
              b.bar_start_ts,
              b.utc_start_ts,
              b.ohlcv_bar_lineage_id,
              b.multiplier,
              b.timespan,
              b.adjusted_flag,
              b.open,
              b.high,
              b.low,
              b.close,
              b.volume,
              b.vwap,
              b.transaction_count,
              b.source,
              b.request_hash,
              b.evidence_ledger_hash,
              b.segment_index,
              b.bar_quality_status,
              b.repair_rule,
              b.raw_bar_json,
              b.repair_evidence_json,
              COALESCE(b.last_downloaded_at_utc, '') AS downloaded_at_utc
            FROM v_ohlcv_bars_unified b
            WHERE {" AND ".join(clauses)}
            ORDER BY {order}
            LIMIT ?
            """,
            params,
        ).fetchall()
    observations = [_bar_observation_from_row(dict(row), view=view) for row in rows]
    return list(reversed(observations)) if latest_mode else observations


def _bar_observation_from_row(row: dict[str, Any], *, view: str) -> dict[str, Any]:
    quality_status = str(row.get("bar_quality_status") or "UNCHECKED")
    consumption = _bar_consumption_status(quality_status)
    bar = {
        "open": row.get("open"),
        "high": row.get("high"),
        "low": row.get("low"),
        "close": row.get("close"),
        "volume": row.get("volume"),
        "vwap": row.get("vwap"),
    }
    if view == "simple":
        return bar
    bar.update(
        {
            "ohlcv_series_id": int(row["ohlcv_series_id"]),
            "ticker": row.get("ticker"),
            "date": row.get("bar_date"),
            "bar_grain": normalize_bar_grain(
                multiplier=int(row["multiplier"]), timespan=str(row["timespan"])
            ).bar_grain,
            "quality_status": quality_status,
            "consumption_status": consumption["status"],
        }
    )
    if view == "detail":
        return bar
    raw_provider_bar = _json_payload(row.get("raw_bar_json"))
    repair_evidence = _json_payload(row.get("repair_evidence_json"))
    bar.update(
        {
            "object_type": "BarObservation",
            "canonical": {
                "bar_start_ts": row.get("bar_start_ts"),
                "utc_start_ts": row.get("utc_start_ts"),
                "session_date": row.get("session_date") or row.get("bar_date"),
                "session_start_time": row.get("session_start_time"),
                "market_session_id": row.get("market_session_id"),
                "calendar_id": row.get("calendar_id"),
                "timezone_name": row.get("timezone_name"),
                "multiplier": row.get("multiplier"),
                "timespan": row.get("timespan"),
                "adjusted": bool(row.get("adjusted_flag")),
                "source": row.get("source"),
                "transaction_count": row.get("transaction_count"),
            },
            "raw_provider_bar": raw_provider_bar,
            "raw_provider_ohlcv": _raw_provider_ohlcv(raw_provider_bar),
            "quality": {
                "bar_quality_status": quality_status,
                "consumption_status": consumption["status"],
                "usable_for_trading": consumption["usable_for_trading"],
                "reason": consumption["reason"],
            },
            "lineage": {
                "ohlcv_bar_lineage_id": row.get("ohlcv_bar_lineage_id"),
                "request_hash": row.get("request_hash") or "",
                "evidence_ledger_hash": row.get("evidence_ledger_hash") or "",
                "segment_index": row.get("segment_index"),
                "repair_rule": row.get("repair_rule") or "",
                "repair_evidence": repair_evidence,
                "downloaded_at_utc": row.get("downloaded_at_utc") or "",
            },
        }
    )
    return bar


def _json_payload(value: Any) -> Any:
    if value in (None, ""):
        return {}
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(str(value))
    except json.JSONDecodeError:
        return {"unparsed": str(value)}


def _raw_provider_ohlcv(raw: Any) -> dict[str, Any]:
    if not isinstance(raw, dict):
        return {}
    values = {
        "open": raw.get("open", raw.get("o")),
        "high": raw.get("high", raw.get("h")),
        "low": raw.get("low", raw.get("l")),
        "close": raw.get("close", raw.get("c")),
        "volume": raw.get("volume", raw.get("v")),
        "vwap": raw.get("vwap", raw.get("vw")),
        "transaction_count": raw.get("transaction_count", raw.get("n")),
    }
    return values if any(value is not None for value in values.values()) else {}


def _bar_consumption_status(quality_status: str) -> dict[str, Any]:
    status = quality_status.upper()
    if status in {"VALIDATED", "VALIDATED_REPAIRED", "OK"}:
        return {
            "status": "usable_for_trading",
            "usable_for_trading": True,
            "reason": "Bar quality status is validated for canonical consumption.",
        }
    if status in {"QUARANTINED", "INVALID", "SUSPECT"}:
        return {
            "status": "not_usable",
            "usable_for_trading": False,
            "reason": "Bar quality status blocks canonical consumption.",
        }
    return {
        "status": "usable_for_research_only",
        "usable_for_trading": False,
        "reason": "Bar quality status is not validated by the quality layer.",
    }


def _bar_observation_next_actions(
    db: str, scope: dict[str, Any], *, actual_result: str
) -> list[dict[str, Any]]:
    series_id = int(scope.get("ohlcv_series_id") or 0)
    db_args = _db_arg_if_override(_db_target(db))
    grain_args = _bar_grain_arg_if_override(str(scope.get("bar_grain") or "1d"))
    actions = [
        {
            "name": "inspect-series-quality",
            "kind": "command",
            "command": {
                "name": "xctx quality-audit",
                "description": "Inspect coverage and quality category for the selected OHLCV series.",
                "args": {
                    **db_args,
                    **grain_args,
                    "ohlcv_series_id": [series_id],
                    "include_healthy": True,
                },
                "reads": [db],
                "writes": [],
            },
            "effects": [
                {
                    "kind": "read",
                    "target": db,
                    "description": "Read active reference-series quality state.",
                }
            ],
            "requires_approval": False,
        },
        {
            "name": "inspect-series-execution-lineage",
            "kind": "command",
            "command": {
                "name": "xctx observe",
                "description": "Inspect execution receipts linked to the direct bar lineage and selected series.",
                "args": {**db_args, "ohlcv_series_id": series_id, "limit": 5},
                "reads": [db],
                "writes": [],
            },
            "effects": [
                {
                    "kind": "read",
                    "target": db,
                    "description": "Read execution receipts, approvals, and plan links.",
                }
            ],
            "requires_approval": False,
        },
    ]
    if actual_result in {"bar_expected_but_missing", "data_not_loaded"}:
        actions.append(
            {
                "name": "plan-series-catch-up",
                "kind": "command",
                "command": {
                    "name": "xctx catch-up-plan",
                    "description": "Plan read-only catch-up work for the selected OHLCV series.",
                    "args": {
                        **db_args,
                        **grain_args,
                        "ohlcv_series_id": [series_id],
                        "view": "simple",
                    },
                    "reads": [db],
                    "writes": [],
                },
                "effects": [
                    {
                        "kind": "read",
                        "target": db,
                        "description": "Read quality-audit state and materialize a plan.",
                    }
                ],
                "requires_approval": False,
                "reason": "The requested absence is a repairable data-loading state.",
            }
        )
    return actions


def _observe(
    args: argparse.Namespace, parser: argparse.ArgumentParser
) -> dict[str, Any]:
    try:
        rows = _execution_audit_read_only(
            args.db,
            request_hash=args.request_hash,
            series_id=args.ohlcv_series_id,
            limit=args.limit,
        )
    except sqlite3.Error as exc:
        db_args = _db_arg_if_override(args.db)
        return {
            "protocol_version": PROTOCOL_VERSION,
            "ok": False,
            "command": "xctx observe",
            "result_type": "RepairError",
            "error": str(exc),
            "repairs": [
                {
                    "name": "provide-existing-sqlite-db",
                    "evidence_kind": "execution_audit",
                    "request": {"db": args.db},
                    "effect": {
                        "kind": "read",
                        "target": args.db,
                        "description": "Provide an existing SQLite DB.",
                    },
                    "reason": "xctx observe uses an existing DB and leaves schema setup to validate-db.",
                    "command": {
                        "name": "stock-universe validate-db",
                        "description": "Initialize or validate DB through the production CLI before observing.",
                        "args": db_args,
                        "reads": [args.db],
                        "writes": [args.db],
                    },
                }
            ],
            "effects": _effects(reads=[args.db], writes=[]),
        }
    action = {
        "name": "review-receipts",
        "kind": "inspection",
        "command": {
            "name": "xctx observe",
            "description": "Inspect receipt and approval evidence.",
            "args": {
                **_db_arg_if_override(args.db),
                "limit": args.limit,
                "view": "detail",
            },
            "reads": [args.db],
            "writes": [],
        },
        "effects": [
            {
                "kind": "none",
                "target": "stdout",
                "description": "Render audit evidence.",
            }
        ],
        "requires_approval": False,
    }
    payload: dict[str, Any] = {
        "protocol_version": PROTOCOL_VERSION,
        "ok": True,
        "cognition_unit": "audit",
        "command": "xctx observe",
        "result_type": "ExecutionAudit",
        "view": args.view,
        "count": len(rows),
        "latest_execution": _compact_execution_row(rows[0]) if rows else None,
        "next_moves": [_compact_action_record(action)],
    }
    if args.view in {"detail", "extra_detail"}:
        payload["executions"] = (
            [_compact_execution_row(row) for row in rows]
            if args.view == "detail"
            else rows
        )
        payload["next_actions"] = [action]
    if args.view == "extra_detail":
        payload["effects"] = _effects(reads=[args.db], writes=[])
    return payload


def _compact_execution_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "execution_receipt_id": row.get("execution_receipt_id"),
        "ohlcv_series_id": row.get("ohlcv_series_id"),
        "receipt_status": row.get("receipt_status"),
        "plan_status": row.get("plan_status"),
        "fetched_bar_count": row.get("fetched_bar_count"),
        "inserted_bar_count": row.get("inserted_bar_count"),
        "started_at_utc": row.get("started_at_utc"),
        "finished_at_utc": row.get("finished_at_utc"),
    }


def _execution_audit_read_only(
    db_path: str,
    *,
    request_hash: str | None,
    series_id: int | None,
    limit: int,
) -> list[dict[str, Any]]:
    clauses = []
    params: list[Any] = []
    if request_hash:
        clauses.append("r.request_hash = ?")
        params.append(request_hash)
    if series_id is not None:
        clauses.append("r.ohlcv_series_id = ?")
        params.append(series_id)
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    params.append(limit)
    with connect_readonly_sqlite(db_path) as conn:
        rows = conn.execute(
            f"""
            SELECT
              r.execution_receipt_id,
              r.request_hash,
              r.evidence_ledger_hash,
              r.ohlcv_series_id,
              r.status AS receipt_status,
              r.approved_by AS receipt_approved_by,
              r.started_at_utc,
              r.finished_at_utc,
              r.fetched_bar_count,
              r.inserted_bar_count,
              r.receipt_hash,
              a.execution_approval_id,
              a.approval_hash,
              a.approved_by AS approval_approved_by,
              a.allow_caution_flag,
              a.reason AS approval_reason,
              a.approved_at_utc,
              p.plan_id,
              p.status AS plan_status,
              p.plan_hash
            FROM execution_receipts r
            LEFT JOIN execution_approvals a
              ON a.request_hash = r.request_hash
             AND a.evidence_ledger_hash = r.evidence_ledger_hash
             AND a.ohlcv_series_id = r.ohlcv_series_id
            LEFT JOIN backfill_plans p
              ON p.request_hash = r.request_hash
             AND p.evidence_ledger_hash = r.evidence_ledger_hash
             AND p.ohlcv_series_id = r.ohlcv_series_id
            {where}
            ORDER BY r.execution_receipt_id DESC
            LIMIT ?
            """,
            params,
        ).fetchall()
    return [dict(row) for row in rows]


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


def _doctor_ok(checks: dict[str, Any], *, require_entrypoint: bool) -> bool:
    required = [bool(checks.get("massive_api_key_present"))]
    if require_entrypoint:
        required.extend(
            [
                bool(checks.get("stock_universe_entrypoint_present")),
                bool(checks.get("xctx_entrypoint_present")),
            ]
        )
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


def _doctor_next_actions(checks: dict[str, Any]) -> list[dict[str, Any]]:
    actions = [
        {
            "name": "discover-xctx-tree",
            "kind": "command",
            "command": {
                "name": "xctx tree",
                "description": "Discover transitions, schemas, binding maps, recipes, and safety boundaries.",
                "args": {},
                "reads": [],
                "writes": [],
            },
            "effects": [
                {
                    "kind": "none",
                    "target": "stdout",
                    "description": "Render the ToolManifest.",
                }
            ],
            "requires_approval": False,
        },
        {
            "name": "inspect-runnable-examples",
            "kind": "command",
            "command": {
                "name": "xctx examples",
                "description": "Show runnable examples for the executable-context learning loop.",
                "args": {},
                "reads": [],
                "writes": [],
            },
            "effects": [
                {"kind": "none", "target": "stdout", "description": "Render examples."}
            ],
            "requires_approval": False,
        },
        {
            "name": "inspect-canonical-universe-status",
            "kind": "command",
            "command": {
                "name": "xctx universe-status",
                "description": "Check canonical DB reference-universe coverage and update completeness.",
                "args": _db_arg_if_override(
                    str(checks.get("db_path") or canonical_db_text())
                ),
                "reads": [str(checks.get("db_path") or canonical_db_text())],
                "writes": [],
            },
            "effects": [
                {
                    "kind": "read",
                    "target": str(checks.get("db_path") or canonical_db_text()),
                    "description": "Read canonical universe DB state.",
                }
            ],
            "requires_approval": False,
        },
    ]
    if not checks.get("massive_api_key_present"):
        actions.append(
            {
                "name": "provide-massive-api-key",
                "kind": "repair",
                "command": {
                    "name": "xctx doctor",
                    "description": "Rerun doctor after passing --api-key or setting MASSIVE_API_KEY.",
                    "args": {"api_key": "{MASSIVE_API_KEY}"},
                    "reads": ["env:MASSIVE_API_KEY"],
                    "writes": [],
                },
                "effects": [
                    {
                        "kind": "read",
                        "target": "env:MASSIVE_API_KEY",
                        "description": "Read provider credential.",
                    }
                ],
                "requires_approval": False,
                "reason": "Live identity resolution and dry-runs need a Massive API key.",
            }
        )
    if checks.get("db_parent_exists") is False:
        actions.append(
            {
                "name": "provide-existing-db-parent",
                "kind": "repair",
                "command": {
                    "name": "stock-universe doctor",
                    "description": "Check a SQLite path whose parent directory exists.",
                    "args": {"db": "{db}"},
                    "reads": ["filesystem"],
                    "writes": [],
                },
                "effects": [
                    {
                        "kind": "read",
                        "target": "filesystem",
                        "description": "Check DB parent path.",
                    }
                ],
                "requires_approval": False,
                "reason": "The requested SQLite parent directory is absent.",
            }
        )
    return actions


def _universe_status_next_actions(status: dict[str, Any]) -> list[dict[str, Any]]:
    db = str(status.get("db") or canonical_db_text())
    db_args = _db_arg_if_override(db)
    actions = [
        {
            "name": "refresh-canonical-reference-universe",
            "kind": "command",
            "command": {
                "name": "stock-universe update-reference-universe",
                "description": "Refresh the canonical active stock reference universe into the single production DB.",
                "args": {
                    **db_args,
                    "market": "stocks",
                    "active": "active",
                    "limit": 1000,
                    "max_pages": 100,
                    "commit": True,
                },
                "reads": ["Massive API"],
                "writes": [db],
            },
            "effects": [
                {
                    "kind": "read",
                    "target": "Massive API",
                    "description": "Fetch active stock reference tickers.",
                },
                {
                    "kind": "write",
                    "target": db,
                    "description": "Upsert reference_universe_snapshots and reference_universe_updates.",
                },
            ],
            "requires_approval": True,
            "reason": "The canonical universe DB should be refreshed explicitly before DB-backed identity and batch workflows.",
        },
        {
            "name": "search-canonical-db",
            "kind": "command",
            "command": {
                "name": "xctx resolve-identity",
                "description": "Search the canonical DB-backed reference universe.",
                "args": {"source": "db", **db_args, "query": "{query}"},
                "reads": [db],
                "writes": [],
            },
            "effects": [
                {
                    "kind": "read",
                    "target": db,
                    "description": "Read persisted reference candidates.",
                }
            ],
            "requires_approval": False,
        },
    ]
    if not status.get("universe_populated"):
        actions[1]["reason"] = (
            "Search is most useful after the reference universe is populated."
        )
    return actions


def _empty_quality_audit_next_actions(db: str) -> list[dict[str, Any]]:
    db_target = _db_target(db)
    db_args = _db_arg_if_override(db_target)
    return [
        {
            "name": "inspect-universe-status",
            "kind": "command",
            "command": {
                "name": "xctx universe-status",
                "description": "Inspect DB schema, reference-universe coverage, and update completeness before auditing quality.",
                "args": db_args,
                "reads": [db_target],
                "writes": [],
            },
            "effects": [
                {
                    "kind": "read",
                    "target": db_target,
                    "description": "Read universe status.",
                }
            ],
            "requires_approval": False,
            "reason": "Active reference series are required for quality auditing.",
        },
        *_reference_universe_repair_actions(
            db=db_target,
            dry_run_reason="Quality audit has zero active reference series to inspect.",
        ),
    ]


def _example_catalog(parser: argparse.ArgumentParser) -> list[dict[str, Any]]:
    base = parser.prog.split()
    examples = [
        {
            "name": "discover-transition-tree",
            "command": "xctx tree",
            "argv": base + ["tree"],
            "source_checkout_argv": ["./stock_universe.cli", "xctx", "tree"],
            "structured_input": {},
            "what_it_teaches": "ToolManifest, transition graph, command schemas, binding maps, recipes, and safety boundaries.",
            "side_effects": {"writes": [], "mutates": False},
        },
        {
            "name": "check-local-readiness",
            "command": "xctx doctor",
            "argv": base + ["doctor"],
            "source_checkout_argv": [
                "./stock_universe.cli",
                "xctx",
                "doctor",
            ],
            "structured_input": {"db": canonical_db_text()},
            "what_it_teaches": "Environment readiness, entrypoint availability, DB path state, and safe next actions.",
            "side_effects": {"writes": [], "mutates": False},
        },
        {
            "name": "inspect-canonical-universe-status",
            "command": "xctx universe-status",
            "argv": base + ["universe-status"],
            "source_checkout_argv": ["./stock_universe.cli", "xctx", "universe-status"],
            "structured_input": {"db": canonical_db_text()},
            "what_it_teaches": "Canonical DB coverage, reference snapshot scope, update completeness, and execution counts.",
            "side_effects": {"writes": [], "mutates": False},
        },
        {
            "name": "resolve-db-identity-candidates",
            "command": "xctx resolve-identity",
            "argv": base
            + ["resolve-identity", "--source", "db", "--query", "Alphabet"],
            "source_checkout_argv": [
                "./stock_universe.cli",
                "xctx",
                "resolve-identity",
                "--source",
                "db",
                "--query",
                "Alphabet",
            ],
            "structured_input": {
                "query": "Alphabet",
                "source": "db",
                "db": canonical_db_text(),
            },
            "what_it_teaches": (
                "Ranked identity candidates, same-CIK context, and the agent rule that OHLCV reporting defaults "
                "to selected_candidate.ohlcv_series_id because ticker is an alias."
            ),
            "side_effects": {"writes": [], "mutates": False},
        },
        {
            "name": "observe-canonical-bars",
            "command": "xctx bars",
            "argv": base + ["bars", "--query", "NVDA", "--date", "2024-06-10"],
            "source_checkout_argv": [
                "./stock_universe.cli",
                "xctx",
                "bars",
                "--query",
                "NVDA",
                "--date",
                "2024-06-10",
            ],
            "structured_input": {
                "db": canonical_db_text(),
                "query": "NVDA",
                "date": "2024-06-10",
                "view": "simple",
                "limit": 5,
            },
            "what_it_teaches": "A compact canonical OHLCV frame; pass --view detail for quality or --view extra_detail for session/UTC/direct-lineage/raw-sidecar provenance.",
            "side_effects": {"writes": [], "mutates": False},
        },
        {
            "name": "audit-bar-provenance",
            "command": "xctx bars",
            "argv": base
            + [
                "bars",
                "--query",
                "NVDA",
                "--date",
                "2024-06-10",
                "--bar-grain",
                "1d",
                "--view",
                "extra_detail",
            ],
            "source_checkout_argv": [
                "./stock_universe.cli",
                "xctx",
                "bars",
                "--query",
                "NVDA",
                "--date",
                "2024-06-10",
                "--bar-grain",
                "1d",
                "--view",
                "extra_detail",
            ],
            "structured_input": {
                "db": canonical_db_text(),
                "query": "NVDA",
                "date": "2024-06-10",
                "bar_grain": "1d",
                "view": "extra_detail",
                "limit": 5,
            },
            "what_it_teaches": "Bar-level audit chain from canonical OHLCV to exchange session keys, UTC key, direct lineage id, quality repair evidence, and raw provider sidecar payload.",
            "side_effects": {"writes": [], "mutates": False},
        },
        {
            "name": "inspect-dry-run-schema",
            "command": "xctx schema",
            "argv": base + ["schema", "--command", "xctx dry-run"],
            "source_checkout_argv": [
                "./stock_universe.cli",
                "xctx",
                "schema",
                "--command",
                "xctx dry-run",
            ],
            "structured_input": {"command": "xctx dry-run"},
            "what_it_teaches": "Accepted dry-run inputs and concrete argv binding.",
            "side_effects": {"writes": [], "mutates": False},
        },
        {
            "name": "dry-run-live-ticker",
            "command": "xctx dry-run",
            "argv": base
            + [
                "dry-run",
                "--ticker",
                "NVDA",
                "--bar-grain",
                "1d",
                "--max-rounds",
                str(DEFAULT_MAX_ROUNDS),
            ],
            "source_checkout_argv": [
                "./stock_universe.cli",
                "xctx",
                "dry-run",
                "--ticker",
                "NVDA",
                "--bar-grain",
                "1d",
                "--max-rounds",
                str(DEFAULT_MAX_ROUNDS),
            ],
            "structured_input": {
                "ticker": "NVDA",
                "bar_grain": "1d",
                "max_rounds": DEFAULT_MAX_ROUNDS,
            },
            "what_it_teaches": "Live ticker-seeded planner envelope, provider reads, decisions, rounds, and concrete execution actions.",
            "side_effects": {"writes": [], "mutates": False},
        },
        {
            "name": "inspect-quality-audit",
            "command": "xctx quality-audit",
            "argv": base + ["quality-audit"],
            "source_checkout_argv": ["./stock_universe.cli", "xctx", "quality-audit"],
            "structured_input": {"db": canonical_db_text(), "view": "simple"},
            "what_it_teaches": "Read-oriented stale/missing-bar/receipt category counts and compact next moves while omitting issue rows.",
            "side_effects": {"writes": [], "mutates": False},
        },
        {
            "name": "plan-database-catch-up",
            "command": "xctx catch-up-plan",
            "argv": base
            + [
                "catch-up-plan",
                "--workers",
                "10",
                "--batch-size",
                "25",
                "--category",
                "bar_expected_but_missing",
                "--category",
                "covered_series_data_stale",
                "--category",
                "listed_common_stock_data_stale",
                "--category",
                "plan_session_gap",
                "--category",
                "provider_zero_bar_response_stale",
            ],
            "source_checkout_argv": [
                "./stock_universe.cli",
                "xctx",
                "catch-up-plan",
                "--workers",
                "10",
                "--batch-size",
                "25",
                "--category",
                "bar_expected_but_missing",
                "--category",
                "covered_series_data_stale",
                "--category",
                "listed_common_stock_data_stale",
                "--category",
                "plan_session_gap",
                "--category",
                "provider_zero_bar_response_stale",
            ],
            "structured_input": {
                "db": canonical_db_text(),
                "workers": 10,
                "batch_size": 25,
                "category": [
                    "bar_expected_but_missing",
                    "covered_series_data_stale",
                    "listed_common_stock_data_stale",
                    "plan_session_gap",
                    "provider_zero_bar_response_stale",
                ],
                "view": "simple",
            },
            "what_it_teaches": "Read-oriented catch-up counts, category totals, run directory, and exact commit/status next actions.",
            "side_effects": {"writes": [], "mutates": False},
        },
        {
            "name": "inspect-catch-up-status",
            "command": "xctx catch-up-status",
            "argv": base + ["catch-up-status", "--latest"],
            "source_checkout_argv": [
                "./stock_universe.cli",
                "xctx",
                "catch-up-status",
                "--latest",
            ],
            "structured_input": {"latest": True},
            "what_it_teaches": "Durable progress events, stop state, stale-running detection, DB reconciliation, batch outcomes, and post-run next actions.",
            "side_effects": {"writes": [], "mutates": False},
        },
        {
            "name": "list-recent-catch-up-runs",
            "command": "xctx catch-up-runs",
            "argv": base + ["catch-up-runs", "--limit", "3"],
            "source_checkout_argv": [
                "./stock_universe.cli",
                "xctx",
                "catch-up-runs",
                "--limit",
                "3",
            ],
            "structured_input": {"limit": 3},
            "what_it_teaches": "Recent catch-up run state, target counts, completion counts, stop state, and latest status action.",
            "side_effects": {"writes": [], "mutates": False},
        },
        {
            "name": "observe-execution-receipts",
            "command": "xctx observe",
            "argv": base + ["observe", "--limit", "20"],
            "source_checkout_argv": [
                "./stock_universe.cli",
                "xctx",
                "observe",
                "--limit",
                "20",
            ],
            "structured_input": {"db": canonical_db_text(), "limit": 20},
            "what_it_teaches": "Receipt, approval, and plan linkage after execution.",
            "side_effects": {"writes": [], "mutates": False},
        },
        {
            "name": "rehearse-reference-universe-update",
            "command": "stock-universe update-reference-universe",
            "argv": [
                "stock-universe",
                "update-reference-universe",
                "--limit",
                "1000",
                "--max-pages",
                "100",
            ],
            "source_checkout_argv": [
                "./stock_universe.cli",
                "update-reference-universe",
                "--limit",
                "1000",
                "--max-pages",
                "100",
            ],
            "structured_input": {"limit": 1000, "max_pages": 100, "commit": False},
            "what_it_teaches": "Reference snapshot fetch scope before the commit-gated DB mutation.",
            "side_effects": {
                "writes": [],
                "mutates": False,
                "requires_approval": False,
            },
        },
        {
            "name": "commit-reference-universe-update",
            "command": "stock-universe update-reference-universe",
            "argv": [
                "stock-universe",
                "update-reference-universe",
                "--limit",
                "1000",
                "--max-pages",
                "100",
                "--commit",
            ],
            "source_checkout_argv": [
                "./stock_universe.cli",
                "update-reference-universe",
                "--limit",
                "1000",
                "--max-pages",
                "100",
                "--commit",
            ],
            "structured_input": {
                "db": canonical_db_text(),
                "limit": 1000,
                "max_pages": 100,
                "commit": True,
            },
            "what_it_teaches": "Explicit reference-universe persistence boundary.",
            "side_effects": {
                "writes": [canonical_db_text()],
                "mutates": True,
                "requires_approval": True,
            },
        },
        {
            "name": "validate-canonical-db",
            "command": "stock-universe validate-db",
            "argv": ["stock-universe", "validate-db"],
            "source_checkout_argv": ["./stock_universe.cli", "validate-db"],
            "structured_input": {"db": canonical_db_text()},
            "what_it_teaches": "Schema version, foreign keys, counts, and durable integrity checks.",
            "side_effects": {
                "writes": ["SQLite DB schema when missing"],
                "mutates": True,
                "requires_approval": True,
            },
        },
        {
            "name": "rehearse-reference-batch",
            "command": "stock-universe backfill-reference-batch",
            "argv": [
                "stock-universe",
                "backfill-reference-batch",
                "--limit",
                "25",
                "--offset",
                "0",
            ],
            "source_checkout_argv": [
                "./stock_universe.cli",
                "backfill-reference-batch",
                "--limit",
                "25",
                "--offset",
                "0",
            ],
            "structured_input": {"limit": 25, "offset": 0, "commit": False},
            "what_it_teaches": "A bounded read-oriented manifest of selected persisted OHLCV series IDs.",
            "side_effects": {
                "writes": [],
                "mutates": False,
                "requires_approval": False,
            },
        },
        {
            "name": "commit-reference-batch",
            "command": "stock-universe backfill-reference-batch",
            "argv": [
                "stock-universe",
                "backfill-reference-batch",
                "--limit",
                "25",
                "--offset",
                "0",
                "--commit",
                "--strict",
            ],
            "source_checkout_argv": [
                "./stock_universe.cli",
                "backfill-reference-batch",
                "--limit",
                "25",
                "--offset",
                "0",
                "--commit",
                "--strict",
            ],
            "structured_input": {
                "limit": 25,
                "offset": 0,
                "commit": True,
                "strict": True,
            },
            "what_it_teaches": "Explicit execution boundary for a bounded persisted reference selection.",
            "side_effects": {
                "writes": [canonical_db_text()],
                "mutates": True,
                "requires_approval": True,
            },
        },
        {
            "name": "rehearse-reference-all-pages",
            "command": "stock-universe backfill-reference-batch",
            "argv": [
                "stock-universe",
                "backfill-reference-batch",
                "--exchange",
                "XNAS",
                "--market",
                "stocks",
                "--bar-grain",
                "1d",
                "--page-size",
                "1000",
                "--all-pages",
            ],
            "source_checkout_argv": [
                "./stock_universe.cli",
                "backfill-reference-batch",
                "--exchange",
                "XNAS",
                "--market",
                "stocks",
                "--bar-grain",
                "1d",
                "--page-size",
                "1000",
                "--all-pages",
            ],
            "structured_input": {
                "exchange": "XNAS",
                "market": "stocks",
                "bar_grain": "1d",
                "limit": 1000,
                "all_pages": True,
                "commit": False,
            },
            "what_it_teaches": "A full internally-paged read manifest for an exchange/market/bar-grain selection.",
            "side_effects": {
                "writes": [],
                "mutates": False,
                "requires_approval": False,
            },
        },
        {
            "name": "commit-reference-all-pages",
            "command": "stock-universe backfill-reference-batch",
            "argv": [
                "stock-universe",
                "backfill-reference-batch",
                "--exchange",
                "XNAS",
                "--market",
                "stocks",
                "--bar-grain",
                "1d",
                "--page-size",
                "1000",
                "--all-pages",
                "--commit",
                "--strict",
            ],
            "source_checkout_argv": [
                "./stock_universe.cli",
                "backfill-reference-batch",
                "--exchange",
                "XNAS",
                "--market",
                "stocks",
                "--bar-grain",
                "1d",
                "--page-size",
                "1000",
                "--all-pages",
                "--commit",
                "--strict",
            ],
            "structured_input": {
                "exchange": "XNAS",
                "market": "stocks",
                "bar_grain": "1d",
                "limit": 1000,
                "all_pages": True,
                "commit": True,
                "strict": True,
            },
            "what_it_teaches": "The execution boundary for a full internally-paged exchange backfill.",
            "side_effects": {
                "writes": [canonical_db_text()],
                "mutates": True,
                "requires_approval": True,
            },
        },
        {
            "name": "commit-database-catch-up",
            "command": "stock-universe catch-up",
            "argv": [
                "stock-universe",
                "catch-up",
                "--workers",
                "10",
                "--batch-size",
                "25",
                "--category",
                "bar_expected_but_missing",
                "--category",
                "covered_series_data_stale",
                "--category",
                "listed_common_stock_data_stale",
                "--category",
                "plan_session_gap",
                "--category",
                "provider_zero_bar_response_stale",
                "--commit",
                "--fail-fast",
            ],
            "source_checkout_argv": [
                "./stock_universe.cli",
                "catch-up",
                "--workers",
                "10",
                "--batch-size",
                "25",
                "--category",
                "bar_expected_but_missing",
                "--category",
                "covered_series_data_stale",
                "--category",
                "listed_common_stock_data_stale",
                "--category",
                "plan_session_gap",
                "--category",
                "provider_zero_bar_response_stale",
                "--commit",
                "--fail-fast",
            ],
            "structured_input": {
                "workers": 10,
                "batch_size": 25,
                "category": [
                    "bar_expected_but_missing",
                    "covered_series_data_stale",
                    "listed_common_stock_data_stale",
                    "plan_session_gap",
                    "provider_zero_bar_response_stale",
                ],
                "commit": True,
                "fail_fast": True,
            },
            "what_it_teaches": "Commit-gated catch-up execution with durable progress, batch artifacts, and hard-error/operator stop state.",
            "side_effects": {
                "writes": [canonical_db_text(), "production_build/catch_up_runs/<run>"],
                "mutates": True,
                "requires_approval": True,
            },
        },
        {
            "name": "commit-data-not-loaded-catch-up",
            "command": "stock-universe catch-up",
            "argv": [
                "stock-universe",
                "catch-up",
                "--workers",
                "10",
                "--batch-size",
                "25",
                "--category",
                "data_not_loaded",
                "--commit",
                "--fail-fast",
            ],
            "source_checkout_argv": [
                "./stock_universe.cli",
                "catch-up",
                "--workers",
                "10",
                "--batch-size",
                "25",
                "--category",
                "data_not_loaded",
                "--commit",
                "--fail-fast",
            ],
            "structured_input": {
                "workers": 10,
                "batch_size": 25,
                "category": ["data_not_loaded"],
                "commit": True,
                "fail_fast": True,
            },
            "what_it_teaches": "High-throughput initial backfill execution for data_not_loaded targets.",
            "side_effects": {
                "writes": [canonical_db_text(), "production_build/catch_up_runs/<run>"],
                "mutates": True,
                "requires_approval": True,
            },
        },
        {
            "name": "request-catch-up-stop",
            "command": "stock-universe catch-up-stop",
            "argv": [
                "stock-universe",
                "catch-up-stop",
                "--run-dir",
                "production_build/catch_up_runs/<run>",
                "--mode",
                "quiesce",
            ],
            "source_checkout_argv": [
                "./stock_universe.cli",
                "catch-up-stop",
                "--run-dir",
                "production_build/catch_up_runs/<run>",
                "--mode",
                "quiesce",
            ],
            "structured_input": {
                "run_dir": "production_build/catch_up_runs/<run>",
                "reason": "operator requested stop",
                "mode": "quiesce",
            },
            "what_it_teaches": "Cooperative catch-up stop request with drain, quiesce, or abort behavior and a resumable run boundary.",
            "side_effects": {
                "writes": ["production_build/catch_up_runs/<run>/stop_request.json"],
                "mutates": True,
                "requires_approval": True,
            },
        },
        {
            "name": "reconcile-catch-up-artifacts",
            "command": "stock-universe catch-up-reconcile",
            "argv": [
                "stock-universe",
                "catch-up-reconcile",
                "--run-dir",
                "production_build/catch_up_runs/<run>",
            ],
            "source_checkout_argv": [
                "./stock_universe.cli",
                "catch-up-reconcile",
                "--run-dir",
                "production_build/catch_up_runs/<run>",
            ],
            "structured_input": {
                "run_dir": "production_build/catch_up_runs/<run>",
                "commit": False,
            },
            "what_it_teaches": "Dry-run adoption of validated DB-completed receipts into recovered artifacts before safe resume.",
            "side_effects": {
                "writes": [],
                "mutates": False,
                "requires_approval": False,
            },
        },
        {
            "name": "commit-catch-up-reconciliation",
            "command": "stock-universe catch-up-reconcile",
            "argv": [
                "stock-universe",
                "catch-up-reconcile",
                "--run-dir",
                "production_build/catch_up_runs/<run>",
                "--commit",
            ],
            "source_checkout_argv": [
                "./stock_universe.cli",
                "catch-up-reconcile",
                "--run-dir",
                "production_build/catch_up_runs/<run>",
                "--commit",
            ],
            "structured_input": {
                "run_dir": "production_build/catch_up_runs/<run>",
                "commit": True,
            },
            "what_it_teaches": "Commit-gated recovered artifact write for stale or killed catch-up runs.",
            "side_effects": {
                "writes": ["production_build/catch_up_runs/<run>/reconciliation.json"],
                "mutates": True,
                "requires_approval": True,
            },
        },
        {
            "name": "list-workflow-recipes",
            "command": "xctx compose",
            "argv": base + ["compose"],
            "source_checkout_argv": ["./stock_universe.cli", "xctx", "compose"],
            "structured_input": {},
            "what_it_teaches": "Composable workflow recipes from discovery through observe.",
            "side_effects": {"writes": [], "mutates": False},
        },
    ]
    normalized = []
    for example in examples:
        normalized_example = dict(example)
        argv = normalized_example.get("argv")
        if isinstance(argv, list) and all(isinstance(part, str) for part in argv):
            runnable = xctx_runnable_argv(argv)
            if runnable != argv:
                normalized_example["logical_argv"] = list(argv)
                normalized_example["argv"] = runnable
        normalized.append(normalized_example)
    return normalized


def _missing_api_key_error(command: str, *, purpose: str) -> dict[str, Any]:
    return _input_repair_error(
        command,
        code="massive_api_key_required",
        what_failed=f"Massive API key is required for {purpose}.",
        minimal_fix="Pass --api-key or set MASSIVE_API_KEY in the environment.",
        reads=["env:MASSIVE_API_KEY"],
        repairs=[
            {
                "name": "provide-massive-api-key",
                "kind": "repair",
                "command": {
                    "name": command,
                    "description": "Rerun the command after passing --api-key or setting MASSIVE_API_KEY.",
                    "args": {"api_key": "{MASSIVE_API_KEY}"},
                    "reads": ["env:MASSIVE_API_KEY"],
                    "writes": [],
                },
                "effects": [
                    {
                        "kind": "read",
                        "target": "env:MASSIVE_API_KEY",
                        "description": "Read provider credential.",
                    }
                ],
                "requires_approval": False,
            }
        ],
    )


def _input_repair_error(
    command: str,
    *,
    code: str,
    what_failed: str,
    minimal_fix: str,
    suggested_inputs: list[dict[str, Any]] | None = None,
    reads: list[str] | None = None,
    repairs: list[dict[str, Any]] | None = None,
    detail: str = "",
) -> dict[str, Any]:
    error: dict[str, Any] = {
        "code": code,
        "what_failed": what_failed,
        "minimal_fix": minimal_fix,
    }
    if suggested_inputs:
        error["suggested_inputs"] = suggested_inputs
    if detail:
        error["detail"] = detail
    return {
        "protocol_version": PROTOCOL_VERSION,
        "ok": False,
        "command": command,
        "result_type": "RepairError",
        "error": what_failed,
        "errors": [error],
        "repairs": repairs or [],
        "effects": _effects(reads=reads or [], writes=[]),
        "next_actions": [
            {
                "name": "inspect-runnable-examples",
                "kind": "command",
                "command": {
                    "name": "xctx examples",
                    "description": "Show known-good source-checkout commands and structured inputs.",
                    "args": {},
                    "reads": [],
                    "writes": [],
                },
                "effects": [
                    {
                        "kind": "none",
                        "target": "stdout",
                        "description": "Render examples.",
                    }
                ],
                "requires_approval": False,
            }
        ],
    }


def _compose(
    args: argparse.Namespace, parser: argparse.ArgumentParser
) -> dict[str, Any]:
    all_recipes = xctx_recipes()
    recipes = all_recipes
    if args.recipe:
        recipes = [recipe for recipe in recipes if recipe["name"] == args.recipe]
        if not recipes:
            return {
                "protocol_version": PROTOCOL_VERSION,
                "ok": False,
                "command": "xctx compose",
                "result_type": "RepairError",
                "error": f"unknown recipe: {args.recipe}",
                "known_recipes": sorted(recipe["name"] for recipe in all_recipes),
                "effects": _effects(reads=[], writes=[]),
                "next_actions": [
                    {
                        "name": "list-workflow-recipes",
                        "kind": "command",
                        "command": {
                            "name": "xctx compose",
                            "description": "List available executable-context recipes.",
                            "args": {},
                            "reads": [],
                            "writes": [],
                        },
                        "effects": [
                            {
                                "kind": "none",
                                "target": "stdout",
                                "description": "Render recipe list.",
                            }
                        ],
                        "requires_approval": False,
                    }
                ],
            }
    return {
        "protocol_version": PROTOCOL_VERSION,
        "ok": bool(recipes),
        "command": "xctx compose",
        "result_type": "RecipeList",
        "recipes": recipes,
        "known_recipes": sorted(recipe["name"] for recipe in all_recipes),
        "effects": _effects(reads=[], writes=[]),
    }


def _effects(*, reads: list[str], writes: list[str]) -> dict[str, list[str]]:
    return {
        "will_read": reads,
        "will_write": writes,
        "did_write": [],
    }


def _db_target(db: str | None) -> str:
    return str(db or canonical_db_text())


def _db_arg_if_override(db: str | None) -> dict[str, str]:
    target = _db_target(db)
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


def _schema_alias_target(
    command: str, schemas: dict[str, dict[str, Any]]
) -> str | None:
    for name, schema in schemas.items():
        if command in schema.get("aliases", []):
            return name
    return None


def _schema_aliases(schemas: dict[str, dict[str, Any]]) -> list[str]:
    return [alias for schema in schemas.values() for alias in schema.get("aliases", [])]


def _command_record(name: str, schema: dict[str, Any]) -> dict[str, Any]:
    payload = {
        "name": name,
        "description": str(schema.get("description") or ""),
        "args": schema.get("args") or {},
        "reads": list(schema.get("reads") or []),
        "writes": list(schema.get("writes") or []),
        "returns": str(schema.get("returns") or ""),
        "mutates": bool(schema.get("mutates")),
        "cognition_unit": str(schema.get("cognition_unit") or ""),
        "aliases": list(schema.get("aliases") or []),
        "input_rule": str(schema.get("input_rule") or ""),
    }
    if schema.get("views"):
        payload["views"] = schema["views"]
    return payload


def _command_summary_record(name: str, schema: dict[str, Any]) -> dict[str, Any]:
    payload = {
        "name": name,
        "description": str(schema.get("description") or ""),
        "returns": str(schema.get("returns") or ""),
        "mutates": bool(schema.get("mutates")),
        "cognition_unit": str(schema.get("cognition_unit") or ""),
    }
    if schema.get("views"):
        payload["views"] = schema["views"]
    if schema.get("aliases"):
        payload["aliases"] = list(schema.get("aliases") or [])
    return payload


def _recipe_summary(recipe: dict[str, Any]) -> dict[str, Any]:
    return {
        "name": recipe["name"],
        "description": recipe.get("description") or "",
        "steps": [
            {
                "transition": step.get("transition") or "",
                "command": step.get("command") or "",
            }
            for step in recipe.get("steps") or []
        ],
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


if __name__ == "__main__":
    raise SystemExit(main())
