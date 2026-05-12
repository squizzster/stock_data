"""Typed executable-context protocol records."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from stock_universe.agent_reporting import (
    backfill_reference_batch_reporting_policy,
    backfill_reporting_policy,
    catch_up_reporting_policy,
    recipe_reporting_policy,
    soft_long_running_reporting_policy,
    update_reference_universe_reporting_policy,
    validate_db_reporting_policy,
)
from stock_universe.defaults import DEFAULT_MAX_ROUNDS
from stock_universe.paths import canonical_db_text


PROTOCOL_VERSION = "xctx.v2"
CANONICAL_DB = canonical_db_text()
BAR_GRAIN_ARG = {"required": False, "enum": ["1d", "1m", "30m"], "default": "1d"}

ActionKind = Literal["command", "inspection", "approval", "repair", "execution"]
EffectKind = Literal["read", "write", "append-evidence-fact", "execute-plan", "none"]
AuthorityLevel = Literal[
    "none",
    "read",
    "network_read",
    "file_write",
    "db_write",
    "execution",
    "approval",
    "repair",
]


XCTX_COMMAND_SCHEMAS: dict[str, dict[str, Any]] = {
    "xctx tree": {
        "description": "Expose the executable-context transition tree.",
        "args": {
            "view": {
                "required": False,
                "enum": ["simple", "detail", "extra_detail"],
                "default": "simple",
            }
        },
        "views": {
            "simple": "Compact commands, transitions, and recipe steps.",
            "detail": "Simple discovery plus command schemas.",
            "extra_detail": "Full schemas, binding maps, and recipe details.",
        },
        "reads": [],
        "writes": [],
        "returns": "ToolManifest",
        "mutates": False,
    },
    "xctx capabilities": {
        "description": "List supported xctx protocol commands.",
        "args": {},
        "reads": [],
        "writes": [],
        "returns": "CapabilityList",
        "mutates": False,
    },
    "xctx doctor": {
        "description": "Check local readiness for xctx-guided workflows with read-oriented checks.",
        "args": {
            "db": {"required": False, "type": "path", "default": CANONICAL_DB},
            "api_key": {"required": False, "type": "string"},
            "require_entrypoint": {
                "required": False,
                "type": "boolean",
                "default": False,
            },
        },
        "reads": ["filesystem", "env:MASSIVE_API_KEY", "SQLite DB when db exists"],
        "writes": [],
        "returns": "DoctorReport",
        "mutates": False,
    },
    "stock-universe doctor": {
        "description": "Production CLI readiness check for entrypoints, API key, DB parent, and existing DB schema.",
        "args": {
            "db": {"required": False, "type": "path", "default": CANONICAL_DB},
            "api_key": {"required": False, "type": "string"},
            "require_entrypoint": {
                "required": False,
                "type": "boolean",
                "default": False,
            },
        },
        "reads": ["filesystem", "env:MASSIVE_API_KEY", "SQLite DB when db exists"],
        "writes": [],
        "returns": "DoctorReport",
        "mutates": False,
    },
    "xctx examples": {
        "description": "Return runnable examples and structured inputs for the executable-context loop.",
        "args": {"command": {"required": False, "type": "string"}},
        "reads": [],
        "writes": [],
        "returns": "ExampleList",
        "mutates": False,
    },
    "xctx describe backfill-plan": {
        "description": "Describe the backfill planning result envelope schema.",
        "args": {"topic": {"required": True, "enum": ["backfill-plan"]}},
        "reads": [],
        "writes": [],
        "returns": "ResultEnvelope schema",
        "mutates": False,
    },
    "xctx schema": {
        "description": "Expose command schemas and structured input binding maps.",
        "args": {"command": {"required": False, "type": "string"}},
        "reads": [],
        "writes": [],
        "returns": "CommandSpec and BindingMap",
        "mutates": False,
    },
    "xctx validate": {
        "description": "Validate fixture evidence and return a typed planning envelope.",
        "args": {
            "fixture": {"required": True, "type": "path"},
            "omit_kind": {"required": False, "type": "array", "items": "evidence_kind"},
            "approve_execution": {
                "required": False,
                "type": "boolean",
                "default": False,
            },
        },
        "input_rule": "Read-only validation. approve_execution only exposes the production backfill command as a next action; xctx does not execute or persist approval.",
        "reads": ["fixture"],
        "writes": [],
        "returns": "ResultEnvelope",
        "mutates": False,
    },
    "xctx dry-run": {
        "description": "Rehearse adaptive planning from fixture evidence or a live ticker seed.",
        "args": {
            "fixture": {"required": False, "type": "path"},
            "ticker": {"required": False, "type": "ticker"},
            "ohlcv_series_id": {"required": False, "type": "integer"},
            "db": {"required": False, "type": "path", "default": CANONICAL_DB},
            "source": {
                "required": False,
                "enum": ["fixture", "live"],
                "default": "fixture",
            },
            "omit_kind": {"required": False, "type": "array", "items": "evidence_kind"},
            "defer_kind": {
                "required": False,
                "type": "array",
                "items": "evidence_kind",
            },
            "api_key": {"required": False, "type": "string"},
            "base_url": {
                "required": False,
                "type": "url",
                "default": "https://api.massive.com",
            },
            "from_date": {
                "required": False,
                "type": "date",
                "default": "rolling_5y",
            },
            "to_date": {"required": False, "type": "date"},
            "bar_grain": BAR_GRAIN_ARG,
            "identity_as_of_date": {"required": False, "type": "date"},
            "max_rounds": {
                "required": False,
                "type": "integer",
                "default": DEFAULT_MAX_ROUNDS,
            },
        },
        "input_rule": "Provide exactly one of fixture, ticker, or ohlcv_series_id. Ticker and ohlcv_series_id inputs use live read-oriented providers; ohlcv_series_id also requires db.",
        "reads": [
            "fixture, Massive reference ticker, or SQLite reference universe",
            "evidence-source",
        ],
        "writes": [],
        "returns": "DryRunPlan and ResultEnvelope",
        "mutates": False,
        "agent_reporting": soft_long_running_reporting_policy(
            action="xctx dry-run",
            status_source="stdout DryRunPlan/ResultEnvelope when complete",
        ),
    },
    "xctx resolve-identity": {
        "description": (
            "Return read-oriented identity candidates for a ticker, company name, CIK, FIGI, or stored OHLCV "
            "series ID, including same-CIK issuer enrichment when a strong operating-company match is found. "
            "For DB candidates, agents must treat ohlcv_series_id as the canonical OHLCV reporting key; ticker "
            "is an alias that can change over time."
        ),
        "args": {
            "query": {"required": True, "type": "string"},
            "source": {"required": False, "enum": ["live", "db"], "default": "live"},
            "db": {"required": False, "type": "path", "default": CANONICAL_DB},
            "api_key": {"required": False, "type": "string"},
            "base_url": {
                "required": False,
                "type": "url",
                "default": "https://api.massive.com",
            },
            "as_of_date": {"required": False, "type": "date"},
            "limit": {"required": False, "type": "integer", "default": 25},
        },
        "reads": ["Massive API or SQLite DB"],
        "writes": [],
        "returns": (
            "IdentityCandidateList with ranked candidates, related_searches issuer context, and "
            "agent_ohlcv_reporting_policy/reporting_policy requiring OHLCV/count/date-range reporting by selected "
            "ohlcv_series_id unless the user asks for a ticker-label slice"
        ),
        "mutates": False,
        "agent_reporting": soft_long_running_reporting_policy(
            action="live identity resolution",
            status_source="stdout IdentityCandidateList when complete",
        ),
    },
    "xctx bars": {
        "description": (
            "Return canonical OHLCV bar observations from the stock system DB. The default simple view returns the "
            "requested bar or a domain-causal actual_result for why no bar exists. Detail adds identity, calendar, "
            "quality, and next actions. "
            "Extra detail adds session keys, direct lineage, sparse quality-exception evidence, and raw provider "
            "payloads from the audit side table."
        ),
        "args": {
            "ohlcv_series_id": {"required": False, "type": "integer"},
            "query": {"required": False, "type": "string"},
            "db": {"required": False, "type": "path", "default": CANONICAL_DB},
            "date": {"required": False, "type": "date"},
            "from_date": {"required": False, "type": "date"},
            "to_date": {"required": False, "type": "date"},
            "bar_grain": BAR_GRAIN_ARG,
            "ticker_label": {"required": False, "type": "ticker"},
            "limit": {"required": False, "type": "integer", "default": 5},
            "view": {
                "required": False,
                "enum": ["simple", "detail", "extra_detail"],
                "default": "simple",
            },
        },
        "input_rule": "Provide exactly one of ohlcv_series_id or query. query is DB-backed identity resolution. Use date for one bar, date range for multiple bars, or omit dates for latest bars. Ambiguous query matches require explicit ohlcv_series_id.",
        "views": {
            "simple": "Decision-relevant lookup truth: succeeded, actual_result, and bar/bars.",
            "detail": "Simple observation plus identity, calendar, quality, and actionable next actions.",
            "extra_detail": "Detail plus session keys, direct lineage id, sparse quality-exception evidence, raw provider sidecar payload, effects, and compact canonical metadata.",
        },
        "actual_results": [
            "bar_found",
            "this_is_a_market_holiday",
            "this_is_a_weekend",
            "this_is_not_a_trading_session",
            "bar_expected_but_missing",
            "ticker_not_resolved",
            "ticker_label_no_match",
            "series_not_covered",
            "data_not_loaded",
        ],
        "reads": ["SQLite DB"],
        "writes": [],
        "returns": "BarObservationList",
        "mutates": False,
    },
    "stock-universe inspect-plan": {
        "description": "Inspect a fixture-seeded plan through the pure planner using fixture evidence.",
        "args": {"fixture": {"required": True, "type": "path"}},
        "reads": ["fixture"],
        "writes": [],
        "returns": "ResultEnvelope with plan summary",
        "mutates": False,
    },
    "stock-universe identity-search": {
        "description": (
            "Production CLI identity-candidate search with same-CIK issuer enrichment for strong operating-company "
            "matches. DB-backed OHLCV reporting defaults to ohlcv_series_id; ticker is an alias."
        ),
        "args": {
            "query": {"required": True, "type": "string"},
            "source": {"required": False, "enum": ["live", "db"], "default": "live"},
            "db": {"required": False, "type": "path", "default": CANONICAL_DB},
            "api_key": {"required": False, "type": "string"},
            "base_url": {
                "required": False,
                "type": "url",
                "default": "https://api.massive.com",
            },
            "as_of_date": {"required": False, "type": "date"},
            "limit": {"required": False, "type": "integer", "default": 25},
            "capture_dir": {"required": False, "type": "path"},
        },
        "reads": ["Massive API or SQLite DB"],
        "writes": ["capture-dir raw files when capture_dir is provided"],
        "returns": (
            "IdentityCandidateList with ranked candidates, related_searches issuer context, and "
            "agent_ohlcv_reporting_policy/reporting_policy that distinguishes canonical ohlcv_series_id from "
            "ticker aliases"
        ),
        "mutates": False,
        "aliases": ["stock-universe search"],
        "agent_reporting": soft_long_running_reporting_policy(
            action="live identity search",
            status_source="stdout IdentityCandidateList when complete",
        ),
    },
    "stock-universe update-reference-universe": {
        "description": "Fetch the live reference-universe snapshot and optionally persist it to the canonical SQLite DB.",
        "args": {
            "db": {"required": False, "type": "path", "default": CANONICAL_DB},
            "api_key": {"required": False, "type": "string"},
            "base_url": {
                "required": False,
                "type": "url",
                "default": "https://api.massive.com",
            },
            "market": {"required": False, "type": "string", "default": "stocks"},
            "exchange": {"required": False, "type": "string"},
            "as_of_date": {"required": False, "type": "date"},
            "active": {
                "required": False,
                "enum": ["active", "inactive", "all"],
                "default": "active",
            },
            "limit": {"required": False, "type": "integer", "default": 1000},
            "max_pages": {"required": False, "type": "integer", "default": 100},
            "capture_dir": {"required": False, "type": "path"},
            "commit": {"required": False, "type": "boolean", "default": False},
            "heartbeat_seconds": {"required": False, "type": "integer", "default": 60},
            "summary_seconds": {"required": False, "type": "integer", "default": 180},
        },
        "input_rule": "Default mode is a dry-run rehearsal. With commit it writes reference snapshots and update receipts to the single canonical SQLite DB.",
        "reads": ["Massive reference tickers"],
        "writes": [
            "SQLite DB when commit=true",
            "capture-dir raw files when capture_dir is provided",
        ],
        "returns": "ReferenceUniverseUpdate",
        "mutates": True,
        "agent_reporting": update_reference_universe_reporting_policy(),
    },
    "stock-universe backfill-reference-batch": {
        "description": "Enumerate persisted reference-universe snapshots and optionally execute selected OHLCV series IDs.",
        "args": {
            "db": {"required": False, "type": "path", "default": CANONICAL_DB},
            "exchange": {"required": False, "type": "string"},
            "market": {"required": False, "type": "string"},
            "security_type": {"required": False, "type": "array", "items": "string"},
            "common_stock": {"required": False, "type": "boolean", "default": False},
            "etf": {"required": False, "type": "boolean", "default": False},
            "warrant": {"required": False, "type": "boolean", "default": False},
            "unit": {"required": False, "type": "boolean", "default": False},
            "adrc": {"required": False, "type": "boolean", "default": False},
            "right": {"required": False, "type": "boolean", "default": False},
            "preferred": {"required": False, "type": "boolean", "default": False},
            "fund": {"required": False, "type": "boolean", "default": False},
            "active": {
                "required": False,
                "enum": ["active", "inactive", "all"],
                "default": "active",
            },
            "ohlcv_series_id": {"required": False, "type": "array", "items": "integer"},
            "identity_as_of_date": {"required": False, "type": "date"},
            "limit": {"required": False, "type": "integer", "default": 25},
            "offset": {"required": False, "type": "integer", "default": 0},
            "api_key": {"required": False, "type": "string"},
            "base_url": {
                "required": False,
                "type": "url",
                "default": "https://api.massive.com",
            },
            "from_date": {"required": False, "type": "date", "default": "rolling_5y"},
            "to_date": {"required": False, "type": "date"},
            "bar_grain": BAR_GRAIN_ARG,
            "max_rounds": {
                "required": False,
                "type": "integer",
                "default": DEFAULT_MAX_ROUNDS,
            },
            "no_caution": {"required": False, "type": "boolean", "default": False},
            "commit": {"required": False, "type": "boolean", "default": False},
            "strict": {"required": False, "type": "boolean", "default": False},
            "heartbeat_seconds": {"required": False, "type": "integer", "default": 60},
            "summary_seconds": {"required": False, "type": "integer", "default": 180},
        },
        "input_rule": "Default mode emits a read-oriented manifest of selected persisted OHLCV series IDs. With commit it executes the selected IDs.",
        "reads": ["SQLite reference universe", "Massive API when commit=true"],
        "writes": ["SQLite DB when commit=true"],
        "returns": "ReferenceBatchManifest",
        "mutates": True,
        "agent_reporting": backfill_reference_batch_reporting_policy(),
    },
    "stock-universe backfill": {
        "description": "Execute approved live backfill effects through the production CLI.",
        "args": {
            "db": {"required": False, "type": "path", "default": CANONICAL_DB},
            "fixture": {"required": False, "type": "array", "items": "path"},
            "ticker": {"required": False, "type": "array", "items": "ticker"},
            "strict": {"required": False, "type": "boolean", "default": False},
            "ohlcv_series_id": {"required": False, "type": "array", "items": "integer"},
            "api_key": {"required": False, "type": "string"},
            "base_url": {
                "required": False,
                "type": "url",
                "default": "https://api.massive.com",
            },
            "from_date": {"required": False, "type": "date", "default": "rolling_5y"},
            "to_date": {"required": False, "type": "date"},
            "bar_grain": BAR_GRAIN_ARG,
            "identity_as_of_date": {"required": False, "type": "date"},
            "max_rounds": {
                "required": False,
                "type": "integer",
                "default": DEFAULT_MAX_ROUNDS,
            },
            "no_caution": {"required": False, "type": "boolean", "default": False},
            "heartbeat_seconds": {"required": False, "type": "integer", "default": 60},
            "summary_seconds": {"required": False, "type": "integer", "default": 180},
        },
        "reads": ["seed facts", "Massive API"],
        "writes": ["SQLite DB"],
        "returns": "ResultEnvelope and execution receipts",
        "mutates": True,
        "agent_reporting": backfill_reporting_policy(),
    },
    "stock-universe dry-run": {
        "description": "Production CLI planning rehearsal from fixture, live ticker, or DB OHLCV series ID.",
        "args": {
            "fixture": {"required": False, "type": "path"},
            "ticker": {"required": False, "type": "ticker"},
            "ohlcv_series_id": {"required": False, "type": "integer"},
            "source": {
                "required": False,
                "enum": ["fixture", "live"],
                "default": "fixture",
            },
            "db": {"required": False, "type": "path", "default": CANONICAL_DB},
            "api_key": {"required": False, "type": "string"},
            "base_url": {
                "required": False,
                "type": "url",
                "default": "https://api.massive.com",
            },
            "capture_dir": {"required": False, "type": "path"},
            "from_date": {"required": False, "type": "date", "default": "rolling_5y"},
            "to_date": {"required": False, "type": "date"},
            "bar_grain": BAR_GRAIN_ARG,
            "identity_as_of_date": {"required": False, "type": "date"},
            "max_rounds": {
                "required": False,
                "type": "integer",
                "default": DEFAULT_MAX_ROUNDS,
            },
            "legacy_json_out": {"required": False, "type": "path"},
            "markdown_out": {"required": False, "type": "path"},
        },
        "input_rule": "Provide exactly one of fixture, ticker, or ohlcv_series_id. Output files are explicit opt-in writes.",
        "reads": ["fixture, Massive API, or SQLite reference universe"],
        "writes": [
            "legacy-json-out file when requested",
            "markdown-out file when requested",
            "capture-dir raw files when requested",
        ],
        "returns": "DryRunPlan and ResultEnvelope",
        "mutates": False,
        "agent_reporting": soft_long_running_reporting_policy(
            action="stock-universe dry-run",
            status_source="stdout DryRunPlan/ResultEnvelope when complete",
        ),
    },
    "xctx observe": {
        "description": "Observe persisted execution receipts and approval links.",
        "args": {
            "db": {"required": False, "type": "path", "default": CANONICAL_DB},
            "request_hash": {"required": False, "type": "string"},
            "ohlcv_series_id": {"required": False, "type": "integer"},
            "limit": {"required": False, "type": "integer", "default": 20},
            "view": {
                "required": False,
                "enum": ["simple", "detail", "extra_detail"],
                "default": "simple",
            },
        },
        "views": {
            "simple": "Receipt count, latest compact receipt, and compact next moves.",
            "detail": "Simple audit plus compact receipt rows and full next actions.",
            "extra_detail": "Full receipt and approval evidence.",
        },
        "reads": ["SQLite DB"],
        "writes": [],
        "returns": "ExecutionAudit",
        "mutates": False,
    },
    "xctx universe-status": {
        "description": "Report canonical DB universe coverage, latest reference snapshots, and update completeness.",
        "args": {"db": {"required": False, "type": "path", "default": CANONICAL_DB}},
        "reads": ["SQLite DB"],
        "writes": [],
        "returns": "UniverseStatus",
        "mutates": False,
    },
    "xctx quality-audit": {
        "description": "Classify active reference-series quality issues, including stale bars, missing bars, plan/session gaps, pending backfills, zero-bar receipts, execution errors, and approved plans missing receipts.",
        "args": {
            "db": {"required": False, "type": "path", "default": CANONICAL_DB},
            "stale_before": {"required": False, "type": "date"},
            "bar_grain": BAR_GRAIN_ARG,
            "category": {
                "required": False,
                "type": "array",
                "items": "quality_category",
            },
            "exchange": {"required": False, "type": "array", "items": "mic"},
            "security_type": {"required": False, "type": "array", "items": "string"},
            "ohlcv_series_id": {"required": False, "type": "array", "items": "integer"},
            "ticker": {"required": False, "type": "array", "items": "ticker"},
            "limit": {"required": False, "type": "integer", "default": 50},
            "include_healthy": {"required": False, "type": "boolean", "default": False},
            "view": {
                "required": False,
                "enum": ["simple", "detail", "extra_detail"],
                "default": "simple",
            },
        },
        "views": {
            "simple": "Counts, filters, category totals, and compact next moves while omitting issue rows.",
            "detail": "Simple status plus bounded issue rows and full next actions.",
            "extra_detail": "Full bounded issue rows controlled by --limit.",
        },
        "reads": ["SQLite DB"],
        "writes": [],
        "returns": "QualityAudit",
        "mutates": False,
    },
    "stock-universe universe-status": {
        "description": "Production CLI universe status report for the single canonical DB.",
        "args": {"db": {"required": False, "type": "path", "default": CANONICAL_DB}},
        "reads": ["SQLite DB"],
        "writes": [],
        "returns": "UniverseStatus",
        "mutates": False,
    },
    "stock-universe quality-audit": {
        "description": "Production CLI read-oriented quality audit for active reference-series bar, plan/session, and execution coverage.",
        "args": {
            "db": {"required": False, "type": "path", "default": CANONICAL_DB},
            "stale_before": {"required": False, "type": "date"},
            "bar_grain": BAR_GRAIN_ARG,
            "category": {
                "required": False,
                "type": "array",
                "items": "quality_category",
            },
            "exchange": {"required": False, "type": "array", "items": "mic"},
            "security_type": {"required": False, "type": "array", "items": "string"},
            "ohlcv_series_id": {"required": False, "type": "array", "items": "integer"},
            "ticker": {"required": False, "type": "array", "items": "ticker"},
            "limit": {"required": False, "type": "integer", "default": 50},
            "include_healthy": {"required": False, "type": "boolean", "default": False},
        },
        "reads": ["SQLite DB"],
        "writes": [],
        "returns": "QualityAudit",
        "mutates": False,
    },
    "xctx catch-up-plan": {
        "description": "Materialize a deterministic read-oriented plan for catching up executable quality-audit targets.",
        "args": {
            "db": {"required": False, "type": "path", "default": CANONICAL_DB},
            "workers": {"required": False, "type": "integer", "default": 10},
            "batch_size": {"required": False, "type": "integer", "default": 25},
            "target_limit": {"required": False, "type": "integer", "default": 0},
            "stale_before": {"required": False, "type": "date"},
            "category": {
                "required": False,
                "type": "array",
                "items": "quality_category",
            },
            "exchange": {"required": False, "type": "array", "items": "mic"},
            "security_type": {"required": False, "type": "array", "items": "string"},
            "ohlcv_series_id": {"required": False, "type": "array", "items": "integer"},
            "ticker": {"required": False, "type": "array", "items": "ticker"},
            "from_date": {"required": False, "type": "date", "default": "rolling_5y"},
            "to_date": {"required": False, "type": "date"},
            "bar_grain": BAR_GRAIN_ARG,
            "run_root": {"required": False, "type": "path"},
            "run_dir": {"required": False, "type": "path"},
            "view": {
                "required": False,
                "enum": ["simple", "detail", "extra_detail"],
                "default": "simple",
            },
            "detail_limit": {"required": False, "type": "integer", "default": 25},
        },
        "input_rule": "Read-oriented planning materializes exact OHLCV series IDs and deterministic batches before catch-up execution.",
        "views": {
            "simple": "Counts, category totals, run_dir, monitoring, and compact next moves.",
            "detail": "Simple plan plus bounded target_detail and batch_detail controlled by --detail-limit.",
            "extra_detail": "Complete materialized target and batch lists.",
        },
        "reads": ["SQLite DB"],
        "writes": [],
        "returns": "CatchUpPlan",
        "mutates": False,
        "agent_reporting": soft_long_running_reporting_policy(
            action="catch-up planning",
            status_source="stdout CatchUpPlan with next_actions when complete",
        ),
    },
    "stock-universe catch-up": {
        "description": "Plan by default, or execute a deterministic catch-up run when --commit is supplied.",
        "args": {
            "db": {"required": False, "type": "path", "default": CANONICAL_DB},
            "workers": {"required": False, "type": "integer", "default": 10},
            "batch_size": {"required": False, "type": "integer", "default": 25},
            "target_limit": {"required": False, "type": "integer", "default": 0},
            "stale_before": {"required": False, "type": "date"},
            "category": {
                "required": False,
                "type": "array",
                "items": "quality_category",
            },
            "exchange": {"required": False, "type": "array", "items": "mic"},
            "security_type": {"required": False, "type": "array", "items": "string"},
            "ohlcv_series_id": {"required": False, "type": "array", "items": "integer"},
            "ticker": {"required": False, "type": "array", "items": "ticker"},
            "from_date": {"required": False, "type": "date", "default": "rolling_5y"},
            "to_date": {"required": False, "type": "date"},
            "bar_grain": BAR_GRAIN_ARG,
            "run_root": {"required": False, "type": "path"},
            "run_dir": {"required": False, "type": "path"},
            "api_key": {"required": False, "type": "string"},
            "base_url": {
                "required": False,
                "type": "url",
                "default": "https://api.massive.com",
            },
            "max_rounds": {
                "required": False,
                "type": "integer",
                "default": DEFAULT_MAX_ROUNDS,
            },
            "no_caution": {"required": False, "type": "boolean", "default": False},
            "commit": {"required": False, "type": "boolean", "default": False},
            "strict": {"required": False, "type": "boolean", "default": False},
            "fail_fast": {"required": False, "type": "boolean", "default": False},
            "resume": {"required": False, "type": "boolean", "default": False},
            "heartbeat_seconds": {"required": False, "type": "integer", "default": 60},
            "mini_summary_seconds": {
                "required": False,
                "type": "integer",
                "default": 240,
            },
            "summary_seconds": {"required": False, "type": "integer", "default": 720},
            "resource_check_seconds": {
                "required": False,
                "type": "integer",
                "default": 600,
            },
        },
        "input_rule": "Default mode is a read-oriented plan. With --commit it writes run artifacts and DB backfill outputs; hard errors, disk-drain stops, SIGINT/SIGTERM drain stops, and stop_request.json operator stops are reported in status artifacts.",
        "reads": ["SQLite DB", "Massive API when commit=true"],
        "writes": ["SQLite DB and catch_up_runs artifacts when commit=true"],
        "returns": "CatchUpPlan or CatchUpRunStatus",
        "mutates": True,
        "agent_reporting": catch_up_reporting_policy(),
    },
    "stock-universe catch-up-stop": {
        "description": "Write stop_request.json so a committed catch-up runner stops scheduling new work using drain, quiesce, or abort semantics.",
        "args": {
            "run_dir": {"required": True, "type": "path"},
            "reason": {
                "required": False,
                "type": "string",
                "default": "operator requested stop",
            },
            "requested_by": {
                "required": False,
                "type": "string",
                "default": "operator",
            },
            "mode": {
                "required": False,
                "enum": ["drain", "quiesce", "abort"],
                "default": "drain",
            },
        },
        "input_rule": "Cooperative stop request. drain finishes in-flight batches, quiesce stops between targets, and abort stops before starting another target; completed target artifacts can later be resumed.",
        "reads": ["catch-up run directory"],
        "writes": ["catch-up stop_request.json artifact"],
        "returns": "CatchUpStopRequest",
        "mutates": True,
    },
    "stock-universe catch-up-reconcile": {
        "description": "Adopt validated DB-completed catch-up receipts into explicit recovered artifacts before resume.",
        "args": {
            "run_dir": {"required": True, "type": "path"},
            "commit": {"required": False, "type": "boolean", "default": False},
        },
        "input_rule": "Default mode is a dry-run. With --commit, it writes recovered_batch_*.json and reconciliation.json. Reconciliation preserves DB rows and is available once a run is stopped.",
        "reads": ["catch-up run directory", "SQLite DB"],
        "writes": ["recovered catch-up artifacts when commit=true"],
        "returns": "CatchUpReconciliation or RepairError",
        "mutates": True,
    },
    "xctx catch-up-status": {
        "description": "Read catch-up plan, status, batch, hard-error, and progress artifacts from a run directory.",
        "args": {
            "run_dir": {"required": False, "type": "path"},
            "latest": {"required": False, "type": "boolean", "default": False},
            "run_root": {"required": False, "type": "path"},
            "view": {
                "required": False,
                "enum": ["simple", "detail", "extra_detail"],
                "default": "simple",
            },
        },
        "input_rule": "Provide run_dir for a known run, or latest=true to read the most recent run under run_root.",
        "views": {
            "simple": "Status, counts, problem flags, latest progress event, and compact monitoring.",
            "detail": "Simple status plus reconciliation, resources, failed-result detail, and full next actions.",
            "extra_detail": "Complete batch_artifacts and progress_events arrays.",
        },
        "reads": ["catch-up run directory"],
        "writes": [],
        "returns": "CatchUpRunStatus",
        "mutates": False,
    },
    "xctx catch-up-runs": {
        "description": "List recent catch-up run summaries from a run root.",
        "args": {
            "run_root": {"required": False, "type": "path"},
            "limit": {"required": False, "type": "integer", "default": 5},
            "view": {
                "required": False,
                "enum": ["simple", "detail", "extra_detail"],
                "default": "simple",
            },
        },
        "views": {
            "simple": "Recent run summaries and compact next moves.",
            "detail": "Simple run list plus full next actions.",
            "extra_detail": "Full run records.",
        },
        "reads": ["catch-up run root"],
        "writes": [],
        "returns": "CatchUpRunList",
        "mutates": False,
    },
    "stock-universe validate-db": {
        "description": "Initialize when needed, then validate SQLite schema, counts, foreign keys, and reference integrity.",
        "args": {
            "db": {"required": False, "type": "path", "default": CANONICAL_DB},
            "heartbeat_seconds": {"required": False, "type": "integer", "default": 60},
            "summary_seconds": {"required": False, "type": "integer", "default": 180},
        },
        "reads": ["SQLite DB"],
        "writes": ["SQLite DB schema when missing"],
        "write_condition": "db_missing_or_schema_missing",
        "returns": "DbValidation",
        "mutates": True,
        "agent_reporting": validate_db_reporting_policy(),
    },
    "stock-universe repair-missing-receipts": {
        "description": "Commit-gated repair that inserts durable error receipts for approved plans missing execution receipts.",
        "args": {
            "db": {"required": False, "type": "path", "default": CANONICAL_DB},
            "ohlcv_series_id": {"required": False, "type": "array", "items": "integer"},
            "limit": {"required": False, "type": "integer", "default": 50},
            "reason": {"required": False, "type": "string"},
            "commit": {"required": False, "type": "boolean", "default": False},
        },
        "input_rule": "Default mode emits a read-oriented manifest. With commit it writes error receipts for approvals currently missing receipts.",
        "reads": ["SQLite DB"],
        "writes": ["SQLite DB when commit=true"],
        "returns": "MissingReceiptRepair",
        "mutates": True,
    },
    "stock-universe audit-executions": {
        "description": "Production CLI read-oriented receipt and approval audit.",
        "args": {
            "db": {"required": False, "type": "path", "default": CANONICAL_DB},
            "request_hash": {"required": False, "type": "string"},
            "ohlcv_series_id": {"required": False, "type": "integer"},
            "limit": {"required": False, "type": "integer", "default": 20},
        },
        "reads": ["SQLite DB"],
        "writes": [],
        "returns": "ExecutionAudit",
        "mutates": False,
    },
    "xctx next": {
        "description": "Return the next valid transitions for the current planning state.",
        "args": {
            "fixture": {"required": True, "type": "path"},
            "omit_kind": {"required": False, "type": "array", "items": "evidence_kind"},
            "approve_execution": {
                "required": False,
                "type": "boolean",
                "default": False,
            },
        },
        "input_rule": "Read-only next-action inspection. approve_execution only exposes the production backfill command as a next action; xctx does not execute or persist approval.",
        "reads": ["fixture"],
        "writes": [],
        "returns": "NextAction list",
        "mutates": False,
    },
    "xctx repair": {
        "description": "Return repair actions for unresolved evidence.",
        "args": {
            "fixture": {"required": True, "type": "path"},
            "omit_kind": {"required": False, "type": "array", "items": "evidence_kind"},
        },
        "reads": ["fixture"],
        "writes": [],
        "returns": "RepairError or RepairAction list",
        "mutates": False,
    },
    "xctx compose": {
        "description": "Return executable-context recipes that compose transitions into workflows.",
        "args": {"recipe": {"required": False, "type": "string"}},
        "reads": [],
        "writes": [],
        "returns": "Recipe list",
        "mutates": False,
    },
}


def xctx_command_schemas() -> dict[str, dict[str, Any]]:
    """Return stable command schemas for xctx clients."""
    return {
        name: _v2_schema(name, schema) for name, schema in XCTX_COMMAND_SCHEMAS.items()
    }


def _v2_schema(name: str, schema: dict[str, Any]) -> dict[str, Any]:
    payload = dict(schema)
    payload.setdefault("cognition_unit", _schema_cognition_unit(name, payload))
    return payload


def _schema_cognition_unit(name: str, schema: dict[str, Any]) -> str:
    result = str(schema.get("returns") or "")
    if name in {
        "xctx tree",
        "xctx capabilities",
        "xctx examples",
        "xctx schema",
        "xctx compose",
        "xctx describe backfill-plan",
    }:
        return "discovery"
    if "Repair" in result or name.endswith("repair"):
        return "repair"
    if name in {"xctx bars", "xctx resolve-identity"}:
        return "observation"
    if (
        "Status" in result
        or "Doctor" in result
        or name
        in {
            "xctx quality-audit",
            "stock-universe quality-audit",
            "xctx universe-status",
            "stock-universe universe-status",
        }
    ):
        return "status"
    if (
        "Plan" in result
        or name.endswith("dry-run")
        or name in {"xctx validate", "xctx next"}
    ):
        return "plan"
    if "Audit" in result or "RunList" in result or name == "xctx observe":
        return "audit"
    return "execution" if bool(schema.get("mutates")) else "observation"


def xctx_runnable_argv(argv: list[str]) -> list[str]:
    """Return argv that is runnable from this source checkout."""
    if not argv:
        return []
    if argv[0] == "./stock_universe.cli":
        return list(argv)
    if argv[0] == "stock-universe":
        return ["./stock_universe.cli", *argv[1:]]
    if argv[0] == "xctx":
        return ["./stock_universe.cli", *argv]
    return list(argv)


def xctx_runnable_command(command: str) -> str:
    """Return a command string that is runnable from this source checkout."""
    if command == "./stock_universe.cli" or command.startswith("./stock_universe.cli "):
        return command
    if command == "stock-universe":
        return "./stock_universe.cli"
    if command.startswith("stock-universe "):
        return f"./stock_universe.cli {command.removeprefix('stock-universe ')}"
    if command == "xctx":
        return "./stock_universe.cli xctx"
    if command.startswith("xctx "):
        return f"./stock_universe.cli {command}"
    return command


def _normalize_binding_argv(binding: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(binding)
    for key, value in binding.items():
        if not (key == "argv" or key.endswith("_argv")):
            continue
        if key.startswith("source_checkout_") or key.startswith("logical_"):
            continue
        if not isinstance(value, list) or not all(
            isinstance(part, str) for part in value
        ):
            continue
        runnable = xctx_runnable_argv(value)
        if runnable != value:
            normalized[f"logical_{key}"] = list(value)
            normalized[key] = runnable
    return normalized


def _normalize_recipe_commands(recipes: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized_recipes: list[dict[str, Any]] = []
    for recipe in recipes:
        normalized_recipe = dict(recipe)
        normalized_steps = []
        for step in recipe.get("steps", []):
            normalized_step = dict(step)
            command = normalized_step.get("command")
            if isinstance(command, str):
                runnable = xctx_runnable_command(command)
                if runnable != command:
                    normalized_step["logical_command"] = command
                    normalized_step["command"] = runnable
            normalized_steps.append(normalized_step)
        normalized_recipe["steps"] = normalized_steps
        normalized_recipes.append(normalized_recipe)
    return normalized_recipes


def xctx_binding_maps() -> dict[str, dict[str, Any]]:
    """Return examples of structured input binding to concrete invocations."""
    bindings = {
        "xctx tree": {
            "structured_input": {"view": "simple|detail|extra_detail"},
            "argv": ["xctx", "tree"],
            "detail_argv": ["xctx", "tree", "--view", "detail"],
            "extra_detail_argv": ["xctx", "tree", "--view", "extra_detail"],
            "source_checkout_argv": ["./stock_universe.cli", "xctx", "tree"],
            "source_checkout_detail_argv": [
                "./stock_universe.cli",
                "xctx",
                "tree",
                "--view",
                "detail",
            ],
            "source_checkout_extra_detail_argv": [
                "./stock_universe.cli",
                "xctx",
                "tree",
                "--view",
                "extra_detail",
            ],
        },
        "xctx capabilities": {
            "structured_input": {},
            "argv": ["xctx", "capabilities"],
            "source_checkout_argv": ["./stock_universe.cli", "xctx", "capabilities"],
        },
        "xctx describe backfill-plan": {
            "structured_input": {"topic": "backfill-plan"},
            "argv": ["xctx", "describe", "backfill-plan"],
            "source_checkout_argv": [
                "./stock_universe.cli",
                "xctx",
                "describe",
                "backfill-plan",
            ],
        },
        "xctx schema": {
            "structured_input": {"command": "command-name?"},
            "argv": ["xctx", "schema"],
            "command_filter_argv": ["xctx", "schema", "--command", "{command}"],
            "source_checkout_argv": ["./stock_universe.cli", "xctx", "schema"],
        },
        "xctx dry-run": {
            "structured_input": {
                "fixture": "path?",
                "ticker": "ticker?",
                "ohlcv_series_id": "integer?",
                "db": CANONICAL_DB,
                "source": "fixture|live",
                "omit_kind": ["evidence_kind"],
                "defer_kind": ["evidence_kind"],
                "api_key": "string?",
                "base_url": "https://api.massive.com",
                "bar_grain": "1d|1m|30m",
                "max_rounds": DEFAULT_MAX_ROUNDS,
            },
            "argv": [
                "xctx",
                "dry-run",
                "--fixture",
                "{fixture}",
                "--source",
                "{source}",
                "--max-rounds",
                "{max_rounds}",
            ],
            "omit_kind_argv": [
                "xctx",
                "dry-run",
                "--fixture",
                "{fixture}",
                "--omit-kind",
                "{kind}",
            ],
            "defer_kind_argv": [
                "xctx",
                "dry-run",
                "--fixture",
                "{fixture}",
                "--defer-kind",
                "{kind}",
            ],
            "source_checkout_argv": [
                "./stock_universe.cli",
                "xctx",
                "dry-run",
                "--fixture",
                "{fixture}",
                "--source",
                "{source}",
                "--max-rounds",
                "{max_rounds}",
            ],
            "ticker_argv": [
                "xctx",
                "dry-run",
                "--ticker",
                "{ticker}",
                "--max-rounds",
                "{max_rounds}",
            ],
            "ticker_bar_grain_argv": [
                "xctx",
                "dry-run",
                "--ticker",
                "{ticker}",
                "--bar-grain",
                "{bar_grain}",
                "--max-rounds",
                "{max_rounds}",
            ],
            "source_checkout_ticker_argv": [
                "./stock_universe.cli",
                "xctx",
                "dry-run",
                "--ticker",
                "{ticker}",
                "--max-rounds",
                "{max_rounds}",
            ],
            "ohlcv_series_id_argv": [
                "xctx",
                "dry-run",
                "--ohlcv-series-id",
                "{ohlcv_series_id}",
                "--max-rounds",
                "{max_rounds}",
            ],
            "source_checkout_ohlcv_series_id_argv": [
                "./stock_universe.cli",
                "xctx",
                "dry-run",
                "--ohlcv-series-id",
                "{ohlcv_series_id}",
                "--max-rounds",
                "{max_rounds}",
            ],
            "source_checkout_ohlcv_series_id_bar_grain_argv": [
                "./stock_universe.cli",
                "xctx",
                "dry-run",
                "--ohlcv-series-id",
                "{ohlcv_series_id}",
                "--bar-grain",
                "{bar_grain}",
                "--max-rounds",
                "{max_rounds}",
            ],
            "db_override_argv": [
                "xctx",
                "dry-run",
                "--ohlcv-series-id",
                "{ohlcv_series_id}",
                "--db",
                "{db}",
                "--max-rounds",
                "{max_rounds}",
            ],
        },
        "xctx resolve-identity": {
            "structured_input": {
                "query": "ticker|name|cik|figi|ohlcv_series_id",
                "source": "live|db",
                "db": CANONICAL_DB,
                "api_key": "string?",
                "base_url": "https://api.massive.com",
                "limit": 25,
            },
            "result_contract": {
                "canonical_ohlcv_field": "ohlcv_series_id",
                "default_ohlcv_reporting_scope": "selected_candidate.ohlcv_series_id",
                "ticker_semantics": "ticker is an alias or point-in-time label and can exclude historical labels.",
                "agent_rule": (
                    "After DB identity resolution, answer OHLCV bar counts, latest days, and date ranges using "
                    "ohlcv_series_id unless the user explicitly asks for rows with a specific ticker label."
                ),
            },
            "argv": [
                "xctx",
                "resolve-identity",
                "--query",
                "{query}",
                "--source",
                "{source}",
                "--limit",
                "{limit}",
            ],
            "source_checkout_argv": [
                "./stock_universe.cli",
                "xctx",
                "resolve-identity",
                "--query",
                "{query}",
                "--source",
                "{source}",
                "--limit",
                "{limit}",
            ],
        },
        "xctx bars": {
            "structured_input": {
                "ohlcv_series_id": "integer?",
                "query": "ticker|name|cik|figi|ohlcv_series_id?",
                "db": CANONICAL_DB,
                "date": "date?",
                "from_date": "date?",
                "to_date": "date?",
                "bar_grain": "1d|1m|30m",
                "ticker_label": "ticker?",
                "limit": 5,
                "view": "simple|detail|extra_detail",
            },
            "result_contract": {
                "canonical_ohlcv_field": "ohlcv_series_id",
                "default_view": "simple",
                "detail_view": "identity, calendar, quality, and compact next actions",
                "extra_detail_view": "session_date, session_start_time, utc_start_ts, market_session_id, direct lineage id, raw provider sidecar payload, sparse quality-exception evidence, effects, and compact canonical metadata",
                "agent_rule": "Use simple view for ordinary price questions; use detail for why/calendar/quality questions; use extra_detail when auditing provenance, split repairs, raw provider payloads, or session/UTC mapping.",
            },
            "argv": [
                "xctx",
                "bars",
                "--ohlcv-series-id",
                "{ohlcv_series_id}",
                "--date",
                "{date}",
            ],
            "bar_grain_argv": [
                "xctx",
                "bars",
                "--ohlcv-series-id",
                "{ohlcv_series_id}",
                "--date",
                "{date}",
                "--bar-grain",
                "{bar_grain}",
            ],
            "query_date_argv": [
                "xctx",
                "bars",
                "--query",
                "{query}",
                "--date",
                "{date}",
            ],
            "date_range_argv": [
                "xctx",
                "bars",
                "--ohlcv-series-id",
                "{ohlcv_series_id}",
                "--from-date",
                "{from_date}",
                "--to-date",
                "{to_date}",
            ],
            "detail_argv": [
                "xctx",
                "bars",
                "--ohlcv-series-id",
                "{ohlcv_series_id}",
                "--date",
                "{date}",
                "--view",
                "detail",
            ],
            "extra_detail_argv": [
                "xctx",
                "bars",
                "--ohlcv-series-id",
                "{ohlcv_series_id}",
                "--date",
                "{date}",
                "--bar-grain",
                "{bar_grain}",
                "--view",
                "extra_detail",
            ],
            "source_checkout_argv": [
                "./stock_universe.cli",
                "xctx",
                "bars",
                "--ohlcv-series-id",
                "{ohlcv_series_id}",
                "--date",
                "{date}",
            ],
            "source_checkout_bar_grain_argv": [
                "./stock_universe.cli",
                "xctx",
                "bars",
                "--ohlcv-series-id",
                "{ohlcv_series_id}",
                "--date",
                "{date}",
                "--bar-grain",
                "{bar_grain}",
            ],
            "source_checkout_query_date_argv": [
                "./stock_universe.cli",
                "xctx",
                "bars",
                "--query",
                "{query}",
                "--date",
                "{date}",
            ],
            "source_checkout_extra_detail_argv": [
                "./stock_universe.cli",
                "xctx",
                "bars",
                "--ohlcv-series-id",
                "{ohlcv_series_id}",
                "--date",
                "{date}",
                "--bar-grain",
                "{bar_grain}",
                "--view",
                "extra_detail",
            ],
            "db_override_argv": [
                "xctx",
                "bars",
                "--db",
                "{db}",
                "--ohlcv-series-id",
                "{ohlcv_series_id}",
                "--date",
                "{date}",
            ],
        },
        "xctx validate": {
            "structured_input": {
                "fixture": "path",
                "omit_kind": ["evidence_kind"],
                "approve_execution": False,
            },
            "argv": ["xctx", "validate", "--fixture", "{fixture}"],
            "source_checkout_argv": [
                "./stock_universe.cli",
                "xctx",
                "validate",
                "--fixture",
                "{fixture}",
            ],
            "omit_kind_argv": [
                "xctx",
                "validate",
                "--fixture",
                "{fixture}",
                "--omit-kind",
                "{kind}",
            ],
        },
        "xctx doctor": {
            "structured_input": {
                "db": CANONICAL_DB,
                "api_key": "string?",
                "require_entrypoint": False,
            },
            "argv": ["xctx", "doctor"],
            "source_checkout_argv": ["./stock_universe.cli", "xctx", "doctor"],
            "db_override_argv": ["xctx", "doctor", "--db", "{db}"],
        },
        "xctx universe-status": {
            "structured_input": {"db": CANONICAL_DB},
            "argv": ["xctx", "universe-status"],
            "source_checkout_argv": ["./stock_universe.cli", "xctx", "universe-status"],
            "db_override_argv": ["xctx", "universe-status", "--db", "{db}"],
        },
        "xctx quality-audit": {
            "structured_input": {
                "db": CANONICAL_DB,
                "category": ["quality_category"],
                "exchange": ["mic"],
                "security_type": ["string"],
                "ticker": ["ticker"],
                "ohlcv_series_id": ["integer"],
                "include_healthy": False,
                "stale_before": "date?",
                "bar_grain": "1d|1m|30m",
                "limit": 50,
                "view": "simple|detail|extra_detail",
            },
            "argv": ["xctx", "quality-audit", "--limit", "{limit}"],
            "detail_argv": ["xctx", "quality-audit", "--view", "detail"],
            "extra_detail_argv": ["xctx", "quality-audit", "--view", "extra_detail"],
            "category_filter_argv": [
                "xctx",
                "quality-audit",
                "--category",
                "{category}",
                "--limit",
                "{limit}",
            ],
            "bar_grain_argv": [
                "xctx",
                "quality-audit",
                "--bar-grain",
                "{bar_grain}",
                "--limit",
                "{limit}",
            ],
            "source_checkout_argv": [
                "./stock_universe.cli",
                "xctx",
                "quality-audit",
                "--limit",
                "{limit}",
            ],
            "source_checkout_detail_argv": [
                "./stock_universe.cli",
                "xctx",
                "quality-audit",
                "--view",
                "detail",
            ],
            "source_checkout_extra_detail_argv": [
                "./stock_universe.cli",
                "xctx",
                "quality-audit",
                "--view",
                "extra_detail",
            ],
            "db_override_argv": [
                "xctx",
                "quality-audit",
                "--db",
                "{db}",
                "--limit",
                "{limit}",
            ],
        },
        "xctx examples": {
            "structured_input": {"command": "command-name?"},
            "argv": ["xctx", "examples"],
            "source_checkout_argv": ["./stock_universe.cli", "xctx", "examples"],
        },
        "xctx next": {
            "structured_input": {
                "fixture": "path",
                "omit_kind": ["evidence_kind"],
                "approve_execution": False,
            },
            "argv": ["xctx", "next", "--fixture", "{fixture}"],
            "source_checkout_argv": [
                "./stock_universe.cli",
                "xctx",
                "next",
                "--fixture",
                "{fixture}",
            ],
            "omit_kind_argv": [
                "xctx",
                "next",
                "--fixture",
                "{fixture}",
                "--omit-kind",
                "{kind}",
            ],
        },
        "xctx repair": {
            "structured_input": {"fixture": "path", "omit_kind": ["evidence_kind"]},
            "argv": [
                "xctx",
                "repair",
                "--fixture",
                "{fixture}",
                "--omit-kind",
                "{kind}",
            ],
            "source_checkout_argv": [
                "./stock_universe.cli",
                "xctx",
                "repair",
                "--fixture",
                "{fixture}",
                "--omit-kind",
                "{kind}",
            ],
        },
        "xctx compose": {
            "structured_input": {"recipe": "recipe-name?"},
            "argv": ["xctx", "compose"],
            "source_checkout_argv": ["./stock_universe.cli", "xctx", "compose"],
        },
        "stock-universe doctor": {
            "structured_input": {
                "db": CANONICAL_DB,
                "api_key": "string?",
                "require_entrypoint": False,
            },
            "argv": ["stock-universe", "doctor"],
            "source_checkout_argv": ["./stock_universe.cli", "doctor"],
            "db_override_argv": ["stock-universe", "doctor", "--db", "{db}"],
        },
        "stock-universe inspect-plan": {
            "structured_input": {"fixture": "path"},
            "argv": ["stock-universe", "inspect-plan", "--fixture", "{fixture}"],
            "source_checkout_argv": [
                "./stock_universe.cli",
                "inspect-plan",
                "--fixture",
                "{fixture}",
            ],
        },
        "stock-universe identity-search": {
            "structured_input": {
                "query": "ticker|name|cik|figi|ohlcv_series_id",
                "source": "live|db",
                "db": CANONICAL_DB,
                "api_key": "string?",
                "base_url": "https://api.massive.com",
                "limit": 25,
                "capture_dir": "path?",
            },
            "result_contract": {
                "canonical_ohlcv_field": "ohlcv_series_id",
                "default_ohlcv_reporting_scope": "selected_candidate.ohlcv_series_id",
                "ticker_semantics": "ticker is an alias or point-in-time label and can exclude historical labels.",
                "agent_rule": (
                    "For DB-backed results, answer OHLCV bar counts, latest days, and date ranges using "
                    "ohlcv_series_id unless the user explicitly asks for rows with a specific ticker label."
                ),
            },
            "argv": [
                "stock-universe",
                "identity-search",
                "--query",
                "{query}",
                "--source",
                "{source}",
                "--limit",
                "{limit}",
            ],
            "db_source_argv": [
                "stock-universe",
                "identity-search",
                "--query",
                "{query}",
                "--source",
                "db",
                "--db",
                "{db}",
                "--limit",
                "{limit}",
            ],
            "source_checkout_argv": [
                "./stock_universe.cli",
                "identity-search",
                "--query",
                "{query}",
                "--source",
                "{source}",
                "--limit",
                "{limit}",
            ],
        },
        "stock-universe update-reference-universe": {
            "structured_input": {
                "db": CANONICAL_DB,
                "api_key": "string?",
                "base_url": "https://api.massive.com",
                "market": "stocks",
                "exchange": "",
                "as_of_date": "date?",
                "active": "active|inactive|all",
                "limit": 1000,
                "max_pages": 100,
                "capture_dir": "path?",
                "commit": False,
                "heartbeat_seconds": 60,
                "summary_seconds": 180,
            },
            "argv": [
                "stock-universe",
                "update-reference-universe",
                "--limit",
                "{limit}",
                "--max-pages",
                "{max_pages}",
            ],
            "commit_argv": [
                "stock-universe",
                "update-reference-universe",
                "--limit",
                "{limit}",
                "--max-pages",
                "{max_pages}",
                "--commit",
            ],
            "progress_argv": [
                "stock-universe",
                "update-reference-universe",
                "--limit",
                "{limit}",
                "--max-pages",
                "{max_pages}",
                "--heartbeat-seconds",
                "{heartbeat_seconds}",
                "--summary-seconds",
                "{summary_seconds}",
            ],
            "db_override_argv": [
                "stock-universe",
                "update-reference-universe",
                "--db",
                "{db}",
                "--limit",
                "{limit}",
                "--max-pages",
                "{max_pages}",
            ],
        },
        "stock-universe dry-run": {
            "structured_input": {
                "fixture": "path?",
                "ticker": "ticker?",
                "ohlcv_series_id": "integer?",
                "source": "fixture|live",
                "db": CANONICAL_DB,
                "api_key": "string?",
                "base_url": "https://api.massive.com",
                "from_date": "date?",
                "to_date": "date?",
                "bar_grain": "1d|1m|30m",
                "identity_as_of_date": "date?",
                "max_rounds": DEFAULT_MAX_ROUNDS,
                "legacy_json_out": "path?",
                "markdown_out": "path?",
            },
            "argv": [
                "stock-universe",
                "dry-run",
                "--fixture",
                "{fixture}",
                "--source",
                "{source}",
                "--max-rounds",
                "{max_rounds}",
            ],
            "ticker_argv": [
                "stock-universe",
                "dry-run",
                "--ticker",
                "{ticker}",
                "--max-rounds",
                "{max_rounds}",
            ],
            "ticker_bar_grain_argv": [
                "stock-universe",
                "dry-run",
                "--ticker",
                "{ticker}",
                "--bar-grain",
                "{bar_grain}",
                "--max-rounds",
                "{max_rounds}",
            ],
            "ohlcv_series_id_argv": [
                "stock-universe",
                "dry-run",
                "--ohlcv-series-id",
                "{ohlcv_series_id}",
                "--max-rounds",
                "{max_rounds}",
            ],
            "report_argv": [
                "stock-universe",
                "dry-run",
                "--fixture",
                "{fixture}",
                "--legacy-json-out",
                "{legacy_json_out}",
                "--markdown-out",
                "{markdown_out}",
            ],
            "source_checkout_argv": [
                "./stock_universe.cli",
                "dry-run",
                "--fixture",
                "{fixture}",
                "--source",
                "{source}",
                "--max-rounds",
                "{max_rounds}",
            ],
        },
        "stock-universe backfill": {
            "structured_input": {
                "db": CANONICAL_DB,
                "ticker": ["ticker"],
                "fixture": ["path"],
                "ohlcv_series_id": ["integer"],
                "api_key": "string?",
                "base_url": "https://api.massive.com",
                "from_date": "date?",
                "to_date": "date?",
                "bar_grain": "1d|1m|30m",
                "identity_as_of_date": "date?",
                "max_rounds": DEFAULT_MAX_ROUNDS,
                "no_caution": False,
                "strict": True,
                "heartbeat_seconds": 60,
                "summary_seconds": 180,
            },
            "argv": ["stock-universe", "backfill", "--ticker", "{ticker}", "--strict"],
            "ticker_bar_grain_argv": [
                "stock-universe",
                "backfill",
                "--ticker",
                "{ticker}",
                "--bar-grain",
                "{bar_grain}",
                "--strict",
            ],
            "fixture_argv": [
                "stock-universe",
                "backfill",
                "--fixture",
                "{fixture}",
                "--strict",
            ],
            "ohlcv_series_id_argv": [
                "stock-universe",
                "backfill",
                "--ohlcv-series-id",
                "{ohlcv_series_id}",
                "--strict",
            ],
            "db_override_argv": [
                "stock-universe",
                "backfill",
                "--db",
                "{db}",
                "--ticker",
                "{ticker}",
                "--strict",
            ],
            "progress_argv": [
                "stock-universe",
                "backfill",
                "--ticker",
                "{ticker}",
                "--strict",
                "--heartbeat-seconds",
                "{heartbeat_seconds}",
                "--summary-seconds",
                "{summary_seconds}",
            ],
        },
        "stock-universe backfill-reference-batch": {
            "structured_input": {
                "db": CANONICAL_DB,
                "exchange": "",
                "market": "",
                "security_type": ["string"],
                "common_stock": False,
                "etf": False,
                "warrant": False,
                "unit": False,
                "adrc": False,
                "right": False,
                "preferred": False,
                "fund": False,
                "active": "active|inactive|all",
                "ohlcv_series_id": ["integer"],
                "identity_as_of_date": "date?",
                "limit": 25,
                "offset": 0,
                "api_key": "string?",
                "base_url": "https://api.massive.com",
                "from_date": "date?",
                "to_date": "date?",
                "bar_grain": "1d|1m|30m",
                "max_rounds": DEFAULT_MAX_ROUNDS,
                "no_caution": False,
                "commit": False,
                "strict": True,
                "heartbeat_seconds": 60,
                "summary_seconds": 180,
            },
            "argv": [
                "stock-universe",
                "backfill-reference-batch",
                "--limit",
                "{limit}",
                "--offset",
                "{offset}",
            ],
            "commit_argv": [
                "stock-universe",
                "backfill-reference-batch",
                "--limit",
                "{limit}",
                "--offset",
                "{offset}",
                "--commit",
                "--strict",
            ],
            "progress_commit_argv": [
                "stock-universe",
                "backfill-reference-batch",
                "--limit",
                "{limit}",
                "--offset",
                "{offset}",
                "--commit",
                "--strict",
                "--heartbeat-seconds",
                "{heartbeat_seconds}",
                "--summary-seconds",
                "{summary_seconds}",
            ],
            "exchange_filter_argv": [
                "stock-universe",
                "backfill-reference-batch",
                "--exchange",
                "{exchange}",
                "--limit",
                "{limit}",
                "--offset",
                "{offset}",
            ],
            "db_override_argv": [
                "stock-universe",
                "backfill-reference-batch",
                "--db",
                "{db}",
                "--limit",
                "{limit}",
                "--offset",
                "{offset}",
            ],
        },
        "stock-universe validate-db": {
            "structured_input": {
                "db": CANONICAL_DB,
                "heartbeat_seconds": 60,
                "summary_seconds": 180,
            },
            "argv": ["stock-universe", "validate-db"],
            "source_checkout_argv": ["./stock_universe.cli", "validate-db"],
            "db_override_argv": ["stock-universe", "validate-db", "--db", "{db}"],
            "progress_argv": [
                "stock-universe",
                "validate-db",
                "--heartbeat-seconds",
                "{heartbeat_seconds}",
                "--summary-seconds",
                "{summary_seconds}",
            ],
        },
        "stock-universe universe-status": {
            "structured_input": {"db": CANONICAL_DB},
            "argv": ["stock-universe", "universe-status"],
            "source_checkout_argv": ["./stock_universe.cli", "universe-status"],
            "db_override_argv": ["stock-universe", "universe-status", "--db", "{db}"],
        },
        "stock-universe quality-audit": {
            "structured_input": {
                "db": CANONICAL_DB,
                "category": ["quality_category"],
                "exchange": ["mic"],
                "security_type": ["string"],
                "ticker": ["ticker"],
                "ohlcv_series_id": ["integer"],
                "include_healthy": False,
                "stale_before": "date?",
                "bar_grain": "1d|1m|30m",
                "limit": 50,
            },
            "argv": ["stock-universe", "quality-audit", "--limit", "{limit}"],
            "category_filter_argv": [
                "stock-universe",
                "quality-audit",
                "--category",
                "{category}",
                "--limit",
                "{limit}",
            ],
            "bar_grain_argv": [
                "stock-universe",
                "quality-audit",
                "--bar-grain",
                "{bar_grain}",
                "--limit",
                "{limit}",
            ],
            "db_override_argv": [
                "stock-universe",
                "quality-audit",
                "--db",
                "{db}",
                "--limit",
                "{limit}",
            ],
        },
        "xctx catch-up-plan": {
            "structured_input": {
                "db": CANONICAL_DB,
                "workers": 10,
                "batch_size": 25,
                "target_limit": 0,
                "stale_before": "date?",
                "category": ["quality_category"],
                "exchange": ["mic"],
                "security_type": ["string"],
                "ohlcv_series_id": ["integer"],
                "ticker": ["ticker"],
                "from_date": "date?",
                "to_date": "date?",
                "bar_grain": "1d|1m|30m",
                "run_root": "path?",
                "run_dir": "path?",
                "view": "simple|detail|extra_detail",
                "detail_limit": 25,
            },
            "argv": ["xctx", "catch-up-plan"],
            "detail_argv": [
                "xctx",
                "catch-up-plan",
                "--view",
                "detail",
                "--detail-limit",
                "{detail_limit}",
            ],
            "extra_detail_argv": ["xctx", "catch-up-plan", "--view", "extra_detail"],
            "bounded_argv": [
                "xctx",
                "catch-up-plan",
                "--target-limit",
                "{target_limit}",
                "--workers",
                "{workers}",
            ],
            "bar_grain_argv": [
                "xctx",
                "catch-up-plan",
                "--bar-grain",
                "{bar_grain}",
                "--workers",
                "{workers}",
            ],
            "source_checkout_argv": ["./stock_universe.cli", "xctx", "catch-up-plan"],
            "source_checkout_detail_argv": [
                "./stock_universe.cli",
                "xctx",
                "catch-up-plan",
                "--view",
                "detail",
                "--detail-limit",
                "{detail_limit}",
            ],
            "source_checkout_extra_detail_argv": [
                "./stock_universe.cli",
                "xctx",
                "catch-up-plan",
                "--view",
                "extra_detail",
            ],
            "db_override_argv": [
                "xctx",
                "catch-up-plan",
                "--db",
                "{db}",
                "--workers",
                "{workers}",
            ],
        },
        "stock-universe catch-up": {
            "structured_input": {
                "db": CANONICAL_DB,
                "workers": 10,
                "batch_size": 25,
                "target_limit": 0,
                "stale_before": "date?",
                "category": ["quality_category"],
                "exchange": ["mic"],
                "security_type": ["string"],
                "ohlcv_series_id": ["integer"],
                "ticker": ["ticker"],
                "from_date": "date?",
                "to_date": "date?",
                "bar_grain": "1d|1m|30m",
                "run_root": "path?",
                "run_dir": "path?",
                "api_key": "string?",
                "base_url": "https://api.massive.com",
                "max_rounds": DEFAULT_MAX_ROUNDS,
                "no_caution": False,
                "commit": False,
                "strict": False,
                "fail_fast": True,
                "resume": False,
                "heartbeat_seconds": 60,
                "mini_summary_seconds": 240,
                "summary_seconds": 720,
                "resource_check_seconds": 600,
            },
            "argv": ["stock-universe", "catch-up", "--workers", "{workers}"],
            "dry_run_argv": [
                "stock-universe",
                "catch-up",
                "--workers",
                "{workers}",
                "--batch-size",
                "{batch_size}",
            ],
            "commit_argv": [
                "stock-universe",
                "catch-up",
                "--workers",
                "{workers}",
                "--commit",
                "--fail-fast",
            ],
            "bar_grain_commit_argv": [
                "stock-universe",
                "catch-up",
                "--workers",
                "{workers}",
                "--bar-grain",
                "{bar_grain}",
                "--commit",
                "--fail-fast",
            ],
            "status_aware_commit_argv": [
                "stock-universe",
                "catch-up",
                "--workers",
                "{workers}",
                "--commit",
                "--fail-fast",
                "--heartbeat-seconds",
                "60",
                "--mini-summary-seconds",
                "240",
                "--summary-seconds",
                "720",
                "--resource-check-seconds",
                "600",
            ],
            "db_override_argv": [
                "stock-universe",
                "catch-up",
                "--db",
                "{db}",
                "--workers",
                "{workers}",
            ],
        },
        "stock-universe catch-up-stop": {
            "structured_input": {
                "run_dir": "path",
                "reason": "operator requested stop",
                "requested_by": "operator",
                "mode": "drain|quiesce|abort",
            },
            "argv": ["stock-universe", "catch-up-stop", "--run-dir", "{run_dir}"],
            "source_checkout_argv": [
                "./stock_universe.cli",
                "catch-up-stop",
                "--run-dir",
                "{run_dir}",
            ],
            "reason_argv": [
                "stock-universe",
                "catch-up-stop",
                "--run-dir",
                "{run_dir}",
                "--reason",
                "{reason}",
                "--requested-by",
                "{requested_by}",
            ],
            "mode_argv": [
                "stock-universe",
                "catch-up-stop",
                "--run-dir",
                "{run_dir}",
                "--mode",
                "{mode}",
                "--reason",
                "{reason}",
                "--requested-by",
                "{requested_by}",
            ],
        },
        "stock-universe catch-up-reconcile": {
            "structured_input": {
                "run_dir": "path",
                "commit": False,
            },
            "argv": ["stock-universe", "catch-up-reconcile", "--run-dir", "{run_dir}"],
            "source_checkout_argv": [
                "./stock_universe.cli",
                "catch-up-reconcile",
                "--run-dir",
                "{run_dir}",
            ],
            "commit_argv": [
                "stock-universe",
                "catch-up-reconcile",
                "--run-dir",
                "{run_dir}",
                "--commit",
            ],
        },
        "xctx catch-up-status": {
            "structured_input": {
                "run_dir": "path?",
                "latest": False,
                "run_root": "path?",
                "view": "simple|detail|extra_detail",
            },
            "argv": ["xctx", "catch-up-status", "--run-dir", "{run_dir}"],
            "detail_argv": [
                "xctx",
                "catch-up-status",
                "--run-dir",
                "{run_dir}",
                "--view",
                "detail",
            ],
            "extra_detail_argv": [
                "xctx",
                "catch-up-status",
                "--run-dir",
                "{run_dir}",
                "--view",
                "extra_detail",
            ],
            "latest_argv": ["xctx", "catch-up-status", "--latest"],
            "source_checkout_argv": [
                "./stock_universe.cli",
                "xctx",
                "catch-up-status",
                "--run-dir",
                "{run_dir}",
            ],
            "source_checkout_extra_detail_argv": [
                "./stock_universe.cli",
                "xctx",
                "catch-up-status",
                "--run-dir",
                "{run_dir}",
                "--view",
                "extra_detail",
            ],
            "source_checkout_latest_argv": [
                "./stock_universe.cli",
                "xctx",
                "catch-up-status",
                "--latest",
            ],
        },
        "xctx catch-up-runs": {
            "structured_input": {
                "run_root": "path?",
                "limit": 5,
                "view": "simple|detail|extra_detail",
            },
            "argv": ["xctx", "catch-up-runs", "--limit", "{limit}"],
            "source_checkout_argv": [
                "./stock_universe.cli",
                "xctx",
                "catch-up-runs",
                "--limit",
                "{limit}",
            ],
        },
        "stock-universe repair-missing-receipts": {
            "structured_input": {
                "db": CANONICAL_DB,
                "ohlcv_series_id": ["integer"],
                "limit": 50,
                "reason": "string?",
                "commit": False,
            },
            "argv": ["stock-universe", "repair-missing-receipts", "--limit", "{limit}"],
            "commit_argv": [
                "stock-universe",
                "repair-missing-receipts",
                "--limit",
                "{limit}",
                "--commit",
            ],
            "ohlcv_series_id_commit_argv": [
                "stock-universe",
                "repair-missing-receipts",
                "--ohlcv-series-id",
                "{ohlcv_series_id}",
                "--commit",
            ],
            "db_override_argv": [
                "stock-universe",
                "repair-missing-receipts",
                "--db",
                "{db}",
                "--limit",
                "{limit}",
            ],
        },
        "stock-universe audit-executions": {
            "structured_input": {
                "db": CANONICAL_DB,
                "request_hash": "string?",
                "ohlcv_series_id": "integer?",
                "limit": 20,
            },
            "argv": ["stock-universe", "audit-executions", "--limit", "{limit}"],
            "source_checkout_argv": [
                "./stock_universe.cli",
                "audit-executions",
                "--limit",
                "{limit}",
            ],
            "db_override_argv": [
                "stock-universe",
                "audit-executions",
                "--db",
                "{db}",
                "--limit",
                "{limit}",
            ],
            "ohlcv_series_id_argv": [
                "stock-universe",
                "audit-executions",
                "--ohlcv-series-id",
                "{ohlcv_series_id}",
                "--limit",
                "{limit}",
            ],
        },
        "xctx observe": {
            "structured_input": {
                "db": CANONICAL_DB,
                "request_hash": "string?",
                "ohlcv_series_id": "integer?",
                "limit": 20,
                "view": "simple|detail|extra_detail",
            },
            "argv": ["xctx", "observe", "--limit", "{limit}"],
            "source_checkout_argv": [
                "./stock_universe.cli",
                "xctx",
                "observe",
                "--limit",
                "{limit}",
            ],
            "db_override_argv": [
                "xctx",
                "observe",
                "--db",
                "{db}",
                "--limit",
                "{limit}",
            ],
            "ohlcv_series_id_argv": [
                "xctx",
                "observe",
                "--ohlcv-series-id",
                "{ohlcv_series_id}",
                "--limit",
                "{limit}",
            ],
        },
    }
    return {
        name: _normalize_binding_argv(binding) for name, binding in bindings.items()
    }


def xctx_transition_graph() -> list[dict[str, Any]]:
    """Return the executable-context learning loop as transitions."""
    return [
        {
            "name": "doctor",
            "from": "local_environment",
            "to": "DoctorReport",
            "command": "xctx doctor",
        },
        {
            "name": "universe-status",
            "from": "canonical_db",
            "to": "UniverseStatus",
            "command": "xctx universe-status",
        },
        {
            "name": "quality-audit",
            "from": "canonical_db",
            "to": "QualityAudit",
            "command": "xctx quality-audit",
        },
        {
            "name": "catch-up-plan",
            "from": "canonical_db",
            "to": "CatchUpPlan",
            "command": "xctx catch-up-plan",
        },
        {
            "name": "catch-up-runs",
            "from": "catch_up_run_root",
            "to": "CatchUpRunList",
            "command": "xctx catch-up-runs",
        },
        {
            "name": "tree",
            "from": "unknown",
            "to": "tool_manifest",
            "command": "xctx tree",
        },
        {
            "name": "describe",
            "from": "tool_manifest",
            "to": "object_contract",
            "command": "xctx describe backfill-plan",
        },
        {
            "name": "schema",
            "from": "object_contract",
            "to": "binding_map",
            "command": "xctx schema",
        },
        {
            "name": "examples",
            "from": "binding_map",
            "to": "ExampleList",
            "command": "xctx examples",
        },
        {
            "name": "resolve-identity",
            "from": "identity_seed",
            "to": "IdentityCandidateList",
            "command": "xctx resolve-identity",
        },
        {
            "name": "bars",
            "from": "IdentityCandidateList",
            "to": "BarObservationList",
            "command": "xctx bars",
        },
        {
            "name": "update-reference-universe",
            "from": "reference_universe_request",
            "to": "ReferenceUniverseUpdate",
            "command": "stock-universe update-reference-universe",
        },
        {
            "name": "backfill-reference-batch",
            "from": "reference_universe_snapshot_selection",
            "to": "ReferenceBatchManifest",
            "command": "stock-universe backfill-reference-batch",
        },
        {
            "name": "validate-db",
            "from": "sqlite_db",
            "to": "DbValidation",
            "command": "stock-universe validate-db",
        },
        {
            "name": "validate",
            "from": "seed_input",
            "to": "ResultEnvelope",
            "command": "xctx validate",
        },
        {
            "name": "dry-run",
            "from": "seed_input",
            "to": "DryRunPlan",
            "command": "xctx dry-run",
        },
        {
            "name": "run",
            "from": "approved_plan",
            "to": "execution_receipt",
            "command": "stock-universe backfill",
        },
        {
            "name": "catch-up-run",
            "from": "CatchUpPlan",
            "to": "CatchUpRunStatus",
            "command": "stock-universe catch-up",
        },
        {
            "name": "catch-up-stop",
            "from": "catch_up_run_artifacts",
            "to": "CatchUpStopRequest",
            "command": "stock-universe catch-up-stop",
        },
        {
            "name": "catch-up-reconcile",
            "from": "stale_or_partial_catch_up_artifacts",
            "to": "CatchUpReconciliation",
            "command": "stock-universe catch-up-reconcile",
        },
        {
            "name": "catch-up-status",
            "from": "catch_up_run_artifacts",
            "to": "CatchUpRunStatus",
            "command": "xctx catch-up-status",
        },
        {
            "name": "observe",
            "from": "execution_receipt",
            "to": "audit_evidence",
            "command": "xctx observe",
        },
        {
            "name": "repair",
            "from": "RepairError",
            "to": "repair_action",
            "command": "xctx repair",
        },
        {
            "name": "next",
            "from": "ResultEnvelope",
            "to": "NextAction",
            "command": "xctx next",
        },
        {
            "name": "compose",
            "from": "transitions",
            "to": "Recipe",
            "command": "xctx compose",
        },
    ]


def xctx_recipes() -> list[dict[str, Any]]:
    """Return workflow recipes composed from executable-context transitions."""
    return _normalize_recipe_commands(
        [
            {
                "name": "fixture-live-backfill",
                "description": "Plan, execute, validate, and observe a fixture-seeded live backfill.",
                "steps": [
                    {"transition": "doctor", "command": "stock-universe doctor"},
                    {
                        "transition": "dry-run",
                        "command": "xctx dry-run --source live --fixture {fixture}",
                    },
                    {"transition": "next", "command": "xctx next --fixture {fixture}"},
                    {
                        "transition": "run",
                        "command": "stock-universe backfill --fixture {fixture} --strict",
                        "agent_reporting": backfill_reporting_policy(),
                    },
                    {"transition": "observe", "command": "xctx observe"},
                ],
            },
            {
                "name": "ticker-live-backfill",
                "description": "Resolve ticker seed facts, execute through the approved CLI, then audit receipts.",
                "steps": [
                    {"transition": "doctor", "command": "stock-universe doctor"},
                    {
                        "transition": "dry-run",
                        "command": f"xctx dry-run --ticker {{ticker}} --max-rounds {DEFAULT_MAX_ROUNDS}",
                    },
                    {
                        "transition": "run",
                        "command": "stock-universe backfill --ticker {ticker} --strict",
                        "agent_reporting": backfill_reporting_policy(),
                    },
                    {"transition": "observe", "command": "xctx observe"},
                ],
            },
            {
                "name": "identity-first-ticker-backfill",
                "description": "Search identity candidates, explicitly choose a ticker seed, execute, then audit receipts.",
                "steps": [
                    {
                        "transition": "resolve-identity",
                        "command": "xctx resolve-identity --query {query} --source live",
                    },
                    {"transition": "doctor", "command": "stock-universe doctor"},
                    {
                        "transition": "dry-run",
                        "command": f"xctx dry-run --ticker {{selected_ticker}} --max-rounds {DEFAULT_MAX_ROUNDS}",
                    },
                    {
                        "transition": "run",
                        "command": "stock-universe backfill --ticker {selected_ticker} --strict",
                        "agent_reporting": backfill_reporting_policy(),
                    },
                    {"transition": "observe", "command": "xctx observe"},
                ],
            },
            {
                "name": "db-ohlcv-series-id-backfill",
                "description": "Resolve candidates from a persisted reference universe, select one OHLCV series ID, dry-run, execute, and observe.",
                "steps": [
                    {
                        "transition": "universe-status",
                        "command": "xctx universe-status",
                    },
                    {
                        "transition": "resolve-identity",
                        "command": "xctx resolve-identity --source db --query {query}",
                    },
                    {
                        "transition": "dry-run",
                        "command": f"xctx dry-run --ohlcv-series-id {{selected_ohlcv_series_id}} --max-rounds {DEFAULT_MAX_ROUNDS}",
                    },
                    {
                        "transition": "run",
                        "command": "stock-universe backfill --ohlcv-series-id {selected_ohlcv_series_id} --strict",
                        "agent_reporting": backfill_reporting_policy(),
                    },
                    {"transition": "observe", "command": "xctx observe"},
                ],
            },
            {
                "name": "db-identity-bar-observation",
                "description": "Resolve a persisted identity, then read the canonical OHLCV frame the stock system would consume.",
                "steps": [
                    {
                        "transition": "universe-status",
                        "command": "xctx universe-status",
                    },
                    {
                        "transition": "resolve-identity",
                        "command": "xctx resolve-identity --source db --query {query}",
                    },
                    {
                        "transition": "bars",
                        "command": "xctx bars --ohlcv-series-id {selected_ohlcv_series_id} --date {date}",
                    },
                ],
            },
            {
                "name": "bar-provenance-audit",
                "description": "Resolve one persisted identity, inspect one bar with session/UTC/direct-lineage/raw-sidecar detail, then validate storage invariants.",
                "steps": [
                    {"transition": "doctor", "command": "xctx doctor"},
                    {
                        "transition": "resolve-identity",
                        "command": "xctx resolve-identity --source db --query {query}",
                    },
                    {
                        "transition": "bars",
                        "command": "xctx bars --ohlcv-series-id {selected_ohlcv_series_id} --date {date} --bar-grain {bar_grain} --view extra_detail",
                    },
                    {
                        "transition": "observe",
                        "command": "xctx observe --ohlcv-series-id {selected_ohlcv_series_id} --limit 5",
                    },
                    {
                        "transition": "validate-db",
                        "command": "stock-universe validate-db",
                        "agent_reporting": validate_db_reporting_policy(),
                    },
                ],
            },
            {
                "name": "reference-universe-maintenance",
                "description": "Refresh, validate, and search the single canonical reference-universe snapshot.",
                "agent_reporting": recipe_reporting_policy(
                    workflow="reference-universe-maintenance"
                ),
                "steps": [
                    {"transition": "doctor", "command": "stock-universe doctor"},
                    {
                        "transition": "update-reference-universe",
                        "command": "stock-universe update-reference-universe --limit 1000 --max-pages 100",
                        "agent_reporting": update_reference_universe_reporting_policy(),
                    },
                    {
                        "transition": "update-reference-universe",
                        "command": "stock-universe update-reference-universe --limit 1000 --max-pages 100 --commit",
                        "agent_reporting": update_reference_universe_reporting_policy(),
                    },
                    {
                        "transition": "validate-db",
                        "command": "stock-universe validate-db",
                        "agent_reporting": validate_db_reporting_policy(),
                    },
                    {
                        "transition": "universe-status",
                        "command": "xctx universe-status",
                    },
                    {
                        "transition": "resolve-identity",
                        "command": "xctx resolve-identity --source db --query {query}",
                    },
                ],
            },
            {
                "name": "reference-batch-backfill",
                "description": "Enumerate a bounded persisted reference-universe slice, commit selected OHLCV series IDs, and observe receipts.",
                "steps": [
                    {
                        "transition": "universe-status",
                        "command": "xctx universe-status",
                    },
                    {
                        "transition": "backfill-reference-batch",
                        "command": "stock-universe backfill-reference-batch --limit {limit} --offset {offset}",
                    },
                    {
                        "transition": "backfill-reference-batch",
                        "command": "stock-universe backfill-reference-batch --limit {limit} --offset {offset} --commit --strict",
                        "agent_reporting": backfill_reference_batch_reporting_policy(),
                    },
                    {"transition": "observe", "command": "xctx observe"},
                ],
            },
            {
                "name": "stock-universe-health-check",
                "description": "Answer current stock-universe status and catch-up need with compact read-oriented outputs.",
                "steps": [
                    {"transition": "doctor", "command": "xctx doctor"},
                    {
                        "transition": "universe-status",
                        "command": "xctx universe-status",
                    },
                    {"transition": "quality-audit", "command": "xctx quality-audit"},
                    {
                        "transition": "catch-up-runs",
                        "command": "xctx catch-up-runs --limit 3",
                    },
                    {
                        "transition": "catch-up-status",
                        "command": "xctx catch-up-status --latest",
                    },
                    {
                        "transition": "catch-up-plan",
                        "command": "xctx catch-up-plan --workers 10 --batch-size 25 --category bar_expected_but_missing --category covered_series_data_stale --category listed_common_stock_data_stale --category plan_session_gap --category provider_zero_bar_response_stale",
                    },
                    {
                        "transition": "catch-up-plan",
                        "command": "xctx catch-up-plan --workers 10 --batch-size 25 --category data_not_loaded",
                    },
                ],
            },
            {
                "name": "database-catch-up",
                "description": "Plan, execute, monitor, and re-audit deterministic non-initial-backfill database catch-up work.",
                "agent_reporting": recipe_reporting_policy(
                    workflow="database-catch-up"
                ),
                "steps": [
                    {"transition": "doctor", "command": "xctx doctor"},
                    {
                        "transition": "universe-status",
                        "command": "xctx universe-status",
                    },
                    {"transition": "quality-audit", "command": "xctx quality-audit"},
                    {
                        "transition": "catch-up-runs",
                        "command": "xctx catch-up-runs --limit 3",
                    },
                    {
                        "transition": "catch-up-status",
                        "command": "xctx catch-up-status --latest",
                    },
                    {
                        "transition": "catch-up-plan",
                        "command": "xctx catch-up-plan --workers 10 --batch-size 25 --category bar_expected_but_missing --category covered_series_data_stale --category listed_common_stock_data_stale --category plan_session_gap --category provider_zero_bar_response_stale",
                        "agent_reporting": soft_long_running_reporting_policy(
                            action="catch-up planning",
                            status_source="stdout CatchUpPlan with commit next_action when complete",
                        ),
                    },
                    {
                        "transition": "catch-up-run",
                        "command": "stock-universe catch-up --workers 10 --batch-size 25 --category bar_expected_but_missing --category covered_series_data_stale --category listed_common_stock_data_stale --category plan_session_gap --category provider_zero_bar_response_stale --commit --fail-fast",
                        "agent_reporting": catch_up_reporting_policy(),
                    },
                    {
                        "transition": "catch-up-status",
                        "command": "xctx catch-up-status --run-dir {run_dir}",
                    },
                    {
                        "transition": "validate-db",
                        "command": "stock-universe validate-db",
                        "agent_reporting": validate_db_reporting_policy(),
                    },
                    {"transition": "quality-audit", "command": "xctx quality-audit"},
                    {"transition": "observe", "command": "xctx observe --limit 50"},
                ],
            },
            {
                "name": "data-not-loaded-catch-up",
                "description": "Plan, execute, monitor, and re-audit initial data_not_loaded work as a separate high-throughput pass.",
                "agent_reporting": recipe_reporting_policy(
                    workflow="data-not-loaded-catch-up"
                ),
                "steps": [
                    {"transition": "doctor", "command": "xctx doctor"},
                    {
                        "transition": "universe-status",
                        "command": "xctx universe-status",
                    },
                    {
                        "transition": "quality-audit",
                        "command": "xctx quality-audit --category data_not_loaded",
                    },
                    {
                        "transition": "catch-up-runs",
                        "command": "xctx catch-up-runs --limit 3",
                    },
                    {
                        "transition": "catch-up-status",
                        "command": "xctx catch-up-status --latest",
                    },
                    {
                        "transition": "catch-up-plan",
                        "command": "xctx catch-up-plan --workers 10 --batch-size 25 --category data_not_loaded",
                        "agent_reporting": soft_long_running_reporting_policy(
                            action="data_not_loaded catch-up planning",
                            status_source="stdout CatchUpPlan with commit next_action when complete",
                        ),
                    },
                    {
                        "transition": "catch-up-run",
                        "command": "stock-universe catch-up --workers 10 --batch-size 25 --category data_not_loaded --commit --fail-fast",
                        "agent_reporting": catch_up_reporting_policy(),
                    },
                    {
                        "transition": "catch-up-status",
                        "command": "xctx catch-up-status --run-dir {run_dir}",
                    },
                    {
                        "transition": "validate-db",
                        "command": "stock-universe validate-db",
                        "agent_reporting": validate_db_reporting_policy(),
                    },
                    {
                        "transition": "quality-audit",
                        "command": "xctx quality-audit --category data_not_loaded",
                    },
                    {"transition": "observe", "command": "xctx observe --limit 50"},
                ],
            },
            {
                "name": "repair-evidence-gap",
                "description": "Turn unresolved evidence into an explicit repair path.",
                "steps": [
                    {
                        "transition": "dry-run",
                        "command": "xctx dry-run --fixture {fixture} --defer-kind {kind}",
                    },
                    {
                        "transition": "repair",
                        "command": "xctx repair --fixture {fixture} --omit-kind {kind}",
                    },
                    {"transition": "next", "command": "xctx next --fixture {fixture}"},
                ],
            },
        ]
    )


def xctx_tool_manifest() -> dict[str, Any]:
    """Return the autodidactic tool manifest for the xctx namespace."""
    return {
        "object_type": "ToolManifest",
        "protocol_version": PROTOCOL_VERSION,
        "name": "Executable Context",
        "namespace": "xctx",
        "design_pattern": "Autodidactic Interfaces",
        "core_claim": "Static context orients agents. Executable context trains agents.",
        "slogan": "The interface is the curriculum when it changes behavior.",
        "execution_rule": "Run recipe command fields or schema argv/source_checkout_argv; command names and logical_command values are logical identifiers.",
        "core_unit": "Transition",
        "entrypoints": {
            "source_checkout": "./stock_universe.cli xctx",
            "installed_nested": "stock-universe xctx",
            "installed_standalone": "xctx",
        },
        "recommended_agent_loop": [
            "./stock_universe.cli xctx doctor",
            "./stock_universe.cli xctx universe-status",
            "./stock_universe.cli xctx tree",
            './stock_universe.cli xctx schema --command "xctx dry-run"',
            './stock_universe.cli xctx schema --command "xctx bars"',
            "./stock_universe.cli xctx examples",
            "./stock_universe.cli xctx compose --recipe bar-provenance-audit",
            "./stock_universe.cli xctx dry-run --fixture tests/fixtures/legacy_plans/simple_current_sfbc.json",
            "./stock_universe.cli xctx next --fixture tests/fixtures/legacy_plans/simple_current_sfbc.json",
            "./stock_universe.cli backfill --fixture <fixture> --strict",
            "./stock_universe.cli xctx observe",
        ],
        "core_loop": [
            "doctor",
            "universe-status",
            "quality-audit",
            "catch-up-plan",
            "catch-up-runs",
            "tree",
            "capabilities",
            "describe",
            "schema",
            "examples",
            "resolve-identity",
            "bars",
            "validate",
            "dry-run",
            "run",
            "catch-up-run",
            "catch-up-stop",
            "catch-up-reconcile",
            "catch-up-status",
            "observe",
            "repair",
            "next",
            "compose",
        ],
        "core_objects": [
            "ToolManifest",
            "CommandSpec",
            "BindingMap",
            "EffectSpec",
            "CapabilityList",
            "CommandDescription",
            "DryRunPlan",
            "IdentityCandidateList",
            "BarObservationList",
            "ReferenceUniverseUpdate",
            "ReferenceBatchManifest",
            "UniverseStatus",
            "QualityAudit",
            "CatchUpPlan",
            "CatchUpRunList",
            "CatchUpRunStatus",
            "CatchUpReconciliation",
            "DbValidation",
            "ResultEnvelope",
            "RepairError",
            "NextAction",
            "Recipe",
            "AgentReportingPolicy",
            "DoctorReport",
            "ExecutionAudit",
        ],
        "transitions": xctx_transition_graph(),
        "commands": list(XCTX_COMMAND_SCHEMAS),
    }


@dataclass(frozen=True)
class CommandSpec:
    name: str
    description: str
    args: dict[str, Any] | None = None
    reads: tuple[str, ...] = ()
    writes: tuple[str, ...] = ()
    agent_reporting: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "name": self.name,
            "description": self.description,
            "args": self.args or {},
            "reads": list(self.reads),
            "writes": list(self.writes),
        }
        if self.agent_reporting is not None:
            payload["agent_reporting"] = self.agent_reporting
        return payload


@dataclass(frozen=True)
class EffectSpec:
    kind: EffectKind
    target: str
    description: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "target": self.target,
            "description": self.description,
        }


@dataclass(frozen=True)
class NextAction:
    name: str
    kind: ActionKind
    command: CommandSpec
    effects: tuple[EffectSpec, ...] = ()
    requires_approval: bool = False
    reason: str = ""
    authority_level: AuthorityLevel | str = ""
    agent_reporting: dict[str, Any] | None = None
    argv: tuple[str, ...] = ()
    source_checkout_argv: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "name": self.name,
            "kind": self.kind,
            "command": self.command.to_dict(),
            "effects": [effect.to_dict() for effect in self.effects],
            "requires_approval": self.requires_approval,
            "authority_level": self.authority_level
            or infer_action_authority(
                kind=self.kind,
                command_name=self.command.name,
                effects=self.effects,
                requires_approval=self.requires_approval,
            ),
        }
        if self.reason:
            payload["reason"] = self.reason
        if self.agent_reporting is not None:
            payload["agent_reporting"] = self.agent_reporting
        if self.argv:
            payload["argv"] = list(self.argv)
        if self.source_checkout_argv:
            payload["source_checkout_argv"] = list(self.source_checkout_argv)
        return payload


def infer_action_authority(
    *,
    kind: str,
    command_name: str = "",
    effects: tuple[EffectSpec, ...] | list[dict[str, Any]] | list[Any] = (),
    requires_approval: bool = False,
) -> AuthorityLevel | str:
    if kind == "approval":
        return "approval"
    if kind == "repair":
        return "repair"
    effect_items = list(effects or [])
    effect_kinds = {_effect_value(effect, "kind") for effect in effect_items}
    targets = [_effect_value(effect, "target") for effect in effect_items]
    command = command_name.lower()
    if "execute-plan" in effect_kinds:
        return "execution"
    if (
        command_name in {"stock-universe backfill", "stock-universe catch-up"}
        and "write" in effect_kinds
    ):
        return "execution"
    if "write" in effect_kinds:
        if any(_is_db_target(target) for target in targets):
            return "db_write"
        return "file_write"
    if "read" in effect_kinds:
        if any(_is_network_target(target) for target in targets):
            return "network_read"
        return "read"
    if requires_approval and (
        "commit" in command or "backfill" in command or "repair" in command
    ):
        return "db_write"
    return "none"


def normalize_action_records(value: Any) -> Any:
    """Add compact authority metadata to typed action records in a payload."""
    if isinstance(value, list):
        return [normalize_action_records(item) for item in value]
    if not isinstance(value, dict):
        return value
    payload = {key: normalize_action_records(item) for key, item in value.items()}
    if _is_action_record(payload) and not payload.get("authority_level"):
        command = (
            payload.get("command") if isinstance(payload.get("command"), dict) else {}
        )
        payload["authority_level"] = infer_action_authority(
            kind=str(payload.get("kind") or ""),
            command_name=str(command.get("name") or ""),
            effects=list(payload.get("effects") or []),
            requires_approval=bool(payload.get("requires_approval")),
        )
    return payload


def _is_action_record(payload: dict[str, Any]) -> bool:
    return {"name", "kind", "command", "effects", "requires_approval"} <= set(payload)


def _effect_value(effect: Any, key: str) -> str:
    if isinstance(effect, EffectSpec):
        return str(getattr(effect, key))
    if isinstance(effect, dict):
        return str(effect.get(key) or "")
    return ""


def _is_db_target(target: str) -> bool:
    lowered = target.lower()
    return "sqlite" in lowered or lowered.endswith(".db") or lowered.endswith(".sqlite")


def _is_network_target(target: str) -> bool:
    lowered = target.lower()
    return (
        "massive" in lowered
        or " api" in lowered
        or lowered.endswith("api")
        or lowered.startswith("http")
    )


@dataclass(frozen=True)
class InvalidAction:
    name: str
    command: CommandSpec
    reason: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "command": self.command.to_dict(),
            "reason": self.reason,
        }


@dataclass(frozen=True)
class RepairAction:
    name: str
    evidence_kind: str
    request: dict[str, Any]
    effect: EffectSpec
    reason: str
    command: CommandSpec

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "evidence_kind": self.evidence_kind,
            "request": self.request,
            "effect": self.effect.to_dict(),
            "reason": self.reason,
            "command": self.command.to_dict(),
        }


def result_envelope_schema() -> dict[str, Any]:
    """Return a compact JSON-schema-like contract for xctx result envelopes."""
    return {
        "protocol_version": PROTOCOL_VERSION,
        "type": "object",
        "required": [
            "protocol_version",
            "ok",
            "command",
            "result_type",
            "next_actions",
        ],
        "properties": {
            "protocol_version": {"const": PROTOCOL_VERSION},
            "ok": {"type": "boolean"},
            "command": {"type": "string"},
            "result_type": {"enum": ["BackfillPlan", "EvidenceNeeded"]},
            "status": {"enum": ["safe", "caution", "blocked"]},
            "evidence_ledger_hash": {"type": "string"},
            "decisions": {"type": "array"},
            "requests": {"type": "array"},
            "next_actions": {"type": "array", "items": {"$ref": "#/$defs/NextAction"}},
            "invalid_next_actions": {
                "type": "array",
                "items": {"$ref": "#/$defs/InvalidAction"},
            },
            "repairs": {"type": "array", "items": {"$ref": "#/$defs/RepairAction"}},
            "command_schemas": {"type": "object"},
            "agent_reporting": {"$ref": "#/$defs/AgentReportingPolicy"},
            "views": {"type": "object"},
        },
        "$defs": {
            "CommandSpec": {
                "type": "object",
                "required": ["name", "description", "args", "reads", "writes"],
                "properties": {
                    "agent_reporting": {"$ref": "#/$defs/AgentReportingPolicy"},
                    "views": {"type": "object"},
                },
            },
            "EffectSpec": {
                "type": "object",
                "required": ["kind", "target", "description"],
            },
            "NextAction": {
                "type": "object",
                "required": [
                    "name",
                    "kind",
                    "command",
                    "effects",
                    "requires_approval",
                    "authority_level",
                ],
                "properties": {
                    "authority_level": {
                        "enum": [
                            "none",
                            "read",
                            "network_read",
                            "file_write",
                            "db_write",
                            "execution",
                            "approval",
                            "repair",
                        ]
                    },
                    "agent_reporting": {"$ref": "#/$defs/AgentReportingPolicy"},
                    "argv": {"type": "array"},
                    "source_checkout_argv": {"type": "array"},
                },
            },
            "InvalidAction": {
                "type": "object",
                "required": ["name", "command", "reason"],
            },
            "RepairAction": {
                "type": "object",
                "required": [
                    "name",
                    "evidence_kind",
                    "request",
                    "effect",
                    "reason",
                    "command",
                ],
            },
            "ToolManifest": {
                "type": "object",
                "required": [
                    "object_type",
                    "protocol_version",
                    "namespace",
                    "core_loop",
                    "transitions",
                ],
            },
            "BindingMap": {
                "type": "object",
                "required": ["structured_input", "argv"],
            },
            "Recipe": {
                "type": "object",
                "required": ["name", "description", "steps"],
                "properties": {
                    "agent_reporting": {"$ref": "#/$defs/AgentReportingPolicy"}
                },
            },
            "AgentReportingPolicy": {
                "type": "object",
                "required": [
                    "version",
                    "applies_when",
                    "native_progress",
                    "poll_seconds",
                    "first_user_update_seconds",
                    "user_update_seconds",
                    "stall_seconds",
                    "quiet_when_healthy",
                    "immediate_on",
                    "final_report",
                    "begin",
                    "routine",
                    "immediate_update_on",
                    "final",
                    "operator_override",
                ],
            },
            "DoctorReport": {
                "type": "object",
                "required": ["ok", "checks"],
            },
            "DryRunPlan": {
                "type": "object",
                "required": ["rounds", "next_actions", "effects"],
            },
            "RepairError": {
                "type": "object",
                "required": ["name", "reason", "repair"],
            },
        },
    }
