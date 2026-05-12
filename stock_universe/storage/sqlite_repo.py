"""SQLite repository for durable planner evidence, plans, bars, and receipts."""

from __future__ import annotations

import datetime as dt
import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable
from zoneinfo import ZoneInfo

from stock_universe.domain import BackfillPlan, EvidenceFact
from stock_universe.domain.common import stable_json_hash
from stock_universe.market_calendar import (
    DEFAULT_US_EQUITY_CALENDAR_ID,
    MarketSession,
    iter_us_equity_sessions,
    us_equity_session_for_date,
    us_equity_session_for_utc_ts,
)
from stock_universe.storage.sqlite_access import (
    connect_readonly_sqlite,
    connect_sqlite_database,
    readonly_db_uri,
)


SCHEMA_VERSION = "stock_universe_sqlite.v10_session_lineage_fact_membership"
SUPPORTED_TIMESPANS = ("day", "hour", "minute")
BAR_TABLE_BY_TIMESPAN = {
    "day": "ohlcv_bars_day",
    "hour": "ohlcv_bars_hour",
    "minute": "ohlcv_bars_minute",
}
QUALITY_TABLE_BY_TIMESPAN = {
    "day": "ohlcv_day_bar_quality_events",
    "hour": "ohlcv_hour_bar_quality_events",
    "minute": "ohlcv_minute_bar_quality_events",
}
NORMAL_BAR_QUALITY_STATUSES = {"VALIDATED", "UNCHECKED"}
SEEDED_US_EQUITY_CALENDAR_IDS = (
    DEFAULT_US_EQUITY_CALENDAR_ID,
    "ARCX",
    "BATS",
    "XASE",
    "XNAS",
    "XNYS",
)


@dataclass(frozen=True)
class StoredOhlcvBar:
    series_id: int
    ticker: str
    bar_date: str
    bar_start_ts: int
    multiplier: int
    timespan: str
    adjusted: bool
    open: float | None
    high: float | None
    low: float | None
    close: float | None
    volume: float | None
    vwap: float | None = None
    transaction_count: int | None = None
    source: str = "massive.aggregate_bars"
    calendar_id: str = DEFAULT_US_EQUITY_CALENDAR_ID
    request_hash: str = ""
    ledger_hash: str = ""
    segment_index: int | None = None
    bar_quality_status: str = "UNCHECKED"
    repair_rule: str = ""
    raw_bar_json: Any = None
    repair_evidence_json: Any = None


@dataclass(frozen=True)
class StoredReferenceSnapshot:
    provider: str
    snapshot_as_of_date: str
    ticker: str
    ohlcv_series_id: int = 0
    active: bool | int | None = None
    company_name: str = ""
    cik: str = ""
    composite_figi: str = ""
    share_class_figi: str = ""
    security_type: str = ""
    primary_exchange: str = ""
    market: str = ""
    locale: str = ""
    identity_status: str = ""
    natural_key: str = ""
    provisional_key: str = ""
    raw: Any = None
    source_request: Any = None


@dataclass(frozen=True)
class _PreparedOhlcvBar:
    bar: StoredOhlcvBar
    scope_id: int
    market_session_id: int
    session_start_time: str
    utc_start_ts: int


@dataclass(frozen=True)
class ValidationReport:
    ok: bool
    checks: tuple[str, ...]
    failures: tuple[str, ...] = ()


def connect_sqlite(path: str | Path) -> sqlite3.Connection:
    db_path = Path(path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = connect_sqlite_database(db_path, timeout=60.0)
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA busy_timeout = 60000")
    return conn


def _reset_incompatible_schema(conn: sqlite3.Connection) -> None:
    views = (
        "v_ohlcv_bars_unified",
        "v_ohlcv_bars_hot_unified",
    )
    tables = (
        "ohlcv_day_bar_quality_events",
        "ohlcv_hour_bar_quality_events",
        "ohlcv_minute_bar_quality_events",
        "ohlcv_bars_day",
        "ohlcv_bars_hour",
        "ohlcv_bars_minute",
        "ohlcv_bar_raw_payloads",
        "ohlcv_bar_lineage",
        "ohlcv_bar_scopes",
        "market_sessions",
        "data_sources",
        "ticker_aliases",
        "evidence_ledger_facts",
        "evidence_facts",
        "backfill_plans",
        "ohlcv_bars",
        "execution_receipts",
        "execution_approvals",
        "reference_universe_snapshots",
        "reference_universe_updates",
        "ohlcv_series",
        "ohlcv_series_id_lookup",
        "schema_metadata",
    )
    try:
        present_tables = {
            str(row[0])
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        }
    except sqlite3.Error:
        return
    if "schema_metadata" not in present_tables:
        if not (present_tables & set(tables)):
            return
        version = ""
    else:
        version_row = conn.execute(
            "SELECT value FROM schema_metadata WHERE key = 'schema_version'"
        ).fetchone()
        version = str(version_row[0]) if version_row else ""
    if version == SCHEMA_VERSION:
        return
    conn.execute("PRAGMA foreign_keys = OFF")
    try:
        for view in views:
            conn.execute(f"DROP VIEW IF EXISTS {view}")
        for table in tables:
            conn.execute(f"DROP TABLE IF EXISTS {table}")
        conn.execute("PRAGMA user_version = 0")
        conn.commit()
    finally:
        conn.execute("PRAGMA foreign_keys = ON")


def initialize_schema(conn: sqlite3.Connection) -> None:
    _reset_incompatible_schema(conn)
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS schema_metadata (
          key TEXT PRIMARY KEY,
          value TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS data_sources (
          source_id INTEGER PRIMARY KEY,
          source_name TEXT NOT NULL UNIQUE,
          CHECK(source_name != '')
        );

        CREATE TABLE IF NOT EXISTS market_sessions (
          market_session_id INTEGER PRIMARY KEY,
          calendar_id TEXT NOT NULL,
          session_date TEXT NOT NULL,
          timezone_name TEXT NOT NULL,
          regular_open_time TEXT NOT NULL,
          regular_close_time TEXT NOT NULL,
          session_open_time TEXT NOT NULL,
          session_close_time TEXT NOT NULL,
          regular_open_utc_ts INTEGER NOT NULL,
          regular_close_utc_ts INTEGER NOT NULL,
          session_open_utc_ts INTEGER NOT NULL,
          session_close_utc_ts INTEGER NOT NULL,
          settlement_date TEXT NOT NULL DEFAULT '',
          source_hash TEXT NOT NULL,
          CHECK(calendar_id != ''),
          CHECK(session_date != ''),
          CHECK(timezone_name != ''),
          CHECK(regular_open_utc_ts < regular_close_utc_ts),
          CHECK(session_open_utc_ts <= regular_open_utc_ts),
          CHECK(regular_close_utc_ts <= session_close_utc_ts),
          UNIQUE(calendar_id, session_date)
        );

        CREATE TABLE IF NOT EXISTS ohlcv_series_id_lookup (
          ohlcv_series_id INTEGER PRIMARY KEY,
          natural_key TEXT NOT NULL UNIQUE,
          first_seen_utc TEXT NOT NULL,
          last_seen_utc TEXT NOT NULL,
          CHECK(natural_key != '')
        );

        CREATE TABLE IF NOT EXISTS ohlcv_series (
          ohlcv_series_id INTEGER PRIMARY KEY,
          composite_figi TEXT NOT NULL DEFAULT '',
          share_class_figi TEXT NOT NULL DEFAULT '',
          latest_ticker TEXT NOT NULL DEFAULT '',
          identity_status TEXT NOT NULL DEFAULT '',
          company_name TEXT NOT NULL DEFAULT '',
          target_json TEXT NOT NULL,
          first_seen_utc TEXT NOT NULL,
          last_seen_utc TEXT NOT NULL,
          CHECK(target_json != ''),
          FOREIGN KEY(ohlcv_series_id) REFERENCES ohlcv_series_id_lookup(ohlcv_series_id)
        );

        CREATE TABLE IF NOT EXISTS ticker_aliases (
          ticker_alias_id INTEGER PRIMARY KEY,
          ohlcv_series_id INTEGER NOT NULL,
          ticker TEXT NOT NULL,
          as_of_date TEXT NOT NULL DEFAULT '',
          active INTEGER,
          source TEXT NOT NULL,
          raw_json TEXT NOT NULL DEFAULT '{}',
          first_seen_utc TEXT NOT NULL,
          last_seen_utc TEXT NOT NULL,
          FOREIGN KEY(ohlcv_series_id) REFERENCES ohlcv_series_id_lookup(ohlcv_series_id),
          UNIQUE(ohlcv_series_id, ticker, as_of_date, source)
        );

        CREATE TABLE IF NOT EXISTS evidence_facts (
          evidence_fact_id INTEGER PRIMARY KEY,
          ohlcv_series_id INTEGER,
          kind TEXT NOT NULL,
          fact_key_json TEXT NOT NULL,
          payload_json TEXT NOT NULL,
          source TEXT NOT NULL,
          fact_hash TEXT NOT NULL,
          inserted_at_utc TEXT NOT NULL,
          FOREIGN KEY(ohlcv_series_id) REFERENCES ohlcv_series_id_lookup(ohlcv_series_id),
          UNIQUE(fact_hash)
        );

        CREATE TABLE IF NOT EXISTS evidence_ledger_facts (
          evidence_ledger_hash TEXT NOT NULL,
          fact_hash TEXT NOT NULL,
          ohlcv_series_id INTEGER NOT NULL,
          inserted_at_utc TEXT NOT NULL,
          PRIMARY KEY(evidence_ledger_hash, fact_hash, ohlcv_series_id),
          FOREIGN KEY(fact_hash) REFERENCES evidence_facts(fact_hash),
          FOREIGN KEY(ohlcv_series_id) REFERENCES ohlcv_series_id_lookup(ohlcv_series_id),
          CHECK(evidence_ledger_hash != ''),
          CHECK(fact_hash != '')
        ) WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS backfill_plans (
          plan_id INTEGER PRIMARY KEY,
          request_hash TEXT NOT NULL,
          evidence_ledger_hash TEXT NOT NULL,
          ohlcv_series_id INTEGER NOT NULL,
          status TEXT NOT NULL,
          planner_version TEXT NOT NULL,
          created_at_utc TEXT NOT NULL,
          plan_json TEXT NOT NULL,
          plan_hash TEXT NOT NULL,
          inserted_at_utc TEXT NOT NULL,
          FOREIGN KEY(ohlcv_series_id) REFERENCES ohlcv_series_id_lookup(ohlcv_series_id),
          UNIQUE(request_hash, evidence_ledger_hash)
        );

        CREATE TABLE IF NOT EXISTS ohlcv_bar_scopes (
          ohlcv_bar_scope_id INTEGER PRIMARY KEY,
          ohlcv_series_id INTEGER NOT NULL,
          calendar_id TEXT NOT NULL DEFAULT 'US_EQUITY',
          timespan TEXT NOT NULL CHECK(timespan IN ('day', 'hour', 'minute')),
          multiplier INTEGER NOT NULL CHECK(multiplier > 0),
          adjusted_flag INTEGER NOT NULL CHECK(adjusted_flag IN (0, 1)),
          source_id INTEGER NOT NULL,
          FOREIGN KEY(ohlcv_series_id) REFERENCES ohlcv_series_id_lookup(ohlcv_series_id),
          FOREIGN KEY(source_id) REFERENCES data_sources(source_id),
          UNIQUE(ohlcv_series_id, calendar_id, timespan, multiplier, adjusted_flag, source_id),
          CHECK(calendar_id != '')
        );

        CREATE TABLE IF NOT EXISTS ohlcv_bar_lineage (
          ohlcv_bar_lineage_id INTEGER PRIMARY KEY,
          ohlcv_bar_scope_id INTEGER NOT NULL,
          request_hash TEXT NOT NULL,
          evidence_ledger_hash TEXT NOT NULL,
          segment_index INTEGER NOT NULL DEFAULT -1,
          plan_id INTEGER,
          execution_receipt_id INTEGER,
          from_utc_start_ts INTEGER NOT NULL,
          to_utc_start_ts INTEGER NOT NULL,
          first_downloaded_at_utc TEXT NOT NULL,
          last_downloaded_at_utc TEXT NOT NULL,
          bar_count INTEGER NOT NULL CHECK(bar_count >= 0),
          quality_exception_count INTEGER NOT NULL DEFAULT 0 CHECK(quality_exception_count >= 0),
          FOREIGN KEY(ohlcv_bar_scope_id) REFERENCES ohlcv_bar_scopes(ohlcv_bar_scope_id),
          FOREIGN KEY(plan_id) REFERENCES backfill_plans(plan_id),
          FOREIGN KEY(execution_receipt_id) REFERENCES execution_receipts(execution_receipt_id),
          UNIQUE(request_hash, evidence_ledger_hash, ohlcv_bar_scope_id, segment_index),
          CHECK(request_hash != ''),
          CHECK(evidence_ledger_hash != ''),
          CHECK(from_utc_start_ts <= to_utc_start_ts)
        );

        CREATE TABLE IF NOT EXISTS ohlcv_bars_day (
          ohlcv_bar_scope_id INTEGER NOT NULL,
          market_session_id INTEGER NOT NULL,
          session_start_time TEXT NOT NULL,
          utc_start_ts INTEGER NOT NULL,
          ohlcv_bar_lineage_id INTEGER NOT NULL,
          open REAL NOT NULL,
          high REAL NOT NULL,
          low REAL NOT NULL,
          close REAL NOT NULL,
          volume REAL NOT NULL,
          vwap REAL,
          transaction_count INTEGER,
          PRIMARY KEY(ohlcv_bar_scope_id, utc_start_ts),
          FOREIGN KEY(ohlcv_bar_scope_id) REFERENCES ohlcv_bar_scopes(ohlcv_bar_scope_id),
          FOREIGN KEY(market_session_id) REFERENCES market_sessions(market_session_id),
          FOREIGN KEY(ohlcv_bar_lineage_id) REFERENCES ohlcv_bar_lineage(ohlcv_bar_lineage_id),
          UNIQUE(ohlcv_bar_scope_id, market_session_id),
          CHECK(session_start_time != ''),
          CHECK(high >= low),
          CHECK(volume >= 0),
          CHECK(transaction_count IS NULL OR transaction_count >= 0)
        ) WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS ohlcv_bars_hour (
          ohlcv_bar_scope_id INTEGER NOT NULL,
          market_session_id INTEGER NOT NULL,
          session_start_time TEXT NOT NULL,
          utc_start_ts INTEGER NOT NULL,
          ohlcv_bar_lineage_id INTEGER NOT NULL,
          open REAL NOT NULL,
          high REAL NOT NULL,
          low REAL NOT NULL,
          close REAL NOT NULL,
          volume REAL NOT NULL,
          vwap REAL,
          transaction_count INTEGER,
          PRIMARY KEY(ohlcv_bar_scope_id, utc_start_ts),
          FOREIGN KEY(ohlcv_bar_scope_id) REFERENCES ohlcv_bar_scopes(ohlcv_bar_scope_id),
          FOREIGN KEY(market_session_id) REFERENCES market_sessions(market_session_id),
          FOREIGN KEY(ohlcv_bar_lineage_id) REFERENCES ohlcv_bar_lineage(ohlcv_bar_lineage_id),
          CHECK(session_start_time != ''),
          CHECK(high >= low),
          CHECK(volume >= 0),
          CHECK(transaction_count IS NULL OR transaction_count >= 0)
        ) WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS ohlcv_bars_minute (
          ohlcv_bar_scope_id INTEGER NOT NULL,
          market_session_id INTEGER NOT NULL,
          session_start_time TEXT NOT NULL,
          utc_start_ts INTEGER NOT NULL,
          ohlcv_bar_lineage_id INTEGER NOT NULL,
          open REAL NOT NULL,
          high REAL NOT NULL,
          low REAL NOT NULL,
          close REAL NOT NULL,
          volume REAL NOT NULL,
          vwap REAL,
          transaction_count INTEGER,
          PRIMARY KEY(ohlcv_bar_scope_id, utc_start_ts),
          FOREIGN KEY(ohlcv_bar_scope_id) REFERENCES ohlcv_bar_scopes(ohlcv_bar_scope_id),
          FOREIGN KEY(market_session_id) REFERENCES market_sessions(market_session_id),
          FOREIGN KEY(ohlcv_bar_lineage_id) REFERENCES ohlcv_bar_lineage(ohlcv_bar_lineage_id),
          CHECK(session_start_time != ''),
          CHECK(high >= low),
          CHECK(volume >= 0),
          CHECK(transaction_count IS NULL OR transaction_count >= 0)
        ) WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS ohlcv_day_bar_quality_events (
          ohlcv_bar_scope_id INTEGER NOT NULL,
          utc_start_ts INTEGER NOT NULL,
          quality_event_seq INTEGER NOT NULL DEFAULT 1,
          bar_quality_status TEXT NOT NULL,
          repair_rule TEXT NOT NULL DEFAULT '',
          repair_evidence_json TEXT NOT NULL DEFAULT '{}',
          created_at_utc TEXT NOT NULL,
          PRIMARY KEY(ohlcv_bar_scope_id, utc_start_ts, quality_event_seq),
          FOREIGN KEY(ohlcv_bar_scope_id, utc_start_ts)
            REFERENCES ohlcv_bars_day(ohlcv_bar_scope_id, utc_start_ts)
            ON DELETE CASCADE,
          CHECK(bar_quality_status != '')
        ) WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS ohlcv_hour_bar_quality_events (
          ohlcv_bar_scope_id INTEGER NOT NULL,
          utc_start_ts INTEGER NOT NULL,
          quality_event_seq INTEGER NOT NULL DEFAULT 1,
          bar_quality_status TEXT NOT NULL,
          repair_rule TEXT NOT NULL DEFAULT '',
          repair_evidence_json TEXT NOT NULL DEFAULT '{}',
          created_at_utc TEXT NOT NULL,
          PRIMARY KEY(ohlcv_bar_scope_id, utc_start_ts, quality_event_seq),
          FOREIGN KEY(ohlcv_bar_scope_id, utc_start_ts)
            REFERENCES ohlcv_bars_hour(ohlcv_bar_scope_id, utc_start_ts)
            ON DELETE CASCADE,
          CHECK(bar_quality_status != '')
        ) WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS ohlcv_minute_bar_quality_events (
          ohlcv_bar_scope_id INTEGER NOT NULL,
          utc_start_ts INTEGER NOT NULL,
          quality_event_seq INTEGER NOT NULL DEFAULT 1,
          bar_quality_status TEXT NOT NULL,
          repair_rule TEXT NOT NULL DEFAULT '',
          repair_evidence_json TEXT NOT NULL DEFAULT '{}',
          created_at_utc TEXT NOT NULL,
          PRIMARY KEY(ohlcv_bar_scope_id, utc_start_ts, quality_event_seq),
          FOREIGN KEY(ohlcv_bar_scope_id, utc_start_ts)
            REFERENCES ohlcv_bars_minute(ohlcv_bar_scope_id, utc_start_ts)
            ON DELETE CASCADE,
          CHECK(bar_quality_status != '')
        ) WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS ohlcv_bar_raw_payloads (
          ohlcv_bar_scope_id INTEGER NOT NULL,
          utc_start_ts INTEGER NOT NULL,
          ohlcv_bar_lineage_id INTEGER NOT NULL,
          raw_bar_json TEXT NOT NULL,
          raw_bar_hash TEXT NOT NULL,
          captured_at_utc TEXT NOT NULL,
          PRIMARY KEY(ohlcv_bar_scope_id, utc_start_ts, ohlcv_bar_lineage_id),
          FOREIGN KEY(ohlcv_bar_lineage_id) REFERENCES ohlcv_bar_lineage(ohlcv_bar_lineage_id),
          CHECK(raw_bar_json != ''),
          CHECK(raw_bar_hash != '')
        ) WITHOUT ROWID;

        CREATE TABLE IF NOT EXISTS execution_receipts (
          execution_receipt_id INTEGER PRIMARY KEY,
          request_hash TEXT NOT NULL,
          evidence_ledger_hash TEXT NOT NULL,
          ohlcv_series_id INTEGER NOT NULL,
          status TEXT NOT NULL,
          approved_by TEXT NOT NULL DEFAULT '',
          started_at_utc TEXT NOT NULL,
          finished_at_utc TEXT NOT NULL,
          planned_segment_count INTEGER NOT NULL,
          fetched_bar_count INTEGER NOT NULL,
          inserted_bar_count INTEGER NOT NULL,
          request_log_json TEXT NOT NULL,
          receipt_json TEXT NOT NULL,
          receipt_hash TEXT NOT NULL,
          FOREIGN KEY(ohlcv_series_id) REFERENCES ohlcv_series_id_lookup(ohlcv_series_id)
        );

        CREATE TABLE IF NOT EXISTS execution_approvals (
          execution_approval_id INTEGER PRIMARY KEY,
          request_hash TEXT NOT NULL,
          evidence_ledger_hash TEXT NOT NULL,
          plan_hash TEXT NOT NULL,
          ohlcv_series_id INTEGER NOT NULL,
          plan_status TEXT NOT NULL,
          approved_by TEXT NOT NULL,
          allow_caution_flag INTEGER NOT NULL,
          reason TEXT NOT NULL DEFAULT '',
          approved_at_utc TEXT NOT NULL,
          approval_json TEXT NOT NULL,
          approval_hash TEXT NOT NULL,
          inserted_at_utc TEXT NOT NULL,
          FOREIGN KEY(ohlcv_series_id) REFERENCES ohlcv_series_id_lookup(ohlcv_series_id),
          UNIQUE(approval_hash)
        );

        CREATE TABLE IF NOT EXISTS reference_universe_snapshots (
          reference_snapshot_id INTEGER PRIMARY KEY,
          provider TEXT NOT NULL,
          snapshot_as_of_date TEXT NOT NULL,
          ticker TEXT NOT NULL,
          ohlcv_series_id INTEGER NOT NULL,
          active_flag INTEGER NOT NULL DEFAULT -1,
          company_name TEXT NOT NULL DEFAULT '',
          cik TEXT NOT NULL DEFAULT '',
          composite_figi TEXT NOT NULL DEFAULT '',
          share_class_figi TEXT NOT NULL DEFAULT '',
          security_type TEXT NOT NULL DEFAULT '',
          primary_exchange TEXT NOT NULL DEFAULT '',
          market TEXT NOT NULL DEFAULT '',
          locale TEXT NOT NULL DEFAULT '',
          identity_status TEXT NOT NULL DEFAULT '',
          provisional_key TEXT NOT NULL DEFAULT '',
          raw_json TEXT NOT NULL,
          source_request_json TEXT NOT NULL DEFAULT '{}',
          first_seen_utc TEXT NOT NULL,
          last_seen_utc TEXT NOT NULL,
          CHECK(provider != ''),
          CHECK(ticker != ''),
          CHECK(active_flag IN (-1, 0, 1)),
          CHECK(raw_json != ''),
          FOREIGN KEY(ohlcv_series_id) REFERENCES ohlcv_series_id_lookup(ohlcv_series_id),
          UNIQUE(provider, snapshot_as_of_date, ohlcv_series_id)
        );

        CREATE TABLE IF NOT EXISTS reference_universe_updates (
          reference_update_id INTEGER PRIMARY KEY,
          provider TEXT NOT NULL,
          snapshot_as_of_date TEXT NOT NULL,
          market TEXT NOT NULL DEFAULT '',
          exchange TEXT NOT NULL DEFAULT '',
          active_mode TEXT NOT NULL DEFAULT '',
          limit_value INTEGER NOT NULL,
          max_pages INTEGER NOT NULL,
          complete_flag INTEGER NOT NULL,
          fetched_count INTEGER NOT NULL,
          page_count INTEGER NOT NULL,
          pending_requests_json TEXT NOT NULL DEFAULT '[]',
          request_json TEXT NOT NULL,
          request_log_json TEXT NOT NULL DEFAULT '[]',
          update_hash TEXT NOT NULL,
          committed_at_utc TEXT NOT NULL,
          CHECK(provider != ''),
          CHECK(snapshot_as_of_date != ''),
          CHECK(complete_flag IN (0, 1)),
          CHECK(request_json != ''),
          UNIQUE(update_hash)
        );

        CREATE INDEX IF NOT EXISTS idx_ohlcv_bar_scopes_series_grain
          ON ohlcv_bar_scopes(ohlcv_series_id, calendar_id, timespan, multiplier, adjusted_flag, source_id);
        CREATE INDEX IF NOT EXISTS idx_ohlcv_bar_lineage_scope_range
          ON ohlcv_bar_lineage(ohlcv_bar_scope_id, from_utc_start_ts, to_utc_start_ts);
        CREATE INDEX IF NOT EXISTS idx_ohlcv_bar_lineage_request_hash
          ON ohlcv_bar_lineage(request_hash);
        CREATE INDEX IF NOT EXISTS idx_ohlcv_bar_lineage_evidence_hash
          ON ohlcv_bar_lineage(evidence_ledger_hash);
        CREATE INDEX IF NOT EXISTS idx_market_sessions_calendar_date
          ON market_sessions(calendar_id, session_date);
        CREATE INDEX IF NOT EXISTS idx_ohlcv_bars_day_session
          ON ohlcv_bars_day(ohlcv_bar_scope_id, market_session_id);
        CREATE INDEX IF NOT EXISTS idx_ohlcv_bars_hour_session_time
          ON ohlcv_bars_hour(ohlcv_bar_scope_id, market_session_id, session_start_time);
        CREATE INDEX IF NOT EXISTS idx_ohlcv_bars_minute_session_time
          ON ohlcv_bars_minute(ohlcv_bar_scope_id, market_session_id, session_start_time);
        CREATE INDEX IF NOT EXISTS idx_ohlcv_day_bar_quality_events_status_rule
          ON ohlcv_day_bar_quality_events(bar_quality_status, repair_rule);
        CREATE INDEX IF NOT EXISTS idx_ohlcv_hour_bar_quality_events_status_rule
          ON ohlcv_hour_bar_quality_events(bar_quality_status, repair_rule);
        CREATE INDEX IF NOT EXISTS idx_ohlcv_minute_bar_quality_events_status_rule
          ON ohlcv_minute_bar_quality_events(bar_quality_status, repair_rule);
        CREATE INDEX IF NOT EXISTS idx_ohlcv_series_lookup_natural_key
          ON ohlcv_series_id_lookup(natural_key);
        CREATE INDEX IF NOT EXISTS idx_evidence_facts_series_kind
          ON evidence_facts(ohlcv_series_id, kind);
        CREATE INDEX IF NOT EXISTS idx_evidence_ledger_facts_series
          ON evidence_ledger_facts(ohlcv_series_id, evidence_ledger_hash);
        CREATE INDEX IF NOT EXISTS idx_execution_approvals_plan
          ON execution_approvals(request_hash, evidence_ledger_hash, plan_hash);
        CREATE INDEX IF NOT EXISTS idx_execution_receipts_series_started_receipt
          ON execution_receipts(ohlcv_series_id, started_at_utc DESC, execution_receipt_id DESC);
        CREATE INDEX IF NOT EXISTS idx_execution_receipts_started_at
          ON execution_receipts(started_at_utc);
        CREATE INDEX IF NOT EXISTS idx_execution_approvals_approved_at
          ON execution_approvals(approved_at_utc);
        CREATE INDEX IF NOT EXISTS idx_reference_universe_ticker
          ON reference_universe_snapshots(ticker, snapshot_as_of_date);
        CREATE INDEX IF NOT EXISTS idx_reference_universe_cik
          ON reference_universe_snapshots(cik);
        CREATE INDEX IF NOT EXISTS idx_reference_universe_figi
          ON reference_universe_snapshots(composite_figi, share_class_figi);
        CREATE INDEX IF NOT EXISTS idx_reference_universe_series
          ON reference_universe_snapshots(ohlcv_series_id);
        CREATE INDEX IF NOT EXISTS idx_reference_universe_updates_scope
          ON reference_universe_updates(snapshot_as_of_date, market, exchange, active_mode);
        """
    )
    _create_ohlcv_guards_and_views(conn)
    _seed_market_sessions(conn)
    conn.execute(
        "INSERT OR REPLACE INTO schema_metadata(key, value) VALUES (?, ?)",
        ("schema_version", SCHEMA_VERSION),
    )
    conn.commit()


def _create_ohlcv_guards_and_views(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TRIGGER IF NOT EXISTS trg_ohlcv_bars_day_scope_insert
        BEFORE INSERT ON ohlcv_bars_day
        FOR EACH ROW
        WHEN (SELECT timespan FROM ohlcv_bar_scopes WHERE ohlcv_bar_scope_id = NEW.ohlcv_bar_scope_id) <> 'day'
        BEGIN
          SELECT RAISE(ABORT, 'ohlcv_bars_day requires a day scope');
        END;

        CREATE TRIGGER IF NOT EXISTS trg_ohlcv_bars_hour_scope_insert
        BEFORE INSERT ON ohlcv_bars_hour
        FOR EACH ROW
        WHEN (SELECT timespan FROM ohlcv_bar_scopes WHERE ohlcv_bar_scope_id = NEW.ohlcv_bar_scope_id) <> 'hour'
        BEGIN
          SELECT RAISE(ABORT, 'ohlcv_bars_hour requires an hour scope');
        END;

        CREATE TRIGGER IF NOT EXISTS trg_ohlcv_bars_minute_scope_insert
        BEFORE INSERT ON ohlcv_bars_minute
        FOR EACH ROW
        WHEN (SELECT timespan FROM ohlcv_bar_scopes WHERE ohlcv_bar_scope_id = NEW.ohlcv_bar_scope_id) <> 'minute'
        BEGIN
          SELECT RAISE(ABORT, 'ohlcv_bars_minute requires a minute scope');
        END;

        CREATE TRIGGER IF NOT EXISTS trg_ohlcv_bars_day_scope_update
        BEFORE UPDATE OF ohlcv_bar_scope_id ON ohlcv_bars_day
        FOR EACH ROW
        WHEN (SELECT timespan FROM ohlcv_bar_scopes WHERE ohlcv_bar_scope_id = NEW.ohlcv_bar_scope_id) <> 'day'
        BEGIN
          SELECT RAISE(ABORT, 'ohlcv_bars_day requires a day scope');
        END;

        CREATE TRIGGER IF NOT EXISTS trg_ohlcv_bars_hour_scope_update
        BEFORE UPDATE OF ohlcv_bar_scope_id ON ohlcv_bars_hour
        FOR EACH ROW
        WHEN (SELECT timespan FROM ohlcv_bar_scopes WHERE ohlcv_bar_scope_id = NEW.ohlcv_bar_scope_id) <> 'hour'
        BEGIN
          SELECT RAISE(ABORT, 'ohlcv_bars_hour requires an hour scope');
        END;

        CREATE TRIGGER IF NOT EXISTS trg_ohlcv_bars_minute_scope_update
        BEFORE UPDATE OF ohlcv_bar_scope_id ON ohlcv_bars_minute
        FOR EACH ROW
        WHEN (SELECT timespan FROM ohlcv_bar_scopes WHERE ohlcv_bar_scope_id = NEW.ohlcv_bar_scope_id) <> 'minute'
        BEGIN
          SELECT RAISE(ABORT, 'ohlcv_bars_minute requires a minute scope');
        END;

        CREATE VIEW IF NOT EXISTS v_ohlcv_bars_hot_unified AS
        SELECT
          'day' AS bar_kind,
          ohlcv_bar_scope_id,
          market_session_id,
          session_start_time,
          utc_start_ts AS bar_start_ts,
          utc_start_ts,
          ohlcv_bar_lineage_id,
          open,
          high,
          low,
          close,
          volume,
          vwap,
          transaction_count
        FROM ohlcv_bars_day
        UNION ALL
        SELECT
          'hour' AS bar_kind,
          ohlcv_bar_scope_id,
          market_session_id,
          session_start_time,
          utc_start_ts AS bar_start_ts,
          utc_start_ts,
          ohlcv_bar_lineage_id,
          open,
          high,
          low,
          close,
          volume,
          vwap,
          transaction_count
        FROM ohlcv_bars_hour
        UNION ALL
        SELECT
          'minute' AS bar_kind,
          ohlcv_bar_scope_id,
          market_session_id,
          session_start_time,
          utc_start_ts AS bar_start_ts,
          utc_start_ts,
          ohlcv_bar_lineage_id,
          open,
          high,
          low,
          close,
          volume,
          vwap,
          transaction_count
        FROM ohlcv_bars_minute;

        CREATE VIEW IF NOT EXISTS v_ohlcv_bars_unified AS
        SELECT
          h.bar_kind,
          sc.ohlcv_series_id,
          COALESCE(
            (
              SELECT ta.ticker
              FROM ticker_aliases ta
              WHERE ta.ohlcv_series_id = sc.ohlcv_series_id
                AND ta.source = ('plan.segment:' || l.segment_index)
              ORDER BY ta.last_seen_utc DESC, ta.ticker_alias_id DESC
              LIMIT 1
            ),
            os.latest_ticker,
            ''
          ) AS ticker,
          ms.session_date AS bar_date,
          ms.session_date,
          ms.calendar_id,
          ms.timezone_name,
          h.session_start_time,
          h.bar_start_ts,
          h.utc_start_ts,
          sc.multiplier,
          sc.timespan,
          sc.adjusted_flag,
          h.open,
          h.high,
          h.low,
          h.close,
          h.volume,
          h.vwap,
          h.transaction_count,
          ds.source_name AS source,
          h.ohlcv_bar_scope_id,
          l.ohlcv_bar_lineage_id,
          h.market_session_id,
          l.request_hash,
          l.evidence_ledger_hash,
          l.segment_index,
          COALESCE(qd.bar_quality_status, qh.bar_quality_status, qm.bar_quality_status, 'VALIDATED') AS bar_quality_status,
          COALESCE(qd.repair_rule, qh.repair_rule, qm.repair_rule, '') AS repair_rule,
          COALESCE(qd.repair_evidence_json, qh.repair_evidence_json, qm.repair_evidence_json, '{}') AS repair_evidence_json,
          COALESCE(rp.raw_bar_json, '{}') AS raw_bar_json,
          l.first_downloaded_at_utc,
          l.last_downloaded_at_utc
        FROM v_ohlcv_bars_hot_unified h
        JOIN ohlcv_bar_scopes sc
          ON sc.ohlcv_bar_scope_id = h.ohlcv_bar_scope_id
        JOIN market_sessions ms
          ON ms.market_session_id = h.market_session_id
        JOIN data_sources ds
          ON ds.source_id = sc.source_id
        LEFT JOIN ohlcv_series os
          ON os.ohlcv_series_id = sc.ohlcv_series_id
        LEFT JOIN ohlcv_bar_lineage l
          ON l.ohlcv_bar_lineage_id = h.ohlcv_bar_lineage_id
        LEFT JOIN ohlcv_bar_raw_payloads rp
          ON rp.ohlcv_bar_scope_id = h.ohlcv_bar_scope_id
         AND rp.utc_start_ts = h.utc_start_ts
         AND rp.ohlcv_bar_lineage_id = h.ohlcv_bar_lineage_id
        LEFT JOIN ohlcv_day_bar_quality_events qd
          ON h.bar_kind = 'day'
         AND qd.ohlcv_bar_scope_id = h.ohlcv_bar_scope_id
         AND qd.utc_start_ts = h.utc_start_ts
         AND qd.quality_event_seq = (
           SELECT MAX(qd_latest.quality_event_seq)
           FROM ohlcv_day_bar_quality_events qd_latest
           WHERE qd_latest.ohlcv_bar_scope_id = h.ohlcv_bar_scope_id
             AND qd_latest.utc_start_ts = h.utc_start_ts
         )
        LEFT JOIN ohlcv_hour_bar_quality_events qh
          ON h.bar_kind = 'hour'
         AND qh.ohlcv_bar_scope_id = h.ohlcv_bar_scope_id
         AND qh.utc_start_ts = h.utc_start_ts
         AND qh.quality_event_seq = (
           SELECT MAX(qh_latest.quality_event_seq)
           FROM ohlcv_hour_bar_quality_events qh_latest
           WHERE qh_latest.ohlcv_bar_scope_id = h.ohlcv_bar_scope_id
             AND qh_latest.utc_start_ts = h.utc_start_ts
         )
        LEFT JOIN ohlcv_minute_bar_quality_events qm
          ON h.bar_kind = 'minute'
         AND qm.ohlcv_bar_scope_id = h.ohlcv_bar_scope_id
         AND qm.utc_start_ts = h.utc_start_ts
         AND qm.quality_event_seq = (
           SELECT MAX(qm_latest.quality_event_seq)
           FROM ohlcv_minute_bar_quality_events qm_latest
           WHERE qm_latest.ohlcv_bar_scope_id = h.ohlcv_bar_scope_id
             AND qm_latest.utc_start_ts = h.utc_start_ts
         );
        """
    )


class SQLiteStockUniverseRepository:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)

    def connect(self) -> sqlite3.Connection:
        conn = connect_sqlite(self.path)
        initialize_schema(conn)
        return conn

    def ensure_schema(self) -> None:
        with self.connect() as conn:
            initialize_schema(conn)

    def ensure_ohlcv_series_id(self, natural_key: str) -> int:
        now = utc_now()
        with self.connect() as conn:
            return _ensure_ohlcv_series_id(conn, natural_key, now)

    def ensure_ohlcv_series_ids(self, natural_keys: Iterable[str]) -> dict[str, int]:
        now = utc_now()
        unique_keys = tuple(
            dict.fromkeys(str(key).strip() for key in natural_keys if str(key).strip())
        )
        with self.connect() as conn:
            return {key: _ensure_ohlcv_series_id(conn, key, now) for key in unique_keys}

    def lookup_ohlcv_series_id(self, natural_key: str) -> int | None:
        key = str(natural_key or "").strip()
        if not key or not self.path.exists():
            return None
        with connect_readonly_sqlite(self.path) as conn:
            if not _sqlite_table_exists(conn, "ohlcv_series_id_lookup"):
                return None
            row = conn.execute(
                "SELECT ohlcv_series_id FROM ohlcv_series_id_lookup WHERE natural_key = ?",
                (key,),
            ).fetchone()
        return int(row["ohlcv_series_id"]) if row else None

    def natural_key_for_ohlcv_series_id(self, ohlcv_series_id: int) -> str | None:
        if not self.path.exists():
            return None
        with connect_readonly_sqlite(self.path) as conn:
            if not _sqlite_table_exists(conn, "ohlcv_series_id_lookup"):
                return None
            row = conn.execute(
                "SELECT natural_key FROM ohlcv_series_id_lookup WHERE ohlcv_series_id = ?",
                (int(ohlcv_series_id),),
            ).fetchone()
        return str(row["natural_key"]) if row else None

    def persist_plan_context(
        self,
        plan: BackfillPlan,
        *,
        evidence_facts: Iterable[EvidenceFact] = (),
    ) -> None:
        now = utc_now()
        with self.connect() as conn:
            _upsert_series(conn, plan, now)
            _upsert_aliases(conn, plan, now)
            _insert_evidence_facts(conn, plan, tuple(evidence_facts), now)
            _insert_plan(conn, plan, now)

    def insert_execution_approval(
        self,
        plan: BackfillPlan,
        approval: Any,
        *,
        reason: str = "",
    ) -> dict[str, Any]:
        now = utc_now()
        payload = _approval_payload(plan, approval, approved_at_utc=now, reason=reason)
        approval_hash = stable_json_hash(payload)
        with self.connect() as conn:
            _upsert_series(conn, plan, now)
            conn.execute(
                """
                INSERT OR IGNORE INTO execution_approvals(
                  request_hash, evidence_ledger_hash, plan_hash, ohlcv_series_id, plan_status,
                  approved_by, allow_caution_flag, reason, approved_at_utc, approval_json,
                  approval_hash, inserted_at_utc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    payload["request_hash"],
                    payload["evidence_ledger_hash"],
                    payload["plan_hash"],
                    payload["ohlcv_series_id"],
                    payload["plan_status"],
                    payload["approved_by"],
                    int(payload["allow_caution"]),
                    payload["reason"],
                    payload["approved_at_utc"],
                    json_dumps(payload),
                    approval_hash,
                    now,
                ),
            )
        return payload | {"approval_hash": approval_hash}

    def execution_approval_for(
        self, plan: BackfillPlan, approval: Any
    ) -> dict[str, Any] | None:
        plan_hash = _plan_hash(plan)
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT approval_json, approval_hash
                FROM execution_approvals
                WHERE request_hash = ?
                  AND evidence_ledger_hash = ?
                  AND plan_hash = ?
                  AND approved_by = ?
                  AND allow_caution_flag = ?
                ORDER BY execution_approval_id DESC
                LIMIT 1
                """,
                (
                    plan.request.request_hash,
                    plan.evidence_ledger_hash,
                    plan_hash,
                    approval.approved_by or "",
                    int(bool(approval.allow_caution)),
                ),
            ).fetchone()
        if row is None:
            return None
        return json.loads(row["approval_json"]) | {
            "approval_hash": row["approval_hash"]
        }

    def has_execution_approval(self, plan: BackfillPlan, approval: Any) -> bool:
        return self.execution_approval_for(plan, approval) is not None

    def insert_bars(self, bars: Iterable[StoredOhlcvBar]) -> int:
        now = utc_now()
        rows = list(bars)
        if not rows:
            return 0
        with self.connect() as conn:
            prepared_rows: list[_PreparedOhlcvBar] = []
            for bar in rows:
                source_id = _ensure_data_source_id(conn, bar.source)
                scope_id = _ensure_ohlcv_bar_scope_id(conn, bar, source_id)
                session = _session_for_bar(bar)
                market_session_id = _ensure_market_session_id(conn, session)
                session_start_time = _session_start_time_for_bar(bar, session)
                utc_start_ts = _canonical_utc_start_ts_for_bar(bar, session)
                prepared_rows.append(
                    _PreparedOhlcvBar(
                        bar=bar,
                        scope_id=scope_id,
                        market_session_id=market_session_id,
                        session_start_time=session_start_time,
                        utc_start_ts=utc_start_ts,
                    )
                )
            grouped_lineage_rows = _group_lineage_rows(prepared_rows)
            lineage_ids: dict[tuple[int, str, str, int], int] = {}
            for key, lineage_rows in grouped_lineage_rows.items():
                scope_id, request_hash, ledger_hash, segment_index = key
                lineage_ids[key] = _upsert_ohlcv_bar_lineage(
                    conn,
                    scope_id=scope_id,
                    request_hash=request_hash,
                    ledger_hash=ledger_hash,
                    segment_index=segment_index,
                    rows=lineage_rows,
                    downloaded_at_utc=now,
                )
            for prepared in prepared_rows:
                lineage_id = lineage_ids[
                    (
                        prepared.scope_id,
                        *_lineage_hashes(prepared.bar),
                        _segment_index_value(prepared.bar.segment_index),
                    )
                ]
                _insert_hot_bar(
                    conn,
                    prepared.bar,
                    prepared.scope_id,
                    market_session_id=prepared.market_session_id,
                    session_start_time=prepared.session_start_time,
                    utc_start_ts=prepared.utc_start_ts,
                    lineage_id=lineage_id,
                )
                _upsert_raw_bar_payload(
                    conn,
                    prepared.bar,
                    prepared.scope_id,
                    utc_start_ts=prepared.utc_start_ts,
                    lineage_id=lineage_id,
                    captured_at_utc=now,
                )
                if _bar_has_quality_exception(prepared.bar):
                    _upsert_quality_exception(
                        conn,
                        prepared.bar,
                        prepared.scope_id,
                        utc_start_ts=prepared.utc_start_ts,
                        created_at_utc=now,
                    )
                else:
                    _delete_quality_exception_events(
                        conn,
                        prepared.bar,
                        prepared.scope_id,
                        utc_start_ts=prepared.utc_start_ts,
                    )
            for (
                scope_id,
                request_hash,
                ledger_hash,
                segment_index,
            ) in grouped_lineage_rows:
                _refresh_ohlcv_bar_lineage_counts(
                    conn,
                    scope_id=scope_id,
                    request_hash=request_hash,
                    ledger_hash=ledger_hash,
                    segment_index=segment_index,
                )
        return len(rows)

    def insert_execution_receipt(self, receipt: dict[str, Any]) -> int:
        now = utc_now()
        persisted_receipt = _without_natural_key(receipt)
        payload = json_dumps(persisted_receipt)
        receipt_hash = stable_json_hash(persisted_receipt)
        with self.connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO execution_receipts(
                  request_hash, evidence_ledger_hash, ohlcv_series_id, status, approved_by,
                  started_at_utc, finished_at_utc, planned_segment_count, fetched_bar_count,
                  inserted_bar_count, request_log_json, receipt_json, receipt_hash
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    receipt["request_hash"],
                    receipt["evidence_ledger_hash"],
                    receipt["ohlcv_series_id"],
                    receipt["status"],
                    receipt.get("approved_by") or "",
                    receipt.get("started_at_utc") or now,
                    receipt.get("finished_at_utc") or now,
                    receipt["planned_segment_count"],
                    receipt["fetched_bar_count"],
                    receipt["inserted_bar_count"],
                    json_dumps(_without_natural_key(receipt.get("request_log") or [])),
                    payload,
                    receipt_hash,
                ),
            )
            receipt_id = int(cursor.lastrowid)
            _attach_receipt_to_lineage(conn, receipt, receipt_id)
            return receipt_id

    def upsert_reference_snapshots(
        self, snapshots: Iterable[StoredReferenceSnapshot]
    ) -> int:
        now = utc_now()
        rows = list(snapshots)
        with self.connect() as conn:
            for snapshot in rows:
                _ensure_data_source_id(conn, snapshot.provider)
                series_id = _ensure_ohlcv_series_id(conn, snapshot.natural_key, now)
                _upsert_reference_series_metadata(conn, snapshot, series_id, now)
                _upsert_reference_current_alias(conn, snapshot, series_id, now)
                conn.execute(
                    """
                    INSERT INTO reference_universe_snapshots(
                      provider, snapshot_as_of_date, ticker, ohlcv_series_id, active_flag,
                      company_name, cik, composite_figi, share_class_figi, security_type, primary_exchange,
                      market, locale, identity_status, provisional_key,
                      raw_json, source_request_json, first_seen_utc, last_seen_utc
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(provider, snapshot_as_of_date, ohlcv_series_id)
                    DO UPDATE SET
                      active_flag=excluded.active_flag,
                      ticker=excluded.ticker,
                      company_name=excluded.company_name,
                      cik=excluded.cik,
                      composite_figi=excluded.composite_figi,
                      share_class_figi=excluded.share_class_figi,
                      security_type=excluded.security_type,
                      primary_exchange=excluded.primary_exchange,
                      market=excluded.market,
                      locale=excluded.locale,
                      identity_status=excluded.identity_status,
                      provisional_key=excluded.provisional_key,
                      raw_json=excluded.raw_json,
                      source_request_json=excluded.source_request_json,
                      last_seen_utc=excluded.last_seen_utc
                    """,
                    (
                        snapshot.provider,
                        snapshot.snapshot_as_of_date,
                        snapshot.ticker,
                        series_id,
                        _active_flag(snapshot.active),
                        snapshot.company_name,
                        snapshot.cik,
                        snapshot.composite_figi,
                        snapshot.share_class_figi,
                        snapshot.security_type,
                        snapshot.primary_exchange,
                        snapshot.market,
                        snapshot.locale,
                        snapshot.identity_status,
                        snapshot.provisional_key,
                        json_dumps(_without_natural_key(snapshot.raw or {})),
                        json_dumps(_without_natural_key(snapshot.source_request or {})),
                        now,
                        now,
                    ),
                )
        return len(rows)

    def insert_reference_universe_update(
        self,
        update: Any,
        *,
        request_log: Any = (),
    ) -> dict[str, Any]:
        now = utc_now()
        request = update.request
        request_payload = request.to_dict()
        pending_requests = list(update.pending_requests)
        payload = {
            "provider": request_payload["provider"],
            "snapshot_as_of_date": request.snapshot_as_of_date,
            "market": request.market,
            "exchange": request.exchange,
            "active_mode": _active_mode(request.active),
            "limit": request.limit,
            "max_pages": request.max_pages,
            "complete": update.complete,
            "fetched_count": len(update.snapshots),
            "page_count": update.page_count,
            "pending_requests": pending_requests,
            "request": request_payload,
            "request_log": request_log,
            "committed_at_utc": now,
        }
        persisted_payload = _without_natural_key(payload)
        update_hash = stable_json_hash(persisted_payload)
        with self.connect() as conn:
            _ensure_data_source_id(conn, payload["provider"])
            conn.execute(
                """
                INSERT OR IGNORE INTO reference_universe_updates(
                  provider, snapshot_as_of_date, market, exchange, active_mode,
                  limit_value, max_pages, complete_flag, fetched_count, page_count,
                  pending_requests_json, request_json, request_log_json, update_hash,
                  committed_at_utc
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    payload["provider"],
                    payload["snapshot_as_of_date"],
                    payload["market"],
                    payload["exchange"],
                    payload["active_mode"],
                    payload["limit"],
                    payload["max_pages"],
                    int(bool(payload["complete"])),
                    payload["fetched_count"],
                    payload["page_count"],
                    json_dumps(persisted_payload["pending_requests"]),
                    json_dumps(persisted_payload["request"]),
                    json_dumps(persisted_payload["request_log"]),
                    update_hash,
                    payload["committed_at_utc"],
                ),
            )
        return payload | {"update_hash": update_hash}

    def reference_snapshot_for_series_id(
        self,
        ohlcv_series_id: int,
        *,
        as_of_date: str | None = None,
    ) -> StoredReferenceSnapshot | None:
        if not self.path.exists():
            return None
        clauses = ["r.ohlcv_series_id = ?"]
        params: list[Any] = [ohlcv_series_id]
        if as_of_date:
            clauses.append("r.snapshot_as_of_date <= ?")
            params.append(as_of_date)
        where = " AND ".join(clauses)
        with connect_readonly_sqlite(self.path) as conn:
            if not _sqlite_table_exists(
                conn, "reference_universe_snapshots"
            ) or not _sqlite_table_exists(
                conn,
                "ohlcv_series_id_lookup",
            ):
                return None
            row = conn.execute(
                f"""
                SELECT
                  r.provider,
                  r.snapshot_as_of_date,
                  r.ticker,
                  r.ohlcv_series_id,
                  r.active_flag,
                  r.company_name,
                  r.cik,
                  r.composite_figi,
                  r.share_class_figi,
                  r.security_type,
                  r.primary_exchange,
                  r.market,
                  r.locale,
                  r.identity_status,
                  l.natural_key,
                  r.provisional_key,
                  r.raw_json,
                  r.source_request_json
                FROM reference_universe_snapshots r
                JOIN ohlcv_series_id_lookup l ON l.ohlcv_series_id = r.ohlcv_series_id
                WHERE {where}
                ORDER BY r.snapshot_as_of_date DESC, r.active_flag DESC, r.reference_snapshot_id DESC
                LIMIT 1
                """,
                params,
            ).fetchone()
        if row is None:
            return None
        return _stored_reference_snapshot_from_row(row)

    def reference_snapshots_for_batch(
        self,
        *,
        exchange: str = "",
        market: str = "",
        security_types: Iterable[str] = (),
        active: bool | None = True,
        as_of_date: str | None = None,
        series_ids: Iterable[int] = (),
        limit: int = 25,
        offset: int = 0,
    ) -> tuple[tuple[StoredReferenceSnapshot, ...], int]:
        if limit < 1:
            raise ValueError("limit must be positive")
        if offset < 0:
            raise ValueError("offset must be non-negative")
        if not self.path.exists():
            return (), 0
        clauses: list[str] = []
        params: list[Any] = []
        selected_series_ids = tuple(int(series_id) for series_id in series_ids)
        if selected_series_ids:
            placeholders = ", ".join("?" for _ in selected_series_ids)
            clauses.append(f"r.ohlcv_series_id IN ({placeholders})")
            params.extend(selected_series_ids)
        if exchange:
            clauses.append("r.primary_exchange = ?")
            params.append(exchange)
        if market:
            clauses.append("r.market = ?")
            params.append(market)
        selected_security_types = tuple(
            dict.fromkeys(
                security_type.strip().upper()
                for security_type in security_types
                if security_type.strip()
            )
        )
        if selected_security_types:
            placeholders = ", ".join("?" for _ in selected_security_types)
            clauses.append(f"UPPER(r.security_type) IN ({placeholders})")
            params.extend(selected_security_types)
        if active is not None:
            clauses.append("r.active_flag = ?")
            params.append(1 if active else 0)
        if as_of_date:
            clauses.append("r.snapshot_as_of_date <= ?")
            params.append(as_of_date)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        ranked_query = f"""
            WITH ranked AS (
              SELECT
                r.provider,
                r.snapshot_as_of_date,
                r.ticker,
                r.ohlcv_series_id,
                r.active_flag,
                r.company_name,
                r.cik,
                r.composite_figi,
                r.share_class_figi,
                r.security_type,
                r.primary_exchange,
                r.market,
                r.locale,
                r.identity_status,
                l.natural_key,
                r.provisional_key,
                r.raw_json,
                r.source_request_json,
                ROW_NUMBER() OVER (
                  PARTITION BY r.ohlcv_series_id
                  ORDER BY r.snapshot_as_of_date DESC, r.active_flag DESC, r.reference_snapshot_id DESC
                ) AS rn
              FROM reference_universe_snapshots r
              JOIN ohlcv_series_id_lookup l ON l.ohlcv_series_id = r.ohlcv_series_id
              {where}
            )
        """
        with connect_readonly_sqlite(self.path) as conn:
            if not _sqlite_table_exists(
                conn, "reference_universe_snapshots"
            ) or not _sqlite_table_exists(
                conn,
                "ohlcv_series_id_lookup",
            ):
                return (), 0
            total = int(
                conn.execute(
                    f"{ranked_query} SELECT COUNT(*) FROM ranked WHERE rn = 1",
                    params,
                ).fetchone()[0]
            )
            rows = conn.execute(
                f"""
                {ranked_query}
                SELECT
                  provider,
                  snapshot_as_of_date,
                  ticker,
                  ohlcv_series_id,
                  active_flag,
                  company_name,
                  cik,
                  composite_figi,
                  share_class_figi,
                  security_type,
                  primary_exchange,
                  market,
                  locale,
                  identity_status,
                  natural_key,
                  provisional_key,
                  raw_json,
                  source_request_json
                FROM ranked
                WHERE rn = 1
                ORDER BY primary_exchange, ticker, ohlcv_series_id
                LIMIT ? OFFSET ?
                """,
                params + [limit, offset],
            ).fetchall()
        return tuple(_stored_reference_snapshot_from_row(row) for row in rows), total

    def counts(self) -> dict[str, int]:
        tables = (
            "data_sources",
            "ohlcv_series_id_lookup",
            "ohlcv_series",
            "ticker_aliases",
            "market_sessions",
            "reference_universe_snapshots",
            "reference_universe_updates",
            "evidence_facts",
            "evidence_ledger_facts",
            "backfill_plans",
            "ohlcv_bar_scopes",
            "ohlcv_bar_lineage",
            "ohlcv_bar_raw_payloads",
            "ohlcv_bars_day",
            "ohlcv_bars_hour",
            "ohlcv_bars_minute",
            "ohlcv_day_bar_quality_events",
            "ohlcv_hour_bar_quality_events",
            "ohlcv_minute_bar_quality_events",
            "execution_approvals",
            "execution_receipts",
        )
        with self.connect() as conn:
            counts = {
                table: int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
                for table in tables
            }
        counts["ohlcv_bars"] = (
            counts["ohlcv_bars_day"]
            + counts["ohlcv_bars_hour"]
            + counts["ohlcv_bars_minute"]
        )
        return counts

    def execution_audit(
        self,
        *,
        request_hash: str | None = None,
        series_id: int | None = None,
        limit: int = 20,
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
        with self.connect() as conn:
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

    def validate(self) -> ValidationReport:
        checks: list[str] = []
        failures: list[str] = []
        with self.connect() as conn:
            fk_failures = conn.execute("PRAGMA foreign_key_check").fetchall()
            if fk_failures:
                failures.append(f"foreign key failures: {len(fk_failures)}")
            else:
                checks.append("foreign keys valid")

            duplicate_bars = _duplicate_hot_bar_keys(conn)
            if duplicate_bars:
                failures.append(f"duplicate bar keys: {duplicate_bars}")
            else:
                checks.append("bar keys unique")

            orphan_bars = conn.execute(
                """
                SELECT COUNT(*)
                FROM ohlcv_bar_scopes sc
                LEFT JOIN ohlcv_series_id_lookup l ON l.ohlcv_series_id = sc.ohlcv_series_id
                WHERE l.ohlcv_series_id IS NULL
                """
            ).fetchone()[0]
            if orphan_bars:
                failures.append(f"orphan bars: {orphan_bars}")
            else:
                checks.append("bars reference series lookup")

            old_bar_table_present = conn.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'ohlcv_bars'"
            ).fetchone()[0]
            if old_bar_table_present:
                failures.append("old fat ohlcv_bars table is present")
            else:
                checks.append("old fat ohlcv_bars table absent")

            market_session_seed_errors = _market_session_seed_error_count(conn)
            if market_session_seed_errors:
                failures.append(
                    f"seeded market session calendar errors: {market_session_seed_errors}"
                )
            else:
                checks.append("market session calendar seeded from us_market_hours.json")

            hot_table_shape_errors = _hot_table_shape_errors(conn)
            if hot_table_shape_errors:
                failures.append(
                    "hot table shape errors: " + "; ".join(hot_table_shape_errors)
                )
            else:
                checks.append(
                    "hot tables store compact session key, UTC key, lineage key, and OHLCV facts"
                )

            hot_without_rowid_errors = _hot_without_rowid_errors(conn)
            if hot_without_rowid_errors:
                failures.append(
                    "hot tables missing WITHOUT ROWID: "
                    + ", ".join(hot_without_rowid_errors)
                )
            else:
                checks.append("hot tables are WITHOUT ROWID")

            lineage_coverage_errors = _lineage_coverage_error_count(conn)
            if lineage_coverage_errors:
                failures.append(f"lineage coverage errors: {lineage_coverage_errors}")
            else:
                checks.append("each hot row has direct lineage")

            session_coverage_errors = _session_coverage_error_count(conn)
            if session_coverage_errors:
                failures.append(
                    f"session coverage errors: {session_coverage_errors}"
                )
            else:
                checks.append("each hot row has direct market session")

            session_time_errors = _session_time_error_count(conn)
            if session_time_errors:
                failures.append(f"session time errors: {session_time_errors}")
            else:
                checks.append("bar session times match UTC keys")

            lineage_bar_count_errors = _lineage_bar_count_error_count(conn)
            if lineage_bar_count_errors:
                failures.append(f"lineage bar_count errors: {lineage_bar_count_errors}")
            else:
                checks.append("lineage bar counts match covered hot rows")

            lineage_quality_count_errors = _lineage_quality_count_error_count(conn)
            if lineage_quality_count_errors:
                failures.append(
                    f"lineage quality_exception_count errors: {lineage_quality_count_errors}"
                )
            else:
                checks.append(
                    "lineage quality exception counts match sparse quality rows"
                )

            lineage_plan_errors = _lineage_plan_error_count(conn)
            if lineage_plan_errors:
                failures.append(
                    f"lineage plan linkage errors: {lineage_plan_errors}"
                )
            else:
                checks.append("lineage rows link to matching plans when plans exist")

            ledger_membership_errors = _evidence_ledger_membership_error_count(conn)
            if ledger_membership_errors:
                failures.append(
                    f"evidence ledger membership errors: {ledger_membership_errors}"
                )
            else:
                checks.append("evidence ledgers retain explicit fact memberships")

            raw_payload_errors = _raw_payload_error_count(conn)
            if raw_payload_errors:
                failures.append(f"raw payload integrity errors: {raw_payload_errors}")
            else:
                checks.append("raw provider payload side table is valid")

            view_count_errors = _ohlcv_view_count_errors(conn)
            if view_count_errors:
                failures.append(
                    "OHLCV view count errors: " + "; ".join(view_count_errors)
                )
            else:
                checks.append("OHLCV compatibility views preserve hot row counts")

            out_of_bounds = conn.execute(
                """
                SELECT COUNT(*)
                FROM v_ohlcv_bars_unified b
                JOIN backfill_plans p
                  ON p.request_hash = b.request_hash
                 AND p.evidence_ledger_hash = b.evidence_ledger_hash
                WHERE b.bar_date < json_extract(p.plan_json, '$.range.from_date')
                   OR b.bar_date > json_extract(p.plan_json, '$.range.to_date')
                """
            ).fetchone()[0]
            if out_of_bounds:
                failures.append(f"bars outside request bounds: {out_of_bounds}")
            else:
                checks.append("bars inside request bounds")

            receipts_without_approval = conn.execute(
                """
                SELECT COUNT(*)
                FROM execution_receipts r
                LEFT JOIN execution_approvals a
                  ON a.request_hash = r.request_hash
                 AND a.evidence_ledger_hash = r.evidence_ledger_hash
                 AND a.ohlcv_series_id = r.ohlcv_series_id
                WHERE a.execution_approval_id IS NULL
                """
            ).fetchone()[0]
            if receipts_without_approval:
                failures.append(
                    f"receipts without approval records: {receipts_without_approval}"
                )
            else:
                checks.append("receipts have approval records")

            duplicate_reference_snapshots = conn.execute(
                """
                SELECT COUNT(*) FROM (
                  SELECT provider, snapshot_as_of_date, ohlcv_series_id
                  FROM reference_universe_snapshots
                  GROUP BY provider, snapshot_as_of_date, ohlcv_series_id
                  HAVING COUNT(*) > 1
                )
                """
            ).fetchone()[0]
            if duplicate_reference_snapshots:
                failures.append(
                    f"duplicate reference snapshots: {duplicate_reference_snapshots}"
                )
            else:
                checks.append("reference snapshots unique")

            invalid_reference_snapshots = conn.execute(
                """
                SELECT COUNT(*)
                FROM reference_universe_snapshots
                WHERE provider = ''
                   OR ticker = ''
                   OR raw_json = ''
                   OR active_flag NOT IN (-1, 0, 1)
                """
            ).fetchone()[0]
            if invalid_reference_snapshots:
                failures.append(
                    f"invalid reference snapshots: {invalid_reference_snapshots}"
                )
            else:
                checks.append("reference snapshots valid")

            invalid_natural_key_columns = conn.execute(
                """
                SELECT COUNT(*)
                FROM sqlite_master m
                JOIN pragma_table_info(m.name) p
                WHERE m.type = 'table'
                  AND p.name = 'natural_key'
                  AND m.name != 'ohlcv_series_id_lookup'
                """
            ).fetchone()[0]
            if invalid_natural_key_columns:
                failures.append(
                    f"natural_key columns outside lookup: {invalid_natural_key_columns}"
                )
            else:
                checks.append("natural_key stored only in series lookup")

            natural_key_json_rows = _persisted_json_natural_key_rows(conn)
            if natural_key_json_rows:
                failures.append(
                    f"persisted JSON natural_key values: {natural_key_json_rows}"
                )
            else:
                checks.append("persisted JSON has no natural_key keys")

            orphan_child_rows = _orphan_lookup_child_rows(conn)
            if orphan_child_rows:
                failures.append(
                    f"child rows without lookup parent: {orphan_child_rows}"
                )
            else:
                checks.append("all child rows reference series lookup")

            duplicate_lookup_keys = conn.execute(
                """
                SELECT COUNT(*) FROM (
                  SELECT natural_key
                  FROM ohlcv_series_id_lookup
                  GROUP BY natural_key
                  HAVING COUNT(*) > 1
                )
                """
            ).fetchone()[0]
            if duplicate_lookup_keys:
                failures.append(
                    f"duplicate lookup natural keys: {duplicate_lookup_keys}"
                )
            else:
                checks.append("lookup natural keys unique")

            checks.append(
                "series lookup rows: "
                f"{conn.execute('SELECT COUNT(*) FROM ohlcv_series_id_lookup').fetchone()[0]}"
            )
            checks.append(
                f"series rows: {conn.execute('SELECT COUNT(*) FROM ohlcv_series').fetchone()[0]}"
            )
            checks.append(
                "reference snapshot rows: "
                f"{conn.execute('SELECT COUNT(*) FROM reference_universe_snapshots').fetchone()[0]}"
            )
            checks.append(f"bar rows: {_hot_bar_count(conn)}")
            checks.append(
                f"bar scope rows: {conn.execute('SELECT COUNT(*) FROM ohlcv_bar_scopes').fetchone()[0]}"
            )
            checks.append(
                f"bar lineage rows: {conn.execute('SELECT COUNT(*) FROM ohlcv_bar_lineage').fetchone()[0]}"
            )
            checks.append(
                f"approval rows: {conn.execute('SELECT COUNT(*) FROM execution_approvals').fetchone()[0]}"
            )
            checks.append(
                f"receipt rows: {conn.execute('SELECT COUNT(*) FROM execution_receipts').fetchone()[0]}"
            )
        return ValidationReport(
            ok=not failures, checks=tuple(checks), failures=tuple(failures)
        )


def _ensure_ohlcv_series_id(
    conn: sqlite3.Connection, natural_key: str, now: str
) -> int:
    key = str(natural_key or "").strip()
    if not key:
        raise ValueError("natural_key is required to allocate ohlcv_series_id")
    conn.execute(
        """
        INSERT INTO ohlcv_series_id_lookup(natural_key, first_seen_utc, last_seen_utc)
        VALUES (?, ?, ?)
        ON CONFLICT(natural_key) DO UPDATE SET last_seen_utc=excluded.last_seen_utc
        """,
        (key, now, now),
    )
    row = conn.execute(
        "SELECT ohlcv_series_id FROM ohlcv_series_id_lookup WHERE natural_key = ?",
        (key,),
    ).fetchone()
    if row is None:
        raise RuntimeError(f"failed to allocate ohlcv_series_id for natural_key={key}")
    return int(row["ohlcv_series_id"])


def _ensure_data_source_id(conn: sqlite3.Connection, source_name: str) -> int:
    name = str(source_name or "").strip() or "unknown"
    conn.execute(
        """
        INSERT OR IGNORE INTO data_sources(source_name)
        VALUES (?)
        """,
        (name,),
    )
    row = conn.execute(
        "SELECT source_id FROM data_sources WHERE source_name = ?",
        (name,),
    ).fetchone()
    if row is None:
        raise RuntimeError(f"failed to allocate source_id for source={name}")
    return int(row["source_id"])


def _seed_market_sessions(conn: sqlite3.Connection) -> None:
    sessions = _seeded_market_sessions()
    seed_hash = stable_json_hash(
        {
            "calendar_ids": SEEDED_US_EQUITY_CALENDAR_IDS,
            "sessions": [_market_session_payload(session) for session in sessions],
        }
    )
    row = conn.execute(
        "SELECT value FROM schema_metadata WHERE key = 'market_sessions_seed_hash'"
    ).fetchone()
    if row is not None and str(row["value"]) == seed_hash:
        return
    for session in sessions:
        _ensure_market_session_id(conn, session)
    conn.execute(
        "INSERT OR REPLACE INTO schema_metadata(key, value) VALUES (?, ?)",
        ("market_sessions_seed_hash", seed_hash),
    )


def _seeded_market_sessions() -> tuple[MarketSession, ...]:
    return tuple(
        session
        for calendar_id in SEEDED_US_EQUITY_CALENDAR_IDS
        for session in iter_us_equity_sessions(calendar_id=calendar_id)
    )


def _market_session_payload(session: MarketSession) -> dict[str, Any]:
    return {
        "calendar_id": session.calendar_id,
        "session_date": session.session_date,
        "timezone_name": session.timezone_name,
        "regular_open_time": session.regular_open_time,
        "regular_close_time": session.regular_close_time,
        "session_open_time": session.session_open_time,
        "session_close_time": session.session_close_time,
        "regular_open_utc_ts": session.regular_open_utc_ts,
        "regular_close_utc_ts": session.regular_close_utc_ts,
        "session_open_utc_ts": session.session_open_utc_ts,
        "session_close_utc_ts": session.session_close_utc_ts,
        "settlement_date": session.settlement_date,
    }


def _ensure_market_session_id(
    conn: sqlite3.Connection, session: MarketSession
) -> int:
    payload = _market_session_payload(session)
    source_hash = stable_json_hash(payload)
    conn.execute(
        """
        INSERT INTO market_sessions(
          calendar_id, session_date, timezone_name, regular_open_time, regular_close_time,
          session_open_time, session_close_time, regular_open_utc_ts, regular_close_utc_ts,
          session_open_utc_ts, session_close_utc_ts, settlement_date, source_hash
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(calendar_id, session_date) DO UPDATE SET
          timezone_name=excluded.timezone_name,
          regular_open_time=excluded.regular_open_time,
          regular_close_time=excluded.regular_close_time,
          session_open_time=excluded.session_open_time,
          session_close_time=excluded.session_close_time,
          regular_open_utc_ts=excluded.regular_open_utc_ts,
          regular_close_utc_ts=excluded.regular_close_utc_ts,
          session_open_utc_ts=excluded.session_open_utc_ts,
          session_close_utc_ts=excluded.session_close_utc_ts,
          settlement_date=excluded.settlement_date,
          source_hash=excluded.source_hash
        """,
        (
            session.calendar_id,
            session.session_date,
            session.timezone_name,
            session.regular_open_time,
            session.regular_close_time,
            session.session_open_time,
            session.session_close_time,
            session.regular_open_utc_ts,
            session.regular_close_utc_ts,
            session.session_open_utc_ts,
            session.session_close_utc_ts,
            session.settlement_date,
            source_hash,
        ),
    )
    row = conn.execute(
        """
        SELECT market_session_id
        FROM market_sessions
        WHERE calendar_id = ?
          AND session_date = ?
        """,
        (session.calendar_id, session.session_date),
    ).fetchone()
    if row is None:
        raise RuntimeError(
            "failed to allocate market_session_id for "
            f"{session.calendar_id} {session.session_date}"
        )
    return int(row["market_session_id"])


def _session_for_bar(bar: StoredOhlcvBar) -> MarketSession:
    calendar_id = str(bar.calendar_id or DEFAULT_US_EQUITY_CALENDAR_ID)
    if _validated_timespan(bar.timespan) == "day":
        session = us_equity_session_for_date(bar.bar_date, calendar_id=calendar_id)
    else:
        session = us_equity_session_for_utc_ts(
            int(bar.bar_start_ts), calendar_id=calendar_id
        )
    if session is None:
        raise ValueError(f"no market session for {calendar_id} bar {bar.bar_date}")
    return session


def _session_start_time_for_bar(
    bar: StoredOhlcvBar, session: MarketSession
) -> str:
    if _validated_timespan(bar.timespan) == "day":
        return session.regular_open_time
    value = (
        dt.datetime.fromtimestamp(int(bar.bar_start_ts) / 1000, dt.UTC)
        .astimezone(ZoneInfo(session.timezone_name))
        .time()
    )
    return value.replace(microsecond=0).isoformat()


def _canonical_utc_start_ts_for_bar(
    bar: StoredOhlcvBar, session: MarketSession
) -> int:
    if _validated_timespan(bar.timespan) == "day":
        return int(session.regular_open_utc_ts)
    return int(bar.bar_start_ts)


def _ensure_ohlcv_bar_scope_id(
    conn: sqlite3.Connection, bar: StoredOhlcvBar, source_id: int
) -> int:
    timespan = _validated_timespan(bar.timespan)
    calendar_id = str(bar.calendar_id or DEFAULT_US_EQUITY_CALENDAR_ID)
    conn.execute(
        """
        INSERT OR IGNORE INTO ohlcv_bar_scopes(
          ohlcv_series_id, calendar_id, timespan, multiplier, adjusted_flag, source_id
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            bar.series_id,
            calendar_id,
            timespan,
            int(bar.multiplier),
            int(bar.adjusted),
            int(source_id),
        ),
    )
    row = conn.execute(
        """
        SELECT ohlcv_bar_scope_id
        FROM ohlcv_bar_scopes
        WHERE ohlcv_series_id = ?
          AND calendar_id = ?
          AND timespan = ?
          AND multiplier = ?
          AND adjusted_flag = ?
          AND source_id = ?
        """,
        (
            bar.series_id,
            calendar_id,
            timespan,
            int(bar.multiplier),
            int(bar.adjusted),
            int(source_id),
        ),
    ).fetchone()
    if row is None:
        raise RuntimeError(
            f"failed to allocate ohlcv_bar_scope_id for series={bar.series_id}"
        )
    return int(row["ohlcv_bar_scope_id"])


def _insert_hot_bar(
    conn: sqlite3.Connection,
    bar: StoredOhlcvBar,
    scope_id: int,
    *,
    market_session_id: int,
    session_start_time: str,
    utc_start_ts: int,
    lineage_id: int,
) -> None:
    table = _hot_bar_table(bar.timespan)
    _require_hot_bar_values(bar)
    conn.execute(
        f"""
        INSERT INTO {table}(
          ohlcv_bar_scope_id, market_session_id, session_start_time,
          utc_start_ts, ohlcv_bar_lineage_id,
          open, high, low, close, volume, vwap, transaction_count
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(ohlcv_bar_scope_id, utc_start_ts)
        DO UPDATE SET
          market_session_id=excluded.market_session_id,
          session_start_time=excluded.session_start_time,
          ohlcv_bar_lineage_id=excluded.ohlcv_bar_lineage_id,
          open=excluded.open,
          high=excluded.high,
          low=excluded.low,
          close=excluded.close,
          volume=excluded.volume,
          vwap=excluded.vwap,
          transaction_count=excluded.transaction_count
        """,
        (
            int(scope_id),
            int(market_session_id),
            session_start_time,
            int(utc_start_ts),
            int(lineage_id),
            bar.open,
            bar.high,
            bar.low,
            bar.close,
            bar.volume,
            bar.vwap,
            bar.transaction_count,
        ),
    )


def _upsert_raw_bar_payload(
    conn: sqlite3.Connection,
    bar: StoredOhlcvBar,
    scope_id: int,
    *,
    utc_start_ts: int,
    lineage_id: int,
    captured_at_utc: str,
) -> None:
    if bar.raw_bar_json in (None, "", {}):
        return
    raw_payload = _without_natural_key(bar.raw_bar_json)
    raw_hash = stable_json_hash(raw_payload)
    conn.execute(
        """
        INSERT INTO ohlcv_bar_raw_payloads(
          ohlcv_bar_scope_id, utc_start_ts, ohlcv_bar_lineage_id,
          raw_bar_json, raw_bar_hash, captured_at_utc
        )
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(ohlcv_bar_scope_id, utc_start_ts, ohlcv_bar_lineage_id)
        DO UPDATE SET
          raw_bar_json=excluded.raw_bar_json,
          raw_bar_hash=excluded.raw_bar_hash,
          captured_at_utc=excluded.captured_at_utc
        """,
        (
            int(scope_id),
            int(utc_start_ts),
            int(lineage_id),
            json_dumps(raw_payload),
            raw_hash,
            captured_at_utc,
        ),
    )


def _group_lineage_rows(
    prepared_rows: list[_PreparedOhlcvBar],
) -> dict[tuple[int, str, str, int], list[_PreparedOhlcvBar]]:
    grouped: dict[tuple[int, str, str, int], list[_PreparedOhlcvBar]] = {}
    for prepared in prepared_rows:
        request_hash, ledger_hash = _lineage_hashes(prepared.bar)
        key = (
            prepared.scope_id,
            request_hash,
            ledger_hash,
            _segment_index_value(prepared.bar.segment_index),
        )
        grouped.setdefault(key, []).append(prepared)
    return grouped


def _plan_id_for_lineage(
    conn: sqlite3.Connection, scope_id: int, request_hash: str, ledger_hash: str
) -> int | None:
    row = conn.execute(
        """
        SELECT p.plan_id
        FROM backfill_plans p
        JOIN ohlcv_bar_scopes sc
          ON sc.ohlcv_bar_scope_id = ?
         AND sc.ohlcv_series_id = p.ohlcv_series_id
        WHERE p.request_hash = ?
          AND p.evidence_ledger_hash = ?
        ORDER BY p.plan_id DESC
        LIMIT 1
        """,
        (int(scope_id), request_hash, ledger_hash),
    ).fetchone()
    return int(row["plan_id"]) if row else None


def _attach_receipt_to_lineage(
    conn: sqlite3.Connection, receipt: dict[str, Any], receipt_id: int
) -> None:
    conn.execute(
        """
        UPDATE ohlcv_bar_lineage
        SET execution_receipt_id = ?
        WHERE request_hash = ?
          AND evidence_ledger_hash = ?
          AND ohlcv_bar_scope_id IN (
            SELECT ohlcv_bar_scope_id
            FROM ohlcv_bar_scopes
            WHERE ohlcv_series_id = ?
          )
        """,
        (
            int(receipt_id),
            receipt["request_hash"],
            receipt["evidence_ledger_hash"],
            int(receipt["ohlcv_series_id"]),
        ),
    )


def _upsert_ohlcv_bar_lineage(
    conn: sqlite3.Connection,
    *,
    scope_id: int,
    request_hash: str,
    ledger_hash: str,
    segment_index: int,
    rows: list[_PreparedOhlcvBar],
    downloaded_at_utc: str,
) -> int:
    from_ts = min(int(row.utc_start_ts) for row in rows)
    to_ts = max(int(row.utc_start_ts) for row in rows)
    plan_id = _plan_id_for_lineage(conn, scope_id, request_hash, ledger_hash)
    conn.execute(
        """
        INSERT INTO ohlcv_bar_lineage(
          ohlcv_bar_scope_id, request_hash, evidence_ledger_hash, segment_index,
          plan_id, from_utc_start_ts, to_utc_start_ts, first_downloaded_at_utc,
          last_downloaded_at_utc, bar_count, quality_exception_count
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(request_hash, evidence_ledger_hash, ohlcv_bar_scope_id, segment_index)
        DO UPDATE SET
          plan_id=COALESCE(excluded.plan_id, plan_id),
          from_utc_start_ts=MIN(from_utc_start_ts, excluded.from_utc_start_ts),
          to_utc_start_ts=MAX(to_utc_start_ts, excluded.to_utc_start_ts),
          first_downloaded_at_utc=MIN(first_downloaded_at_utc, excluded.first_downloaded_at_utc),
          last_downloaded_at_utc=MAX(last_downloaded_at_utc, excluded.last_downloaded_at_utc),
          bar_count=excluded.bar_count,
          quality_exception_count=excluded.quality_exception_count
        """,
        (
            int(scope_id),
            request_hash,
            ledger_hash,
            int(segment_index),
            plan_id,
            from_ts,
            to_ts,
            downloaded_at_utc,
            downloaded_at_utc,
            len(rows),
            sum(1 for row in rows if _bar_has_quality_exception(row.bar)),
        ),
    )
    row = conn.execute(
        """
        SELECT ohlcv_bar_lineage_id
        FROM ohlcv_bar_lineage
        WHERE request_hash = ?
          AND evidence_ledger_hash = ?
          AND ohlcv_bar_scope_id = ?
          AND segment_index = ?
        """,
        (request_hash, ledger_hash, int(scope_id), int(segment_index)),
    ).fetchone()
    if row is None:
        raise RuntimeError("failed to allocate ohlcv_bar_lineage_id")
    return int(row["ohlcv_bar_lineage_id"])


def _assert_no_lineage_overlap(
    conn: sqlite3.Connection,
    *,
    scope_id: int,
    request_hash: str,
    ledger_hash: str,
    segment_index: int,
    from_ts: int,
    to_ts: int,
) -> None:
    row = conn.execute(
        """
        SELECT ohlcv_bar_lineage_id
        FROM ohlcv_bar_lineage
        WHERE ohlcv_bar_scope_id = ?
          AND from_utc_start_ts <= ?
          AND to_utc_start_ts >= ?
          AND NOT (
            request_hash = ?
            AND evidence_ledger_hash = ?
            AND segment_index = ?
          )
        LIMIT 1
        """,
        (
            int(scope_id),
            int(to_ts),
            int(from_ts),
            request_hash,
            ledger_hash,
            int(segment_index),
        ),
    ).fetchone()
    if row is not None:
        raise ValueError(
            "ohlcv_bar_lineage intervals may not overlap for the same scope "
            f"(scope_id={scope_id}, from_ts={from_ts}, to_ts={to_ts})"
        )


def _refresh_ohlcv_bar_lineage_counts(
    conn: sqlite3.Connection,
    *,
    scope_id: int,
    request_hash: str,
    ledger_hash: str,
    segment_index: int,
) -> None:
    lineage = conn.execute(
        """
        SELECT ohlcv_bar_lineage_id
        FROM ohlcv_bar_lineage
        WHERE ohlcv_bar_scope_id = ?
          AND request_hash = ?
          AND evidence_ledger_hash = ?
          AND segment_index = ?
        """,
        (int(scope_id), request_hash, ledger_hash, int(segment_index)),
    ).fetchone()
    if lineage is None:
        return
    lineage_id = int(lineage["ohlcv_bar_lineage_id"])
    table = _hot_bar_table(_scope_timespan(conn, scope_id))
    bar_count = int(
        conn.execute(
            f"""
            SELECT COUNT(*)
            FROM {table}
            WHERE ohlcv_bar_scope_id = ?
              AND ohlcv_bar_lineage_id = ?
            """,
            (int(scope_id), lineage_id),
        ).fetchone()[0]
    )
    quality_count = _quality_exception_count_for_lineage(conn, scope_id, lineage_id)
    conn.execute(
        """
        UPDATE ohlcv_bar_lineage
        SET bar_count = ?,
            quality_exception_count = ?
        WHERE ohlcv_bar_scope_id = ?
          AND request_hash = ?
          AND evidence_ledger_hash = ?
          AND segment_index = ?
        """,
        (
            bar_count,
            quality_count,
            int(scope_id),
            request_hash,
            ledger_hash,
            int(segment_index),
        ),
    )


def _scope_timespan(conn: sqlite3.Connection, scope_id: int) -> str:
    row = conn.execute(
        "SELECT timespan FROM ohlcv_bar_scopes WHERE ohlcv_bar_scope_id = ?",
        (int(scope_id),),
    ).fetchone()
    if row is None:
        raise RuntimeError(f"missing ohlcv_bar_scope_id={scope_id}")
    return _validated_timespan(str(row["timespan"]))


def _quality_exception_count_for_lineage(
    conn: sqlite3.Connection,
    scope_id: int,
    lineage_id: int,
) -> int:
    timespan = _scope_timespan(conn, scope_id)
    hot_table = _hot_bar_table(timespan)
    quality_table = _quality_table(timespan)
    return int(
        conn.execute(
            f"""
            SELECT COUNT(*) FROM (
              SELECT q.ohlcv_bar_scope_id, q.utc_start_ts
              FROM {hot_table} b
              JOIN {quality_table} q
                ON q.ohlcv_bar_scope_id = b.ohlcv_bar_scope_id
               AND q.utc_start_ts = b.utc_start_ts
              WHERE b.ohlcv_bar_scope_id = ?
                AND b.ohlcv_bar_lineage_id = ?
              GROUP BY q.ohlcv_bar_scope_id, q.utc_start_ts
            )
            """,
            (int(scope_id), int(lineage_id)),
        ).fetchone()[0]
    )


def _upsert_quality_exception(
    conn: sqlite3.Connection,
    bar: StoredOhlcvBar,
    scope_id: int,
    *,
    utc_start_ts: int,
    created_at_utc: str,
) -> None:
    table = _quality_table(bar.timespan)
    conn.execute(
        f"""
        INSERT INTO {table}(
          ohlcv_bar_scope_id, utc_start_ts, quality_event_seq,
          bar_quality_status, repair_rule, repair_evidence_json, created_at_utc
        )
        VALUES (?, ?, 1, ?, ?, ?, ?)
        ON CONFLICT(ohlcv_bar_scope_id, utc_start_ts, quality_event_seq)
        DO UPDATE SET
          bar_quality_status=excluded.bar_quality_status,
          repair_rule=excluded.repair_rule,
          repair_evidence_json=excluded.repair_evidence_json,
          created_at_utc=excluded.created_at_utc
        """,
        (
            int(scope_id),
            int(utc_start_ts),
            str(bar.bar_quality_status or "UNCHECKED"),
            str(bar.repair_rule or ""),
            json_dumps(_without_natural_key(bar.repair_evidence_json or {})),
            created_at_utc,
        ),
    )


def _delete_quality_exception_events(
    conn: sqlite3.Connection,
    bar: StoredOhlcvBar,
    scope_id: int,
    *,
    utc_start_ts: int,
) -> None:
    table = _quality_table(bar.timespan)
    conn.execute(
        f"""
        DELETE FROM {table}
        WHERE ohlcv_bar_scope_id = ?
          AND utc_start_ts = ?
        """,
        (int(scope_id), int(utc_start_ts)),
    )


def _lineage_hashes(bar: StoredOhlcvBar) -> tuple[str, str]:
    request_hash = str(bar.request_hash or "").strip()
    ledger_hash = str(bar.ledger_hash or "").strip()
    if request_hash and ledger_hash:
        return request_hash, ledger_hash
    fallback = f"manual:{bar.series_id}:{bar.multiplier}:{_validated_timespan(bar.timespan)}:{int(bar.adjusted)}"
    return request_hash or fallback, ledger_hash or fallback


def _segment_index_value(segment_index: int | None) -> int:
    return int(segment_index) if segment_index is not None else -1


def _bar_has_quality_exception(bar: StoredOhlcvBar) -> bool:
    status = str(bar.bar_quality_status or "UNCHECKED")
    return bool(bar.repair_rule) or status not in NORMAL_BAR_QUALITY_STATUSES


def _hot_bar_table(timespan: str) -> str:
    return BAR_TABLE_BY_TIMESPAN[_validated_timespan(timespan)]


def _quality_table(timespan: str) -> str:
    return QUALITY_TABLE_BY_TIMESPAN[_validated_timespan(timespan)]


def _validated_timespan(timespan: str) -> str:
    value = str(timespan or "").strip().lower()
    if value not in BAR_TABLE_BY_TIMESPAN:
        raise ValueError(f"unsupported OHLCV timespan: {timespan!r}")
    return value


def _require_hot_bar_values(bar: StoredOhlcvBar) -> None:
    missing = [
        name
        for name in ("open", "high", "low", "close", "volume")
        if getattr(bar, name) is None
    ]
    if missing:
        raise ValueError(
            f"hot OHLCV bars require non-null values: {', '.join(missing)}"
        )


def _hot_bar_count(conn: sqlite3.Connection) -> int:
    return sum(
        int(conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0])
        for table in BAR_TABLE_BY_TIMESPAN.values()
    )


def _duplicate_hot_bar_keys(conn: sqlite3.Connection) -> int:
    parts = []
    for table in BAR_TABLE_BY_TIMESPAN.values():
        parts.append(
            f"""
            SELECT ohlcv_bar_scope_id, utc_start_ts
            FROM {table}
            GROUP BY ohlcv_bar_scope_id, utc_start_ts
            HAVING COUNT(*) > 1
            """
        )
    return int(
        conn.execute(
            "SELECT COUNT(*) FROM (" + " UNION ALL ".join(parts) + ")"
        ).fetchone()[0]
    )


def _lineage_coverage_error_count(conn: sqlite3.Connection) -> int:
    parts = []
    for timespan, table in BAR_TABLE_BY_TIMESPAN.items():
        parts.append(
            f"""
            SELECT '{timespan}' AS timespan, b.ohlcv_bar_scope_id, b.utc_start_ts
            FROM {table} b
            LEFT JOIN ohlcv_bar_lineage l
              ON l.ohlcv_bar_lineage_id = b.ohlcv_bar_lineage_id
            WHERE l.ohlcv_bar_lineage_id IS NULL
            """
        )
    return int(
        conn.execute(
            "SELECT COUNT(*) FROM (" + " UNION ALL ".join(parts) + ")"
        ).fetchone()[0]
    )


def _session_coverage_error_count(conn: sqlite3.Connection) -> int:
    parts = []
    for timespan, table in BAR_TABLE_BY_TIMESPAN.items():
        parts.append(
            f"""
            SELECT '{timespan}' AS timespan, b.ohlcv_bar_scope_id, b.utc_start_ts
            FROM {table} b
            LEFT JOIN market_sessions ms
              ON ms.market_session_id = b.market_session_id
            WHERE ms.market_session_id IS NULL
            """
        )
    return int(
        conn.execute(
            "SELECT COUNT(*) FROM (" + " UNION ALL ".join(parts) + ")"
        ).fetchone()[0]
    )


def _market_session_seed_error_count(conn: sqlite3.Connection) -> int:
    expected = {
        (session.calendar_id, session.session_date)
        for session in _seeded_market_sessions()
    }
    placeholders = ", ".join("?" for _ in SEEDED_US_EQUITY_CALENDAR_IDS)
    actual = {
        (str(row["calendar_id"]), str(row["session_date"]))
        for row in conn.execute(
            f"""
            SELECT calendar_id, session_date
            FROM market_sessions
            WHERE calendar_id IN ({placeholders})
            """,
            SEEDED_US_EQUITY_CALENDAR_IDS,
        ).fetchall()
    }
    return len(expected - actual)


def _session_time_error_count(conn: sqlite3.Connection) -> int:
    errors = 0
    errors += int(
        conn.execute(
            """
            SELECT COUNT(*)
            FROM ohlcv_bars_day b
            JOIN market_sessions ms
              ON ms.market_session_id = b.market_session_id
            WHERE b.session_start_time <> ms.regular_open_time
               OR b.utc_start_ts <> ms.regular_open_utc_ts
            """
        ).fetchone()[0]
    )
    for table in ("ohlcv_bars_hour", "ohlcv_bars_minute"):
        rows = conn.execute(
            f"""
            SELECT
              b.utc_start_ts,
              b.session_start_time,
              ms.timezone_name,
              ms.session_open_utc_ts,
              ms.session_close_utc_ts
            FROM {table} b
            JOIN market_sessions ms
              ON ms.market_session_id = b.market_session_id
            """
        ).fetchall()
        for row in rows:
            utc_start_ts = int(row["utc_start_ts"])
            if utc_start_ts < int(row["session_open_utc_ts"]):
                errors += 1
                continue
            if utc_start_ts > int(row["session_close_utc_ts"]):
                errors += 1
                continue
            actual_time = (
                dt.datetime.fromtimestamp(utc_start_ts / 1000, dt.UTC)
                .astimezone(ZoneInfo(str(row["timezone_name"])))
                .time()
                .replace(microsecond=0)
                .isoformat()
            )
            if str(row["session_start_time"]) != actual_time:
                errors += 1
    return errors


def _lineage_bar_count_error_count(conn: sqlite3.Connection) -> int:
    errors = 0
    for timespan, table in BAR_TABLE_BY_TIMESPAN.items():
        errors += int(
            conn.execute(
                f"""
                SELECT COUNT(*) FROM (
                  SELECT
                    l.ohlcv_bar_lineage_id,
                    l.bar_count,
                    COUNT(b.utc_start_ts) AS actual_bar_count
                  FROM ohlcv_bar_lineage l
                  JOIN ohlcv_bar_scopes sc
                    ON sc.ohlcv_bar_scope_id = l.ohlcv_bar_scope_id
                   AND sc.timespan = ?
                  LEFT JOIN {table} b
                    ON b.ohlcv_bar_lineage_id = l.ohlcv_bar_lineage_id
                  GROUP BY l.ohlcv_bar_lineage_id, l.bar_count
                  HAVING l.bar_count <> actual_bar_count
                )
                """,
                (timespan,),
            ).fetchone()[0]
        )
    return errors


def _lineage_quality_count_error_count(conn: sqlite3.Connection) -> int:
    errors = 0
    for timespan, hot_table in BAR_TABLE_BY_TIMESPAN.items():
        quality_table = QUALITY_TABLE_BY_TIMESPAN[timespan]
        errors += int(
            conn.execute(
                f"""
                SELECT COUNT(*) FROM (
                  SELECT
                    l.ohlcv_bar_lineage_id,
                    l.quality_exception_count,
                    COUNT(q.utc_start_ts) AS actual_quality_exception_count
                  FROM ohlcv_bar_lineage l
                  JOIN ohlcv_bar_scopes sc
                    ON sc.ohlcv_bar_scope_id = l.ohlcv_bar_scope_id
                   AND sc.timespan = ?
                  LEFT JOIN {hot_table} b
                    ON b.ohlcv_bar_lineage_id = l.ohlcv_bar_lineage_id
                  LEFT JOIN (
                    SELECT ohlcv_bar_scope_id, utc_start_ts
                    FROM {quality_table}
                    GROUP BY ohlcv_bar_scope_id, utc_start_ts
                  ) q
                    ON q.ohlcv_bar_scope_id = b.ohlcv_bar_scope_id
                   AND q.utc_start_ts = b.utc_start_ts
                  GROUP BY l.ohlcv_bar_lineage_id, l.quality_exception_count
                  HAVING l.quality_exception_count <> actual_quality_exception_count
                )
                """,
                (timespan,),
            ).fetchone()[0]
        )
    return errors


def _lineage_plan_error_count(conn: sqlite3.Connection) -> int:
    return int(
        conn.execute(
            """
            SELECT COUNT(*)
            FROM ohlcv_bar_lineage l
            JOIN backfill_plans p
              ON p.request_hash = l.request_hash
             AND p.evidence_ledger_hash = l.evidence_ledger_hash
            JOIN ohlcv_bar_scopes sc
              ON sc.ohlcv_bar_scope_id = l.ohlcv_bar_scope_id
             AND sc.ohlcv_series_id = p.ohlcv_series_id
            WHERE l.plan_id IS NULL
               OR l.plan_id <> p.plan_id
            """
        ).fetchone()[0]
    )


def _evidence_ledger_membership_error_count(conn: sqlite3.Connection) -> int:
    facts_without_membership = int(
        conn.execute(
            """
            SELECT COUNT(*)
            FROM evidence_facts f
            WHERE NOT EXISTS (
              SELECT 1
              FROM evidence_ledger_facts lf
              WHERE lf.fact_hash = f.fact_hash
                AND lf.ohlcv_series_id = f.ohlcv_series_id
            )
            """
        ).fetchone()[0]
    )
    orphan_membership = int(
        conn.execute(
            """
            SELECT COUNT(*)
            FROM evidence_ledger_facts lf
            LEFT JOIN evidence_facts f
              ON f.fact_hash = lf.fact_hash
            WHERE f.fact_hash IS NULL
            """
        ).fetchone()[0]
    )
    return facts_without_membership + orphan_membership


def _raw_payload_error_count(conn: sqlite3.Connection) -> int:
    return int(
        conn.execute(
            """
            SELECT COUNT(*)
            FROM ohlcv_bar_raw_payloads rp
            LEFT JOIN v_ohlcv_bars_hot_unified b
              ON b.ohlcv_bar_scope_id = rp.ohlcv_bar_scope_id
             AND b.utc_start_ts = rp.utc_start_ts
             AND b.ohlcv_bar_lineage_id = rp.ohlcv_bar_lineage_id
            WHERE b.ohlcv_bar_scope_id IS NULL
               OR NOT json_valid(rp.raw_bar_json)
               OR rp.raw_bar_hash = ''
               OR rp.captured_at_utc = ''
            """
        ).fetchone()[0]
    )


def _hot_table_shape_errors(conn: sqlite3.Connection) -> list[str]:
    expected = [
        "ohlcv_bar_scope_id",
        "market_session_id",
        "session_start_time",
        "utc_start_ts",
        "ohlcv_bar_lineage_id",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "vwap",
        "transaction_count",
    ]
    errors: list[str] = []
    for table in BAR_TABLE_BY_TIMESPAN.values():
        actual = [
            str(row["name"])
            for row in conn.execute(f"PRAGMA table_info({table})").fetchall()
        ]
        if actual != expected:
            errors.append(f"{table} columns={actual}")
    return errors


def _hot_without_rowid_errors(conn: sqlite3.Connection) -> list[str]:
    errors: list[str] = []
    for table in BAR_TABLE_BY_TIMESPAN.values():
        row = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?",
            (table,),
        ).fetchone()
        sql = str(row["sql"] or "") if row else ""
        if "WITHOUT ROWID" not in sql.upper():
            errors.append(table)
    return errors


def _ohlcv_view_count_errors(conn: sqlite3.Connection) -> list[str]:
    hot_count = _hot_bar_count(conn)
    checks = {
        "v_ohlcv_bars_hot_unified": int(
            conn.execute("SELECT COUNT(*) FROM v_ohlcv_bars_hot_unified").fetchone()[0]
        ),
        "v_ohlcv_bars_unified": int(
            conn.execute("SELECT COUNT(*) FROM v_ohlcv_bars_unified").fetchone()[0]
        ),
    }
    return [
        f"{name}={count} hot={hot_count}"
        for name, count in checks.items()
        if count != hot_count
    ]


def _persisted_plan_payload(plan: BackfillPlan) -> dict[str, Any]:
    return _without_natural_key(plan.to_legacy_dict())


def _without_natural_key(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            str(key): _without_natural_key(item)
            for key, item in value.items()
            if str(key) != "natural_key"
        }
    if isinstance(value, list):
        return [_without_natural_key(item) for item in value]
    if isinstance(value, tuple):
        return [_without_natural_key(item) for item in value]
    return value


def _persisted_json_natural_key_rows(conn: sqlite3.Connection) -> int:
    checks = (
        ("ohlcv_series", "target_json"),
        ("ticker_aliases", "raw_json"),
        ("evidence_facts", "payload_json"),
        ("backfill_plans", "plan_json"),
        ("execution_receipts", "request_log_json"),
        ("execution_receipts", "receipt_json"),
        ("execution_approvals", "approval_json"),
        ("reference_universe_snapshots", "raw_json"),
        ("reference_universe_snapshots", "source_request_json"),
        ("reference_universe_updates", "pending_requests_json"),
        ("reference_universe_updates", "request_json"),
        ("reference_universe_updates", "request_log_json"),
        ("ohlcv_bar_raw_payloads", "raw_bar_json"),
    )
    total = 0
    for table, column in checks:
        total += int(
            conn.execute(
                f"""
                WITH RECURSIVE walk(value) AS (
                  SELECT {column}
                  FROM {table}
                  WHERE json_valid({column})
                  UNION ALL
                  SELECT json_each.value
                  FROM walk, json_each(walk.value)
                  WHERE json_valid(walk.value)
                    AND json_type(walk.value) IN ('object', 'array')
                )
                SELECT COUNT(*)
                FROM walk, json_each(walk.value)
                WHERE json_valid(walk.value)
                  AND json_type(walk.value) = 'object'
                  AND json_each.key = 'natural_key'
                """
            ).fetchone()[0]
        )
    return total


def _orphan_lookup_child_rows(conn: sqlite3.Connection) -> int:
    child_tables = (
        "ohlcv_series",
        "ticker_aliases",
        "evidence_facts",
        "evidence_ledger_facts",
        "backfill_plans",
        "ohlcv_bar_scopes",
        "execution_receipts",
        "execution_approvals",
        "reference_universe_snapshots",
    )
    total = 0
    for table in child_tables:
        total += int(
            conn.execute(
                f"""
                SELECT COUNT(*)
                FROM {table} child
                LEFT JOIN ohlcv_series_id_lookup parent
                  ON parent.ohlcv_series_id = child.ohlcv_series_id
                WHERE child.ohlcv_series_id IS NOT NULL
                  AND parent.ohlcv_series_id IS NULL
                """
            ).fetchone()[0]
        )
    return total


def _upsert_reference_series_metadata(
    conn: sqlite3.Connection,
    snapshot: StoredReferenceSnapshot,
    series_id: int,
    now: str,
) -> None:
    target = {
        "cik": snapshot.cik,
        "company_id": None,
        "company_name": snapshot.company_name,
        "composite_figi": snapshot.composite_figi,
        "current_company_name": snapshot.company_name,
        "identity_status": snapshot.identity_status or "unknown",
        "known_alias_tickers": [],
        "latest_primary_exchange": snapshot.primary_exchange,
        "latest_ticker": snapshot.ticker,
        "locale": snapshot.locale,
        "market": snapshot.market,
        "ohlcv_series_id": series_id,
        "provisional_key": snapshot.provisional_key or None,
        "security_id": None,
        "security_type": snapshot.security_type or None,
        "share_class_figi": snapshot.share_class_figi,
    }
    conn.execute(
        """
        INSERT INTO ohlcv_series(
          ohlcv_series_id, composite_figi, share_class_figi, latest_ticker,
          identity_status, company_name, target_json, first_seen_utc, last_seen_utc
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(ohlcv_series_id) DO UPDATE SET
          composite_figi=excluded.composite_figi,
          share_class_figi=excluded.share_class_figi,
          latest_ticker=excluded.latest_ticker,
          identity_status=excluded.identity_status,
          company_name=excluded.company_name,
          target_json=excluded.target_json,
          last_seen_utc=excluded.last_seen_utc
        """,
        (
            series_id,
            snapshot.composite_figi,
            snapshot.share_class_figi,
            snapshot.ticker,
            snapshot.identity_status or "unknown",
            snapshot.company_name,
            json_dumps(target),
            now,
            now,
        ),
    )


def _upsert_reference_current_alias(
    conn: sqlite3.Connection,
    snapshot: StoredReferenceSnapshot,
    series_id: int,
    now: str,
) -> None:
    if not snapshot.ticker:
        return
    _ensure_data_source_id(conn, snapshot.provider or "reference_universe.snapshot")
    conn.execute(
        """
        INSERT INTO ticker_aliases(
          ohlcv_series_id, ticker, as_of_date, active, source, raw_json, first_seen_utc, last_seen_utc
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(ohlcv_series_id, ticker, as_of_date, source) DO UPDATE SET
          active=excluded.active,
          raw_json=excluded.raw_json,
          last_seen_utc=excluded.last_seen_utc
        """,
        (
            series_id,
            snapshot.ticker,
            snapshot.snapshot_as_of_date,
            _active_flag(snapshot.active),
            snapshot.provider or "reference_universe.snapshot",
            json_dumps(_without_natural_key(snapshot.raw or {})),
            now,
            now,
        ),
    )


def _upsert_series(conn: sqlite3.Connection, plan: BackfillPlan, now: str) -> None:
    if not plan.target.natural_key:
        raise ValueError(
            "TargetIdentity.natural_key is required before persisting a series"
        )
    allocated_id = _ensure_ohlcv_series_id(conn, plan.target.natural_key, now)
    if allocated_id != plan.target.ohlcv_series_id:
        raise ValueError(
            "TargetIdentity.ohlcv_series_id does not match central lookup: "
            f"target={plan.target.ohlcv_series_id} lookup={allocated_id} natural_key={plan.target.natural_key}"
        )
    target = _without_natural_key(plan.target.to_legacy_dict())
    conn.execute(
        """
        INSERT INTO ohlcv_series(
          ohlcv_series_id, composite_figi, share_class_figi, latest_ticker,
          identity_status, company_name, target_json, first_seen_utc, last_seen_utc
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(ohlcv_series_id) DO UPDATE SET
          composite_figi=excluded.composite_figi,
          share_class_figi=excluded.share_class_figi,
          latest_ticker=excluded.latest_ticker,
          identity_status=excluded.identity_status,
          company_name=excluded.company_name,
          target_json=excluded.target_json,
          last_seen_utc=excluded.last_seen_utc
        """,
        (
            plan.target.ohlcv_series_id,
            plan.target.composite_figi,
            plan.target.share_class_figi,
            plan.target.latest_ticker,
            plan.target.identity_status,
            plan.target.company_name or plan.target.current_company_name,
            json_dumps(target),
            now,
            now,
        ),
    )


def _upsert_aliases(conn: sqlite3.Connection, plan: BackfillPlan, now: str) -> None:
    aliases: list[dict[str, Any]] = []
    if plan.target.latest_ticker:
        aliases.append(
            {
                "ticker": plan.target.latest_ticker,
                "source": "target.latest_ticker",
                "active": 1,
            }
        )
    for alias in plan.known_aliases:
        aliases.append(alias.to_legacy_dict() | {"source": "plan.known_aliases"})
    for segment in plan.segments:
        aliases.append(
            {
                "ticker": segment.ticker,
                "source": f"plan.segment:{segment.segment_index}",
                "active": None,
            }
        )

    for alias in aliases:
        ticker = str(alias.get("ticker") or alias.get("symbol_text") or "")
        if not ticker:
            continue
        _ensure_data_source_id(conn, str(alias.get("source") or "plan"))
        conn.execute(
            """
            INSERT INTO ticker_aliases(
              ohlcv_series_id, ticker, as_of_date, active, source, raw_json, first_seen_utc, last_seen_utc
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(ohlcv_series_id, ticker, as_of_date, source) DO UPDATE SET
              active=excluded.active,
              raw_json=excluded.raw_json,
              last_seen_utc=excluded.last_seen_utc
            """,
            (
                plan.target.ohlcv_series_id,
                ticker,
                str(alias.get("as_of_date") or ""),
                alias.get("active"),
                str(alias.get("source") or "plan"),
                json_dumps(_without_natural_key(alias)),
                now,
                now,
            ),
        )


def _insert_evidence_facts(
    conn: sqlite3.Connection,
    plan: BackfillPlan,
    facts: tuple[EvidenceFact, ...],
    now: str,
) -> None:
    for fact in facts:
        payload = fact.to_legacy_dict()
        persisted_payload = _without_natural_key(fact.payload_value())
        fact_hash = stable_json_hash(_without_natural_key(payload))
        series_id = _fact_series_id(fact)
        persisted_series_id = (
            series_id
            if series_id == plan.target.ohlcv_series_id
            else plan.target.ohlcv_series_id
        )
        _ensure_data_source_id(conn, fact.source)
        conn.execute(
            """
            INSERT OR IGNORE INTO evidence_facts(
              ohlcv_series_id, kind, fact_key_json, payload_json, source, fact_hash, inserted_at_utc
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                persisted_series_id,
                fact.kind,
                json_dumps(list(fact.key)),
                json_dumps(persisted_payload),
                fact.source,
                fact_hash,
                now,
            ),
        )
        conn.execute(
            """
            INSERT OR IGNORE INTO evidence_ledger_facts(
              evidence_ledger_hash, fact_hash, ohlcv_series_id, inserted_at_utc
            )
            VALUES (?, ?, ?, ?)
            """,
            (
                plan.evidence_ledger_hash,
                fact_hash,
                persisted_series_id,
                now,
            ),
        )


def _insert_plan(conn: sqlite3.Connection, plan: BackfillPlan, now: str) -> None:
    plan_payload = _persisted_plan_payload(plan)
    conn.execute(
        """
        INSERT OR IGNORE INTO backfill_plans(
          request_hash, evidence_ledger_hash, ohlcv_series_id, status, planner_version,
          created_at_utc, plan_json, plan_hash, inserted_at_utc
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            plan.request.request_hash,
            plan.evidence_ledger_hash,
            plan.target.ohlcv_series_id,
            plan.status,
            plan.planner_version,
            plan.created_at_utc,
            json_dumps(plan_payload),
            stable_json_hash(plan_payload),
            now,
        ),
    )


def _approval_payload(
    plan: BackfillPlan,
    approval: Any,
    *,
    approved_at_utc: str,
    reason: str,
) -> dict[str, Any]:
    return {
        "request_hash": plan.request.request_hash,
        "evidence_ledger_hash": plan.evidence_ledger_hash,
        "plan_hash": _plan_hash(plan),
        "ohlcv_series_id": plan.target.ohlcv_series_id,
        "plan_status": plan.status,
        "approved_by": approval.approved_by or "",
        "allow_caution": bool(approval.allow_caution),
        "reason": reason,
        "approved_at_utc": approved_at_utc,
    }


def _plan_hash(plan: BackfillPlan) -> str:
    return stable_json_hash(_persisted_plan_payload(plan))


def _fact_series_id(fact: EvidenceFact) -> int | None:
    if not fact.key:
        return None
    try:
        return int(fact.key[0])
    except ValueError:
        return None


def _sqlite_table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return bool(
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
            (table,),
        ).fetchone()
    )


def _readonly_db_uri(path: Path) -> str:
    return readonly_db_uri(path)


def _active_flag(value: bool | int | None) -> int:
    if value is True:
        return 1
    if value is False:
        return 0
    if value in (0, 1):
        return int(value)
    return -1


def _active_mode(value: bool | int | None) -> str:
    if value is True or value == 1:
        return "active"
    if value is False or value == 0:
        return "inactive"
    return "all"


def _active_value(flag: Any) -> bool | None:
    if flag == 1:
        return True
    if flag == 0:
        return False
    return None


def _stored_reference_snapshot_from_row(row: sqlite3.Row) -> StoredReferenceSnapshot:
    return StoredReferenceSnapshot(
        provider=str(row["provider"] or ""),
        snapshot_as_of_date=str(row["snapshot_as_of_date"] or ""),
        ticker=str(row["ticker"] or ""),
        ohlcv_series_id=int(row["ohlcv_series_id"]),
        active=_active_value(row["active_flag"]),
        company_name=str(row["company_name"] or ""),
        cik=str(row["cik"] or ""),
        composite_figi=str(row["composite_figi"] or ""),
        share_class_figi=str(row["share_class_figi"] or ""),
        security_type=str(row["security_type"] or ""),
        primary_exchange=str(row["primary_exchange"] or ""),
        market=str(row["market"] or ""),
        locale=str(row["locale"] or ""),
        identity_status=str(row["identity_status"] or ""),
        natural_key=str(row["natural_key"] or ""),
        provisional_key=str(row["provisional_key"] or ""),
        raw=_json_loads(row["raw_json"]),
        source_request=_json_loads(row["source_request_json"]),
    )


def _json_loads(value: Any) -> Any:
    if not value:
        return {}
    try:
        return json.loads(str(value))
    except ValueError:
        return {}


def json_dumps(payload: Any) -> str:
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def utc_now() -> str:
    return dt.datetime.now(dt.UTC).isoformat()
