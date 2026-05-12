from __future__ import annotations

import json
import sqlite3
from dataclasses import replace
from pathlib import Path

import pytest

from stock_universe.domain import (
    BackfillPlan,
    BackfillRequest,
    EvidenceFact,
    PlannedSegment,
    TargetIdentity,
)
from stock_universe.executors import ExecutionApproval
from stock_universe.quality_audit import quality_audit
from stock_universe.storage.sqlite_repo import (
    SCHEMA_VERSION,
    SEEDED_US_EQUITY_CALENDAR_IDS,
    SQLiteStockUniverseRepository,
    StoredOhlcvBar,
    StoredReferenceSnapshot,
)


def test_schema_seeds_market_session_calendar_from_us_market_hours(
    tmp_path: Path,
) -> None:
    repository = SQLiteStockUniverseRepository(tmp_path / "stock_universe.sqlite")

    repository.ensure_schema()

    with repository.connect() as conn:
        seeded_calendar_ids = {
            row["calendar_id"]
            for row in conn.execute(
                "SELECT DISTINCT calendar_id FROM market_sessions"
            ).fetchall()
        }
        xnas_count = conn.execute(
            "SELECT COUNT(*) FROM market_sessions WHERE calendar_id = 'XNAS'"
        ).fetchone()[0]
        row = conn.execute(
            """
            SELECT
              session_date,
              regular_open_time,
              regular_open_utc_ts,
              session_open_time,
              session_close_time
            FROM market_sessions
            WHERE calendar_id = 'XNAS'
              AND session_date = '2024-06-10'
            """
        ).fetchone()

    assert set(SEEDED_US_EQUITY_CALENDAR_IDS).issubset(seeded_calendar_ids)
    assert xnas_count >= 2500
    assert row["session_date"] == "2024-06-10"
    assert row["regular_open_time"] == "09:30:00"
    assert row["regular_open_utc_ts"] == 1718026200000
    assert row["session_open_time"] == "04:00:00"
    assert row["session_close_time"] == "20:00:00"
    validation = repository.validate()
    assert validation.ok is True
    assert "market session calendar seeded from us_market_hours.json" in validation.checks


def test_schema_rejects_non_fresh_db_without_metadata(tmp_path: Path) -> None:
    db = tmp_path / "stock_universe.sqlite"
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE unrelated_data(id INTEGER PRIMARY KEY)")

    repository = SQLiteStockUniverseRepository(db)

    with pytest.raises(RuntimeError, match="not fresh"):
        repository.ensure_schema()

    with sqlite3.connect(db) as conn:
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'unrelated_data'"
            ).fetchone()[0]
            == 1
        )


def test_schema_rejects_incompatible_schema_version_without_resetting(
    tmp_path: Path,
) -> None:
    db = tmp_path / "stock_universe.sqlite"
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE schema_metadata(key TEXT PRIMARY KEY, value TEXT)")
        conn.execute("CREATE TABLE retained_data(id INTEGER PRIMARY KEY)")
        conn.execute(
            "INSERT INTO schema_metadata(key, value) VALUES ('schema_version', 'stock_universe_sqlite.v0')"
        )

    repository = SQLiteStockUniverseRepository(db)

    with pytest.raises(RuntimeError, match="Unsupported stock-universe SQLite schema"):
        repository.ensure_schema()

    with sqlite3.connect(db) as conn:
        assert (
            conn.execute(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'retained_data'"
            ).fetchone()[0]
            == 1
        )


def test_daily_bar_is_session_canonical_and_raw_payload_is_sidecar(
    tmp_path: Path,
) -> None:
    repository = SQLiteStockUniverseRepository(tmp_path / "stock_universe.sqlite")
    series_id = _seed_reference(repository, ticker="NVDA", natural_key="test:NVDA")
    plan = _plan(
        series_id=series_id,
        natural_key="test:NVDA",
        ticker="NVDA",
        from_date="2024-06-10",
        to_date="2024-06-10",
        ledger_hash="ledger-daily",
    )
    repository.persist_plan_context(
        plan,
        evidence_facts=(
            EvidenceFact(
                "provider_request",
                (str(series_id), "daily"),
                {"endpoint": "aggs", "ticker": "NVDA"},
                "test",
            ),
        ),
    )
    repository.insert_execution_approval(
        plan,
        ExecutionApproval(plan.request.request_hash, approved_by="pytest"),
        reason="foundation test",
    )

    repository.insert_bars(
        [
            StoredOhlcvBar(
                series_id=series_id,
                ticker="NVDA",
                bar_date="2024-06-10",
                bar_start_ts=1717977600000,
                multiplier=1,
                timespan="day",
                adjusted=True,
                open=120.37,
                high=123.10,
                low=117.01,
                close=121.79,
                volume=314157461,
                vwap=121.1155,
                transaction_count=1024,
                request_hash=plan.request.request_hash,
                ledger_hash=plan.evidence_ledger_hash,
                segment_index=1,
                bar_quality_status="VALIDATED_REPAIRED",
                repair_rule="provider-raw-split-anomaly",
                raw_bar_json={
                    "t": 1717977600000,
                    "o": 120.37,
                    "h": 195.95,
                    "l": 117.01,
                    "c": 121.79,
                    "v": 314157461,
                },
                repair_evidence_json={"canonical_high": 123.10, "raw_high": 195.95},
            )
        ]
    )
    receipt_id = repository.insert_execution_receipt(
        {
            "request_hash": plan.request.request_hash,
            "evidence_ledger_hash": plan.evidence_ledger_hash,
            "ohlcv_series_id": series_id,
            "status": "ok",
            "approved_by": "pytest",
            "started_at_utc": "2026-05-12T00:00:00+00:00",
            "finished_at_utc": "2026-05-12T00:00:01+00:00",
            "planned_segment_count": 1,
            "fetched_bar_count": 1,
            "inserted_bar_count": 1,
            "request_log": [],
        }
    )

    with repository.connect() as conn:
        schema_version = conn.execute(
            "SELECT value FROM schema_metadata WHERE key = 'schema_version'"
        ).fetchone()[0]
        hot_columns = [
            row["name"] for row in conn.execute("PRAGMA table_info(ohlcv_bars_day)")
        ]
        row = conn.execute(
            """
            SELECT
              b.utc_start_ts,
              b.session_start_time,
              ms.session_date,
              ms.regular_open_time,
              ms.regular_open_utc_ts,
              l.plan_id,
              l.execution_receipt_id,
              l.from_utc_start_ts,
              l.to_utc_start_ts,
              l.bar_count,
              l.quality_exception_count,
              rp.raw_bar_json,
              v.bar_date,
              v.high,
              v.raw_bar_json AS view_raw_bar_json
            FROM ohlcv_bars_day b
            JOIN market_sessions ms ON ms.market_session_id = b.market_session_id
            JOIN ohlcv_bar_lineage l
              ON l.ohlcv_bar_lineage_id = b.ohlcv_bar_lineage_id
            JOIN ohlcv_bar_raw_payloads rp
              ON rp.ohlcv_bar_scope_id = b.ohlcv_bar_scope_id
             AND rp.utc_start_ts = b.utc_start_ts
             AND rp.ohlcv_bar_lineage_id = b.ohlcv_bar_lineage_id
            JOIN v_ohlcv_bars_unified v
              ON v.ohlcv_bar_scope_id = b.ohlcv_bar_scope_id
             AND v.utc_start_ts = b.utc_start_ts
            """
        ).fetchone()

    assert schema_version == SCHEMA_VERSION
    assert hot_columns == [
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
    assert row["utc_start_ts"] == 1718026200000
    assert row["session_start_time"] == "09:30:00"
    assert row["session_date"] == "2024-06-10"
    assert row["regular_open_time"] == "09:30:00"
    assert row["regular_open_utc_ts"] == 1718026200000
    assert row["plan_id"] is not None
    assert row["execution_receipt_id"] == receipt_id
    assert row["from_utc_start_ts"] == 1718026200000
    assert row["to_utc_start_ts"] == 1718026200000
    assert row["bar_count"] == 1
    assert row["quality_exception_count"] == 1
    assert json.loads(row["raw_bar_json"])["t"] == 1717977600000
    assert json.loads(row["view_raw_bar_json"])["h"] == 195.95
    assert row["bar_date"] == "2024-06-10"
    assert row["high"] == 123.10

    validation = repository.validate()
    assert validation.ok is True
    assert "each hot row has direct lineage" in validation.checks
    assert "each hot row has direct market session" in validation.checks
    assert "bar session times match UTC keys" in validation.checks
    assert "raw provider payload side table is valid" in validation.checks


def test_thirty_minute_bar_keeps_utc_key_and_exchange_session_time(
    tmp_path: Path,
) -> None:
    repository = SQLiteStockUniverseRepository(tmp_path / "stock_universe.sqlite")
    series_id = _seed_reference(repository, ticker="NVDA", natural_key="test:NVDA")

    repository.insert_bars(
        [
            StoredOhlcvBar(
                series_id=series_id,
                ticker="NVDA",
                bar_date="2026-01-09",
                bar_start_ts=1767970800000,
                multiplier=30,
                timespan="minute",
                adjusted=True,
                open=184.01,
                high=184.30,
                low=183.99,
                close=184.20,
                volume=5000,
                vwap=184.15,
                transaction_count=12,
                request_hash="request-30m",
                ledger_hash="ledger-30m",
                segment_index=0,
                bar_quality_status="VALIDATED",
            )
        ]
    )

    with repository.connect() as conn:
        row = conn.execute(
            """
            SELECT
              b.utc_start_ts,
              b.session_start_time,
              ms.session_date,
              ms.timezone_name,
              ms.session_open_utc_ts,
              ms.session_close_utc_ts,
              v.bar_date,
              v.calendar_id
            FROM ohlcv_bars_minute b
            JOIN market_sessions ms ON ms.market_session_id = b.market_session_id
            JOIN v_ohlcv_bars_unified v
              ON v.ohlcv_bar_scope_id = b.ohlcv_bar_scope_id
             AND v.utc_start_ts = b.utc_start_ts
            """
        ).fetchone()

    assert row["utc_start_ts"] == 1767970800000
    assert row["session_start_time"] == "10:00:00"
    assert row["session_date"] == "2026-01-09"
    assert row["timezone_name"] == "America/New_York"
    assert row["session_open_utc_ts"] <= row["utc_start_ts"] <= row["session_close_utc_ts"]
    assert row["bar_date"] == "2026-01-09"
    assert row["calendar_id"] == "US_EQUITY"
    assert repository.validate().ok is True


def test_evidence_fact_identity_is_separate_from_ledger_membership(
    tmp_path: Path,
) -> None:
    repository = SQLiteStockUniverseRepository(tmp_path / "stock_universe.sqlite")
    series_id = repository.ensure_ohlcv_series_id("test:NVDA")
    fact = EvidenceFact(
        "provider_request",
        (str(series_id), "shared"),
        {"endpoint": "aggs", "ticker": "NVDA"},
        "test",
    )

    first = _plan(
        series_id=series_id,
        natural_key="test:NVDA",
        ticker="NVDA",
        from_date="2024-06-10",
        to_date="2024-06-10",
        ledger_hash="ledger-one",
    )
    second = replace(first, evidence_ledger_hash="ledger-two")
    repository.persist_plan_context(first, evidence_facts=(fact,))
    repository.persist_plan_context(second, evidence_facts=(fact,))

    with repository.connect() as conn:
        evidence_columns = [
            row["name"] for row in conn.execute("PRAGMA table_info(evidence_facts)")
        ]
        fact_count = conn.execute("SELECT COUNT(*) FROM evidence_facts").fetchone()[0]
        memberships = conn.execute(
            """
            SELECT evidence_ledger_hash
            FROM evidence_ledger_facts
            ORDER BY evidence_ledger_hash
            """
        ).fetchall()

    assert "ledger_hash" not in evidence_columns
    assert fact_count == 1
    assert [row["evidence_ledger_hash"] for row in memberships] == [
        "ledger-one",
        "ledger-two",
    ]
    validation = repository.validate()
    assert validation.ok is True
    assert "evidence ledgers retain explicit fact memberships" in validation.checks


def test_quality_audit_flags_plan_session_gaps(
    tmp_path: Path,
) -> None:
    db = tmp_path / "stock_universe.sqlite"
    repository = SQLiteStockUniverseRepository(db)
    series_id = _seed_reference(repository, ticker="NVDA", natural_key="test:NVDA")
    plan = _plan(
        series_id=series_id,
        natural_key="test:NVDA",
        ticker="NVDA",
        from_date="2024-06-10",
        to_date="2024-06-12",
        ledger_hash="ledger-gap",
    )
    repository.persist_plan_context(plan)
    repository.insert_bars(
        [
            _daily_bar(series_id, "NVDA", "2024-06-10", plan),
            _daily_bar(series_id, "NVDA", "2024-06-12", plan),
        ]
    )

    report = quality_audit(db, stale_before="2024-06-12", series_ids=(series_id,))

    assert report["issue_count"] == 1
    assert report["issues"][0]["category"] == "plan_session_gap"
    assert report["issues"][0]["plan_expected_session_count"] == 3
    assert report["issues"][0]["plan_loaded_session_count"] == 2
    assert report["issues"][0]["plan_missing_session_count"] == 1
    assert report["issues"][0]["first_missing_session_date"] == "2024-06-11"
    assert "--from-date 2024-06-11" in report["issues"][0]["suggested_next_command"]


def test_quality_audit_ignores_non_actionable_future_session_gap(
    tmp_path: Path,
) -> None:
    db = tmp_path / "stock_universe.sqlite"
    repository = SQLiteStockUniverseRepository(db)
    series_id = _seed_reference(repository, ticker="NVDA", natural_key="test:NVDA")
    plan = _plan(
        series_id=series_id,
        natural_key="test:NVDA",
        ticker="NVDA",
        from_date="2024-06-10",
        to_date="2024-06-12",
        ledger_hash="ledger-current-day",
    )
    repository.persist_plan_context(plan)
    repository.insert_bars(
        [
            _daily_bar(series_id, "NVDA", "2024-06-10", plan),
            _daily_bar(series_id, "NVDA", "2024-06-11", plan),
        ]
    )

    report = quality_audit(
        db,
        stale_before="2024-06-11",
        series_ids=(series_id,),
        include_healthy=True,
    )

    assert report["issue_count"] == 0
    assert report["issues"][0]["category"] == "no_action_needed"
    assert report["issues"][0]["plan_total_missing_session_count"] == 1
    assert report["issues"][0]["plan_missing_session_count"] == 0


def _seed_reference(
    repository: SQLiteStockUniverseRepository, *, ticker: str, natural_key: str
) -> int:
    repository.upsert_reference_snapshots(
        [
            StoredReferenceSnapshot(
                provider="massive.reference_tickers",
                snapshot_as_of_date="2026-05-08",
                ticker=ticker,
                active=True,
                company_name=f"{ticker} Inc.",
                cik="0000000000",
                composite_figi=f"BBG{ticker}",
                share_class_figi=f"BBG{ticker}1",
                security_type="CS",
                primary_exchange="XNAS",
                market="stocks",
                locale="us",
                identity_status="permanent",
                natural_key=natural_key,
                raw={"ticker": ticker},
            )
        ]
    )
    series_id = repository.lookup_ohlcv_series_id(natural_key)
    assert series_id is not None
    return series_id


def _daily_bar(
    series_id: int, ticker: str, bar_date: str, plan: BackfillPlan
) -> StoredOhlcvBar:
    return StoredOhlcvBar(
        series_id=series_id,
        ticker=ticker,
        bar_date=bar_date,
        bar_start_ts=0,
        multiplier=1,
        timespan="day",
        adjusted=True,
        open=100,
        high=101,
        low=99,
        close=100.5,
        volume=1000,
        calendar_id=plan.target.latest_primary_exchange,
        request_hash=plan.request.request_hash,
        ledger_hash=plan.evidence_ledger_hash,
        segment_index=1,
        bar_quality_status="VALIDATED",
    )


def _plan(
    *,
    series_id: int,
    natural_key: str,
    ticker: str,
    from_date: str,
    to_date: str,
    ledger_hash: str,
) -> BackfillPlan:
    request = BackfillRequest(
        series_id=series_id,
        from_date=from_date,
        to_date=to_date,
        multiplier=1,
        timespan="day",
        adjusted=True,
    )
    target = TargetIdentity(
        ohlcv_series_id=series_id,
        latest_ticker=ticker,
        latest_primary_exchange="XNAS",
        identity_status="permanent",
        natural_key=natural_key,
        market="stocks",
        locale="us",
        security_type="CS",
    )
    segment = PlannedSegment(
        segment_index=1,
        ticker=ticker,
        from_date=from_date,
        to_date=to_date,
        source="unit_test",
        valid=True,
    )
    return BackfillPlan(
        request=request,
        status="ok",
        target=target,
        segments=(segment,),
        decisions=(),
        evidence_ledger_hash=ledger_hash,
        planner_version="unit-test",
        created_at_utc="2026-05-12T00:00:00+00:00",
    )
