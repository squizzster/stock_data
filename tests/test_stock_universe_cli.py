from __future__ import annotations

import json
import sqlite3
import subprocess
import time
from dataclasses import replace
from pathlib import Path

import pytest

from stock_universe import cli as stock_universe_cli_module
from stock_universe.cli import main as stock_universe_main
from stock_universe.domain import (
    BackfillPlan,
    BackfillRequest,
    PlannedSegment,
    RuleDecision,
    TargetIdentity,
)
from stock_universe.storage import (
    SQLiteStockUniverseRepository,
    StoredReferenceSnapshot,
    ValidationReport,
)
from stock_universe.xctx import cli as xctx_cli_module


ROOT = Path(__file__).resolve().parents[1]


def _payload(capsys):
    return json.loads(capsys.readouterr().out)


def _query_plan(conn: sqlite3.Connection, sql: str) -> str:
    return "\n".join(str(row[-1]) for row in conn.execute(f"EXPLAIN QUERY PLAN {sql}"))


def test_stock_universe_help_points_agents_to_xctx(capsys) -> None:
    with pytest.raises(SystemExit) as exc:
        stock_universe_main(["--help"])

    assert exc.value.code == 0
    out = capsys.readouterr().out
    assert "Agent / Executable Context" in out
    assert "stock-universe xctx tree" in out
    assert "stock-universe xctx examples" in out


def test_stock_universe_missing_command_prints_agent_help(capsys) -> None:
    with pytest.raises(SystemExit) as exc:
        stock_universe_main([])

    assert exc.value.code == 2
    err = capsys.readouterr().err
    assert "Agent / Executable Context" in err
    assert "stock-universe xctx doctor" in err
    assert "stock-universe: error: the following arguments are required: command" in err


def test_stock_universe_nested_xctx_help_teaches_agent_loop(capsys) -> None:
    with pytest.raises(SystemExit) as exc:
        stock_universe_main(["xctx", "--help"])

    assert exc.value.code == 0
    out = capsys.readouterr().out
    assert "Recommended agent loop" in out
    assert "1. stock-universe xctx doctor" in out
    assert "2. stock-universe xctx universe-status" in out
    assert "3. stock-universe xctx tree" in out
    assert "9. stock-universe xctx observe" in out
    assert "command.name and logical_command values" in out
    assert "stock-universe xctx tree" in out
    assert "./stock_universe.cli xctx examples" in out


def test_stock_universe_nested_xctx_missing_command_prints_protocol_help(
    capsys,
) -> None:
    with pytest.raises(SystemExit) as exc:
        stock_universe_main(["xctx"])

    assert exc.value.code == 2
    err = capsys.readouterr().err
    assert "Recommended agent loop" in err
    assert "Protocol:" in err
    assert "follow recipe command fields or schema" in err
    assert (
        "stock-universe xctx: error: the following arguments are required: command"
        in err
    )


def test_stock_universe_nested_xctx_tree_routes_to_protocol(capsys) -> None:
    assert stock_universe_main(["xctx", "tree"]) == 0
    payload = _payload(capsys)

    assert payload["ok"] is True
    assert payload["namespace"] == "xctx"
    assert payload["result_type"] == "ToolManifest"
    assert payload["entrypoints"]["source_checkout"] == "./stock_universe.cli xctx"


def test_stock_universe_nested_xctx_tree_pipe_to_head_is_quiet() -> None:
    result = subprocess.run(
        ["bash", "-lc", "set -o pipefail; ./stock_universe.cli xctx tree | head -5"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=20,
    )

    assert result.returncode == 0
    assert result.stdout.startswith("{")
    assert "BrokenPipeError" not in result.stderr


def test_stock_universe_nested_xctx_help_pipe_to_head_is_quiet() -> None:
    result = subprocess.run(
        ["bash", "-lc", "set -o pipefail; ./stock_universe.cli xctx --help | head -5"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=20,
    )

    assert result.returncode == 0
    assert "usage:" in result.stdout
    assert "BrokenPipeError" not in result.stderr


def test_source_checkout_xctx_help_uses_runnable_wrapper_path() -> None:
    result = subprocess.run(
        ["./stock_universe.cli", "xctx", "--help"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        timeout=20,
    )

    assert result.returncode == 0
    assert "1. ./stock_universe.cli xctx doctor" in result.stdout
    assert (
        "8. ./stock_universe.cli backfill --ohlcv-series-id <ohlcv_series_id> --strict"
        in result.stdout
    )


class _InterruptParser:
    def parse_args(self, argv: list[str] | None) -> None:
        raise KeyboardInterrupt


def test_stock_universe_main_keyboard_interrupt_returns_130(
    monkeypatch, capsys
) -> None:
    monkeypatch.setattr(
        stock_universe_cli_module, "_parser", lambda *, prog: _InterruptParser()
    )

    assert stock_universe_cli_module.main(["doctor"]) == 130
    assert "stock-universe: interrupted" in capsys.readouterr().err


def test_xctx_main_keyboard_interrupt_returns_130(monkeypatch, capsys) -> None:
    monkeypatch.setattr(xctx_cli_module, "_parser", lambda *, prog: _InterruptParser())

    assert xctx_cli_module.main(["tree"], prog="stock-universe xctx") == 130
    assert "stock-universe xctx: interrupted" in capsys.readouterr().err


def test_stock_universe_validate_db_initializes_and_validates(
    tmp_path: Path, capsys
) -> None:
    db = tmp_path / "stock_universe.sqlite"

    assert stock_universe_main(["validate-db", "--db", str(db)]) == 0
    payload = _payload(capsys)

    assert payload["ok"] is True
    assert payload["db"] == str(db)
    assert payload["counts"]["ohlcv_series_id_lookup"] == 0
    assert payload["counts"]["ohlcv_series"] == 0
    assert payload["counts"]["reference_universe_snapshots"] == 0
    assert "foreign keys valid" in payload["validation"]["checks"]
    assert "reference snapshots valid" in payload["validation"]["checks"]


def test_sqlite_schema_indexes_execution_status_queries(tmp_path: Path) -> None:
    db = tmp_path / "stock_universe.sqlite"
    SQLiteStockUniverseRepository(db).ensure_schema()

    with sqlite3.connect(db) as conn:
        indexes = {
            row[0]
            for row in conn.execute(
                """
                SELECT name
                FROM sqlite_master
                WHERE type = 'index'
                  AND tbl_name IN ('execution_receipts', 'execution_approvals')
                """
            )
        }

        assert {
            "idx_execution_receipts_series_started_receipt",
            "idx_execution_receipts_started_at",
            "idx_execution_approvals_approved_at",
        } <= indexes

        receipt_series_plan = _query_plan(
            conn,
            """
            SELECT *
            FROM execution_receipts
            WHERE ohlcv_series_id = 123
            ORDER BY started_at_utc DESC, execution_receipt_id DESC
            LIMIT 1
            """,
        )
        receipt_reconciliation_plan = _query_plan(
            conn,
            """
            SELECT *
            FROM execution_receipts
            WHERE started_at_utc >= '2026-05-09T00:00:00+00:00'
            ORDER BY started_at_utc, execution_receipt_id
            """,
        )
        approval_reconciliation_plan = _query_plan(
            conn,
            """
            SELECT *
            FROM execution_approvals
            WHERE approved_at_utc >= '2026-05-09T00:00:00+00:00'
            ORDER BY approved_at_utc, execution_approval_id
            """,
        )

    assert "idx_execution_receipts_series_started_receipt" in receipt_series_plan
    assert "idx_execution_receipts_started_at" in receipt_reconciliation_plan
    assert "idx_execution_approvals_approved_at" in approval_reconciliation_plan


def test_stock_universe_validate_db_emits_progress_to_stderr(
    monkeypatch, tmp_path: Path, capsys
) -> None:
    db = tmp_path / "stock_universe.sqlite"

    class SlowRepository:
        def __init__(self, path: str | Path) -> None:
            self.path = Path(path)

        def ensure_schema(self) -> None:
            return None

        def validate(self) -> ValidationReport:
            time.sleep(1.2)
            return ValidationReport(ok=True, checks=("slow validation complete",))

        def counts(self) -> dict[str, int]:
            return {"ohlcv_series_id_lookup": 0}

    monkeypatch.setattr(
        stock_universe_cli_module, "SQLiteStockUniverseRepository", SlowRepository
    )

    assert (
        stock_universe_main(
            [
                "validate-db",
                "--db",
                str(db),
                "--heartbeat-seconds",
                "1",
                "--summary-seconds",
                "1",
            ]
        )
        == 0
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    progress = [json.loads(line) for line in captured.err.splitlines()]

    assert payload["ok"] is True
    assert [event["event_type"] for event in progress] == [
        "starting",
        "heartbeat",
        "summary",
        "finished",
    ]
    assert progress[0]["message"] == "STARTING validate-db"
    assert progress[0]["polling_interval_seconds"] == 1
    assert progress[0]["user_update_interval_seconds"] == 1
    assert "polling_interval_seconds" not in progress[1]
    assert "user_update_interval_seconds" not in progress[1]


def test_stock_universe_backfill_emits_progress_to_stderr(
    monkeypatch, tmp_path: Path, capsys
) -> None:
    db = tmp_path / "stock_universe.sqlite"

    def fake_execute_one_ticker(ticker, args, api_key, repository):
        time.sleep(1.2)
        return {
            "ticker": ticker,
            "status": "ok",
            "fetched_bar_count": 2,
            "inserted_bar_count": 2,
        }

    monkeypatch.setattr(
        stock_universe_cli_module, "_execute_one_ticker", fake_execute_one_ticker
    )

    assert (
        stock_universe_main(
            [
                "backfill",
                "--db",
                str(db),
                "--ticker",
                "TEST",
                "--api-key",
                "secret",
                "--heartbeat-seconds",
                "1",
                "--summary-seconds",
                "1",
            ]
        )
        == 0
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    progress = [
        json.loads(line.removeprefix("backfill progress: "))
        for line in captured.err.splitlines()
        if line.startswith("backfill progress: ")
    ]
    events = [event["event_type"] for event in progress]

    assert payload["ok"] is True
    assert events[:2] == ["started", "input_started"]
    assert "heartbeat" in events
    assert "summary" in events
    assert events[-1] == "finished"
    assert progress[-1]["counts"]["inserted_bars"] == 2
    assert all(event["command"] == "stock-universe backfill" for event in progress)


def test_sqlite_repository_persists_reference_snapshot_evidence(tmp_path: Path) -> None:
    db = tmp_path / "stock_universe.sqlite"
    repository = SQLiteStockUniverseRepository(db)

    written = repository.upsert_reference_snapshots(
        [
            StoredReferenceSnapshot(
                provider="massive.reference_tickers",
                snapshot_as_of_date="2026-05-07",
                ticker="GOOG",
                ohlcv_series_id=-123,
                active=True,
                company_name="Alphabet Inc. Class C Capital Stock",
                cik="0001652044",
                composite_figi="BBG009S3NB30",
                share_class_figi="BBG009S3NB21",
                security_type="CS",
                primary_exchange="XNAS",
                market="stocks",
                locale="us",
                identity_status="permanent",
                natural_key="massive:composite_figi:BBG009S3NB30",
                raw={"ticker": "GOOG"},
                source_request={"exchange": "XNAS"},
            )
        ]
    )

    assert written == 1
    counts = repository.counts()
    assert counts["ohlcv_series_id_lookup"] == 1
    assert counts["ohlcv_series"] == 1
    assert counts["ticker_aliases"] == 1
    assert counts["reference_universe_snapshots"] == 1
    with sqlite3.connect(db) as conn:
        row = conn.execute(
            """
            SELECT os.latest_ticker,
                   json_extract(os.target_json, '$.latest_primary_exchange') AS primary_exchange,
                   json_extract(os.target_json, '$.natural_key') AS natural_key,
                   ta.ticker AS alias_ticker,
                   ta.source AS alias_source
            FROM ohlcv_series os
            JOIN ticker_aliases ta ON ta.ohlcv_series_id = os.ohlcv_series_id
            """
        ).fetchone()
    assert row == ("GOOG", "XNAS", None, "GOOG", "massive.reference_tickers")
    validation = repository.validate()
    assert validation.ok is True
    assert "reference snapshots unique" in validation.checks
    assert "natural_key stored only in series lookup" in validation.checks


def test_v5_schema_centralizes_natural_keys_and_reference_commits_are_idempotent(
    tmp_path: Path,
) -> None:
    db = tmp_path / "stock_universe.sqlite"
    repository = SQLiteStockUniverseRepository(db)
    snapshot = StoredReferenceSnapshot(
        provider="massive.reference_tickers",
        snapshot_as_of_date="2026-05-07",
        ticker="GOOG",
        active=True,
        company_name="Alphabet Inc. Class C Capital Stock",
        cik="0001652044",
        composite_figi="BBG009S3NB30",
        share_class_figi="BBG009S3NB21",
        security_type="CS",
        primary_exchange="XNAS",
        market="stocks",
        locale="us",
        identity_status="permanent",
        natural_key="massive:composite_figi:BBG009S3NB30",
        raw={"ticker": "GOOG", "name": "Alphabet Inc. Class C Capital Stock"},
        source_request={"exchange": "XNAS"},
    )

    assert repository.upsert_reference_snapshots([snapshot]) == 1
    first_id = repository.lookup_ohlcv_series_id(snapshot.natural_key)
    assert repository.upsert_reference_snapshots([snapshot]) == 1

    assert repository.lookup_ohlcv_series_id(snapshot.natural_key) == first_id
    assert repository.counts()["ohlcv_series_id_lookup"] == 1
    assert repository.counts()["reference_universe_snapshots"] == 1
    validation = repository.validate()
    assert validation.ok is True
    assert "persisted JSON has no natural_key keys" in validation.checks
    assert "all child rows reference series lookup" in validation.checks

    with sqlite3.connect(db) as conn:
        natural_key_columns = [
            (table, row[1])
            for (table,) in conn.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
            for row in conn.execute(f"PRAGMA table_info({table})")
            if row[1] == "natural_key"
        ]
        assert natural_key_columns == [("ohlcv_series_id_lookup", "natural_key")]
        reference_fks = conn.execute(
            "PRAGMA foreign_key_list(reference_universe_snapshots)"
        ).fetchall()
        assert any(row[2] == "ohlcv_series_id_lookup" for row in reference_fks)


def test_reference_snapshot_refresh_updates_series_target_json(tmp_path: Path) -> None:
    db = tmp_path / "stock_universe.sqlite"
    repository = SQLiteStockUniverseRepository(db)
    snapshot = StoredReferenceSnapshot(
        provider="massive.reference_tickers",
        snapshot_as_of_date="2026-05-07",
        ticker="ABCD",
        active=True,
        company_name="Old Company Name",
        cik="0000000001",
        composite_figi="BBGOLD",
        share_class_figi="BBGSHARE",
        security_type="CS",
        primary_exchange="XNAS",
        market="stocks",
        locale="us",
        identity_status="permanent",
        natural_key="massive:composite_figi:BBGOLD",
        raw={"ticker": "ABCD", "name": "Old Company Name"},
    )

    repository.upsert_reference_snapshots([snapshot])
    repository.upsert_reference_snapshots(
        [
            replace(
                snapshot,
                company_name="New Company Name",
                primary_exchange="XNYS",
                raw={"ticker": "ABCD", "name": "New Company Name"},
            )
        ]
    )

    with sqlite3.connect(db) as conn:
        row = conn.execute(
            """
            SELECT company_name,
                   json_extract(target_json, '$.company_name') AS target_company_name,
                   json_extract(target_json, '$.latest_primary_exchange') AS target_exchange
            FROM ohlcv_series
            """
        ).fetchone()

    assert row == ("New Company Name", "New Company Name", "XNYS")


def test_persisted_series_target_json_omits_natural_key(tmp_path: Path) -> None:
    db = tmp_path / "stock_universe.sqlite"
    repository = SQLiteStockUniverseRepository(db)
    plan = _plan_with_allocated_lookup(repository, _sample_plan())

    repository.persist_plan_context(plan)

    with sqlite3.connect(db) as conn:
        assert (
            conn.execute(
                "SELECT json_extract(target_json, '$.natural_key') FROM ohlcv_series"
            ).fetchone()[0]
            is None
        )
        assert (
            conn.execute(
                "SELECT json_extract(plan_json, '$.target.natural_key') FROM backfill_plans"
            ).fetchone()[0]
            is None
        )
    validation = repository.validate()
    assert validation.ok is True
    assert "persisted JSON has no natural_key keys" in validation.checks


def test_stock_universe_audit_executions_handles_empty_db(
    tmp_path: Path, capsys
) -> None:
    db = tmp_path / "stock_universe.sqlite"

    assert stock_universe_main(["audit-executions", "--db", str(db)]) == 0
    payload = _payload(capsys)

    assert payload["ok"] is True
    assert payload["count"] == 0
    assert payload["executions"] == []


def test_stock_universe_doctor_checks_db_parent(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    db = tmp_path / "doctor.sqlite"
    monkeypatch.setattr(
        "stock_universe.cli.shutil.which",
        lambda name: (_ for _ in ()).throw(
            AssertionError("default doctor must not inspect installed entrypoints")
        ),
    )

    assert stock_universe_main(["doctor", "--db", str(db), "--api-key", "secret"]) == 0
    payload = _payload(capsys)

    assert payload["ok"] is True
    assert payload["checks"]["massive_api_key_present"] is True
    assert "stock_universe_entrypoint_present" not in payload["checks"]
    assert payload["checks"]["db_exists"] is False
    assert payload["checks"]["db_parent_writable"] is True


def test_stock_universe_doctor_reports_existing_db_schema(
    tmp_path: Path, capsys
) -> None:
    db = tmp_path / "doctor.sqlite"
    SQLiteStockUniverseRepository(db).ensure_schema()

    assert stock_universe_main(["doctor", "--db", str(db), "--api-key", "secret"]) == 0
    payload = _payload(capsys)

    assert payload["ok"] is True
    assert payload["checks"]["db_exists"] is True
    assert payload["checks"]["db_schema_current"] is True
    assert payload["checks"]["db_required_tables_present"] is True
    assert payload["checks"]["db_missing_tables"] == []


def test_stock_universe_doctor_can_require_installed_entrypoint(
    monkeypatch, capsys
) -> None:
    monkeypatch.setattr("stock_universe.cli.shutil.which", lambda name: None)

    assert (
        stock_universe_main(["doctor", "--api-key", "secret", "--require-entrypoint"])
        == 1
    )
    payload = _payload(capsys)

    assert payload["ok"] is False
    assert payload["checks"]["stock_universe_entrypoint_present"] is False


def _plan_with_allocated_lookup(repository: SQLiteStockUniverseRepository, plan):
    series_id = repository.ensure_ohlcv_series_id(plan.target.natural_key)
    target = replace(plan.target, ohlcv_series_id=series_id)
    request = BackfillRequest(
        series_id=series_id,
        from_date=plan.request.from_date,
        to_date=plan.request.to_date,
        multiplier=plan.request.multiplier,
        timespan=plan.request.timespan,
        adjusted=plan.request.adjusted,
    )
    return replace(plan, target=target, request=request)


def _sample_plan() -> BackfillPlan:
    target = TargetIdentity(
        ohlcv_series_id=0,
        company_name="Sound Financial Bancorp Inc.",
        cik="0001495925",
        composite_figi="BBG000SFBC01",
        share_class_figi="BBG000SFBC02",
        identity_status="permanent",
        latest_ticker="SFBC",
        latest_primary_exchange="XNAS",
        locale="us",
        market="stocks",
        natural_key="massive:composite_figi:BBG000SFBC01",
        security_type="CS",
    )
    request = BackfillRequest(
        series_id=0,
        from_date="2021-01-04",
        to_date="2021-01-05",
        multiplier=1,
        timespan="day",
        adjusted=True,
    )
    return BackfillPlan(
        request=request,
        status="safe",
        target=target,
        segments=(
            PlannedSegment(
                segment_index=1,
                ticker="SFBC",
                from_date="2021-01-04",
                to_date="2021-01-05",
                source="unit-test",
            ),
        ),
        decisions=(
            RuleDecision(
                rule_name="unit.safe",
                outcome="pass",
                segment_id="segment:1",
                reason="unit test plan is executable",
            ),
        ),
        evidence_ledger_hash="unit-ledger",
        planner_version="unit-test",
        created_at_utc="2026-05-12T00:00:00Z",
    )
