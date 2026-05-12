from __future__ import annotations

import datetime as dt
import json
from urllib.parse import parse_qs, urlparse

import pytest

from stock_universe.cli import main as stock_universe_main
from stock_universe.providers import (
    HttpJsonResponse,
    MassiveProviderConfig,
    MassiveReadOnlyClient,
)
from stock_universe.domain import BackfillRequest, TargetIdentity
from stock_universe.domain import EvidenceNeeded
from stock_universe.storage import SQLiteStockUniverseRepository
from stock_universe.universe_status import universe_status
from stock_universe.workflows import (
    DryRunPlanningTrace,
    PlanningRound,
    ReferenceUniverseRequest,
    fetch_massive_reference_universe,
    massive_live_source_from_series_id,
    reference_snapshot_seed_base_facts,
    sqlite_identity_search,
)
from stock_universe.xctx.cli import main as xctx_main


class ReferenceUniverseTransport:
    def __init__(self) -> None:
        self.urls: list[str] = []

    def get_json(self, url: str, *, timeout_seconds: float) -> HttpJsonResponse:
        self.urls.append(url)
        parsed = urlparse(url)
        query = parse_qs(parsed.query)
        cursor = query.get("cursor", [""])[0]
        if cursor == "page-2":
            return HttpJsonResponse(
                200,
                {
                    "status": "OK",
                    "results": [
                        _reference_row(
                            "GOOGL",
                            "Alphabet Inc. Class A Common Stock",
                            "BBG009S39JX6",
                            "BBG009S39JY5",
                        ),
                        _reference_row(
                            "GOOX",
                            "T-Rex 2X Long Alphabet Daily Target ETF",
                            "BBG01KYKF9W1",
                            "BBG01KYKFBR2",
                            security_type="ETF",
                        ),
                    ],
                },
            )
        return HttpJsonResponse(
            200,
            {
                "status": "OK",
                "next_url": "https://api.massive.com/v3/reference/tickers?cursor=page-2",
                "results": [
                    _reference_row(
                        "GOOG",
                        "Alphabet Inc. Class C Capital Stock",
                        "BBG009S3NB30",
                        "BBG009S3NB21",
                    ),
                ],
            },
        )


class FakeReferenceUniverseClient:
    def __init__(self, config: MassiveProviderConfig, raw_capture_dir=None) -> None:
        self.inner = MassiveReadOnlyClient(
            config, ReferenceUniverseTransport(), raw_capture_dir=raw_capture_dir
        )

    @property
    def request_log(self):
        return self.inner.request_log

    def get(self, endpoint: str, params: dict[str, str] | None = None) -> dict:
        return self.inner.get(endpoint, params)


def test_reference_universe_default_as_of_date_uses_last_market_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FrozenDateTime(dt.datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 1, 1, tzinfo=tz)

    monkeypatch.setattr(
        "stock_universe.workflows.reference_universe.dt.datetime", FrozenDateTime
    )

    assert ReferenceUniverseRequest().as_of_date == "2025-12-31"


def test_reference_universe_fetch_returns_bounded_rehearsal_with_pending_cursor() -> (
    None
):
    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"),
        ReferenceUniverseTransport(),
    )

    update = fetch_massive_reference_universe(
        client,
        ReferenceUniverseRequest(
            exchange="XNAS", as_of_date="2026-05-07", limit=1, max_pages=1
        ),
    )

    assert update.complete is False
    assert update.page_count == 1
    assert len(update.snapshots) == 1
    assert update.snapshots[0].ticker == "GOOG"
    assert update.snapshots[0].natural_key == "massive:composite_figi:BBG009S3NB30"
    assert update.pending_requests[0]["params"] == {"cursor": "page-2"}
    assert client.request_log[0].params_without_api_key == (
        ("active", "true"),
        ("date", "2026-05-07"),
        ("exchange", "XNAS"),
        ("limit", "1"),
        ("market", "stocks"),
        ("order", "asc"),
        ("sort", "ticker"),
    )


def test_reference_universe_fetch_follows_cursor_within_max_pages() -> None:
    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"),
        ReferenceUniverseTransport(),
    )

    update = fetch_massive_reference_universe(
        client,
        ReferenceUniverseRequest(
            exchange="XNAS", as_of_date="2026-05-07", limit=1, max_pages=2
        ),
    )

    assert update.complete is True
    assert update.page_count == 2
    assert [snapshot.ticker for snapshot in update.snapshots] == [
        "GOOG",
        "GOOGL",
        "GOOX",
    ]
    assert client.request_log[1].params_without_api_key == (("cursor", "page-2"),)


def test_reference_universe_cli_dry_run_does_not_create_db(
    monkeypatch, tmp_path, capsys
) -> None:
    monkeypatch.setattr(
        "stock_universe.cli.MassiveReadOnlyClient", FakeReferenceUniverseClient
    )
    db = tmp_path / "stock_universe.sqlite"

    assert (
        stock_universe_main(
            [
                "update-reference-universe",
                "--db",
                str(db),
                "--api-key",
                "secret",
                "--base-url",
                "https://example.test",
                "--exchange",
                "XNAS",
                "--as-of-date",
                "2026-05-07",
                "--limit",
                "1",
                "--max-pages",
                "1",
            ]
        )
        == 0
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    progress = [
        json.loads(line.removeprefix("update-reference-universe progress: "))
        for line in captured.err.splitlines()
        if line.startswith("update-reference-universe progress: ")
    ]

    assert payload["ok"] is True
    assert payload["dry_run"] is True
    assert payload["complete"] is False
    assert payload["fetched_count"] == 1
    assert payload["effects"]["will_write"] == []
    assert db.exists() is False
    assert [event["event_type"] for event in progress] == [
        "started",
        "page_fetched",
        "finished",
    ]
    assert progress[1]["page_count"] == 1


def test_reference_universe_cli_commit_feeds_db_identity_search(
    monkeypatch, tmp_path, capsys
) -> None:
    monkeypatch.setattr(
        "stock_universe.cli.MassiveReadOnlyClient", FakeReferenceUniverseClient
    )
    db = tmp_path / "stock_universe.sqlite"

    assert (
        stock_universe_main(
            [
                "update-reference-universe",
                "--db",
                str(db),
                "--api-key",
                "secret",
                "--base-url",
                "https://example.test",
                "--exchange",
                "XNAS",
                "--as-of-date",
                "2026-05-07",
                "--limit",
                "1",
                "--max-pages",
                "2",
                "--commit",
            ]
        )
        == 0
    )
    update_payload = json.loads(capsys.readouterr().out)

    assert update_payload["ok"] is True
    assert update_payload["dry_run"] is False
    assert update_payload["complete"] is True
    assert update_payload["upserted_count"] == 3
    assert update_payload["counts"]["reference_universe_snapshots"] == 3
    assert update_payload["counts"]["reference_universe_updates"] == 1
    assert update_payload["reference_update"]["complete"] is True
    assert update_payload["reference_update"]["fetched_count"] == 3

    status = universe_status(db)
    assert status["schema_current"] is True
    assert status["reference_universe"]["row_count"] == 3
    assert status["reference_universe"]["latest_update"]["fetched_count"] == 3

    result = sqlite_identity_search(db, "Alphabet")
    assert [candidate.ticker for candidate in result.candidates] == [
        "GOOG",
        "GOOGL",
        "GOOX",
    ]
    assert {candidate.source for candidate in result.candidates} == {
        "sqlite.massive.reference_tickers"
    }
    assert result.related_searches[0]["query"] == "0001652044"

    assert (
        stock_universe_main(
            ["identity-search", "Alphabet", "--source", "db", "--db", str(db)]
        )
        == 0
    )
    search_payload = json.loads(capsys.readouterr().out)
    assert [candidate["ticker"] for candidate in search_payload["candidates"]] == [
        "GOOG",
        "GOOGL",
        "GOOX",
    ]
    assert search_payload["related_searches"][0]["query"] == "0001652044"


def test_series_id_seed_facts_preserve_selected_reference_identity(
    monkeypatch, tmp_path, capsys
) -> None:
    monkeypatch.setattr(
        "stock_universe.cli.MassiveReadOnlyClient", FakeReferenceUniverseClient
    )
    db = tmp_path / "stock_universe.sqlite"
    assert (
        stock_universe_main(
            [
                "update-reference-universe",
                "--db",
                str(db),
                "--api-key",
                "secret",
                "--base-url",
                "https://example.test",
                "--exchange",
                "XNAS",
                "--as-of-date",
                "2026-05-07",
                "--limit",
                "1",
                "--max-pages",
                "2",
                "--commit",
            ]
        )
        == 0
    )
    capsys.readouterr()
    series_id = sqlite_identity_search(db, "GOOG").candidates[0].ohlcv_series_id
    snapshot = SQLiteStockUniverseRepository(db).reference_snapshot_for_series_id(
        series_id
    )

    facts = reference_snapshot_seed_base_facts(
        snapshot, from_date="2024-01-01", to_date="2024-01-31"
    )
    by_kind = {fact.kind: fact.payload_value() for fact in facts}
    target = TargetIdentity.from_legacy_dict(by_kind["target_identity"])
    request = BackfillRequest.from_legacy_dict(
        target.ohlcv_series_id, by_kind["backfill_request"]
    )

    assert target.ohlcv_series_id == snapshot.ohlcv_series_id
    assert target.latest_ticker == "GOOG"
    assert target.natural_key == "massive:composite_figi:BBG009S3NB30"
    assert target.composite_figi == "BBG009S3NB30"
    assert request.from_date.isoformat() == "2024-01-01"
    assert by_kind["known_aliases"][0]["symbol_text"] == "GOOG"
    assert (
        by_kind["plan_metadata"]["identity_discovery"]["seed"]
        == "reference_universe_series_id"
    )


def test_series_id_live_source_uses_db_snapshot_without_reference_lookup(
    monkeypatch, tmp_path, capsys
) -> None:
    monkeypatch.setattr(
        "stock_universe.cli.MassiveReadOnlyClient", FakeReferenceUniverseClient
    )
    db = tmp_path / "stock_universe.sqlite"
    assert (
        stock_universe_main(
            [
                "update-reference-universe",
                "--db",
                str(db),
                "--api-key",
                "secret",
                "--base-url",
                "https://example.test",
                "--exchange",
                "XNAS",
                "--as-of-date",
                "2026-05-07",
                "--limit",
                "1",
                "--max-pages",
                "2",
                "--commit",
            ]
        )
        == 0
    )
    capsys.readouterr()
    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"),
        ReferenceUniverseTransport(),
    )

    series_id = sqlite_identity_search(db, "GOOG").candidates[0].ohlcv_series_id
    source, returned_client, snapshot = massive_live_source_from_series_id(
        db,
        series_id,
        client=client,
        from_date="2024-01-01",
        to_date="2024-01-31",
    )

    assert returned_client is client
    assert snapshot.ticker == "GOOG"
    assert client.request_log == []
    initial_kinds = {fact.kind for fact in source.initial_facts()}
    assert {
        "target_identity",
        "backfill_request",
        "known_aliases",
        "plan_metadata",
    } <= initial_kinds


def test_series_id_lookup_does_not_initialize_missing_db(tmp_path) -> None:
    db = tmp_path / "missing.sqlite"

    with pytest.raises(
        ValueError, match="ohlcv_series_id not found in reference universe: 123"
    ):
        massive_live_source_from_series_id(
            db, 123, api_key="secret", base_url="https://example.test"
        )

    assert db.exists() is False


def test_stock_universe_dry_run_series_id_loads_selected_db_identity(
    monkeypatch, tmp_path, capsys
) -> None:
    db = _committed_reference_db(monkeypatch, tmp_path, capsys)

    def fake_trace(source, *, max_rounds):
        target_fact = next(
            fact for fact in source.initial_facts() if fact.kind == "target_identity"
        )
        target = TargetIdentity.from_legacy_dict(target_fact.payload_value())
        assert target.latest_ticker == "GOOG"
        result = EvidenceNeeded(requests=())
        return DryRunPlanningTrace(
            result=result, rounds=(PlanningRound(1, "ledger", result),)
        )

    monkeypatch.setattr(
        "stock_universe.cli.run_backfill_source_dry_run_trace", fake_trace
    )
    series_id = sqlite_identity_search(db, "GOOG").candidates[0].ohlcv_series_id

    assert (
        stock_universe_main(
            [
                "dry-run",
                "--ohlcv-series-id",
                str(series_id),
                "--db",
                str(db),
                "--api-key",
                "secret",
                "--base-url",
                "https://example.test",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)

    assert payload["command"] == "stock-universe dry-run:live-ohlcv-series-id"
    assert payload["selected_identity"]["ohlcv_series_id"] == series_id
    assert payload["selected_identity"]["ticker"] == "GOOG"
    assert payload["request_log"] == []


def test_xctx_dry_run_series_id_loads_selected_db_identity(
    monkeypatch, tmp_path, capsys
) -> None:
    db = _committed_reference_db(monkeypatch, tmp_path, capsys)

    def fake_trace(source, *, max_rounds):
        target_fact = next(
            fact for fact in source.initial_facts() if fact.kind == "target_identity"
        )
        target = TargetIdentity.from_legacy_dict(target_fact.payload_value())
        assert target.latest_ticker == "GOOG"
        result = EvidenceNeeded(requests=())
        return DryRunPlanningTrace(
            result=result, rounds=(PlanningRound(1, "ledger", result),)
        )

    monkeypatch.setattr(
        "stock_universe.xctx.cli.run_backfill_source_dry_run_trace", fake_trace
    )
    series_id = sqlite_identity_search(db, "GOOG").candidates[0].ohlcv_series_id

    assert (
        xctx_main(
            [
                "dry-run",
                "--ohlcv-series-id",
                str(series_id),
                "--db",
                str(db),
                "--api-key",
                "secret",
                "--base-url",
                "https://example.test",
            ]
        )
        == 0
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert payload["ok"] is False
    assert payload["command"] == "xctx dry-run"
    assert payload["selected_identity"]["ohlcv_series_id"] == series_id
    assert payload["selected_identity"]["ticker"] == "GOOG"
    assert payload["request_log"] == []


def test_stock_universe_backfill_series_id_can_skip_after_rehearsal(
    monkeypatch, tmp_path, capsys
) -> None:
    db = _committed_reference_db(monkeypatch, tmp_path, capsys)

    def fake_trace(source, *, max_rounds):
        result = EvidenceNeeded(requests=())
        return DryRunPlanningTrace(
            result=result, rounds=(PlanningRound(1, "ledger", result),)
        )

    monkeypatch.setattr(
        "stock_universe.cli.run_backfill_source_dry_run_trace", fake_trace
    )
    series_id = sqlite_identity_search(db, "GOOG").candidates[0].ohlcv_series_id

    assert (
        stock_universe_main(
            [
                "backfill",
                "--db",
                str(db),
                "--ohlcv-series-id",
                str(series_id),
                "--api-key",
                "secret",
                "--base-url",
                "https://example.test",
                "--strict",
            ]
        )
        == 1
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert payload["ok"] is False
    assert payload["results"][0]["ohlcv_series_id"] == series_id
    assert payload["results"][0]["status"] == "skipped"
    assert payload["results"][0]["selected_identity"]["ticker"] == "GOOG"


def test_reference_batch_dry_run_enumerates_db_series_ids_without_api_key(
    monkeypatch, tmp_path, capsys
) -> None:
    db = _committed_reference_db(monkeypatch, tmp_path, capsys)

    assert (
        stock_universe_main(
            [
                "backfill-reference-batch",
                "--db",
                str(db),
                "--exchange",
                "XNAS",
                "--limit",
                "1",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)

    assert payload["ok"] is True
    assert payload["dry_run"] is True
    assert payload["counts"]["total_available"] == 3
    assert payload["counts"]["selected"] == 1
    assert payload["counts"]["pending"] == 2
    assert payload["selected_ohlcv_series_ids"] == [
        payload["selected_snapshots"][0]["ohlcv_series_id"]
    ]
    assert payload["selected_snapshots"][0]["ticker"] == "GOOG"
    assert payload["pending_items"][0]["ticker"] == "GOOGL"
    assert payload["effects"]["will_write"] == []
    assert payload["next_action"] == "commit_selected_batch"
    commit_action = next(
        action
        for action in payload["next_actions"]
        if action["name"] == "commit-selected-reference-batch"
    )
    assert commit_action["requires_approval"] is True
    assert "ticker" not in commit_action["command"]["args"]


def test_reference_batch_can_filter_by_common_stock_alias(
    monkeypatch, tmp_path, capsys
) -> None:
    db = _committed_reference_db(monkeypatch, tmp_path, capsys)

    assert (
        stock_universe_main(
            [
                "backfill-reference-batch",
                "--db",
                str(db),
                "--exchange",
                "XNAS",
                "--common-stock",
                "--limit",
                "2",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)

    assert payload["ok"] is True
    assert payload["selection"]["security_types"] == ["CS"]
    assert payload["counts"]["total_available"] == 2
    assert [item["ticker"] for item in payload["selected_snapshots"]] == [
        "GOOG",
        "GOOGL",
    ]
    assert {item["security_type"] for item in payload["selected_snapshots"]} == {"CS"}
    assert payload["counts"]["pending"] == 0
    assert payload["next_actions"][0]["command"]["args"]["security_type"] == ["CS"]


def test_reference_batch_can_filter_by_security_type(
    monkeypatch, tmp_path, capsys
) -> None:
    db = _committed_reference_db(monkeypatch, tmp_path, capsys)

    assert (
        stock_universe_main(
            [
                "backfill-reference-batch",
                "--db",
                str(db),
                "--exchange",
                "XNAS",
                "--security-type",
                "ETF",
                "--limit",
                "1",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)

    assert payload["selection"]["security_types"] == ["ETF"]
    assert payload["counts"]["total_available"] == 1
    assert payload["selected_snapshots"][0]["ticker"] == "GOOX"
    assert payload["selected_snapshots"][0]["security_type"] == "ETF"


def test_reference_batch_commit_runs_selected_ohlcv_series_ids(
    monkeypatch, tmp_path, capsys
) -> None:
    db = _committed_reference_db(monkeypatch, tmp_path, capsys)

    def fake_trace(source, *, max_rounds):
        result = EvidenceNeeded(requests=())
        return DryRunPlanningTrace(
            result=result, rounds=(PlanningRound(1, "ledger", result),)
        )

    monkeypatch.setattr(
        "stock_universe.cli.run_backfill_source_dry_run_trace", fake_trace
    )

    assert (
        stock_universe_main(
            [
                "backfill-reference-batch",
                "--db",
                str(db),
                "--exchange",
                "XNAS",
                "--limit",
                "1",
                "--commit",
                "--strict",
                "--api-key",
                "secret",
                "--base-url",
                "https://example.test",
            ]
        )
        == 1
    )
    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    progress = [
        json.loads(line.removeprefix("backfill-reference-batch progress: "))
        for line in captured.err.splitlines()
        if line.startswith("backfill-reference-batch progress: ")
    ]

    assert payload["ok"] is False
    assert payload["dry_run"] is False
    assert payload["counts"]["selected"] == 1
    assert payload["counts"]["skipped"] == 1
    assert (
        payload["results"][0]["ohlcv_series_id"]
        == payload["selected_ohlcv_series_ids"][0]
    )
    assert payload["results"][0]["status"] == "skipped"
    assert payload["next_action"] == "repair_failures"
    assert (
        payload["repair_hints"][0]["command"]["args"]["ohlcv_series_id"]
        == payload["selected_ohlcv_series_ids"][0]
    )
    assert [event["event_type"] for event in progress] == [
        "started",
        "input_started",
        "input_finished",
        "summary",
        "finished",
    ]
    assert progress[-1]["counts"]["skipped"] == 1


def test_reference_batch_empty_db_exposes_reference_update_repair(
    tmp_path, capsys
) -> None:
    db = tmp_path / "stock_universe.sqlite"
    SQLiteStockUniverseRepository(db).ensure_schema()

    assert (
        stock_universe_main(
            [
                "backfill-reference-batch",
                "--db",
                str(db),
                "--exchange",
                "XNAS",
                "--limit",
                "1",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)

    assert payload["ok"] is False
    assert payload["counts"]["selected"] == 0
    assert payload["next_action"] == "update_reference_universe"
    assert payload["repair_hints"][0]["name"] == "update-reference-universe"
    assert (
        payload["repair_hints"][0]["command"]["name"]
        == "stock-universe update-reference-universe"
    )


def _committed_reference_db(monkeypatch, tmp_path, capsys):
    monkeypatch.setattr(
        "stock_universe.cli.MassiveReadOnlyClient", FakeReferenceUniverseClient
    )
    db = tmp_path / "stock_universe.sqlite"
    assert (
        stock_universe_main(
            [
                "update-reference-universe",
                "--db",
                str(db),
                "--api-key",
                "secret",
                "--base-url",
                "https://example.test",
                "--exchange",
                "XNAS",
                "--as-of-date",
                "2026-05-07",
                "--limit",
                "1",
                "--max-pages",
                "2",
                "--commit",
            ]
        )
        == 0
    )
    capsys.readouterr()
    return db


def _reference_row(
    ticker: str,
    name: str,
    composite_figi: str,
    share_class_figi: str,
    *,
    security_type: str = "CS",
) -> dict:
    return {
        "active": True,
        "cik": "0001652044",
        "composite_figi": composite_figi,
        "locale": "us",
        "market": "stocks",
        "name": name,
        "primary_exchange": "XNAS",
        "share_class_figi": share_class_figi,
        "ticker": ticker,
        "type": security_type,
    }
