from __future__ import annotations

import json
from pathlib import Path
from urllib.parse import parse_qs, unquote, urlparse

from stock_universe.cli import main as stock_universe_main
from stock_universe.providers import (
    HttpJsonResponse,
    MassiveProviderConfig,
    MassiveReadOnlyClient,
)
from stock_universe.storage import (
    SQLiteStockUniverseRepository,
    StoredReferenceSnapshot,
)
from stock_universe.workflows import live_identity_search, sqlite_identity_search
from stock_universe.xctx.cli import main as xctx_main


class FakeIdentitySearchTransport:
    def __init__(self, payload: dict) -> None:
        self.payload = payload
        self.urls: list[str] = []

    def get_json(self, url: str, *, timeout_seconds: float) -> HttpJsonResponse:
        self.urls.append(url)
        return HttpJsonResponse(200, self.payload)


class RoutingIdentitySearchTransport:
    def __init__(self, rows: list[dict]) -> None:
        self.rows = rows
        self.urls: list[str] = []

    def get_json(self, url: str, *, timeout_seconds: float) -> HttpJsonResponse:
        self.urls.append(url)
        parsed = urlparse(url)
        query = {key: values[0] for key, values in parse_qs(parsed.query).items()}
        rows = self.rows
        if parsed.path.startswith("/v3/reference/tickers/"):
            ticker = unquote(parsed.path.rsplit("/", 1)[-1]).upper()
            rows = [
                row
                for row in self.rows
                if str(row.get("ticker") or "").upper() == ticker
            ]
        elif query.get("cik"):
            cik = query["cik"].zfill(10)
            rows = [
                row for row in self.rows if str(row.get("cik") or "").zfill(10) == cik
            ]
        elif query.get("ticker.gte"):
            lower = query["ticker.gte"]
            upper = query.get("ticker.lt", "")
            rows = [
                row
                for row in self.rows
                if str(row.get("ticker") or "").upper() >= lower
                and (not upper or str(row.get("ticker") or "").upper() < upper)
            ]
        elif query.get("search"):
            term = query["search"].lower()
            rows = [
                row for row in self.rows if term in str(row.get("name") or "").lower()
            ]
        return HttpJsonResponse(200, _reference_list_payload(*rows))


class FakeIdentitySearchClient:
    def __init__(self, config: MassiveProviderConfig, raw_capture_dir=None) -> None:
        self.inner = MassiveReadOnlyClient(
            config,
            FakeIdentitySearchTransport(_alphabet_payload()),
            raw_capture_dir=raw_capture_dir,
        )

    @property
    def request_log(self):
        return self.inner.request_log

    def get(self, endpoint: str, params: dict[str, str] | None = None) -> dict:
        return self.inner.get(endpoint, params)


class RoutingIdentitySearchClient:
    def __init__(self, config: MassiveProviderConfig, raw_capture_dir=None) -> None:
        self.inner = MassiveReadOnlyClient(
            config,
            RoutingIdentitySearchTransport(_alphabet_rows()),
            raw_capture_dir=raw_capture_dir,
        )

    @property
    def request_log(self):
        return self.inner.request_log

    def get(self, endpoint: str, params: dict[str, str] | None = None) -> dict:
        return self.inner.get(endpoint, params)


def test_live_identity_search_cik_returns_share_class_candidates() -> None:
    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"),
        FakeIdentitySearchTransport(
            _reference_list_payload(
                _reference_row(
                    "GOOG",
                    "Alphabet Inc. Class C Capital Stock",
                    "0001652044",
                    "BBG009S3NB30",
                    "BBG009S3NB21",
                ),
                _reference_row(
                    "GOOGL",
                    "Alphabet Inc. Class A Common Stock",
                    "0001652044",
                    "BBG009S39JX6",
                    "BBG009S39JY5",
                ),
            )
        ),
    )

    result = live_identity_search("0001652044", client=client, as_of_date="2026-05-05")
    candidates = [candidate.to_dict() for candidate in result.candidates]

    assert [candidate["ticker"] for candidate in candidates] == ["GOOG", "GOOGL"]
    assert {candidate["match_reason"] for candidate in candidates} == {"cik_exact"}
    assert candidates[0]["ohlcv_series_id"] is None
    assert candidates[1]["ohlcv_series_id"] is None
    assert candidates[0]["natural_key"] != candidates[1]["natural_key"]
    assert {candidate["lookup_status"] for candidate in candidates} == {"not_looked_up"}
    assert {candidate["identity_status"] for candidate in candidates} == {"permanent"}
    assert client.request_log[0].params_without_api_key == (
        ("active", "true"),
        ("cik", "0001652044"),
        ("date", "2026-05-05"),
        ("limit", "25"),
    )


def test_live_identity_search_company_word_keeps_etf_candidate_visible() -> None:
    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"),
        FakeIdentitySearchTransport(_alphabet_payload()),
    )

    result = live_identity_search("Alphabet", client=client, as_of_date="2026-05-05")
    candidates = [candidate.to_dict() for candidate in result.candidates]

    assert [candidate["ticker"] for candidate in candidates] == [
        "GOOG",
        "GOOGL",
        "GOOX",
    ]
    assert [candidate["match_reason"] for candidate in candidates] == [
        "issuer_cik_enrichment",
        "issuer_cik_enrichment",
        "company_name_word",
    ]
    assert candidates[2]["company_name"] == "T-Rex 2X Long Alphabet Daily Target ETF"
    assert result.to_dict()["related_searches"][0]["query"] == "0001652044"
    assert client.request_log[0].params_without_api_key == (
        ("active", "true"),
        ("date", "2026-05-05"),
        ("limit", "25"),
        ("search", "Alphabet"),
    )
    assert ("cik", "0001652044") in client.request_log[-1].params_without_api_key


def test_live_identity_search_exact_ticker_enriches_same_cik_share_classes() -> None:
    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"),
        RoutingIdentitySearchTransport(_alphabet_rows()),
    )

    result = live_identity_search("GOOGL", client=client, as_of_date="2026-05-05")
    payload = result.to_dict()

    assert [candidate["ticker"] for candidate in payload["candidates"]] == [
        "GOOGL",
        "GOOG",
    ]
    assert [candidate["match_reason"] for candidate in payload["candidates"]] == [
        "ticker_exact_case",
        "issuer_cik_enrichment",
    ]
    assert payload["related_searches"] == [
        {
            "kind": "issuer_cik_enrichment",
            "source": "massive.reference_tickers",
            "query": "0001652044",
            "seed_ticker": "GOOGL",
            "seed_company_name": "Alphabet Inc. Class A Common Stock",
            "seed_match_reason": "ticker_exact_case",
            "returned_count": 2,
            "reason": "A strong operating-company candidate had a CIK, so the resolver searched the issuer CIK to reveal related listed share classes.",
        }
    ]
    assert [item.endpoint for item in client.request_log] == [
        "/v3/reference/tickers/GOOGL",
        "/v3/reference/tickers",
        "/v3/reference/tickers",
    ]
    assert ("cik", "0001652044") in client.request_log[-1].params_without_api_key


def test_live_identity_search_ticker_exact_precedes_prefix_matches() -> None:
    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"),
        FakeIdentitySearchTransport(
            _reference_list_payload(
                _reference_row(
                    "MUA",
                    "Blackrock Muni Assets Fund, Inc.",
                    "0000901243",
                    "BBG000BHYBF1",
                    "BBG001S7DTL0",
                    security_type="FUND",
                ),
                _reference_row(
                    "MU",
                    "Micron Technology, Inc.",
                    "0000723125",
                    "BBG000C5Z1S3",
                    "BBG001S6P675",
                ),
                _reference_row(
                    "MUB",
                    "iShares National Muni Bond ETF",
                    "0001100663",
                    "BBG000TC0WT9",
                    "BBG001SZV978",
                    security_type="ETF",
                ),
            )
        ),
    )

    result = live_identity_search("MU", client=client, as_of_date="2026-05-05")
    candidates = [candidate.to_dict() for candidate in result.candidates]

    assert [candidate["ticker"] for candidate in candidates] == ["MU", "MUA", "MUB"]
    assert candidates[0]["match_reason"] == "ticker_exact_case"
    assert {candidate["match_reason"] for candidate in candidates[1:]} == {
        "ticker_prefix"
    }
    assert [item.endpoint for item in client.request_log] == [
        "/v3/reference/tickers/MU",
        "/v3/reference/tickers",
        "/v3/reference/tickers",
    ]
    assert ("ticker.gte", "MU") in client.request_log[1].params_without_api_key
    assert ("ticker.lt", "MV") in client.request_log[1].params_without_api_key
    assert ("cik", "0000723125") in client.request_log[2].params_without_api_key


def test_sqlite_identity_search_reads_persisted_series_id(tmp_path: Path) -> None:
    db = tmp_path / "stock_universe.sqlite"
    repository = SQLiteStockUniverseRepository(db)
    snapshot = _stored_reference_snapshot(
        "SFBC",
        "Sound Financial Bancorp Inc.",
        "0001495925",
        "BBG000SFBC01",
        "BBG000SFBC02",
    )
    repository.upsert_reference_snapshots([snapshot])
    series_id = repository.lookup_ohlcv_series_id(snapshot.natural_key)

    result = sqlite_identity_search(db, "SFBC")
    candidate = result.candidates[0].to_dict()

    assert candidate["ohlcv_series_id"] == series_id
    assert candidate["lookup_status"] == "resolved"
    assert candidate["ticker"] == "SFBC"
    assert candidate["match_reason"] == "ticker_exact_case"


def test_sqlite_identity_search_exact_ticker_enriches_same_cik_share_classes(
    tmp_path: Path,
) -> None:
    db = tmp_path / "stock_universe.sqlite"
    repository = SQLiteStockUniverseRepository(db)
    repository.ensure_schema()
    repository.upsert_reference_snapshots(
        [
            _stored_reference_snapshot(
                "GOOG",
                "Alphabet Inc. Class C Capital Stock",
                "0001652044",
                "BBG009S3NB30",
                "BBG009S3NB21",
            ),
            _stored_reference_snapshot(
                "GOOGL",
                "Alphabet Inc. Class A Common Stock",
                "0001652044",
                "BBG009S39JX6",
                "BBG009S39JY5",
            ),
        ]
    )

    result = sqlite_identity_search(db, "GOOGL")
    payload = result.to_dict()

    assert [candidate["ticker"] for candidate in payload["candidates"]] == [
        "GOOGL",
        "GOOG",
    ]
    assert [candidate["match_reason"] for candidate in payload["candidates"]] == [
        "ticker_exact_case",
        "issuer_cik_enrichment",
    ]
    assert payload["related_searches"][0]["source"] == "sqlite.identity_catalog"
    assert payload["related_searches"][0]["query"] == "0001652044"


def test_stock_universe_identity_search_cli_uses_live_reference_candidates(
    monkeypatch, capsys
) -> None:
    monkeypatch.setattr(
        "stock_universe.cli.MassiveReadOnlyClient", FakeIdentitySearchClient
    )

    assert (
        stock_universe_main(
            [
                "identity-search",
                "Alphabet",
                "--api-key",
                "secret",
                "--base-url",
                "https://example.test",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)

    assert payload["ok"] is True
    assert payload["command"] == "stock-universe identity-search"
    assert [candidate["ticker"] for candidate in payload["candidates"]] == [
        "GOOG",
        "GOOGL",
        "GOOX",
    ]
    assert payload["related_searches"][0]["query"] == "0001652044"
    assert payload["request_log"][0]["endpoint"] == "/v3/reference/tickers"


def test_xctx_resolve_identity_exposes_explicit_selection_action(
    monkeypatch, capsys
) -> None:
    monkeypatch.setattr(
        "stock_universe.xctx.cli.MassiveReadOnlyClient", RoutingIdentitySearchClient
    )

    assert (
        xctx_main(
            [
                "resolve-identity",
                "--query",
                "Alphabet",
                "--api-key",
                "secret",
                "--base-url",
                "https://example.test",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)

    assert payload["ok"] is True
    assert payload["result_type"] == "IdentityCandidateList"
    assert payload["agent_ohlcv_reporting_policy"] == payload["reporting_policy"]
    assert payload["reporting_policy"]["canonical_ohlcv_field"] == "ohlcv_series_id"
    assert (
        payload["reporting_policy"]["default_ohlcv_reporting_scope"]
        == "ohlcv_series_id"
    )
    assert payload["count"] == 3
    assert [candidate["ticker"] for candidate in payload["candidates"]] == [
        "GOOG",
        "GOOGL",
        "GOOX",
    ]
    assert payload["related_searches"][0]["kind"] == "issuer_cik_enrichment"
    dry_run_action = next(
        action
        for action in payload["next_actions"]
        if action["name"] == "dry-run-selected-ticker"
    )
    assert dry_run_action["command"]["name"] == "xctx dry-run"
    assert dry_run_action["requires_selection"] is True


def test_xctx_resolve_identity_db_missing_returns_repair_error(
    tmp_path: Path, capsys
) -> None:
    db = tmp_path / "missing.sqlite"

    assert (
        xctx_main(
            ["resolve-identity", "--source", "db", "--query", "SFBC", "--db", str(db)]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)

    assert payload["ok"] is False
    assert payload["result_type"] == "RepairError"
    assert payload["repairs"][0]["name"] == "provide-existing-sqlite-db"
    assert db.exists() is False


def test_xctx_resolve_identity_empty_db_teaches_reference_universe_update(
    tmp_path: Path, capsys
) -> None:
    db = tmp_path / "stock_universe.sqlite"
    SQLiteStockUniverseRepository(db).ensure_schema()

    assert (
        xctx_main(
            [
                "resolve-identity",
                "--source",
                "db",
                "--query",
                "Alphabet",
                "--db",
                str(db),
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)

    assert payload["ok"] is True
    assert payload["count"] == 0
    action_names = [action["name"] for action in payload["next_actions"]]
    assert "dry-run-reference-universe-update" in action_names
    assert "commit-reference-universe-update" in action_names
    commit_action = next(
        action
        for action in payload["next_actions"]
        if action["name"] == "commit-reference-universe-update"
    )
    assert commit_action["requires_approval"] is True
    assert (
        commit_action["command"]["name"] == "stock-universe update-reference-universe"
    )


def test_xctx_resolve_identity_db_candidates_use_series_id_dry_run(
    tmp_path: Path, capsys
) -> None:
    db = tmp_path / "stock_universe.sqlite"
    repository = SQLiteStockUniverseRepository(db)
    snapshot = _stored_reference_snapshot(
        "SFBC",
        "Sound Financial Bancorp Inc.",
        "0001495925",
        "BBG000SFBC01",
        "BBG000SFBC02",
    )
    repository.upsert_reference_snapshots([snapshot])

    assert (
        xctx_main(
            ["resolve-identity", "--source", "db", "--query", "SFBC", "--db", str(db)]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)

    bars_action = next(
        action
        for action in payload["next_actions"]
        if action["name"] == "observe-selected-ohlcv-series-bars"
    )
    dry_run_action = next(
        action
        for action in payload["next_actions"]
        if action["name"] == "dry-run-selected-ohlcv-series-id"
    )
    assert payload["agent_ohlcv_reporting_policy"] == payload["reporting_policy"]
    assert payload["reporting_policy"]["canonical_ohlcv_field"] == "ohlcv_series_id"
    assert (
        "selected_candidate.ohlcv_series_id"
        in payload["reporting_policy"]["ohlcv_query_rule"]
    )
    assert bars_action["command"]["name"] == "xctx bars"
    assert (
        bars_action["command"]["args"]["ohlcv_series_id"]
        == "{selected_candidate.ohlcv_series_id}"
    )
    assert bars_action["command"]["args"]["db"] == str(db)
    assert "ticker is only an alias" in bars_action["reason"]
    assert dry_run_action["command"]["name"] == "xctx dry-run"
    assert (
        dry_run_action["command"]["args"]["ohlcv_series_id"]
        == "{selected_candidate.ohlcv_series_id}"
    )
    assert dry_run_action["command"]["args"]["db"] == str(db)
    assert "ticker is an alias" in dry_run_action["reason"]


def _alphabet_payload() -> dict:
    return _reference_list_payload(*_alphabet_rows())


def _alphabet_rows() -> list[dict]:
    return [
        _reference_row(
            "GOOG",
            "Alphabet Inc. Class C Capital Stock",
            "0001652044",
            "BBG009S3NB30",
            "BBG009S3NB21",
        ),
        _reference_row(
            "GOOX",
            "T-Rex 2X Long Alphabet Daily Target ETF",
            "",
            "BBG01KYKF9W1",
            "BBG01KYKFBR2",
            security_type="ETS",
            exchange="BATS",
        ),
        _reference_row(
            "GOOGL",
            "Alphabet Inc. Class A Common Stock",
            "0001652044",
            "BBG009S39JX6",
            "BBG009S39JY5",
        ),
    ]


def _stored_reference_snapshot(
    ticker: str,
    name: str,
    cik: str,
    composite_figi: str,
    share_class_figi: str,
    *,
    security_type: str = "CS",
    exchange: str = "XNAS",
) -> StoredReferenceSnapshot:
    return StoredReferenceSnapshot(
        provider="massive.reference_tickers",
        snapshot_as_of_date="2026-05-07",
        ticker=ticker,
        ohlcv_series_id=-sum(
            (index + 1) * ord(char)
            for index, char in enumerate(f"{ticker}:{composite_figi}")
        ),
        active=True,
        company_name=name,
        cik=cik,
        composite_figi=composite_figi,
        share_class_figi=share_class_figi,
        security_type=security_type,
        primary_exchange=exchange,
        market="stocks",
        locale="us",
        identity_status="permanent",
        natural_key=f"massive:composite_figi:{composite_figi}",
        raw={"ticker": ticker, "name": name},
        source_request={"test": "identity_search"},
    )


def _reference_list_payload(*rows: dict) -> dict:
    return {"status": "OK", "results": list(rows)}


def _reference_row(
    ticker: str,
    name: str,
    cik: str,
    composite_figi: str,
    share_class_figi: str,
    *,
    security_type: str = "CS",
    exchange: str = "XNAS",
) -> dict:
    return {
        "active": True,
        "cik": cik,
        "composite_figi": composite_figi,
        "locale": "us",
        "market": "stocks",
        "name": name,
        "primary_exchange": exchange,
        "share_class_figi": share_class_figi,
        "ticker": ticker,
        "type": security_type,
    }
