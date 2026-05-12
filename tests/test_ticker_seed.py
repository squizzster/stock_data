from __future__ import annotations

from stock_universe.domain import BackfillRequest, TargetIdentity
from stock_universe.providers import (
    HttpJsonResponse,
    MassiveProviderConfig,
    MassiveReadOnlyClient,
)
from stock_universe.storage import SQLiteStockUniverseRepository
from stock_universe.workflows import (
    massive_live_source_from_ticker,
    ticker_seed_base_facts,
)


class FakeReferenceTransport:
    def __init__(self, payload: dict) -> None:
        self.payload = payload
        self.urls: list[str] = []

    def get_json(self, url: str, *, timeout_seconds: float) -> HttpJsonResponse:
        self.urls.append(url)
        return HttpJsonResponse(200, self.payload)


def test_ticker_seed_base_facts_preserve_reference_identity() -> None:
    facts = ticker_seed_base_facts(
        "GOOG",
        _reference_payload(
            ticker="GOOG",
            name="Alphabet Inc. Class C",
            composite_figi="BBG009S3NB30",
            share_class_figi="BBG009S3NB21",
        ),
        ohlcv_series_id=1,
        from_date="2024-01-01",
        to_date="2024-01-31",
    )
    by_kind = {fact.kind: fact.payload_value() for fact in facts}

    target = TargetIdentity.from_payload(by_kind["target_identity"])
    request = BackfillRequest.from_payload(
        target.ohlcv_series_id, by_kind["backfill_request"]
    )

    assert target.ohlcv_series_id == 1
    assert target.latest_ticker == "GOOG"
    assert target.company_name == "Alphabet Inc. Class C"
    assert target.identity_status == "permanent"
    assert target.composite_figi == "BBG009S3NB30"
    assert target.share_class_figi == "BBG009S3NB21"
    assert target.natural_key == "massive:composite_figi:BBG009S3NB30"
    assert target.security_type == "CS"
    assert request.from_date.isoformat() == "2024-01-01"
    assert request.to_date.isoformat() == "2024-01-31"
    assert by_kind["known_aliases"][0]["symbol_text"] == "GOOG"
    assert by_kind["known_aliases"][0]["primary_exchange"] == "XNAS"


def test_ticker_seed_keeps_alphabet_share_classes_separate() -> None:
    goog = ticker_seed_base_facts(
        "GOOG",
        _reference_payload(
            ticker="GOOG",
            name="Alphabet Inc. Class C",
            composite_figi="BBG009S3NB30",
            share_class_figi="BBG009S3NB21",
        ),
        ohlcv_series_id=1,
        from_date="2024-01-01",
        to_date="2024-01-31",
    )
    googl = ticker_seed_base_facts(
        "GOOGL",
        _reference_payload(
            ticker="GOOGL",
            name="Alphabet Inc. Class A",
            composite_figi="BBG009S39JX6",
            share_class_figi="BBG009S39JY5",
        ),
        ohlcv_series_id=2,
        from_date="2024-01-01",
        to_date="2024-01-31",
    )

    goog_target = TargetIdentity.from_payload(
        _payload_by_kind(goog)["target_identity"]
    )
    googl_target = TargetIdentity.from_payload(
        _payload_by_kind(googl)["target_identity"]
    )

    assert goog_target.ohlcv_series_id != googl_target.ohlcv_series_id
    assert goog_target.latest_ticker == "GOOG"
    assert googl_target.latest_ticker == "GOOGL"
    assert goog_target.composite_figi != googl_target.composite_figi
    assert goog_target.share_class_figi != googl_target.share_class_figi


def test_ticker_seed_keeps_cik_only_candidates_provisional_and_separate() -> None:
    first = ticker_seed_base_facts(
        "AAA",
        _reference_payload(
            ticker="AAA",
            name="Issuer First Security",
            composite_figi="",
            share_class_figi="",
        ),
        ohlcv_series_id=1,
        from_date="2024-01-01",
        to_date="2024-01-31",
    )
    second = ticker_seed_base_facts(
        "BBB",
        _reference_payload(
            ticker="BBB",
            name="Issuer Second Security",
            composite_figi="",
            share_class_figi="",
        ),
        ohlcv_series_id=2,
        from_date="2024-01-01",
        to_date="2024-01-31",
    )

    first_target = TargetIdentity.from_payload(
        _payload_by_kind(first)["target_identity"]
    )
    second_target = TargetIdentity.from_payload(
        _payload_by_kind(second)["target_identity"]
    )

    assert first_target.cik == second_target.cik == "0001652044"
    assert first_target.identity_status == "provisional"
    assert second_target.identity_status == "provisional"
    assert first_target.ohlcv_series_id != second_target.ohlcv_series_id
    assert first_target.provisional_key != second_target.provisional_key


def test_massive_live_source_from_ticker_resolves_reference_before_planning(
    tmp_path,
) -> None:
    transport = FakeReferenceTransport(
        _reference_payload(
            ticker="GOOG",
            name="Alphabet Inc. Class C",
            composite_figi="BBG009S3NB30",
            share_class_figi="BBG009S3NB21",
        )
    )
    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"), transport
    )
    db = tmp_path / "stock_universe.sqlite"
    repository = SQLiteStockUniverseRepository(db)
    series_id = repository.ensure_ohlcv_series_id("massive:composite_figi:BBG009S3NB30")

    source, returned_client = massive_live_source_from_ticker(
        "goog",
        client=client,
        db_path=db,
        require_existing_identity=True,
        from_date="2024-01-01",
        to_date="2024-01-31",
    )

    assert returned_client is client
    assert client.request_log[0].endpoint == "/v3/reference/tickers/GOOG"
    assert client.request_log[0].params_without_api_key == ()
    assert transport.urls[0].startswith(
        "https://example.test/v3/reference/tickers/GOOG?"
    )
    initial_kinds = {fact.kind for fact in source.initial_facts()}
    assert {
        "target_identity",
        "backfill_request",
        "known_aliases",
        "plan_metadata",
    } <= initial_kinds
    target_fact = next(
        fact for fact in source.initial_facts() if fact.kind == "target_identity"
    )
    assert target_fact.payload_value()["ohlcv_series_id"] == series_id


def _payload_by_kind(facts):
    return {fact.kind: fact.payload_value() for fact in facts}


def _reference_payload(
    *,
    ticker: str,
    name: str,
    composite_figi: str,
    share_class_figi: str,
) -> dict:
    return {
        "status": "OK",
        "results": {
            "active": True,
            "cik": "0001652044",
            "composite_figi": composite_figi,
            "locale": "us",
            "market": "stocks",
            "name": name,
            "primary_exchange": "XNAS",
            "share_class_figi": share_class_figi,
            "ticker": ticker,
            "type": "CS",
        },
    }
