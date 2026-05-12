from __future__ import annotations

from backfill_test_support import *
from stock_universe.providers.massive.payloads import _aggregate_bars_payload
from stock_universe.providers.massive.reference_helpers import (
    _first_matching_suffix_boundary_fact,
)


def test_massive_ticker_events_provider_returns_typed_fact_without_planner_fetching() -> (
    None
):
    payload = {
        "status": "OK",
        "results": {
            "cik": "0001326801",
            "composite_figi": "BBG000MM2P62",
            "name": "Meta Platforms, Inc. Class A Common Stock",
            "events": [
                {
                    "date": "2022-06-09",
                    "ticker_change": {"ticker": "META"},
                    "type": "ticker_change",
                },
                {
                    "date": "2012-05-18",
                    "ticker_change": {"ticker": "FB"},
                    "type": "ticker_change",
                },
            ],
        },
    }
    transport = FakeHttpJsonTransport(HttpJsonResponse(200, payload))
    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"), transport
    )
    provider = MassiveTickerEventsProvider(client)
    target = TargetIdentity(
        ohlcv_series_id=7034, composite_figi="BBG000MM2P62", latest_ticker="META"
    )

    facts = provider.requested_facts(
        BackfillRequest(series_id=7034, from_date="2022-06-07", to_date="2022-06-10"),
        target,
        (EvidenceRequest("ticker_events", ("7034",)),),
    )

    assert len(facts) == 1
    assert facts[0].kind == "ticker_events"
    assert facts[0].payload_value()["identifier"] == "BBG000MM2P62"
    assert facts[0].payload_value()["identifier_type"] == "composite_figi"
    assert facts[0].payload_value()["events"] == [
        {"date": "2012-05-18", "ticker": "FB", "type": "ticker_change"},
        {"date": "2022-06-09", "ticker": "META", "type": "ticker_change"},
    ]
    assert client.request_log[0].endpoint == "/vX/reference/tickers/BBG000MM2P62/events"
    assert client.request_log[0].params_without_api_key == (("types", "ticker_change"),)
    assert "apiKey=secret" in transport.urls[0]


def test_massive_ticker_events_provider_uses_latest_ticker_when_figi_is_missing() -> (
    None
):
    transport = FakeHttpJsonTransport(
        HttpJsonResponse(200, {"status": "OK", "results": {"events": []}})
    )
    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"), transport
    )
    provider = MassiveTickerEventsProvider(client)
    target = TargetIdentity(ohlcv_series_id=1, latest_ticker="META")

    facts = provider.requested_facts(
        BackfillRequest(series_id=1, from_date="2024-01-01", to_date="2024-01-02"),
        target,
        (EvidenceRequest("ticker_events", ("1",)),),
    )

    assert facts[0].payload_value()["identifier"] == "META"
    assert facts[0].payload_value()["identifier_type"] == "ticker"
    assert client.request_log[0].endpoint == "/vX/reference/tickers/META/events"


def test_massive_reference_boundary_provider_returns_typed_fact_for_explicit_probe() -> (
    None
):
    payload = {
        "status": "OK",
        "results": {
            "ticker": "FB",
            "cik": "0001326801",
            "composite_figi": "BBG000MM2P62",
            "share_class_figi": "BBG001SQCQC5",
            "primary_exchange": "XNAS",
            "type": "CS",
            "active": True,
        },
    }
    transport = FakeHttpJsonTransport(HttpJsonResponse(200, payload))
    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"), transport
    )
    provider = MassiveReferenceBoundaryProvider(client)
    target = TargetIdentity(
        ohlcv_series_id=7034, composite_figi="BBG000MM2P62", latest_ticker="META"
    )

    facts = provider.requested_facts(
        BackfillRequest(series_id=7034, from_date="2022-06-07", to_date="2022-06-10"),
        target,
        (EvidenceRequest("reference_boundary", ("7034", "FB", "2022-06-07", "start")),),
    )

    assert len(facts) == 1
    payload = facts[0].payload_value()
    assert facts[0].kind == "reference_boundary"
    assert payload["ticker"] == "FB"
    assert payload["as_of_date"] == "2022-06-07"
    assert payload["matched"] is True
    assert payload["match_reason"] == "composite_figi_match"
    assert payload["payload"]["point"] == "start"
    assert client.request_log[0].endpoint == "/v3/reference/tickers/FB"
    assert client.request_log[0].params_without_api_key == (("date", "2022-06-07"),)


def test_reference_boundary_can_match_by_cik_when_figi_is_unavailable() -> None:
    payload = {
        "status": "OK",
        "results": {
            "ticker": "CNHI",
            "cik": "0001567094",
            "composite_figi": "",
            "share_class_figi": "",
            "primary_exchange": "XNYS",
        },
    }
    transport = FakeHttpJsonTransport(HttpJsonResponse(200, payload))
    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"), transport
    )
    provider = MassiveReferenceBoundaryProvider(client)
    target = TargetIdentity(
        ohlcv_series_id=2299,
        cik="0001567094",
        latest_ticker="CNH",
        identity_status="provisional",
    )

    facts = provider.requested_facts(
        BackfillRequest(series_id=2299, from_date="2024-05-17", to_date="2024-05-20"),
        target,
        (
            EvidenceRequest(
                "reference_boundary", ("2299", "CNHI", "2024-05-17", "start")
            ),
        ),
    )

    assert facts[0].payload_value()["matched"] is True
    assert facts[0].payload_value()["match_reason"] == "cik_match"


def test_reference_boundary_explicit_probe_accepts_same_cik_historical_figi_rekey() -> (
    None
):
    payload = {
        "status": "OK",
        "results": {
            "ticker": "BALY",
            "name": "Bally's Corporation",
            "cik": "0001747079",
            "composite_figi": "BBG005Q22HG8",
            "share_class_figi": "BBG005Q22HH7",
            "primary_exchange": "XNYS",
            "type": "CS",
        },
    }
    transport = FakeHttpJsonTransport(HttpJsonResponse(200, payload))
    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"), transport
    )
    provider = MassiveReferenceBoundaryProvider(client)
    target = TargetIdentity(
        ohlcv_series_id=1029,
        cik="0001747079",
        composite_figi="BBG01QPXB2Q6",
        share_class_figi="BBG01QPXB3P5",
        latest_ticker="BALY",
        latest_primary_exchange="XNYS",
        security_type="CS",
    )

    facts = provider.requested_facts(
        BackfillRequest(series_id=1029, from_date="2024-11-20", to_date="2026-05-08"),
        target,
        (
            EvidenceRequest(
                "reference_boundary", ("1029", "BALY", "2024-11-20", "start")
            ),
        ),
    )

    assert len(facts) == 1
    payload_value = facts[0].payload_value()
    assert payload_value["matched"] is True
    assert payload_value["match_reason"].startswith(
        "provider_historical_figi_rekey_same_ticker_cik_type"
    )
    assert (
        payload_value["payload"]["validation_override"]["reason"]
        == "provider_historical_figi_rekey"
    )
    assert (
        payload_value["payload"]["validation_override"]["historical_composite"]
        == "BBG005Q22HG8"
    )
    assert (
        payload_value["payload"]["validation_override"]["target_composite_figi"]
        == "BBG01QPXB2Q6"
    )


def test_reference_boundary_explicit_probe_rejects_historical_figi_rekey_with_different_cik() -> (
    None
):
    payload = {
        "status": "OK",
        "results": {
            "ticker": "BALY",
            "name": "Bally's Corporation",
            "cik": "0000000000",
            "composite_figi": "BBG005Q22HG8",
            "share_class_figi": "BBG005Q22HH7",
            "primary_exchange": "XNYS",
            "type": "CS",
        },
    }
    transport = FakeHttpJsonTransport(HttpJsonResponse(200, payload))
    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"), transport
    )
    provider = MassiveReferenceBoundaryProvider(client)
    target = TargetIdentity(
        ohlcv_series_id=1029,
        cik="0001747079",
        composite_figi="BBG01QPXB2Q6",
        share_class_figi="BBG01QPXB3P5",
        latest_ticker="BALY",
        latest_primary_exchange="XNYS",
        security_type="CS",
    )

    facts = provider.requested_facts(
        BackfillRequest(series_id=1029, from_date="2024-11-20", to_date="2026-05-08"),
        target,
        (
            EvidenceRequest(
                "reference_boundary", ("1029", "BALY", "2024-11-20", "start")
            ),
        ),
    )

    payload_value = facts[0].payload_value()
    assert payload_value["matched"] is False
    assert "validation_override" not in payload_value["payload"]


def test_reference_boundary_rejects_cik_match_when_security_type_differs() -> None:
    payload = {
        "status": "OK",
        "results": {
            "ticker": "COF",
            "cik": "0000927628",
            "composite_figi": "",
            "share_class_figi": "",
            "primary_exchange": "XNYS",
            "type": "CS",
        },
    }
    transport = FakeHttpJsonTransport(HttpJsonResponse(200, payload))
    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"), transport
    )
    provider = MassiveReferenceBoundaryProvider(client)
    target = TargetIdentity(
        ohlcv_series_id=2362,
        cik="0000927628",
        latest_ticker="COFpL",
        identity_status="provisional",
        security_type="PFD",
    )

    facts = provider.requested_facts(
        BackfillRequest(series_id=2362, from_date="2021-05-04", to_date="2021-05-05"),
        target,
        (
            EvidenceRequest(
                "reference_boundary", ("2362", "COF", "2021-05-04", "start")
            ),
        ),
    )

    payload_value = facts[0].payload_value()
    assert payload_value["matched"] is False
    assert payload_value["match_reason"] == "security_type detail=CS target=PFD"


def test_reference_boundary_can_match_provisional_latest_ticker_without_ids() -> None:
    payload = {
        "status": "OK",
        "results": {
            "ticker": "CNH",
            "cik": "",
            "composite_figi": "",
            "share_class_figi": "",
            "primary_exchange": "XNYS",
        },
    }
    transport = FakeHttpJsonTransport(HttpJsonResponse(200, payload))
    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"), transport
    )
    provider = MassiveReferenceBoundaryProvider(client)
    target = TargetIdentity(
        ohlcv_series_id=2299, latest_ticker="CNH", identity_status="provisional"
    )

    facts = provider.requested_facts(
        BackfillRequest(series_id=2299, from_date="2024-05-17", to_date="2024-05-20"),
        target,
        (
            EvidenceRequest(
                "reference_boundary", ("2299", "CNH", "2024-05-20", "start")
            ),
        ),
    )

    assert facts[0].payload_value()["matched"] is True
    assert (
        facts[0].payload_value()["match_reason"]
        == "ticker_only_provisional_match_missing_cik"
    )


def test_reference_boundary_provider_returns_first_bar_boundary_for_start_gap() -> None:
    target = TargetIdentity(
        ohlcv_series_id=812, composite_figi="BBG011C6KJG8", latest_ticker="ATAT"
    )

    class StartGapTransport:
        def __init__(self) -> None:
            self.urls: list[str] = []

        def get_json(self, url: str, *, timeout_seconds: float) -> HttpJsonResponse:
            self.urls.append(url)
            parsed = urlparse(url)
            if "/v2/aggs/" in parsed.path:
                return HttpJsonResponse(
                    200,
                    {
                        "status": "OK",
                        "resultsCount": 1,
                        "results": [
                            {"t": 1668124800000, "o": 1, "h": 1, "l": 1, "c": 1, "v": 1}
                        ],
                    },
                )
            as_of_date = parse_qs(parsed.query).get("date", [""])[0]
            if as_of_date == "2021-07-13":
                return HttpJsonResponse(404, {"status": "NOT_FOUND", "results": None})
            return HttpJsonResponse(
                200,
                {
                    "status": "OK",
                    "results": {
                        "ticker": "ATAT",
                        "composite_figi": "BBG011C6KJG8",
                    },
                },
            )

    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"),
        StartGapTransport(),
    )
    provider = MassiveReferenceBoundaryProvider(client)

    facts = provider.requested_facts(
        BackfillRequest(series_id=812, from_date="2021-07-07", to_date="2022-11-14"),
        target,
        (
            EvidenceRequest(
                "reference_boundary", ("812", "ATAT", "2021-07-13", "start")
            ),
        ),
    )

    assert [fact.kind for fact in facts] == ["reference_boundary", "reference_boundary"]
    assert facts[0].payload_value()["matched"] is False
    assert facts[1].payload_value()["as_of_date"] == "2022-11-11"
    assert facts[1].payload_value()["matched"] is True


def test_reference_boundary_provider_scans_to_first_valid_bar_boundary_for_start_gap() -> (
    None
):
    target = TargetIdentity(
        ohlcv_series_id=4846, composite_figi="BBG01N6CGW43", latest_ticker="GRAL"
    )

    class FirstValidBarTransport:
        def get_json(self, url: str, *, timeout_seconds: float) -> HttpJsonResponse:
            parsed = urlparse(url)
            if "/v2/aggs/" in parsed.path:
                return HttpJsonResponse(
                    200,
                    {
                        "status": "OK",
                        "results": [
                            {
                                "t": 1718668800000,
                                "o": 1,
                                "h": 1,
                                "l": 1,
                                "c": 1,
                                "v": 1,
                            },
                            {
                                "t": 1718755200000,
                                "o": 1,
                                "h": 1,
                                "l": 1,
                                "c": 1,
                                "v": 1,
                            },
                        ],
                    },
                )
            as_of_date = parse_qs(parsed.query).get("date", [""])[0]
            if as_of_date in {"2024-06-12", "2024-06-18"}:
                return HttpJsonResponse(404, {"status": "NOT_FOUND", "results": None})
            return HttpJsonResponse(
                200,
                {
                    "status": "OK",
                    "results": {
                        "ticker": "GRAL",
                        "composite_figi": "BBG01N6CGW43",
                    },
                },
            )

    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"),
        FirstValidBarTransport(),
    )
    provider = MassiveReferenceBoundaryProvider(client)

    facts = provider.requested_facts(
        BackfillRequest(series_id=4846, from_date="2024-06-12", to_date="2024-06-26"),
        target,
        (
            EvidenceRequest(
                "reference_boundary", ("4846", "GRAL", "2024-06-12", "start")
            ),
        ),
    )

    assert [fact.kind for fact in facts] == ["reference_boundary", "reference_boundary"]
    assert facts[0].payload_value()["matched"] is False
    assert facts[1].payload_value()["as_of_date"] == "2024-06-19"
    assert facts[1].payload_value()["matched"] is True


def test_reference_boundary_provider_scans_deep_start_gaps_and_accepts_scoped_historical_rekey() -> (
    None
):
    target = TargetIdentity(
        ohlcv_series_id=7766,
        cik="0001865631",
        composite_figi="BBG017XG00X4",
        latest_ticker="NN",
        security_type="CS",
    )

    class DeepHistoricalStartGapTransport:
        def get_json(self, url: str, *, timeout_seconds: float) -> HttpJsonResponse:
            parsed = urlparse(url)
            if "/v2/aggs/" in parsed.path:
                start = dt.date(2021, 11, 1)
                return HttpJsonResponse(
                    200,
                    {
                        "status": "OK",
                        "results": [
                            {
                                "t": int(
                                    dt.datetime.combine(
                                        start + dt.timedelta(days=index),
                                        dt.time(),
                                        dt.UTC,
                                    ).timestamp()
                                    * 1000
                                ),
                                "o": 1,
                                "h": 1,
                                "l": 1,
                                "c": 1,
                                "v": 1,
                            }
                            for index in range(40)
                        ],
                    },
                )
            as_of_date = parse_qs(parsed.query).get("date", [""])[0]
            if as_of_date < "2021-12-10":
                return HttpJsonResponse(
                    200,
                    {
                        "status": "OK",
                        "results": {
                            "ticker": "NN",
                            "cik": "0001865631",
                            "composite_figi": "BBG00XP8D3W6",
                            "type": "CS",
                        },
                    },
                )
            return HttpJsonResponse(
                200,
                {
                    "status": "OK",
                    "results": {
                        "ticker": "NN",
                        "cik": "0001865631",
                        "composite_figi": "BBG017XG00X4",
                        "type": "CS",
                    },
                },
            )

    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"),
        DeepHistoricalStartGapTransport(),
    )
    provider = MassiveReferenceBoundaryProvider(client)

    facts = provider.requested_facts(
        BackfillRequest(series_id=7766, from_date="2021-10-29", to_date="2026-05-04"),
        target,
        (EvidenceRequest("reference_boundary", ("7766", "NN", "2021-10-29", "start")),),
    )

    assert [fact.kind for fact in facts] == ["reference_boundary"]
    assert facts[0].payload_value()["matched"] is True
    assert (
        facts[0]
        .payload_value()["match_reason"]
        .startswith("provider_historical_figi_rekey_same_ticker_cik_type")
    )
    assert all("/v2/aggs/" not in row.endpoint for row in client.request_log)


def test_reference_boundary_provider_scans_past_first_32_bars_for_listing_gap() -> None:
    target = TargetIdentity(
        ohlcv_series_id=12806, composite_figi="BBG011S2J821", latest_ticker="BCOW"
    )
    first_bar = dt.date(2021, 5, 10)

    class DeepListingGapTransport:
        def get_json(self, url: str, *, timeout_seconds: float) -> HttpJsonResponse:
            parsed = urlparse(url)
            if "/v2/aggs/" in parsed.path:
                return HttpJsonResponse(
                    200,
                    {
                        "status": "OK",
                        "results": [
                            {
                                "t": int(
                                    dt.datetime.combine(
                                        first_bar + dt.timedelta(days=index),
                                        dt.time(),
                                        dt.UTC,
                                    ).timestamp()
                                    * 1000
                                ),
                                "o": 1,
                                "h": 1,
                                "l": 1,
                                "c": 1,
                                "v": 1,
                            }
                            for index in range(40)
                        ],
                    },
                )
            as_of_date = dt.date.fromisoformat(
                parse_qs(parsed.query).get("date", [""])[0]
            )
            if as_of_date < first_bar + dt.timedelta(days=35):
                return HttpJsonResponse(404, {"status": "NOT_FOUND", "results": None})
            return HttpJsonResponse(
                200,
                {
                    "status": "OK",
                    "results": {"ticker": "BCOW", "composite_figi": "BBG011S2J821"},
                },
            )

    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"),
        DeepListingGapTransport(),
    )
    provider = MassiveReferenceBoundaryProvider(client)

    facts = provider.requested_facts(
        BackfillRequest(series_id=12806, from_date="2021-05-04", to_date="2026-05-04"),
        target,
        (
            EvidenceRequest(
                "reference_boundary", ("12806", "BCOW", "2021-05-04", "start")
            ),
        ),
    )

    assert facts[1].payload_value()["as_of_date"] == "2021-06-14"
    assert facts[1].payload_value()["matched"] is True
    assert (
        facts[1].payload_value()["payload"]["boundary_search"]["right_anchor_date"]
        == "2021-06-18"
    )


def test_reference_boundary_provider_finds_deep_start_gap_without_linear_scan() -> None:
    target = TargetIdentity(
        ohlcv_series_id=3, composite_figi="BBG01B0JRCS6", latest_ticker="AAA"
    )
    first_bar = dt.date(2021, 5, 10)
    transition = first_bar + dt.timedelta(days=330)

    class DeepStartGapTransport:
        def __init__(self) -> None:
            self.urls: list[str] = []

        def get_json(self, url: str, *, timeout_seconds: float) -> HttpJsonResponse:
            self.urls.append(url)
            parsed = urlparse(url)
            if "/v2/aggs/" in parsed.path:
                return HttpJsonResponse(
                    200,
                    {
                        "status": "OK",
                        "results": [
                            {
                                "t": int(
                                    dt.datetime.combine(
                                        first_bar + dt.timedelta(days=index),
                                        dt.time(),
                                        dt.UTC,
                                    ).timestamp()
                                    * 1000
                                ),
                                "o": 1,
                                "h": 1,
                                "l": 1,
                                "c": 1,
                                "v": 1,
                            }
                            for index in range(400)
                        ],
                    },
                )
            as_of_date = dt.date.fromisoformat(
                parse_qs(parsed.query).get("date", [""])[0]
            )
            if as_of_date < transition:
                return HttpJsonResponse(
                    200,
                    {
                        "status": "OK",
                        "results": {
                            "ticker": "AAA",
                            "composite_figi": "BBG00X5FSP48",
                            "type": "ETF",
                        },
                    },
                )
            return HttpJsonResponse(
                200,
                {
                    "status": "OK",
                    "results": {
                        "ticker": "AAA",
                        "composite_figi": "BBG01B0JRCS6",
                        "type": "ETF",
                    },
                },
            )

    transport = DeepStartGapTransport()
    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"),
        transport,
    )
    provider = MassiveReferenceBoundaryProvider(client)

    facts = provider.requested_facts(
        BackfillRequest(series_id=3, from_date="2021-05-04", to_date="2022-10-16"),
        target,
        (EvidenceRequest("reference_boundary", ("3", "AAA", "2021-05-04", "start")),),
    )

    reference_detail_calls = [
        url
        for url in transport.urls
        if urlparse(url).path == "/v3/reference/tickers/AAA"
    ]

    assert facts[1].payload_value()["as_of_date"] == transition.isoformat()
    assert facts[1].payload_value()["matched"] is True
    assert (
        facts[1].payload_value()["payload"]["boundary_search"]["algorithm"]
        == "anchored_final_suffix_lower_bound"
    )
    assert len(reference_detail_calls) < 20


def test_reference_boundary_provider_defers_deep_start_gap_when_alias_history_is_requested() -> (
    None
):
    target = TargetIdentity(
        ohlcv_series_id=3, composite_figi="BBG01B0JRCS6", latest_ticker="AAA"
    )

    class AliasHistoryRoundTransport:
        def __init__(self) -> None:
            self.urls: list[str] = []

        def get_json(self, url: str, *, timeout_seconds: float) -> HttpJsonResponse:
            self.urls.append(url)
            parsed = urlparse(url)
            assert "/v2/aggs/" not in parsed.path
            return HttpJsonResponse(
                200,
                {
                    "status": "OK",
                    "results": {
                        "ticker": "AAA",
                        "composite_figi": "BBG00X5FSP48",
                        "type": "ETF",
                    },
                },
            )

    transport = AliasHistoryRoundTransport()
    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"),
        transport,
    )
    provider = MassiveReferenceBoundaryProvider(client)

    facts = provider.requested_facts(
        BackfillRequest(series_id=3, from_date="2021-05-04", to_date="2022-10-16"),
        target,
        (
            EvidenceRequest("reference_boundary", ("3", "AAA", "2021-05-04", "start")),
            EvidenceRequest("alias_history", ("3", "2021-05-04", "2022-10-17", "AAA")),
        ),
    )

    assert len(facts) == 1
    assert facts[0].payload_value()["matched"] is False
    assert [urlparse(url).path for url in transport.urls] == [
        "/v3/reference/tickers/AAA"
    ]


def test_reference_boundary_suffix_search_matches_legacy_linear_result_inside_cap() -> (
    None
):
    target = TargetIdentity(
        ohlcv_series_id=3, composite_figi="BBG01B0JRCS6", latest_ticker="AAA"
    )
    first_bar = dt.date(2021, 5, 10)
    transition = first_bar + dt.timedelta(days=42)

    class LegacyComparableTransport:
        def __init__(self) -> None:
            self.urls: list[str] = []

        def get_json(self, url: str, *, timeout_seconds: float) -> HttpJsonResponse:
            self.urls.append(url)
            parsed = urlparse(url)
            if "/v2/aggs/" in parsed.path:
                return HttpJsonResponse(
                    200,
                    {
                        "status": "OK",
                        "results": [
                            {
                                "t": int(
                                    dt.datetime.combine(
                                        first_bar + dt.timedelta(days=index),
                                        dt.time(),
                                        dt.UTC,
                                    ).timestamp()
                                    * 1000
                                ),
                                "o": 1,
                                "h": 1,
                                "l": 1,
                                "c": 1,
                                "v": 1,
                            }
                            for index in range(120)
                        ],
                    },
                )
            as_of_date = dt.date.fromisoformat(
                parse_qs(parsed.query).get("date", [""])[0]
            )
            return HttpJsonResponse(
                200,
                {
                    "status": "OK",
                    "results": {
                        "ticker": "AAA",
                        "composite_figi": "BBG01B0JRCS6"
                        if as_of_date >= transition
                        else "BBG00X5FSP48",
                        "type": "ETF",
                    },
                },
            )

    transport = LegacyComparableTransport()
    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"),
        transport,
    )
    provider = MassiveReferenceBoundaryProvider(client)

    facts = provider.requested_facts(
        BackfillRequest(series_id=3, from_date="2021-05-04", to_date="2021-09-30"),
        target,
        (EvidenceRequest("reference_boundary", ("3", "AAA", "2021-05-04", "start")),),
    )

    reference_detail_calls = [
        url
        for url in transport.urls
        if urlparse(url).path == "/v3/reference/tickers/AAA"
    ]

    assert facts[1].payload_value()["as_of_date"] == transition.isoformat()
    assert facts[1].payload_value()["matched"] is True
    assert (
        facts[1].payload_value()["payload"]["boundary_search"]["right_anchor_date"]
        == (first_bar + dt.timedelta(days=119)).isoformat()
    )
    assert len(reference_detail_calls) < 20


def test_reference_boundary_suffix_search_preserves_legacy_fallback_without_right_anchor() -> (
    None
):
    target = TargetIdentity(
        ohlcv_series_id=3, composite_figi="BBG01B0JRCS6", latest_ticker="AAA"
    )
    first_bar = dt.date(2021, 5, 10)
    transient_match = first_bar + dt.timedelta(days=8)

    class NonSuffixTransport:
        def get_json(self, url: str, *, timeout_seconds: float) -> HttpJsonResponse:
            parsed = urlparse(url)
            if "/v2/aggs/" in parsed.path:
                return HttpJsonResponse(
                    200,
                    {
                        "status": "OK",
                        "results": [
                            {
                                "t": int(
                                    dt.datetime.combine(
                                        first_bar + dt.timedelta(days=index),
                                        dt.time(),
                                        dt.UTC,
                                    ).timestamp()
                                    * 1000
                                ),
                                "o": 1,
                                "h": 1,
                                "l": 1,
                                "c": 1,
                                "v": 1,
                            }
                            for index in range(40)
                        ],
                    },
                )
            as_of_date = dt.date.fromisoformat(
                parse_qs(parsed.query).get("date", [""])[0]
            )
            return HttpJsonResponse(
                200,
                {
                    "status": "OK",
                    "results": {
                        "ticker": "AAA",
                        "composite_figi": "BBG01B0JRCS6"
                        if as_of_date == transient_match
                        else "BBG00X5FSP48",
                        "type": "ETF",
                    },
                },
            )

    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"),
        NonSuffixTransport(),
    )
    provider = MassiveReferenceBoundaryProvider(client)

    facts = provider.requested_facts(
        BackfillRequest(series_id=3, from_date="2021-05-04", to_date="2021-06-30"),
        target,
        (EvidenceRequest("reference_boundary", ("3", "AAA", "2021-05-04", "start")),),
    )

    assert facts[1].payload_value()["as_of_date"] == transient_match.isoformat()
    assert facts[1].payload_value()["matched"] is True


def test_reference_boundary_suffix_search_handles_all_monotonic_suffix_starts() -> None:
    target = TargetIdentity(
        ohlcv_series_id=3, composite_figi="TARGET", latest_ticker="AAA"
    )
    first_bar = dt.date(2024, 1, 2)
    dates = tuple(
        (first_bar + dt.timedelta(days=index)).isoformat() for index in range(96)
    )

    class SuffixTransport:
        def __init__(self, suffix_start: str) -> None:
            self.suffix_start = suffix_start

        def get_json(self, url: str, *, timeout_seconds: float) -> HttpJsonResponse:
            parsed = urlparse(url)
            as_of_date = parse_qs(parsed.query).get("date", [""])[0]
            return HttpJsonResponse(
                200,
                {
                    "status": "OK",
                    "results": {
                        "ticker": "AAA",
                        "composite_figi": "TARGET"
                        if as_of_date >= self.suffix_start
                        else "OTHER",
                        "type": "ETF",
                    },
                },
            )

    for suffix_index in (0, 1, 2, 3, 7, 15, 31, 32, 47, 63, 80, 95):
        client = MassiveReadOnlyClient(
            MassiveProviderConfig("secret", base_url="https://example.test"),
            SuffixTransport(dates[suffix_index]),
        )

        fact = _first_matching_suffix_boundary_fact(
            client,
            BackfillRequest(series_id=3, from_date=dates[0], to_date=dates[-1]),
            target,
            "AAA",
            dates,
            point="start",
            source="test.suffix_search",
            allow_historical_rekey=False,
        )

        assert fact is not None
        assert fact.as_of_date.isoformat() == dates[suffix_index]
        assert (
            fact.to_legacy_dict()["payload"]["boundary_search"]["candidate_date"]
            == dates[suffix_index]
        )
        assert len(client.request_log) <= 2 * len(dates).bit_length() + 3


def test_reference_boundary_suffix_search_handles_empty_dates() -> None:
    target = TargetIdentity(
        ohlcv_series_id=3, composite_figi="TARGET", latest_ticker="AAA"
    )

    class EmptyDateTransport:
        def get_json(self, url: str, *, timeout_seconds: float) -> HttpJsonResponse:
            raise AssertionError("empty suffix search must not issue HTTP requests")

    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"),
        EmptyDateTransport(),
    )

    fact = _first_matching_suffix_boundary_fact(
        client,
        BackfillRequest(series_id=3, from_date="2024-01-02", to_date="2024-01-03"),
        target,
        "AAA",
        (),
        point="start",
        source="test.suffix_search",
        allow_historical_rekey=False,
    )

    assert fact is None
    assert client.request_log == []


def test_reference_boundary_suffix_search_requires_matching_right_anchor() -> None:
    target = TargetIdentity(
        ohlcv_series_id=3, composite_figi="TARGET", latest_ticker="AAA"
    )
    first_bar = dt.date(2024, 1, 2)
    dates = tuple(
        (first_bar + dt.timedelta(days=index)).isoformat() for index in range(16)
    )

    class NoRightAnchorTransport:
        def get_json(self, url: str, *, timeout_seconds: float) -> HttpJsonResponse:
            parsed = urlparse(url)
            as_of_date = parse_qs(parsed.query).get("date", [""])[0]
            return HttpJsonResponse(
                200,
                {
                    "status": "OK",
                    "results": {
                        "ticker": "AAA",
                        "composite_figi": "TARGET"
                        if as_of_date == dates[4]
                        else "OTHER",
                        "type": "ETF",
                    },
                },
            )

    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"),
        NoRightAnchorTransport(),
    )

    fact = _first_matching_suffix_boundary_fact(
        client,
        BackfillRequest(series_id=3, from_date=dates[0], to_date=dates[-1]),
        target,
        "AAA",
        dates,
        point="start",
        source="test.suffix_search",
        allow_historical_rekey=False,
    )

    assert fact is None
    assert len(client.request_log) == 1


def test_reference_boundary_suffix_search_ignores_transient_match_before_final_suffix() -> (
    None
):
    target = TargetIdentity(
        ohlcv_series_id=3, composite_figi="TARGET", latest_ticker="AAA"
    )
    first_bar = dt.date(2024, 1, 2)
    dates = tuple(
        (first_bar + dt.timedelta(days=index)).isoformat() for index in range(72)
    )
    transient_match = dates[5]
    suffix_start = dates[48]

    class TransientThenSuffixTransport:
        def get_json(self, url: str, *, timeout_seconds: float) -> HttpJsonResponse:
            parsed = urlparse(url)
            as_of_date = parse_qs(parsed.query).get("date", [""])[0]
            matches_target = as_of_date == transient_match or as_of_date >= suffix_start
            return HttpJsonResponse(
                200,
                {
                    "status": "OK",
                    "results": {
                        "ticker": "AAA",
                        "composite_figi": "TARGET" if matches_target else "OTHER",
                        "type": "ETF",
                    },
                },
            )

    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"),
        TransientThenSuffixTransport(),
    )

    fact = _first_matching_suffix_boundary_fact(
        client,
        BackfillRequest(series_id=3, from_date=dates[0], to_date=dates[-1]),
        target,
        "AAA",
        dates,
        point="start",
        source="test.suffix_search",
        allow_historical_rekey=False,
    )

    assert fact is not None
    assert fact.as_of_date.isoformat() == suffix_start
    assert (
        fact.to_legacy_dict()["payload"]["boundary_search"]["candidate_date"]
        == suffix_start
    )
    assert fact.to_legacy_dict()["payload"]["boundary_search"]["rule"].startswith(
        "The rightmost bar date must validate"
    )


def test_massive_reference_boundary_provider_ignores_vague_reference_requests() -> None:
    transport = FakeHttpJsonTransport(
        HttpJsonResponse(200, {"status": "OK", "results": {}})
    )
    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"), transport
    )
    provider = MassiveReferenceBoundaryProvider(client)

    facts = provider.requested_facts(
        BackfillRequest(series_id=1, from_date="2024-01-01", to_date="2024-01-02"),
        TargetIdentity(ohlcv_series_id=1, latest_ticker="META"),
        (EvidenceRequest("reference_boundary", ("1", "2024-01-01")),),
    )

    assert facts == ()
    assert client.request_log == []


def test_massive_bar_probe_provider_returns_typed_fact_for_explicit_probe() -> None:
    payload = {
        "status": "OK",
        "resultsCount": 2,
        "results": [
            {"t": 1714521600000, "o": 10, "h": 11, "l": 9, "c": 10.5, "v": 100},
            {"t": 1714608000000, "o": 10.5, "h": 12, "l": 10, "c": 11.5, "v": 120},
        ],
    }
    transport = FakeHttpJsonTransport(HttpJsonResponse(200, payload))
    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"), transport
    )
    provider = MassiveBarProbeProvider(client)

    facts = provider.requested_facts(
        BackfillRequest(
            series_id=1, from_date="2024-05-01", to_date="2024-05-02", adjusted=True
        ),
        TargetIdentity(ohlcv_series_id=1, latest_ticker="ABC"),
        (EvidenceRequest("bar_probe", ("1", "ABC", "2024-05-01", "2024-05-02")),),
    )

    assert len(facts) == 1
    payload = facts[0].payload_value()
    assert facts[0].kind == "bar_probe"
    assert payload["ticker"] == "ABC"
    assert payload["from_date"] == "2024-05-01"
    assert payload["to_date"] == "2024-05-02"
    assert payload["bar_count"] == 2
    assert payload["api_status"] == "OK"
    assert (
        client.request_log[0].endpoint
        == "/v2/aggs/ticker/ABC/range/1/day/2024-05-01/2024-05-02"
    )
    assert client.request_log[0].params_without_api_key == (
        ("adjusted", "true"),
        ("limit", "50000"),
        ("sort", "asc"),
    )


def test_aggregate_bars_payload_reuses_duplicate_request_within_client() -> None:
    payload = {
        "status": "OK",
        "resultsCount": 1,
        "results": [
            {"t": 1714521600000, "o": 10, "h": 11, "l": 9, "c": 10.5, "v": 100}
        ],
    }
    transport = FakeHttpJsonTransport(HttpJsonResponse(200, payload))
    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"), transport
    )
    request = BackfillRequest(
        series_id=1, from_date="2024-05-01", to_date="2024-05-02", adjusted=True
    )

    first = _aggregate_bars_payload(client, request, "ABC", "2024-05-01", "2024-05-02")
    second = _aggregate_bars_payload(client, request, "ABC", "2024-05-01", "2024-05-02")

    assert first == second == payload
    assert len(client.request_log) == 1


def test_aggregate_bars_payload_uses_request_grain_by_default() -> None:
    payload = {
        "status": "OK",
        "resultsCount": 1,
        "results": [
            {"t": 1714568400000, "o": 10, "h": 11, "l": 9, "c": 10.5, "v": 100}
        ],
    }
    transport = FakeHttpJsonTransport(HttpJsonResponse(200, payload))
    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"), transport
    )
    request = BackfillRequest(
        series_id=1,
        from_date="2024-05-01",
        to_date="2024-05-02",
        multiplier=30,
        timespan="minute",
        adjusted=True,
    )

    assert (
        _aggregate_bars_payload(client, request, "ABC", "2024-05-01", "2024-05-02")
        == payload
    )
    assert (
        client.request_log[0].endpoint
        == "/v2/aggs/ticker/ABC/range/30/minute/2024-05-01/2024-05-02"
    )


def test_massive_bar_probe_provider_ignores_vague_bar_requests() -> None:
    transport = FakeHttpJsonTransport(
        HttpJsonResponse(200, {"status": "OK", "results": []})
    )
    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"), transport
    )
    provider = MassiveBarProbeProvider(client)

    facts = provider.requested_facts(
        BackfillRequest(series_id=1, from_date="2024-01-01", to_date="2024-01-02"),
        TargetIdentity(ohlcv_series_id=1, latest_ticker="ABC"),
        (EvidenceRequest("bar_probe", ("1", "ABC")),),
    )

    assert facts == ()
    assert client.request_log == []


def test_massive_identity_scan_provider_returns_typed_fact_for_explicit_scan() -> None:
    payload = {
        "status": "OK",
        "results": [
            {
                "ticker": "ABML",
                "name": "American Battery Technology Company",
                "composite_figi": "BBG00X",
                "share_class_figi": "BBG00Y",
                "cik": "0001576873",
                "primary_exchange": "XNAS",
                "type": "CS",
                "active": False,
            }
        ],
    }
    transport = FakeHttpJsonTransport(HttpJsonResponse(200, payload))
    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"), transport
    )
    provider = MassiveIdentityScanProvider(client)

    facts = provider.requested_facts(
        BackfillRequest(series_id=1, from_date="2023-09-11", to_date="2023-09-20"),
        TargetIdentity(ohlcv_series_id=1, latest_ticker="ABAT"),
        (EvidenceRequest("identity_scan", ("1", "ABML", "2023-09-11")),),
    )

    assert len(facts) == 1
    payload = facts[0].payload_value()
    assert facts[0].kind == "identity_scan"
    assert payload["query"] == "ABML"
    assert payload["as_of_date"] == "2023-09-11"
    assert payload["matches"] == [
        {
            "active": False,
            "cik": "0001576873",
            "composite_figi": "BBG00X",
            "name": "American Battery Technology Company",
            "primary_exchange": "XNAS",
            "share_class_figi": "BBG00Y",
            "ticker": "ABML",
            "type": "CS",
        },
    ]
    assert client.request_log[0].endpoint == "/v3/reference/tickers"
    assert client.request_log[0].params_without_api_key == (
        ("active", "false"),
        ("date", "2023-09-11"),
        ("limit", "100"),
        ("search", "ABML"),
    )


def test_massive_identity_scan_provider_preserves_empty_match_result() -> None:
    transport = FakeHttpJsonTransport(
        HttpJsonResponse(200, {"status": "OK", "results": []})
    )
    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"), transport
    )
    provider = MassiveIdentityScanProvider(client)

    facts = provider.requested_facts(
        BackfillRequest(series_id=1, from_date="2023-09-11", to_date="2023-09-20"),
        TargetIdentity(ohlcv_series_id=1, latest_ticker="ABAT"),
        (EvidenceRequest("identity_scan", ("1", "ABML", "2023-09-11")),),
    )

    assert facts[0].payload_value()["matches"] == []


def test_massive_identity_scan_provider_ignores_vague_scan_requests() -> None:
    transport = FakeHttpJsonTransport(
        HttpJsonResponse(200, {"status": "OK", "results": []})
    )
    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"), transport
    )
    provider = MassiveIdentityScanProvider(client)

    facts = provider.requested_facts(
        BackfillRequest(series_id=1, from_date="2023-09-11", to_date="2023-09-20"),
        TargetIdentity(ohlcv_series_id=1, latest_ticker="ABAT"),
        (EvidenceRequest("identity_scan", ("1", "ABML")),),
    )

    assert facts == ()
    assert client.request_log == []
