from __future__ import annotations

from backfill_test_support import *
from stock_universe.providers.massive.ticker_replacement import (
    _same_preferred_issue_terms,
)


def test_massive_ticker_replacement_provider_derives_single_valid_alias() -> None:
    target = TargetIdentity(
        ohlcv_series_id=2005,
        composite_figi="BBG014KFRNP7",
        latest_ticker="CEG",
        known_alias_tickers=("CEGVV", "CEG"),
    )

    class CegReplacementTransport:
        def __init__(self) -> None:
            self.urls: list[str] = []

        def get_json(self, url: str, *, timeout_seconds: float) -> HttpJsonResponse:
            self.urls.append(url)
            parsed = urlparse(url)
            ticker = parsed.path.rsplit("/", 1)[-1]
            as_of_date = parse_qs(parsed.query).get("date", [""])[0]
            if ticker == "CEGVV":
                return HttpJsonResponse(
                    200,
                    {
                        "status": "OK",
                        "results": {
                            "ticker": ticker,
                            "composite_figi": "BBG014KFRNP7",
                            "share_class_figi": "BBG014KFRPJ9",
                            "queried_date": as_of_date,
                        },
                    },
                )
            return HttpJsonResponse(404, {"status": "NOT_FOUND", "results": None})

    transport = CegReplacementTransport()
    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"), transport
    )
    provider = MassiveTickerReplacementProvider(client)

    facts = provider.requested_facts(
        BackfillRequest(series_id=2005, from_date="2022-01-19", to_date="2022-02-02"),
        target,
        (
            EvidenceRequest(
                "ticker_replacement", ("2005", "CEGV", "2022-01-19", "2022-02-01")
            ),
        ),
    )

    assert len(facts) == 1
    payload = facts[0].payload_value()
    assert facts[0].kind == "ticker_replacement"
    assert payload["old_ticker"] == "CEGV"
    assert payload["new_ticker"] == "CEGVV"
    assert payload["replacement_reason"] == "known_alias_boundary_validation"
    assert [row["point"] for row in payload["validation"]] == ["start", "end"]
    endpoints = [item.endpoint for item in client.request_log]
    assert endpoints[:2] == [
        "/v3/reference/tickers/CEGVV",
        "/v3/reference/tickers/CEGVV",
    ]
    assert "/v2/aggs/ticker/CEG/range/1/day/2022-01-19/2022-02-01" in endpoints


def test_preferred_issue_name_match_rejects_different_series_and_coupon() -> None:
    assert (
        _same_preferred_issue_terms(
            "Federal Agricultural Mortgage Corporation 6.500% Non-Cumulative Preferred Stock, Series H",
            "Federal Agricultural Mortgage Corporation 5.875% Non-Cumulative Preferred Stock, Series A",
        )
        is False
    )


def test_massive_ticker_replacement_provider_can_prove_omitted_event_segment() -> None:
    target = TargetIdentity(
        ohlcv_series_id=46,
        composite_figi="BBG000TARGET",
        latest_ticker="ABAT",
        known_alias_tickers=("ABAT",),
    )

    class OmittedSegmentTransport:
        def __init__(self) -> None:
            self.urls: list[str] = []

        def get_json(self, url: str, *, timeout_seconds: float) -> HttpJsonResponse:
            self.urls.append(url)
            parsed = urlparse(url)
            if parsed.path.startswith("/v2/aggs/"):
                return HttpJsonResponse(
                    200, {"status": "OK", "resultsCount": 0, "results": []}
                )
            if parsed.path == "/v3/reference/tickers":
                return HttpJsonResponse(200, {"status": "OK", "results": []})
            return HttpJsonResponse(404, {"status": "NOT_FOUND", "results": None})

    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"),
        OmittedSegmentTransport(),
    )
    provider = MassiveTickerReplacementProvider(client)

    facts = provider.requested_facts(
        BackfillRequest(series_id=46, from_date="2023-09-11", to_date="2023-09-22"),
        target,
        (
            EvidenceRequest(
                "ticker_replacement", ("46", "ABML", "2023-09-11", "2023-09-20")
            ),
        ),
    )

    assert len(facts) == 1
    assert facts[0].kind == "omitted_segment"
    payload = facts[0].payload_value()
    assert payload["ticker"] == "ABML"
    assert payload["proof"]["bar_probe"]["bar_count"] == 0


def test_massive_ticker_replacement_ignores_incompatible_same_cik_scan_alias() -> None:
    target = TargetIdentity(
        ohlcv_series_id=85,
        cik="0001814287",
        latest_ticker="ABXL",
        latest_primary_exchange="XNYS",
        security_type="SP",
        company_name="Abacus Global Management, Inc. 9.875% Fixed Rate Senior Notes due 2028",
        known_alias_tickers=("ABXL",),
    )

    class SameCikWarrantScanTransport:
        def __init__(self) -> None:
            self.urls: list[str] = []

        def get_json(self, url: str, *, timeout_seconds: float) -> HttpJsonResponse:
            self.urls.append(url)
            parsed = urlparse(url)
            if parsed.path == "/v3/reference/tickers":
                return HttpJsonResponse(
                    200,
                    {
                        "status": "OK",
                        "results": [
                            {
                                "ticker": "ABLLW",
                                "name": "Abacus Global Management, Inc. Warrant",
                                "cik": "0001814287",
                                "type": "WARRANT",
                                "primary_exchange": "XNAS",
                            }
                        ],
                    },
                )
            if "/v2/aggs/ticker/ABLLW/" in parsed.path:
                return HttpJsonResponse(
                    200,
                    {
                        "status": "OK",
                        "resultsCount": 1,
                        "results": [
                            {"t": 1700611200000, "o": 1, "h": 1, "l": 1, "c": 1, "v": 1}
                        ],
                    },
                )
            if parsed.path.startswith("/v2/aggs/"):
                return HttpJsonResponse(
                    200, {"status": "OK", "resultsCount": 0, "results": []}
                )
            return HttpJsonResponse(404, {"status": "NOT_FOUND", "results": None})

    transport = SameCikWarrantScanTransport()
    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"),
        transport,
    )
    provider = MassiveTickerReplacementProvider(client)

    facts = provider.requested_facts(
        BackfillRequest(series_id=85, from_date="2023-11-22", to_date="2025-12-29"),
        target,
        (
            EvidenceRequest(
                "ticker_replacement", ("85", "ABLL", "2023-11-22", "2025-12-29")
            ),
        ),
    )

    assert len(facts) == 1
    assert facts[0].kind == "omitted_segment"
    payload = facts[0].payload_value()
    assert payload["ticker"] == "ABLL"
    assert payload["proof"]["end_identity_scan"]["match_count"] == 1
    assert not any("/v2/aggs/ticker/ABLLW/" in url for url in transport.urls)


def test_massive_ticker_replacement_provider_derives_bar_backed_known_alias() -> None:
    target = TargetIdentity(
        ohlcv_series_id=375,
        composite_figi="BBG010W1PNX6",
        latest_ticker="AISPW",
        known_alias_tickers=("AISPW",),
    )

    class BarBackedReplacementTransport:
        def __init__(self) -> None:
            self.urls: list[str] = []

        def get_json(self, url: str, *, timeout_seconds: float) -> HttpJsonResponse:
            self.urls.append(url)
            parsed = urlparse(url)
            if "/v2/aggs/ticker/AISPW/" in parsed.path:
                return HttpJsonResponse(
                    200,
                    {
                        "status": "OK",
                        "resultsCount": 2,
                        "results": [
                            {
                                "t": 1703203200000,
                                "o": 1,
                                "h": 1,
                                "l": 1,
                                "c": 1,
                                "v": 1,
                            },
                            {
                                "t": 1703808000000,
                                "o": 1,
                                "h": 1,
                                "l": 1,
                                "c": 1,
                                "v": 1,
                            },
                        ],
                    },
                )
            ticker = parsed.path.rsplit("/", 1)[-1]
            as_of_date = parse_qs(parsed.query).get("date", [""])[0]
            if ticker == "AISPW" and as_of_date in {"2023-12-22", "2023-12-29"}:
                return HttpJsonResponse(
                    200,
                    {
                        "status": "OK",
                        "results": {
                            "ticker": "AISPW",
                            "composite_figi": "BBG010W1PNX6",
                        },
                    },
                )
            return HttpJsonResponse(404, {"status": "NOT_FOUND", "results": None})

    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"),
        BarBackedReplacementTransport(),
    )
    provider = MassiveTickerReplacementProvider(client)

    facts = provider.requested_facts(
        BackfillRequest(series_id=375, from_date="2023-12-20", to_date="2023-12-29"),
        target,
        (
            EvidenceRequest(
                "ticker_replacement", ("375", "AISP", "2023-12-20", "2023-12-29")
            ),
        ),
    )

    assert len(facts) == 1
    payload = facts[0].payload_value()
    assert payload["old_ticker"] == "AISP"
    assert payload["new_ticker"] == "AISPW"
    assert payload["from_date"] == "2023-12-22"
    assert payload["to_date"] == "2023-12-29"
    assert (
        payload["replacement_reason"]
        == "known_alias_target_valid_bar_window_inside_invalid_event_segment"
    )


def test_massive_ticker_replacement_derives_component_symbol_alias() -> None:
    target = TargetIdentity(
        ohlcv_series_id=2723,
        cik="0001868419",
        composite_figi="BBG016PH4WS5",
        latest_ticker="CYCUW",
        security_type="WARRANT",
        known_alias_tickers=("CYCUW",),
    )

    class ComponentSymbolTransport:
        def __init__(self) -> None:
            self.urls: list[str] = []

        def get_json(self, url: str, *, timeout_seconds: float) -> HttpJsonResponse:
            self.urls.append(url)
            parsed = urlparse(url)
            if parsed.path.startswith("/v2/aggs/"):
                if "/v2/aggs/ticker/WAVS/" in parsed.path:
                    return HttpJsonResponse(
                        200,
                        {
                            "status": "OK",
                            "resultsCount": 1,
                            "results": [{"t": 1649635200000, "c": 1}],
                        },
                    )
                return HttpJsonResponse(
                    200, {"status": "OK", "resultsCount": 0, "results": []}
                )
            ticker = parsed.path.rsplit("/", 1)[-1]
            if ticker == "WAVSW":
                return HttpJsonResponse(
                    200,
                    {
                        "status": "OK",
                        "results": {
                            "ticker": "WAVSW",
                            "name": "Western Acquisition Ventures Corp. Warrant",
                            "type": "WARRANT",
                            "composite_figi": "BBG016PH4WS5",
                        },
                    },
                )
            if ticker == "WAVS":
                return HttpJsonResponse(
                    200,
                    {
                        "status": "OK",
                        "results": {
                            "ticker": "WAVS",
                            "name": "Western Acquisition Ventures Corp. Common Stock",
                            "type": "CS",
                            "composite_figi": "BBG0133V46L9",
                        },
                    },
                )
            return HttpJsonResponse(404, {"status": "NOT_FOUND", "results": None})

    transport = ComponentSymbolTransport()
    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"),
        transport,
    )
    provider = MassiveTickerReplacementProvider(client)

    facts = provider.requested_facts(
        BackfillRequest(series_id=2723, from_date="2022-04-11", to_date="2025-02-18"),
        target,
        (
            EvidenceRequest(
                "ticker_replacement", ("2723", "WAVS", "2022-04-11", "2025-02-17")
            ),
        ),
    )

    assert len(facts) == 1
    payload = facts[0].payload_value()
    assert facts[0].kind == "ticker_replacement"
    assert payload["old_ticker"] == "WAVS"
    assert payload["new_ticker"] == "WAVSW"
    assert payload["replacement_reason"] == "known_alias_boundary_validation"
    assert any("/v3/reference/tickers/WAVSW" in url for url in transport.urls)


def test_massive_ticker_replacement_splits_temporary_event_ticker_back_to_current_alias() -> (
    None
):
    target = TargetIdentity(
        ohlcv_series_id=4126,
        cik="0001889123",
        composite_figi="BBG0142PTRC8",
        share_class_figi="BBG0142PTS63",
        latest_ticker="FLD",
        security_type="CS",
        known_alias_tickers=("FLD",),
    )

    class TemporaryEventTickerTransport:
        def get_json(self, url: str, *, timeout_seconds: float) -> HttpJsonResponse:
            parsed = urlparse(url)
            if parsed.path.startswith("/v2/aggs/ticker/FLDD/"):
                return HttpJsonResponse(
                    200,
                    {
                        "status": "OK",
                        "resultsCount": 2,
                        "results": [
                            {
                                "t": 1734480000000,
                                "o": 1,
                                "h": 1,
                                "l": 1,
                                "c": 1,
                                "v": 1,
                            },
                            {
                                "t": 1739836800000,
                                "o": 1,
                                "h": 1,
                                "l": 1,
                                "c": 1,
                                "v": 1,
                            },
                        ],
                    },
                )
            if parsed.path.startswith("/v2/aggs/ticker/FLD/"):
                return HttpJsonResponse(
                    200,
                    {
                        "status": "OK",
                        "resultsCount": 3,
                        "results": [
                            {
                                "t": 1734480000000,
                                "o": 1,
                                "h": 1,
                                "l": 1,
                                "c": 1,
                                "v": 1,
                            },
                            {
                                "t": 1739923200000,
                                "o": 1,
                                "h": 1,
                                "l": 1,
                                "c": 1,
                                "v": 1,
                            },
                            {
                                "t": 1778198400000,
                                "o": 1,
                                "h": 1,
                                "l": 1,
                                "c": 1,
                                "v": 1,
                            },
                        ],
                    },
                )
            ticker = parsed.path.rsplit("/", 1)[-1]
            as_of_date = parse_qs(parsed.query).get("date", [""])[0]
            if ticker == "FLDD" and as_of_date == "2026-05-08":
                return HttpJsonResponse(404, {"status": "NOT_FOUND", "results": None})
            if ticker == "FLD" and as_of_date == "2026-05-08":
                return HttpJsonResponse(
                    200,
                    {
                        "status": "OK",
                        "results": {
                            "ticker": "FLD",
                            "name": "Fold Holdings, Inc. Class A Common Stock",
                            "type": "CS",
                            "cik": "0001889123",
                            "composite_figi": "BBG0142PTRC8",
                            "share_class_figi": "BBG0142PTS63",
                            "list_date": "2025-02-19",
                        },
                    },
                )
            if ticker in {"FLD", "FLDD"}:
                return HttpJsonResponse(
                    200,
                    {
                        "status": "OK",
                        "results": {
                            "ticker": ticker,
                            "name": "Fold Holdings, Inc. Class A Common Stock",
                            "type": "CS",
                            "cik": "0001889123",
                            "composite_figi": "BBG0142PTRC8",
                            "share_class_figi": "BBG0142PTS63",
                        },
                    },
                )
            return HttpJsonResponse(404, {"status": "NOT_FOUND", "results": None})

    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"),
        TemporaryEventTickerTransport(),
    )
    provider = MassiveTickerReplacementProvider(client)

    facts = provider.requested_facts(
        BackfillRequest(series_id=4126, from_date="2024-08-02", to_date="2026-05-08"),
        target,
        (
            EvidenceRequest(
                "ticker_replacement", ("4126", "FLDD", "2024-12-18", "2026-05-08")
            ),
        ),
    )

    assert len(facts) == 2
    assert [
        (
            fact.payload_value()["new_ticker"],
            fact.payload_value()["from_date"],
            fact.payload_value()["to_date"],
        )
        for fact in facts
    ] == [
        ("FLDD", "2024-12-18", "2025-02-18"),
        ("FLD", "2025-02-19", "2026-05-08"),
    ]
    assert (
        facts[1].payload_value()["replacement_reason"]
        == "event_ticker_then_current_alias_split"
    )


def test_massive_ticker_replacement_split_starts_after_market_holiday_gap() -> None:
    target = TargetIdentity(
        ohlcv_series_id=777,
        cik="0000000777",
        composite_figi="BBG000777777",
        share_class_figi="BBG000777778",
        latest_ticker="NEW",
        security_type="CS",
        known_alias_tickers=("NEW", "OLD"),
    )

    class HolidaySplitTransport:
        def get_json(self, url: str, *, timeout_seconds: float) -> HttpJsonResponse:
            parsed = urlparse(url)
            ticker = parsed.path.rsplit("/", 1)[-1]
            as_of_date = parse_qs(parsed.query).get("date", [""])[0]
            if parsed.path.startswith("/v2/aggs/ticker/OLD/"):
                return HttpJsonResponse(
                    200,
                    {
                        "status": "OK",
                        "resultsCount": 2,
                        "results": [
                            {
                                "t": int(
                                    dt.datetime(2026, 6, 30, tzinfo=dt.UTC).timestamp()
                                    * 1000
                                ),
                                "o": 1,
                                "h": 1,
                                "l": 1,
                                "c": 1,
                                "v": 1,
                            },
                            {
                                "t": int(
                                    dt.datetime(2026, 7, 2, tzinfo=dt.UTC).timestamp()
                                    * 1000
                                ),
                                "o": 1,
                                "h": 1,
                                "l": 1,
                                "c": 1,
                                "v": 1,
                            },
                        ],
                    },
                )
            if parsed.path.startswith("/v2/aggs/ticker/NEW/"):
                return HttpJsonResponse(
                    200,
                    {
                        "status": "OK",
                        "resultsCount": 1,
                        "results": [
                            {
                                "t": int(
                                    dt.datetime(2026, 7, 6, tzinfo=dt.UTC).timestamp()
                                    * 1000
                                ),
                                "o": 1,
                                "h": 1,
                                "l": 1,
                                "c": 1,
                                "v": 1,
                            }
                        ],
                    },
                )
            if ticker == "OLD" and as_of_date == "2026-07-06":
                return HttpJsonResponse(404, {"status": "NOT_FOUND", "results": None})
            if ticker in {"OLD", "NEW"}:
                return HttpJsonResponse(
                    200,
                    {
                        "status": "OK",
                        "results": {
                            "ticker": ticker,
                            "name": "Holiday Split Test Common Stock",
                            "type": "CS",
                            "cik": "0000000777",
                            "composite_figi": "BBG000777777",
                            "share_class_figi": "BBG000777778",
                            "list_date": "2026-07-06"
                            if ticker == "NEW"
                            else "2026-06-30",
                        },
                    },
                )
            return HttpJsonResponse(404, {"status": "NOT_FOUND", "results": None})

    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"),
        HolidaySplitTransport(),
    )
    provider = MassiveTickerReplacementProvider(client)

    facts = provider.requested_facts(
        BackfillRequest(series_id=777, from_date="2026-06-30", to_date="2026-07-06"),
        target,
        (
            EvidenceRequest(
                "ticker_replacement", ("777", "OLD", "2026-06-30", "2026-07-06")
            ),
        ),
    )

    assert [
        (
            fact.payload_value()["new_ticker"],
            fact.payload_value()["from_date"],
            fact.payload_value()["to_date"],
        )
        for fact in facts
    ] == [
        ("OLD", "2026-06-30", "2026-07-02"),
        ("NEW", "2026-07-06", "2026-07-06"),
    ]


def test_massive_ticker_replacement_accepts_missing_durable_start_when_end_and_bars_validate() -> (
    None
):
    target = TargetIdentity(
        ohlcv_series_id=375,
        composite_figi="BBG010W1PNX6",
        latest_ticker="AISPW",
        identity_status="permanent",
        security_type="WARRANT",
        known_alias_tickers=("AISPW",),
    )

    class MissingDurableStartTransport:
        def get_json(self, url: str, *, timeout_seconds: float) -> HttpJsonResponse:
            parsed = urlparse(url)
            if "/v2/aggs/ticker/AISPW/" in parsed.path:
                return HttpJsonResponse(
                    200,
                    {
                        "status": "OK",
                        "resultsCount": 2,
                        "results": [
                            {
                                "t": 1703221200000,
                                "o": 1,
                                "h": 1,
                                "l": 1,
                                "c": 1,
                                "v": 1,
                            },
                            {
                                "t": 1703826000000,
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
            if as_of_date == "2023-12-22":
                return HttpJsonResponse(
                    200,
                    {"status": "OK", "results": {"ticker": "AISPW", "type": "WARRANT"}},
                )
            return HttpJsonResponse(
                200,
                {
                    "status": "OK",
                    "results": {
                        "ticker": "AISPW",
                        "type": "WARRANT",
                        "composite_figi": "BBG010W1PNX6",
                    },
                },
            )

    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"),
        MissingDurableStartTransport(),
    )
    provider = MassiveTickerReplacementProvider(client)

    facts = provider.requested_facts(
        BackfillRequest(series_id=375, from_date="2023-12-22", to_date="2023-12-29"),
        target,
        (
            EvidenceRequest(
                "ticker_replacement", ("375", "AISP", "2023-12-22", "2023-12-29")
            ),
        ),
    )

    assert len(facts) == 1
    payload = facts[0].payload_value()
    assert payload["new_ticker"] == "AISPW"
    assert (
        payload["replacement_reason"]
        == "known_alias_start_reference_missing_durable_ids"
    )
    assert (
        payload["validation"][0]["match_reason"]
        == "start_reference_missing_durable_ids_bar_backed_end_match"
    )


def test_massive_ticker_replacement_accepts_historical_figi_rekey_bar_alias_current_end() -> (
    None
):
    target = TargetIdentity(
        ohlcv_series_id=7767,
        cik="0001865631",
        composite_figi="BBG017XGGR13",
        latest_ticker="NNAVW",
        identity_status="permanent",
        security_type="WARRANT",
        known_alias_tickers=("NNAVW",),
    )

    class HistoricalWarrantRekeyTransport:
        def get_json(self, url: str, *, timeout_seconds: float) -> HttpJsonResponse:
            parsed = urlparse(url)
            if "/v2/aggs/ticker/NNAVW/" in parsed.path:
                return HttpJsonResponse(
                    200,
                    {
                        "status": "OK",
                        "resultsCount": 2,
                        "results": [
                            {
                                "t": 1635480000000,
                                "o": 1,
                                "h": 1,
                                "l": 1,
                                "c": 1,
                                "v": 1,
                            },
                            {
                                "t": 1777852800000,
                                "o": 1,
                                "h": 1,
                                "l": 1,
                                "c": 1,
                                "v": 1,
                            },
                        ],
                    },
                )
            if "/v2/aggs/ticker/NNAV/" in parsed.path:
                return HttpJsonResponse(
                    200, {"status": "OK", "resultsCount": 0, "results": []}
                )
            ticker = parsed.path.rsplit("/", 1)[-1]
            as_of_date = parse_qs(parsed.query).get("date", [""])[0]
            if ticker == "NNAV":
                return HttpJsonResponse(404, {"status": "NOT_FOUND", "results": None})
            if ticker == "NNAVW" and as_of_date == "2021-10-29":
                return HttpJsonResponse(
                    200,
                    {
                        "status": "OK",
                        "results": {
                            "ticker": "NNAVW",
                            "name": "NextNav Inc. Warrant",
                            "composite_figi": "BBG00Y1D4117",
                            "primary_exchange": "XNAS",
                            "type": "WARRANT",
                        },
                    },
                )
            if ticker == "NNAVW" and as_of_date == "2026-05-04":
                return HttpJsonResponse(
                    200,
                    {
                        "status": "OK",
                        "results": {
                            "ticker": "NNAVW",
                            "name": "NextNav Inc. Warrant",
                            "cik": "0001865631",
                            "composite_figi": "BBG017XGGR13",
                            "primary_exchange": "XNAS",
                            "type": "WARRANT",
                        },
                    },
                )
            raise AssertionError(f"unexpected URL {url}")

    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"),
        HistoricalWarrantRekeyTransport(),
    )
    provider = MassiveTickerReplacementProvider(client)

    facts = provider.requested_facts(
        BackfillRequest(series_id=7767, from_date="2021-10-29", to_date="2026-05-04"),
        target,
        (
            EvidenceRequest(
                "ticker_replacement", ("7767", "NNAV", "2021-10-29", "2026-05-04")
            ),
        ),
    )

    assert len(facts) == 1
    payload = facts[0].payload_value()
    assert payload["old_ticker"] == "NNAV"
    assert payload["new_ticker"] == "NNAVW"
    assert (
        payload["replacement_reason"]
        == "known_alias_historical_figi_rekey_bar_alias_current_end"
    )
    assert payload["validation"][0]["match_reason"].startswith(
        "provider_historical_figi_rekey_bar_alias_current_end"
    )
    assert (
        payload["start_alias_identity_bridge"]["historical_reference"]["composite_figi"]
        == "BBG00Y1D4117"
    )


def test_massive_ticker_replacement_rejects_historical_rekey_when_event_ticker_has_bars() -> (
    None
):
    target = TargetIdentity(
        ohlcv_series_id=7767,
        cik="0001865631",
        composite_figi="BBG017XGGR13",
        latest_ticker="NNAVW",
        identity_status="permanent",
        security_type="WARRANT",
        known_alias_tickers=("NNAVW",),
    )

    class EventTickerBarsTransport:
        def get_json(self, url: str, *, timeout_seconds: float) -> HttpJsonResponse:
            parsed = urlparse(url)
            if "/v2/aggs/ticker/NNAVW/" in parsed.path:
                return HttpJsonResponse(
                    200,
                    {
                        "status": "OK",
                        "results": [
                            {
                                "t": 1635480000000,
                                "o": 1,
                                "h": 1,
                                "l": 1,
                                "c": 1,
                                "v": 1,
                            },
                            {
                                "t": 1777852800000,
                                "o": 1,
                                "h": 1,
                                "l": 1,
                                "c": 1,
                                "v": 1,
                            },
                        ],
                    },
                )
            if "/v2/aggs/ticker/NNAV/" in parsed.path:
                return HttpJsonResponse(
                    200,
                    {
                        "status": "OK",
                        "resultsCount": 1,
                        "results": [
                            {"t": 1635480000000, "o": 1, "h": 1, "l": 1, "c": 1, "v": 1}
                        ],
                    },
                )
            if parsed.path == "/v3/reference/tickers":
                return HttpJsonResponse(200, {"status": "OK", "results": []})
            ticker = parsed.path.rsplit("/", 1)[-1]
            as_of_date = parse_qs(parsed.query).get("date", [""])[0]
            if ticker == "NNAV":
                return HttpJsonResponse(404, {"status": "NOT_FOUND", "results": None})
            if ticker == "NNAVW" and as_of_date == "2021-10-29":
                return HttpJsonResponse(
                    200,
                    {
                        "status": "OK",
                        "results": {
                            "ticker": "NNAVW",
                            "name": "NextNav Inc. Warrant",
                            "composite_figi": "BBG00Y1D4117",
                            "primary_exchange": "XNAS",
                            "type": "WARRANT",
                        },
                    },
                )
            if ticker == "NNAVW" and as_of_date == "2026-05-04":
                return HttpJsonResponse(
                    200,
                    {
                        "status": "OK",
                        "results": {
                            "ticker": "NNAVW",
                            "name": "NextNav Inc. Warrant",
                            "cik": "0001865631",
                            "composite_figi": "BBG017XGGR13",
                            "primary_exchange": "XNAS",
                            "type": "WARRANT",
                        },
                    },
                )
            raise AssertionError(f"unexpected URL {url}")

    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"),
        EventTickerBarsTransport(),
    )
    provider = MassiveTickerReplacementProvider(client)

    facts = provider.requested_facts(
        BackfillRequest(series_id=7767, from_date="2021-10-29", to_date="2026-05-04"),
        target,
        (
            EvidenceRequest(
                "ticker_replacement", ("7767", "NNAV", "2021-10-29", "2026-05-04")
            ),
        ),
    )

    assert facts == ()
