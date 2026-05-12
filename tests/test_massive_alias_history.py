from __future__ import annotations

from backfill_test_support import *


def test_massive_alias_history_provider_derives_single_bar_backed_alias() -> None:
    target = TargetIdentity(
        ohlcv_series_id=989,
        composite_figi="BBG000BB07P9",
        share_class_figi="BBG001S5N9P3",
        latest_ticker="B",
        known_alias_tickers=("GOLD", "B"),
    )

    class AliasHistoryTransport:
        def __init__(self) -> None:
            self.urls: list[str] = []

        def get_json(self, url: str, *, timeout_seconds: float) -> HttpJsonResponse:
            self.urls.append(url)
            parsed = urlparse(url)
            if "/v2/aggs/ticker/GOLD/" in parsed.path:
                return HttpJsonResponse(
                    200,
                    {
                        "status": "OK",
                        "resultsCount": 1,
                        "results": [
                            {"t": 1746662400000, "o": 1, "h": 1, "l": 1, "c": 1, "v": 1}
                        ],
                    },
                )
            if "/v2/aggs/ticker/B/" in parsed.path:
                return HttpJsonResponse(
                    200, {"status": "OK", "resultsCount": 0, "results": []}
                )
            ticker = parsed.path.rsplit("/", 1)[-1]
            return HttpJsonResponse(
                200,
                {
                    "status": "OK",
                    "results": {
                        "ticker": ticker,
                        "composite_figi": "BBG000BB07P9",
                        "share_class_figi": "BBG001S5N9P3",
                    },
                },
            )

    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"),
        AliasHistoryTransport(),
    )
    provider = MassiveAliasHistoryProvider(client)

    facts = provider.requested_facts(
        BackfillRequest(series_id=989, from_date="2025-05-08", to_date="2025-05-09"),
        target,
        (EvidenceRequest("alias_history", ("989", "2025-05-08", "2025-05-09")),),
    )

    assert [fact.kind for fact in facts] == [
        "alias_history",
        "reference_boundary",
        "reference_boundary",
    ]
    span = facts[0].payload_value()["spans"][0]
    assert span["ticker"] == "GOLD"
    assert span["from_date"] == "2025-05-08"
    assert span["to_date"] == "2025-05-08"
    assert span["source"] == "massive.known_alias_pre_event_bar_validation"


def test_massive_alias_history_provider_can_probe_first_event_ticker() -> None:
    target = TargetIdentity(
        ohlcv_series_id=46,
        composite_figi="BBG004M1KJN5",
        share_class_figi="BBG004M1KJP3",
        latest_ticker="ABAT",
    )

    class EventTickerAliasTransport:
        def get_json(self, url: str, *, timeout_seconds: float) -> HttpJsonResponse:
            parsed = urlparse(url)
            if "/v2/aggs/ticker/ABML/" in parsed.path:
                return HttpJsonResponse(
                    200,
                    {
                        "status": "OK",
                        "resultsCount": 2,
                        "results": [
                            {
                                "t": int(
                                    dt.datetime(2021, 12, 31, tzinfo=dt.UTC).timestamp()
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
                                    dt.datetime(2023, 9, 8, tzinfo=dt.UTC).timestamp()
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
            if "/v2/aggs/ticker/ABAT/" in parsed.path:
                return HttpJsonResponse(
                    200, {"status": "OK", "resultsCount": 0, "results": []}
                )
            ticker = parsed.path.rsplit("/", 1)[-1]
            if ticker == "ABML":
                return HttpJsonResponse(
                    200,
                    {
                        "status": "OK",
                        "results": {
                            "ticker": "ABML",
                            "composite_figi": "BBG004M1KJN5",
                            "share_class_figi": "BBG004M1KJP3",
                        },
                    },
                )
            return HttpJsonResponse(404, {"status": "NOT_FOUND", "results": None})

    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"),
        EventTickerAliasTransport(),
    )
    provider = MassiveAliasHistoryProvider(client)

    facts = provider.requested_facts(
        BackfillRequest(series_id=46, from_date="2021-05-10", to_date="2026-05-09"),
        target,
        (EvidenceRequest("alias_history", ("46", "2021-05-10", "2023-09-11", "ABML")),),
    )

    assert [fact.kind for fact in facts] == [
        "alias_history",
        "reference_boundary",
        "reference_boundary",
    ]
    span = facts[0].payload_value()["spans"][0]
    assert span["ticker"] == "ABML"
    assert span["from_date"] == "2021-12-31"
    assert span["to_date"] == "2023-09-08"


def test_massive_alias_history_accepts_same_ticker_etp_historical_figi_rekey() -> None:
    target = TargetIdentity(
        ohlcv_series_id=36,
        cik="0001848758",
        composite_figi="BBG01QY0GMH9",
        share_class_figi="BBG01QY0GNH7",
        latest_ticker="AAPY",
        latest_primary_exchange="BATS",
        security_type="ETF",
        company_name="Kurv Yield Premium Strategy Apple (AAPL) ETF",
    )

    class SameTickerEtpRekeyTransport:
        def get_json(self, url: str, *, timeout_seconds: float) -> HttpJsonResponse:
            parsed = urlparse(url)
            if "/v2/aggs/ticker/AAPY/" in parsed.path:
                return HttpJsonResponse(
                    200,
                    {
                        "status": "OK",
                        "resultsCount": 3,
                        "results": [
                            {
                                "t": int(
                                    dt.datetime(2023, 10, 27, tzinfo=dt.UTC).timestamp()
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
                                    dt.datetime(2024, 6, 3, tzinfo=dt.UTC).timestamp()
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
                                    dt.datetime(2024, 11, 15, tzinfo=dt.UTC).timestamp()
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
            return HttpJsonResponse(
                200,
                {
                    "status": "OK",
                    "results": {
                        "ticker": "AAPY",
                        "name": "Kurv Yield Premium Strategy Apple (AAPL) ETF",
                        "active": True,
                        "cik": parse_qs(parsed.query).get("date", [""])[0]
                        != "2023-10-27"
                        and "0001848758"
                        or None,
                        "composite_figi": "BBG01JXKG7J3",
                        "share_class_figi": "BBG01JXKG8C8",
                        "type": "ETS",
                        "primary_exchange": "BATS",
                    },
                },
            )

    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"),
        SameTickerEtpRekeyTransport(),
    )
    provider = MassiveAliasHistoryProvider(client)

    facts = provider.requested_facts(
        BackfillRequest(series_id=36, from_date="2021-05-10", to_date="2026-05-08"),
        target,
        (EvidenceRequest("alias_history", ("36", "2021-05-10", "2024-11-18", "AAPY")),),
    )

    assert [fact.kind for fact in facts] == [
        "alias_history",
        "reference_boundary",
        "reference_boundary",
    ]
    span = facts[0].payload_value()["spans"][0]
    assert span["ticker"] == "AAPY"
    assert span["from_date"] == "2023-10-27"
    assert span["to_date"] == "2024-11-15"
    assert span["validation"][0]["match_reason"].startswith(
        "provider_historical_figi_rekey_same_ticker_name_exchange_type_missing_cik"
    )
    assert span["validation"][1]["match_reason"].startswith(
        "provider_historical_figi_rekey_same_ticker_cik_type"
    )


def test_massive_alias_history_uses_previous_market_session_before_event() -> None:
    class HolidayGapTransport:
        def __init__(self) -> None:
            self.urls: list[str] = []

        def get_json(self, url: str, *, timeout_seconds: float) -> HttpJsonResponse:
            self.urls.append(url)
            parsed = urlparse(url)
            if "/v2/aggs/ticker/AAA/" in parsed.path:
                return HttpJsonResponse(
                    200, {"status": "OK", "resultsCount": 0, "results": []}
                )
            return HttpJsonResponse(404, {"status": "NOT_FOUND", "results": None})

    transport = HolidayGapTransport()
    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"),
        transport,
    )
    provider = MassiveAliasHistoryProvider(client)

    facts = provider.requested_facts(
        BackfillRequest(series_id=1, from_date="2026-07-02", to_date="2026-07-06"),
        TargetIdentity(ohlcv_series_id=1, latest_ticker="AAA"),
        (EvidenceRequest("alias_history", ("1", "2026-07-02", "2026-07-06", "AAA")),),
    )

    assert facts == ()
    assert any(
        "/v2/aggs/ticker/AAA/range/1/day/2026-07-02/2026-07-02" in url
        for url in transport.urls
    )
    assert not any("2026-07-05" in url for url in transport.urls)


def test_massive_alias_history_uses_current_list_date_for_same_ticker_pre_event_bar() -> (
    None
):
    target = TargetIdentity(
        ohlcv_series_id=61,
        cik="0001957489",
        latest_ticker="ABLV",
        latest_primary_exchange="XNAS",
        security_type="CS",
        company_name="Able View Global Inc. Class B Ordinary Shares",
    )

    class CurrentListDateTransport:
        def get_json(self, url: str, *, timeout_seconds: float) -> HttpJsonResponse:
            parsed = urlparse(url)
            if "/v2/aggs/ticker/ABLV/" in parsed.path:
                return HttpJsonResponse(
                    200,
                    {
                        "status": "OK",
                        "resultsCount": 1,
                        "results": [
                            {
                                "t": int(
                                    dt.datetime(2023, 8, 18, tzinfo=dt.UTC).timestamp()
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
            as_of_date = parse_qs(parsed.query).get("date", [""])[0]
            if as_of_date == "2026-05-08":
                return HttpJsonResponse(
                    200,
                    {
                        "status": "OK",
                        "results": {
                            "ticker": "ABLV",
                            "name": "Able View Global Inc. Class B Ordinary Shares",
                            "active": True,
                            "cik": "0001957489",
                            "type": "CS",
                            "primary_exchange": "XNAS",
                            "list_date": "2023-08-18",
                        },
                    },
                )
            return HttpJsonResponse(404, {"status": "NOT_FOUND", "results": None})

    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"),
        CurrentListDateTransport(),
    )
    provider = MassiveAliasHistoryProvider(client)

    facts = provider.requested_facts(
        BackfillRequest(series_id=61, from_date="2021-05-10", to_date="2026-05-08"),
        target,
        (EvidenceRequest("alias_history", ("61", "2021-05-10", "2023-08-21", "ABLV")),),
    )

    assert [fact.kind for fact in facts] == [
        "alias_history",
        "reference_boundary",
        "reference_boundary",
    ]
    span = facts[0].payload_value()["spans"][0]
    assert span["ticker"] == "ABLV"
    assert span["from_date"] == "2023-08-18"
    assert span["to_date"] == "2023-08-18"
    assert span["source"] == "massive.current_reference_list_date_bar_window"
    assert (
        span["validation"][0]["match_reason"]
        == "current_reference_list_date_same_ticker_bar_window"
    )


def test_massive_alias_history_accepts_same_ticker_successor_cik_rollover() -> None:
    target = TargetIdentity(
        ohlcv_series_id=814,
        cik="0002081043",
        composite_figi="BBG0107FR3K9",
        share_class_figi="BBG0107FR4D5",
        latest_ticker="ATAI",
        latest_primary_exchange="XNAS",
        security_type="CS",
        company_name="AtaiBeckley Inc. Common Stock",
    )

    class SameTickerSuccessorTransport:
        def get_json(self, url: str, *, timeout_seconds: float) -> HttpJsonResponse:
            parsed = urlparse(url)
            if "/v2/aggs/ticker/ATAI/" in parsed.path:
                return HttpJsonResponse(
                    200,
                    {
                        "status": "OK",
                        "resultsCount": 2,
                        "results": [
                            {
                                "t": int(
                                    dt.datetime(2021, 6, 18, tzinfo=dt.UTC).timestamp()
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
                                    dt.datetime(2025, 12, 31, tzinfo=dt.UTC).timestamp()
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
            as_of_date = parse_qs(parsed.query).get("date", [""])[0]
            if as_of_date == "2026-01-02":
                return HttpJsonResponse(
                    200,
                    {
                        "status": "OK",
                        "results": {
                            "ticker": "ATAI",
                            "name": "AtaiBeckley Inc. Common Stock",
                            "cik": "0001840904",
                            "composite_figi": "BBG0107FR3K9",
                            "share_class_figi": "BBG0107FR4D5",
                            "type": "CS",
                            "primary_exchange": "XNAS",
                        },
                    },
                )
            name = (
                "ATAI Life Sciences N.V. Common Shares"
                if as_of_date == "2021-06-18"
                else "Atai Beckley N.V Common Shares"
            )
            return HttpJsonResponse(
                200,
                {
                    "status": "OK",
                    "results": {
                        "ticker": "ATAI",
                        "name": name,
                        "cik": "0001840904",
                        "type": "CS",
                        "primary_exchange": "XNAS",
                    },
                },
            )

    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"),
        SameTickerSuccessorTransport(),
    )
    provider = MassiveAliasHistoryProvider(client)

    facts = provider.requested_facts(
        BackfillRequest(series_id=814, from_date="2021-05-04", to_date="2026-05-08"),
        target,
        (
            EvidenceRequest(
                "alias_history", ("814", "2021-05-04", "2026-01-02", "ATAI")
            ),
        ),
    )

    assert [fact.kind for fact in facts] == [
        "alias_history",
        "reference_boundary",
        "reference_boundary",
    ]
    span = facts[0].payload_value()["spans"][0]
    assert span["ticker"] == "ATAI"
    assert span["from_date"] == "2021-06-18"
    assert span["to_date"] == "2025-12-31"
    assert span["source"] == "massive.same_ticker_successor_bar_window"
    assert (
        span["validation"][0]["match_reason"]
        == "same_ticker_successor_cik_rollover_bar_window"
    )


def test_massive_alias_history_provider_trims_to_first_target_valid_bar() -> None:
    target = TargetIdentity(
        ohlcv_series_id=13255,
        composite_figi="BBG01BX1BLQ8",
        latest_ticker="ECDAW",
    )

    class FirstValidAliasTransport:
        def get_json(self, url: str, *, timeout_seconds: float) -> HttpJsonResponse:
            parsed = urlparse(url)
            if "/v2/aggs/ticker/ECDAW/" in parsed.path:
                return HttpJsonResponse(
                    200,
                    {
                        "status": "OK",
                        "resultsCount": 2,
                        "results": [
                            {
                                "t": 1706659200000,
                                "o": 1,
                                "h": 1,
                                "l": 1,
                                "c": 1,
                                "v": 1,
                            },
                            {
                                "t": 1706745600000,
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
            if as_of_date == "2024-01-31":
                return HttpJsonResponse(
                    200,
                    {
                        "status": "OK",
                        "results": {
                            "ticker": "ECDAW",
                            "type": "WARRANT",
                        },
                    },
                )
            return HttpJsonResponse(
                200,
                {
                    "status": "OK",
                    "results": {
                        "ticker": "ECDAW",
                        "composite_figi": "BBG01BX1BLQ8",
                        "type": "WARRANT",
                    },
                },
            )

    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"),
        FirstValidAliasTransport(),
    )
    provider = MassiveAliasHistoryProvider(client)

    facts = provider.requested_facts(
        BackfillRequest(series_id=13255, from_date="2024-01-01", to_date="2024-02-05"),
        target,
        (EvidenceRequest("alias_history", ("13255", "2024-01-01", "2024-02-05")),),
    )

    assert [fact.kind for fact in facts] == [
        "alias_history",
        "reference_boundary",
        "reference_boundary",
    ]
    span = facts[0].payload_value()["spans"][0]
    assert span["ticker"] == "ECDAW"
    assert span["from_date"] == "2024-02-01"
    assert span["to_date"] == "2024-02-01"
    assert span["source"] == "massive.known_alias_first_target_valid_bar_window"
    assert span["validation"][0]["match_reason"] == "composite_figi_match"
    assert (
        span["validation"][0]["boundary_search"]["algorithm"]
        == "anchored_final_suffix_lower_bound"
    )


def test_massive_alias_history_provider_finds_deep_valid_suffix_without_linear_scan() -> (
    None
):
    target = TargetIdentity(
        ohlcv_series_id=3, composite_figi="BBG01B0JRCS6", latest_ticker="AAA"
    )
    first_bar = dt.date(2021, 5, 10)
    transition = first_bar + dt.timedelta(days=330)
    last_bar = first_bar + dt.timedelta(days=399)
    event_date = last_bar + dt.timedelta(days=1)

    class DeepValidSuffixTransport:
        def __init__(self) -> None:
            self.urls: list[str] = []

        def get_json(self, url: str, *, timeout_seconds: float) -> HttpJsonResponse:
            self.urls.append(url)
            parsed = urlparse(url)
            if "/v2/aggs/ticker/AAA/" in parsed.path:
                return HttpJsonResponse(
                    200,
                    {
                        "status": "OK",
                        "resultsCount": 400,
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

    transport = DeepValidSuffixTransport()
    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"),
        transport,
    )
    provider = MassiveAliasHistoryProvider(client)

    facts = provider.requested_facts(
        BackfillRequest(
            series_id=3, from_date="2021-05-04", to_date=event_date.isoformat()
        ),
        target,
        (
            EvidenceRequest(
                "alias_history", ("3", "2021-05-04", event_date.isoformat(), "AAA")
            ),
        ),
    )

    reference_detail_calls = [
        url
        for url in transport.urls
        if urlparse(url).path == "/v3/reference/tickers/AAA"
    ]
    span = facts[0].payload_value()["spans"][0]

    assert span["ticker"] == "AAA"
    assert span["from_date"] == transition.isoformat()
    assert span["to_date"] == last_bar.isoformat()
    assert span["source"] == "massive.known_alias_first_target_valid_bar_window"
    assert (
        span["validation"][0]["boundary_search"]["right_anchor_date"]
        == last_bar.isoformat()
    )
    assert span["validation"][0]["boundary_search"]["seeded_dates"] == [
        last_bar.isoformat()
    ]
    assert len(reference_detail_calls) < 20
    assert (
        sum(
            parse_qs(urlparse(url).query).get("date", [""])[0] == last_bar.isoformat()
            for url in reference_detail_calls
        )
        == 1
    )


def test_massive_alias_history_suffix_search_matches_prior_linear_result_inside_cap() -> (
    None
):
    target = TargetIdentity(
        ohlcv_series_id=3, composite_figi="BBG01B0JRCS6", latest_ticker="AAA"
    )
    first_bar = dt.date(2021, 5, 10)
    transition = first_bar + dt.timedelta(days=36)
    last_bar = first_bar + dt.timedelta(days=119)
    event_date = last_bar + dt.timedelta(days=1)

    class LegacyComparableAliasTransport:
        def __init__(self) -> None:
            self.urls: list[str] = []

        def get_json(self, url: str, *, timeout_seconds: float) -> HttpJsonResponse:
            self.urls.append(url)
            parsed = urlparse(url)
            if "/v2/aggs/ticker/AAA/" in parsed.path:
                return HttpJsonResponse(
                    200,
                    {
                        "status": "OK",
                        "resultsCount": 120,
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

    transport = LegacyComparableAliasTransport()
    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"),
        transport,
    )
    provider = MassiveAliasHistoryProvider(client)

    facts = provider.requested_facts(
        BackfillRequest(
            series_id=3, from_date="2021-05-04", to_date=event_date.isoformat()
        ),
        target,
        (
            EvidenceRequest(
                "alias_history", ("3", "2021-05-04", event_date.isoformat(), "AAA")
            ),
        ),
    )

    reference_detail_calls = [
        url
        for url in transport.urls
        if urlparse(url).path == "/v3/reference/tickers/AAA"
    ]
    span = facts[0].payload_value()["spans"][0]

    assert span["from_date"] == transition.isoformat()
    assert span["to_date"] == last_bar.isoformat()
    assert (
        span["validation"][0]["boundary_search"]["candidate_date"]
        == transition.isoformat()
    )
    assert len(reference_detail_calls) < 20


def test_massive_alias_history_provider_derives_non_overlapping_bar_backed_aliases() -> (
    None
):
    target = TargetIdentity(
        ohlcv_series_id=12984,
        composite_figi="BBG005D7PF34",
        share_class_figi="BBG005D7PF43",
        latest_ticker="CDAY",
        known_alias_tickers=("CDAY", "DAY"),
    )

    class DayAliasTransport:
        def __init__(self) -> None:
            self.urls: list[str] = []

        def get_json(self, url: str, *, timeout_seconds: float) -> HttpJsonResponse:
            self.urls.append(url)
            parsed = urlparse(url)
            if "/v2/aggs/ticker/CDAY/" in parsed.path:
                return HttpJsonResponse(
                    200,
                    {
                        "status": "OK",
                        "resultsCount": 1,
                        "results": [
                            {"t": 1706659200000, "o": 1, "h": 1, "l": 1, "c": 1, "v": 1}
                        ],
                    },
                )
            if "/v2/aggs/ticker/DAY/" in parsed.path:
                return HttpJsonResponse(
                    200,
                    {
                        "status": "OK",
                        "resultsCount": 1,
                        "results": [
                            {"t": 1706745600000, "o": 1, "h": 1, "l": 1, "c": 1, "v": 1}
                        ],
                    },
                )
            ticker = parsed.path.rsplit("/", 1)[-1]
            return HttpJsonResponse(
                200,
                {
                    "status": "OK",
                    "results": {
                        "ticker": ticker,
                        "composite_figi": "BBG005D7PF34",
                        "share_class_figi": "BBG005D7PF43",
                    },
                },
            )

    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"),
        DayAliasTransport(),
    )
    provider = MassiveAliasHistoryProvider(client)

    facts = provider.requested_facts(
        BackfillRequest(series_id=12984, from_date="2024-01-31", to_date="2024-02-01"),
        target,
        (EvidenceRequest("alias_history", ("12984", "2024-01-31", "2024-02-02")),),
    )

    spans = facts[0].payload_value()["spans"]
    assert [span["ticker"] for span in spans] == ["CDAY", "DAY"]
    assert [(span["from_date"], span["to_date"]) for span in spans] == [
        ("2024-01-31", "2024-01-31"),
        ("2024-02-01", "2024-02-01"),
    ]


def test_massive_alias_history_provider_skips_no_bar_alias_when_gap_has_no_market_sessions() -> (
    None
):
    target = TargetIdentity(
        ohlcv_series_id=2676,
        composite_figi="BBG000MZYM65",
        latest_ticker="CWBC",
        known_alias_tickers=("CVCY", "CWBC"),
    )

    class NoBarAliasTransport:
        def get_json(self, url: str, *, timeout_seconds: float) -> HttpJsonResponse:
            parsed = urlparse(url)
            if "/v2/aggs/" in parsed.path:
                return HttpJsonResponse(
                    200, {"status": "OK", "resultsCount": 0, "results": []}
                )
            ticker = parsed.path.rsplit("/", 1)[-1]
            if ticker == "CVCY":
                return HttpJsonResponse(
                    200,
                    {
                        "status": "OK",
                        "results": {"ticker": "CVCY", "composite_figi": "BBG000MZYM65"},
                    },
                )
            return HttpJsonResponse(
                200,
                {
                    "status": "OK",
                    "results": {"ticker": "CWBC", "composite_figi": "BBG000OTHER"},
                },
            )

    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"),
        NoBarAliasTransport(),
    )
    provider = MassiveAliasHistoryProvider(client)

    facts = provider.requested_facts(
        BackfillRequest(series_id=2676, from_date="2024-03-29", to_date="2024-04-02"),
        target,
        (EvidenceRequest("alias_history", ("2676", "2024-03-29", "2024-04-01")),),
    )

    assert facts == ()


def test_massive_alias_history_provider_rejects_no_bar_alias_with_different_security_type() -> (
    None
):
    target = TargetIdentity(
        ohlcv_series_id=2362,
        cik="0000927628",
        latest_ticker="COFpL",
        known_alias_tickers=("COF", "COFpL"),
        identity_status="provisional",
        security_type="PFD",
    )

    class PreferredAliasTransport:
        def get_json(self, url: str, *, timeout_seconds: float) -> HttpJsonResponse:
            parsed = urlparse(url)
            if "/v2/aggs/" in parsed.path:
                return HttpJsonResponse(
                    200, {"status": "OK", "resultsCount": 0, "results": []}
                )
            ticker = parsed.path.rsplit("/", 1)[-1]
            if ticker == "COF":
                return HttpJsonResponse(
                    200,
                    {
                        "status": "OK",
                        "results": {
                            "ticker": "COF",
                            "cik": "0000927628",
                            "type": "CS",
                        },
                    },
                )
            return HttpJsonResponse(404, {"status": "NOT_FOUND"})

    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"),
        PreferredAliasTransport(),
    )
    provider = MassiveAliasHistoryProvider(client)

    facts = provider.requested_facts(
        BackfillRequest(series_id=2362, from_date="2021-05-04", to_date="2021-05-06"),
        target,
        (
            EvidenceRequest(
                "alias_history", ("2362", "2021-05-04", "2021-05-06", "COF")
            ),
        ),
    )

    assert facts == ()


def test_massive_alias_history_provider_trims_same_ticker_identity_reuse_to_valid_prefix() -> (
    None
):
    target = TargetIdentity(
        ohlcv_series_id=13153,
        composite_figi="BBG000JF5K69",
        latest_ticker="CWBC",
        known_alias_tickers=("CWBC",),
    )

    class PrefixAliasTransport:
        def get_json(self, url: str, *, timeout_seconds: float) -> HttpJsonResponse:
            parsed = urlparse(url)
            if "/v2/aggs/ticker/CWBC/" in parsed.path:
                return HttpJsonResponse(
                    200,
                    {
                        "status": "OK",
                        "resultsCount": 3,
                        "results": [
                            {
                                "t": 1620345600000,
                                "o": 1,
                                "h": 1,
                                "l": 1,
                                "c": 1,
                                "v": 1,
                            },
                            {
                                "t": 1711584000000,
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
            as_of_date = parse_qs(parsed.query).get("date", [""])[0]
            figi = (
                "BBG000JF5K69"
                if as_of_date in {"2021-05-07", "2024-03-28"}
                else "BBG000MZYM65"
            )
            return HttpJsonResponse(
                200,
                {"status": "OK", "results": {"ticker": "CWBC", "composite_figi": figi}},
            )

    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"),
        PrefixAliasTransport(),
    )
    provider = MassiveAliasHistoryProvider(client)

    facts = provider.requested_facts(
        BackfillRequest(series_id=13153, from_date="2021-05-04", to_date="2026-05-04"),
        target,
        (EvidenceRequest("alias_history", ("13153", "2021-05-04", "2026-05-05")),),
    )

    span = facts[0].payload_value()["spans"][0]
    assert span["ticker"] == "CWBC"
    assert span["from_date"] == "2021-05-07"
    assert span["to_date"] == "2024-03-28"
    assert span["source"] == "massive.known_alias_target_valid_bar_window"
