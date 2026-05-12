from __future__ import annotations

from backfill_test_support import *
from stock_universe.providers.massive.reference_helpers import (
    _reference_is_conclusive_non_target,
)


def test_massive_coverage_accounting_provider_proves_absent_gap_and_terminal_no_bars() -> (
    None
):
    class CoverageTransport:
        def get_json(self, url: str, *, timeout_seconds: float) -> HttpJsonResponse:
            parsed = urlparse(url)
            if "/v2/aggs/" in parsed.path:
                return HttpJsonResponse(
                    200, {"status": "OK", "resultsCount": 0, "results": []}
                )
            if parsed.path == "/v3/reference/tickers":
                return HttpJsonResponse(200, {"status": "OK", "results": []})
            return HttpJsonResponse(404, {"status": "NOT_FOUND", "results": None})

    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"),
        CoverageTransport(),
    )
    provider = MassiveCoverageAccountingProvider(client)

    facts = provider.requested_facts(
        BackfillRequest(series_id=13133, from_date="2021-05-04", to_date="2026-05-04"),
        TargetIdentity(ohlcv_series_id=13133, latest_ticker="CTCXW"),
        (
            EvidenceRequest(
                "coverage_gap", ("13133", "XAGE", "2021-05-04", "2021-09-19")
            ),
            EvidenceRequest(
                "terminal_coverage", ("13133", "CTCX", "2025-03-08", "2026-05-04")
            ),
        ),
    )

    assert [fact.kind for fact in facts] == ["omitted_segment", "terminal_coverage"]
    assert facts[0].payload_value()["ticker"] == "XAGE"
    assert facts[1].payload_value()["ticker"] == "CTCX"


def test_reference_helper_treats_same_cik_other_preferred_series_as_non_target() -> (
    None
):
    target = TargetIdentity(
        ohlcv_series_id=289,
        latest_ticker="AGMpH",
        cik="0000845877",
        security_type="PFD",
        company_name="Federal Agricultural Mortgage Corporation 6.500% Non-Cumulative Preferred Stock, Series H",
    )
    snapshot = ReferenceSnapshot(
        ticker="AGMpA",
        as_of_date="2021-05-10",
        api_status="OK",
        response_ticker="AGMpA",
        cik="0000845877",
        security_type="PFD",
        raw={
            "ticker": "AGMpA",
            "name": "Federal Agricultural Mortgage Corporation 5.875% Non-Cumulative Preferred Stock, Series A",
        },
    )

    assert _reference_is_conclusive_non_target(target, snapshot) is True


def test_massive_coverage_provider_omits_no_bar_gap_with_one_target_boundary() -> None:
    target = TargetIdentity(
        ohlcv_series_id=5136, composite_figi="BBG000BF3BG8", latest_ticker="HHS"
    )

    class OneBoundaryGapTransport:
        def get_json(self, url: str, *, timeout_seconds: float) -> HttpJsonResponse:
            parsed = urlparse(url)
            if "/v2/aggs/" in parsed.path:
                return HttpJsonResponse(
                    200, {"status": "OK", "resultsCount": 0, "results": []}
                )
            if parsed.path == "/v3/reference/tickers":
                return HttpJsonResponse(200, {"status": "OK", "results": []})
            as_of_date = parse_qs(parsed.query).get("date", [""])[0]
            if as_of_date == "2021-09-23":
                return HttpJsonResponse(
                    200,
                    {
                        "status": "OK",
                        "results": {"ticker": "HRTH", "composite_figi": "BBG000BF3BG8"},
                    },
                )
            return HttpJsonResponse(404, {"status": "NOT_FOUND", "results": None})

    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"),
        OneBoundaryGapTransport(),
    )
    provider = MassiveCoverageAccountingProvider(client)

    facts = provider.requested_facts(
        BackfillRequest(series_id=5136, from_date="2021-09-23", to_date="2026-05-04"),
        target,
        (
            EvidenceRequest(
                "coverage_gap", ("5136", "HRTH", "2021-09-23", "2021-11-30")
            ),
        ),
    )

    assert len(facts) == 1
    assert facts[0].kind == "omitted_segment"
    assert facts[0].source == "massive.non_downloadable_ticker_interval"


def test_massive_coverage_provider_omits_no_bar_gap_with_target_boundaries() -> None:
    target = TargetIdentity(
        ohlcv_series_id=10985,
        composite_figi="BBG000BKQQY9",
        share_class_figi="BBG001S7GCT6",
        latest_ticker="TLF",
    )

    class TargetNoBarGapTransport:
        def get_json(self, url: str, *, timeout_seconds: float) -> HttpJsonResponse:
            parsed = urlparse(url)
            if "/v2/aggs/" in parsed.path:
                return HttpJsonResponse(
                    200, {"status": "OK", "resultsCount": 0, "results": []}
                )
            if parsed.path == "/v3/reference/tickers":
                return HttpJsonResponse(200, {"status": "OK", "results": []})
            return HttpJsonResponse(
                200,
                {
                    "status": "OK",
                    "results": {
                        "ticker": "TLFA",
                        "name": "TANDY LEATHER FACTORY INC",
                        "type": "CS",
                        "composite_figi": "BBG000BKQQY9",
                        "share_class_figi": "BBG001S7GCT6",
                    },
                },
            )

    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"),
        TargetNoBarGapTransport(),
    )
    provider = MassiveCoverageAccountingProvider(client)

    facts = provider.requested_facts(
        BackfillRequest(series_id=10985, from_date="2021-05-04", to_date="2026-05-08"),
        target,
        (
            EvidenceRequest(
                "coverage_gap", ("10985", "TLFA", "2021-09-23", "2021-12-30")
            ),
        ),
    )

    assert len(facts) == 1
    assert facts[0].kind == "omitted_segment"
    assert (
        "validated the target at both interval boundaries"
        in facts[0].payload_value()["reason"]
    )


def test_massive_coverage_provider_omits_no_bar_gap_with_inconclusive_reference_boundary() -> (
    None
):
    target = TargetIdentity(
        ohlcv_series_id=1596,
        cik="0001487197",
        composite_figi="BBG001P11Y10",
        latest_ticker="BRFH",
        latest_primary_exchange="XNAS",
        security_type="CS",
        company_name="Barfresh Food Group Inc. Common Stock",
    )

    class InconclusiveReferenceGapTransport:
        def get_json(self, url: str, *, timeout_seconds: float) -> HttpJsonResponse:
            parsed = urlparse(url)
            if "/v2/aggs/" in parsed.path:
                return HttpJsonResponse(
                    200, {"status": "OK", "resultsCount": 0, "results": []}
                )
            if parsed.path == "/v3/reference/tickers":
                return HttpJsonResponse(200, {"status": "OK", "results": []})
            as_of_date = parse_qs(parsed.query).get("date", [""])[0]
            if as_of_date == "2021-12-28":
                return HttpJsonResponse(
                    200,
                    {
                        "status": "OK",
                        "results": {
                            "ticker": "BRFH",
                            "name": "BARFRESH FOOD GROUP INC",
                            "market": "otc",
                            "primary_exchange": "OTC Link",
                            "type": "CS",
                        },
                    },
                )
            return HttpJsonResponse(404, {"status": "NOT_FOUND", "results": None})

    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"),
        InconclusiveReferenceGapTransport(),
    )
    provider = MassiveCoverageAccountingProvider(client)

    facts = provider.requested_facts(
        BackfillRequest(series_id=1596, from_date="2021-05-04", to_date="2026-05-08"),
        target,
        (
            EvidenceRequest(
                "coverage_gap", ("1596", "BRFH", "2021-05-04", "2021-12-28")
            ),
        ),
    )

    assert len(facts) == 1
    assert facts[0].kind == "omitted_segment"
    assert facts[0].source == "massive.non_downloadable_ticker_interval"
    assert (
        "did not contain enough durable identifiers"
        in facts[0].payload_value()["reason"]
    )


def test_massive_coverage_provider_ignores_same_cik_other_preferred_series_scan_aliases() -> (
    None
):
    target = TargetIdentity(
        ohlcv_series_id=289,
        cik="0000845877",
        latest_ticker="AGMpH",
        security_type="PFD",
        company_name="Federal Agricultural Mortgage Corporation 6.500% Non-Cumulative Preferred Stock, Series H",
    )

    class OtherPreferredSeriesScanTransport:
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
                                "ticker": "AGMpA",
                                "name": "Federal Agricultural Mortgage Corporation 5.875% Ser A",
                                "type": "PFD",
                                "cik": "0000845877",
                            },
                            {
                                "ticker": "AGMpB",
                                "name": "Federal Agricultural Mtge Corp.",
                                "type": "PFD",
                                "cik": "0000845877",
                            },
                        ],
                    },
                )
            if parsed.path.startswith("/v2/aggs/"):
                if (
                    "/v2/aggs/ticker/AGMP/" in parsed.path
                    or "/v2/aggs/ticker/AGMpH/" in parsed.path
                ):
                    return HttpJsonResponse(
                        200, {"status": "OK", "resultsCount": 0, "results": []}
                    )
                raise AssertionError(
                    f"other preferred series should not be probed for bars: {url}"
                )
            return HttpJsonResponse(404, {"status": "NOT_FOUND", "results": None})

    transport = OtherPreferredSeriesScanTransport()
    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"),
        transport,
    )
    provider = MassiveCoverageAccountingProvider(client)

    facts = provider.requested_facts(
        BackfillRequest(series_id=289, from_date="2021-05-04", to_date="2026-05-08"),
        target,
        (EvidenceRequest("coverage_gap", ("289", "AGMP", "2021-05-04", "2025-08-21")),),
    )

    assert len(facts) == 1
    assert facts[0].kind == "omitted_segment"
    assert facts[0].source == "massive.non_downloadable_ticker_interval"
    assert facts[0].payload_value()["ticker"] == "AGMP"
    assert not any(
        "/v2/aggs/ticker/AGMpA/" in url or "/v2/aggs/ticker/AGMpB/" in url
        for url in transport.urls
    )


def test_massive_terminal_coverage_accepts_successor_cik_rollover_same_preferred_series() -> (
    None
):
    target = TargetIdentity(
        ohlcv_series_id=8773,
        cik="0002082866",
        latest_ticker="PNFPpA",
        latest_primary_exchange="XNYS",
        security_type="PFD",
        company_name="Pinnacle Financial Partners, Inc. Fixed-to-Floating Rate Non-Cumulative Perpetual Preferred Stock, Series A",
    )

    class SuccessorPreferredTransport:
        def get_json(self, url: str, *, timeout_seconds: float) -> HttpJsonResponse:
            parsed = urlparse(url)
            if parsed.path.startswith("/v2/aggs/"):
                return HttpJsonResponse(
                    200,
                    {
                        "status": "OK",
                        "resultsCount": 2,
                        "results": [
                            {
                                "t": 1767312000000,
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
            as_of_date = parse_qs(parsed.query).get("date", [""])[0]
            if as_of_date == "2026-01-02":
                return HttpJsonResponse(
                    200,
                    {
                        "status": "OK",
                        "results": {
                            "ticker": "PNFPpA",
                            "name": (
                                "Pinnacle Financial Partners, Inc. Fixed-to-Floating Rate Non-Cumulative "
                                "Perpetual Preferred Stock, Series A"
                            ),
                            "type": "PFD",
                            "primary_exchange": "XNYS",
                            "cik": "0001115055",
                        },
                    },
                )
            return HttpJsonResponse(
                200,
                {
                    "status": "OK",
                    "results": {
                        "ticker": "PNFPpA",
                        "name": (
                            "Pinnacle Financial Partners, Inc. Fixed-to-Floating Rate Non-Cumulative "
                            "Perpetual Preferred Stock, Series A"
                        ),
                        "type": "PFD",
                        "primary_exchange": "XNYS",
                        "cik": "0002082866",
                    },
                },
            )

    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"),
        SuccessorPreferredTransport(),
    )
    provider = MassiveCoverageAccountingProvider(client)

    facts = provider.requested_facts(
        BackfillRequest(series_id=8773, from_date="2021-05-04", to_date="2026-05-08"),
        target,
        (
            EvidenceRequest(
                "terminal_coverage", ("8773", "PNFPpA", "2021-05-04", "2026-05-08")
            ),
        ),
    )

    assert len(facts) == 1
    assert facts[0].kind == "handoff_segment"
    payload = facts[0].payload_value()
    assert payload["ticker"] == "PNFPpA"
    assert payload["from_date"] == "2026-01-02"
    assert payload["validation"][0]["match_reason"].startswith(
        "provider_successor_cik_rollover"
    )


def test_massive_coverage_provider_omits_intrabar_non_target_interval() -> None:
    target = TargetIdentity(
        ohlcv_series_id=9323, composite_figi="BBG01TYGV980", latest_ticker="REMG"
    )

    class IntrabarNonTargetTransport:
        def get_json(self, url: str, *, timeout_seconds: float) -> HttpJsonResponse:
            parsed = urlparse(url)
            if "/v2/aggs/" in parsed.path:
                return HttpJsonResponse(
                    200,
                    {
                        "status": "OK",
                        "resultsCount": 2,
                        "results": [
                            {
                                "t": 1641877200000,
                                "o": 1,
                                "h": 1,
                                "l": 1,
                                "c": 1,
                                "v": 1,
                            },
                            {
                                "t": 1681171200000,
                                "o": 1,
                                "h": 1,
                                "l": 1,
                                "c": 1,
                                "v": 1,
                            },
                        ],
                    },
                )
            if parsed.path == "/v3/reference/tickers":
                return HttpJsonResponse(200, {"status": "OK", "results": []})
            as_of_date = parse_qs(parsed.query).get("date", [""])[0]
            if as_of_date in {"2021-05-04", "2025-05-29"}:
                return HttpJsonResponse(404, {"status": "NOT_FOUND", "results": None})
            return HttpJsonResponse(
                200,
                {
                    "status": "OK",
                    "results": {"ticker": "REMG", "composite_figi": "BBG014H7H8C7"},
                },
            )

    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"),
        IntrabarNonTargetTransport(),
    )
    provider = MassiveCoverageAccountingProvider(client)

    facts = provider.requested_facts(
        BackfillRequest(series_id=9323, from_date="2021-05-04", to_date="2026-05-04"),
        target,
        (
            EvidenceRequest(
                "coverage_gap", ("9323", "REMG", "2021-05-04", "2025-05-29")
            ),
        ),
    )

    assert len(facts) == 1
    payload = facts[0].payload_value()
    assert payload["ticker"] == "REMG"
    assert payload["proof"]["last_bar_reference"]["composite_figi"] == "BBG014H7H8C7"


def test_massive_coverage_provider_omits_intrabar_non_target_with_one_missing_boundary() -> (
    None
):
    target = TargetIdentity(
        ohlcv_series_id=1234,
        cik="0001066764",
        composite_figi="BBG01Z5R6955",
        latest_ticker="BESS.WS",
        security_type="WARRANT",
    )

    class OneMissingBoundaryNonTargetTransport:
        def get_json(self, url: str, *, timeout_seconds: float) -> HttpJsonResponse:
            parsed = urlparse(url)
            if "/v2/aggs/ticker/BESS.WS/" in parsed.path:
                return HttpJsonResponse(
                    200, {"status": "OK", "resultsCount": 0, "results": []}
                )
            if "/v2/aggs/" in parsed.path:
                return HttpJsonResponse(
                    200,
                    {
                        "status": "OK",
                        "resultsCount": 2,
                        "results": [
                            {
                                "t": 1740960000000,
                                "o": 1,
                                "h": 1,
                                "l": 1,
                                "c": 1,
                                "v": 1,
                            },
                            {
                                "t": 1771459200000,
                                "o": 1,
                                "h": 1,
                                "l": 1,
                                "c": 1,
                                "v": 1,
                            },
                        ],
                    },
                )
            if parsed.path == "/v3/reference/tickers":
                return HttpJsonResponse(
                    200,
                    {
                        "status": "OK",
                        "results": [
                            {
                                "ticker": "BESS.WS",
                                "cik": "0001066764",
                                "type": "WARRANT",
                                "composite_figi": "BBG01Z5R6955",
                            }
                        ],
                    },
                )
            ticker = parsed.path.rsplit("/", 1)[-1]
            as_of_date = parse_qs(parsed.query).get("date", [""])[0]
            if ticker == "BESS" and as_of_date == "2021-05-04":
                return HttpJsonResponse(404, {"status": "NOT_FOUND", "results": None})
            if ticker == "BESS" and as_of_date == "2026-02-19":
                return HttpJsonResponse(
                    200,
                    {
                        "status": "OK",
                        "results": {
                            "ticker": "BESS",
                            "type": "CS",
                            "composite_figi": "BBG000DWCHR4",
                        },
                    },
                )
            if ticker == "BESS":
                return HttpJsonResponse(
                    200,
                    {
                        "status": "OK",
                        "results": {
                            "ticker": "BESS",
                            "type": "CS",
                            "composite_figi": "BBG000DWCHR4",
                        },
                    },
                )
            return HttpJsonResponse(404, {"status": "NOT_FOUND", "results": None})

    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"),
        OneMissingBoundaryNonTargetTransport(),
    )
    provider = MassiveCoverageAccountingProvider(client)

    facts = provider.requested_facts(
        BackfillRequest(series_id=1234, from_date="2021-05-04", to_date="2026-05-08"),
        target,
        (
            EvidenceRequest(
                "coverage_gap", ("1234", "BESS", "2021-05-04", "2026-02-19")
            ),
        ),
    )

    assert len(facts) == 1
    assert facts[0].kind == "omitted_segment"
    assert facts[0].source == "massive.non_downloadable_ticker_interval"


def test_massive_coverage_provider_ignores_same_cik_scan_alias_with_different_figi() -> (
    None
):
    target = TargetIdentity(
        ohlcv_series_id=2417,
        cik="0001839341",
        composite_figi="BBG01L5JY524",
        latest_ticker="CORZZ",
        security_type="WARRANT",
    )

    class DifferentFigiScanAliasTransport:
        def __init__(self) -> None:
            self.urls: list[str] = []

        def get_json(self, url: str, *, timeout_seconds: float) -> HttpJsonResponse:
            self.urls.append(url)
            parsed = urlparse(url)
            if "/v2/aggs/ticker/CORZW/" in parsed.path:
                return HttpJsonResponse(
                    200,
                    {
                        "status": "OK",
                        "resultsCount": 1,
                        "results": [
                            {"t": 1642636800000, "o": 1, "h": 1, "l": 1, "c": 1, "v": 1}
                        ],
                    },
                )
            if "/v2/aggs/" in parsed.path:
                return HttpJsonResponse(
                    200,
                    {
                        "status": "OK",
                        "resultsCount": 2,
                        "results": [
                            {
                                "t": 1642636800000,
                                "o": 1,
                                "h": 1,
                                "l": 1,
                                "c": 1,
                                "v": 1,
                            },
                            {
                                "t": 1672358400000,
                                "o": 1,
                                "h": 1,
                                "l": 1,
                                "c": 1,
                                "v": 1,
                            },
                        ],
                    },
                )
            if parsed.path == "/v3/reference/tickers":
                return HttpJsonResponse(
                    200,
                    {
                        "status": "OK",
                        "results": [
                            {
                                "ticker": "CORZW",
                                "cik": "0001839341",
                                "type": "WARRANT",
                                "composite_figi": "BBG00Z5H44X1",
                            }
                        ],
                    },
                )
            as_of_date = parse_qs(parsed.query).get("date", [""])[0]
            if as_of_date in {"2021-05-04", "2024-01-23"}:
                return HttpJsonResponse(404, {"status": "NOT_FOUND", "results": None})
            return HttpJsonResponse(
                200,
                {
                    "status": "OK",
                    "results": {
                        "ticker": "CORZ",
                        "type": "CS",
                        "composite_figi": "BBG00Z5H4460",
                    },
                },
            )

    transport = DifferentFigiScanAliasTransport()
    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"),
        transport,
    )
    provider = MassiveCoverageAccountingProvider(client)

    facts = provider.requested_facts(
        BackfillRequest(series_id=2417, from_date="2021-05-04", to_date="2026-05-08"),
        target,
        (
            EvidenceRequest(
                "coverage_gap", ("2417", "CORZ", "2021-05-04", "2024-01-23")
            ),
        ),
    )

    assert len(facts) == 1
    assert facts[0].kind == "omitted_segment"
    assert not any("/v2/aggs/ticker/CORZW/" in url for url in transport.urls)


def test_massive_coverage_provider_omits_bars_before_first_target_reference() -> None:
    target = TargetIdentity(
        ohlcv_series_id=627,
        cik="0001144879",
        composite_figi="BBG000DSJYS8",
        latest_ticker="APLD",
        latest_primary_exchange="XNAS",
        security_type="CS",
        company_name="Applied Digital Corporation Common Stock",
    )

    class PreTargetReferenceTransport:
        def get_json(self, url: str, *, timeout_seconds: float) -> HttpJsonResponse:
            parsed = urlparse(url)
            if "/v2/aggs/ticker/APLD/" in parsed.path:
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
                                    dt.datetime(2022, 4, 12, tzinfo=dt.UTC).timestamp()
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
            if parsed.path == "/v3/reference/tickers":
                return HttpJsonResponse(200, {"status": "OK", "results": []})
            as_of_date = parse_qs(parsed.query).get("date", [""])[0]
            if as_of_date == "2022-04-13":
                return HttpJsonResponse(
                    200,
                    {
                        "status": "OK",
                        "results": {
                            "ticker": "APLD",
                            "name": "Applied Blockchain, Inc. Common Stock",
                            "cik": "0001144879",
                            "composite_figi": "BBG000DSJYS8",
                            "share_class_figi": "BBG001SK4K76",
                            "type": "CS",
                            "primary_exchange": "XNAS",
                        },
                    },
                )
            if as_of_date == "2026-05-08":
                return HttpJsonResponse(
                    200,
                    {
                        "status": "OK",
                        "results": {
                            "ticker": "APLD",
                            "name": "Applied Digital Corporation Common Stock",
                            "cik": "0001144879",
                            "composite_figi": "BBG000DSJYS8",
                            "type": "CS",
                            "primary_exchange": "XNAS",
                            "list_date": "2002-07-22",
                        },
                    },
                )
            return HttpJsonResponse(
                200,
                {
                    "status": "OK",
                    "results": {
                        "ticker": "APLD",
                        "name": "APPLIED BLOCKCHAIN INC",
                        "type": "CS",
                        "primary_exchange": "OTC Link",
                    },
                },
            )

    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"),
        PreTargetReferenceTransport(),
    )
    provider = MassiveCoverageAccountingProvider(client)

    facts = provider.requested_facts(
        BackfillRequest(series_id=627, from_date="2021-05-10", to_date="2026-05-08"),
        target,
        (EvidenceRequest("coverage_gap", ("627", "APLD", "2021-05-10", "2022-04-12")),),
    )

    assert len(facts) == 1
    assert facts[0].kind == "omitted_segment"
    assert facts[0].source == "massive.pre_target_reference_interval"
    assert (
        facts[0].payload_value()["proof"]["next_target_reference"]["as_of_date"]
        == "2022-04-13"
    )


def test_massive_coverage_provider_keeps_current_list_date_bars_for_alias_history() -> (
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
            if parsed.path == "/v3/reference/tickers":
                return HttpJsonResponse(200, {"status": "OK", "results": []})
            as_of_date = parse_qs(parsed.query).get("date", [""])[0]
            if as_of_date == "2026-05-08":
                return HttpJsonResponse(
                    200,
                    {
                        "status": "OK",
                        "results": {
                            "ticker": "ABLV",
                            "name": "Able View Global Inc. Class B Ordinary Shares",
                            "cik": "0001957489",
                            "type": "CS",
                            "primary_exchange": "XNAS",
                            "list_date": "2023-08-18",
                        },
                    },
                )
            if as_of_date == "2023-08-21":
                return HttpJsonResponse(
                    200,
                    {
                        "status": "OK",
                        "results": {
                            "ticker": "ABLV",
                            "name": "Able View Global Inc. Class B Ordinary Shares",
                            "cik": "0001957489",
                            "type": "CS",
                            "primary_exchange": "XNAS",
                        },
                    },
                )
            return HttpJsonResponse(404, {"status": "NOT_FOUND", "results": None})

    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"),
        CurrentListDateTransport(),
    )
    provider = MassiveCoverageAccountingProvider(client)

    facts = provider.requested_facts(
        BackfillRequest(series_id=61, from_date="2021-05-10", to_date="2026-05-08"),
        target,
        (EvidenceRequest("coverage_gap", ("61", "ABLV", "2021-05-10", "2023-08-20")),),
    )

    assert facts == ()


def test_massive_coverage_provider_derives_known_alias_replacement_for_gap() -> None:
    target = TargetIdentity(
        ohlcv_series_id=12716,
        composite_figi="BBG00Z9114K8",
        latest_ticker="AILE",
        known_alias_tickers=("ARRW", "AILE"),
    )

    class KnownAliasGapTransport:
        def get_json(self, url: str, *, timeout_seconds: float) -> HttpJsonResponse:
            parsed = urlparse(url)
            if "/v2/aggs/ticker/ARRW/" in parsed.path:
                return HttpJsonResponse(
                    200,
                    {
                        "status": "OK",
                        "resultsCount": 2,
                        "results": [
                            {
                                "t": 1620360000000,
                                "o": 1,
                                "h": 1,
                                "l": 1,
                                "c": 1,
                                "v": 1,
                            },
                            {
                                "t": 1713225600000,
                                "o": 1,
                                "h": 1,
                                "l": 1,
                                "c": 1,
                                "v": 1,
                            },
                        ],
                    },
                )
            if "/v2/aggs/" in parsed.path:
                return HttpJsonResponse(
                    200, {"status": "OK", "resultsCount": 0, "results": []}
                )
            ticker = parsed.path.rsplit("/", 1)[-1]
            if ticker == "ARRW":
                return HttpJsonResponse(
                    200,
                    {
                        "status": "OK",
                        "results": {"ticker": "ARRW", "composite_figi": "BBG00Z9114K8"},
                    },
                )
            return HttpJsonResponse(404, {"status": "NOT_FOUND", "results": None})

    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"),
        KnownAliasGapTransport(),
    )
    provider = MassiveCoverageAccountingProvider(client)

    facts = provider.requested_facts(
        BackfillRequest(series_id=12716, from_date="2021-05-04", to_date="2026-05-04"),
        target,
        (
            EvidenceRequest(
                "coverage_gap", ("12716", "AILE", "2021-05-04", "2024-04-16")
            ),
        ),
    )

    assert len(facts) == 1
    payload = facts[0].payload_value()
    assert facts[0].kind == "ticker_replacement"
    assert payload["old_ticker"] == "AILE"
    assert payload["new_ticker"] == "ARRW"
    assert (
        payload["replacement_reason"]
        == "known_alias_target_valid_bar_window_inside_coverage_gap"
    )


def test_massive_coverage_provider_preserves_original_ticker_tails_around_temporary_d_suffix() -> (
    None
):
    target = TargetIdentity(
        ohlcv_series_id=803,
        cik="0001350102",
        composite_figi="BBG000PT5SS1",
        share_class_figi="BBG001SRD9B1",
        latest_ticker="ASTI",
        security_type="CS",
        known_alias_tickers=("ASTI",),
    )

    class TemporaryDSuffixTailTransport:
        def get_json(self, url: str, *, timeout_seconds: float) -> HttpJsonResponse:
            parsed = urlparse(url)
            if parsed.path.startswith("/v2/aggs/ticker/"):
                ticker, from_date, to_date = _aggregate_request(parsed.path)
                if (
                    ticker == "ASTID"
                    and from_date <= "2022-01-31"
                    and to_date >= "2022-02-28"
                ):
                    return _bars_response("2022-01-31", "2022-02-28")
                if ticker == "ASTI" and (from_date, to_date) in {
                    ("2021-12-31", "2022-01-28"),
                    ("2022-03-01", "2024-08-14"),
                }:
                    return _bars_response(from_date, to_date)
                return HttpJsonResponse(
                    200, {"status": "OK", "resultsCount": 0, "results": []}
                )
            if parsed.path == "/v3/reference/tickers":
                return HttpJsonResponse(200, {"status": "OK", "results": []})
            ticker = parsed.path.rsplit("/", 1)[-1]
            as_of_date = parse_qs(parsed.query).get("date", [""])[0]
            if ticker == "ASTI" and as_of_date in {
                "2021-12-31",
                "2022-01-28",
                "2022-03-01",
                "2024-08-14",
            }:
                return _reference_response("ASTI", as_of_date)
            if ticker == "ASTID" and as_of_date in {"2022-01-31", "2022-02-28"}:
                return _reference_response(
                    "ASTID", as_of_date, name="ASCENT SOLAR TECH INC NEW"
                )
            return HttpJsonResponse(404, {"status": "NOT_FOUND", "results": None})

    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"),
        TemporaryDSuffixTailTransport(),
    )
    provider = MassiveCoverageAccountingProvider(client)

    facts = provider.requested_facts(
        BackfillRequest(series_id=803, from_date="2021-05-11", to_date="2026-05-08"),
        target,
        (
            EvidenceRequest(
                "coverage_gap", ("803", "ASTI", "2021-12-31", "2022-01-28")
            ),
            EvidenceRequest(
                "coverage_gap", ("803", "ASTI", "2022-03-01", "2024-08-14")
            ),
        ),
    )

    assert [fact.kind for fact in facts] == ["ticker_replacement", "ticker_replacement"]
    assert [
        (
            fact.payload_value()["old_ticker"],
            fact.payload_value()["new_ticker"],
            fact.payload_value()["from_date"],
            fact.payload_value()["to_date"],
        )
        for fact in facts
    ] == [
        ("ASTI", "ASTI", "2021-12-31", "2022-01-28"),
        ("ASTI", "ASTI", "2022-03-01", "2024-08-14"),
    ]
    assert {fact.payload_value()["replacement_reason"] for fact in facts} == {
        "temporary_d_suffix_original_ticker_tail"
    }
    assert {
        fact.payload_value()["temporary_d_suffix_bridge"]["temporary_ticker"]
        for fact in facts
    } == {"ASTID"}


def test_massive_coverage_provider_does_not_preserve_original_ticker_tail_without_d_suffix_bridge() -> (
    None
):
    target = TargetIdentity(
        ohlcv_series_id=803,
        composite_figi="BBG000PT5SS1",
        share_class_figi="BBG001SRD9B1",
        latest_ticker="ASTI",
        security_type="CS",
    )

    class NoBridgeTransport:
        def get_json(self, url: str, *, timeout_seconds: float) -> HttpJsonResponse:
            parsed = urlparse(url)
            if parsed.path.startswith("/v2/aggs/ticker/"):
                ticker, from_date, to_date = _aggregate_request(parsed.path)
                if ticker == "ASTI" and (from_date, to_date) == (
                    "2021-12-31",
                    "2022-01-28",
                ):
                    return _bars_response(from_date, to_date)
                return HttpJsonResponse(
                    200, {"status": "OK", "resultsCount": 0, "results": []}
                )
            if parsed.path == "/v3/reference/tickers":
                return HttpJsonResponse(200, {"status": "OK", "results": []})
            ticker = parsed.path.rsplit("/", 1)[-1]
            as_of_date = parse_qs(parsed.query).get("date", [""])[0]
            if ticker == "ASTI" and as_of_date in {"2021-12-31", "2022-01-28"}:
                return _reference_response("ASTI", as_of_date)
            return HttpJsonResponse(404, {"status": "NOT_FOUND", "results": None})

    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"),
        NoBridgeTransport(),
    )
    provider = MassiveCoverageAccountingProvider(client)

    facts = provider.requested_facts(
        BackfillRequest(series_id=803, from_date="2021-05-11", to_date="2026-05-08"),
        target,
        (EvidenceRequest("coverage_gap", ("803", "ASTI", "2021-12-31", "2022-01-28")),),
    )

    assert facts == ()


def test_massive_coverage_provider_rejects_temporary_d_suffix_bridge_for_different_identity() -> (
    None
):
    target = TargetIdentity(
        ohlcv_series_id=803,
        composite_figi="BBG000PT5SS1",
        share_class_figi="BBG001SRD9B1",
        latest_ticker="ASTI",
        security_type="CS",
    )

    class DifferentIdentityBridgeTransport:
        def get_json(self, url: str, *, timeout_seconds: float) -> HttpJsonResponse:
            parsed = urlparse(url)
            if parsed.path.startswith("/v2/aggs/ticker/"):
                ticker, from_date, to_date = _aggregate_request(parsed.path)
                if ticker == "ASTI" and (from_date, to_date) == (
                    "2021-12-31",
                    "2022-01-28",
                ):
                    return _bars_response(from_date, to_date)
                if (
                    ticker == "ASTID"
                    and from_date <= "2022-01-31"
                    and to_date >= "2022-02-28"
                ):
                    return _bars_response("2022-01-31", "2022-02-28")
                return HttpJsonResponse(
                    200, {"status": "OK", "resultsCount": 0, "results": []}
                )
            if parsed.path == "/v3/reference/tickers":
                return HttpJsonResponse(200, {"status": "OK", "results": []})
            ticker = parsed.path.rsplit("/", 1)[-1]
            as_of_date = parse_qs(parsed.query).get("date", [""])[0]
            if ticker == "ASTI" and as_of_date in {"2021-12-31", "2022-01-28"}:
                return _reference_response("ASTI", as_of_date)
            if ticker == "ASTID" and as_of_date in {"2022-01-31", "2022-02-28"}:
                return _reference_response(
                    "ASTID",
                    as_of_date,
                    composite_figi="BBGDIFFERENT",
                    share_class_figi="BBGDIFFERENTCLASS",
                )
            return HttpJsonResponse(404, {"status": "NOT_FOUND", "results": None})

    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"),
        DifferentIdentityBridgeTransport(),
    )
    provider = MassiveCoverageAccountingProvider(client)

    facts = provider.requested_facts(
        BackfillRequest(series_id=803, from_date="2021-05-11", to_date="2026-05-08"),
        target,
        (EvidenceRequest("coverage_gap", ("803", "ASTI", "2021-12-31", "2022-01-28")),),
    )

    assert facts == ()


def _aggregate_request(path: str) -> tuple[str, str, str]:
    parts = path.split("/")
    return parts[4], parts[8], parts[9]


def _bars_response(*dates: str) -> HttpJsonResponse:
    results = [
        {
            "t": int(
                dt.datetime.fromisoformat(date).replace(tzinfo=dt.UTC).timestamp()
                * 1000
            ),
            "o": 1,
            "h": 1,
            "l": 1,
            "c": 1,
            "v": 1,
        }
        for date in dates
    ]
    return HttpJsonResponse(
        200, {"status": "OK", "resultsCount": len(results), "results": results}
    )


def _reference_response(
    ticker: str,
    as_of_date: str,
    *,
    composite_figi: str = "BBG000PT5SS1",
    share_class_figi: str = "BBG001SRD9B1",
    name: str = "Ascent Solar Technologies, Inc. Common Stock",
) -> HttpJsonResponse:
    return HttpJsonResponse(
        200,
        {
            "status": "OK",
            "results": {
                "ticker": ticker,
                "active": True,
                "cik": "0001350102",
                "composite_figi": composite_figi,
                "share_class_figi": share_class_figi,
                "primary_exchange": "XNAS",
                "type": "CS",
                "name": name,
                "queried_date": as_of_date,
            },
        },
    )


def test_massive_coverage_provider_derives_terminal_target_bar_window() -> None:
    target = TargetIdentity(
        ohlcv_series_id=2687,
        composite_figi="BBG000GZFVB7",
        share_class_figi="BBG001SD4192",
        latest_ticker="CVU",
    )

    class TerminalTargetBarsTransport:
        def get_json(self, url: str, *, timeout_seconds: float) -> HttpJsonResponse:
            parsed = urlparse(url)
            if "/v2/aggs/ticker/CVU/" in parsed.path:
                return HttpJsonResponse(
                    200,
                    {
                        "status": "DELAYED",
                        "resultsCount": 2,
                        "results": [
                            {
                                "t": 1664942400000,
                                "o": 1,
                                "h": 1,
                                "l": 1,
                                "c": 1,
                                "v": 1,
                            },
                            {
                                "t": 1778212800000,
                                "o": 1,
                                "h": 1,
                                "l": 1,
                                "c": 1,
                                "v": 1,
                            },
                        ],
                    },
                )
            if parsed.path.endswith("/CVU"):
                return HttpJsonResponse(
                    200,
                    {
                        "status": "OK",
                        "results": {
                            "ticker": "CVU",
                            "active": True,
                            "cik": "0000889348",
                            "composite_figi": "BBG000GZFVB7",
                            "share_class_figi": "BBG001SD4192",
                            "primary_exchange": "XASE",
                            "type": "CS",
                        },
                    },
                )
            return HttpJsonResponse(404, {"status": "NOT_FOUND", "results": None})

    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"),
        TerminalTargetBarsTransport(),
    )
    provider = MassiveCoverageAccountingProvider(client)

    facts = provider.requested_facts(
        BackfillRequest(series_id=2687, from_date="2021-05-04", to_date="2026-05-09"),
        target,
        (
            EvidenceRequest(
                "terminal_coverage", ("2687", "CVU", "2022-05-20", "2026-05-09")
            ),
        ),
    )

    assert len(facts) == 1
    assert facts[0].kind == "handoff_segment"
    payload = facts[0].payload_value()
    assert payload["ticker"] == "CVU"
    assert payload["from_date"] == "2022-10-05"
    assert payload["to_date"] == "2026-05-08"
    assert (
        payload["event_ticker_handoff"]["handoff_reason"]
        == "terminal_target_ticker_bar_window"
    )


def test_massive_read_only_provider_set_composes_live_planning_providers() -> None:
    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"),
        FakeHttpJsonTransport(HttpJsonResponse(200, {"status": "OK", "results": []})),
    )

    provider_set = massive_read_only_provider_set(client)

    assert [provider.__class__.__name__ for provider in provider_set.providers] == [
        "MassiveTickerEventsProvider",
        "MassiveReferenceBoundaryProvider",
        "MassiveAliasHistoryProvider",
        "MassiveTickerReplacementProvider",
        "MassiveCoverageAccountingProvider",
        "MassiveBarProbeProvider",
        "MassiveIdentityScanProvider",
    ]


def test_massive_client_raw_capture_is_disabled_by_default(tmp_path: Path) -> None:
    transport = FakeHttpJsonTransport(
        HttpJsonResponse(200, {"status": "OK", "results": []})
    )
    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"), transport
    )

    client.get("/v3/reference/tickers/ABC", {"date": "2024-01-02"})

    assert not list(tmp_path.iterdir())


def test_massive_client_raw_capture_excludes_api_key(tmp_path: Path) -> None:
    transport = FakeHttpJsonTransport(
        HttpJsonResponse(200, {"status": "OK", "results": {"ticker": "ABC"}})
    )
    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"),
        transport,
        raw_capture_dir=tmp_path,
    )

    client.get("/v3/reference/tickers/ABC", {"date": "2024-01-02"})

    captures = list(tmp_path.glob("*.json"))
    assert len(captures) == 1
    captured_text = captures[0].read_text()
    captured = json.loads(captured_text)
    assert "secret" not in captured_text
    assert "apiKey" not in captured_text
    assert captured["endpoint"] == "/v3/reference/tickers/ABC"
    assert captured["params_without_api_key"] == [["date", "2024-01-02"]]
    assert captured["payload"]["results"]["ticker"] == "ABC"


def test_source_dry_run_trace_returns_evidence_needed_when_provider_cannot_collect() -> (
    None
):
    legacy = load_fixture("ticker_rename_meta.json")
    source_with_supplemental = StaticBackfillEvidenceSource.from_legacy_plan(
        legacy,
        include_candidate_segments=False,
        defer_kinds=("ticker_events",),
    )
    source = StaticBackfillEvidenceSource(source_with_supplemental.seed_facts)

    trace = run_backfill_source_dry_run_trace(source)

    assert trace.result.__class__.__name__ == "EvidenceNeeded"
    assert [request.kind for request in trace.result.requests] == ["ticker_events"]
    assert len(trace.rounds) == 1
    assert trace.rounds[0].collected_facts == ()


def test_source_dry_run_trace_does_not_repeat_already_requested_evidence() -> None:
    legacy = load_fixture("barrick_gold_b.json")
    base_source = StaticBackfillEvidenceSource.from_legacy_plan(
        legacy,
        include_candidate_segments=False,
        defer_kinds=("ticker_events", "alias_history", "reference_boundary"),
    )
    ticker_events = [
        fact for fact in base_source.supplemental_facts if fact.kind == "ticker_events"
    ]
    reference_fact = ReferenceBoundaryFact(
        ticker="GOLD",
        as_of_date="2025-05-08",
        api_status="OK",
        matched=True,
        match_reason="composite_figi_match",
        payload={"point": "start"},
    ).to_evidence_fact(989)
    calls: list[tuple[tuple[str, tuple[str, ...]], ...]] = []

    class RepeatingSource:
        def initial_facts(self) -> tuple[EvidenceFact, ...]:
            return base_source.seed_facts

        def requested_facts(
            self, requests: tuple[EvidenceRequest, ...]
        ) -> tuple[EvidenceFact, ...]:
            calls.append(tuple((request.kind, request.key) for request in requests))
            if requests[0].kind == "ticker_events":
                return tuple(ticker_events)
            return (reference_fact,)

    trace = run_backfill_source_dry_run_trace(RepeatingSource())

    assert trace.result.__class__.__name__ == "EvidenceNeeded"
    assert len(trace.rounds) == 3
    assert calls == [
        (("ticker_events", ()),),
        (
            ("alias_history", ("989", "2025-05-08", "2025-05-09", "B")),
            ("coverage_gap", ("989", "B", "2025-05-08", "2025-05-08")),
            ("reference_boundary", ("989", "GOLD", "2025-05-08", "start")),
        ),
    ]


def test_live_dry_run_base_facts_exclude_legacy_decisions_and_candidates() -> None:
    legacy = load_fixture("ticker_rename_meta.json")

    facts = live_dry_run_base_facts_from_legacy_plan(legacy)

    assert {fact.kind for fact in facts} == {
        "backfill_request",
        "known_aliases",
        "plan_metadata",
        "target_identity",
    }


def test_live_dry_run_source_can_collect_ticker_events_with_fake_transport() -> None:
    legacy = load_fixture("ticker_rename_meta.json")
    events_payload = {
        "status": "OK",
        "results": {
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

    class MetaDryRunTransport:
        def __init__(self) -> None:
            self.urls: list[str] = []

        def get_json(self, url: str, *, timeout_seconds: float) -> HttpJsonResponse:
            self.urls.append(url)
            parsed = urlparse(url)
            if parsed.path.endswith("/events"):
                return HttpJsonResponse(200, events_payload)
            ticker = parsed.path.rsplit("/", 1)[-1]
            return HttpJsonResponse(
                200,
                {
                    "status": "OK",
                    "results": {
                        "ticker": ticker,
                        "composite_figi": "BBG000MM2P62",
                        "share_class_figi": "BBG001SQCQC5",
                        "primary_exchange": "XNAS",
                        "active": True,
                        "queried_date": parse_qs(parsed.query).get("date", [""])[0],
                    },
                },
            )

    transport = MetaDryRunTransport()
    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"),
        transport,
    )
    source, returned_client = massive_live_dry_run_source_from_legacy_plan(
        legacy, client=client
    )

    trace = run_backfill_source_dry_run_trace(source)

    assert returned_client is client
    assert isinstance(trace.result, BackfillPlan)
    assert [segment.ticker for segment in trace.result.segments] == ["FB", "META"]
    assert client.request_log[0].endpoint == "/vX/reference/tickers/BBG000MM2P62/events"
    assert [item.endpoint for item in client.request_log] == [
        "/vX/reference/tickers/BBG000MM2P62/events",
        "/v3/reference/tickers/FB",
        "/v3/reference/tickers/FB",
        "/v3/reference/tickers/META",
        "/v3/reference/tickers/META",
    ]


def test_live_dry_run_source_can_derive_ceg_ticker_replacement_with_fake_transport() -> (
    None
):
    legacy = load_fixture("ceg_invalid_event_ticker_replacement.json")

    class CegDryRunTransport:
        def __init__(self) -> None:
            self.urls: list[str] = []

        def get_json(self, url: str, *, timeout_seconds: float) -> HttpJsonResponse:
            self.urls.append(url)
            parsed = urlparse(url)
            if parsed.path.endswith("/events"):
                return HttpJsonResponse(
                    200, {"status": "OK", "results": legacy["event_lookup"]}
                )
            ticker = parsed.path.rsplit("/", 1)[-1]
            as_of_date = parse_qs(parsed.query).get("date", [""])[0]
            if ticker == "CEGVV" or (ticker == "CEG" and as_of_date == "2022-02-02"):
                return HttpJsonResponse(
                    200,
                    {
                        "status": "OK",
                        "results": {
                            "ticker": ticker,
                            "cik": "0001868275",
                            "composite_figi": "BBG014KFRNP7",
                            "share_class_figi": "BBG014KFRPJ9",
                            "primary_exchange": "XNAS",
                            "active": True,
                            "queried_date": as_of_date,
                        },
                    },
                )
            return HttpJsonResponse(404, {"status": "NOT_FOUND", "results": None})

    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"),
        CegDryRunTransport(),
    )
    source, returned_client = massive_live_dry_run_source_from_legacy_plan(
        legacy, client=client
    )

    trace = run_backfill_source_dry_run_trace(source)

    assert returned_client is client
    assert isinstance(trace.result, BackfillPlan)
    assert_core_parity(legacy_plan_dict(trace.result), legacy)
    assert any(
        item.endpoint == "/v3/reference/tickers/CEGVV" for item in client.request_log
    )
    assert any(
        round_item.result.__class__.__name__ == "EvidenceNeeded"
        and [request.kind for request in round_item.result.requests]
        == ["ticker_replacement"]
        for round_item in trace.rounds
    )


def test_live_dry_run_allows_pre_listing_gap_before_first_event_ticker() -> None:
    target = TargetIdentity(
        ohlcv_series_id=95,
        cik="0001881487",
        composite_figi="BBG013PRYPJ2",
        share_class_figi="BBG013PRYQC7",
        latest_ticker="ACDC",
        latest_primary_exchange="XNAS",
        security_type="CS",
        company_name="ProFrac Holding Corp. Class A Common Stock",
        natural_key="massive:composite_figi:BBG013PRYPJ2",
    )
    request = BackfillRequest(
        series_id=95, from_date="2021-05-10", to_date="2022-11-04"
    )
    base_facts = (
        EvidenceFact("target_identity", ("95",), target.to_legacy_dict(), "test"),
        EvidenceFact("backfill_request", ("95",), request.to_legacy_dict(), "test"),
        EvidenceFact(
            "known_aliases",
            ("95",),
            [
                {
                    "ticker": "ACDC",
                    "active": True,
                    "company_name": target.company_name,
                    "primary_exchange": "XNAS",
                }
            ],
            "test",
        ),
        EvidenceFact(
            "plan_metadata",
            ("95",),
            {"generated_at_utc": "2026-01-01T00:00:00+00:00"},
            "test",
        ),
    )

    class ProFracTransport:
        def __init__(self) -> None:
            self.urls: list[str] = []

        def get_json(self, url: str, *, timeout_seconds: float) -> HttpJsonResponse:
            self.urls.append(url)
            parsed = urlparse(url)
            if parsed.path.endswith("/events"):
                return HttpJsonResponse(
                    200,
                    {
                        "status": "OK",
                        "results": {
                            "name": target.company_name,
                            "composite_figi": target.composite_figi,
                            "cik": target.cik,
                            "events": [
                                {
                                    "date": "2022-11-02",
                                    "ticker_change": {"ticker": "ACDC"},
                                    "type": "ticker_change",
                                },
                                {
                                    "date": "2022-05-12",
                                    "ticker_change": {"ticker": "PFHC"},
                                    "type": "ticker_change",
                                },
                            ],
                        },
                    },
                )
            if parsed.path == "/v3/reference/tickers":
                return HttpJsonResponse(200, {"status": "OK", "results": []})
            if "/v2/aggs/ticker/PFHC/" in parsed.path:
                if "/2021-05-10/2022-05-11" in parsed.path:
                    return HttpJsonResponse(
                        200, {"status": "OK", "resultsCount": 0, "results": []}
                    )
                return HttpJsonResponse(
                    200,
                    {
                        "status": "OK",
                        "resultsCount": 2,
                        "results": [
                            {
                                "t": int(
                                    dt.datetime(2022, 5, 13, tzinfo=dt.UTC).timestamp()
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
                                    dt.datetime(2022, 11, 1, tzinfo=dt.UTC).timestamp()
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
            if "/v2/aggs/ticker/ACDC/" in parsed.path:
                return HttpJsonResponse(
                    200, {"status": "OK", "resultsCount": 0, "results": []}
                )
            ticker = parsed.path.rsplit("/", 1)[-1]
            as_of_date = parse_qs(parsed.query).get("date", [""])[0]
            if ticker == "PFHC" and as_of_date in {"2022-05-13", "2022-11-01"}:
                return HttpJsonResponse(
                    200, {"status": "OK", "results": _profrac_reference("PFHC")}
                )
            if ticker == "ACDC" and as_of_date >= "2022-11-02":
                return HttpJsonResponse(
                    200, {"status": "OK", "results": _profrac_reference("ACDC")}
                )
            return HttpJsonResponse(404, {"status": "NOT_FOUND", "results": None})

    def _profrac_reference(ticker: str) -> dict[str, object]:
        return {
            "ticker": ticker,
            "name": target.company_name,
            "active": True,
            "cik": target.cik,
            "composite_figi": target.composite_figi,
            "share_class_figi": target.share_class_figi,
            "type": "CS",
            "primary_exchange": "XNAS",
        }

    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"),
        ProFracTransport(),
    )
    source = ProviderBackfillEvidenceSource(
        base_facts, massive_read_only_provider_set(client)
    )

    trace = run_backfill_source_dry_run_trace(source, max_rounds=8)

    assert isinstance(trace.result, BackfillPlan)
    assert [
        (segment.ticker, segment.from_date.isoformat(), segment.to_date.isoformat())
        for segment in trace.result.segments
    ] == [
        ("PFHC", "2022-05-13", "2022-11-01"),
        ("ACDC", "2022-11-02", "2022-11-04"),
    ]
    assert any(
        fact.kind == "omitted_segment" and fact.payload_value()["ticker"] == "PFHC"
        for round_item in trace.rounds
        for fact in round_item.collected_facts
    )
    assert any(
        round_item.result.__class__.__name__ == "EvidenceNeeded"
        and ("coverage_gap", ("95", "PFHC", "2021-05-10", "2022-05-11"))
        in [(request.kind, request.key) for request in round_item.result.requests]
        for round_item in trace.rounds
    )
