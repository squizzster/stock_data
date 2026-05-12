from __future__ import annotations

from backfill_test_support import *


@pytest.mark.parametrize("fixture_name", ALL_LEGACY_FIXTURES)
def test_all_legacy_fixtures_preserve_core_shape(fixture_name: str) -> None:
    legacy = load_fixture(fixture_name)
    result = plan_backfill(ledger_from_legacy_plan(legacy).snapshot())

    assert_core_parity(legacy_plan_dict(result), legacy)


def test_simple_current_ticker_legacy_shape() -> None:
    legacy = load_fixture("simple_current_sfbc.json")
    result = plan_backfill(ledger_from_legacy_plan(legacy).snapshot())

    actual = legacy_plan_dict(result)
    assert_core_parity(actual, legacy)
    assert [segment["ticker"] for segment in actual["segments"]] == ["SFBC"]
    assert "Status: `safe`" in render_backfill_plan_markdown(result)


def test_ticker_rename_legacy_shape() -> None:
    legacy = load_fixture("ticker_rename_meta.json")
    result = plan_backfill(ledger_from_legacy_plan(legacy).snapshot())

    actual = legacy_plan_dict(result)
    assert_core_parity(actual, legacy)
    assert [segment["ticker"] for segment in actual["segments"]] == ["FB", "META"]
    assert [segment["from_date"] for segment in actual["segments"]] == [
        "2022-06-07",
        "2022-06-09",
    ]
    assert [segment["to_date"] for segment in actual["segments"]] == [
        "2022-06-08",
        "2022-06-10",
    ]


def test_ticker_events_can_derive_initial_segments_without_legacy_candidates() -> None:
    legacy = load_fixture("ticker_rename_meta.json")
    ledger = without_facts(ledger_from_legacy_plan(legacy), "candidate_segments")

    result = plan_backfill(ledger.snapshot())
    actual = legacy_plan_dict(result)

    assert actual["status"] == "safe"
    assert [
        (segment["ticker"], segment["from_date"], segment["to_date"], segment["source"])
        for segment in actual["segments"]
    ] == [
        ("FB", "2022-06-07", "2022-06-08", "ticker_events"),
        ("META", "2022-06-09", "2022-06-10", "ticker_events"),
    ]


@pytest.mark.parametrize(
    ("fixture_name", "expected"),
    [
        (
            "ticker_rename_meta.json",
            [("FB", "2022-06-07", "2022-06-08"), ("META", "2022-06-09", "2022-06-10")],
        ),
        (
            "block_xyz.json",
            [("SQ", "2025-01-17", "2025-01-17"), ("XYZ", "2025-01-21", "2025-01-21")],
        ),
        (
            "t1_energy_te.json",
            [("FREY", "2025-02-28", "2025-02-28"), ("TE", "2025-03-03", "2025-03-03")],
        ),
    ],
)
def test_ticker_events_derive_simple_rename_segments(
    fixture_name: str, expected: list[tuple[str, str, str]]
) -> None:
    legacy = load_fixture(fixture_name)
    ledger = without_facts(ledger_from_legacy_plan(legacy), "candidate_segments")

    result = plan_backfill(ledger.snapshot())
    actual = legacy_plan_dict(result)

    assert [
        (segment["ticker"], segment["from_date"], segment["to_date"])
        for segment in actual["segments"]
    ] == expected


def test_pre_event_alias_gap_requires_alias_history_without_legacy_candidates() -> None:
    legacy = load_fixture("barrick_gold_b.json")
    ledger = without_facts(
        ledger_from_legacy_plan(legacy), "candidate_segments", "alias_history"
    )

    result = plan_backfill(ledger.snapshot())

    assert result.__class__.__name__ == "EvidenceNeeded"
    assert [request.kind for request in result.requests] == [
        "alias_history",
        "coverage_gap",
        "reference_boundary",
    ]
    assert result.requests[1].key == ("989", "B", "2025-05-08", "2025-05-08")
    assert result.requests[2].key == ("989", "GOLD", "2025-05-08", "start")
    assert "pre-event interval" in result.decisions[0].reason


def test_pre_event_gap_without_known_aliases_requests_absence_proof() -> None:
    target = TargetIdentity(
        ohlcv_series_id=4488, composite_figi="BBG020WZZZ26", latest_ticker="GAVA"
    )
    facts = (
        EvidenceFact(
            "backfill_request",
            ("4488",),
            BackfillRequest(
                series_id=4488, from_date="2026-03-10", to_date="2026-03-17"
            ).to_legacy_dict(),
            "test",
        ),
        EvidenceFact("target_identity", ("4488",), target.to_legacy_dict(), "test"),
        EvidenceFact("known_aliases", ("4488",), [], "test"),
        EvidenceFact(
            "plan_metadata",
            ("4488",),
            {"generated_at_utc": "2026-01-01T00:00:00+00:00"},
            "test",
        ),
        TickerEventFact(
            "BBG020WZZZ26",
            "composite_figi",
            "OK",
            [{"date": "2026-03-12", "ticker": "GAVA"}],
        ).to_evidence_fact(4488),
    )

    result = plan_backfill(EvidenceLedger(facts).snapshot())

    assert isinstance(result, EvidenceNeeded)
    assert [request.kind for request in result.requests] == [
        "alias_history",
        "coverage_gap",
    ]
    assert result.requests[1].key == ("4488", "GAVA", "2026-03-10", "2026-03-11")


def test_pre_event_absence_proof_allows_later_event_segment() -> None:
    target = TargetIdentity(
        ohlcv_series_id=4488, composite_figi="BBG020WZZZ26", latest_ticker="GAVA"
    )
    facts = (
        EvidenceFact(
            "backfill_request",
            ("4488",),
            BackfillRequest(
                series_id=4488, from_date="2026-03-10", to_date="2026-03-17"
            ).to_legacy_dict(),
            "test",
        ),
        EvidenceFact("target_identity", ("4488",), target.to_legacy_dict(), "test"),
        EvidenceFact("known_aliases", ("4488",), [], "test"),
        EvidenceFact(
            "plan_metadata",
            ("4488",),
            {"generated_at_utc": "2026-01-01T00:00:00+00:00"},
            "test",
        ),
        TickerEventFact(
            "BBG020WZZZ26",
            "composite_figi",
            "OK",
            [{"date": "2026-03-12", "ticker": "GAVA"}],
        ).to_evidence_fact(4488),
        OmittedSegmentFact(
            "GAVA", "2026-03-10", "2026-03-11", "no target bars", source="test"
        ).to_evidence_fact(4488),
        ReferenceBoundaryFact(
            "GAVA",
            "2026-03-12",
            "OK",
            True,
            "composite_figi_match",
            {"point": "start"},
        ).to_evidence_fact(4488),
        ReferenceBoundaryFact(
            "GAVA",
            "2026-03-17",
            "OK",
            True,
            "composite_figi_match",
            {"point": "end"},
        ).to_evidence_fact(4488),
    )

    result = plan_backfill(EvidenceLedger(facts).snapshot())

    assert isinstance(result, BackfillPlan)
    assert [
        (segment.ticker, segment.from_date.isoformat(), segment.to_date.isoformat())
        for segment in result.segments
    ] == [("GAVA", "2026-03-12", "2026-03-17")]


def test_alias_history_fills_pre_event_gap_without_legacy_candidates() -> None:
    legacy = load_fixture("barrick_gold_b.json")
    ledger = without_facts(ledger_from_legacy_plan(legacy), "candidate_segments")

    result = plan_backfill(ledger.snapshot())
    actual = legacy_plan_dict(result)

    assert actual["status"] == "safe"
    assert [
        (segment["ticker"], segment["from_date"], segment["to_date"])
        for segment in actual["segments"]
    ] == [
        ("GOLD", "2025-05-08", "2025-05-08"),
        ("B", "2025-05-09", "2025-05-09"),
    ]
    assert actual["segments"][0]["source"] == "known_alias_pre_event_bar_validation"


def test_start_gap_fixture_preserves_trimmed_boundary_warning() -> None:
    legacy = load_fixture("aapg_start_gap.json")
    result = plan_backfill(ledger_from_legacy_plan(legacy).snapshot())
    actual = legacy_plan_dict(result)

    assert actual["status"] == "safe"
    assert [
        (segment["ticker"], segment["from_date"], segment["to_date"])
        for segment in actual["segments"]
    ] == [("AAPG", "2025-01-27", "2025-01-28")]
    assert len(actual["warnings"]) == 1
    assert "provider reference was NOT_FOUND before then" in actual["warnings"][0]


def test_reference_boundary_facts_trim_event_start_without_legacy_candidates() -> None:
    legacy = load_fixture("aapg_start_gap.json")
    ledger = without_facts(ledger_from_legacy_plan(legacy), "candidate_segments")

    result = plan_backfill(ledger.snapshot())
    actual = legacy_plan_dict(result)

    assert actual["status"] == "safe"
    assert [
        (segment["ticker"], segment["from_date"], segment["to_date"], segment["source"])
        for segment in actual["segments"]
    ] == [
        (
            "AAPG",
            "2025-01-27",
            "2025-01-28",
            "ticker_events+reference_start_gap_with_no_bars",
        )
    ]


def test_reference_start_gap_replaces_stale_start_validation_row() -> None:
    legacy = load_fixture("aapg_start_gap.json")
    ledger = without_facts(ledger_from_legacy_plan(legacy), "candidate_segments")

    result = plan_backfill(ledger.snapshot())
    actual = legacy_plan_dict(result)

    segment = actual["segments"][0]
    assert segment["from_date"] == "2025-01-27"
    assert segment["validation"][0]["point"] == "start"
    assert segment["validation"][0]["date"] == "2025-01-27"
    assert segment["validation"][0]["match_reason"] == "composite_figi_match"


def test_failed_start_boundary_requests_next_start_probe() -> None:
    legacy = load_fixture("aapg_start_gap.json")
    ledger = without_facts(
        ledger_from_legacy_plan(legacy), "candidate_segments", "reference_boundary"
    )
    failed_start = ReferenceBoundaryFact(
        ticker="AAPG",
        as_of_date="2025-01-24",
        api_status="NOT_FOUND",
        matched=False,
        match_reason="not_found",
        payload={"point": "start"},
    ).to_evidence_fact(30)
    matched_end = ReferenceBoundaryFact(
        ticker="AAPG",
        as_of_date="2025-01-28",
        api_status="OK",
        matched=True,
        match_reason="composite_figi_match",
        payload={"point": "end"},
    ).to_evidence_fact(30)

    result = plan_backfill(ledger.merge((failed_start, matched_end)).snapshot())

    assert result.__class__.__name__ == "EvidenceNeeded"
    assert result.requests == (
        EvidenceRequest("reference_boundary", ("30", "AAPG", "2025-01-27", "start")),
    )


def test_latest_duplicate_reference_boundary_fact_wins() -> None:
    legacy = load_fixture("aapg_start_gap.json")
    ledger = without_facts(
        ledger_from_legacy_plan(legacy), "candidate_segments", "reference_boundary"
    )
    failed_start = ReferenceBoundaryFact(
        ticker="AAPG",
        as_of_date="2025-01-24",
        api_status="NOT_FOUND",
        matched=False,
        match_reason="not_found",
        payload={"point": "start"},
    ).to_evidence_fact(30)
    corrected_start = ReferenceBoundaryFact(
        ticker="AAPG",
        as_of_date="2025-01-24",
        api_status="OK",
        matched=True,
        match_reason="composite_figi_match",
        payload={"point": "start"},
    ).to_evidence_fact(30)
    matched_end = ReferenceBoundaryFact(
        ticker="AAPG",
        as_of_date="2025-01-28",
        api_status="OK",
        matched=True,
        match_reason="composite_figi_match",
        payload={"point": "end"},
    ).to_evidence_fact(30)

    result = plan_backfill(
        ledger.merge((failed_start, corrected_start, matched_end)).snapshot()
    )
    actual = legacy_plan_dict(result)

    assert actual["status"] == "safe"
    assert [
        (segment["ticker"], segment["from_date"], segment["to_date"])
        for segment in actual["segments"]
    ] == [("AAPG", "2025-01-24", "2025-01-28")]


def test_empty_event_segment_fixture_preserves_drop_warning() -> None:
    legacy = load_fixture("abat_empty_abml_segment.json")
    result = plan_backfill(ledger_from_legacy_plan(legacy).snapshot())
    actual = legacy_plan_dict(result)

    assert actual["status"] == "safe"
    assert [
        (segment["ticker"], segment["from_date"], segment["to_date"])
        for segment in actual["segments"]
    ] == [("ABAT", "2023-09-21", "2023-09-22")]
    assert len(actual["warnings"]) == 1
    assert "ABML" in actual["warnings"][0]
    assert "was omitted" in actual["warnings"][0]


def test_omitted_segment_evidence_drops_empty_event_segment_without_legacy_candidates() -> (
    None
):
    legacy = load_fixture("abat_empty_abml_segment.json")
    ledger = without_facts(ledger_from_legacy_plan(legacy), "candidate_segments")

    result = plan_backfill(ledger.snapshot())
    actual = legacy_plan_dict(result)

    assert actual["status"] == "safe"
    assert [
        (segment["ticker"], segment["from_date"], segment["to_date"])
        for segment in actual["segments"]
    ] == [("ABAT", "2023-09-21", "2023-09-22")]


def test_all_omitted_event_segments_finish_as_blocked_empty_plan() -> None:
    target = TargetIdentity(
        ohlcv_series_id=7767, composite_figi="BBG017XGGR13", latest_ticker="NNAVW"
    )
    facts = (
        EvidenceFact(
            "backfill_request",
            ("7767",),
            BackfillRequest(
                series_id=7767, from_date="2021-10-29", to_date="2026-05-04"
            ).to_legacy_dict(),
            "test",
        ),
        EvidenceFact("target_identity", ("7767",), target.to_legacy_dict(), "test"),
        EvidenceFact("known_aliases", ("7767",), [], "test"),
        EvidenceFact(
            "plan_metadata",
            ("7767",),
            {"generated_at_utc": "2026-01-01T00:00:00+00:00"},
            "test",
        ),
        TickerEventFact(
            "BBG017XGGR13",
            "composite_figi",
            "OK",
            [{"date": "2021-10-29", "ticker": "NNAV"}],
        ).to_evidence_fact(7767),
        OmittedSegmentFact(
            "NNAV", "2021-10-29", "2026-05-04", "no target bars", source="test"
        ).to_evidence_fact(7767),
    )

    result = plan_backfill(EvidenceLedger(facts).snapshot())

    assert isinstance(result, BackfillPlan)
    assert result.status == "blocked"
    assert result.segments == ()
    assert result.errors == ("No validated ticker segments were produced.",)


def test_invalid_event_ticker_replacement_fixture_preserves_legacy_shape() -> None:
    legacy = load_fixture("ceg_invalid_event_ticker_replacement.json")
    result = plan_backfill(ledger_from_legacy_plan(legacy).snapshot())
    actual = legacy_plan_dict(result)

    assert_core_parity(actual, legacy)
    assert [
        (segment["ticker"], segment["from_date"], segment["to_date"])
        for segment in actual["segments"]
    ] == [
        ("CEGVV", "2022-01-19", "2022-02-01"),
        ("CEG", "2022-02-02", "2022-02-02"),
    ]
    assert "ticker changed from CEGV to CEGVV" in actual["warnings"][0]


def test_ticker_replacement_fact_replaces_invalid_event_ticker_without_legacy_candidates() -> (
    None
):
    legacy = load_fixture("ceg_invalid_event_ticker_replacement.json")
    ledger = without_facts(ledger_from_legacy_plan(legacy), "candidate_segments")

    result = plan_backfill(ledger.snapshot())
    actual = legacy_plan_dict(result)

    assert actual["status"] == "safe"
    assert [
        (segment["ticker"], segment["from_date"], segment["to_date"], segment["source"])
        for segment in actual["segments"]
    ] == [
        (
            "CEGVV",
            "2022-01-19",
            "2022-02-01",
            "ticker_events+known_alias_boundary_validation",
        ),
        ("CEG", "2022-02-02", "2022-02-02", "ticker_events"),
    ]
    assert actual["segments"][0]["ticker_replacement"]["old_ticker"] == "CEGV"
    assert actual["segments"][0]["ticker_replacement"]["new_ticker"] == "CEGVV"


def test_partial_ticker_replacement_requires_coverage_gap_evidence() -> None:
    legacy = load_fixture("ceg_invalid_event_ticker_replacement.json")
    ledger = without_facts(ledger_from_legacy_plan(legacy), "candidate_segments")
    facts = []
    for fact in ledger.facts:
        if fact.kind == "ticker_replacement":
            payload = fact.payload_value()
            payload["to_date"] = "2022-01-25"
            facts.append(EvidenceFact(fact.kind, fact.key, payload, fact.source))
        else:
            facts.append(fact)

    result = plan_backfill(EvidenceLedger(tuple(facts)).snapshot())

    assert isinstance(result, EvidenceNeeded)
    assert [request.kind for request in result.requests] == ["coverage_gap"]
    assert result.requests[0].key == ("2005", "CEGV", "2022-01-26", "2022-02-01")
    assert (
        result.decisions[0].decision_id
        == "segments:coverage_gap:CEGV:2022-01-26:2022-02-01"
    )


def test_terminal_replacement_tail_requires_valid_window_evidence() -> None:
    legacy = load_fixture("arrw_event_ticker_handoff.json")
    ledger = without_facts(
        ledger_from_legacy_plan(legacy), "candidate_segments", "terminal_coverage"
    )

    result = plan_backfill(ledger.snapshot())

    assert isinstance(result, EvidenceNeeded)
    assert [request.kind for request in result.requests] == ["terminal_coverage"]
    assert result.requests[0].key == ("12716", "AILE", "2025-01-02", "2026-05-04")
    assert (
        result.decisions[0].decision_id
        == "segments:terminal_coverage:AILE:2025-01-02:2026-05-04"
    )


def test_terminal_coverage_fact_accounts_for_valid_window_tail() -> None:
    legacy = load_fixture("arrw_event_ticker_handoff.json")
    ledger = without_facts(
        ledger_from_legacy_plan(legacy), "candidate_segments", "terminal_coverage"
    )
    ledger = ledger.append(
        TerminalCoverageFact(
            "AILE",
            "2025-01-02",
            "2026-05-04",
            "event ticker valid bars ended before the requested terminal date",
        ).to_evidence_fact(12716)
    )

    result = plan_backfill(ledger.snapshot())
    actual = legacy_plan_dict(result)

    assert_core_parity(actual, legacy)


def test_pre_event_omission_branch_also_drops_omitted_event_segments() -> None:
    request = BackfillRequest(series_id=1, from_date="2021-01-01", to_date="2021-01-10")
    target = TargetIdentity(
        ohlcv_series_id=1,
        composite_figi="BBG00TARGET",
        latest_ticker="NEW",
        identity_status="permanent",
    )
    facts = (
        EvidenceFact("backfill_request", ("1",), request.to_legacy_dict(), "test"),
        EvidenceFact("target_identity", ("1",), target.to_legacy_dict(), "test"),
        EvidenceFact("known_aliases", ("1",), [], "test"),
        EvidenceFact("plan_metadata", ("1",), {"generated_at_utc": "test"}, "test"),
        TickerEventFact(
            "BBG00TARGET",
            "composite_figi",
            "OK",
            [
                {"date": "2021-01-03", "ticker": "OLD"},
                {"date": "2021-01-06", "ticker": "NEW"},
            ],
        ).to_evidence_fact(1),
        OmittedSegmentFact(
            "OLD", "2021-01-01", "2021-01-02", "pre-event no target bars", source="test"
        ).to_evidence_fact(1),
        OmittedSegmentFact(
            "OLD", "2021-01-03", "2021-01-05", "event no target bars", source="test"
        ).to_evidence_fact(1),
        ReferenceBoundaryFact(
            "NEW",
            "2021-01-06",
            "OK",
            True,
            "composite_figi_match",
            {"point": "start", "matched": True, "match_reason": "composite_figi_match"},
        ).to_evidence_fact(1),
        ReferenceBoundaryFact(
            "NEW",
            "2021-01-10",
            "OK",
            True,
            "composite_figi_match",
            {"point": "end", "matched": True, "match_reason": "composite_figi_match"},
        ).to_evidence_fact(1),
    )

    result = plan_backfill(EvidenceLedger(facts).snapshot())

    assert isinstance(result, BackfillPlan)
    assert [
        (segment.ticker, segment.from_date.isoformat(), segment.to_date.isoformat())
        for segment in result.segments
    ] == [("NEW", "2021-01-06", "2021-01-10")]


def test_multiple_ticker_replacements_apply_without_legacy_candidates() -> None:
    legacy = load_fixture("bncww_multi_ticker_replacement.json")
    ledger = without_facts(ledger_from_legacy_plan(legacy), "candidate_segments")

    result = plan_backfill(ledger.snapshot())
    actual = legacy_plan_dict(result)

    assert actual["status"] == "safe"
    assert [
        (
            segment["ticker"],
            segment["from_date"],
            segment["to_date"],
            segment["ticker_replacement"]["old_ticker"],
        )
        for segment in actual["segments"]
    ] == [
        ("CEADW", "2025-06-10", "2025-06-12", "CEAD"),
        ("VAPEW", "2025-06-13", "2025-08-05", "VAPE"),
        ("BNCWW", "2025-08-06", "2025-08-08", "BNCW"),
    ]
    assert len(actual["warnings"]) == 5


def test_event_ticker_handoff_applies_without_legacy_candidates() -> None:
    legacy = load_fixture("arrw_event_ticker_handoff.json")
    ledger = without_facts(ledger_from_legacy_plan(legacy), "candidate_segments")

    result = plan_backfill(ledger.snapshot())
    actual = legacy_plan_dict(result)

    assert actual["status"] == "safe"
    assert [
        (segment["ticker"], segment["from_date"], segment["to_date"], segment["source"])
        for segment in actual["segments"]
    ] == [
        (
            "ARRW",
            "2021-05-04",
            "2024-04-16",
            "ticker_events+known_alias_target_valid_bar_window_inside_invalid_event_segment",
        ),
        (
            "AILE",
            "2024-04-17",
            "2024-12-31",
            "ticker_events+event_ticker_target_valid_bar_window_after_known_alias",
        ),
    ]
    assert actual["segments"][1]["event_ticker_handoff"]["event_ticker"] == "AILE"
    assert (
        "split between known alias ARRW and event ticker AILE" in actual["warnings"][0]
    )


def test_multi_alias_split_applies_without_legacy_candidates() -> None:
    legacy = load_fixture("shpww_multi_alias_split.json")
    ledger = without_facts(ledger_from_legacy_plan(legacy), "candidate_segments")

    result = plan_backfill(ledger.snapshot())
    actual = legacy_plan_dict(result)

    assert actual["status"] == "safe"
    assert [
        (
            segment["ticker"],
            segment["from_date"],
            segment["to_date"],
            segment["ticker_replacement"]["old_ticker"],
        )
        for segment in actual["segments"]
    ] == [
        ("SHPW.WS", "2021-09-30", "2023-07-31", "SHPW"),
        ("SHPWW", "2023-08-01", "2024-07-16", "SHPW"),
    ]
    assert actual["segments"][0]["ticker_replacement"]["multi_alias_split"][
        "reason"
    ] == ("non_overlapping_target_valid_known_alias_windows")
    assert "split across multiple known aliases" in actual["warnings"][0]
