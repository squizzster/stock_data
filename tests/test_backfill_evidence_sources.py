from __future__ import annotations

from backfill_test_support import *


@pytest.mark.parametrize(
    "fixture_name",
    [
        "ceg_invalid_event_ticker_replacement.json",
        "arrw_event_ticker_handoff.json",
        "shpww_multi_alias_split.json",
    ],
)
def test_static_collector_reproduces_complex_fixtures_without_candidate_segments(
    fixture_name: str,
) -> None:
    legacy = load_fixture(fixture_name)
    ledger = ledger_from_static_source_without_candidates(legacy)

    assert all(fact.kind != "candidate_segments" for fact in ledger.facts)
    assert validate_collected_backfill_facts(ledger.facts) == ()
    result = plan_backfill(ledger.snapshot())

    assert_core_parity(legacy_plan_dict(result), legacy)


def test_static_collector_satisfies_requested_alias_history_round() -> None:
    legacy = load_fixture("barrick_gold_b.json")
    source = StaticBackfillEvidenceSource.from_legacy_plan(
        legacy,
        include_candidate_segments=False,
        defer_kinds=("alias_history",),
    )
    initial = collect_initial_backfill_evidence(source)

    first = plan_backfill(initial.snapshot())
    assert first.__class__.__name__ == "EvidenceNeeded"
    collected = collect_requested_evidence(first, source)

    assert [fact.kind for fact in collected] == ["alias_history"]
    final = plan_backfill(initial.merge(collected).snapshot())
    assert_core_parity(legacy_plan_dict(final), legacy)


def test_evidence_contract_rejects_candidate_segments_by_default() -> None:
    legacy = load_fixture("simple_current_sfbc.json")
    source = StaticBackfillEvidenceSource.from_legacy_plan(
        legacy, include_candidate_segments=True
    )
    issues = validate_collected_backfill_facts(source.initial_facts())

    assert [issue.code for issue in issues] == ["candidate_segments_not_allowed"]


def test_validated_initial_collection_rejects_candidate_segments_by_default() -> None:
    legacy = load_fixture("simple_current_sfbc.json")
    source = StaticBackfillEvidenceSource.from_legacy_plan(
        legacy, include_candidate_segments=True
    )

    with pytest.raises(EvidenceCollectionError) as exc_info:
        collect_initial_backfill_evidence(source, validate=True)

    assert [issue.code for issue in exc_info.value.issues] == [
        "candidate_segments_not_allowed"
    ]


def test_evidence_contract_rejects_unvalidated_replacement_fact() -> None:
    legacy = load_fixture("ceg_invalid_event_ticker_replacement.json")
    ledger = ledger_from_static_source_without_candidates(legacy)
    broken_facts = []
    for fact in ledger.facts:
        if fact.kind == "ticker_replacement":
            payload = fact.payload_value()
            payload["validation"] = []
            broken_facts.append(EvidenceFact(fact.kind, fact.key, payload, fact.source))
        else:
            broken_facts.append(fact)

    issues = validate_collected_backfill_facts(tuple(broken_facts))

    assert "replacement_validation_missing" in {issue.code for issue in issues}


def test_evidence_contract_rejects_unproved_omitted_segment_fact() -> None:
    fact = OmittedSegmentFact(
        "ABML",
        "2023-09-11",
        "2023-09-20",
        "reference and bars were absent",
        source="provider.test",
    ).to_evidence_fact(46)

    issues = validate_collected_backfill_facts((fact,))

    assert {issue.code for issue in issues} == {"omitted_segment_proof_missing"}


def test_evidence_contract_rejects_unproved_terminal_coverage_fact() -> None:
    fact = EvidenceFact(
        "terminal_coverage",
        ("12716", "AILE", "2025-01-01", "2026-05-04"),
        {"ticker": "AILE", "from_date": "2025-01-01", "to_date": "2026-05-04"},
        "test",
    )

    issues = validate_collected_backfill_facts((fact,))

    assert [issue.code for issue in issues] == ["terminal_coverage_field_missing"]


def test_validated_requested_collection_rejects_unvalidated_replacement_fact() -> None:
    legacy = load_fixture("ceg_invalid_event_ticker_replacement.json")
    source = StaticBackfillEvidenceSource.from_legacy_plan(
        legacy,
        include_candidate_segments=False,
        defer_kinds=("ticker_replacement",),
    )
    needed = EvidenceNeeded(
        requests=(EvidenceRequest(kind="ticker_replacement", key=()),)
    )
    broken = []
    for fact in source.requested_facts(needed.requests):
        payload = fact.payload_value()
        payload["validation"] = []
        broken.append(EvidenceFact(fact.kind, fact.key, payload, fact.source))
    broken_source = StaticBackfillEvidenceSource(source.seed_facts, tuple(broken))

    with pytest.raises(EvidenceCollectionError) as exc_info:
        collect_requested_evidence(needed, broken_source, validate=True)

    assert "replacement_validation_missing" in {
        issue.code for issue in exc_info.value.issues
    }


@pytest.mark.parametrize(
    "fixture_name",
    [
        "ceg_invalid_event_ticker_replacement.json",
        "arrw_event_ticker_handoff.json",
        "shpww_multi_alias_split.json",
    ],
)
def test_provider_backed_source_reproduces_complex_fixtures_without_candidate_segments(
    fixture_name: str,
) -> None:
    legacy = load_fixture(fixture_name)
    source = provider_source_from_legacy_plan(
        legacy,
        seed_provider_kinds=(
            "handoff_segment",
            "omitted_segment",
            "reference_boundary",
            "terminal_coverage",
            "ticker_events",
            "ticker_replacement",
        ),
    )

    ledger = collect_initial_backfill_evidence(source, validate=True)
    assert all(fact.kind != "candidate_segments" for fact in ledger.facts)
    result = plan_backfill(ledger.snapshot())

    assert_core_parity(legacy_plan_dict(result), legacy)


def test_provider_backed_source_satisfies_requested_alias_history_round() -> None:
    legacy = load_fixture("barrick_gold_b.json")
    source = provider_source_from_legacy_plan(
        legacy,
        seed_provider_kinds=("ticker_events",),
    )
    initial = collect_initial_backfill_evidence(source, validate=True)

    first = plan_backfill(initial.snapshot())
    assert isinstance(first, EvidenceNeeded)
    collected = collect_requested_evidence(first, source, validate=True)

    assert "alias_history" in [fact.kind for fact in collected]
    final = plan_backfill(initial.merge(collected).snapshot())
    assert_core_parity(legacy_plan_dict(final), legacy)


def test_source_planning_trace_runs_validated_provider_backed_plan() -> None:
    legacy = load_fixture("ceg_invalid_event_ticker_replacement.json")
    source = provider_source_from_legacy_plan(
        legacy,
        seed_provider_kinds=(
            "reference_boundary",
            "ticker_events",
            "ticker_replacement",
        ),
    )

    trace = run_backfill_source_planning_trace(source)

    assert len(trace.rounds) == 1
    assert_core_parity(legacy_plan_dict(trace.plan), legacy)


def test_source_planning_trace_runs_requested_evidence_round() -> None:
    legacy = load_fixture("barrick_gold_b.json")
    source = provider_source_from_legacy_plan(
        legacy, seed_provider_kinds=("ticker_events",)
    )

    trace = run_backfill_source_planning_trace(source)

    assert len(trace.rounds) == 2
    assert isinstance(trace.rounds[0].result, EvidenceNeeded)
    assert "alias_history" in [fact.kind for fact in trace.rounds[0].collected_facts]
    assert_core_parity(legacy_plan_dict(trace.plan), legacy)


def test_reference_snapshot_normalizes_matched_boundary_fact() -> None:
    target = TargetIdentity(ohlcv_series_id=1, composite_figi="BBG000TARGET")
    snapshot = ReferenceSnapshot(
        ticker="ABC",
        as_of_date="2024-01-02",
        api_status="OK",
        composite_figi="BBG000TARGET",
        response_ticker="ABC",
    )

    fact = reference_boundary_fact_from_snapshot(1, target, snapshot, point="start")
    payload = fact.to_legacy_dict()

    assert payload["matched"] is True
    assert payload["match_reason"] == "composite_figi_match"
    assert payload["payload"]["point"] == "start"
    assert payload["payload"]["requested_ticker"] == "ABC"


def test_reference_snapshot_allows_same_ticker_truncated_preferred_issue_name() -> None:
    target = TargetIdentity(
        ohlcv_series_id=1,
        latest_ticker="COFpL",
        latest_primary_exchange="XNYS",
        cik="0000927628",
        security_type="PFD",
        company_name=(
            "Capital One Financial Corporation Depositary Shares, Each Representing a 1/40th Interest "
            "in a Share of Fixed Rate Non- Cumulative Perpetual Preferred Stock, Series L"
        ),
    )
    snapshot = ReferenceSnapshot(
        ticker="COFpL",
        as_of_date="2021-05-10",
        api_status="OK",
        response_ticker="COFpL",
        cik="0000927628",
        primary_exchange="XNYS",
        security_type="PFD",
        raw={
            "ticker": "COFpL",
            "name": "Capital One Financial Corporation Depositary Shares, Each Representing a 1/40th Interest in a Share",
        },
    )

    fact = reference_boundary_fact_from_snapshot(1, target, snapshot, point="start")
    payload = fact.to_legacy_dict()

    assert payload["matched"] is True
    assert payload["match_reason"] == "cik_match"


def test_reference_snapshot_rejects_same_cik_other_preferred_series_without_figi() -> (
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

    fact = reference_boundary_fact_from_snapshot(289, target, snapshot, point="start")
    payload = fact.to_legacy_dict()

    assert payload["matched"] is False
    assert payload["match_reason"] == "cik_match_rejected_distinct_issue_name_mismatch"


def test_reference_snapshot_normalizes_mismatch_boundary_fact() -> None:
    target = TargetIdentity(ohlcv_series_id=1, composite_figi="BBG000TARGET")
    snapshot = ReferenceSnapshot(
        ticker="ABC",
        as_of_date="2024-01-02",
        api_status="OK",
        composite_figi="BBG000OTHER",
    )

    fact = reference_boundary_fact_from_snapshot(1, target, snapshot, point="end")
    payload = fact.to_legacy_dict()

    assert payload["matched"] is False
    assert (
        "composite_figi detail=BBG000OTHER target=BBG000TARGET"
        in payload["match_reason"]
    )


def test_reference_boundary_probe_requires_explicit_point() -> None:
    snapshot = ReferenceSnapshot("ABC", "2024-01-02", "OK")

    with pytest.raises(ValueError, match="point must be"):
        ReferenceBoundaryProbe("ABC", "2024-01-02", "boundary", snapshot)


def test_static_provider_read_fact_provider_uses_explicit_boundary_probe_point() -> (
    None
):
    target = TargetIdentity(ohlcv_series_id=1, composite_figi="BBG000TARGET")
    provider = StaticProviderReadFactProvider(
        reference_boundary_probes=(
            ReferenceBoundaryProbe(
                "ABC",
                "2024-01-02",
                "end",
                ReferenceSnapshot(
                    "ABC", "2024-01-02", "OK", composite_figi="BBG000TARGET"
                ),
            ),
        ),
        seed_kinds=("reference_boundary",),
    )

    facts = provider.initial_facts(
        BackfillRequest(series_id=1, from_date="2024-01-02", to_date="2024-01-02"),
        target,
    )
    payload = facts[0].payload_value()

    assert payload["payload"]["point"] == "end"


def test_static_provider_read_fact_provider_does_not_guess_boundary_point() -> None:
    target = TargetIdentity(ohlcv_series_id=1, composite_figi="BBG000TARGET")
    provider = StaticProviderReadFactProvider(
        reference_snapshots=(
            ReferenceSnapshot("ABC", "2024-01-02", "OK", composite_figi="BBG000TARGET"),
        ),
        seed_kinds=("reference_boundary",),
    )

    facts = provider.initial_facts(
        BackfillRequest(series_id=1, from_date="2024-01-02", to_date="2024-01-02"),
        target,
    )

    assert facts == ()


def test_bar_probe_result_normalizes_bar_probe_fact() -> None:
    result = BarProbeResult(
        ticker="ABC",
        from_date="2024-01-02",
        to_date="2024-01-05",
        bar_count=3,
        api_status="OK",
    )

    fact = bar_probe_fact_from_result(1, result)
    payload = fact.to_legacy_dict()

    assert payload == {
        "api_status": "OK",
        "bar_count": 3,
        "from_date": "2024-01-02",
        "ticker": "ABC",
        "to_date": "2024-01-05",
    }


def test_identity_scan_result_normalizes_identity_scan_fact() -> None:
    result = IdentityScanResult("ABML", "2023-09-11", matches=())

    fact = identity_scan_fact_from_result(46, result)

    assert fact.to_evidence_fact(46).to_legacy_dict() == {
        "kind": "identity_scan",
        "key": ["46", "ABML", "2023-09-11"],
        "payload": {"as_of_date": "2023-09-11", "matches": [], "query": "ABML"},
        "source": "provider.identity_scan",
    }


def test_absent_reference_and_no_bars_normalizes_omitted_segment_fact() -> None:
    start_reference = ReferenceSnapshot("ABML", "2023-09-11", "NOT_FOUND")
    end_reference = ReferenceSnapshot("ABML", "2023-09-20", "NOT_FOUND")
    bars = BarProbeResult("ABML", "2023-09-11", "2023-09-20", 0, api_status="OK")
    start_scan = IdentityScanResult("ABML", "2023-09-11", matches=())
    end_scan = IdentityScanResult("ABML", "2023-09-20", matches=())

    fact = omitted_segment_fact_from_absent_reference_and_bars(
        46,
        ticker="ABML",
        from_date="2023-09-11",
        to_date="2023-09-20",
        start_reference=start_reference,
        end_reference=end_reference,
        bar_probe=bars,
        start_identity_scan=start_scan,
        end_identity_scan=end_scan,
    )

    assert fact is not None
    assert fact.to_evidence_fact(46).kind == "omitted_segment"
    payload = fact.to_legacy_dict()
    assert payload["from_date"] == "2023-09-11"
    assert payload["ticker"] == "ABML"
    assert payload["to_date"] == "2023-09-20"
    assert payload["proof"]["start_reference"]["api_status"] == "NOT_FOUND"
    assert payload["proof"]["end_reference"]["api_status"] == "NOT_FOUND"
    assert payload["proof"]["bar_probe"]["bar_count"] == 0
    assert payload["proof"]["start_identity_scan"]["match_count"] == 0
    assert payload["proof"]["end_identity_scan"]["match_count"] == 0


def test_absent_reference_does_not_omit_when_bars_exist() -> None:
    start_reference = ReferenceSnapshot("ABML", "2023-09-11", "NOT_FOUND")
    end_reference = ReferenceSnapshot("ABML", "2023-09-20", "NOT_FOUND")
    bars = BarProbeResult("ABML", "2023-09-11", "2023-09-20", 2, api_status="OK")
    start_scan = IdentityScanResult("ABML", "2023-09-11", matches=())
    end_scan = IdentityScanResult("ABML", "2023-09-20", matches=())

    fact = omitted_segment_fact_from_absent_reference_and_bars(
        46,
        ticker="ABML",
        from_date="2023-09-11",
        to_date="2023-09-20",
        start_reference=start_reference,
        end_reference=end_reference,
        bar_probe=bars,
        start_identity_scan=start_scan,
        end_identity_scan=end_scan,
    )

    assert fact is None


def test_absent_reference_does_not_omit_when_identity_scan_has_match() -> None:
    start_reference = ReferenceSnapshot("ABML", "2023-09-11", "NOT_FOUND")
    end_reference = ReferenceSnapshot("ABML", "2023-09-20", "NOT_FOUND")
    bars = BarProbeResult("ABML", "2023-09-11", "2023-09-20", 0, api_status="OK")

    fact = omitted_segment_fact_from_absent_reference_and_bars(
        46,
        ticker="ABML",
        from_date="2023-09-11",
        to_date="2023-09-20",
        start_reference=start_reference,
        end_reference=end_reference,
        bar_probe=bars,
        start_identity_scan=IdentityScanResult(
            "ABML", "2023-09-11", matches=({"ticker": "ABML"},)
        ),
        end_identity_scan=IdentityScanResult("ABML", "2023-09-20", matches=()),
    )

    assert fact is None


def test_target_valid_alias_window_normalizes_ticker_replacement_fact() -> None:
    target = TargetIdentity(ohlcv_series_id=2005, composite_figi="BBG00AENPS55")
    start_reference = ReferenceSnapshot(
        "CEGVV",
        "2022-01-19",
        "OK",
        composite_figi="BBG00AENPS55",
        response_ticker="CEGVV",
    )
    end_reference = ReferenceSnapshot(
        "CEGVV",
        "2022-02-01",
        "OK",
        composite_figi="BBG00AENPS55",
        response_ticker="CEGVV",
    )

    fact = ticker_replacement_fact_from_target_valid_alias_window(
        2005,
        target,
        old_ticker="CEGV",
        new_ticker="CEGVV",
        from_date="2022-01-19",
        to_date="2022-02-01",
        start_reference=start_reference,
        end_reference=end_reference,
        event_date="2022-01-19",
    )

    assert fact is not None
    payload = fact.to_evidence_fact(2005).payload_value()
    assert payload["old_ticker"] == "CEGV"
    assert payload["new_ticker"] == "CEGVV"
    assert payload["replacement_reason"] == "known_alias_boundary_validation"
    assert [row["point"] for row in payload["validation"]] == ["start", "end"]
    assert all(row["matched"] is True for row in payload["validation"])


def test_target_valid_alias_window_rejects_unmatched_boundary() -> None:
    target = TargetIdentity(ohlcv_series_id=2005, composite_figi="BBG00AENPS55")
    start_reference = ReferenceSnapshot(
        "CEGVV", "2022-01-19", "OK", composite_figi="BBG00AENPS55"
    )
    end_reference = ReferenceSnapshot(
        "CEGVV", "2022-02-01", "OK", composite_figi="BBG00OTHER"
    )

    fact = ticker_replacement_fact_from_target_valid_alias_window(
        2005,
        target,
        old_ticker="CEGV",
        new_ticker="CEGVV",
        from_date="2022-01-19",
        to_date="2022-02-01",
        start_reference=start_reference,
        end_reference=end_reference,
    )

    assert fact is None


def test_derived_ticker_replacement_reproduces_ceg_without_legacy_replacement_fact() -> (
    None
):
    legacy = load_fixture("ceg_invalid_event_ticker_replacement.json")
    facts = [
        fact
        for fact in facts_from_legacy_plan(legacy, include_candidate_segments=False)
        if fact.kind != "ticker_replacement"
    ]
    target = TargetIdentity.from_legacy_dict(legacy["target"])
    derived = ticker_replacement_fact_from_target_valid_alias_window(
        2005,
        target,
        old_ticker="CEGV",
        new_ticker="CEGVV",
        from_date="2022-01-19",
        to_date="2022-02-01",
        start_reference=ReferenceSnapshot(
            "CEGVV", "2022-01-19", "OK", composite_figi=target.composite_figi
        ),
        end_reference=ReferenceSnapshot(
            "CEGVV", "2022-02-01", "OK", composite_figi=target.composite_figi
        ),
        event_date="2022-01-19",
    )
    assert derived is not None
    ledger = EvidenceLedger(tuple(facts) + (derived.to_evidence_fact(2005),))

    result = plan_backfill(ledger.snapshot())

    assert_core_parity(legacy_plan_dict(result), legacy)


def test_target_valid_event_window_normalizes_handoff_segment_fact() -> None:
    target = TargetIdentity(ohlcv_series_id=12716, composite_figi="BBG00TARGET")
    start_reference = ReferenceSnapshot(
        "AILE", "2024-04-17", "OK", composite_figi="BBG00TARGET"
    )
    end_reference = ReferenceSnapshot(
        "AILE", "2024-12-31", "OK", composite_figi="BBG00TARGET"
    )

    fact = handoff_segment_fact_from_target_valid_event_window(
        12716,
        target,
        event_ticker="AILE",
        from_date="2024-04-17",
        to_date="2024-12-31",
        start_reference=start_reference,
        end_reference=end_reference,
        candidate_ticker="ARRW",
        event_date="2024-04-17",
    )

    assert fact is not None
    payload = fact.to_evidence_fact(12716).payload_value()
    assert payload["ticker"] == "AILE"
    assert payload["event_ticker_handoff"]["candidate_ticker"] == "ARRW"
    assert payload["event_ticker_handoff"]["event_ticker"] == "AILE"
    assert [row["point"] for row in payload["validation"]] == ["start", "end"]


def test_target_valid_event_window_rejects_unmatched_handoff_boundary() -> None:
    target = TargetIdentity(ohlcv_series_id=12716, composite_figi="BBG00TARGET")
    start_reference = ReferenceSnapshot(
        "AILE", "2024-04-17", "OK", composite_figi="BBG00TARGET"
    )
    end_reference = ReferenceSnapshot(
        "AILE", "2024-12-31", "OK", composite_figi="BBG00OTHER"
    )

    fact = handoff_segment_fact_from_target_valid_event_window(
        12716,
        target,
        event_ticker="AILE",
        from_date="2024-04-17",
        to_date="2024-12-31",
        start_reference=start_reference,
        end_reference=end_reference,
        candidate_ticker="ARRW",
    )

    assert fact is None


def test_derived_handoff_segment_reproduces_arrw_without_legacy_handoff_fact() -> None:
    legacy = load_fixture("arrw_event_ticker_handoff.json")
    facts = [
        fact
        for fact in facts_from_legacy_plan(legacy, include_candidate_segments=False)
        if fact.kind != "handoff_segment"
    ]
    target = TargetIdentity.from_legacy_dict(legacy["target"])
    derived = handoff_segment_fact_from_target_valid_event_window(
        12716,
        target,
        event_ticker="AILE",
        from_date="2024-04-17",
        to_date="2024-12-31",
        start_reference=ReferenceSnapshot(
            "AILE", "2024-04-17", "OK", composite_figi=target.composite_figi
        ),
        end_reference=ReferenceSnapshot(
            "AILE", "2024-12-31", "OK", composite_figi=target.composite_figi
        ),
        candidate_ticker="ARRW",
        event_date="2024-04-17",
    )
    assert derived is not None
    ledger = EvidenceLedger(tuple(facts) + (derived.to_evidence_fact(12716),))

    result = plan_backfill(ledger.snapshot())

    assert_core_parity(legacy_plan_dict(result), legacy)


def test_static_provider_read_fact_provider_derives_omitted_segment_fact() -> None:
    legacy = load_fixture("abat_empty_abml_segment.json")
    source = provider_source_from_legacy_plan(
        legacy, seed_provider_kinds=("ticker_events",)
    )
    read_provider = StaticProviderReadFactProvider(
        reference_boundary_probes=(
            boundary_probe(
                "ABML",
                "2023-09-11",
                "start",
                ReferenceSnapshot("ABML", "2023-09-11", "NOT_FOUND"),
            ),
            boundary_probe(
                "ABML",
                "2023-09-20",
                "end",
                ReferenceSnapshot("ABML", "2023-09-20", "NOT_FOUND"),
            ),
        ),
        bar_probes=(
            BarProbeResult("ABML", "2023-09-11", "2023-09-20", 0, api_status="OK"),
        ),
        identity_scans=(
            IdentityScanResult("ABML", "2023-09-11", matches=()),
            IdentityScanResult("ABML", "2023-09-20", matches=()),
        ),
        omitted_segments=(OmittedSegmentProbe("ABML", "2023-09-11", "2023-09-20"),),
        seed_kinds=("omitted_segment",),
    )
    source = ProviderBackfillEvidenceSource(
        source.base_facts,
        BackfillProviderSet(source.providers.providers + (read_provider,)),
    )

    result = run_backfill_source_planning_trace(source).plan

    assert_core_parity(legacy_plan_dict(result), legacy)


def test_static_provider_read_fact_provider_derives_replacement_fact() -> None:
    legacy = load_fixture("ceg_invalid_event_ticker_replacement.json")
    source = provider_source_from_legacy_plan(
        legacy, seed_provider_kinds=("ticker_events",)
    )
    target = TargetIdentity.from_legacy_dict(legacy["target"])
    read_provider = StaticProviderReadFactProvider(
        reference_boundary_probes=(
            boundary_probe(
                "CEGVV",
                "2022-01-19",
                "start",
                ReferenceSnapshot(
                    "CEGVV", "2022-01-19", "OK", composite_figi=target.composite_figi
                ),
            ),
            boundary_probe(
                "CEGVV",
                "2022-02-01",
                "end",
                ReferenceSnapshot(
                    "CEGVV", "2022-02-01", "OK", composite_figi=target.composite_figi
                ),
            ),
        ),
        ticker_replacements=(
            TickerReplacementWindow(
                "CEGV", "CEGVV", "2022-01-19", "2022-02-01", event_date="2022-01-19"
            ),
        ),
        seed_kinds=("ticker_replacement",),
    )
    source = ProviderBackfillEvidenceSource(
        source.base_facts,
        BackfillProviderSet(source.providers.providers + (read_provider,)),
    )

    result = run_backfill_source_planning_trace(source).plan

    assert_core_parity(legacy_plan_dict(result), legacy)


def test_static_provider_read_fact_provider_derives_handoff_fact() -> None:
    legacy = load_fixture("arrw_event_ticker_handoff.json")
    source = provider_source_from_legacy_plan(
        legacy, seed_provider_kinds=("ticker_events", "ticker_replacement")
    )
    target = TargetIdentity.from_legacy_dict(legacy["target"])
    read_provider = StaticProviderReadFactProvider(
        reference_boundary_probes=(
            boundary_probe(
                "AILE",
                "2024-04-17",
                "start",
                ReferenceSnapshot(
                    "AILE", "2024-04-17", "OK", composite_figi=target.composite_figi
                ),
            ),
            boundary_probe(
                "AILE",
                "2024-12-31",
                "end",
                ReferenceSnapshot(
                    "AILE", "2024-12-31", "OK", composite_figi=target.composite_figi
                ),
            ),
        ),
        handoffs=(
            HandoffWindow(
                "AILE", "ARRW", "2024-04-17", "2024-12-31", event_date="2024-04-17"
            ),
        ),
        seed_kinds=("handoff_segment",),
    )
    source = ProviderBackfillEvidenceSource(
        source.base_facts,
        BackfillProviderSet(source.providers.providers + (read_provider,)),
    )

    result = run_backfill_source_planning_trace(source).plan

    assert_core_parity(legacy_plan_dict(result), legacy)


def test_static_provider_read_fact_provider_derives_multi_alias_split_facts() -> None:
    legacy = load_fixture("shpww_multi_alias_split.json")
    source = provider_source_from_legacy_plan(
        legacy, seed_provider_kinds=("ticker_events",)
    )
    target = TargetIdentity.from_legacy_dict(legacy["target"])
    read_provider = StaticProviderReadFactProvider(
        reference_boundary_probes=(
            boundary_probe(
                "SHPS",
                "2021-05-04",
                "start",
                ReferenceSnapshot("SHPS", "2021-05-04", "NOT_FOUND"),
            ),
            boundary_probe(
                "SHPS",
                "2021-09-29",
                "end",
                ReferenceSnapshot("SHPS", "2021-09-29", "NOT_FOUND"),
            ),
            boundary_probe(
                "SHPW.WS",
                "2021-09-30",
                "start",
                ReferenceSnapshot(
                    "SHPW.WS", "2021-09-30", "OK", composite_figi=target.composite_figi
                ),
            ),
            boundary_probe(
                "SHPW.WS",
                "2023-07-31",
                "end",
                ReferenceSnapshot(
                    "SHPW.WS", "2023-07-31", "OK", composite_figi=target.composite_figi
                ),
            ),
            boundary_probe(
                "SHPWW",
                "2023-08-01",
                "start",
                ReferenceSnapshot(
                    "SHPWW", "2023-08-01", "OK", composite_figi=target.composite_figi
                ),
            ),
            boundary_probe(
                "SHPWW",
                "2024-07-16",
                "end",
                ReferenceSnapshot(
                    "SHPWW", "2024-07-16", "OK", composite_figi=target.composite_figi
                ),
            ),
        ),
        bar_probes=(
            BarProbeResult("SHPS", "2021-05-04", "2021-09-29", 0, api_status="OK"),
        ),
        identity_scans=(
            IdentityScanResult("SHPS", "2021-05-04", matches=()),
            IdentityScanResult("SHPS", "2021-09-29", matches=()),
        ),
        omitted_segments=(OmittedSegmentProbe("SHPS", "2021-05-04", "2021-09-29"),),
        ticker_replacements=(
            TickerReplacementWindow(
                "SHPW",
                "SHPW.WS",
                "2021-09-30",
                "2023-07-31",
                event_date="2021-09-30",
                replacement_reason="known_alias_target_valid_bar_window_inside_invalid_event_segment",
            ),
            TickerReplacementWindow(
                "SHPW",
                "SHPWW",
                "2023-08-01",
                "2024-07-16",
                event_date="2021-09-30",
                replacement_reason="known_alias_target_valid_bar_window_inside_invalid_event_segment",
            ),
        ),
        seed_kinds=("omitted_segment", "ticker_replacement"),
    )
    source = ProviderBackfillEvidenceSource(
        source.base_facts,
        BackfillProviderSet(source.providers.providers + (read_provider,)),
    )

    result = run_backfill_source_planning_trace(source).plan

    assert_core_parity(legacy_plan_dict(result), legacy)
