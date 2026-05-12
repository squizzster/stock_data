from __future__ import annotations

import json
from types import SimpleNamespace

from backfill_test_support import *
from stock_universe.bar_quality import OhlcvValues, structural_issues
from stock_universe.domain import PlannedSegment
from stock_universe.market_calendar import MARKET_CALENDAR_ENV
from stock_universe.planner.coverage import _drop_omitted_segments, _uncovered_ranges
from stock_universe.planner.initial_segments import _event_segments_for_range
from stock_universe.quality_audit import _next_action_for_row, quality_audit
from stock_universe.quality_repair import repair_missing_execution_receipts
from stock_universe.storage import StoredOhlcvBar, StoredReferenceSnapshot


def test_ticker_events_request_more_evidence_when_no_event_intersects_range() -> None:
    legacy = load_fixture("ticker_rename_meta.json")
    legacy["range"]["from_date"] = "2010-01-01"
    legacy["range"]["to_date"] = "2010-01-05"
    ledger = without_facts(ledger_from_legacy_plan(legacy), "candidate_segments")

    result = plan_backfill(ledger.snapshot())

    assert result.__class__.__name__ == "EvidenceNeeded"
    assert [request.kind for request in result.requests] == ["alias_history"]
    assert result.requests[0].key == ("7034", "2010-01-01", "2010-01-06")
    assert "alias-history evidence" in result.decisions[0].reason


def test_caution_plan_legacy_shape() -> None:
    legacy = load_fixture("caution_cnh.json")
    result = plan_backfill(ledger_from_legacy_plan(legacy).snapshot())

    actual = legacy_plan_dict(result)
    assert_core_parity(actual, legacy)
    assert actual["status"] == "caution"
    assert actual["errors"] == []
    assert len(actual["warnings"]) == 4


def test_blocked_plan_legacy_shape() -> None:
    legacy = load_fixture("blocked_cnh.json")
    result = plan_backfill(ledger_from_legacy_plan(legacy).snapshot())

    actual = legacy_plan_dict(result)
    assert_core_parity(actual, legacy)
    assert actual["status"] == "blocked"
    assert len(actual["errors"]) == 2


def test_missing_evidence_returns_evidence_needed() -> None:
    result = plan_backfill(EvidenceLedger().snapshot())

    assert result.__class__.__name__ == "EvidenceNeeded"
    assert {request.kind for request in result.requests} == {
        "backfill_request",
        "target_identity",
        "known_aliases",
        "ticker_events",
        "plan_metadata",
    }


def test_backfill_request_accepts_supported_intraday_grains() -> None:
    one_minute = BackfillRequest(
        series_id=1,
        from_date="2026-05-01",
        to_date="2026-05-01",
        multiplier=1,
        timespan="minute",
    )
    thirty_minute = BackfillRequest.from_legacy_dict(
        1,
        {
            "from_date": "2026-05-01",
            "to_date": "2026-05-01",
            "bar_grain": "30m",
            "adjusted": True,
        },
    )

    assert one_minute.to_legacy_dict()["timespan"] == "minute"
    assert (
        one_minute.request_hash
        != BackfillRequest(
            series_id=1, from_date="2026-05-01", to_date="2026-05-01"
        ).request_hash
    )
    assert thirty_minute.multiplier == 30
    assert thirty_minute.timespan == "minute"


def test_backfill_request_rejects_unsupported_intraday_grain() -> None:
    try:
        BackfillRequest(
            series_id=1,
            from_date="2026-05-01",
            to_date="2026-05-01",
            multiplier=5,
            timespan="minute",
        )
    except ValueError as exc:
        assert "supported bar grains" in str(exc)
    else:
        raise AssertionError("expected unsupported minute grain rejection")


def test_overlapping_segments_block_even_without_legacy_errors() -> None:
    legacy = load_fixture("ticker_rename_meta.json")
    legacy["errors"] = []
    legacy["warnings"] = []
    legacy["segments"][1]["from_date"] = legacy["segments"][0]["to_date"]

    result = plan_backfill(ledger_from_legacy_plan(legacy).snapshot())
    actual = legacy_plan_dict(result)

    assert actual["status"] == "blocked"
    assert any("overlaps prior segment" in error for error in actual["errors"])


def test_out_of_range_segment_blocks_even_without_legacy_errors() -> None:
    legacy = load_fixture("simple_current_sfbc.json")
    legacy["errors"] = []
    legacy["warnings"] = []
    legacy["segments"][0]["from_date"] = "2021-05-03"

    result = plan_backfill(ledger_from_legacy_plan(legacy).snapshot())
    actual = legacy_plan_dict(result)

    assert actual["status"] == "blocked"
    assert any("falls outside requested range" in error for error in actual["errors"])


def test_ledger_hash_changes_when_evidence_changes() -> None:
    legacy = load_fixture("simple_current_sfbc.json")
    first = ledger_from_legacy_plan(legacy)
    second = first.append(EvidenceFact("note", ("fixture",), {"value": 1}, "test"))

    assert first.ledger_hash != second.ledger_hash


def test_ticker_event_fact_normalizes_provider_payload() -> None:
    provider_payload = {
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

    fact = TickerEventFact.from_provider_payload(
        "BBG000MM2P62", "composite_figi", provider_payload
    )

    assert fact.to_legacy_dict()["events"] == [
        {"date": "2012-05-18", "ticker": "FB", "type": "ticker_change"},
        {"date": "2022-06-09", "ticker": "META", "type": "ticker_change"},
    ]
    assert fact.to_evidence_fact(7034).kind == "ticker_events"


def test_boundary_probe_facts_are_evidence_records() -> None:
    reference = ReferenceBoundaryFact(
        ticker="META",
        as_of_date="2022-06-09",
        api_status="OK",
        matched=True,
        match_reason="composite_figi_match",
        payload={"composite_figi": "BBG000MM2P62"},
    ).to_evidence_fact(7034)
    bars = BarProbeFact(
        "META", "2022-06-09", "2022-06-10", 2, api_status="OK"
    ).to_evidence_fact(7034)

    assert reference.kind == "reference_boundary"
    assert reference.payload_value()["payload"]["composite_figi"] == "BBG000MM2P62"
    assert bars.kind == "bar_probe"
    assert bars.payload_value()["bar_count"] == 2


def test_alias_history_fact_extracts_legacy_pre_event_segments() -> None:
    legacy = load_fixture("barrick_gold_b.json")

    fact = AliasHistoryFact.from_legacy_segments(legacy["segments"]).to_evidence_fact(
        989
    )

    assert fact.kind == "alias_history"
    assert fact.payload_value()["spans"][0]["ticker"] == "GOLD"
    assert (
        fact.payload_value()["spans"][0]["source"]
        == "known_alias_pre_event_bar_validation"
    )


def test_omitted_segment_fact_is_evidence_record() -> None:
    fact = OmittedSegmentFact(
        "ABML", "2023-09-11", "2023-09-20", "no reference or bars"
    ).to_evidence_fact(46)

    assert fact.kind == "omitted_segment"
    assert fact.payload_value()["ticker"] == "ABML"
    assert fact.payload_value()["reason"] == "no reference or bars"


def test_ticker_replacement_fact_extracts_legacy_replacement_segment() -> None:
    legacy = load_fixture("ceg_invalid_event_ticker_replacement.json")

    fact = TickerReplacementFact.from_legacy_segment(
        legacy["segments"][0]
    ).to_evidence_fact(2005)

    assert fact.kind == "ticker_replacement"
    assert fact.payload_value()["old_ticker"] == "CEGV"
    assert fact.payload_value()["new_ticker"] == "CEGVV"
    assert (
        fact.payload_value()["replacement_reason"] == "known_alias_boundary_validation"
    )


def test_handoff_segment_fact_extracts_legacy_handoff_segment() -> None:
    legacy = load_fixture("arrw_event_ticker_handoff.json")

    fact = HandoffSegmentFact.from_legacy_segment(
        legacy["segments"][1]
    ).to_evidence_fact(12716)

    assert fact.kind == "handoff_segment"
    assert fact.payload_value()["ticker"] == "AILE"
    assert fact.payload_value()["event_ticker_handoff"]["candidate_ticker"] == "ARRW"


def test_planning_trace_exposes_evidence_needed_round() -> None:
    legacy = load_fixture("ticker_rename_meta.json")
    full_ledger = ledger_from_legacy_plan(legacy)
    initial = without_facts(full_ledger, "candidate_segments", "ticker_events")

    def collect(result):
        assert result.__class__.__name__ == "EvidenceNeeded"
        return tuple(fact for fact in full_ledger.facts if fact.kind == "ticker_events")

    trace = run_backfill_planning_trace(initial, collect, max_rounds=2)

    assert trace.plan.status == "safe"
    assert len(trace.rounds) == 2
    assert trace.rounds[0].result.__class__.__name__ == "EvidenceNeeded"
    assert trace.rounds[0].collected_facts[0].kind == "ticker_events"


def test_planning_trace_stops_cleanly_when_non_dry_evidence_is_unresolved() -> None:
    legacy = load_fixture("ticker_rename_meta.json")
    full_ledger = ledger_from_legacy_plan(legacy)
    initial = without_facts(full_ledger, "candidate_segments", "ticker_events")

    trace = run_backfill_planning_trace(initial, lambda needed: (), max_rounds=2)

    assert isinstance(trace.result, EvidenceNeeded)
    assert [request.kind for request in trace.result.requests] == ["ticker_events"]
    assert len(trace.rounds) == 1
    assert trace.rounds[0].collected_facts == ()
    with pytest.raises(RuntimeError, match="final plan"):
        trace.plan


def test_xctx_evidence_needed_envelope_blocks_execution() -> None:
    result = plan_backfill(EvidenceLedger().snapshot())
    envelope = result_envelope("backfill-plan", result)

    assert envelope["ok"] is False
    assert envelope["result_type"] == "EvidenceNeeded"
    assert envelope["protocol_version"] == "xctx.v2"
    assert action_names(envelope["next_actions"]) == ["collect-missing-evidence"]
    assert envelope["invalid_next_actions"][0]["name"] == "execute-approved-plan"
    assert envelope["next_actions"][0]["command"]["name"] == "xctx dry-run"


def test_xctx_alias_history_gap_exposes_manual_repair_action() -> None:
    legacy = load_fixture("barrick_gold_b.json")
    ledger = without_facts(
        ledger_from_legacy_plan(legacy), "candidate_segments", "alias_history"
    )
    result = plan_backfill(ledger.snapshot())

    envelope = result_envelope("backfill-plan", result)

    assert action_names(envelope["next_actions"]) == [
        "collect-missing-evidence",
        "apply-repair",
    ]
    assert envelope["repairs"] == [
        {
            "name": "provide-alias-history",
            "evidence_kind": "alias_history",
            "request": {
                "kind": "alias_history",
                "key": ["989", "2025-05-08", "2025-05-09", "B"],
            },
            "effect": {
                "kind": "append-evidence-fact",
                "target": "alias_history",
                "description": "Append a typed evidence fact outside xctx; xctx only emits the protocol payload.",
            },
            "reason": (
                "No live alias-history collector is available; provide an explicit AliasHistoryFact "
                "covering the requested pre-event interval."
            ),
            "command": {
                "name": "xctx repair",
                "description": "Emit repair guidance for unresolved evidence.",
                "args": {},
                "reads": [],
                "writes": [],
            },
        },
        {
            "name": "resolve-coverage-gap",
            "evidence_kind": "coverage_gap",
            "request": {
                "kind": "coverage_gap",
                "key": ["989", "B", "2025-05-08", "2025-05-08"],
            },
            "effect": {
                "kind": "append-evidence-fact",
                "target": "coverage_gap",
                "description": "Append a typed evidence fact outside xctx; xctx only emits the protocol payload.",
            },
            "reason": (
                "Append ticker-replacement, handoff, omitted-segment, or terminal-coverage evidence "
                "that fully accounts for the requested uncovered interval."
            ),
            "command": {
                "name": "xctx repair",
                "description": "Emit repair guidance for unresolved evidence.",
                "args": {},
                "reads": [],
                "writes": [],
            },
        },
    ]


def test_xctx_coverage_gap_exposes_repair_action() -> None:
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

    envelope = result_envelope("backfill-plan", result)

    assert envelope["repairs"] == [
        {
            "name": "resolve-coverage-gap",
            "evidence_kind": "coverage_gap",
            "request": {
                "kind": "coverage_gap",
                "key": ["2005", "CEGV", "2022-01-26", "2022-02-01"],
            },
            "effect": {
                "kind": "append-evidence-fact",
                "target": "coverage_gap",
                "description": "Append a typed evidence fact outside xctx; xctx only emits the protocol payload.",
            },
            "reason": (
                "Append ticker-replacement, handoff, omitted-segment, or terminal-coverage evidence "
                "that fully accounts for the requested uncovered interval."
            ),
            "command": {
                "name": "xctx repair",
                "description": "Emit repair guidance for unresolved evidence.",
                "args": {},
                "reads": [],
                "writes": [],
            },
        }
    ]


def test_omitted_segments_account_for_replacement_coverage_gaps() -> None:
    target = TargetIdentity(
        ohlcv_series_id=1, composite_figi="BBG000TARGET", latest_ticker="NEW"
    )
    facts = (
        EvidenceFact(
            "backfill_request",
            ("1",),
            BackfillRequest(
                series_id=1, from_date="2024-01-01", to_date="2024-01-05"
            ).to_legacy_dict(),
            "test",
        ),
        EvidenceFact("target_identity", ("1",), target.to_legacy_dict(), "test"),
        EvidenceFact("known_aliases", ("1",), [], "test"),
        EvidenceFact(
            "plan_metadata",
            ("1",),
            {"generated_at_utc": "2026-01-01T00:00:00+00:00"},
            "test",
        ),
        TickerEventFact(
            "BBG000TARGET",
            "composite_figi",
            "OK",
            [{"date": "2024-01-01", "ticker": "OLD"}],
        ).to_evidence_fact(1),
        TickerReplacementFact(
            old_ticker="OLD",
            new_ticker="NEW",
            from_date="2024-01-02",
            to_date="2024-01-03",
            replacement_reason="test_replacement",
            source="test",
        ).to_evidence_fact(1),
        OmittedSegmentFact(
            "OLD", "2024-01-01", "2024-01-01", "no target bars", source="test"
        ).to_evidence_fact(1),
        OmittedSegmentFact(
            "OLD", "2024-01-04", "2024-01-05", "no target bars", source="test"
        ).to_evidence_fact(1),
        ReferenceBoundaryFact(
            "NEW", "2024-01-02", "OK", True, "composite_figi_match", {"point": "start"}
        ).to_evidence_fact(1),
        ReferenceBoundaryFact(
            "NEW", "2024-01-03", "OK", True, "composite_figi_match", {"point": "end"}
        ).to_evidence_fact(1),
    )

    result = plan_backfill(EvidenceLedger(facts).snapshot())

    assert isinstance(result, BackfillPlan)
    assert [
        (segment.ticker, segment.from_date.isoformat(), segment.to_date.isoformat())
        for segment in result.segments
    ] == [("NEW", "2024-01-02", "2024-01-03")]


def test_omitted_segments_do_not_drop_validated_alias_windows() -> None:
    evidence = EvidenceLedger(
        (
            OmittedSegmentFact(
                "ATAI",
                "2021-05-10",
                "2026-01-01",
                "same ticker successor gap before new reference identity",
                source="test",
            ).to_evidence_fact(814),
        )
    ).snapshot()
    segments = [
        PlannedSegment(
            segment_index=1,
            ticker="ATAI",
            from_date="2021-06-18",
            to_date="2025-12-31",
            source="massive.same_ticker_successor_bar_window",
            validation={
                "matched": True,
                "match_reason": "same_ticker_successor_identity",
            },
        ),
        PlannedSegment(
            segment_index=2,
            ticker="ATAI",
            from_date="2021-06-18",
            to_date="2025-12-31",
            source="ticker_events",
        ),
    ]

    kept = _drop_omitted_segments(evidence, segments)

    assert [(segment.segment_index, segment.source) for segment in kept] == [
        (1, "massive.same_ticker_successor_bar_window")
    ]


def test_uncovered_ranges_skip_market_holidays_between_segments() -> None:
    evidence = EvidenceLedger(()).snapshot()
    original = [
        PlannedSegment(
            segment_index=1,
            ticker="HOL",
            from_date="2026-07-02",
            to_date="2026-07-06",
            source="ticker_events",
        )
    ]
    transformed = [
        PlannedSegment(
            segment_index=1,
            ticker="HOL",
            from_date="2026-07-02",
            to_date="2026-07-02",
            source="test",
        ),
        PlannedSegment(
            segment_index=2,
            ticker="HOL",
            from_date="2026-07-06",
            to_date="2026-07-06",
            source="test",
        ),
    ]

    assert _uncovered_ranges(evidence, original, transformed) == ()


def test_event_segments_end_on_previous_market_session_before_next_event() -> None:
    request = BackfillRequest(series_id=1, from_date="2026-07-02", to_date="2026-07-06")
    segments = _event_segments_for_range(
        request,
        [
            {"date": dt.date(2026, 7, 2), "ticker": "OLD"},
            {"date": dt.date(2026, 7, 6), "ticker": "NEW"},
        ],
    )

    assert [
        (segment.ticker, segment.from_date.isoformat(), segment.to_date.isoformat())
        for segment in segments
    ] == [
        ("OLD", "2026-07-02", "2026-07-02"),
        ("NEW", "2026-07-06", "2026-07-06"),
    ]


def test_omitted_terminal_event_segment_requires_latest_ticker_coverage() -> None:
    target = TargetIdentity(
        ohlcv_series_id=1, composite_figi="BBG000TARGET", latest_ticker="NEW"
    )
    facts = (
        EvidenceFact(
            "backfill_request",
            ("1",),
            BackfillRequest(
                series_id=1, from_date="2024-01-01", to_date="2024-01-10"
            ).to_legacy_dict(),
            "test",
        ),
        EvidenceFact("target_identity", ("1",), target.to_legacy_dict(), "test"),
        EvidenceFact("known_aliases", ("1",), [], "test"),
        EvidenceFact(
            "plan_metadata",
            ("1",),
            {"generated_at_utc": "2026-01-01T00:00:00+00:00"},
            "test",
        ),
        TickerEventFact(
            "BBG000TARGET",
            "composite_figi",
            "OK",
            [
                {"date": "2024-01-01", "ticker": "OLD"},
                {"date": "2024-01-06", "ticker": "BAD"},
            ],
        ).to_evidence_fact(1),
        OmittedSegmentFact(
            "BAD", "2024-01-06", "2024-01-10", "non-target event ticker", source="test"
        ).to_evidence_fact(1),
        ReferenceBoundaryFact(
            "OLD", "2024-01-01", "OK", True, "composite_figi_match", {"point": "start"}
        ).to_evidence_fact(1),
        ReferenceBoundaryFact(
            "OLD", "2024-01-05", "OK", True, "composite_figi_match", {"point": "end"}
        ).to_evidence_fact(1),
    )

    result = plan_backfill(EvidenceLedger(facts).snapshot())

    assert isinstance(result, EvidenceNeeded)
    assert result.requests == (
        EvidenceRequest("terminal_coverage", ("1", "NEW", "2024-01-08", "2024-01-10")),
    )


def test_xctx_terminal_coverage_gap_exposes_repair_action() -> None:
    legacy = load_fixture("arrw_event_ticker_handoff.json")
    ledger = without_facts(
        ledger_from_legacy_plan(legacy), "candidate_segments", "terminal_coverage"
    )
    result = plan_backfill(ledger.snapshot())

    envelope = result_envelope("backfill-plan", result)

    assert envelope["repairs"] == [
        {
            "name": "provide-terminal-coverage",
            "evidence_kind": "terminal_coverage",
            "request": {
                "kind": "terminal_coverage",
                "key": ["12716", "AILE", "2025-01-02", "2026-05-04"],
            },
            "effect": {
                "kind": "append-evidence-fact",
                "target": "terminal_coverage",
                "description": "Append a typed evidence fact outside xctx; xctx only emits the protocol payload.",
            },
            "reason": (
                "Append an explicit TerminalCoverageFact proving the final transformed ticker "
                "validly covers the requested terminal interval."
            ),
            "command": {
                "name": "xctx repair",
                "description": "Emit repair guidance for unresolved evidence.",
                "args": {},
                "reads": [],
                "writes": [],
            },
        }
    ]


def test_xctx_safe_plan_requires_explicit_execution_approval() -> None:
    legacy = load_fixture("simple_current_sfbc.json")
    result = plan_backfill(ledger_from_legacy_plan(legacy).snapshot())

    envelope = result_envelope("backfill-plan", result)

    assert envelope["ok"] is True
    assert envelope["result_type"] == "BackfillPlan"
    assert action_names(envelope["next_actions"]) == ["review-plan", "approve-plan"]
    assert envelope["next_actions"][1]["kind"] == "approval"
    assert action_names(envelope["next_actions"]) != [
        "review-plan",
        "execute-approved-plan",
    ]
    assert envelope["invalid_next_actions"][0]["name"] == "execute-approved-plan"
    assert envelope["invalid_next_actions"][0]["reason"] == (
        "plan review and explicit execution approval are required first"
    )


def test_xctx_approved_safe_plan_can_offer_execution() -> None:
    legacy = load_fixture("simple_current_sfbc.json")
    result = plan_backfill(ledger_from_legacy_plan(legacy).snapshot())

    envelope = result_envelope("backfill-plan", result, execution_approved=True)

    assert action_names(envelope["next_actions"]) == [
        "review-plan",
        "execute-approved-plan",
    ]
    execute_action = envelope["next_actions"][1]
    assert execute_action["kind"] == "execution"
    assert execute_action["command"]["name"] == "stock-universe backfill"
    assert execute_action["requires_approval"] is True
    assert execute_action["authority_level"] == "execution"
    assert "invalid_next_actions" not in envelope


def test_xctx_blocked_plan_never_offers_execution() -> None:
    legacy = load_fixture("blocked_cnh.json")
    result = plan_backfill(ledger_from_legacy_plan(legacy).snapshot())

    envelope = result_envelope("backfill-plan", result, execution_approved=True)

    assert envelope["ok"] is False
    assert action_names(envelope["next_actions"]) == [
        "inspect-decision",
        "collect-missing-evidence",
    ]
    assert envelope["invalid_next_actions"][0]["name"] == "execute-approved-plan"
    assert (
        envelope["invalid_next_actions"][0]["reason"] == "blocked plans cannot execute"
    )


def test_xctx_result_envelope_schema_names_typed_protocol() -> None:
    schema = result_envelope_schema()

    assert schema["protocol_version"] == "xctx.v2"
    assert schema["properties"]["next_actions"]["items"]["$ref"] == "#/$defs/NextAction"
    assert (
        schema["properties"]["agent_reporting"]["$ref"]
        == "#/$defs/AgentReportingPolicy"
    )
    assert schema["properties"]["views"]["type"] == "object"
    assert "authority_level" in schema["$defs"]["NextAction"]["required"]
    assert (
        "execution"
        in schema["$defs"]["NextAction"]["properties"]["authority_level"]["enum"]
    )
    assert (
        schema["$defs"]["NextAction"]["properties"]["agent_reporting"]["$ref"]
        == "#/$defs/AgentReportingPolicy"
    )
    assert schema["$defs"]["AgentReportingPolicy"]["required"] == [
        "version",
        "applies_when",
        "native_progress",
        "poll_seconds",
        "first_user_update_seconds",
        "user_update_seconds",
        "stall_seconds",
        "quiet_when_healthy",
        "immediate_on",
        "final_report",
        "begin",
        "routine",
        "immediate_update_on",
        "final",
        "operator_override",
    ]
    assert "RepairAction" in schema["$defs"]
    assert "ToolManifest" in schema["$defs"]


def test_xctx_tool_manifest_centers_transitions_over_commands() -> None:
    manifest = xctx_tool_manifest()

    assert manifest["namespace"] == "xctx"
    assert manifest["core_unit"] == "Transition"
    assert manifest["core_loop"] == [
        "doctor",
        "universe-status",
        "quality-audit",
        "catch-up-plan",
        "catch-up-runs",
        "tree",
        "capabilities",
        "describe",
        "schema",
        "examples",
        "resolve-identity",
        "bars",
        "validate",
        "dry-run",
        "run",
        "catch-up-run",
        "catch-up-stop",
        "catch-up-reconcile",
        "catch-up-status",
        "observe",
        "repair",
        "next",
        "compose",
    ]
    assert manifest["entrypoints"]["source_checkout"] == "./stock_universe.cli xctx"
    assert manifest["recommended_agent_loop"][0] == "./stock_universe.cli xctx doctor"
    assert (
        "./stock_universe.cli xctx universe-status"
        in manifest["recommended_agent_loop"]
    )
    assert "./stock_universe.cli xctx examples" in manifest["recommended_agent_loop"]
    assert (
        "./stock_universe.cli backfill --fixture <fixture> --strict"
        in manifest["recommended_agent_loop"]
    )
    assert {
        "CapabilityList",
        "QualityAudit",
        "CatchUpPlan",
        "CatchUpRunList",
        "CatchUpRunStatus",
        "CatchUpReconciliation",
        "ExecutionAudit",
        "DbValidation",
        "AgentReportingPolicy",
        "BarObservationList",
    } <= set(manifest["core_objects"])
    assert {transition["name"] for transition in manifest["transitions"]} >= {
        "doctor",
        "examples",
        "validate",
        "dry-run",
        "run",
        "catch-up-plan",
        "catch-up-runs",
        "catch-up-run",
        "catch-up-reconcile",
        "catch-up-status",
        "bars",
        "observe",
    }


def test_xctx_command_schemas_expose_inputs_returns_and_effects() -> None:
    schemas = xctx_command_schemas()

    dry_run = schemas["xctx dry-run"]
    doctor = schemas["xctx doctor"]
    examples = schemas["xctx examples"]
    backfill = schemas["stock-universe backfill"]
    reference_update = schemas["stock-universe update-reference-universe"]
    reference_batch = schemas["stock-universe backfill-reference-batch"]
    catch_up_plan = schemas["xctx catch-up-plan"]
    catch_up = schemas["stock-universe catch-up"]
    catch_up_reconcile = schemas["stock-universe catch-up-reconcile"]

    assert dry_run["args"]["fixture"]["required"] is False
    assert dry_run["args"]["ticker"]["required"] is False
    assert (
        dry_run["input_rule"]
        == "Provide exactly one of fixture, ticker, or ohlcv_series_id. Ticker and ohlcv_series_id inputs use live read-oriented providers; ohlcv_series_id also requires db."
    )
    assert dry_run["args"]["ohlcv_series_id"]["type"] == "integer"
    assert dry_run["writes"] == []
    assert dry_run["returns"] == "DryRunPlan and ResultEnvelope"
    assert doctor["returns"] == "DoctorReport"
    assert doctor["writes"] == []
    assert examples["returns"] == "ExampleList"
    assert backfill["mutates"] is True
    assert backfill["writes"] == ["SQLite DB"]
    assert reference_update["args"]["commit"]["default"] is False
    assert reference_update["returns"] == "ReferenceUniverseUpdate"
    assert reference_update["agent_reporting"]["routine"]["system_poll_seconds"] == 60
    assert reference_update["agent_reporting"]["routine"]["first_update_seconds"] == 180
    assert reference_update["agent_reporting"]["version"] == "agent_reporting.v2"
    assert (
        reference_update["agent_reporting"]["native_progress"]["prefix"]
        == "update-reference-universe progress: "
    )
    assert reference_update["agent_reporting"]["poll_seconds"] == 60
    assert reference_batch["args"]["commit"]["default"] is False
    assert reference_batch["returns"] == "ReferenceBatchManifest"
    assert "selected persisted OHLCV series IDs" in reference_batch["input_rule"]
    assert (
        reference_batch["agent_reporting"]["routine"]["default_update_seconds"] == 300
    )
    assert "len(ohlcv_series_id)" in reference_batch["agent_reporting"]["applies_when"]
    assert "len(series_id)" not in reference_batch["agent_reporting"]["applies_when"]
    assert (
        reference_batch["agent_reporting"]["native_progress"]["prefix"]
        == "backfill-reference-batch progress: "
    )
    assert (
        backfill["agent_reporting"]["native_progress"]["prefix"]
        == "backfill progress: "
    )
    assert catch_up_plan["mutates"] is False
    assert catch_up_plan["returns"] == "CatchUpPlan"
    assert "agent_reporting" in catch_up_plan
    assert catch_up_plan["agent_reporting"]["native_progress"]["mode"] == "none"
    assert catch_up_plan["args"]["view"]["enum"] == ["simple", "detail", "extra_detail"]
    assert catch_up_plan["args"]["detail_limit"]["default"] == 25
    assert "bounded target_detail" in catch_up_plan["views"]["detail"]
    assert schemas["xctx catch-up-runs"]["returns"] == "CatchUpRunList"
    assert catch_up["args"]["workers"]["default"] == 10
    assert catch_up["args"]["commit"]["default"] is False
    assert (
        catch_up["agent_reporting"]["native_progress"]["prefix"]
        == "catch-up progress: "
    )
    assert "regular status paths" in catch_up["agent_reporting"]["monitoring_guidance"]
    assert (
        catch_up["agent_reporting"]["operator_override"]
        == "Latest user instruction wins."
    )
    assert catch_up_reconcile["args"]["commit"]["default"] is False
    assert "recovered_batch" in catch_up_reconcile["input_rule"]
    assert "agent_reporting" not in doctor
    assert "agent_reporting" not in schemas["xctx tree"]


def test_agent_reporting_native_progress_contract_is_machine_readable() -> None:
    schemas = xctx_command_schemas()

    for name, schema in schemas.items():
        reporting = schema.get("agent_reporting")
        if not reporting:
            continue
        assert reporting["version"] == "agent_reporting.v2", name
        assert (
            reporting["poll_seconds"] == reporting["routine"]["system_poll_seconds"]
        ), name
        assert (
            reporting["first_user_update_seconds"]
            == reporting["routine"]["first_update_seconds"]
        ), name
        assert (
            reporting["user_update_seconds"]
            == reporting["routine"]["default_update_seconds"]
        ), name
        assert reporting["immediate_on"] == reporting["immediate_update_on"], name
        native = reporting["native_progress"]
        assert native["mode"] in {"none", "stderr_jsonl"}, name
        if native["mode"] == "none":
            assert "prefix" not in native, name
            continue
        prefix = native.get("prefix")
        if prefix:
            assert prefix.endswith(" progress: "), name
        assert {"started", "starting"} & set(native["events"]), name
        assert "finished" in native["events"], name
        heartbeat_arg = native["heartbeat_arg"].lstrip("-").replace("-", "_")
        summary_arg = native["summary_arg"].lstrip("-").replace("-", "_")
        assert heartbeat_arg in schema["args"], name
        assert summary_arg in schema["args"], name
        if prefix:
            assert any(
                prefix.strip() in source for source in reporting["status_sources"]
            ), name
        else:
            assert any(
                "stderr" in source and "JSON" in source
                for source in reporting["status_sources"]
            ), name


def test_xctx_cli_capabilities(capsys: pytest.CaptureFixture[str]) -> None:
    assert xctx_main(["capabilities"]) == 0

    payload = json.loads(capsys.readouterr().out)

    assert payload["ok"] is True
    assert payload["protocol_version"] == "xctx.v2"
    assert "xctx dry-run" in {command["name"] for command in payload["commands"]}
    assert "xctx doctor" in {command["name"] for command in payload["commands"]}
    assert "xctx examples" in {command["name"] for command in payload["commands"]}
    assert "stock-universe backfill" in {
        command["name"] for command in payload["commands"]
    }
    assert {transition["name"] for transition in payload["transitions"]} >= {
        "schema",
        "observe",
        "compose",
        "update-reference-universe",
        "backfill-reference-batch",
        "catch-up-plan",
        "catch-up-runs",
        "catch-up-run",
        "catch-up-reconcile",
        "catch-up-status",
    }


def test_xctx_cli_tree_returns_manifest_and_recipes(
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert xctx_main(["tree"]) == 0

    payload = json.loads(capsys.readouterr().out)

    assert payload["ok"] is True
    assert payload["object_type"] == "ToolManifest"
    assert payload["result_type"] == "ToolManifest"
    assert payload["view"] == "simple"
    assert payload["namespace"] == "xctx"
    assert "command_schemas" not in payload
    assert "binding_maps" not in payload
    assert "xctx dry-run" in {command["name"] for command in payload["commands"]}
    assert "views" in next(
        command
        for command in payload["commands"]
        if command["name"] == "xctx catch-up-plan"
    )
    assert {recipe["name"] for recipe in payload["recipes"]} >= {
        "fixture-live-backfill",
        "ticker-live-backfill",
        "reference-universe-maintenance",
        "reference-batch-backfill",
        "stock-universe-health-check",
        "database-catch-up",
    }

    assert xctx_main(["tree", "--view", "extra_detail"]) == 0
    full_payload = json.loads(capsys.readouterr().out)
    assert full_payload["view"] == "extra_detail"
    assert "xctx dry-run" in full_payload["command_schemas"]
    assert "xctx examples" in full_payload["command_schemas"]


def test_xctx_cli_describe_backfill_plan(capsys: pytest.CaptureFixture[str]) -> None:
    assert xctx_main(["describe", "backfill-plan"]) == 0

    payload = json.loads(capsys.readouterr().out)

    assert payload["ok"] is True
    assert payload["mutates"] is False
    assert (
        payload["schema"]["properties"]["next_actions"]["items"]["$ref"]
        == "#/$defs/NextAction"
    )
    assert "command_schemas" not in payload
    assert "binding_maps" not in payload


def test_xctx_cli_schema_filters_command_binding(
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert xctx_main(["schema", "--command", "xctx dry-run"]) == 0

    payload = json.loads(capsys.readouterr().out)

    assert payload["ok"] is True
    assert (
        payload["command_schema"]["xctx dry-run"]["args"]["fixture"]["type"] == "path"
    )
    assert (
        payload["command_schema"]["xctx dry-run"]["args"]["ticker"]["type"] == "ticker"
    )
    assert payload["binding_map"]["xctx dry-run"]["argv"][:3] == [
        "./stock_universe.cli",
        "xctx",
        "dry-run",
    ]
    assert payload["binding_map"]["xctx dry-run"]["logical_argv"][1] == "dry-run"
    assert payload["binding_map"]["xctx dry-run"]["source_checkout_argv"][:2] == [
        "./stock_universe.cli",
        "xctx",
    ]
    assert payload["binding_map"]["xctx dry-run"]["source_checkout_ticker_argv"][
        :2
    ] == ["./stock_universe.cli", "xctx"]
    assert payload["binding_map"]["xctx dry-run"]["ticker_argv"] == [
        "./stock_universe.cli",
        "xctx",
        "dry-run",
        "--ticker",
        "{ticker}",
        "--max-rounds",
        "{max_rounds}",
    ]


def test_xctx_cli_schema_exposes_quality_repair_bindings(
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert (
        xctx_main(["schema", "--command", "stock-universe repair-missing-receipts"])
        == 0
    )
    repair_payload = json.loads(capsys.readouterr().out)

    assert (
        repair_payload["command_schema"]["stock-universe repair-missing-receipts"][
            "mutates"
        ]
        is True
    )
    assert repair_payload["binding_map"]["stock-universe repair-missing-receipts"][
        "commit_argv"
    ] == [
        "./stock_universe.cli",
        "repair-missing-receipts",
        "--limit",
        "{limit}",
        "--commit",
    ]

    assert xctx_main(["schema", "--command", "stock-universe quality-audit"]) == 0
    audit_payload = json.loads(capsys.readouterr().out)

    assert (
        audit_payload["command_schema"]["stock-universe quality-audit"]["mutates"]
        is False
    )
    assert audit_payload["binding_map"]["stock-universe quality-audit"][
        "category_filter_argv"
    ][:3] == [
        "./stock_universe.cli",
        "quality-audit",
        "--category",
    ]

    assert xctx_main(["schema", "--command", "xctx quality-audit"]) == 0
    xctx_audit_payload = json.loads(capsys.readouterr().out)

    assert xctx_audit_payload["command_schema"]["xctx quality-audit"]["args"]["view"][
        "enum"
    ] == ["simple", "detail", "extra_detail"]
    assert xctx_audit_payload["binding_map"]["xctx quality-audit"]["detail_argv"] == [
        "./stock_universe.cli",
        "xctx",
        "quality-audit",
        "--view",
        "detail",
    ]


def test_xctx_cli_doctor_reports_readiness_without_mutation(
    tmp_path: Path, monkeypatch, capsys: pytest.CaptureFixture[str]
) -> None:
    db = tmp_path / "doctor.sqlite"
    monkeypatch.setattr(
        "stock_universe.xctx.cli.shutil.which",
        lambda name: (_ for _ in ()).throw(
            AssertionError("default doctor must not inspect installed entrypoints")
        ),
    )

    assert xctx_main(["doctor", "--db", str(db), "--api-key", "secret"]) == 0
    payload = json.loads(capsys.readouterr().out)

    assert payload["ok"] is True
    assert payload["result_type"] == "DoctorReport"
    assert payload["checks"]["massive_api_key_present"] is True
    assert payload["checks"]["db_exists"] is False
    assert "stock_universe_entrypoint_present" not in payload["checks"]
    assert "xctx_entrypoint_present" not in payload["checks"]
    assert payload["effects"]["will_write"] == []
    assert "discover-xctx-tree" in action_names(payload["next_actions"])


def test_xctx_cli_doctor_can_require_installed_entrypoints(
    tmp_path: Path, monkeypatch, capsys: pytest.CaptureFixture[str]
) -> None:
    db = tmp_path / "doctor.sqlite"
    monkeypatch.setattr("stock_universe.xctx.cli.shutil.which", lambda name: None)

    assert (
        xctx_main(
            ["doctor", "--db", str(db), "--api-key", "secret", "--require-entrypoint"]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)

    assert payload["ok"] is False
    assert payload["checks"]["stock_universe_entrypoint_present"] is False
    assert payload["checks"]["xctx_entrypoint_present"] is False


def test_xctx_cli_missing_fixture_returns_repair_json(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    missing = tmp_path / "missing.json"

    assert xctx_main(["dry-run", "--fixture", str(missing)]) == 0
    payload = json.loads(capsys.readouterr().out)

    assert payload["ok"] is False
    assert payload["result_type"] == "RepairError"
    assert payload["errors"][0]["code"] == "fixture_not_found"
    assert payload["effects"]["will_read"] == [str(missing)]
    assert "inspect-runnable-examples" in action_names(payload["next_actions"])


def test_xctx_cli_missing_api_key_returns_repair_json(
    monkeypatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.delenv("MASSIVE_API_KEY", raising=False)

    assert xctx_main(["resolve-identity", "--query", "Alphabet"]) == 0
    payload = json.loads(capsys.readouterr().out)

    assert payload["ok"] is False
    assert payload["result_type"] == "RepairError"
    assert payload["errors"][0]["code"] == "massive_api_key_required"
    assert payload["repairs"][0]["name"] == "provide-massive-api-key"


def test_xctx_cli_invalid_identity_input_returns_repair_json(
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert (
        xctx_main(
            [
                "resolve-identity",
                "--limit",
                "0",
                "--query",
                "Alphabet",
                "--api-key",
                "secret",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)

    assert payload["ok"] is False
    assert payload["result_type"] == "RepairError"
    assert payload["errors"][0]["code"] == "limit_not_positive"


def test_xctx_cli_examples_return_runnable_source_checkout_argv(
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert xctx_main(["examples", "--command", "xctx dry-run"]) == 0
    payload = json.loads(capsys.readouterr().out)

    assert payload["ok"] is True
    assert payload["result_type"] == "ExampleList"
    assert payload["examples"][0]["command"] == "xctx dry-run"
    assert payload["examples"][0]["argv"][:2] == ["./stock_universe.cli", "xctx"]
    assert payload["examples"][0]["source_checkout_argv"][:2] == [
        "./stock_universe.cli",
        "xctx",
    ]
    assert payload["examples"][0]["side_effects"]["mutates"] is False
    assert payload["effects"]["will_write"] == []


def test_xctx_cli_validate_fixture_emits_typed_next_actions(
    capsys: pytest.CaptureFixture[str],
) -> None:
    fixture = str(FIXTURE_DIR / "simple_current_sfbc.json")

    assert xctx_main(["validate", "--fixture", fixture]) == 0

    payload = json.loads(capsys.readouterr().out)

    assert payload["result_type"] == "BackfillPlan"
    assert action_names(payload["next_actions"]) == ["review-plan", "approve-plan"]
    assert payload["effects"]["will_write"] == []


def test_xctx_cli_dry_run_reproduces_fixture_behavior(
    capsys: pytest.CaptureFixture[str],
) -> None:
    fixture = str(FIXTURE_DIR / "ticker_rename_meta.json")

    assert (
        xctx_main(["dry-run", "--fixture", fixture, "--defer-kind", "ticker_events"])
        == 0
    )

    payload = json.loads(capsys.readouterr().out)

    assert payload["result_type"] == "BackfillPlan"
    assert len(payload["rounds"]) == 2
    assert payload["effects"]["will_write"] == []


def test_xctx_cli_dry_run_accepts_ticker_seed(
    monkeypatch, capsys: pytest.CaptureFixture[str]
) -> None:
    legacy = load_fixture("simple_current_sfbc.json")
    calls = {}

    def fake_source_from_ticker(
        ticker: str,
        *,
        api_key: str,
        base_url: str,
        db_path: str,
        require_existing_identity: bool,
        from_date: str,
        to_date: str | None,
        bar_grain: str,
    ):
        calls.update(
            {
                "ticker": ticker,
                "api_key": api_key,
                "base_url": base_url,
                "db_path": db_path,
                "require_existing_identity": require_existing_identity,
                "from_date": from_date,
                "to_date": to_date,
                "bar_grain": bar_grain,
            }
        )
        source = StaticBackfillEvidenceSource.from_legacy_plan(
            legacy, include_candidate_segments=False
        )
        client = SimpleNamespace(
            request_log=(
                SimpleNamespace(
                    endpoint="/v3/reference/tickers/SFBC",
                    params_without_api_key=(),
                    http_code=200,
                    api_status="OK",
                    elapsed_seconds=0.0,
                ),
            )
        )
        return source, client

    monkeypatch.setattr(
        "stock_universe.xctx.cli.massive_live_source_from_ticker",
        fake_source_from_ticker,
    )

    assert (
        xctx_main(
            [
                "dry-run",
                "--ticker",
                "SFBC",
                "--api-key",
                "secret",
                "--base-url",
                "https://example.test",
                "--from-date",
                "2024-01-01",
                "--to-date",
                "2024-01-31",
            ]
        )
        == 0
    )
    payload = json.loads(capsys.readouterr().out)

    assert payload["ok"] is True
    assert payload["result_type"] == "BackfillPlan"
    assert payload["plan"]["target"]["latest_ticker"] == "SFBC"
    assert payload["effects"]["will_write"] == []
    assert payload["effects"]["will_read"][0] == "massive.reference_ticker:SFBC"
    assert payload["request_log"][0]["endpoint"] == "/v3/reference/tickers/SFBC"
    assert calls == {
        "ticker": "SFBC",
        "api_key": "secret",
        "base_url": "https://example.test",
        "db_path": "production_build/stock_universe.sqlite",
        "require_existing_identity": True,
        "from_date": "2024-01-01",
        "to_date": "2024-01-31",
        "bar_grain": "1d",
    }


def test_xctx_cli_next_hides_execution_until_approval(
    capsys: pytest.CaptureFixture[str],
) -> None:
    fixture = str(FIXTURE_DIR / "simple_current_sfbc.json")

    assert xctx_main(["next", "--fixture", fixture]) == 0
    unapproved = json.loads(capsys.readouterr().out)
    assert "execute-approved-plan" not in action_names(unapproved["next_actions"])

    assert xctx_main(["next", "--fixture", fixture, "--approve-execution"]) == 0
    approved = json.loads(capsys.readouterr().out)
    assert "execute-approved-plan" in action_names(approved["next_actions"])
    execute_action = next(
        action
        for action in approved["next_actions"]
        if action["name"] == "execute-approved-plan"
    )
    assert execute_action["command"]["name"] == "stock-universe backfill"
    assert execute_action["source_checkout_argv"] == [
        "./stock_universe.cli",
        "backfill",
        "--fixture",
        fixture,
        "--strict",
    ]
    assert execute_action["authority_level"] == "execution"


def test_xctx_cli_repair_outputs_protocol_without_mutation(
    capsys: pytest.CaptureFixture[str],
) -> None:
    fixture = str(FIXTURE_DIR / "barrick_gold_b.json")

    assert (
        xctx_main(
            [
                "repair",
                "--fixture",
                fixture,
                "--omit-kind",
                "candidate_segments",
                "--omit-kind",
                "alias_history",
            ]
        )
        == 0
    )

    payload = json.loads(capsys.readouterr().out)

    assert payload["ok"] is True
    assert payload["repairs"][0]["name"] == "provide-alias-history"
    assert payload["repairs"][0]["effect"]["kind"] == "append-evidence-fact"
    assert payload["effects"]["did_write"] == []


def test_xctx_cli_observe_reads_existing_db_without_mutation(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db = tmp_path / "observe.sqlite"
    SQLiteStockUniverseRepository(db).ensure_schema()

    assert xctx_main(["observe", "--db", str(db)]) == 0

    payload = json.loads(capsys.readouterr().out)

    assert payload["ok"] is True
    assert payload["result_type"] == "ExecutionAudit"
    assert payload["count"] == 0
    assert "effects" not in payload


def test_xctx_cli_observe_missing_db_returns_repair_error(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    db = tmp_path / "missing.sqlite"

    assert xctx_main(["observe", "--db", str(db)]) == 0

    payload = json.loads(capsys.readouterr().out)

    assert payload["ok"] is False
    assert payload["result_type"] == "RepairError"
    assert payload["repairs"][0]["name"] == "provide-existing-sqlite-db"
    assert db.exists() is False


def test_xctx_cli_compose_returns_workflow_recipes(
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert xctx_main(["compose", "--recipe", "ticker-live-backfill"]) == 0

    payload = json.loads(capsys.readouterr().out)

    assert payload["ok"] is True
    assert payload["recipes"][0]["name"] == "ticker-live-backfill"
    assert [step["transition"] for step in payload["recipes"][0]["steps"]] == [
        "doctor",
        "dry-run",
        "run",
        "observe",
    ]
    assert (
        payload["recipes"][0]["steps"][1]["command"]
        == "./stock_universe.cli xctx dry-run --ticker {ticker} --max-rounds 20"
    )
    assert (
        payload["recipes"][0]["steps"][1]["logical_command"]
        == "xctx dry-run --ticker {ticker} --max-rounds 20"
    )


def test_xctx_cli_compose_returns_reference_universe_recipe(
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert xctx_main(["compose", "--recipe", "reference-universe-maintenance"]) == 0

    payload = json.loads(capsys.readouterr().out)

    assert payload["ok"] is True
    assert payload["recipes"][0]["name"] == "reference-universe-maintenance"
    assert [step["transition"] for step in payload["recipes"][0]["steps"]] == [
        "doctor",
        "update-reference-universe",
        "update-reference-universe",
        "validate-db",
        "universe-status",
        "resolve-identity",
    ]
    assert payload["recipes"][0]["steps"][2]["command"].endswith("--commit")


def test_xctx_cli_compose_returns_reference_batch_recipe(
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert xctx_main(["compose", "--recipe", "reference-batch-backfill"]) == 0

    payload = json.loads(capsys.readouterr().out)

    assert payload["ok"] is True
    assert payload["recipes"][0]["name"] == "reference-batch-backfill"
    assert [step["transition"] for step in payload["recipes"][0]["steps"]] == [
        "universe-status",
        "backfill-reference-batch",
        "backfill-reference-batch",
        "observe",
    ]
    assert payload["recipes"][0]["steps"][2]["command"].endswith("--commit --strict")


def test_executor_contract_accepts_safe_approved_plan() -> None:
    legacy = load_fixture("simple_current_sfbc.json")
    result = plan_backfill(ledger_from_legacy_plan(legacy).snapshot())

    report = validate_approved_plan(
        result, ExecutionApproval(request_hash=result.request.request_hash)
    )

    assert report.ok is True
    assert "request hash matched" in report.checks


def test_executor_contract_rejects_unapproved_caution_plan() -> None:
    legacy = load_fixture("caution_cnh.json")
    result = plan_backfill(ledger_from_legacy_plan(legacy).snapshot())

    try:
        validate_approved_plan(
            result, ExecutionApproval(request_hash=result.request.request_hash)
        )
    except ExecutionContractError as exc:
        assert "caution plans require explicit caution approval" in exc.checks
    else:
        raise AssertionError("expected caution plan to require explicit approval")


def test_executor_contract_accepts_approved_caution_plan() -> None:
    legacy = load_fixture("caution_cnh.json")
    result = plan_backfill(ledger_from_legacy_plan(legacy).snapshot())

    report = validate_approved_plan(
        result,
        ExecutionApproval(
            request_hash=result.request.request_hash,
            allow_caution=True,
            approved_by="test",
        ),
    )

    assert report.ok is True


def test_executor_contract_rejects_blocked_plan() -> None:
    legacy = load_fixture("blocked_cnh.json")
    result = plan_backfill(ledger_from_legacy_plan(legacy).snapshot())

    try:
        validate_approved_plan(
            result,
            ExecutionApproval(
                request_hash=result.request.request_hash,
                allow_caution=True,
                approved_by="test",
            ),
        )
    except ExecutionContractError as exc:
        assert "blocked plans cannot execute" in exc.checks
    else:
        raise AssertionError("expected blocked plan rejection")


def test_executor_contract_rejects_request_hash_mismatch() -> None:
    legacy = load_fixture("simple_current_sfbc.json")
    result = plan_backfill(ledger_from_legacy_plan(legacy).snapshot())

    try:
        validate_approved_plan(result, ExecutionApproval(request_hash="wrong"))
    except ExecutionContractError as exc:
        assert "approval request hash does not match plan request" in exc.checks
    else:
        raise AssertionError("expected request hash mismatch rejection")


def test_sqlite_repository_persists_plan_bars_and_receipt(tmp_path: Path) -> None:
    legacy = load_fixture("simple_current_sfbc.json")
    result = plan_backfill(ledger_from_legacy_plan(legacy).snapshot())
    transport = QueueHttpJsonTransport(
        [
            HttpJsonResponse(
                200,
                {
                    "status": "OK",
                    "resultsCount": 2,
                    "results": [
                        {
                            "t": 1620086400000,
                            "o": 10,
                            "h": 11,
                            "l": 9,
                            "c": 10.5,
                            "v": 100,
                            "vw": 10.2,
                            "n": 3,
                        },
                        {
                            "t": 1620172800000,
                            "o": 10.5,
                            "h": 12,
                            "l": 10,
                            "c": 11.5,
                            "v": 120,
                            "vw": 11.1,
                            "n": 4,
                        },
                    ],
                },
            )
        ]
    )
    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"), transport
    )
    repository = SQLiteStockUniverseRepository(tmp_path / "stock_universe.sqlite")
    result = plan_with_allocated_lookup(repository, result)
    approval = ExecutionApproval(
        request_hash=result.request.request_hash, approved_by="test"
    )
    approval_record = repository.insert_execution_approval(
        result, approval, reason="unit test approval"
    )

    receipt = execute_live_bar_backfill(
        result,
        approval,
        client,
        repository,
        evidence_facts=tuple(
            facts_from_legacy_plan(legacy, include_candidate_segments=False)
        ),
    )

    assert receipt.ok is True
    assert receipt.fetched_bar_count == 2
    assert receipt.inserted_bar_count == 2
    assert transport.urls[0].startswith(
        "https://example.test/v2/aggs/ticker/SFBC/range/1/day/"
    )
    assert repository.counts()["ohlcv_bars"] == 2
    assert repository.counts()["execution_approvals"] == 1
    assert repository.counts()["execution_receipts"] == 1
    assert approval_record["approval_hash"]
    validation = repository.validate()
    assert validation.ok is True
    assert "foreign keys valid" in validation.checks
    assert "receipts have approval records" in validation.checks
    audit_rows = repository.execution_audit(request_hash=result.request.request_hash)
    assert len(audit_rows) == 1
    assert audit_rows[0]["approval_hash"] == approval_record["approval_hash"]
    assert audit_rows[0]["inserted_bar_count"] == 2


def test_manual_lineage_bar_count_refreshes_when_interval_expands(
    tmp_path: Path,
) -> None:
    repository = SQLiteStockUniverseRepository(tmp_path / "stock_universe.sqlite")
    repository.upsert_reference_snapshots(
        [
            StoredReferenceSnapshot(
                provider="massive.reference_tickers",
                snapshot_as_of_date="2026-05-08",
                ticker="LINE",
                active=True,
                company_name="Lineage Count Inc.",
                cik="1",
                composite_figi="BBGLINE",
                share_class_figi="",
                security_type="CS",
                primary_exchange="XNAS",
                market="stocks",
                locale="us",
                identity_status="permanent",
                natural_key="massive:composite_figi:BBGLINE",
                raw={"ticker": "LINE"},
            )
        ]
    )
    series_id = repository.lookup_ohlcv_series_id("massive:composite_figi:BBGLINE")
    assert series_id is not None
    repository.insert_bars(
        [
            StoredOhlcvBar(
                series_id=series_id,
                ticker="LINE",
                bar_date="2026-05-01",
                bar_start_ts=1777593600000,
                multiplier=1,
                timespan="day",
                adjusted=True,
                open=10,
                high=11,
                low=9,
                close=10.5,
                volume=100,
                bar_quality_status="VALIDATED",
            )
        ]
    )
    repository.insert_bars(
        [
            StoredOhlcvBar(
                series_id=series_id,
                ticker="LINE",
                bar_date="2026-05-04",
                bar_start_ts=1777852800000,
                multiplier=1,
                timespan="day",
                adjusted=True,
                open=11,
                high=12,
                low=10,
                close=11.5,
                volume=120,
                bar_quality_status="SUSPECT",
                repair_rule="unit_test_quality_exception",
                repair_evidence_json={"reason": "unit test"},
            )
        ]
    )

    with repository.connect() as conn:
        rows = conn.execute(
            """
            SELECT from_utc_start_ts, to_utc_start_ts, bar_count, quality_exception_count
            FROM ohlcv_bar_lineage
            """
        ).fetchall()

    assert [tuple(row) for row in rows] == [(1777642200000, 1777901400000, 2, 1)]
    validation = repository.validate()
    assert validation.ok is True
    assert "lineage bar counts match covered hot rows" in validation.checks
    assert (
        "lineage quality exception counts match sparse quality rows"
        in validation.checks
    )

    repository.insert_bars(
        [
            StoredOhlcvBar(
                series_id=series_id,
                ticker="LINE",
                bar_date="2026-05-04",
                bar_start_ts=1777852800000,
                multiplier=1,
                timespan="day",
                adjusted=True,
                open=11,
                high=12,
                low=10,
                close=11.5,
                volume=120,
                bar_quality_status="VALIDATED",
            )
        ]
    )
    with repository.connect() as conn:
        lineage = conn.execute(
            """
            SELECT bar_count, quality_exception_count
            FROM ohlcv_bar_lineage
            """
        ).fetchone()
        quality_event_count = conn.execute(
            "SELECT COUNT(*) FROM ohlcv_day_bar_quality_events"
        ).fetchone()[0]
        view_status = conn.execute(
            """
            SELECT bar_quality_status, repair_rule
            FROM v_ohlcv_bars_unified
            WHERE utc_start_ts = 1777901400000
            """
        ).fetchone()

    assert tuple(lineage) == (2, 0)
    assert quality_event_count == 0
    assert tuple(view_status) == ("VALIDATED", "")
    assert repository.validate().ok is True


def test_live_bar_executor_persists_minute_bars_with_bounded_windows_and_pagination(
    tmp_path: Path,
) -> None:
    legacy = load_fixture("simple_current_sfbc.json")
    result = plan_backfill(ledger_from_legacy_plan(legacy).snapshot())
    request = BackfillRequest(
        series_id=result.request.series_id,
        from_date="2021-05-04",
        to_date="2021-05-05",
        multiplier=1,
        timespan="minute",
    )
    segment = replace(
        result.segments[0], from_date=dt.date(2021, 5, 4), to_date=dt.date(2021, 5, 5)
    )
    result = replace(result, request=request, segments=(segment,))
    transport = QueueHttpJsonTransport(
        [
            HttpJsonResponse(
                200,
                {
                    "status": "OK",
                    "resultsCount": 1,
                    "results": [
                        {
                            "t": 1620135000000,
                            "o": 10,
                            "h": 10.5,
                            "l": 9.9,
                            "c": 10.2,
                            "v": 100,
                            "vw": 10.1,
                            "n": 3,
                        },
                    ],
                    "next_url": "https://example.test/v2/aggs/ticker/SFBC/range/1/minute/2021-05-04/2021-05-04?cursor=abc&apiKey=leaked",
                },
            ),
            HttpJsonResponse(
                200,
                {
                    "status": "OK",
                    "resultsCount": 2,
                    "results": [
                        {
                            "t": 1620135060000,
                            "o": 10.2,
                            "h": 10.8,
                            "l": 10.1,
                            "c": 10.4,
                            "v": 110,
                            "vw": 10.3,
                            "n": 4,
                        },
                        {
                            "t": 1620221400000,
                            "o": 10.5,
                            "h": 11,
                            "l": 10.4,
                            "c": 10.7,
                            "v": 120,
                            "vw": 10.6,
                            "n": 5,
                        },
                    ],
                },
            ),
        ]
    )
    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"), transport
    )
    repository = SQLiteStockUniverseRepository(tmp_path / "stock_universe.sqlite")
    result = plan_with_allocated_lookup(repository, result)
    approval = ExecutionApproval(
        request_hash=result.request.request_hash, approved_by="test"
    )
    repository.insert_execution_approval(result, approval, reason="minute approval")

    receipt = execute_live_bar_backfill(
        result,
        approval,
        client,
        repository,
        evidence_facts=tuple(
            facts_from_legacy_plan(legacy, include_candidate_segments=False)
        ),
    )

    assert receipt.ok is True
    assert receipt.fetched_bar_count == 3
    assert receipt.inserted_bar_count == 3
    assert "/range/1/minute/2021-05-04/2021-05-05" in transport.urls[0]
    assert "cursor=abc" in transport.urls[1]
    assert len(transport.urls) == 2
    with repository.connect() as conn:
        rows = conn.execute(
            """
            SELECT bar_date, multiplier, timespan, bar_quality_status
            FROM v_ohlcv_bars_unified
            ORDER BY bar_start_ts
            """
        ).fetchall()
    assert [tuple(row) for row in rows] == [
        ("2021-05-04", 1, "minute", "VALIDATED"),
        ("2021-05-04", 1, "minute", "VALIDATED"),
        ("2021-05-05", 1, "minute", "VALIDATED"),
    ]


def test_live_bar_executor_repairs_nvda_2024_06_10_daily_high_from_intraday_envelope(
    tmp_path: Path,
) -> None:
    legacy = load_fixture("simple_current_sfbc.json")
    result = plan_backfill(ledger_from_legacy_plan(legacy).snapshot())
    repository = SQLiteStockUniverseRepository(tmp_path / "stock_universe.sqlite")
    result = plan_with_allocated_lookup(repository, result)
    trade_date = dt.date(2024, 6, 10)
    request = BackfillRequest(
        series_id=result.target.ohlcv_series_id,
        from_date=trade_date,
        to_date=trade_date,
        multiplier=1,
        timespan="day",
        adjusted=True,
    )
    segment = PlannedSegment(
        segment_index=result.segments[0].segment_index,
        ticker="NVDA",
        from_date=trade_date,
        to_date=trade_date,
        source="unit-test",
        valid=True,
    )
    result = replace(result, request=request, segments=(segment,))
    daily_ts = int(dt.datetime(2024, 6, 10, tzinfo=dt.UTC).timestamp() * 1000)
    high_ts = int(dt.datetime(2024, 6, 10, 16, 54, tzinfo=dt.UTC).timestamp() * 1000)
    transport = QueueHttpJsonTransport(
        [
            HttpJsonResponse(
                200,
                {
                    "status": "OK",
                    "resultsCount": 1,
                    "results": [
                        {
                            "t": daily_ts,
                            "o": 120.37,
                            "h": 195.95,
                            "l": 117.01,
                            "c": 121.79,
                            "v": 314157461.0,
                            "vw": 121.1155,
                            "n": 2798563,
                        }
                    ],
                },
            ),
            HttpJsonResponse(
                200,
                {
                    "status": "OK",
                    "resultsCount": 2,
                    "results": [
                        {
                            "t": high_ts - 1800000,
                            "o": 121.0,
                            "h": 122.5,
                            "l": 117.01,
                            "c": 122.0,
                            "v": 1000,
                        },
                        {
                            "t": high_ts,
                            "o": 122.0,
                            "h": 123.10,
                            "l": 121.9,
                            "c": 122.7,
                            "v": 1000,
                        },
                    ],
                },
            ),
            HttpJsonResponse(
                200,
                {
                    "status": "OK",
                    "resultsCount": 3,
                    "results": [
                        {
                            "t": high_ts - 60000,
                            "o": 122.1,
                            "h": 122.8,
                            "l": 117.01,
                            "c": 122.5,
                            "v": 100,
                        },
                        {
                            "t": high_ts,
                            "o": 122.8,
                            "h": 123.10,
                            "l": 122.6,
                            "c": 122.9,
                            "v": 100,
                        },
                        {
                            "t": high_ts + 60000,
                            "o": 122.9,
                            "h": 123.0,
                            "l": 122.2,
                            "c": 122.4,
                            "v": 100,
                        },
                    ],
                },
            ),
        ]
    )
    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"), transport
    )
    approval = ExecutionApproval(
        request_hash=result.request.request_hash, approved_by="test"
    )
    repository.insert_execution_approval(result, approval, reason="unit test approval")

    receipt = execute_live_bar_backfill(result, approval, client, repository)

    assert receipt.ok is True
    assert receipt.fetched_bar_count == 1
    assert receipt.inserted_bar_count == 1
    assert len(transport.urls) == 3
    assert "/range/1/day/2024-06-10/2024-06-10" in transport.urls[0]
    assert "/range/30/minute/2024-06-10/2024-06-10" in transport.urls[1]
    assert "/range/1/minute/2024-06-10/2024-06-10" in transport.urls[2]
    with repository.connect() as conn:
        row = conn.execute(
            """
            SELECT open, high, low, close, volume, vwap, transaction_count,
                   bar_quality_status, repair_rule, repair_evidence_json
            FROM v_ohlcv_bars_unified
            WHERE ticker = 'NVDA' AND bar_date = '2024-06-10'
            """
        ).fetchone()
    assert row["open"] == 120.37
    assert row["high"] == 123.10
    assert row["low"] == 117.01
    assert row["close"] == 121.79
    assert row["volume"] == 314157461.0
    assert row["vwap"] == 121.1155
    assert row["transaction_count"] == 2798563
    assert row["bar_quality_status"] == "VALIDATED_REPAIRED"
    assert row["repair_rule"] == "DAILY_HIGH_EXCEEDS_INTRADAY_ENVELOPE"
    repair_evidence = json.loads(row["repair_evidence_json"])
    assert repair_evidence["repair"]["canonical_high"] == 123.10
    assert repair_evidence["proof_ladder"][0]["max_high"] == 123.10
    assert repair_evidence["proof_ladder"][1]["max_high"] == 123.10


def test_live_bar_executor_fetches_1m_envelope_for_event_sensitive_daily_bar(
    tmp_path: Path,
) -> None:
    legacy = load_fixture("simple_current_sfbc.json")
    result = plan_backfill(ledger_from_legacy_plan(legacy).snapshot())
    repository = SQLiteStockUniverseRepository(tmp_path / "stock_universe.sqlite")
    result = plan_with_allocated_lookup(repository, result)
    trade_date = dt.date(2024, 6, 10)
    request = BackfillRequest(
        series_id=result.target.ohlcv_series_id,
        from_date=trade_date,
        to_date=trade_date,
        multiplier=1,
        timespan="day",
        adjusted=True,
    )
    segment = PlannedSegment(
        segment_index=result.segments[0].segment_index,
        ticker="NVDA",
        from_date=trade_date,
        to_date=trade_date,
        source="ticker_change",
        valid=True,
        event_date=trade_date,
    )
    result = replace(result, request=request, segments=(segment,))
    daily_ts = int(dt.datetime(2024, 6, 10, tzinfo=dt.UTC).timestamp() * 1000)
    intraday_ts = int(dt.datetime(2024, 6, 10, 16, 0, tzinfo=dt.UTC).timestamp() * 1000)
    transport = QueueHttpJsonTransport(
        [
            HttpJsonResponse(
                200,
                {
                    "status": "OK",
                    "resultsCount": 1,
                    "results": [
                        {
                            "t": daily_ts,
                            "o": 120.0,
                            "h": 123.0,
                            "l": 117.0,
                            "c": 121.0,
                            "v": 1000,
                            "vw": 121.0,
                        },
                    ],
                },
            ),
            HttpJsonResponse(
                200,
                {
                    "status": "OK",
                    "resultsCount": 1,
                    "results": [
                        {
                            "t": intraday_ts,
                            "o": 120.0,
                            "h": 123.0,
                            "l": 117.0,
                            "c": 121.0,
                            "v": 1000,
                        },
                    ],
                },
            ),
        ]
    )
    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"), transport
    )
    approval = ExecutionApproval(
        request_hash=result.request.request_hash, approved_by="test"
    )
    repository.insert_execution_approval(result, approval, reason="unit test approval")

    receipt = execute_live_bar_backfill(result, approval, client, repository)

    assert receipt.ok is True
    assert len(transport.urls) == 2
    assert "/range/1/day/2024-06-10/2024-06-10" in transport.urls[0]
    assert "/range/1/minute/2024-06-10/2024-06-10" in transport.urls[1]
    with repository.connect() as conn:
        row = conn.execute(
            """
            SELECT high, bar_quality_status, repair_rule, repair_evidence_json
            FROM v_ohlcv_bars_unified
            WHERE ticker = 'NVDA' AND bar_date = '2024-06-10'
            """
        ).fetchone()
    repair_evidence = json.loads(row["repair_evidence_json"])
    assert row["high"] == 123.0
    assert row["bar_quality_status"] == "VALIDATED"
    assert row["repair_rule"] == ""
    assert repair_evidence == {}


def test_live_bar_executor_fetches_1m_envelope_for_event_adjacent_days(
    tmp_path: Path,
) -> None:
    legacy = load_fixture("simple_current_sfbc.json")
    result = plan_backfill(ledger_from_legacy_plan(legacy).snapshot())
    repository = SQLiteStockUniverseRepository(tmp_path / "stock_universe.sqlite")
    result = plan_with_allocated_lookup(repository, result)
    from_date = dt.date(2024, 6, 7)
    event_date = dt.date(2024, 6, 10)
    to_date = dt.date(2024, 6, 11)
    request = BackfillRequest(
        series_id=result.target.ohlcv_series_id,
        from_date=from_date,
        to_date=to_date,
        multiplier=1,
        timespan="day",
        adjusted=True,
    )
    segment = PlannedSegment(
        segment_index=result.segments[0].segment_index,
        ticker="NVDA",
        from_date=from_date,
        to_date=to_date,
        source="split",
        valid=True,
        event_date=event_date,
    )
    result = replace(result, request=request, segments=(segment,))
    day_payloads = []
    minute_payloads = []
    for offset, day in enumerate((from_date, event_date, to_date)):
        daily_ts = int(dt.datetime.combine(day, dt.time(), dt.UTC).timestamp() * 1000)
        minute_ts = int(
            dt.datetime.combine(day, dt.time(16, 0), dt.UTC).timestamp() * 1000
        )
        day_payloads.append(
            {
                "t": daily_ts,
                "o": 120.0 + offset,
                "h": 123.0 + offset,
                "l": 117.0 + offset,
                "c": 121.0 + offset,
                "v": 1000 + offset,
                "vw": 121.0 + offset,
            }
        )
        minute_payloads.append(
            HttpJsonResponse(
                200,
                {
                    "status": "OK",
                    "resultsCount": 1,
                    "results": [
                        {
                            "t": minute_ts,
                            "o": 120.0 + offset,
                            "h": 123.0 + offset,
                            "l": 117.0 + offset,
                            "c": 121.0 + offset,
                            "v": 1000 + offset,
                        },
                    ],
                },
            )
        )
    transport = QueueHttpJsonTransport(
        [
            HttpJsonResponse(
                200, {"status": "OK", "resultsCount": 3, "results": day_payloads}
            ),
            *minute_payloads,
        ]
    )
    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"), transport
    )
    approval = ExecutionApproval(
        request_hash=result.request.request_hash, approved_by="test"
    )
    repository.insert_execution_approval(result, approval, reason="unit test approval")

    receipt = execute_live_bar_backfill(result, approval, client, repository)

    assert receipt.ok is True
    assert receipt.fetched_bar_count == 3
    assert len(transport.urls) == 4
    assert "/range/1/day/2024-06-07/2024-06-11" in transport.urls[0]
    assert "/range/1/minute/2024-06-07/2024-06-07" in transport.urls[1]
    assert "/range/1/minute/2024-06-10/2024-06-10" in transport.urls[2]
    assert "/range/1/minute/2024-06-11/2024-06-11" in transport.urls[3]
    with repository.connect() as conn:
        rows = conn.execute(
            """
            SELECT bar_date, bar_quality_status, repair_evidence_json
            FROM v_ohlcv_bars_unified
            WHERE ticker = 'NVDA'
            ORDER BY bar_date
            """
        ).fetchall()
    assert [row["bar_date"] for row in rows] == [
        "2024-06-07",
        "2024-06-10",
        "2024-06-11",
    ]
    assert [row["bar_quality_status"] for row in rows] == [
        "VALIDATED",
        "VALIDATED",
        "VALIDATED",
    ]
    assert [json.loads(row["repair_evidence_json"]) for row in rows] == [{}, {}, {}]


def test_live_bar_executor_event_window_uses_market_calendar_holiday_gap(
    tmp_path: Path, monkeypatch
) -> None:
    calendar = tmp_path / "sessions.json"
    calendar.write_text(
        json.dumps(
            [
                {"date": "2026-03-06", "open": "09:30", "close": "16:00"},
                {"date": "2026-03-10", "open": "09:30", "close": "16:00"},
                {"date": "2026-03-11", "open": "09:30", "close": "16:00"},
            ]
        )
    )
    monkeypatch.setenv(MARKET_CALENDAR_ENV, str(calendar))
    legacy = load_fixture("simple_current_sfbc.json")
    result = plan_backfill(ledger_from_legacy_plan(legacy).snapshot())
    repository = SQLiteStockUniverseRepository(tmp_path / "stock_universe.sqlite")
    result = plan_with_allocated_lookup(repository, result)
    from_date = dt.date(2026, 3, 6)
    event_date = dt.date(2026, 3, 9)
    to_date = dt.date(2026, 3, 11)
    request = BackfillRequest(
        series_id=result.target.ohlcv_series_id,
        from_date=from_date,
        to_date=to_date,
        multiplier=1,
        timespan="day",
        adjusted=True,
    )
    segment = PlannedSegment(
        segment_index=result.segments[0].segment_index,
        ticker="NVDA",
        from_date=from_date,
        to_date=to_date,
        source="split",
        valid=True,
        event_date=event_date,
    )
    result = replace(result, request=request, segments=(segment,))
    day_payloads = []
    minute_payloads = []
    for offset, day in enumerate((from_date, dt.date(2026, 3, 10), to_date)):
        daily_ts = int(dt.datetime.combine(day, dt.time(), dt.UTC).timestamp() * 1000)
        minute_ts = int(
            dt.datetime.combine(day, dt.time(16, 0), dt.UTC).timestamp() * 1000
        )
        day_payloads.append(
            {
                "t": daily_ts,
                "o": 20.0 + offset,
                "h": 23.0 + offset,
                "l": 17.0 + offset,
                "c": 21.0 + offset,
                "v": 1000 + offset,
                "vw": 21.0 + offset,
            }
        )
        minute_payloads.append(
            HttpJsonResponse(
                200,
                {
                    "status": "OK",
                    "resultsCount": 1,
                    "results": [
                        {
                            "t": minute_ts,
                            "o": 20.0 + offset,
                            "h": 23.0 + offset,
                            "l": 17.0 + offset,
                            "c": 21.0 + offset,
                            "v": 1000 + offset,
                        },
                    ],
                },
            )
        )
    transport = QueueHttpJsonTransport(
        [
            HttpJsonResponse(
                200, {"status": "OK", "resultsCount": 3, "results": day_payloads}
            ),
            *minute_payloads,
        ]
    )
    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"), transport
    )
    approval = ExecutionApproval(
        request_hash=result.request.request_hash, approved_by="test"
    )
    repository.insert_execution_approval(result, approval, reason="unit test approval")

    receipt = execute_live_bar_backfill(result, approval, client, repository)

    assert receipt.ok is True
    assert len(transport.urls) == 4
    assert "/range/1/minute/2026-03-06/2026-03-06" in transport.urls[1]
    assert "/range/1/minute/2026-03-10/2026-03-10" in transport.urls[2]
    assert "/range/1/minute/2026-03-11/2026-03-11" in transport.urls[3]
    assert not any("/2026-03-09/2026-03-09" in url for url in transport.urls)
    with repository.connect() as conn:
        rows = conn.execute(
            """
            SELECT bar_date, bar_quality_status, repair_evidence_json
            FROM v_ohlcv_bars_unified
            WHERE ticker = 'NVDA'
            ORDER BY bar_date
            """
        ).fetchall()
    assert [row["bar_date"] for row in rows] == [
        "2026-03-06",
        "2026-03-10",
        "2026-03-11",
    ]
    assert [row["bar_quality_status"] for row in rows] == [
        "VALIDATED",
        "VALIDATED",
        "VALIDATED",
    ]
    assert [json.loads(row["repair_evidence_json"]) for row in rows] == [{}, {}, {}]


def test_live_bar_executor_persists_suspect_open_close_envelope_conflict(
    tmp_path: Path,
) -> None:
    legacy = load_fixture("simple_current_sfbc.json")
    result = plan_backfill(ledger_from_legacy_plan(legacy).snapshot())
    repository = SQLiteStockUniverseRepository(tmp_path / "stock_universe.sqlite")
    result = plan_with_allocated_lookup(repository, result)
    trade_date = dt.date(2024, 6, 10)
    request = BackfillRequest(
        series_id=result.target.ohlcv_series_id,
        from_date=trade_date,
        to_date=trade_date,
        multiplier=1,
        timespan="day",
        adjusted=True,
    )
    segment = PlannedSegment(
        segment_index=result.segments[0].segment_index,
        ticker="NVDA",
        from_date=trade_date,
        to_date=trade_date,
        source="unit-test",
        valid=True,
    )
    result = replace(result, request=request, segments=(segment,))
    daily_ts = int(dt.datetime(2024, 6, 10, tzinfo=dt.UTC).timestamp() * 1000)
    intraday_ts = int(dt.datetime(2024, 6, 10, 16, 0, tzinfo=dt.UTC).timestamp() * 1000)
    transport = QueueHttpJsonTransport(
        [
            HttpJsonResponse(
                200,
                {
                    "status": "OK",
                    "resultsCount": 1,
                    "results": [
                        {
                            "t": daily_ts,
                            "o": 150.0,
                            "h": 151.0,
                            "l": 99.0,
                            "c": 100.0,
                            "v": 1000,
                            "vw": 110.0,
                        },
                    ],
                },
            ),
            HttpJsonResponse(
                200,
                {
                    "status": "OK",
                    "resultsCount": 1,
                    "results": [
                        {
                            "t": intraday_ts,
                            "o": 100.0,
                            "h": 110.0,
                            "l": 99.0,
                            "c": 105.0,
                            "v": 1000,
                        },
                    ],
                },
            ),
            HttpJsonResponse(
                200,
                {
                    "status": "OK",
                    "resultsCount": 1,
                    "results": [
                        {
                            "t": intraday_ts,
                            "o": 100.0,
                            "h": 110.0,
                            "l": 99.0,
                            "c": 105.0,
                            "v": 1000,
                        },
                    ],
                },
            ),
        ]
    )
    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"), transport
    )
    approval = ExecutionApproval(
        request_hash=result.request.request_hash, approved_by="test"
    )
    repository.insert_execution_approval(result, approval, reason="unit test approval")

    receipt = execute_live_bar_backfill(result, approval, client, repository)

    assert receipt.ok is True
    assert receipt.fetched_bar_count == 1
    assert receipt.inserted_bar_count == 1
    assert len(transport.urls) == 3
    assert repository.counts()["ohlcv_bars"] == 1
    assert repository.counts()["execution_receipts"] == 1
    audit_rows = repository.execution_audit(request_hash=result.request.request_hash)
    assert audit_rows[0]["receipt_status"] == "ok"
    with repository.connect() as conn:
        row = conn.execute(
            """
            SELECT bar_quality_status, repair_rule, repair_evidence_json
            FROM v_ohlcv_bars_unified
            WHERE ticker = 'NVDA' AND bar_date = '2024-06-10'
            """
        ).fetchone()
    repair_evidence = json.loads(row["repair_evidence_json"])
    assert row["bar_quality_status"] == "SUSPECT"
    assert row["repair_rule"] == "DAILY_OPEN_OUTSIDE_INTRADAY_ENVELOPE"
    assert repair_evidence["unrepaired_conflicts"] == [
        "DAILY_OPEN_OUTSIDE_INTRADAY_ENVELOPE"
    ]


def test_non_price_daily_fields_are_not_structural_failures() -> None:
    issues = structural_issues(
        OhlcvValues(
            open=146.74,
            high=146.99,
            low=144.75,
            close=145.48,
            volume=-1.0,
            vwap=144.4205,
            transaction_count=-1,
        )
    )

    assert "vwap_outside_ohlc_envelope" not in issues
    assert issues == ()


def test_live_bar_executor_treats_not_authorized_as_skipped_receipt(
    tmp_path: Path,
) -> None:
    legacy = load_fixture("simple_current_sfbc.json")
    result = plan_backfill(ledger_from_legacy_plan(legacy).snapshot())
    transport = QueueHttpJsonTransport(
        [HttpJsonResponse(200, {"status": "NOT_AUTHORIZED", "results": []})]
    )
    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"), transport
    )
    repository = SQLiteStockUniverseRepository(tmp_path / "stock_universe.sqlite")
    result = plan_with_allocated_lookup(repository, result)
    approval = ExecutionApproval(
        request_hash=result.request.request_hash, approved_by="test"
    )
    approval_record = repository.insert_execution_approval(
        result, approval, reason="unit test approval"
    )

    receipt = execute_live_bar_backfill(
        result,
        approval,
        client,
        repository,
        evidence_facts=tuple(
            facts_from_legacy_plan(legacy, include_candidate_segments=False)
        ),
    )

    assert receipt.ok is False
    assert receipt.status == "skipped"
    assert receipt.skip_reason == "provider_not_authorized"
    assert receipt.provider_status == "NOT_AUTHORIZED"
    assert repository.counts()["ohlcv_bars"] == 0
    assert repository.counts()["execution_receipts"] == 1
    audit_rows = repository.execution_audit(request_hash=result.request.request_hash)
    assert audit_rows[0]["receipt_status"] == "skipped"
    assert audit_rows[0]["approval_hash"] == approval_record["approval_hash"]


def test_quality_audit_classifies_not_authorized_receipt_as_provider_entitlement_skip(
    tmp_path: Path,
) -> None:
    legacy = load_fixture("simple_current_sfbc.json")
    result = plan_backfill(ledger_from_legacy_plan(legacy).snapshot())
    transport = QueueHttpJsonTransport(
        [HttpJsonResponse(200, {"status": "NOT_AUTHORIZED", "results": []})]
    )
    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"), transport
    )
    repository = SQLiteStockUniverseRepository(tmp_path / "stock_universe.sqlite")
    result = plan_with_allocated_lookup(repository, result)
    repository.upsert_reference_snapshots(
        [
            StoredReferenceSnapshot(
                provider="massive.reference_tickers",
                snapshot_as_of_date="2026-05-08",
                ticker=result.target.latest_ticker,
                ohlcv_series_id=result.target.ohlcv_series_id,
                active=True,
                company_name=result.target.company_name,
                cik=result.target.cik,
                composite_figi=result.target.composite_figi,
                share_class_figi=result.target.share_class_figi,
                security_type=result.target.security_type,
                primary_exchange=result.target.latest_primary_exchange,
                market="stocks",
                locale="us",
                identity_status=result.target.identity_status,
                natural_key=result.target.natural_key,
                raw={"ticker": result.target.latest_ticker},
            )
        ]
    )
    approval = ExecutionApproval(
        request_hash=result.request.request_hash, approved_by="test"
    )
    repository.insert_execution_approval(result, approval, reason="unit test approval")

    execute_live_bar_backfill(
        result,
        approval,
        client,
        repository,
        evidence_facts=tuple(
            facts_from_legacy_plan(legacy, include_candidate_segments=False)
        ),
    )
    report = quality_audit(tmp_path / "stock_universe.sqlite")

    assert report["category_counts"] == {"provider_not_authorized": 1}
    assert report["issues"][0]["category"] == "provider_not_authorized"
    assert report["issues"][0]["last_receipt_status"] == "skipped"
    assert report["issues"][0]["last_receipt_skip_reason"] == "provider_not_authorized"
    assert "execution_error" not in report["category_counts"]


def test_quality_audit_next_action_names_use_ohlcv_series_id_contract() -> None:
    dry_run_action = _next_action_for_row(
        {"category": "data_not_loaded", "ohlcv_series_id": 123, "ticker": "ABC"},
        db="stock.sqlite",
        global_min_bar_date="",
    )
    observe_action = _next_action_for_row(
        {
            "category": "provider_not_authorized",
            "ohlcv_series_id": 456,
            "ticker": "DEF",
        },
        db="stock.sqlite",
        global_min_bar_date="",
    )

    assert dry_run_action["name"] == "dry-run-ohlcv-series-backfill"
    assert dry_run_action["command"]["args"]["ohlcv_series_id"] == 123
    assert "series_id" not in dry_run_action["command"]["args"]
    assert observe_action["name"] == "observe-ohlcv-series-executions"
    assert observe_action["command"]["args"]["ohlcv_series_id"] == 456
    assert "series_id" not in observe_action["command"]["args"]


def test_quality_audit_scopes_bar_counts_by_grain(tmp_path: Path) -> None:
    db = tmp_path / "stock_universe.sqlite"
    repository = SQLiteStockUniverseRepository(db)
    repository.upsert_reference_snapshots(
        [
            StoredReferenceSnapshot(
                provider="massive.reference_tickers",
                snapshot_as_of_date="2026-05-08",
                ticker="DAILY",
                active=True,
                company_name="Daily Bars Inc.",
                cik="1",
                composite_figi="BBGDAILY",
                share_class_figi="",
                security_type="CS",
                primary_exchange="XNAS",
                market="stocks",
                locale="us",
                identity_status="permanent",
                natural_key="massive:composite_figi:BBGDAILY",
                raw={"ticker": "DAILY"},
            ),
            StoredReferenceSnapshot(
                provider="massive.reference_tickers",
                snapshot_as_of_date="2026-05-08",
                ticker="MINUTE",
                active=True,
                company_name="Minute Bars Inc.",
                cik="2",
                composite_figi="BBGMINUTE",
                share_class_figi="",
                security_type="CS",
                primary_exchange="XNAS",
                market="stocks",
                locale="us",
                identity_status="permanent",
                natural_key="massive:composite_figi:BBGMINUTE",
                raw={"ticker": "MINUTE"},
            ),
            StoredReferenceSnapshot(
                provider="massive.reference_tickers",
                snapshot_as_of_date="2026-05-08",
                ticker="THIRTY",
                active=True,
                company_name="Thirty Minute Bars Inc.",
                cik="3",
                composite_figi="BBGTHIRTY",
                share_class_figi="",
                security_type="CS",
                primary_exchange="XNAS",
                market="stocks",
                locale="us",
                identity_status="permanent",
                natural_key="massive:composite_figi:BBGTHIRTY",
                raw={"ticker": "THIRTY"},
            ),
        ]
    )
    daily_series_id = repository.lookup_ohlcv_series_id(
        "massive:composite_figi:BBGDAILY"
    )
    minute_series_id = repository.lookup_ohlcv_series_id(
        "massive:composite_figi:BBGMINUTE"
    )
    thirty_series_id = repository.lookup_ohlcv_series_id(
        "massive:composite_figi:BBGTHIRTY"
    )
    assert daily_series_id is not None
    assert minute_series_id is not None
    assert thirty_series_id is not None
    repository.insert_bars(
        [
            StoredOhlcvBar(
                series_id=daily_series_id,
                ticker="DAILY",
                bar_date="2026-05-01",
                bar_start_ts=1777593600000,
                multiplier=1,
                timespan="day",
                adjusted=True,
                open=1,
                high=2,
                low=1,
                close=2,
                volume=100,
                bar_quality_status="VALIDATED",
            ),
            StoredOhlcvBar(
                series_id=minute_series_id,
                ticker="MINUTE",
                bar_date="2026-05-01",
                bar_start_ts=1777627800000,
                multiplier=1,
                timespan="minute",
                adjusted=True,
                open=3,
                high=4,
                low=3,
                close=4,
                volume=200,
                bar_quality_status="VALIDATED",
            ),
            StoredOhlcvBar(
                series_id=thirty_series_id,
                ticker="THIRTY",
                bar_date="2026-05-01",
                bar_start_ts=1777627800000,
                multiplier=30,
                timespan="minute",
                adjusted=True,
                open=5,
                high=6,
                low=5,
                close=6,
                volume=300,
                bar_quality_status="VALIDATED",
            ),
        ]
    )

    daily_report = quality_audit(db, include_healthy=True)
    minute_report = quality_audit(db, include_healthy=True, bar_grain="1m")
    thirty_report = quality_audit(db, include_healthy=True, bar_grain="30m")
    daily_by_ticker = {row["ticker"]: row for row in daily_report["issues"]}
    minute_by_ticker = {row["ticker"]: row for row in minute_report["issues"]}
    thirty_by_ticker = {row["ticker"]: row for row in thirty_report["issues"]}

    assert daily_report["bar_grain"] == "1d"
    assert minute_report["bar_grain"] == "1m"
    assert thirty_report["bar_grain"] == "30m"
    assert daily_by_ticker["DAILY"]["category"] == "no_action_needed"
    assert daily_by_ticker["MINUTE"]["category"] == "data_not_loaded"
    assert daily_by_ticker["THIRTY"]["category"] == "data_not_loaded"
    assert minute_by_ticker["DAILY"]["category"] == "data_not_loaded"
    assert minute_by_ticker["MINUTE"]["category"] == "no_action_needed"
    assert minute_by_ticker["THIRTY"]["category"] == "data_not_loaded"
    assert thirty_by_ticker["DAILY"]["category"] == "data_not_loaded"
    assert thirty_by_ticker["MINUTE"]["category"] == "data_not_loaded"
    assert thirty_by_ticker["THIRTY"]["category"] == "no_action_needed"
    assert "--bar-grain 1m" in minute_by_ticker["MINUTE"]["suggested_next_command"]
    assert "--bar-grain 30m" in thirty_by_ticker["THIRTY"]["suggested_next_command"]


def test_quality_audit_classifies_legacy_not_authorized_error_as_provider_not_authorized(
    tmp_path: Path,
) -> None:
    legacy = load_fixture("simple_current_sfbc.json")
    result = plan_backfill(ledger_from_legacy_plan(legacy).snapshot())
    repository = SQLiteStockUniverseRepository(tmp_path / "stock_universe.sqlite")
    result = plan_with_allocated_lookup(repository, result)
    repository.upsert_reference_snapshots(
        [
            StoredReferenceSnapshot(
                provider="massive.reference_tickers",
                snapshot_as_of_date="2026-05-08",
                ticker=result.target.latest_ticker,
                ohlcv_series_id=result.target.ohlcv_series_id,
                active=True,
                company_name=result.target.company_name,
                cik=result.target.cik,
                composite_figi=result.target.composite_figi,
                share_class_figi=result.target.share_class_figi,
                security_type=result.target.security_type,
                primary_exchange=result.target.latest_primary_exchange,
                market="stocks",
                locale="us",
                identity_status=result.target.identity_status,
                natural_key=result.target.natural_key,
                raw={"ticker": result.target.latest_ticker},
            )
        ]
    )
    repository.insert_execution_receipt(
        {
            "request_hash": result.request.request_hash,
            "evidence_ledger_hash": result.evidence_ledger_hash,
            "ohlcv_series_id": result.target.ohlcv_series_id,
            "status": "error",
            "approved_by": "pytest",
            "started_at_utc": "2026-05-09T00:00:00+00:00",
            "finished_at_utc": "2026-05-09T00:00:01+00:00",
            "planned_segment_count": 1,
            "fetched_bar_count": 0,
            "inserted_bar_count": 0,
            "request_log": [],
            "error_type": "RuntimeError",
            "error_message": "bar fetch failed for SFBC: provider status NOT_AUTHORIZED",
        }
    )

    report = quality_audit(tmp_path / "stock_universe.sqlite")

    assert report["category_counts"] == {"provider_not_authorized": 1}
    assert report["issues"][0]["category"] == "provider_not_authorized"
    assert report["issues"][0]["last_receipt_status"] == "error"
    assert "execution_error" not in report["category_counts"]


def test_live_bar_executor_requires_durable_approval_record(tmp_path: Path) -> None:
    legacy = load_fixture("simple_current_sfbc.json")
    result = plan_backfill(ledger_from_legacy_plan(legacy).snapshot())
    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"),
        QueueHttpJsonTransport([]),
    )
    repository = SQLiteStockUniverseRepository(tmp_path / "stock_universe.sqlite")
    result = plan_with_allocated_lookup(repository, result)

    with pytest.raises(ExecutionContractError, match="durable execution approval"):
        execute_live_bar_backfill(
            result,
            ExecutionApproval(
                request_hash=result.request.request_hash, approved_by="test"
            ),
            client,
            repository,
        )

    assert repository.counts()["ohlcv_bars"] == 0


def test_live_bar_executor_rejects_provider_bars_outside_planned_segment(
    tmp_path: Path,
) -> None:
    legacy = load_fixture("simple_current_sfbc.json")
    result = plan_backfill(ledger_from_legacy_plan(legacy).snapshot())
    transport = QueueHttpJsonTransport(
        [
            HttpJsonResponse(
                200,
                {
                    "status": "OK",
                    "resultsCount": 1,
                    "results": [
                        {
                            "t": 1609459200000,
                            "o": 10,
                            "h": 11,
                            "l": 9,
                            "c": 10.5,
                            "v": 100,
                        },
                    ],
                },
            )
        ]
    )
    client = MassiveReadOnlyClient(
        MassiveProviderConfig("secret", base_url="https://example.test"), transport
    )
    repository = SQLiteStockUniverseRepository(tmp_path / "stock_universe.sqlite")
    result = plan_with_allocated_lookup(repository, result)
    approval = ExecutionApproval(
        request_hash=result.request.request_hash, approved_by="test"
    )
    repository.insert_execution_approval(result, approval, reason="unit test approval")

    with pytest.raises(RuntimeError, match="out-of-segment"):
        execute_live_bar_backfill(
            result,
            approval,
            client,
            repository,
        )

    assert repository.counts()["ohlcv_bars"] == 0
    assert repository.counts()["execution_receipts"] == 1
    audit_rows = repository.execution_audit(request_hash=result.request.request_hash)
    assert audit_rows[0]["receipt_status"] == "error"
    assert audit_rows[0]["fetched_bar_count"] == 0
    assert audit_rows[0]["inserted_bar_count"] == 0


def test_quality_audit_flags_approved_plan_missing_receipt(tmp_path: Path) -> None:
    legacy = load_fixture("simple_current_sfbc.json")
    result = plan_backfill(ledger_from_legacy_plan(legacy).snapshot())
    repository = SQLiteStockUniverseRepository(tmp_path / "stock_universe.sqlite")
    result = plan_with_allocated_lookup(repository, result)
    repository.upsert_reference_snapshots(
        [
            StoredReferenceSnapshot(
                provider="massive.reference_tickers",
                snapshot_as_of_date="2026-05-08",
                ticker=result.target.latest_ticker,
                active=True,
                company_name=result.target.company_name,
                cik=result.target.cik,
                composite_figi=result.target.composite_figi,
                share_class_figi=result.target.share_class_figi,
                security_type=result.target.security_type,
                primary_exchange=result.target.latest_primary_exchange,
                market="stocks",
                locale="us",
                identity_status=result.target.identity_status,
                natural_key=result.target.natural_key,
                raw={"ticker": result.target.latest_ticker},
            )
        ]
    )
    repository.persist_plan_context(result)
    repository.insert_execution_approval(
        result,
        ExecutionApproval(request_hash=result.request.request_hash, approved_by="test"),
        reason="unit test approval",
    )

    report = quality_audit(tmp_path / "stock_universe.sqlite")

    assert report["category_counts"]["approved_plan_missing_receipt"] == 1
    assert report["issues"][0]["category"] == "approved_plan_missing_receipt"
    assert report["issues"][0]["ohlcv_series_id"] == result.target.ohlcv_series_id


def test_quality_audit_category_filters_apply_to_summary_counts(tmp_path: Path) -> None:
    legacy = load_fixture("simple_current_sfbc.json")
    result = plan_backfill(ledger_from_legacy_plan(legacy).snapshot())
    repository = SQLiteStockUniverseRepository(tmp_path / "stock_universe.sqlite")
    result = plan_with_allocated_lookup(repository, result)
    repository.upsert_reference_snapshots(
        [
            StoredReferenceSnapshot(
                provider="massive.reference_tickers",
                snapshot_as_of_date="2026-05-08",
                ticker=result.target.latest_ticker,
                active=True,
                company_name=result.target.company_name,
                cik=result.target.cik,
                composite_figi=result.target.composite_figi,
                share_class_figi=result.target.share_class_figi,
                security_type=result.target.security_type,
                primary_exchange=result.target.latest_primary_exchange,
                market="stocks",
                locale="us",
                identity_status=result.target.identity_status,
                natural_key=result.target.natural_key,
                raw={"ticker": result.target.latest_ticker},
            ),
            StoredReferenceSnapshot(
                provider="massive.reference_tickers",
                snapshot_as_of_date="2026-05-08",
                ticker="ZZZQ",
                active=True,
                company_name="Unplanned Test Issuer",
                cik="0000000000",
                security_type="CS",
                primary_exchange="XNAS",
                market="stocks",
                locale="us",
                identity_status="active",
                natural_key="test:unplanned:issuer",
                raw={"ticker": "ZZZQ"},
            ),
        ]
    )
    repository.persist_plan_context(result)
    repository.insert_execution_approval(
        result,
        ExecutionApproval(request_hash=result.request.request_hash, approved_by="test"),
        reason="unit test approval",
    )

    report = quality_audit(
        tmp_path / "stock_universe.sqlite",
        categories=("approved_plan_missing_receipt",),
    )

    assert report["matched_series_count"] == 1
    assert report["issue_count"] == 1
    assert report["category_counts"] == {"approved_plan_missing_receipt": 1}
    assert report["unfiltered_category_counts"]["approved_plan_missing_receipt"] == 1
    assert report["unfiltered_category_counts"]["data_not_loaded"] == 1
    assert [issue["category"] for issue in report["issues"]] == [
        "approved_plan_missing_receipt"
    ]


def test_quality_audit_does_not_repeat_fresh_zero_fetch_stale_receipt(
    tmp_path: Path,
) -> None:
    repository = SQLiteStockUniverseRepository(tmp_path / "stock_universe.sqlite")
    repository.upsert_reference_snapshots(
        [
            StoredReferenceSnapshot(
                provider="massive.reference_tickers",
                snapshot_as_of_date="2026-05-09",
                ticker="NOBAR",
                active=True,
                company_name="No Bar Today Inc.",
                cik="0000000000",
                composite_figi="BBGTESTNOBAR",
                share_class_figi="BBGTESTNOBAR1",
                security_type="FUND",
                primary_exchange="ARCX",
                market="stocks",
                locale="us",
                identity_status="active",
                natural_key="test:NOBAR",
                raw={"ticker": "NOBAR"},
            )
        ]
    )
    series_id = repository.lookup_ohlcv_series_id("test:NOBAR")
    assert series_id is not None
    repository.insert_bars(
        [
            StoredOhlcvBar(
                series_id=series_id,
                ticker="NOBAR",
                bar_date="2026-05-06",
                bar_start_ts=1778025600000,
                multiplier=1,
                timespan="day",
                adjusted=True,
                open=10,
                high=11,
                low=9,
                close=10,
                volume=100,
            )
        ]
    )

    stale_report = quality_audit(
        tmp_path / "stock_universe.sqlite", stale_before="2026-05-08"
    )
    assert stale_report["issues"][0]["category"] == "covered_series_data_stale"

    repository.insert_execution_receipt(
        {
            "request_hash": "request-nobar",
            "evidence_ledger_hash": "ledger-nobar",
            "ohlcv_series_id": series_id,
            "status": "ok",
            "approved_by": "pytest",
            "started_at_utc": "2026-05-09T00:00:00+00:00",
            "finished_at_utc": "2026-05-09T00:00:01+00:00",
            "planned_segment_count": 1,
            "fetched_bar_count": 0,
            "inserted_bar_count": 0,
            "request_log": [],
        }
    )

    fresh_report = quality_audit(
        tmp_path / "stock_universe.sqlite", stale_before="2026-05-08"
    )
    assert fresh_report["issue_count"] == 0
    assert fresh_report["category_counts"] == {}

    next_day_report = quality_audit(
        tmp_path / "stock_universe.sqlite", stale_before="2026-05-10"
    )
    assert next_day_report["issues"][0]["category"] == "covered_series_data_stale"


def test_repair_missing_execution_receipts_writes_error_receipt(tmp_path: Path) -> None:
    legacy = load_fixture("simple_current_sfbc.json")
    result = plan_backfill(ledger_from_legacy_plan(legacy).snapshot())
    repository = SQLiteStockUniverseRepository(tmp_path / "stock_universe.sqlite")
    result = plan_with_allocated_lookup(repository, result)
    repository.persist_plan_context(result)
    repository.insert_execution_approval(
        result,
        ExecutionApproval(request_hash=result.request.request_hash, approved_by="test"),
        reason="unit test approval",
    )

    dry_run = repair_missing_execution_receipts(
        tmp_path / "stock_universe.sqlite", series_ids=(result.target.ohlcv_series_id,)
    )
    committed = repair_missing_execution_receipts(
        tmp_path / "stock_universe.sqlite",
        series_ids=(result.target.ohlcv_series_id,),
        commit=True,
    )

    assert dry_run["selected_count"] == 1
    assert dry_run["repaired_count"] == 0
    assert committed["repaired_count"] == 1
    audit_rows = repository.execution_audit(request_hash=result.request.request_hash)
    assert audit_rows[0]["receipt_status"] == "error"
    assert audit_rows[0]["fetched_bar_count"] == 0
    assert audit_rows[0]["inserted_bar_count"] == 0
    assert (
        repair_missing_execution_receipts(
            tmp_path / "stock_universe.sqlite",
            series_ids=(result.target.ohlcv_series_id,),
        )["selected_count"]
        == 0
    )
