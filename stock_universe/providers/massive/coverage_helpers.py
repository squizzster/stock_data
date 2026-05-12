"""Coverage accounting helper rules for Massive providers."""

from __future__ import annotations

import urllib.parse
from typing import Any

from stock_universe.domain import (
    BackfillRequest,
    EvidenceFact,
    HandoffSegmentFact,
    OmittedSegmentFact,
    TargetIdentity,
    TerminalCoverageFact,
    TickerReplacementFact,
)
from stock_universe.domain.records import parse_date, unfreeze_json
from stock_universe.evidence.normalizers import (
    omitted_segment_fact_from_absent_reference_and_bars,
    ticker_replacement_fact_from_target_valid_alias_window,
)
from stock_universe.market_calendar import (
    next_us_equity_trading_date,
    previous_us_equity_trading_date,
)
from stock_universe.providers.massive.client import MassiveReadOnlyClient
from stock_universe.providers.massive.payloads import (
    _aggregate_bars_payload,
    _bar_dates_from_payload,
    _bar_probe_result_from_payload,
    _identity_scan_result_from_payload,
    _reference_snapshot_from_payload,
)
from stock_universe.providers.massive.reference_helpers import (
    _reference_boundary_fact_with_historical_rekey,
    _reference_is_conclusive_non_target,
    _reference_is_target_match,
    _security_types_compatible_for_historical_identity,
    _segment_validation_row,
)
from stock_universe.providers.models import (
    BarProbeResult,
    IdentityScanResult,
    ReferenceSnapshot,
)

AGGREGATE_USABLE_STATUSES = {"DELAYED", "OK"}


def _omitted_fact_for_absent_ticker_interval(
    client: MassiveReadOnlyClient,
    request: BackfillRequest,
    target: TargetIdentity,
    ticker: str,
    from_date: str,
    to_date: str,
) -> EvidenceFact | None:
    start_payload = client.get(
        f"/v3/reference/tickers/{urllib.parse.quote(ticker, safe='')}",
        {"date": from_date},
    )
    end_payload = client.get(
        f"/v3/reference/tickers/{urllib.parse.quote(ticker, safe='')}",
        {"date": to_date},
    )
    bars_payload = _aggregate_bars_payload(client, request, ticker, from_date, to_date)
    start_scan_payload = client.get(
        "/v3/reference/tickers",
        {"search": ticker, "date": from_date, "active": "false", "limit": "100"},
    )
    end_scan_payload = client.get(
        "/v3/reference/tickers",
        {"search": ticker, "date": to_date, "active": "false", "limit": "100"},
    )
    omitted = omitted_segment_fact_from_absent_reference_and_bars(
        request.series_id,
        ticker=ticker,
        from_date=from_date,
        to_date=to_date,
        start_reference=_reference_snapshot_from_payload(
            ticker, from_date, start_payload
        ),
        end_reference=_reference_snapshot_from_payload(ticker, to_date, end_payload),
        bar_probe=_bar_probe_result_from_payload(
            ticker, from_date, to_date, bars_payload
        ),
        start_identity_scan=_identity_scan_result_from_payload(
            ticker, from_date, start_scan_payload
        ),
        end_identity_scan=_identity_scan_result_from_payload(
            ticker, to_date, end_scan_payload
        ),
        source="massive.absent_reference_bars_identity_scan",
    )
    if omitted:
        return omitted.to_evidence_fact(request.series_id)
    omitted = _omitted_fact_from_non_downloadable_interval(
        client,
        request,
        target,
        ticker=ticker,
        from_date=from_date,
        to_date=to_date,
        start_reference=_reference_snapshot_from_payload(
            ticker, from_date, start_payload
        ),
        end_reference=_reference_snapshot_from_payload(ticker, to_date, end_payload),
        bar_probe=_bar_probe_result_from_payload(
            ticker, from_date, to_date, bars_payload
        ),
        start_identity_scan=_identity_scan_result_from_payload(
            ticker, from_date, start_scan_payload
        ),
        end_identity_scan=_identity_scan_result_from_payload(
            ticker, to_date, end_scan_payload
        ),
    )
    if omitted is None:
        omitted = _omitted_fact_from_intrabar_non_target_interval(
            client,
            request,
            target,
            ticker=ticker,
            from_date=from_date,
            to_date=to_date,
            bars_payload=bars_payload,
            start_reference=_reference_snapshot_from_payload(
                ticker, from_date, start_payload
            ),
            end_reference=_reference_snapshot_from_payload(
                ticker, to_date, end_payload
            ),
            bar_probe=_bar_probe_result_from_payload(
                ticker, from_date, to_date, bars_payload
            ),
            start_identity_scan=_identity_scan_result_from_payload(
                ticker, from_date, start_scan_payload
            ),
            end_identity_scan=_identity_scan_result_from_payload(
                ticker, to_date, end_scan_payload
            ),
        )
    if omitted is None:
        omitted = _omitted_fact_before_first_target_reference(
            client,
            request,
            target,
            ticker=ticker,
            from_date=from_date,
            to_date=to_date,
            bars_payload=bars_payload,
            start_reference=_reference_snapshot_from_payload(
                ticker, from_date, start_payload
            ),
            end_reference=_reference_snapshot_from_payload(
                ticker, to_date, end_payload
            ),
            bar_probe=_bar_probe_result_from_payload(
                ticker, from_date, to_date, bars_payload
            ),
            start_identity_scan=_identity_scan_result_from_payload(
                ticker, from_date, start_scan_payload
            ),
            end_identity_scan=_identity_scan_result_from_payload(
                ticker, to_date, end_scan_payload
            ),
        )
    return omitted.to_evidence_fact(request.series_id) if omitted else None


def _terminal_coverage_fact(
    client: MassiveReadOnlyClient,
    request: BackfillRequest,
    target: TargetIdentity,
    ticker: str,
    from_date: str,
    to_date: str,
) -> EvidenceFact | None:
    bars_payload = _aggregate_bars_payload(client, request, ticker, from_date, to_date)
    bar_probe = _bar_probe_result_from_payload(ticker, from_date, to_date, bars_payload)
    if bar_probe.api_status not in AGGREGATE_USABLE_STATUSES:
        return None
    if bar_probe.bar_count == 0:
        fact = TerminalCoverageFact(
            ticker=ticker,
            from_date=from_date,
            to_date=to_date,
            reason="aggregate bars were absent for the terminal interval; no terminal bars are available to download",
            source="massive.terminal_no_bar_coverage",
        )
        return fact.to_evidence_fact(request.series_id)

    dates = _bar_dates_from_payload(bars_payload)
    if not dates:
        return None
    start_payload = client.get(
        f"/v3/reference/tickers/{urllib.parse.quote(ticker, safe='')}",
        {"date": dates[0]},
    )
    end_payload = client.get(
        f"/v3/reference/tickers/{urllib.parse.quote(ticker, safe='')}",
        {"date": dates[-1]},
    )
    start_reference = _reference_snapshot_from_payload(ticker, dates[0], start_payload)
    end_reference = _reference_snapshot_from_payload(ticker, dates[-1], end_payload)
    start_fact = _reference_boundary_fact_with_historical_rekey(
        request.series_id,
        target,
        start_reference,
        point="start",
        source="massive.terminal_target_bar_window",
    )
    end_fact = _reference_boundary_fact_with_historical_rekey(
        request.series_id,
        target,
        end_reference,
        point="end",
        source="massive.terminal_target_bar_window",
    )
    if start_fact.matched is True and end_fact.matched is True:
        fact = HandoffSegmentFact(
            ticker=ticker,
            from_date=dates[0],
            to_date=dates[-1],
            source="massive.terminal_target_bar_window",
            event_ticker_handoff={
                "bar_count": len(dates),
                "candidate_ticker": target.latest_ticker or ticker,
                "event_ticker": ticker,
                "first_bar_date": dates[0],
                "handoff_reason": "terminal_target_ticker_bar_window",
                "last_bar_date": dates[-1],
                "requested_from_date": from_date,
                "requested_to_date": to_date,
            },
            validation=(
                _segment_validation_row(start_fact.to_legacy_dict(), "start"),
                _segment_validation_row(end_fact.to_legacy_dict(), "end"),
            ),
            extra={
                "terminal_coverage_probe": {
                    "api_status": bar_probe.api_status,
                    "bar_count": bar_probe.bar_count,
                    "from_date": from_date,
                    "ticker": ticker,
                    "to_date": to_date,
                }
            },
        )
        return fact.to_evidence_fact(request.series_id)
    if start_fact.matched is True or end_fact.matched is True:
        return None
    if not (
        _reference_is_conclusive_non_target(target, start_reference)
        or _reference_is_conclusive_non_target(target, end_reference)
    ):
        return None
    fact = TerminalCoverageFact(
        ticker=ticker,
        from_date=from_date,
        to_date=to_date,
        reason=(
            "terminal aggregate bars exist under the stale event ticker, but reference boundaries for those bars "
            "do not validate the target identity."
        ),
        source="massive.terminal_non_target_bar_coverage",
    )
    return fact.to_evidence_fact(request.series_id)


def _omitted_fact_from_non_downloadable_interval(
    client: MassiveReadOnlyClient,
    request: BackfillRequest,
    target: TargetIdentity,
    *,
    ticker: str,
    from_date: str,
    to_date: str,
    start_reference: ReferenceSnapshot,
    end_reference: ReferenceSnapshot,
    bar_probe: BarProbeResult,
    start_identity_scan: IdentityScanResult,
    end_identity_scan: IdentityScanResult,
) -> OmittedSegmentFact | None:
    if not _probe_dates_align(
        ticker, from_date, to_date, start_reference, end_reference, bar_probe
    ):
        return None
    start_matches = _reference_is_target_match(target, start_reference)
    end_matches = _reference_is_target_match(target, end_reference)
    start_non_target = _reference_is_conclusive_non_target(target, start_reference)
    end_non_target = _reference_is_conclusive_non_target(target, end_reference)
    no_bars = bar_probe.bar_count == 0 and bar_probe.api_status in {
        *AGGREGATE_USABLE_STATUSES,
        "NOT_AUTHORIZED",
    }
    if (
        no_bars
        and start_reference.api_status == "NOT_FOUND"
        and end_reference.api_status == "NOT_FOUND"
    ):
        aliases = _target_identity_scan_aliases(
            target, start_identity_scan
        ) + _target_identity_scan_aliases(target, end_identity_scan)
        if not _scan_aliases_have_no_bars(
            client, request, tuple(dict.fromkeys(aliases)), from_date, to_date
        ):
            return None
        reason = (
            "provider reference was NOT_FOUND at both boundaries and aggregate bars were absent; identity scans "
            "did not reveal a target alias with downloadable bars for the interval."
        )
    elif no_bars and start_matches and end_matches:
        reason = (
            "provider reference validated the target at both interval boundaries, but aggregate bars were absent; "
            "there are no downloadable bars for this target interval."
        )
    elif (
        no_bars
        and (start_matches or end_matches)
        and (
            start_reference.api_status == "NOT_FOUND"
            or end_reference.api_status == "NOT_FOUND"
        )
    ):
        reason = (
            "event ticker had no aggregate bars in the uncovered interval; one boundary validated the target "
            "while the other boundary was unavailable, so there were no downloadable target bars for that ticker."
        )
    elif (
        no_bars
        and (
            start_reference.api_status == "NOT_FOUND"
            or end_reference.api_status == "NOT_FOUND"
        )
        and not start_matches
        and not end_matches
        and not start_non_target
        and not end_non_target
    ):
        aliases = _target_identity_scan_aliases(
            target, start_identity_scan
        ) + _target_identity_scan_aliases(target, end_identity_scan)
        if not _scan_aliases_have_no_bars(
            client, request, tuple(dict.fromkeys(aliases)), from_date, to_date
        ):
            return None
        reason = (
            "event ticker had no aggregate bars in the uncovered interval; one boundary was unavailable and the "
            "other boundary did not contain enough durable identifiers to validate the target, while identity scans "
            "did not reveal a compatible target alias with downloadable bars."
        )
    elif (
        (start_non_target or end_non_target)
        and (start_non_target or start_reference.api_status == "NOT_FOUND")
        and (end_non_target or end_reference.api_status == "NOT_FOUND")
    ):
        aliases = _target_identity_scan_aliases(
            target, start_identity_scan
        ) + _target_identity_scan_aliases(target, end_identity_scan)
        if not _scan_aliases_have_no_bars(
            client, request, tuple(dict.fromkeys(aliases)), from_date, to_date
        ):
            return None
        reason = (
            "ticker-events returned a different durable instrument for the uncovered interval; downloading that "
            "ticker would ingest non-target bars."
        )
    else:
        return None
    return OmittedSegmentFact(
        ticker=ticker,
        from_date=from_date,
        to_date=to_date,
        reason=reason,
        source="massive.non_downloadable_ticker_interval",
        proof=_omitted_proof(
            start_reference,
            end_reference,
            bar_probe,
            start_identity_scan,
            end_identity_scan,
        ),
    )


def _omitted_fact_from_intrabar_non_target_interval(
    client: MassiveReadOnlyClient,
    request: BackfillRequest,
    target: TargetIdentity,
    *,
    ticker: str,
    from_date: str,
    to_date: str,
    bars_payload: dict[str, Any],
    start_reference: ReferenceSnapshot,
    end_reference: ReferenceSnapshot,
    bar_probe: BarProbeResult,
    start_identity_scan: IdentityScanResult,
    end_identity_scan: IdentityScanResult,
) -> OmittedSegmentFact | None:
    if not _probe_dates_align(
        ticker, from_date, to_date, start_reference, end_reference, bar_probe
    ):
        return None
    if (
        bar_probe.api_status not in AGGREGATE_USABLE_STATUSES
        or bar_probe.bar_count == 0
    ):
        return None
    start_boundary_absent_or_non_target = (
        start_reference.api_status == "NOT_FOUND"
        or _reference_is_conclusive_non_target(
            target,
            start_reference,
        )
    )
    end_boundary_absent_or_non_target = (
        end_reference.api_status == "NOT_FOUND"
        or _reference_is_conclusive_non_target(
            target,
            end_reference,
        )
    )
    if not start_boundary_absent_or_non_target or not end_boundary_absent_or_non_target:
        return None
    dates = _bar_dates_from_payload(bars_payload)
    if not dates:
        return None
    first_payload = client.get(
        f"/v3/reference/tickers/{urllib.parse.quote(ticker, safe='')}",
        {"date": dates[0]},
    )
    last_payload = client.get(
        f"/v3/reference/tickers/{urllib.parse.quote(ticker, safe='')}",
        {"date": dates[-1]},
    )
    first_reference = _reference_snapshot_from_payload(ticker, dates[0], first_payload)
    last_reference = _reference_snapshot_from_payload(ticker, dates[-1], last_payload)
    if _reference_is_target_match(
        target, first_reference
    ) or _reference_is_target_match(target, last_reference):
        return None
    if not (
        _reference_is_conclusive_non_target(target, first_reference)
        or _reference_is_conclusive_non_target(target, last_reference)
    ):
        return None
    aliases = _target_identity_scan_aliases(
        target, start_identity_scan
    ) + _target_identity_scan_aliases(target, end_identity_scan)
    if not _scan_aliases_have_no_bars(
        client, request, tuple(dict.fromkeys(aliases)), from_date, to_date
    ):
        return None
    proof = _omitted_proof(
        start_reference,
        end_reference,
        bar_probe,
        start_identity_scan,
        end_identity_scan,
    )
    proof["first_bar_reference"] = {
        "api_status": first_reference.api_status,
        "as_of_date": first_reference.as_of_date.isoformat(),
        "cik": first_reference.cik,
        "composite_figi": first_reference.composite_figi,
        "ticker": first_reference.ticker,
    }
    proof["last_bar_reference"] = {
        "api_status": last_reference.api_status,
        "as_of_date": last_reference.as_of_date.isoformat(),
        "cik": last_reference.cik,
        "composite_figi": last_reference.composite_figi,
        "ticker": last_reference.ticker,
    }
    return OmittedSegmentFact(
        ticker=ticker,
        from_date=from_date,
        to_date=to_date,
        reason=(
            "aggregate bars exist inside the interval, but point-in-time references for those bars did not "
            "validate the target identity."
        ),
        source="massive.intrabar_non_target_interval",
        proof=proof,
    )


def _omitted_fact_before_first_target_reference(
    client: MassiveReadOnlyClient,
    request: BackfillRequest,
    target: TargetIdentity,
    *,
    ticker: str,
    from_date: str,
    to_date: str,
    bars_payload: dict[str, Any],
    start_reference: ReferenceSnapshot,
    end_reference: ReferenceSnapshot,
    bar_probe: BarProbeResult,
    start_identity_scan: IdentityScanResult,
    end_identity_scan: IdentityScanResult,
) -> OmittedSegmentFact | None:
    if not _probe_dates_align(
        ticker, from_date, to_date, start_reference, end_reference, bar_probe
    ):
        return None
    if (
        bar_probe.api_status not in AGGREGATE_USABLE_STATUSES
        or bar_probe.bar_count == 0
    ):
        return None
    if _reference_is_target_match(
        target, start_reference
    ) or _reference_is_target_match(target, end_reference):
        return None
    dates = _bar_dates_from_payload(bars_payload)
    if not dates:
        return None
    if _current_reference_list_date_has_bars_inside_gap(
        client, request, target, ticker, from_date, to_date, dates
    ):
        return None
    next_date = next_us_equity_trading_date(to_date)
    next_payload = client.get(
        f"/v3/reference/tickers/{urllib.parse.quote(ticker, safe='')}",
        {"date": next_date},
    )
    next_reference = _reference_snapshot_from_payload(ticker, next_date, next_payload)
    next_fact = _reference_boundary_fact_with_historical_rekey(
        request.series_id,
        target,
        next_reference,
        point="start",
        source="massive.pre_target_reference_next_boundary",
    )
    if next_fact.matched is not True:
        return None
    proof = _omitted_proof(
        start_reference,
        end_reference,
        bar_probe,
        start_identity_scan,
        end_identity_scan,
    )
    proof["first_bar_date"] = dates[0]
    proof["last_bar_date"] = dates[-1]
    proof["next_target_reference"] = {
        "api_status": next_fact.api_status,
        "as_of_date": next_fact.as_of_date.isoformat(),
        "matched": next_fact.matched,
        "match_reason": next_fact.match_reason,
        "ticker": next_fact.ticker,
    }
    return OmittedSegmentFact(
        ticker=ticker,
        from_date=from_date,
        to_date=to_date,
        reason=(
            "aggregate bars exist before the first target-valid reference event, but the interval boundaries did "
            "not validate the target identity; bars before the target reference start are omitted."
        ),
        source="massive.pre_target_reference_interval",
        proof=proof,
    )


def _current_reference_list_date_has_bars_inside_gap(
    client: MassiveReadOnlyClient,
    request: BackfillRequest,
    target: TargetIdentity,
    ticker: str,
    from_date: str,
    to_date: str,
    bar_dates: tuple[str, ...],
) -> bool:
    if ticker != target.latest_ticker:
        return False
    payload = client.get(
        f"/v3/reference/tickers/{urllib.parse.quote(ticker, safe='')}",
        {"date": request.to_date.isoformat()},
    )
    current = _reference_snapshot_from_payload(
        ticker, request.to_date.isoformat(), payload
    )
    current_fact = _reference_boundary_fact_with_historical_rekey(
        request.series_id,
        target,
        current,
        point="current",
        source="massive.pre_target_reference_current_detail",
    )
    if current_fact.matched is not True:
        return False
    raw = unfreeze_json(current.raw)
    if not isinstance(raw, dict):
        return False
    list_date = str(raw.get("list_date") or "")
    if not list_date or not (from_date <= list_date <= to_date):
        return False
    return any(date >= list_date for date in bar_dates)


def _known_alias_replacement_for_gap(
    client: MassiveReadOnlyClient,
    request: BackfillRequest,
    target: TargetIdentity,
    *,
    old_ticker: str,
    from_date: str,
    to_date: str,
) -> EvidenceFact | None:
    candidates = tuple(
        alias
        for alias in dict.fromkeys(
            target.known_alias_tickers
            + ((target.latest_ticker,) if target.latest_ticker else ())
        )
        if alias and alias != old_ticker
    )
    candidates = candidates + tuple(
        alias
        for alias in _temporary_d_suffix_candidates(target)
        if alias and alias != old_ticker and alias not in candidates
    )
    validated: list[EvidenceFact] = []
    for candidate in candidates:
        fact = _bar_backed_replacement_fact_for_window(
            client,
            request,
            target,
            old_ticker=old_ticker,
            new_ticker=candidate,
            from_date=from_date,
            to_date=to_date,
            replacement_reason="known_alias_target_valid_bar_window_inside_coverage_gap",
        )
        if fact is not None:
            validated.append(fact)
    if len(validated) == 1:
        return validated[0]
    if not validated:
        preserved = _temporary_d_suffix_original_ticker_fact_for_gap(
            client,
            request,
            target,
            old_ticker=old_ticker,
            from_date=from_date,
            to_date=to_date,
        )
        if preserved is not None:
            return preserved
    return None


def _temporary_d_suffix_candidates(target: TargetIdentity) -> tuple[str, ...]:
    latest = str(target.latest_ticker or "").strip()
    if str(target.security_type or "").upper() != "CS":
        return ()
    if not latest or latest.upper().endswith("D"):
        return ()
    return (f"{latest}D",)


def _temporary_d_suffix_original_ticker_fact_for_gap(
    client: MassiveReadOnlyClient,
    request: BackfillRequest,
    target: TargetIdentity,
    *,
    old_ticker: str,
    from_date: str,
    to_date: str,
) -> EvidenceFact | None:
    latest = str(target.latest_ticker or "").strip()
    if not latest or old_ticker != latest:
        return None
    candidates = _temporary_d_suffix_candidates(target)
    if len(candidates) != 1:
        return None
    temporary_ticker = candidates[0]
    if not _temporary_d_suffix_has_no_bars(
        client, request, temporary_ticker, from_date, to_date
    ):
        return None
    bridge = _temporary_d_suffix_bridge_inside_request(
        client,
        request,
        target,
        old_ticker=old_ticker,
        temporary_ticker=temporary_ticker,
        from_date=from_date,
        to_date=to_date,
    )
    if bridge is None:
        return None
    return _target_valid_original_ticker_fact_for_window(
        client,
        request,
        target,
        ticker=old_ticker,
        from_date=from_date,
        to_date=to_date,
        bridge=bridge,
    )


def _temporary_d_suffix_has_no_bars(
    client: MassiveReadOnlyClient,
    request: BackfillRequest,
    ticker: str,
    from_date: str,
    to_date: str,
) -> bool:
    bars_payload = _aggregate_bars_payload(client, request, ticker, from_date, to_date)
    bar_probe = _bar_probe_result_from_payload(ticker, from_date, to_date, bars_payload)
    return (
        bar_probe.api_status in AGGREGATE_USABLE_STATUSES and bar_probe.bar_count == 0
    )


def _temporary_d_suffix_bridge_inside_request(
    client: MassiveReadOnlyClient,
    request: BackfillRequest,
    target: TargetIdentity,
    *,
    old_ticker: str,
    temporary_ticker: str,
    from_date: str,
    to_date: str,
) -> dict[str, Any] | None:
    for bridge_from, bridge_to in _outside_gap_windows_inside_request(
        request, from_date, to_date
    ):
        fact = _bar_backed_replacement_fact_for_window(
            client,
            request,
            target,
            old_ticker=old_ticker,
            new_ticker=temporary_ticker,
            from_date=bridge_from,
            to_date=bridge_to,
            replacement_reason="temporary_d_suffix_target_valid_window_inside_request",
        )
        if fact is None:
            continue
        payload = fact.payload_value()
        return {
            "temporary_ticker": temporary_ticker,
            "from_date": payload["from_date"],
            "to_date": payload["to_date"],
            "replacement_reason": payload["replacement_reason"],
        }
    return None


def _outside_gap_windows_inside_request(
    request: BackfillRequest,
    from_date: str,
    to_date: str,
) -> tuple[tuple[str, str], ...]:
    windows: list[tuple[str, str]] = []
    gap_start = parse_date(from_date)
    gap_end = parse_date(to_date)
    before_end = parse_date(previous_us_equity_trading_date(gap_start))
    if request.from_date <= before_end:
        windows.append((request.from_date.isoformat(), before_end.isoformat()))
    after_start = parse_date(next_us_equity_trading_date(gap_end))
    if after_start <= request.to_date:
        windows.append((after_start.isoformat(), request.to_date.isoformat()))
    return tuple(windows)


def _target_valid_original_ticker_fact_for_window(
    client: MassiveReadOnlyClient,
    request: BackfillRequest,
    target: TargetIdentity,
    *,
    ticker: str,
    from_date: str,
    to_date: str,
    bridge: dict[str, Any],
) -> EvidenceFact | None:
    bars_payload = _aggregate_bars_payload(client, request, ticker, from_date, to_date)
    bar_probe = _bar_probe_result_from_payload(ticker, from_date, to_date, bars_payload)
    if bar_probe.api_status not in AGGREGATE_USABLE_STATUSES:
        return None
    dates = _bar_dates_from_payload(bars_payload)
    if not dates or dates[0] != from_date or dates[-1] != to_date:
        return None
    start_payload = client.get(
        f"/v3/reference/tickers/{urllib.parse.quote(ticker, safe='')}",
        {"date": from_date},
    )
    end_payload = client.get(
        f"/v3/reference/tickers/{urllib.parse.quote(ticker, safe='')}",
        {"date": to_date},
    )
    start_reference = _reference_snapshot_from_payload(ticker, from_date, start_payload)
    end_reference = _reference_snapshot_from_payload(ticker, to_date, end_payload)
    start_fact = _reference_boundary_fact_with_historical_rekey(
        request.series_id,
        target,
        start_reference,
        point="start",
        source="massive.temporary_d_suffix_original_ticker_tail",
    )
    end_fact = _reference_boundary_fact_with_historical_rekey(
        request.series_id,
        target,
        end_reference,
        point="end",
        source="massive.temporary_d_suffix_original_ticker_tail",
    )
    if start_fact.matched is not True or end_fact.matched is not True:
        return None
    reason = "temporary_d_suffix_original_ticker_tail"
    fact = TickerReplacementFact(
        old_ticker=ticker,
        new_ticker=ticker,
        from_date=from_date,
        to_date=to_date,
        replacement_reason=reason,
        source=f"ticker_events+{reason}",
        event_date=from_date,
        validation=(
            _segment_validation_row(start_fact.to_legacy_dict(), "start"),
            _segment_validation_row(end_fact.to_legacy_dict(), "end"),
        ),
        metadata={
            "ticker_replacement": {
                "new_ticker": ticker,
                "old_ticker": ticker,
                "replacement_reason": reason,
                "new_start_reason": start_fact.match_reason,
                "new_end_reason": end_fact.match_reason,
            },
            "temporary_d_suffix_bridge": {
                "original_bar_count": bar_probe.bar_count,
                "first_bar_date": dates[0],
                "last_bar_date": dates[-1],
                "temporary_ticker": bridge["temporary_ticker"],
                "temporary_from_date": bridge["from_date"],
                "temporary_to_date": bridge["to_date"],
                "temporary_replacement_reason": bridge["replacement_reason"],
            },
        },
    )
    return fact.to_evidence_fact(request.series_id)


def _bar_backed_replacement_fact_for_window(
    client: MassiveReadOnlyClient,
    request: BackfillRequest,
    target: TargetIdentity,
    *,
    old_ticker: str,
    new_ticker: str,
    from_date: str,
    to_date: str,
    replacement_reason: str,
) -> EvidenceFact | None:
    bars_payload = _aggregate_bars_payload(
        client, request, new_ticker, from_date, to_date
    )
    dates = _bar_dates_from_payload(bars_payload)
    if not dates:
        return None
    first_date = dates[0]
    last_date = dates[-1]
    start_payload = client.get(
        f"/v3/reference/tickers/{urllib.parse.quote(new_ticker, safe='')}",
        {"date": first_date},
    )
    end_payload = client.get(
        f"/v3/reference/tickers/{urllib.parse.quote(new_ticker, safe='')}",
        {"date": last_date},
    )
    fact = ticker_replacement_fact_from_target_valid_alias_window(
        request.series_id,
        target,
        old_ticker=old_ticker,
        new_ticker=new_ticker,
        from_date=first_date,
        to_date=last_date,
        start_reference=_reference_snapshot_from_payload(
            new_ticker, first_date, start_payload
        ),
        end_reference=_reference_snapshot_from_payload(
            new_ticker, last_date, end_payload
        ),
        replacement_reason=replacement_reason,
        event_date=from_date,
        source_prefix="ticker_events",
    )
    return fact.to_evidence_fact(request.series_id) if fact else None


def _probe_dates_align(
    ticker: str,
    from_date: str,
    to_date: str,
    start_reference: ReferenceSnapshot,
    end_reference: ReferenceSnapshot,
    bar_probe: BarProbeResult,
) -> bool:
    return (
        start_reference.ticker == ticker
        and end_reference.ticker == ticker
        and bar_probe.ticker == ticker
        and start_reference.as_of_date.isoformat() == from_date
        and end_reference.as_of_date.isoformat() == to_date
        and bar_probe.from_date.isoformat() == from_date
        and bar_probe.to_date.isoformat() == to_date
    )


def _omitted_proof(
    start_reference: ReferenceSnapshot,
    end_reference: ReferenceSnapshot,
    bar_probe: BarProbeResult,
    start_identity_scan: IdentityScanResult,
    end_identity_scan: IdentityScanResult,
) -> dict[str, Any]:
    return {
        "bar_probe": {
            "api_status": bar_probe.api_status,
            "bar_count": bar_probe.bar_count,
            "from_date": bar_probe.from_date.isoformat(),
            "ticker": bar_probe.ticker,
            "to_date": bar_probe.to_date.isoformat(),
        },
        "end_identity_scan": {
            "as_of_date": end_identity_scan.as_of_date.isoformat(),
            "match_count": len(end_identity_scan.matches),
            "query": end_identity_scan.query,
        },
        "end_reference": {
            "api_status": end_reference.api_status,
            "as_of_date": end_reference.as_of_date.isoformat(),
            "composite_figi": end_reference.composite_figi,
            "cik": end_reference.cik,
            "ticker": end_reference.ticker,
        },
        "start_identity_scan": {
            "as_of_date": start_identity_scan.as_of_date.isoformat(),
            "match_count": len(start_identity_scan.matches),
            "query": start_identity_scan.query,
        },
        "start_reference": {
            "api_status": start_reference.api_status,
            "as_of_date": start_reference.as_of_date.isoformat(),
            "composite_figi": start_reference.composite_figi,
            "cik": start_reference.cik,
            "ticker": start_reference.ticker,
        },
    }


def _target_identity_scan_aliases(
    target: TargetIdentity, scan: IdentityScanResult
) -> tuple[str, ...]:
    aliases: list[str] = []
    for match in unfreeze_json(scan.matches):
        ticker = str(match.get("ticker") or "")
        if not ticker:
            continue
        match_composite = str(match.get("composite_figi") or "")
        match_share_class = str(match.get("share_class_figi") or "")
        if (
            target.composite_figi
            and match_composite
            and match_composite != target.composite_figi
        ):
            continue
        if (
            target.share_class_figi
            and match_share_class
            and match_share_class != target.share_class_figi
        ):
            continue
        match_type = str(match.get("type") or match.get("security_type") or "")
        if (
            target.security_type
            and match_type
            and not _security_types_compatible_for_historical_identity(
                target.security_type, match_type
            )
        ):
            continue
        if (
            target.composite_figi
            and str(match.get("composite_figi") or "") == target.composite_figi
        ):
            aliases.append(ticker)
        elif (
            target.share_class_figi
            and str(match.get("share_class_figi") or "") == target.share_class_figi
        ):
            aliases.append(ticker)
        elif (
            target.cik
            and str(match.get("cik") or "") == target.cik
            and _same_cik_scan_match_can_be_target_alias(target, match)
        ):
            aliases.append(ticker)
    return tuple(aliases)


def _same_cik_scan_match_can_be_target_alias(
    target: TargetIdentity, match: dict[str, Any]
) -> bool:
    ticker = str(match.get("ticker") or "")
    if target.latest_ticker and ticker == target.latest_ticker:
        return True
    target_name = _normalized_name(target.company_name or target.current_company_name)
    match_name = _normalized_name(str(match.get("name") or ""))
    return bool(target_name and match_name and target_name == match_name)


def _normalized_name(value: str) -> str:
    return " ".join(str(value or "").upper().replace(",", " ").split())


def _scan_aliases_have_no_bars(
    client: MassiveReadOnlyClient,
    request: BackfillRequest,
    aliases: tuple[str, ...],
    from_date: str,
    to_date: str,
) -> bool:
    for alias in aliases:
        bars_payload = _aggregate_bars_payload(
            client, request, alias, from_date, to_date
        )
        if (
            _bar_probe_result_from_payload(
                alias, from_date, to_date, bars_payload
            ).bar_count
            != 0
        ):
            return False
    return True
