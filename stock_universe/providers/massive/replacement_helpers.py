"""Ticker-replacement helper rules for Massive providers."""

from __future__ import annotations

import urllib.parse

from stock_universe.domain import BackfillRequest, TargetIdentity, TickerReplacementFact
from stock_universe.evidence.normalizers import reference_boundary_fact_from_snapshot
from stock_universe.providers.massive.client import MassiveReadOnlyClient
from stock_universe.providers.massive.payloads import (
    _aggregate_bars_payload,
    _bar_probe_result_from_payload,
    _reference_snapshot_from_payload,
)
from stock_universe.providers.massive.reference_helpers import (
    _reference_missing_durable_ids_without_contradiction,
    _reference_name,
    _segment_validation_row,
)
from stock_universe.providers.models import ReferenceSnapshot


def _missing_durable_start_replacement_fact(
    series_id: int | str,
    target: TargetIdentity,
    *,
    old_ticker: str,
    new_ticker: str,
    from_date: str,
    to_date: str,
    start_reference: ReferenceSnapshot,
    end_reference: ReferenceSnapshot,
    event_date: str,
) -> TickerReplacementFact | None:
    if not _reference_missing_durable_ids_without_contradiction(
        target, start_reference, new_ticker
    ):
        return None
    end_fact = reference_boundary_fact_from_snapshot(
        series_id,
        target,
        end_reference,
        point="end",
        source="massive.ticker_replacement.end_reference",
    )
    if end_fact.matched is not True:
        return None
    start_row = start_reference.to_payload()
    start_row["matched"] = True
    start_row["match_reason"] = (
        "start_reference_missing_durable_ids_bar_backed_end_match"
    )
    start_row["point"] = "start"
    validation = (
        start_row,
        _segment_validation_row(end_fact.to_payload(), "end"),
    )
    metadata = {
        "ticker_replacement": {
            "new_ticker": new_ticker,
            "old_ticker": old_ticker,
            "replacement_reason": "known_alias_start_reference_missing_durable_ids",
            "new_start_reason": "start_reference_missing_durable_ids_bar_backed_end_match",
            "new_end_reason": end_fact.match_reason,
        },
        "start_validation_override": {
            "reason": "provider start reference omitted durable identifiers but did not contradict target",
            "start_reference": start_reference.to_payload(),
        },
    }
    return TickerReplacementFact(
        old_ticker=old_ticker,
        new_ticker=new_ticker,
        from_date=from_date,
        to_date=to_date,
        replacement_reason="known_alias_start_reference_missing_durable_ids",
        source="ticker_events+known_alias_start_reference_missing_durable_ids",
        event_date=event_date,
        validation=validation,
        metadata=metadata,
    )


def _historical_figi_rekey_bar_alias_replacement_fact(
    client: MassiveReadOnlyClient,
    request: BackfillRequest,
    target: TargetIdentity,
    *,
    old_ticker: str,
    new_ticker: str,
    from_date: str,
    to_date: str,
    requested_from_date: str,
    requested_to_date: str,
    start_reference: ReferenceSnapshot,
    end_reference: ReferenceSnapshot,
    bar_count: int,
    event_date: str,
) -> TickerReplacementFact | None:
    if from_date != requested_from_date or to_date != requested_to_date:
        return None
    if not _event_ticker_is_absent_and_has_no_bars(
        client, request, old_ticker, requested_from_date, requested_to_date
    ):
        return None
    reason = _historical_bar_alias_current_end_reason(
        target,
        start_reference=start_reference,
        end_reference=end_reference,
        expected_ticker=new_ticker,
        bar_count=bar_count,
    )
    if not reason:
        return None
    end_fact = reference_boundary_fact_from_snapshot(
        request.series_id,
        target,
        end_reference,
        point="end",
        source="massive.ticker_replacement.historical_rekey.end_reference",
    )
    if end_fact.matched is not True:
        return None
    start_row = start_reference.to_payload()
    start_row["matched"] = True
    start_row["match_reason"] = reason
    start_row["point"] = "start"
    start_row["validation_override"] = {
        "reason": "provider_historical_figi_rekey_bar_alias_current_end",
        "bar_count": bar_count,
        "first_bar_date": from_date,
        "last_bar_date": to_date,
        "historical_composite": start_reference.composite_figi,
        "historical_share_class_figi": start_reference.share_class_figi,
        "target_composite_figi": target.composite_figi,
        "target_share_class_figi": target.share_class_figi,
        "target_cik": target.cik,
        "target_security_type": target.security_type,
        "ticker": new_ticker,
    }
    validation = (
        start_row,
        _segment_validation_row(end_fact.to_payload(), "end"),
    )
    metadata = {
        "ticker_replacement": {
            "new_ticker": new_ticker,
            "old_ticker": old_ticker,
            "replacement_reason": "known_alias_historical_figi_rekey_bar_alias_current_end",
            "new_start_reason": reason,
            "new_end_reason": end_fact.match_reason,
            "candidate_bar_span": {
                "bar_count": bar_count,
                "first_bar_date": from_date,
                "last_bar_date": to_date,
                "original_bar_count": 0,
                "original_first_bar_date": "",
                "original_last_bar_date": "",
            },
        },
        "start_alias_identity_bridge": {
            "bar_count": bar_count,
            "first_bar_date": from_date,
            "last_bar_date": to_date,
            "historical_reference": start_reference.to_payload(),
            "current_reference": end_reference.to_payload(),
            "current_reference_match_reason": end_fact.match_reason,
            "historical_reference_direct_match_reason": (
                f"composite_figi_mismatch detail={start_reference.composite_figi} target={target.composite_figi}"
                if start_reference.composite_figi and target.composite_figi
                else "historical reference did not directly match target"
            ),
            "match_reason": reason,
            "reason": "provider_historical_figi_rekey_bar_alias_current_end",
            "target_cik": target.cik,
            "target_composite_figi": target.composite_figi,
            "target_security_type": target.security_type,
            "target_share_class_figi": target.share_class_figi or "",
            "ticker": new_ticker,
        },
        "start_validation_override": {
            "matched": True,
            "match_reason": reason,
            "evidence": {
                "bar_count": bar_count,
                "first_bar_date": from_date,
                "last_bar_date": to_date,
                "historical_reference": start_reference.to_payload(),
                "current_reference": end_reference.to_payload(),
                "current_reference_match_reason": end_fact.match_reason,
                "match_reason": reason,
                "reason": "provider_historical_figi_rekey_bar_alias_current_end",
                "target_cik": target.cik,
                "target_composite_figi": target.composite_figi,
                "target_security_type": target.security_type,
                "target_share_class_figi": target.share_class_figi or "",
                "ticker": new_ticker,
            },
        },
    }
    return TickerReplacementFact(
        old_ticker=old_ticker,
        new_ticker=new_ticker,
        from_date=from_date,
        to_date=to_date,
        replacement_reason="known_alias_historical_figi_rekey_bar_alias_current_end",
        source="ticker_events+known_alias_historical_figi_rekey_bar_alias_current_end",
        event_date=event_date,
        validation=validation,
        metadata=metadata,
    )


def _event_ticker_is_absent_and_has_no_bars(
    client: MassiveReadOnlyClient,
    request: BackfillRequest,
    old_ticker: str,
    from_date: str,
    to_date: str,
) -> bool:
    start_payload = client.get(
        f"/v3/reference/tickers/{urllib.parse.quote(old_ticker, safe='')}",
        {"date": from_date},
    )
    end_payload = client.get(
        f"/v3/reference/tickers/{urllib.parse.quote(old_ticker, safe='')}",
        {"date": to_date},
    )
    bars_payload = _aggregate_bars_payload(
        client, request, old_ticker, from_date, to_date
    )
    start_reference = _reference_snapshot_from_payload(
        old_ticker, from_date, start_payload
    )
    end_reference = _reference_snapshot_from_payload(old_ticker, to_date, end_payload)
    bar_probe = _bar_probe_result_from_payload(
        old_ticker, from_date, to_date, bars_payload
    )
    return (
        start_reference.api_status == "NOT_FOUND"
        and end_reference.api_status == "NOT_FOUND"
        and bar_probe.api_status == "OK"
        and bar_probe.bar_count == 0
    )


def _historical_bar_alias_current_end_reason(
    target: TargetIdentity,
    *,
    start_reference: ReferenceSnapshot,
    end_reference: ReferenceSnapshot,
    expected_ticker: str,
    bar_count: int,
) -> str:
    if bar_count <= 0:
        return ""
    if start_reference.api_status != "OK" or end_reference.api_status != "OK":
        return ""
    if (
        start_reference.response_ticker != expected_ticker
        or end_reference.response_ticker != expected_ticker
    ):
        return ""
    if start_reference.response_ticker != end_reference.response_ticker:
        return ""
    start_name = _reference_name(start_reference)
    if not start_name or start_name != _reference_name(end_reference):
        return ""
    if (
        not start_reference.primary_exchange
        or start_reference.primary_exchange != end_reference.primary_exchange
    ):
        return ""
    if (
        not start_reference.security_type
        or start_reference.security_type != end_reference.security_type
    ):
        return ""
    if (
        target.security_type
        and start_reference.security_type.upper() != str(target.security_type).upper()
    ):
        return ""
    if start_reference.cik and target.cik and start_reference.cik != target.cik:
        return ""
    if (
        target.share_class_figi
        and start_reference.share_class_figi
        and start_reference.share_class_figi != target.share_class_figi
    ):
        return ""
    if (
        not target.composite_figi
        or end_reference.composite_figi != target.composite_figi
    ):
        return ""
    if (
        not start_reference.composite_figi
        or start_reference.composite_figi == target.composite_figi
    ):
        return ""
    end_fact = reference_boundary_fact_from_snapshot(
        target.ohlcv_series_id,
        target,
        end_reference,
        point="end",
        source="massive.historical_rekey.end_reference_check",
    )
    if end_fact.matched is not True:
        return ""
    return (
        "provider_historical_figi_rekey_bar_alias_current_end "
        f"first_bar={start_reference.as_of_date.isoformat()} "
        f"last_bar={end_reference.as_of_date.isoformat()} "
        f"historical_composite={start_reference.composite_figi} "
        f"target_composite={target.composite_figi}"
    )
