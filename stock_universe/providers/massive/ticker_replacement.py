"""Massive ticker-replacement evidence provider."""

from __future__ import annotations

import datetime as dt
import urllib.parse
from dataclasses import dataclass

from stock_universe.domain import (
    BackfillRequest,
    EvidenceFact,
    EvidenceRequest,
    ReferenceBoundaryFact,
    TargetIdentity,
    TickerReplacementFact,
)
from stock_universe.domain.records import unfreeze_json
from stock_universe.evidence.normalizers import (
    ticker_replacement_fact_from_target_valid_alias_window,
)
from stock_universe import security_names
from stock_universe.market_calendar import (
    first_us_equity_trading_date_on_or_after,
    next_us_equity_trading_date,
)
from stock_universe.providers.massive.client import MassiveReadOnlyClient
from stock_universe.providers.massive.common import (
    _aggregate_bars_payload,
    _bar_dates_from_payload,
    _historical_figi_rekey_bar_alias_replacement_fact,
    _missing_durable_start_replacement_fact,
    _omitted_fact_for_absent_ticker_interval,
    _reference_boundary_fact_with_historical_rekey,
    _reference_name,
    _reference_snapshot_from_payload,
    _segment_validation_row,
)


@dataclass
class MassiveTickerReplacementProvider:
    client: MassiveReadOnlyClient

    def initial_facts(
        self,
        request: BackfillRequest,
        target: TargetIdentity,
    ) -> tuple[EvidenceFact, ...]:
        return ()

    def requested_facts(
        self,
        request: BackfillRequest,
        target: TargetIdentity,
        evidence_requests: tuple[EvidenceRequest, ...],
    ) -> tuple[EvidenceFact, ...]:
        facts: list[EvidenceFact] = []
        for evidence_request in evidence_requests:
            if (
                evidence_request.kind != "ticker_replacement"
                or len(evidence_request.key) < 4
            ):
                continue
            _, old_ticker, from_date, to_date = evidence_request.key[:4]
            facts.extend(
                self._replacement_facts_for_window(
                    request, target, old_ticker, from_date, to_date
                )
            )
        return tuple(facts)

    def _replacement_facts_for_window(
        self,
        request: BackfillRequest,
        target: TargetIdentity,
        old_ticker: str,
        from_date: str,
        to_date: str,
    ) -> tuple[EvidenceFact, ...]:
        candidates = tuple(
            alias
            for alias in dict.fromkeys(
                target.known_alias_tickers
                + ((target.latest_ticker,) if target.latest_ticker else ())
                + ((old_ticker,) if old_ticker else ())
                + _component_symbol_candidates(old_ticker, target)
            )
            if alias
        )
        validated = []
        for candidate in candidates:
            start_payload = self.client.get(
                f"/v3/reference/tickers/{urllib.parse.quote(candidate, safe='')}",
                {"date": from_date},
            )
            end_payload = self.client.get(
                f"/v3/reference/tickers/{urllib.parse.quote(candidate, safe='')}",
                {"date": to_date},
            )
            fact = ticker_replacement_fact_from_target_valid_alias_window(
                request.series_id,
                target,
                old_ticker=old_ticker,
                new_ticker=candidate,
                from_date=from_date,
                to_date=to_date,
                start_reference=_reference_snapshot_from_payload(
                    candidate, from_date, start_payload
                ),
                end_reference=_reference_snapshot_from_payload(
                    candidate, to_date, end_payload
                ),
                event_date=from_date,
                source_prefix="ticker_events",
            )
            if fact is not None:
                validated.append(fact.to_evidence_fact(request.series_id))
                continue
            overridden = _target_valid_alias_window_with_overrides(
                request,
                target,
                old_ticker=old_ticker,
                new_ticker=candidate,
                from_date=from_date,
                to_date=to_date,
                start_reference=_reference_snapshot_from_payload(
                    candidate, from_date, start_payload
                ),
                end_reference=_reference_snapshot_from_payload(
                    candidate, to_date, end_payload
                ),
                replacement_reason="known_alias_boundary_validation_override",
                event_date=from_date,
            )
            if overridden is not None:
                validated.append(overridden)
                continue
            bar_backed = self._bar_backed_replacement_fact(
                request,
                target,
                old_ticker,
                candidate,
                from_date,
                to_date,
            )
            if bar_backed is not None:
                validated.append(bar_backed)
        preferred = _preferred_latest_issue_fact(
            target, tuple(validated), from_date, to_date
        )
        if preferred is not None:
            return (preferred,)
        if len(validated) > 1:
            split = self._event_ticker_then_current_alias_split(
                request,
                target,
                old_ticker=old_ticker,
                from_date=from_date,
                to_date=to_date,
                validated=tuple(validated),
            )
            if split:
                return split
        preferred = _preferred_complete_latest_fact(
            target, tuple(validated), from_date, to_date
        )
        if preferred is not None:
            return (preferred,)
        if len(validated) != 1:
            omitted = self._omitted_fact_for_absent_event_ticker(
                request, target, old_ticker, from_date, to_date
            )
            return (omitted,) if omitted is not None else ()
        return (validated[0],)

    def _event_ticker_then_current_alias_split(
        self,
        request: BackfillRequest,
        target: TargetIdentity,
        *,
        old_ticker: str,
        from_date: str,
        to_date: str,
        validated: tuple[EvidenceFact, ...],
    ) -> tuple[EvidenceFact, ...]:
        latest = target.latest_ticker or ""
        if not latest or latest == old_ticker:
            return ()
        payloads = [(fact, fact.payload_value()) for fact in validated]
        old_windows = [
            (fact, payload)
            for fact, payload in payloads
            if payload.get("new_ticker") == old_ticker
            and payload.get("from_date")
            and payload.get("to_date")
        ]
        current_windows = [
            payload
            for _, payload in payloads
            if payload.get("new_ticker") == latest
            and payload.get("from_date")
            and payload.get("to_date")
        ]
        if len(old_windows) != 1 or len(current_windows) != 1:
            return ()
        old_fact, old_payload = old_windows[0]
        old_end = dt.date.fromisoformat(str(old_payload["to_date"]))
        requested_to = dt.date.fromisoformat(to_date)
        if old_end >= requested_to:
            return ()
        current_payload = current_windows[0]
        current_to = dt.date.fromisoformat(str(current_payload["to_date"]))
        if current_to < requested_to:
            return ()
        trim_start = dt.date.fromisoformat(next_us_equity_trading_date(old_end))
        current_start = dt.date.fromisoformat(str(current_payload["from_date"]))
        if current_start <= old_end:
            list_date = self._current_reference_list_date(
                latest, request.to_date.isoformat()
            )
            if list_date is None:
                return ()
            list_session = dt.date.fromisoformat(
                first_us_equity_trading_date_on_or_after(list_date)
            )
            if list_session != trim_start:
                return ()
            current_start = list_session
        if current_start > requested_to:
            return ()
        start = current_start.isoformat()
        end = requested_to.isoformat()
        start_payload = self.client.get(
            f"/v3/reference/tickers/{urllib.parse.quote(latest, safe='')}",
            {"date": start},
        )
        end_payload = self.client.get(
            f"/v3/reference/tickers/{urllib.parse.quote(latest, safe='')}",
            {"date": end},
        )
        current_fact = ticker_replacement_fact_from_target_valid_alias_window(
            request.series_id,
            target,
            old_ticker=old_ticker,
            new_ticker=latest,
            from_date=start,
            to_date=end,
            start_reference=_reference_snapshot_from_payload(
                latest, start, start_payload
            ),
            end_reference=_reference_snapshot_from_payload(latest, end, end_payload),
            replacement_reason="event_ticker_then_current_alias_split",
            event_date=from_date,
            source_prefix="ticker_events",
        )
        if current_fact is None:
            return ()
        return (old_fact, current_fact.to_evidence_fact(request.series_id))

    def _current_reference_list_date(
        self, ticker: str, as_of_date: str
    ) -> dt.date | None:
        payload = self.client.get(
            f"/v3/reference/tickers/{urllib.parse.quote(ticker, safe='')}",
            {"date": as_of_date},
        )
        snapshot = _reference_snapshot_from_payload(ticker, as_of_date, payload)
        if snapshot.api_status != "OK" or snapshot.response_ticker != ticker:
            return None
        raw = unfreeze_json(snapshot.raw)
        if not isinstance(raw, dict):
            raw = {}
        list_date = str(raw.get("list_date") or "")
        if not list_date:
            return None
        try:
            return dt.date.fromisoformat(list_date)
        except ValueError:
            return None

    def _bar_backed_replacement_fact(
        self,
        request: BackfillRequest,
        target: TargetIdentity,
        old_ticker: str,
        candidate: str,
        from_date: str,
        to_date: str,
    ) -> EvidenceFact | None:
        bars_payload = _aggregate_bars_payload(
            self.client, request, candidate, from_date, to_date
        )
        dates = _bar_dates_from_payload(bars_payload)
        if not dates:
            return None
        first_date = dates[0]
        last_date = dates[-1]
        start_payload = self.client.get(
            f"/v3/reference/tickers/{urllib.parse.quote(candidate, safe='')}",
            {"date": first_date},
        )
        end_payload = self.client.get(
            f"/v3/reference/tickers/{urllib.parse.quote(candidate, safe='')}",
            {"date": last_date},
        )
        start_reference = _reference_snapshot_from_payload(
            candidate, first_date, start_payload
        )
        end_reference = _reference_snapshot_from_payload(
            candidate, last_date, end_payload
        )
        fact = ticker_replacement_fact_from_target_valid_alias_window(
            request.series_id,
            target,
            old_ticker=old_ticker,
            new_ticker=candidate,
            from_date=first_date,
            to_date=last_date,
            start_reference=start_reference,
            end_reference=end_reference,
            replacement_reason="known_alias_target_valid_bar_window_inside_invalid_event_segment",
            event_date=from_date,
            source_prefix="ticker_events",
        )
        if fact:
            return fact.to_evidence_fact(request.series_id)
        overridden = _target_valid_alias_window_with_overrides(
            request,
            target,
            old_ticker=old_ticker,
            new_ticker=candidate,
            from_date=first_date,
            to_date=last_date,
            start_reference=start_reference,
            end_reference=end_reference,
            replacement_reason="known_alias_bar_window_boundary_validation_override",
            event_date=from_date,
        )
        if overridden is not None:
            return overridden
        missing_start = _missing_durable_start_replacement_fact(
            request.series_id,
            target,
            old_ticker=old_ticker,
            new_ticker=candidate,
            from_date=first_date,
            to_date=last_date,
            start_reference=start_reference,
            end_reference=end_reference,
            event_date=from_date,
        )
        if missing_start:
            return missing_start.to_evidence_fact(request.series_id)
        historical_rekey = _historical_figi_rekey_bar_alias_replacement_fact(
            self.client,
            request,
            target,
            old_ticker=old_ticker,
            new_ticker=candidate,
            from_date=first_date,
            to_date=last_date,
            requested_from_date=from_date,
            requested_to_date=to_date,
            start_reference=start_reference,
            end_reference=end_reference,
            bar_count=len(dates),
            event_date=from_date,
        )
        return (
            historical_rekey.to_evidence_fact(request.series_id)
            if historical_rekey
            else None
        )

    def _omitted_fact_for_absent_event_ticker(
        self,
        request: BackfillRequest,
        target: TargetIdentity,
        ticker: str,
        from_date: str,
        to_date: str,
    ) -> EvidenceFact | None:
        return _omitted_fact_for_absent_ticker_interval(
            self.client, request, target, ticker, from_date, to_date
        )


def _component_symbol_candidates(
    old_ticker: str, target: TargetIdentity
) -> tuple[str, ...]:
    security_type = (target.security_type or "").upper()
    if security_type not in {"WARRANT", "UNIT", "RIGHT"}:
        return ()
    old = (old_ticker or "").strip().upper()
    latest = (target.latest_ticker or "").strip().upper()
    if not old or not latest:
        return ()
    suffixes: list[str] = []
    if "." in latest:
        suffix = latest[latest.find(".") :]
        if suffix in {".WS", ".WT", ".W", ".U", ".R", ".RT"}:
            suffixes.append(suffix)
    expected_suffix = {"WARRANT": "W", "UNIT": "U", "RIGHT": "R"}[security_type]
    if latest.endswith(expected_suffix):
        suffixes.append(expected_suffix)
    candidates: list[str] = []
    for suffix in dict.fromkeys(suffixes):
        if old.endswith(suffix):
            continue
        candidates.append(f"{old}{suffix}")
    return tuple(candidates)


def _target_valid_alias_window_with_overrides(
    request: BackfillRequest,
    target: TargetIdentity,
    *,
    old_ticker: str,
    new_ticker: str,
    from_date: str,
    to_date: str,
    start_reference: Any,
    end_reference: Any,
    replacement_reason: str,
    event_date: str,
) -> EvidenceFact | None:
    start_fact = _target_valid_boundary_with_overrides(
        request,
        target,
        new_ticker,
        start_reference,
        point="start",
    )
    end_fact = _target_valid_boundary_with_overrides(
        request,
        target,
        new_ticker,
        end_reference,
        point="end",
    )
    if start_fact.matched is not True or end_fact.matched is not True:
        return None
    reason = replacement_reason
    validation = (
        _segment_validation_row(start_fact.to_legacy_dict(), "start"),
        _segment_validation_row(end_fact.to_legacy_dict(), "end"),
    )
    fact = TickerReplacementFact(
        old_ticker=old_ticker,
        new_ticker=new_ticker,
        from_date=from_date,
        to_date=to_date,
        replacement_reason=reason,
        source=f"ticker_events+{reason}",
        event_date=event_date,
        validation=validation,
        metadata={
            "ticker_replacement": {
                "new_ticker": new_ticker,
                "old_ticker": old_ticker,
                "replacement_reason": reason,
                "new_start_reason": start_fact.match_reason,
                "new_end_reason": end_fact.match_reason,
            }
        },
    )
    return fact.to_evidence_fact(request.series_id)


def _target_valid_boundary_with_overrides(
    request: BackfillRequest,
    target: TargetIdentity,
    new_ticker: str,
    snapshot: Any,
    *,
    point: str,
) -> ReferenceBoundaryFact:
    fact = _reference_boundary_fact_with_historical_rekey(
        request.series_id,
        target,
        snapshot,
        point=point,
        source="massive.ticker_replacement.boundary_validation_override",
    )
    if fact.matched is True:
        return fact
    if not _provisional_named_preferred_issue_rebrand(target, snapshot, new_ticker):
        return fact
    payload = snapshot.to_payload()
    payload["matched"] = True
    payload["match_reason"] = "provisional_named_preferred_issue_rebrand"
    payload["point"] = point
    payload["validation_override"] = {
        "reason": "provisional_named_preferred_issue_rebrand",
        "target_name": target.company_name or target.current_company_name,
        "reference_name": _reference_name(snapshot),
        "ticker": snapshot.response_ticker,
    }
    return ReferenceBoundaryFact(
        ticker=snapshot.ticker,
        as_of_date=snapshot.as_of_date,
        api_status=snapshot.api_status,
        matched=True,
        match_reason="provisional_named_preferred_issue_rebrand",
        payload=payload,
        source="massive.ticker_replacement.provisional_named_issue_rebrand",
    )


def _provisional_named_preferred_issue_rebrand(
    target: TargetIdentity, snapshot: Any, new_ticker: str
) -> bool:
    if snapshot.api_status != "OK" or snapshot.response_ticker != new_ticker:
        return False
    if target.identity_status != "provisional":
        return False
    if target.composite_figi or target.share_class_figi:
        return False
    target_exchange = str(target.latest_primary_exchange or "").upper()
    snapshot_exchange = str(snapshot.primary_exchange or "").upper()
    if target_exchange and snapshot_exchange and target_exchange != snapshot_exchange:
        return False
    target_type = str(target.security_type or "").upper()
    snapshot_type = str(snapshot.security_type or "").upper()
    if target_type and snapshot_type and target_type != snapshot_type:
        return False
    target_name = target.company_name or target.current_company_name
    snapshot_name = _reference_name(snapshot)
    return _same_preferred_issue_terms(target_name, snapshot_name)


def _same_preferred_issue_terms(left: str, right: str) -> bool:
    return security_names.preferred_issue_terms_compatible(left, right)


def _preferred_latest_issue_fact(
    target: TargetIdentity,
    facts: tuple[EvidenceFact, ...],
    from_date: str,
    to_date: str,
) -> EvidenceFact | None:
    latest = str(target.latest_ticker or "")
    if not latest or len(facts) <= 1:
        return None
    latest_facts = [
        fact
        for fact in facts
        if fact.payload_value().get("new_ticker") == latest
        and fact.payload_value().get("from_date") == from_date
        and fact.payload_value().get("to_date") == to_date
    ]
    if len(latest_facts) != 1:
        return None
    target_name = target.company_name or target.current_company_name
    if not _distinct_issue_name(target_name):
        return None
    for fact in facts:
        payload = fact.payload_value()
        if payload.get("new_ticker") == latest:
            continue
        if _replacement_validation_names_compatible(target_name, payload):
            return None
    return latest_facts[0]


def _preferred_complete_latest_fact(
    target: TargetIdentity,
    facts: tuple[EvidenceFact, ...],
    from_date: str,
    to_date: str,
) -> EvidenceFact | None:
    latest = str(target.latest_ticker or "")
    if not latest or len(facts) <= 1:
        return None
    latest_full = [
        fact
        for fact in facts
        if fact.payload_value().get("new_ticker") == latest
        and fact.payload_value().get("from_date") == from_date
        and fact.payload_value().get("to_date") == to_date
    ]
    if len(latest_full) != 1:
        return None
    non_latest_full = [
        fact
        for fact in facts
        if fact.payload_value().get("new_ticker") != latest
        and fact.payload_value().get("from_date") == from_date
        and fact.payload_value().get("to_date") == to_date
    ]
    if non_latest_full:
        return None
    return latest_full[0]


def _replacement_validation_names_compatible(
    target_name: str, payload: dict[str, Any]
) -> bool:
    for row in payload.get("validation") or ():
        raw = row.get("raw") if isinstance(row, dict) else None
        if isinstance(raw, dict) and _distinct_issue_terms_match(
            target_name, str(raw.get("name") or "")
        ):
            return True
    return False


def _distinct_issue_name(name: str) -> bool:
    tokens = set(_name_tokens(name))
    return bool(
        tokens
        & {
            "note",
            "notes",
            "preferred",
            "depositary",
            "debenture",
            "warrant",
            "unit",
            "right",
        }
    )


def _distinct_issue_terms_match(left: str, right: str) -> bool:
    return security_names.distinct_issue_terms_match(left, right)


def _name_tokens(value: str) -> tuple[str, ...]:
    return security_names.name_tokens(value)
