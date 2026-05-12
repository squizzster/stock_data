"""Massive alias-history evidence provider."""

from __future__ import annotations

import datetime as dt
import urllib.parse
from dataclasses import dataclass
from typing import Any

from stock_universe.domain import (
    AliasHistoryFact,
    BackfillRequest,
    EvidenceFact,
    EvidenceRequest,
    ReferenceBoundaryFact,
    TargetIdentity,
)
from stock_universe.domain.records import unfreeze_json
from stock_universe.evidence.normalizers import reference_boundary_fact_from_snapshot
from stock_universe.market_calendar import previous_us_equity_trading_date
from stock_universe.providers.massive.client import MassiveReadOnlyClient
from stock_universe.providers.massive.common import (
    _aggregate_bars_payload,
    _bar_dates_from_payload,
    _first_matching_suffix_boundary_fact,
    _reference_boundary_fact_with_historical_rekey,
    _reference_snapshot_from_payload,
    _retag_reference_boundary_fact,
    _segment_validation_row,
)


@dataclass
class MassiveAliasHistoryProvider:
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
                evidence_request.kind != "alias_history"
                or len(evidence_request.key) < 3
            ):
                continue
            _, from_date, event_date = evidence_request.key[:3]
            event_ticker = (
                str(evidence_request.key[3]) if len(evidence_request.key) >= 4 else ""
            )
            facts.extend(
                self._alias_history_facts_for_gap(
                    request, target, from_date, event_date, event_ticker=event_ticker
                )
            )
        return tuple(facts)

    def _alias_history_facts_for_gap(
        self,
        request: BackfillRequest,
        target: TargetIdentity,
        from_date: str,
        event_date: str,
        *,
        event_ticker: str = "",
    ) -> tuple[EvidenceFact, ...]:
        gap_end = dt.date.fromisoformat(previous_us_equity_trading_date(event_date))
        if gap_end < dt.date.fromisoformat(from_date):
            return ()

        candidates = [
            ticker
            for ticker in dict.fromkeys(
                ((event_ticker,) if event_ticker else ())
                + target.known_alias_tickers
                + ((target.latest_ticker,) if target.latest_ticker else ())
            )
            if ticker
        ]
        accepted: list[dict[str, Any]] = []
        boundary_facts: list[EvidenceFact] = []
        no_bar_accepted: list[dict[str, Any]] = []
        no_bar_boundary_facts: list[EvidenceFact] = []
        for ticker in candidates:
            bars_payload = _aggregate_bars_payload(
                self.client, request, ticker, from_date, gap_end.isoformat()
            )
            dates = _bar_dates_from_payload(bars_payload)
            if not dates:
                no_bar_span = self._no_bar_reference_backed_alias_span(
                    request,
                    target,
                    ticker,
                    from_date,
                    gap_end.isoformat(),
                    event_date,
                )
                if no_bar_span is not None:
                    span, start_fact, end_fact = no_bar_span
                    no_bar_accepted.append(span)
                    no_bar_boundary_facts.extend(
                        (
                            start_fact.to_evidence_fact(request.series_id),
                            end_fact.to_evidence_fact(request.series_id),
                        )
                    )
                continue
            first_date = dates[0]
            last_date = dates[-1]
            start_payload = self.client.get(
                f"/v3/reference/tickers/{urllib.parse.quote(ticker, safe='')}",
                {"date": first_date},
            )
            end_payload = self.client.get(
                f"/v3/reference/tickers/{urllib.parse.quote(ticker, safe='')}",
                {"date": last_date},
            )
            start_fact = _reference_boundary_fact_with_historical_rekey(
                request.series_id,
                target,
                _reference_snapshot_from_payload(ticker, first_date, start_payload),
                point="start",
                source="massive.alias_history.reference_boundary",
            )
            end_fact = _reference_boundary_fact_with_historical_rekey(
                request.series_id,
                target,
                _reference_snapshot_from_payload(ticker, last_date, end_payload),
                point="end",
                source="massive.alias_history.reference_boundary",
            )
            source = "massive.known_alias_pre_event_bar_validation"
            if start_fact.matched is True and end_fact.matched is not True:
                prefix = self._target_valid_bar_prefix_end(
                    request, target, ticker, dates
                )
                if prefix is None:
                    continue
                last_date, end_fact = prefix
                source = "massive.known_alias_target_valid_bar_window"
            elif start_fact.matched is not True and end_fact.matched is True:
                suffix = self._target_valid_bar_suffix_start(
                    request, target, ticker, dates, right_anchor_fact=end_fact
                )
                if suffix is None:
                    continue
                first_date, start_fact = suffix
                source = "massive.known_alias_first_target_valid_bar_window"
            elif start_fact.matched is not True or end_fact.matched is not True:
                list_date_window = self._current_list_date_bar_window(
                    request,
                    target,
                    ticker,
                    dates,
                    event_date,
                )
                if list_date_window is not None:
                    first_date, last_date, start_fact, end_fact = list_date_window
                    source = "massive.current_reference_list_date_bar_window"
                else:
                    successor_window = self._same_ticker_successor_bar_window(
                        request,
                        target,
                        ticker,
                        dates,
                        event_date,
                        start_fact=start_fact,
                        end_fact=end_fact,
                    )
                    if successor_window is not None:
                        first_date, last_date, start_fact, end_fact = successor_window
                        source = "massive.same_ticker_successor_bar_window"
                    else:
                        missing_durable_window = (
                            self._same_ticker_missing_durable_bar_window(
                                request,
                                target,
                                ticker,
                                dates,
                                event_date,
                                start_fact=start_fact,
                                end_fact=end_fact,
                            )
                        )
                        if missing_durable_window is not None:
                            first_date, last_date, start_fact, end_fact = (
                                missing_durable_window
                            )
                            source = "massive.same_ticker_missing_durable_bar_window"
                        else:
                            continue
            accepted.append(
                {
                    "event_date": event_date,
                    "from_date": first_date,
                    "segment_index": len(accepted) + 1,
                    "source": source,
                    "ticker": ticker,
                    "to_date": last_date,
                    "valid": True,
                    "validation": [
                        _segment_validation_row(start_fact.to_legacy_dict(), "start"),
                        _segment_validation_row(end_fact.to_legacy_dict(), "end"),
                    ],
                }
            )
            boundary_facts.extend(
                (
                    start_fact.to_evidence_fact(request.series_id),
                    end_fact.to_evidence_fact(request.series_id),
                )
            )

        if not accepted and no_bar_accepted:
            accepted = no_bar_accepted
            boundary_facts = no_bar_boundary_facts
        accepted.sort(
            key=lambda item: (item["from_date"], item["to_date"], item["ticker"])
        )
        previous: dict[str, Any] | None = None
        for index, item in enumerate(accepted, 1):
            if previous is not None and item["from_date"] <= previous["to_date"]:
                return ()
            item["segment_index"] = index
            previous = item
        if not accepted:
            return ()
        alias_fact = AliasHistoryFact(
            accepted, source="massive.alias_history"
        ).to_evidence_fact(request.series_id)
        return (alias_fact, *boundary_facts)

    def _target_valid_bar_prefix_end(
        self,
        request: BackfillRequest,
        target: TargetIdentity,
        ticker: str,
        bar_dates: tuple[str, ...],
    ) -> tuple[str, Any] | None:
        low = 0
        high = len(bar_dates) - 1
        best: tuple[str, Any] | None = None
        while low <= high:
            mid = (low + high) // 2
            date = bar_dates[mid]
            payload = self.client.get(
                f"/v3/reference/tickers/{urllib.parse.quote(ticker, safe='')}",
                {"date": date},
            )
            fact = reference_boundary_fact_from_snapshot(
                request.series_id,
                target,
                _reference_snapshot_from_payload(ticker, date, payload),
                point="end",
                source="massive.alias_history.target_valid_prefix_boundary",
            )
            if fact.matched is True:
                best = (date, fact)
                low = mid + 1
            else:
                high = mid - 1
        return best

    def _target_valid_bar_suffix_start(
        self,
        request: BackfillRequest,
        target: TargetIdentity,
        ticker: str,
        bar_dates: tuple[str, ...],
        *,
        right_anchor_fact: Any | None = None,
    ) -> tuple[str, Any] | None:
        source = "massive.alias_history.first_target_valid_bar_boundary"
        seed_facts = {}
        if (
            bar_dates
            and right_anchor_fact is not None
            and right_anchor_fact.matched is True
        ):
            seed_facts[bar_dates[-1]] = _retag_reference_boundary_fact(
                right_anchor_fact,
                point="start",
                source=source,
            )
        fact = _first_matching_suffix_boundary_fact(
            self.client,
            request,
            target,
            ticker,
            bar_dates,
            point="start",
            source=source,
            allow_historical_rekey=True,
            seed_facts=seed_facts,
        )
        return (fact.as_of_date.isoformat(), fact) if fact is not None else None

    def _current_list_date_bar_window(
        self,
        request: BackfillRequest,
        target: TargetIdentity,
        ticker: str,
        bar_dates: tuple[str, ...],
        event_date: str,
    ) -> tuple[str, str, ReferenceBoundaryFact, ReferenceBoundaryFact] | None:
        if ticker != target.latest_ticker or not bar_dates:
            return None
        payload = self.client.get(
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
            source="massive.alias_history.current_reference_list_date",
        )
        if current_fact.matched is not True:
            return None
        raw = unfreeze_json(current.raw)
        if not isinstance(raw, dict):
            return None
        list_date = str(raw.get("list_date") or "")
        if not list_date:
            return None
        event_day = dt.date.fromisoformat(event_date)
        list_day = dt.date.fromisoformat(list_date)
        if list_day < request.from_date or list_day >= event_day:
            return None
        eligible_dates = tuple(
            date for date in bar_dates if list_date <= date < event_date
        )
        if not eligible_dates:
            return None
        if (event_day - list_day).days > 7 and eligible_dates[0] != list_date:
            return None
        first_date = eligible_dates[0]
        last_date = eligible_dates[-1]
        reason = "current_reference_list_date_same_ticker_bar_window"
        return (
            first_date,
            last_date,
            self._list_date_boundary_fact(
                current_fact, first_date, point="start", reason=reason
            ),
            self._list_date_boundary_fact(
                current_fact, last_date, point="end", reason=reason
            ),
        )

    def _list_date_boundary_fact(
        self,
        current_fact: ReferenceBoundaryFact,
        as_of_date: str,
        *,
        point: str,
        reason: str,
    ) -> ReferenceBoundaryFact:
        payload = dict(unfreeze_json(current_fact.payload))
        payload["point"] = point
        payload["date"] = as_of_date
        payload["current_reference_as_of_date"] = current_fact.as_of_date.isoformat()
        payload["validation_override"] = {
            "reason": reason,
            "current_reference_as_of_date": current_fact.as_of_date.isoformat(),
            "current_reference_match_reason": current_fact.match_reason,
            "list_date": str((payload.get("raw") or {}).get("list_date") or ""),
        }
        return ReferenceBoundaryFact(
            ticker=current_fact.ticker,
            as_of_date=as_of_date,
            api_status=current_fact.api_status,
            matched=True,
            match_reason=reason,
            payload=payload,
            source="massive.alias_history.current_reference_list_date_boundary",
        )

    def _same_ticker_successor_bar_window(
        self,
        request: BackfillRequest,
        target: TargetIdentity,
        ticker: str,
        bar_dates: tuple[str, ...],
        event_date: str,
        *,
        start_fact: ReferenceBoundaryFact,
        end_fact: ReferenceBoundaryFact,
    ) -> tuple[str, str, ReferenceBoundaryFact, ReferenceBoundaryFact] | None:
        if ticker != target.latest_ticker or not bar_dates:
            return None
        first_date = bar_dates[0]
        last_date = bar_dates[-1]
        event_day = dt.date.fromisoformat(event_date)
        last_day = dt.date.fromisoformat(last_date)
        if last_day >= event_day or (event_day - last_day).days > 10:
            return None
        if not self._same_ticker_successor_reference(target, ticker, start_fact):
            return None
        if not self._same_ticker_successor_reference(target, ticker, end_fact):
            return None
        event_payload = self.client.get(
            f"/v3/reference/tickers/{urllib.parse.quote(ticker, safe='')}",
            {"date": event_date},
        )
        event_fact = _reference_boundary_fact_with_historical_rekey(
            request.series_id,
            target,
            _reference_snapshot_from_payload(ticker, event_date, event_payload),
            point="event",
            source="massive.alias_history.same_ticker_successor_event_boundary",
        )
        if event_fact.matched is not True:
            return None
        reason = "same_ticker_successor_cik_rollover_bar_window"
        return (
            first_date,
            last_date,
            self._successor_boundary_fact(
                start_fact, point="start", reason=reason, event_fact=event_fact
            ),
            self._successor_boundary_fact(
                end_fact, point="end", reason=reason, event_fact=event_fact
            ),
        )

    def _same_ticker_missing_durable_bar_window(
        self,
        request: BackfillRequest,
        target: TargetIdentity,
        ticker: str,
        bar_dates: tuple[str, ...],
        event_date: str,
        *,
        start_fact: ReferenceBoundaryFact,
        end_fact: ReferenceBoundaryFact,
    ) -> tuple[str, str, ReferenceBoundaryFact, ReferenceBoundaryFact] | None:
        if ticker != target.latest_ticker or not bar_dates:
            return None
        first_date = bar_dates[0]
        last_date = bar_dates[-1]
        event_day = dt.date.fromisoformat(event_date)
        last_day = dt.date.fromisoformat(last_date)
        if last_day >= event_day or (event_day - last_day).days > 10:
            return None
        if not self._same_ticker_missing_durable_reference(target, ticker, start_fact):
            return None
        if not self._same_ticker_missing_durable_reference(target, ticker, end_fact):
            return None
        current_payload = self.client.get(
            f"/v3/reference/tickers/{urllib.parse.quote(ticker, safe='')}",
            {"date": request.to_date.isoformat()},
        )
        current_fact = _reference_boundary_fact_with_historical_rekey(
            request.series_id,
            target,
            _reference_snapshot_from_payload(
                ticker, request.to_date.isoformat(), current_payload
            ),
            point="current",
            source="massive.alias_history.same_ticker_missing_durable_current_boundary",
        )
        if current_fact.matched is not True:
            return None
        reason = "same_ticker_missing_durable_ids_current_reference_bridge"
        return (
            first_date,
            last_date,
            self._successor_boundary_fact(
                start_fact, point="start", reason=reason, event_fact=current_fact
            ),
            self._successor_boundary_fact(
                end_fact, point="end", reason=reason, event_fact=current_fact
            ),
        )

    def _same_ticker_missing_durable_reference(
        self,
        target: TargetIdentity,
        ticker: str,
        fact: ReferenceBoundaryFact,
    ) -> bool:
        if fact.api_status != "OK":
            return False
        payload = dict(unfreeze_json(fact.payload))
        if str(payload.get("response_ticker") or payload.get("ticker") or "") != ticker:
            return False
        if (
            payload.get("composite_figi")
            or payload.get("share_class_figi")
            or payload.get("cik")
        ):
            return False
        target_type = str(target.security_type or "").upper()
        payload_type = str(payload.get("type") or "").upper()
        if target_type and payload_type and target_type != payload_type:
            return False
        return _meaningful_name_overlap(
            target.company_name or target.current_company_name, _payload_name(payload)
        )

    def _same_ticker_successor_reference(
        self,
        target: TargetIdentity,
        ticker: str,
        fact: ReferenceBoundaryFact,
    ) -> bool:
        if fact.api_status != "OK":
            return False
        payload = dict(unfreeze_json(fact.payload))
        if str(payload.get("response_ticker") or payload.get("ticker") or "") != ticker:
            return False
        if (
            target.composite_figi
            and payload.get("composite_figi")
            and payload.get("composite_figi") != target.composite_figi
        ):
            return False
        if (
            target.share_class_figi
            and payload.get("share_class_figi")
            and payload.get("share_class_figi") != target.share_class_figi
        ):
            return False
        target_type = str(target.security_type or "").upper()
        payload_type = str(payload.get("type") or "").upper()
        if target_type and payload_type and target_type != payload_type:
            return False
        target_exchange = str(target.latest_primary_exchange or "").upper()
        payload_exchange = str(payload.get("primary_exchange") or "").upper()
        if target_exchange and payload_exchange and target_exchange != payload_exchange:
            return False
        return _meaningful_name_overlap(
            target.company_name or target.current_company_name, _payload_name(payload)
        )

    def _successor_boundary_fact(
        self,
        base_fact: ReferenceBoundaryFact,
        *,
        point: str,
        reason: str,
        event_fact: ReferenceBoundaryFact,
    ) -> ReferenceBoundaryFact:
        payload = dict(unfreeze_json(base_fact.payload))
        payload["point"] = point
        payload["matched"] = True
        payload["match_reason"] = reason
        payload["validation_override"] = {
            "reason": reason,
            "event_reference_as_of_date": event_fact.as_of_date.isoformat(),
            "event_reference_match_reason": event_fact.match_reason,
            "event_reference_ticker": event_fact.ticker,
        }
        return ReferenceBoundaryFact(
            ticker=base_fact.ticker,
            as_of_date=base_fact.as_of_date,
            api_status=base_fact.api_status,
            matched=True,
            match_reason=reason,
            payload=payload,
            source="massive.alias_history.same_ticker_successor_boundary",
        )

    def _no_bar_reference_backed_alias_span(
        self,
        request: BackfillRequest,
        target: TargetIdentity,
        ticker: str,
        from_date: str,
        to_date: str,
        event_date: str,
    ) -> tuple[dict[str, Any], Any, Any] | None:
        start_payload = self.client.get(
            f"/v3/reference/tickers/{urllib.parse.quote(ticker, safe='')}",
            {"date": from_date},
        )
        end_payload = self.client.get(
            f"/v3/reference/tickers/{urllib.parse.quote(ticker, safe='')}",
            {"date": to_date},
        )
        start_fact = _reference_boundary_fact_with_historical_rekey(
            request.series_id,
            target,
            _reference_snapshot_from_payload(ticker, from_date, start_payload),
            point="start",
            source="massive.alias_history.no_bar_reference_boundary",
        )
        end_fact = _reference_boundary_fact_with_historical_rekey(
            request.series_id,
            target,
            _reference_snapshot_from_payload(ticker, to_date, end_payload),
            point="end",
            source="massive.alias_history.no_bar_reference_boundary",
        )
        if start_fact.matched is not True or end_fact.matched is not True:
            return None
        span = {
            "event_date": event_date,
            "from_date": from_date,
            "segment_index": 1,
            "source": "massive.known_alias_reference_no_bar_window",
            "ticker": ticker,
            "to_date": to_date,
            "valid": True,
            "validation": [
                _segment_validation_row(start_fact.to_legacy_dict(), "start"),
                _segment_validation_row(end_fact.to_legacy_dict(), "end"),
            ],
        }
        return span, start_fact, end_fact


def _payload_name(payload: dict[str, Any]) -> str:
    raw = payload.get("raw")
    if isinstance(raw, dict):
        return str(raw.get("name") or "")
    return ""


def _meaningful_name_overlap(left: str, right: str) -> bool:
    ignored = {
        "a",
        "class",
        "common",
        "corp",
        "corporation",
        "inc",
        "limited",
        "nv",
        "ordinary",
        "plc",
        "share",
        "shares",
        "stock",
        "the",
    }
    left_tokens = {
        token
        for token in _name_tokens(left)
        if token not in ignored and len(token) >= 3
    }
    right_tokens = {
        token
        for token in _name_tokens(right)
        if token not in ignored and len(token) >= 3
    }
    if not left_tokens or not right_tokens:
        return False
    if left_tokens.intersection(right_tokens):
        return True
    return any(
        len(left_token) >= 4
        and len(right_token) >= 4
        and (left_token in right_token or right_token in left_token)
        for left_token in left_tokens
        for right_token in right_tokens
    )


def _name_tokens(value: str) -> tuple[str, ...]:
    normalized = "".join(
        char.lower() if char.isalnum() else " " for char in str(value or "")
    )
    return tuple(normalized.split())
