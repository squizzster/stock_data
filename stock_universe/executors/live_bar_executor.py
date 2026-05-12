"""Approved-plan live bar executor."""

from __future__ import annotations

import datetime as dt
import urllib.parse
from dataclasses import dataclass, replace
from typing import Any
from zoneinfo import ZoneInfo

from stock_universe.bar_quality import (
    DAILY_BAR_STRUCTURAL_VALIDATION,
    DAILY_CLOSE_OUTSIDE_INTRADAY_ENVELOPE,
    DAILY_HIGH_EXCEEDS_INTRADAY_ENVELOPE,
    DAILY_LOW_BELOW_INTRADAY_ENVELOPE,
    DAILY_OPEN_OUTSIDE_INTRADAY_ENVELOPE,
    INTRADAY_EVIDENCE_INCOMPLETE,
    SUSPECT,
    VALIDATED,
    VALIDATED_REPAIRED,
    IntradayEnvelope,
    OhlcvValues,
    component_outside_envelope,
    high_exceeds_envelope,
    intraday_envelope_from_payload,
    low_below_envelope,
    status_for_structural_issues,
    structural_issues,
    suspicion_reasons,
)
from stock_universe.domain import BackfillPlan, PlannedSegment
from stock_universe.executors.backfill_executor import (
    ExecutionApproval,
    ExecutionContractError,
    validate_approved_plan,
)
from stock_universe.market_calendar import (
    DEFAULT_US_EQUITY_CALENDAR_ID,
    first_us_equity_trading_date_on_or_after,
    next_us_equity_trading_date,
    previous_us_equity_trading_date,
    us_equity_session_for_utc_ts,
)
from stock_universe.providers.live import MassiveReadOnlyClient
from stock_universe.storage import SQLiteStockUniverseRepository, StoredOhlcvBar


PROVIDER_ENTITLEMENT_SKIP_REASON = "provider_not_authorized"
PROVIDER_ENTITLEMENT_STATUSES = {"NOT_AUTHORIZED"}
US_EASTERN = ZoneInfo("America/New_York")


class ProviderEntitlementUnavailable(RuntimeError):
    def __init__(self, *, ticker: str, provider_status: str) -> None:
        self.ticker = ticker
        self.provider_status = provider_status
        super().__init__(
            f"bar fetch skipped for {ticker}: provider status {provider_status}"
        )


@dataclass(frozen=True)
class LiveExecutionReceipt:
    ok: bool
    request_hash: str
    evidence_ledger_hash: str
    ohlcv_series_id: int
    planned_segment_count: int
    fetched_bar_count: int
    inserted_bar_count: int
    started_at_utc: str
    finished_at_utc: str
    request_log: tuple[dict[str, Any], ...]
    status: str = ""
    skip_reason: str = ""
    provider_status: str = ""
    error_type: str = ""
    error_message: str = ""

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "ok": self.ok,
            "status": self.status or ("ok" if self.ok else "error"),
            "request_hash": self.request_hash,
            "evidence_ledger_hash": self.evidence_ledger_hash,
            "ohlcv_series_id": self.ohlcv_series_id,
            "planned_segment_count": self.planned_segment_count,
            "fetched_bar_count": self.fetched_bar_count,
            "inserted_bar_count": self.inserted_bar_count,
            "started_at_utc": self.started_at_utc,
            "finished_at_utc": self.finished_at_utc,
            "request_log": list(self.request_log),
        }
        if self.skip_reason:
            payload["skip_reason"] = self.skip_reason
        if self.provider_status:
            payload["provider_status"] = self.provider_status
        if self.error_type:
            payload["error_type"] = self.error_type
        if self.error_message:
            payload["error_message"] = self.error_message
        return payload


def execute_live_bar_backfill(
    plan: BackfillPlan,
    approval: ExecutionApproval,
    client: MassiveReadOnlyClient,
    repository: SQLiteStockUniverseRepository,
    *,
    evidence_facts: tuple[Any, ...] = (),
) -> LiveExecutionReceipt:
    """Fetch exact planned aggregate bars and persist them transactionally."""
    validate_approved_plan(plan, approval)
    approval_record = repository.execution_approval_for(plan, approval)
    if approval_record is None:
        raise ExecutionContractError(("durable execution approval record is required",))
    started = _utc_now()
    fetched: list[StoredOhlcvBar] = []
    inserted = 0
    try:
        repository.persist_plan_context(plan, evidence_facts=evidence_facts)
        for segment in plan.segments:
            fetched.extend(_fetch_segment_bars(plan, segment, client))
        inserted = repository.insert_bars(fetched)
    except ProviderEntitlementUnavailable as exc:
        finished = _utc_now()
        skipped_receipt = LiveExecutionReceipt(
            ok=False,
            status="skipped",
            request_hash=plan.request.request_hash,
            evidence_ledger_hash=plan.evidence_ledger_hash,
            ohlcv_series_id=plan.target.ohlcv_series_id,
            planned_segment_count=len(plan.segments),
            fetched_bar_count=len(fetched),
            inserted_bar_count=inserted,
            started_at_utc=started,
            finished_at_utc=finished,
            request_log=_request_log_payload(client),
            skip_reason=PROVIDER_ENTITLEMENT_SKIP_REASON,
            provider_status=exc.provider_status,
            error_type=exc.__class__.__name__,
            error_message=str(exc),
        )
        repository.insert_execution_receipt(
            skipped_receipt.to_dict()
            | {
                "approved_by": approval.approved_by,
                "approval_hash": approval_record["approval_hash"],
            }
        )
        return skipped_receipt
    except Exception as exc:
        finished = _utc_now()
        error_receipt = LiveExecutionReceipt(
            ok=False,
            request_hash=plan.request.request_hash,
            evidence_ledger_hash=plan.evidence_ledger_hash,
            ohlcv_series_id=plan.target.ohlcv_series_id,
            planned_segment_count=len(plan.segments),
            fetched_bar_count=len(fetched),
            inserted_bar_count=inserted,
            started_at_utc=started,
            finished_at_utc=finished,
            request_log=_request_log_payload(client),
            error_type=exc.__class__.__name__,
            error_message=str(exc),
        )
        try:
            repository.insert_execution_receipt(
                error_receipt.to_dict()
                | {
                    "approved_by": approval.approved_by,
                    "approval_hash": approval_record["approval_hash"],
                }
            )
        except Exception:
            pass
        raise
    finished = _utc_now()
    receipt = LiveExecutionReceipt(
        ok=True,
        request_hash=plan.request.request_hash,
        evidence_ledger_hash=plan.evidence_ledger_hash,
        ohlcv_series_id=plan.target.ohlcv_series_id,
        planned_segment_count=len(plan.segments),
        fetched_bar_count=len(fetched),
        inserted_bar_count=inserted,
        started_at_utc=started,
        finished_at_utc=finished,
        request_log=_request_log_payload(client),
    )
    repository.insert_execution_receipt(
        receipt.to_dict()
        | {
            "approved_by": approval.approved_by,
            "approval_hash": approval_record["approval_hash"],
        }
    )
    return receipt


def _request_log_payload(client: MassiveReadOnlyClient) -> tuple[dict[str, Any], ...]:
    return tuple(
        {
            "endpoint": item.endpoint,
            "params_without_api_key": item.params_without_api_key,
            "http_code": item.http_code,
            "api_status": item.api_status,
            "elapsed_seconds": item.elapsed_seconds,
        }
        for item in client.request_log
    )


def _fetch_segment_bars(
    plan: BackfillPlan,
    segment: PlannedSegment,
    client: MassiveReadOnlyClient,
) -> tuple[StoredOhlcvBar, ...]:
    bars: list[StoredOhlcvBar] = []
    previous_ts: int | None = None
    for window_from, window_to in _aggregate_request_windows(plan, segment):
        endpoint = (
            f"/v2/aggs/ticker/{urllib.parse.quote(segment.ticker, safe='')}/range/"
            f"{plan.request.multiplier}/{urllib.parse.quote(plan.request.timespan, safe='')}/"
            f"{window_from}/{window_to}"
        )
        params = {
            "adjusted": str(plan.request.adjusted).lower(),
            "sort": "asc",
            "limit": "50000",
        }
        for payload in _aggregate_payload_pages(client, endpoint, params):
            for item in _payload_results_or_raise(payload, ticker=segment.ticker):
                if not isinstance(item, dict):
                    continue
                bar = _bar_from_payload(plan, segment, item)
                bar = _quality_checked_bar(plan, segment, client, item, bar)
                _raise_for_unusable_bar(segment, bar)
                if (
                    bar.bar_date < segment.from_date.isoformat()
                    or bar.bar_date > segment.to_date.isoformat()
                ):
                    raise RuntimeError(
                        f"provider returned out-of-segment bar for {segment.ticker}: {bar.bar_date} "
                        f"outside {segment.from_date.isoformat()}..{segment.to_date.isoformat()}"
                    )
                if previous_ts is not None and bar.bar_start_ts <= previous_ts:
                    raise RuntimeError(
                        f"provider returned unordered bars for {segment.ticker}"
                    )
                previous_ts = bar.bar_start_ts
                bars.append(bar)
    return tuple(bars)


def _aggregate_request_windows(
    plan: BackfillPlan, segment: PlannedSegment
) -> tuple[tuple[str, str], ...]:
    if plan.request.timespan != "minute":
        return ((segment.from_date.isoformat(), segment.to_date.isoformat()),)
    windows = []
    current = segment.from_date
    while current <= segment.to_date:
        month_end = _month_end(current)
        window_to = min(month_end, segment.to_date)
        windows.append((current.isoformat(), window_to.isoformat()))
        current = window_to + dt.timedelta(days=1)
    return tuple(windows)


def _month_end(day: dt.date) -> dt.date:
    if day.month == 12:
        return dt.date(day.year, 12, 31)
    return dt.date(day.year, day.month + 1, 1) - dt.timedelta(days=1)


def _aggregate_payload_pages(
    client: MassiveReadOnlyClient,
    endpoint: str,
    params: dict[str, str],
) -> tuple[dict[str, Any], ...]:
    pages: list[dict[str, Any]] = []
    next_endpoint = endpoint
    next_params = dict(params)
    while next_endpoint:
        payload = client.get(next_endpoint, next_params)
        pages.append(payload)
        next_url = str(payload.get("next_url") or "")
        if not next_url:
            break
        next_endpoint, next_params = _endpoint_and_params_from_next_url(next_url)
    return tuple(pages)


def _endpoint_and_params_from_next_url(next_url: str) -> tuple[str, dict[str, str]]:
    parsed = urllib.parse.urlparse(next_url)
    endpoint = parsed.path or next_url
    params = {
        key: value
        for key, value in urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
        if key.lower() != "apikey"
    }
    return endpoint, params


def _payload_results_or_raise(payload: dict[str, Any], *, ticker: str) -> list[Any]:
    status = str(payload.get("status") or "")
    if status in PROVIDER_ENTITLEMENT_STATUSES:
        raise ProviderEntitlementUnavailable(ticker=ticker, provider_status=status)
    if status and status not in {"OK", "DELAYED"}:
        raise RuntimeError(f"bar fetch failed for {ticker}: provider status {status}")
    results = payload.get("results") or []
    if not isinstance(results, list):
        raise RuntimeError(f"bar fetch failed for {ticker}: non-list results")
    return results


def _raise_for_unusable_bar(segment: PlannedSegment, bar: StoredOhlcvBar) -> None:
    if bar.bar_quality_status in {VALIDATED, VALIDATED_REPAIRED, SUSPECT}:
        return
    raise RuntimeError(
        "bar quality hard failure for "
        f"{segment.ticker} {bar.bar_date}: status={bar.bar_quality_status} repair_rule={bar.repair_rule or 'none'}"
    )


def _quality_checked_bar(
    plan: BackfillPlan,
    segment: PlannedSegment,
    client: MassiveReadOnlyClient,
    raw_item: dict[str, Any],
    bar: StoredOhlcvBar,
) -> StoredOhlcvBar:
    if plan.request.multiplier != 1 or plan.request.timespan != "day":
        return replace(bar, bar_quality_status=VALIDATED, raw_bar_json=raw_item)
    values = OhlcvValues(
        open=bar.open,
        high=bar.high,
        low=bar.low,
        close=bar.close,
        volume=bar.volume,
        vwap=bar.vwap,
        transaction_count=bar.transaction_count,
    )
    issues = structural_issues(values)
    if issues:
        return replace(
            bar,
            bar_quality_status=status_for_structural_issues(issues),
            repair_rule=DAILY_BAR_STRUCTURAL_VALIDATION,
            raw_bar_json=raw_item,
            repair_evidence_json={"structural_issues": list(issues)},
        )

    reasons = suspicion_reasons(values)
    event_sensitive_reason = _event_sensitive_1m_reason(plan, segment, bar)
    if not reasons and not event_sensitive_reason:
        return replace(bar, bar_quality_status=VALIDATED, raw_bar_json=raw_item)

    evidence: dict[str, Any] = {
        "suspicion_reasons": list(reasons),
        "raw_daily": {
            "open": bar.open,
            "high": bar.high,
            "low": bar.low,
            "close": bar.close,
            "volume": bar.volume,
            "vwap": bar.vwap,
            "transaction_count": bar.transaction_count,
        },
        "proof_ladder": [],
    }
    if event_sensitive_reason:
        evidence["event_sensitive_reason"] = event_sensitive_reason
        envelope_1m = _fetch_intraday_envelope(
            segment.ticker, bar.bar_date, 1, "minute", plan, client
        )
        evidence["proof_ladder"].append(envelope_1m.to_dict())
        return _bar_with_terminal_envelope_decision(
            bar, raw_item, evidence, envelope_1m
        )

    envelope_30m = _fetch_intraday_envelope(
        segment.ticker, bar.bar_date, 30, "minute", plan, client
    )
    evidence["proof_ladder"].append(envelope_30m.to_dict())

    envelope_1m = _fetch_intraday_envelope(
        segment.ticker, bar.bar_date, 1, "minute", plan, client
    )
    evidence["proof_ladder"].append(envelope_1m.to_dict())
    return _bar_with_terminal_envelope_decision(bar, raw_item, evidence, envelope_1m)


def _bar_with_terminal_envelope_decision(
    bar: StoredOhlcvBar,
    raw_item: dict[str, Any],
    evidence: dict[str, Any],
    envelope: IntradayEnvelope,
) -> StoredOhlcvBar:
    if not envelope.ok:
        return replace(
            bar,
            bar_quality_status=SUSPECT,
            repair_rule=INTRADAY_EVIDENCE_INCOMPLETE,
            raw_bar_json=raw_item,
            repair_evidence_json=evidence,
        )

    high_conflict = high_exceeds_envelope(bar.high, envelope)
    low_conflict = low_below_envelope(bar.low, envelope)
    unrepaired_conflicts = []
    if component_outside_envelope(bar.open, envelope):
        unrepaired_conflicts.append(DAILY_OPEN_OUTSIDE_INTRADAY_ENVELOPE)
    if component_outside_envelope(bar.close, envelope):
        unrepaired_conflicts.append(DAILY_CLOSE_OUTSIDE_INTRADAY_ENVELOPE)
    if unrepaired_conflicts:
        evidence["unrepaired_conflicts"] = unrepaired_conflicts
        return replace(
            bar,
            bar_quality_status=SUSPECT,
            repair_rule=";".join(unrepaired_conflicts),
            raw_bar_json=raw_item,
            repair_evidence_json=evidence,
        )
    if not high_conflict and not low_conflict:
        return replace(
            bar,
            bar_quality_status=VALIDATED,
            raw_bar_json=raw_item,
            repair_evidence_json=evidence,
        )

    repaired_high = envelope.max_high if high_conflict else bar.high
    repaired_low = envelope.min_low if low_conflict else bar.low
    repair_rules = []
    if high_conflict:
        repair_rules.append(DAILY_HIGH_EXCEEDS_INTRADAY_ENVELOPE)
    if low_conflict:
        repair_rules.append(DAILY_LOW_BELOW_INTRADAY_ENVELOPE)
    evidence["repair"] = {
        "rules": repair_rules,
        "canonical_high": repaired_high,
        "canonical_low": repaired_low,
    }
    return replace(
        bar,
        high=repaired_high,
        low=repaired_low,
        bar_quality_status=VALIDATED_REPAIRED,
        repair_rule=";".join(repair_rules),
        raw_bar_json=raw_item,
        repair_evidence_json=evidence,
    )


def _event_sensitive_1m_reason(
    plan: BackfillPlan, segment: PlannedSegment, bar: StoredOhlcvBar
) -> str:
    bar_date = bar.bar_date
    if segment.event_date is not None and _date_within_event_window(
        bar_date, segment.event_date.isoformat()
    ):
        return "segment_event_window"
    if _date_is_segment_boundary(segment, bar_date) and _event_sensitive_text(
        segment.source
    ):
        return "event_sensitive_segment_source"
    for payload in _iter_dict_payloads(plan.event_lookup):
        if _event_payload_matches_bar(payload, segment.ticker, bar_date):
            return "event_lookup"
    for payload in _iter_dict_payloads(segment.validation):
        if _event_payload_matches_bar(payload, segment.ticker, bar_date):
            return "segment_validation"
    for payload in _iter_dict_payloads(segment.extra):
        if _event_payload_matches_bar(payload, segment.ticker, bar_date):
            return "segment_extra"
    return ""


def _date_is_segment_boundary(segment: PlannedSegment, bar_date: str) -> bool:
    return bar_date in {segment.from_date.isoformat(), segment.to_date.isoformat()}


def _event_payload_matches_bar(
    payload: dict[str, Any], ticker: str, bar_date: str
) -> bool:
    event_date = str(
        payload.get("date")
        or payload.get("effective_date")
        or payload.get("execution_date")
        or payload.get("ex_date")
        or ""
    )
    if not _date_within_event_window(bar_date, event_date):
        return False
    if (
        ticker
        and payload.get("ticker")
        and str(payload.get("ticker")).upper() != ticker.upper()
    ):
        nested_ticker = _nested_ticker_text(payload)
        if ticker.upper() not in nested_ticker:
            return False
    return _event_sensitive_text(_flatten_text(payload))


def _date_within_event_window(bar_date: str, event_date: str) -> bool:
    if not event_date:
        return False
    try:
        dt.date.fromisoformat(bar_date)
        dt.date.fromisoformat(event_date)
    except ValueError:
        return False
    return bar_date in _event_trading_window_dates(event_date)


def _event_trading_window_dates(event_date: str) -> set[str]:
    event_session = first_us_equity_trading_date_on_or_after(event_date)
    return {
        previous_us_equity_trading_date(event_session),
        event_session,
        next_us_equity_trading_date(event_session),
    }


def _event_sensitive_text(value: str) -> bool:
    lowered = value.lower()
    terms = (
        "ticker_change",
        "symbol_change",
        "ticker change",
        "symbol change",
        "split",
        "reverse_split",
        "reverse split",
        "stock dividend",
        "spinoff",
        "spin-off",
        "merger",
        "reorganization",
        "reclassification",
        "cusip",
        "figi",
        "accounting",
    )
    return any(term in lowered for term in terms)


def _iter_dict_payloads(value: Any) -> tuple[dict[str, Any], ...]:
    found: list[dict[str, Any]] = []

    def walk(item: Any) -> None:
        if isinstance(item, dict):
            found.append(item)
            for child in item.values():
                walk(child)
        elif isinstance(item, (list, tuple)):
            for child in item:
                walk(child)

    walk(value)
    return tuple(found)


def _flatten_text(value: Any) -> str:
    parts: list[str] = []

    def walk(item: Any) -> None:
        if isinstance(item, dict):
            for key, child in item.items():
                parts.append(str(key))
                walk(child)
        elif isinstance(item, (list, tuple)):
            for child in item:
                walk(child)
        else:
            parts.append(str(item))

    walk(value)
    return " ".join(parts)


def _nested_ticker_text(payload: dict[str, Any]) -> str:
    values: list[str] = []
    for key, value in payload.items():
        if "ticker" in str(key).lower():
            values.append(_flatten_text(value).upper())
    return " ".join(values)


def _fetch_intraday_envelope(
    ticker: str,
    date: str,
    multiplier: int,
    timespan: str,
    plan: BackfillPlan,
    client: MassiveReadOnlyClient,
) -> IntradayEnvelope:
    endpoint = (
        f"/v2/aggs/ticker/{urllib.parse.quote(ticker, safe='')}/range/"
        f"{multiplier}/{urllib.parse.quote(timespan, safe='')}/{date}/{date}"
    )
    payload = client.get(
        endpoint,
        {
            "adjusted": str(plan.request.adjusted).lower(),
            "sort": "asc",
            "limit": "50000",
        },
    )
    return intraday_envelope_from_payload(
        ticker=ticker,
        date=date,
        multiplier=multiplier,
        timespan=timespan,
        payload=payload,
    )


def _bar_from_payload(
    plan: BackfillPlan, segment: PlannedSegment, item: dict[str, Any]
) -> StoredOhlcvBar:
    ts = int(item["t"])
    calendar_id = plan.target.latest_primary_exchange or DEFAULT_US_EQUITY_CALENDAR_ID
    session = us_equity_session_for_utc_ts(ts, calendar_id=calendar_id)
    date = (
        session.session_date
        if session is not None
        else dt.datetime.fromtimestamp(ts / 1000, US_EASTERN).date().isoformat()
    )
    return StoredOhlcvBar(
        series_id=plan.target.ohlcv_series_id,
        ticker=segment.ticker,
        bar_date=date,
        bar_start_ts=ts,
        multiplier=plan.request.multiplier,
        timespan=plan.request.timespan,
        adjusted=plan.request.adjusted,
        open=_optional_float(item.get("o")),
        high=_optional_float(item.get("h")),
        low=_optional_float(item.get("l")),
        close=_optional_float(item.get("c")),
        volume=_optional_float(item.get("v")),
        vwap=_optional_float(item.get("vw")),
        transaction_count=_optional_int(item.get("n")),
        calendar_id=calendar_id,
        request_hash=plan.request.request_hash,
        ledger_hash=plan.evidence_ledger_hash,
        segment_index=segment.segment_index,
    )


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)


def _utc_now() -> str:
    return dt.datetime.now(dt.UTC).isoformat()
