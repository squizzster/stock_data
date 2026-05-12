"""Payload normalization helpers for Massive providers."""

from __future__ import annotations

import datetime as dt
import urllib.parse
from typing import Any

from stock_universe.domain import BackfillRequest, normalize_bar_grain
from stock_universe.providers.massive.client import MassiveReadOnlyClient
from stock_universe.providers.models import (
    BarProbeResult,
    IdentityScanResult,
    ReferenceSnapshot,
)


def _reference_snapshot_from_payload(
    ticker: str, as_of_date: str, payload: dict[str, Any]
) -> ReferenceSnapshot:
    result = payload.get("results") or {}
    if isinstance(result, list):
        result = result[0] if result else {}
    if not isinstance(result, dict):
        result = {}
    return ReferenceSnapshot(
        ticker=ticker,
        as_of_date=as_of_date,
        api_status=str(payload.get("status") or ""),
        response_ticker=str(result.get("ticker") or ticker),
        composite_figi=str(result.get("composite_figi") or ""),
        share_class_figi=str(result.get("share_class_figi") or ""),
        cik=str(result.get("cik") or ""),
        primary_exchange=str(result.get("primary_exchange") or ""),
        security_type=str(result.get("type") or ""),
        active=result.get("active"),
        raw=result,
    )


def _aggregate_bars_payload(
    client: MassiveReadOnlyClient,
    request: BackfillRequest,
    ticker: str,
    from_date: str,
    to_date: str,
    *,
    bar_grain: str | None = None,
) -> dict[str, Any]:
    spec = (
        normalize_bar_grain(bar_grain)
        if bar_grain
        else normalize_bar_grain(
            multiplier=request.multiplier, timespan=request.timespan
        )
    )
    endpoint = (
        f"/v2/aggs/ticker/{urllib.parse.quote(ticker, safe='')}/range/"
        f"{spec.multiplier}/{urllib.parse.quote(spec.timespan, safe='')}/{from_date}/{to_date}"
    )
    params = {
        "adjusted": str(request.adjusted).lower(),
        "sort": "asc",
        "limit": "50000",
    }
    return client.get(endpoint, params)


def _bar_dates_from_payload(payload: dict[str, Any]) -> tuple[str, ...]:
    results = payload.get("results") or ()
    if not isinstance(results, list):
        return ()
    dates = []
    for item in results:
        if not isinstance(item, dict) or item.get("t") is None:
            continue
        dates.append(
            dt.datetime.fromtimestamp(int(item["t"]) / 1000, dt.UTC).date().isoformat()
        )
    return tuple(sorted(dict.fromkeys(dates)))


def _bar_probe_result_from_payload(
    ticker: str,
    from_date: str,
    to_date: str,
    payload: dict[str, Any],
) -> BarProbeResult:
    results = payload.get("results") or ()
    if not isinstance(results, list):
        results = []
    return BarProbeResult(
        ticker=ticker,
        from_date=from_date,
        to_date=to_date,
        bar_count=int(payload.get("resultsCount") or len(results)),
        api_status=str(payload.get("status") or ""),
    )


def _identity_scan_result_from_payload(
    query: str,
    as_of_date: str,
    payload: dict[str, Any],
) -> IdentityScanResult:
    results = payload.get("results") or ()
    if not isinstance(results, list):
        results = []
    matches = []
    for item in results:
        if not isinstance(item, dict):
            continue
        matches.append(
            {
                "active": item.get("active"),
                "cik": str(item.get("cik") or ""),
                "composite_figi": str(item.get("composite_figi") or ""),
                "name": str(item.get("name") or ""),
                "primary_exchange": str(item.get("primary_exchange") or ""),
                "share_class_figi": str(item.get("share_class_figi") or ""),
                "ticker": str(item.get("ticker") or ""),
                "type": str(item.get("type") or ""),
            }
        )
    return IdentityScanResult(
        query=query, as_of_date=as_of_date, matches=tuple(matches)
    )
