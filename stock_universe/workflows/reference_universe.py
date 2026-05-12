"""Reference-universe snapshot maintenance workflows."""

from __future__ import annotations

import datetime as dt
import urllib.parse
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from stock_universe.domain import (
    BackfillRequest,
    EvidenceFact,
    KnownAlias,
    TargetIdentity,
    normalize_bar_grain,
)
from stock_universe.evidence import ProviderBackfillEvidenceSource
from stock_universe.market_calendar import (
    default_us_equity_history_start_date,
    last_us_equity_trading_date_on_or_before,
)
from stock_universe.providers import MassiveReadOnlyClient
from stock_universe.providers import (
    MassiveProviderConfig,
    massive_read_only_provider_set,
)
from stock_universe.providers.massive.payloads import _reference_snapshot_from_payload
from stock_universe.storage import (
    SQLiteStockUniverseRepository,
    StoredReferenceSnapshot,
)
from stock_universe.workflows.ticker_seed import identity_seed_from_reference_snapshot


REFERENCE_UNIVERSE_PROVIDER = "massive.reference_tickers"
DEFAULT_SERIES_ID_SEED_FROM_DATE: str | None = None
ProgressSink = Callable[[dict[str, Any]], None]


@dataclass(frozen=True)
class ReferenceUniverseRequest:
    market: str = "stocks"
    exchange: str = ""
    as_of_date: str = ""
    active: bool | None = True
    limit: int = 1000
    max_pages: int = 100

    def __post_init__(self) -> None:
        if self.limit < 1 or self.limit > 1000:
            raise ValueError("limit must be between 1 and 1000")
        if self.max_pages < 1:
            raise ValueError("max_pages must be positive")
        if not self.as_of_date:
            object.__setattr__(self, "as_of_date", _latest_stock_session_date())

    @property
    def snapshot_as_of_date(self) -> str:
        return self.as_of_date

    def to_dict(self) -> dict[str, Any]:
        return {
            "active": self.active,
            "as_of_date": self.as_of_date,
            "exchange": self.exchange,
            "limit": self.limit,
            "market": self.market,
            "max_pages": self.max_pages,
            "provider": REFERENCE_UNIVERSE_PROVIDER,
            "snapshot_as_of_date": self.snapshot_as_of_date,
        }


@dataclass(frozen=True)
class ReferenceUniverseUpdate:
    request: ReferenceUniverseRequest
    snapshots: tuple[StoredReferenceSnapshot, ...]
    page_count: int
    pending_requests: tuple[dict[str, Any], ...] = ()

    @property
    def complete(self) -> bool:
        return not self.pending_requests

    def to_dict(self) -> dict[str, Any]:
        return {
            "complete": self.complete,
            "fetched_count": len(self.snapshots),
            "page_count": self.page_count,
            "pending_requests": list(self.pending_requests),
            "request": self.request.to_dict(),
            "snapshots": [_snapshot_payload(snapshot) for snapshot in self.snapshots],
        }


def fetch_massive_reference_universe(
    client: MassiveReadOnlyClient,
    request: ReferenceUniverseRequest,
    *,
    progress_sink: ProgressSink | None = None,
) -> ReferenceUniverseUpdate:
    snapshots: list[StoredReferenceSnapshot] = []
    pending_requests: list[dict[str, Any]] = []
    page_count = 0
    endpoint = "/v3/reference/tickers"
    snapshot_as_of_date = request.snapshot_as_of_date

    for active in _active_values(request.active):
        cursor = ""
        for page_index in range(request.max_pages):
            params = (
                {"cursor": cursor}
                if cursor
                else _reference_request_params(request, active=active)
            )
            payload = client.get(endpoint, params)
            page_count += 1
            if str(payload.get("status") or "") != "OK":
                raise ValueError(
                    "reference universe lookup failed: "
                    f"status={payload.get('status') or 'unknown'} active={active}"
                )
            snapshots.extend(
                _snapshots_from_payload(
                    payload,
                    snapshot_as_of_date=snapshot_as_of_date,
                    source_request={
                        "endpoint": endpoint,
                        "params": params,
                        "page_index": page_index,
                    },
                )
            )
            cursor = _next_cursor(payload)
            if progress_sink is not None:
                progress_sink(
                    {
                        "event_type": "page_fetched",
                        "message": "reference page fetched",
                        "active": active,
                        "fetched_count": len(snapshots),
                        "has_next_page": bool(cursor),
                        "page_count": page_count,
                        "page_index": page_index,
                    }
                )
            if not cursor:
                break
        if cursor:
            pending_requests.append(
                {
                    "endpoint": endpoint,
                    "params": {"cursor": cursor},
                    "reason": "max_pages_reached",
                }
            )
    return ReferenceUniverseUpdate(
        request=request,
        snapshots=tuple(_unique_snapshots(snapshots)),
        page_count=page_count,
        pending_requests=tuple(pending_requests),
    )


def massive_live_source_from_series_id(
    db_path: str | Path,
    ohlcv_series_id: int,
    *,
    api_key: str | None = None,
    base_url: str = "https://api.massive.com",
    from_date: str | None = DEFAULT_SERIES_ID_SEED_FROM_DATE,
    to_date: str | None = None,
    bar_grain: str = "1d",
    as_of_date: str | None = None,
    capture_dir: Path | None = None,
    client: MassiveReadOnlyClient | None = None,
) -> tuple[
    ProviderBackfillEvidenceSource, MassiveReadOnlyClient, StoredReferenceSnapshot
]:
    snapshot = SQLiteStockUniverseRepository(db_path).reference_snapshot_for_series_id(
        ohlcv_series_id,
        as_of_date=as_of_date,
    )
    if snapshot is None:
        raise ValueError(
            f"ohlcv_series_id not found in reference universe: {ohlcv_series_id}"
        )
    if client is None:
        if not api_key:
            raise ValueError("api_key is required when client is not provided")
        client = MassiveReadOnlyClient(
            MassiveProviderConfig(api_key=api_key, base_url=base_url),
            raw_capture_dir=capture_dir,
        )
    resolved_to_date = to_date or _latest_stock_session_date()
    resolved_from_date = from_date or default_us_equity_history_start_date(
        resolved_to_date
    )
    source = ProviderBackfillEvidenceSource(
        reference_snapshot_seed_base_facts(
            snapshot,
            from_date=resolved_from_date,
            to_date=resolved_to_date,
            bar_grain=bar_grain,
        ),
        massive_read_only_provider_set(client),
    )
    return source, client, snapshot


def _latest_stock_session_date() -> str:
    return last_us_equity_trading_date_on_or_before(dt.datetime.now(dt.UTC).date())


def reference_snapshot_seed_base_facts(
    snapshot: StoredReferenceSnapshot,
    *,
    from_date: str,
    to_date: str,
    bar_grain: str = "1d",
) -> tuple[EvidenceFact, ...]:
    target = target_identity_from_stored_reference_snapshot(snapshot)
    grain = normalize_bar_grain(bar_grain)
    request = BackfillRequest(
        series_id=target.ohlcv_series_id,
        from_date=from_date,
        to_date=to_date,
        multiplier=grain.multiplier,
        timespan=grain.timespan,
    )
    known_aliases = (
        KnownAlias(
            ticker=snapshot.ticker,
            active=snapshot.active,
            company_name=snapshot.company_name,
            primary_exchange=snapshot.primary_exchange,
        ),
    )
    metadata = {
        "generated_at_utc": dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat(),
        "api_requests": 0,
        "raw_dir": "",
        "identity_discovery": {
            "seed": "reference_universe_series_id",
            "ohlcv_series_id": snapshot.ohlcv_series_id,
            "snapshot_as_of_date": snapshot.snapshot_as_of_date,
            "source": snapshot.provider,
        },
        "plan_files": {},
    }
    series_id = target.ohlcv_series_id
    return (
        EvidenceFact(
            "target_identity",
            (str(series_id),),
            target.to_legacy_dict(),
            "sqlite.reference_universe",
        ),
        EvidenceFact(
            "backfill_request",
            (str(series_id),),
            request.to_legacy_dict(),
            "series_id_seed",
        ),
        EvidenceFact(
            "known_aliases",
            (str(series_id),),
            [alias.to_legacy_dict() for alias in known_aliases],
            "sqlite.reference_universe",
        ),
        EvidenceFact("plan_metadata", (str(series_id),), metadata, "series_id_seed"),
    )


def target_identity_from_stored_reference_snapshot(
    snapshot: StoredReferenceSnapshot,
) -> TargetIdentity:
    return TargetIdentity(
        ohlcv_series_id=snapshot.ohlcv_series_id,
        company_name=snapshot.company_name,
        current_company_name=snapshot.company_name,
        cik=snapshot.cik,
        composite_figi=snapshot.composite_figi,
        share_class_figi=snapshot.share_class_figi,
        identity_status=snapshot.identity_status or "unknown",
        latest_ticker=snapshot.ticker,
        latest_primary_exchange=snapshot.primary_exchange,
        locale=snapshot.locale,
        market=snapshot.market,
        natural_key=snapshot.natural_key,
        provisional_key=snapshot.provisional_key or None,
        security_type=snapshot.security_type or None,
    )


def _reference_request_params(
    request: ReferenceUniverseRequest, *, active: bool
) -> dict[str, str]:
    params = {
        "active": str(active).lower(),
        "limit": str(request.limit),
        "market": request.market,
        "order": "asc",
        "sort": "ticker",
    }
    if request.exchange:
        params["exchange"] = request.exchange
    if request.as_of_date:
        params["date"] = request.as_of_date
    return params


def _snapshots_from_payload(
    payload: dict[str, Any],
    *,
    snapshot_as_of_date: str,
    source_request: dict[str, Any],
) -> tuple[StoredReferenceSnapshot, ...]:
    results = payload.get("results") or []
    if not isinstance(results, list):
        return ()
    snapshots = []
    for item in results:
        if not isinstance(item, dict):
            continue
        ticker = str(item.get("ticker") or "")
        if not ticker:
            continue
        snapshot = _reference_snapshot_from_payload(
            ticker,
            snapshot_as_of_date,
            {"status": str(payload.get("status") or ""), "results": item},
        )
        seed = identity_seed_from_reference_snapshot(
            snapshot,
            company_name=str(item.get("name") or ""),
        )
        snapshots.append(
            StoredReferenceSnapshot(
                provider=REFERENCE_UNIVERSE_PROVIDER,
                snapshot_as_of_date=snapshot_as_of_date,
                ticker=snapshot.response_ticker or snapshot.ticker,
                active=snapshot.active,
                company_name=seed.company_name,
                cik=snapshot.cik,
                composite_figi=snapshot.composite_figi,
                share_class_figi=snapshot.share_class_figi,
                security_type=snapshot.security_type,
                primary_exchange=snapshot.primary_exchange,
                market=str(item.get("market") or ""),
                locale=str(item.get("locale") or ""),
                identity_status=seed.identity_status,
                natural_key=seed.natural_key,
                provisional_key=seed.provisional_key or "",
                raw=item,
                source_request=source_request,
            )
        )
    return tuple(snapshots)


def _next_cursor(payload: dict[str, Any]) -> str:
    next_url = str(payload.get("next_url") or "")
    if not next_url:
        return ""
    parsed = urllib.parse.urlparse(next_url)
    return urllib.parse.parse_qs(parsed.query).get("cursor", [""])[0]


def _active_values(active: bool | None) -> tuple[bool, ...]:
    if active is None:
        return (True, False)
    return (bool(active),)


def _unique_snapshots(
    snapshots: list[StoredReferenceSnapshot],
) -> list[StoredReferenceSnapshot]:
    unique: dict[tuple[str, str, str, str], StoredReferenceSnapshot] = {}
    for snapshot in snapshots:
        key = (
            snapshot.provider,
            snapshot.snapshot_as_of_date,
            snapshot.ticker,
            snapshot.natural_key,
        )
        unique[key] = snapshot
    return list(unique.values())


def _snapshot_payload(snapshot: StoredReferenceSnapshot) -> dict[str, Any]:
    return {
        "active": snapshot.active,
        "cik": snapshot.cik,
        "company_name": snapshot.company_name,
        "composite_figi": snapshot.composite_figi,
        "identity_status": snapshot.identity_status,
        "market": snapshot.market,
        "natural_key": snapshot.natural_key,
        "ohlcv_series_id": snapshot.ohlcv_series_id or None,
        "primary_exchange": snapshot.primary_exchange,
        "security_type": snapshot.security_type,
        "share_class_figi": snapshot.share_class_figi,
        "snapshot_as_of_date": snapshot.snapshot_as_of_date,
        "source": snapshot.provider,
        "ticker": snapshot.ticker,
    }
