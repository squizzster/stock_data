"""Build live provider-backed evidence sources from ticker reference data."""

from __future__ import annotations

import datetime as dt
import urllib.parse
from pathlib import Path
from typing import Any

from stock_universe.domain import (
    BackfillRequest,
    EvidenceFact,
    KnownAlias,
    ProviderIdentitySeed,
    TargetIdentity,
    normalize_bar_grain,
)
from stock_universe.evidence import ProviderBackfillEvidenceSource
from stock_universe.market_calendar import (
    default_us_equity_history_start_date,
    last_us_equity_trading_date_on_or_before,
)
from stock_universe.providers import (
    MassiveProviderConfig,
    MassiveReadOnlyClient,
    massive_read_only_provider_set,
)
from stock_universe.providers.models import ReferenceSnapshot
from stock_universe.providers.massive.payloads import _reference_snapshot_from_payload
from stock_universe.storage import SQLiteStockUniverseRepository


DEFAULT_TICKER_SEED_FROM_DATE: str | None = None


def massive_live_source_from_ticker(
    ticker: str,
    *,
    api_key: str | None = None,
    base_url: str = "https://api.massive.com",
    db_path: str | Path | None = None,
    allocate_identity: bool = False,
    require_existing_identity: bool = False,
    from_date: str | None = DEFAULT_TICKER_SEED_FROM_DATE,
    to_date: str | None = None,
    bar_grain: str = "1d",
    capture_dir: Path | None = None,
    client: MassiveReadOnlyClient | None = None,
) -> tuple[ProviderBackfillEvidenceSource, MassiveReadOnlyClient]:
    """Resolve a ticker into seed facts, then attach the live provider set."""
    normalized_ticker = ticker.strip().upper()
    if not normalized_ticker:
        raise ValueError("ticker is required")
    if client is None:
        if not api_key:
            raise ValueError("api_key is required when client is not provided")
        client = MassiveReadOnlyClient(
            MassiveProviderConfig(api_key=api_key, base_url=base_url),
            raw_capture_dir=capture_dir,
        )
    payload = client.get(
        f"/v3/reference/tickers/{urllib.parse.quote(normalized_ticker, safe='')}", {}
    )
    resolved_to_date = to_date or _latest_stock_session_date()
    resolved_from_date = from_date or default_us_equity_history_start_date(
        resolved_to_date
    )
    snapshot = _reference_snapshot_from_payload(
        normalized_ticker,
        resolved_to_date,
        payload,
    )
    seed = identity_seed_from_reference_snapshot(snapshot)
    series_id = _resolve_seed_series_id(
        seed,
        db_path=db_path,
        allocate_identity=allocate_identity,
        require_existing_identity=require_existing_identity,
    )
    source = ProviderBackfillEvidenceSource(
        ticker_seed_base_facts_from_snapshot(
            normalized_ticker,
            snapshot,
            ohlcv_series_id=series_id,
            from_date=resolved_from_date,
            to_date=resolved_to_date,
            bar_grain=bar_grain,
        ),
        massive_read_only_provider_set(client),
    )
    return source, client


def _latest_stock_session_date() -> str:
    return last_us_equity_trading_date_on_or_before(dt.datetime.now(dt.UTC).date())


def ticker_seed_base_facts(
    ticker: str,
    reference_payload: dict[str, Any],
    *,
    ohlcv_series_id: int,
    from_date: str,
    to_date: str,
    bar_grain: str = "1d",
) -> tuple[EvidenceFact, ...]:
    snapshot = _reference_snapshot_from_payload(ticker, to_date, reference_payload)
    return ticker_seed_base_facts_from_snapshot(
        ticker,
        snapshot,
        ohlcv_series_id=ohlcv_series_id,
        from_date=from_date,
        to_date=to_date,
        bar_grain=bar_grain,
    )


def ticker_seed_base_facts_from_snapshot(
    ticker: str,
    snapshot: ReferenceSnapshot,
    *,
    ohlcv_series_id: int,
    from_date: str,
    to_date: str,
    bar_grain: str = "1d",
) -> tuple[EvidenceFact, ...]:
    if snapshot.api_status != "OK":
        raise ValueError(
            f"ticker reference lookup failed for {ticker}: status={snapshot.api_status or 'unknown'}"
        )
    raw = snapshot.to_payload()
    raw_result = raw.get("raw") if isinstance(raw.get("raw"), dict) else {}
    name = str(raw_result.get("name") or "")
    target = target_identity_from_reference_snapshot(
        snapshot, ohlcv_series_id=ohlcv_series_id, company_name=name
    )
    series_id = target.ohlcv_series_id
    latest_ticker = snapshot.response_ticker or ticker
    grain = normalize_bar_grain(bar_grain)
    request = BackfillRequest(
        series_id=series_id,
        from_date=from_date,
        to_date=to_date,
        multiplier=grain.multiplier,
        timespan=grain.timespan,
    )
    known_aliases = (
        KnownAlias(
            ticker=latest_ticker,
            active=snapshot.active,
            company_name=name,
            primary_exchange=snapshot.primary_exchange,
        ),
    )
    metadata = {
        "generated_at_utc": dt.datetime.now(dt.UTC).replace(microsecond=0).isoformat(),
        "api_requests": 1,
        "raw_dir": "",
        "identity_discovery": {"seed": "ticker_reference", "ticker": ticker},
        "plan_files": {},
    }
    return (
        EvidenceFact(
            "target_identity",
            (str(series_id),),
            target.to_legacy_dict(),
            "massive.ticker_seed",
        ),
        EvidenceFact(
            "backfill_request",
            (str(series_id),),
            request.to_legacy_dict(),
            "ticker_seed",
        ),
        EvidenceFact(
            "known_aliases",
            (str(series_id),),
            [alias.to_legacy_dict() for alias in known_aliases],
            "massive.ticker_seed",
        ),
        EvidenceFact("plan_metadata", (str(series_id),), metadata, "ticker_seed"),
    )


def target_identity_from_reference_snapshot(
    snapshot: ReferenceSnapshot,
    *,
    ohlcv_series_id: int,
    company_name: str | None = None,
) -> TargetIdentity:
    return identity_seed_from_reference_snapshot(
        snapshot, company_name=company_name
    ).to_target_identity(ohlcv_series_id)


def identity_seed_from_reference_snapshot(
    snapshot: ReferenceSnapshot,
    *,
    company_name: str | None = None,
) -> ProviderIdentitySeed:
    raw = snapshot.to_payload()
    raw_result = raw.get("raw") if isinstance(raw.get("raw"), dict) else {}
    name = str(
        company_name if company_name is not None else raw_result.get("name") or ""
    )
    latest_ticker = snapshot.response_ticker or snapshot.ticker
    natural_key, provisional_key = reference_identity_keys(snapshot)
    return ProviderIdentitySeed(
        company_name=name,
        current_company_name=name,
        cik=snapshot.cik,
        composite_figi=snapshot.composite_figi,
        share_class_figi=snapshot.share_class_figi,
        identity_status=reference_identity_status(snapshot),
        latest_ticker=latest_ticker,
        latest_primary_exchange=snapshot.primary_exchange,
        locale=str(raw_result.get("locale") or "us"),
        market=str(raw_result.get("market") or "stocks"),
        natural_key=natural_key,
        provisional_key=provisional_key,
        security_type=snapshot.security_type or None,
    )


def reference_identity_status(snapshot: ReferenceSnapshot) -> str:
    return (
        "permanent"
        if snapshot.composite_figi or snapshot.share_class_figi
        else "provisional"
    )


def reference_identity_keys(snapshot: ReferenceSnapshot) -> tuple[str, str | None]:
    if snapshot.composite_figi:
        return f"massive:composite_figi:{snapshot.composite_figi}", None
    if snapshot.share_class_figi:
        return f"massive:share_class_figi:{snapshot.share_class_figi}", None
    provisional_key = _provisional_reference_key(snapshot)
    return provisional_key, provisional_key


def _provisional_reference_key(snapshot: ReferenceSnapshot) -> str:
    latest_ticker = snapshot.response_ticker or snapshot.ticker
    return ":".join(
        (
            "massive",
            "provisional_ticker",
            latest_ticker,
            snapshot.primary_exchange,
            snapshot.security_type,
            snapshot.cik,
        )
    )


def _resolve_seed_series_id(
    seed: ProviderIdentitySeed,
    *,
    db_path: str | Path | None,
    allocate_identity: bool,
    require_existing_identity: bool,
) -> int:
    if allocate_identity and require_existing_identity:
        raise ValueError(
            "allocate_identity and require_existing_identity are mutually exclusive"
        )
    if db_path is None:
        raise ValueError("db_path is required to resolve a ticker seed ohlcv_series_id")
    repository = SQLiteStockUniverseRepository(db_path)
    if allocate_identity:
        return repository.ensure_ohlcv_series_id(seed.natural_key)
    series_id = repository.lookup_ohlcv_series_id(seed.natural_key)
    if series_id is None and require_existing_identity:
        raise ValueError(
            f"ohlcv_series_id not found for natural_key: {seed.natural_key}"
        )
    if series_id is None:
        raise ValueError("ticker seed ohlcv_series_id is unresolved")
    return series_id
