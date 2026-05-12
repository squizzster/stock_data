"""Read-only identity candidate discovery."""

from __future__ import annotations

import json
import re
import sqlite3
import urllib.parse
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from stock_universe.domain.common import freeze_json, unfreeze_json
from stock_universe.providers import MassiveReadOnlyClient
from stock_universe.providers.massive.payloads import _reference_snapshot_from_payload
from stock_universe.providers.models import ReferenceSnapshot
from stock_universe.storage.sqlite_access import connect_readonly_sqlite
from stock_universe.workflows.ticker_seed import identity_seed_from_reference_snapshot


IDENTITY_SEARCH_FIELDS = (
    "ohlcv_series_id",
    "lookup_status",
    "active",
    "ticker",
    "company_name",
    "cik",
    "composite_figi",
    "share_class_figi",
    "security_type",
    "primary_exchange",
    "identity_status",
    "as_of_date",
    "match_rank",
    "match_reason",
    "source",
)

ISSUER_ENRICHMENT_MAX_CIKS = 3
ISSUER_ENRICHMENT_SECURITY_TYPES = {"CS", "ADRC", "PFD"}
IDENTITY_REPORTING_POLICY = {
    "canonical_ohlcv_field": "ohlcv_series_id",
    "default_ohlcv_reporting_scope": "ohlcv_series_id",
    "ticker_field_semantics": "ticker is a point-in-time symbol or alias for display and matching.",
    "ohlcv_query_rule": (
        "For bar counts, date ranges, latest days, and other OHLCV questions, filter and group by "
        "selected_candidate.ohlcv_series_id unless the user explicitly asks for a ticker-label slice."
    ),
    "why": "A single OHLCV series can contain multiple ticker labels after ticker changes.",
}


@dataclass(frozen=True)
class IdentitySearchCandidate:
    ohlcv_series_id: int | None
    active: bool | int | None
    ticker: str
    company_name: str
    cik: str
    composite_figi: str
    share_class_figi: str
    security_type: str
    primary_exchange: str
    identity_status: str
    as_of_date: str
    match_rank: int
    match_reason: str
    source: str
    lookup_status: str = ""
    natural_key: str = ""
    provisional_key: str = ""
    raw: Any = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "raw", freeze_json(self.raw))

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "active": self.active,
            "as_of_date": self.as_of_date,
            "cik": self.cik,
            "company_name": self.company_name,
            "composite_figi": self.composite_figi,
            "identity_status": self.identity_status,
            "match_rank": self.match_rank,
            "match_reason": self.match_reason,
            "ohlcv_series_id": self.ohlcv_series_id,
            "lookup_status": self.lookup_status
            or ("resolved" if self.ohlcv_series_id is not None else "unresolved"),
            "primary_exchange": self.primary_exchange,
            "security_type": self.security_type,
            "share_class_figi": self.share_class_figi,
            "source": self.source,
            "ticker": self.ticker,
        }
        if self.natural_key:
            payload["natural_key"] = self.natural_key
        if self.provisional_key:
            payload["provisional_key"] = self.provisional_key
        return payload


@dataclass(frozen=True)
class IdentitySearchResult:
    query: str
    source: str
    candidates: tuple[IdentitySearchCandidate, ...]
    as_of_date: str = ""
    related_searches: tuple[Any, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        reporting_policy = dict(IDENTITY_REPORTING_POLICY)
        return {
            "query": self.query,
            "source": self.source,
            "as_of_date": self.as_of_date,
            "agent_ohlcv_reporting_policy": reporting_policy,
            "fields": list(IDENTITY_SEARCH_FIELDS),
            "count": len(self.candidates),
            "reporting_policy": reporting_policy,
            "candidates": [candidate.to_dict() for candidate in self.candidates],
            "related_searches": [unfreeze_json(item) for item in self.related_searches],
        }


def live_identity_search(
    query: str,
    *,
    client: MassiveReadOnlyClient,
    as_of_date: str | None = None,
    limit: int = 25,
) -> IdentitySearchResult:
    normalized_query = _normalized_query(query)
    payloads = [
        client.get(endpoint, params)
        for endpoint, params in _live_search_requests(
            normalized_query, as_of_date=as_of_date, limit=limit
        )
    ]
    candidates: list[IdentitySearchCandidate] = []
    related_searches: list[dict[str, Any]] = []
    for payload in payloads:
        candidates.extend(
            _candidates_from_massive_payload(
                normalized_query,
                payload,
                as_of_date=as_of_date or "",
            )
        )
    if not candidates:
        fallback_payloads = [
            client.get(endpoint, params)
            for endpoint, params in _inactive_fallback_requests(
                normalized_query, as_of_date=as_of_date, limit=limit
            )
        ]
        for payload in fallback_payloads:
            candidates.extend(
                _candidates_from_massive_payload(
                    normalized_query,
                    payload,
                    as_of_date=as_of_date or "",
                )
            )
    for seed in _issuer_enrichment_seeds(
        normalized_query, _ranked_unique(candidates, limit)
    ):
        cik = _candidate_cik(seed)
        payload = client.get(
            "/v3/reference/tickers",
            _live_search_params(
                {"cik": cik, "sort": "ticker"},
                as_of_date=as_of_date,
                limit=limit,
                active=True,
            ),
        )
        enriched = [
            _replace_score(candidate, 5, "issuer_cik_enrichment")
            for candidate in _candidates_from_massive_payload(
                cik, payload, as_of_date=as_of_date or ""
            )
        ]
        candidates.extend(enriched)
        related_searches.append(
            _issuer_enrichment_record(
                seed,
                cik=cik,
                source="massive.reference_tickers",
                returned_count=len(enriched),
            )
        )
    return IdentitySearchResult(
        query=normalized_query,
        source="massive.reference_tickers",
        candidates=tuple(_ranked_unique(candidates, limit)),
        as_of_date=as_of_date or "",
        related_searches=tuple(related_searches),
    )


def sqlite_identity_search(
    db_path: str | Path,
    query: str,
    *,
    limit: int = 25,
) -> IdentitySearchResult:
    normalized_query = _normalized_query(query)
    all_candidates = list(_sqlite_identity_candidates(db_path))
    candidates = [
        candidate
        for candidate in all_candidates
        if _score_candidate(normalized_query, candidate)[0] < 99
    ]
    scored = [
        _replace_score(candidate, *_score_candidate(normalized_query, candidate))
        for candidate in candidates
    ]
    related_searches: list[dict[str, Any]] = []
    for seed in _issuer_enrichment_seeds(
        normalized_query, _ranked_unique(scored, limit)
    ):
        cik = _candidate_cik(seed)
        enriched = [
            _replace_score(candidate, 5, "issuer_cik_enrichment")
            for candidate in all_candidates
            if _candidate_cik(candidate) == cik
            and _is_issuer_enrichment_security(candidate)
        ]
        scored.extend(enriched)
        related_searches.append(
            _issuer_enrichment_record(
                seed,
                cik=cik,
                source="sqlite.identity_catalog",
                returned_count=len(enriched),
            )
        )
    return IdentitySearchResult(
        query=normalized_query,
        source="sqlite.identity_catalog",
        candidates=tuple(_ranked_unique(scored, limit)),
        related_searches=tuple(related_searches),
    )


def _live_search_requests(
    query: str,
    *,
    as_of_date: str | None,
    limit: int,
    active: bool = True,
) -> list[tuple[str, dict[str, str]]]:
    if _looks_like_cik(query):
        return [
            (
                "/v3/reference/tickers",
                _live_search_params(
                    {"cik": _zero_pad_cik(query)},
                    as_of_date=as_of_date,
                    limit=limit,
                    active=active,
                ),
            )
        ]
    if _looks_like_ticker_symbol(query):
        ticker = query.upper()
        return [
            (
                f"/v3/reference/tickers/{urllib.parse.quote(ticker, safe='')}",
                _date_param(as_of_date),
            ),
            (
                "/v3/reference/tickers",
                _live_search_params(
                    {
                        "ticker.gte": ticker,
                        "ticker.lt": _ticker_prefix_upper_bound(ticker),
                        "sort": "ticker",
                    },
                    as_of_date=as_of_date,
                    limit=limit,
                    active=active,
                ),
            ),
        ]
    return [
        (
            "/v3/reference/tickers",
            _live_search_params(
                {"search": query}, as_of_date=as_of_date, limit=limit, active=active
            ),
        )
    ]


def _live_search_params(
    params: dict[str, str],
    *,
    as_of_date: str | None,
    limit: int,
    active: bool,
) -> dict[str, str]:
    result = dict(params)
    result.update(
        {
            "active": str(active).lower(),
            "limit": str(limit),
        }
    )
    if as_of_date:
        result["date"] = as_of_date
    return result


def _date_param(as_of_date: str | None) -> dict[str, str]:
    if not as_of_date:
        return {}
    return {"date": as_of_date}


def _ticker_prefix_upper_bound(ticker: str) -> str:
    return ticker[:-1] + chr(ord(ticker[-1]) + 1)


def _inactive_fallback_requests(
    query: str,
    *,
    as_of_date: str | None,
    limit: int,
) -> list[tuple[str, dict[str, str]]]:
    return _live_search_requests(
        query, as_of_date=as_of_date, limit=limit, active=False
    )


def _payload_results(payload: dict[str, Any]) -> list[Any]:
    results = payload.get("results") or []
    if isinstance(results, list):
        return results
    if isinstance(results, dict):
        return [results]
    return []


def _candidates_from_massive_payload(
    query: str,
    payload: dict[str, Any],
    *,
    as_of_date: str,
) -> tuple[IdentitySearchCandidate, ...]:
    results = _payload_results(payload)
    candidates = []
    for item in results:
        if not isinstance(item, dict):
            continue
        snapshot = _snapshot_from_result(
            item, status=str(payload.get("status") or ""), as_of_date=as_of_date
        )
        seed = identity_seed_from_reference_snapshot(snapshot)
        rank, reason = _score_snapshot(query, snapshot)
        if rank >= 99:
            continue
        candidates.append(
            IdentitySearchCandidate(
                ohlcv_series_id=None,
                active=snapshot.active,
                ticker=snapshot.response_ticker or snapshot.ticker,
                company_name=seed.company_name,
                cik=snapshot.cik,
                composite_figi=snapshot.composite_figi,
                share_class_figi=snapshot.share_class_figi,
                security_type=snapshot.security_type,
                primary_exchange=snapshot.primary_exchange,
                identity_status=seed.identity_status,
                as_of_date=as_of_date,
                match_rank=rank,
                match_reason=reason,
                source="massive.reference_tickers",
                lookup_status="not_looked_up",
                natural_key=seed.natural_key,
                provisional_key=seed.provisional_key or "",
                raw=item,
            )
        )
    return tuple(candidates)


def _snapshot_from_result(
    item: dict[str, Any], *, status: str, as_of_date: str
) -> ReferenceSnapshot:
    ticker = str(item.get("ticker") or "")
    return _reference_snapshot_from_payload(
        ticker,
        as_of_date or "1970-01-01",
        {"status": status, "results": item},
    )


def _sqlite_identity_candidates(
    db_path: str | Path,
) -> tuple[IdentitySearchCandidate, ...]:
    with connect_readonly_sqlite(db_path) as conn:
        rows = _sqlite_plan_identity_rows(conn)
        reference_rows = _sqlite_reference_identity_rows(conn)
    candidates = []
    for row in rows:
        raw = _json_alias_raw(row["alias_raw_json"])
        candidates.append(
            IdentitySearchCandidate(
                ohlcv_series_id=int(row["ohlcv_series_id"]),
                active=row["active"],
                ticker=str(row["ticker"] or ""),
                company_name=str(row["company_name"] or ""),
                cik=str(row["cik"] or ""),
                composite_figi=str(row["composite_figi"] or ""),
                share_class_figi=str(row["share_class_figi"] or ""),
                security_type=str(row["security_type"] or ""),
                primary_exchange=str(row["primary_exchange"] or ""),
                identity_status=str(row["identity_status"] or ""),
                as_of_date=str(row["as_of_date"] or ""),
                match_rank=99,
                match_reason="unknown",
                source=f"sqlite.{row['alias_source'] or 'ohlcv_series'}",
                lookup_status="resolved",
                natural_key=str(row["natural_key"] or ""),
                provisional_key=str(row["provisional_key"] or ""),
                raw=raw,
            )
        )
    for row in reference_rows:
        candidates.append(
            IdentitySearchCandidate(
                ohlcv_series_id=int(row["ohlcv_series_id"]),
                active=_active_from_flag(row["active_flag"]),
                ticker=str(row["ticker"] or ""),
                company_name=str(row["company_name"] or ""),
                cik=str(row["cik"] or ""),
                composite_figi=str(row["composite_figi"] or ""),
                share_class_figi=str(row["share_class_figi"] or ""),
                security_type=str(row["security_type"] or ""),
                primary_exchange=str(row["primary_exchange"] or ""),
                identity_status=str(row["identity_status"] or ""),
                as_of_date=str(row["snapshot_as_of_date"] or ""),
                match_rank=99,
                match_reason="unknown",
                source=f"sqlite.{row['provider'] or 'reference_universe_snapshots'}",
                lookup_status="resolved",
                natural_key=str(row["natural_key"] or ""),
                provisional_key=str(row["provisional_key"] or ""),
                raw=_json_alias_raw(row["raw_json"]),
            )
        )
    return tuple(candidates)


def _sqlite_plan_identity_rows(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    if not _sqlite_table_exists(conn, "ohlcv_series") or not _sqlite_table_exists(
        conn, "ohlcv_series_id_lookup"
    ):
        return []
    return conn.execute(
        """
        SELECT
          os.ohlcv_series_id,
          COALESCE(NULLIF(ta.ticker, ''), os.latest_ticker) AS ticker,
          COALESCE(NULLIF(ta.as_of_date, ''), '') AS as_of_date,
          ta.active,
          COALESCE(NULLIF(json_extract(os.target_json, '$.company_name'), ''), os.company_name) AS company_name,
          COALESCE(json_extract(os.target_json, '$.cik'), '') AS cik,
          os.composite_figi,
          os.share_class_figi,
          COALESCE(json_extract(os.target_json, '$.security_type'), '') AS security_type,
          COALESCE(json_extract(os.target_json, '$.latest_primary_exchange'), '') AS primary_exchange,
          os.identity_status,
          l.natural_key,
          COALESCE(json_extract(os.target_json, '$.provisional_key'), '') AS provisional_key,
          ta.source AS alias_source,
          ta.raw_json AS alias_raw_json
        FROM ohlcv_series os
        JOIN ohlcv_series_id_lookup l ON l.ohlcv_series_id = os.ohlcv_series_id
        LEFT JOIN ticker_aliases ta ON ta.ohlcv_series_id = os.ohlcv_series_id
        """
    ).fetchall()


def _sqlite_reference_identity_rows(conn: sqlite3.Connection) -> list[sqlite3.Row]:
    if not _sqlite_table_exists(
        conn, "reference_universe_snapshots"
    ) or not _sqlite_table_exists(
        conn,
        "ohlcv_series_id_lookup",
    ):
        return []
    return conn.execute(
        """
        SELECT
          r.ohlcv_series_id,
          r.provider,
          r.snapshot_as_of_date,
          r.ticker,
          r.active_flag,
          r.company_name,
          r.cik,
          r.composite_figi,
          r.share_class_figi,
          r.security_type,
          r.primary_exchange,
          r.identity_status,
          l.natural_key,
          r.provisional_key,
          r.raw_json
        FROM reference_universe_snapshots r
        JOIN ohlcv_series_id_lookup l ON l.ohlcv_series_id = r.ohlcv_series_id
        """
    ).fetchall()


def _sqlite_table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table,),
    ).fetchone()
    return row is not None


def _active_from_flag(value: Any) -> bool | None:
    if value == 1:
        return True
    if value == 0:
        return False
    return None


def _json_alias_raw(value: Any) -> Any:
    if not value:
        return {}
    try:
        return json.loads(str(value))
    except ValueError:
        return {}


def _replace_score(
    candidate: IdentitySearchCandidate, rank: int, reason: str
) -> IdentitySearchCandidate:
    return IdentitySearchCandidate(
        ohlcv_series_id=candidate.ohlcv_series_id,
        active=candidate.active,
        ticker=candidate.ticker,
        company_name=candidate.company_name,
        cik=candidate.cik,
        composite_figi=candidate.composite_figi,
        share_class_figi=candidate.share_class_figi,
        security_type=candidate.security_type,
        primary_exchange=candidate.primary_exchange,
        identity_status=candidate.identity_status,
        as_of_date=candidate.as_of_date,
        match_rank=rank,
        match_reason=reason,
        source=candidate.source,
        lookup_status=candidate.lookup_status,
        natural_key=candidate.natural_key,
        provisional_key=candidate.provisional_key,
        raw=unfreeze_json(candidate.raw),
    )


def _score_snapshot(query: str, snapshot: ReferenceSnapshot) -> tuple[int, str]:
    raw = snapshot.to_payload()
    raw_result = raw.get("raw") if isinstance(raw.get("raw"), dict) else {}
    candidate = IdentitySearchCandidate(
        ohlcv_series_id=None,
        active=snapshot.active,
        ticker=snapshot.response_ticker or snapshot.ticker,
        company_name=str(raw_result.get("name") or ""),
        cik=snapshot.cik,
        composite_figi=snapshot.composite_figi,
        share_class_figi=snapshot.share_class_figi,
        security_type=snapshot.security_type,
        primary_exchange=snapshot.primary_exchange,
        identity_status="",
        as_of_date=snapshot.as_of_date.isoformat(),
        match_rank=99,
        match_reason="unknown",
        source="massive.reference_tickers",
        lookup_status="not_looked_up",
    )
    return _score_candidate(query, candidate)


def _score_candidate(query: str, candidate: IdentitySearchCandidate) -> tuple[int, str]:
    q = _normalized_query(query)
    q_lower = q.lower()
    ticker = candidate.ticker
    ticker_lc = ticker.lower()
    company_lc = candidate.company_name.lower()
    company_words = _word_set(candidate.company_name)
    query_words = _word_list(q)
    cik_lc = candidate.cik.lower()
    composite_lc = candidate.composite_figi.lower()
    share_lc = candidate.share_class_figi.lower()
    security_type_lc = candidate.security_type.lower()
    if candidate.ohlcv_series_id is not None and str(candidate.ohlcv_series_id) == q:
        return 0, "ohlcv_series_id_exact"
    if ticker == q:
        return 1, "ticker_exact_case"
    if ticker_lc == q_lower:
        return 2, "ticker_exact_case_insensitive"
    if ticker_lc.startswith(q_lower):
        return 3, "ticker_prefix"
    if q_lower in ticker_lc:
        return 4, "ticker_contains"
    if cik_lc == q_lower or (_looks_like_cik(q) and cik_lc == _zero_pad_cik(q).lower()):
        return 5, "cik_exact"
    if composite_lc == q_lower:
        return 5, "composite_figi_exact"
    if share_lc == q_lower:
        return 5, "share_class_figi_exact"
    if company_lc == q_lower:
        return 6, "company_name_exact"
    if query_words and all(word in company_words for word in query_words):
        return 7, "company_name_word"
    if query_words and any(
        any(word.startswith(query_word) for word in company_words)
        for query_word in query_words
    ):
        return 8, "company_name_word_prefix"
    if q_lower and (
        q_lower in cik_lc or q_lower in composite_lc or q_lower in share_lc
    ):
        return 9, "identifier_contains"
    if q_lower and q_lower in security_type_lc:
        return 10, "security_type_contains"
    if q_lower and q_lower in company_lc:
        return 11, "company_name_contains"
    return 99, "unknown"


def _issuer_enrichment_seeds(
    query: str,
    candidates: list[IdentitySearchCandidate],
    *,
    max_ciks: int = ISSUER_ENRICHMENT_MAX_CIKS,
) -> list[IdentitySearchCandidate]:
    if _looks_like_cik(query):
        return []
    seeds: list[IdentitySearchCandidate] = []
    seen_ciks: set[str] = set()
    for candidate in candidates:
        cik = _candidate_cik(candidate)
        if not cik or cik in seen_ciks:
            continue
        if not _is_issuer_enrichment_seed(candidate):
            continue
        seeds.append(candidate)
        seen_ciks.add(cik)
        if len(seeds) >= max_ciks:
            break
    return seeds


def _is_issuer_enrichment_seed(candidate: IdentitySearchCandidate) -> bool:
    if not _is_issuer_enrichment_security(candidate):
        return False
    return candidate.match_reason in {
        "ticker_exact_case",
        "ticker_exact_case_insensitive",
        "cik_exact",
        "composite_figi_exact",
        "share_class_figi_exact",
        "company_name_exact",
        "company_name_word",
    }


def _is_issuer_enrichment_security(candidate: IdentitySearchCandidate) -> bool:
    return (candidate.security_type or "").upper() in ISSUER_ENRICHMENT_SECURITY_TYPES


def _candidate_cik(candidate: IdentitySearchCandidate) -> str:
    cik = str(candidate.cik or "").strip()
    if not _looks_like_cik(cik):
        return ""
    return _zero_pad_cik(cik)


def _issuer_enrichment_record(
    seed: IdentitySearchCandidate,
    *,
    cik: str,
    source: str,
    returned_count: int,
) -> dict[str, Any]:
    return {
        "kind": "issuer_cik_enrichment",
        "source": source,
        "query": cik,
        "seed_ticker": seed.ticker,
        "seed_company_name": seed.company_name,
        "seed_match_reason": seed.match_reason,
        "returned_count": returned_count,
        "reason": "A strong operating-company candidate had a CIK, so the resolver searched the issuer CIK to reveal related listed share classes.",
    }


def _ranked_unique(
    candidates: list[IdentitySearchCandidate], limit: int
) -> list[IdentitySearchCandidate]:
    unique: dict[tuple[Any, str, str], IdentitySearchCandidate] = {}
    for candidate in candidates:
        key = (
            candidate.ohlcv_series_id
            if candidate.ohlcv_series_id is not None
            else candidate.natural_key,
            candidate.ticker,
            candidate.as_of_date,
        )
        existing = unique.get(key)
        if existing is None or _sort_key(candidate) < _sort_key(existing):
            unique[key] = candidate
    return sorted(unique.values(), key=_sort_key)[:limit]


def _sort_key(candidate: IdentitySearchCandidate) -> tuple[Any, ...]:
    active = (
        -1 if candidate.active in (True, 1) else 0 if candidate.active is None else 1
    )
    return (
        candidate.match_rank,
        active,
        _reverse_date(candidate.as_of_date),
        len(candidate.ticker or ""),
        candidate.ticker,
        candidate.ohlcv_series_id if candidate.ohlcv_series_id is not None else 0,
    )


def _reverse_date(value: str) -> str:
    return "".join(chr(255 - ord(char)) for char in value)


def _normalized_query(query: str) -> str:
    normalized = query.strip()
    if not normalized:
        raise ValueError("query is required")
    return normalized


def _looks_like_cik(query: str) -> bool:
    return bool(re.fullmatch(r"\d{1,10}", query.strip()))


def _looks_like_ticker_symbol(query: str) -> bool:
    stripped = query.strip()
    return bool(re.fullmatch(r"[A-Za-z0-9.\-]{1,6}", stripped))


def _zero_pad_cik(query: str) -> str:
    return query.strip().zfill(10)


def _word_list(value: str) -> list[str]:
    return re.findall(r"[a-z0-9]+", value.lower())


def _word_set(value: str) -> set[str]:
    return set(_word_list(value))
