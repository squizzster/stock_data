"""Read-only data-quality audit for the stock universe SQLite database."""

from __future__ import annotations

import sqlite3
from collections import Counter
from pathlib import Path
from typing import Any

from stock_universe.defaults import DEFAULT_MAX_ROUNDS
from stock_universe.domain import normalize_bar_grain
from stock_universe.paths import CANONICAL_DB_PATH
from stock_universe.storage.sqlite_access import (
    connect_readonly_sqlite,
    readonly_db_uri,
)

ISSUE_CATEGORIES = {
    "approved_plan_missing_receipt",
    "bar_expected_but_missing",
    "covered_series_data_stale",
    "data_not_loaded",
    "execution_error",
    "listed_common_stock_data_stale",
    "no_action_needed",
    "plan_session_gap",
    "provider_not_authorized",
    "provider_zero_bar_response_stale",
}


def quality_audit(
    db_path: str | Path | None = None,
    *,
    stale_before: str | None = None,
    limit: int = 50,
    categories: tuple[str, ...] = (),
    exchanges: tuple[str, ...] = (),
    security_types: tuple[str, ...] = (),
    series_ids: tuple[int, ...] = (),
    tickers: tuple[str, ...] = (),
    include_healthy: bool = False,
    bar_grain: str = "1d",
) -> dict[str, Any]:
    db = Path(db_path or CANONICAL_DB_PATH)
    grain = normalize_bar_grain(bar_grain)
    with connect_readonly_sqlite(db) as conn:
        global_min_bar = _scalar(
            conn,
            "SELECT COALESCE(MIN(bar_date), '') FROM v_ohlcv_bars_unified WHERE multiplier = ? AND timespan = ?",
            (grain.multiplier, grain.timespan),
        )
        global_max_bar = _scalar(
            conn,
            "SELECT COALESCE(MAX(bar_date), '') FROM v_ohlcv_bars_unified WHERE multiplier = ? AND timespan = ?",
            (grain.multiplier, grain.timespan),
        )
        effective_stale_before = stale_before or str(global_max_bar or "")
        rows = [
            _classify_row(
                dict(row), effective_stale_before, str(global_min_bar or ""), grain
            )
            for row in conn.execute(
                _audit_sql(), _audit_params(grain, effective_stale_before)
            ).fetchall()
        ]
    filtered = [
        row
        for row in rows
        if _matches_filters(
            row,
            categories=categories,
            exchanges=exchanges,
            security_types=security_types,
            series_ids=series_ids,
            tickers=tickers,
            include_healthy=include_healthy,
        )
    ]
    filtered.sort(key=_issue_sort_key)
    issue_rows = filtered[: max(0, limit)]
    all_counts = Counter(row["category"] for row in rows)
    filtered_counts = Counter(row["category"] for row in filtered)
    issue_count = sum(
        count
        for category, count in filtered_counts.items()
        if category != "no_action_needed"
    )
    unfiltered_issue_count = sum(
        count
        for category, count in all_counts.items()
        if category != "no_action_needed"
    )
    return {
        "db": str(db),
        "bar_grain": grain.bar_grain,
        "multiplier": grain.multiplier,
        "timespan": grain.timespan,
        "latest_reference_snapshot_as_of_date": max(
            (row["snapshot_as_of_date"] for row in rows), default=""
        ),
        "global_min_bar_date": global_min_bar,
        "global_max_bar_date": global_max_bar,
        "stale_before": effective_stale_before,
        "active_reference_series": len(rows),
        "matched_series_count": len(filtered),
        "issue_count": issue_count,
        "category_counts": dict(sorted(filtered_counts.items())),
        "unfiltered_issue_count": unfiltered_issue_count,
        "unfiltered_category_counts": dict(sorted(all_counts.items())),
        "filters": {
            "categories": list(categories),
            "exchanges": list(exchanges),
            "include_healthy": include_healthy,
            "limit": limit,
            "security_types": list(security_types),
            "ohlcv_series_ids": list(series_ids),
            "tickers": list(tickers),
            "bar_grain": grain.bar_grain,
        },
        "issues": issue_rows,
        "next_actions": _next_actions(
            issue_rows,
            db=str(db),
            global_min_bar_date=str(global_min_bar or ""),
            bar_grain=grain.bar_grain,
        ),
    }


def _audit_params(grain: Any, stale_before: str) -> tuple[Any, ...]:
    daily_orphan_receipts = 1 if grain.bar_grain == "1d" else 0
    return (
        grain.multiplier,
        grain.timespan,
        grain.multiplier,
        grain.timespan,
        grain.multiplier,
        grain.timespan,
        stale_before,
        stale_before,
        stale_before,
        stale_before,
        stale_before,
        stale_before,
        daily_orphan_receipts,
    )


def _audit_sql() -> str:
    return """
        WITH latest_snapshot AS (
          SELECT COALESCE(MAX(snapshot_as_of_date), '') AS snapshot_as_of_date
          FROM reference_universe_snapshots
          WHERE active_flag = 1
        ),
        active_ref AS (
          SELECT r.*
          FROM reference_universe_snapshots r
          JOIN latest_snapshot ls ON ls.snapshot_as_of_date = r.snapshot_as_of_date
          WHERE r.active_flag = 1
        ),
        bars AS (
          SELECT
            ohlcv_series_id,
            COUNT(*) AS bar_count,
            COUNT(DISTINCT market_session_id) AS loaded_session_count,
            MIN(bar_date) AS min_bar_date,
            MAX(bar_date) AS max_bar_date
          FROM v_ohlcv_bars_unified
          WHERE multiplier = ? AND timespan = ?
          GROUP BY ohlcv_series_id
        ),
        plan_scope AS (
          SELECT *
          FROM backfill_plans
          WHERE CAST(COALESCE(json_extract(plan_json, '$.range.multiplier'), 1) AS INTEGER) = ?
            AND COALESCE(json_extract(plan_json, '$.range.timespan'), 'day') = ?
        ),
        plan_expected_sessions AS (
          SELECT
            p.ohlcv_series_id,
            ms.market_session_id,
            ms.session_date
          FROM plan_scope p
          JOIN active_ref ar ON ar.ohlcv_series_id = p.ohlcv_series_id
          JOIN market_sessions ms
            ON ms.calendar_id = COALESCE(
              NULLIF(json_extract(p.plan_json, '$.target.latest_primary_exchange'), ''),
              NULLIF(ar.primary_exchange, ''),
              'US_EQUITY'
            )
           AND ms.session_date BETWEEN json_extract(p.plan_json, '$.range.from_date')
                                  AND json_extract(p.plan_json, '$.range.to_date')
        ),
        plan_loaded_sessions AS (
          SELECT DISTINCT
            ohlcv_series_id,
            market_session_id
          FROM v_ohlcv_bars_unified
          WHERE multiplier = ? AND timespan = ?
        ),
        plan_session_agg AS (
          SELECT
            ps.ohlcv_series_id,
            COUNT(DISTINCT ps.market_session_id) AS plan_expected_session_count,
            COUNT(DISTINCT pls.market_session_id) AS plan_loaded_session_count,
            COUNT(DISTINCT CASE
              WHEN pls.market_session_id IS NULL THEN ps.market_session_id
            END) AS plan_total_missing_session_count,
            COUNT(DISTINCT CASE
              WHEN pls.market_session_id IS NULL
               AND (COALESCE(?, '') = '' OR ps.session_date <= ?)
              THEN ps.market_session_id
            END) AS plan_missing_session_count,
            MIN(CASE
              WHEN pls.market_session_id IS NULL
               AND (COALESCE(?, '') = '' OR ps.session_date <= ?)
              THEN ps.session_date
            END) AS first_missing_session_date,
            MAX(CASE
              WHEN pls.market_session_id IS NULL
               AND (COALESCE(?, '') = '' OR ps.session_date <= ?)
              THEN ps.session_date
            END) AS last_missing_session_date
          FROM plan_expected_sessions ps
          LEFT JOIN plan_loaded_sessions pls
            ON pls.ohlcv_series_id = ps.ohlcv_series_id
           AND pls.market_session_id = ps.market_session_id
          GROUP BY ps.ohlcv_series_id
        ),
        receipt_scope AS (
          SELECT r.*
          FROM execution_receipts r
          LEFT JOIN plan_scope p
            ON p.request_hash = r.request_hash
           AND p.evidence_ledger_hash = r.evidence_ledger_hash
           AND p.ohlcv_series_id = r.ohlcv_series_id
          LEFT JOIN backfill_plans any_plan
            ON any_plan.request_hash = r.request_hash
           AND any_plan.evidence_ledger_hash = r.evidence_ledger_hash
           AND any_plan.ohlcv_series_id = r.ohlcv_series_id
          WHERE p.plan_id IS NOT NULL
             OR (? = 1 AND any_plan.plan_id IS NULL)
        ),
        receipt_agg AS (
          SELECT
            ohlcv_series_id,
            COUNT(*) AS receipt_count,
            SUM(CASE WHEN status = 'ok' THEN 1 ELSE 0 END) AS ok_receipt_count,
            SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) AS execution_error_receipt_count,
            SUM(CASE WHEN status = 'ok' AND fetched_bar_count = 0 AND inserted_bar_count = 0 THEN 1 ELSE 0 END)
              AS provider_zero_bar_receipt_count
          FROM receipt_scope
          GROUP BY ohlcv_series_id
        ),
        receipt_ranked AS (
          SELECT
            r.*,
            ROW_NUMBER() OVER (
              PARTITION BY r.ohlcv_series_id
              ORDER BY r.started_at_utc DESC, r.execution_receipt_id DESC
            ) AS rn
          FROM receipt_scope r
        ),
        last_receipt AS (
          SELECT *
          FROM receipt_ranked
          WHERE rn = 1
        ),
        plan_agg AS (
          SELECT
            ohlcv_series_id,
            COUNT(*) AS plan_count,
            MAX(plan_id) AS last_plan_id,
            MAX(created_at_utc) AS last_plan_created_at_utc
          FROM backfill_plans
          WHERE plan_id IN (SELECT plan_id FROM plan_scope)
          GROUP BY ohlcv_series_id
        ),
        approved_missing AS (
          SELECT
            p.ohlcv_series_id,
            COUNT(*) AS approved_plan_missing_receipt_count,
            MAX(a.execution_approval_id) AS last_missing_approval_id
          FROM plan_scope p
          JOIN execution_approvals a
            ON a.request_hash = p.request_hash
           AND a.evidence_ledger_hash = p.evidence_ledger_hash
           AND a.plan_hash = p.plan_hash
           AND a.ohlcv_series_id = p.ohlcv_series_id
          LEFT JOIN execution_receipts r
            ON r.request_hash = p.request_hash
           AND r.evidence_ledger_hash = p.evidence_ledger_hash
           AND r.ohlcv_series_id = p.ohlcv_series_id
          WHERE r.execution_receipt_id IS NULL
          GROUP BY p.ohlcv_series_id
        )
        SELECT
          ar.ohlcv_series_id,
          ar.ticker,
          ar.company_name,
          ar.security_type,
          ar.primary_exchange,
          ar.market,
          ar.locale,
          ar.cik,
          ar.composite_figi,
          ar.share_class_figi,
          ar.snapshot_as_of_date,
          COALESCE(b.bar_count, 0) AS bar_count,
          COALESCE(b.loaded_session_count, 0) AS loaded_session_count,
          COALESCE(b.min_bar_date, '') AS min_bar_date,
          COALESCE(b.max_bar_date, '') AS max_bar_date,
          COALESCE(psa.plan_expected_session_count, 0) AS plan_expected_session_count,
          COALESCE(psa.plan_loaded_session_count, 0) AS plan_loaded_session_count,
          COALESCE(psa.plan_total_missing_session_count, 0) AS plan_total_missing_session_count,
          COALESCE(psa.plan_missing_session_count, 0) AS plan_missing_session_count,
          COALESCE(psa.first_missing_session_date, '') AS first_missing_session_date,
          COALESCE(psa.last_missing_session_date, '') AS last_missing_session_date,
          COALESCE(pa.plan_count, 0) AS plan_count,
          COALESCE(pa.last_plan_id, 0) AS last_plan_id,
          COALESCE(pa.last_plan_created_at_utc, '') AS last_plan_created_at_utc,
          COALESCE(ra.receipt_count, 0) AS receipt_count,
          COALESCE(ra.ok_receipt_count, 0) AS ok_receipt_count,
          COALESCE(ra.execution_error_receipt_count, 0) AS execution_error_receipt_count,
          COALESCE(ra.provider_zero_bar_receipt_count, 0) AS provider_zero_bar_receipt_count,
          COALESCE(lr.execution_receipt_id, 0) AS last_receipt_id,
          COALESCE(lr.status, '') AS last_receipt_status,
          COALESCE(lr.fetched_bar_count, 0) AS last_fetched_bar_count,
          COALESCE(lr.inserted_bar_count, 0) AS last_inserted_bar_count,
          COALESCE(lr.started_at_utc, '') AS last_receipt_started_at_utc,
          COALESCE(lr.finished_at_utc, '') AS last_receipt_finished_at_utc,
          COALESCE(json_extract(lr.receipt_json, '$.skip_reason'), '') AS last_receipt_skip_reason,
          COALESCE(json_extract(lr.receipt_json, '$.provider_status'), '') AS last_receipt_provider_status,
          COALESCE(json_extract(lr.receipt_json, '$.error_message'), '') AS last_receipt_error_message,
          COALESCE(am.approved_plan_missing_receipt_count, 0) AS approved_plan_missing_receipt_count,
          COALESCE(am.last_missing_approval_id, 0) AS last_missing_approval_id
        FROM active_ref ar
        LEFT JOIN bars b ON b.ohlcv_series_id = ar.ohlcv_series_id
        LEFT JOIN plan_session_agg psa ON psa.ohlcv_series_id = ar.ohlcv_series_id
        LEFT JOIN plan_agg pa ON pa.ohlcv_series_id = ar.ohlcv_series_id
        LEFT JOIN receipt_agg ra ON ra.ohlcv_series_id = ar.ohlcv_series_id
        LEFT JOIN last_receipt lr ON lr.ohlcv_series_id = ar.ohlcv_series_id
        LEFT JOIN approved_missing am ON am.ohlcv_series_id = ar.ohlcv_series_id
    """


def _classify_row(
    row: dict[str, Any], stale_before: str, global_min_bar_date: str, grain: Any
) -> dict[str, Any]:
    category = "no_action_needed"
    if int(row["approved_plan_missing_receipt_count"] or 0) > 0:
        category = "approved_plan_missing_receipt"
    elif _is_provider_entitlement_receipt(row):
        category = "provider_not_authorized"
    elif str(row["last_receipt_status"] or "") == "error":
        category = "execution_error"
    elif _has_fresh_zero_bar_receipt(row, stale_before):
        category = "no_action_needed"
    elif (
        int(row["provider_zero_bar_receipt_count"] or 0) > 0
        and int(row["bar_count"] or 0) == 0
    ):
        category = "provider_zero_bar_response_stale"
    elif int(row["bar_count"] or 0) == 0 and int(row["plan_count"] or 0) == 0:
        category = "data_not_loaded"
    elif int(row["bar_count"] or 0) == 0:
        category = "bar_expected_but_missing"
    elif int(row["plan_missing_session_count"] or 0) > 0:
        category = "plan_session_gap"
    elif stale_before and str(row["max_bar_date"] or "") < stale_before:
        if row["security_type"] == "CS" and row["primary_exchange"] in {
            "XASE",
            "XNAS",
            "XNYS",
        }:
            category = "listed_common_stock_data_stale"
        else:
            category = "covered_series_data_stale"
    row["category"] = category
    row["bar_grain"] = grain.bar_grain
    row["multiplier"] = grain.multiplier
    row["timespan"] = grain.timespan
    row["actual_result"] = _row_actual_result(row, category)
    row["repair_needed"] = category in {
        "approved_plan_missing_receipt",
        "bar_expected_but_missing",
        "covered_series_data_stale",
        "data_not_loaded",
        "execution_error",
        "listed_common_stock_data_stale",
        "plan_session_gap",
        "provider_zero_bar_response_stale",
    }
    row["suggested_next_command"] = _suggested_command(
        row, global_min_bar_date, bar_grain=grain.bar_grain
    )
    return row


def _row_actual_result(row: dict[str, Any], category: str) -> str:
    if (
        category == "no_action_needed"
        and int(row["bar_count"] or 0) == 0
        and int(row["provider_zero_bar_receipt_count"] or 0) > 0
    ):
        return "series_not_covered"
    return category


def _is_provider_entitlement_receipt(row: dict[str, Any]) -> bool:
    status = str(row["last_receipt_status"] or "")
    skip_reason = str(row["last_receipt_skip_reason"] or "")
    provider_status = str(row["last_receipt_provider_status"] or "")
    error_message = str(row.get("last_receipt_error_message") or "")
    if status == "skipped" and skip_reason == "provider_not_authorized":
        return True
    return status == "error" and (
        provider_status == "NOT_AUTHORIZED"
        or "provider status NOT_AUTHORIZED" in error_message
    )


def _has_fresh_zero_bar_receipt(row: dict[str, Any], stale_before: str) -> bool:
    if not stale_before:
        return False
    if str(row["last_receipt_status"] or "") != "ok":
        return False
    if (
        int(row["last_fetched_bar_count"] or 0) != 0
        or int(row["last_inserted_bar_count"] or 0) != 0
    ):
        return False
    receipt_date = str(
        row["last_receipt_finished_at_utc"] or row["last_receipt_started_at_utc"] or ""
    )[:10]
    return bool(receipt_date and receipt_date >= stale_before)


def _suggested_command(
    row: dict[str, Any], global_min_bar_date: str, *, bar_grain: str
) -> str:
    series_id = int(row["ohlcv_series_id"])
    first_missing = str(row.get("first_missing_session_date") or "")
    if str(row["category"]) == "plan_session_gap" and first_missing:
        from_arg = f" --from-date {first_missing}"
    else:
        from_arg = f" --from-date {global_min_bar_date}" if global_min_bar_date else ""
    grain_arg = "" if bar_grain == "1d" else f" --bar-grain {bar_grain}"
    category = str(row["category"])
    if category == "approved_plan_missing_receipt":
        return f"stock-universe repair-missing-receipts --ohlcv-series-id {series_id} --commit"
    if category in {
        "bar_expected_but_missing",
        "covered_series_data_stale",
        "data_not_loaded",
        "listed_common_stock_data_stale",
        "plan_session_gap",
        "provider_zero_bar_response_stale",
    }:
        return f"stock-universe xctx dry-run --ohlcv-series-id {series_id}{from_arg}{grain_arg} --max-rounds {DEFAULT_MAX_ROUNDS}"
    if category == "execution_error":
        return f"stock-universe xctx observe --ohlcv-series-id {series_id} --limit 5"
    if category == "provider_not_authorized":
        return f"stock-universe xctx observe --ohlcv-series-id {series_id} --limit 5"
    return f"stock-universe xctx dry-run --ohlcv-series-id {series_id}{grain_arg}"


def _matches_filters(
    row: dict[str, Any],
    *,
    categories: tuple[str, ...],
    exchanges: tuple[str, ...],
    security_types: tuple[str, ...],
    series_ids: tuple[int, ...],
    tickers: tuple[str, ...],
    include_healthy: bool,
) -> bool:
    if not include_healthy and row["category"] == "no_action_needed":
        return False
    if categories and row["category"] not in categories:
        return False
    if exchanges and row["primary_exchange"] not in exchanges:
        return False
    if security_types and row["security_type"] not in security_types:
        return False
    if series_ids and int(row["ohlcv_series_id"]) not in series_ids:
        return False
    normalized_tickers = {ticker.upper() for ticker in tickers}
    if normalized_tickers and str(row["ticker"]).upper() not in normalized_tickers:
        return False
    return True


def _issue_sort_key(row: dict[str, Any]) -> tuple[int, str, int]:
    priority = {
        "approved_plan_missing_receipt": 0,
        "execution_error": 1,
        "plan_session_gap": 2,
        "listed_common_stock_data_stale": 3,
        "provider_zero_bar_response_stale": 4,
        "bar_expected_but_missing": 5,
        "data_not_loaded": 6,
        "provider_not_authorized": 7,
        "covered_series_data_stale": 8,
        "no_action_needed": 9,
    }
    return (
        priority.get(str(row["category"]), 99),
        str(row["max_bar_date"] or ""),
        int(row["ohlcv_series_id"]),
    )


def _next_actions(
    rows: list[dict[str, Any]], *, db: str, global_min_bar_date: str, bar_grain: str
) -> list[dict[str, Any]]:
    actions: list[dict[str, Any]] = []
    seen_categories: set[str] = set()
    for row in rows:
        category = str(row["category"])
        if category in seen_categories:
            continue
        seen_categories.add(category)
        actions.append(
            _next_action_for_row(
                row, db=db, global_min_bar_date=global_min_bar_date, bar_grain=bar_grain
            )
        )
    return actions


def _next_action_for_row(
    row: dict[str, Any], *, db: str, global_min_bar_date: str, bar_grain: str = "1d"
) -> dict[str, Any]:
    category = str(row["category"])
    series_id = int(row["ohlcv_series_id"])
    ticker = str(row["ticker"] or "")
    if category == "approved_plan_missing_receipt":
        argv = [
            "./stock_universe.cli",
            "repair-missing-receipts",
            "--db",
            db,
            "--ohlcv-series-id",
            str(series_id),
            "--commit",
        ]
        return _action(
            name="repair-missing-execution-receipts",
            command_name="stock-universe repair-missing-receipts",
            description="Persist durable error receipts for approved plans missing receipts.",
            args={"db": db, "ohlcv_series_id": [series_id], "commit": True},
            reads=[db],
            writes=[db],
            effects=[
                {
                    "kind": "write",
                    "target": db,
                    "description": "Insert missing-receipt repair rows.",
                }
            ],
            requires_approval=True,
            category=category,
            series_id=series_id,
            ticker=ticker,
            argv=argv,
            reason="Approved-plan accounting issues require the repair workflow.",
        )
    if category in {
        "bar_expected_but_missing",
        "covered_series_data_stale",
        "data_not_loaded",
        "listed_common_stock_data_stale",
        "plan_session_gap",
        "provider_zero_bar_response_stale",
    }:
        argv = [
            "./stock_universe.cli",
            "xctx",
            "dry-run",
            "--ohlcv-series-id",
            str(series_id),
            "--db",
            db,
        ]
        args: dict[str, Any] = {
            "db": db,
            "ohlcv_series_id": series_id,
            "max_rounds": DEFAULT_MAX_ROUNDS,
            "bar_grain": bar_grain,
        }
        from_date = (
            str(row.get("first_missing_session_date") or "")
            if category == "plan_session_gap"
            else global_min_bar_date
        )
        if from_date:
            argv.extend(["--from-date", from_date])
            args["from_date"] = from_date
        if bar_grain != "1d":
            argv.extend(["--bar-grain", bar_grain])
        argv.extend(["--max-rounds", str(DEFAULT_MAX_ROUNDS)])
        return _action(
            name="dry-run-ohlcv-series-backfill",
            command_name="xctx dry-run",
            description="Build a read-only evidence-backed backfill plan for the selected OHLCV series.",
            args=args,
            reads=[db, "Massive API"],
            writes=[],
            effects=[
                {
                    "kind": "read",
                    "target": db,
                    "description": "Read selected reference-universe identity.",
                },
                {
                    "kind": "read",
                    "target": "Massive API",
                    "description": "Collect planning evidence.",
                },
            ],
            requires_approval=False,
            category=category,
            series_id=series_id,
            ticker=ticker,
            argv=argv,
            reason="Executable quality category needs a safe dry-run before any write.",
        )
    argv = [
        "./stock_universe.cli",
        "xctx",
        "observe",
        "--db",
        db,
        "--ohlcv-series-id",
        str(series_id),
        "--limit",
        "5",
    ]
    return _action(
        name="observe-ohlcv-series-executions",
        command_name="xctx observe",
        description="Inspect recent execution receipts before retrying or overriding.",
        args={"db": db, "ohlcv_series_id": series_id, "limit": 5},
        reads=[db],
        writes=[],
        effects=[
            {"kind": "read", "target": db, "description": "Read execution receipts."}
        ],
        requires_approval=False,
        category=category,
        series_id=series_id,
        ticker=ticker,
        argv=argv,
        reason="Review-only category should be observed before retry.",
    )


def _action(
    *,
    name: str,
    command_name: str,
    description: str,
    args: dict[str, Any],
    reads: list[str],
    writes: list[str],
    effects: list[dict[str, str]],
    requires_approval: bool,
    category: str,
    series_id: int,
    ticker: str,
    argv: list[str],
    reason: str,
) -> dict[str, Any]:
    return {
        "name": name,
        "kind": "command",
        "category": category,
        "ohlcv_series_id": series_id,
        "ticker": ticker,
        "command": {
            "name": command_name,
            "description": description,
            "args": args,
            "reads": reads,
            "writes": writes,
        },
        "effects": effects,
        "requires_approval": requires_approval,
        "authority_level": _authority_level(
            effects=effects, requires_approval=requires_approval
        ),
        "reason": reason,
        "argv": argv,
        "source_checkout_argv": argv,
    }


def _authority_level(*, effects: list[dict[str, str]], requires_approval: bool) -> str:
    if any(effect.get("kind") == "write" for effect in effects):
        return "db_write" if requires_approval else "file_write"
    if any("api" in str(effect.get("target") or "").lower() for effect in effects):
        return "network_read"
    if any(effect.get("kind") == "read" for effect in effects):
        return "read"
    return "none"


def _scalar(conn: sqlite3.Connection, sql: str, params: tuple[Any, ...] = ()) -> Any:
    row = conn.execute(sql, params).fetchone()
    return row[0] if row else None


def _readonly_db_uri(path: Path) -> str:
    return readonly_db_uri(path)
