"""Markdown renderer compatible with the backfill plan report."""

from __future__ import annotations

from typing import Any

from stock_universe.domain import BackfillPlan


def render_backfill_plan_markdown(plan: BackfillPlan | dict[str, Any]) -> str:
    payload = plan.to_payload() if isinstance(plan, BackfillPlan) else plan
    lines = [
        "# Backfill Plan",
        "",
        f"Generated: {payload['generated_at_utc']}",
        f"Status: `{payload['status']}`",
        "",
        "## Target",
        "",
        f"- `ohlcv_series_id`: {payload['target']['ohlcv_series_id']}",
        f"- `company`: {payload['target'].get('company_name') or ''}",
        f"- `CIK`: {payload['target'].get('cik') or ''}",
        f"- `composite_figi`: {payload['target'].get('composite_figi') or ''}",
        f"- `share_class_figi`: {payload['target'].get('share_class_figi') or ''}",
        f"- `identity_status`: {payload['target'].get('identity_status') or ''}",
        "",
        "## Requested Range",
        "",
        f"- `{payload['range']['from_date']}` to `{payload['range']['to_date']}`",
        f"- `{payload['range']['multiplier']}` `{payload['range']['timespan']}` adjusted={payload['range']['adjusted']}",
        "",
        "## Segments",
        "",
        "| # | Ticker | From | To | Source | Valid | Validation |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    for segment in payload["segments"]:
        validation = "; ".join(
            f"{check['point']}:{check['match_reason']}"
            for check in segment.get("validation", [])
        )
        lines.append(
            f"| {segment['segment_index']} | {segment['ticker']} | {segment['from_date']} | {segment['to_date']} | "
            f"{segment.get('source', '')} | {segment.get('valid')} | {validation} |"
        )
    if payload.get("warnings"):
        lines.extend(["", "## Warnings", ""])
        lines.extend(f"- {warning}" for warning in payload["warnings"])
    if payload.get("errors"):
        lines.extend(["", "## Errors", ""])
        lines.extend(f"- {error}" for error in payload["errors"])
    return "\n".join(lines) + "\n"
