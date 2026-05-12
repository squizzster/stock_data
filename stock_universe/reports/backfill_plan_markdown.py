"""Markdown renderer compatible with the legacy backfill plan report."""

from __future__ import annotations

from typing import Any

from stock_universe.domain import BackfillPlan


def render_backfill_plan_markdown(plan: BackfillPlan | dict[str, Any]) -> str:
    legacy = plan.to_legacy_dict() if isinstance(plan, BackfillPlan) else plan
    lines = [
        "# Backfill Plan",
        "",
        f"Generated: {legacy['generated_at_utc']}",
        f"Status: `{legacy['status']}`",
        "",
        "## Target",
        "",
        f"- `ohlcv_series_id`: {legacy['target']['ohlcv_series_id']}",
        f"- `company`: {legacy['target'].get('company_name') or ''}",
        f"- `CIK`: {legacy['target'].get('cik') or ''}",
        f"- `composite_figi`: {legacy['target'].get('composite_figi') or ''}",
        f"- `share_class_figi`: {legacy['target'].get('share_class_figi') or ''}",
        f"- `identity_status`: {legacy['target'].get('identity_status') or ''}",
        "",
        "## Requested Range",
        "",
        f"- `{legacy['range']['from_date']}` to `{legacy['range']['to_date']}`",
        f"- `{legacy['range']['multiplier']}` `{legacy['range']['timespan']}` adjusted={legacy['range']['adjusted']}",
        "",
        "## Segments",
        "",
        "| # | Ticker | From | To | Source | Valid | Validation |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    for segment in legacy["segments"]:
        validation = "; ".join(
            f"{check['point']}:{check['match_reason']}"
            for check in segment.get("validation", [])
        )
        lines.append(
            f"| {segment['segment_index']} | {segment['ticker']} | {segment['from_date']} | {segment['to_date']} | "
            f"{segment.get('source', '')} | {segment.get('valid')} | {validation} |"
        )
    if legacy.get("warnings"):
        lines.extend(["", "## Warnings", ""])
        lines.extend(f"- {warning}" for warning in legacy["warnings"])
    if legacy.get("errors"):
        lines.extend(["", "## Errors", ""])
        lines.extend(f"- {error}" for error in legacy["errors"])
    return "\n".join(lines) + "\n"
