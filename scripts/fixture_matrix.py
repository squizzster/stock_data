#!/usr/bin/env python3
"""Summarize backfill legacy plan fixtures without reading provider raw files."""

from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
FIXTURE_DIR = ROOT / "tests" / "fixtures" / "legacy_plans"


def fixture_rows() -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for path in sorted(FIXTURE_DIR.glob("*.json")):
        data = json.loads(path.read_text())
        target = data.get("target") or {}
        segments = data.get("segments") or []
        event_lookup = data.get("event_lookup") or {}
        request_to_date = (data.get("range") or {}).get("to_date")
        last_segment_to_date = max(
            (str(segment.get("to_date") or "") for segment in segments), default=""
        )
        rows.append(
            {
                "candidate_replacements": sum(
                    1 for segment in segments if segment.get("ticker_replacement")
                ),
                "handoff_segments": sum(
                    1 for segment in segments if segment.get("event_ticker_handoff")
                ),
                "event_count": len(event_lookup.get("events") or []),
                "fixture": path.name,
                "ohlcv_series_id": target.get("ohlcv_series_id"),
                "ticker": target.get("latest_ticker"),
                "status": data.get("status"),
                "segments": len(segments),
                "segment_tickers": ",".join(
                    str(segment.get("ticker") or "") for segment in segments
                ),
                "terminal_tail": bool(
                    request_to_date
                    and last_segment_to_date
                    and last_segment_to_date < request_to_date
                ),
                "warnings": len(data.get("warnings") or []),
                "errors": len(data.get("errors") or []),
            }
        )
    return rows


def main() -> int:
    print(json.dumps({"fixtures": fixture_rows()}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
