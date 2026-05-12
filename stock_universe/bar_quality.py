"""Daily OHLCV bar validation and targeted repair helpers."""

from __future__ import annotations

import datetime as dt
import math
from dataclasses import dataclass
from typing import Any


UNCHECKED = "UNCHECKED"
VALIDATED = "VALIDATED"
VALIDATED_REPAIRED = "VALIDATED_REPAIRED"
SUSPECT = "SUSPECT"
BEST_GUESS_DATA = "BEST_GUESS_DATA"
BEST_GUESS_COMPUTED = "BEST_GUESS_COMPUTED"
INVALID_MISSING = "INVALID_MISSING"
INVALID_CONFLICT = "INVALID_CONFLICT"
QUARANTINED = "QUARANTINED"

DAILY_BAR_STRUCTURAL_VALIDATION = "DAILY_BAR_STRUCTURAL_VALIDATION"
DAILY_HIGH_EXCEEDS_INTRADAY_ENVELOPE = "DAILY_HIGH_EXCEEDS_INTRADAY_ENVELOPE"
DAILY_LOW_BELOW_INTRADAY_ENVELOPE = "DAILY_LOW_BELOW_INTRADAY_ENVELOPE"
DAILY_OPEN_OUTSIDE_INTRADAY_ENVELOPE = "DAILY_OPEN_OUTSIDE_INTRADAY_ENVELOPE"
DAILY_CLOSE_OUTSIDE_INTRADAY_ENVELOPE = "DAILY_CLOSE_OUTSIDE_INTRADAY_ENVELOPE"
INTRADAY_EVIDENCE_INCOMPLETE = "INTRADAY_EVIDENCE_INCOMPLETE"


@dataclass(frozen=True)
class OhlcvValues:
    open: float | None
    high: float | None
    low: float | None
    close: float | None
    volume: float | None = None
    vwap: float | None = None
    transaction_count: int | None = None


@dataclass(frozen=True)
class IntradayEnvelope:
    ticker: str
    date: str
    multiplier: int
    timespan: str
    api_status: str
    row_count: int
    max_high: float | None
    min_low: float | None
    max_high_ts: int | None = None
    min_low_ts: int | None = None
    issues: tuple[str, ...] = ()

    @property
    def ok(self) -> bool:
        return (
            not self.issues
            and self.row_count > 0
            and self.max_high is not None
            and self.min_low is not None
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "ticker": self.ticker,
            "date": self.date,
            "multiplier": self.multiplier,
            "timespan": self.timespan,
            "api_status": self.api_status,
            "row_count": self.row_count,
            "max_high": self.max_high,
            "min_low": self.min_low,
            "max_high_ts": self.max_high_ts,
            "min_low_ts": self.min_low_ts,
            "ok": self.ok,
            "issues": list(self.issues),
        }


def structural_issues(values: OhlcvValues) -> tuple[str, ...]:
    issues: list[str] = []
    required = {
        "open": values.open,
        "high": values.high,
        "low": values.low,
        "close": values.close,
    }
    for name, value in required.items():
        if value is None:
            issues.append(f"{name}_missing")
        elif not is_finite_number(value):
            issues.append(f"{name}_non_finite")
    if issues:
        return tuple(issues)

    assert values.open is not None
    assert values.high is not None
    assert values.low is not None
    assert values.close is not None
    if values.high < max(values.open, values.low, values.close):
        issues.append("high_below_ohlc_component")
    if values.low > min(values.open, values.high, values.close):
        issues.append("low_above_ohlc_component")
    return tuple(issues)


def suspicion_reasons(values: OhlcvValues) -> tuple[str, ...]:
    if structural_issues(values):
        return ()
    assert values.open is not None
    assert values.high is not None
    assert values.low is not None
    assert values.close is not None
    reasons: list[str] = []
    reference_prices = [
        price for price in (values.open, values.close) if price and price > 0
    ]
    if reference_prices:
        reference = min(reference_prices)
        if values.high / reference >= 1.25:
            reasons.append("daily_high_extreme_vs_open_close")
    if values.close > 0 and values.low / values.close <= 0.75:
        reasons.append("daily_low_extreme_vs_close")
    if values.open > 0 and values.low / values.open <= 0.75:
        reasons.append("daily_low_extreme_vs_open")
    min_open_close = min(values.open, values.close)
    if min_open_close > 0 and max(values.open, values.close) / min_open_close >= 1.25:
        reasons.append("daily_open_close_gap_extreme")
    return tuple(reasons)


def status_for_structural_issues(issues: tuple[str, ...]) -> str:
    if any(
        issue.endswith("_missing") or issue.endswith("_non_finite") for issue in issues
    ):
        return INVALID_MISSING
    return INVALID_CONFLICT


def price_tolerance(price: float | None) -> float:
    if price is None or not is_finite_number(price):
        return 0.02
    abs_price = abs(price)
    tick_size = 0.01 if abs_price >= 1.0 else 0.0001
    return max(2.0 * tick_size, 0.0001 * abs_price)


def is_finite_number(value: Any) -> bool:
    return (
        isinstance(value, (int, float))
        and not isinstance(value, bool)
        and math.isfinite(float(value))
    )


def intraday_envelope_from_payload(
    *,
    ticker: str,
    date: str,
    multiplier: int,
    timespan: str,
    payload: dict[str, Any],
) -> IntradayEnvelope:
    issues: list[str] = []
    status = str(payload.get("status") or "")
    if status and status not in {"OK", "DELAYED"}:
        issues.append(f"provider_status_{status}")
    results = payload.get("results") or []
    if not isinstance(results, list):
        return IntradayEnvelope(
            ticker=ticker,
            date=date,
            multiplier=multiplier,
            timespan=timespan,
            api_status=status,
            row_count=0,
            max_high=None,
            min_low=None,
            issues=("non_list_results",),
        )
    if payload.get("next_url"):
        issues.append("pagination_next_url_present")
    if len(results) >= 50000:
        issues.append("result_limit_cap_reached")
    if not results:
        issues.append("no_intraday_rows")

    max_high: float | None = None
    min_low: float | None = None
    max_high_ts: int | None = None
    min_low_ts: int | None = None
    previous_ts: int | None = None
    window_start, window_end = _intraday_timestamp_window_ms(date)

    for index, item in enumerate(results):
        if not isinstance(item, dict):
            issues.append(f"row_{index}_non_object")
            continue
        try:
            ts = int(item["t"])
        except (KeyError, TypeError, ValueError):
            issues.append(f"row_{index}_timestamp_invalid")
            continue
        if previous_ts is not None and ts <= previous_ts:
            issues.append("timestamps_not_strictly_ascending")
        previous_ts = ts
        if ts < window_start or ts > window_end:
            issues.append("timestamp_outside_requested_window")
        high = _optional_float(item.get("h"))
        low = _optional_float(item.get("l"))
        open_ = _optional_float(item.get("o"))
        close = _optional_float(item.get("c"))
        if high is None or low is None:
            issues.append(f"row_{index}_missing_intraday_high_low")
            continue
        if high < low:
            issues.append(f"row_{index}_intraday_high_below_low")
        if open_ is not None and (open_ > high or open_ < low):
            issues.append(f"row_{index}_intraday_open_outside_high_low")
        if close is not None and (close > high or close < low):
            issues.append(f"row_{index}_intraday_close_outside_high_low")
        if max_high is None or high > max_high:
            max_high = high
            max_high_ts = ts
        if min_low is None or low < min_low:
            min_low = low
            min_low_ts = ts

    return IntradayEnvelope(
        ticker=ticker,
        date=date,
        multiplier=multiplier,
        timespan=timespan,
        api_status=status,
        row_count=len(results),
        max_high=max_high,
        min_low=min_low,
        max_high_ts=max_high_ts,
        min_low_ts=min_low_ts,
        issues=tuple(dict.fromkeys(issues)),
    )


def high_exceeds_envelope(daily_high: float | None, envelope: IntradayEnvelope) -> bool:
    if daily_high is None or envelope.max_high is None:
        return False
    return daily_high > envelope.max_high + price_tolerance(
        max(abs(daily_high), abs(envelope.max_high))
    )


def low_below_envelope(daily_low: float | None, envelope: IntradayEnvelope) -> bool:
    if daily_low is None or envelope.min_low is None:
        return False
    return daily_low < envelope.min_low - price_tolerance(
        max(abs(daily_low), abs(envelope.min_low))
    )


def component_outside_envelope(value: float | None, envelope: IntradayEnvelope) -> bool:
    if value is None or envelope.max_high is None or envelope.min_low is None:
        return False
    tolerance = price_tolerance(
        max(abs(value), abs(envelope.max_high), abs(envelope.min_low))
    )
    return value > envelope.max_high + tolerance or value < envelope.min_low - tolerance


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(parsed):
        return None
    return parsed


def _intraday_timestamp_window_ms(date: str) -> tuple[int, int]:
    day = dt.date.fromisoformat(date)
    start = dt.datetime.combine(day, dt.time(0, 0), dt.UTC)
    end = start + dt.timedelta(days=1, hours=6)
    return int(start.timestamp() * 1000), int(end.timestamp() * 1000)
