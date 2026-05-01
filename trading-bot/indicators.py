"""Technical indicators — all inputs are plain Python lists of floats."""

import math
from typing import List, Optional


def calc_ma(closes: List[float], period: int = 20) -> List[Optional[float]]:
    """Simple moving average. First period-1 values are None."""
    result: List[Optional[float]] = []
    for i in range(len(closes)):
        if i < period - 1:
            result.append(None)
        else:
            result.append(sum(closes[i - period + 1: i + 1]) / period)
    return result


def calc_rsi(closes: List[float], period: int = 14) -> List[Optional[float]]:
    """RSI using Wilder's exponential smoothing. First period values are None."""
    if len(closes) < period + 1:
        return [None] * len(closes)

    result: List[Optional[float]] = [None] * period
    gains, losses = [], []
    for i in range(1, period + 1):
        diff = closes[i] - closes[i - 1]
        gains.append(max(diff, 0.0))
        losses.append(max(-diff, 0.0))

    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period

    for i in range(period, len(closes)):
        if i > period:
            diff = closes[i] - closes[i - 1]
            gain = max(diff, 0.0)
            loss = max(-diff, 0.0)
            avg_gain = (avg_gain * (period - 1) + gain) / period
            avg_loss = (avg_loss * (period - 1) + loss) / period

        if avg_loss == 0:
            result.append(100.0)
        else:
            rs = avg_gain / avg_loss
            result.append(100.0 - (100.0 / (1.0 + rs)))

    return result


def calc_bollinger(
    closes: List[float], period: int = 20, std_dev: float = 2.0
) -> tuple[List[Optional[float]], List[Optional[float]], List[Optional[float]]]:
    """Returns (upper, mid, lower). First period-1 values are None."""
    mid = calc_ma(closes, period)
    upper: List[Optional[float]] = []
    lower: List[Optional[float]] = []

    for i in range(len(closes)):
        if i < period - 1 or mid[i] is None:
            upper.append(None)
            lower.append(None)
        else:
            window = closes[i - period + 1: i + 1]
            mean = mid[i]
            variance = sum((x - mean) ** 2 for x in window) / period
            std = math.sqrt(variance)
            upper.append(mean + std_dev * std)
            lower.append(mean - std_dev * std)

    return upper, mid, lower


def calc_volume_ma(volumes: List[float], period: int = 20) -> List[Optional[float]]:
    """Volume moving average."""
    return calc_ma(volumes, period)


def classify_ma_position(price: float, ma_value: Optional[float]) -> str:
    if ma_value is None or ma_value == 0:
        return "unknown"
    diff_pct = abs(price - ma_value) / ma_value
    if diff_pct <= 0.001:
        return "at"
    return "above" if price > ma_value else "below"


def classify_bb_position(
    price: float,
    upper: Optional[float],
    mid: Optional[float],
    lower: Optional[float],
) -> str:
    if upper is None or lower is None or mid is None:
        return "unknown"
    band_width = upper - lower
    if band_width == 0:
        return "mid_zone"
    near_pct = 0.01 * band_width
    if price > upper:
        return "above_upper"
    elif price >= upper - near_pct:
        return "near_upper"
    elif price <= lower:
        return "below_lower"
    elif price <= lower + near_pct:
        return "near_lower"
    else:
        return "mid_zone"


def classify_volume_trend(volumes: List[float], lookback: int = 3) -> str:
    if len(volumes) < lookback * 2:
        return "flat"
    recent_avg = sum(volumes[-lookback:]) / lookback
    prev_avg   = sum(volumes[-(lookback * 2):-lookback]) / lookback
    if prev_avg == 0:
        return "flat"
    ratio = recent_avg / prev_avg
    if ratio > 1.05:
        return "increasing"
    elif ratio < 0.95:
        return "decreasing"
    return "flat"
