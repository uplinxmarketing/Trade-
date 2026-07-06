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


def calc_ema(values: List[float], period: int) -> List[Optional[float]]:
    """EMA as a full list. First period-1 entries are None."""
    result: List[Optional[float]] = [None] * (period - 1)
    if len(values) < period:
        return [None] * len(values)
    sma = sum(values[:period]) / period
    result.append(sma)
    k = 2 / (period + 1)
    for v in values[period:]:
        result.append(v * k + result[-1] * (1 - k))
    return result


def calc_macd(
    closes: List[float],
    fast: int = 12,
    slow: int = 26,
    signal: int = 9,
) -> tuple:
    """
    Returns (macd_line, sig_padded, histogram) — all lists aligned to closes.

    macd_line[i]  = ema_fast[i] - ema_slow[i], None if either is None
    sig_padded    = EMA(signal period) of the non-None macd values,
                    padded with Nones at the front to match len(closes)
    histogram[i]  = macd_line[i] - sig_padded[i], None if either is None
    """
    ema_fast = calc_ema(closes, fast)
    ema_slow = calc_ema(closes, slow)

    macd_line: List[Optional[float]] = []
    for f, s in zip(ema_fast, ema_slow):
        if f is None or s is None:
            macd_line.append(None)
        else:
            macd_line.append(f - s)

    # Collect non-None macd values to compute signal EMA
    macd_values = [v for v in macd_line if v is not None]
    sig_raw = calc_ema(macd_values, signal) if len(macd_values) >= signal else [None] * len(macd_values)

    # Pad sig_raw with Nones at the front so it aligns with closes
    none_count = len(closes) - len(sig_raw)
    sig_padded: List[Optional[float]] = [None] * none_count + list(sig_raw)

    histogram: List[Optional[float]] = []
    for m, s in zip(macd_line, sig_padded):
        if m is None or s is None:
            histogram.append(None)
        else:
            histogram.append(m - s)

    return macd_line, sig_padded, histogram


def bb_buy_allowed(close: float, bb_upper, bb_mid) -> bool:
    """Hard veto: block buy when price is in the upper 30% of the Bollinger Band range."""
    if bb_upper is None or bb_mid is None:
        return False  # no data — block to be safe, not allow
    threshold = bb_mid + (bb_upper - bb_mid) * 0.7
    if close >= threshold:
        return False
    return True


def is_5m_bullish(candles_5m: list) -> bool:
    """Return True only when the 5m timeframe is clearly bullish."""
    if not candles_5m or len(candles_5m) < 21:
        return False  # missing data — DO block
    closes = [c["close"] for c in candles_5m]
    ema9  = calc_ema(closes, 9)
    ema21 = calc_ema(closes, 21)
    if ema9[-1] is None or ema21[-1] is None:
        return False
    return ema9[-1] > ema21[-1] and closes[-1] > ema21[-1]


def aggregate_candles(candles_1m: list, group: int = 5) -> list:
    """Aggregate 1m candles into N-minute candles (default 5m).

    Accepts a list of dicts with at least a ``close`` key (``open``/``high``/
    ``low``/``volume`` are used when present). When ``open_time`` (ms) is
    available, candles are bucketed on real N-minute boundaries; otherwise they
    are chunked sequentially from the NEWEST candle backwards so the most
    recent aggregate always ends on the latest close. The trailing in-progress
    bucket is dropped only in the time-bucketed path (sequential chunks are
    complete by construction). Used to derive a 5m view from stored 1m history
    right after a restart, while the WebSocket 5m buffer refills.
    """
    if not candles_1m or group <= 1:
        return list(candles_1m or [])

    def _f(c, key, fallback=None):
        v = c.get(key)
        if v is None and fallback is not None:
            v = c.get(fallback)
        try:
            return float(v)
        except (TypeError, ValueError):
            return None

    def _merge(chunk: list) -> Optional[dict]:
        closes = [_f(c, "close") for c in chunk]
        closes = [c for c in closes if c is not None]
        if not closes:
            return None
        opens = _f(chunk[0], "open", "close")
        highs = [(_f(c, "high", "close")) for c in chunk]
        lows  = [(_f(c, "low",  "close")) for c in chunk]
        vols  = [(_f(c, "volume") or 0.0) for c in chunk]
        out = {
            "open":   opens if opens is not None else closes[0],
            "high":   max(h for h in highs if h is not None),
            "low":    min(l for l in lows if l is not None),
            "close":  closes[-1],
            "volume": sum(vols),
        }
        ot = chunk[0].get("open_time")
        if ot is not None:
            out["open_time"] = ot
        return out

    bucket_ms = group * 60 * 1000
    if all(c.get("open_time") is not None for c in candles_1m):
        buckets: dict = {}
        order: list = []
        for c in candles_1m:
            key = int(c["open_time"]) // bucket_ms
            if key not in buckets:
                buckets[key] = []
                order.append(key)
            buckets[key].append(c)
        # Drop the trailing (possibly in-progress) bucket when incomplete
        if order and len(buckets[order[-1]]) < group:
            order.pop()
        merged = [_merge(buckets[k]) for k in order]
        return [m for m in merged if m is not None]

    # No timestamps — chunk backwards from the newest candle in full groups
    chunks = []
    i = len(candles_1m)
    while i - group >= 0:
        chunks.append(candles_1m[i - group: i])
        i -= group
    chunks.reverse()
    merged = [_merge(ch) for ch in chunks]
    return [m for m in merged if m is not None]


def calc_atr(candles: list, period: int = 14) -> Optional[float]:
    """Average True Range over candles (list of dicts with high/low/close)."""
    trs = []
    for i in range(1, len(candles)):
        high       = candles[i]["high"]
        low        = candles[i]["low"]
        prev_close = candles[i - 1]["close"]
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        trs.append(tr)
    if len(trs) < period:
        return None
    atr = sum(trs[:period]) / period
    for tr in trs[period:]:
        atr = (atr * (period - 1) + tr) / period
    return atr


def atr_is_tradeable(
    atr: Optional[float],
    current_price: float,
    min_pct: float = 0.0015,
    max_pct: float = 0.015,
) -> bool:
    """True when ATR is between min_pct and max_pct of price (enough but not too volatile)."""
    if atr is None or current_price <= 0:
        return False
    atr_pct = atr / current_price
    return min_pct <= atr_pct <= max_pct


def calc_obv(candles: list) -> list:
    """On-Balance Volume — cumulative volume driven by price direction."""
    obv = 0
    result = [0]
    for i in range(1, len(candles)):
        if candles[i]["close"] > candles[i - 1]["close"]:
            obv += candles[i]["volume"]
        elif candles[i]["close"] < candles[i - 1]["close"]:
            obv -= candles[i]["volume"]
        result.append(obv)
    return result


def obv_is_bullish(candles: list, lookback: int = 3) -> bool:
    """True when OBV is higher now than it was `lookback` candles ago (buying pressure)."""
    obv = calc_obv(candles)
    if len(obv) < lookback + 1:
        return False
    return obv[-1] > obv[-lookback - 1]


def calc_stoch_rsi(closes: List[float], rsi_period: int = 14, stoch_period: int = 14) -> Optional[float]:
    """Stochastic RSI: normalises RSI within its own range over stoch_period bars.
    Returns a value in [0, 100], or None if insufficient data."""
    rsi_vals = calc_rsi(closes, rsi_period)
    valid = [v for v in rsi_vals if v is not None]
    if len(valid) < stoch_period:
        return None
    window = valid[-stoch_period:]
    lo = min(window)
    hi = max(window)
    if hi == lo:
        return 50.0  # flat RSI → neutral
    return (window[-1] - lo) / (hi - lo) * 100.0
