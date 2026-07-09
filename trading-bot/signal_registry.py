"""
Signal registry for WolfBot trading engine.

Each signal has:
  - id: unique identifier
  - category: trend | momentum | volume | volatility | price_level | reversal | execution | time
  - description: human-readable
  - compute_fn: (symbol, signal_data, strategy) -> (fired: bool, raw_value: Any)

Veto signals fire=True means BLOCK (inverted convention from scored signals).

Phase 1: 6 existing signals as registered wrappers.
Phase 2: 5 new signals — P1, R1, E1, TM1, M2.
"""
from __future__ import annotations

import collections
import logging
import threading as _threading
import time
import urllib.request
import json as _json
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple

log = logging.getLogger(__name__)

# NOTE (v0.4 memory-leak fix): the two histories below are LIVE in-memory
# rolling traces bounded ONLY by these per-key caps + a time-window filter on
# read. They are independent of DB retention/pruning — the OOM leak was
# unbounded in-memory retention (heavy parsed-config objects held in these
# deques) that DB pruning never touched. Keep caps small and store ONLY plain
# str/int/float/bool primitives here so no reference into the json-parsed
# strategy/signal-config graph is ever retained.

# Per-signal firing tracker (rolling window). Cap kept modest: readers
# (get_signal_fire_rates) filter by a TIME window anyway, so a huge count cap
# only wastes memory.
_signal_fire_history: dict = {}
_signal_fire_lock = _threading.Lock()
_SIGNAL_HISTORY_MAX_PER_SIGNAL = 5000

# Per-coin evaluation history. Written ~once per evaluation per symbol; a small
# rolling cap is plenty for get_coin_trace's recent-snapshot view.
_coin_evaluation_history: dict = {}
_coin_eval_lock = _threading.Lock()
_COIN_EVAL_MAX_PER_SYMBOL = 60


def record_signal_fire(signal_id: str, fired: bool) -> None:
    now = time.time()
    with _signal_fire_lock:
        if signal_id not in _signal_fire_history:
            _signal_fire_history[signal_id] = collections.deque(maxlen=_SIGNAL_HISTORY_MAX_PER_SIGNAL)
        _signal_fire_history[signal_id].append((now, fired))


def get_signal_fire_rates(window_hours: float = 1.0) -> dict:
    now = time.time()
    cutoff = now - (window_hours * 3600)
    result = {}
    with _signal_fire_lock:
        for sig_id, history in _signal_fire_history.items():
            in_window = [(ts, f) for ts, f in history if ts >= cutoff]
            if not in_window:
                result[sig_id] = {"evaluations": 0, "fires": 0, "fire_rate_pct": 0.0, "window_hours": window_hours}
                continue
            total = len(in_window)
            fires = sum(1 for _, f in in_window if f)
            result[sig_id] = {"evaluations": total, "fires": fires, "fire_rate_pct": round(fires / total * 100, 2), "window_hours": window_hours}
    return result


# ── Rolling 24h per-signal fire-rate telemetry (B Step 2.1) ───────────────────
# 24 hourly buckets keyed by epoch-hour: {hour: {signal_id: {"evaluated", "fired"}}}.
# O(1) per evaluation; pruned to the trailing window on write.
_TELEMETRY_WINDOW_HOURS = 24
_signal_telemetry_buckets: Dict[int, Dict[str, Dict[str, int]]] = {}
_signal_telemetry_lock = _threading.Lock()


def _telemetry_record(signal_id: str, fired: bool) -> None:
    """Count one evaluation (and optionally one fire) in the current hour bucket."""
    hour = int(time.time() // 3600)
    with _signal_telemetry_lock:
        bucket = _signal_telemetry_buckets.setdefault(hour, {})
        ent = bucket.setdefault(signal_id, {"evaluated": 0, "fired": 0})
        ent["evaluated"] += 1
        if fired:
            ent["fired"] += 1
        # Prune buckets older than the window (dict stays <= 25 keys)
        if len(_signal_telemetry_buckets) > _TELEMETRY_WINDOW_HOURS:
            cutoff = hour - _TELEMETRY_WINDOW_HOURS
            for h in [h for h in _signal_telemetry_buckets if h <= cutoff]:
                del _signal_telemetry_buckets[h]


def get_signal_telemetry() -> dict:
    """Rolling-24h per-signal counters.

    Returns {"window_hours": 24,
             "signals": {signal_id: {"evaluated": int, "fired": int, "fire_rate": float}}}
    """
    now_hour = int(time.time() // 3600)
    cutoff = now_hour - _TELEMETRY_WINDOW_HOURS
    signals: Dict[str, Dict[str, Any]] = {}
    with _signal_telemetry_lock:
        for h, bucket in _signal_telemetry_buckets.items():
            if h <= cutoff:
                continue
            for sig_id, ent in bucket.items():
                agg = signals.setdefault(sig_id, {"evaluated": 0, "fired": 0})
                agg["evaluated"] += ent["evaluated"]
                agg["fired"]     += ent["fired"]
    for agg in signals.values():
        agg["fire_rate"] = (round(agg["fired"] / agg["evaluated"], 4)
                            if agg["evaluated"] > 0 else 0.0)
    return {"window_hours": _TELEMETRY_WINDOW_HOURS, "signals": signals}


def _summarize_results(results: Any) -> str:
    """Collapse a [(signal_id, fired_bool), ...] list into a compact plain-string
    summary like 'T1=1,M3=0'. Returns primitives only — never retains a
    reference into the original (possibly json-parsed-config-backed) objects.
    """
    parts: List[str] = []
    try:
        for item in (results or []):
            # Each item is normally a (signal_id, fired) tuple; be defensive.
            if isinstance(item, (tuple, list)) and len(item) >= 2:
                sig_id, fired = item[0], item[1]
                parts.append(f"{str(sig_id)}={1 if fired else 0}")
            else:
                parts.append(str(item))
    except Exception:
        return ""
    return ",".join(parts)


def record_coin_evaluation(symbol: str, evaluation: dict) -> None:
    """Append a LIGHTWEIGHT trace record for `symbol`.

    v0.4 leak fix: we deliberately do NOT store the heavy nested
    mandatory_results / scored_results / veto_results lists, nor the raw
    fired_signals list — those objects hold references into the json-parsed
    strategy/signal config and were the retained structures behind the OOM.
    Instead we normalize everything to plain str/int/float/bool primitives and
    store compact string summaries + counts, which is all get_coin_trace needs.
    """
    now = time.time()
    fired_signals = [str(s) for s in (evaluation.get("fired_signals") or [])]
    mandatory_results = evaluation.get("mandatory_results") or []
    scored_results = evaluation.get("scored_results") or []
    veto_results = evaluation.get("veto_results") or []
    record = {
        "ts": now,
        "score": int(evaluation.get("score", 0) or 0),
        "allowed": bool(evaluation.get("allowed", False)),
        "reason": str(evaluation.get("reason", "")),
        "fired_signals": fired_signals,                     # plain strings only
        "fired_count": len(fired_signals),
        "mandatory_count": len(mandatory_results),
        "scored_count": len(scored_results),
        "veto_count": len(veto_results),
        # Compact plain-string summaries in place of the heavy nested lists.
        "mandatory_summary": _summarize_results(mandatory_results),
        "scored_summary": _summarize_results(scored_results),
        "veto_summary": _summarize_results(veto_results),
    }
    with _coin_eval_lock:
        if symbol not in _coin_evaluation_history:
            _coin_evaluation_history[symbol] = collections.deque(maxlen=_COIN_EVAL_MAX_PER_SYMBOL)
        _coin_evaluation_history[symbol].append(record)


def get_coin_trace(symbol: str, hours: float = 1.0) -> dict:
    now = time.time()
    cutoff = now - (hours * 3600)
    with _coin_eval_lock:
        history = list(_coin_evaluation_history.get(symbol, []))
    in_window = [e for e in history if e["ts"] >= cutoff]
    if not in_window:
        return {"symbol": symbol, "evaluations_count": 0, "buy_allowed_count": 0, "rejection_reasons": {}, "recent_snapshots": []}
    reasons = collections.Counter(e["reason"] for e in in_window if not e["allowed"])
    buy_allowed_count = sum(1 for e in in_window if e["allowed"])
    return {
        "symbol": symbol,
        "window_hours": hours,
        "evaluations_count": len(in_window),
        "buy_allowed_count": buy_allowed_count,
        "rejection_reasons": dict(reasons.most_common()),
        "recent_snapshots": in_window[-5:],
    }


# ── Registry ──────────────────────────────────────────────────────────────────

@dataclass
class SignalDef:
    id: str
    category: str
    description: str
    compute_fn: Callable[[str, dict, dict], Tuple[bool, Any]]


SIGNAL_REGISTRY: Dict[str, SignalDef] = {}


def register_signal(signal_def: SignalDef) -> None:
    SIGNAL_REGISTRY[signal_def.id] = signal_def


def get_signal(signal_id: str) -> Optional[SignalDef]:
    return SIGNAL_REGISTRY.get(signal_id)


def get_all_signals_by_category(category: str) -> List[SignalDef]:
    return [s for s in SIGNAL_REGISTRY.values() if s.category == category]


def list_all_signal_ids() -> List[str]:
    return list(SIGNAL_REGISTRY.keys())


# ── Phase 1: 6 existing signals as registered wrappers ───────────────────────

def _signal_ema_trend(symbol: str, data: dict, strategy: dict) -> Tuple[bool, Any]:
    """T1: EMA9 > EMA21 on 1h candles."""
    fired = bool(data.get("trend", False))
    return fired, fired


def _signal_rsi(symbol: str, data: dict, strategy: dict) -> Tuple[bool, Any]:
    """M1: RSI below configured threshold."""
    rsi_value = data.get("rsi_value")
    # Read from signal_thresholds (matches UI display and M2 pattern).
    # Explicit None check avoids the "0 is falsy" trap where a threshold of 0
    # would fall through to the top-level key via `or`.
    nested = strategy.get("signal_thresholds", {}).get("rsi_buy_threshold")
    if nested is None:
        threshold = float(strategy.get("rsi_buy_threshold", 40.0))
    else:
        threshold = float(nested)
    if rsi_value is None:
        return bool(data.get("rsi", False)), None
    return rsi_value < threshold, rsi_value


def _signal_macd_rising(symbol: str, data: dict, strategy: dict) -> Tuple[bool, Any]:
    """M3: MACD histogram positive AND rising."""
    fired = bool(data.get("macd", False))
    return fired, fired


def _signal_volume(symbol: str, data: dict, strategy: dict) -> Tuple[bool, Any]:
    """V1: Volume above recent average."""
    fired = bool(data.get("volume", False))
    return fired, fired


def _signal_obv(symbol: str, data: dict, strategy: dict) -> Tuple[bool, Any]:
    """V2: OBV rising (accumulation pressure)."""
    fired = bool(data.get("obv", False))
    return fired, fired


def _signal_atr(symbol: str, data: dict, strategy: dict) -> Tuple[bool, Any]:
    """X1: ATR within tradeable range.

    NOTE (v0.4 F3): This raw signal is TRUE when ATR is tradeable and fires
    ~99.9% of the time, so it discriminates nothing as a scored signal — it was
    a "free point" inflating scores on trendless tape. Per spec §3.2 it is no
    longer in the scored default set (DEFAULT role → 'off'). Its useful
    information (is volatility outside the tradeable band?) is captured instead
    by the X1_atr_untradeable VETO below, which BLOCKS when ATR is not tradeable
    (too low = dead tape, too high = chaos). The raw signal stays registered so
    persisted roles maps that still reference 'X1_atr_sufficient' resolve.
    """
    fired = bool(data.get("atr", False))
    return fired, fired


def _signal_atr_untradeable(symbol: str, data: dict, strategy: dict) -> Tuple[bool, Any]:
    """X1 VETO: fires (BLOCKS) when ATR is OUTSIDE the tradeable range.

    Inverted semantics vs the raw X1 signal: X1's compute returns True when ATR
    IS tradeable, so this veto fires when NOT tradeable — i.e. volatility is too
    low (dead) or too high (chaotic) to trade. FAIL-OPEN: if the tradeability
    flag is missing/None the veto cannot evaluate and must NOT fire, so a missing
    'atr' key never blocks an entry.
    """
    atr_tradeable = data.get("atr")
    if atr_tradeable is None:
        return False, "no_data"
    tradeable = bool(atr_tradeable)
    # Veto fires (block) when NOT tradeable.
    return (not tradeable), ("tradeable" if tradeable else "untradeable")


register_signal(SignalDef("T1_ema_short_long",      "trend",       "EMA9 > EMA21 on 1h (short-term uptrend)",           _signal_ema_trend))
register_signal(SignalDef("M1_rsi_below_threshold",  "momentum",    "RSI(14) below configured threshold (oversold entry)", _signal_rsi))
register_signal(SignalDef("M3_macd_rising",          "momentum",    "MACD histogram positive AND rising",                 _signal_macd_rising))
register_signal(SignalDef("V1_volume_above_average", "volume",      "Current volume > recent average",                    _signal_volume))
register_signal(SignalDef("V2_obv_rising",           "volume",      "OBV rising (accumulation pressure)",                 _signal_obv))
register_signal(SignalDef("X1_atr_sufficient",       "volatility",  "ATR within tradeable range",                         _signal_atr))
register_signal(SignalDef(
    "X1_atr_untradeable",
    "volatility",
    "VETO: ATR outside tradeable range (too low = dead, too high = chaos)",
    _signal_atr_untradeable,
))


# ── Phase 2: 5 new signals ────────────────────────────────────────────────────

def _signal_near_24h_low(symbol: str, data: dict, strategy: dict) -> Tuple[bool, Any]:
    """P1: Price within configured % of 24h low."""
    threshold_pct = float(
        strategy.get("signal_thresholds", {}).get("near_low_pct", 2.0)
    )
    low_24h = data.get("low_24h")
    current_price = data.get("current_price") or data.get("price")
    if low_24h is None or current_price is None or low_24h <= 0:
        return False, None
    distance_pct = (current_price - low_24h) / low_24h * 100.0
    return distance_pct <= threshold_pct, round(distance_pct, 3)


register_signal(SignalDef(
    "P1_near_24h_low",
    "price_level",
    "Price within X% of 24h low (mean-reversion entry zone)",
    _signal_near_24h_low,
))


_reversal_sr_cache: Dict[str, dict] = {}
_REVERSAL_SR_CACHE_TTL = 20.0


def _signal_reversal_confirmed(symbol: str, data: dict, strategy: dict) -> Tuple[bool, Any]:
    """R1: Last candle green AND volume above prior 4-candle average.

    Prefers 5m candles (klines_5m) when the data layer provides them; falls
    back to klines_1m so nothing breaks before the 5m data agent lands.
    """
    now = time.time()
    cached = _reversal_sr_cache.get(symbol)
    if cached and (now - cached["ts"]) < _REVERSAL_SR_CACHE_TTL:
        return cached["result"]

    klines = data.get("klines_5m") or data.get("klines_1m") or []
    if len(klines) < 5:
        result = (False, "no_data")
        _reversal_sr_cache[symbol] = {"ts": now, "result": result}
        return result

    last   = klines[-1]
    prev_4 = klines[-5:-1]

    last_close = float(last.get("close", 0))
    last_open  = float(last.get("open",  0))
    last_vol   = float(last.get("volume", 0))

    if last_close <= last_open:
        result = (False, "candle_red")
        _reversal_sr_cache[symbol] = {"ts": now, "result": result}
        return result

    vol_mult    = float(strategy.get("signal_thresholds", {}).get("reversal_volume_multiplier", 1.10))
    prev_avg_vol = sum(float(c.get("volume", 0)) for c in prev_4) / 4.0

    if prev_avg_vol > 0 and last_vol < prev_avg_vol * vol_mult:
        result = (False, "weak_volume")
        _reversal_sr_cache[symbol] = {"ts": now, "result": result}
        return result

    result = (True, "ok")
    _reversal_sr_cache[symbol] = {"ts": now, "result": result}
    return result


register_signal(SignalDef(
    "R1_reversal_confirmed",
    "reversal",
    "Last candle green with volume above recent average (5m preferred, 1m fallback)",
    _signal_reversal_confirmed,
))


_spread_sr_cache: Dict[str, dict] = {}
_SPREAD_CACHE_TTL = 30.0


def _signal_spread_too_wide(symbol: str, data: dict, strategy: dict) -> Tuple[bool, Any]:
    """E1 VETO: fired=True when bid-ask spread exceeds threshold (inverted: True = block)."""
    now = time.time()
    threshold = float(strategy.get("signal_thresholds", {}).get("spread_max_pct", 0.15))
    cached = _spread_sr_cache.get(symbol)
    if cached and (now - cached["ts"]) < _SPREAD_CACHE_TTL:
        return cached["spread_pct"] > threshold, cached["spread_pct"]

    try:
        url = f"https://api.binance.com/api/v3/ticker/bookTicker?symbol={symbol}"
        req = urllib.request.Request(url, headers={"User-Agent": "WolfBot/1.0"})
        with urllib.request.urlopen(req, timeout=2.0) as r:
            d = _json.loads(r.read())
        bid = float(d.get("bidPrice", 0))
        ask = float(d.get("askPrice", 0))
    except Exception:
        _spread_sr_cache[symbol] = {"ts": now, "spread_pct": 0.0}
        return False, None  # fail safe: don't veto on uncertainty

    if bid <= 0 or ask <= 0:
        return False, None

    mid        = (bid + ask) / 2.0
    spread_pct = (ask - bid) / mid * 100.0
    _spread_sr_cache[symbol] = {"ts": now, "spread_pct": spread_pct}
    return spread_pct > threshold, round(spread_pct, 4)


register_signal(SignalDef(
    "E1_spread_too_wide",
    "execution",
    "VETO: bid-ask spread exceeds threshold (skip illiquid coins)",
    _signal_spread_too_wide,
))


def _signal_bad_hour(symbol: str, data: dict, strategy: dict) -> Tuple[bool, Any]:
    """TM1 VETO: fired=True when current UTC hour is outside configured trading window."""
    import datetime
    hour = datetime.datetime.utcnow().hour
    allowed_str = (
        strategy.get("signal_thresholds", {})
        .get("allowed_trading_hours_utc", "13,14,15,16,17,18,19,20,21,22")
    )
    try:
        allowed = {int(h.strip()) for h in allowed_str.split(",") if h.strip()}
    except Exception:
        allowed = set(range(24))  # fail open
    return hour not in allowed, hour


register_signal(SignalDef(
    "TM1_bad_hour",
    "time",
    "VETO: current UTC hour outside configured trading window",
    _signal_bad_hour,
))


def _signal_stoch_rsi_oversold(symbol: str, data: dict, strategy: dict) -> Tuple[bool, Any]:
    """M2: Stochastic RSI below threshold (faster oversold detection)."""
    stoch_rsi_value = data.get("stoch_rsi_value")
    if stoch_rsi_value is None:
        return False, None
    threshold = float(strategy.get("signal_thresholds", {}).get("stoch_rsi_threshold", 25.0))
    return stoch_rsi_value < threshold, stoch_rsi_value


register_signal(SignalDef(
    "M2_stoch_rsi_oversold",
    "momentum",
    "Stochastic RSI below threshold (faster oversold detection)",
    _signal_stoch_rsi_oversold,
))


_pullback_cache: Dict[str, dict] = {}
_PULLBACK_CACHE_TTL = 10.0


def _signal_micro_pullback(symbol: str, data: dict, strategy: dict) -> Tuple[bool, Any]:
    """M4: Price is at least dip_pct% below the highest candle high over the last
    lookback_candles candles — a micro-pullback from a recent local top.

    Prefers 5m candles (klines_5m) when present; falls back to klines_1m.
    Read dip_pct (default 0.3) and lookback_candles (default 5) from
    strategy['signal_thresholds']. Never blocks if candle data is unavailable.
    """
    now = time.time()
    cached = _pullback_cache.get(symbol)
    if cached and (now - cached["ts"]) < _PULLBACK_CACHE_TTL:
        return cached["result"]

    thresholds = strategy.get("signal_thresholds", {})
    dip_pct  = float(thresholds.get("dip_pct",          0.3))
    lookback = int(thresholds.get("lookback_candles",   5))

    cur = float(data.get("current_price") or data.get("price") or 0)
    if cur <= 0:
        _pullback_cache[symbol] = {"ts": now, "result": (False, None)}
        return False, None

    klines = list(data.get("klines_5m") or data.get("klines_1m") or [])

    # Fall back to DB if the in-memory store is too thin
    if len(klines) < 2:
        try:
            import database as _db
            import config as _cfg
            db_candles = _db.get_candles(symbol, _cfg.CANDLE_TIMEFRAME, limit=lookback)
            if db_candles:
                klines = db_candles
        except Exception:
            pass

    if not klines:
        _pullback_cache[symbol] = {"ts": now, "result": (False, None)}
        return False, None

    recent = klines[-lookback:] if len(klines) >= lookback else klines
    highs  = [float(c.get("high", c.get("close", 0))) for c in recent if c.get("high") or c.get("close")]
    local_high = max(highs) if highs else 0.0

    if local_high <= 0 or local_high <= cur:
        # Price is at or above the local high — no pullback yet
        result = (False, round((cur - local_high) / local_high * 100.0, 4) if local_high > 0 else None)
        _pullback_cache[symbol] = {"ts": now, "result": result}
        return result

    dip_from_high = (local_high - cur) / local_high * 100.0
    result = (dip_from_high >= dip_pct, round(dip_from_high, 4))
    _pullback_cache[symbol] = {"ts": now, "result": result}
    return result


register_signal(SignalDef(
    "M4_micro_pullback",
    "momentum",
    "Price dipped ≥ dip_pct% below recent local high (micro-pullback entry)",
    _signal_micro_pullback,
))


# ── Phase 3 (§3.1/§3.2): 5m-native signals, regime + BB vetoes ────────────────

def _signal_rsi_zone(symbol: str, data: dict, strategy: dict) -> Tuple[bool, Any]:
    """M5: RSI(14, 5m) within the entry zone [rsi_zone_low, rsi_zone_high].

    Replaces the RSI<40 blanket mandatory (§3.2). Defensive: missing rsi_value
    → (False, "no_data").
    """
    rsi_value = data.get("rsi_value")
    if rsi_value is None:
        return False, "no_data"
    thresholds = strategy.get("signal_thresholds", {})
    zone_low  = float(thresholds.get("rsi_zone_low",  30.0))
    zone_high = float(thresholds.get("rsi_zone_high", 55.0))
    return zone_low <= float(rsi_value) <= zone_high, round(float(rsi_value), 2)


register_signal(SignalDef(
    "M5_rsi_zone",
    "momentum",
    "RSI(14, 5m) within [rsi_zone_low, rsi_zone_high] entry zone",
    _signal_rsi_zone,
))


def _signal_bb_upper_touch_5m(symbol: str, data: dict, strategy: dict) -> Tuple[bool, Any]:
    """P2 VETO: price at/above the 5m Bollinger upper band (fired=True = block).

    Reads signal_data['bb_position_5m'] provided by the data layer. FAIL-OPEN:
    if the key is missing/None the veto cannot evaluate and must NOT fire —
    we return (False, "no_data") so an entry is never blocked on missing data.
    """
    bb_position = data.get("bb_position_5m")
    if bb_position is None:
        return False, "no_data"
    return bb_position in ("above_upper", "at_upper"), bb_position


register_signal(SignalDef(
    "P2_bb_upper_touch_5m",
    "price_level",
    "VETO: price at/above 5m Bollinger upper band (chasing extension)",
    _signal_bb_upper_touch_5m,
))


def _signal_btc_regime_risk_off(symbol: str, data: dict, strategy: dict) -> Tuple[bool, Any]:
    """REGIME VETO: BTC regime is 'risk_off' (fired=True = block all entries).

    Neutral-regime sizing is handled by the engine, not here. FAIL-OPEN:
    missing/None regime → (False, "no_data") — a veto that can't evaluate
    must not fire.
    """
    btc_regime = data.get("btc_regime")
    if btc_regime is None:
        return False, "no_data"
    return btc_regime == "risk_off", btc_regime


register_signal(SignalDef(
    "REGIME_risk_off",
    "trend",
    "VETO: BTC market regime is risk_off (block all new entries)",
    _signal_btc_regime_risk_off,
))


def _signal_ema50_15m_slope(symbol: str, data: dict, strategy: dict) -> Tuple[bool, Any]:
    """T2: EMA50 slope on 15m-aggregated data is positive (context signal §3.1).

    Defensive: missing/None slope → (False, "no_data").
    """
    slope = data.get("ema50_15m_slope")
    if slope is None:
        return False, "no_data"
    try:
        slope = float(slope)
    except (TypeError, ValueError):
        return False, "no_data"
    return slope > 0.0, round(slope, 5)


register_signal(SignalDef(
    "T2_ema50_15m_slope",
    "trend",
    "EMA50 slope on 15m rising (higher-timeframe context uptrend)",
    _signal_ema50_15m_slope,
))


# ── Default signal_engine config (used when block absent in strategy.json) ───

# v0.4 §3.2 / F3 starting set:
#   • T1_ema_short_long is the single MANDATORY trend gate (restored — F3 found
#     it had drifted to 'scored' and fired only ~5.8%, so entries needed no
#     trend and the bot bought trendless tape). It is now a hard gate again.
#   • SCORED = the 5 "real" discriminating signals {M3, V1, V2, M4, R1} plus
#     T2_ema50_15m_slope (15m EMA50 slope) as a genuine higher-timeframe
#     trend-confirm that helps offset X1 leaving the scored pool. min_scored 3.
#   • X1_atr_sufficient is REMOVED from scored (F3: fired ~99.9% → discriminated
#     nothing, a free point). Its information is now a VETO (X1_atr_untradeable)
#     that BLOCKS when ATR is outside the tradeable band. The raw X1 signal keeps
#     DEFAULT role 'off'.
#   • VETOES = E1 (spread), P2 (5m BB upper touch), REGIME (BTC risk_off),
#     X1_atr_untradeable (ATR untradeable).
#   • M1/M2/M5/P1/TM1 stay registered but default-off (measure first).
DEFAULT_SIGNAL_ENGINE: Dict[str, Any] = {
    "enabled": True,
    "mandatory_signals": ["T1_ema_short_long"],
    "scored_signals": [
        "M3_macd_rising",
        "V1_volume_above_average",
        "V2_obv_rising",
        "M4_micro_pullback",
        "R1_reversal_confirmed",
        "T2_ema50_15m_slope",
    ],
    "min_scored": 3,
    "veto_signals": [
        "E1_spread_too_wide",
        "P2_bb_upper_touch_5m",
        "REGIME_risk_off",
        "X1_atr_untradeable",
    ],
}

DEFAULT_SIGNAL_THRESHOLDS: Dict[str, Any] = {
    "near_low_pct": 2.0,
    "reversal_volume_multiplier": 1.10,
    "spread_max_pct": 0.15,
    "allowed_trading_hours_utc": "13,14,15,16,17,18,19,20,21,22",
    "stoch_rsi_threshold": 25.0,
    "dip_pct": 0.3,
    "lookback_candles": 5,
    "rsi_zone_low": 30.0,
    "rsi_zone_high": 55.0,
}


# ── Config-driven roles (B Step 2.3) ──────────────────────────────────────────
# strategy["signal_engine"]["roles"] = {signal_id: "scored"|"mandatory"|"veto"|"off"}
# When present it fully derives the mandatory/scored/veto lists ("off" ids are
# excluded everywhere; ids absent from the map fall back to their DEFAULT role
# so newly shipped signals activate for old persisted strategies). Hot-reload
# comes for free: callers pass the mtime-cached strategy dict on every
# evaluation.

_VALID_SIGNAL_ROLES = {"scored", "mandatory", "veto", "off"}
_roles_warned: set = set()          # one-time log keys (unknown id / invalid role)
_roles_warned_lock = _threading.Lock()


def _warn_roles_once(key: str, message: str, *, error: bool = False) -> None:
    with _roles_warned_lock:
        if key in _roles_warned:
            return
        _roles_warned.add(key)
    (log.error if error else log.warning)(message)


def _default_role_for(signal_id: str) -> str:
    """The role a signal has in DEFAULT_SIGNAL_ENGINE ('off' if absent)."""
    if signal_id in DEFAULT_SIGNAL_ENGINE["mandatory_signals"]:
        return "mandatory"
    if signal_id in DEFAULT_SIGNAL_ENGINE["veto_signals"]:
        return "veto"
    if signal_id in DEFAULT_SIGNAL_ENGINE["scored_signals"]:
        return "scored"
    return "off"


def _derive_role_lists(roles: dict) -> Tuple[List[str], List[str], List[str]]:
    """Derive (mandatory_ids, scored_ids, veto_ids) from a roles map.

    Unknown signal ids are ignored (one-time warning). Invalid role strings
    fall back to that signal's default role (one-time error).

    Migration safety: registered signals ABSENT from the roles map get their
    DEFAULT role (from DEFAULT_SIGNAL_ENGINE). This keeps persisted strategy
    files working when new signals ship — a new default veto/scored signal
    takes effect without the user re-saving their roles map. To disable a
    signal, set it to "off" explicitly.
    """
    mandatory_ids: List[str] = []
    scored_ids:    List[str] = []
    veto_ids:      List[str] = []
    merged = {sig_id: _default_role_for(sig_id) for sig_id in SIGNAL_REGISTRY}
    merged.update(roles)
    for sig_id, role in merged.items():
        if sig_id not in SIGNAL_REGISTRY:
            _warn_roles_once(
                f"unknown::{sig_id}",
                f"signal_engine.roles: unknown signal id '{sig_id}' — entry ignored "
                f"(known ids: {sorted(SIGNAL_REGISTRY.keys())})",
            )
            continue
        norm = role.strip().lower() if isinstance(role, str) else None
        if norm not in _VALID_SIGNAL_ROLES:
            fallback = _default_role_for(sig_id)
            _warn_roles_once(
                f"invalid::{sig_id}::{role!r}",
                f"signal_engine.roles: invalid role {role!r} for '{sig_id}' "
                f"(valid: scored|mandatory|veto|off) — falling back to default "
                f"role '{fallback}'",
                error=True,
            )
            norm = fallback
        if norm == "mandatory":
            mandatory_ids.append(sig_id)
        elif norm == "scored":
            scored_ids.append(sig_id)
        elif norm == "veto":
            veto_ids.append(sig_id)
        # "off" → excluded from every list
    return mandatory_ids, scored_ids, veto_ids


# ── Evaluation ────────────────────────────────────────────────────────────────

def evaluate_signals(symbol: str, signal_data: dict, strategy: dict) -> Dict[str, Any]:
    """Evaluate all registered signals. Returns fired list + per-signal results."""
    results: Dict[str, Any] = {}
    fired: List[str] = []
    for signal_id, sig_def in SIGNAL_REGISTRY.items():
        try:
            did_fire, raw = sig_def.compute_fn(symbol, signal_data, strategy)
            results[signal_id] = {"fired": did_fire, "raw_value": raw}
            record_signal_fire(signal_id, did_fire)
            _telemetry_record(signal_id, did_fire)
            if did_fire:
                fired.append(signal_id)
        except Exception as e:
            log.warning("Signal %s failed for %s: %s", signal_id, symbol, e)
            results[signal_id] = {"fired": False, "raw_value": None, "error": str(e)}
            record_signal_fire(signal_id, False)
            _telemetry_record(signal_id, False)
    return {"fired_signals": fired, "all_results": results}


def evaluate_buy_decision(symbol: str, signal_data: dict, strategy: dict) -> Dict[str, Any]:
    """
    Layered buy decision: vetoes → mandatory → scored.

    When strategy["signal_engine"]["enabled"] is True, uses the configured
    signal lists. Otherwise falls back to Phase 1 legacy mapping.

    Veto convention: fired=True means BLOCK (opposite of scored/mandatory).
    Returns: {allowed, reason, mandatory_results, scored_results, veto_results, score, fired_signals}
    """
    evaluation = evaluate_signals(symbol, signal_data, strategy)
    fired_ids   = set(evaluation["fired_signals"])

    engine_cfg = strategy.get("signal_engine", {})
    if isinstance(engine_cfg, dict) and engine_cfg.get("enabled", False):
        _roles = engine_cfg.get("roles")
        if isinstance(_roles, dict) and _roles:
            # Roles map is authoritative when present: "off" signals are
            # excluded everywhere; unlisted signals get their DEFAULT role
            # (migration safety for new signals); list keys below are ignored.
            mandatory_ids, scored_ids, veto_ids = _derive_role_lists(_roles)
        else:
            mandatory_ids = engine_cfg.get("mandatory_signals", DEFAULT_SIGNAL_ENGINE["mandatory_signals"])
            scored_ids    = engine_cfg.get("scored_signals",    DEFAULT_SIGNAL_ENGINE["scored_signals"])
            veto_ids      = engine_cfg.get("veto_signals",      DEFAULT_SIGNAL_ENGINE["veto_signals"])
        min_scored    = int(engine_cfg.get("min_scored",    DEFAULT_SIGNAL_ENGINE["min_scored"]))
    else:
        # Phase 1 legacy fallback
        mandatory_enabled = bool(strategy.get("mandatory_signals_enabled", True))
        mandatory_ids = ["T1_ema_short_long", "M1_rsi_below_threshold"] if mandatory_enabled else []
        scored_ids = [
            "T1_ema_short_long", "M1_rsi_below_threshold", "M3_macd_rising",
            "V1_volume_above_average", "V2_obv_rising", "X1_atr_sufficient",
        ]
        veto_ids   = []
        min_scored = int(strategy.get("min_signals", 4))

    # 1. Vetoes first (cheapest to short-circuit)
    veto_results: List[Tuple[str, bool]] = []
    for sig_id in veto_ids:
        fired = sig_id in fired_ids
        veto_results.append((sig_id, fired))
        if fired:
            _veto_result = {
                "allowed": False, "reason": f"veto_{sig_id}_fired",
                "mandatory_results": [], "scored_results": [],
                "veto_results": veto_results, "score": 0,
                "fired_signals": list(fired_ids),
            }
            record_coin_evaluation(symbol, _veto_result)
            return _veto_result

    # 2. Mandatory gate
    mandatory_results: List[Tuple[str, bool]] = []
    for sig_id in mandatory_ids:
        did_fire = sig_id in fired_ids
        mandatory_results.append((sig_id, did_fire))
        if not did_fire:
            _mandatory_result = {
                "allowed": False, "reason": f"mandatory_{sig_id}_not_fired",
                "mandatory_results": mandatory_results, "scored_results": [],
                "veto_results": veto_results, "score": 0,
                "fired_signals": list(fired_ids),
            }
            record_coin_evaluation(symbol, _mandatory_result)
            return _mandatory_result

    # 3. Score gate
    scored_results: List[Tuple[str, bool]] = []
    score = 0
    for sig_id in scored_ids:
        did_fire = sig_id in fired_ids
        scored_results.append((sig_id, did_fire))
        if did_fire:
            score += 1

    if score < min_scored:
        _score_result = {
            "allowed": False, "reason": f"score_{score}_below_min_{min_scored}",
            "mandatory_results": mandatory_results, "scored_results": scored_results,
            "veto_results": veto_results, "score": score,
            "fired_signals": list(fired_ids),
        }
        record_coin_evaluation(symbol, _score_result)
        return _score_result

    _success_result = {
        "allowed": True, "reason": "all_checks_passed",
        "mandatory_results": mandatory_results, "scored_results": scored_results,
        "veto_results": veto_results, "score": score,
        "fired_signals": list(fired_ids),
    }
    record_coin_evaluation(symbol, _success_result)
    return _success_result
