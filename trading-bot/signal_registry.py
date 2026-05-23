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

# Per-signal firing tracker (rolling window)
_signal_fire_history: dict = {}
_signal_fire_lock = _threading.Lock()
_SIGNAL_HISTORY_MAX_PER_SIGNAL = 50000

# Per-coin evaluation history
_coin_evaluation_history: dict = {}
_coin_eval_lock = _threading.Lock()
_COIN_EVAL_MAX_PER_SYMBOL = 500


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


def record_coin_evaluation(symbol: str, evaluation: dict) -> None:
    now = time.time()
    with _coin_eval_lock:
        if symbol not in _coin_evaluation_history:
            _coin_evaluation_history[symbol] = collections.deque(maxlen=_COIN_EVAL_MAX_PER_SYMBOL)
        _coin_evaluation_history[symbol].append({
            "ts": now,
            "fired_signals": evaluation.get("fired_signals", []),
            "score": evaluation.get("score", 0),
            "allowed": evaluation.get("allowed", False),
            "reason": evaluation.get("reason", ""),
            "mandatory_results": evaluation.get("mandatory_results", []),
            "scored_results": evaluation.get("scored_results", []),
            "veto_results": evaluation.get("veto_results", []),
        })


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
    """X1: ATR within tradeable range."""
    fired = bool(data.get("atr", False))
    return fired, fired


register_signal(SignalDef("T1_ema_short_long",      "trend",       "EMA9 > EMA21 on 1h (short-term uptrend)",           _signal_ema_trend))
register_signal(SignalDef("M1_rsi_below_threshold",  "momentum",    "RSI(14) below configured threshold (oversold entry)", _signal_rsi))
register_signal(SignalDef("M3_macd_rising",          "momentum",    "MACD histogram positive AND rising",                 _signal_macd_rising))
register_signal(SignalDef("V1_volume_above_average", "volume",      "Current volume > recent average",                    _signal_volume))
register_signal(SignalDef("V2_obv_rising",           "volume",      "OBV rising (accumulation pressure)",                 _signal_obv))
register_signal(SignalDef("X1_atr_sufficient",       "volatility",  "ATR within tradeable range",                         _signal_atr))


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
    """R1: Last 1m candle green AND volume above prior 4-candle average."""
    now = time.time()
    cached = _reversal_sr_cache.get(symbol)
    if cached and (now - cached["ts"]) < _REVERSAL_SR_CACHE_TTL:
        return cached["result"]

    klines_1m = data.get("klines_1m", [])
    if len(klines_1m) < 5:
        result = (False, "no_data")
        _reversal_sr_cache[symbol] = {"ts": now, "result": result}
        return result

    last   = klines_1m[-1]
    prev_4 = klines_1m[-5:-1]

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
    "Last 1m candle green with volume above recent average",
    _signal_reversal_confirmed,
))


_spread_sr_cache: Dict[str, dict] = {}
_SPREAD_CACHE_TTL = 30.0


def _signal_spread_too_wide(symbol: str, data: dict, strategy: dict) -> Tuple[bool, Any]:
    """E1 VETO: fired=True when bid-ask spread exceeds threshold (inverted: True = block)."""
    now = time.time()
    threshold = float(strategy.get("signal_thresholds", {}).get("spread_max_pct", 0.10))
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
    lookback_candles 1m candles — a micro-pullback from a recent local top.

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

    klines = list(data.get("klines_1m", []))

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


# ── Default signal_engine config (used when block absent in strategy.json) ───

DEFAULT_SIGNAL_ENGINE: Dict[str, Any] = {
    "enabled": True,
    "mandatory_signals": ["M4_micro_pullback"],
    "scored_signals": [
        "T1_ema_short_long",
        "M3_macd_rising",
        "V1_volume_above_average",
        "V2_obv_rising",
        "X1_atr_sufficient",
        "R1_reversal_confirmed",
    ],
    "min_scored": 2,
    "veto_signals": ["E1_spread_too_wide"],
}

DEFAULT_SIGNAL_THRESHOLDS: Dict[str, Any] = {
    "near_low_pct": 2.0,
    "reversal_volume_multiplier": 1.10,
    "spread_max_pct": 0.10,
    "allowed_trading_hours_utc": "13,14,15,16,17,18,19,20,21,22",
    "stoch_rsi_threshold": 25.0,
    "dip_pct": 0.3,
    "lookback_candles": 5,
}


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
            if did_fire:
                fired.append(signal_id)
        except Exception as e:
            log.warning("Signal %s failed for %s: %s", signal_id, symbol, e)
            results[signal_id] = {"fired": False, "raw_value": None, "error": str(e)}
            record_signal_fire(signal_id, False)
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
