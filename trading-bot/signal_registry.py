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

import logging
import time
import urllib.request
import json as _json
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple

log = logging.getLogger(__name__)


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
    threshold = float(strategy.get("rsi_buy_threshold", 40.0))
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


# ── Default signal_engine config (used when block absent in strategy.json) ───

DEFAULT_SIGNAL_ENGINE: Dict[str, Any] = {
    "enabled": False,
    "mandatory_signals": ["T1_ema_short_long", "M1_rsi_below_threshold"],
    "scored_signals": [
        "M3_macd_rising",
        "V1_volume_above_average",
        "V2_obv_rising",
        "X1_atr_sufficient",
        "P1_near_24h_low",
        "R1_reversal_confirmed",
    ],
    "min_scored": 3,
    "veto_signals": ["E1_spread_too_wide", "TM1_bad_hour"],
}

DEFAULT_SIGNAL_THRESHOLDS: Dict[str, Any] = {
    "near_low_pct": 2.0,
    "reversal_volume_multiplier": 1.10,
    "spread_max_pct": 0.10,
    "allowed_trading_hours_utc": "13,14,15,16,17,18,19,20,21,22",
    "stoch_rsi_threshold": 25.0,
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
            if did_fire:
                fired.append(signal_id)
        except Exception as e:
            log.warning("Signal %s failed for %s: %s", signal_id, symbol, e)
            results[signal_id] = {"fired": False, "raw_value": None, "error": str(e)}
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
            return {
                "allowed": False, "reason": f"veto_{sig_id}_fired",
                "mandatory_results": [], "scored_results": [],
                "veto_results": veto_results, "score": 0,
                "fired_signals": list(fired_ids),
            }

    # 2. Mandatory gate
    mandatory_results: List[Tuple[str, bool]] = []
    for sig_id in mandatory_ids:
        did_fire = sig_id in fired_ids
        mandatory_results.append((sig_id, did_fire))
        if not did_fire:
            return {
                "allowed": False, "reason": f"mandatory_{sig_id}_not_fired",
                "mandatory_results": mandatory_results, "scored_results": [],
                "veto_results": veto_results, "score": 0,
                "fired_signals": list(fired_ids),
            }

    # 3. Score gate
    scored_results: List[Tuple[str, bool]] = []
    score = 0
    for sig_id in scored_ids:
        did_fire = sig_id in fired_ids
        scored_results.append((sig_id, did_fire))
        if did_fire:
            score += 1

    if score < min_scored:
        return {
            "allowed": False, "reason": f"score_{score}_below_min_{min_scored}",
            "mandatory_results": mandatory_results, "scored_results": scored_results,
            "veto_results": veto_results, "score": score,
            "fired_signals": list(fired_ids),
        }

    return {
        "allowed": True, "reason": "all_checks_passed",
        "mandatory_results": mandatory_results, "scored_results": scored_results,
        "veto_results": veto_results, "score": score,
        "fired_signals": list(fired_ids),
    }
