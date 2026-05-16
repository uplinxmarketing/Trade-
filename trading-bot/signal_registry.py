"""
Signal registry for WolfBot trading engine.

This module centralizes signal definitions. Each signal has:
  - id: unique identifier (e.g. "T1_ema_short_long")
  - category: trend | momentum | volume | volatility
  - description: human-readable
  - compute_fn: function(symbol, signal_data, strategy) -> (fired: bool, raw_value: Any)

Phase 1 only implements the 6 existing signals. Future phases add more signals to this
registry without changing the engine.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple

log = logging.getLogger(__name__)


@dataclass
class SignalDef:
    """Definition of a single tradeable signal."""
    id: str
    category: str
    description: str
    compute_fn: Callable[[str, dict, dict], Tuple[bool, Any]]


# ── Global registry ───────────────────────────────────────────────────────────

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
# Wrappers delegate to pre-computed values in signal_data.
# Phase 1 does NOT change computation logic — it only makes signals discoverable.

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


register_signal(SignalDef(
    id="T1_ema_short_long",
    category="trend",
    description="EMA9 > EMA21 on 1h (short-term uptrend)",
    compute_fn=_signal_ema_trend,
))
register_signal(SignalDef(
    id="M1_rsi_below_threshold",
    category="momentum",
    description="RSI(14) below configured threshold (oversold entry)",
    compute_fn=_signal_rsi,
))
register_signal(SignalDef(
    id="M3_macd_rising",
    category="momentum",
    description="MACD histogram positive AND rising",
    compute_fn=_signal_macd_rising,
))
register_signal(SignalDef(
    id="V1_volume_above_average",
    category="volume",
    description="Current volume > recent average",
    compute_fn=_signal_volume,
))
register_signal(SignalDef(
    id="V2_obv_rising",
    category="volume",
    description="OBV rising (accumulation pressure)",
    compute_fn=_signal_obv,
))
register_signal(SignalDef(
    id="X1_atr_sufficient",
    category="volatility",
    description="ATR within tradeable range",
    compute_fn=_signal_atr,
))


# ── Evaluation helpers ────────────────────────────────────────────────────────

def evaluate_signals(
    symbol: str,
    signal_data: dict,
    strategy: dict,
) -> Dict[str, Any]:
    """Evaluate all registered signals for a symbol. Returns fired list + full results."""
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


def evaluate_buy_decision(
    symbol: str,
    signal_data: dict,
    strategy: dict,
) -> Dict[str, Any]:
    """
    Evaluate the layered buy decision using the registry.

    Phase 1 behavior: maps existing strategy.json keys onto the new architecture so
    results are identical to _check_buys_from_cache. Future phases will support the
    new 'signal_engine' sub-key for finer control.

    Returns: {allowed, reason, mandatory_results, scored_results, score, fired_signals}
    """
    evaluation = evaluate_signals(symbol, signal_data, strategy)
    fired_ids = set(evaluation["fired_signals"])

    # New-style config (Phase 3+) takes precedence over old keys
    signal_engine_config = strategy.get("signal_engine")
    if signal_engine_config and isinstance(signal_engine_config, dict):
        mandatory_ids = signal_engine_config.get("mandatory_signals", [])
        scored_ids = signal_engine_config.get("scored_signals", [])
        min_scored = int(signal_engine_config.get("min_scored", 0))
    else:
        # Phase 1: map old keys exactly
        mandatory_enabled = bool(strategy.get("mandatory_signals_enabled", True))
        mandatory_ids = ["T1_ema_short_long", "M1_rsi_below_threshold"] if mandatory_enabled else []
        scored_ids = [
            "T1_ema_short_long",
            "M1_rsi_below_threshold",
            "M3_macd_rising",
            "V1_volume_above_average",
            "V2_obv_rising",
            "X1_atr_sufficient",
        ]
        min_scored = int(strategy.get("min_signals", 4))

    # Mandatory gate
    mandatory_results: List[Tuple[str, bool]] = []
    for sig_id in mandatory_ids:
        did_fire = sig_id in fired_ids
        mandatory_results.append((sig_id, did_fire))
        if not did_fire:
            return {
                "allowed": False,
                "reason": f"mandatory_{sig_id}_not_fired",
                "mandatory_results": mandatory_results,
                "scored_results": [],
                "score": 0,
                "fired_signals": list(fired_ids),
            }

    # Score gate
    scored_results: List[Tuple[str, bool]] = []
    score = 0
    for sig_id in scored_ids:
        did_fire = sig_id in fired_ids
        scored_results.append((sig_id, did_fire))
        if did_fire:
            score += 1

    if score < min_scored:
        return {
            "allowed": False,
            "reason": f"score_{score}_below_min_{min_scored}",
            "mandatory_results": mandatory_results,
            "scored_results": scored_results,
            "score": score,
            "fired_signals": list(fired_ids),
        }

    return {
        "allowed": True,
        "reason": "all_checks_passed",
        "mandatory_results": mandatory_results,
        "scored_results": scored_results,
        "score": score,
        "fired_signals": list(fired_ids),
    }
