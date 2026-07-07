"""
backtest.py — deterministic replay engine (WolfBot v0.4 Phase 1, spec §1.4).

Replays stored 1m klines (database.get_klines) through the LIVE signal
registry (signal_registry.evaluate_buy_decision) — signal/role/veto/scoring
logic is never duplicated here; this module only builds the signal_data dict
from klines (the same feature plumbing trade_engine's signal cache performs)
and simulates fills/exits.

DETERMINISM CONTRACT
    run_backtest() uses no wall-clock time and no randomness in anything that
    reaches its outputs: identical inputs (same DB contents, same arguments,
    same strategy dict) produce byte-identical results. Two registry-internal
    wall-clock caches would break this (R1/M4 cache results per-symbol for
    10–20s of *wall* time), so they are cleared before every evaluation; the
    E1 spread veto (a live network call) is pre-seeded as "no spread" and
    excluded from the decision lists (as is TM1, which reads the wall-clock
    hour) — both are meaningless when replaying history.

FILL SIMULATION RULES (spec, encoded exactly)
    * Phase 3 §3.1: entries are evaluated on each 5m candle CLOSE (the 1m
      candle whose close lands on a 5-minute boundary); the fill happens at
      the NEXT 1m candle's open ± slippage. When 1m candles are missing at
      that point (data gap), the pending fill lands on the next available
      candle's open — which is the next 5m open when a whole 5m block is
      absent. Legacy escape hatch entries.tick_entries=true restores
      per-1m-close evaluation (old behavior).
    * Taker fills cross half the modeled spread (backtest.spread_bps, default
      2 bps) plus tiered slippage.
    * Maker fills only occur if the candle's range crosses the limit price.
    * If TP and SL both fall inside one candle: SL is filled FIRST
      (conservative bias).
    * Slippage by trailing-24h quote-volume tier (sum of kline quote_v):
      5 bps (>= 50M), 10 bps (>= 5M), 20 bps below — configurable via
      strategy["backtest"]["slippage_tiers"] = [[min_quote_vol, bps], ...].
    * Fees come from fees.for_symbol(symbol, strategy) incl. per-symbol
      overrides.

CLI
    python backtest.py --start 2026-01-01 --end 2026-06-30 --symbols approved
"""
from __future__ import annotations

import argparse
import copy
import json
import math
import sys
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

import config
import database
import fees
import indicators
import signal_registry

# ── Constants ─────────────────────────────────────────────────────────────────

MINUTE_MS = 60_000
WARMUP_CANDLES = 1_500          # fetched before start_ms (24h low + indicators)
MIN_EVAL_CANDLES = 40           # MACD(12,26,9) + EMA21 need ~35 closed candles
INDICATOR_WINDOW = 120          # candles fed to the indicator stack per eval
LOW_24H_CANDLES = 1_440         # trailing 24h of 1m candles

DEFAULT_SLIPPAGE_TIERS = [[50_000_000.0, 5.0], [5_000_000.0, 10.0], [0.0, 20.0]]
DEFAULT_SPREAD_BPS = 2.0

# Wall-clock / network-dependent signals excluded from replay decisions.
_NON_REPLAYABLE_SIGNALS = ("E1_spread_too_wide", "TM1_bad_hour")


# ── Small helpers shared with attribution.py ─────────────────────────────────

def slippage_tiers(strategy: dict) -> List[List[float]]:
    """[[min_24h_quote_vol, bps], ...] sorted descending by threshold."""
    bt = strategy.get("backtest") if isinstance(strategy, dict) else None
    raw = bt.get("slippage_tiers") if isinstance(bt, dict) else None
    tiers = []
    if isinstance(raw, list):
        for row in raw:
            try:
                tiers.append([float(row[0]), float(row[1])])
            except (TypeError, ValueError, IndexError):
                continue
    if not tiers:
        tiers = [list(t) for t in DEFAULT_SLIPPAGE_TIERS]
    return sorted(tiers, key=lambda t: -t[0])


def slippage_bps_for(quote_vol_24h: float, tiers: List[List[float]]) -> float:
    for min_qv, bps in tiers:
        if quote_vol_24h >= min_qv:
            return bps
    return tiers[-1][1] if tiers else 20.0


def spread_bps(strategy: dict) -> float:
    bt = strategy.get("backtest") if isinstance(strategy, dict) else None
    try:
        return max(0.0, float(bt.get("spread_bps", DEFAULT_SPREAD_BPS)))
    except (TypeError, ValueError, AttributeError):
        return DEFAULT_SPREAD_BPS


def taker_fill_price(px: float, side: str, slip_bps: float, spread_bps_: float) -> float:
    """Taker fill: cross half the modeled spread plus tiered slippage.
    side='buy' pays up; side='sell' gives up."""
    adj = (slip_bps + spread_bps_ / 2.0) / 10_000.0
    return px * (1.0 + adj) if side == "buy" else px * (1.0 - adj)


# ── Phase 3 §3.1/§3.3/§3.4 — entries / regime config (live-engine parity) ────
# Keep defaults in sync with trade_engine._ENTRIES_DEFAULTS /
# trade_engine._neutral_size_mult.

ENTRIES_DEFAULTS: Dict[str, Any] = {
    "eval_heartbeat_sec":     15.0,   # live-only (tick cadence); unused in replay
    "tick_entries":           False,  # true → legacy per-1m-close evaluation
    "falling_knife_atr_mult": 1.0,    # knife threshold = mult × atr_pct (5m)
    "cooldown_after_sl_min":  15.0,   # per-symbol re-entry cooldown after SL
}

# §3.4c global correlated-dump pause (same constants as trade_engine)
STOP_PAUSE_COUNT        = 3
STOP_PAUSE_WINDOW_MS    = 600_000
STOP_PAUSE_DURATION_MS  = 300_000


def entries_config(strategy: dict) -> dict:
    raw = strategy.get("entries") if isinstance(strategy, dict) else None
    raw = raw if isinstance(raw, dict) else {}
    cfg: Dict[str, Any] = {}
    for key, default in ENTRIES_DEFAULTS.items():
        val = raw.get(key, default)
        try:
            cfg[key] = bool(val) if isinstance(default, bool) else float(val)
        except (TypeError, ValueError):
            cfg[key] = default
    return cfg


def neutral_size_mult(strategy: dict) -> float:
    """§3.3 strategy.regime.neutral_size_mult (default 0.5) — budget
    multiplier applied when the BTC regime is 'neutral'."""
    try:
        raw = strategy.get("regime", {}) if isinstance(strategy, dict) else {}
        return max(0.0, min(1.0, float(raw.get("neutral_size_mult", 0.5))))
    except Exception:
        return 0.5


def _ema_last(values: List[float], period: int) -> float:
    """Seed-at-first-value EMA (same as trade_engine._ema_calc) — returns the
    final EMA value."""
    if not values or period <= 0:
        return 0.0
    k = 2.0 / (period + 1.0)
    ema_val = values[0]
    for v in values[1:]:
        ema_val = v * k + ema_val * (1 - k)
    return ema_val


def btc_regime_from_closes(closes: List[float]) -> str:
    """§3.3 3-state BTC regime classifier over 1h closes (oldest→newest).
    MUST stay in sync with trade_engine.compute_btc_regime_from_closes
    (backtest cannot import trade_engine — trade_engine imports this module):
      risk_off : price < EMA50 OR pct_4h < -2.0
      risk_on  : price > EMA20 > EMA50 AND pct_4h > -0.5
      neutral  : everything else (also when <55 closes are available)."""
    if not closes or len(closes) < 55:
        return "neutral"
    price  = closes[-1]
    ema_20 = _ema_last(closes, 20)
    ema_50 = _ema_last(closes, 50)
    pct_4h = (closes[-1] - closes[-5]) / closes[-5] * 100 if closes[-5] > 0 else 0.0
    if price < ema_50 or pct_4h < -2.0:
        return "risk_off"
    if price > ema_20 > ema_50 and pct_4h > -0.5:
        return "risk_on"
    return "neutral"


def _bb_position_5m(closes5: List[float]) -> Optional[str]:
    """'above_upper' | 'at_upper' | 'inside' from 5m closes (P2 veto input).
    None when the 20-period band can't be computed (P2 fails open)."""
    if not closes5 or len(closes5) < 20:
        return None
    bb_u, _, bb_l = indicators.calc_bollinger(closes5)
    upper, lower = bb_u[-1], bb_l[-1]
    if upper is None or lower is None:
        return None
    px = closes5[-1]
    if px > upper:
        return "above_upper"
    band = upper - lower
    if band > 0 and px >= upper - 0.01 * band:
        return "at_upper"
    return "inside"


def _ema50_15m_slope_from_5m(window5: List[dict]) -> Optional[float]:
    """EMA50 slope (% change across the last two 15m candles) from closed 5m
    candles aggregated 3→1. Needs >=55 15m candles (i.e. >=165 5m candles),
    else None — same rule as the live engine."""
    if not window5 or len(window5) < 55 * 3:
        return None
    series_15m = indicators.aggregate_candles(list(window5), group=3)
    if len(series_15m) < 55:
        return None
    closes = [c["close"] for c in series_15m]
    ema50 = indicators.calc_ema(closes, 50)
    if len(ema50) < 2 or ema50[-1] is None or ema50[-2] is None or ema50[-2] == 0:
        return None
    return round((ema50[-1] - ema50[-2]) / ema50[-2] * 100.0, 6)


def _load_btc_1h(start_ms: int, end_ms: int) -> List[dict]:
    """BTCUSDT 1h candles for §3.3 regime replay, derived from the stored
    kline history: prefer stored 1m aggregated to 1h, else stored 5m
    aggregated 12→1. Empty list → regime stays 'neutral' for the whole run
    (documented fallback)."""
    warmup = 80 * 60 * MINUTE_MS   # 80h of history so EMA50 has warmup
    for interval, group in (("1m", 60), ("5m", 12)):
        try:
            rows = database.get_klines("BTCUSDT", interval,
                                       start_ms=start_ms - warmup,
                                       end_ms=end_ms)
        except Exception:
            rows = []
        candles = [c for c in (_row_to_candle(r) for r in rows) if c is not None]
        if candles:
            hourly = indicators.aggregate_candles(candles, group=group)
            if hourly:
                return hourly
    return []


# ── Phase 2 §2.6 — exits config (SINGLE SOURCE for live + replay) ─────────────
# trade_engine._exit_cfg() imports exit_config() from here (with an identical
# local fallback) so the SAME strategy dict always produces the SAME effective
# exit configuration in the live engine and in the backtester. If you change
# EXIT_DEFAULTS or the legacy mapping, both surfaces pick it up together.

EXIT_DEFAULTS: Dict[str, Any] = {
    "k_sl":                       1.2,    # SL distance = k_sl × ATR%(14, 5m)
    "sl_min_pct":                 0.5,    # clamp floor for SL distance
    "sl_max_pct":                 2.5,    # clamp ceiling for SL distance
    "hard_sl_pct":                3.0,    # crash stop from entry — bypasses ticks/min-hold
    "rr_ratio":                   1.6,    # TP distance = rr_ratio × SL distance
    "tp_buffer_pct":              0.05,   # TP floored at BEP × (1 + buffer/100)
    "min_profit_usdt":            0.01,   # read live via trade_engine._min_profit_usdt
    "breakeven_at_r":             1.0,    # move stop to BEP at +1R unrealized
    "k_trail":                    0.8,    # trail gap = k_trail × ATR% below peak (<=0 disables)
    "smart_hold_score_gate":      False,  # fresh-score gate before trail exits above TP
    "sl_confirm_ticks":           2,      # confirmation ticks for stop_price exits
    "min_hold_sec":               10.0,   # min hold (SL-only, like the legacy guard)
    "maker_tp":                   True,   # consumed by exit_orders.py
    "maker_tp_timeout_ms":        1500,   # consumed by exit_orders.py
    "oco_enabled":                False,  # consumed by exit_orders.py
    "oco_stop_limit_buffer_pct":  0.5,    # consumed by exit_orders.py
    "oco_skip_rescue_sec":        3.0,    # consumed by exit_orders.py
}

LEGACY_EXIT_KEYS = (
    "stop_loss_pct", "take_profit_pct", "trailing_stop_pct",
    "stop_loss_enabled", "take_profit_enabled", "smart_hold_enabled",
)


def exit_config(strategy: dict) -> dict:
    """Effective exits configuration from a strategy dict (§2.6).

    Reads strategy["exits"] with EXIT_DEFAULTS. LEGACY COMPATIBILITY: when the
    strategy has NO "exits" block but carries legacy exit keys, they are mapped
    into the effective config so existing user settings keep behaving until
    the Phase 5 UI writes the new block:

      * stop_loss_pct        → sl_min_pct == sl_max_pct == stop_loss_pct
                               (FIXED stop distance; ATR scaling disabled, k_sl=0)
      * stop_loss_enabled    → sl_enabled (False → no stop_price at all)
      * take_profit_pct      → rr_ratio=None + tp_pct (fixed-TP path:
                               tp = max(entry×(1+tp_pct/100), BEP), buffer=0)
      * take_profit_enabled  → tp_enabled (False → TP trigger = bare BEP)
      * smart_hold_enabled   → smart_hold_score_gate (old score-gated hold)
      * trailing_stop_pct    → legacy_trailing_stop_pct (old smart-hold trail %)
      * hard SL / BE-move / ATR trail are DISABLED (hard_sl_pct=None,
        breakeven_at_r=None, k_trail=0) — the old immediate-sell-at-target
        path stays in force.

    Extra derived keys: legacy_mode, sl_enabled, tp_enabled, tp_pct,
    legacy_trailing_stop_pct.
    """
    strategy = strategy if isinstance(strategy, dict) else {}
    exits_raw = strategy.get("exits")
    has_exits = isinstance(exits_raw, dict)
    exits = exits_raw if has_exits else {}

    cfg: dict = {}
    for key, default in EXIT_DEFAULTS.items():
        val = exits.get(key, default)
        try:
            if isinstance(default, bool):
                cfg[key] = bool(val)
            elif isinstance(default, int) and not isinstance(default, bool):
                cfg[key] = int(val)
            else:
                cfg[key] = float(val)
        except (TypeError, ValueError):
            cfg[key] = default

    cfg["legacy_mode"] = False
    cfg["sl_enabled"] = True
    cfg["tp_enabled"] = True
    cfg["tp_pct"] = None                    # fixed-TP path (legacy only)
    cfg["legacy_trailing_stop_pct"] = None  # old smart-hold trail % (legacy only)

    if not has_exits and any(k in strategy for k in LEGACY_EXIT_KEYS):
        # ── Legacy mapping (see docstring) ────────────────────────────────────
        try:
            sl_pct = float(strategy.get("stop_loss_pct", 0.4))
        except (TypeError, ValueError):
            sl_pct = 0.4
        try:
            tp_pct = float(strategy.get("take_profit_pct", 0.1))
        except (TypeError, ValueError):
            tp_pct = 0.1
        try:
            trail_pct = float(strategy.get("trailing_stop_pct", 0.5))
        except (TypeError, ValueError):
            trail_pct = 0.5
        cfg["legacy_mode"] = True
        cfg["sl_enabled"] = bool(strategy.get("stop_loss_enabled", True))
        cfg["sl_min_pct"] = sl_pct
        cfg["sl_max_pct"] = sl_pct
        cfg["k_sl"] = 0.0                 # fixed distance — ATR scaling off
        cfg["hard_sl_pct"] = None         # no crash stop in legacy behavior
        cfg["rr_ratio"] = None            # fixed tp_pct path instead of R multiple
        cfg["tp_enabled"] = bool(strategy.get("take_profit_enabled", True))
        cfg["tp_pct"] = tp_pct
        cfg["tp_buffer_pct"] = 0.0        # legacy trigger floors at bare BEP
        cfg["breakeven_at_r"] = None      # no BE-move in legacy behavior
        cfg["k_trail"] = 0.0              # immediate sell at target (old path)
        cfg["smart_hold_score_gate"] = bool(strategy.get("smart_hold_enabled", False))
        cfg["legacy_trailing_stop_pct"] = trail_pct
    return cfg


def exit_levels(entry_px: float, qty: float, cost_usdt: float,
                strategy: dict, fee_model: "fees.FeeModel",
                atr_pct: Optional[float] = None) -> dict:
    """Phase 2 §2.1/§2.2 exit geometry — SAME math as the live engine.

      * BEP (same formula as trade_engine.compute_real_breakeven_price):
            bep = (cost + min_profit) / (qty * (1 - taker))
        cost = actually-deployed USDT incl. entry fee; min_profit clamped
        >= 0.001 (engine parity — _min_profit_usdt applies the same clamp).
      * sl_distance_pct = clamp(k_sl × atr_pct, sl_min_pct, sl_max_pct);
        ATR unavailable → sl_min_pct (conservative tight stop, flagged).
      * stop  = entry × (1 − sl_distance/100)         (None when disabled)
      * tp    = max(entry × (1 + rr_ratio×sl_distance/100),
                    bep × (1 + tp_buffer_pct/100))
      * hard  = entry × (1 − hard_sl_pct/100)         (None in legacy)

    Legacy mapping (exit_config) reproduces Phase 1 geometry exactly:
    fixed SL distance, tp = max(entry×(1+tp_pct/100), bep), no hard stop.
    """
    cfg = exit_config(strategy)
    try:
        min_profit = max(0.001, float(cfg.get("min_profit_usdt", 0.01)))
    except (TypeError, ValueError):
        min_profit = 0.01

    taker = fee_model.taker()
    if qty > 0 and taker < 1.0:
        bep = (cost_usdt + min_profit) / (qty * (1.0 - taker))
    else:
        bep = entry_px

    atr_unavailable = False
    if cfg["legacy_mode"]:
        sl_dist: Optional[float] = cfg["sl_min_pct"] if cfg["sl_enabled"] else None
    elif atr_pct is None or atr_pct <= 0:
        sl_dist = cfg["sl_min_pct"]
        atr_unavailable = True
    else:
        sl_dist = min(max(cfg["k_sl"] * atr_pct, cfg["sl_min_pct"]), cfg["sl_max_pct"])

    sl = entry_px * (1.0 - sl_dist / 100.0) if sl_dist else None

    if cfg["rr_ratio"]:
        tp_dist = cfg["rr_ratio"] * (sl_dist if sl_dist else cfg["sl_min_pct"])
    else:  # legacy fixed-TP path
        tp_dist = (cfg["tp_pct"] or 0.0) if cfg["tp_enabled"] else 0.0
    tp = max(entry_px * (1.0 + tp_dist / 100.0),
             bep * (1.0 + (cfg["tp_buffer_pct"] or 0.0) / 100.0))

    hard = (entry_px * (1.0 - cfg["hard_sl_pct"] / 100.0)
            if cfg.get("hard_sl_pct") else None)

    return {
        "tp": tp, "sl": sl, "hard_sl": hard, "bep": bep,
        "sl_distance_pct": sl_dist, "tp_distance_pct": tp_dist,
        "atr_pct": atr_pct, "atr_unavailable": atr_unavailable,
        "cfg": cfg,
    }


def strategy_exit_levels(entry_px: float, qty: float, cost_usdt: float,
                         strategy: dict, fee_model: "fees.FeeModel",
                         atr_pct: Optional[float] = None,
                         ) -> Tuple[float, Optional[float], float]:
    """Back-compat wrapper (attribution.py) — returns (tp, sl_or_None, bep).
    Phase 2: delegates to exit_levels(); without atr_pct a non-legacy strategy
    gets the conservative sl_min_pct stop."""
    lv = exit_levels(entry_px, qty, cost_usdt, strategy, fee_model, atr_pct)
    return lv["tp"], lv["sl"], lv["bep"]


# ── Strategy sanitisation for deterministic replay ────────────────────────────

def _sanitize_strategy(strategy: dict) -> dict:
    """Deep-copy the strategy and force wall-clock/network signals (E1, TM1)
    out of the decision lists. Also seeds signal_registry's E1 spread cache so
    evaluate_signals never issues a network request during replay."""
    s = copy.deepcopy(strategy) if isinstance(strategy, dict) else {}
    eng = s.get("signal_engine")
    if isinstance(eng, dict):
        roles = eng.get("roles")
        if isinstance(roles, dict) and roles:
            for sig in _NON_REPLAYABLE_SIGNALS:
                roles[sig] = "off"
        else:
            defaults = signal_registry.DEFAULT_SIGNAL_ENGINE
            for key in ("mandatory_signals", "scored_signals", "veto_signals"):
                ids = eng.get(key, defaults[key])
                eng[key] = [i for i in ids if i not in _NON_REPLAYABLE_SIGNALS]
    return s


def _seed_spread_cache(symbols: List[str]) -> None:
    """Pre-seed E1's per-symbol spread cache far in the future so the signal
    never performs a live HTTP request mid-replay (fired=False, spread=0)."""
    import time as _t
    far_future = _t.time() + 1e12
    for sym in symbols:
        signal_registry._spread_sr_cache[sym] = {"ts": far_future, "spread_pct": 0.0}


def _clear_registry_caches(symbol: str) -> None:
    """R1 and M4 cache per-symbol results for 10–20s of WALL time — poison for
    a replay where thousands of simulated minutes pass per wall second."""
    signal_registry._reversal_sr_cache.pop(symbol, None)
    signal_registry._pullback_cache.pop(symbol, None)


# ── Data loading / feature building ──────────────────────────────────────────

def _row_to_candle(r: dict) -> Optional[dict]:
    try:
        c = float(r["c"]) if r.get("c") is not None else None
        if c is None:
            return None
        o = float(r["o"]) if r.get("o") is not None else c
        h = float(r["h"]) if r.get("h") is not None else max(o, c)
        l = float(r["l"]) if r.get("l") is not None else min(o, c)
        return {
            "open_time": int(r["open_time"]),
            "open": o, "high": h, "low": l, "close": c,
            "volume": float(r.get("v") or 0.0),
            "quote_v": float(r.get("quote_v") or 0.0),
        }
    except (TypeError, ValueError, KeyError):
        return None


def _load_symbol(symbol: str, start_ms: int, end_ms: int) -> List[dict]:
    rows = database.get_klines(symbol, "1m",
                               start_ms=start_ms - WARMUP_CANDLES * MINUTE_MS,
                               end_ms=end_ms)
    candles = [c for c in (_row_to_candle(r) for r in rows) if c is not None]
    return candles


def _load_5m(symbol: str, candles_1m: List[dict], start_ms: int, end_ms: int) -> List[dict]:
    """5m candles from the store when present, else derived from the 1m series."""
    rows = database.get_klines(symbol, "5m",
                               start_ms=start_ms - WARMUP_CANDLES * MINUTE_MS,
                               end_ms=end_ms)
    fivem = [c for c in (_row_to_candle(r) for r in rows) if c is not None]
    if fivem:
        return fivem
    return indicators.aggregate_candles(candles_1m, group=5)


def _build_signal_data(window: List[dict], low_24h: Optional[float]) -> dict:
    """signal_data dict for signal_registry, built from CLOSED 1m candles.

    Mirrors the feature plumbing of trade_engine's signal cache (its
    evaluate_signals booleans + rsi_value/stoch/low_24h/klines_1m), evaluated
    on the last COMPLETED candle — window[-1] here corresponds to the live
    buffer's [-2] (the live buffer carries an in-progress candle on top).
    The decision logic itself lives in signal_registry, not here.
    """
    closes = [c["close"] for c in window]
    volumes = [c["volume"] for c in window]

    ema9 = indicators.calc_ema(closes, 9)
    ema21 = indicators.calc_ema(closes, 21)
    rsi_vals = indicators.calc_rsi(closes, 14)
    _, _, histo = indicators.calc_macd(closes)
    vol_ma = indicators.calc_volume_ma(volumes, 20)

    trend = bool(ema9[-1] is not None and ema21[-1] is not None
                 and ema9[-1] > ema21[-1])
    rsi_bool = bool(rsi_vals[-1] is not None
                    and config.RSI_BUY_MIN <= rsi_vals[-1] <= config.RSI_BUY_MAX)
    macd = bool(len(histo) >= 2 and histo[-1] is not None and histo[-2] is not None
                and histo[-1] > 0 and histo[-1] > histo[-2])
    volume = bool(vol_ma[-1] is not None and vol_ma[-1] > 0
                  and volumes[-1] >= vol_ma[-1] * config.VOLUME_RATIO_MIN)
    obv = indicators.obv_is_bullish(window)
    atr = indicators.atr_is_tradeable(
        indicators.calc_atr(window, config.ATR_PERIOD),
        closes[-1], config.ATR_MIN_PCT, config.ATR_MAX_PCT)
    try:
        stoch = indicators.calc_stoch_rsi(closes)
    except Exception:
        stoch = None

    return {
        "trend": trend, "rsi": rsi_bool, "macd": macd,
        "volume": volume, "obv": obv, "atr": atr,
        "rsi_value": rsi_vals[-1] if rsi_vals[-1] is not None else 0.0,
        "current_price": closes[-1],
        "low_24h": low_24h,
        "klines_1m": [dict(c) for c in window[-15:]],
        "stoch_rsi_value": stoch,
    }


def _build_signal_data_5m(window5: List[dict], window1m: List[dict],
                          low_24h: Optional[float],
                          atr_pct: Optional[float],
                          btc_regime: str) -> dict:
    """Phase 3 §3.1 signal_data: booleans + rsi/stoch evaluated on CLOSED 5m
    candles (parity with trade_engine.on_kline5m_close, which evaluates the
    5m buffer at [-1]), plus the Phase 3 registry fields:
    klines_5m / ema50_15m_slope / bb_position_5m / atr_pct / btc_regime.
    klines_1m stays 1m-sourced (freshest execution context).

    `window5` may be longer than INDICATOR_WINDOW (the 15m EMA50 slope needs
    ~165 5m candles); the boolean/RSI stack runs on the last INDICATOR_WINDOW
    candles like the 1m path."""
    base = _build_signal_data(window5[-INDICATOR_WINDOW:], low_24h)  # [-1] closed convention
    closes5 = [c["close"] for c in window5[-INDICATOR_WINDOW:]]
    base.update({
        "klines_1m":       [dict(c) for c in window1m[-15:]],
        "klines_5m":       [dict(c) for c in window5[-30:]],
        "ema50_15m_slope": _ema50_15m_slope_from_5m(window5),
        "bb_position_5m":  _bb_position_5m(closes5),
        "atr_pct":         atr_pct,
        "btc_regime":      btc_regime,
    })
    return base


def _entry_atr_pct(fm_series: List[dict], ptr: int, candles_1m: List[dict],
                   i: int, fill_px: float) -> Optional[float]:
    """ATR(14) as % of fill price at entry time (§2.1) — replay mirror of the
    live engine's source ladder: 5m candles (stored or 1m-aggregated, which is
    what _load_5m returns) → 1m ATR × sqrt(5) approximation. `ptr` points one
    past the last CLOSED 5m candle at the fill timestamp; `i` is the index of
    the fill candle (1m candles < i are closed)."""
    window5 = fm_series[max(0, ptr - 40): ptr]
    if len(window5) >= 15:
        atr = indicators.calc_atr(window5, 14)
        if atr and fill_px > 0:
            return atr / fill_px * 100.0
    window1 = candles_1m[max(0, i - 40): i]
    if len(window1) >= 15:
        atr1 = indicators.calc_atr(window1, 14)
        if atr1 and fill_px > 0:
            return atr1 * math.sqrt(5.0) / fill_px * 100.0
    return None


def _bb_gate_ok(window: List[dict]) -> bool:
    closes = [c["close"] for c in window]
    bb_u, bb_m, _ = indicators.calc_bollinger(closes)
    return indicators.bb_buy_allowed(closes[-1], bb_u[-1], bb_m[-1])


# ── Budget / sizing ───────────────────────────────────────────────────────────

def fixed_budget(strategy: dict) -> float:
    try:
        return max(0.0, float(strategy.get("budget_fixed_usdt",
                                           config.BUDGET_FIXED_USDT)))
    except (TypeError, ValueError):
        return float(config.BUDGET_FIXED_USDT)


def _budget_for(strategy: dict, cash: float, deployed: float) -> Optional[float]:
    """Fixed / percent / capped sizing (kept deliberately simple, spec §1.4)."""
    mode = str(strategy.get("budget_mode", "fixed")).lower()
    if mode == "percent":
        try:
            pct = float(strategy.get("budget_pct_of_free", config.BUDGET_PCT_OF_FREE))
        except (TypeError, ValueError):
            pct = config.BUDGET_PCT_OF_FREE
        budget = cash * max(0.0, pct) / 100.0
    elif mode == "capped":
        budget = fixed_budget(strategy)
        try:
            cap = float(strategy.get("budget_total_cap_usdt",
                                     config.BUDGET_TOTAL_CAP_USDT))
        except (TypeError, ValueError):
            cap = config.BUDGET_TOTAL_CAP_USDT
        if cap > 0 and deployed + budget > cap:
            return None
    else:  # fixed (default)
        budget = fixed_budget(strategy)
    if budget <= 0 or budget * 1.01 > cash:   # keep headroom for the entry fee
        return None
    return budget


# ── Core engine ───────────────────────────────────────────────────────────────

def run_backtest(start_ms: int, end_ms: int, symbols: list, strategy: dict,
                 progress_cb: Optional[Callable[[float], None]] = None) -> dict:
    """Deterministic historical replay. See module docstring for the contract.

    Returns {"trades": [...], "equity_curve": [[ts_ms, equity], ...],
             "stats": {...}} — see module docstring / spec §1.4.
    """
    strategy = strategy if isinstance(strategy, dict) else {}
    bt_cfg = strategy.get("backtest") if isinstance(strategy.get("backtest"), dict) else {}
    eval_strategy = _sanitize_strategy(strategy)

    tiers = slippage_tiers(strategy)
    spr_bps = spread_bps(strategy)
    # ── Phase 3 §3.1/§3.3/§3.4 (live-engine parity) ──────────────────────────
    ent_cfg = entries_config(strategy)
    tick_entries = bool(ent_cfg["tick_entries"])        # legacy per-1m eval
    knife_mult = float(ent_cfg["falling_knife_atr_mult"])
    cooldown_ms = int(float(ent_cfg["cooldown_after_sl_min"]) * 60_000)
    neutral_mult = neutral_size_mult(strategy)
    entry_is_maker = bool(bt_cfg.get("entry_is_maker", False))
    exit_is_maker = bool(bt_cfg.get("exit_is_maker", False))
    bb_gate_enabled = bool(bt_cfg.get("bb_gate_enabled", True))
    use_5m_veto = bool(bt_cfg.get("use_5m_veto", True))
    try:
        starting_balance = float(bt_cfg.get("starting_balance_usdt", 1000.0))
    except (TypeError, ValueError):
        starting_balance = 1000.0
    try:
        max_positions = int(strategy.get("max_positions", config.MAX_OPEN_POSITIONS))
    except (TypeError, ValueError):
        max_positions = config.MAX_OPEN_POSITIONS

    start_ms, end_ms = int(start_ms), int(end_ms)

    # ── Load data ────────────────────────────────────────────────────────────
    syms = sorted({str(s).upper() for s in (symbols or [])})
    data: Dict[str, List[dict]] = {}
    fivem: Dict[str, List[dict]] = {}
    fee_models: Dict[str, fees.FeeModel] = {}
    skipped: List[str] = []
    for sym in syms:
        candles = _load_symbol(sym, start_ms, end_ms)
        in_range = [c for c in candles if start_ms <= c["open_time"] <= end_ms]
        if len(in_range) < 2 or len(candles) < MIN_EVAL_CANDLES + 1:
            skipped.append(sym)
            continue
        data[sym] = candles
        # 5m series is always loaded now: the §2.1 ATR-based stop needs it at
        # entry time even when the 5m buy veto is disabled.
        fivem[sym] = _load_5m(sym, candles, start_ms, end_ms)
        fee_models[sym] = fees.for_symbol(sym, strategy)

    _seed_spread_cache(list(data.keys()))

    # ── §3.3 BTC regime replay ───────────────────────────────────────────────
    # Regime is computed from STORED BTCUSDT klines (1m→1h aggregate, else
    # 5m→1h) with the same classifier thresholds as the live engine. When no
    # BTCUSDT history exists the regime stays 'neutral' for the whole run
    # (documented fallback) — entries then size at neutral_size_mult, and the
    # REGIME_risk_off veto never fires.
    btc_1h = _load_btc_1h(start_ms, end_ms)
    _btc_state = {"ptr": 0, "ts": -1, "regime": "neutral"}

    def _regime_at(t_ms: int) -> str:
        """3-state regime using 1h candles CLOSED at t_ms. Query times are
        nondecreasing across the replay loop, so a single forward pointer
        keeps this O(total) and deterministic."""
        if not btc_1h:
            return "neutral"
        if t_ms == _btc_state["ts"]:
            return _btc_state["regime"]
        p = _btc_state["ptr"]
        while p < len(btc_1h) and btc_1h[p]["open_time"] + 3_600_000 <= t_ms:
            p += 1
        # Fill queries (at ts) interleave with eval queries (at ts+1m) within
        # one timeline step — backtrack the (at most one-candle) overshoot so
        # every query sees exactly the candles closed at ITS timestamp.
        while p > 0 and btc_1h[p - 1]["open_time"] + 3_600_000 > t_ms:
            p -= 1
        _btc_state["ptr"] = p
        closes = [c["close"] for c in btc_1h[max(0, p - 72): p]]
        _btc_state["ts"] = t_ms
        _btc_state["regime"] = btc_regime_from_closes(closes)
        return _btc_state["regime"]

    # ── Timeline ─────────────────────────────────────────────────────────────
    ts_events: Dict[int, List[Tuple[str, int]]] = {}
    for sym, candles in data.items():
        for i, c in enumerate(candles):
            ts_events.setdefault(c["open_time"], []).append((sym, i))
    timeline = sorted(ts_events.keys())
    total_steps = sum(len(v) for v in ts_events.values()) or 1

    # ── State ────────────────────────────────────────────────────────────────
    cash = starting_balance
    positions: Dict[str, dict] = {}      # sym -> {qty, entry_px, entry_ts, cost, fee, tp, sl}
    pending: Dict[str, int] = {}         # sym -> candle index to fill at (its open)
    last_close: Dict[str, float] = {}
    low_deques: Dict[str, list] = {s: [] for s in data}      # monotonic (idx, low)
    qv_sums: Dict[str, float] = {s: 0.0 for s in data}       # trailing 24h quote vol
    fivem_ptr: Dict[str, int] = {s: 0 for s in data}
    atr5_ptr: Dict[str, int] = {s: 0 for s in data}          # closed-5m ptr for entry ATR
    # §3.4b/§3.4c protections state (live parity)
    cooldown_until: Dict[str, int] = {}   # sym -> ms until which entries are blocked
    stop_out_ts: List[int] = []           # rolling stop-out timestamps (ms)
    pause_until_ms = 0                    # global correlated-dump pause
    trades: List[dict] = []
    equity_curve: List[List[float]] = []
    exposed_ts = 0
    in_range_ts = 0
    step = 0

    def _equity() -> float:
        eq = cash
        for s, p in positions.items():
            eq += p["qty"] * last_close.get(s, p["entry_px"])
        return eq

    def _mark(ts: int) -> None:
        eq = round(_equity(), 8)
        if equity_curve and equity_curve[-1][0] == ts:
            equity_curve[-1][1] = eq
        else:
            equity_curve.append([ts, eq])

    def _close_position(sym: str, ts: int, raw_px: float, label: str,
                        maker: bool) -> None:
        nonlocal cash, pause_until_ms
        pos = positions.pop(sym)
        fm = fee_models[sym]
        if maker:
            exit_px = raw_px
            fee_frac = fm.maker()
        else:
            slip = slippage_bps_for(qv_sums[sym], tiers)
            exit_px = taker_fill_price(raw_px, "sell", slip, spr_bps)
            fee_frac = fm.taker()
        proceeds = pos["qty"] * exit_px
        exit_fee = proceeds * fee_frac
        cash += proceeds - exit_fee
        gross = pos["qty"] * (exit_px - pos["entry_px"])
        total_fees = pos["fee"] + exit_fee
        trades.append({
            "symbol": sym,
            "entry_ts": pos["entry_ts"],
            "exit_ts": ts,
            "entry_px": pos["entry_px"],
            "exit_px": exit_px,
            "qty": pos["qty"],
            "gross_pnl": gross,
            "fees": total_fees,
            "net_pnl": gross - total_fees,
            "exit_label": label,
            "hold_sec": (ts - pos["entry_ts"]) / 1000.0,
        })
        # ── §3.4b/§3.4c — stop-out protections (live parity) ─────────────────
        # Stop-loss family exits set the per-symbol cooldown and count toward
        # the global correlated-dump pause (>=3 stop-outs in 10 min → 5 min
        # pause of ALL entries). TP/trail/end-of-run exits do not.
        if label in ("backtest_sl", "backtest_hard_sl"):
            cooldown_until[sym] = max(cooldown_until.get(sym, 0),
                                      ts + cooldown_ms)
            stop_out_ts.append(ts)
            stop_out_ts[:] = [t for t in stop_out_ts
                              if t >= ts - STOP_PAUSE_WINDOW_MS]
            if len(stop_out_ts) >= STOP_PAUSE_COUNT and ts >= pause_until_ms:
                pause_until_ms = ts + STOP_PAUSE_DURATION_MS
        _mark(ts)

    # ── Replay ───────────────────────────────────────────────────────────────
    for ts in timeline:
        for sym, i in sorted(ts_events[ts]):
            step += 1
            candles = data[sym]
            candle = candles[i]

            # Rolling structures (also during warmup)
            dq = low_deques[sym]
            while dq and dq[-1][1] >= candle["low"]:
                dq.pop()
            dq.append((i, candle["low"]))
            while dq and dq[0][0] <= i - LOW_24H_CANDLES:
                dq.pop(0)
            qv_sums[sym] += candle["quote_v"]
            if i >= LOW_24H_CANDLES:
                qv_sums[sym] -= candles[i - LOW_24H_CANDLES]["quote_v"]

            in_replay = ts >= start_ms

            # 1) Pending entry scheduled for this candle → fill at its open.
            if pending.get(sym) == i:
                del pending[sym]
                # §3.3/§3.4 fill-time guards (live parity — the live buy loop
                # re-checks these right before executing):
                #   * risk_off regime → entry vetoed, NEVER sized
                #   * neutral regime  → budget × neutral_size_mult
                #   * per-symbol SL cooldown / global stop pause → skip
                _fill_regime = _regime_at(ts)
                if (in_replay and sym not in positions
                        and len(positions) < max_positions
                        and _fill_regime != "risk_off"
                        and ts >= cooldown_until.get(sym, 0)
                        and ts >= pause_until_ms):
                    deployed = sum(p["cost"] for p in positions.values())
                    budget = _budget_for(strategy, cash, deployed)
                    if budget is not None and _fill_regime == "neutral":
                        budget = round(budget * neutral_mult, 8)
                        if budget <= 0:
                            budget = None
                    if budget is not None:
                        fm = fee_models[sym]
                        filled = True
                        if entry_is_maker:
                            # Maker: limit at prev close; fills only if the
                            # candle's range crosses the limit price.
                            limit_px = candles[i - 1]["close"]
                            if candle["low"] <= limit_px:
                                fill_px, fee_frac = limit_px, fm.maker()
                            else:
                                filled = False
                        else:
                            slip = slippage_bps_for(qv_sums[sym], tiers)
                            fill_px = taker_fill_price(candle["open"], "buy",
                                                       slip, spr_bps)
                            fee_frac = fm.taker()
                        if filled and fill_px > 0:
                            qty = budget / fill_px
                            entry_fee = qty * fill_px * fee_frac
                            cost = budget + entry_fee
                            cash -= cost
                            # ── Phase 2 §2.1/§2.2 geometry at entry ──────────
                            fm_series = fivem[sym]
                            aptr = atr5_ptr[sym]
                            while (aptr < len(fm_series)
                                   and fm_series[aptr]["open_time"]
                                   + 5 * MINUTE_MS <= ts):
                                aptr += 1
                            atr5_ptr[sym] = aptr
                            atr_pct = _entry_atr_pct(fm_series, aptr,
                                                     candles, i, fill_px)
                            lv = exit_levels(fill_px, qty, cost, strategy,
                                             fm, atr_pct)
                            cfg = lv["cfg"]
                            atr_ref = (lv["atr_pct"] or lv["sl_distance_pct"]
                                       or cfg["sl_min_pct"])
                            positions[sym] = {
                                "qty": qty, "entry_px": fill_px,
                                "entry_ts": ts, "cost": cost,
                                "fee": entry_fee,
                                "tp": lv["tp"], "sl": lv["sl"],
                                "hard_sl": lv["hard_sl"], "bep": lv["bep"],
                                "sl_dist": lv["sl_distance_pct"],
                                "atr_pct": lv["atr_pct"],
                                # trail parameters frozen at entry (like live)
                                "k_trail": float(cfg["k_trail"] or 0.0),
                                "trail_gap_pct": float(cfg["k_trail"] or 0.0)
                                                 * float(atr_ref),
                                "be_at_r": cfg["breakeven_at_r"],
                                "tp_buffer_pct": float(cfg["tp_buffer_pct"]
                                                       or 0.0),
                                "be_moved": False, "be_stop": 0.0,
                                "peak": 0.0, "ratchet": 0.0,
                            }
                            _mark(ts)

            # 2) Exits — ADVERSE FIRST when several levels sit inside one
            #    candle (SL-first bias extended, §2.1-§2.3): hard SL, then
            #    stop, then trail floor, and only then TP / peak updates.
            if sym in positions:
                pos = positions[sym]
                sl, tp, hard = pos["sl"], pos["tp"], pos["hard_sl"]
                closed = False
                if hard is not None and candle["open"] <= hard:
                    _close_position(sym, ts, candle["open"],
                                    "backtest_hard_sl", False)
                    closed = True
                elif hard is not None and candle["low"] <= hard:
                    _close_position(sym, ts, hard, "backtest_hard_sl", False)
                    closed = True
                elif sl is not None and candle["open"] <= sl:
                    _close_position(sym, ts, candle["open"], "backtest_sl",
                                    False)
                    closed = True
                elif sl is not None and candle["low"] <= sl:
                    _close_position(sym, ts, sl, "backtest_sl", False)
                    closed = True

                if not closed and pos["k_trail"] > 0:
                    # §2.3 BE-move + ATR trail. Adverse-first: the floor from
                    # PRIOR candles is checked before this candle's high may
                    # raise the peak (a spike-and-retrace inside one candle
                    # exits on the pre-spike floor — conservative).
                    if pos["be_moved"]:
                        eff = max(pos["be_stop"], pos["ratchet"],
                                  pos["peak"]
                                  * (1.0 - pos["trail_gap_pct"] / 100.0))
                        if candle["open"] <= eff:
                            _close_position(sym, ts, candle["open"],
                                            "backtest_trail", False)
                            closed = True
                        elif candle["low"] <= eff:
                            _close_position(sym, ts, eff, "backtest_trail",
                                            False)
                            closed = True
                    if not closed:
                        hi = candle["high"]
                        if not pos["be_moved"]:
                            gain_pct = (hi / pos["entry_px"] - 1.0) * 100.0
                            armed = (pos["be_at_r"] is not None
                                     and pos["sl_dist"]
                                     and gain_pct >= pos["sl_dist"]
                                     * float(pos["be_at_r"])) or hi >= tp
                            if armed:
                                # BE-move: stop can no longer lose (§2.3)
                                pos["be_moved"] = True
                                pos["be_stop"] = max(sl or 0.0, pos["bep"])
                                pos["peak"] = hi
                        else:
                            pos["peak"] = max(pos["peak"], hi)
                        if hi >= tp:
                            # TP-touch ratchet: full round-trip below TP
                            # can't happen once the target printed.
                            pos["ratchet"] = max(
                                pos["ratchet"],
                                tp * (1.0 - pos["tp_buffer_pct"] / 100.0))
                elif not closed:
                    # Trailing disabled (k_trail <= 0) or legacy mapping:
                    # old immediate-sell-at-target path.
                    if candle["open"] >= tp:
                        _close_position(sym, ts, candle["open"],
                                        "backtest_tp", exit_is_maker)
                    elif candle["high"] >= tp:
                        _close_position(sym, ts, tp, "backtest_tp",
                                        exit_is_maker)

            # 3) Entry evaluation → schedule next-open fill.
            #    Phase 3 §3.1: entries are evaluated on 5m candle CLOSES only
            #    (the 1m close landing on a 5-minute boundary) and filled at
            #    the next 1m candle's open per the fill rules; when 1m data
            #    has a gap there, the fill lands on the next available candle
            #    (the next 5m open when a whole block is missing). Legacy
            #    escape hatch entries.tick_entries=true evaluates every 1m
            #    close (old behavior).
            close_ts = ts + MINUTE_MS
            is_5m_close = (close_ts % (5 * MINUTE_MS) == 0)
            if (in_replay and sym not in positions and sym not in pending
                    and i + 1 < len(candles) and i + 1 >= MIN_EVAL_CANDLES
                    and (tick_entries or is_5m_close)
                    and close_ts >= cooldown_until.get(sym, 0)   # §3.4b SL cooldown
                    and close_ts >= pause_until_ms):             # §3.4c global pause
                window = candles[max(0, i - INDICATOR_WINDOW + 1): i + 1]
                # Advance the closed-5m pointer ALWAYS (signal data + ATR need
                # it, not just the 5m veto).
                fm_series = fivem[sym]
                ptr = fivem_ptr[sym]
                while (ptr < len(fm_series)
                       and fm_series[ptr]["open_time"] + 5 * MINUTE_MS <= close_ts):
                    ptr += 1
                fivem_ptr[sym] = ptr
                regime = _regime_at(close_ts)
                gates_ok = True
                # §3.3 macro-gate parity: risk_off vetoes ALL entries (the
                # REGIME_risk_off registry veto would also fire — this keeps
                # the gate active even when that veto is configured off).
                if regime == "risk_off":
                    gates_ok = False
                if gates_ok and bb_gate_enabled and not _bb_gate_ok(window):
                    gates_ok = False
                if gates_ok and use_5m_veto:
                    if not indicators.is_5m_bullish(fm_series[max(0, ptr - 60): ptr]):
                        gates_ok = False
                # §3.4a falling-knife (ATR-scaled): drop over the last 3
                # closed 1m candles vs falling_knife_atr_mult × atr_pct
                # (5m ATR% ladder); fixed 0.4% when ATR is unavailable.
                atr_pct_eval = None
                if gates_ok:
                    atr_pct_eval = _entry_atr_pct(fm_series, ptr, candles,
                                                  i + 1, candle["close"])
                    if i >= 3 and candles[i - 3]["close"] > 0:
                        pct_3min = ((candle["close"] - candles[i - 3]["close"])
                                    / candles[i - 3]["close"] * 100.0)
                        knife_thr = (knife_mult * atr_pct_eval
                                     if atr_pct_eval and atr_pct_eval > 0 else 0.4)
                        if pct_3min < -knife_thr:
                            gates_ok = False
                if gates_ok:
                    low_24h = min(v for _, v in dq) if dq else None
                    if tick_entries:
                        # Legacy 1m evaluation + Phase 3 registry fields.
                        sig_data = _build_signal_data(window, low_24h)
                        window5 = fm_series[max(0, ptr - 200): ptr]
                        sig_data.update({
                            "klines_5m":       [dict(c) for c in window5[-30:]],
                            "ema50_15m_slope": _ema50_15m_slope_from_5m(window5),
                            "bb_position_5m":  _bb_position_5m(
                                [c["close"] for c in window5[-INDICATOR_WINDOW:]]),
                            "atr_pct":         atr_pct_eval,
                            "btc_regime":      regime,
                        })
                    else:
                        # §3.1 default: signals evaluated on closed 5m candles
                        # (200-candle slice so the 15m EMA50 slope has its
                        # 165-candle requirement).
                        window5 = fm_series[max(0, ptr - 200): ptr]
                        if len(window5) < 3:
                            window5 = []
                        if not window5:
                            sig_data = None
                        else:
                            sig_data = _build_signal_data_5m(
                                window5, window, low_24h, atr_pct_eval, regime)
                    if sig_data is not None:
                        _clear_registry_caches(sym)
                        decision = signal_registry.evaluate_buy_decision(
                            sym, sig_data, eval_strategy)
                        if decision.get("allowed"):
                            pending[sym] = i + 1

            last_close[sym] = candle["close"]

            if progress_cb is not None and step % 5000 == 0:
                try:
                    progress_cb(step / total_steps)
                except Exception:
                    pass

        if ts >= start_ms:
            in_range_ts += 1
            if positions:
                exposed_ts += 1
            if ts % 3_600_000 == 0 or not equity_curve:
                _mark(ts)

    # Force-close leftovers at each symbol's final close (taker).
    for sym in sorted(positions.keys()):
        candle = data[sym][-1]
        _close_position(sym, candle["open_time"], candle["close"],
                        "backtest_end", False)

    if timeline:
        _mark(timeline[-1])
    if progress_cb is not None:
        try:
            progress_cb(1.0)
        except Exception:
            pass

    return {
        "trades": trades,
        "equity_curve": equity_curve,
        "stats": _compute_stats(trades, equity_curve, starting_balance,
                                exposed_ts, in_range_ts, skipped),
    }


def _compute_stats(trades: List[dict], equity_curve: List[List[float]],
                   starting_balance: float, exposed_ts: int, total_ts: int,
                   skipped: List[str]) -> dict:
    wins = [t for t in trades if t["net_pnl"] > 0]
    losses = [t for t in trades if t["net_pnl"] <= 0]
    total_fees = sum(t["fees"] for t in trades)
    gross_wins = sum(t["gross_pnl"] for t in trades if t["gross_pnl"] > 0)
    gross_losses = -sum(t["gross_pnl"] for t in trades if t["gross_pnl"] < 0)
    net_sum = sum(t["net_pnl"] for t in trades)

    peak, max_dd = None, 0.0
    for _, eq in equity_curve:
        if peak is None or eq > peak:
            peak = eq
        elif peak > 0:
            max_dd = max(max_dd, (peak - eq) / peak)

    n = len(trades)
    return {
        "trades": n,
        "win_rate": round(len(wins) / n, 4) if n else 0.0,
        "avg_win": (sum(t["net_pnl"] for t in wins) / len(wins)) if wins else 0.0,
        "avg_loss": (sum(t["net_pnl"] for t in losses) / len(losses)) if losses else 0.0,
        "expectancy_per_trade": (net_sum / n) if n else 0.0,
        "profit_factor": (round(gross_wins / gross_losses, 4)
                          if gross_losses > 0 else None),
        "max_drawdown": round(max_dd, 6),
        "total_fees": total_fees,
        "fee_share_of_gross": (round(total_fees / gross_wins, 4)
                               if gross_wins > 0 else None),
        "avg_hold_time": (sum(t["hold_sec"] for t in trades) / n) if n else 0.0,
        "exposure_pct": round(exposed_ts / total_ts * 100.0, 2) if total_ts else 0.0,
        "starting_balance": starting_balance,
        "ending_equity": equity_curve[-1][1] if equity_curve else starting_balance,
        "net_pnl": net_sum,
        "skipped_symbols": skipped,
    }


# ── CLI ───────────────────────────────────────────────────────────────────────

def _load_strategy_file(path: Optional[str] = None) -> dict:
    p = path or getattr(config, "STRATEGY_FILE", None)
    if not p:
        return {}
    try:
        with open(p, "r") as f:
            s = json.load(f)
        return s if isinstance(s, dict) else {}
    except Exception:
        return {}


def _approved_symbols(strategy: dict) -> List[str]:
    coins = strategy.get("approved_coins", []) if isinstance(strategy, dict) else []
    out = []
    for c in coins:
        if isinstance(c, str):
            out.append(c.upper())
        elif isinstance(c, dict) and c.get("approved") and c.get("symbol"):
            out.append(str(c["symbol"]).upper())
    return out


def _parse_date_ms(s: str) -> int:
    try:
        dt = datetime.strptime(s, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(description="WolfBot deterministic backtest")
    p.add_argument("--start", required=True, help="YYYY-MM-DD (UTC)")
    p.add_argument("--end", required=True, help="YYYY-MM-DD (UTC, inclusive)")
    p.add_argument("--symbols", default="approved",
                   help="'approved' (strategy.json approved_coins) or CSV list")
    p.add_argument("--strategy", default=None,
                   help="path to a strategy JSON (default: live strategy.json)")
    p.add_argument("--out", default=None, help="write full result JSON here")
    args = p.parse_args(argv)

    strategy = _load_strategy_file(args.strategy)
    if args.symbols.strip().lower() == "approved":
        symbols = _approved_symbols(strategy)
        if not symbols:
            print("No approved coins in strategy.json — pass --symbols SYM1,SYM2")
            return 1
    else:
        symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]

    start_ms = _parse_date_ms(args.start)
    end_ms = _parse_date_ms(args.end) + 86_400_000 - 1   # inclusive end day

    def _progress(frac: float) -> None:
        sys.stderr.write(f"\r[Backtest] {frac * 100.0:5.1f}%")
        sys.stderr.flush()

    result = run_backtest(start_ms, end_ms, symbols, strategy,
                          progress_cb=_progress)
    sys.stderr.write("\n")

    if args.out:
        with open(args.out, "w") as f:
            json.dump(result, f, indent=2)
        print(f"Full result written to {args.out}")
    print(json.dumps({"stats": result["stats"],
                      "trades": len(result["trades"])}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
