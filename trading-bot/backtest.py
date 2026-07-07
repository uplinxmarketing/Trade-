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
    * Entries are evaluated on candle CLOSE; the fill happens at the NEXT
      candle's open ± slippage.
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


def strategy_exit_levels(entry_px: float, qty: float, cost_usdt: float,
                         strategy: dict, fee_model: "fees.FeeModel",
                         ) -> Tuple[float, Optional[float], float]:
    """STRATEGY-EXIT (current live engine's exit geometry, replicated).

    NOTE: Phase 2 will parameterize this into pluggable exit strategies;
    for Phase 1 it deliberately mirrors trade_engine's geometry:

      * BEP floor (same formula shape as compute_real_breakeven_price):
            bep = (cost + min_profit) / (qty * (1 - taker))
        where cost = actually-deployed USDT incl. entry fee.
      * TP = max(BEP, entry * (1 + take_profit_pct/100))
      * SL = entry * (1 - stop_loss_pct/100)   (None when disabled)

    Reads exits.min_profit_usdt (default 0.01), take_profit_pct (default 0.1),
    stop_loss_pct (default 0.4), stop_loss_enabled (default True) from the
    strategy dict. Returns (tp_price, sl_price_or_None, bep_price).
    """
    exits = strategy.get("exits") if isinstance(strategy.get("exits"), dict) else {}
    try:
        min_profit = max(0.0, float(exits.get("min_profit_usdt", 0.01)))
    except (TypeError, ValueError):
        min_profit = 0.01
    try:
        tp_pct = float(strategy.get("take_profit_pct", 0.1))
    except (TypeError, ValueError):
        tp_pct = 0.1
    try:
        sl_pct = float(strategy.get("stop_loss_pct", 0.4))
    except (TypeError, ValueError):
        sl_pct = 0.4
    sl_enabled = bool(strategy.get("stop_loss_enabled", True))

    taker = fee_model.taker()
    if qty > 0 and taker < 1.0:
        bep = (cost_usdt + min_profit) / (qty * (1.0 - taker))
    else:
        bep = entry_px
    tp = max(bep, entry_px * (1.0 + tp_pct / 100.0))
    sl = entry_px * (1.0 - sl_pct / 100.0) if sl_enabled else None
    return tp, sl, bep


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
        fivem[sym] = _load_5m(sym, candles, start_ms, end_ms) if use_5m_veto else []
        fee_models[sym] = fees.for_symbol(sym, strategy)

    _seed_spread_cache(list(data.keys()))

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
        nonlocal cash
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
                if in_replay and sym not in positions and len(positions) < max_positions:
                    deployed = sum(p["cost"] for p in positions.values())
                    budget = _budget_for(strategy, cash, deployed)
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
                            tp, sl, _bep = strategy_exit_levels(
                                fill_px, qty, cost, strategy, fm)
                            positions[sym] = {
                                "qty": qty, "entry_px": fill_px,
                                "entry_ts": ts, "cost": cost,
                                "fee": entry_fee, "tp": tp, "sl": sl,
                            }
                            _mark(ts)

            # 2) Exits — SL first when both levels sit inside one candle.
            if sym in positions:
                pos = positions[sym]
                sl, tp = pos["sl"], pos["tp"]
                if sl is not None and candle["open"] <= sl:
                    _close_position(sym, ts, candle["open"], "backtest_sl", False)
                elif sl is not None and candle["low"] <= sl:
                    _close_position(sym, ts, sl, "backtest_sl", False)
                elif candle["open"] >= tp:
                    _close_position(sym, ts, candle["open"], "backtest_tp",
                                    exit_is_maker)
                elif candle["high"] >= tp:
                    _close_position(sym, ts, tp, "backtest_tp", exit_is_maker)

            # 3) Entry evaluation on candle close → schedule next-open fill.
            if (in_replay and sym not in positions and sym not in pending
                    and i + 1 < len(candles) and i + 1 >= MIN_EVAL_CANDLES):
                window = candles[max(0, i - INDICATOR_WINDOW + 1): i + 1]
                gates_ok = True
                if bb_gate_enabled and not _bb_gate_ok(window):
                    gates_ok = False
                if gates_ok and use_5m_veto:
                    fm_series = fivem[sym]
                    ptr = fivem_ptr[sym]
                    close_ts = ts + MINUTE_MS
                    while (ptr < len(fm_series)
                           and fm_series[ptr]["open_time"] + 5 * MINUTE_MS <= close_ts):
                        ptr += 1
                    fivem_ptr[sym] = ptr
                    if not indicators.is_5m_bullish(fm_series[max(0, ptr - 60): ptr]):
                        gates_ok = False
                if gates_ok:
                    low_24h = min(v for _, v in dq) if dq else None
                    sig_data = _build_signal_data(window, low_24h)
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
