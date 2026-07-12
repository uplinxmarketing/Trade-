"""mr_shadow.py — lightweight WolfScore-MR shadow validator.

Runs MR ALONGSIDE the live engine WITHOUT placing any real orders: each scan tick
it takes the MR scores + live prices, opens VIRTUAL entries (score >= threshold,
top-ranked, <= max virtual slots, not already held), and steps each virtual
position through a simulation of the REAL exit parameters (ratchet arm at a peak
price gain, ATR trail, give-back, and the hard disaster stop). Closed virtual
trades are aggregated into the validation report the operator gates go-live on:
n, win%, avg win %, avg loss %, profit factor, disaster-stop count, $/day, and the
realized entry cost per trade (spread + fee) — because the backtest is very
fee-sensitive.

This is intentionally NOT the heavy paper_shadow flywheel (which caused GIL/lock
contention): it holds a handful of virtual positions in memory, does no DB writes
on the hot path, and never touches Binance. Fully guarded — never raises into the
caller.
"""

import time
import threading
from typing import Dict, List, Optional

# ── Config (the backtested MR optimums; the sim mirrors the real exit PARAMS) ────
MR_SHADOW_THRESHOLD   = 55.0     # buy_score_threshold for MR
MR_SHADOW_MAX_SLOTS   = 8        # backtest: 8 slots (do NOT raise)
MR_TICKET_USDT        = 22.0     # sizing the backtest used ($22 × 8)
_RATCHET_ACTIVATE_PCT = 0.015    # arm ratchet at +1.5% peak price gain
_RATCHET_K_ATR        = 1.0      # trail = peak - k_atr × ATR(price)
_RATCHET_GIVEBACK     = 0.50     # exit if profit gives back >= 50% of peak
_HARD_SL_PCT          = 0.20     # 20% disaster stop (price drop from entry)
_BASE_STOP_ATR_MULT   = 3.0      # base protective stop = entry - 3×ATR (pre-arm)
_MAX_HOLD_SEC         = 48 * 3600
_MAX_CLOSED           = 1000     # bounded history

_lock = threading.Lock()
_open: Dict[str, dict] = {}      # sym -> virtual position
_closed: List[dict] = []         # ring of closed virtual trades
_started_ts = time.time()


def _fee_frac_default() -> float:
    try:
        import config as _cfg
        return float(getattr(_cfg, "FEE_RATE", 0.001) or 0.001)
    except Exception:
        return 0.001


def mr_shadow_tick(scored: Dict[str, dict], prices: Dict[str, float],
                   atr_price: Dict[str, float], fee_frac: Dict[str, float],
                   spread_frac: Dict[str, float]) -> None:
    """One shadow step. `scored` = {sym: mr_result_dict (pct, hard_gate)}, `prices`
    = current price per sym, `atr_price` = ATR in PRICE units, `fee_frac`/`spread_frac`
    = per-sym round-trip fee & spread fractions (for realized entry cost). Steps open
    virtual positions through the exit sim, then opens new top-ranked entries."""
    try:
        now = time.time()
        with _lock:
            # 1) step + maybe close existing virtual positions
            for sym in list(_open.keys()):
                pos = _open[sym]
                px = float(prices.get(sym) or 0.0)
                if px <= 0:
                    continue
                _step_position(sym, pos, px, float(atr_price.get(sym) or 0.0), now)

            # 2) open new entries — top-ranked MR coins >= threshold, slots free
            free = MR_SHADOW_MAX_SLOTS - len(_open)
            if free > 0:
                cand = []
                for sym, res in (scored or {}).items():
                    if sym in _open:
                        continue
                    if not isinstance(res, dict) or res.get("hard_gate"):
                        continue
                    pct = res.get("pct")
                    if pct is None or float(pct) < MR_SHADOW_THRESHOLD:
                        continue
                    px = float(prices.get(sym) or 0.0)
                    if px <= 0:
                        continue
                    cand.append((float(pct), sym, px))
                cand.sort(reverse=True)
                for _pct, sym, px in cand[:free]:
                    _ff = float(fee_frac.get(sym) or _fee_frac_default())
                    _sf = float(spread_frac.get(sym) or 0.0)
                    # realized entry cost = spread crossed + round-trip fee, on ticket
                    entry_cost = MR_TICKET_USDT * (_sf + 2.0 * _ff)
                    _open[sym] = {
                        "entry": px, "peak": px, "armed": False,
                        "ts": now, "score": _pct, "qty": MR_TICKET_USDT / px,
                        "fee_frac": _ff, "spread_frac": _sf, "entry_cost": entry_cost,
                    }
    except Exception:
        pass


def _step_position(sym: str, pos: dict, px: float, atr_px: float, now: float) -> None:
    """Advance one virtual position; close it (record trade) when an exit fires."""
    entry = float(pos["entry"])
    if entry <= 0:
        return
    pos["peak"] = max(float(pos["peak"]), px)
    peak = float(pos["peak"])
    gain = px / entry - 1.0

    reason = None
    # disaster stop (20% price drop) — the no-stop strategy's tail guard
    if px <= entry * (1.0 - _HARD_SL_PCT):
        reason = "disaster"
    # base protective stop before the ratchet arms (ATR-based)
    elif (not pos["armed"]) and atr_px > 0 and px <= entry - _BASE_STOP_ATR_MULT * atr_px:
        reason = "base_stop"
    else:
        # arm the ratchet at +1.5% peak gain
        if not pos["armed"] and peak >= entry * (1.0 + _RATCHET_ACTIVATE_PCT):
            pos["armed"] = True
        if pos["armed"]:
            trail = peak - _RATCHET_K_ATR * atr_px if atr_px > 0 else peak * (1.0 - _RATCHET_ACTIVATE_PCT)
            peak_gain = peak / entry - 1.0
            giveback_floor = entry * (1.0 + peak_gain * (1.0 - _RATCHET_GIVEBACK))
            if px <= trail or px <= giveback_floor:
                reason = "ratchet"
    if reason is None and (now - float(pos["ts"])) >= _MAX_HOLD_SEC:
        reason = "timeout"
    if reason is None:
        return

    # close: net PnL after round-trip fees + the crossed spread on entry
    qty = float(pos["qty"])
    ff = float(pos["fee_frac"]); sf = float(pos["spread_frac"])
    gross = (px - entry) * qty
    fees = (entry * qty + px * qty) * ff
    net = gross - fees - (entry * qty * sf)   # spread paid crossing in
    _closed.append({
        "symbol": sym, "entry": entry, "exit": px, "reason": reason,
        "pnl_pct": round(gain * 100.0, 3),
        "net_usdt": round(net, 5), "gross_usdt": round(gross, 5),
        "entry_cost_usdt": round(float(pos["entry_cost"]), 5),
        "hold_sec": round(now - float(pos["ts"]), 1),
        "score": round(float(pos["score"]), 1), "ts": now,
    })
    while len(_closed) > _MAX_CLOSED:
        _closed.pop(0)
    _open.pop(sym, None)


def get_mr_shadow_report() -> dict:
    """Validation report — the numbers the operator gates go-live on."""
    with _lock:
        trades = list(_closed)
        n_open = len(_open)
    n = len(trades)
    if n == 0:
        return {"n": 0, "open": n_open, "note": "no closed virtual MR trades yet",
                "threshold": MR_SHADOW_THRESHOLD, "ticket_usdt": MR_TICKET_USDT,
                "started_ts": _started_ts}
    wins = [t for t in trades if t["net_usdt"] > 0]
    losses = [t for t in trades if t["net_usdt"] <= 0]
    win_pct = 100.0 * len(wins) / n
    avg_win_pct = (sum(t["pnl_pct"] for t in wins) / len(wins)) if wins else 0.0
    avg_loss_pct = (sum(t["pnl_pct"] for t in losses) / len(losses)) if losses else 0.0
    gross_win = sum(t["net_usdt"] for t in wins)
    gross_loss = -sum(t["net_usdt"] for t in losses)
    pf = (gross_win / gross_loss) if gross_loss > 0 else None
    disasters = sum(1 for t in trades if t["reason"] == "disaster")
    total_net = sum(t["net_usdt"] for t in trades)
    total_fees = sum(t["entry_cost_usdt"] for t in trades)
    elapsed_days = max(1e-6, (time.time() - _started_ts) / 86400.0)
    return {
        "n": n, "open": n_open,
        "win_pct": round(win_pct, 1),
        "avg_win_pct": round(avg_win_pct, 3),
        "avg_loss_pct": round(avg_loss_pct, 3),
        "profit_factor": (round(pf, 3) if pf is not None else None),
        "disaster_stops": disasters,
        "total_net_usdt": round(total_net, 4),
        "usd_per_day": round(total_net / elapsed_days, 4),
        "avg_entry_cost_usdt": round(total_fees / n, 5),
        "entry_cost_pct_of_ticket": round(100.0 * (total_fees / n) / MR_TICKET_USDT, 3),
        "threshold": MR_SHADOW_THRESHOLD, "ticket_usdt": MR_TICKET_USDT,
        "max_slots": MR_SHADOW_MAX_SLOTS,
        "go_live_bar": {"min_trades": 200, "win_pct_gt": 88.0, "pf_gt": 1.5},
        "meets_bar": bool(n >= 200 and win_pct > 88.0 and (pf or 0) > 1.5),
        "started_ts": _started_ts, "elapsed_days": round(elapsed_days, 3),
        "exit_by_reason": {r: sum(1 for t in trades if t["reason"] == r)
                           for r in ("ratchet", "base_stop", "disaster", "timeout")},
    }
