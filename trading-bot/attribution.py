"""
attribution.py — signal edge report (WolfBot v0.4 Phase 1, spec §1.5).

build_edge_report(days=7, source='live') answers two questions:

  1. Per SIGNAL: among executed trades, what was the expectancy (avg net
     profit) when the signal FIRED at entry vs when it DIDN'T? Trades are
     joined to their entry_snapshots (executed=True) by symbol + nearest
     timestamp within a small window.

  2. Per VETO / near-miss REASON: what did the block "save"? Each near-miss
     snapshot (executed=False) is replayed as a hypothetical position against
     stored klines — entry at the next 1m candle open after the snapshot ts,
     exit via the SAME rules as the backtester (backtest.strategy_exit_levels;
     SL-first; max hold 24h). Positive saved_usdt = the veto prevented losses.

run_nightly() builds the 7-day report and stores it in the settings table
(edge_report_json / edge_report_ts). A separate module schedules and exposes
it — this function only needs to be importable and safe (it never raises).

Graceful on sparse data: any section with fewer than MIN_SAMPLE trades
reports {"insufficient_data": true, "n": count}.
"""
from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import config
import database
import fees
import signal_registry
import backtest

MIN_SAMPLE = 5                 # below this a section is insufficient_data
JOIN_WINDOW_SEC = 180.0        # trade timestamp_buy ↔ snapshot ts tolerance
MAX_HOLD_CANDLES = 1_440       # 24h max hold for hypothetical replays
MAX_REPLAYS = 500              # cap per report run (nightly cost control)
TRADE_FETCH_LIMIT = 100_000
# Memory: each snapshot carries heavy parsed raw_json + gates_json dicts, and
# build_edge_report fetches snapshots TWICE (executed + near-misses). At 20_000
# this materialized ~800k JSON objects at once and spiked RSS mid-warmup,
# tripping the 800MB soft-cap → restart loop. 2_000 is ample for stable
# per-signal expectancy and the capped (MAX_REPLAYS) veto replays.
SNAPSHOT_FETCH_LIMIT = 2_000


# ── Helpers ───────────────────────────────────────────────────────────────────

def _load_strategy() -> dict:
    try:
        with open(config.STRATEGY_FILE, "r") as f:
            s = json.load(f)
        return s if isinstance(s, dict) else {}
    except Exception:
        return {}


def _parse_ts(value: Any) -> Optional[float]:
    """ISO timestamp (trades.timestamp_buy) → epoch seconds. Naive = UTC."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except (ValueError, TypeError):
        return None


def _fired_ids(snap: dict) -> set:
    """Extract the set of signal ids that FIRED from a snapshot's raw JSON.
    Tolerant of several plausible shapes (fired_signals list, nested
    evaluation dict, all_results {id: {fired: bool}})."""
    raw = snap.get("raw") or {}
    if not isinstance(raw, dict):
        return set()
    candidates = [raw, raw.get("evaluation"), raw.get("decision"),
                  raw.get("signals")]
    for c in candidates:
        if isinstance(c, dict) and isinstance(c.get("fired_signals"), list):
            return {str(x) for x in c["fired_signals"]}
    for c in candidates:
        if not isinstance(c, dict):
            continue
        ar = c.get("all_results")
        if isinstance(ar, dict):
            return {k for k, v in ar.items()
                    if isinstance(v, dict) and v.get("fired")}
    return set()


def _veto_reason(snap: dict) -> str:
    """Best-effort rejection reason for a near-miss snapshot."""
    raw = snap.get("raw") or {}
    if isinstance(raw, dict):
        for c in (raw, raw.get("evaluation"), raw.get("decision")):
            if isinstance(c, dict):
                r = c.get("reason") or c.get("rejection_reason")
                if r:
                    return str(r)
    gates = snap.get("gates") or {}
    if isinstance(gates, dict):
        failed = sorted(k for k, v in gates.items()
                        if v in (False, 0, "fail", "failed", "blocked"))
        if failed:
            return f"gate_{failed[0]}"
    return str(snap.get("origin") or "unknown")


def _group_stats(pnls: List[float]) -> dict:
    n = len(pnls)
    if n < MIN_SAMPLE:
        return {"insufficient_data": True, "n": n}
    wins = sum(1 for p in pnls if p > 0)
    return {
        "trades": n,
        "expectancy": round(sum(pnls) / n, 6),
        "win_rate": round(wins / n, 4),
    }


# ── Trade ↔ snapshot join ────────────────────────────────────────────────────

def _joined_trades(trades: List[dict], snapshots: List[dict]
                   ) -> List[Tuple[dict, dict]]:
    """Pair each executed trade with the nearest executed-entry snapshot for
    the same symbol within JOIN_WINDOW_SEC. Each snapshot is used once."""
    by_symbol: Dict[str, List[dict]] = {}
    for s in snapshots:
        if s.get("symbol"):
            by_symbol.setdefault(str(s["symbol"]), []).append(s)
    for lst in by_symbol.values():
        lst.sort(key=lambda s: s.get("ts") or 0.0)

    used: set = set()
    pairs: List[Tuple[dict, dict]] = []
    for t in trades:
        t_ts = _parse_ts(t.get("timestamp_buy"))
        if t_ts is None:
            continue
        best, best_d = None, None
        for s in by_symbol.get(str(t.get("coin")), []):
            if s.get("id") in used or s.get("ts") is None:
                continue
            d = abs(float(s["ts"]) - t_ts)
            if d <= JOIN_WINDOW_SEC and (best_d is None or d < best_d):
                best, best_d = s, d
        if best is not None:
            used.add(best.get("id"))
            pairs.append((t, best))
    return pairs


# ── Hypothetical replay of near-misses ────────────────────────────────────────

def _replay_near_miss(snap: dict, strategy: dict) -> Optional[dict]:
    """Replay one blocked entry as a hypothetical position.

    Entry: taker fill at the open of the first stored 1m candle AFTER the
    snapshot ts. Exit: backtest exit geometry (SL first), max hold 24h,
    else close of the last available candle ('timeout'). Returns
    {net_pnl, exit_label} or None when kline data is missing."""
    sym = snap.get("symbol")
    snap_ts = snap.get("ts")
    if not sym or snap_ts is None:
        return None
    snap_ms = int(float(snap_ts) * 1000)
    rows = database.get_klines(sym, "1m", start_ms=snap_ms,
                               end_ms=snap_ms + (MAX_HOLD_CANDLES + 5) * 60_000)
    candles = [c for c in (backtest._row_to_candle(r) for r in rows)
               if c is not None and c["open_time"] > snap_ms]
    if not candles:
        return None

    fee_model = fees.for_symbol(sym, strategy)
    tiers = backtest.slippage_tiers(strategy)
    spr = backtest.spread_bps(strategy)

    # Trailing 24h quote volume before entry → slippage tier.
    prior = database.get_klines(sym, "1m", start_ms=snap_ms - 86_400_000,
                                end_ms=snap_ms)
    qv_24h = sum(float(r.get("quote_v") or 0.0) for r in prior)
    slip = backtest.slippage_bps_for(qv_24h, tiers)

    entry_candle = candles[0]
    entry_px = backtest.taker_fill_price(entry_candle["open"], "buy", slip, spr)
    if entry_px <= 0:
        return None
    budget = backtest.fixed_budget(strategy)
    qty = budget / entry_px
    entry_fee = qty * entry_px * fee_model.taker()
    cost = budget + entry_fee
    tp, sl, _bep = backtest.strategy_exit_levels(entry_px, qty, cost,
                                                 strategy, fee_model)

    exit_px, label = None, "timeout"
    for candle in candles[: MAX_HOLD_CANDLES]:
        if sl is not None and candle["open"] <= sl:
            exit_px, label = candle["open"], "sl"
            break
        if sl is not None and candle["low"] <= sl:      # SL first (conservative)
            exit_px, label = sl, "sl"
            break
        if candle["open"] >= tp:
            exit_px, label = candle["open"], "tp"
            break
        if candle["high"] >= tp:
            exit_px, label = tp, "tp"
            break
    if exit_px is None:
        exit_px = candles[: MAX_HOLD_CANDLES][-1]["close"]
    exit_fill = backtest.taker_fill_price(exit_px, "sell", slip, spr)
    exit_fee = qty * exit_fill * fee_model.taker()
    net = qty * (exit_fill - entry_px) - entry_fee - exit_fee
    return {"net_pnl": net, "exit_label": label}


# ── Public API ────────────────────────────────────────────────────────────────

def build_edge_report(days: int = 7, source: str = "live") -> dict:
    """Edge report over the trailing `days`.

    source: trade mode filter — 'live', 'paper', 'testnet' or 'all'.
    Returns {"generated_at", "days", "source",
             "signals": {sig_id: {"fired": {...}, "not_fired": {...}, "lift"}},
             "vetoes": {reason: {"blocked", "replayed", "hypothetical_wins",
                                 "hypothetical_expectancy", "saved_usdt", ...}},
             "joined_trades": int, "unmatched_trades": int}
    """
    now = time.time()
    cutoff = now - float(days) * 86_400.0
    strategy = _load_strategy()

    # Executed trades in window
    all_trades = database.get_recent_trades(limit=TRADE_FETCH_LIMIT)
    trades = []
    for t in all_trades:
        if t.get("exit_price") is None:
            continue
        if source not in (None, "", "all") and t.get("mode") != source:
            continue
        t_ts = _parse_ts(t.get("timestamp_buy"))
        if t_ts is None or t_ts < cutoff:
            continue
        trades.append(t)

    snaps_exec = database.get_entry_snapshots(since_ts=cutoff, executed=True,
                                              limit=SNAPSHOT_FETCH_LIMIT)
    pairs = _joined_trades(trades, snaps_exec)

    # ── Per-signal fired vs not-fired expectancy ─────────────────────────────
    # Memory: reduce each joined pair to (trade, frozenset_of_fired_ids) — the
    # only per-snapshot data the signal loop needs — then DROP the heavy parsed
    # snapshot dicts (snaps_exec, pairs) BEFORE fetching the near-miss batch, so
    # the two big fetches are never fully materialized at the same time.
    signal_ids = set(signal_registry.list_all_signal_ids())
    fired_sets: List[Tuple[dict, frozenset]] = []
    for t, s in pairs:
        f = frozenset(_fired_ids(s))
        signal_ids |= f
        fired_sets.append((t, f))
    n_pairs = len(pairs)
    del snaps_exec, pairs   # release the executed-snapshot batch before near-miss fetch

    signals_report: Dict[str, Any] = {}
    for sig_id in sorted(signal_ids):
        fired_pnls, not_fired_pnls = [], []
        for t, fired in fired_sets:
            pnl = t.get("net_profit")
            if pnl is None:
                continue
            (fired_pnls if sig_id in fired else not_fired_pnls).append(float(pnl))
        f_stats = _group_stats(fired_pnls)
        nf_stats = _group_stats(not_fired_pnls)
        lift = None
        if "expectancy" in f_stats and "expectancy" in nf_stats:
            lift = round(f_stats["expectancy"] - nf_stats["expectancy"], 6)
        signals_report[sig_id] = {"fired": f_stats, "not_fired": nf_stats,
                                  "lift": lift}

    # ── Veto / near-miss saves ───────────────────────────────────────────────
    # Memory: stream the near-miss batch — for each snapshot keep only the veto
    # reason plus the two primitives the replay needs (symbol, ts), never the
    # heavy parsed dict. Track the full blocked count per reason separately, and
    # cap each retained replay-input list at MAX_REPLAYS (we can never replay
    # more than that total, so a larger list would only waste memory). The heavy
    # near_misses batch is released before any replay runs.
    near_misses = database.get_entry_snapshots(since_ts=cutoff, executed=False,
                                               limit=SNAPSHOT_FETCH_LIMIT)
    blocked_counts: Dict[str, int] = {}
    by_reason: Dict[str, List[dict]] = {}
    for s in near_misses:
        reason = _veto_reason(s)
        blocked_counts[reason] = blocked_counts.get(reason, 0) + 1
        lst = by_reason.setdefault(reason, [])
        if len(lst) < MAX_REPLAYS:
            lst.append({"symbol": s.get("symbol"), "ts": s.get("ts")})
    del near_misses   # release the near-miss snapshot batch before replays

    replays_left = MAX_REPLAYS
    vetoes_report: Dict[str, Any] = {}
    for reason in sorted(blocked_counts.keys()):
        snaps = by_reason.get(reason, [])
        results = []
        for s in snaps:
            if replays_left <= 0:
                break
            r = _replay_near_miss(s, strategy)
            replays_left -= 1
            if r is not None:
                results.append(r)
        blocked = blocked_counts[reason]
        if blocked < MIN_SAMPLE:
            vetoes_report[reason] = {"insufficient_data": True, "n": blocked,
                                     "blocked": blocked}
            continue
        pnls = [r["net_pnl"] for r in results]
        vetoes_report[reason] = {
            "blocked": blocked,
            "replayed": len(results),
            "hypothetical_wins": sum(1 for p in pnls if p > 0),
            "hypothetical_expectancy": (round(sum(pnls) / len(pnls), 6)
                                        if pnls else None),
            # positive saved = the veto prevented losses
            "saved_usdt": round(-sum(pnls), 6) if pnls else 0.0,
        }

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "days": days,
        "source": source,
        "signals": signals_report,
        "vetoes": vetoes_report,
        "joined_trades": n_pairs,
        "unmatched_trades": len(trades) - n_pairs,
    }


def run_nightly() -> dict:
    """Build + persist the 7-day edge report. Never raises — safe to call from
    any scheduler (a parallel agent wires the scheduling/exposure)."""
    try:
        report = build_edge_report(days=7, source="live")
    except Exception as e:
        report = {"error": str(e),
                  "generated_at": datetime.now(timezone.utc).isoformat()}
    try:
        database.save_setting("edge_report_json", json.dumps(report, default=str))
        database.save_setting("edge_report_ts",
                              datetime.now(timezone.utc).isoformat())
    except Exception:
        pass
    return report


if __name__ == "__main__":
    print(json.dumps(build_edge_report(), indent=2, default=str))
