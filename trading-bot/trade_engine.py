"""
Trade execution engine — unified real-time architecture.

realtime_monitor:  Called on every WebSocket trade tick (~100 ms).
                   Handles both stop-loss/take-profit exits AND buy entry.
                   Buys read from an in-memory signal cache (no I/O per tick).

signal_scanner:    Async coroutine, runs every SCAN_INTERVAL_SEC (60 s).
                   ws-first mode (default): poll-based buy backup + DB refresh
                   for thin-WS-buffer symbols + health stats — NO REST.
                   legacy mode (strategy data.legacy_rest_scan=true): the old
                   full REST refresh pass (emergency fallback).
                   Does NOT execute trades — buy execution is in realtime_monitor.

update_coin_signals: Called by data_collector on every 1-minute kline close
                   (WebSocket-driven, runs on a worker thread). C §6.2: rebuilds
                   the FULL signal-cache entry from the ws_candles buffers —
                   real-ATR signals, bb_ok, 5m_ok, low_24h, klines_1m,
                   stoch_rsi_val — merge-preserving keys set by other paths.
"""

import asyncio
import json
import json as _json
import logging
import os
import time
import math
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import config
import database
import fees
import indicators
import learning
import connection
from connection import get_mode
import binance_direct

# WolfBot v0.5 Part S — EV buy-scoring engine. Guarded import: every ev_model
# call in the buy path is individually try/guarded so a missing or broken module
# degrades gracefully to the pre-EV behavior (highest raw score, first-ready).
try:
    import ev_model
except Exception:  # pragma: no cover
    ev_model = None  # type: ignore

# Phase 2 §2.4/§2.5 — exchange-side exit-order managers (maker TP / OCO).
# Guarded import: the engine must keep trading on pure local monitoring if
# the module is missing or fails to import.
try:
    import exit_orders
except Exception:  # pragma: no cover
    exit_orders = None

try:
    import thread_health as _thread_health
except Exception:
    _thread_health = None

log = logging.getLogger(__name__)


# ── M2 — crash instrumentation: faulthandler ─────────────────────────────────
# Always-on, guarded. Dumps a C-level traceback to <data dir>/faulthandler.log
# on a fatal signal (SIGSEGV/SIGFPE/SIGABRT/SIGBUS/SIGILL) so the NEXT hard
# crash / OOM-kill is diagnosable (two unclean restarts in ~24h prompted this).
# Keep the file handle alive at module scope so it is never garbage-collected
# out from under the C signal handler. Reuses database's resolved data dir.
_faulthandler_fh = None
_faulthandler_wd_fh = None   # N2 — watchdog dump file handle (kept alive at module scope)
try:
    import faulthandler as _faulthandler
    try:
        _fh_dir = os.path.dirname(database.DB_PATH) or "."
        _faulthandler_fh = open(os.path.join(_fh_dir, "faulthandler.log"), "a")
        _faulthandler.enable(file=_faulthandler_fh, all_threads=True)
    except Exception:
        # Fall back to stderr if the data dir isn't writable — a fatal-signal
        # traceback anywhere beats none.
        try:
            _faulthandler.enable(all_threads=True)
        except Exception:
            pass
    # N2 — HANG/wedge watchdog: dump ALL thread stacks periodically so a wedge
    # under concurrent heavy REST (which never raises a fatal signal, so
    # faulthandler.enable() alone would capture nothing) still leaves a stack
    # trace. repeat=True keeps re-arming; the timer thread is a daemon. Reuses
    # the same data-dir resolution as faulthandler.log.
    try:
        _fh_dir = os.path.dirname(database.DB_PATH) or "."
        _faulthandler_wd_fh = open(os.path.join(_fh_dir, "faulthandler_watchdog.log"), "a")
        _faulthandler.dump_traceback_later(300, repeat=True, file=_faulthandler_wd_fh)
    except Exception:
        pass
except Exception:
    pass


# ── N2 — uncaught-exception hooks (threads + main) ────────────────────────────
# A background thread dying (or an uncaught main-thread exception) can be silent
# today: the process just goes and systemd restarts it, leaving no trace. Route
# BOTH threading.excepthook and sys.excepthook through a logger that records the
# full traceback via log_activity(..., "error") AND appends it to a crash file,
# chaining any previously-installed hook so nothing else's handler is lost.
import sys as _sys

_crash_log_fh = None
try:
    _crash_dir = os.path.dirname(database.DB_PATH) or "."
    _crash_log_fh = open(os.path.join(_crash_dir, "uncaught_crash.log"), "a")
except Exception:
    _crash_log_fh = None

_prev_sys_excepthook = getattr(_sys, "excepthook", None)
try:
    _prev_threading_excepthook = threading.excepthook
except Exception:
    _prev_threading_excepthook = None


def _write_crash_file(header: str, tb_text: str) -> None:
    """Append a traceback to the crash file. Never raises."""
    try:
        if _crash_log_fh is not None:
            _crash_log_fh.write(
                f"\n===== {header} @ {datetime.now(timezone.utc).isoformat()} =====\n")
            _crash_log_fh.write(tb_text)
            _crash_log_fh.write("\n")
            _crash_log_fh.flush()
    except Exception:
        pass


def _log_uncaught(header: str, exc_type, exc_value, exc_tb) -> None:
    """Log an uncaught exception to the activity log (error) + crash file."""
    try:
        import traceback as _tb
        tb_text = "".join(_tb.format_exception(exc_type, exc_value, exc_tb))
    except Exception:
        tb_text = f"{exc_type}: {exc_value}"
    try:
        database.log_activity(
            f"{header}: {getattr(exc_type, '__name__', exc_type)}: {exc_value}", "error")
    except Exception:
        pass
    _write_crash_file(header, tb_text)


def _trade_engine_sys_excepthook(exc_type, exc_value, exc_tb):
    _log_uncaught("UNCAUGHT EXCEPTION (main thread)", exc_type, exc_value, exc_tb)
    try:
        if callable(_prev_sys_excepthook):
            _prev_sys_excepthook(exc_type, exc_value, exc_tb)
    except Exception:
        pass


def _trade_engine_threading_excepthook(args):
    thr = getattr(args, "thread", None)
    name = getattr(thr, "name", "?") if thr is not None else "?"
    _log_uncaught(
        f"UNCAUGHT EXCEPTION (thread {name})",
        args.exc_type, args.exc_value, args.exc_traceback)
    try:
        if callable(_prev_threading_excepthook):
            _prev_threading_excepthook(args)
    except Exception:
        pass


try:
    _sys.excepthook = _trade_engine_sys_excepthook
except Exception:
    pass
try:
    threading.excepthook = _trade_engine_threading_excepthook
except Exception:
    pass


# ── C §6.4 — Binance rate-limit accounting (binance_limits module) ────────────
# Wired DEFENSIVELY: the module may land after this file (parallel delivery),
# so import lazily and degrade to no-ops when unavailable. can_spend defaults
# to True (never block trading because accounting is missing).
_blimits = None
_blimits_import_failed = False


def _get_blimits():
    global _blimits, _blimits_import_failed
    if _blimits is None and not _blimits_import_failed:
        try:
            import binance_limits as _bl
            _blimits = _bl
        except Exception:
            _blimits_import_failed = True
    return _blimits


def _limits_can_spend(weight: int, critical: bool) -> bool:
    """Gate a REST call through binance_limits right before issuing it.
    Prefers spend() (gate + charge the background budget, per that module's
    contract) and falls back to can_spend(); allows on any failure."""
    bl = _get_blimits()
    fn = (getattr(bl, "spend", None) or getattr(bl, "can_spend", None)) if bl else None
    if fn is None:
        return True
    try:
        return bool(fn(weight, critical=critical))
    except TypeError:
        try:
            return bool(fn(weight, critical))
        except Exception:
            return True
    except Exception:
        return True


def _limits_record_headers(headers: dict) -> None:
    bl = _get_blimits()
    fn = getattr(bl, "record_response_headers", None) if bl else None
    if fn is None or not headers:
        return
    try:
        fn(headers)
    except Exception:
        pass


def _retry_after_sec(headers: Optional[dict], default: float) -> float:
    try:
        for k, v in (headers or {}).items():
            if str(k).lower() == "retry-after":
                return max(1.0, float(v))
    except Exception:
        pass
    return default


def _limits_on_429(headers: Optional[dict] = None) -> None:
    """Forward a 429 to binance_limits.on_429(retry_after_sec)."""
    bl = _get_blimits()
    fn = getattr(bl, "on_429", None) if bl else None
    if fn is None:
        return
    ra = _retry_after_sec(headers, 0.0)
    try:
        fn(ra if ra > 0 else None)
    except TypeError:
        try:
            fn()
        except Exception:
            pass
    except Exception:
        pass


def _limits_on_418(headers: Optional[dict] = None) -> None:
    """Forward a 418 to binance_limits.on_418(retry_after_sec)."""
    bl = _get_blimits()
    fn = getattr(bl, "on_418", None) if bl else None
    if fn is None:
        return
    ra = _retry_after_sec(headers, 0.0)
    try:
        fn(ra if ra > 0 else None)
    except TypeError:
        try:
            fn()
        except Exception:
            pass
    except Exception:
        pass


# ── Client transport helpers ─────────────────────────────────────────────────
# Always resolve the client through the connection module at call time so the
# auto-reconnect loop's rebinding (connection.client = new_client) takes effect
# here — a `from connection import client` snapshot would hold the stale
# PaperClient forever after a live reconnect.
#
# In LIVE mode all signed calls (account reads, order placement) go through
# binance_direct (urllib + HMAC): python-binance uses the `requests` library,
# which is geo-blocked on datacenter IPs (HTTP 451 / APIError code=0).
# Paper and testnet modes keep using the connection client unchanged.

def _client():
    """Current client — never cache the returned object across reconnects."""
    return connection.client


def _acct() -> dict:
    """Account snapshot. Live mode uses the geo-block-safe direct transport."""
    if get_mode() == "live":
        return binance_direct.get_account()
    return connection.client.get_account()


def _market_buy(symbol: str, quote_order_qty: float) -> dict:
    """Market buy via the mode-appropriate transport.

    Live mode: geo-block-safe signed call. If live mode is running on the
    paper fallback client (Binance connection failed at boot), buys are
    BLOCKED entirely — never silently 'trade' the paper client while the
    records would be stamped mode='live'.
    """
    if get_mode() == "live":
        if connection.is_using_paper_fallback():
            raise RuntimeError(
                "live mode is in paper fallback (Binance client unavailable) — buy blocked"
            )
        return binance_direct.order_market_buy(symbol, quote_order_qty)
    return connection.client.order_market_buy(symbol=symbol, quoteOrderQty=quote_order_qty)


def _market_sell(symbol: str, quantity: float) -> dict:
    """Live-mode market sell via the geo-block-safe direct transport."""
    if get_mode() == "live":
        return binance_direct.order_market_sell(symbol, quantity)
    return connection.client.order_market_sell(symbol=symbol, quantity=quantity)


def _log_order_intent(action: str, symbol: str, qty: float, intended_price: float):
    try:
        database.log_activity(
            f"[ORDER_SEND] {action} {symbol} qty={qty:.6f} "
            f"intended_price={intended_price:.6f} "
            f"intended_notional=${qty * intended_price:.4f}",
            "info"
        )
    except Exception:
        pass


def _log_order_result(action: str, symbol: str, intended_qty: float, intended_price: float, result: dict):
    try:
        fills = result.get("fills", [])
        filled_qty = float(result.get("executedQty") or sum(float(f.get("qty", 0)) for f in fills))
        actual_quote = float(result.get("cummulativeQuoteQty") or 0)
        actual_avg = actual_quote / filled_qty if filled_qty > 0 else 0
        slippage = ((actual_avg - intended_price) / intended_price * 100) if intended_price > 0 else 0
        database.log_activity(
            f"[ORDER_REPLY] {action} {symbol} status={result.get('status')} "
            f"filled={filled_qty:.6f} (intended {intended_qty:.6f}) "
            f"avg_price={actual_avg:.6f} (intended {intended_price:.6f}) "
            f"slippage={slippage:+.3f}% quote=${actual_quote:.4f}",
            "info"
        )
    except Exception:
        pass


# ── §3.5 Maker-first entries (live mode) ─────────────────────────────────────

def _parse_entry_order(sym: str, order: dict, fallback_price: float,
                       is_maker: bool) -> dict:
    """Normalize a BUY order payload into position-entry numbers.

    Mirrors the market-buy fill parsing exactly: VWAP entry price
    (cummulativeQuoteQty / executedQty — fills[0] alone understates multi-fill
    cost), commission-adjusted qty, and the USDT fee across all fills.

    LIMIT_MAKER caveat: GET /api/v3/order and DELETE /api/v3/order payloads
    carry executedQty/cummulativeQuoteQty but NO fills[] array. Without fills
    the commission is estimated from the FeeModel: fee = quote_spent × maker
    fraction, and (unless bnb_discount is on — then the fee comes out of the
    BNB balance) the base-asset commission is deducted from qty so the
    recorded position matches the wallet credit (prevents -2010 on the sell).
    """
    fills = order.get("fills") or []
    exec_qty  = float(order.get("executedQty", 0) or 0)
    cum_quote = float(order.get("cummulativeQuoteQty", 0) or 0)
    if exec_qty > 0 and cum_quote > 0:
        fill_price = cum_quote / exec_qty
    elif fills:
        fill_price = float(fills[0].get("price", fallback_price))
    else:
        fill_price = fallback_price
    try:
        fm = fees.get_fee_model(sym)
        fee_frac = fm.maker() if is_maker else fm.taker()
        bnb_fee = bool(fm.bnb_discount)
    except Exception:
        fee_frac = _fee_rate_for(sym)
        bnb_fee = False
    spent = cum_quote if cum_quote > 0 else exec_qty * fill_price
    if fills:
        # Same math as the market path: subtract base-asset commission from
        # executedQty; convert all commissions to USDT (BNB via live price).
        base_asset = sym[:-4] if sym.endswith("USDT") else sym
        base_comm = sum(
            float(f.get("commission") or 0) for f in fills
            if f.get("commissionAsset") == base_asset
        )
        qty = max(0.0, exec_qty - base_comm)
        fee_usdt, _ = _fills_fee_usdt(fills, spent * fee_frac)
    else:
        fee_usdt = spent * fee_frac
        qty = exec_qty if bnb_fee else max(0.0, exec_qty * (1.0 - fee_frac))
    return {
        "fill_price":     fill_price,
        "qty":            qty,
        "buy_fee_usdt":   fee_usdt,
        "spent_quote":    spent,
        "entry_is_maker": is_maker,
    }


def _maker_best_bid(sym: str, max_age_sec: float = 5.0):
    """Fresh best bid from the @bookTicker feed; None when absent/stale."""
    try:
        import data_collector as _dc
        bt = _dc.book_ticker.get(sym)
        if not bt:
            return None
        if (time.time() - float(bt.get("ts", 0) or 0)) > max_age_sec:
            return None
        bid = float(bt.get("bid") or 0)
        return bid if bid > 0 else None
    except Exception:
        return None


# ── G1a — on-demand best-bid for maker-first entries ─────────────────────────
# data_collector subscribes @bookTicker for HELD symbols only. With 0 open
# positions there is NO bookTicker stream, so _maker_best_bid returns None and
# every maker entry aborted ("best bid unavailable/stale"). Fix: when the WS
# book is missing/stale, fetch a fresh quote via the PUBLIC REST bookTicker
# (weight 2, geo-block-safe host) and cache it briefly. Preference order:
#   fresh WS bookTicker (<5s)  →  2s on-demand cache  →  on-demand REST.
# When entries.bookticker_universe is on, the data agent streams @bookTicker for
# the whole universe, so we TRUST the WS book and skip the REST fetch entirely.
_bookticker_cache: Dict[str, tuple] = {}   # sym -> (bid, ask, ts)
_BOOKTICKER_CACHE_TTL = 2.0                 # seconds


def _fresh_maker_bid(sym: str):
    """Best bid for maker pricing: fresh WS book → 2s on-demand cache →
    on-demand PUBLIC REST bookTicker (gated). Returns a positive float bid, or
    None only when every source fails."""
    # 1. Fresh WS bookTicker (held-symbol stream, <5s).
    bid = _maker_best_bid(sym)
    if bid is not None:
        return bid
    # 2. Recent on-demand cache (2s TTL) — avoids re-fetching inside a chase.
    ce = _bookticker_cache.get(sym)
    if ce and (time.time() - ce[2]) <= _BOOKTICKER_CACHE_TTL:
        return ce[0] if ce[0] > 0 else None
    # 3. Universe streaming on → trust the WS book, do not spend REST weight.
    try:
        if _entries_cfg().get("bookticker_universe"):
            return None
    except Exception:
        pass
    # 4. On-demand REST fetch (rare/staggered on a 5m strategy; ~2 weight).
    #    Gate through binance_limits.can_spend(2, critical=True) so it defers
    #    under a 429/418 rather than piling on.
    try:
        bl = _get_blimits()
        if bl is not None and not bl.can_spend(2, critical=True):
            return None
    except Exception:
        pass
    try:
        bt = binance_direct.get_book_ticker(sym)
        bid = float(bt.get("bidPrice") or 0)
        ask = float(bt.get("askPrice") or 0)
        _bookticker_cache[sym] = (bid, ask, time.time())
        return bid if bid > 0 else None
    except Exception as _bt_e:
        database.log_activity(
            f"{sym}: on-demand bookTicker fetch failed ({_bt_e}) — "
            f"no best bid for maker entry", "warn")
        return None


# ── L (v0.4) — live bid/ask + spread for the taker fallback and friction gate ─
def _fresh_book(sym: str):
    """(bid, ask) from the freshest source used for maker pricing — fresh WS
    bookTicker (<5s) → 2s on-demand cache → on-demand PUBLIC REST bookTicker
    (gated). (None, None) when every source fails. Mirrors _fresh_maker_bid's
    ladder so the spread seen here matches the book the maker post priced off."""
    # 1. Fresh WS bookTicker (held-symbol stream, <5s) — carries bid + ask.
    try:
        import data_collector as _dc_bk
        bt = _dc_bk.book_ticker.get(sym)
        if bt and (time.time() - float(bt.get("ts", 0) or 0)) <= 5.0:
            b = float(bt.get("bid") or 0)
            a = float(bt.get("ask") or 0)
            if b > 0 and a > 0:
                return b, a
    except Exception:
        pass
    # 2. Recent on-demand cache (2s TTL) — (bid, ask, ts).
    ce = _bookticker_cache.get(sym)
    if ce and (time.time() - ce[2]) <= _BOOKTICKER_CACHE_TTL:
        if ce[0] > 0 and ce[1] > 0:
            return ce[0], ce[1]
    # 3. Universe streaming on → trust the WS book, do not spend REST weight.
    try:
        if _entries_cfg().get("bookticker_universe"):
            return None, None
    except Exception:
        pass
    # 4. On-demand REST fetch, gated through binance_limits.can_spend.
    try:
        bl = _get_blimits()
        if bl is not None and not bl.can_spend(2, critical=True):
            return None, None
    except Exception:
        pass
    try:
        bt = binance_direct.get_book_ticker(sym)
        b = float(bt.get("bidPrice") or 0)
        a = float(bt.get("askPrice") or 0)
        _bookticker_cache[sym] = (b, a, time.time())
        if b > 0 and a > 0:
            return b, a
    except Exception:
        pass
    return None, None


def _spread_pcts(sym: str):
    """(full_spread_pct, half_spread_pct) over mid from the freshest book, or
    (None, None) when unavailable. full = (ask-bid)/mid×100; half ≈ the taker
    cost of crossing a marketable order over mid."""
    bid, ask = _fresh_book(sym)
    if not bid or not ask or bid <= 0 or ask <= 0 or ask < bid:
        return None, None
    mid = (bid + ask) / 2.0
    if mid <= 0:
        return None, None
    full = (ask - bid) / mid * 100.0
    return full, full / 2.0


# ── L2.2 — 24h quote-volume liquidity source ─────────────────────────────────
_qv24h_cache: Dict[str, tuple] = {}   # sym -> (qv_usd or None, ts)
_QV24H_CACHE_TTL = 300.0              # 5 min — 24h volume moves slowly
# L2.2 — thin liquidity is a stable property (not transient), so a failed
# liquidity check parks the symbol for a longer cooldown than the 60s candidacy
# cooldown used for transient re-check failures.
_LIQUIDITY_COOLDOWN_SEC = 1800.0     # 30 min


def _quote_volume_24h_usd(symbol: str):
    """24h quote volume in USDT for `symbol`, summed from the durable 1m kline
    store (quote_v column) over the last 24h; falls back to Σ close×base_volume
    when quote_v is absent (WS-persisted klines store base volume only). Cached
    5 min. Returns None when no kline data exists at all (caller fails open —
    a missing liquidity stat must never block a real candidate). No REST weight."""
    now = time.time()
    ce = _qv24h_cache.get(symbol)
    if ce and (now - ce[1]) < _QV24H_CACHE_TTL:
        return ce[0]
    qv = None
    try:
        start_ms = int((now - 86400.0) * 1000)
        rows = database.get_klines(symbol, config.CANDLE_TIMEFRAME, start_ms=start_ms)
        vals = [float(r["quote_v"]) for r in (rows or [])
                if r.get("quote_v") is not None]
        if vals:
            qv = sum(vals)
        elif rows:
            approx = [float(r["c"]) * float(r["v"]) for r in rows
                      if r.get("c") is not None and r.get("v") is not None]
            if approx:
                qv = sum(approx)
    except Exception:
        qv = None
    _qv24h_cache[symbol] = (qv, now)
    return qv


def _planned_sl_distance_pct(symbol: str, price: float):
    """The 1R stop distance (%) the exit geometry WOULD assign to an entry at
    `price` right now — same clamp/legacy rules as _apply_entry_exit_geometry
    (which only runs post-fill). Used by the L2.1 friction gate to size friction
    against the risk budget BEFORE ordering. None when the stop is disabled
    (legacy mapping) → the gate then fails open."""
    try:
        cfg = _exit_cfg()
    except Exception:
        return None
    if price <= 0:
        return None
    if cfg["legacy_mode"]:
        return cfg["sl_min_pct"] if cfg["sl_enabled"] else None
    atr_pct, _ = _atr_pct_5m_at_entry(symbol, price)
    if atr_pct is None:
        return cfg["sl_min_pct"]   # conservative tight stop (matches geometry)
    return min(max(cfg["k_sl"] * atr_pct, cfg["sl_min_pct"]), cfg["sl_max_pct"])


# ── G1a — maker-entry abandon churn control ──────────────────────────────────
# A symbol whose maker entry abandons repeatedly (no book, taker disabled, etc.)
# would otherwise re-select every scan and log a warning every ~10s. After
# entries.maker_abandon_max consecutive abandonments it gets a 5-min candidacy
# cooldown (reusing the F6 _candidacy_cooldown gate checked at selection time)
# and ONE deduped log line. The counter resets on a successful entry.
_maker_abandon_counts: Dict[str, int] = {}
_MAKER_ABANDON_COOLDOWN_SEC = 300.0


def _note_maker_abandon(sym: str) -> None:
    """Increment the per-symbol maker-abandon counter; on reaching
    entries.maker_abandon_max, arm a 5-min candidacy cooldown + one deduped log
    and reset the counter."""
    try:
        limit = max(1, int(_entries_cfg().get("maker_abandon_max", 3)))
    except Exception:
        limit = 3
    n = _maker_abandon_counts.get(sym, 0) + 1
    _maker_abandon_counts[sym] = n
    if n >= limit:
        _candidacy_cooldown[sym] = time.time() + _MAKER_ABANDON_COOLDOWN_SEC
        _maker_abandon_counts[sym] = 0
        _log_skip_dedup(
            sym, "maker_abandon_cooldown",
            f"[SKIP] {sym}: maker entry abandoned {n}× in a row — "
            f"candidacy cooldown for {_MAKER_ABANDON_COOLDOWN_SEC/60:.0f} min "
            f"(no best bid / chase exhausted). Will retry after cooldown.",
            "warn")


def _reset_maker_abandon(sym: str) -> None:
    """Clear the maker-abandon counter after a successful entry."""
    _maker_abandon_counts.pop(sym, None)


# ── G1b — lot-step rounding-waste diagnostics ────────────────────────────────
# Rounding DOWN to the lot step is normal (an $10.86 fill vs an $11.00 budget is
# fine). The buy path only SKIPS when the rounded notional falls below the
# exchange minNotional, or when the waste exceeds entries.max_lot_waste_pct
# (chronically-oversized ticket for this coin). Symbols hitting that ceiling are
# surfaced here so the API/UI can badge "ticket too small for this coin".
_lot_waste_flags: Dict[str, dict] = {}   # sym -> {"waste_pct": float, "ts": float}


def get_lot_waste_flags() -> Dict[str, dict]:
    """Symbols whose last buy attempt was SKIPPED for lot-step rounding waste
    above entries.max_lot_waste_pct: {sym: {"waste_pct": float, "ts": float}}.
    Consumed by the diagnostics API/UI. A copy so callers can't mutate state."""
    return {s: dict(v) for s, v in _lot_waste_flags.items()}


def _log_high_order_count(sym: str, context: str) -> None:
    """§3.5e — each maker post+cancel spends per-account order-count budget.
    Log the rolling 10s order count (from response-header telemetry) when it
    climbs past 20 so an aggressive chase is visible before Binance 429s."""
    try:
        bl = _get_blimits()
        if bl is None:
            return
        oc = int(bl.get_limits_health().get("order_count_10s", 0) or 0)
        if oc > 20:
            database.log_activity(
                f"{sym}: {context} — rolling 10s order count is {oc} "
                f"(maker chase posts+cancels count toward account order limits)",
                "warn")
    except Exception:
        pass


def _execute_maker_first_buy(sym: str, budget: float,
                             signal_still_holds=None):
    """§3.5 — maker-first live entry: LIMIT_MAKER at best bid, chase, fallback.

    Lifecycle:
      * best bid from data_collector.book_ticker (fresh <5 s). Unavailable →
        ONE taker market buy iff entries.taker_fallback, else abandon.
      * post binance_direct.order_limit_maker(BUY, floor_to_lot(budget/bid),
        tick-floored bid). A -2010 'would immediately match' reject → re-read
        the bid and repost (counts as a repost).
      * poll get_order every ~0.5 s for entries.chase_seconds. FILLED → done.
        PARTIALLY_FILLED at timeout → cancel remainder, keep the filled part.
        Unfilled → cancel + repost, up to entries.max_reposts reposts after
        the initial post.
      * chase exhausted → one taker market buy (L1.1) iff `signal_still_holds()`
        (quick re-check of the cached engine decision) AND EITHER legacy
        entries.taker_fallback=true (always cross) OR the live full spread% is
        <= entries.taker_fallback_max_spread_pct (spread-guarded, default 0.05).
        Otherwise abandon (return None — the CALLER releases the _buying claim
        and records the 'maker_chase_abandoned' rejection).

    Paper mode never reaches this function — the existing simulated market-buy
    path is unchanged (paper maker simulation is out of scope).

    Returns the _parse_entry_order dict (fill_price / qty / buy_fee_usdt /
    spent_quote / entry_is_maker) or None on abandonment.
    """
    chase_sec  = max(0.5, float(_entries_cfg()["chase_seconds"]))
    max_reposts = max(0, int(_entries_cfg()["max_reposts"]))

    # L1.2 — count order_posted exactly once per entry attempt (a chase may
    # repost several times; the taker fallback may follow a maker post). The
    # flag ensures the funnel stage is incremented once, not per order.
    _posted = {"done": False}

    def _mark_posted():
        if not _posted["done"]:
            _posted["done"] = True
            _funnel_incr("order_posted")

    def _taker_fallback_buy(why: str):
        # L1.1 — spread-conditional taker fallback. Config is read at time of
        # use. Two ways to cross instead of abandoning:
        #   (a) legacy entries.taker_fallback=true  → always cross (old behavior)
        #   (b) new spread-guarded path (default ON via
        #       entries.taker_fallback_max_spread_pct=0.05): cross ONLY when the
        #       live full spread% is <= the ceiling; else abandon.
        _cfg_fb = _entries_cfg()
        legacy_fb  = bool(_cfg_fb["taker_fallback"])
        max_spread = float(_cfg_fb["taker_fallback_max_spread_pct"])
        if legacy_fb:
            fb_msg = f"{sym}: maker chase → taker fallback market buy ({why})"
        elif max_spread > 0:
            full_sp, half_sp = _spread_pcts(sym)
            if full_sp is None:
                database.log_activity(
                    f"{sym}: maker-first entry abandoned — {why}; live spread "
                    f"unavailable, spread-guarded taker fallback cannot verify "
                    f"cost — abandoning", "warn")
                return None
            if full_sp > max_spread:
                database.log_activity(
                    f"{sym}: maker-first entry abandoned — {why}; spread "
                    f"{full_sp:.2f}% > {max_spread:.2f}% taker-fallback ceiling "
                    f"(too wide to cross)", "warn")
                return None
            fb_msg = (f"{sym}: maker chase exhausted, spread {full_sp:.2f}% "
                      f"<= {max_spread:.2f}% — taker fallback filled "
                      f"(cost ~{half_sp:.3f}% half-spread)")
        else:
            database.log_activity(
                f"{sym}: maker-first entry abandoned — {why} "
                f"(entries.taker_fallback=false, "
                f"taker_fallback_max_spread_pct=0)", "warn")
            return None
        # Existing safety gate — only cross while the cached decision still holds.
        if signal_still_holds is not None:
            try:
                if not signal_still_holds():
                    database.log_activity(
                        f"{sym}: maker-first entry abandoned — {why}, and the "
                        f"signal no longer holds (taker fallback skipped)", "warn")
                    return None
            except Exception:
                pass
        database.log_activity(fb_msg, "info")
        result = _market_buy(sym, budget)
        _log_order_result("BUY", sym, 0.0, 0.0, result)
        _mark_posted()
        return _parse_entry_order(sym, result, 0.0, is_maker=False)

    reposts = 0            # reposts AFTER the initial post (-2010 counts too)
    while True:
        # G1a: fresh WS book → 2s cache → on-demand REST bookTicker. Only None
        # when the on-demand fetch ALSO fails.
        bid = _fresh_maker_bid(sym)
        if bid is None:
            return _taker_fallback_buy(
                "best bid unavailable — WS book absent/stale and on-demand "
                "REST bookTicker failed")
        price = _floor_price_tick(bid, sym)
        qty = _floor_qty(budget / bid, sym)
        if qty <= 0 or price <= 0:
            database.log_activity(
                f"{sym}: maker-first entry abandoned — qty floors to 0 "
                f"(budget=${budget:.2f} bid=${bid:.6f})", "warn")
            return None

        try:
            _log_order_intent("BUY_MAKER", sym, qty, price)
            order = binance_direct.order_limit_maker(sym, "BUY", qty, price)
        except binance_direct.BinanceDirectError as e:
            if e.code == -2010:
                # Would immediately match and take — bid moved through our
                # price between the book-ticker read and the post. Re-read
                # and repost; this consumes a repost.
                reposts += 1
                _log_high_order_count(sym, "maker post rejected -2010")
                if reposts > max_reposts:
                    return _taker_fallback_buy(
                        f"chase exhausted ({reposts - 1} reposts + -2010 reject)")
                continue
            raise
        _log_high_order_count(sym, "maker order posted")
        _mark_posted()   # L1.2 — first maker post placed (once per attempt)

        order_id = int(order.get("orderId", 0) or 0)
        status = order.get("status", "NEW")
        deadline = time.time() + chase_sec
        while status not in ("FILLED", "CANCELED", "EXPIRED", "REJECTED") \
                and time.time() < deadline:
            time.sleep(0.5)
            try:
                order = binance_direct.get_order(sym, order_id)
                status = order.get("status", status)
            except Exception as _poll_e:
                database.log_activity(
                    f"{sym}: maker chase poll error ({_poll_e}) — retrying", "warn")

        if status == "FILLED":
            _log_order_result("BUY_MAKER", sym, qty, price, order)
            return _parse_entry_order(sym, order, price, is_maker=True)

        if status in ("CANCELED", "EXPIRED", "REJECTED"):
            # Cancelled/expired externally — treat like an unfilled timeout.
            filled = float(order.get("executedQty", 0) or 0)
            if filled > 0:
                _log_order_result("BUY_MAKER", sym, qty, price, order)
                return _parse_entry_order(sym, order, price, is_maker=True)
        else:
            # Timeout with the order still resting — cancel the remainder.
            try:
                cancelled = binance_direct.cancel_order(sym, order_id)
                if isinstance(cancelled, dict) and cancelled.get("executedQty") is not None:
                    order = cancelled
            except binance_direct.BinanceDirectError as e:
                if e.code == -2011:
                    # Raced a fill: the order completed before the cancel.
                    try:
                        order = binance_direct.get_order(sym, order_id)
                    except Exception:
                        pass
                else:
                    database.log_activity(
                        f"{sym}: maker chase cancel failed ({e}) — abandoning "
                        f"(order {order_id} may still rest)", "error")
                    return None
            _log_high_order_count(sym, "maker order cancelled")
            filled = float(order.get("executedQty", 0) or 0)
            if filled > 0:
                # PARTIALLY_FILLED at timeout: keep the filled part as the
                # position; qty/fees recomputed from the actual fill numbers.
                _log_order_result("BUY_MAKER", sym, qty, price, order)
                database.log_activity(
                    f"{sym}: maker chase partial fill kept — "
                    f"{filled:.8f}/{qty:.8f} filled, remainder cancelled", "info")
                return _parse_entry_order(sym, order, price, is_maker=True)

        reposts += 1
        if reposts > max_reposts:
            return _taker_fallback_buy(f"chase exhausted ({max_reposts} reposts)")


# ── §3.6 zero-maker-fee promo-pair advisory ──────────────────────────────────
# The watchlist owns symbol choice: auto-switching BTCUSDT→BTCFDUSD would
# change the traded universe (quote balance, data feeds, exit orders), so this
# stays advisory-only — an INFO log at buy-decision time (throttled 30 min per
# symbol) plus a promo_pair_available flag in /api/signals-summary entries.
_promo_advice_ts: Dict[str, float] = {}
_PROMO_ADVICE_THROTTLE_SEC = 1800.0


def promo_pair_available(sym: str) -> bool:
    """True when entries.prefer_fee_promo_pairs is on, `sym` quotes in USDT,
    and a matching <BASE>FDUSD entry with maker_pct==0 is configured in
    strategy fees.per_symbol_overrides (e.g. BTCUSDT → BTCFDUSD)."""
    try:
        if not _entries_cfg()["prefer_fee_promo_pairs"]:
            return False
        sym_u = str(sym).strip().upper()
        if not sym_u.endswith("USDT"):
            return False
        target = sym_u[:-4] + "FDUSD"
        fees_blk = _load_strategy().get("fees")
        overrides = fees_blk.get("per_symbol_overrides") if isinstance(fees_blk, dict) else None
        if not isinstance(overrides, dict):
            return False
        for k, v in overrides.items():
            if str(k).strip().upper() == target and isinstance(v, dict):
                try:
                    if float(v.get("maker_pct")) == 0.0:
                        return True
                except (TypeError, ValueError):
                    pass
        return False
    except Exception:
        return False


def _maybe_log_promo_pair(sym: str) -> None:
    """§3.6 advisory log (throttled) — suggests the zero-maker-fee pair but
    never switches symbols."""
    if not promo_pair_available(sym):
        return
    now = time.time()
    if now - _promo_advice_ts.get(sym, 0.0) < _PROMO_ADVICE_THROTTLE_SEC:
        return
    _promo_advice_ts[sym] = now
    try:
        database.log_activity(
            f"{sym}: zero-maker-fee promo pair {sym[:-4].upper()}FDUSD is "
            f"configured (fees.per_symbol_overrides maker_pct=0) — consider "
            f"trading it instead for free maker entries. Advisory only: the "
            f"watchlist owns symbol choice, no auto-switch.", "info")
    except Exception:
        pass


# Phase 1: Signal registry — shadow mode only (use_new_signal_engine=False by default).
# Falls back gracefully if the file is absent so a partial deploy can't break the bot.
try:
    from signal_registry import evaluate_buy_decision as _sr_evaluate_buy_decision
    from signal_registry import SIGNAL_REGISTRY, list_all_signal_ids
    SIGNAL_REGISTRY_AVAILABLE = True
except Exception as _sr_import_err:
    SIGNAL_REGISTRY_AVAILABLE = False

_positions: List[dict] = []
_positions_lock = threading.Lock()

_strategy_mtime: float = 0.0
_strategy_cache: dict = {}

# Legacy module default — kept ONLY for external legacy reads (e.g. control_api
# imports _te._fee_rate). Refreshed from the FeeModel in _refresh_risk_params.
# All fee math INSIDE this module goes through _fee_rate_for(symbol).
_fee_rate = config.FEE_RATE


def _fee_rate_for(symbol: str = "") -> float:
    """Taker fee FRACTION (0.001 == 0.10%) for `symbol` from fees.FeeModel.

    Single source of truth (spec §1.1): strategy.json fees.* (mtime-cached,
    hot-reload, per-symbol overrides). Falls back to config.FEE_RATE only when
    the fees module itself fails. BEP↔gate PARITY: compute_real_breakeven_price
    and _profitable_sell_check MUST both read this same function."""
    try:
        return fees.get_fee_model(symbol or None).taker()
    except Exception:
        return config.FEE_RATE
# Minimum multiplier needed to cover fees (breakeven floor).

def _fills_fee_usdt(fills: list, fallback_usdt: float) -> tuple:
    """
    Sum commissions across ALL fills and convert to USDT.

    Binance splits a single market order into multiple fills at different price
    levels. Each fill carries its own commission amount and asset.

    commissionAsset is usually:
      "BNB"  — BNB-fee-discount mode (fee deducted from BNB balance, NOT USDT)
      "USDT" — standard mode for sell orders (deducted from quote proceeds)
      <coin> — rare; base coin deducted from quantity received

    Returns (fee_in_usdt, commission_asset_of_first_fill).
    Falls back to (fallback_usdt, "estimated") when conversion is impossible.
    """
    if not fills:
        return fallback_usdt, "estimated"
    try:
        from data_collector import prices as _px
    except Exception:
        return fallback_usdt, "estimated"

    total = 0.0
    first_asset = fills[0].get("commissionAsset", "estimated")
    for f in fills:
        amount = float(f.get("commission") or 0)
        asset  = f.get("commissionAsset", "")
        if asset in ("USDT", "BUSD", "USDC"):
            total += amount
        elif asset:
            px = _px.get(asset + "USDT", 0)
            if px:
                total += amount * px
            else:
                # Price unavailable — fall back to estimate for entire order
                return fallback_usdt, "estimated"
    return total, first_asset
# ~1.002003 at default fees — exact cost of a taker round-trip. Refreshed from
# the FeeModel in _refresh_risk_params so strategy.json fee edits hot-reload.
_FEE_FLOOR = 1.0 / ((1.0 - config.FEE_RATE) ** 2)

# Per-coin override buffers: {"SHIBUSDT": 0.30, ...} — populated from strategy.json
_BUFFER_OVERRIDES: Dict[str, float] = {}

def _get_breakeven_mult(entry_price: float, symbol: str = "") -> float:
    """Adaptive sell threshold multiplier based on coin price tier.

    Tier  | Price range    | Buffer  | Total required gain
    ------|----------------|---------|--------------------
    ultra | >= $1 000      | 0.08%   | ~0.28%
    high  | $10 - $1 000   | 0.10%   | ~0.30%
    mid   | $0.10 - $10    | 0.15%   | ~0.35%
    sub   | $0.001 - $0.10 | 0.20%   | ~0.40%
    micro | < $0.001       | 0.30%   | ~0.50%

    Per-coin overrides in strategy.json key "slippage_buffer_overrides" take priority.
    """
    if symbol and symbol in _BUFFER_OVERRIDES:
        buf = float(_BUFFER_OVERRIDES[symbol])
    elif entry_price >= 1000.0:
        buf = 0.08
    elif entry_price >= 10.0:
        buf = 0.10
    elif entry_price >= 0.1:
        buf = 0.15
    elif entry_price >= 0.001:
        buf = 0.20
    else:
        buf = 0.30
    return _FEE_FLOOR * (1.0 + buf / 100.0)

def _min_profit_usdt() -> float:
    """Minimum net profit (USDT) any take-profit sell must clear.

    Read from strategy.json exits.min_profit_usdt (default 0.01), clamped to
    >= 0.001. _load_strategy() is mtime-cached, so edits hot-reload without a
    restart.

    PARITY RULE (A1): this SAME value MUST be used by the sell trigger
    (compute_real_breakeven_price), the profit gate (_profitable_sell_check),
    and any (future) breakeven-move stop. If they ever resolve different
    numbers, the trigger fires sells that the gate then vetoes forever.
    """
    try:
        raw = _load_strategy().get("exits", {}).get("min_profit_usdt", 0.01)
        return max(0.001, float(raw))
    except Exception:
        return 0.01


# ── P1: the min_profit_usdt floor is a PROFIT-TAKING guard ONLY ───────────────
# Live safety-inversion bug: a breakeven-stop (stop moved to BEP at +1R) was
# rejected by the pre-sell profit gate because a flat exit could not clear the
# 0.01 USDT minimum, so it never sold and price rode back down to the real -1R
# stop — converting a scratch into a full loss. The floor must apply to
# take-profit and profit-ratchet exits ONLY. Every PROTECTIVE / risk exit
# (stop-loss, hard-stop, breakeven-stop, trailing stop, force, delist, reconcile,
# ghost, recycler, manual) MUST execute at market REGARDLESS of the floor.
_PROFIT_TAKING_EXIT_REASONS = frozenset({"take-profit", "profit-ratchet"})

# Known protective / risk exit reasons — the floor is EXEMPT for these. Kept
# explicit so the P1.3 regression assertion can detect a future edit that
# accidentally classifies a protective stop as profit-taking.
_PROTECTIVE_STOP_REASONS = frozenset({
    "stop-loss", "hard-stop-loss", "breakeven-stop", "trail", "smart-hold-trail",
    "oco-sl", "force-sell", "manual", "user-initiated", "auto-recycle",
    "delist", "ghost", "below-breakeven", "slippage-loss", "reconcile",
})


def _is_profit_taking_exit(reason: str) -> bool:
    """True when `reason` is a profit-taking exit the min_profit_usdt floor
    applies to (take-profit, profit-ratchet). Every other reason is a
    protective / risk exit that MUST fire at market regardless of the floor."""
    return str(reason or "").strip().lower() in _PROFIT_TAKING_EXIT_REASONS


def compute_real_breakeven_price(pos: dict, min_profit: Optional[float] = None) -> float:
    """Return the REAL price at which selling pos would net at least min_profit USDT.

    min_profit=None (the default — use it everywhere) resolves via
    _min_profit_usdt() so trigger, gate and future breakeven-move stop all read
    the same config value and can never disagree. Only pass an explicit value
    in tests.

    Uses ACTUAL deployed capital (qty * entry_price), not the requested budget.
    This matters when lot-step rounding reduces the actual quantity below what
    the budget would have bought — the position only needs to recover what was
    actually spent, plus fees, plus min_profit.

    Accounts for quantity rounding loss, buy fee already paid, and sell fee.
    Used as the sell trigger in both realtime_monitor and _sell_monitor_loop so
    trigger and profit gate use identical math and never disagree.
    """
    try:
        if min_profit is None:
            min_profit = _min_profit_usdt()
        entry_price = float(pos.get("entry_price") or pos.get("avg_entry_price") or 0)
        buy_fee     = float(pos.get("buy_fee_usdt") or 0)
        qty         = float(pos.get("quantity", 0))
        if qty <= 0 or entry_price <= 0:
            return 0.0
        actual_cost = qty * entry_price + buy_fee
        # qty * price * (1 - sell_fee_frac) >= actual_cost + min_profit.
        # Sell fee comes from the FeeModel (spec §1.1) — the SAME call the
        # profit gate (_profitable_sell_check) uses, so trigger and gate can
        # never disagree on the fee.
        sell_fee_frac = _fee_rate_for(pos.get("symbol", ""))
        return (actual_cost + min_profit) / (qty * (1.0 - sell_fee_frac))
    except Exception:
        return 0.0


# Configurable exit multipliers — refreshed from strategy.json every buy/sell cycle.
# _user_tp_mult:    raw user TP target (1 + tp_pct/100); compared to per-position breakeven.
# _take_profit_mult: kept for UI/diagnostic exports; updated by _refresh_risk_params.
# _stop_loss_mult:   price must fall to entry * this to trigger a stop-loss sell.
_user_tp_mult:    float = 1.001              # 0.1% above entry; updated by _refresh_risk_params
_take_profit_mult: float = _FEE_FLOOR * 1.0010  # kept for legacy compat; updated each cycle
_stop_loss_mult:   float = 1.0 - 0.02           # default: -2% stop loss

_stop_loss_confirmation: Dict[str, int] = {}
_STOP_LOSS_CONFIRMATION_TICKS = 2

# Post-loss cooldown: after a confirmed slippage-loss fill, skip re-buying that coin for 30 min
_loss_cooldown: Dict[str, float] = {}
_LOSS_COOLDOWN_SEC = 1800  # 30 minutes

# ── Phase 3 §3.1/§3.4 — entries config + protections state ────────────────────
# strategy.json "entries" block (all hot-reloadable via _load_strategy):
#   eval_heartbeat_sec      (15)   veto-only re-check cadence between 5m closes
#   tick_entries            (False) legacy escape hatch: restore per-tick buy
#                                   dispatch from realtime_monitor
#   falling_knife_atr_mult  (1.0)  falling-knife threshold = mult × atr_pct
#                                   (5m ATR%, Phase 2 ladder); fallback 0.4%
#   cooldown_after_sl_min   (15)   per-symbol re-entry cooldown after a
#                                   stop-loss / hard-stop-loss exit
# §3.5 maker-first entry keys (live mode only):
#   maker_first             (True)  post LIMIT_MAKER at best bid instead of a
#                                   taker market buy for automatic live entries
#   chase_seconds           (3)     poll window per post before cancel+repost
#   max_reposts             (3)     reposts allowed after the initial post
#                                   (-2010 'would match' rejections count too)
#   taker_fallback          (False) after the chase is exhausted, fall back to
#                                   ONE taker market buy if the signal holds
# §3.6:
#   prefer_fee_promo_pairs  (False) advisory-only: log + flag when a matching
#                                   zero-maker-fee FDUSD pair is configured
# G1a/G1b (v0.4 Part G) — keys READ here; the strategy_config agent owns the
# JSON-schema additions. Defaults are merged over strategy.json below so entries
# keep working even before the schema lands:
#   bookticker_universe     (False) when True, data_collector streams @bookTicker
#                                   for ALL covered symbols (data agent's job);
#                                   here it only tells the maker-first path to
#                                   TRUST the WS book and skip the on-demand REST
#                                   best-bid fetch.
#   maker_abandon_max       (3)     consecutive maker-entry abandonments before a
#                                   symbol gets a 5-min candidacy cooldown (F6
#                                   machinery) + ONE deduped log, instead of a
#                                   warning every scan.
#   max_lot_waste_pct       (5.0)   lot-step rounding-waste ceiling (% of budget);
#                                   below it the buy PROCEEDS with the floored qty
#                                   (only exchange minNotional is a hard floor).
# v0.4 Part L — entry-quality keys (READ here at time of use):
#   taker_fallback_max_spread_pct (0.05) L1.1: when the maker chase is exhausted
#                                   and legacy taker_fallback is OFF, cross with a
#                                   MARKET buy anyway IFF the live full spread% is
#                                   <= this ceiling (0 disables the spread-guarded
#                                   fallback → abandon as before).
#   max_friction_of_stop    (15.0)  L2.1: skip an entry when friction
#                                   (half_spread% + recent avg exit slippage%)
#                                   exceeds this percent of the planned 1R stop
#                                   distance (structurally poor entry).
#   min_quote_volume_24h_usd (20000000) L2.2: skip symbols whose 24h quote
#                                   volume (USDT) is below this liquidity floor.
# v0.4 Part O5 — near-live buy execution (READ here at time of use):
#   confirm_seconds         (10.0)  O5.1/O5.3: how long a candidate must HOLD its
#                                   fresh-re-check-confirmed buy-ready state on
#                                   the fast re-check path before the entry fires.
#                                   0 → fire on the first confirmed tick; 300 →
#                                   require a full candle of held confirmation.
#                                   It tunes confirm LATENCY only — it never lets
#                                   a buy skip the fresh re-check gate.
#   cooldown_recheck_fail_min (1.0) O5.2: candidacy cooldown after a fresh
#                                   re-check FAIL / near-miss (near-zero — the
#                                   coin may qualify again next tick; must NOT
#                                   bench for 30 min).
#   cooldown_thin_min       (5.0)   O5.2: candidacy cooldown after a thin-
#                                   liquidity skip (replaces the flat 30 min).
#   cooldown_spread_min     (5.0)   O5.2: candidacy cooldown after a wide-spread /
#                                   high-friction skip.
_ENTRIES_DEFAULTS = {
    "eval_heartbeat_sec":     15.0,
    "tick_entries":           False,
    "falling_knife_atr_mult": 1.0,
    "cooldown_after_sl_min":  15.0,
    "maker_first":            True,
    "chase_seconds":          3.0,
    "max_reposts":            3,
    "taker_fallback":         False,
    "prefer_fee_promo_pairs": False,
    "bookticker_universe":    False,
    "maker_abandon_max":      3,
    "max_lot_waste_pct":      5.0,
    "taker_fallback_max_spread_pct": 0.05,
    "max_friction_of_stop":   15.0,
    "min_quote_volume_24h_usd": 20000000.0,
    # v0.4 Part O5 — near-live buy execution
    "confirm_seconds":            10.0,
    "cooldown_recheck_fail_min":  1.0,
    "cooldown_thin_min":          5.0,
    "cooldown_spread_min":        5.0,
    # v0.4 Part Q1 — when slots are free, fire the highest-scoring ready
    # candidate immediately instead of stranding it in the confirm_seconds hold
    # (the pydantic schema addition is handled separately; read defensively).
    "instant_fire_when_slots_free": True,
    # WolfBot v0.5 Part S2 — EV-based buy SELECTION. When enabled, already-
    # eligible ready candidates are processed in DESCENDING win-probability order
    # (buy the 72% before the 51%). min_win_probability is a probability FLOOR
    # (0-1) that only gates a real buy when the active model is trained AND has
    # ≥300 clean trades (ev_model.model_status()['floor_active']); otherwise it is
    # display/advisory only and never blocks a buy. Both are read defensively.
    "ev_ranking_enabled":         True,
    "min_win_probability":        0.0,
    # WolfBot v0.5 Part S-2 — WolfScore v3 buy SELECTION. The buy floor is
    # regime-aware: a candidate must clear BOTH the absolute floor AND the
    # distribution rule. min_win_probability_floor is the ABSOLUTE 0-100 floor;
    # ev_floor_mode selects the distribution rule ('p75' | 'meanstd' | 'off');
    # ev_floor_meanstd_k scales the stdev term in 'meanstd'. The floor only GATES
    # a real buy when the active model is trained AND past the clean-trade
    # guardrail (ev_model.model_status()['floor_active']); otherwise advisory.
    # NOTE: ev_floor_mode is a STRING — read it directly from strategy.entries
    # (not via _entries_cfg, whose numeric coercion would reject a string).
    "min_win_probability_floor":  55.0,
    "ev_floor_meanstd_k":         0.5,
}


def _entries_cfg() -> dict:
    """Effective strategy.entries config (§3.1/§3.4/§3.5/§3.6) — defaults
    merged over strategy.json, mtime-cached via _load_strategy so edits
    hot-reload."""
    raw = _load_strategy().get("entries")
    raw = raw if isinstance(raw, dict) else {}
    cfg = {}
    for key, default in _ENTRIES_DEFAULTS.items():
        val = raw.get(key, default)
        try:
            if isinstance(default, bool):
                cfg[key] = bool(val)
            elif isinstance(default, int):
                cfg[key] = int(float(val))
            else:
                cfg[key] = float(val)
        except (TypeError, ValueError):
            cfg[key] = default
    return cfg


def _neutral_size_mult() -> float:
    """§3.3 strategy.regime.neutral_size_mult (default 0.5): budget multiplier
    applied when the BTC regime is 'neutral' (live + paper)."""
    try:
        raw = _load_strategy().get("regime", {})
        return max(0.0, min(1.0, float(raw.get("neutral_size_mult", 0.5))))
    except Exception:
        return 0.5


def _neutral_scaling_mode_cfg() -> str:
    """M1.2 strategy.regime.neutral_scaling_mode (auto|size|slots|off, default
    "auto"). The RAW setting; _resolve_neutral_scaling turns "auto" into a
    concrete mode at entry time."""
    try:
        raw = _load_strategy().get("regime", {})
        mode = str(raw.get("neutral_scaling_mode", "auto")).strip().lower()
        return mode if mode in ("auto", "size", "slots", "off") else "auto"
    except Exception:
        return "auto"


def _regime_risk_off_pct_4h() -> float:
    """F2.2 strategy.regime.risk_off_pct_4h (default −1.0): the 4h-return
    threshold BTC must break (AND price<EMA50) to classify risk_off."""
    try:
        raw = _load_strategy().get("regime", {})
        return float(raw.get("risk_off_pct_4h", -1.0))
    except Exception:
        return -1.0


# §3.4c — global correlated-dump pause: >=3 stop-outs within 10 minutes pause
# ALL entries for 5 minutes (module state; surfaced as gate blocker
# 'global_stop_pause' and in the activity log).
_STOP_PAUSE_COUNT      = 3
_STOP_PAUSE_WINDOW_SEC = 600.0
_STOP_PAUSE_DURATION_SEC = 300.0
_stop_out_ts: List[float] = []          # rolling stop-out timestamps
_stop_out_lock = threading.Lock()
_global_stop_pause_until: float = 0.0
_last_stop_pause_log_ts: float = 0.0    # throttle the "paused" log to 1/min


def _global_stop_pause_active(now: Optional[float] = None) -> bool:
    return (now if now is not None else time.time()) < _global_stop_pause_until


def _record_stop_out(sym: str, reason: str) -> None:
    """§3.4b/§3.4c — called when a position exits via stop-loss/hard-stop-loss.

    1. Per-symbol cooldown: entries.cooldown_after_sl_min minutes (replaces
       the 30 s COOLDOWN_AFTER_LOSS for stop-outs specifically; other losing
       exits keep their existing behavior — e.g. the slippage-loss cooldown).
    2. Global pause: 3 stop-outs in 10 min → pause ALL entries for 5 min
       (correlated-dump protection).
    """
    global _global_stop_pause_until
    now = time.time()
    try:
        cd_min = _entries_cfg()["cooldown_after_sl_min"]
    except Exception:
        cd_min = _ENTRIES_DEFAULTS["cooldown_after_sl_min"]
    _cooldowns[sym] = now + cd_min * 60.0
    try:
        database.log_activity(
            f"{sym}: stop-out ({reason}) — re-entry cooldown {cd_min:.0f} min", "info")
    except Exception:
        pass
    triggered = False
    with _stop_out_lock:
        _stop_out_ts.append(now)
        _stop_out_ts[:] = [t for t in _stop_out_ts if t >= now - _STOP_PAUSE_WINDOW_SEC]
        n_recent = len(_stop_out_ts)
        if n_recent >= _STOP_PAUSE_COUNT and now >= _global_stop_pause_until:
            _global_stop_pause_until = now + _STOP_PAUSE_DURATION_SEC
            triggered = True
    if triggered:
        try:
            database.log_activity(
                f"GLOBAL STOP PAUSE: {n_recent} stop-outs within "
                f"{int(_STOP_PAUSE_WINDOW_SEC // 60)} min — ALL entries paused for "
                f"{int(_STOP_PAUSE_DURATION_SEC // 60)} min (correlated dump protection)",
                "warn")
        except Exception:
            pass
        try:
            log_diag_issue("entries", "warn",
                           f"global_stop_pause armed after {n_recent} stop-outs")
        except Exception:
            pass

# Abort-log throttle — SELL ABORTED fires every 250ms per stuck position; cap to 1/min per symbol
_last_abort_log_ts: Dict[str, float] = {}
_ABORT_LOG_THROTTLE_SEC = 60.0

# Minimum hold time after a buy — prevents race-condition sells within seconds of entry
_MIN_HOLD_SEC = 10.0

# ── Phase 2 §2.6 — exits config ───────────────────────────────────────────────
# The CANONICAL exit-config resolver (defaults + legacy-key mapping) lives in
# backtest.exit_config so live engine and replay always resolve identical
# geometry from identical strategy dicts. A byte-identical local fallback keeps
# the live engine independent of backtest.py importability (backtest pulls in
# signal_registry, which this module already treats as optional).
#
# LEGACY MAPPING (documented in backtest.exit_config; summary):
#   no "exits" block + legacy keys present →
#     stop_loss_pct   → sl_min_pct == sl_max_pct (fixed distance, k_sl=0)
#     stop_loss_enabled → sl_enabled
#     take_profit_pct → rr_ratio=None + tp_pct (tp = max(entry×(1+tp%), BEP))
#     take_profit_enabled → tp_enabled
#     smart_hold_enabled  → smart_hold_score_gate
#     trailing_stop_pct   → legacy_trailing_stop_pct
#     hard_sl / BE-move / ATR-trail disabled (old behavior preserved).
_exit_cfg_cache: dict = {}
_exit_cfg_cache_mtime: float = -1.0
_exit_cfg_impl = None  # resolved lazily: backtest.exit_config or local fallback


def _exit_config_fallback(strategy: dict) -> dict:
    """Identical fallback copy of backtest.exit_config — keep in sync."""
    defaults = {
        "k_sl": 1.2, "sl_min_pct": 0.5, "sl_max_pct": 2.5, "hard_sl_pct": 3.0,
        "rr_ratio": 1.6, "tp_buffer_pct": 0.05, "min_profit_usdt": 0.01,
        # F1: BE-move arms at +1.2R (was +1.0R) so the TP target has room to
        # print before the stop rises to BEP — keeps a +1R-then-fade a small
        # BEP scratch instead of a floor-scratch masquerading as a win.
        "breakeven_at_r": 1.2, "k_trail": 0.8, "smart_hold_score_gate": False,
        "sl_confirm_ticks": 2, "min_hold_sec": 10.0,
        "maker_tp": True, "maker_tp_timeout_ms": 1500,
        "oco_enabled": False, "oco_stop_limit_buffer_pct": 0.5,
        "oco_skip_rescue_sec": 3.0,
    }
    legacy_keys = ("stop_loss_pct", "take_profit_pct", "trailing_stop_pct",
                   "stop_loss_enabled", "take_profit_enabled",
                   "smart_hold_enabled")
    strategy = strategy if isinstance(strategy, dict) else {}
    exits_raw = strategy.get("exits")
    has_exits = isinstance(exits_raw, dict)
    exits = exits_raw if has_exits else {}
    cfg: dict = {}
    for key, default in defaults.items():
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
    cfg["tp_pct"] = None
    cfg["legacy_trailing_stop_pct"] = None
    if not has_exits and any(k in strategy for k in legacy_keys):
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
        cfg["k_sl"] = 0.0
        cfg["hard_sl_pct"] = None
        cfg["rr_ratio"] = None
        cfg["tp_enabled"] = bool(strategy.get("take_profit_enabled", True))
        cfg["tp_pct"] = tp_pct
        cfg["tp_buffer_pct"] = 0.0
        cfg["breakeven_at_r"] = None
        cfg["k_trail"] = 0.0
        cfg["smart_hold_score_gate"] = bool(strategy.get("smart_hold_enabled", False))
        cfg["legacy_trailing_stop_pct"] = trail_pct
    return cfg


def _exit_cfg() -> dict:
    """Effective exits config (§2.6) from strategy.json — hot-reloadable.

    Cached on the strategy file mtime (via _load_strategy), so edits apply on
    the next tick without a restart. Returns the dict documented in
    backtest.exit_config (k_sl, sl_min_pct, sl_max_pct, hard_sl_pct, rr_ratio,
    tp_buffer_pct, min_profit_usdt, breakeven_at_r, k_trail,
    smart_hold_score_gate, sl_confirm_ticks, min_hold_sec, maker_tp,
    maker_tp_timeout_ms, oco_enabled, oco_stop_limit_buffer_pct,
    oco_skip_rescue_sec + derived legacy_mode/sl_enabled/tp_enabled/tp_pct/
    legacy_trailing_stop_pct)."""
    global _exit_cfg_cache, _exit_cfg_cache_mtime, _exit_cfg_impl
    strategy = _load_strategy()
    if _exit_cfg_cache and _exit_cfg_cache_mtime == _strategy_mtime:
        return _exit_cfg_cache
    if _exit_cfg_impl is None:
        try:
            from backtest import exit_config as _exit_cfg_impl_fn
            _exit_cfg_impl = _exit_cfg_impl_fn
        except Exception:
            _exit_cfg_impl = _exit_config_fallback
    try:
        cfg = _exit_cfg_impl(strategy)
    except Exception:
        cfg = _exit_config_fallback(strategy)
    _exit_cfg_cache = cfg
    _exit_cfg_cache_mtime = _strategy_mtime
    return cfg


def _sl_confirm_ticks() -> int:
    """Stop-loss confirmation ticks — exits.sl_confirm_ticks (default: the
    legacy module constant _STOP_LOSS_CONFIRMATION_TICKS)."""
    try:
        return max(1, int(_exit_cfg().get("sl_confirm_ticks",
                                          _STOP_LOSS_CONFIRMATION_TICKS)))
    except Exception:
        return _STOP_LOSS_CONFIRMATION_TICKS


def _min_hold_sec() -> float:
    """Minimum hold before a stop-loss may fire — exits.min_hold_sec (default:
    the legacy module constant _MIN_HOLD_SEC)."""
    try:
        return max(0.0, float(_exit_cfg().get("min_hold_sec", _MIN_HOLD_SEC)))
    except Exception:
        return _MIN_HOLD_SEC


# ── P2: ATR-based profit-ratchet trailing stop ────────────────────────────────
# Locks gains once a trade is meaningfully green (between the BE-move and the
# +rr_ratio TP target). Config keys live in strategy.json exits.* — they are NOT
# in backtest.EXIT_DEFAULTS, so _exit_cfg() does not surface them; read the raw
# exits block directly (hot-reloadable via _load_strategy's mtime cache).
def _ratchet_cfg() -> dict:
    """Effective profit-ratchet config, read live at time of use."""
    try:
        exits = _load_strategy().get("exits", {}) or {}
    except Exception:
        exits = {}

    def _f(key, default):
        try:
            return float(exits.get(key, default))
        except (TypeError, ValueError):
            return default

    def _b(key, default):
        val = exits.get(key, default)
        try:
            return bool(val)
        except (TypeError, ValueError):
            return default

    return {
        "enabled":       _b("ratchet_enabled", True),
        "activate_r":    _f("ratchet_activate_r", 0.4),
        "activate_usdt": _f("ratchet_activate_usdt", 0.02),
        "k_atr":         _f("ratchet_k_atr", 0.6),
        "giveback_pct":  _f("ratchet_giveback_pct", 50.0),
    }


# Per-symbol profit-ratchet state — {armed, peak_price, peak_profit}. Cleared on
# close (the _execute_sell finally, like _pos_peaks) and in purge_symbol_state so
# a re-opened position never inherits a stale high-water mark.
_ratchet_state: Dict[str, dict] = {}


def _unrealized_net_profit(pos: dict, price: float, symbol: str):
    """Net unrealized profit (USDT) if pos were market-sold at `price` NOW —
    identical math to _profitable_sell_check (deployed capital + taker fee)."""
    try:
        entry   = float(pos.get("entry_price") or pos.get("avg_entry_price") or 0)
        qty     = float(pos.get("quantity") or 0)
        buy_fee = float(pos.get("buy_fee_usdt") or 0)
        if entry <= 0 or qty <= 0 or price <= 0:
            return None
        gross_quote  = price * qty
        net_returned = gross_quote - gross_quote * _fee_rate_for(symbol)
        return net_returned - (qty * entry + buy_fee)
    except Exception:
        return None


def _evaluate_ratchet(pos: dict, sym: str, price: float, entry: float,
                      now: float, cfg: dict) -> bool:
    """P2 profit-ratchet. Maintains per-symbol arm/peak state and returns True
    when the ratchet should fire a 'profit-ratchet' exit.

    Semantics (all numbers read from config at time of use, hot-reload):
      • Activation — arm once unrealized profit >= ratchet_activate_r × 1R (the
        position's OWN planned stop distance, scales per coin) OR >=
        ratchet_activate_usdt. Once armed, stays armed.
      • Peak — track highest price AND highest unrealized profit since arming.
      • ATR trail — ratchet stop = peak_price − ratchet_k_atr × ATR(price units).
      • Give-back cap — also exit if profit <= (1 − giveback_pct/100) × peak_profit.
        Whichever (ATR trail or give-back) triggers first fires the exit.
      • Profit floor — the ratchet only ever exits IN PROFIT: if the current exit
        would net < min_profit_usdt, HOLD (the protective stop from P1 is the
        backstop). Ratchet = profit-taking → floor applies.
    """
    rc = _ratchet_cfg()
    if not rc["enabled"]:
        _ratchet_state.pop(sym, None)
        return False

    profit = _unrealized_net_profit(pos, price, sym)
    if profit is None:
        return False

    qty     = float(pos.get("quantity") or 0)
    sl_dist = pos.get("sl_distance_pct")
    # 1R in USDT from the position's own planned stop distance.
    r_usdt = (qty * entry * float(sl_dist) / 100.0
              if (sl_dist and qty > 0 and entry > 0) else None)

    st = _ratchet_state.get(sym)
    if st is None:
        st = {"armed": False, "peak_price": price, "peak_profit": profit}
        _ratchet_state[sym] = st

    # ── Activation ────────────────────────────────────────────────────────────
    if not st["armed"]:
        armed = False
        if r_usdt and r_usdt > 0 and profit >= rc["activate_r"] * r_usdt:
            armed = True          # preferred R form (scales per coin)
        elif profit >= rc["activate_usdt"]:
            armed = True
        if not armed:
            return False
        st["armed"] = True
        st["peak_price"]  = price
        st["peak_profit"] = profit

    # ── Peak tracking since activation ────────────────────────────────────────
    if price > st["peak_price"]:
        st["peak_price"] = price
    if profit > st["peak_profit"]:
        st["peak_profit"] = profit
    peak_price  = st["peak_price"]
    peak_profit = st["peak_profit"]

    # ── ATR trail (ATR converted to price units for this symbol) ──────────────
    atr_pct = (pos.get("atr_pct_at_entry") or sl_dist or cfg.get("sl_min_pct") or 0.0)
    atr_price = entry * float(atr_pct) / 100.0 if atr_pct else 0.0
    ratchet_stop = peak_price - rc["k_atr"] * atr_price
    trail_hit = atr_price > 0 and price <= ratchet_stop

    # ── Give-back cap ─────────────────────────────────────────────────────────
    giveback_floor = (1.0 - rc["giveback_pct"] / 100.0) * peak_profit
    giveback_hit = peak_profit > 0 and profit <= giveback_floor

    if not (trail_hit or giveback_hit):
        return False

    # ── Profit floor — the ratchet only ever exits IN PROFIT ─────────────────
    # If a pullback would drop the exit below the floor, HOLD; the protective
    # stop (P1, now unblocked) is the backstop.
    if profit < _min_profit_usdt():
        return False
    return True

# Per-symbol throttle for SELL_TRACE diagnostic log (1 per 60s per symbol)
_sell_trace_log_ts: Dict[str, float] = {}

# ── Shadow-eval log dedupe (B Step 5) ─────────────────────────────────────────
# The shadow disagreement/failure logs fire on every buy-loop pass — for a
# persistently disagreeing symbol that's one diag row every few seconds.
# Dedupe per (symbol, reason): identical entries within 15 min are only
# counted; the next emitted line carries "(×N in last 15m)".
_shadow_log_dedupe: Dict[Tuple[str, str], dict] = {}
_SHADOW_DEDUPE_WINDOW_SEC = 900.0  # 15 minutes


def _log_shadow_dedup(symbol: str, reason: str, message: str) -> None:
    """Emit a signal_shadow diag line, deduped per (symbol, reason) per 15 min."""
    key = (symbol, reason)
    now = time.time()
    ent = _shadow_log_dedupe.get(key)
    if ent and (now - ent["last_ts"]) < _SHADOW_DEDUPE_WINDOW_SEC:
        ent["suppressed"] += 1
        return
    suffix = ""
    if ent and ent["suppressed"] > 0:
        suffix = f" (×{ent['suppressed'] + 1} in last 15m)"
    _shadow_log_dedupe[key] = {"last_ts": now, "suppressed": 0}
    log_diag_issue("signal_shadow", "warn", message + suffix)


# ── F6: candidate-churn control ───────────────────────────────────────────────
# A fresh-engine re-check FAIL in the buy path (e.g. score_2_below_min) used to
# leave the STALE high score in the signal cache, so the fast pre-check
# re-selected the same coin every ~7s (TRXUSDT diagnostic). We now (1) write the
# fresh score back to the cache so the pre-check reflects reality, and (2) apply
# a per-symbol candidacy cooldown so a just-failed coin is not re-evaluated for
# 60s. [SKIP] activity lines are deduped per (symbol, reason) over 15 min.
_candidacy_cooldown: Dict[str, float] = {}          # sym -> ts until re-eligible
_CANDIDACY_COOLDOWN_SEC = 60.0
_skip_log_dedupe: Dict[Tuple[str, str], dict] = {}  # (sym, reason) -> {last_ts, suppressed}
_SKIP_DEDUPE_WINDOW_SEC = 900.0                     # 15 minutes


def _log_skip_dedup(symbol: str, reason: str, message: str,
                    level: str = "info") -> None:
    """Emit a [SKIP] activity line deduped per (symbol, reason) per 15 min —
    identical entries inside the window are counted, the next emitted line
    carries '(×N in last 15m)'."""
    key = (symbol, reason)
    now = time.time()
    ent = _skip_log_dedupe.get(key)
    if ent and (now - ent["last_ts"]) < _SKIP_DEDUPE_WINDOW_SEC:
        ent["suppressed"] += 1
        return
    suffix = ""
    if ent and ent["suppressed"] > 0:
        suffix = f" (×{ent['suppressed'] + 1} in last 15m)"
    _skip_log_dedupe[key] = {"last_ts": now, "suppressed": 0}
    try:
        database.log_activity(message + suffix, level)
    except Exception:
        pass


def _note_candidacy_fail(symbol: str, fresh_score, reason: str = "") -> None:
    """F6: on a fresh re-check FAIL, write the fresh score back to the signal
    cache (so the fast pre-check stops re-selecting the coin) and arm the
    reason-specific candidacy cooldown (O5.2: recheck-fail → near-zero, not the
    old flat 30 min). Also resets the O5.1 confirm-then-fire timer so a failed
    re-check restarts the confirmation window from the next fresh confirmation."""
    try:
        if fresh_score is not None:
            with _signal_cache_lock:
                ent = _signal_cache.get(symbol)
                if ent is not None:
                    ent["score"] = int(fresh_score)
                    ent["buy_ready"] = False
    except Exception:
        pass
    # O5.1 — a fresh re-check FAIL invalidates any held confirmation.
    _clear_buy_ready(symbol)
    # O5.2 — recheck-fail cooldown (config-driven, read at time of use).
    _candidacy_cooldown[symbol] = time.time() + _cooldown_secs_for("recheck_fail")


def _in_candidacy_cooldown(symbol: str) -> bool:
    """True while the F6 per-symbol candidacy cooldown is active."""
    until = _candidacy_cooldown.get(symbol, 0.0)
    if until <= 0.0:
        return False
    if time.time() >= until:
        _candidacy_cooldown.pop(symbol, None)
        return False
    return True


# ── O5 — near-live buy execution helpers ──────────────────────────────────────
# Confirm-then-fire (O5.1/O5.3): _buy_ready_since[sym] records WHEN a candidate
# FIRST became fresh-re-check-confirmed buy-ready (it passed the pre-buy fresh
# candle re-check inside _check_buys_from_cache). The fast re-check loop and the
# 15 s heartbeat both flow through the SAME gated buy path; the entry fires only
# once the candidate has HELD that confirmed state for entries.confirm_seconds.
# The stamp is cleared on any fresh re-check FAIL (via _note_candidacy_fail),
# when a legacy cached-green candidate flips back below min_signals, and after a
# fill — so the confirmation window always restarts from a fresh confirmation.
_buy_ready_since: Dict[str, float] = {}
_buy_ready_lock = threading.Lock()


def _mark_buy_ready(symbol: str) -> float:
    """Stamp (once) the moment `symbol` became fresh-re-check-confirmed buy-ready
    and return that timestamp. Idempotent — repeated calls keep the FIRST stamp
    so the confirm window measures HELD confirmation, not the latest tick."""
    now = time.time()
    with _buy_ready_lock:
        ts = _buy_ready_since.get(symbol)
        if ts is None:
            ts = now
            _buy_ready_since[symbol] = ts
        return ts


def _clear_buy_ready(symbol: str) -> None:
    """Reset the confirm-then-fire timer (fresh re-check failed, cached-green
    flipped off, or the entry filled)."""
    with _buy_ready_lock:
        _buy_ready_since.pop(symbol, None)


def _has_pending_buy_confirmation() -> bool:
    """True while at least one symbol is mid-confirmation — the fast re-check
    loop uses this (alongside a cached-green candidate) as its cheap dispatch
    gate so it advances timers without re-scoring the whole universe every 2-3s."""
    with _buy_ready_lock:
        return bool(_buy_ready_since)


def _cooldown_secs_for(reason: str) -> float:
    """O5.2 — reason-specific candidacy cooldown (seconds), config-driven and
    read at time of use (hot-reload). Replaces the flat ~30-min bench that parked
    any skipped coin for ~6 candles:
      recheck_fail → entries.cooldown_recheck_fail_min (near-zero: the coin may
                     qualify again next tick, so it must NOT sit 30 min)
      thin         → entries.cooldown_thin_min   (thin liquidity)
      spread       → entries.cooldown_spread_min (wide spread / high friction)
    Maker-abandon and budget cooldowns are intentionally NOT routed here — they
    keep their existing 5-min value at their own call sites."""
    cfg = _entries_cfg()
    r = (reason or "").lower()
    if r in ("recheck_fail", "fresh_score_below_min", "stale_signals",
             "near_miss"):
        return max(0.0, float(cfg.get("cooldown_recheck_fail_min", 1.0))) * 60.0
    if r in ("thin", "thin_liquidity"):
        return max(0.0, float(cfg.get("cooldown_thin_min", 5.0))) * 60.0
    if r in ("spread", "wide_spread", "high_friction", "friction"):
        return max(0.0, float(cfg.get("cooldown_spread_min", 5.0))) * 60.0
    # Unknown reason → the historical 60 s transient candidacy cooldown.
    return _CANDIDACY_COOLDOWN_SEC


# ── Phase 1 §1.3 — entry snapshots (executed buys + near-misses) ──────────────
# Near-miss snapshot throttle: one row per (symbol, reason) per 5 minutes so a
# persistent blocker (e.g. cooldown, bb_upper) can't write thousands of rows.
_near_miss_snap_ts: Dict[Tuple[str, str], float] = {}
_NEAR_MISS_SNAP_THROTTLE_SEC = 300.0

# Rejection reasons that occur BEFORE the fast pre-check passes — these are not
# "near-misses" (the coin never signalled a buy) and are never snapshotted.
_NON_NEAR_MISS_REASONS = frozenset({
    "signal_cache_empty", "pre_gate_below_min_signals", "below_min_signals",
})


def _snapshot_raw_from_cache(cached: dict, price) -> dict:
    """Numeric signal context for an entry snapshot, from a signal-cache entry."""
    cached = cached or {}
    sigs = cached.get("signals") or {}
    return {
        "price":         price,
        "rsi_val":       cached.get("rsi_val"),
        "stoch_rsi_val": cached.get("stoch_rsi_val"),
        "score":         cached.get("score"),
        "low_24h":       cached.get("low_24h"),
        "signals":       {k: bool(v) for k, v in sigs.items()},
        "bb_ok":         cached.get("bb_ok"),
        "5m_ok":         cached.get("5m_ok"),
        "cache_ts":      cached.get("ts"),
    }


# ── WolfBot v0.5 Part S2 — EV buy-scoring helpers ─────────────────────────────
# All ev_model access flows through these guards so a scoring failure NEVER
# affects the buy decision beyond falling back to today's ordering/gating.

def _ev_raw_from_cache(cached: dict) -> dict:
    """Richest raw signal dict for EV scoring, from a signal-cache entry. Reuses
    the entry-snapshot builder (so EV features match what the snapshot records)
    and adds the magnitude aliases ev_model recognizes when they're present."""
    cached = cached or {}
    raw = _snapshot_raw_from_cache(cached, cached.get("price"))
    if raw.get("rsi_val") is not None:
        raw.setdefault("rsi_value", raw["rsi_val"])
    for _k in ("atr_pct", "spread_pct", "vol_ratio", "ema_gap_pct",
               "macd_hist", "obv_slope", "near_low_pct", "volume_ratio"):
        _v = cached.get(_k)
        if _v is not None:
            raw[_k] = _v
    return raw


def _ev_score_cached(cached: dict) -> Optional[dict]:
    """Guarded ev_model.score() from a signal-cache entry. None on any failure or
    when the module is unavailable — callers must degrade to raw-score behavior."""
    if ev_model is None:
        return None
    try:
        return ev_model.score(_ev_raw_from_cache(cached))
    except Exception:
        return None


def _ev_floor_active() -> bool:
    """True only when the active model is trained AND ≥ the clean-trade guardrail
    (S4). Below this the probability floor is DISPLAY-ONLY (never gates a buy)."""
    if ev_model is None:
        return False
    try:
        return bool(ev_model.model_status().get("floor_active", False))
    except Exception:
        return False


# ── WolfBot v0.5 Part S-2 — WolfScore v3 sub-metric inputs + scoring ──────────
# WolfScore replaces the generic EV score for buy SELECTION. Everything here is
# guarded so a scoring failure NEVER affects the buy beyond falling back to the
# pre-existing first-ready / raw-score behavior. The 8 sub-metrics are built from
# what the engine already computes for a symbol; any feed we can't source yet is
# OMITTED (compute_submetrics then degrades that sub-metric to 0/neutral — we
# never fabricate a value).

_btc_roc1h_cache: Dict[str, Any] = {"ts": 0.0, "val": None}
_BTC_ROC1H_TTL_SEC = 60.0


def _btc_roc_1h_frac() -> Optional[float]:
    """BTC's last-closed 1h ROC as a FRACTION (+0.01 = +1%), for WolfScore's
    regime tilt (ev_model.regime_tilt expects a fraction, not %). Reuses the
    existing 1h-kline fetch; cached 60s. None on any failure → tilt degrades to 0
    (neutral). 1h (not 15m) is deliberate — the tilt formula assumes 1h ROC."""
    now = time.time()
    c = _btc_roc1h_cache
    if c["val"] is not None and (now - c["ts"]) < _BTC_ROC1H_TTL_SEC:
        return c["val"]
    val = None
    try:
        kl = _fetch_btc_1h_klines()
        closes = [float(k["close"]) for k in (kl or []) if k.get("close") is not None]
        if len(closes) >= 2 and closes[-2] > 0:
            val = (closes[-1] - closes[-2]) / closes[-2]
    except Exception:
        val = None
    c["ts"] = now
    c["val"] = val
    return val


def _wolf_roc_15m(cached: dict) -> Optional[float]:
    """15m ROC as a FRACTION from the freshest 5m klines (3 intervals = 15m).
    None when fewer than 4 closes are cached. Used for both R (per-coin) and the
    cohort median."""
    try:
        k5 = (cached or {}).get("klines_5m") or []
        closes = [float(c["close"]) for c in k5
                  if isinstance(c, dict) and c.get("close") is not None]
        if len(closes) >= 4 and closes[-4] > 0:
            return (closes[-1] - closes[-4]) / closes[-4]
    except Exception:
        pass
    return None


def _wolf_inputs(sym: str, cached: dict) -> dict:
    """Assemble the WolfScore v3 sub-metric inputs for `sym` from what the engine
    already computes (5m klines + cached ATR% + live book / slippage / planned
    stop). Every key is OPTIONAL — an unavailable feed is OMITTED and
    compute_submetrics degrades that sub-metric to 0/neutral (we never fabricate).

    Real inputs today:
      T  ema9, ema21, atr_pct           (EMA9/21 from 5m closes; ATR% cached)
      M  macd_hist, rolling_max_abs_hist_20 (MACD hist; needs >=35 closes)
      R  roc_15m                        (this coin's 15m ROC; cohort median from caller)
      W  p_mid, vwap_15m                (15m VWAP over 3×5m candles vs mid price)
      V  vol_5m, avg_vol_20             (last 5m volume vs 20-bar average)
      X  atr_pct, atr_target, atr_halfwidth (tradeable ATR band the engine gates on)
      F  half_spread_pct, avg_slippage_pct, planned_stop_pct (highest-weight term)
    Degrades to neutral: C (taker CVD buy/sell split is NOT streamed → omitted)."""
    inp: dict = {}
    cached = cached or {}
    price = cached.get("price")
    k5 = cached.get("klines_5m") or []
    closes = [float(c["close"]) for c in k5
              if isinstance(c, dict) and c.get("close") is not None]
    vols = [float(c["volume"]) for c in k5
            if isinstance(c, dict) and c.get("volume") is not None]

    # ATR% (cached, as a percent) → feeds both T (normalizer) and X.
    atr_pct = cached.get("atr_pct")
    if atr_pct is not None:
        try:
            inp["atr_pct"] = float(atr_pct)
        except (TypeError, ValueError):
            pass

    # T — EMA9 / EMA21 on 5m closes (gap normalized by ATR% inside the model).
    try:
        if len(closes) >= 21:
            e9 = indicators.calc_ema(closes, 9)
            e21 = indicators.calc_ema(closes, 21)
            if e9 and e9[-1] is not None:
                inp["ema9"] = e9[-1]
            if e21 and e21[-1] is not None:
                inp["ema21"] = e21[-1]
    except Exception:
        pass

    # M — MACD histogram + rolling max |hist| over the last 20. calc_macd needs
    # >=35 closes (26 slow + 9 signal); the 5m cache holds only ~30, so fall back
    # to the deeper 1m buffer when it carries enough history. Both are scale-
    # invariant (normalized by their own rolling max) so the timeframe mix is OK.
    try:
        m_closes = closes if len(closes) >= 35 else None
        if m_closes is None:
            k1 = cached.get("klines_1m") or []
            c1 = [float(c["close"]) for c in k1
                  if isinstance(c, dict) and c.get("close") is not None]
            if len(c1) >= 35:
                m_closes = c1
        if m_closes:
            _, _, hist = indicators.calc_macd(m_closes)
            hvals = [h for h in hist if h is not None]
            if hvals:
                inp["macd_hist"] = hvals[-1]
                inp["rolling_max_abs_hist_20"] = max(abs(h) for h in hvals[-20:])
    except Exception:
        pass

    # R — this coin's 15m ROC (caller supplies the cohort median for decoupling).
    _roc = _wolf_roc_15m(cached)
    if _roc is not None:
        inp["roc_15m"] = _roc

    # W — anti-chasing VWAP room: 15m VWAP (typical price × volume over the last
    # 3×5m candles) vs the mid (last price). Omitted when volume is all zero.
    try:
        if price and len(k5) >= 3:
            num = 0.0
            den = 0.0
            for c in k5[-3:]:
                if not isinstance(c, dict) or c.get("close") is None:
                    continue
                cl = float(c["close"])
                tp = (float(c.get("high", cl) or cl)
                      + float(c.get("low", cl) or cl) + cl) / 3.0
                v = float(c.get("volume", 0.0) or 0.0)
                num += tp * v
                den += v
            if den > 0:
                inp["vwap_15m"] = num / den
                inp["p_mid"] = float(price)
    except Exception:
        pass

    # V — volume confirmation: last 5m volume vs its 20-bar average.
    try:
        if vols:
            inp["vol_5m"] = vols[-1]
            _avg_src = vols[-20:]
            if _avg_src:
                inp["avg_vol_20"] = sum(_avg_src) / len(_avg_src)
    except Exception:
        pass

    # X — volatility fitness: tent centred on the middle of the tradeable ATR
    # band the engine already gates on (config.ATR_MIN_PCT..ATR_MAX_PCT are
    # fractions; convert to percent to match cached atr_pct).
    try:
        _lo = float(config.ATR_MIN_PCT) * 100.0
        _hi = float(config.ATR_MAX_PCT) * 100.0
        inp["atr_target"] = (_lo + _hi) / 2.0
        inp["atr_halfwidth"] = max((_hi - _lo) / 2.0, 1e-6)
    except Exception:
        pass

    # F — friction (highest-weight term): (half-spread% + recent avg exit
    # slippage%) / planned 1R stop distance%. All three are real engine values.
    try:
        _full, _half = _spread_pcts(sym)
        if _half is not None:
            inp["half_spread_pct"] = _half
    except Exception:
        pass
    try:
        _slip_bps = _avg_slippage_bps(sym)
        if _slip_bps is not None:
            inp["avg_slippage_pct"] = _slip_bps / 100.0
    except Exception:
        pass
    try:
        if price:
            _stop = _planned_sl_distance_pct(sym, float(price))
            if _stop:
                inp["planned_stop_pct"] = _stop
    except Exception:
        pass

    return inp


def _wolf_score_cached(sym: str, cached: dict, cohort: dict, tilt: float) -> Optional[dict]:
    """Guarded WolfScore v3 for a signal-cache entry:
    compute_submetrics → wolfscore. Returns the full decomposition dict
    (pct, submetrics, families, regime, regime_tilt, hard_gate, top_reasons,
    trained, version) or None on any failure / when ev_model is unavailable.
    S3-2 — the up-regime anti-chasing veto/threshold are read from entries config
    and applied inside wolfscore (extended-uptrend coins hard-gate)."""
    if ev_model is None:
        return None
    try:
        _ec = _entries_cfg()
        _veto = bool(_ec.get("up_extension_veto", True))
        _wthr = float(_ec.get("up_extension_w_thr", 0.0) or 0.0)
    except Exception:
        _veto, _wthr = True, 0.0
    try:
        sub = ev_model.compute_submetrics(_wolf_inputs(sym, cached), cohort or {})
        return ev_model.wolfscore(sub, float(tilt or 0.0),
                                  up_extension_veto=_veto, up_extension_w_thr=_wthr)
    except Exception:
        return None


def _wolf_cohort_from(items) -> dict:
    """Cohort context for R (decoupling): median 15m ROC across the given
    (sym, cached) candidates. Empty → {} (R degrades to neutral)."""
    rocs = []
    for _it in items:
        try:
            _c = _it[1]
        except Exception:
            continue
        r = _wolf_roc_15m(_c)
        if r is not None:
            rocs.append(r)
    if not rocs:
        return {}
    s = sorted(rocs)
    m = len(s)
    med = s[m // 2] if m % 2 else (s[m // 2 - 1] + s[m // 2]) / 2.0
    return {"median_roc_15m": med}


def _wolf_adaptive_floor(pcts, abs_floor: float, mode: str, k: float) -> Optional[dict]:
    """ev_model.adaptive_floor with an explicit 'off' short-circuit (only the
    absolute floor applies). None on failure → floor becomes inactive."""
    if ev_model is None:
        return None
    try:
        if str(mode) == "off":
            return {"threshold": round(float(abs_floor), 2),
                    "abs_floor": float(abs_floor),
                    "dist_threshold": round(float(abs_floor), 2),
                    "mode": "off", "n": len(list(pcts))}
        return ev_model.adaptive_floor(list(pcts), abs_floor=float(abs_floor),
                                       mode=str(mode), k=float(k))
    except Exception:
        return None


_ev_scores_cache: Dict[str, Any] = {"ts": 0.0, "data": {}}
_ev_scores_cache_lock = threading.Lock()
_EV_SCORES_TTL_SEC = 5.0  # serve the UI/diagnostics feed from a memo; recompute
                          # at most once per this window (single-flight below).


def get_live_ev_scores() -> dict:
    """S5 data feed for control_api / UI: per currently-tracked symbol, the latest
    WolfScore v3, plus a top-level '__meta__' block. Served from a short-TTL,
    single-flight memo so the /api/ev/scores poll AND the diagnostics bundle never
    each recompute WolfScore over the whole universe on every request — under a
    73-coin universe that per-request O(universe) recompute was starving the box
    and 504-ing the EV + diagnostics endpoints. The lock makes at most ONE compute
    happen per TTL window; concurrent callers reuse the same result."""
    now = time.time()
    with _ev_scores_cache_lock:
        _cached = _ev_scores_cache.get("data")
        if _cached and (now - float(_ev_scores_cache.get("ts", 0.0)) < _EV_SCORES_TTL_SEC):
            return _cached
        data = _compute_live_ev_scores()
        _ev_scores_cache["ts"] = time.time()
        _ev_scores_cache["data"] = data
        return data


def _compute_live_ev_scores() -> dict:
    """Uncached WolfScore-v3 computation over the live signal cache. O(universe);
    guarded so a scoring failure yields an empty/partial map rather than raising.
    Returns {} when ev_model is unavailable. Callers should go through
    get_live_ev_scores() (memoized) — this is the cold path."""
    if ev_model is None:
        return {}
    try:
        with _signal_cache_lock:
            snap = dict(_signal_cache)
    except Exception:
        return {}
    # Score ONLY the active (approved) universe. A coin de-selected from the
    # watchlist can linger in _signal_cache — the refresh loop only ever writes
    # approved coins, it never deletes stale keys. Without this filter that
    # coin's last-computed WolfScore is served to the UI feed forever: it looks
    # frozen (never refreshed) AND shows for a coin the user no longer tracks.
    # purge_symbol_state() on de-select clears it at the source; this is the
    # backstop so a missed purge can never leak a stale score into the feed.
    _universe = _active_universe
    if _universe:
        snap = {s: v for s, v in snap.items() if s in _universe}
    try:
        tilt = ev_model.regime_tilt(_btc_roc_1h_frac())
    except Exception:
        tilt = 0.0
    cohort = _wolf_cohort_from(list(snap.items()))
    out: Dict[str, dict] = {}
    pcts: List[float] = []
    for _sym, _cached in snap.items():
        _ws = _wolf_score_cached(_sym, _cached, cohort, tilt)
        if not _ws:
            continue
        out[_sym] = {
            "pct":         _ws.get("pct"),
            "submetrics":  _ws.get("submetrics"),
            "families":    _ws.get("families"),
            "regime":      _ws.get("regime"),
            "regime_tilt": _ws.get("regime_tilt"),
            "top_reasons": _ws.get("top_reasons", []),
            "trained":     _ws.get("trained"),
            "version":     _ws.get("version"),
            "hard_gate":   _ws.get("hard_gate"),
        }
        if _ws.get("hard_gate") is None and _ws.get("pct") is not None:
            try:
                pcts.append(float(_ws["pct"]))
            except (TypeError, ValueError):
                pass
    try:
        _cfg = _entries_cfg()
        _abs = float(_cfg.get("min_win_probability_floor", 55.0) or 55.0)
        _k = float(_cfg.get("ev_floor_meanstd_k", 0.5) or 0.5)
        _mode = str((_load_strategy().get("entries") or {}).get("ev_floor_mode", "p75") or "p75")
    except Exception:
        _abs, _k, _mode = 55.0, 0.5, "p75"
    _t = float(tilt or 0.0)
    out["__meta__"] = {
        "regime_tilt":    round(_t, 4),
        "regime":         "up" if _t > 0.15 else "down" if _t < -0.15 else "side",
        "adaptive_floor": _wolf_adaptive_floor(pcts, _abs, _mode, _k),
        "floor_active":   _ev_floor_active(),
    }
    return out


def _save_ev_training_sample_safe(symbol: str, pos: dict, realized_r: float) -> None:
    """Write ONE live WolfScore training sample when a position closes (read-only
    on the exit path — the exit decision / timing / price are UNCHANGED; only the
    label payload is written here). Persists the WolfScore SUBMETRICS + regime
    tilt captured at entry so ev_model.train_wolfscore can consume the row:
      features = {"submetrics": {...T,M,R,C,W,V,X,F...}, "regime_tilt": float}
    Fully guarded — save_training_sample is added by a parallel change, so a
    missing attribute or any error is swallowed."""
    if database is None:
        return
    _save = getattr(database, "save_training_sample", None)
    if not callable(_save):
        return
    try:
        _sub = pos.get("ev_submetrics")
        _tilt = pos.get("ev_regime_tilt")
        feats = {
            "submetrics":  _sub if isinstance(_sub, dict) else {},
            "regime_tilt": float(_tilt) if _tilt is not None else 0.0,
        }
        label = 1 if realized_r > 0 else 0
        _save("live", symbol, feats, label, realized_r)
    except Exception:
        pass


def _save_entry_snapshot_safe(symbol: str, origin: str, executed: bool, price,
                              raw: dict, gates: dict) -> None:
    """database.save_entry_snapshot wrapper — snapshots must NEVER break a buy."""
    try:
        strategy = _load_strategy()
        database.save_entry_snapshot(
            symbol, origin=origin, executed=executed, price=price,
            raw=raw or {}, gates=gates or {},
            config_hash=database.config_hash(strategy),
        )
    except Exception as _snap_exc:
        try:
            log.debug("entry snapshot failed for %s: %s", symbol, _snap_exc)
        except Exception:
            pass


def _snapshot_near_miss(symbol: str, reason: str, detail: str, score) -> None:
    """Record a near-miss entry snapshot (executed=False, origin='near_miss').

    A near-miss = a coin that passed the fast pre-check (legacy score >=
    min_signals, or the signal engine is active and evaluated it) but was
    rejected by a later gate. Throttled per (symbol, reason) to 1 per 5 min."""
    try:
        if reason in _NON_NEAR_MISS_REASONS:
            return
        now = time.time()
        key = (symbol, reason)
        if now - _near_miss_snap_ts.get(key, 0.0) < _NEAR_MISS_SNAP_THROTTLE_SEC:
            return
        strategy = _load_strategy()
        min_sigs = int(strategy.get("min_signals", config.MIN_SIGNALS_TO_BUY))
        engine_on = (SIGNAL_REGISTRY_AVAILABLE
                     and bool(strategy.get("signal_engine", {}).get("enabled", False)))
        try:
            _score_i = int(score or 0)
        except Exception:
            _score_i = 0
        if _score_i < min_sigs and not engine_on:
            return  # never passed the fast pre-check — not a near-miss
        _near_miss_snap_ts[key] = now
        with _signal_cache_lock:
            cached = dict(_signal_cache.get(symbol, {}))
        try:
            _btc = get_btc_state()
            _regime = _btc.get("regime") if _btc else None
        except Exception:
            _regime = None
        gates = {
            "rejected":       True,
            "reason":         reason,
            "detail":         str(detail)[:300] if detail else "",
            "trading_active": bool(strategy.get("trading_active", True)),
            "regime":         _regime,
            "bb_ok":          cached.get("bb_ok"),
            "5m_ok":          cached.get("5m_ok"),
            "score":          _score_i,
            "min_signals":    min_sigs,
            "signal_engine":  engine_on,
        }
        _save_entry_snapshot_safe(
            symbol, origin="near_miss", executed=False,
            price=cached.get("price"),
            raw=_snapshot_raw_from_cache(cached, cached.get("price")),
            gates=gates,
        )
    except Exception:
        pass  # near-miss telemetry must never affect the scan loop


# ── Phase 1 §1.3 — exit labels ────────────────────────────────────────────────
# Canonical exit-label mapping for trades.exit_label. The recycler gets its OWN
# label — it was an invisible loss channel when lumped in with force sells.
_EXIT_LABEL_MAP = {
    "take-profit":     "tp",
    "maker-tp":        "tp",
    "oco-tp":          "tp",
    "oco-sl":          "sl",
    "stop-loss":       "sl",
    "hard-stop-loss":  "hard_sl",
    "trail":           "trail",
    "smart-hold-trail": "trail",
    "breakeven-stop":  "breakeven",   # F1: faded back to BEP after the BE-move armed
    "profit-ratchet":  "ratchet",     # P2: ATR profit-ratchet trailing stop
    "auto-recycle":    "recycler",
    "force-sell":      "force",
    "manual":          "manual",
    "user-initiated":  "manual",
    "slippage-loss":   "slippage",
    "below-breakeven": "below_bep",
    "ghost":           "force",
    "delist":          "delist",
}


def _exit_label_for(reason: str) -> str:
    """Map a sell-reason string to its canonical exit label (fallback: raw reason)."""
    r = str(reason or "").strip().lower()
    if r in _EXIT_LABEL_MAP:
        return _EXIT_LABEL_MAP[r]
    if "recycle" in r:
        return "recycler"
    if "delist" in r or "-1013" in r or "market closed" in r or "market is closed" in r:
        return "delist"
    if "ghost" in r:
        return "force"
    if "force" in r:  # e.g. "force-sell-failed: ..."
        return "force"
    if "manual" in r or "user" in r:
        return "manual"
    return r or "unknown"


# WebSocket price freshness — updated by data_collector on every @trade or @miniTicker event.
# Used by the pre-sell check to decide whether a REST re-fetch is needed.
_last_ws_price_ts: Dict[str, float] = {}

# ── Binance REST health — updated by _fetch_rest_prices (zero extra calls) ────
_binance_health: Dict = {
    "last_rest_ok_ts":      0.0,
    "last_rest_latency_ms": 0.0,
    "used_weight_1m":       0,
    "used_weight_pct":      0.0,
    "rest_error_count":     0,
    "last_error_ts":        0.0,
    "last_error_msg":       "",
    # Claude API counters (separate from Binance REST errors)
    "claude_error_count":   0,
    "claude_last_error_ts": 0.0,
    "claude_last_error_msg": "",
    "claude_disabled_until": 0.0,
}
_binance_health_lock = threading.Lock()

# ── Circuit breaker — prevents runaway 400s from burning Binance weight ───────
_consecutive_400_count: int = 0
_circuit_breaker_until: float = 0.0

# ── Per-source exponential backoff — dampens timeout cascades ─────────────────
# Keyed by the `source` tag passed to _binance_request (e.g. "batch_prices",
# "btc_klines"). On consecutive failures the caller is skipped for an
# increasing window: 2s → 4 → 8 → 16 → 32 → 60s (cap). First success resets.
_rest_backoff: dict = {}
_REST_BACKOFF_BASE_SEC = 2.0
_REST_BACKOFF_MAX_SEC  = 60.0
_REST_BACKOFF_LOCK     = threading.Lock()


def _backoff_should_skip(source: str) -> bool:
    with _REST_BACKOFF_LOCK:
        s = _rest_backoff.get(source)
        return bool(s and s["failures"] > 0 and time.time() < s["next_retry_ts"])


def _backoff_record_failure(source: str) -> None:
    with _REST_BACKOFF_LOCK:
        s = _rest_backoff.setdefault(source, {"failures": 0, "next_retry_ts": 0.0})
        s["failures"] += 1
        delay = min(_REST_BACKOFF_BASE_SEC * (2 ** (s["failures"] - 1)), _REST_BACKOFF_MAX_SEC)
        s["next_retry_ts"] = time.time() + delay


def _backoff_record_success(source: str) -> None:
    with _REST_BACKOFF_LOCK:
        if source in _rest_backoff:
            _rest_backoff[source] = {"failures": 0, "next_retry_ts": 0.0}


def _check_circuit_breaker() -> bool:
    return time.time() < _circuit_breaker_until


def _trip_circuit_breaker() -> None:
    global _consecutive_400_count, _circuit_breaker_until
    _consecutive_400_count += 1
    if _consecutive_400_count >= 50:
        _circuit_breaker_until = time.time() + 60.0
        _consecutive_400_count = 0
        try:
            log_diag_issue(
                "binance", "warn",
                "Circuit breaker tripped — pausing REST 60s after 50 consecutive 400s",
            )
        except Exception:
            pass


def _reset_circuit_breaker() -> None:
    global _consecutive_400_count
    _consecutive_400_count = 0


def _binance_request(url: str, timeout: float = 3.0, source: str = "unknown",
                     weight: int = 1, critical: bool = False):
    """Unified Binance REST caller that captures FULL error context.

    Returns (success, data, headers, latency_ms).
    On failure records the exact URL + Binance JSON body in the diag log so we
    can see which call site fails and what Binance is actually complaining about.

    C §6.4: every call is gated by binance_limits.can_spend(weight, critical)
    (no-op allow when the module is absent), response headers are routed into
    binance_limits.record_response_headers, 429 → on_429 + existing per-source
    backoff, 418 → on_418 + immediate circuit-break (IP ban — stop everything).
    """
    import urllib.request as _ur_b
    import urllib.error as _ue_b
    import traceback as _tb_b
    global _circuit_breaker_until

    if _check_circuit_breaker():
        return (False, None, {}, 0.0)
    if _backoff_should_skip(source):
        return (False, None, {}, 0.0)
    if not _limits_can_spend(weight, critical):
        # Budget refused for a non-critical background call — skip gracefully;
        # callers already handle (False, ...) via their cache/WS/DB fallbacks.
        return (False, None, {}, 0.0)

    t0 = time.time()
    try:
        req = _ur_b.Request(url, headers={"User-Agent": "WolfBot/1.0"})
        with _ur_b.urlopen(req, timeout=timeout) as r:
            body = r.read()
            hdrs = dict(r.headers)
        latency_ms = (time.time() - t0) * 1000
        try:
            _record_rest_health(hdrs, latency_ms)
        except Exception:
            pass
        _limits_record_headers(hdrs)
        _reset_circuit_breaker()
        _backoff_record_success(source)
        return (True, json.loads(body), hdrs, latency_ms)
    except _ue_b.HTTPError as he:
        latency_ms = (time.time() - t0) * 1000
        try:
            body_text = he.read().decode("utf-8", errors="replace")[:600]
        except Exception:
            body_text = "<unreadable>"
        try:
            err_hdrs = dict(he.headers) if he.headers else {}
            _record_rest_health(err_hdrs, latency_ms)
        except Exception:
            err_hdrs = {}
        _limits_record_headers(err_hdrs)
        if he.code == 400:
            _trip_circuit_breaker()
        elif he.code == 429:
            # Rate-limited: inform the limits module + existing per-source backoff.
            _limits_on_429(err_hdrs)
            _backoff_record_failure(source)
        elif he.code == 418:
            # IP auto-ban: inform limits module and circuit-break ALL REST for
            # the Retry-After window (min 120s) — continuing would extend the ban.
            _limits_on_418(err_hdrs)
            _circuit_breaker_until = max(
                _circuit_breaker_until,
                time.time() + _retry_after_sec(err_hdrs, 120.0),
            )
            try:
                log_diag_issue(
                    "binance", "error",
                    "HTTP 418 (IP ban) — circuit-breaking all REST calls",
                    detail=f"source={source} retry_after={_retry_after_sec(err_hdrs, 120.0):.0f}s",
                )
            except Exception:
                pass
        try:
            _record_rest_error(
                f"[{source}] HTTP {he.code}: {he.reason}",
                url=url, response_body=body_text,
            )
        except TypeError:
            try:
                _record_rest_error(f"[{source}] HTTP {he.code} | {url[:200]} | {body_text[:200]}")
            except Exception:
                pass
        return (False, None, {}, latency_ms)
    except Exception as e:
        latency_ms = (time.time() - t0) * 1000
        tb = _tb_b.format_exc()[-400:]
        _backoff_record_failure(source)
        try:
            _record_rest_error(f"[{source}] {type(e).__name__}: {e}", url=url, response_body=tb)
        except TypeError:
            try:
                _record_rest_error(f"[{source}] {type(e).__name__}: {e} | {url[:200]}")
            except Exception:
                pass
        return (False, None, {}, latency_ms)


# ── In-memory diagnostic ring buffer — last 100 issues, never written to DB ───
from collections import deque as _deque

_diag_log: "_deque[dict]" = _deque(maxlen=100)
_diag_log_lock = threading.Lock()

# ── F1 exit-R analytics — last 100 closed trades' realized_r vs planned_rr ─────
# The trades table has no realized_r/planned_rr columns (fixed-column INSERT), so
# the R stats live here (in-memory ring) + the activity log. get_exit_r_stats()
# exposes a summary for the analytics API (control_api adds the endpoint).
_exit_r_stats: "_deque[dict]" = _deque(maxlen=100)
_exit_r_stats_lock = threading.Lock()


def _record_exit_r(sym: str, entry: float, exit_px: float,
                   sl_dist_pct, planned_rr, label: str,
                   pos: Optional[dict] = None) -> None:
    """Compute signed realized_r for a close and log/store it (F1.1).

    realized_r = (exit_px − entry) / (entry × sl_distance_pct/100) — how many
    R-multiples the exit landed at, signed. Losers cap near −1R (stop), a
    BEP scratch is ~0R, a clean TP is ≈ planned_rr, a trailed runner ≥ that.

    WolfBot v0.5 Part S2 — READ-ONLY EV training hook: when `pos` is provided and
    realized_r is computable, write ONE live training sample (features + win/loss
    label + realized_r). This is purely additive at the existing close point — it
    does NOT read/alter the exit decision, label, price, or timing; a failure is
    swallowed so it can never affect the close."""
    try:
        entry = float(entry or 0)
        exit_px = float(exit_px or 0)
        sl_dist = float(sl_dist_pct) if sl_dist_pct else 0.0
        realized_r = None
        if entry > 0 and sl_dist > 0:
            realized_r = (exit_px - entry) / (entry * sl_dist / 100.0)
        # ── Part S2 — live training label (guarded, additive, non-authoritative)
        if pos is not None and realized_r is not None:
            _save_ev_training_sample_safe(sym, pos, realized_r)
        prr = None
        try:
            prr = float(planned_rr) if planned_rr is not None else None
        except (TypeError, ValueError):
            prr = None
        rec = {
            "symbol": sym, "label": label,
            "entry": entry, "exit_px": exit_px,
            "sl_distance_pct": sl_dist or None,
            "realized_r": (round(realized_r, 4) if realized_r is not None else None),
            "planned_rr": prr, "ts": time.time(),
        }
        with _exit_r_stats_lock:
            _exit_r_stats.append(rec)
        try:
            database.log_activity(
                f"EXIT {sym} label={label} "
                f"planned_rr={('%.2f' % prr) if prr is not None else 'na'} "
                f"realized_r={('%.2f' % realized_r) if realized_r is not None else 'na'} "
                f"planned_tp={exit_px if realized_r is None else round(entry * (1 + (prr or 0) * sl_dist / 100.0), 6)} "
                f"exit_px={exit_px:.6f}", "info")
        except Exception:
            pass
    except Exception:
        pass


def get_exit_r_stats(since_ts: "float | None" = None) -> dict:
    """F1 analytics accessor: rolling stats over the last (≤100) closed trades'
    realized_r vs planned_rr. Shape:
      {n, avg_realized_r, median_realized_r, avg_planned_rr, win_rate,
       by_label:{label:{n, avg_realized_r}}, samples:[last 20 records]}.
    win_rate here = fraction with realized_r > 0. Empty-safe.

    I3 — range-scoping: when `since_ts` (a UNIX timestamp) is provided, the ring
    is filtered to entries with ts >= since_ts BEFORE computing every field
    (n/avgs/median/win_rate/by_label/samples), so control_api can pass
    process_start for a since-deploy view. Each ring entry carries a `ts`
    (set in _record_exit_r). The no-arg call is unchanged (engine-lifetime)."""
    with _exit_r_stats_lock:
        _snap = list(_exit_r_stats)
    if since_ts is not None:
        try:
            _cut = float(since_ts)
            _snap = [r for r in _snap if float(r.get("ts", 0) or 0) >= _cut]
        except (TypeError, ValueError):
            pass
    rows = [r for r in _snap if r.get("realized_r") is not None]
    all_rows = _snap
    if not rows:
        return {"n": 0, "avg_realized_r": None, "median_realized_r": None,
                "avg_planned_rr": None, "win_rate": None, "by_label": {},
                "samples": all_rows[-20:]}
    rs = sorted(r["realized_r"] for r in rows)
    n = len(rs)
    mid = n // 2
    median = rs[mid] if n % 2 else (rs[mid - 1] + rs[mid]) / 2.0
    prrs = [r["planned_rr"] for r in rows if r.get("planned_rr") is not None]
    by_label: dict = {}
    for r in rows:
        b = by_label.setdefault(r["label"], {"n": 0, "_sum": 0.0})
        b["n"] += 1
        b["_sum"] += r["realized_r"]
    for b in by_label.values():
        b["avg_realized_r"] = round(b.pop("_sum") / b["n"], 4)
    return {
        "n": n,
        "avg_realized_r": round(sum(rs) / n, 4),
        "median_realized_r": round(median, 4),
        "avg_planned_rr": (round(sum(prrs) / len(prrs), 4) if prrs else None),
        "win_rate": round(sum(1 for x in rs if x > 0) / n, 4),
        "by_label": by_label,
        "samples": all_rows[-20:],
    }


def log_diag_issue(source: str, severity: str, message: str, detail: str = "") -> None:
    """Append an entry to the in-memory diagnostic ring buffer.

    source:   'binance' | 'websocket' | 'signal_scanner' | 'sell_monitor' |
              'price_refresher' | 'force_sell' | 'startup' | 'system'
    severity: 'error' | 'warn' | 'info'
    """
    try:
        entry = {
            "ts":       time.time(),
            "iso":      datetime.now(timezone.utc).isoformat(),
            "source":   source,
            "severity": severity,
            "message":  str(message)[:500],
            "detail":   str(detail)[:1000] if detail else "",
        }
        with _diag_log_lock:
            _diag_log.append(entry)
    except Exception:
        pass


def get_diag_log(limit: int = 50, since_ts: float = 0.0, severity_filter: str = "") -> list:
    """Return entries from the ring buffer, newest first."""
    try:
        with _diag_log_lock:
            entries = list(_diag_log)
        entries.reverse()
        if since_ts:
            entries = [e for e in entries if e["ts"] > since_ts]
        if severity_filter:
            entries = [e for e in entries if e["severity"] == severity_filter]
        return entries[:limit]
    except Exception:
        return []


def clear_diag_log() -> int:
    """Clear the ring buffer. Returns count cleared."""
    try:
        with _diag_log_lock:
            n = len(_diag_log)
            _diag_log.clear()
        return n
    except Exception:
        return 0


def _record_rest_health(response_headers: dict, latency_ms: float):
    """Record health metrics from a Binance REST response. Zero cost — headers are free."""
    try:
        with _binance_health_lock:
            _binance_health["last_rest_ok_ts"]      = time.time()
            _binance_health["last_rest_latency_ms"] = round(latency_ms, 1)
            w = (response_headers.get("x-mbx-used-weight-1m")
                 or response_headers.get("X-MBX-USED-WEIGHT-1M"))
            if w:
                used = int(w)
                _binance_health["used_weight_1m"]  = used
                _binance_health["used_weight_pct"] = round(used / 6000.0 * 100.0, 1)
    except Exception:
        pass


_last_binance_err_log_ts: Dict[str, float] = {}
_BINANCE_ERR_LOG_THROTTLE_SEC = 30.0


def _record_rest_error(err_msg: str, url: str = "", response_body: str = "") -> None:
    try:
        with _binance_health_lock:
            _binance_health["rest_error_count"] += 1
            _binance_health["last_error_ts"]     = time.time()
            _binance_health["last_error_msg"]    = str(err_msg)[:200]
        # Throttle ring-buffer writes — same error type at most once per 30s
        err_key = str(err_msg)[:80]
        now = time.time()
        if now - _last_binance_err_log_ts.get(err_key, 0) >= _BINANCE_ERR_LOG_THROTTLE_SEC:
            _last_binance_err_log_ts[err_key] = now
            detail_parts = []
            if url:
                detail_parts.append(f"url={url[:300]}")
            if response_body:
                detail_parts.append(f"response={response_body[:300]}")
            log_diag_issue(
                "binance", "error",
                f"REST: {str(err_msg)[:150]} (×{_binance_health['rest_error_count']} since start)",
                detail=" | ".join(detail_parts) if detail_parts else "",
            )
    except Exception:
        pass


# ── Claude API error tracking (separate from Binance REST errors) ─────────────
_claude_consecutive_401s: int = 0
_CLAUDE_DISABLE_SECS = 86400  # 24 h after 3 consecutive 401s


def _record_claude_error(err_msg: str, is_auth_error: bool = False) -> None:
    """Record a Claude API error. After 3 consecutive auth failures, disable Claude for 24 h."""
    global _claude_consecutive_401s
    try:
        now = time.time()
        with _binance_health_lock:
            _binance_health["claude_error_count"]   += 1
            _binance_health["claude_last_error_ts"]  = now
            _binance_health["claude_last_error_msg"] = str(err_msg)[:200]

        if is_auth_error:
            _claude_consecutive_401s += 1
            if _claude_consecutive_401s >= 3:
                with _binance_health_lock:
                    _binance_health["claude_disabled_until"] = now + _CLAUDE_DISABLE_SECS
                log_diag_issue(
                    "claude", "error",
                    f"Claude API disabled for 24h — 3 consecutive auth failures",
                    detail=str(err_msg)[:300],
                )
            else:
                log_diag_issue(
                    "claude", "error",
                    f"Claude API auth error ({_claude_consecutive_401s}/3 before disable)",
                    detail=str(err_msg)[:300],
                )
        else:
            _claude_consecutive_401s = 0  # reset streak on non-auth errors
            log_diag_issue(
                "claude", "error",
                f"Claude API error (×{_binance_health['claude_error_count']} since start)",
                detail=str(err_msg)[:300],
            )
    except Exception:
        pass


def is_claude_disabled() -> bool:
    """Return True if Claude has been rate-disabled due to repeated auth failures."""
    with _binance_health_lock:
        return time.time() < _binance_health["claude_disabled_until"]


def reset_claude_errors() -> None:
    """Reset Claude error counters and re-enable Claude (called after key is fixed)."""
    global _claude_consecutive_401s
    _claude_consecutive_401s = 0
    with _binance_health_lock:
        _binance_health["claude_error_count"]    = 0
        _binance_health["claude_last_error_ts"]  = 0.0
        _binance_health["claude_last_error_msg"] = ""
        _binance_health["claude_disabled_until"] = 0.0


# Per-stage scan timing (diagnosis instrumentation). Updated each buy-check pass;
# surfaced in _signal_scanner_health["stage_ms"] and /api/diagnostics/entry-report.
_scan_stage_ms: Dict[str, Any] = {
    "wolfscore_ms": 0.0, "gate_loop_ms": 0.0, "buy_check_ms": 0.0,
    "n_scored": 0, "updated_ts": 0.0,
}


# ── Signal scanner health ──────────────────────────────────────────────────────
_signal_scanner_health: Dict = {
    "last_refresh_ts":  0.0,
    "last_duration_ms": 0.0,
    "scans_completed":  0,
    "interval_sec":     float(30),  # will be updated at runtime from config.SCAN_INTERVAL_SEC
    # C §6.1 scan stabilizers
    "scan_skipped_overlap":   0,      # refreshes skipped because one was already in flight
    "effective_interval_sec": float(30),  # adaptive sleep (>= SCAN_INTERVAL_SEC, <= 120)
    "universe_size":          0,      # symbols in the last scan pass
    # C §6.2/§6.5 — WS-first signal engine
    "mode":                  "ws-first",  # "ws-first" | "legacy" (strategy data.legacy_rest_scan)
    "stale_signal_count":    0,           # symbols whose cache entry is older than 180s
    "last_event_refresh_ts": 0.0,         # last kline-close (event-driven) full cache rebuild
    "db_fallback_refreshes": 0,           # thin-WS-buffer symbols refreshed from DB (no REST)
}

# C §6.2d — cache freshness: entries older than this are "stale" (~1 candle + margin)
_STALE_SIGNAL_SEC = 180.0

# Active symbol universe (approved coins) — set by both scanner modes; used by
# stale_signal_syms() so freshness is judged against symbols we SHOULD track.
_active_universe: set = set()

# One-per-change mode logging ("ws-first" vs "legacy")
_last_logged_scan_mode: str = ""

# C §6.3 — held-position exit watchdog state
_held_max_price_age_sec: float = 0.0          # freshest-price age of the worst held symbol
_watchdog_fire_ts: "_deque[float]" = _deque(maxlen=50000)  # REST-fire timestamps (24h window)
_watchdog_lock = threading.Lock()
_WATCHDOG_STALE_SEC = 3.0                     # any held age above this → ONE batched REST fetch
_WATCHDOG_ALERT_SEC = 5.0                     # any held age above this → price_feed diag error
_watchdog_alert_log_ts: float = 0.0           # 60s throttle for the price_feed alert

# C §6.1a — single-flight guard: only ONE _refresh_signal_cache may run at a
# time. Overlapping invocations are SKIPPED (never queued) via
# acquire(blocking=False); a second concurrent refresh must never stack up.
_signal_refresh_inflight = threading.Lock()

# C §6.1c — hard budgets for a refresh pass
_SCAN_SYMBOL_FETCH_TIMEOUT_SEC = 5.0    # per-symbol REST fetch ceiling (one retry)
_SCAN_PASS_BUDGET_SEC          = 120.0  # whole-pass ceiling, checked at batch boundaries

# C §6.1d — low_24h cache: {sym: (value, ts)}; reused within 10 minutes so the
# 1440-row DB read (heaviest per-scan cost) runs at most once per symbol per window.
_low24h_cache: Dict[str, Tuple[Optional[float], float]] = {}
_LOW24H_CACHE_TTL_SEC = 600.0

# Throttle for the "universe is empty" warning (once per 10 min, not every pass)
_last_empty_universe_warn_ts: float = 0.0

# New exit-mode flags (strategy.json controlled)
_take_profit_enabled: bool = True    # False → exit at breakeven (fees covered) only
_smart_hold_enabled:  bool = False   # True  → hold if signals still bullish; trail then exit
_trailing_stop_pct:   float = 0.5    # % drop from peak that triggers smart-hold exit

# Per-position high-water mark for smart hold (no lock needed — only written in sell thread)
_pos_peaks: Dict[str, float] = {}

_cooldowns: dict = {}

# ── Position index — rebuilt on every WebSocket tick for O(1) sell lookups ───
_pos_by_symbol: Dict[str, dict] = {}

# ── In-progress sell guard — prevents double-sells from monitor + guardian ────
_selling: set = set()
_selling_lock = threading.Lock()
_selling_ts: Dict[str, float] = {}   # when each sym was added — for watchdog

# ── In-progress BUY guard — mirrors _selling pattern to prevent double-buys ──
# Without this, two concurrent buy paths (scanner iteration overlap, force-buy
# racing scheduled buy, retry-after-failure) can fire two buys for the same
# symbol within milliseconds. Confirmed by duplicate DOT and IMX entries in DB.
_buying: set = set()
_buying_lock = threading.Lock()
_buying_ts: Dict[str, float] = {}
_BUYING_TIMEOUT_SEC = 30.0   # auto-release if a buy attempt crashes silently

# ── Bad-symbol blacklist — populated when Binance returns -1013 (market closed)
# Prevents repeated buy attempts and rate-limits sell retries on closed markets.
_bad_symbols: set = set()
_sell_last_failed_ts: Dict[str, float] = {}  # last sell failure time per symbol
_sell_last_failed_reason: Dict[str, str] = {}  # reason that triggered the failed sell
_ghost_check_fails: Dict[str, int] = {}  # consecutive -2010 check_failed count per symbol
_SELL_RETRY_COOLDOWN_PROFIT = 0.5   # take-profit: 0.5s breaks retry loop without delaying exit
_SELL_RETRY_COOLDOWN_LOSS   = 0.0   # stop-loss / force-sell: retry immediately

# ── H1 fix 3: cached live account snapshot ───────────────────────────────────
# The pre-sell balance clamp used to call _acct() (a signed REST GET) on EVERY
# sell attempt. When a sell is stuck retrying every ~0.75s that hammered the
# signed-API budget. _cached_acct() serves a snapshot at most _ACCT_CACHE_TTL
# old; a cancel/fill event forces a refresh via _invalidate_acct_cache().
_acct_cache_lock = threading.Lock()
_acct_cache: dict = {"ts": 0.0, "acct": None}
_ACCT_CACHE_TTL = 5.0   # seconds


def _cached_acct(force: bool = False) -> dict:
    """Live account snapshot, cached for _ACCT_CACHE_TTL to stop the stuck-sell
    loop from calling the signed account endpoint every retry. force=True (used
    right after a cancel that releases locked balance) fetches fresh."""
    now = time.time()
    with _acct_cache_lock:
        ent = _acct_cache
        if (not force and ent["acct"] is not None
                and (now - ent["ts"]) < _ACCT_CACHE_TTL):
            return ent["acct"]
    acct = _acct()
    with _acct_cache_lock:
        _acct_cache["ts"] = time.time()
        _acct_cache["acct"] = acct
    return acct


def _invalidate_acct_cache() -> None:
    """Drop the cached account snapshot so the next _cached_acct() refetches
    (call after any cancel/fill that changes free/locked balances)."""
    with _acct_cache_lock:
        _acct_cache["ts"] = 0.0
        _acct_cache["acct"] = None


def _asset_free_locked(acct: dict, asset: str):
    """(free, locked) floats for `asset` from an account payload, or (None,None)."""
    for b in acct.get("balances", []):
        if b.get("asset") == asset:
            try:
                return float(b.get("free") or 0.0), float(b.get("locked") or 0.0)
            except (TypeError, ValueError):
                return None, None
    return None, None


# ── H1 fix 3: stuck-position escalation ──────────────────────────────────────
# A sell that keeps skipping (e.g. below_min_qty from our own locked maker-TP,
# or a repeated -2010) leaves the position UNPROTECTED while the loop spins.
# Track consecutive skips per symbol; >10 within 60s escalates to a CRITICAL
# log + diag issue and is surfaced via get_stuck_positions() so the UI can show
# a persistent "position stuck — manual action may be required" banner.
_stuck_lock = threading.Lock()
_sell_skip_track: Dict[str, dict] = {}   # sym -> {count, first_ts, last_ts, reason}
_stuck_positions: Dict[str, dict] = {}   # sym -> {skips, since_ts, reason}
_STUCK_SKIP_THRESHOLD = 10               # skips within the window before escalation
_STUCK_SKIP_WINDOW_SEC = 60.0


def _note_sell_skip(sym: str, reason: str) -> None:
    """Record one consecutive sell skip for `sym`. Escalates to the stuck
    registry + a CRITICAL log once >_STUCK_SKIP_THRESHOLD skips land inside a
    rolling _STUCK_SKIP_WINDOW_SEC window."""
    now = time.time()
    with _stuck_lock:
        ent = _sell_skip_track.get(sym)
        if ent is None or (now - ent["first_ts"]) > _STUCK_SKIP_WINDOW_SEC:
            ent = {"count": 0, "first_ts": now, "last_ts": now, "reason": reason}
            _sell_skip_track[sym] = ent
        ent["count"] += 1
        ent["last_ts"] = now
        ent["reason"] = reason
        escalate = ent["count"] > _STUCK_SKIP_THRESHOLD
        if escalate:
            first_stuck = sym not in _stuck_positions
            _stuck_positions[sym] = {
                "skips": ent["count"],
                "since_ts": ent["first_ts"],
                "reason": reason,
            }
    if escalate:
        try:
            database.log_activity(
                f"[SELL_STUCK] CRITICAL {sym}: sell skipped {ent['count']}x in "
                f"<{_STUCK_SKIP_WINDOW_SEC:.0f}s (reason={reason}) — position may be "
                f"UNPROTECTED; manual action may be required", "error")
        except Exception:
            pass
        if first_stuck:
            log_diag_issue("sell_stuck", "error",
                           f"{sym}: sell stuck ({ent['count']} skips, reason={reason})",
                           detail=f"consecutive sell skips within {_STUCK_SKIP_WINDOW_SEC:.0f}s")


def _clear_sell_skip(sym: str) -> None:
    """Reset the stuck-skip counter for `sym` — call on any sell progress
    (cancel released balance, order placed, position finalized/closed)."""
    with _stuck_lock:
        _sell_skip_track.pop(sym, None)
        _stuck_positions.pop(sym, None)


def get_stuck_positions() -> Dict[str, dict]:
    """Symbols whose local sell is stuck (repeated skips) — for control_api/UI.

    Returns {sym: {'skips': int, 'since_ts': float, 'reason': str}} (a copy).
    A symbol appears once it has skipped >10 times inside a 60s window and is
    removed on the first successful sell progress (_clear_sell_skip)."""
    with _stuck_lock:
        return {s: dict(e) for s, e in _stuck_positions.items()}

# ── Sell executor — parallel sells so 10 simultaneous exits never queue up ───
# Each position gets its own worker thread; _selling guard prevents duplicates.
_sell_executor = ThreadPoolExecutor(max_workers=12, thread_name_prefix="sell-worker")

# ── Buy-check executor — keeps REST calls off the event loop thread ───────────
# _check_buys_from_cache calls _get_usdt_balance() which is a blocking Binance
# REST call in live mode. Running it inline on the WS event loop starves sell
# triggers during WS reconnect bursts (observed: 21s trigger-to-noticed gap).
# 2 workers is plenty — buy checks are throttled to 0.1s internally.
_buy_check_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="buy-check")
_buy_check_in_flight: bool = False
_buy_check_lock = threading.Lock()

# ── Real-time signal cache — updated on every kline close ────────────────────
_signal_cache: Dict[str, dict] = {}
_signal_cache_lock = threading.Lock()
_last_buy_check: float = 0.0
_last_no_signal_log: float = 0.0   # throttle "no coins ready" log to once per 60 s
_last_buy_scan_log: float = 0.0    # throttle "Buy scan: ..." to once per 30 s
_last_at_capacity_log: float = 0.0 # throttle "at max capacity" log to once per 60 s
_last_fallback_block_log: float = 0.0  # throttle "paper fallback — buys blocked" to once per 60 s
_last_paused_log: float = 0.0          # throttle "bot is paused" warn to once per 60 s
_last_fresh_nodata_log: float = 0.0    # throttle "fresh re-check has no data" warn to once per 60 s
_last_reversal_nodata_log: float = 0.0 # throttle "reversal check has no data" warn to once per 60 s
_warned_5m_warmup: bool = False        # one-shot "5m veto neutral during warmup" startup log

# Buy stagger — prevents mass simultaneous buys on stale cache signals
_last_buy_ts: float = 0.0
_buys_this_scan: int = 0
_BUY_STAGGER_SEC  = 15.0  # minimum seconds between consecutive buys
_MAX_BUYS_PER_SCAN = 2    # max buys per scan cycle

# Per-coin timestamp of last inline tick-driven signal refresh
_tick_signal_ts: Dict[str, float] = {}
_TICK_REFRESH_SEC = 1.0   # recompute at most every 1 s per coin from price ticks


# ── Balance guard + budget helpers ──────────────────────────────────────────────

def can_execute_buy(coin_cfg: dict, client) -> tuple[bool, str]:
    """
    Final pre-buy safety guard.
    Reads the shared _positions list directly (never a copy) so that every
    position appended since bot start is visible here.
    """
    budget = coin_cfg.get("budget_usdt", config.BUDGET_FIXED_USDT)
    try:
        # Geo-block-safe in live mode; param `client` kept for signature compat.
        account = _acct()
        balances = {b["asset"]: float(b["free"]) for b in account["balances"]}
        usdt_free = balances.get("USDT", 0.0)
    except Exception as e:
        return False, f"Cannot fetch account: {e}"

    if usdt_free < budget * 1.002:
        return False, f"Insufficient USDT: have {usdt_free:.2f}, need {budget * 1.002:.2f}"

    sym = coin_cfg["symbol"]
    max_concurrent = int(coin_cfg.get("max_concurrent", 1))

    # Read the single shared mutable list — never a copy — so appends from
    # earlier in the same scan loop are always visible here.
    with _positions_lock:
        open_this_coin = [p for p in _positions if p["symbol"] == sym]

    if len(open_this_coin) >= max_concurrent:
        return False, f"Max concurrent for {sym} reached ({max_concurrent})"

    return True, ""


def get_budget_for_coin(symbol: str, free_usdt: float) -> float:
    """Trade size in USDT (see _get_budget_for_coin_base) with §3.3/M1.2 regime
    sizing applied. In NEUTRAL regime the budget is multiplied by
    strategy.regime.neutral_size_mult ONLY when the resolved neutral_scaling_mode
    is "size" (legacy). "slots"/"off" keep the FULL ticket — neutral risk is then
    reduced by capping concurrent new entries + doubling pacing (see
    _check_buys_from_cache), never by shrinking the ticket below the tradeable
    minimum (the $5.50<$10 bug). risk_off never reaches sizing (vetoed upstream
    by the REGIME_risk_off veto); risk_on is unscaled. Identical live + paper."""
    try:
        return _resolve_entry_budget(symbol, free_usdt)["resolved"]
    except Exception:
        # Sizing must never fail a buy because regime/slot data is missing.
        return _get_budget_for_coin_base(symbol, free_usdt)


def _get_budget_for_coin_base(symbol: str, free_usdt: float) -> float:
    """Return trade size in USDT based on BUDGET_MODE (config defaults or strategy.json overrides).

    If bot_allocation_usdt > 0, the bot is restricted to that USDT cap across all
    concurrent positions — works identically in paper and live mode. The
    "effective free" balance for budget math becomes:
        min(free_usdt, allocation - sum_of_open_position_usdt)
    """
    strategy = _load_strategy()
    mode = strategy.get("budget_mode", config.BUDGET_MODE)
    reinvest = bool(strategy.get("reinvest_profits", False))
    allocation = float(strategy.get("bot_allocation_usdt", config.BOT_ALLOCATION_USDT))

    # Apply allocation cap: subtract value already locked in open positions.
    if allocation > 0:
        with _positions_lock:
            in_positions = sum(p.get("budget_usdt", 0.0) for p in _positions)
        effective_free = max(0.0, min(free_usdt, allocation - in_positions))
    else:
        effective_free = free_usdt

    # Authoritative starting balance: DB setting (written at wallet reset) takes priority
    # over strategy.json so a stale initial_balance_usdt never inflates reinvest scaling.
    initial_from_strategy = float(strategy.get("initial_balance_usdt", 0))
    if initial_from_strategy > 0:
        initial = initial_from_strategy
    else:
        starting_str = database.get_setting("paper_starting_balance")
        initial = float(starting_str) if starting_str else free_usdt

    if mode == "fixed":
        base = float(strategy.get("budget_fixed_usdt", config.BUDGET_FIXED_USDT))

    elif mode == "percent":
        pct = float(strategy.get("budget_pct_of_free", config.BUDGET_PCT_OF_FREE))
        # percent mode already scales with balance — reinvest has no extra effect
        return round(min(effective_free * (pct / 100), effective_free * 0.9), 2)

    elif mode == "capped":
        cap = float(strategy.get("budget_total_cap_usdt", config.BUDGET_TOTAL_CAP_USDT))
        with _positions_lock:
            total_in_positions = sum(p["budget_usdt"] for p in _positions)
        remaining_cap = cap - total_in_positions
        per_trade = cap / config.MAX_OPEN_POSITIONS
        base = min(per_trade, max(0.0, remaining_cap))

    elif mode == "per_coin":
        per_coin = strategy.get("budget_per_coin", config.BUDGET_PER_COIN)
        base = float(per_coin.get(symbol, config.BUDGET_FIXED_USDT))

    elif mode == "coin_pct":
        coin_pct = strategy.get("budget_coin_pct", {})
        pct = float(coin_pct.get(symbol, 5.0))
        # coin_pct already scales with balance — no extra reinvest scaling needed
        return round(min(effective_free * (pct / 100), effective_free * 0.9), 2)

    else:
        base = config.BUDGET_FIXED_USDT

    # Reinvest profits: scale budget proportionally to balance growth.
    # e.g. started 10 000 USDT, now have 12 000 → budget × 1.2 (20% more per trade).
    # Clamped to [0.5, 2.0] — tighter ceiling prevents runaway budget on small initial values.
    if reinvest and initial > 0 and mode in ("fixed", "per_coin", "capped"):
        scale = max(0.5, min(2.0, free_usdt / initial))
        base = base * scale

    # In fixed/per_coin mode return the full configured amount (when no allocation
    # cap is set) — can_execute_buy will reject the buy if free USDT is insufficient,
    # giving a clean "have X, need Y" message rather than silently trading a partial amount.
    # When bot_allocation_usdt IS set, enforce it here too: without this clamp the
    # early return bypassed the cap entirely and the bot could spend the whole wallet.
    if mode in ("fixed", "per_coin"):
        if allocation > 0:
            return round(min(base, effective_free), 2)
        return round(base, 2)
    # For capped mode cap to 90% so a tiny buffer remains for fees.
    return round(min(base, effective_free * 0.9), 2)


# ── M1.1/M1.2 — tradeable minimum + neutral-scaling resolution ───────────────
def _tradeable_min(symbol: Optional[str] = None) -> float:
    """M1.1 — the true minimum tradeable notional (USDT) after ALL multipliers:
    max(exchange minNotional for `symbol` if available, sizing.min_position_usdt,
    10.0). Call with no symbol for account-level checks (config floor only; the
    exchange minNotional is per-symbol)."""
    min_notional = 0.0
    if symbol:
        try:
            from exchange_info import get_symbol_filters as _gsf_tm
            min_notional = float((_gsf_tm(symbol) or {}).get("min_notional", 0.0) or 0.0)
        except Exception:
            min_notional = 0.0
    try:
        min_pos = float(_sizing_cfg().get("min_position_usdt", 10.0))
    except Exception:
        min_pos = 10.0
    return max(min_notional, min_pos, 10.0)


def _resolve_neutral_scaling(cfg: dict, effective_allocation: float,
                             max_positions: int, tradeable_min: float) -> str:
    """M1.2 — resolve the concrete neutral-regime scaling mode at entry time.

    `cfg` carries the raw setting under "neutral_scaling_mode"
    (auto|size|slots|off). "auto" resolves to "slots" for small accounts — when
    the per-slot allocation can't fund 2× the tradeable minimum, so a ticket
    multiplier would push notional near/under minNotional — else "size". Returns
    one of "size" | "slots" | "off" (never "auto")."""
    raw = str((cfg or {}).get("neutral_scaling_mode", "auto")).strip().lower()
    if raw in ("size", "slots", "off"):
        return raw
    try:
        if max_positions > 0 and tradeable_min > 0:
            per_slot = effective_allocation / max_positions
            if per_slot < 2.0 * tradeable_min:
                return "slots"
    except Exception:
        return "size"
    return "size"


def _resolve_entry_budget(symbol: str, free_usdt: float) -> dict:
    """M1.1/M1.2 — resolved per-trade budget WITH the full computation chain so
    the buy path can print an honest, arithmetic skip message.

    Returns {base, mult, mult_label, resolved, mode, regime}. In neutral regime
    the ticket is multiplied by neutral_size_mult ONLY when the resolved neutral
    scaling mode is "size"; "slots"/"off" keep the FULL ticket (risk is reduced
    via slot/pacing limits, not ticket size — so a small ticket never silently
    dies below minNotional)."""
    base = _get_budget_for_coin_base(symbol, free_usdt)
    mult = 1.0
    mult_label = ""
    mode = None
    try:
        regime = get_btc_regime()
    except Exception:
        regime = "neutral"
    if base > 0 and regime == "neutral":
        try:
            _si = effective_slots()
            mode = _resolve_neutral_scaling(
                {"neutral_scaling_mode": _neutral_scaling_mode_cfg()},
                _si["effective_allocation"], _si["max_positions"],
                _tradeable_min(symbol))
        except Exception:
            mode = "off"
        if mode == "size":
            m = _neutral_size_mult()
            if m < 1.0:
                mult = m
                mult_label = "regime neutral"
    return {
        "base":       round(base, 2),
        "mult":       mult,
        "mult_label": mult_label,
        "resolved":   round(base * mult, 2),
        "mode":       mode,
        "regime":     regime,
    }


# M1.2 — transition-only logging of the resolved neutral-scaling decision.
_last_neutral_mode_logged: Optional[Tuple[str, str]] = None
_last_neutral_mode_lock = threading.Lock()


def _log_neutral_scaling_transition(regime: str, mode: str, slots_info: dict,
                                    eff_slots: Optional[int] = None) -> None:
    """M1.2 — log the resolved neutral-scaling decision ONLY when the
    (regime, mode) pair CHANGES (never per eval)."""
    global _last_neutral_mode_logged
    key = (regime, mode)
    with _last_neutral_mode_lock:
        if key == _last_neutral_mode_logged:
            return
        _last_neutral_mode_logged = key
    try:
        if regime == "neutral" and mode == "slots":
            database.log_activity(
                f"Neutral scaling: mode=slots — FULL ticket, new-entry slots "
                f"capped to {eff_slots} of {slots_info.get('effective_slots')}, "
                f"entry pacing doubled (alloc {slots_info.get('effective_allocation')}).",
                "info")
        elif regime == "neutral" and mode == "size":
            database.log_activity(
                f"Neutral scaling: mode=size — ticket × {_neutral_size_mult():g} "
                f"(legacy), floored at the tradeable minimum.", "info")
        elif regime == "neutral" and mode == "off":
            database.log_activity(
                "Neutral scaling: mode=off — full ticket, full slots.", "info")
        else:
            database.log_activity(
                f"Neutral scaling: regime={regime} — no neutral scaling applied.",
                "info")
    except Exception:
        pass


# ── Cooldown helpers ────────────────────────────────────────────────────

def _refresh_risk_params():
    """Read stop_loss_enabled/pct, take_profit_pct and new exit flags from strategy.json."""
    global _user_tp_mult, _take_profit_mult, _stop_loss_mult, _take_profit_enabled, _smart_hold_enabled, _trailing_stop_pct, _BUFFER_OVERRIDES
    global _fee_rate, _FEE_FLOOR
    strategy = _load_strategy()

    # Spec §1.1: refresh the legacy module fee default + breakeven floor from
    # the FeeModel so strategy.json fee edits hot-reload everywhere.
    try:
        _base_taker = fees.get_fee_model().taker()
        if 0.0 <= _base_taker < 0.02:
            _fee_rate  = _base_taker
            _FEE_FLOOR = 1.0 / ((1.0 - _base_taker) ** 2)
    except Exception:
        pass  # keep previous values — never break risk refresh on fee errors
    tp_pct = float(strategy.get("take_profit_pct", 0.1))   # e.g. 0.5 → 0.5%
    sl_pct = float(strategy.get("stop_loss_pct",   0.4))   # default 0.4% from BEP
    sl_on  = bool(strategy.get("stop_loss_enabled", True))  # always on by default
    _take_profit_enabled = bool(strategy.get("take_profit_enabled", True))
    _smart_hold_enabled  = bool(strategy.get("smart_hold_enabled",  False))
    _trailing_stop_pct   = float(strategy.get("trailing_stop_pct",  0.5))

    # Per-coin buffer overrides from strategy.json (e.g. {"SHIBUSDT": 0.30, "BTCUSDT": 0.08})
    overrides = strategy.get("slippage_buffer_overrides", {})
    if isinstance(overrides, dict):
        _BUFFER_OVERRIDES = {k: float(v) for k, v in overrides.items()}

    # User TP raw multiplier — per-position breakeven is computed by _get_breakeven_mult
    _user_tp_mult = 1.0 + (tp_pct / 100.0)

    # _take_profit_mult kept for legacy exports/diagnostics — use a mid-range reference price
    _take_profit_mult = (_FEE_FLOOR * 1.0010 if not _take_profit_enabled
                         else max(_FEE_FLOOR * 1.0010, _user_tp_mult))
    # Stop loss: set to 0.0 when disabled so the check (price <= 0.0) never fires
    _stop_loss_mult   = (1.0 - sl_pct / 100.0) if sl_on else 0.0


def _profitable_sell_check(pos: dict, price: float, force_fresh: bool = False) -> bool:
    """Return True only when selling at `price` nets at least min_profit + small slippage cushion.

    Uses ACTUAL deployed capital (qty * entry_price) — same basis as the trigger
    in compute_real_breakeven_price — so the trigger and the safety gate cannot
    disagree on what 'profitable' means.

    When force_fresh=True, does a single-symbol REST fetch and overrides the
    passed price with the freshest available.

    Keeps a 0.05% slippage cushion (reduced from 0.15%) — calibrated for the
    liquid coins the bot trades. Any take-profit sell that passes this check
    should never result in a net loss.
    """
    symbol = pos.get("symbol", "")
    # Only re-fetch when the triggering price is NOT fresh. A WS tick that just
    # crossed the target (<2s old) must be honored as-is: re-quoting via REST
    # adds up to 3s latency and can veto the sell if a sub-second spike already
    # retraced — exactly the exit the user asked for.
    _px_age = time.time() - _last_ws_price_ts.get(symbol, 0)
    if force_fresh and symbol and _px_age >= 2.0:
        try:
            # Own source tag: gate-fetch failures must not escalate the shared
            # batch_prices backoff and starve the price refreshers.
            _fresh = _fetch_batch_prices([symbol], source="presell_fresh")
            _fp = _fresh.get(symbol, 0) if _fresh else 0
            if _fp > 0:
                price = _fp
                _rest_px[symbol] = _fp
                _rest_px_sym_ts[symbol] = time.time()
                _last_ws_price_ts[symbol] = time.time()
                try:
                    import data_collector as _dc_psc
                    _dc_psc.prices[symbol] = _fp
                except Exception:
                    pass
        except Exception:
            pass  # fall through to use passed price

    entry   = float(pos.get("entry_price") or pos.get("avg_entry_price") or 0)
    qty     = float(pos.get("quantity", 0))
    buy_fee = float(pos.get("buy_fee_usdt") or 0)
    if entry <= 0 or qty <= 0 or price <= 0:
        return False
    # Use ACTUAL deployed capital (matches compute_real_breakeven_price)
    actual_cost   = qty * entry + buy_fee
    gross_quote   = price * qty
    # FeeModel taker fraction — SAME call as compute_real_breakeven_price
    # (BEP↔gate parity, spec §1.1).
    est_sell_fee  = gross_quote * _fee_rate_for(symbol)
    net_returned  = gross_quote - est_sell_fee
    # GATE == TRIGGER: this check must verify exactly the same threshold the
    # sell trigger fires at (compute_real_breakeven_price with the default
    # min_profit resolved via _min_profit_usdt()). The SAME config value MUST
    # be used in the BEP trigger, this profit gate, and any (future)
    # breakeven-move stop — see _min_profit_usdt.
    # The gate previously demanded max(min_profit+slippage, cost*tp_pct) NET —
    # i.e. more profit than the price at the exit target delivers after fees —
    # so every take-profit sell was vetoed AT the target and only executed
    # 0.05-0.25% higher (or never, if price retreated). The trigger already
    # guarantees price >= max(breakeven, entry*tp); the gate only needs to
    # confirm the sell nets a real profit, not re-add its own margin on top.
    min_profit = _min_profit_usdt()
    estimated_profit = net_returned - actual_cost
    return estimated_profit >= min_profit


def _set_cooldown(symbol: str):
    _cooldowns[symbol] = time.time() + config.COOLDOWN_AFTER_LOSS


def _in_cooldown(symbol: str) -> bool:
    exp = _cooldowns.get(symbol, 0)
    return time.time() < exp


# ── DB / startup helpers ─────────────────────────────────────────────────

def _rebuild_pos_index():
    """Rebuild O(1) symbol→position lookup. Call whenever _positions changes."""
    global _pos_by_symbol
    with _positions_lock:
        _pos_by_symbol = {p["symbol"]: p for p in _positions}


def _apply_coin_restore(coins: list):
    """Write a list of coin symbols back into strategy.json approved_coins."""
    if not (coins and isinstance(coins, list) and len(coins) > 0):
        return
    try:
        if os.path.exists(config.STRATEGY_FILE):
            with open(config.STRATEGY_FILE) as f:
                strat = json.load(f)
            existing = {c["symbol"]: c for c in strat.get("approved_coins", [])}
            strat["approved_coins"] = [
                {
                    "symbol":         sym,
                    "approved":       True,
                    "budget_usdt":    existing.get(sym, {}).get("budget_usdt", config.BUDGET_PER_TRADE_USDT),
                    "max_concurrent": existing.get(sym, {}).get("max_concurrent", 3),
                    "confidence":     existing.get(sym, {}).get("confidence", 0.5),
                    "reason":         "Restored from Supabase",
                }
                for sym in coins
            ]
            strat["updated_at"] = datetime.now(timezone.utc).isoformat()
            tmp = config.STRATEGY_FILE + ".tmp"
            with open(tmp, "w") as f:
                json.dump(strat, f, indent=2)
                f.flush()
                try:
                    os.fsync(f.fileno())
                except Exception:
                    pass
            os.replace(tmp, config.STRATEGY_FILE)
            print(f"[TradeEngine] Restored {len(coins)} coins from Supabase → strategy.json.")
    except Exception as ce:
        print(f"[TradeEngine] Coin restore to strategy.json failed: {ce}")


# Symbols/assets already warned about by the orphan scanner — log once per
# process, not on every startup loop.
_orphan_warned: set = set()


def _has_unclosed_bot_buy(sym: str) -> bool:
    """True when the DB records a bot LIVE BUY of `sym` with no later closing sell.

    The trades table only stores completed round-trips (a row is written at sell
    time), so the buy-time evidence lives in activity_log: every live buy logs
    "[LIVE] BOUGHT {sym} ..." and every close logs "[LIVE] SOLD {sym} ..." (or a
    force-close message). Read-only query — never adopt a wallet holding as a
    sellable position unless the bot itself bought it.
    """
    try:
        import sqlite3 as _sq_ob
        conn = _sq_ob.connect(database.DB_PATH)
        try:
            buy_id = conn.execute(
                "SELECT MAX(id) FROM activity_log WHERE message LIKE ?",
                (f"[LIVE] BOUGHT {sym} %",),
            ).fetchone()[0]
            if buy_id is None:
                return False
            sell_id = conn.execute(
                "SELECT MAX(id) FROM activity_log WHERE message LIKE ? "
                "OR message LIKE ? OR message LIKE ?",
                (f"[LIVE] SOLD {sym} %",
                 f"{sym}: force-closing position%",
                 f"{sym}: position removed from records%"),
            ).fetchone()[0]
        finally:
            conn.close()
        return sell_id is None or buy_id > sell_id
    except Exception:
        return False  # uncertainty must never fabricate a sellable position


# Quote/stable/fiat assets that never have a tradeable <asset>USDT spot pair the
# bot should adopt/auto-sell (e.g. USDUSDT → -1121 Invalid symbol).
_NON_TRADEABLE_BASE = (
    "USDT", "BNB", "BUSD", "USDC", "USD", "FDUSD", "TUSD", "DAI", "USDP",
    "AEUR", "EUR", "GBP", "TRY", "BRL", "AUD", "UAH", "RUB", "NGN", "ZAR",
    "PLN", "ARS", "JPY", "MXN", "CZK", "COP",
)


def recover_orphan_positions() -> dict:
    """N2b — boot risk-integrity: adopt and RE-PROTECT live coins the bot holds on
    Binance but has no open position record for, BEFORE entry evaluation is armed.
    A crash must never leave a live coin with no stop.

    For every non-dust base asset with an <asset>USDT pair:
      • already tracked (open position record) → skip.
      • balance with a recorded UNCLOSED bot BUY but no open record → ADOPT:
        recreate the in-memory position from the recorded entry (current price is
        the safest entry estimate after a DB wipe) and IMMEDIATELY re-attach the
        SAME exit geometry + (maker/OCO) stop/TP a normal open uses
        (_apply_entry_exit_geometry + _place_managed_exit).
      • balance with NO recorded bot buy → LOUD 'UNMANAGED BALANCE' warning; never
        auto-sold (it is the user's own holding).

    Fail-safe: every asset is processed in its own try/except and the whole scan
    is best-effort — recovery NEVER crashes boot. A held position that cannot be
    re-protected is flagged via the H1 stuck-position escalation so the UI
    surfaces it for manual action.

    Returns {'adopted': [...], 'unmanaged': [...], 'reprotected': [...]}. Safe to
    call standalone (control_api boot) or in-line from load_positions_from_db;
    MUST run before entries are armed (it does — load_positions_from_db precedes
    start_entry_heartbeat / the _entries_armed gate in the boot sequence).
    """
    result: Dict[str, list] = {"adopted": [], "unmanaged": [], "reprotected": [],
                               "exited_underwater": []}
    if get_mode() != "live":
        return result
    try:
        acc = _acct()  # geo-block-safe direct transport
    except Exception as e:
        database.log_activity(f"Orphan recovery: account fetch failed (non-fatal): {e}", "warn")
        return result

    with _positions_lock:
        tracked_symbols = {p["symbol"] for p in _positions}
    now_ts = datetime.now(timezone.utc).isoformat()

    for b in acc.get("balances", []):
        try:
            asset  = b["asset"]
            free   = float(b["free"])
            locked = float(b["locked"])
            total  = free + locked
            if asset in _NON_TRADEABLE_BASE or total <= 0:
                continue
            # Binance Earn wrappers (LDPYTH etc.) are not tradeable spot assets.
            if asset.startswith("LD"):
                continue
            sym = asset + "USDT"
            if sym in tracked_symbols:
                continue

            # Current price — cached REST price first, then the public data-api
            # ticker (geo-block-safe, no signed call needed).
            price = _rest_px.get(sym, 0) or 0
            if price <= 0:
                try:
                    price = float(_fetch_batch_prices([sym]).get(sym, 0) or 0)
                except Exception:
                    pass

            # Only FREE balance is market-sellable; locked qty would -2010.
            sell_qty = free if free > 0 else total
            value = sell_qty * price if price > 0 else 0

            # Dust below Binance's ~$10 min notional can't be sold — skip (log
            # once per process).
            if price > 0 and value < 10.0:
                if value >= 1.0 and asset not in _orphan_warned:
                    _orphan_warned.add(asset)
                    database.log_activity(
                        f"Orphan scan: skipping {asset} (~${value:.2f}) — below $10 "
                        f"min notional, unsellable dust", "info")
                continue

            # ONLY adopt holdings the bot itself bought. Anything else is the
            # user's personal holding — fabricating a position would let the sell
            # monitor liquidate the user's own coins.
            if not _has_unclosed_bot_buy(sym):
                if asset not in _orphan_warned:
                    _orphan_warned.add(asset)
                    database.log_activity(
                        f"UNMANAGED BALANCE {asset} (~${value:.2f}) — manual review. "
                        f"Held on Binance with no recorded bot buy; NOT adopted and "
                        f"NOT auto-sold.", "warn")
                result["unmanaged"].append(asset)
                continue

            if price <= 0 or value <= 1.0:
                if asset not in _orphan_warned:
                    _orphan_warned.add(asset)
                    msg = (f"⚠ ORPHANED COIN: {asset} qty={total:.8f} on Binance "
                           f"but no price available — sell manually via Binance app.")
                    print(f"[TradeEngine] {msg}")
                    database.log_activity(msg, "warn")
                continue

            # ── ADOPT ────────────────────────────────────────────────────────
            # Entry is unknown after a DB wipe — current price is the safest
            # default: the bot monitors from here and exits on TP/SL.
            exit_target = round(price * _get_breakeven_mult(price, sym), 8)
            pos_record = {
                "symbol":       sym,
                "entry_price":  price,
                "exit_target":  exit_target,
                "quantity":     sell_qty,
                "budget_usdt":  round(value, 2),
                "buy_fee_usdt": round(value * _fee_rate_for(sym), 6),
                "timestamp":    now_ts,
                "mode":         "live",
                # P4: orphan-recovered holdings are tagged 'recovered' (origin is
                # never None — a None origin is the BNB tagging bug).
                "origin":       "recovered",
                # P3: explicit bool from birth.
                "be_moved":     False,
            }
            # Re-attach the SAME exit geometry a normal open computes (§2.1) —
            # this alone gives the local sell monitor a stop/TP to defend.
            _apply_entry_exit_geometry(pos_record)
            # P4: recovery must attach a CORRECT stop — never one the market has
            # already passed. entry==current price here, so the ATR stop is
            # normally below price, but guard defensively (ATR/BEP edge cases).
            # _underwater is True when the intended stop was already past price.
            _underwater = _guard_stale_recovery_stop(pos_record, price)
            pos_id = database.save_position(pos_record)
            pos_record["id"] = pos_id
            with _positions_lock:
                _positions.append(pos_record)
            tracked_symbols.add(sym)
            result["adopted"].append(sym)
            database.log_activity(
                f"AUTO-RECOVERED orphaned live position: {sym} "
                f"qty={total:.8f} @ ${price:.4f} (~${value:.2f} USDT) — "
                f"entry price is ESTIMATED (current price used). "
                f"Adjust stop-loss in dashboard if needed.", "warn")

            # ── R2.1 — intended stop ALREADY underwater at adoption (the BNB
            # case). Part P clamped it just below price so P1 fires NEXT cycle;
            # R2 strengthens that: don't hold an unprotected bag for a cycle —
            # fire an IMMEDIATE protective market exit via the (now-unblocked) P1
            # protective sell path. Guarded: a failure just logs + escalates via
            # the H1 stuck path, never crashes boot. Skip the exchange re-protect
            # below since we're flattening the position right now.
            if _underwater:
                try:
                    dispatched = False
                    with _selling_lock:
                        if sym not in _selling:
                            _selling.add(sym)
                            _selling_ts[sym] = time.time()
                            dispatched = True
                    if dispatched:
                        # "stop-loss" is a protective reason — floor-exempt, fires
                        # at market (see _PROTECTIVE_STOP_REASONS / P1.3).
                        _sell_executor.submit(_execute_sell, pos_record, price, "stop-loss")
                    result["exited_underwater"].append(sym)
                    database.log_activity(
                        f"RECOVERY {sym}: intended stop underwater at adoption — "
                        f"dispatched IMMEDIATE protective market exit (not held "
                        f"unprotected for a cycle).", "warn")
                except Exception as ue:
                    database.log_activity(
                        f"RECOVERY {sym}: immediate underwater exit FAILED ({ue}) — "
                        f"escalating stuck for manual review; position remains "
                        f"clamped for the P1 next-cycle exit.", "error")
                    try:
                        for _ in range(_STUCK_SKIP_THRESHOLD + 1):
                            _note_sell_skip(sym, "recovery_underwater_exit_failed")
                    except Exception:
                        pass
                continue   # underwater path handled — skip the exchange re-protect

            # ── RE-PROTECT before entries arm: attach the exchange-side (maker
            # TP / OCO) exit a normal open places. Geometry above already gives a
            # local stop/TP; this adds the exchange backup. Failure to re-protect
            # a held position is escalated via the H1 stuck-position path.
            try:
                has_protection = bool(pos_record.get("stop_price")
                                      or pos_record.get("tp_price"))
                _place_managed_exit(pos_record)   # no-op in paper fallback
                if has_protection:
                    result["reprotected"].append(sym)
                    database.log_activity(
                        f"Re-protected orphan {sym}: stop={pos_record.get('stop_price')} "
                        f"tp={pos_record.get('tp_price')} re-attached before entries armed",
                        "info")
                else:
                    raise RuntimeError("no stop_price/tp_price after geometry")
            except Exception as pe:
                database.log_activity(
                    f"Orphan {sym}: RE-PROTECTION FAILED ({pe}) — flagging stuck for "
                    f"manual review; position is adopted but may lack an exchange stop",
                    "error")
                try:
                    for _ in range(_STUCK_SKIP_THRESHOLD + 1):
                        _note_sell_skip(sym, "orphan_reprotect_failed")
                except Exception:
                    pass
        except Exception as be:
            # Per-asset fail-safe: one bad balance row never aborts recovery/boot.
            try:
                database.log_activity(
                    f"Orphan recovery: {b.get('asset', '?')} skipped (non-fatal): {be}",
                    "warn")
            except Exception:
                pass
            continue

    if result["adopted"]:
        _rebuild_pos_index()
    try:
        lvl = "warn" if (result["adopted"] or result["unmanaged"]) else "info"
        database.log_activity(
            f"Orphan recovery summary: adopted={result['adopted']} "
            f"reprotected={result['reprotected']} "
            f"exited_underwater={result['exited_underwater']} "
            f"unmanaged={result['unmanaged']}", lvl)
    except Exception:
        pass
    # ── R2.3 — ONE clear boot line so a crash's position impact is visible.
    try:
        n_ad = len(result["adopted"]); m_rp = len(result["reprotected"])
        k_uw = len(result["exited_underwater"]); u_um = len(result["unmanaged"])
        database.log_activity(
            f"RECOVERY: adopted {n_ad} (reprotected {m_rp}, "
            f"exited-underwater {k_uw}, unmanaged {u_um}) — "
            f"adopted={result['adopted']} reprotected={result['reprotected']} "
            f"exited_underwater={result['exited_underwater']} "
            f"unmanaged={result['unmanaged']}",
            "warn" if (n_ad or u_um or k_uw) else "info")
    except Exception:
        pass
    # ── R2.4 — every held position must end boot with a protective stop.
    try:
        _verify_boot_protection()
    except Exception:
        pass
    return result


def _verify_boot_protection() -> None:
    """R2.4 — after recovery, guarantee EVERY currently-held position ends boot
    with a protective stop attached (stop_price OR hard_sl_price; be_moved may be
    False). A position mid-exit (dispatched underwater) is being flattened and is
    skipped. A held position with NEITHER stop is a live unprotected bag → log
    CRITICAL and escalate via the H1 stuck path for manual action. Never raises."""
    try:
        with _positions_lock:
            snapshot = list(_positions)
    except Exception:
        return
    for pos in snapshot:
        try:
            sym = pos.get("symbol")
            with _selling_lock:
                if sym in _selling:      # being flattened right now — not a bag
                    continue
            if pos.get("stop_price") or pos.get("hard_sl_price"):
                continue
            database.log_activity(
                f"CRITICAL: {sym} ended boot with NO protective stop "
                f"(stop_price/hard_sl_price both unset) — escalating stuck for "
                f"manual review; position is UNPROTECTED.", "critical")
            for _ in range(_STUCK_SKIP_THRESHOLD + 1):
                _note_sell_skip(sym, "boot_no_protective_stop")
        except Exception:
            pass


def load_positions_from_db():
    """
    Called on startup.  Restores open positions from SQLite when available,
    or from Supabase when SQLite is empty (fresh Railway deploy / no volume).
    Coins and balance are ALWAYS restored from Supabase so the user's
    watchlist and wallet survive every redeploy regardless of SQLite state.
    """
    global _positions
    # Filter by current mode so live positions never load into a paper session
    # and vice versa — cross-loading caused paper client to "sell" live positions.
    _current_mode = get_mode()
    rows = database.load_positions(mode=_current_mode)

    # ── Startup migration: fix positions saved with wrong/missing mode tag ──────
    # Positions created before the mode-isolation fix default to mode='paper'
    # even when bought in live mode. If we're in live mode and find no positions
    # but the unfiltered table has rows, migrate them rather than losing them.
    if not rows:
        all_rows = database.load_positions()  # unfiltered
        if all_rows:
            database.migrate_positions_mode(_current_mode)
            rows = database.load_positions(mode=_current_mode)
            if rows:
                database.log_activity(
                    f"Startup: migrated {len(rows)} position(s) to mode='{_current_mode}' "
                    f"(were stored with wrong/missing mode tag)", "warn"
                )

    # Always fetch from Supabase so coins + balance are current even when
    # SQLite still has stale data from a previous deploy.
    supa_ok = False
    try:
        import concurrent.futures as _cf
        import supabase_sync
        with _cf.ThreadPoolExecutor(max_workers=1) as _ex:
            _fut = _ex.submit(supabase_sync.restore_from_supabase)
            try:
                restored = _fut.result(timeout=5)
            except _cf.TimeoutError:
                raise Exception("Supabase restore timed out after 5s")
        supa_ok = bool(restored)

        # ── Coins: ALWAYS restore — user selection must survive every deploy ──
        _apply_coin_restore(restored.get("selected_coins"))

        # ── Positions: only restore when SQLite is empty (avoids duplicates) ──
        if not rows and restored.get("positions"):
            for pos in restored["positions"]:
                pos_id = database.save_position(pos)
                pos["id"] = pos_id
            rows = database.load_positions(mode=_current_mode)
            n_pos = len(rows)
            print(f"[TradeEngine] Restored {n_pos} position(s) from Supabase.")
            database.log_activity(
                f"Restored {n_pos} open position(s) from Supabase after redeploy", "info"
            )
        elif rows:
            database.log_activity(
                f"Startup: {len(rows)} position(s) loaded from local DB (Supabase backup intact)", "info"
            )
        else:
            database.log_activity(
                "Startup: no positions in local DB or Supabase — fresh wallet", "info"
            )

        # ── Trade history: restore when SQLite trades table is empty ────────────
        # Rebuilds win-rate, P&L stats and trade list from bot_trade_history.
        try:
            if not database.get_recent_trades(limit=1) and restored.get("trades"):
                imported = 0
                for trade in restored["trades"]:
                    try:
                        database.log_trade(trade)
                        imported += 1
                    except Exception:
                        pass
                if imported:
                    print(f"[TradeEngine] Imported {imported} trade(s) from Supabase history.")
                    database.log_activity(
                        f"Restored {imported} trade record(s) from Supabase history", "info"
                    )
            elif database.get_recent_trades(limit=1):
                database.log_activity(
                    f"Trade history intact in local DB — no restore needed", "info"
                )
        except Exception as te:
            print(f"[TradeEngine] Trade history restore failed (non-fatal): {te}")

        # ── Balance: restore when SQLite paper-state is empty or zero ──────────
        if restored.get("usdt_balance") is not None:
            usdt = restored["usdt_balance"]
            _pc = _client()
            if hasattr(_pc, "_balances"):
                with _pc._lock:
                    current = _pc._balances.get("USDT", 0.0)
                # Restore from Supabase when:
                #   a) local balance is zero / negative
                #   b) no open positions in SQLite (fresh deploy / no volume)
                #   c) balance is at the ENV starting default AND Supabase has a
                #      meaningfully different value (paper_state was never persisted)
                _starting_usdt = float(os.getenv("STARTING_PAPER_USDT", "10000.0"))
                _at_default    = abs(current - _starting_usdt) < 0.01
                _supa_differs  = abs(usdt - current) > 1.0
                if current <= 0 or not rows or (_at_default and _supa_differs):
                    with _pc._lock:
                        _pc._balances["USDT"] = usdt
                        snapshot = dict(_pc._balances)
                    database.save_paper_state(snapshot)
                    print(f"[TradeEngine] Restored USDT balance from Supabase: {usdt:.2f}")
                    database.log_activity(
                        f"Restored USDT balance from Supabase: {usdt:.2f} USDT", "info"
                    )

    except Exception as e:
        print(f"[TradeEngine] Supabase restore failed (non-fatal): {e}")
        database.log_activity(f"Supabase restore failed — data may not survive redeploy: {e}", "warn")

    with _positions_lock:
        _positions = list(rows)
    _rebuild_pos_index()
    print(f"[TradeEngine] Loaded {len(_positions)} open position(s) from DB.")

    # Phase 2 §2.1: the positions table has fixed columns, so exit geometry
    # (atr_pct_at_entry / sl_distance_pct / stop_price / tp_price /
    # hard_sl_price) does not survive a restart — recompute it for every
    # restored position that is missing it. Uses the current 5m/1m data
    # sources; when none are warm yet the conservative sl_min_pct stop applies.
    for _pos_geo in _positions:
        try:
            if not _pos_geo.get("tp_price"):
                _apply_entry_exit_geometry(_pos_geo, log_open=False)  # restore: no OPEN log
            # P3/P4: the positions table is fixed-column, so be_moved and origin
            # never survive a restart. Guarantee both are set (be_moved an
            # explicit bool, origin never None) — a restored position whose
            # origin metadata was lost is tagged 'recovered'.
            _pos_geo.setdefault("be_moved", False)
            if not _pos_geo.get("origin"):
                _pos_geo["origin"] = "recovered"
            # P4: a stop recomputed from the ORIGINAL (higher) entry can land
            # above the current price — an already-blown stop (the BNB case).
            # Clamp it just below the live price so the protective exit (P1) can
            # fire instead of holding forever above a dead stop.
            _cur_px = _rest_px.get(_pos_geo.get("symbol"), 0) or 0
            if _cur_px > 0:
                _guard_stale_recovery_stop(_pos_geo, _cur_px)
        except Exception:
            pass  # geometry restore must never break startup

    # Push current state to Supabase immediately so the next redeploy can restore it.
    # This runs even when SQLite had data (not just after a restore) so Supabase
    # always has an up-to-date snapshot regardless of how the data got into SQLite.
    try:
        import supabase_sync
        usdt = _get_usdt_balance()
        supabase_sync.sync_all(list(rows), usdt)
        database.log_activity(
            f"Supabase snapshot pushed — {len(rows)} position(s), {usdt:.2f} USDT backed up", "info"
        )
    except Exception as e:
        database.log_activity(f"Supabase snapshot push failed: {e}", "warn")

    # N2b — orphan recovery: adopt & RE-PROTECT any live Binance holding that has
    # a recorded bot buy but no open position record, BEFORE entries are armed.
    # Runs in-line here (load_positions_from_db precedes start_entry_heartbeat /
    # the _entries_armed gate) so a crash never leaves a live coin without a stop.
    # Fully self-contained + fail-safe — never crashes boot.
    try:
        recover_orphan_positions()
    except Exception as e:
        database.log_activity(f"Orphan scan/recovery failed (non-fatal): {e}", "warn")

    # R2/I1 — boot ordering: reconcile/recover orphans → SEED HELD PRICES (with
    # retry) → arm entries. Proactively seed a fresh REST price for every restored
    # open position now, BEFORE the entry-arming gate can open, so a restart never
    # leaves held positions unpriced (their stops would be blind). The _entries_armed
    # gate re-checks + re-seeds too, so this is best-effort here; failures degrade
    # (logged) and never crash boot.
    try:
        _still_unpriced = _seed_held_prices(reason="boot_restore")
        if _still_unpriced:
            database.log_activity(
                f"[Boot] {len(_still_unpriced)} held positions unpriced after seed "
                f"(entry-arming gate + held watchdog will keep retrying): "
                f"{_still_unpriced}", "warn")
    except Exception as e:
        database.log_activity(f"Held-price seed at boot failed (non-fatal): {e}", "warn")

    # Sync PaperClient coin balances from the restored positions.
    # After a restart, PaperClient loads from saved paper_state, but if that
    # state is out of sync with the positions table (e.g. a partial crash),
    # sells will fail with "Insufficient balance". Self-heal by crediting the
    # exact quantity that each open position holds.
    _pc2 = _client()
    if hasattr(_pc2, "_balances") and _positions:
        changed = False
        for pos in _positions:
            sym  = pos["symbol"]
            coin = sym[:-4]  # strip USDT
            qty  = float(pos.get("quantity", 0))
            if qty <= 0:
                continue
            with _pc2._lock:
                current = _pc2._balances.get(coin, 0.0)
                if current < qty * 0.99:
                    _pc2._balances[coin] = qty
                    changed = True
                    print(f"[TradeEngine] Synced paper balance: {coin}={qty:.8f} (was {current:.8f})")
        if changed:
            database.save_paper_state(dict(_pc2._balances))


def get_open_positions() -> List[dict]:
    with _positions_lock:
        return list(_positions)


# ── exchangeInfo cache warm-up ─────────────────────────────────────────
try:
    from exchange_info import get_symbol_filters as _warmup_gsf
    _warmup_gsf("BTCUSDT")  # warms the exchangeInfo cache at startup
    del _warmup_gsf
except Exception:
    pass


# ── Strategy loader ────────────────────────────────────────────────────

def _load_strategy() -> dict:
    global _strategy_mtime, _strategy_cache
    path = config.STRATEGY_FILE
    if not os.path.exists(path):
        return {}
    try:
        mtime = os.path.getmtime(path)
        if mtime != _strategy_mtime:
            with open(path, "r") as f:
                _strategy_cache = json.load(f)
            _strategy_mtime = mtime
    except Exception:
        pass
    return _strategy_cache


def _data_cfg() -> dict:
    """strategy.data.* config block (mtime-cached via _load_strategy so edits
    hot-reload). Callers read keys like rss_soft_cap_mb / tracemalloc_enabled."""
    raw = _load_strategy().get("data")
    return raw if isinstance(raw, dict) else {}


# ── Account helpers ────────────────────────────────────────────────────

def _get_usdt_balance() -> float:
    try:
        if get_mode() != "live":
            # Paper mode: read directly from PaperClient's in-memory balance dict
            _pc = _client()
            if hasattr(_pc, "_balances"):
                with _pc._lock:
                    return float(_pc._balances.get("USDT", 0.0))
            return float(os.getenv("STARTING_PAPER_USDT", "10000.0"))
        # Live mode: query Binance spot wallet — use free balance only (tradeable).
        # Geo-block-safe direct transport (python-binance requests are blocked).
        acc = _acct()
        for b in acc["balances"]:
            if b["asset"] == "USDT":
                return float(b["free"])
    except Exception as e:
        database.log_activity(f"get_usdt_balance error: {e}", "warn")
    return 0.0


# ── F9: initial_balance semantics ─────────────────────────────────────────────
# initial_balance_usdt is the P&L baseline the UI measures growth against. It
# was captured as free USDT ALONE at an arbitrary moment (45.60 while the true
# portfolio was 96.73), so any USDT already tied up in open positions was
# silently excluded and the baseline read far too low. Correct definition:
#
#   initial_balance_usdt = free USDT + current in-position (mark) value
#
# i.e. total portfolio value at arming time. rebaseline_initial_balance()
# recomputes and persists it on demand (control_api exposes an endpoint).
def _in_position_value_usdt() -> float:
    """Mark value of all open positions (Σ quantity × current price; falls back
    to entry price when no live quote is available)."""
    total = 0.0
    for p in get_open_positions():
        try:
            qty = float(p.get("quantity") or 0)
            if qty <= 0:
                continue
            sym = p.get("symbol") or ""
            px = _current_price_for(sym) or float(p.get("entry_price") or 0)
            total += qty * float(px or 0)
        except Exception:
            continue
    return total


def rebaseline_initial_balance() -> dict:
    """F9: recompute initial_balance_usdt = free USDT + in-position value NOW,
    persist it to strategy.json, and return the new baseline.

    Returns {"initial_balance_usdt", "free_usdt", "in_position_usdt", "ok"}.
    Safe to call from an API thread; never raises."""
    free = 0.0
    inpos = 0.0
    try:
        free = float(_get_usdt_balance() or 0.0)
        inpos = float(_in_position_value_usdt() or 0.0)
    except Exception:
        pass
    new_baseline = round(free + inpos, 2)
    ok = False
    try:
        path = config.STRATEGY_FILE
        s = {}
        if os.path.exists(path):
            with open(path, "r") as f:
                s = json.load(f)
        s["initial_balance_usdt"] = new_baseline
        tmp = f"{path}.rebaseline.tmp"
        with open(tmp, "w") as f:
            json.dump(s, f, indent=2)
        os.replace(tmp, path)
        ok = True
    except Exception as _e:
        try:
            database.log_activity(
                f"rebaseline_initial_balance persist failed: {_e}", "warn")
        except Exception:
            pass
    try:
        database.log_activity(
            f"initial_balance rebaselined → {new_baseline} USDT "
            f"(free={round(free, 2)} + in_position={round(inpos, 2)})", "info")
    except Exception:
        pass
    return {"initial_balance_usdt": new_baseline, "free_usdt": round(free, 2),
            "in_position_usdt": round(inpos, 2), "ok": ok}


def _get_actual_balance(symbol: str) -> float:
    """Return free+locked balance for the base asset of symbol, or 0.0 on error."""
    try:
        base = symbol.replace("USDT", "").replace("usdt", "")
        acc = _acct()
        for b in acc.get("balances", []):
            if b["asset"] == base:
                return float(b.get("free", 0)) + float(b.get("locked", 0))
    except Exception as e:
        log_diag_issue("ghost_check", "warn", f"_get_actual_balance failed for {symbol}: {e}")
    return 0.0


def _is_truly_ghost_position(symbol: str, expected_qty: float) -> Tuple[bool, str]:
    """Verify whether a -2010 sell failure means the position is truly gone.

    Returns (is_ghost, reason). Only returns is_ghost=True when the asset
    balance on Binance is genuinely zero — not when the bot just has the wrong
    quantity on record.  Errors during the check return (False, 'check_failed')
    so uncertainty never causes an accidental force-close.
    """
    try:
        base = symbol.replace("USDT", "").replace("usdt", "")
        acc = _acct()
        for b in acc.get("balances", []):
            if b["asset"] == base:
                total = float(b.get("free", 0)) + float(b.get("locked", 0))
                if total <= 1e-8:
                    return True, "zero_balance"
                if total < expected_qty * 0.95:
                    return False, f"qty_mismatch (have {total:.6f}, expected {expected_qty:.6f})"
                return False, "balance_ok"
        # Asset absent from balance list → genuinely not owned
        return True, "asset_not_in_account"
    except Exception as e:
        log_diag_issue("ghost_check", "error", f"Balance check failed for {symbol}: {e}")
        return False, "check_failed"


_lot_step_cache: Dict[str, float] = {}

# ── Buy-rejection instrumentation — pure observation, no logic changes ────────
from collections import defaultdict

_rejection_counts: dict = defaultdict(int)
_rejection_examples: dict = {}
_rejection_lock = threading.Lock()
_rejection_reset_ts: float = time.time()


# ── J1 — per-symbol decision trace ────────────────────────────────────────────
# The operator repeatedly saw a coin show "green / buy_ready=1" but no purchase
# and NO log lines for that symbol. This records, per symbol, WHY it did/didn't
# act so control_api can surface it. All fields are raw (control_api computes
# ages). Updated in-line on the buy/evaluation path; O(1) per eval.
#   last_evaluated_ts — set every time the symbol is evaluated (fresh re-check).
#   last_attempt_ts   — set when an actual entry attempt begins (order start).
#   last_block_reason/last_block_ts — most recent reason it did NOT buy (reuses
#                       the existing _record_rejection reason taxonomy).
#   cached_green       — cached (last-candle) verdict says buy-ready.
#   engine_ready       — the FRESH re-check verdict passed.
_decision_trace: Dict[str, dict] = {}
_decision_trace_lock = threading.Lock()
_decision_heartbeat: int = 0            # +1 per _check_buys_from_cache pass
_DECISION_GAP_HEARTBEATS = 2            # ready for >N cycles w/o attempt/block = bug
# Transition-only dedupe for the DECISION-GAP warn: sym -> the _ready_since_hb
# streak we last warned about (so one warn per stuck streak, no spam).
_decision_gap_warned: Dict[str, int] = {}


def _new_trace_entry() -> dict:
    return {
        "last_evaluated_ts": 0.0,
        "last_attempt_ts":   0.0,
        "last_block_reason": None,
        "last_block_ts":     0.0,
        "cached_green":      False,
        "engine_ready":      False,
        # internal (stripped from the public reader):
        "_ready_since_hb":   0,     # heartbeat the current ready-streak began
        "_was_ready":        False,
        "_last_touch_hb":    -1,    # heartbeat a fresh attempt/block was stamped
    }


def _trace_mark_evaluated(symbol: str, cached_green: bool) -> None:
    """Set last_evaluated_ts + cached_green, and maintain the ready-streak used
    by the DECISION-GAP detector. Called once per symbol per heartbeat."""
    if not symbol or symbol == "(all)":
        return
    now = time.time()
    with _decision_trace_lock:
        ent = _decision_trace.get(symbol)
        if ent is None:
            ent = _new_trace_entry()
            _decision_trace[symbol] = ent
        ent["last_evaluated_ts"] = now
        ent["cached_green"] = bool(cached_green)
        ready = bool(cached_green) or bool(ent.get("engine_ready"))
        if ready and not ent.get("_was_ready"):
            ent["_ready_since_hb"] = _decision_heartbeat
        ent["_was_ready"] = ready
        if ready:
            _maybe_warn_decision_gap_locked(symbol, ent)
        else:
            _decision_gap_warned.pop(symbol, None)


def _trace_mark_engine_ready(symbol: str) -> None:
    """FRESH re-check verdict passed (engine-ready, distinct from cached-green)."""
    if not symbol or symbol == "(all)":
        return
    with _decision_trace_lock:
        ent = _decision_trace.get(symbol) or _new_trace_entry()
        _decision_trace[symbol] = ent
        ent["engine_ready"] = True


def _trace_mark_attempt(symbol: str) -> None:
    """An actual entry attempt began — candidate passed gates, order placement
    started. Resets the ready-streak (an action was taken this heartbeat)."""
    if not symbol or symbol == "(all)":
        return
    with _decision_trace_lock:
        ent = _decision_trace.get(symbol) or _new_trace_entry()
        _decision_trace[symbol] = ent
        ent["last_attempt_ts"] = time.time()
        ent["_ready_since_hb"] = _decision_heartbeat
        ent["_last_touch_hb"] = _decision_heartbeat
        _decision_gap_warned.pop(symbol, None)


def _trace_mark_block(symbol: str, reason: str) -> None:
    """Most recent reason the symbol did NOT buy (reuses existing reason
    strings). Resets the ready-streak (the cycle produced a block reason)."""
    if not symbol or symbol == "(all)":
        return
    with _decision_trace_lock:
        ent = _decision_trace.get(symbol)
        if ent is None:
            ent = _new_trace_entry()
            _decision_trace[symbol] = ent
        ent["last_block_reason"] = reason
        ent["last_block_ts"] = time.time()
        ent["engine_ready"] = False
        ent["_ready_since_hb"] = _decision_heartbeat
        ent["_last_touch_hb"] = _decision_heartbeat
        _decision_gap_warned.pop(symbol, None)


def _maybe_warn_decision_gap_locked(symbol: str, ent: dict) -> None:
    """J1.3 — a symbol buy-ready for >2 heartbeats with NO attempt advance and
    NO block reason is a bug (the operator's 'green but nothing happens'). Emit
    ONE transition-only WARN. Caller holds _decision_trace_lock."""
    since = ent.get("_ready_since_hb", _decision_heartbeat)
    n = _decision_heartbeat - since
    if n <= _DECISION_GAP_HEARTBEATS:
        return
    if _decision_gap_warned.get(symbol) == since:
        return  # already warned for this stuck streak
    _decision_gap_warned[symbol] = since
    try:
        database.log_activity(
            f"DECISION-GAP: {symbol} buy-ready for {n} cycles with no attempt "
            f"and no block reason", "warn")
    except Exception:
        pass


def get_decision_trace() -> Dict[str, dict]:
    """Public reader (control_api): shallow copy of the per-symbol decision
    trace. Raw ts fields are returned as-is; control_api computes ages. Internal
    bookkeeping fields (underscore-prefixed) are stripped."""
    out: Dict[str, dict] = {}
    with _decision_trace_lock:
        for sym, ent in _decision_trace.items():
            out[sym] = {k: v for k, v in ent.items() if not k.startswith("_")}
    return out


# ── L1.2 — entry-funnel stage counters ───────────────────────────────────────
# Per-UTC-day counts of how many symbol-evaluations reached each buy-pipeline
# stage, so "coins staying on buy" always decomposes into a named stage instead
# of silently vanishing. Stages (in funnel order):
#   ready              → symbol was buy-ready / a candidate this heartbeat
#   fresh_recheck_pass → passed the fresh re-evaluation before committing
#   budget_pass        → resolved budget >= tradeable min
#   order_posted       → a maker/taker order was actually placed
#   filled             → the entry confirmed (qty > 0)
# NOTE: budget resolution happens upstream of the fresh re-check in this
# pipeline, so budget_pass may exceed fresh_recheck_pass — each counter is
# incremented at the point its own condition is genuinely determined, not
# forced into strict funnel monotonicity. O(1) per event (a dict increment
# under a lock); NO per-event rows are persisted. Counters roll over at the
# UTC-day boundary, keeping the previous day's snapshot for comparison.
_FUNNEL_STAGES = ("ready", "fresh_recheck_pass", "budget_pass",
                  "order_posted", "filled")
_funnel_lock = threading.Lock()


def _funnel_utc_day() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


_funnel_counts: Dict[str, int] = {s: 0 for s in _FUNNEL_STAGES}
_funnel_day: str = _funnel_utc_day()
_funnel_prev: Dict[str, int] = {s: 0 for s in _FUNNEL_STAGES}
_funnel_prev_day: Optional[str] = None


def _funnel_rollover_locked() -> None:
    """Roll counters to a new UTC day, snapshotting the finished day into
    _funnel_prev. Caller holds _funnel_lock."""
    global _funnel_counts, _funnel_day, _funnel_prev, _funnel_prev_day
    today = _funnel_utc_day()
    if today != _funnel_day:
        _funnel_prev = dict(_funnel_counts)
        _funnel_prev_day = _funnel_day
        _funnel_counts = {s: 0 for s in _FUNNEL_STAGES}
        _funnel_day = today


def _funnel_incr(stage: str) -> None:
    """Increment one entry-funnel stage counter for today (UTC). O(1); an
    unknown stage is ignored so a typo can never raise on the hot buy path."""
    with _funnel_lock:
        if stage not in _funnel_counts:
            return
        _funnel_rollover_locked()
        _funnel_counts[stage] += 1


def get_funnel_stats() -> dict:
    """L1.2 public reader: today's entry-funnel stage counts plus yesterday's
    snapshot. Shape:
      {day, ready, fresh_recheck_pass, budget_pass, order_posted, filled,
       prev_day: {day, ready, fresh_recheck_pass, budget_pass,
                  order_posted, filled}}"""
    with _funnel_lock:
        _funnel_rollover_locked()
        out: dict = {"day": _funnel_day}
        out.update({s: _funnel_counts[s] for s in _FUNNEL_STAGES})
        prev: dict = {"day": _funnel_prev_day}
        prev.update({s: _funnel_prev.get(s, 0) for s in _FUNNEL_STAGES})
        out["prev_day"] = prev
        return out


def _record_rejection(symbol: str, score, reason: str, detail: str = ""):
    # NOTE: no score filter here — engine-path rejections carry the ENGINE
    # score (often 0-2) and systematic gates (pre-gate, min-notional, vetoes)
    # must be visible in diagnostics regardless of the legacy score.
    try:
        with _rejection_lock:
            _rejection_counts[reason] += 1
            ex_list = _rejection_examples.setdefault(reason, [])
            ex_list.append({
                "ts": time.time(),
                "symbol": symbol,
                "score": int(score or 0),
                "detail": str(detail)[:200],
            })
            if len(ex_list) > 5:
                ex_list.pop(0)
    except Exception:
        pass
    try:
        database.record_buy_rejection(symbol, reason, str(detail)[:500] if detail else None, int(score or 0), None)
    except Exception:
        pass
    # Phase 1 §1.3: near-miss entry snapshot (throttled; never raises).
    _snapshot_near_miss(symbol, reason, detail, score)
    # J1 — feed the per-symbol decision trace with the existing reason string
    # (global "(all)" rejections are ignored — they aren't per-symbol blocks).
    _trace_mark_block(symbol, reason)

def _mark_ready_no_slot(strategy: dict, k: int, n: int) -> None:
    """Q1 — when the buy check returns early at capacity (before the per-symbol
    loop), every currently buy-ready candidate would otherwise be left with NO
    attempt and NO block reason → a silent DECISION-GAP. Stamp each ready coin
    with an explicit machine-readable 'waiting: no free slot (k/n)' block so the
    capacity throttle always shows a state. Never raises on the hot path."""
    try:
        min_sigs = int(strategy.get("min_signals", config.MIN_SIGNALS_TO_BUY))
        approved = {c["symbol"] for c in strategy.get("approved_coins", [])
                    if c.get("approved")}
        with _signal_cache_lock:
            snap = dict(_signal_cache)
        reason = f"waiting: no free slot ({k}/{n})"
        for s, v in snap.items():
            if s in approved and v.get("score", 0) >= min_sigs:
                _trace_mark_block(s, reason)
    except Exception:
        pass


def get_rejection_stats() -> dict:
    with _rejection_lock:
        return {
            "counts":   dict(_rejection_counts),
            "examples": {k: list(v) for k, v in _rejection_examples.items()},
            "reset_ts": _rejection_reset_ts,
        }


def clear_rejection_stats() -> int:
    global _rejection_reset_ts
    with _rejection_lock:
        n = sum(_rejection_counts.values())
        _rejection_counts.clear()
        _rejection_examples.clear()
        _rejection_reset_ts = time.time()
        return n


def evaluate_buy_gates(sym: str) -> dict:
    """Mirror of the per-symbol gate chain in _check_buys_from_cache, WITHOUT
    executing anything. Used by /api/signals-summary so the frontend only shows
    a coin as a buy candidate when the bot would ACTUALLY buy it right now —
    a coin that passes the signal score but fails a veto (5m downtrend, BB top,
    cooldown, macro gate, paused bot…) must not display as 'BUY'.

    Returns {"ready": bool, "blockers": [str, ...]} — blockers lists every gate
    currently failing (empty when ready). Transient gates the buy loop retries
    on its own (buy stagger, per-scan cap) are intentionally excluded.
    """
    blockers: list = []
    strategy = _load_strategy()

    if not strategy.get("trading_active", True):
        blockers.append("bot_paused")
    if get_mode() == "live" and connection.is_using_paper_fallback():
        blockers.append("live_connection_down")

    # §4.1/A3: capacity is the auto-degraded effective slot count, not the raw
    # configured max_positions — the blocker shows open/EFFECTIVE.
    _slots = effective_slots()
    max_pos = _slots["effective_slots"]
    with _positions_lock:
        n_open = len(_positions)
        already_held = any(p["symbol"] == sym for p in _positions)
    if n_open >= max_pos:
        blockers.append(f"max_positions ({n_open}/{max_pos})")
    if already_held:
        blockers.append("already_held")

    approved = {c["symbol"] for c in strategy.get("approved_coins", []) if c.get("approved")}
    if approved and sym not in approved:
        blockers.append("not_in_approved_coins")

    if _in_cooldown(sym):
        blockers.append("cooldown_after_loss")
    if _loss_cooldown.get(sym, 0) > time.time():
        blockers.append("slippage_loss_cooldown")
    if _global_stop_pause_active():
        blockers.append("global_stop_pause")

    # ── Phase 4 §4.2/§4.3/§4.5 circuit breakers (mirror of the buy loop) ────
    if _check_daily_loss_stop():
        blockers.append("daily_loss_stop")
    if time.time() < _consec_loss_pause_until:
        blockers.append("consecutive_loss_pause")
    _slip_avg = _slippage_veto_active(sym)
    if _slip_avg is not None:
        blockers.append(f"slippage_veto (avg {_slip_avg:.0f}bps)")
    if _corr_guard_state()["blocked"]:
        blockers.append("btc_red_entry_limit")

    # F2.1: the legacy binary macro gate is gone — surface the 3-state regime
    # veto directly. Only an actual risk_off classification blocks buys (mirrors
    # the REGIME_risk_off registry veto); neutral only downsizes, never blocks.
    if get_btc_regime() == "risk_off":
        blockers.append("regime_risk_off")

    with _signal_cache_lock:
        cached = _signal_cache.get(sym)
    if not cached:
        blockers.append("no_signal_data")
        return {"ready": False, "blockers": blockers}

    min_sigs = int(strategy.get("min_signals", config.MIN_SIGNALS_TO_BUY))
    signal_engine_active = (
        SIGNAL_REGISTRY_AVAILABLE
        and bool(strategy.get("signal_engine", {}).get("enabled", False))
    )
    if signal_engine_active:
        # Mirror the fixed pipeline: when the engine is enabled it is the sole
        # signal authority — no legacy-score blocker. Surface the engine's own
        # decision so the UI matches what the buy loop would actually do.
        try:
            _sig_data = {
                **cached.get("signals", {}),
                "rsi_value":       cached.get("rsi_val", 0.0),
                "current_price":   cached.get("price", 0.0),
                "low_24h":         cached.get("low_24h"),
                "klines_1m":       cached.get("klines_1m", []),
                "stoch_rsi_value": cached.get("stoch_rsi_val"),
                # Phase 3 §3.1/§3.3 fields (populated on 5m closes)
                "klines_5m":       cached.get("klines_5m", []),
                "ema50_15m_slope": cached.get("ema50_15m_slope"),
                "bb_position_5m":  cached.get("bb_position_5m"),
                "atr_pct":         cached.get("atr_pct"),
                "btc_regime":      get_btc_regime(),
            }
            _dec = _sr_evaluate_buy_decision(sym, _sig_data, strategy)
            if not _dec.get("allowed"):
                blockers.append(f"engine:{_dec.get('reason', 'blocked')}")
        except Exception:
            pass
    else:
        if cached.get("score", 0) < min_sigs:
            blockers.append(f"score {cached.get('score', 0)}/{min_sigs}")
        if bool(strategy.get("mandatory_signals_enabled", True)):
            sigs = cached.get("signals", {})
            rsi_v = cached.get("rsi_val", 0.0)
            # Single source of truth: nested signal_thresholds first, root
            # key as fallback — matches the buy loop's legacy mandatory gate.
            rsi_threshold = float(
                strategy.get("signal_thresholds", {}).get(
                    "rsi_buy_threshold", strategy.get("rsi_buy_threshold", 40.0)
                )
            )
            if not sigs.get("trend", False):
                blockers.append("mandatory_ema_down")
            if rsi_v <= 0 or rsi_v >= rsi_threshold:
                blockers.append(f"rsi {rsi_v:.0f} >= {rsi_threshold:.0f}")

    if not cached.get("bb_ok", True):
        blockers.append("price_at_upper_bollinger")
    if not cached.get("5m_ok", True):
        blockers.append("5m_downtrend")

    return {"ready": not blockers, "blockers": blockers}


# ── BTC market regime filter ───────────────────────────────────────────────────
_market_regime_cache: dict = {"ts": 0.0, "regime": "unknown", "details": {}}
_MARKET_REGIME_TTL_SEC = 120.0

def _fetch_btc_1h_klines() -> Optional[List[dict]]:
    # C §6.4 — non-critical background fetch: when can_spend refuses, callers
    # (get_market_regime / get_btc_state) fall back to their cached values.
    # limit=72: EMA50 on 1h needs >=50 closed candles (§3.3 3-state regime);
    # 72 gives EMA warmup headroom while staying a single weight-2 request.
    url = "https://data-api.binance.vision/api/v3/klines?symbol=BTCUSDT&interval=1h&limit=72"
    ok, data, _, _ = _binance_request(url, timeout=4.0, source="btc_klines",
                                      weight=2, critical=False)
    if not ok or not isinstance(data, list):
        return None
    try:
        return [{"open": float(k[1]), "high": float(k[2]), "low": float(k[3]),
                 "close": float(k[4]), "volume": float(k[5])} for k in data]
    except Exception:
        return None

def _ema_calc(values: List[float], period: int) -> float:
    if not values or period <= 0:
        return 0.0
    k = 2.0 / (period + 1.0)
    ema_val = values[0]
    for v in values[1:]:
        ema_val = v * k + ema_val * (1 - k)
    return ema_val

def get_market_regime() -> dict:
    """Returns regime: 'bullish'|'choppy'|'bearish'|'unknown'. Cached 2 min."""
    now = time.time()
    if (now - _market_regime_cache["ts"]) < _MARKET_REGIME_TTL_SEC:
        return _market_regime_cache
    klines = _fetch_btc_1h_klines()
    if not klines or len(klines) < 12:
        _market_regime_cache.update({"ts": now, "regime": "unknown", "details": {"error": "no_data"}})
        return _market_regime_cache
    closes = [k["close"] for k in klines]
    ema_8  = _ema_calc(closes, 8)
    ema_24 = _ema_calc(closes, 24)
    pct_24h = (closes[-1] - closes[0]) / closes[0] * 100 if closes[0] > 0 else 0
    pct_4h  = (closes[-1] - closes[-4]) / closes[-4] * 100 if closes[-4] > 0 else 0
    if ema_8 > ema_24 and pct_24h > 0.5:
        regime = "bullish"
    elif ema_8 < ema_24 and pct_24h < -2.0:
        regime = "bearish"
    elif pct_4h < -1.5:
        regime = "bearish"
    else:
        regime = "choppy"
    details = {"btc_price": closes[-1], "ema_8": round(ema_8, 2), "ema_24": round(ema_24, 2), "pct_24h": round(pct_24h, 3), "pct_4h": round(pct_4h, 3), "ts": now}
    _market_regime_cache.update({"ts": now, "regime": regime, "details": details})
    return _market_regime_cache


# ── §3.3 BTC regime — 3-state (risk_on / neutral / risk_off), 60 s cache ─────
# Computed from BTCUSDT 1h klines (single existing fetch): EMA structure
# (price vs EMA20 vs EMA50 on 1h) + 4h momentum. Thresholds (documented):
#   risk_off : price < EMA50  OR  pct_4h < -2.0   — clear downtrend / dump;
#              -2% in 4h matches the old binary "bearish" dump trigger.
#   risk_on  : price > EMA20 > EMA50  AND  pct_4h > -0.5 — intact uptrend
#              structure with at most mild short-term give-back.
#   neutral  : everything else (chop / transition) → entries allowed at
#              reduced size (strategy.regime.neutral_size_mult, default 0.5).
# risk_off is checked FIRST so a dump inside an uptrend still de-risks.
# Refreshed every 60 s (was 120 s for the old binary gate). Called from
# buy-check / executor threads — NEVER from the event loop.
_btc_state_cache: dict = {"ts": 0.0, "data": None}
_BTC_STATE_TTL_SEC = 60.0


def compute_btc_regime_from_closes(closes: List[float],
                                   risk_off_pct_4h: float = -1.0) -> Tuple[str, dict]:
    """Pure §3.3 classifier over 1h closes (oldest→newest). Returns
    (regime, details). Shared shape with backtest.btc_regime_from_closes —
    keep the thresholds in sync. <55 closes → neutral (EMA50 not meaningful).

    F2.2: risk_off requires BOTH a lagging-EMA break AND real 4h weakness —
    `price < ema_50 AND pct_4h < risk_off_pct_4h` (default −1.0). The old
    `price < ema_50 OR pct_4h < -2.0` flagged risk_off on any green day where
    BTC merely sat below the sluggish 50-EMA (e.g. +1.9% 24h / −0.09% 4h), which
    spammed the now-removed legacy macro gate. risk_on is unchanged."""
    if not closes or len(closes) < 55:
        return "neutral", {"error": "insufficient_1h_data", "n": len(closes or [])}
    price   = closes[-1]
    ema_20  = _ema_calc(closes, 20)
    ema_50  = _ema_calc(closes, 50)
    pct_4h  = (closes[-1] - closes[-5]) / closes[-5] * 100 if closes[-5] > 0 else 0.0
    if price < ema_50 and pct_4h < risk_off_pct_4h:
        regime = "risk_off"
    elif price > ema_20 > ema_50 and pct_4h > -0.5:
        regime = "risk_on"
    else:
        regime = "neutral"
    return regime, {
        "price":  price,
        "ema_20": round(ema_20, 2),
        "ema_50": round(ema_50, 2),
        "pct_4h": round(pct_4h, 3),
    }


# F2.3: last logged 3-state regime — transition-only logging (no per-eval spam).
_last_logged_regime: Optional[str] = None
_last_logged_regime_lock = threading.Lock()


def _log_btc_regime_transition(regime3: str, det: dict) -> None:
    """Log the BTC regime only when it CHANGES state. Idempotent per state."""
    global _last_logged_regime
    with _last_logged_regime_lock:
        prev = _last_logged_regime
        if regime3 == prev:
            return
        _last_logged_regime = regime3
    try:
        database.log_activity(
            f"BTC regime: {prev or 'unknown'} → {regime3} "
            f"(price={det.get('price')} ema20={det.get('ema_20')} "
            f"ema50={det.get('ema_50')} 4h={det.get('pct_4h')}%)", "info")
    except Exception:
        pass


def get_btc_state() -> Optional[dict]:
    """BTC macro state (legacy dict shape) for the UI. 60 s cache.
    Must run on a buy-check/worker thread (REST fetch).

    §3.3: the legacy binary "regime" key now maps from the 3-state classifier
    so macro_gate_enabled semantics are preserved: bearish ⇔ risk_off (gate
    blocks), bullish ⇔ risk_on, choppy ⇔ neutral. The 3-state value is carried
    alongside as "regime3"."""
    now = time.time()
    if _btc_state_cache["data"] and (now - _btc_state_cache["ts"]) < _BTC_STATE_TTL_SEC:
        return _btc_state_cache["data"]
    klines = _fetch_btc_1h_klines()
    if not klines or len(klines) < 12:
        return _btc_state_cache.get("data")
    closes = [k["close"] for k in klines]
    ema_8  = _ema_calc(closes, 8)
    ema_24 = _ema_calc(closes, 24)
    pct_24h = ((closes[-1] - closes[-25]) / closes[-25] * 100
               if len(closes) >= 25 and closes[-25] > 0
               else ((closes[-1] - closes[0]) / closes[0] * 100 if closes[0] > 0 else 0))
    pct_4h  = (closes[-1] - closes[-5]) / closes[-5] * 100 if closes[-5] > 0 else 0
    regime3, det = compute_btc_regime_from_closes(closes, _regime_risk_off_pct_4h())
    legacy = {"risk_off": "bearish", "risk_on": "bullish"}.get(regime3, "choppy")
    # F2.3: log BTC regime ONLY on a state change (risk_on↔neutral↔risk_off),
    # never per-evaluation — the old macro gate spammed a line every ~10s.
    _log_btc_regime_transition(regime3, det)
    data = {
        "price":   closes[-1],
        "ema_8":   round(ema_8, 2),
        "ema_24":  round(ema_24, 2),
        "ema_20":  det.get("ema_20"),
        "ema_50":  det.get("ema_50"),
        "pct_4h":  round(pct_4h, 3),
        "pct_24h": round(pct_24h, 3),
        "regime":  legacy,     # legacy binary-gate key: bearish ⇔ risk_off
        "regime3": regime3,    # §3.3 3-state key
    }
    _btc_state_cache["ts"]   = now
    _btc_state_cache["data"] = data
    return data


def get_btc_regime(detailed: bool = False):
    """§3.3 public accessor: 'risk_on' | 'neutral' | 'risk_off' (60 s cache).
    detailed=True returns the full state dict instead. Unknown/unfetchable →
    'neutral' (fail-open for sizing; the REGIME veto fails open separately)."""
    state = None
    try:
        state = get_btc_state()
    except Exception:
        state = None
    if detailed:
        return state
    return (state or {}).get("regime3") or "neutral"


# ═════════════════════════════════════════════════════════════════════════════
# Phase 4 §4.1-§4.5 — sizing + risk management (WolfBot v0.4)
# ═════════════════════════════════════════════════════════════════════════════

# §4.2/§4.3/§4.5 config — strategy.risk block. Defaults per spec.
_RISK_DEFAULTS: Dict[str, float] = {
    "daily_loss_stop_pct":           2.0,    # §4.2a: daily stop at −2% of allocation
    "flatten_on_stop":               False,  # §4.2a: force-sell everything on trip
    "max_consecutive_losses":        4,      # §4.2b: pause buys 60 min at this count
    "max_avg_slippage_bps":          15.0,   # §4.3: per-symbol rolling-avg veto
    "max_new_entries_when_btc_red":  2,      # §4.5: entries per 5 min while BTC red
}


def _risk_cfg() -> dict:
    """strategy.risk with defaults — hot-reloadable via _load_strategy."""
    raw = _load_strategy().get("risk")
    raw = raw if isinstance(raw, dict) else {}
    cfg: dict = {}
    for key, default in _RISK_DEFAULTS.items():
        val = raw.get(key, default)
        try:
            cfg[key] = bool(val) if isinstance(default, bool) else float(val)
        except (TypeError, ValueError):
            cfg[key] = default
    return cfg


def _risk_db_rows(sql: str, params: tuple) -> list:
    """Small read-only query helper against the trades DB (risk features only).
    Uses database's own lock/connection factory; returns [] on any error —
    risk checks must never crash the buy/sell paths."""
    try:
        with database._lock:
            conn = database._conn()
            try:
                rows = conn.execute(sql, params).fetchall()
            finally:
                conn.close()
        return [dict(r) for r in rows]
    except Exception:
        return []


# ── §4.1 + A3 — sizing: auto-degrade slots ────────────────────────────────────
# NEW strategy block:
#   strategy.sizing = {"max_positions": 9, "min_position_usdt": 10}   (A4 defaults)
# LEGACY FALLBACK (documented): when the sizing block is ABSENT, the root
# strategy.max_positions key (default 10, as before) still wins — existing
# configs keep their exact behavior until the UI writes the new block.
# A3: when the allocation can't fund max_positions slots of min_position_usdt
# each, the SLOT COUNT degrades (min(max_positions, floor(alloc/min))) instead
# of refusing to arm. Budget-mode formulas are untouched.

_SIZING_DEFAULTS = {"max_positions": 9, "min_position_usdt": 10.0}
_eff_balance_cache: dict = {"ts": 0.0, "free": 0.0}
_EFF_BALANCE_TTL_SEC = 30.0
_slots_last_logged: dict = {"slots": None, "ts": 0.0}
_SLOTS_LOG_THROTTLE_SEC = 60.0


def _sizing_cfg() -> dict:
    strategy = _load_strategy()
    sizing = strategy.get("sizing")
    if isinstance(sizing, dict):
        try:
            max_pos = int(sizing.get("max_positions", _SIZING_DEFAULTS["max_positions"]))
        except (TypeError, ValueError):
            max_pos = _SIZING_DEFAULTS["max_positions"]
        try:
            min_pos = float(sizing.get("min_position_usdt", _SIZING_DEFAULTS["min_position_usdt"]))
        except (TypeError, ValueError):
            min_pos = _SIZING_DEFAULTS["min_position_usdt"]
    else:
        # Legacy fallback: root max_positions wins when the sizing block is absent.
        try:
            max_pos = int(strategy.get("max_positions", 10))
        except (TypeError, ValueError):
            max_pos = 10
        min_pos = _SIZING_DEFAULTS["min_position_usdt"]
    return {"max_positions": max(0, max_pos), "min_position_usdt": max(0.0, min_pos)}


def _free_balance_snapshot() -> float:
    """USDT free balance, cached 30 s (live mode = REST call)."""
    now = time.time()
    if now - _eff_balance_cache["ts"] < _EFF_BALANCE_TTL_SEC:
        return float(_eff_balance_cache["free"])
    try:
        free = float(_get_usdt_balance())
    except Exception:
        return float(_eff_balance_cache["free"])
    _eff_balance_cache["ts"] = now
    _eff_balance_cache["free"] = free
    return free


def effective_slots() -> dict:
    """§4.1/A3 — slot capacity from capital, degrading instead of refusing.

    effective_allocation = bot_allocation_usdt when > 0 (same allocation the
    budget math in get_budget_for_coin enforces), else the bot's total capital
    snapshot (free USDT + USDT deployed in open positions — free alone would
    shrink the slot count as capital deploys and block valid entries).
    effective_slots = min(max_positions, floor(effective_allocation /
    min_position_usdt)), floored at 0. Slot-count changes are logged (throttled).
    """
    cfg = _sizing_cfg()
    max_pos, min_pos = cfg["max_positions"], cfg["min_position_usdt"]
    strategy = _load_strategy()
    try:
        allocation = float(strategy.get("bot_allocation_usdt", config.BOT_ALLOCATION_USDT))
    except (TypeError, ValueError):
        allocation = 0.0
    if allocation > 0:
        eff_alloc = allocation
    else:
        with _positions_lock:
            deployed = sum(float(p.get("budget_usdt") or 0.0) for p in _positions)
        eff_alloc = _free_balance_snapshot() + deployed
    if min_pos > 0:
        eff = min(max_pos, int(math.floor(eff_alloc / min_pos)))
    else:
        eff = max_pos
    eff = max(0, eff)
    # Log slot degradation on CHANGE (throttled) — A3 audit trail.
    now = time.time()
    prev = _slots_last_logged["slots"]
    if prev is not None and prev != eff and now - _slots_last_logged["ts"] >= _SLOTS_LOG_THROTTLE_SEC:
        _slots_last_logged["ts"] = now
        try:
            database.log_activity(
                f"SIZING: running {prev} → {eff} slots: allocation {eff_alloc:.2f} "
                f"(min_position_usdt={min_pos:.2f}, max_positions={max_pos})",
                "warn" if eff < prev else "info")
        except Exception:
            pass
    _slots_last_logged["slots"] = eff
    return {
        "max_positions":        max_pos,
        "min_position_usdt":    min_pos,
        "effective_allocation": round(eff_alloc, 2),
        "effective_slots":      eff,
        "degraded":             eff < max_pos,
    }


# ── §4.2a — daily loss stop (circuit breaker that ACTS) ───────────────────────
# Realized PnL for the current UTC day + fee-adjusted unrealized PnL of open
# positions. When total <= −daily_loss_stop_pct% × effective_allocation, BUYS
# pause until the next UTC midnight (module state _daily_stop_until). EXITS
# KEEP RUNNING (only the buy path consults the breaker). flatten_on_stop=true
# additionally force-sells every open position ONCE through the normal force
# path. Evaluated lazily in the buy path with a 30 s cache — no new thread.
# Manual resume: resume_daily_stop() (called by control_api) clears the pause
# and suppresses re-tripping for the rest of the same UTC day.

_daily_stop_until: float = 0.0
_daily_stop_lock = threading.Lock()
_daily_pnl_cache: dict = {"ts": 0.0, "data": None}
_DAILY_PNL_TTL_SEC = 30.0
_daily_stop_logged_day: str = ""      # ERROR log fired once per UTC day
_daily_stop_flattened_day: str = ""   # flatten fired once per UTC day
_daily_stop_resumed_day: str = ""     # manual resume disarms re-trip for the day
# I3.4 — transition-only logging: True while the daily stop is latched. Set when
# the stop trips (ERROR logged once), cleared when it releases (INFO logged
# once). The persistent "stopped" flag for the UI stays in get_risk_status();
# the log stream only carries the two transition lines, not a 60 s heartbeat.
_daily_stop_logged: bool = False
# J2 — trip context captured when the daily stop trips (pnl/limit at trip),
# persisted alongside the latch so a restart can show why it was armed.
_daily_stop_trip_ctx: dict = {}


def _utc_day_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _next_utc_midnight_ts() -> float:
    from datetime import timedelta as _td
    now_dt = datetime.now(timezone.utc)
    nxt = (now_dt + _td(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    return nxt.timestamp()


def _daily_limit_snapshot() -> tuple:
    """(limit_pct, limit_usdt) resolved FRESH from strategy.json on every call
    (Part E: a UI edit to risk.daily_loss_stop_pct must apply immediately, not
    after the 30 s PnL cache expires). effective_slots() reads config fresh
    too; only its balance snapshot is cached."""
    slots = effective_slots()
    try:
        pct = float(_risk_cfg()["daily_loss_stop_pct"])
    except Exception:
        pct = _RISK_DEFAULTS["daily_loss_stop_pct"]
    limit_usdt = max(0.0, pct / 100.0 * float(slots["effective_allocation"]))
    return pct, round(limit_usdt, 6)


def _daily_pnl_state() -> dict:
    """Realized (trades table, timestamp_sell in today UTC, current mode) +
    unrealized (open positions, fee-adjusted estimate at freshest price) PnL.
    PnL numbers are cached 30 s; limit_pct/limit_usdt are recomputed from the
    hot config on EVERY call so risk.daily_loss_stop_pct edits apply without
    waiting out the cache. Never raises."""
    now = time.time()
    cached = _daily_pnl_cache.get("data")
    if cached and now - _daily_pnl_cache["ts"] < _DAILY_PNL_TTL_SEC:
        pct, limit_usdt = _daily_limit_snapshot()
        return {**cached, "limit_pct": pct, "limit_usdt": limit_usdt}
    day = _utc_day_str()
    rows = _risk_db_rows(
        "SELECT COALESCE(SUM(net_profit), 0.0) AS pnl FROM trades "
        "WHERE mode = ? AND substr(timestamp_sell, 1, 10) = ?",
        (get_mode(), day))
    realized = float(rows[0]["pnl"]) if rows else 0.0
    unrealized = 0.0
    try:
        import data_collector as _dc_pnl
        with _positions_lock:
            snap = [dict(p) for p in _positions]
        for p in snap:
            sym = p.get("symbol", "")
            entry = float(p.get("entry_price") or 0)
            qty = float(p.get("quantity") or 0)
            buy_fee = float(p.get("buy_fee_usdt") or 0)
            px = float(_dc_pnl.prices.get(sym) or _rest_px.get(sym, 0) or entry)
            if entry <= 0 or qty <= 0 or px <= 0:
                continue
            # Fee-adjusted estimate: what selling at px would net vs cost.
            proceeds = px * qty * (1.0 - _fee_rate_for(sym))
            unrealized += proceeds - (qty * entry + buy_fee)
    except Exception:
        pass
    pct, limit_usdt = _daily_limit_snapshot()
    data = {
        "day":        day,
        "realized":   round(realized, 6),
        "unrealized": round(unrealized, 6),
        "pnl_today":  round(realized + unrealized, 6),
        "limit_pct":  pct,
        "limit_usdt": limit_usdt,
    }
    _daily_pnl_cache["ts"] = now
    _daily_pnl_cache["data"] = data
    return data


def _flatten_all_positions(context: str) -> int:
    """Force-sell every open position through the normal force path.
    Returns the number of sells dispatched."""
    try:
        import data_collector as _dc_fl
        px_map = _dc_fl.prices
    except Exception:
        px_map = {}
    with _positions_lock:
        snap = list(_positions)
    dispatched = 0
    for pos in snap:
        sym = pos.get("symbol", "")
        if not sym:
            continue
        with _selling_lock:
            if sym in _selling:
                continue
            _selling.add(sym)
            _selling_ts[sym] = time.time()
        price = float(px_map.get(sym, 0) or _rest_px.get(sym, 0)
                      or pos.get("entry_price", 0) or 0)
        _sell_executor.submit(_execute_sell, pos, price, "force-sell")
        dispatched += 1
    try:
        database.log_activity(
            f"FLATTEN ({context}): dispatched force-sell for {dispatched} open position(s)",
            "error")
    except Exception:
        pass
    return dispatched


def _check_daily_loss_stop() -> bool:
    """Lazy §4.2a breaker evaluation (30 s cached PnL). Returns True while
    buys must stay paused. Exits are unaffected — only buy paths call this."""
    global _daily_stop_until, _daily_stop_logged_day, _daily_stop_flattened_day
    global _daily_stop_logged, _daily_stop_trip_ctx
    now = time.time()
    if now < _daily_stop_until:
        return True
    cfg = _risk_cfg()
    if cfg["daily_loss_stop_pct"] <= 0:
        return False
    day = _utc_day_str()
    if _daily_stop_resumed_day == day:
        return False  # manually resumed — do not re-trip today
    st = _daily_pnl_state()
    if st["limit_usdt"] <= 0 or st["pnl_today"] > -st["limit_usdt"]:
        return False
    with _daily_stop_lock:
        if now < _daily_stop_until:          # lost the race — already tripped
            return True
        _daily_stop_until = _next_utc_midnight_ts()
        first_trip_today = _daily_stop_logged_day != day
        _daily_stop_logged_day = day
        _daily_stop_logged = True   # latched — release will log the INFO line
        # J2 — capture the trip context and persist the latch so a restart
        # before UTC midnight reloads (rather than silently clearing) it.
        _daily_stop_trip_ctx = {
            "day":        day,
            "pnl_today":  st.get("pnl_today"),
            "limit_usdt": st.get("limit_usdt"),
            "limit_pct":  st.get("limit_pct"),
        }
    persist_risk_latches()
    if first_trip_today:
        try:
            database.log_activity(
                f"DAILY LOSS STOP: today's PnL {st['pnl_today']:.2f} USDT "
                f"(realized {st['realized']:.2f} + unrealized {st['unrealized']:.2f}) "
                f"breached −{st['limit_pct']}% of allocation "
                f"({st['limit_usdt']:.2f} USDT) — BUYS PAUSED until next UTC "
                f"midnight. Exits keep running.", "error")
        except Exception:
            pass
        try:
            log_diag_issue("risk", "error",
                           f"daily_loss_stop tripped: pnl_today={st['pnl_today']:.2f} "
                           f"limit={st['limit_usdt']:.2f}")
        except Exception:
            pass
    if cfg["flatten_on_stop"] and _daily_stop_flattened_day != day:
        _daily_stop_flattened_day = day
        _flatten_all_positions("daily_loss_stop")
    return True


def resume_daily_stop() -> dict:
    """Manual resume (control_api): clears the daily-stop pause and disarms
    re-tripping for the remainder of the current UTC day."""
    global _daily_stop_until, _daily_stop_resumed_day, _daily_stop_logged
    was_stopped = time.time() < _daily_stop_until
    _daily_stop_until = 0.0
    _daily_stop_resumed_day = _utc_day_str()
    _daily_pnl_cache["ts"] = 0.0   # force fresh state on next check
    # I3.4: this IS the release transition — clear the latch so the buy loop
    # does not also emit a release line, and log it once here.
    _daily_stop_logged = False
    if was_stopped:
        try:
            database.log_activity(
                "DAILY LOSS STOP: manually resumed — buys re-enabled "
                "(breaker disarmed for the rest of today UTC)", "info")
        except Exception:
            pass
    # J2 — persist the release (records resumed_day so a restart today does not
    # re-arm) and clears the stored active latch.
    persist_risk_latches()
    return {"resumed": was_stopped, "day": _daily_stop_resumed_day}


# ── §4.2b — consecutive-loss pause ────────────────────────────────────────────
# In-memory counter of consecutive losing closed trades (net_profit < 0),
# lazily seeded from the DB (last trades of the current mode — equivalent to
# startup seeding since the first access happens on the first buy check or
# trade close). At >= risk.max_consecutive_losses → buys pause 60 min. Any
# non-losing close resets the counter.

_consec_lock = threading.Lock()
_consec_losses: Optional[int] = None       # None → not yet seeded from DB
_consec_loss_pause_until: float = 0.0
_CONSEC_PAUSE_SEC = 3600.0                 # 60 min
# I3.4 — transition-only logging for the consecutive-loss pause: True while the
# pause is latched (ERROR logged once when armed), cleared + INFO logged once
# when it expires. No 60 s heartbeat while latched.
_consec_pause_logged: bool = False
# K1.1 — trigger context captured at the moment the pause arms (counter value,
# wall-clock trip time, and the losing streak's last symbol if known). Persisted
# alongside the latch so a restart / the UI can explain WHY buys are paused.
_consec_trip_ctx: Optional[dict] = None


def _seed_consec_losses_locked() -> int:
    """Seed from the DB: count leading losses in the last 50 closed trades of
    the current mode. Caller holds _consec_lock."""
    rows = _risk_db_rows(
        "SELECT net_profit FROM trades WHERE mode = ? ORDER BY id DESC LIMIT 50",
        (get_mode(),))
    n = 0
    for r in rows:
        np_ = r.get("net_profit")
        if np_ is not None and float(np_) < 0:
            n += 1
        else:
            break
    return n


def _consec_loss_count() -> int:
    global _consec_losses
    with _consec_lock:
        if _consec_losses is None:
            _consec_losses = _seed_consec_losses_locked()
        return _consec_losses


def _maybe_trigger_consec_pause_locked(count: int,
                                       last_symbol: Optional[str] = None) -> None:
    """Arm the 60-min pause when the counter reaches the limit. Caller holds
    _consec_lock. K1.1 — fires reliably right after every counter increment;
    captures trigger context so the latch can explain itself across restarts."""
    global _consec_loss_pause_until, _consec_pause_logged, _consec_trip_ctx
    try:
        limit = int(_risk_cfg()["max_consecutive_losses"])
    except Exception:
        limit = int(_RISK_DEFAULTS["max_consecutive_losses"])
    now = time.time()
    if limit > 0 and count >= limit and now >= _consec_loss_pause_until:
        _consec_loss_pause_until = now + _CONSEC_PAUSE_SEC
        _consec_pause_logged = True   # latched — expiry will log the INFO line
        _consec_trip_ctx = {"count": count, "trip_ts": now,
                            "last_symbol": last_symbol}
        try:
            database.log_activity(
                f"CONSECUTIVE-LOSS PAUSE: {count} losing trades in a row "
                f"(limit {limit}) — buys paused for "
                f"{int(_CONSEC_PAUSE_SEC // 60)} min", "error")
        except Exception:
            pass
        # J2 — persist the pause expiry so a restart within the window reloads it.
        persist_risk_latches()


# ── J2 — persist risk latches across restarts ─────────────────────────────────
# A process restart used to silently clear the daily-loss latch and the
# consecutive-loss pause, so with several deploys/day the breaker was
# decorative. We persist both latches to a small table OWNED by this file and
# reload them at boot — but ONLY if each latch is still valid by its OWN rule
# (daily stop: until_ts in the future AND same UTC day; consec pause: expiry in
# the future). A latch whose rule already expired is NOT restored — it releases
# by its rule, never by restart, and never wrongly re-arms.
_RISK_LATCH_KEYS = ("daily_stop", "consec_pause")


def _risk_latch_ensure_table_locked(conn) -> None:
    """Create the risk_latches table idempotently. Caller holds database._lock."""
    conn.execute(
        "CREATE TABLE IF NOT EXISTS risk_latches("
        "key TEXT PRIMARY KEY, active INTEGER, until_ts REAL, "
        "context TEXT, updated_ts REAL)")


def persist_risk_latches() -> None:
    """Write the current daily-stop and consecutive-loss pause state to the DB.
    Called on every latch trip/release. Never raises."""
    try:
        now = time.time()
        daily_active = 1 if _daily_stop_until > now else 0
        daily_ctx = _json.dumps({
            "day":           _daily_stop_logged_day,
            "resumed_day":   _daily_stop_resumed_day,
            "flattened_day": _daily_stop_flattened_day,
            "pnl_today":     (_daily_stop_trip_ctx or {}).get("pnl_today"),
            "limit_usdt":    (_daily_stop_trip_ctx or {}).get("limit_usdt"),
            "limit_pct":     (_daily_stop_trip_ctx or {}).get("limit_pct"),
        })
        consec_active = 1 if _consec_loss_pause_until > now else 0
        # K1.2 — persist the full trip context while the pause is armed so a
        # restart can explain WHY buys are paused; fall back to the bare count.
        if consec_active and _consec_trip_ctx:
            consec_ctx = _json.dumps({"count": _consec_losses, **_consec_trip_ctx})
        else:
            consec_ctx = _json.dumps({"count": _consec_losses})
        rows = [
            ("daily_stop",   daily_active,  _daily_stop_until,        daily_ctx,  now),
            ("consec_pause", consec_active, _consec_loss_pause_until, consec_ctx, now),
        ]
        with database._lock:
            conn = database._conn()
            try:
                _risk_latch_ensure_table_locked(conn)
                conn.executemany(
                    "INSERT INTO risk_latches(key, active, until_ts, context, updated_ts) "
                    "VALUES(?,?,?,?,?) "
                    "ON CONFLICT(key) DO UPDATE SET active=excluded.active, "
                    "until_ts=excluded.until_ts, context=excluded.context, "
                    "updated_ts=excluded.updated_ts", rows)
                conn.commit()
            finally:
                conn.close()
    except Exception:
        pass


def load_risk_latches() -> None:
    """Boot-time restore (control_api calls this once before the engine loop).
    Restores each latch ONLY if still valid by its own rule. Safe to call before
    the engine starts and a clean no-op when the table/rows are missing."""
    global _daily_stop_until, _daily_stop_logged_day, _daily_stop_logged
    global _daily_stop_flattened_day, _daily_stop_resumed_day, _daily_stop_trip_ctx
    global _consec_loss_pause_until, _consec_pause_logged, _consec_losses
    global _consec_trip_ctx
    try:
        with database._lock:
            conn = database._conn()
            try:
                _risk_latch_ensure_table_locked(conn)
                rows = conn.execute(
                    "SELECT key, active, until_ts, context FROM risk_latches").fetchall()
            finally:
                conn.close()
    except Exception:
        return
    if not rows:
        return
    now = time.time()
    today = _utc_day_str()
    for r in rows:
        try:
            key = r["key"]
            until = float(r["until_ts"] or 0.0)
            try:
                ctx = _json.loads(r["context"] or "{}") or {}
            except Exception:
                ctx = {}
        except Exception:
            continue
        if key == "daily_stop":
            resumed_day = ctx.get("resumed_day") or ""
            # A manual resume today disarms re-tripping for the rest of the day —
            # restore that fact even though the latch itself is not active.
            if resumed_day == today:
                _daily_stop_resumed_day = today
                continue
            trip_day = ctx.get("day") or ""
            # Rule: restore only if the pause has not elapsed AND we are still on
            # the same UTC day it was tripped (UTC midnight passing releases it).
            if until > now and trip_day == today:
                _daily_stop_until = until
                _daily_stop_logged_day = trip_day
                _daily_stop_flattened_day = ctx.get("flattened_day") or ""
                # Re-arm without re-logging the trigger ERROR: set the latched
                # flag True so the release transition still fires once later.
                _daily_stop_logged = True
                _daily_stop_trip_ctx = {
                    "day":        trip_day,
                    "pnl_today":  ctx.get("pnl_today"),
                    "limit_usdt": ctx.get("limit_usdt"),
                    "limit_pct":  ctx.get("limit_pct"),
                }
                try:
                    _until_iso = datetime.fromtimestamp(
                        until, timezone.utc).isoformat()
                    database.log_activity(
                        f"restored daily-stop latch, active until {_until_iso}",
                        "info")
                except Exception:
                    pass
        elif key == "consec_pause":
            # Rule: restore only if the pause expiry is still in the future.
            if until > now:
                _consec_loss_pause_until = until
                _consec_pause_logged = True
                _cnt = ctx.get("count")
                if isinstance(_cnt, int):
                    _consec_losses = _cnt
                # Preserve the original trip context across the restart.
                _consec_trip_ctx = {
                    "count":       ctx.get("count"),
                    "trip_ts":     ctx.get("trip_ts"),
                    "last_symbol": ctx.get("last_symbol"),
                }
                try:
                    _until_iso = datetime.fromtimestamp(
                        until, timezone.utc).isoformat()
                    database.log_activity(
                        f"restored consecutive-loss pause latch, active until "
                        f"{_until_iso}", "info")
                except Exception:
                    pass


def consec_pause_boot_selfcheck() -> None:
    """K1.3 — fail-CLOSED boot guard for the consecutive-loss breaker.

    control_api calls this once at boot AFTER load_risk_latches(). The DB-derived
    streak (via _seed_consec_losses_locked — the same seed used everywhere) is the
    source of truth: if it already meets/exceeds risk.max_consecutive_losses and
    there is NO valid (future) pause active, engage a fresh 60-min pause right now,
    persist it, and log ONE WARN.

    This closes the exact hole live evidence exposed (consecutive_losses 8/4,
    paused_until=None): a restart seeds the counter from trade history but the arm
    check only ran on a *new* close, so a process that crossed the limit before its
    last restart would resume with the counter over the limit yet never gated."""
    global _consec_losses, _consec_loss_pause_until, _consec_pause_logged
    global _consec_trip_ctx
    try:
        limit = int(_risk_cfg()["max_consecutive_losses"])
    except Exception:
        limit = int(_RISK_DEFAULTS["max_consecutive_losses"])
    try:
        with _consec_lock:
            derived = _seed_consec_losses_locked()
            _consec_losses = derived
            now = time.time()
            if limit > 0 and derived >= limit and now >= _consec_loss_pause_until:
                _consec_loss_pause_until = now + _CONSEC_PAUSE_SEC
                _consec_pause_logged = True
                _consec_trip_ctx = {"count": derived, "trip_ts": now,
                                    "last_symbol": None}
                try:
                    database.log_activity(
                        f"CONSECUTIVE-LOSS PAUSE: boot self-check: {derived}/{limit} "
                        f"consecutive losses with no active pause — engaging fresh "
                        f"{int(_CONSEC_PAUSE_SEC // 60)}m pause (fail-closed)", "warn")
                except Exception:
                    pass
                persist_risk_latches()
    except Exception:
        pass


def _note_trade_closed(sym: str, net_profit: float) -> None:
    """Risk hook — called from every close path (_do_execute_sell and
    _finalize_managed_exit) AFTER the trade row is written. Updates the
    consecutive-loss counter (§4.2b) and refreshes the symbol's slippage
    window (§4.3). Never raises."""
    global _consec_losses
    try:
        _avg_slippage_bps(sym, force=True)
    except Exception:
        pass
    try:
        with _consec_lock:
            if _consec_losses is None:
                # First access: the just-closed trade is already in the DB —
                # seeding includes it, so do NOT also increment.
                _consec_losses = _seed_consec_losses_locked()
            elif net_profit < 0:
                _consec_losses += 1
            else:
                _consec_losses = 0
            _maybe_trigger_consec_pause_locked(_consec_losses, sym)
    except Exception:
        pass


# ── §4.3 — slippage auto-veto ─────────────────────────────────────────────────
# Rolling per-symbol mean of |slippage_bps| over the last 20 closed trades of
# the current mode, restricted to trades younger than 7 days (vetoed symbols
# can't trade, so old fills must decay out of the window for the veto to
# expire). DB query cached 5 min per symbol; force-refreshed on that symbol's
# trade close. avg > risk.max_avg_slippage_bps → veto; re-check <= threshold
# → auto-clear.

_slippage_lock = threading.Lock()
_slippage_cache: Dict[str, dict] = {}    # sym -> {"avg_bps", "n", "ts"}
_slippage_vetoed: Dict[str, dict] = {}   # sym -> {"avg_bps", "since_ts"}
_SLIPPAGE_CACHE_TTL_SEC = 300.0
_SLIPPAGE_WINDOW_TRADES = 20
_SLIPPAGE_MAX_AGE_DAYS = 7.0


def _avg_slippage_bps(sym: str, force: bool = False) -> Optional[float]:
    """Rolling mean of |slippage_bps| for sym (see section comment). Updates
    the veto set as a side effect. Returns None when no qualifying trades."""
    now = time.time()
    with _slippage_lock:
        ent = _slippage_cache.get(sym)
        if ent and not force and now - ent["ts"] < _SLIPPAGE_CACHE_TTL_SEC:
            return ent["avg_bps"]
    from datetime import timedelta as _td
    since_iso = (datetime.now(timezone.utc)
                 - _td(days=_SLIPPAGE_MAX_AGE_DAYS)).isoformat()
    rows = _risk_db_rows(
        "SELECT slippage_bps FROM trades "
        "WHERE coin = ? AND mode = ? AND slippage_bps IS NOT NULL "
        "AND timestamp_sell >= ? ORDER BY id DESC LIMIT ?",
        (sym, get_mode(), since_iso, _SLIPPAGE_WINDOW_TRADES))
    vals = [abs(float(r["slippage_bps"])) for r in rows
            if r.get("slippage_bps") is not None]
    avg = (sum(vals) / len(vals)) if vals else None
    try:
        threshold = float(_risk_cfg()["max_avg_slippage_bps"])
    except Exception:
        threshold = _RISK_DEFAULTS["max_avg_slippage_bps"]
    with _slippage_lock:
        _slippage_cache[sym] = {"avg_bps": avg, "n": len(vals), "ts": now}
        vetoed = sym in _slippage_vetoed
        if avg is not None and threshold > 0 and avg > threshold:
            if not vetoed:
                _slippage_vetoed[sym] = {"avg_bps": round(avg, 2), "since_ts": now}
                log_it = ("added", avg)
            else:
                _slippage_vetoed[sym]["avg_bps"] = round(avg, 2)
                log_it = None
        elif vetoed:
            _slippage_vetoed.pop(sym, None)
            log_it = ("cleared", avg)
        else:
            log_it = None
    if log_it:
        action, a = log_it
        try:
            database.log_activity(
                f"SLIPPAGE VETO {action.upper()}: {sym} avg "
                f"{'n/a' if a is None else f'{a:.1f}bps'} vs limit "
                f"{threshold:.1f}bps (last {_SLIPPAGE_WINDOW_TRADES} trades, "
                f"<= {_SLIPPAGE_MAX_AGE_DAYS:.0f}d old)",
                "warn" if action == "added" else "info")
        except Exception:
            pass
    return avg


def _slippage_veto_active(sym: str) -> Optional[float]:
    """Returns the offending avg |slippage| in bps when sym is vetoed, else
    None. Refreshes the 5-min cache (which also auto-clears expired vetoes)."""
    _avg_slippage_bps(sym)
    with _slippage_lock:
        ent = _slippage_vetoed.get(sym)
        return float(ent["avg_bps"]) if ent else None


def get_slippage_vetoes() -> Dict[str, dict]:
    """{symbol: {"avg_bps": float, "since_ts": float}} — current §4.3 vetoes."""
    with _slippage_lock:
        return {s: dict(v) for s, v in _slippage_vetoed.items()}


# ── §4.5 — correlation guard ──────────────────────────────────────────────────
# Rolling 5-min window of executed-entry timestamps. While the CURRENT BTCUSDT
# 5m candle is red (close-so-far < open: open proxied by the last CLOSED 5m
# candle's close from data_collector.ws_candles_5m, close-so-far = live price)
# at most risk.max_new_entries_when_btc_red entries may fire per 5 minutes.
# Fails open when BTC candle/price data is unavailable.

_corr_lock = threading.Lock()
_corr_entry_ts: "_deque[float]" = _deque(maxlen=500)
_CORR_WINDOW_SEC = 300.0
_btc_red_cache: dict = {"ts": 0.0, "red": False}
_BTC_RED_TTL_SEC = 5.0


def _record_corr_entry() -> None:
    with _corr_lock:
        _corr_entry_ts.append(time.time())


def _entries_last_5min(now: Optional[float] = None) -> int:
    now = now if now is not None else time.time()
    with _corr_lock:
        return sum(1 for t in _corr_entry_ts if t > now - _CORR_WINDOW_SEC)


def _btc_5m_red() -> bool:
    """True when the in-progress BTCUSDT 5m candle is red. Open is proxied by
    the last closed 5m candle's close (ws_candles_5m holds closed candles
    only); close-so-far is the live price. Stale buffer (>10 min) or missing
    live price → False (fail-open). Cached 5 s."""
    now = time.time()
    if now - _btc_red_cache["ts"] < _BTC_RED_TTL_SEC:
        return bool(_btc_red_cache["red"])
    red = False
    try:
        import data_collector as _dc_cg
        buf = list((getattr(_dc_cg, "ws_candles_5m", None) or {}).get("BTCUSDT") or [])
        live = float(_dc_cg.prices.get("BTCUSDT", 0) or _rest_px.get("BTCUSDT", 0) or 0)
        if buf and live > 0:
            last = buf[-1]                      # [open_time_ms, o, h, l, c, v]
            last_open_s = float(last[0]) / 1000.0
            if now - last_open_s <= 600.0:      # closed within the last 10 min
                open_proxy = float(last[4])     # close of last closed 5m candle
                red = live < open_proxy
    except Exception:
        red = False
    _btc_red_cache["ts"] = now
    _btc_red_cache["red"] = red
    return red


def _corr_guard_state() -> dict:
    """{"blocked", "entries_5min", "limit", "btc_5m_red"} — §4.5 snapshot."""
    try:
        limit = int(_risk_cfg()["max_new_entries_when_btc_red"])
    except Exception:
        limit = int(_RISK_DEFAULTS["max_new_entries_when_btc_red"])
    entries = _entries_last_5min()
    red = _btc_5m_red()
    return {
        "blocked":      bool(red and limit > 0 and entries >= limit),
        "entries_5min": entries,
        "limit":        limit,
        "btc_5m_red":   red,
    }


# ── §4.4 — BNB fee balance (engine part) ──────────────────────────────────────

_BNB_LOW_USDT = 2.0
_bnb_topup_last_ts: float = 0.0
_BNB_TOPUP_MIN_INTERVAL_SEC = 3600.0
_BNB_TOPUP_USDT = 10.0


def get_bnb_fee_status() -> dict:
    """{"enabled", "bnb_free", "bnb_usdt_value", "low"} — §4.4 status.
    bnb_free/bnb_usdt_value are live-only (account cache via _acct, guarded);
    None in paper mode or when the lookup fails."""
    try:
        enabled = bool(fees.get_fee_model().bnb_discount)
    except Exception:
        enabled = bool(_load_strategy().get("fees", {}).get("bnb_discount", False))
    bnb_free: Optional[float] = None
    bnb_usdt: Optional[float] = None
    if get_mode() == "live":
        try:
            acc = _acct()
            for b in acc.get("balances", []):
                if b.get("asset") == "BNB":
                    bnb_free = float(b.get("free", 0) or 0)
                    break
            else:
                bnb_free = 0.0
        except Exception:
            bnb_free = None
        if bnb_free is not None:
            try:
                import data_collector as _dc_bnb
                px = float(_dc_bnb.prices.get("BNBUSDT", 0)
                           or _rest_px.get("BNBUSDT", 0) or 0)
                if px > 0:
                    bnb_usdt = round(bnb_free * px, 4)
            except Exception:
                bnb_usdt = None
    if bnb_usdt is not None:
        low = bnb_usdt < _BNB_LOW_USDT
    elif bnb_free is not None:
        low = bnb_free <= 0.0
    else:
        low = False
    return {"enabled": enabled, "bnb_free": bnb_free,
            "bnb_usdt_value": bnb_usdt, "low": low}


def maybe_topup_bnb() -> dict:
    """§4.4 — market-buy ~10 USDT of BNB when (and ONLY when) ALL of:
    fees.auto_topup_bnb=true (default false), fees.bnb_discount enabled,
    live mode, BNB balance low. NEVER auto-buys otherwise. Called from the
    API layer's periodic check — the engine never calls this on its own.
    Throttled to once per hour. Returns an action/status dict."""
    global _bnb_topup_last_ts
    status = get_bnb_fee_status()
    auto = bool(_load_strategy().get("fees", {}).get("auto_topup_bnb", False))
    out = {"acted": False, "status": status, "auto_topup_bnb": auto}
    if not auto:
        out["reason"] = "auto_topup_bnb disabled (default)"
        return out
    if get_mode() != "live":
        out["reason"] = "not live mode"
        return out
    if not status["enabled"]:
        out["reason"] = "fees.bnb_discount disabled"
        return out
    if not status["low"]:
        out["reason"] = "BNB balance not low"
        return out
    now = time.time()
    if now - _bnb_topup_last_ts < _BNB_TOPUP_MIN_INTERVAL_SEC:
        out["reason"] = "throttled (max one top-up per hour)"
        return out
    _bnb_topup_last_ts = now
    try:
        database.log_activity(
            f"BNB AUTO-TOPUP: fee balance low "
            f"(free={status['bnb_free']}, ≈{status['bnb_usdt_value']} USDT) — "
            f"market-buying {_BNB_TOPUP_USDT:.0f} USDT of BNB "
            f"(fees.auto_topup_bnb=true)", "error")
    except Exception:
        pass
    try:
        result = _market_buy("BNBUSDT", _BNB_TOPUP_USDT)
        qty = float(result.get("executedQty", 0) or 0)
        out.update({"acted": True, "bought_qty": qty, "spent_usdt": _BNB_TOPUP_USDT})
        try:
            database.log_activity(
                f"BNB AUTO-TOPUP: bought {qty:.6f} BNB for ~{_BNB_TOPUP_USDT:.0f} USDT",
                "warn")
        except Exception:
            pass
    except Exception as e:
        out["reason"] = f"buy failed: {e}"
        try:
            database.log_activity(f"BNB AUTO-TOPUP FAILED: {e}", "error")
        except Exception:
            pass
    return out


# ── §4 — consolidated risk status (single payload for the API layer) ─────────

def get_risk_status() -> dict:
    """One dict covering every Phase 4 risk feature — consumed by control_api."""
    daily = _daily_pnl_state()
    now = time.time()
    stopped = now < _daily_stop_until
    return {
        "daily": {
            "pnl_today":       daily["pnl_today"],
            "realized":        daily["realized"],
            "unrealized":      daily["unrealized"],
            "limit_usdt":      daily["limit_usdt"],
            "limit_pct":       daily["limit_pct"],
            "stopped":         stopped,
            "stop_until":      _daily_stop_until if stopped else None,
            "flatten_on_stop": bool(_risk_cfg()["flatten_on_stop"]),
            "resumed_today":   _daily_stop_resumed_day == _utc_day_str(),
        },
        "consecutive": {
            "count":        _consec_loss_count(),
            "limit":        int(_risk_cfg()["max_consecutive_losses"]),
            "paused_until": (_consec_loss_pause_until
                             if now < _consec_loss_pause_until else None),
        },
        "slippage_vetoes": get_slippage_vetoes(),
        "correlation": _corr_guard_state(),
        "slots": effective_slots(),
        "bnb":   get_bnb_fee_status(),
    }


def purge_symbol_state(symbol: str) -> None:
    """Clean symbol removal (supports control_api I4 auto-remove of a delisted
    symbol). Clears EVERY per-symbol engine cache/cooldown/tracker so no code
    path can touch the symbol again. Idempotent and safe for an unknown symbol
    (every key access is guarded — a missing key is fine).

    NEVER purges a symbol that currently has an open position: _positions is the
    source of truth for held coins and control_api guards against removing them,
    but as a defensive backstop we log and skip if one is somehow present. Global
    (non-per-symbol) state such as _corr_entry_ts is intentionally untouched."""
    if not symbol:
        return
    # Defensive: never purge state for a coin we still hold.
    try:
        with _positions_lock:
            held = any(p.get("symbol") == symbol for p in _positions)
    except Exception:
        held = False
    if held:
        try:
            database.log_activity(
                f"purge_symbol_state({symbol}) SKIPPED — symbol has an open "
                f"position; state retained", "warn")
        except Exception:
            pass
        return

    # Plain per-symbol dicts (accessed under the GIL — pop is atomic).
    for _d in (
        _cooldowns, _loss_cooldown, _candidacy_cooldown, _buy_ready_since,
        _sell_last_failed_ts, _sell_last_failed_reason, _ghost_check_fails,
        _maker_abandon_counts, _lot_waste_flags, _bookticker_cache,
        _rest_px, _rest_px_sym_ts, _last_ws_price_ts, _pos_peaks,
        _stop_loss_confirmation, _slippage_cache, _slippage_vetoed,
        _ratchet_state,
    ):
        try:
            _d.pop(symbol, None)
        except Exception:
            pass
    # Per-symbol sets.
    try:
        _bad_symbols.discard(symbol)
    except Exception:
        pass

    # Lock-guarded caches.
    try:
        with _signal_cache_lock:
            _signal_cache.pop(symbol, None)
    except Exception:
        pass
    try:
        with _stuck_lock:
            _stuck_positions.pop(symbol, None)
            _sell_skip_track.pop(symbol, None)
    except Exception:
        pass
    # Buy-rejection example cache: keyed by reason, each a list of example dicts
    # carrying a "symbol" — drop this symbol's examples from every reason bucket.
    try:
        with _rejection_lock:
            for _reason, _examples in list(_rejection_examples.items()):
                kept = [e for e in _examples if e.get("symbol") != symbol]
                if kept:
                    _rejection_examples[_reason] = kept
                else:
                    _rejection_examples.pop(_reason, None)
    except Exception:
        pass

    try:
        database.log_activity(
            f"purged engine state for {symbol} (removed from watchlist)", "info")
    except Exception:
        pass


# ── Reversal confirmation ──────────────────────────────────────────────────────
_reversal_cache: dict = {}
_REVERSAL_TTL_SEC = 30.0

def _fetch_1m_klines_rc(symbol: str, limit: int = 6) -> Optional[List[dict]]:
    """Fetch 1m klines for reversal confirmation — tries every Binance base
    (same fallback list as the scanner) instead of a single hardcoded host."""
    for _base in _KLINE_BASES:
        url = f"{_base}/api/v3/klines?symbol={symbol}&interval=1m&limit={limit}"
        _src = f"reversal_klines_{_base.split('//', 1)[-1].split('.', 1)[0]}"
        # C §6.4 — non-critical: WS closed-candle buffer is the fallback
        ok, data, _, _ = _binance_request(url, timeout=3.0, source=_src,
                                          weight=2, critical=False)
        if not ok or not isinstance(data, list) or len(data) < limit:
            continue
        try:
            return [{"open": float(k[1]), "high": float(k[2]), "low": float(k[3]),
                     "close": float(k[4]), "volume": float(k[5])} for k in data]
        except Exception:
            continue
    return None


def _reversal_candles(symbol: str) -> Optional[List[dict]]:
    """Return the last 5 COMPLETED 1m candles for reversal confirmation.

    REST returns the in-progress candle last, so fetch 6 and drop it. When
    REST fails, fall back to the WebSocket closed-candle buffer (which holds
    only completed candles). Returns None when no source has enough data.
    """
    klines = _fetch_1m_klines_rc(symbol, limit=6)
    if klines and len(klines) >= 6:
        return klines[:-1]  # drop the in-progress candle
    try:
        import data_collector as _dc_rc
        buf = list(_dc_rc.ws_candles.get(symbol, []))[-6:]
        if len(buf) >= 5:
            return [{"open": float(r[1]), "high": float(r[2]), "low": float(r[3]),
                     "close": float(r[4]), "volume": float(r[5])} for r in buf[-5:]]
    except Exception:
        pass
    return None


def is_reversal_confirmed(symbol: str) -> tuple:
    """Returns (confirmed: bool, reason: str).

    Evaluates the last COMPLETED 1m candle (green + volume surge vs the prior
    4 completed candles) — never the in-progress candle, whose partial volume
    made confirmation near-impossible at arbitrary buy moments. When no candle
    data is available from any source (REST + WS), PASSES with a throttled
    warning: the pre-buy fresh re-check has already validated momentum, and
    data unavailability alone must never permanently block buying."""
    global _last_reversal_nodata_log
    now = time.time()
    cached_rc = _reversal_cache.get(symbol)
    if cached_rc and (now - cached_rc["ts"]) < _REVERSAL_TTL_SEC:
        return cached_rc["confirmed"], cached_rc.get("reason", "cached")
    candles = _reversal_candles(symbol)
    if not candles or len(candles) < 5:
        if now - _last_reversal_nodata_log >= 60.0:
            _last_reversal_nodata_log = now
            try:
                database.log_activity(
                    f"{symbol}: reversal confirmation has no candle data (REST+WS) — "
                    f"passing through (fresh re-check already validated momentum)", "warn"
                )
            except Exception:
                pass
        _reversal_cache[symbol] = {"ts": now, "confirmed": True, "reason": "no_data_pass"}
        return True, "no_data_pass"
    last = candles[-1]          # last COMPLETED candle
    prev = candles[:-1]         # the 4 completed candles before it
    if last["close"] <= last["open"]:
        _reversal_cache[symbol] = {"ts": now, "confirmed": False, "reason": "last_candle_red"}
        return False, "last_candle_red"
    avg_prev_vol = sum(c["volume"] for c in prev) / 4.0
    if avg_prev_vol > 0 and last["volume"] < avg_prev_vol * 1.15:
        _reversal_cache[symbol] = {"ts": now, "confirmed": False, "reason": "weak_volume"}
        return False, "weak_volume"
    recent_low = min(c["low"] for c in prev)
    if last["close"] <= recent_low:
        _reversal_cache[symbol] = {"ts": now, "confirmed": False, "reason": "still_below_lows"}
        return False, "still_below_lows"
    _reversal_cache[symbol] = {"ts": now, "confirmed": True, "reason": "ok"}
    return True, "ok"


# ── Pre-buy fresh kline fetch — multi-source, never fail-closed ───────────────

def _fetch_fresh_1m_candles(sym: str, limit: int = 50, min_candles: int = 36) -> list:
    """Fetch fresh 1m OHLCV candles for the pre-buy re-check.

    limit=50 gives MACD parity with the 60s scan (histogram[-2] needs >=36
    closes — the old limit=30 made the macd signal structurally impossible).

    Source order: every Binance REST base in _KLINE_BASES (per-base backoff
    keys so one dead host doesn't blind the others), then the in-memory
    WebSocket closed-candle buffer, then DB candles.

    Returns a list of candle dicts (open/high/low/close/volume) with at least
    ``min_candles`` entries, or [] when NO source has enough data. Callers
    must treat [] as "no fresh data available" and proceed on the cached
    signal decision — never as a failed signal check.
    """
    import urllib.parse as _up_fr
    qs = _up_fr.urlencode({"symbol": sym, "interval": "1m", "limit": limit})
    for _base in _KLINE_BASES:
        _src = f"klines_pre_buy_{_base.split('//', 1)[-1].split('.', 1)[0]}"
        # C §6.4 — non-critical: WS buffer + DB candles are the fallbacks
        ok, raw, _, _ = _binance_request(f"{_base}/api/v3/klines?{qs}",
                                         timeout=2.0, source=_src,
                                         weight=2, critical=False)
        if not ok or not isinstance(raw, list) or len(raw) < min_candles:
            continue
        try:
            return [{"open": float(k[1]), "high": float(k[2]), "low": float(k[3]),
                     "close": float(k[4]), "volume": float(k[5])} for k in raw]
        except Exception:
            continue
    try:
        import data_collector as _dc_fr
        buf = list(_dc_fr.ws_candles.get(sym, []))
        if len(buf) >= min_candles:
            return [{"open": float(r[1]), "high": float(r[2]), "low": float(r[3]),
                     "close": float(r[4]), "volume": float(r[5])} for r in buf[-limit:]]
    except Exception:
        pass
    try:
        rows = database.get_candles(sym, config.CANDLE_TIMEFRAME, limit=limit)
        if len(rows) >= min_candles:
            return [{"open":   float(r.get("open") or r["close"]),
                     "high":   float(r.get("high") or r["close"]),
                     "low":    float(r.get("low") or r["close"]),
                     "close":  float(r["close"]),
                     "volume": float(r.get("volume") or 0.0)} for r in rows]
    except Exception:
        pass
    return []


def _floor_qty(qty: float, symbol: str = "", decimals: int = 8) -> float:
    """Floor quantity to the symbol's LOT_SIZE stepSize to avoid Binance -1111 errors.
    Falls back to flat 8 decimal places if symbol is unknown or API call fails."""
    if symbol:
        step = _lot_step_cache.get(symbol)
        if step is None:
            try:
                info = _client().get_symbol_info(symbol)
                for f in (info or {}).get("filters", []):
                    if f.get("filterType") == "LOT_SIZE":
                        step = float(f["stepSize"])
                        break
            except Exception:
                step = 0.0
            # Cache only successful lookups — a transient failure (timeout,
            # geo-block, paper client) must not disable LOT_SIZE rounding for
            # this symbol permanently; retry on the next call instead.
            if step and step > 0:
                _lot_step_cache[symbol] = step
        if step and step > 0:
            factor = 1.0 / step
            return math.floor(qty * factor) / factor
    factor = 10 ** decimals
    return math.floor(qty * factor) / factor


# ── PRICE_FILTER tick size (Phase 2 §2.4/§2.5 exit-order pricing) ────────────
# exchange_info.get_symbol_filters only caches LOT_SIZE/NOTIONAL (step_size,
# min_qty, min_notional) — it does NOT expose PRICE_FILTER. Tick size is
# therefore fetched from the client's get_symbol_info (same source and cache
# pattern as _floor_qty's LOT_SIZE lookup). Fallback: 8-decimal rounding —
# Binance price precision never exceeds 8 dp, so an un-tick-rounded price at
# 8 dp only risks a -1013 PRICE_FILTER reject, which the placement callers
# already treat as "fall back to local monitoring".
_tick_size_cache: Dict[str, float] = {}


def _tick_size(symbol: str) -> Optional[float]:
    """PRICE_FILTER tickSize for symbol (cached), or None when unavailable."""
    if not symbol:
        return None
    tick = _tick_size_cache.get(symbol)
    if tick is None:
        try:
            info = _client().get_symbol_info(symbol)
            for f in (info or {}).get("filters", []):
                if f.get("filterType") == "PRICE_FILTER":
                    tick = float(f["tickSize"])
                    break
        except Exception:
            tick = 0.0
        # Cache only successful lookups (mirror _floor_qty): transient failures
        # must retry on the next call instead of pinning a bad value.
        if tick and tick > 0:
            _tick_size_cache[symbol] = tick
    return tick if tick and tick > 0 else None


def _floor_price_tick(price: float, symbol: str) -> float:
    """Floor a price to the symbol's tick size (8-dp floor when tick unknown)."""
    tick = _tick_size(symbol)
    if tick:
        return math.floor(price / tick + 1e-9) * tick
    return math.floor(price * 1e8) / 1e8


# ── Indicator helpers (derive from candle dict) ─────────────────────────────

def _derive_ma_pos(price: float, ma20: Optional[float]) -> Optional[str]:
    if ma20 is None or ma20 == 0:
        return None
    diff = abs(price - ma20) / ma20
    if diff <= 0.001:
        return "at"
    return "above" if price > ma20 else "below"


def _derive_bb_pos(price: float, candle: dict) -> Optional[str]:
    upper = candle.get("bb_upper")
    lower = candle.get("bb_lower")
    mid   = candle.get("bb_mid")
    if upper is None or lower is None or mid is None:
        return None
    bw = upper - lower
    if bw == 0:
        return "mid_zone"
    near = 0.01 * bw
    if price > upper:
        return "above_upper"
    if price >= upper - near:
        return "near_upper"
    if price <= lower:
        return "below_lower"
    if price <= lower + near:
        return "near_lower"
    return "mid_zone"


# ── Signal evaluation — 6-signal dict ────────────────────────────────────

def evaluate_signals(candles: list) -> dict:
    """
    Evaluate all six technical signals on the last COMPLETED candle (index -2).
    Returns {"trend", "rsi", "macd", "volume", "obv", "atr"} — all booleans.

    candles: list of dicts with keys close, volume, high, low.
    When high/low are unavailable (tick data), pass high=low=close — ATR will
    return None → atr_is_tradeable returns False → atr signal = False.

    Short/empty input returns all-False instead of raising (the [-2]/[-3]
    indexing below needs at least 3 candles) — callers must decide whether
    thin data means "skip the check", never rely on an IndexError.
    """
    if not candles or len(candles) < 3:
        return {"trend": False, "rsi": False, "macd": False,
                "volume": False, "obv": False, "atr": False}
    closes  = [c["close"]  for c in candles]
    volumes = [c["volume"] for c in candles]

    ema9        = indicators.calc_ema(closes, 9)
    ema21       = indicators.calc_ema(closes, 21)
    rsi_vals    = indicators.calc_rsi(closes, 14)
    _, _, histo = indicators.calc_macd(closes)
    vol_ma      = indicators.calc_volume_ma(volumes, 20)

    trend = bool(
        ema9[-2]  is not None
        and ema21[-2] is not None
        and ema9[-2] > ema21[-2]
    )
    rsi = bool(
        rsi_vals[-2] is not None
        and config.RSI_BUY_MIN <= rsi_vals[-2] <= config.RSI_BUY_MAX
    )
    # MACD: histogram positive AND rising (confirms momentum, not just a positive reading).
    macd = bool(
        histo[-2] is not None
        and histo[-3] is not None
        and histo[-2] > 0
        and histo[-2] > histo[-3]
    )
    volume = bool(
        volumes[-2] is not None
        and vol_ma[-2] is not None
        and vol_ma[-2] > 0
        and volumes[-2] >= vol_ma[-2] * config.VOLUME_RATIO_MIN
    )
    obv = indicators.obv_is_bullish(candles)
    atr = indicators.atr_is_tradeable(
        indicators.calc_atr(candles, config.ATR_PERIOD),
        candles[-2]["close"],
        config.ATR_MIN_PCT,
        config.ATR_MAX_PCT,
    )

    return {"trend": trend, "rsi": rsi, "macd": macd, "volume": volume, "obv": obv, "atr": atr}


# ── C §6.2 — WS-first full cache rebuild helpers ─────────────────────────────

def _row_to_candle(r) -> Optional[dict]:
    """Normalize one buffer row (raw kline list OR dict) into a candle dict.
    Defensive against the data_collector buffer format changing shape."""
    try:
        if isinstance(r, dict):
            c = float(r["close"])
            out = {
                "open":   float(r.get("open",  c) or c),
                "high":   float(r.get("high",  c) or c),
                "low":    float(r.get("low",   c) or c),
                "close":  c,
                "volume": float(r.get("volume", 0) or 0.0),
            }
            if r.get("open_time") is not None:
                out["open_time"] = int(r["open_time"])
            return out
        # Raw kline row: [open_time, open, high, low, close, volume, ...]
        return {
            "open_time": int(r[0]),
            "open":      float(r[1]),
            "high":      float(r[2]),
            "low":       float(r[3]),
            "close":     float(r[4]),
            "volume":    float(r[5]),
        }
    except Exception:
        return None


def _ws_buffer_candles(symbol: str, min_candles: int = 16) -> list:
    """Real-OHLCV candle dicts from data_collector.ws_candles (120×1m buffer).
    Returns [] when the buffer is missing/thin/malformed — callers fall back."""
    try:
        import data_collector as _dc_wb
        buf = list((getattr(_dc_wb, "ws_candles", None) or {}).get(symbol) or [])
    except Exception:
        return []
    if len(buf) < min_candles:
        return []
    out = [c for c in (_row_to_candle(r) for r in buf) if c is not None]
    return out if len(out) >= min_candles else []


# ── Phase 2 §2.1 — ATR-based stop geometry at entry ──────────────────────────

def _atr_pct_5m_at_entry(symbol: str, price: float) -> Tuple[Optional[float], str]:
    """ATR(14) on 5m candles as a % of `price` (§2.1). Source ladder:

      1. data_collector.ws_candles_5m (>= 15 candles)         → "ws_5m"
      2. WS 1m buffer aggregated to 5m (aggregate_candles)    → "ws_1m_agg"
      3. DB 1m candles aggregated to 5m                       → "db_1m_agg"
      4. 1m ATR × sqrt(5) approximation (WS buffer, flagged)  → "1m_sqrt5_approx"

    Returns (atr_pct or None, source). None → caller uses sl_min_pct and flags
    the position "atr_unavailable" (conservative tight stop)."""
    if price <= 0:
        return None, "unavailable"
    # 1) live 5m WS buffer
    try:
        import data_collector as _dc_atr
        buf5 = list((getattr(_dc_atr, "ws_candles_5m", None) or {}).get(symbol) or [])
        c5 = [c for c in (_row_to_candle(r) for r in buf5) if c is not None]
        if len(c5) >= 15:
            atr = indicators.calc_atr(c5, 14)
            if atr:
                return atr / price * 100.0, "ws_5m"
    except Exception:
        pass
    # 2) 1m WS buffer aggregated → 5m
    try:
        one_m = _ws_buffer_candles(symbol, min_candles=75)
        if one_m:
            c5 = indicators.aggregate_candles(one_m, group=5)
            if len(c5) >= 15:
                atr = indicators.calc_atr(c5, 14)
                if atr:
                    return atr / price * 100.0, "ws_1m_agg"
    except Exception:
        pass
    # 3) DB 1m candles aggregated → 5m (covers cold restart)
    try:
        rows = database.get_candles(symbol, config.CANDLE_TIMEFRAME, limit=120)
        c1 = [c for c in (_row_to_candle(r) for r in (rows or [])) if c is not None]
        if c1:
            c5 = indicators.aggregate_candles(c1, group=5)
            if len(c5) >= 15:
                atr = indicators.calc_atr(c5, 14)
                if atr:
                    return atr / price * 100.0, "db_1m_agg"
            # 4) final fallback — 1m ATR scaled by sqrt(5), flagged
            if len(c1) >= 15:
                atr1 = indicators.calc_atr(c1, 14)
                if atr1:
                    return atr1 * math.sqrt(5.0) / price * 100.0, "1m_sqrt5_approx"
    except Exception:
        pass
    # 4b) sqrt(5) approximation from the WS 1m buffer
    try:
        one_m = _ws_buffer_candles(symbol, min_candles=16)
        if one_m:
            atr1 = indicators.calc_atr(one_m, 14)
            if atr1:
                return atr1 * math.sqrt(5.0) / price * 100.0, "1m_sqrt5_approx"
    except Exception:
        pass
    return None, "unavailable"


def _apply_entry_exit_geometry(pos: dict, log_open: bool = True) -> None:
    """Compute and STORE §2.1/§2.2 exit geometry on the position dict:

      atr_pct_at_entry  ATR(14, 5m) / price × 100 (None + atr_source flag when
                        no source could produce it)
      sl_distance_pct   clamp(k_sl × atr_pct, sl_min_pct, sl_max_pct)
                        (ATR unavailable → sl_min_pct, atr_unavailable=True;
                         legacy mapping → fixed stop_loss_pct, None when the
                         legacy stop is disabled)
      stop_price        entry × (1 − sl_distance_pct/100)
      tp_distance_pct   rr_ratio × sl_distance_pct (legacy: fixed tp_pct)
      tp_price          max(entry × (1 + tp_dist/100), BEP × (1 + tp_buffer/100))
      hard_sl_price     entry × (1 − hard_sl_pct/100) (None in legacy mapping)

    The positions table is a fixed-column INSERT (database.save_position
    ignores unknown keys), so these live in-memory only —
    load_positions_from_db recomputes them for restored positions.

    PARITY: same formulas as backtest.exit_levels — identical strategy dicts
    must produce identical geometry live and in replay."""
    try:
        cfg = _exit_cfg()
        sym = pos.get("symbol", "")
        entry = float(pos.get("entry_price") or 0)
        if entry <= 0:
            return
        atr_pct, atr_src = _atr_pct_5m_at_entry(sym, entry)
        pos["atr_pct_at_entry"] = round(atr_pct, 6) if atr_pct else None
        pos["atr_source"] = atr_src
        pos["atr_unavailable"] = atr_pct is None

        if cfg["legacy_mode"]:
            sl_dist = cfg["sl_min_pct"] if cfg["sl_enabled"] else None
        elif atr_pct is None:
            sl_dist = cfg["sl_min_pct"]   # conservative tight stop, flagged above
        else:
            sl_dist = min(max(cfg["k_sl"] * atr_pct, cfg["sl_min_pct"]),
                          cfg["sl_max_pct"])

        pos["sl_distance_pct"] = round(sl_dist, 6) if sl_dist is not None else None
        pos["stop_price"] = (entry * (1.0 - sl_dist / 100.0)) if sl_dist else None

        # BEP floor at entry — recomputed AGAIN at trigger time (§2.2) so the
        # profit gate can never veto a trigger.
        bep = compute_real_breakeven_price(pos)
        if cfg["rr_ratio"]:
            tp_dist = cfg["rr_ratio"] * (sl_dist if sl_dist else cfg["sl_min_pct"])
        else:  # legacy fixed-TP path
            tp_dist = (cfg["tp_pct"] or 0.0) if cfg["tp_enabled"] else 0.0
        pos["tp_distance_pct"] = round(tp_dist, 6)
        tp = entry * (1.0 + tp_dist / 100.0)
        if bep > 0:
            tp = max(tp, bep * (1.0 + (cfg["tp_buffer_pct"] or 0.0) / 100.0))
        pos["tp_price"] = tp
        pos["hard_sl_price"] = (entry * (1.0 - cfg["hard_sl_pct"] / 100.0)
                                if cfg.get("hard_sl_pct") else None)
        # P3: be_moved is an explicit bool from birth — NEVER None. It flips True
        # ONLY in _evaluate_exit_decision when the breakeven move actually
        # executes AND the stored stop_price is promoted to BEP. setdefault so a
        # position that already armed the BE-move (geometry re-applied on a live
        # pos) is never reset to False.
        pos.setdefault("be_moved", False)
        # F1.1 instrumentation — store the full planned geometry on the pos so
        # the exit path can compute realized_r and the OPEN log records intent.
        pos["bep"] = bep if bep > 0 else None
        try:
            pos["planned_rr"] = ((tp_dist / sl_dist)
                                 if (sl_dist and tp_dist) else None)
        except (TypeError, ZeroDivisionError):
            pos["planned_rr"] = None
        if log_open:
            try:
                database.log_activity(
                    f"OPEN {sym} entry={entry:.6f} "
                    f"atr%={pos.get('atr_pct_at_entry')} "
                    f"sl_dist={pos.get('sl_distance_pct')}% "
                    f"stop={pos.get('stop_price')} tp={pos.get('tp_price')} "
                    f"tp_dist={pos.get('tp_distance_pct')}% "
                    f"bep={pos.get('bep')} planned_rr={pos.get('planned_rr')}",
                    "info")
            except Exception:
                pass
    except Exception as _ge:
        try:
            log_diag_issue("exit_geometry", "warn",
                           f"{pos.get('symbol')}: entry exit-geometry failed: {_ge}")
        except Exception:
            pass


# P4: how far below current price a clamped stale recovery stop is placed.
_STALE_STOP_BUFFER_PCT = 0.1


def _guard_stale_recovery_stop(pos: dict, price: float) -> bool:
    """P4 — never leave a recovered/restored position with a stop the market has
    ALREADY passed. Long-only geometry: a stop at/above the current price is an
    already-blown stop (BNB showed a stop above current price). Rather than sit
    forever above a dead stop, clamp it to JUST below the current price and flag
    it so the protective path (P1, now unblocked) exits at market the moment
    price ticks down. Also clamps an already-passed hard stop. Returns True when
    anything was clamped. Best-effort — never raises."""
    try:
        price = float(price or 0)
        if price <= 0:
            return False
        clamped = False
        _buf = price * (1.0 - _STALE_STOP_BUFFER_PCT / 100.0)
        stop = pos.get("stop_price")
        if stop is not None and float(stop) >= price:
            if pos.get("orig_stop_price") is None:
                pos["orig_stop_price"] = stop      # preserve the intended level
            pos["stop_price"] = _buf
            pos["stale_stop_clamped"] = True
            clamped = True
        hard = pos.get("hard_sl_price")
        if hard is not None and float(hard) >= price:
            pos["hard_sl_price"] = _buf
            pos["stale_stop_clamped"] = True
            clamped = True
        if clamped:
            try:
                database.log_activity(
                    f"P4 {pos.get('symbol')}: recomputed stop was at/above current "
                    f"price ${price:.6f} (already blown) — clamped to ${_buf:.6f} "
                    f"just below price so the protective exit can fire; not left "
                    f"stale above the market.", "warn")
            except Exception:
                pass
        return clamped
    except Exception:
        return False


def _five_m_state(symbol: str, prev: dict) -> tuple:
    """(5m_ok, 5m_ts) for the WS-first rebuild.

    Source order: ws_candles_5m (≥21 candles, reuse is_5m_bullish) → 5m derived
    from the deep 1m WS buffer (120×1m → 24×5m) → warmup-neutral rule: preserve
    a previously computed value if one exists, else neutral (True) so data
    availability alone never vetoes buys after a restart."""
    global _warned_5m_warmup
    candles_5m: list = []
    try:
        import data_collector as _dc_5m
        buf5 = list((getattr(_dc_5m, "ws_candles_5m", None) or {}).get(symbol) or [])
        candles_5m = [c for c in (_row_to_candle(r) for r in buf5) if c is not None]
    except Exception:
        candles_5m = []
    if len(candles_5m) < 21:
        try:
            one_m = _ws_buffer_candles(symbol, min_candles=105)
            if one_m:
                derived = indicators.aggregate_candles(one_m, group=5)
                if len(derived) >= 21:
                    candles_5m = derived
        except Exception:
            pass
    if len(candles_5m) >= 21:
        try:
            return bool(indicators.is_5m_bullish(candles_5m)), time.time()
        except Exception:
            pass
    # Warmup-neutral (kept from the REST scan): not enough 5m data anywhere.
    prev_ts = prev.get("5m_ts", 0) or 0
    if prev_ts:
        return prev.get("5m_ok", True), prev_ts
    if not _warned_5m_warmup:
        _warned_5m_warmup = True
        try:
            # Estimate remaining backfill from how many 5m candles are still
            # missing (need 21; each spans ~300 s). Cheap and approximate.
            _rem_s = max(0, 21 - len(candles_5m)) * 300
            database.log_activity(
                f"5m trend backfilling (~{_rem_s}s remaining) — treated neutral "
                f"until buffer ready", "warn"
            )
        except Exception:
            pass
    return True, 0


def _low_24h_for(symbol: str, candles: list) -> Optional[float]:
    """24h low: existing 10-min cache → min(DB 1440-row low, WS-buffer low).
    The WS buffer covers the freshest ~2h (rows may not be persisted yet);
    the DB read supplies the true 24h span."""
    _l24 = _low24h_cache.get(symbol)
    if _l24 is not None and (time.time() - _l24[1]) < _LOW24H_CACHE_TTL_SEC:
        return _l24[0]
    lows: list = []
    try:
        _db_rows_24h = database.get_candles(symbol, config.CANDLE_TIMEFRAME, limit=1440)
        if len(_db_rows_24h) >= 60:
            lows.append(min(float(r.get("low") or r["close"]) for r in _db_rows_24h))
    except Exception:
        pass
    try:
        if candles:
            lows.append(min(c["low"] for c in candles))
    except Exception:
        pass
    low_24h = min(lows) if lows else None
    if low_24h is not None:
        _low24h_cache[symbol] = (low_24h, time.time())
    return low_24h


def _rebuild_full_entry(symbol: str, candles: list) -> bool:
    """C §6.2a — rebuild the FULL signal-cache entry from real-OHLCV candles:
    6 signals with REAL high/low (accurate ATR), RSI display value, bb_ok,
    stoch_rsi_val, klines_1m (last 15 real OHLCV), low_24h, 5m_ok.

    Merge-preserve semantics: keys set by other paths are never dropped.
    Runs on a worker thread (kline handler dispatches via run_in_executor).
    Returns True when the entry was stored."""
    if not candles or len(candles) < 16:
        return False
    try:
        closes  = [c["close"]  for c in candles]
        volumes = [c["volume"] for c in candles]
        signals = evaluate_signals(candles)   # real high/low → ATR accurate
        score   = sum(signals.values())
        rsi_list    = indicators.calc_rsi(closes, 14)
        rsi_display = rsi_list[-2] if rsi_list[-2] is not None else 0.0
        # BB veto on the last completed candle (same semantics as the REST scan)
        bb_u, bb_m, _ = indicators.calc_bollinger(closes)
        bb_ok = indicators.bb_buy_allowed(closes[-2], bb_u[-2], bb_m[-2])
        try:
            stoch_rsi_val = indicators.calc_stoch_rsi(closes)
        except Exception:
            stoch_rsi_val = None

        with _signal_cache_lock:
            prev = dict(_signal_cache.get(symbol, {}))
        five_m_ok, five_m_ts = _five_m_state(symbol, prev)
        low_24h = _low_24h_for(symbol, candles)

        # §3.1d — when entries are evaluated on 5m closes (default), the ENTRY
        # signal fields (signals/score/rsi_val/stoch_rsi_val) are OWNED by the
        # 5m path (on_kline5m_close) and this 1m rebuild must not overwrite
        # them; it keeps refreshing price/ts/bb_ok/5m_ok/low_24h/klines_1m for
        # exits and diagnostics. Before the first 5m close (no entry_ts_5m in
        # the cache yet) the 1m values still seed the entry so the bot isn't
        # blind during warmup. All 5m-derived keys (klines_5m, ema50_15m_slope,
        # bb_position_5m, atr_pct, btc_regime) are merge-preserved by design —
        # entry.update() below never touches them.
        preserve_5m = (not _entries_cfg()["tick_entries"]) and bool(prev.get("entry_ts_5m"))

        with _signal_cache_lock:
            entry = dict(_signal_cache.get(symbol, {}))  # re-read under write lock
            updates = {
                "signals":       signals,
                "score":         score,
                "price":         closes[-1],
                "rsi_val":       rsi_display,
                "bb_ok":         bb_ok,
                "5m_ok":         five_m_ok,
                "5m_ts":         five_m_ts,
                "ts":            time.time(),
                "low_24h":       low_24h,
                "klines_1m":     [dict(c) for c in candles[-15:]],
                "stoch_rsi_val": stoch_rsi_val,
            }
            if preserve_5m and entry.get("entry_ts_5m"):
                for _k in ("signals", "score", "rsi_val", "stoch_rsi_val"):
                    updates.pop(_k, None)
            entry.update(updates)
            _signal_cache[symbol] = entry
        _signal_scanner_health["last_event_refresh_ts"] = time.time()
        return True
    except Exception as e:
        print(f"[TradeEngine] Full entry rebuild error {symbol}: {e}")
        return False


def update_coin_signals(symbol: str, closes: list, volumes: list):
    """Update the signal cache on every kline close (WebSocket-driven).

    C §6.2a — WS-first: when the data_collector ws_candles buffer has real
    OHLCV depth, rebuild the FULL cache entry from it (accurate ATR, bb_ok,
    5m_ok, low_24h, klines_1m, stoch_rsi_val) — no REST needed. Called on a
    worker thread (data_collector's kline handler runs _persist_and_signal
    via run_in_executor), so the DB reads here never touch the event loop.

    Fallback (thin WS buffer, e.g. right after restart): the legacy minimal
    path below — fake candles (high=low=close) so OBV works; ATR/BB/5m values
    from the last full refresh are preserved via the prev cache entry.
    """
    if len(closes) < 16:  # minimum for RSI-14 to produce a valid value at [-2]
        return
    try:
        _full = _ws_buffer_candles(symbol)
        if _full and _rebuild_full_entry(symbol, _full):
            return
    except Exception as e:
        print(f"[TradeEngine] WS-first rebuild error {symbol}: {e}")
    try:
        candles = [
            {"high": c, "low": c, "close": c, "volume": v}
            for c, v in zip(closes, volumes)
        ]
        signals = evaluate_signals(candles)
        # ATR requires real high/low data — tick candles have high=low=close so ATR=0.
        # Preserve the last REST-computed ATR value instead of overwriting with False.
        with _signal_cache_lock:
            prev = _signal_cache.get(symbol, {})
        signals["atr"] = prev.get("signals", {}).get("atr", False)
        score   = sum(signals.values())
        rsi_list    = indicators.calc_rsi(closes, 14)
        rsi_display = rsi_list[-2] if rsi_list[-2] is not None else 0.0

        with _signal_cache_lock:
            # MERGE into the previous entry instead of replacing it — the old
            # replace dropped klines_1m/low_24h/stoch_rsi_val/5m_ts written by
            # the REST scan, starving M4_micro_pullback (mandatory!), R1 and
            # M2 of data every time a 1m kline closed, and defeating the 180s
            # 5m-veto cache (5m_ts loss forced a REST refetch every scan).
            prev  = _signal_cache.get(symbol, {})  # re-read under the write lock
            entry = dict(prev)
            _upd = {
                "signals":  signals,
                "score":    score,
                "price":    closes[-1],
                "rsi_val":  rsi_display,
                "bb_ok":    prev.get("bb_ok",  False),  # preserved from last REST scan
                "5m_ok":    prev.get("5m_ok",  False),  # preserved from last REST scan
                "ts":       time.time(),
            }
            # §3.1d — entry signals are owned by the 5m path (see
            # _rebuild_full_entry); this thin-buffer fallback also only
            # refreshes price/ts once a 5m evaluation exists.
            if (not _entries_cfg()["tick_entries"]) and prev.get("entry_ts_5m"):
                _upd.pop("signals", None)
                _upd.pop("score",   None)
                _upd.pop("rsi_val", None)
            entry.update(_upd)
            # Append the newly closed candle to klines_1m so R1/M4 keep seeing
            # fresh data between REST scans (cap at 60 — more than the engine
            # needs). Skip the append when the REST scan already recorded it.
            try:
                _k1        = list(entry.get("klines_1m") or [])
                _new_close = float(closes[-1])
                _new_vol   = float(volumes[-1]) if volumes else 0.0
                _last_k    = _k1[-1] if _k1 else None
                _is_dup = bool(
                    _last_k
                    and abs(float(_last_k.get("close", 0)) - _new_close) < 1e-12
                    and abs(float(_last_k.get("volume", 0)) - _new_vol) < 1e-12
                )
                if not _is_dup:
                    _open = float(closes[-2]) if len(closes) >= 2 else _new_close
                    _k1.append({
                        "open":   _open,
                        "high":   max(_open, _new_close),
                        "low":    min(_open, _new_close),
                        "close":  _new_close,
                        "volume": _new_vol,
                    })
                entry["klines_1m"] = _k1[-60:]
            except Exception:
                pass
            _signal_cache[symbol] = entry
    except Exception as e:
        print(f"[TradeEngine] Signal cache error {symbol}: {e}")


# ── Phase 3 §3.1 — entry evaluation on 5m closes ─────────────────────────────
# Entries no longer arm/fire per tick: on each symbol's 5m kline CLOSE the
# ENTRY signal fields are rebuilt from the 5m buffer and a buy check runs for
# that moment; between closes a small heartbeat only re-checks VETO conditions
# (spread, regime, cooldowns…) so an armed setup can enter when a veto clears.
# Expected trade rate drops to ~3-15/day BY DESIGN (was: tick-driven).

def _evaluate_signals_at_close(candles: list) -> dict:
    """Six signal booleans evaluated on the LAST candle of `candles`, which
    must all be CLOSED candles (the ws_candles_5m buffer only holds closed
    ones). This is the [-1] twin of evaluate_signals (which assumes an
    in-progress candle on top and evaluates [-2]) and matches
    backtest._build_signal_data's closed-candle convention."""
    if not candles or len(candles) < 3:
        return {"trend": False, "rsi": False, "macd": False,
                "volume": False, "obv": False, "atr": False}
    closes  = [c["close"]  for c in candles]
    volumes = [c["volume"] for c in candles]
    ema9        = indicators.calc_ema(closes, 9)
    ema21       = indicators.calc_ema(closes, 21)
    rsi_vals    = indicators.calc_rsi(closes, 14)
    _, _, histo = indicators.calc_macd(closes)
    vol_ma      = indicators.calc_volume_ma(volumes, 20)
    trend = bool(ema9[-1] is not None and ema21[-1] is not None
                 and ema9[-1] > ema21[-1])
    rsi = bool(rsi_vals[-1] is not None
               and config.RSI_BUY_MIN <= rsi_vals[-1] <= config.RSI_BUY_MAX)
    macd = bool(len(histo) >= 2 and histo[-1] is not None and histo[-2] is not None
                and histo[-1] > 0 and histo[-1] > histo[-2])
    volume = bool(vol_ma[-1] is not None and vol_ma[-1] > 0
                  and volumes[-1] >= vol_ma[-1] * config.VOLUME_RATIO_MIN)
    obv = indicators.obv_is_bullish(candles)
    atr = indicators.atr_is_tradeable(
        indicators.calc_atr(candles, config.ATR_PERIOD),
        closes[-1], config.ATR_MIN_PCT, config.ATR_MAX_PCT)
    return {"trend": trend, "rsi": rsi, "macd": macd,
            "volume": volume, "obv": obv, "atr": atr}


def bb_position_5m_from_closes(closes: list) -> Optional[str]:
    """§3.1 bb_position_5m for the P2 veto: 'above_upper' | 'at_upper' |
    'inside' from 5m closes (BB 20/2 on the last closed candle). None when
    the band can't be computed (P2 fails open on None)."""
    try:
        if not closes or len(closes) < 20:
            return None
        bb_u, bb_m, bb_l = indicators.calc_bollinger(closes)
        upper, lower = bb_u[-1], bb_l[-1]
        if upper is None or lower is None:
            return None
        px = closes[-1]
        if px > upper:
            return "above_upper"
        band = upper - lower
        if band > 0 and px >= upper - 0.01 * band:
            return "at_upper"
        return "inside"
    except Exception:
        return None


_EMA50_15M_MIN_CANDLES = 55   # EMA50 over 15m needs >=55 candles to be meaningful


def _fetch_real_15m(symbol: str, limit: int = 60) -> list:
    """I1.3 — real 15m candles from the data layer for T2's EMA50-15m slope.
    Prefers data_collector.fetch_15m_candles(sym, limit); falls back to the
    data_collector.ws_candles_15m buffer. Returns normalized candle dicts
    ({open,high,low,close,volume}) or [] when neither source is available
    (older data_collector) — callers then use the 5m→15m aggregate."""
    try:
        import data_collector as _dc_15
    except Exception:
        return []
    rows = None
    fn = getattr(_dc_15, "fetch_15m_candles", None)
    if callable(fn):
        try:
            rows = fn(symbol, limit)
        except Exception:
            rows = None
    if not rows:
        buf = getattr(_dc_15, "ws_candles_15m", None)
        if buf is not None:
            try:
                rows = list(buf.get(symbol) or [])
            except Exception:
                rows = None
    if not rows:
        return []
    return [c for c in (_row_to_candle(r) for r in rows) if c is not None]


def _ema50_15m_slope_for(symbol: str, candles_5m: list) -> Optional[float]:
    """§3.1 ema50_15m_slope: EMA50 over the 15m series, slope as the % change
    of EMA50 across the last two 15m candles (>0 = rising, feeds T2).

    Source order (I1.3): REAL 15m candles from the data layer
    (data_collector.fetch_15m_candles / ws_candles_15m) when available; else
    the 5m buffer aggregated 3→1; else stored 1m DB history aggregated
    group=15. Returns None when no source reaches the min 15m candle count —
    T2 is SCORED, so a None simply means T2 can't contribute (it does NOT
    block the symbol)."""
    series_15m: list = []
    # I1.3 — prefer the real 15m source when the data layer exposes it.
    try:
        real_15m = _fetch_real_15m(symbol, 60)
        if len(real_15m) >= _EMA50_15M_MIN_CANDLES:
            series_15m = real_15m
    except Exception:
        series_15m = []
    if len(series_15m) < _EMA50_15M_MIN_CANDLES:
        try:
            if candles_5m:
                _agg = indicators.aggregate_candles(list(candles_5m), group=3)
                if len(_agg) > len(series_15m):
                    series_15m = _agg
        except Exception:
            pass
    if len(series_15m) < _EMA50_15M_MIN_CANDLES:
        try:
            rows = database.get_candles(symbol, config.CANDLE_TIMEFRAME,
                                        limit=15 * (_EMA50_15M_MIN_CANDLES + 5))
            c1 = [c for c in (_row_to_candle(r) for r in (rows or [])) if c is not None]
            if c1:
                derived = indicators.aggregate_candles(c1, group=15)
                if len(derived) > len(series_15m):
                    series_15m = derived
        except Exception:
            pass
    if len(series_15m) < _EMA50_15M_MIN_CANDLES:
        return None
    try:
        closes_15m = [c["close"] for c in series_15m]
        ema50 = indicators.calc_ema(closes_15m, 50)
        if len(ema50) < 2 or ema50[-1] is None or ema50[-2] is None or ema50[-2] == 0:
            return None
        return round((ema50[-1] - ema50[-2]) / ema50[-2] * 100.0, 6)
    except Exception:
        return None


_entry_mode_logged: str = ""      # last logged entry-timing mode ("" = never)


def _log_entry_timing_mode_once() -> None:
    """Log the §3.1 entry-timing mode once at startup (and again only if the
    tick_entries escape hatch is flipped at runtime)."""
    global _entry_mode_logged
    try:
        mode = "tick (legacy)" if _entries_cfg()["tick_entries"] else "5m-close"
    except Exception:
        mode = "5m-close"
    if mode == _entry_mode_logged:
        return
    _entry_mode_logged = mode
    hb = _entries_cfg().get("eval_heartbeat_sec", 15)
    msg = (f"Entry timing mode: {mode} — entry signals refresh on 5m kline closes; "
           f"{hb:.0f}s heartbeat re-checks vetoes only. Expected trade rate "
           f"~3-15/day by design."
           if mode == "5m-close" else
           "Entry timing mode: tick (legacy escape hatch entries.tick_entries=true) "
           "— per-tick buy dispatch restored.")
    print(f"[TradeEngine] {msg}")
    try:
        database.log_activity(msg, "info")
    except Exception:
        pass


def _dispatch_buy_check(prices_snapshot: Dict[str, float]) -> None:
    """Single-flight dispatch of _check_buys_from_cache to the buy-check
    executor (same guard realtime_monitor used per tick). Shared by the 5m
    close path, the veto heartbeat, and the legacy tick path."""
    global _buy_check_in_flight
    with _buy_check_lock:
        if _buy_check_in_flight:
            return
        _buy_check_in_flight = True

    def _run_buy_check():
        global _buy_check_in_flight
        try:
            _check_buys_from_cache(prices_snapshot)
        finally:
            with _buy_check_lock:
                _buy_check_in_flight = False

    _buy_check_executor.submit(_run_buy_check)


def on_kline5m_close(symbol: str, closed_row: list, buf_snapshot: list) -> None:
    """§3.1b — per-symbol 5m kline-close handler (worker thread; wired via
    data_collector.register_kline5m_callback).

    Rebuilds the symbol's ENTRY signal fields from the 5m buffer:
      * six signal booleans (trend/macd/volume/obv/atr + rsi bool) on 5m closes
      * rsi_val + stoch_rsi_val on 5m closes
      * klines_5m (last ~30 candles)                      → R1/M4 prefer these
      * ema50_15m_slope (5m→15m aggregate, EMA50, §3.1)   → T2
      * bb_position_5m (above_upper/at_upper/inside)      → P2 veto
      * atr_pct (Phase 2 §2.1 ATR ladder)                 → sizing/knife/registry
      * btc_regime (§3.3 3-state, 60 s cache)             → REGIME veto
    Everything is merged into the existing _signal_cache entry (keys owned by
    other paths — bb_ok, 5m_ok, low_24h, klines_1m — are preserved), then a
    buy check is triggered for this moment (single-flight)."""
    try:
        _log_entry_timing_mode_once()
        candles_5m = [c for c in (_row_to_candle(r) for r in (buf_snapshot or []))
                      if c is not None]
        if len(candles_5m) < 3:
            return
        closes_5m = [c["close"] for c in candles_5m]
        signals = _evaluate_signals_at_close(candles_5m)
        score = sum(signals.values())
        rsi_list = indicators.calc_rsi(closes_5m, 14)
        rsi_display = rsi_list[-1] if rsi_list and rsi_list[-1] is not None else 0.0
        try:
            stoch_rsi_val = indicators.calc_stoch_rsi(closes_5m)
        except Exception:
            stoch_rsi_val = None
        bb_pos_5m = bb_position_5m_from_closes(closes_5m)
        ema_slope = _ema50_15m_slope_for(symbol, candles_5m)
        px = closes_5m[-1]
        atr_pct, _atr_src = _atr_pct_5m_at_entry(symbol, px)
        try:
            btc_regime = get_btc_regime()
        except Exception:
            btc_regime = None

        with _signal_cache_lock:
            entry = dict(_signal_cache.get(symbol, {}))
            entry.update({
                "signals":         signals,
                "score":           score,
                "price":           px,
                "rsi_val":         rsi_display,
                "stoch_rsi_val":   stoch_rsi_val,
                "klines_5m":       [dict(c) for c in candles_5m[-30:]],
                "ema50_15m_slope": ema_slope,
                "bb_position_5m":  bb_pos_5m,
                "atr_pct":         round(atr_pct, 6) if atr_pct else None,
                "btc_regime":      btc_regime,
                "entry_ts_5m":     time.time(),
                "ts":              time.time(),
            })
            _signal_cache[symbol] = entry
        _signal_scanner_health["last_event_refresh_ts"] = time.time()

        # §3.1c(i) — the 5m close IS the entry moment: trigger a buy check now.
        if not _entries_cfg()["tick_entries"]:
            try:
                import data_collector as _dc_5c
                _dispatch_buy_check(dict(_dc_5c.prices))
            except Exception:
                _dispatch_buy_check({})
    except Exception as e:
        print(f"[TradeEngine] 5m entry rebuild error {symbol}: {e}")


# ── K3.3 — restart-reason detection (crash vs clean shutdown) ─────────────────
# On a graceful shutdown we drop a clean-shutdown marker (settings KV). A liveness
# heartbeat setting is refreshed from the entry-heartbeat loop so a crash leaves a
# recent timestamp. At boot control_api calls detect_restart_reason(): a marker
# present (and not stale vs the last heartbeat) means the previous stop was clean;
# absent means an unclean/crash restart because the graceful path never ran. The
# marker is cleared after reading so the NEXT boot cannot misread a stale marker.
_CLEAN_SHUTDOWN_KEY = "clean_shutdown_ts"
_LAST_HEARTBEAT_KEY = "last_heartbeat_ts"
_clean_shutdown_written = False


def _write_clean_shutdown_marker() -> None:
    """Record a clean-shutdown marker (atexit / signal path). Idempotent within a
    process; never raises."""
    global _clean_shutdown_written
    if _clean_shutdown_written:
        return
    _clean_shutdown_written = True
    try:
        database.save_setting(_CLEAN_SHUTDOWN_KEY, str(time.time()))
        database.log_activity("clean-shutdown marker written", "info")
    except Exception:
        pass


def _refresh_heartbeat_ts() -> None:
    """Persist a recent liveness heartbeat so a crash leaves a fresh timestamp
    that detect_restart_reason() can report. Called from the entry-heartbeat
    loop (~every eval_heartbeat_sec). Never raises."""
    try:
        database.save_setting(_LAST_HEARTBEAT_KEY, str(time.time()))
    except Exception:
        pass


# ── M2 — process RSS sampling (leak / OOM instrumentation) ───────────────────
# ru_maxrss (peak resident set) is portable and dependency-free; on Linux it is
# reported in KB. Sampled from the entry heartbeat loop (alongside
# _refresh_heartbeat_ts) and exposed via get_memory_stats() so control_api can
# surface memory growth in the boot report after an unclean restart.
try:
    import resource as _resource
except Exception:
    _resource = None

_rss_lock = threading.Lock()
_last_rss_kb: int = 0
_rss_peak_kb: int = 0
_rss_history: List[Tuple[float, int]] = []   # (ts, kb), newest appended
_RSS_HISTORY_MAX = 240                        # ~1h at a 15s heartbeat
_RSS_MIN_SAMPLES = 20                         # need this many before warning
_RSS_WARN_ABS_KB = 500 * 1024                 # 500 MB absolute floor for a warn
_RSS_WARN_GROWTH_MULT = 1.5                   # latest > 1.5× first-in-window
_last_rss_warn_ts: float = 0.0
_RSS_WARN_THROTTLE_SEC = 1800.0               # warn at most once / 30 min

# ── R1 — tracemalloc leak instrumentation ───────────────────────────────────
# Started at boot (guarded) so the RSS heartbeat can pinpoint WHERE the process
# is leaking (the RSS climb 190→590 MB → cgroup OOM). Every ~5 min it snapshots
# the top allocation sites, logs them, diffs vs the previous snapshot (growth is
# the real signal) and caches the top-3 for get_memory_stats() / the diagnostics
# bundle. This is diagnosis, not a fix.
_tracemalloc_started = False
_tracemalloc_started_ts: float = 0.0          # boot/uptime reference for first-snap timing
_tracemalloc_top: List[str] = []              # latest formatted top-3 sites
_last_tracemalloc_snap_ts: float = 0.0
# The leak drives RSS to the soft cap (→ self-restart) in ~5 min, so the FIRST
# snapshot must land well before that: fire at ~60s of uptime, then every ~120s.
_TRACEMALLOC_FIRST_SNAP_SEC = 60.0            # first snapshot at ~60s uptime
_TRACEMALLOC_SNAP_INTERVAL_SEC = 120.0        # subsequent snapshots ~every 2 min
_TRACEMALLOC_PRE_RESTART_KEY = "tracemalloc_pre_restart"  # settings KV for post-restart bundle
_tracemalloc_prev_snapshot = None             # previous snapshot for the growth diff

# ── R4 — RSS soft-cap guardrail state (safety net, NOT the leak fix) ─────────
_memory_restart_pending = False               # exposed via get_memory_stats() for the UI banner
_rss_over_cap_count = 0                        # consecutive samples over the cap (2-sample grace)


def _soft_cap_mb() -> float:
    """R4 — data.rss_soft_cap_mb (default 800; 0 disables). Never raises."""
    try:
        return float(_data_cfg().get("rss_soft_cap_mb", 800) or 0)
    except Exception:
        return 800.0


def _maybe_start_tracemalloc() -> None:
    """R1.1 — start tracemalloc at boot when data.tracemalloc_enabled (default
    True). nframe=8 (v0.4) so each allocation carries its caller stack: the raw
    leak site is a stdlib line (json/decoder.py) that a 1-frame trace can't
    attribute, so 8 frames let the top-3 dump name the WolfBot line that RETAINS
    the parsed object. Guarded so a failure NEVER blocks boot; idempotent."""
    global _tracemalloc_started, _tracemalloc_started_ts
    if _tracemalloc_started:
        return
    try:
        if not bool(_data_cfg().get("tracemalloc_enabled", True)):
            return
        import tracemalloc
        if not tracemalloc.is_tracing():
            tracemalloc.start(8)   # 8 frames: capture the caller stack, not just file:line
        _tracemalloc_started = True
        _tracemalloc_started_ts = time.time()   # uptime reference for first-snap timing
        try:
            database.log_activity("R1: tracemalloc started (leak instrumentation)", "info")
        except Exception:
            pass
    except Exception:
        # Instrumentation must never break boot.
        pass


def _tracemalloc_top10_lines(snap) -> List[str]:
    """Format a tracemalloc snapshot's top-10 'lineno' stats as
    'file:line size_kb count' strings. Shared by the periodic sampler and the
    pre-restart dump so both emit identical lines. Never raises."""
    lines: List[str] = []
    try:
        for st in snap.statistics('lineno')[:10]:
            try:
                fr = st.traceback[0]
                lines.append(f"{fr.filename}:{fr.lineno} {st.size // 1024}KB {st.count}")
            except Exception:
                continue
    except Exception:
        pass
    return lines


def _tracemalloc_top3_tracebacks(snap) -> List[str]:
    """R1 (v0.4) — full multi-frame tracebacks for the top-3 allocation sites via
    snap.statistics('traceback'). tracemalloc runs with nframe=8, so each block's
    stat.traceback.format() names the WolfBot line that RETAINS the parsed object
    (not just the stdlib json/decoder.py allocation line a 1-frame trace shows).
    Only the top-3 are formatted to keep the sampler cheap. Never raises."""
    blocks: List[str] = []
    try:
        for st in snap.statistics('traceback')[:3]:
            try:
                tb = "\n    ".join(st.traceback.format())
                blocks.append(f"{st.size // 1024}KB {st.count} blocks:\n    {tb}")
            except Exception:
                continue
    except Exception:
        pass
    return blocks


def _sample_tracemalloc() -> None:
    """R1.2 — take tracemalloc.take_snapshot().statistics('lineno')[:10], log a
    single INFO 'TRACEMALLOC TOP10' block (file:line size_kb count), diff against
    the previous snapshot to surface GROWTH, and cache the top-3 in
    _tracemalloc_top for get_memory_stats(). The FIRST snapshot fires at ~60s of
    uptime and subsequent ones ~every 120s, so the leak source is captured well
    before the RSS soft-cap self-restart. Never raises."""
    global _last_tracemalloc_snap_ts, _tracemalloc_prev_snapshot, _tracemalloc_top
    if not _tracemalloc_started:
        return
    now = time.time()
    if _last_tracemalloc_snap_ts <= 0.0:
        # First snapshot: wait until ~60s of uptime (not immediately at boot).
        if now - _tracemalloc_started_ts < _TRACEMALLOC_FIRST_SNAP_SEC:
            return
    elif now - _last_tracemalloc_snap_ts < _TRACEMALLOC_SNAP_INTERVAL_SEC:
        return
    _last_tracemalloc_snap_ts = now
    try:
        import tracemalloc
        snap = tracemalloc.take_snapshot()
        lines = _tracemalloc_top10_lines(snap)
        # Growth diff vs the previous snapshot — growth is the leak signal.
        grow: List[str] = []
        if _tracemalloc_prev_snapshot is not None:
            try:
                for d in snap.compare_to(_tracemalloc_prev_snapshot, 'lineno')[:5]:
                    if d.size_diff <= 0:
                        continue
                    fr = d.traceback[0]
                    grow.append(f"{fr.filename}:{fr.lineno} +{d.size_diff // 1024}KB "
                                f"(+{d.count_diff})")
            except Exception:
                pass
        _tracemalloc_prev_snapshot = snap
        _tracemalloc_top = lines[:3]
        block = "TRACEMALLOC TOP10:\n  " + "\n  ".join(lines)
        if grow:
            block += "\nTRACEMALLOC GROWTH (top5 vs prev):\n  " + "\n  ".join(grow)
        # v0.4 — full caller stack for the top-3 sites so a WolfBot line that
        # retains the parsed JSON is named (nframe=8). Only top-3 to stay cheap.
        tb3 = _tracemalloc_top3_tracebacks(snap)
        if tb3:
            block += "\nTRACEMALLOC TRACEBACK (top3):\n  " + "\n  ".join(tb3)
        try:
            log.info(block)
        except Exception:
            pass
        try:
            # Surface the top-3 in the activity log too so the diagnostics bundle
            # after a restart shows the leak source without the app logs.
            database.log_activity("TRACEMALLOC TOP10 — " + " | ".join(lines[:3]), "info")
        except Exception:
            pass
    except Exception:
        pass


def _dump_tracemalloc_pre_restart(rss_mb: int) -> None:
    """R4/R1 — force a final tracemalloc snapshot on the soft-cap self-restart
    path, BEFORE os._exit(0). Logs 'TRACEMALLOC TOP10 (pre-restart)' with the
    top-10 'file:line size_kb count' lines and persists the top-3 to the settings
    KV under _TRACEMALLOC_PRE_RESTART_KEY (JSON {ts, rss_mb, top:[...]}) so
    control_api can surface what was leaking right before the process died in a
    diagnostics bundle pulled AFTER the restart. Fully guarded (tracemalloc may be
    disabled) — never raises, never blocks the restart."""
    try:
        import tracemalloc
        if not tracemalloc.is_tracing():
            return
        snap = tracemalloc.take_snapshot()
        lines = _tracemalloc_top10_lines(snap)
        if not lines:
            return
        block = "TRACEMALLOC TOP10 (pre-restart):\n  " + "\n  ".join(lines)
        # v0.4 — full caller stack for the top-3 sites at death (nframe=8).
        tb3 = _tracemalloc_top3_tracebacks(snap)
        if tb3:
            block += "\nTRACEMALLOC TRACEBACK (top3, pre-restart):\n  " + "\n  ".join(tb3)
        try:
            log.critical(block)
        except Exception:
            pass
        try:
            database.log_activity(
                "TRACEMALLOC TOP10 (pre-restart) — " + " | ".join(lines[:3]), "critical")
        except Exception:
            pass
        try:
            database.save_setting(
                _TRACEMALLOC_PRE_RESTART_KEY,
                _json.dumps({"ts": time.time(), "rss_mb": rss_mb, "top": lines[:3]}))
        except Exception:
            pass
    except Exception:
        # Instrumentation must NEVER block the restart.
        pass


def _check_rss_soft_cap(rss_kb: int) -> None:
    """R4 — RSS soft-cap guardrail. This is a SAFETY NET, not the leak fix: it
    converts the unclean cgroup-OOM loop into CLEAN self-restarts that preserve
    shutdown persistence. When RSS_MB exceeds data.rss_soft_cap_mb (default 800;
    0 disables) for 2 CONSECUTIVE samples (a grace so a transient spike can't
    bounce the process), flush the clean-shutdown marker + risk latches and
    os._exit(0) so systemd restarts us as a clean exit rather than an OOM kill.
    Never raises (except the intentional process exit)."""
    global _rss_over_cap_count, _memory_restart_pending
    cap_mb = _soft_cap_mb()
    if cap_mb <= 0:                    # disabled — never trigger
        _rss_over_cap_count = 0
        return
    rss_mb = rss_kb // 1024
    if rss_mb <= cap_mb:
        _rss_over_cap_count = 0
        return
    _rss_over_cap_count += 1
    if _rss_over_cap_count < 2:        # 2-sample grace against a transient spike
        return
    _memory_restart_pending = True     # UI/control_api banner via get_memory_stats()
    try:
        database.log_activity(
            f"MEMORY: RSS {rss_mb}MB > soft cap {int(cap_mb)}MB — restarting cleanly",
            "critical")
    except Exception:
        pass
    try:
        log.critical("MEMORY: RSS %dMB > soft cap %dMB — restarting cleanly",
                     rss_mb, int(cap_mb))
    except Exception:
        pass
    # Graceful self-restart: persist the clean-shutdown marker (Part K) and risk
    # latches BEFORE exiting so the restart preserves shutdown persistence, then
    # os._exit(0) — a CLEAN exit systemd restarts, not an OOM kill. os._exit
    # avoids re-entrant atexit / thread-teardown hangs while memory-starved.
    try:
        _write_clean_shutdown_marker()
    except Exception:
        pass
    try:
        persist_risk_latches()
    except Exception:
        pass
    # Capture the leak source IMMEDIATELY before exiting — this is the whole point:
    # the operator pulls a bundle post-restart and sees what was leaking at death.
    _dump_tracemalloc_pre_restart(rss_mb)
    try:
        database.log_activity("MEMORY: clean self-restart now (os._exit 0)", "warn")
    except Exception:
        pass
    os._exit(0)


# S-gate — the Python leaks are fixed (tracemalloc-tracked allocations dropped
# ~40×); the residual RSS climb is NATIVE (invisible to tracemalloc) — glibc
# malloc fragmentation from sustained JSON-parse + candle churn that free() never
# returns to the OS. malloc_trim(0) forces glibc to release freed pages. We call
# it on a timer and log how much it reclaims so fragmentation is provable.
_libc_for_trim = None
_last_malloc_trim_ts: float = 0.0
_MALLOC_TRIM_INTERVAL_SEC: float = 180.0


def _read_current_rss_kb() -> int:
    """CURRENT resident set (KB), not the monotonic ru_maxrss peak — so a
    malloc_trim reclaim is actually visible and the soft cap tracks real usage.
    /proc/self/statm field 2 = resident pages. Falls back to ru_maxrss."""
    try:
        with open("/proc/self/statm") as _f:
            resident_pages = int(_f.read().split()[1])
        return resident_pages * (os.sysconf("SC_PAGE_SIZE") // 1024)
    except Exception:
        try:
            return int(_resource.getrusage(_resource.RUSAGE_SELF).ru_maxrss)
        except Exception:
            return 0


def _maybe_malloc_trim(rss_kb_before: int) -> None:
    """Every ~3 min, ask glibc to return freed heap pages to the OS (Linux only).
    Logs the reclaim so a native-fragmentation leak is diagnosable. Never raises."""
    global _libc_for_trim, _last_malloc_trim_ts
    now = time.time()
    if now - _last_malloc_trim_ts < _MALLOC_TRIM_INTERVAL_SEC:
        return
    _last_malloc_trim_ts = now
    try:
        if _libc_for_trim is None:
            import ctypes as _ct
            _libc_for_trim = _ct.CDLL("libc.so.6", use_errno=False)
        _libc_for_trim.malloc_trim(0)
        after = _read_current_rss_kb()
        reclaimed = rss_kb_before - after
        if reclaimed > 20_000:  # >20 MB returned to OS → fragmentation confirmed
            database.log_activity(
                f"MEMORY: malloc_trim reclaimed {reclaimed // 1024} MB "
                f"({rss_kb_before // 1024}→{after // 1024} MB) — glibc fragmentation, "
                f"not a Python leak.", "info")
    except Exception:
        pass


def _sample_rss() -> None:
    """M2 — sample CURRENT process RSS (KB) into the rolling history, update
    _last_rss_kb / _rss_peak_kb, WARN on sustained growth, and periodically
    malloc_trim to release glibc-fragmented native memory. Never raises."""
    global _last_rss_kb, _rss_peak_kb, _last_rss_warn_ts
    kb = _read_current_rss_kb()
    if kb <= 0:
        return
    now = time.time()
    warn = False
    first_kb = 0
    n = 0
    with _rss_lock:
        _last_rss_kb = kb
        if kb > _rss_peak_kb:
            _rss_peak_kb = kb
        _rss_history.append((now, kb))
        if len(_rss_history) > _RSS_HISTORY_MAX:
            del _rss_history[:len(_rss_history) - _RSS_HISTORY_MAX]
        n = len(_rss_history)
        if n >= _RSS_MIN_SAMPLES:
            first_kb = _rss_history[0][1]
            if (first_kb > 0 and kb > _RSS_WARN_GROWTH_MULT * first_kb
                    and kb > _RSS_WARN_ABS_KB
                    and now - _last_rss_warn_ts >= _RSS_WARN_THROTTLE_SEC):
                _last_rss_warn_ts = now
                warn = True
    if warn:
        try:
            database.log_activity(
                f"MEMORY: RSS grew to {kb // 1024} MB (from {first_kb // 1024} MB "
                f"over {n} samples) — possible leak; watch for an OOM restart.",
                "warn")
        except Exception:
            pass
    # Release glibc-fragmented native memory before the soft-cap check, so a
    # reclaimable spike doesn't needlessly trigger a restart.
    _maybe_malloc_trim(kb)
    # R4 — soft-cap guardrail (safety net): runs every sample; may os._exit(0).
    # Re-read current RSS post-trim so the cap reflects reclaimed memory.
    _check_rss_soft_cap(_read_current_rss_kb())


def _read_tracemalloc_pre_restart():
    """Read back the last pre-restart tracemalloc snapshot from the settings KV
    (JSON {ts, rss_mb, top:[...]}). Returns None if absent/unparseable. Guarded."""
    try:
        raw = database.get_setting(_TRACEMALLOC_PRE_RESTART_KEY)
        if not raw:
            return None
        return _json.loads(raw)
    except Exception:
        return None


def _best_effort_task_count():
    """Best-effort count of asyncio tasks in THIS thread's running loop. Returns
    None when there is no running loop here (the common case for this thread) —
    the authoritative count is sampled elsewhere. Never raises."""
    try:
        return len(asyncio.all_tasks())
    except Exception:
        return None


def _ws_loop_task_count():
    """v0.4 — authoritative asyncio task count from data_collector's WS loop.
    get_memory_stats() runs on the entry-heartbeat thread, which has NO asyncio
    loop, so a local asyncio.all_tasks() here is empty/None. data_collector samples
    the real WS-loop task count into _ws_health["asyncio_tasks"]; pull that (guarded
    lazy import). Falls back to the local best-effort count only when the ws_health
    value is absent. Never raises."""
    try:
        import data_collector as _dc_wt
        n = getattr(_dc_wt, "_ws_health", {}).get("asyncio_tasks")
        if n is not None:
            return n
    except Exception:
        pass
    return _best_effort_task_count()


def get_memory_stats() -> dict:
    """M2 — public snapshot of process memory for control_api / the boot report.
    growth_kb_per_hr is a linear estimate over the current rolling window."""
    with _rss_lock:
        n = len(_rss_history)
        rss_kb = _last_rss_kb
        peak = _rss_peak_kb
        growth = 0
        if n >= 2:
            t0, k0 = _rss_history[0]
            t1, k1 = _rss_history[-1]
            dt = t1 - t0
            # Need a meaningful window: over < ~60s the extrapolation to KB/hr
            # blows up into the garbage huge number we saw. Report 0 until the
            # history spans a real window, then clamp so a transient RSS jump
            # can't emit an absurd rate.
            if dt >= 60.0:
                rate = (k1 - k0) / (dt / 3600.0)
                growth = int(max(-10_000_000, min(10_000_000, rate)))
    return {
        "rss_kb":           rss_kb,
        "rss_peak_kb":      peak,
        "rss_mb":           rss_kb // 1024,
        "samples":          n,
        "growth_kb_per_hr": growth,
        # R4 — soft-cap guardrail state (UI banner / control_api).
        "soft_cap_mb":            _soft_cap_mb(),
        "memory_restart_pending": _memory_restart_pending,
        # R1 — latest tracemalloc top-3 allocation sites (leak source).
        "tracemalloc_top":        list(_tracemalloc_top),
        # R1/R4 — last pre-restart snapshot (read back from settings KV, guarded)
        # so live growth AND the leak state at the last self-restart are visible
        # in one place. None when no restart has been recorded yet.
        "tracemalloc_pre_restart": _read_tracemalloc_pre_restart(),
        # Authoritative WS-loop task count sampled by data_collector into
        # _ws_health (this thread has no asyncio loop of its own); falls back to
        # the local best-effort count only when that value is absent.
        "asyncio_tasks":          _ws_loop_task_count(),
    }


# ── R1.3 — periodic stale per-symbol state sweep ─────────────────────────────
# Symbols leave the trading universe on rotation, but this file's high-churn
# per-symbol dicts key by symbol and were never pruned — keys accumulate for the
# process lifetime (an unbounded contributor to the RSS climb). This sweep drops
# keys for symbols no longer in _active_universe, NEVER touching a symbol we hold
# (_positions/_pos_by_symbol) or are mid buy/sell (_buying/_selling). O(dict
# size), infrequent (~every 3 min). It complements purge_symbol_state (the full
# per-symbol teardown used on explicit watchlist removal) as a lightweight
# periodic backstop for keys that leaked past that path.
_last_state_sweep_ts: float = 0.0
_STATE_SWEEP_INTERVAL_SEC = 180.0             # ~3 min


def _sweep_stale_symbol_state() -> None:
    """R1.3 — drop per-symbol keys for symbols no longer in the active universe
    from the churn dicts, and keep _stop_out_ts pruned to its rolling window.
    Never drops a held (_pos_by_symbol) or in-flight (_buying/_selling) symbol.
    Never raises."""
    global _last_state_sweep_ts
    now = time.time()
    if now - _last_state_sweep_ts < _STATE_SWEEP_INTERVAL_SEC:
        return
    _last_state_sweep_ts = now
    try:
        universe = set(_active_universe)
    except Exception:
        universe = set()
    if not universe:
        # Unknown/empty universe (early boot / scanner not warm) — don't risk
        # dropping live keys; wait for a populated universe.
        return
    # Symbols that must NEVER be swept, regardless of universe membership.
    keep: set = set(universe)
    try:
        with _positions_lock:
            keep.update(p.get("symbol") for p in _positions)
    except Exception:
        pass
    try:
        keep.update(_pos_by_symbol.keys())
    except Exception:
        pass
    try:
        with _buying_lock:
            keep.update(_buying)
    except Exception:
        pass
    try:
        with _selling_lock:
            keep.update(_selling)
    except Exception:
        pass

    # Every churn dict named in R1.3. Some are keyed by a plain symbol, some by
    # a (symbol, reason) tuple — take element 0 for tuples so both prune cleanly.
    churn_dicts = (
        _bookticker_cache, _maker_abandon_counts, _lot_waste_flags,
        _loss_cooldown, _candidacy_cooldown, _buy_ready_since, _ratchet_state,
        _sell_last_failed_ts, _sell_last_failed_reason, _ghost_check_fails,
        _last_ws_price_ts, _pos_peaks, _stop_loss_confirmation,
        _last_abort_log_ts, _sell_trace_log_ts, _last_binance_err_log_ts,
        _skip_log_dedupe, _shadow_log_dedupe, _near_miss_snap_ts,
    )
    dropped = 0
    for _d in churn_dicts:
        try:
            stale = [k for k in list(_d.keys())
                     if (k[0] if isinstance(k, tuple) else k) not in keep]
            for k in stale:
                _d.pop(k, None)
                dropped += 1
        except Exception:
            pass

    # _stop_out_ts is a plain rolling list (not per-symbol) — verify it stays
    # bounded to its window rather than being appended forever.
    try:
        cutoff = now - _STOP_PAUSE_WINDOW_SEC
        _stop_out_ts[:] = [t for t in _stop_out_ts if t >= cutoff]
    except Exception:
        pass

    if dropped:
        try:
            database.log_activity(
                f"state sweep: dropped {dropped} stale per-symbol key(s) for "
                f"symbols outside the {len(universe)}-symbol universe (mem cleanup)",
                "info")
        except Exception:
            pass


def capture_recent_log_lines(n: int = 50) -> List[str]:
    """M2 — the most recent N activity-log rows (oldest→newest) as formatted
    strings, so control_api can fold them into the boot report after an unclean
    restart. Reuses database.get_activity_log; never raises."""
    try:
        rows = database.get_activity_log(limit=max(1, int(n)))
    except Exception:
        return []
    out: List[str] = []
    for r in reversed(rows):   # get_activity_log returns newest-first
        ts = r.get("timestamp", "")
        lvl = str(r.get("level", "info")).upper()
        msg = r.get("message", "")
        out.append(f"[{ts}] {lvl}: {msg}")
    return out


def detect_restart_reason() -> dict:
    """K3.3 — classify this boot as clean vs crash. control_api calls this once at
    boot (after load_risk_latches) and logs the result. Returns
    {clean: bool, last_heartbeat_ts, note}. The clean-shutdown marker is cleared
    after reading so it is consumed exactly once (a marker surviving to a boot must
    have been written by the previous run's shutdown, i.e. after the last boot)."""
    last_hb: Optional[float] = None
    marker_ts: Optional[float] = None
    try:
        raw_hb = database.get_setting(_LAST_HEARTBEAT_KEY)
        last_hb = float(raw_hb) if raw_hb not in (None, "") else None
    except Exception:
        last_hb = None
    try:
        raw_marker = database.get_setting(_CLEAN_SHUTDOWN_KEY)
        marker_ts = float(raw_marker) if raw_marker not in (None, "") else None
    except Exception:
        marker_ts = None
    if marker_ts is not None:
        # Marker present → graceful stop, UNLESS a heartbeat is newer than the
        # marker (process kept running after writing it → treat as crash).
        if last_hb is not None and last_hb > marker_ts + 1.0:
            clean = False
            note = ("clean-shutdown marker older than last heartbeat — process "
                    "ran on past the marker; treating as unclean restart")
        else:
            clean = True
            note = "clean-shutdown marker present — previous stop was graceful"
    else:
        clean = False
        note = ("no clean-shutdown marker — previous run did not reach the "
                "graceful shutdown path (crash / kill / OOM / hard restart)")
    # Rotate the marker so the next boot cannot misread this one.
    try:
        database.save_setting(_CLEAN_SHUTDOWN_KEY, "")
    except Exception:
        pass
    return {"clean": clean, "last_heartbeat_ts": last_hb, "note": note}


import atexit as _atexit
_atexit.register(_write_clean_shutdown_marker)

# A `systemctl restart` / normal stop sends SIGTERM, which kills the process
# WITHOUT running atexit — so every deploy looked like an "unclean" crash in the
# restart-reason detector. Handle SIGTERM/SIGINT: write the clean marker, then
# re-raise the default behavior so shutdown proceeds normally. Only installed in
# the main thread (signals can't be set elsewhere); guarded so it never blocks import.
def _graceful_signal_shutdown(_signum, _frame):  # pragma: no cover - signal path
    try:
        _write_clean_shutdown_marker()
    except Exception:
        pass
    try:
        persist_risk_latches()
    except Exception:
        pass
    os._exit(0)


try:
    import signal as _signal
    import threading as _thr_sig
    if _thr_sig.current_thread() is _thr_sig.main_thread():
        _signal.signal(_signal.SIGTERM, _graceful_signal_shutdown)
        _signal.signal(_signal.SIGINT, _graceful_signal_shutdown)
except Exception:
    pass

# R1.1 — start leak instrumentation at module import (guarded; never blocks boot).
_maybe_start_tracemalloc()


# §3.1c(ii) — veto-only heartbeat: every entries.eval_heartbeat_sec (default
# 15 s) re-run the buy check so an armed setup (5m signals already passing)
# can enter the moment a VETO clears (spread narrows, regime flips back,
# cooldown expires). Entry SIGNALS themselves refresh ONLY on 5m closes —
# _check_buys_from_cache reads the cache, it never recomputes signals.
_entry_heartbeat_thread: Optional[threading.Thread] = None


def _entry_heartbeat_loop() -> None:
    _log_entry_timing_mode_once()
    while True:
        try:
            _refresh_heartbeat_ts()   # K3.3 — liveness marker for crash detection
            _sample_rss()             # M2 — RSS sampling (+ R4 soft-cap guardrail)
            _sample_tracemalloc()     # R1.2 — ~5-min tracemalloc TOP10 leak snapshot
            _sweep_stale_symbol_state()  # R1.3 — ~3-min stale per-symbol key sweep
            cfg = _entries_cfg()
            interval = max(1.0, float(cfg.get("eval_heartbeat_sec", 15.0)))
            if not cfg["tick_entries"]:   # tick mode has its own dispatch path
                try:
                    import data_collector as _dc_hb
                    _dispatch_buy_check(dict(_dc_hb.prices))
                except Exception:
                    pass
        except Exception:
            interval = 15.0
        time.sleep(interval)


def start_entry_heartbeat() -> None:
    """Start (or restart) the §3.1 veto-recheck heartbeat thread."""
    global _entry_heartbeat_thread
    if _entry_heartbeat_thread is not None and _entry_heartbeat_thread.is_alive():
        return
    _entry_heartbeat_thread = threading.Thread(
        target=_entry_heartbeat_loop, name="entry-heartbeat", daemon=True)
    _entry_heartbeat_thread.start()


# ── O5.1/O5.3 — fast re-check (confirm-then-fire) loop ────────────────────────
# The veto heartbeat above only re-checks buys every eval_heartbeat_sec (~15 s),
# so an armed candidate could wait most of a candle before its fresh re-check
# even ran (the live J1 buy-lag). This loop re-checks the SAME gated buy path on
# a ~2.5 s cadence — NOT tick-chasing: it merely triggers the identical
# _dispatch_buy_check / _check_buys_from_cache pipeline sooner, and the fresh
# re-check + confirm-then-fire gate inside it still decide every entry. To keep
# CPU sane it dispatches ONLY when there is a cached-green candidate OR a symbol
# is mid-confirmation (a pending _buy_ready_since timer) — it never re-scores the
# whole universe just to spin. Indicator computation stays event-driven (Part C);
# this only re-evaluates the DECISION on already-cached fresh indicators.
_FAST_RECHECK_SEC = 2.5
_fast_recheck_thread: Optional[threading.Thread] = None


def _fast_recheck_loop() -> None:
    while True:
        interval = _FAST_RECHECK_SEC
        try:
            cfg = _entries_cfg()
            # Legacy per-tick mode has its own dispatch; and never run before
            # entries are armed (same choke point _check_buys_from_cache uses).
            if not cfg["tick_entries"] and _entries_armed():
                strat = _load_strategy()
                _min_sigs = int(strat.get("min_signals", config.MIN_SIGNALS_TO_BUY))
                with _signal_cache_lock:
                    _has_green = any(
                        v.get("score", 0) >= _min_sigs
                        for v in _signal_cache.values())
                # Cheap dispatch gate: a cached-green candidate to (re-)confirm,
                # or a symbol already mid-confirmation whose timer must advance.
                if _has_green or _has_pending_buy_confirmation():
                    try:
                        import data_collector as _dc_fr
                        _dispatch_buy_check(dict(_dc_fr.prices))
                    except Exception:
                        _dispatch_buy_check({})
        except Exception:
            interval = _FAST_RECHECK_SEC
        time.sleep(interval)


def start_fast_recheck() -> None:
    """Start (or restart) the O5.1 fast re-check (confirm-then-fire) thread."""
    global _fast_recheck_thread
    if _fast_recheck_thread is not None and _fast_recheck_thread.is_alive():
        return
    _fast_recheck_thread = threading.Thread(
        target=_fast_recheck_loop, name="fast-recheck", daemon=True)
    _fast_recheck_thread.start()


# ── I1 — no scoring/entry on partial buffers; arm entries last ────────────────
# A symbol whose 1m/5m buffers aren't backfilled yet is EXCLUDED from entry
# evaluation (reason backfill_warmup) — NOT scored 0, NOT a rejection/veto — so
# T2/P2/M4 never read empty buffers and silently zero the score post-restart.
# Separately, the whole buy path stays disarmed until the data layer reports
# backfill_complete (or a ~2min grace elapses). EXITS are independent of both.

def _entry_backfill_ready(sym: str) -> bool:
    """I1.1 — per-symbol entry gate. Prefers data_collector.backfill_ready(sym)
    (True once 1m≥16 AND 5m≥21 for that symbol). When that function is absent
    (older data_collector) returns True so the existing MIN-candle checks in the
    data layer remain the only gate — no behavior change."""
    try:
        import data_collector as _dc_br
        fn = getattr(_dc_br, "backfill_ready", None)
        if callable(fn):
            return bool(fn(sym))
    except Exception:
        pass
    return True   # function absent → fall back to existing checks (fail-open)


_backfill_warmup_log: Dict[str, float] = {}
_backfill_warmup_lock = threading.Lock()


def _note_backfill_warmup(sym: str) -> None:
    """I1.1 — record that `sym` was excluded from entry as backfill_warmup.
    Deduped to one activity log per symbol per 5 min so the warmup window never
    spams the log; NOT routed through _record_rejection (this is not a
    rejection/veto and must not inflate rejection diagnostics or score the
    symbol 0)."""
    now = time.time()
    with _backfill_warmup_lock:
        last = _backfill_warmup_log.get(sym, 0.0)
        if now - last < 300.0:
            return
        _backfill_warmup_log[sym] = now
    try:
        database.log_activity(
            f"{sym}: entry skipped — backfill_warmup (buffers not ready)", "info")
    except Exception:
        pass


_entries_armed_flag: bool = False
_entries_arm_deadline: Optional[float] = None
_ENTRY_ARM_GRACE_SEC: float = 120.0   # ~2min fallback so a stuck backfill
_entries_arm_lock = threading.Lock()  # can't permanently disable trading
# R2/I1 — hard safety ceiling: even if held prices can NEVER be seeded (persistent
# REST outage), trading can't stay disabled forever. Past this deadline entries arm
# LOUDLY with a CRITICAL log flagging the still-unpriced held positions.
_entries_arm_hard_deadline: Optional[float] = None
_ENTRY_ARM_HARD_CEILING_SEC: float = 600.0   # 10min absolute cap
_entries_arm_notready_log_ts: float = 0.0    # throttle the "NOT armed" retry log


def _entries_armed() -> bool:
    """I1.2 — arm entries LAST. The buy path stays disarmed until the data layer
    reports get_data_health()['backfill_complete'] is True, or a ~2min grace
    elapses (fallback). Logs the transition once. Held-symbol EXIT monitoring is
    a separate path and is unaffected by this gate. Once armed, stays armed.

    R2/I1 — CRITICAL boot invariant: held-position prices MUST be seeded (a fresh
    price for EVERY open position) BEFORE entries arm. A restart must never leave
    open positions unpriced. Neither backfill-complete NOR the grace timeout arms
    while any held position lacks a fresh price — instead the batched REST held-
    price seed is retried and arming is withheld. New-buy evaluation stays
    separately gated per-symbol on backfill_warmup, so 'backfill 0%' alone never
    blocks arming once held prices are seeded. Only a hard safety ceiling can arm
    with still-unpriced held positions (and it does so LOUDLY)."""
    global _entries_armed_flag, _entries_arm_deadline, _entries_arm_hard_deadline
    global _entries_arm_notready_log_ts
    with _entries_arm_lock:
        if _entries_armed_flag:
            return True
        now = time.time()
        if _entries_arm_deadline is None:
            _entries_arm_deadline = now + _ENTRY_ARM_GRACE_SEC
            _entries_arm_hard_deadline = now + _ENTRY_ARM_HARD_CEILING_SEC
        complete = False
        pct = None
        try:
            import data_collector as _dc_arm
            fn = getattr(_dc_arm, "get_data_health", None)
            if callable(fn):
                h = fn() or {}
                complete = bool(h.get("backfill_complete"))
                for _k in ("backfill_pct", "backfill_percent", "backfill_progress"):
                    if h.get(_k) is not None:
                        try:
                            pct = float(h.get(_k))
                        except (TypeError, ValueError):
                            pct = None
                        break
        except Exception:
            complete = False

        # ── R2/I1 held-price gate (runs BEFORE any arming decision) ───────────
        # Every open position must have a fresh price. If not, retry the batched
        # REST held-price seed and withhold arming — even past the grace timeout.
        try:
            unpriced = _held_unpriced_symbols()
            if unpriced:
                unpriced = _seed_held_prices(reason="arming_gate")
        except Exception:
            # Never let the seed path crash boot. If we can't even confirm held
            # readiness, be conservative and withhold arming until the hard
            # ceiling — trading can't wedge forever, but we won't arm blind early.
            try:
                unpriced = _held_unpriced_symbols()
            except Exception:
                unpriced = ["<unknown>"]
        if unpriced:
            if _entries_arm_hard_deadline is not None and now >= _entries_arm_hard_deadline:
                # Hard safety ceiling — trading can't stay disabled forever, but
                # arm LOUDLY so it's unmistakable that held stops may be blind.
                _entries_armed_flag = True
                try:
                    database.log_activity(
                        f"entries armed — HARD SAFETY CEILING; {len(unpriced)} held "
                        f"positions STILL UNPRICED: {unpriced} — held stops may be "
                        f"flying blind", "error")
                except Exception:
                    pass
                try:
                    log_diag_issue(
                        "price_feed", "error",
                        "Entries armed with unpriced held positions (hard ceiling)",
                        detail=f"{unpriced}")
                except Exception:
                    pass
                return True
            if now - _entries_arm_notready_log_ts >= 10.0:
                _entries_arm_notready_log_ts = now
                try:
                    database.log_activity(
                        f"entries NOT armed — {len(unpriced)} held positions still "
                        f"unpriced (retrying)", "warn")
                except Exception:
                    pass
            return False

        # ── Held prices seeded — apply the (unchanged) backfill arming gate ───
        if complete:
            _entries_armed_flag = True
            try:
                database.log_activity(
                    "entries armed — backfill complete (held prices seeded)", "info")
            except Exception:
                pass
            return True
        if now >= _entries_arm_deadline:
            _entries_armed_flag = True
            _pct_s = f"{pct:.0f}%" if pct is not None else "unknown"
            try:
                database.log_activity(
                    f"entries armed — grace timeout, backfill {_pct_s} "
                    f"(held prices seeded)", "warn")
            except Exception:
                pass
            return True
        return False


# ── Shared sell execution (used by both realtime_monitor and signal_scanner) ──

def _min_lot_qty(symbol: str) -> float:
    """Symbol's LOT_SIZE minQty (0.0 when unknown). Lets the cancel-first
    release check tell a released balance from genuine dust."""
    try:
        from exchange_info import get_symbol_filters
        f = get_symbol_filters(symbol) or {}
        return float(f.get("min_qty") or 0.0)
    except Exception:
        return 0.0


def _cancel_resting_before_sell(pos: dict, sym: str, reason: str, mode: str) -> str:
    """H1 fix 1: UNIVERSAL cancel-first for EVERY local sell path (sl, hard-stop,
    breakeven-stop, trail, take-profit, force-sell, recycler, manual, delist/
    ghost). Runs at the TOP of _execute_sell — inside the _selling critical
    section held by the caller — BEFORE the balance clamp, so a resting maker-TP
    / OCO leg that has LOCKED the base asset is cancelled and the locked balance
    released before we clamp to FREE balance. Without this, free≈0 rounds the
    sell qty to 0 → 'below_min_qty' skip → the loop spins forever and the
    position is UNPROTECTED on the downside.

    Returns:
      'released' — nothing rests, or the resting order was cancelled and the
                   locked balance is released → proceed to clamp + market sell.
      'filled'   — a resting order FILLED during cancel (-2011) and the base
                   balance is now ~0 → the position was closed by that fill
                   (finalized here for managed orders, else left for the manager
                   poll) → caller ABORTS the market sell (no double sell).
      'retry'    — cancel failed / balance not released / balance re-fetch
                   failed → abort this attempt, next tick retries.

    Paper mode / paper fallback: no managed/exchange orders exist → fast no-op
    'released' (guarded on live mode and not paper fallback).
    """
    if exit_orders is None or mode != "live" or connection.is_using_paper_fallback():
        return "released"

    asset = sym[:-4] if sym.endswith("USDT") else sym
    pos_qty = float(pos.get("quantity") or 0.0)

    # 1a. Managed tracking (local dict — cheap) AND live open orders (source of
    #     truth: a maker-TP / OCO leg may rest even if get_managed lost it across
    #     a restart). get_open_orders is per-symbol (weight 6).
    managed = None
    try:
        managed = exit_orders.get_managed(sym)
    except Exception:
        managed = None
    open_orders = []
    try:
        open_orders = binance_direct.get_open_orders(sym) or []
    except Exception as _oe:
        database.log_activity(
            f"[ManagedExit] {sym}: get_open_orders failed before sell "
            f"({type(_oe).__name__}: {_oe})", "warn")
        # Managed tracking (if any) still drives the cancel below; if there is no
        # managed tracking either we cannot confirm a resting order — proceed and
        # let the clamp's locked-aware guard route back here if coins are locked.
        if not managed:
            return "released"

    if not managed and not open_orders:
        return "released"   # nothing rests — normal clamp/sell

    # 1b. Cancel the managed exit first (handles the -2011 finalize-from-fill via
    #     the existing NO-DOUBLE-SELL helper).
    if managed:
        proceed = _clear_managed_exit_before_sell(pos, sym, reason)
        _invalidate_acct_cache()
        if not proceed:
            with _positions_lock:
                still_open = any(p.get("id") == pos.get("id") for p in _positions)
            if still_open:
                return "retry"       # cancel failed — retry next tick
            _clear_sell_skip(sym)
            return "filled"          # finalized from the fill — nothing to sell

    # 1c. Sweep any RAW open orders (restart: managed tracking lost but the
    #     maker-TP still rests and locks the qty). Tolerate -2011 (already gone).
    try:
        raw_orders = binance_direct.get_open_orders(sym) or []
    except Exception:
        raw_orders = open_orders
    for o in raw_orders:
        oid = o.get("orderId")
        if oid is None:
            continue
        try:
            binance_direct.cancel_order(sym, oid)
            _invalidate_acct_cache()
            database.log_activity(
                f"[ManagedExit] {sym}: cancelled orphan open order {oid} before "
                f"local sell ({reason})", "warn")
        except binance_direct.BinanceDirectError as _ce:
            if _ce.code == -2011:
                _invalidate_acct_cache()   # already filled/cancelled — release check below decides
                continue
            database.log_activity(
                f"[ManagedExit] {sym}: cancel of open order {oid} failed "
                f"({_ce.code}: {_ce.msg}) — sell aborted, will retry", "warn")
            _sell_last_failed_ts[sym] = time.time()
            _sell_last_failed_reason[sym] = reason
            return "retry"

    # 1c/step 2. Confirm the release: poll up to ~2s until free ≈ position qty
    #            (locked coins released). If a -2011 fill collapsed free+locked to
    #            ~0 the position already exited → 'filled' (let the manager poll
    #            finalize when it wasn't a managed order we could book here).
    min_lot = _min_lot_qty(sym)
    dust = max(min_lot, 1e-12)
    deadline = time.time() + 2.0
    while True:
        try:
            acct = _cached_acct(force=True)
        except Exception as _ae:
            database.log_activity(
                f"[ManagedExit] {sym}: balance re-fetch after cancel failed "
                f"({type(_ae).__name__}: {_ae}) — will retry", "warn")
            return "retry"
        free, locked = _asset_free_locked(acct, asset)
        if free is None:
            free, locked = 0.0, 0.0
        total = free + locked
        if total < dust and pos_qty > 0:
            database.log_activity(
                f"[ManagedExit] {sym}: base balance ~0 after cancel — resting order "
                f"already filled/closed the position; aborting market sell ({reason})",
                "warn")
            _clear_sell_skip(sym)
            return "filled"
        if locked < dust or free >= min(pos_qty, total) * 0.999:
            _clear_sell_skip(sym)   # release confirmed — sell can proceed
            return "released"
        if time.time() >= deadline:
            database.log_activity(
                f"[ManagedExit] {sym}: locked balance not released after cancel "
                f"(free={free}, locked={locked}) — will retry ({reason})", "warn")
            _sell_last_failed_ts[sym] = time.time()
            _sell_last_failed_reason[sym] = reason
            return "retry"
        time.sleep(0.2)


def _execute_sell(pos: dict, price: float, reason: str):
    """Execute a market sell for pos at price. Logs trade, cleans up state.
    Caller MUST have already added pos['symbol'] to _selling before submitting
    to the executor — this function just verifies and executes."""
    from datetime import timezone as _tz
    pos["_sell_picked_up_ts"] = time.time()
    sym  = pos["symbol"]
    try:
        database.log_activity(
            f"[SELL_START] {sym} ({reason}): raw_qty={pos.get('quantity')} price={price}",
            "info"
        )
    except Exception:
        pass
    qty  = _floor_qty(pos["quantity"], pos["symbol"])
    mode = get_mode()
    now  = datetime.now(_tz.utc).isoformat()

    if qty <= 0 or price <= 0:
        database.log_activity(
            f"[SELL_EARLY_RETURN] {sym} ({reason}): qty={qty} price={price} — invalid, skipping",
            "warn"
        )
        with _selling_lock:
            _selling.discard(sym)
            _selling_ts.pop(sym, None)
        return

    # ── H1 fix 1: UNIVERSAL cancel-first ──────────────────────────────────────
    # Release any resting maker-TP / OCO that LOCKED the base asset BEFORE the
    # balance clamp, so a stop / breakeven-stop / hard-stop / trail / take-profit
    # / force-sell / recycler / manual sell never spins 'below_min_qty' on our
    # own locked coins. No-op in paper mode / paper fallback.
    if mode == "live":
        _rel = _cancel_resting_before_sell(pos, sym, reason, mode)
        if _rel in ("filled", "retry"):
            # 'filled' → a resting order already closed the position (finalized
            # here, or the manager poll will); 'retry' → cancel failed / balance
            # not released. Either way abort this attempt; the guard is released.
            if _rel == "filled":
                _clear_sell_skip(sym)
            with _selling_lock:
                _selling.discard(sym)
                _selling_ts.pop(sym, None)
            return

    # ── Lot-step rounding (toggleable via strategy.use_lot_step_rounding) ───
    _strategy_ls = _load_strategy()
    if _strategy_ls.get("use_lot_step_rounding", True):
        try:
            from exchange_info import compute_sell_quantity
            _ls_qty, _ls_leftover, _ls_reason = compute_sell_quantity(sym, qty, current_price=price)
            if _ls_qty <= 0:
                database.log_activity(
                    f"[SELL_SKIPPED] {sym} ({reason}): lot_step — {_ls_reason} (raw_qty={qty})",
                    "warn"
                )
                with _selling_lock:
                    _selling.discard(sym)
                    _selling_ts.pop(sym, None)
                return
            if _ls_leftover > 0.0:
                log.info("%s: lot_step rounding sells %s (leftover %s)", sym, _ls_qty, _ls_leftover)
            qty = _ls_qty
        except Exception as _ls_exc:
            log.warning("%s: lot_step rounding failed (%s), using raw qty", sym, _ls_exc)

    # ── Clamp to actual Binance free balance (prevents APIError -2010) ─────────
    # H1 fix 2/3: read FREE and LOCKED and use the 5s-cached account snapshot so
    # a stuck sell no longer hammers the signed account endpoint every ~0.75s.
    # LOCKED-BALANCE-AWARE clamp: when the rounded free qty is below the min lot
    # but the coins are merely LOCKED by our own resting order (free+locked still
    # covers the lot), route BACK to the cancel-first path instead of skipping
    # 'below_min_qty'. Only skip as dust when free+locked is genuinely below min.
    if mode == "live":
        try:
            _asset = sym[:-4]  # e.g. "XRP" from "XRPUSDT"
            _acc = _cached_acct()  # geo-block-safe direct transport, 5s cached
            _free, _locked = _asset_free_locked(_acc, _asset)
            if _free is not None:
                if abs(qty - _free) > qty * 0.001:  # > 0.1% mismatch — log it
                    log.warning(
                        "%s: qty mismatch — db=%s, binance_free=%s, using %s for sell (%s)",
                        sym, qty, _free, min(qty, _free), reason
                    )
                qty = min(qty, _free)
                # Re-apply lot-step rounding after balance clamp
                if _strategy_ls.get("use_lot_step_rounding", True):
                    try:
                        from exchange_info import compute_sell_quantity
                        _clamped_qty, _, _cr = compute_sell_quantity(sym, qty)
                        if _clamped_qty <= 0:
                            _dust = max(_min_lot_qty(sym), 1e-12)
                            _tot = (_free or 0.0) + (_locked or 0.0)
                            if (_locked or 0.0) >= _dust and _tot >= _dust:
                                # Coins are LOCKED by a resting bot order (not
                                # dust) → cancel-first, do NOT skip.
                                database.log_activity(
                                    f"[SELL_LOCKED] {sym} ({reason}): free~{_free} "
                                    f"locked={_locked} — {_cr}; coins locked by resting "
                                    f"order, cancelling instead of skipping", "warn")
                                _invalidate_acct_cache()
                                _rel2 = _cancel_resting_before_sell(pos, sym, reason, mode)
                                if _rel2 == "released":
                                    _acc = _cached_acct(force=True)
                                    _free2, _ = _asset_free_locked(_acc, _asset)
                                    qty = min(_floor_qty(pos["quantity"], sym), _free2 or 0.0)
                                    _clamped_qty, _, _cr = compute_sell_quantity(sym, qty)
                                    if _clamped_qty > 0:
                                        qty = _clamped_qty
                                    else:
                                        _note_sell_skip(sym, "below_min_qty")
                                        _log_skip_dedup(
                                            sym, "below_min_qty",
                                            f"[SELL_SKIPPED] {sym} ({reason}): after "
                                            f"balance clamp — {_cr}", "warn")
                                        with _selling_lock:
                                            _selling.discard(sym)
                                            _selling_ts.pop(sym, None)
                                        return
                                else:
                                    # filled → position closed; retry → try again.
                                    if _rel2 == "filled":
                                        _clear_sell_skip(sym)
                                    with _selling_lock:
                                        _selling.discard(sym)
                                        _selling_ts.pop(sym, None)
                                    return
                            else:
                                # Genuine dust (free+locked below min lot) — skip,
                                # deduped, and track for stuck escalation.
                                _note_sell_skip(sym, "below_min_qty")
                                _log_skip_dedup(
                                    sym, "below_min_qty",
                                    f"[SELL_SKIPPED] {sym} ({reason}): after balance "
                                    f"clamp — {_cr}", "warn")
                                with _selling_lock:
                                    _selling.discard(sym)
                                    _selling_ts.pop(sym, None)
                                return
                        else:
                            qty = _clamped_qty
                    except Exception:
                        pass
        except Exception as _bal_exc:
            log.warning("%s: failed to fetch Binance balance for sell clamp: %s", sym, _bal_exc)

    # Verify position still exists — another executor task may have already sold it
    # (e.g. force-sell came in while we were queued).
    with _positions_lock:
        pos_id = pos.get("id")
        if pos_id and not any(p.get("id") == pos_id for p in _positions):
            database.log_activity(
                f"[SELL_EARLY_RETURN] {sym} ({reason}): position id={pos_id} already removed — skipping duplicate sell",
                "warn"
            )
            with _selling_lock:
                _selling.discard(sym)
                _selling_ts.pop(sym, None)
            return  # already sold

    try:
        _do_execute_sell(pos, sym, qty, price, reason, mode, now)
    except Exception as _exc_sell:
        database.log_activity(
            f"[SELL_EXCEPTION] {sym} ({reason}): {type(_exc_sell).__name__}: {_exc_sell}",
            "error"
        )
        log_diag_issue(
            "force_sell" if reason == "force-sell" else "sell_monitor",
            "error",
            f"Sell failed: {sym} ({reason}) — {type(_exc_sell).__name__}",
            detail=str(_exc_sell),
        )
        # Force-sell / manual: if we hit an exception, the user explicitly wanted the
        # position gone. Remove from records so retries stop rather than leaving a ghost.
        if reason in ("force-sell", "manual", "user-initiated"):
            # Log a best-effort trade record before deleting so audit trail is preserved.
            try:
                _fs_qty    = float(pos.get("quantity", 0))
                _fs_entry  = float(pos.get("entry_price", 0))
                _fs_cost   = _fs_qty * _fs_entry
                _fs_quote  = price * _fs_qty
                _fs_fee_frac = _fee_rate_for(sym)
                _fs_sfee   = _fs_quote * _fs_fee_frac
                _fs_bfee   = float(pos.get("buy_fee_usdt") or _fs_cost * _fs_fee_frac)
                _fs_profit = _fs_quote - _fs_sfee - _fs_cost - _fs_bfee
                database.log_trade({
                    "coin":             sym,
                    "mode":             mode,
                    "entry_price":      _fs_entry,
                    "exit_price":       price,
                    "quantity":         _fs_qty,
                    "budget_usdt":      pos.get("budget_usdt"),
                    "buy_fee":          _fs_bfee,
                    "sell_fee":         _fs_sfee,
                    "net_profit":       _fs_profit,
                    "profitable":       1 if _fs_profit > 0 else 0,
                    "entry_rsi":        pos.get("entry_rsi"),
                    "timestamp_buy":    pos.get("timestamp"),
                    "timestamp_sell":   now,
                    "sell_reason":      f"force-sell-failed: {str(_exc_sell)[:200]}",
                    "signal_snapshot":  pos.get("signal_snapshot"),
                },
                    exit_label="force",
                    entry_fee_usdt=_fs_bfee,
                    exit_fee_usdt=_fs_sfee,
                    hold_time_sec=(time.time() - float(pos.get("opened_at_ts") or 0)
                                   if pos.get("opened_at_ts") else None),
                    origin=pos.get("origin", "auto"))
                database.log_activity(
                    f"[FORCE-SELL-FAIL] {sym}: trade record logged with estimated P&L "
                    f"(est_profit={_fs_profit:.4f})",
                    "warn"
                )
            except Exception as _log_err:
                database.log_activity(
                    f"[FORCE-SELL-FAIL] {sym}: also failed to log trade record: {_log_err}",
                    "error"
                )
            with _positions_lock:
                _positions[:] = [p for p in _positions if p.get("id") != pos.get("id")]
            if pos.get("id"):
                try:
                    database.delete_position(pos["id"])
                except Exception:
                    pass
            _rebuild_pos_index()
            database.log_activity(
                f"[FORCE_CLEAN] {sym}: force-sell raised exception — position removed from records",
                "warn"
            )
    finally:
        with _selling_lock:
            _selling.discard(sym)
            _selling_ts.pop(sym, None)
        # Always clear the smart-hold peak — even on sell failure — so that a
        # later position re-opened on the same symbol doesn't inherit a stale
        # high-water mark and instantly trip its trailing stop.
        _pos_peaks.pop(sym, None)
        # P2: clear the per-symbol profit-ratchet high-water mark for the same
        # reason — a re-opened position must arm the ratchet from scratch.
        _ratchet_state.pop(sym, None)


def _do_execute_sell(pos: dict, sym: str, qty: float, price: float, reason: str, mode: str, now: str):
    """Inner sell logic — called only when the _selling guard is held."""
    from datetime import timezone as _tz

    # Phase 1 §1.3: the price passed into _execute_sell is the trigger price —
    # capture it BEFORE any fresh-quote reassignment; slippage_bps baseline.
    _trigger_price = price

    # ── Phase 2 §2.4/§2.5 NO-DOUBLE-SELL contract ─────────────────────────────
    # A managed exchange-side exit (maker TP / OCO) for this symbol MUST be
    # cancelled BEFORE any local market sell — otherwise the resting order can
    # fill while our market sell is in flight and the coins get sold twice.
    # Runs inside the _selling critical section (guard held by the caller).
    if exit_orders is not None and mode == "live":
        if not _clear_managed_exit_before_sell(pos, sym, reason):
            # Either the exchange order turned out FILLED (position finalized
            # from that fill — nothing left to sell) or the cancel failed and
            # selling now would risk a double sell. Abort this attempt; the
            # caller's finally releases the _selling claim.
            return

    # Ensure PaperClient price is current before the sell
    try:
        _client().update_price(sym, price)
    except Exception:
        pass

    # Paper mode: self-heal coin balance if it went missing after a restart.
    # open position record is the source of truth for what we own.
    _pc = _client()
    if mode != "live" and hasattr(_pc, "_balances"):
        coin = sym[:-4]
        with _pc._lock:
            current_coin_bal = _pc._balances.get(coin, 0.0)
            if current_coin_bal < qty * 0.99:
                _pc._balances[coin] = qty
                snapshot = dict(_pc._balances)
            else:
                snapshot = None
        if snapshot is not None:
            database.save_paper_state(snapshot)
            database.log_activity(
                f"[PaperWallet] Auto-credited {qty:.8f} {coin} "
                f"(had {current_coin_bal:.8f}) — balance was out of sync after restart",
                "warn",
            )

    _is_paper = (mode != "live")
    pos["_sell_gate_start_ts"] = time.time()
    # ── FINAL SAFETY GATE (P1) — the min_profit_usdt floor is a PROFIT-TAKING
    # guard ONLY. It applies to take-profit and profit-ratchet exits. EVERY
    # protective / risk exit (stop-loss, hard-stop, breakeven-stop, trailing
    # stop, force, delist, reconcile, ghost, recycler, manual) executes at market
    # REGARDLESS of the floor. Previously a breakeven-stop that could not clear
    # the 0.01 USDT minimum was vetoed and HELD — price then rode back to the
    # real -1R stop and a scratch became a full loss (the UNI/BNB inversion).
    _profit_take = _is_profit_taking_exit(reason)
    # P1.3 regression assertion: the profit-taking and protective-stop sets MUST
    # stay disjoint. If a future edit ever classifies a protective stop as
    # profit-taking, a breakeven/ATR/hard stop could be blocked by the floor
    # again — that is the exact safety-inversion bug. Scream on the CRITICAL path
    # and force PROTECTIVE handling (never gate it, never silently hold).
    if _profit_take and reason in _PROTECTIVE_STOP_REASONS:
        try:
            database.log_activity(
                f"CRITICAL P1 REGRESSION {sym}: protective exit '{reason}' is "
                f"classified as profit-taking — the min_profit_usdt floor would "
                f"BLOCK a protective stop. Bypassing the floor and selling at "
                f"market so the stop can never be inverted into a full loss.",
                "error")
        except Exception:
            pass
        _profit_take = False
    if _profit_take:
        # force_fresh=True: _profitable_sell_check fetches a fresh REST price for this
        # symbol RIGHT NOW before evaluating. This prevents a stale price from passing
        # the check while the actual Binance fill comes back at a lower level.
        if not _profitable_sell_check(pos, price, force_fresh=True):
            # Price is not profitable even at the freshest REST quote.
            # Abort — position stays open, next tick re-evaluates. This can ONLY
            # be a profit-taking exit (take-profit / profit-ratchet); protective
            # stops never reach here.
            _now_abort = time.time()
            if _now_abort - _last_abort_log_ts.get(sym, 0) >= _ABORT_LOG_THROTTLE_SEC:
                _last_abort_log_ts[sym] = _now_abort
                _fresh_now = _rest_px.get(sym, 0) or price
                database.log_activity(
                    f"SELL ABORTED {sym} ({reason}): freshest REST price ${_fresh_now:.6f} "
                    f"would not net the {_min_profit_usdt():.4f} USDT minimum profit. "
                    f"Position held — will retry on next favourable tick (log throttled 60s).",
                    "warn"
                )
            _sell_last_failed_ts[sym] = time.time()
            _sell_last_failed_reason[sym] = reason
            return  # exit before placing order
        # If the gate fetched a fresh REST quote, use it as the paper-mode
        # execution price. Only when actually fresh — an old _rest_px entry
        # must not override the WS trigger price.
        if (time.time() - _rest_px_sym_ts.get(sym, 0)) < 5.0:
            price = _rest_px.get(sym, 0) or price

    pos["_sell_gate_done_ts"] = time.time()
    pos["_sell_binance_start_ts"] = time.time()
    try:
        # Paper mode: pass the trigger price directly so concurrent WebSocket/REST
        # updates cannot cause the sell to execute at the wrong price.
        _log_order_intent("SELL", sym, qty, price)
        if _is_paper:
            result = _client().order_market_sell(symbol=sym, quantity=qty, price=price)
        else:
            # Live mode: geo-block-safe direct signed transport
            result = _market_sell(sym, qty)
        _log_order_result("SELL", sym, qty, price, result)
        pos["_sell_binance_done_ts"] = time.time()
        _ghost_check_fails.pop(sym, None)  # reset -2010 backoff on success
        _clear_sell_skip(sym)              # H1 fix 3: sell progressed — clear stuck state
        _invalidate_acct_cache()           # balance changed — force a fresh snapshot next read
        # Local sell completed — drop any managed-exit tracking (defensive: the
        # pre-sell cancel above normally already forgot it).
        if exit_orders is not None:
            try:
                exit_orders.forget(sym)
            except Exception:
                pass
    except Exception as e:
        pos["_sell_binance_done_ts"] = time.time()
        err_str = str(e)
        msg = f"SELL failed {sym} ({reason}): {e}"
        print(f"[TradeEngine] {msg}")
        database.log_activity(msg, "error")
        _sell_last_failed_ts[sym] = time.time()
        _sell_last_failed_reason[sym] = reason

        # -1013: market closed/delisted — blacklist and force-close
        # -2010: insufficient balance — could be ghost OR a quantity/lot-size
        #        mismatch on a real position. Verify with a balance check first.
        is_possible_ghost = "-2010" in err_str or "insufficient balance" in err_str.lower()
        is_closed = "-1013" in err_str or "Market is closed" in err_str
        if is_closed:
            _bad_symbols.add(sym)

        should_force_close = is_closed
        force_close_label = ""

        if is_possible_ghost and not is_closed:
            is_ghost, ghost_reason = _is_truly_ghost_position(sym, float(pos.get("quantity", 0)))
            if is_ghost:
                # Genuinely zero — safe to force-close
                database.log_activity(
                    f"[GHOST CONFIRMED] {sym}: balance={ghost_reason} — closing position tracker", "warn"
                )
                should_force_close = True
                force_close_label = f"ghost position ({ghost_reason})"
            else:
                # Real position — quantity mismatch or lot-size issue
                if ghost_reason == "check_failed":
                    # Exponential backoff: while the signed account endpoint is
                    # down, each 0.5s retry burns a live order attempt on the
                    # same -2010. Back off 1s→2s→4s… capped at 30s, and escalate
                    # a diag error after 5 consecutive failures.
                    n = _ghost_check_fails.get(sym, 0) + 1
                    _ghost_check_fails[sym] = n
                    _sell_last_failed_ts[sym] = time.time() + min(30.0, (2 ** min(n, 5)) * 0.5)
                    database.log_activity(
                        f"[GHOST CHECK FAILED] {sym}: -2010 but balance check inconclusive — "
                        f"retry #{n}, backing off {min(30.0, (2 ** min(n, 5)) * 0.5):.0f}s", "warn"
                    )
                    log_diag_issue("sell_monitor", "error" if n >= 5 else "warn",
                                   f"{sym}: -2010 balance check failed ×{n}, will retry", detail=err_str[:400])
                else:
                    actual_qty = _get_actual_balance(sym)
                    database.log_activity(
                        f"[GHOST FALSE ALARM] {sym}: {ghost_reason} — updating qty to {actual_qty:.6f}", "warn"
                    )
                    log_diag_issue("sell_monitor", "warn",
                                   f"{sym}: qty mismatch ({ghost_reason}), adjusted to {actual_qty:.6f}",
                                   detail=err_str[:400])
                    if actual_qty > 0:
                        with _positions_lock:
                            pos["quantity"] = actual_qty
                if reason in ("force-sell", "manual", "user-initiated"):
                    raise RuntimeError(
                        f"force-sell rejected by Binance (-2010): {err_str[:200]} — "
                        f"ghost_reason={ghost_reason}"
                    )
                return  # let sell retry next tick with corrected qty
        elif is_closed:
            force_close_label = "market closed/delisted"

        if should_force_close:
            database.log_activity(
                f"{sym}: force-closing position — {force_close_label}", "warn"
            )
            log_diag_issue(
                "sell_monitor", "warn",
                f"{sym}: force-closed — {force_close_label}",
                detail=err_str[:400],
            )
            # Phase 1 §1.3: record the force-close as a trade so delist/ghost
            # losses stop being an invisible channel. Estimated P&L (no fill).
            try:
                _fc_qty   = float(pos.get("quantity") or 0)
                _fc_entry = float(pos.get("entry_price") or 0)
                _fc_cost  = _fc_qty * _fc_entry
                _fc_fee_frac = _fee_rate_for(sym)
                _fc_quote = price * _fc_qty
                _fc_sfee  = _fc_quote * _fc_fee_frac
                _fc_bfee  = float(pos.get("buy_fee_usdt") or _fc_cost * _fc_fee_frac)
                _fc_net   = _fc_quote - _fc_sfee - _fc_cost - _fc_bfee
                database.log_trade({
                    "coin":            sym,
                    "mode":            mode,
                    "entry_price":     _fc_entry,
                    "exit_price":      price,
                    "quantity":        _fc_qty,
                    "budget_usdt":     pos.get("budget_usdt"),
                    "buy_fee":         _fc_bfee,
                    "sell_fee":        _fc_sfee,
                    "net_profit":      _fc_net,
                    "profitable":      1 if _fc_net > 0 else 0,
                    "entry_rsi":       pos.get("entry_rsi"),
                    "timestamp_buy":   pos.get("timestamp"),
                    "timestamp_sell":  now,
                    "sell_reason":     f"force-close: {force_close_label}",
                    "signal_snapshot": pos.get("signal_snapshot"),
                },
                    exit_label=("delist" if is_closed else "force"),
                    entry_fee_usdt=_fc_bfee,
                    exit_fee_usdt=_fc_sfee,
                    hold_time_sec=(time.time() - float(pos.get("opened_at_ts") or 0)
                                   if pos.get("opened_at_ts") else None),
                    origin=pos.get("origin", "auto"))
            except Exception:
                pass
            with _positions_lock:
                before = len(_positions)
                _positions[:] = [p for p in _positions if p.get("symbol") != sym]
                if len(_positions) < before:
                    # Remove from DB too so it doesn't come back on restart
                    pos_id = pos.get("id")
                    if pos_id:
                        try:
                            database.delete_position(pos_id)
                        except Exception:
                            pass
                    database.log_activity(f"{sym}: position removed from records", "warn")
            _rebuild_pos_index()
            if exit_orders is not None:
                try:
                    exit_orders.forget(sym)
                except Exception:
                    pass
        return

    # ── Fee-aware fill parsing ───────────────────────────────────────────────────
    # Market orders can split across multiple fills at different price levels.
    # Each fill has its own commission amount and asset.
    # Wrap in try/except so an unexpected Binance response format never causes
    # a silent position ghost (sell executed on Binance, position stays in bot).
    try:
        fills      = result.get("fills", [])
        raw_quote  = float(result.get("cummulativeQuoteQty") or 0)
        _exec_qty  = float(result.get("executedQty") or 0)
        # Exit price = VWAP across ALL fills (cummulativeQuoteQty / executedQty).
        # fills[0] is only the best-priced tranche of a multi-fill market order.
        # (Live only: PaperClient's cummulativeQuoteQty is already net of fee.)
        if mode == "live" and raw_quote > 0 and _exec_qty > 0:
            fill_price = raw_quote / _exec_qty
        elif fills:
            fill_price = float(fills[0].get("price", price))
        else:
            fill_price = price
        budget     = pos["budget_usdt"]
        # Use ACTUAL deployed capital as cost basis, not the requested budget —
        # and use the SAME quantity for cost basis as for proceeds. Lot-step
        # rounding / balance clamps can sell less than pos['quantity']; charging
        # the full recorded quantity against partial proceeds books a phantom loss.
        _pos_qty   = float(pos.get("quantity") or 0)
        _sold_qty  = _exec_qty if _exec_qty > 0 else qty
        _sold_frac = (_sold_qty / _pos_qty) if _pos_qty > 0 else 1.0
        actual_cost = _sold_qty * float(pos["entry_price"])

        if mode == "live":
            sell_fee, fee_asset = _fills_fee_usdt(fills, raw_quote * _fee_rate_for(sym))
            # Binance sell-fee semantics: a stablecoin commission IS deducted from
            # the quote proceeds (received = cummulativeQuoteQty - commission);
            # a BNB commission comes out of the separate BNB balance, so the
            # full quote amount is received. ("estimated" fallback is treated as
            # quote-deducted — the conservative assumption.)
            if fee_asset in ("USDT", "BUSD", "USDC", "estimated"):
                usdt_returned = raw_quote - sell_fee
            else:
                usdt_returned = raw_quote
            buy_fee = float(pos.get("buy_fee_usdt") or _pos_qty * float(pos["entry_price"]) * _fee_rate_for(sym))
            buy_fee *= _sold_frac  # charge only the sold portion's share of the buy fee
        else:
            sell_fee      = sum(float(f.get("commission") or 0) for f in fills)
            usdt_returned = raw_quote
            buy_fee       = actual_cost * _fee_rate_for(sym)

        net_profit = usdt_returned - actual_cost - buy_fee
    except Exception as parse_err:
        # Sell DID execute on Binance — fall back to estimates so trade is
        # still recorded and position is properly cleaned up.
        database.log_activity(
            f"SELL {sym} ({reason}): fill parse error ({parse_err}) — using estimates", "warn"
        )
        fill_price    = price
        raw_quote     = price * pos.get("quantity", 0)
        actual_cost   = float(pos.get("quantity", 0)) * float(pos.get("entry_price", 0))
        _pe_fee_frac  = _fee_rate_for(sym)
        sell_fee      = raw_quote * _pe_fee_frac
        buy_fee       = float(pos.get("buy_fee_usdt") or actual_cost * _pe_fee_frac)
        usdt_returned = raw_quote - sell_fee
        net_profit    = usdt_returned - actual_cost - buy_fee

    # Post-fill integrity check: if a take-profit sell ended up net-negative,
    # classify the cause honestly rather than always blaming slippage.
    #
    # Genuine slippage: fill price materially worse than the trigger price.
    # Fee/rounding loss: fill matched the trigger but fees ate the margin —
    #   not a market execution problem, just a thin-margin edge case.
    #   These should NOT trigger the re-buy cooldown.
    if net_profit < 0 and reason == "take-profit":
        _trig_px  = price                     # the trigger price we checked against
        _fill_px  = fill_price                # what Binance actually filled at
        _slip_pct = ((_trig_px - _fill_px) / _trig_px * 100) if _trig_px > 0 else 0
        # Slippage threshold: 0.05% — smaller moves are fee/rounding artefacts
        _SLIP_THRESHOLD_PCT = 0.05
        if _slip_pct > _SLIP_THRESHOLD_PCT:
            reason = "slippage-loss"
            _loss_cooldown[sym] = time.time() + _LOSS_COOLDOWN_SEC
            database.log_activity(
                f"SLIPPAGE-LOSS {sym}: trigger=${_trig_px:.6f} fill=${_fill_px:.6f} "
                f"slip={_slip_pct:.3f}% net={net_profit:.4f} USDT "
                f"— relabeled 'slippage-loss', cooldown {_LOSS_COOLDOWN_SEC // 60}min", "warn"
            )
        else:
            reason = "below-breakeven"
            # No cooldown — this was a fee/rounding edge, not a bad fill.
            database.log_activity(
                f"BELOW-BREAKEVEN {sym}: trigger=${_trig_px:.6f} fill=${_fill_px:.6f} "
                f"slip={_slip_pct:.3f}% net={net_profit:.4f} USDT "
                f"— relabeled 'below-breakeven' (no cooldown)", "warn"
            )

    buy_ts  = pos.get("timestamp", now)
    sell_ts = now
    try:
        buy_dt  = datetime.fromisoformat(buy_ts.replace("Z", "+00:00"))
        sell_dt = datetime.fromisoformat(sell_ts.replace("Z", "+00:00"))
        # Both are timezone-aware; subtraction works directly
        if buy_dt.tzinfo and sell_dt.tzinfo:
            duration = int((sell_dt - buy_dt).total_seconds())
        else:
            duration = 0
    except Exception:
        duration = 0
        buy_dt   = datetime.now(_tz.utc)

    _intended_buy_price = pos.get("intended_buy_price")
    _pos_buy_slippage   = pos.get("buy_slippage_pct")
    _intended_sell_price = price
    _sell_slippage_pct = ((fill_price - price) / price * 100) if price > 0 else None
    trade_record = {
        "coin":               sym,
        "mode":               mode,
        "entry_price":        pos["entry_price"],
        "exit_price":         fill_price,
        "quantity":           qty,
        "budget_usdt":        pos["budget_usdt"],
        "buy_fee":            buy_fee,
        "sell_fee":           sell_fee,
        "net_profit":         net_profit,
        "profitable":         1 if net_profit > 0 else 0,
        "duration_seconds":   duration,
        "entry_rsi":          pos.get("entry_rsi"),
        "entry_ma_position":  pos.get("entry_ma_position"),
        "entry_bb_position":  pos.get("entry_bb_position"),
        "entry_volume_trend": pos.get("entry_volume_trend"),
        "hour_of_day":        buy_dt.hour,
        "day_of_week":        buy_dt.weekday(),
        "timestamp_buy":      buy_ts,
        "timestamp_sell":     sell_ts,
        "sell_reason":        reason,
        "signal_snapshot":    pos.get("signal_snapshot"),
        "intended_buy_price":  _intended_buy_price,
        "intended_sell_price": _intended_sell_price,
        "buy_slippage_pct":    _pos_buy_slippage,
        "sell_slippage_pct":   _sell_slippage_pct,
    }
    # ── Sell timing diagnostic ────────────────────────────────────────────────
    try:
        _t_done = time.time()
        pos["_sell_complete_ts"] = _t_done
        _trigger  = pos.get("_sell_trigger_ts", 0)
        _pickup   = pos.get("_sell_picked_up_ts", 0)
        _gate_s   = pos.get("_sell_gate_start_ts", 0)
        _gate_e   = pos.get("_sell_gate_done_ts", 0)
        _bin_s    = pos.get("_sell_binance_start_ts", 0)
        _bin_e    = pos.get("_sell_binance_done_ts", _t_done)
        _crossed = pos.get("_sell_target_crossed_ts", 0)
        if _trigger > 0:
            stages = {
                "target_crossed_to_trigger_ms": (_trigger - _crossed) * 1000 if _crossed and _crossed <= _trigger else 0,
                "trigger_to_pickup_ms": (_pickup - _trigger) * 1000 if _pickup else 0,
                "pickup_to_gate_ms":    (_gate_s - _pickup) * 1000 if _gate_s and _pickup else 0,
                "gate_ms":              (_gate_e - _gate_s) * 1000 if _gate_e and _gate_s else 0,
                "gate_to_binance_ms":   (_bin_s - _gate_e) * 1000 if _bin_s and _gate_e else 0,
                "binance_ms":           (_bin_e - _bin_s) * 1000 if _bin_e and _bin_s else 0,
                "total_ms":             (_t_done - _trigger) * 1000,
            }
            detail = " | ".join(f"{k}={v:.0f}" for k, v in stages.items() if v >= 1)
            if stages["total_ms"] > 2000:
                log_diag_issue("sell_timing", "warn",
                    f"{sym} sell slow: {stages['total_ms']:.0f}ms total ({reason})", detail=detail)
            else:
                log_diag_issue("sell_timing", "info",
                    f"{sym} sold in {stages['total_ms']:.0f}ms ({reason})", detail=detail)
    except Exception:
        pass

    # ── Sell latency for DB ───────────────────────────────────────────────────
    try:
        _fill_ts = time.time()
        _target_crossed_ts_val = pos.get("_sell_target_crossed_ts")
        _trigger_ts_val        = pos.get("_sell_trigger_ts")

        _target_to_trigger_ms: Optional[int] = None
        _trigger_to_filled_ms: Optional[int] = None
        _target_crossed_iso:   Optional[str] = None

        if _target_crossed_ts_val and _trigger_ts_val:
            _target_to_trigger_ms = int((_trigger_ts_val - _target_crossed_ts_val) * 1000)
        if _trigger_ts_val:
            _trigger_to_filled_ms = int((_fill_ts - _trigger_ts_val) * 1000)
        if _target_crossed_ts_val:
            from datetime import datetime as _dt2, timezone as _tz2
            _target_crossed_iso = _dt2.fromtimestamp(_target_crossed_ts_val, tz=_tz2.utc).isoformat()
    except Exception:
        _target_to_trigger_ms = None
        _trigger_to_filled_ms = None
        _target_crossed_iso   = None

    # Write the closed-trade record BEFORE deleting the position row — a crash
    # or DB error between the two must never lose the trade record. A leftover
    # position row is recoverable (the next sell attempt gets -2010 and the
    # ghost check cleans it up); a missing trade row is lost P&L history.
    # Phase 1 §1.3 measurement kwargs — exit label, slippage, fees, hold time.
    try:
        _slip_bps = ((fill_price - _trigger_price) / _trigger_price * 10000.0
                     if _trigger_price and _trigger_price > 0 else None)
    except Exception:
        _slip_bps = None
    try:
        _opened_ts = float(pos.get("opened_at_ts") or 0)
        _hold_sec = (time.time() - _opened_ts) if _opened_ts > 0 else (
            float(duration) if duration > 0 else None)
    except Exception:
        _hold_sec = float(duration) if duration else None
    # F1.1: realized_r vs planned_rr for this close (activity log + R-stats ring).
    _record_exit_r(sym, pos.get("entry_price"), fill_price,
                   pos.get("sl_distance_pct"), pos.get("planned_rr"),
                   _exit_label_for(reason), pos=pos)
    # F7: every close MUST carry an explicit exit_label. _exit_label_for never
    # returns empty, but assert loudly + default to 'unknown' so an unlabeled
    # exit can never silently reach the DB (6-of-16 unlabeled diagnostic).
    _exit_lbl = _exit_label_for(reason) or "unknown"
    if not _exit_label_for(reason):
        log_diag_issue("unlabeled_exit", "warn",
                       f"UNLABELED EXIT {sym}: reason={reason!r} — defaulted to 'unknown'")
    try:
        database.log_trade(trade_record,
                           target_crossed_to_trigger_ms=_target_to_trigger_ms,
                           trigger_to_filled_ms=_trigger_to_filled_ms,
                           target_crossed_ts=_target_crossed_iso,
                           exit_label=_exit_lbl,
                           slippage_bps=_slip_bps,
                           entry_fee_usdt=buy_fee,
                           exit_fee_usdt=sell_fee,
                           hold_time_sec=_hold_sec,
                           origin=pos.get("origin", "auto"))
    except Exception as te:
        database.log_activity(f"log_trade error ({sym}): {te}", "warn")

    # ── Phase 4 §4.2b/§4.3 risk hooks (after the trade row is written):
    # consecutive-loss counter update + slippage-window refresh. Never raises.
    _note_trade_closed(sym, net_profit)

    # Remove position from memory and DB — sell is confirmed on Binance and the
    # trade record is already written. Supabase sync and learning run after so
    # a slow network never delays cleanup.
    if pos.get("id"):
        try:
            database.delete_position(pos["id"])
        except Exception:
            pass
    with _positions_lock:
        _positions[:] = [p for p in _positions if p.get("id") != pos.get("id")]
    _rebuild_pos_index()
    _pos_peaks.pop(sym, None)

    # ── §3.4b/§3.4c — stop-out protections ───────────────────────────────────
    # Stop-loss family exits (stop-loss / hard-stop-loss / oco-sl, i.e. exit
    # label sl|hard_sl) set the per-symbol cooldown_after_sl_min re-entry
    # cooldown AND count toward the global correlated-dump pause. Other losing
    # exits keep their existing behavior (e.g. slippage-loss 30-min cooldown).
    try:
        if _exit_label_for(reason) in ("sl", "hard_sl"):
            _record_stop_out(sym, reason)
    except Exception:
        pass

    try:
        import supabase_sync
        # Paper mode: read balance from memory (instant, no network call).
        # Live mode: skip the get_account() REST call — Supabase sync gets
        # an estimate from net_profit instead, avoiding a 10s blocking call
        # on the sell worker thread.
        _pc_sync = _client()
        if get_mode() != "live" and hasattr(_pc_sync, "_balances"):
            with _pc_sync._lock:
                usdt_now = float(_pc_sync._balances.get("USDT", 0.0))
        else:
            usdt_now = _get_usdt_balance()
        supabase_sync.sync_sell_result_sync(trade_record, sym, usdt_now)
    except Exception as se:
        err_msg = f"Supabase sync error after selling {sym}: {se}"
        print(f"[TradeEngine] {err_msg}")
        try:
            database.log_activity(err_msg, "error")
        except Exception:
            pass
    try:
        learning.learn_from_trade(trade_record)
    except Exception as le:
        database.log_activity(f"learn_from_trade error ({sym}): {le}", "warn")

    pnl_sign = "+" if net_profit >= 0 else ""
    mode_tag = "LIVE" if mode == "live" else "PAPER"
    sell_msg = (
        f"[{mode_tag}] SOLD {sym} @ ${fill_price:.4f} "
        f"· received {usdt_returned:.4f} USDT · P&L: {pnl_sign}{net_profit:.4f} USDT"
        f"  ({reason}, held {duration}s)"
    )
    print(f"[{sell_ts}] {sell_msg}")
    try:
        database.log_activity(sell_msg, "info")
    except Exception:
        pass


# ── Real-time buy check from signal cache (called from realtime_monitor) ──────

def _check_buys_from_cache(prices: Dict[str, float]):
    """
    Real-time buy executor. Reads the pre-computed signal cache — zero I/O per
    call except when a buy is actually about to fire. Throttled to at most once
    every 3 s to avoid hammering get_account() on every WebSocket tick.
    """
    global _last_buy_check, _buys_this_scan, _last_buy_ts
    _buys_this_scan = 0  # reset per-scan counter each invocation

    # ── I1.2 — arm entries LAST: withhold ALL buy-check evaluation until the
    # data layer reports backfill_complete (or a ~2min grace elapses). This is
    # the single choke point every buy path flows through (5m-close, veto
    # heartbeat, signal_scanner, legacy tick). Held-symbol EXIT monitoring runs
    # on a separate path (sell monitor / held watchdog) and is NOT gated here.
    if not _entries_armed():
        return

    # Load strategy once up front (_load_strategy is mtime-cached — cheap).
    strategy = _load_strategy()
    _pre_min_sigs = int(strategy.get("min_signals", config.MIN_SIGNALS_TO_BUY))
    # Engine-aware pre-gate: when the signal engine is enabled the legacy
    # 6-signal score is NOT the buy criterion, so the any_ready shortcut must
    # not gate the engine's own per-coin evaluation.
    _pre_engine_active = (
        SIGNAL_REGISTRY_AVAILABLE
        and bool(strategy.get("signal_engine", {}).get("enabled", False))
    )

    # Paused bot: check BEFORE the any_ready pre-gate — previously this warn
    # sat below the pre-gate and was unreachable unless a coin coincidentally
    # passed the legacy score, so a silently-paused bot logged nothing.
    if not strategy.get("trading_active", True):
        global _last_paused_log
        _now_p = time.time()
        if _now_p - _last_paused_log >= 60.0:
            _last_paused_log = _now_p
            database.log_activity("Buy check: trading_active=False — bot is paused", "warn")
        return

    # ── §3.4c global correlated-dump pause: >=3 stop-outs in 10 min → ALL
    # entries paused for 5 min. Exits are unaffected.
    if _global_stop_pause_active():
        global _last_stop_pause_log_ts
        _now_gp = time.time()
        if _now_gp - _last_stop_pause_log_ts >= 60.0:
            _last_stop_pause_log_ts = _now_gp
            _rem_gp = int(_global_stop_pause_until - _now_gp)
            database.log_activity(
                f"Buy check: global_stop_pause active ({_rem_gp}s remaining) — "
                f"all entries paused after correlated stop-outs", "warn")
            _record_rejection("(all)", 0, "global_stop_pause", f"{_rem_gp}s remaining")
        return

    # ── §4.2a daily loss stop: pause ALL buys until next UTC midnight (or
    # manual resume). EXITS KEEP RUNNING — this only returns from the buy path.
    # I3.4: transition-only logging. The ERROR trip line is emitted once by
    # _check_daily_loss_stop(); here we only keep the UI rejection reason fresh
    # while latched and log a single INFO line when the stop releases. No 60 s
    # heartbeat. Persistent "stopped" state stays in get_risk_status().
    global _daily_stop_logged
    if _check_daily_loss_stop():
        _st_ds = _daily_pnl_state()
        _record_rejection("(all)", 0, "daily_loss_stop",
                          f"pnl_today={_st_ds['pnl_today']:.2f}")
        return
    if _daily_stop_logged:
        _daily_stop_logged = False
        database.log_activity(
            "DAILY LOSS STOP: released — buys re-enabled "
            "(new UTC day or manual resume); exits were never paused", "info")
        persist_risk_latches()   # J2 — record the release transition

    # ── §4.2b consecutive-loss pause: buys paused 60 min after N losses in a row.
    global _consec_pause_logged
    _now_cl = time.time()
    if _now_cl < _consec_loss_pause_until:
        _rem_cl = int(_consec_loss_pause_until - _now_cl)
        _record_rejection("(all)", 0, "consecutive_loss_pause",
                          f"{_rem_cl}s remaining")
        return
    if _consec_pause_logged:
        _consec_pause_logged = False
        database.log_activity(
            "CONSECUTIVE-LOSS PAUSE: expired — buys re-enabled", "info")
        persist_risk_latches()   # J2 — record the release transition

    # Fast pre-check: any coin signalling BUY? (no lock needed for scalar read)
    # Use the runtime min_signals setting — the hardcoded config default would
    # silently defeat a user threshold set below it. Legacy path only: with the
    # signal engine active we proceed to the per-coin loop and let it decide.
    with _signal_cache_lock:
        any_ready = any(v["score"] >= _pre_min_sigs for v in _signal_cache.values())
        cache_size = len(_signal_cache)

    if cache_size == 0 or (not any_ready and not _pre_engine_active):
        global _last_no_signal_log
        now_ns = time.time()
        if now_ns - _last_no_signal_log >= 60.0:
            _last_no_signal_log = now_ns
            if cache_size == 0:
                import data_collector as _diag_dc
                n_ticks   = sum(1 for v in _diag_dc.price_ticks.values()   if len(v) >= _diag_dc._MIN_TICKS)
                n_samples = sum(1 for v in _diag_dc.price_samples.values() if len(v) >= _diag_dc._MIN_SAMPLES)
                n_ws      = sum(1 for v in _diag_dc.ws_candles.values()    if len(v) >= _diag_dc._MIN_CANDLES)
                database.log_activity(
                    f"Signal cache empty — data sources: ticks={n_ticks}, samples={n_samples}, ws_candles={n_ws}", "info"
                )
                _record_rejection("(all)", 0, "signal_cache_empty",
                                  f"ticks={n_ticks} samples={n_samples} ws_candles={n_ws}")
            else:
                with _signal_cache_lock:
                    snap = dict(_signal_cache)
                top = sorted(snap.items(), key=lambda x: -x[1]["score"])[:5]
                detail = " | ".join(
                    f"{s}:score={v['score']} RSI={v['signals'].get('rsi',False)} trend={v['signals'].get('trend',False)}"
                    for s, v in top
                )
                database.log_activity(
                    f"Buy check: {cache_size} coins, none at score≥{_pre_min_sigs} — top5: {detail}", "info"
                )
                if top:
                    _record_rejection(top[0][0], top[0][1].get("score", 0),
                                      "pre_gate_below_min_signals",
                                      f"none of {cache_size} coins at score>={_pre_min_sigs}")
        return

    # Throttle: don't call get_account() faster than every 1 s
    now = time.time()
    if now - _last_buy_check < 0.1:
        return
    _last_buy_check = now

    # Always refresh risk params first — even when at capacity, so the sell monitor
    # uses current TP/SL settings rather than stale cached values.
    _refresh_risk_params()

    # Enforce position capacity — §4.1/A3: the EFFECTIVE slot count (raw
    # max_positions auto-degraded by floor(effective_allocation /
    # min_position_usdt)). Budget-mode formulas are untouched; only the slot
    # count degrades. Legacy configs (no strategy.sizing block) resolve
    # max_positions from the root key as before.
    _slots_info = effective_slots()
    max_pos = _slots_info["effective_slots"]

    # ── M1.2 — neutral-regime risk reduction via SLOTS (not ticket size) ──────
    # Resolve the effective neutral-scaling mode once per scan. In "slots" mode
    # we do NOT shrink the ticket (get_budget_for_coin keeps full size); instead
    # we cap concurrent NEW entries (≈half the slots) AND double the entry pacing
    # so notional stays tradeable. "size" keeps the legacy ticket multiplier;
    # "off" disables scaling. The decision is logged once per transition.
    _eff_stagger  = _BUY_STAGGER_SEC
    _eff_max_buys = _MAX_BUYS_PER_SCAN
    try:
        _regime_now = get_btc_regime()
        if _regime_now == "neutral":
            _neutral_mode = _resolve_neutral_scaling(
                {"neutral_scaling_mode": _neutral_scaling_mode_cfg()},
                _slots_info["effective_allocation"],
                _slots_info["max_positions"],
                _tradeable_min(),
            )
            if _neutral_mode == "slots" and max_pos > 0:
                _nmult = _neutral_size_mult()
                _slot_cap = max(1, min(math.ceil(max_pos * _nmult),
                                       max(1, max_pos // 2)))
                max_pos = min(max_pos, _slot_cap)
                _eff_stagger  = _BUY_STAGGER_SEC * 2.0
                _eff_max_buys = max(1, _MAX_BUYS_PER_SCAN // 2)
            _log_neutral_scaling_transition(_regime_now, _neutral_mode,
                                            _slots_info, max_pos)
        else:
            _log_neutral_scaling_transition(_regime_now, "n/a", _slots_info, max_pos)
    except Exception:
        pass

    with _positions_lock:
        n_open = len(_positions)
    if n_open >= max_pos:
        # Periodic heartbeat so the user sees the bot is still alive at capacity.
        # Without this, no buy logs are emitted while at max_positions and the
        # UI looks like it has been paused for no reason.
        global _last_at_capacity_log
        _now_cap = time.time()
        if _now_cap - _last_at_capacity_log >= 60.0:
            _last_at_capacity_log = _now_cap
            _degr_tag = (f" (degraded from {_slots_info['max_positions']}: "
                         f"allocation {_slots_info['effective_allocation']:.2f})"
                         if _slots_info["degraded"] else "")
            database.log_activity(
                f"At max positions ({n_open}/{max_pos}){_degr_tag} — bot active, "
                f"waiting for an exit before opening new trades",
                "info",
            )
        # Q1 — mark every buy-ready candidate 'no free slot' so this early return
        # never leaves a ready coin with no attempt AND no block reason.
        _mark_ready_no_slot(strategy, n_open, max_pos)
        return  # already at capacity — buys resume automatically after a sell

    # Enforce configurable min_signals threshold (overrides config.MIN_SIGNALS_TO_BUY)
    min_sigs = int(strategy.get("min_signals", config.MIN_SIGNALS_TO_BUY))

    # Phase 2+3: new signal engine — active only when strategy["signal_engine"]["enabled"]=True
    signal_engine_active = (
        SIGNAL_REGISTRY_AVAILABLE
        and bool(strategy.get("signal_engine", {}).get("enabled", False))
    )

    approved = {
        c["symbol"]: c
        for c in strategy.get("approved_coins", [])
        if c.get("approved")
    }
    if not approved:
        database.log_activity("Buy check: no approved coins in strategy.json", "warn")
        return

    from datetime import timezone as _tz
    mode = get_mode()

    # HARD BLOCK: live mode running on the paper fallback client must never buy —
    # the fills would be simulated but recorded as mode='live'. Sells/monitoring
    # continue via the direct transport; buys resume when reconnect succeeds.
    if mode == "live" and connection.is_using_paper_fallback():
        global _last_fallback_block_log
        _now_fb = time.time()
        if _now_fb - _last_fallback_block_log >= 60.0:
            _last_fallback_block_log = _now_fb
            database.log_activity(
                "Buy check: live mode is in paper fallback (Binance client unavailable) "
                "— all buys blocked until the live connection is restored", "warn"
            )
        return

    usdt_balance = _get_usdt_balance()
    ts_now = datetime.now(_tz.utc).isoformat()

    # Build the cache snapshot but only emit the activity log every 30 s — these
    # writes hold the global DB lock and previously fired on every WS-tick scan,
    # serializing the sell monitor and signal scanner behind them.
    with _signal_cache_lock:
        cache_snapshot = dict(_signal_cache)
    ready_syms = [s for s, v in cache_snapshot.items() if v["score"] >= min_sigs and s in approved]
    global _last_buy_scan_log
    _now_ts = time.time()
    if ready_syms or (_now_ts - _last_buy_scan_log) >= 30.0:
        _last_buy_scan_log = _now_ts
        database.log_activity(
            f"Buy scan: USDT={usdt_balance:.2f} | {len(ready_syms)} coin(s) ready (min {min_sigs}/6 signals): "
            + (", ".join(f"{s}(score={cache_snapshot[s]['score']})" for s in ready_syms[:6]) or "none"),
            "info"
        )

    # ── F2.1: legacy binary BTC macro gate REMOVED ───────────────────────────
    # The 3-state regime is now the sole macro control: risk_off is enforced by
    # the REGIME_risk_off veto in signal_registry (per-coin), and neutral halves
    # size via get_budget_for_coin/_neutral_size_mult. The old scan-level gate
    # here duplicated the risk_off veto and — on the OR-based classifier — paused
    # ALL buys on green days, spamming "MACRO GATE: BTC bearish" every ~10s.
    # Regime transitions are now logged once via _log_btc_regime_transition.

    # Read mandatory signal thresholds once (hot-reloadable from strategy.json).
    # Single source of truth for the RSI buy threshold: the nested
    # signal_thresholds key (what the UI writes and the engine's M1 reads)
    # first, then the legacy root key, then 40 — the old root-only read left
    # the legacy gate and the engine enforcing two different thresholds.
    mandatory_enabled = bool(strategy.get("mandatory_signals_enabled", True))
    rsi_threshold     = float(
        strategy.get("signal_thresholds", {}).get(
            "rsi_buy_threshold", strategy.get("rsi_buy_threshold", 40.0)
        )
    )

    # J1 — one evaluation heartbeat per pass that reaches the per-symbol loop.
    global _decision_heartbeat
    _decision_heartbeat += 1

    # Q1 — helpers so the per-symbol loop never leaves a buy-ready candidate
    # without an explicit machine-readable state.
    _items = list(cache_snapshot.items())
    # Q1 — every symbol evaluated buy-ready (cached-green) THIS pass. The
    # end-of-pass backstop stamps a catch-all reason on any of these that
    # reached the end of the loop with neither an attempt nor a fresh block
    # reason, so the DECISION-GAP tripwire can never fire "no block reason".
    _ready_syms_pass: set = set()

    # ── Perf: batch the per-coin DB reads into ONE query each per scan ──────────
    # The buy-check used to fire ~2 get_candles + 1 slippage query PER COIN (~280
    # DB hits / 280 database._lock acquisitions per scan), which queued behind the
    # paper-shadow's lock and turned a 35ms scan into 120s. Prefetch the latest
    # candle per coin and the recent slippage per coin in ONE query each; the loop
    # then reads from these in-memory maps. Slippage prefetch also warms
    # _slippage_cache so _wolf_inputs → _avg_slippage_bps never touches the DB.
    _approved_list = list(approved)
    try:
        _latest_candles = database.get_latest_candle_bulk(
            _approved_list, config.CANDLE_TIMEFRAME)
    except Exception:
        _latest_candles = {}
    try:
        from datetime import timedelta as _td_sl
        _since_sl = (datetime.now(timezone.utc)
                     - _td_sl(days=_SLIPPAGE_MAX_AGE_DAYS)).isoformat()
        _slip_bulk = database.get_slippage_avg_bulk(
            _approved_list, get_mode(), _since_sl)
        _now_sl = time.time()
        with _slippage_lock:
            for _sy in _approved_list:
                _slippage_cache[_sy] = {
                    "avg_bps": _slip_bulk.get(_sy), "n": 1 if _sy in _slip_bulk else 0,
                    "ts": _now_sl}
    except Exception:
        pass

    # Operator model: WolfScore is the SOLE gate. When on, the legacy signal-count
    # pre-gate is retired — EVERY approved coin is scored by WolfScore and the ≥65
    # floor decides eligibility (so a high-WolfScore coin is never excluded just for
    # having few legacy signals). Config-revert-proof (defaults True in schema).
    _wolfscore_sole_gate = bool(_entries_cfg().get("wolfscore_sole_gate", True))

    def _cached_ready(_c) -> bool:
        if _wolfscore_sole_gate:
            return True   # WolfScore ≥65 floor is the real gate, not signal count
        return bool(_c.get("score", 0) >= min_sigs)

    # ── WolfBot v0.5 Part S-2 — WolfScore v3 buy SELECTION ────────────────────
    # Score every approved, cached-ready candidate ONCE this pass (guarded) with
    # WolfScore v3 and, when entries.ev_ranking_enabled, process eligible coins in
    # DESCENDING WolfScore order (buy the 72 before the 51). This ONLY reorders the
    # iteration among already-eligible coins and feeds the friction hard-gate +
    # adaptive floor below; every existing veto/gate is unchanged and still
    # authoritative — the score never overrides a veto. A scoring failure (or
    # ev_model missing) leaves _wolf_scores empty → today's raw-score / first-ready
    # behavior. Ranking never lets a coin skip a gate.
    #
    # Cohort context (R = decoupling needs the basket median 15m ROC) and the BTC
    # 1h regime tilt are computed ONCE per cycle and passed to every candidate.
    _entries_cfg_s2 = _entries_cfg()
    _ev_ranking_on   = bool(_entries_cfg_s2.get("ev_ranking_enabled", True))
    # THE buy gate (operator model): WolfScore ≥ buy_score_threshold (default 65).
    # buy_score_threshold is canonical; min_win_probability_floor is the legacy
    # alias kept for back-compat.
    _wolf_abs_floor  = float(_entries_cfg_s2.get("buy_score_threshold",
                             _entries_cfg_s2.get("min_win_probability_floor", 65.0)) or 65.0)
    _wolf_floor_k    = float(_entries_cfg_s2.get("ev_floor_meanstd_k", 0.5) or 0.5)
    try:
        _wolf_floor_mode = str((_load_strategy().get("entries") or {})
                               .get("ev_floor_mode", "absolute") or "absolute")
    except Exception:
        _wolf_floor_mode = "absolute"
    _wolf_scores: Dict[str, dict] = {}
    _wolf_cohort: Dict[str, Any] = {}
    _wolf_tilt = 0.0
    _wolf_af: Optional[dict] = None
    _t_wolf0 = time.perf_counter()   # ── scan instrumentation: WolfScore stage
    _n_scored = 0
    if ev_model is not None:
        try:
            _wolf_tilt = ev_model.regime_tilt(_btc_roc_1h_frac())
        except Exception:
            _wolf_tilt = 0.0
        _cand_items = [(_s, _c) for _s, _c in _items
                       if _s in approved and _cached_ready(_c)]
        _wolf_cohort = _wolf_cohort_from(_cand_items)   # median 15m ROC of the pass (ONCE)
        for _s, _c in _cand_items:
            _ws = _wolf_score_cached(_s, _c, _wolf_cohort, _wolf_tilt)
            if _ws:
                _wolf_scores[_s] = _ws
        _n_scored = len(_cand_items)
        # Adaptive floor over THIS pass's live scores (friction hard-gated ones
        # score 0 and are excluded so they don't drag the distribution down).
        _live_pcts = [float(_v.get("pct")) for _v in _wolf_scores.values()
                      if _v.get("hard_gate") is None and _v.get("pct") is not None]
        _wolf_af = _wolf_adaptive_floor(_live_pcts, _wolf_abs_floor,
                                        _wolf_floor_mode, _wolf_floor_k)
    _wolf_stage_ms = round((time.perf_counter() - _t_wolf0) * 1000.0, 1)
    _scan_stage_ms["wolfscore_ms"] = _wolf_stage_ms
    _scan_stage_ms["n_scored"] = _n_scored
    _t_gate0 = time.perf_counter()   # ── gate/veto loop stage starts below
    # The floor GATES a real buy when the active model is trained AND past the ≥300
    # clean-trade guardrail — OR when the operator has explicitly opted into the
    # "proven-slice" go-live (entries.ev_floor_live_untrained): gate live at the
    # raw-data cliff (≥ min_win_probability_floor, absolute mode) even while the
    # model is untrained, because that cliff is proven by the paper data and is
    # here PAIRED with the up-regime restriction below (the safe subset).
    _ev_floor_live_untrained = bool(_entries_cfg_s2.get("ev_floor_live_untrained", False))
    # Under the sole-gate model the WolfScore floor MUST gate (block < buy_score_
    # threshold) — with the legacy count removed, an advisory floor would mean NO
    # gate at all (buy everything). So sole-gate forces the floor authoritative.
    _ev_floor_on = bool(ev_model is not None
                        and (_ev_floor_active() or _ev_floor_live_untrained
                             or _wolfscore_sole_gate))
    # Go-live regime restriction — up-regime is the proven loss source (paper: 26%
    # win, −1.00R). Until a trained model proves it fixed, live entries in the up
    # regime are vetoed (mode='veto') or held to a higher score (mode='min_score').
    # Paper-shadow is UNAFFECTED (it keeps sampling every regime for training).
    _live_up_mode = str(_entries_cfg_s2.get("live_up_regime_mode", "allow") or "allow")
    _live_up_min_pct = float(_entries_cfg_s2.get("live_up_regime_min_pct", 70.0) or 70.0)
    if _ev_ranking_on and _wolf_scores:
        # Eligible candidates first, DESCENDING WolfScore pct, ties by DESCENDING
        # raw signal score; symbols without a WolfScore fall to the tail (-1 pct).
        def _wolf_rank_key(_it):
            _s, _c = _it
            _e = _wolf_scores.get(_s)
            _p = float(_e.get("pct", -1.0)) if _e else -1.0
            return (_p, float(_c.get("score", 0) or 0))
        _items = sorted(_items, key=_wolf_rank_key, reverse=True)

    def _mark_rest_no_slot(_from_idx: int, _k: int, _n: int) -> None:
        # Capacity hit mid-loop → every still-unprocessed ready candidate gets an
        # explicit 'no free slot' block instead of a silent gap.
        _reason = f"waiting: no free slot ({_k}/{_n})"
        for _s, _c in _items[_from_idx:]:
            if _s in approved and _cached_ready(_c):
                _trace_mark_block(_s, _reason)

    def _top_ready_symbol() -> Optional[str]:
        # WolfBot v0.5 Part S-2 — highest-WolfScore approved buy-ready candidate
        # this pass (instant-fire picks the best win-probability, not the highest
        # raw score). Falls back to highest raw score, then first-in-iteration,
        # when WolfScore is unavailable (all _p == -1 → pure raw-score ranking =
        # today's behavior). Used by instant_fire_when_slots_free.
        _best_s, _best_key = None, None
        for _s, _c in _items:
            if _s in approved and _cached_ready(_c):
                _e = _wolf_scores.get(_s)
                _p = float(_e.get("pct", -1.0)) if _e else -1.0
                _key = (_p, int(_c.get("score", 0)))
                if _best_key is None or _key > _best_key:
                    _best_key, _best_s = _key, _s
        return _best_s

    for _idx, (sym, cached) in enumerate(_items):
        # Re-check capacity before every individual buy — the pre-loop check only
        # guards the entry; without this, all ready coins buy in sequence and blow
        # past the configured max_positions limit.
        with _positions_lock:
            if len(_positions) >= max_pos:
                _mark_rest_no_slot(_idx, len(_positions), max_pos)
                break

        if sym not in approved:
            continue
        # J1 — record that this symbol was evaluated this heartbeat, with its
        # cached (last-candle) buy-ready verdict. This drives the two-state
        # cached-green vs engine-ready-fresh distinction and the DECISION-GAP
        # detector. cached_green mirrors the pre-check's ready test.
        _cached_green = cached.get("score", 0) >= min_sigs
        _trace_mark_evaluated(sym, _cached_green)
        # L1.2 — funnel stage 1: this symbol is buy-ready (cached-green) this
        # heartbeat. Counted alongside the Part J decision-trace hook.
        if _cached_green:
            _funnel_incr("ready")
            _ready_syms_pass.add(sym)
        elif not signal_engine_active:
            # O5.1 — legacy path cached-green flipped OFF → reset the confirm-
            # then-fire timer so the window restarts on the next confirmation.
            _clear_buy_ready(sym)
        # F6: skip coins in the post-fail candidacy cooldown (prevents ~7s churn
        # of a coin the fresh engine re-check just rejected).
        if _in_candidacy_cooldown(sym):
            _record_rejection(sym, cached["score"], "candidacy_cooldown")
            continue
        if not signal_engine_active and cached["score"] < min_sigs:
            _record_rejection(sym, cached["score"], "below_min_signals", f"needed {min_sigs}, got {cached['score']}")
            continue
        if _in_cooldown(sym):
            _record_rejection(sym, cached["score"], "cooldown")
            database.log_activity(f"{sym}: buy skipped — in cooldown", "info")
            continue
        if _loss_cooldown.get(sym, 0) > time.time():
            rem = int(_loss_cooldown[sym] - time.time())
            _record_rejection(sym, cached["score"], "loss_cooldown", f"{rem}s remaining")
            database.log_activity(f"{sym}: buy skipped — post-slippage-loss cooldown ({rem}s remaining)", "info")
            continue

        # ── §4.3 slippage auto-veto: avg |slippage| over the last 20 closed
        # trades (<=7 days old) above risk.max_avg_slippage_bps blocks entries
        # for this symbol until the rolling window decays back under.
        _slip_avg = _slippage_veto_active(sym)
        if _slip_avg is not None:
            _record_rejection(sym, cached["score"], "slippage_veto",
                              f"avg {_slip_avg:.1f}bps")
            continue

        # ── §4.5 correlation guard: while the current BTC 5m candle is red,
        # at most risk.max_new_entries_when_btc_red entries per 5 minutes.
        _corr = _corr_guard_state()
        if _corr["blocked"]:
            _record_rejection(sym, cached["score"], "btc_red_entry_limit",
                              f"{_corr['entries_5min']} entries in 5min "
                              f"(limit {_corr['limit']}, BTC 5m red)")
            # Q1 — surface the friendly waiting-state in the per-symbol trace
            # (overrides the taxonomy reason _record_rejection just stamped).
            _trace_mark_block(
                sym, f"waiting: correlation cap "
                     f"({_corr['entries_5min']}/{_corr['limit']})")
            continue

        with _positions_lock:
            already_held = any(p["symbol"] == sym for p in _positions)

        if already_held:
            _record_rejection(sym, cached["score"], "already_held")
            continue

        # ── I1.1 — exclude not-ready symbols from ENTRY evaluation ────────────
        # A symbol whose 1m/5m buffers aren't backfilled yet is SKIPPED for
        # entry (reason backfill_warmup) — NOT scored 0, NOT counted as a
        # rejection/veto — so T2/P2/M4 never read empty buffers and silently
        # zero the score post-restart. EXITS are unaffected (held symbols hit
        # the already_held continue above and are monitored on the sell path).
        if not _entry_backfill_ready(sym):
            _note_backfill_warmup(sym)
            # Q1 — a cached-green coin still warming up is a state, not a gap.
            if _cached_green:
                _trace_mark_block(sym, "waiting: backfill warmup")
            continue

        # ── WolfBot v0.5 Part S-2 — WolfScore friction hard-gate + floor ──────
        # Two WolfScore checks among already-eligible candidates (both AFTER every
        # veto/safety gate above — the score NEVER overrides a veto):
        #   1) Friction HARD gate: F>0.5 (round-trip cost > half the 1R stop) →
        #      hard_gate=='friction' → always skip (structurally poor entry).
        #   2) Adaptive floor: skip a candidate whose WolfScore pct is below the
        #      regime-aware threshold (absolute floor AND the p75/meanstd rule).
        #      GATES a real buy ONLY when the model is trained AND past the ≥300
        #      clean-trade guardrail (_ev_floor_on); otherwise advisory — the buy
        #      still proceeds. Guarded: no WolfScore → no floor action.
        if ev_model is not None:
            _ws_here = _wolf_scores.get(sym)
            if _ws_here is None:
                _ws_here = _wolf_score_cached(sym, cached, _wolf_cohort, _wolf_tilt)
            if _ws_here is None and _wolfscore_sole_gate:
                # Fail-closed: WolfScore is the sole gate, so a coin we couldn't
                # score cannot be verified as eligible — never buy it blindly.
                _record_rejection(sym, cached.get("score", 0), "no_wolfscore",
                                  "WolfScore unavailable (sole-gate → skip)")
                _trace_mark_block(sym, "blocked: no WolfScore")
                continue
            if _ws_here is not None:
                # 1) Friction hard gate — always authoritative.
                if _ws_here.get("hard_gate") == "friction":
                    _record_rejection(sym, cached.get("score", 0),
                                      "high_friction", "WolfScore friction >50% of stop")
                    _trace_mark_block(sym, "blocked: friction >50% of stop")
                    continue
                # 1b) S3-2 up-regime anti-chasing hard gate — a RULE (not a model
                # probability), so it is authoritative immediately, independent of
                # the trained/clean-trade guardrail. Operator-toggleable via
                # entries.up_extension_veto (when off, no such gate is produced).
                if _ws_here.get("hard_gate") == "extended_uptrend":
                    _record_rejection(sym, cached.get("score", 0),
                                      "up_extension", "extended vs VWAP in uptrend (anti-chase)")
                    _trace_mark_block(sym, "blocked: extended vs VWAP in uptrend")
                    continue
                # 1c) Go-live regime restriction — up-regime is the proven loss
                # source (paper: 26% win, −1.00R). Until a trained model proves it
                # fixed, LIVE up-regime entries are vetoed (or held to a higher
                # score). Applies to live selection only; paper-shadow still samples
                # every regime. mode='allow' disables it.
                if _ws_here.get("regime") == "up" and _live_up_mode != "allow":
                    _pct_up = float(_ws_here.get("pct", 0.0) or 0.0)
                    if _live_up_mode == "min_score" and _pct_up >= _live_up_min_pct:
                        pass  # strong enough to allow limited up-regime exposure
                    else:
                        _reason_up = ("up_regime_paused" if _live_up_mode == "veto"
                                      else "up_regime_below_min")
                        _detail_up = (f"up-regime live entries paused"
                                      if _live_up_mode == "veto"
                                      else f"WolfScore {_pct_up:.0f} < up-regime min "
                                           f"{_live_up_min_pct:.0f}")
                        _record_rejection(sym, cached.get("score", 0),
                                          _reason_up, _detail_up)
                        _trace_mark_block(sym, f"blocked: {_detail_up}")
                        continue
                # 2) Adaptive floor over this pass's WolfScore distribution.
                _pct_here = float(_ws_here.get("pct", 0.0) or 0.0)
                _thr = float(_wolf_af.get("threshold")) if isinstance(_wolf_af, dict) \
                    and _wolf_af.get("threshold") is not None else None
                if _thr is not None and _pct_here < _thr:
                    _floor_msg = f"WolfScore {_pct_here:.0f} < floor {_thr:.0f}"
                    if _ev_floor_on:
                        _record_rejection(sym, cached.get("score", 0),
                                          "ev_prob_floor", _floor_msg)
                        _trace_mark_block(sym, f"blocked: {_floor_msg}")
                        continue
                    # Untrained / below guardrail → advisory only; allow the buy.
                    _trace_mark_block(
                        sym, f"waiting: below WolfScore floor {_thr:.0f} (advisory)")
                    _log_skip_dedup(
                        sym, "ev_floor_advisory",
                        f"{sym}: WolfScore floor advisory — {_floor_msg} "
                        f"(model untrained or below the clean-trade guardrail; "
                        f"buy allowed).", "info")

        # ── Extract signal snapshot for this symbol ────────────────────────────
        sigs    = cached.get("signals", {})
        bb_ok   = cached.get("bb_ok",  True)
        five_ok = cached.get("5m_ok",  True)
        score   = cached["score"]
        rsi_v   = cached.get("rsi_val", 0.0)
        sig_str = (
            f"EMA{'↑' if sigs.get('trend')  else '↓'} "
            f"RSI:{rsi_v:.0f} "
            f"MACD{'+'  if sigs.get('macd')   else '-'} "
            f"Vol{'✓'   if sigs.get('volume') else '✗'} "
            f"OBV{'✓'   if sigs.get('obv')    else '✗'} "
            f"ATR{'✓'   if sigs.get('atr')    else '✗'}"
        )
        bb_str  = "BB:PASS" if bb_ok   else "BB:FAIL"
        m5_str  = "5m:PASS" if five_ok else "5m:FAIL"

        # ── Buy decision: new signal engine OR legacy mandatory/score path ───────
        _buy_decision: dict = {}  # populated below for signal_snapshot capture
        if signal_engine_active:
            _sig_data = {
                **sigs,
                "rsi_value":      rsi_v,
                "current_price":  cached.get("price", 0.0),
                "low_24h":        cached.get("low_24h"),
                "klines_1m":      cached.get("klines_1m", []),
                "stoch_rsi_value": cached.get("stoch_rsi_val"),
                # Phase 3 §3.1/§3.3 fields (populated on 5m closes; the
                # regime is read live — 60 s cache — so the REGIME veto
                # reacts within a minute of BTC flipping risk_off)
                "klines_5m":       cached.get("klines_5m", []),
                "ema50_15m_slope": cached.get("ema50_15m_slope"),
                "bb_position_5m":  cached.get("bb_position_5m"),
                "atr_pct":         cached.get("atr_pct"),
                "btc_regime":      get_btc_regime(),
            }
            _dec = _sr_evaluate_buy_decision(sym, _sig_data, strategy)
            _buy_decision = _dec
            if not _dec["allowed"]:
                _dec_reason = str(_dec.get("reason", ""))
                _is_safety_veto = _dec_reason.startswith("veto_")
                # WolfScore is the SOLE selection gate (operator model). The legacy
                # signal-count ("score_N_below_min") and mandatory-signal rejects
                # must NOT block — the coin already cleared WolfScore ≥ the buy gate
                # above. Only real safety VETOES (spread / ATR-untradeable / regime
                # risk_off) still stop the buy. This keeps the old engine's count out
                # of the buy path regardless of min_score/min_scored config.
                if _wolfscore_sole_gate and not _is_safety_veto:
                    pass  # ignore score/mandatory reject; fall through to vetoes+exec
                else:
                    _record_rejection(sym, _dec.get("score", score), _dec["reason"],
                                      f"score={_dec['score']} fired={_dec['fired_signals']}")
                    _note_candidacy_fail(sym, _dec.get("score"), _dec["reason"])
                    continue
            # Passed the gate — fall through to existing veto checks below
        else:
            # ── Legacy mandatory signal layer ──────────────────────────────────
            # Mandatory 1: EMA trend up (EMA9 > EMA21) — don't buy into a downtrend.
            # Mandatory 2: RSI below threshold — require a real dip, not mid-channel noise.
            if mandatory_enabled:
                if not sigs.get("trend", False):
                    _record_rejection(sym, score, "mandatory_ema_down", "EMA9 < EMA21")
                    continue
                if rsi_v <= 0 or rsi_v >= rsi_threshold:
                    _record_rejection(sym, score, "mandatory_rsi_too_high",
                                      f"rsi={rsi_v:.1f} threshold={rsi_threshold}")
                    continue

        # ── Hard veto checks: BB position and 5m trend ────────────────────────
        if not bb_ok:
            _record_rejection(sym, score, "bb_upper", sig_str)
            database.log_activity(
                f"[SKIP] {sym}: price near upper Bollinger Band | "
                f"{sig_str} | {bb_str} | SKIP(upper band)", "info"
            )
            continue
        if not five_ok:
            _record_rejection(sym, score, "5m_downtrend", sig_str)
            database.log_activity(
                f"[SKIP] {sym}: 5m downtrend | "
                f"{sig_str} | {bb_str} {m5_str} | SKIP(5m downtrend)", "info"
            )
            continue

        # ── 1m BB veto: skip when price is at or above the 1m upper band ──────
        # The 5m BB check (bb_ok above) catches medium-term tops; this catches
        # local 1m tops that the 5m hasn't reflected yet (e.g. INJUSDT case).
        try:
            _c1 = _latest_candles.get(sym)   # in-memory (batched once per scan)
            if _c1:
                bb_pos_1m = _c1.get("bb_position")
                if bb_pos_1m in ("above_upper", "at_upper"):
                    _record_rejection(sym, score, "bb_upper", f"1m {bb_pos_1m}")
                    database.log_activity(
                        f"[SKIP] {sym}: 1m BB {bb_pos_1m} — local top, wait for pullback | "
                        f"{sig_str} | SKIP(1m_top)", "info"
                    )
                    continue
        except Exception:
            pass

        # ── Stagger gate — max _eff_max_buys buys per cycle (M1.2: halved in
        # neutral "slots" mode; _eff_stagger is likewise doubled there) ────────
        if _buys_this_scan >= _eff_max_buys:
            database.log_activity(
                f"Buy scan: capped at {_eff_max_buys} buys this cycle — "
                f"remaining coins evaluated next cycle", "info"
            )
            # Q1 — every still-unprocessed ready candidate is deferred by the
            # per-scan pacing cap, not silently dropped.
            _reason_pace = (f"waiting: buy pacing cap "
                            f"({_buys_this_scan}/{_eff_max_buys} this scan)")
            for _s, _c in _items[_idx:]:
                if _s in approved and _cached_ready(_c):
                    _trace_mark_block(_s, _reason_pace)
            break
        if time.time() - _last_buy_ts < _eff_stagger:
            # Q1 — this slot fired too recently (entry stagger); the deferred
            # ready candidate is waiting, not a silent gap.
            if _cached_green:
                _trace_mark_block(
                    sym, f"waiting: entry stagger ({_eff_stagger:.0f}s)")
            continue  # this slot too soon — try next coin in case it's been longer

        # ── Falling knife filter (§3.4a — volatility-scaled) ──────────────────
        # Block buys when price has dropped more than
        # entries.falling_knife_atr_mult × atr_pct (5m ATR%, Phase 2 ladder)
        # over the last ~3 minutes of samples — a fixed 0.4% was too tight for
        # volatile coins and too loose for quiet ones. When ATR is unavailable
        # fall back to the old fixed 0.4% threshold.
        try:
            import data_collector as _dc_fk
            recent_closes = list(_dc_fk.price_samples.get(sym, []))
            if len(recent_closes) >= 180:
                price_now     = recent_closes[-1]
                price_3min    = recent_closes[-180]
                pct_3min      = (price_now - price_3min) / price_3min * 100 if price_3min > 0 else 0
                _fk_atr = cached.get("atr_pct")
                if _fk_atr is None:
                    _fk_atr, _ = _atr_pct_5m_at_entry(sym, price_now)
                if _fk_atr and _fk_atr > 0:
                    _knife_thr = _entries_cfg()["falling_knife_atr_mult"] * float(_fk_atr)
                    _knife_src = f"atr_mult({_fk_atr:.3f}%×{_entries_cfg()['falling_knife_atr_mult']})"
                else:
                    _knife_thr = 0.4   # ATR unavailable → legacy fixed threshold
                    _knife_src = "fixed_0.4"
                if pct_3min < -_knife_thr:
                    _record_rejection(sym, score, "falling_knife",
                                      f"{pct_3min:.2f}% in 3min (thr={_knife_thr:.2f}% {_knife_src})")
                    database.log_activity(
                        f"[SKIP] {sym}: falling knife — down {pct_3min:.2f}% in 3min "
                        f"(threshold {_knife_thr:.2f}%, {_knife_src}) | "
                        f"{sig_str} | SKIP(downward momentum)", "info"
                    )
                    continue
        except Exception:
            pass

        # ── Trend health gate ──────────────────────────────────────────────────
        # Block buys when price is below MA20 AND RSI is not deeply oversold (<35).
        # Also block when volume trend is decreasing (no buying pressure).
        try:
            _last_c = _latest_candles.get(sym)   # in-memory (batched once per scan)
            if _last_c:
                _ma_pos  = _last_c.get("ma_position") or _derive_ma_pos(price, _last_c.get("ma20"))
                _vol_tr  = _last_c.get("volume_trend")
                if _ma_pos == "below" and rsi_v > 35:
                    _record_rejection(sym, score, "trend_health", f"below MA20 RSI={rsi_v:.0f}")
                    database.log_activity(
                        f"[SKIP] {sym}: below MA20 with RSI {rsi_v:.0f} (not oversold) | "
                        f"{sig_str} | SKIP(downtrend)", "info"
                    )
                    continue
                if _vol_tr == "decreasing":
                    _record_rejection(sym, score, "volume_decreasing", f"vol_trend={_vol_tr}")
                    database.log_activity(
                        f"[SKIP] {sym}: volume decreasing — no buying pressure | "
                        f"{sig_str} | SKIP(weak volume)", "info"
                    )
                    continue
        except Exception:
            pass

        # ── Shadow evaluation — compare old vs new signal engine (Phase 1) ────────
        # Runs AFTER all veto/gate checks so it sees the same yes/no that the old
        # engine sees at the moment a buy would fire.  Logs any disagreement but
        # does NOT alter the buy decision — the old code path remains authoritative.
        if SIGNAL_REGISTRY_AVAILABLE and bool(strategy.get("shadow_evaluate_signals", True)):
            try:
                _shadow_data = dict(sigs)
                _shadow_data["rsi_value"] = rsi_v
                _new_dec = _sr_evaluate_buy_decision(sym, _shadow_data, strategy)
                _old_allowed = True  # reached this point → old engine approved
                if _old_allowed != _new_dec["allowed"]:
                    # B Step 5: deduped per (symbol, reason) — see _log_shadow_dedup
                    _log_shadow_dedup(
                        sym, str(_new_dec["reason"]),
                        f"{sym}: old=True new={_new_dec['allowed']} reason={_new_dec['reason']}",
                    )
            except Exception as _shadow_exc:
                _log_shadow_dedup(
                    sym, f"shadow_eval_failed:{type(_shadow_exc).__name__}",
                    f"Shadow eval failed for {sym}: {_shadow_exc}",
                )

        # §3.6 — advisory only: gates/blockers are unaffected; the watchlist
        # owns symbol choice (full auto-switching would change the traded
        # universe — out of scope, documented on _maybe_log_promo_pair).
        _maybe_log_promo_pair(sym)

        _budget_info = _resolve_entry_budget(sym, usdt_balance)
        budget = _budget_info["resolved"]
        if budget <= 0:
            _record_rejection(sym, score, "no_capital", f"usdt={usdt_balance:.2f}")
            database.log_activity(f"{sym}: buy skipped — budget=0 (mode={mode}, usdt={usdt_balance:.2f})", "warn")
            continue

        # ── M1.1 — sizing floor AFTER all multipliers, with an honest message ──
        # tradeable_min = max(exchange minNotional, sizing.min_position_usdt, 10).
        # A shortfall the ENGINE created by multiplying (neutral "size" mode)
        # prints the full arithmetic chain and NEVER tells the operator to change
        # a setting; only a RAW-configured shortfall (no multiplier applied) may
        # point at Settings. K2.3 candidacy-cooldown + 15-min dedupe are kept so
        # the skip is not re-spammed every heartbeat.
        _tmin = _tradeable_min(sym)
        if budget < _tmin:
            _candidacy_cooldown[sym] = time.time() + _MAKER_ABANDON_COOLDOWN_SEC
            if _budget_info["mult"] < 1.0:
                _chain = (f"${_budget_info['base']:.2f} × {_budget_info['mult']:g} "
                          f"({_budget_info['mult_label']}) = ${budget:.2f}")
                _record_rejection(sym, score, "min_notional",
                                  f"{_chain} < ${_tmin:.2f} min notional")
                _log_skip_dedup(
                    sym, "budget_below_min_notional",
                    f"[SKIP] {sym}: {_chain} < ${_tmin:.2f} min notional — skipping "
                    f"(engine-scaled ticket below tradeable minimum; candidacy "
                    f"cooldown {_MAKER_ABANDON_COOLDOWN_SEC/60:.0f} min).", "warn")
            else:
                _record_rejection(sym, score, "min_notional",
                                  f"configured=${budget:.2f} < ${_tmin:.2f} min notional")
                _log_skip_dedup(
                    sym, "budget_below_min_notional",
                    f"[SKIP] {sym}: configured trade size ${budget:.2f} < ${_tmin:.2f} "
                    f"min notional — increase trade size in Settings (candidacy "
                    f"cooldown {_MAKER_ABANDON_COOLDOWN_SEC/60:.0f} min).", "warn")
            continue
        # L1.2 — funnel stage: resolved budget cleared the tradeable minimum.
        _funnel_incr("budget_pass")

        price = prices.get(sym) or cached["price"]
        if not price:
            _record_rejection(sym, score, "no_price")
            database.log_activity(f"{sym}: buy skipped — no price available", "warn")
            continue

        # G1b — lot-step rounding guard. Rounding DOWN to the lot step is normal
        # and NOT a failure: an $10.86 fill against an $11.00 budget should
        # TRADE. The ONLY hard floors are (a) qty rounds to 0, (b) the rounded
        # notional falls below the exchange minNotional, or (c) the waste exceeds
        # entries.max_lot_waste_pct (chronically-oversized ticket for this coin).
        # waste_pct = (budget − rounded_notional)/budget × 100. This relaxes the
        # SKIP decision only; the qty math below is unchanged (still floored to
        # the lot step, never buying more than affordable).
        _ideal_qty_pre = budget / price
        _actual_qty_pre = _floor_qty(_ideal_qty_pre, sym)
        if _actual_qty_pre <= 0:
            _record_rejection(sym, score, "lot_step_loss", f"budget=${budget:.2f} price=${price:.4f} qty=0")
            database.log_activity(
                f"[SKIP] {sym}: lot-step too large for ${budget:.2f} at ${price:.4f} — "
                f"would receive 0 qty. Increase budget or remove this coin.", "warn"
            )
            continue
        _rounded_notional = _actual_qty_pre * price
        _waste_pct = (budget - _rounded_notional) / budget * 100 if budget > 0 else 0.0
        # Exchange minNotional — the true hard floor (read-only exchange_info).
        _min_notional = 0.0
        try:
            from exchange_info import get_symbol_filters as _gsf_wr
            _min_notional = float((_gsf_wr(sym) or {}).get("min_notional", 0.0) or 0.0)
        except Exception:
            _min_notional = 0.0
        try:
            _max_waste_pct = float(_entries_cfg().get("max_lot_waste_pct", 5.0))
        except Exception:
            _max_waste_pct = 5.0
        if _min_notional > 0 and _rounded_notional < _min_notional:
            _record_rejection(sym, score, "min_notional",
                              f"rounded=${_rounded_notional:.2f} < minNotional=${_min_notional:.2f}")
            _log_skip_dedup(
                sym, "lot_below_min_notional",
                f"[SKIP] {sym}: rounded notional ${_rounded_notional:.2f} below "
                f"exchange minNotional ${_min_notional:.2f} (budget=${budget:.2f}, "
                f"price=${price:.4f}) — ticket too small for this coin.", "warn")
            _lot_waste_flags[sym] = {"waste_pct": round(_waste_pct, 3), "ts": time.time()}
            continue
        if _waste_pct > _max_waste_pct:
            _record_rejection(sym, score, "lot_step_loss",
                              f"{_waste_pct:.2f}% waste > {_max_waste_pct:.2f}% "
                              f"budget=${budget:.2f} price=${price:.4f}")
            _log_skip_dedup(
                sym, "lot_waste_over_max",
                f"[SKIP] {sym}: lot-step rounding wastes {_waste_pct:.2f}% of the "
                f"${budget:.2f} ticket (> {_max_waste_pct:.2f}% max; "
                f"rounded=${_rounded_notional:.2f}, step={_lot_step_cache.get(sym, '?')}) "
                f"— ticket too small for this coin.", "warn")
            _lot_waste_flags[sym] = {"waste_pct": round(_waste_pct, 3), "ts": time.time()}
            continue
        # Within tolerance — proceed and clear any stale oversized-ticket flag.
        _lot_waste_flags.pop(sym, None)

        # ── L2.2 — liquidity floor (cheap check first) ────────────────────────
        # Skip symbols whose 24h quote volume (USDT) is below the configured
        # floor: thin books slip badly on both entry and exit. 24h quote volume
        # is derived from the durable kline store (no REST weight). Unavailable
        # → fail-open (never block a real candidate on a missing stat), logged
        # via the deduper. Thinness is a stable property, so a failure routes to
        # a 30-min cooldown rather than the 60s candidacy cooldown. Config read
        # at time of use.
        _qv_floor = float(_entries_cfg().get("min_quote_volume_24h_usd", 0.0))
        if _qv_floor > 0:
            _qv = _quote_volume_24h_usd(sym)
            if _qv is None:
                _log_skip_dedup(
                    sym, "liquidity_stat_missing",
                    f"{sym}: 24h quote volume unavailable (no kline store rows) "
                    f"— liquidity floor not enforced (fail-open)", "info")
            elif _qv < _qv_floor:
                # O5.2 — thin-liquidity cooldown (config-driven, read at time of
                # use; replaces the flat 30-min _LIQUIDITY_COOLDOWN_SEC bench).
                _thin_cd = _cooldown_secs_for("thin")
                _candidacy_cooldown[sym] = time.time() + _thin_cd
                _record_rejection(
                    sym, score, "thin_liquidity",
                    f"24h quote vol ${_qv/1e6:.1f}M < ${_qv_floor/1e6:.0f}M floor")
                _log_skip_dedup(
                    sym, "thin_liquidity",
                    f"[SKIP] {sym}: 24h quote vol ${_qv/1e6:.1f}M < "
                    f"${_qv_floor/1e6:.0f}M floor — skipping (thin). Cooldown "
                    f"{_thin_cd/60:.0f} min.", "warn")
                continue

        # ── L2.1 — friction gate ──────────────────────────────────────────────
        # friction = half_spread% + recent avg exit slippage%. When friction eats
        # more than entries.max_friction_of_stop percent of the planned 1R stop
        # distance, the entry is structurally poor (round-trip cost swamps the
        # risk budget) — skip and route to the candidacy cooldown. Missing
        # slippage history → 0 (never block a symbol's first trade); missing
        # spread or stop distance → fail-open. Config read at time of use.
        _fric_cap_pct = float(_entries_cfg().get("max_friction_of_stop", 0.0))
        if _fric_cap_pct > 0:
            _full_sp, _half_sp = _spread_pcts(sym)
            _stop_pct = _planned_sl_distance_pct(sym, price)
            if _half_sp is not None and _stop_pct and _stop_pct > 0:
                _slip_bps = _avg_slippage_bps(sym)   # rolling |exit slippage|
                _slip_pct = (_slip_bps / 100.0) if _slip_bps is not None else 0.0
                _friction = _half_sp + _slip_pct
                _stop_budget = _fric_cap_pct / 100.0 * _stop_pct
                if _friction > _stop_budget:
                    # O5.2 — wide-spread / high-friction cooldown (config-driven,
                    # read at time of use; friction is spread-dominated).
                    _candidacy_cooldown[sym] = (
                        time.time() + _cooldown_secs_for("high_friction"))
                    _record_rejection(
                        sym, score, "high_friction",
                        f"friction {_friction:.3f}% > {_fric_cap_pct:g}% of "
                        f"{_stop_pct:.3f}% stop ({_stop_budget:.3f}%)")
                    _log_skip_dedup(
                        sym, "high_friction",
                        f"[SKIP] {sym}: friction {_friction:.2f}% > "
                        f"{_fric_cap_pct:g}% of {_stop_pct:.2g}% stop "
                        f"({_stop_budget:.3f}%) — skipping "
                        f"(structurally poor entry).", "warn")
                    continue

        _client().update_price(sym, price)

        buy_cfg = {**approved[sym], "symbol": sym, "budget_usdt": budget}
        allowed, reason = can_execute_buy(buy_cfg, _client())
        if not allowed:
            database.log_activity(f"{sym}: buy skipped — {reason}", "info")
            # Q1 — surface can_execute_buy's reason (min-hold, per-symbol cap,
            # cooldown, …) in the decision trace so a ready coin blocked here is
            # never a silent gap.
            _trace_mark_block(sym, f"blocked: {reason}")
            continue

        if sym in _bad_symbols:
            database.log_activity(f"{sym}: buy skipped — market closed/delisted (blacklisted this session)", "warn")
            _trace_mark_block(sym, "blocked: market closed/delisted (blacklisted)")
            continue

        # Binance minimum notional ($10 for most spot pairs) is now enforced
        # up-front by the M1.1 sizing floor (resolved_budget < tradeable_min),
        # which prints the honest computation chain instead of the old
        # "increase trade size in Settings" message that blamed a config value
        # the engine itself had shrunk by multiplying. The post-lot-rounding
        # minNotional guard above still catches lot-step shortfalls.

        # ── Atomic claim — BEFORE the slow REST gates below ─────────────────────
        # The fresh re-check + reversal confirmation take multiple seconds. If the
        # claim happened after them (as it used to), a concurrent scan could buy
        # this symbol inside that window, release the claim, and this thread would
        # then claim unopposed and buy it AGAIN (confirmed duplicate DOT/IMX rows).
        # Claim first; release on every failure path.
        with _buying_lock:
            _now_b = time.time()
            _stale_b = [s for s, ts in _buying_ts.items() if (_now_b - ts) > _BUYING_TIMEOUT_SEC]
            for _s in _stale_b:
                _buying.discard(_s)
                _buying_ts.pop(_s, None)
            if sym in _buying:
                database.log_activity(f"{sym}: buy skipped — concurrent buy already in flight", "info")
                _record_rejection(sym, score, "in_progress_buy")
                # Q1 — queued behind a single-flight claim on THIS symbol.
                _trace_mark_block(sym, "waiting: buy in-flight")
                continue
            _buying.add(sym)
            _buying_ts[sym] = _now_b
            _other_buying = len(_buying) - 1  # in-flight claims on OTHER symbols

        def _release_buy_claim(_s=sym):
            with _buying_lock:
                _buying.discard(_s)
                _buying_ts.pop(_s, None)

        # Re-verify under the claim: another thread may have bought this symbol
        # or filled the last slot while this scan was mid-loop. Counting other
        # in-flight claims as reserved slots keeps concurrent scans from
        # collectively exceeding max_positions.
        with _positions_lock:
            _n_pos_now = len(_positions)
            _held_now = any(p["symbol"] == sym for p in _positions)
            _at_capacity_now = (_n_pos_now + _other_buying) >= max_pos
        if _held_now:
            _release_buy_claim()
            _record_rejection(sym, score, "already_held")
            continue
        if _at_capacity_now:
            _release_buy_claim()
            # Q1 — a slot filled (or was reserved by another in-flight claim)
            # while this scan looped; the queued ready candidates aren't a gap.
            _mark_rest_no_slot(_idx, _n_pos_now + _other_buying, max_pos)
            break

        # ── Fresh signal re-check before committing ────────────────────────────
        # Cache can be 30-60s stale. Re-verify at execution time using fresh
        # candles (REST multi-base → WS buffer → DB, 50 klines for MACD parity
        # with the scan). When the signal engine approved this buy, re-verify
        # with the SAME engine — not the legacy min_signals score, which tests
        # the opposite (momentum-up) hypothesis of an engine dip entry. When
        # no data source is available, proceed on the cached decision (the
        # scanner refreshed it ≤60s ago) — data unavailability alone must
        # never fail-closed and block every buy.
        try:
            _fresh_candles = _fetch_fresh_1m_candles(sym)
            if not _fresh_candles:
                global _last_fresh_nodata_log
                _now_fr = time.time()
                if _now_fr - _last_fresh_nodata_log >= 60.0:
                    _last_fresh_nodata_log = _now_fr
                    database.log_activity(
                        f"{sym}: pre-buy fresh re-check has no data source (REST/WS/DB) — "
                        f"proceeding on cached signals "
                        f"(age={round(time.time() - cached.get('ts', 0), 1)}s)", "warn"
                    )
            else:
                _fresh_closes = [c["close"] for c in _fresh_candles]
                _fresh_sigs   = evaluate_signals(_fresh_candles)
                if signal_engine_active:
                    # Re-run the engine that approved the buy on the fresh data.
                    _fresh_rsi_list = indicators.calc_rsi(_fresh_closes, 14)
                    _fresh_rsi = _fresh_rsi_list[-2] if _fresh_rsi_list[-2] is not None else 0.0
                    try:
                        _fresh_stoch = indicators.calc_stoch_rsi(_fresh_closes)
                    except Exception:
                        _fresh_stoch = cached.get("stoch_rsi_val")
                    _fresh_data = {
                        **_fresh_sigs,
                        "rsi_value":       _fresh_rsi,
                        "current_price":   _fresh_closes[-1],
                        "low_24h":         cached.get("low_24h"),
                        "klines_1m":       _fresh_candles[-15:],
                        "stoch_rsi_value": _fresh_stoch,
                        # Phase 3 fields — carried from the 5m-close cache
                        # (their cadence IS 5m; a 1m fresh re-check cannot
                        # produce fresher values for them)
                        "klines_5m":       cached.get("klines_5m", []),
                        "ema50_15m_slope": cached.get("ema50_15m_slope"),
                        "bb_position_5m":  cached.get("bb_position_5m"),
                        "atr_pct":         cached.get("atr_pct"),
                        "btc_regime":      get_btc_regime(),
                    }
                    _fresh_dec = _sr_evaluate_buy_decision(sym, _fresh_data, strategy)
                    if not _fresh_dec["allowed"]:
                        cache_age = round(time.time() - cached.get("ts", 0), 1)
                        # F6: dedupe the [SKIP] line per (symbol, reason) 15 min.
                        _log_skip_dedup(
                            sym, f"fresh_engine:{_fresh_dec['reason']}",
                            f"[SKIP] {sym}: fresh engine re-check FAILED — "
                            f"{_fresh_dec['reason']} (cache age={cache_age}s)", "warn")
                        _record_rejection(sym, _fresh_dec.get("score", score), "stale_signals",
                                          f"fresh_engine={_fresh_dec['reason']} age={cache_age}s")
                        # F6: write fresh score back + candidacy cooldown so the
                        # pre-check stops re-selecting this coin every ~7s.
                        _note_candidacy_fail(sym, _fresh_dec.get("score"), _fresh_dec["reason"])
                        _release_buy_claim()
                        continue
                else:
                    _fresh_score = sum(_fresh_sigs.values())
                    if _fresh_score < min_sigs:
                        cache_age = round(time.time() - cached.get("ts", 0), 1)
                        _log_skip_dedup(
                            sym, f"fresh_score_below_{min_sigs}",
                            f"[SKIP] {sym}: fresh re-check FAILED — score {_fresh_score}/6 < {min_sigs} "
                            f"(cache had {score}/6, age={cache_age}s)", "warn")
                        _record_rejection(sym, score, "stale_signals",
                                          f"fresh={_fresh_score} cache={score} age={cache_age}s")
                        _note_candidacy_fail(sym, _fresh_score, "fresh_score_below_min")
                        _release_buy_claim()
                        continue
                _live_price = _fresh_closes[-1]
                if price > 0 and abs(_live_price - price) / price > 0.005:
                    database.log_activity(
                        f"[SKIP] {sym}: price moved {(_live_price - price)/price*100:.2f}% "
                        f"since cache (${price:.4f} → ${_live_price:.4f}) — skipping", "warn"
                    )
                    # Q1 — price-drift skip is a state, not a silent gap.
                    _record_rejection(sym, score, "price_moved",
                                      f"{(_live_price - price)/price*100:.2f}% since cache")
                    _release_buy_claim()
                    continue
                price = _live_price
                _client().update_price(sym, price)
        except Exception as _fresh_e:
            # Unexpected error (not data unavailability — that path never
            # raises). Proceed on the cached decision rather than reinstating
            # the old fail-closed skip that killed every buy.
            database.log_activity(
                f"{sym}: fresh re-check errored ({_fresh_e}) — proceeding on cached signals", "warn"
            )

        # ── Reversal confirmation ──────────────────────────────────────────────
        if bool(strategy.get("reversal_confirmation_enabled", True)):
            _rev_ok, _rev_reason = is_reversal_confirmed(sym)
            if not _rev_ok:
                _record_rejection(sym, score, "no_reversal_confirmed", _rev_reason)
                _release_buy_claim()
                continue

        # ── O5.1/O5.3 — confirm-then-fire latency gate ────────────────────────
        # O5.5 DISCIPLINE BOUNDARY: entries.confirm_seconds ONLY tunes how long a
        # candidate must HOLD its fresh-re-check-confirmed buy-ready state before
        # the entry fires — it is NOT a switch to buy on a single unconfirmed
        # cached-green tick. The fresh re-check above (fresh candles + engine/
        # score re-evaluation) has ALREADY run and PASSED on THIS pass and gates
        # every fire, so a coin that is cached-green but fails the fresh score
        # (the XRP "cached green, fresh score 1/3" bug) never reaches this gate.
        # This gate can only DEFER or RELEASE a fire, never bypass a fresh
        # confirmation. confirm_seconds=0 → fire on the first confirmed tick;
        # confirm_seconds=300 → require a full candle of held confirmation.
        _confirm_sec = max(0.0, float(_entries_cfg().get("confirm_seconds", 10.0)))
        _ready_since = _mark_buy_ready(sym)   # stamp on the FIRST confirmed pass
        _held_sec = time.time() - _ready_since
        # Q1 — instant-fire-when-slots-free: reaching this gate means we already
        # passed the _at_capacity_now check, so at least one slot is free right
        # now. When the flag is on, the HIGHEST-SCORING ready candidate skips the
        # confirm hold and fires immediately (all safety gates + the fresh
        # re-check above have ALREADY passed on this pass); the confirm_seconds
        # hold is reserved for marginal / subsequent candidates so a burst of
        # ready coins never all strand behind the timer while slots sit open.
        _instant_ok = bool(_entries_cfg().get("instant_fire_when_slots_free", True))
        _instant_fire = _instant_ok and (_top_ready_symbol() == sym)
        if _held_sec < _confirm_sec and not _instant_fire:
            # Not held long enough yet — defer. The fast re-check loop (~2.5 s)
            # will re-run the fresh re-check and fire once the window elapses;
            # the ready-streak is held (block-marked with elapsed/target so it
            # reads as an explicit 'confirming' state, never a silent gap) and
            # the claim is released so the next pass can re-claim.
            _trace_mark_block(sym, f"confirming ({_held_sec:.0f}s/{_confirm_sec:.0f}s)")
            _log_skip_dedup(
                sym, "confirming",
                f"{sym}: buy-ready confirmed — holding {_held_sec:.1f}s/"
                f"{_confirm_sec:.0f}s before firing (confirm-then-fire).", "info")
            _release_buy_claim()
            continue
        if _instant_fire and _held_sec < _confirm_sec:
            _log_skip_dedup(
                sym, "instant_fire",
                f"{sym}: instant-fire — free slot + top ready candidate "
                f"(score={cached.get('score', 0)}), skipping confirm hold "
                f"({_held_sec:.1f}s/{_confirm_sec:.0f}s).", "info")

        # J1 — the candidate passed every gate + the fresh re-check; order
        # placement is about to start. Record the fresh engine-ready verdict and
        # the entry attempt, and emit ONE deduped INFO line so the operator sees
        # the engine actually tried (the "green but no purchase, no logs" gap).
        _trace_mark_engine_ready(sym)
        # L1.2 — funnel stage: passed the fresh re-evaluation (engine-ready).
        _funnel_incr("fresh_recheck_pass")
        _trace_mark_attempt(sym)
        _log_skip_dedup(
            sym, "entry_attempt",
            f"{sym}: entry attempt — all gates passed, placing order "
            f"(budget=${budget:.2f} @ ~${price:.4f})", "info")

        # §3.5 — maker-first live entries. Paper mode (and live-on-paper-
        # fallback, which _market_buy blocks anyway) keeps the existing
        # simulated market-buy path unchanged — paper maker simulation is
        # out of scope.
        _entry_is_maker = False
        _use_maker_first = (
            mode == "live"
            and _entries_cfg()["maker_first"]
            and not connection.is_using_paper_fallback()
        )
        try:
            if _use_maker_first:
                _mfr = _execute_maker_first_buy(
                    sym, budget,
                    # Quick re-check of the cached engine decision: the chase
                    # can run (chase_seconds × posts) seconds — only take the
                    # taker fallback when the approval that got us here still
                    # stands and the cache isn't ancient.
                    signal_still_holds=lambda: (
                        bool(_buy_decision.get("allowed", True))
                        and (time.time() - cached.get("ts", 0)) < 300.0
                    ),
                )
                if _mfr is None:
                    _record_rejection(sym, score, "maker_chase_abandoned",
                                      f"chase={_entries_cfg()['chase_seconds']}s "
                                      f"reposts={_entries_cfg()['max_reposts']}")
                    # G1a: count the abandon; after maker_abandon_max in a row
                    # this arms a 5-min candidacy cooldown + one deduped log
                    # instead of re-selecting and warning every scan.
                    _note_maker_abandon(sym)
                    _release_buy_claim()
                    continue
                # Successful maker entry — clear the abandon streak.
                _reset_maker_abandon(sym)
                fill_price      = _mfr["fill_price"]
                qty             = _mfr["qty"]
                buy_fee_usdt    = _mfr["buy_fee_usdt"]
                _entry_is_maker = bool(_mfr["entry_is_maker"])
                # Partial fills spend less than the requested budget — account
                # for what was actually deployed, not what was asked for.
                if _mfr.get("spent_quote", 0) > 0:
                    budget = float(_mfr["spent_quote"])
            else:
                _log_order_intent("BUY", sym, budget / max(price, 1e-12), price)
                # Live mode: geo-block-safe direct transport; raises in paper fallback.
                result = _market_buy(sym, budget)
                _log_order_result("BUY", sym, budget / max(price, 1e-12), price, result)
                # L1.2 — funnel stage: taker/paper market order placed. (The
                # maker-first path increments order_posted internally.)
                _funnel_incr("order_posted")
        except Exception as e:
            _release_buy_claim()
            err_str = str(e)
            print(f"[RealtimeBuy] BUY failed {sym}: {e}")
            database.log_activity(f"{sym}: BUY failed — {e}", "error")
            if "-1013" in err_str or "Market is closed" in err_str:
                _bad_symbols.add(sym)
                database.log_activity(f"{sym}: blacklisted — market closed/delisted on Binance", "warn")
            continue

        if not _use_maker_first:
            buy_fills  = result.get("fills", [])
            _exec_qty  = float(result.get("executedQty", 0) or 0)
            _cum_quote = float(result.get("cummulativeQuoteQty", 0) or 0)
            # Entry price = VWAP across ALL fills (cummulativeQuoteQty / executedQty).
            # fills[0] is only the best-priced tranche — using it understates the true
            # average cost on multi-fill market buys and corrupts breakeven math.
            if _exec_qty > 0 and _cum_quote > 0:
                fill_price = _cum_quote / _exec_qty
            elif buy_fills:
                fill_price = float(buy_fills[0].get("price", price))
            else:
                fill_price = price
            # Subtract base-asset commission from executedQty: without the BNB fee
            # discount Binance takes the fee in the bought coin, so the wallet is
            # credited executedQty − Σcommission. Recording the gross qty causes
            # -2010 "insufficient balance" on the eventual sell.
            _base_asset = sym[:-4] if sym.endswith("USDT") else sym
            _base_commission = sum(
                float(f.get("commission") or 0) for f in buy_fills
                if f.get("commissionAsset") == _base_asset
            )
            qty = max(0.0, _exec_qty - _base_commission)

        if qty <= 0:
            _release_buy_claim()
            continue
        # L1.2 — funnel stage: the entry is confirmed filled (qty > 0), for both
        # the maker-first and taker/paper paths.
        _funnel_incr("filled")

        if not _use_maker_first:
            # Compute actual buy fee in USDT across all fills.
            # In live BNB-fee mode, commission is in BNB — convert using live price.
            # Stored on the position so _do_execute_sell can use the real value.
            if mode == "live":
                buy_fee_usdt, _ = _fills_fee_usdt(buy_fills, budget * _fee_rate_for(sym))
            else:
                buy_fee_usdt = budget * _fee_rate_for(sym)

        # Gather entry indicators from latest DB candle
        entry_rsi = entry_ma = entry_bb = entry_vol = None
        try:
            candles = database.get_candles(sym, config.CANDLE_TIMEFRAME, limit=1)
            if candles:
                last      = candles[-1]
                entry_rsi = last.get("rsi14")
                entry_ma  = last.get("ma_position") or _derive_ma_pos(fill_price, last.get("ma20"))
                entry_bb  = last.get("bb_position") or _derive_bb_pos(fill_price, last)
                entry_vol = last.get("volume_trend")
        except Exception:
            pass

        _bep_mult_buy = _get_breakeven_mult(fill_price, sym)
        if _take_profit_enabled:
            _eff_tp_mult = max(_bep_mult_buy, _user_tp_mult)
        else:
            _eff_tp_mult = _bep_mult_buy
        exit_target = round(fill_price * _eff_tp_mult, 8)
        _pos_peaks.pop(sym, None)
        _ratchet_state.pop(sym, None)   # fresh position — no stale ratchet peak
        _buy_slippage_pct = ((fill_price - price) / price * 100) if price > 0 else None
        pos_record = {
            "symbol":             sym,
            "entry_price":        fill_price,
            # P3: be_moved is an explicit bool from entry — never None.
            "be_moved":           False,
            # B Step 4 — origin tag: this buy was decided by the automated
            # scanner (vs. a user-initiated/manual buy). Lives in the in-memory
            # position dict + activity log only: the positions and trades
            # tables are fixed-column INSERTs (database.save_position /
            # database.log_trade), so persisting it would need a schema
            # migration. save_position ignores unknown keys — safe to carry.
            "origin":             "auto",
            # §3.5 — True when the entry filled as a LIMIT_MAKER (maker fee);
            # False for taker market buys and paper fills. Snapshots / fee
            # analytics can use it later (FeeModel.round_trip entry leg).
            "entry_is_maker":     _entry_is_maker,
            "exit_target":        exit_target,
            "breakeven_mult_at_buy": round(_bep_mult_buy, 8),
            "quantity":           qty,
            "budget_usdt":        budget,
            "buy_fee_usdt":       buy_fee_usdt,
            "opened_at_ts":       time.time(),  # for minimum-hold-time guard
            "timestamp":          ts_now,
            "mode":               mode,
            "entry_rsi":          entry_rsi,
            "entry_ma_position":  entry_ma,
            "entry_bb_position":  entry_bb,
            "entry_volume_trend": entry_vol,
            "intended_buy_price": price,
            "buy_slippage_pct":   _buy_slippage_pct,
            "buy_signals_snapshot": {
                "score":         score,
                "trend":         sigs.get("trend"),
                "rsi":           sigs.get("rsi"),
                "rsi_value":     rsi_v,
                "macd":          sigs.get("macd"),
                "volume":        sigs.get("volume"),
                "obv":           sigs.get("obv"),
                "atr":           sigs.get("atr"),
                "bb_ok":         bb_ok,
                "5m_ok":         five_ok,
                "cache_age_sec": round(time.time() - cached.get("ts", 0), 1),
            },
            "signal_snapshot": _json.dumps({
                "fired_signals":   _buy_decision.get("fired_signals", []),
                "score":           _buy_decision.get("score", 0),
                "mandatory_pass":  all(fired for _, fired in _buy_decision.get("mandatory_results", [])),
                "engine_enabled":  bool(strategy.get("signal_engine", {}).get("enabled", False)),
            }),
        }
        # ── WolfBot v0.5 Part S-2 — capture the WolfScore snapshot at entry ────
        # Additive in-memory fields (save_position ignores unknown keys). The
        # SUBMETRICS + regime tilt are persisted as the training-label payload when
        # this position closes (_save_ev_training_sample_safe) so train_wolfscore
        # can consume the row. Does NOT affect sizing/exit/geometry in any way.
        # Guarded so a scoring failure can never break the buy.
        if ev_model is not None:
            try:
                _ws_entry = _wolf_scores.get(sym) or _wolf_score_cached(
                    sym, cached, _wolf_cohort, _wolf_tilt)
                if _ws_entry is not None:
                    pos_record["ev_submetrics"]  = _ws_entry.get("submetrics")
                    pos_record["ev_regime_tilt"] = _ws_entry.get("regime_tilt")
                    pos_record["ev_score"]       = _ws_entry.get("pct")
            except Exception:
                pass
        # Phase 2 §2.1/§2.2 — ATR stop / TP / hard-SL geometry, stored on the
        # position dict (in-memory; the positions table is fixed-column and
        # save_position ignores unknown keys — load_positions_from_db
        # recomputes geometry for restored positions).
        _apply_entry_exit_geometry(pos_record)
        if pos_record.get("atr_unavailable"):
            database.log_activity(
                f"{sym}: ATR unavailable at entry — stop set to sl_min_pct "
                f"({pos_record.get('sl_distance_pct')}%) as a conservative tight stop",
                "warn"
            )
        # Update stagger tracking
        _last_buy_ts = time.time()
        _buys_this_scan += 1
        _record_corr_entry()   # §4.5 — executed buy enters the 5-min window
        pos_id = database.save_position(pos_record)
        pos_record["id"] = pos_id

        with _positions_lock:
            _positions.append(pos_record)
        _rebuild_pos_index()
        # O5.1 — entry filled: clear the confirm-then-fire timer so a future
        # re-entry starts a fresh confirmation window.
        _clear_buy_ready(sym)

        # ── Phase 2 §2.4/§2.5: exchange-side exit order (LIVE only) ───────────
        # Placed AFTER the position is fully created and geometry applied.
        # Any failure/rejection falls back to full local monitoring — it can
        # never block the position. Paper mode / paper fallback: no-op.
        try:
            _place_managed_exit(pos_record)
            start_managed_exit_poller()
        except Exception as _mex:
            log_diag_issue("managed_exit", "warn",
                           f"{sym}: managed-exit placement hook failed: {_mex}")

        # ── Phase 1 §1.3: entry snapshot for the EXECUTED auto buy ────────────
        # Wrapped so a snapshot failure can never break the buy path.
        try:
            _snap_btc = get_btc_state()
            _snap_regime = _snap_btc.get("regime") if _snap_btc else None
        except Exception:
            _snap_regime = None
        _save_entry_snapshot_safe(
            sym, origin=pos_record.get("origin", "auto"), executed=True,
            price=fill_price,
            raw=_snapshot_raw_from_cache(cached, fill_price),
            gates={
                "trading_active": bool(strategy.get("trading_active", True)),
                "regime":         _snap_regime,
                "bb_ok":          bb_ok,
                "5m_ok":          five_ok,
                "score":          score,
                "min_signals":    min_sigs,
                "signal_engine":  signal_engine_active,
                "engine_reason":  _buy_decision.get("reason"),
            },
        )

        try:
            import supabase_sync
            # Parallel sync to Supabase — both calls run concurrently, max 4 s total.
            supabase_sync.sync_buy_result_sync(
                pos_record, usdt_balance - budget - buy_fee_usdt
            )
        except Exception as _be:
            try:
                database.log_activity(f"Supabase sync error after buying {sym}: {_be}", "error")
            except Exception:
                pass

        usdt_balance -= budget + buy_fee_usdt

        mode_tag  = "LIVE" if mode == "live" else "PAPER"
        cache_age = round(time.time() - cached.get("ts", 0), 1)
        _btc_now  = get_btc_state()
        _regime_tag = _btc_now.get("regime", "?") if _btc_now else "?"
        _rsi_thr_tag = f"<{rsi_threshold:.0f}" if mandatory_enabled else "any"
        msg = (
            f"[{mode_tag}] BOUGHT {sym} @ ${fill_price:.4f} "
            f"| MANDATORY[EMA↑ RSI:{rsi_v:.0f}{_rsi_thr_tag}] "
            f"| SCORED[MACD{'+'  if sigs.get('macd')   else '-'} "
            f"Vol{'✓'   if sigs.get('volume') else '✗'} "
            f"OBV{'✓'   if sigs.get('obv')    else '✗'} "
            f"ATR{'✓'   if sigs.get('atr')    else '✗'}] "
            f"| score:{score}/6 regime:{_regime_tag} | EXIT TARGET=${exit_target:.4f} "
            f"| qty={qty:.6f} | cache_age:{cache_age}s | origin:auto"
        )
        print(f"[RealtimeBuy] {msg}")
        database.log_activity(msg, "info")
        with _buying_lock:
            _buying.discard(sym)
            _buying_ts.pop(sym, None)

    # ── Q1 — DECISION-GAP backstop (final safety net) ─────────────────────────
    # The per-symbol skip sites above stamp SPECIFIC machine-readable reasons,
    # but the invariant must hold unconditionally: any symbol that was buy-ready
    # (cached-green) THIS pass and got neither an entry attempt nor a fresh
    # block reason this heartbeat would otherwise reach the DECISION-GAP
    # tripwire with "no block reason". Stamp a catch-all so the gap can never be
    # truly silent. Specific reasons are always preferred — this only fires for
    # a ready candidate that slipped past every explicit skip site (e.g. a
    # falling-knife/trend-health veto whose indicator read raised and fell
    # through, or a future skip added without its own _trace_mark_block).
    _hb_now = _decision_heartbeat
    for _rs in _ready_syms_pass:
        with _decision_trace_lock:
            _ent_rs = _decision_trace.get(_rs)
            _touched = bool(_ent_rs) and _ent_rs.get("_last_touch_hb") == _hb_now
        if not _touched:
            _trace_mark_block(_rs, "waiting: no attempt this cycle")


# ── Inline tick-driven signal refresh ────────────────────────────────────────────

def _inline_refresh_from_ticks(sym: str, price: float):
    """
    Recompute signals for sym using the best available price series — no I/O.
    Preference order: time-sampled prices (quality RSI) → raw ticks (last resort).
    """
    import data_collector as _dc

    # Prefer 30-second sampled prices — spans real time, gives meaningful RSI
    closes = list(_dc.price_samples.get(sym, []))
    if len(closes) >= _dc._MIN_SAMPLES:
        closes = closes[:]   # copy
        closes[-1] = price   # inject latest price
    else:
        # Fall back to raw ticks (available in seconds but RSI less reliable)
        closes = list(_dc.price_ticks.get(sym, []))
        if len(closes) < _dc._MIN_TICKS:
            return
        closes[-1] = price

    # Don't compute signals from fake volume — preserve existing signal entry
    # and only update price + RSI. Volume signal stays from last REST scan.
    import data_collector as _dc2
    with _signal_cache_lock:
        prev_entry = _signal_cache.get(sym)
    if prev_entry:
        with _signal_cache_lock:
            if sym in _signal_cache:
                _signal_cache[sym]["price"] = closes[-1]
                _signal_cache[sym]["ts"] = time.time()
        return
    vols = [1.0] * len(closes)
    update_coin_signals(sym, closes, vols)


# ── Phase 2 §2.1-2.3 — shared exit decision (A1 parity) ──────────────────────
# ONE evaluator used by BOTH realtime_monitor and _sell_monitor_loop so the
# two monitors can never disagree on trigger geometry.

def _fresh_bullish_score(sym: str, now: float) -> bool:
    """True when the signal cache holds a FRESH (<=120s) score >= 3 for sym.
    A stale score must never hold a profitable position (ported from the
    original smart-hold gate)."""
    with _signal_cache_lock:
        _sc = _signal_cache.get(sym, {})
        score = _sc.get("score", 0)
        score_ts = _sc.get("ts", 0)
    return score >= 3 and (now - score_ts) <= 120


def _evaluate_exit_decision(pos: dict, sym: str, price: float,
                            now: float) -> Tuple[Optional[str], bool]:
    """Decide whether `pos` should be sold at `price`. Returns
    (sell_reason_or_None, target_crossed).

    Mutates shared decision state exactly like the old inline monitor code:
    _stop_loss_confirmation counters, _pos_peaks (legacy smart-hold), and the
    per-position BE-move/trail fields (be_moved, be_stop_price, peak_price,
    tp_ratchet_floor).

    Exit ladder (§2.1-2.3):
      hard-stop-loss  price <= hard_sl_price — bypasses confirmation ticks,
                      min-hold AND the profit gate (crash stop)
      stop-loss       price <= stop_price — sl_confirm_ticks + min-hold apply
      trail           post-BE-move trail/floor crossed — 1 tick, profit-side
      take-profit     price >= TP trigger while trailing is disabled
    TP trigger = max(pos['tp_price'], compute_real_breakeven_price(pos)) — the
    BEP floor is recomputed at trigger time (fees can change) so the profit
    gate (_profitable_sell_check) can never veto a trigger (A1 parity).

    Legacy path (no exits block / positions without stored geometry): the
    original global-mult logic verbatim — stop from _stop_loss_mult, target
    from _user_tp_mult, optional smart-hold trailing with the score gate."""
    cfg = _exit_cfg()
    entry = pos["entry_price"]
    opened_ts = pos.get("opened_at_ts", 0)
    # Minimum hold applies to STOP-LOSS only (prevents race-condition flips
    # right after the buy). Take-profit/trail are never delayed; hard SL
    # bypasses everything.
    in_min_hold = opened_ts > 0 and (now - opened_ts) < _min_hold_sec()

    # ── HARD SL (§2.1) — immediate, bypasses ticks + min-hold ────────────────
    hard_sl = pos.get("hard_sl_price")
    if hard_sl and price <= hard_sl:
        _stop_loss_confirmation.pop(sym, None)
        return "hard-stop-loss", False

    # Real breakeven trigger — SAME call as the profit gate (A1 parity).
    real_target = compute_real_breakeven_price(pos)
    if real_target <= 0:
        _bep_m = pos.get("breakeven_mult_at_buy") or _get_breakeven_mult(entry, sym)
        real_target = entry * _bep_m  # fallback for incomplete positions

    use_new = (not cfg["legacy_mode"]) and pos.get("tp_price")
    if not use_new:
        # ── LEGACY global-mult path (pre-Phase-2 behavior, verbatim) ─────────
        stop = entry * _stop_loss_mult
        if _take_profit_enabled:
            target = max(real_target, entry * _user_tp_mult)
        else:
            target = real_target
        if price >= target:
            if _smart_hold_enabled and target > real_target:
                _pos_peaks[sym] = max(_pos_peaks.get(sym, target), price)
                peak = _pos_peaks[sym]
                # Trail hard-floored at the exit target (never give back below it)
                trail_stop = max(peak * (1.0 - _trailing_stop_pct / 100.0), target)
                if price <= trail_stop:
                    return "smart-hold-trail", True
                if not _fresh_bullish_score(sym, now):
                    return "take-profit", True
                return None, True   # fresh bullish score defers the sell
            return "take-profit", True
        if _stop_loss_mult < 1.0 and price <= stop and not in_min_hold:
            _stop_loss_confirmation[sym] = _stop_loss_confirmation.get(sym, 0) + 1
            if _stop_loss_confirmation[sym] >= _sl_confirm_ticks():
                _stop_loss_confirmation.pop(sym, None)
                return "stop-loss", False
            return None, False
        _stop_loss_confirmation.pop(sym, None)
        return None, False

    # ── NEW geometry (§2.1-2.3, F1-decoupled) — per-position stop/tp at entry ─
    stop_price = pos.get("stop_price")
    sl_dist = pos.get("sl_distance_pct")
    # §2.2: BEP floor recomputed at trigger time — gate can never veto.
    tp_price = float(pos.get("tp_price") or 0.0)
    tp_trigger = max(tp_price, real_target)
    k_trail = float(cfg.get("k_trail") or 0.0)
    trailing_on = k_trail > 0
    crossed = price >= tp_trigger

    # ── F1 §2.3: BE-move (CAPITAL PROTECTION only) ───────────────────────────
    # At +breakeven_at_r × R the stop rises to BEP so the position can no longer
    # lose. This ONLY raises the protective stop; it NEVER arms a trailing exit
    # below tp_price. That is the F1 fix: a +1R-then-fade now scratches at BEP
    # (a real breakeven), instead of the old trail firing at ~BEP+min_profit
    # and being booked as a floor-scale "win" that never reached the RR target.
    if not pos.get("be_moved"):
        gain_pct = (price / entry - 1.0) * 100.0
        be_r = cfg.get("breakeven_at_r")
        if be_r is not None and sl_dist and gain_pct >= float(sl_dist) * float(be_r):
            pos["be_moved"] = True
            _bep_stop = max(stop_price or 0.0, real_target)
            pos["be_stop_price"] = _bep_stop
            # H1 fix 4: the BE-move must UPDATE the STORED stop, not merely set a
            # flag. Before this fix pos['stop_price'] stayed at the original ATR
            # stop (below entry) while be_moved=True, so the /api/positions
            # display — and the cancel-first sell's price reference — showed a
            # stop that no longer reflected reality. Preserve the original ATR
            # stop in orig_stop_price so the stop-loss vs breakeven-stop label
            # split below can still distinguish a BEP scratch from a true
            # stop-out, then promote stored stop_price to the BEP.
            if pos.get("orig_stop_price") is None:
                pos["orig_stop_price"] = stop_price
            pos["stop_price"] = _bep_stop
            stop_price = _bep_stop

    # Effective protective stop BELOW tp: original stop, escalated to BEP once
    # the BE-move armed. There is deliberately NO trailing exit below tp_price.
    protective_stop = stop_price
    if pos.get("be_moved"):
        protective_stop = max(float(pos.get("be_stop_price") or 0.0),
                              float(stop_price or 0.0))

    # ── F1 §2.3: trailing ARMS ONLY at/after price reaches tp_price ──────────
    # Trailing off → reaching tp is an immediate clean take-profit. Trailing on
    # → arm the trail (hard-floored at tp_price) and let a runner ride: a fade
    # back to the tp floor exits 'take-profit' (label 'tp', ≥ the RR target); a
    # fade from a higher peak exits 'trail' (still ≥ tp_price). A trailing exit
    # can never fire below tp_price.
    if crossed:
        if not trailing_on:
            _stop_loss_confirmation.pop(sym, None)
            return "take-profit", True
        pos["trail_armed"] = True

    if pos.get("trail_armed"):
        peak = pos["peak_price"] = max(float(pos.get("peak_price") or entry), price)
        # Trail gap = k_trail × ATR% (entry ATR; sl_distance fallback when ATR
        # was unavailable at entry).
        atr_ref = (pos.get("atr_pct_at_entry") or sl_dist or cfg["sl_min_pct"])
        trail_stop = peak * (1.0 - k_trail * float(atr_ref) / 100.0)
        floor = tp_trigger                       # HARD floor — never give back below TP
        eff_trail = max(trail_stop, floor)
        if price <= eff_trail:
            # Optional smart-hold score gate (default OFF): a FRESH bullish
            # score may defer an above-TP trail exit.
            if cfg.get("smart_hold_score_gate") and _fresh_bullish_score(sym, now):
                return None, True
            _stop_loss_confirmation.pop(sym, None)
            # At the tp floor → clean 'take-profit'; above it (peak ran past TP)
            # → 'trail'. Both land ≥ tp_price.
            return ("trail" if trail_stop > floor else "take-profit"), True
        return None, True

    # ── Below tp_price: ONLY the (BE-escalated) protective stop can exit ─────
    if protective_stop and price <= protective_stop and not in_min_hold:
        _stop_loss_confirmation[sym] = _stop_loss_confirmation.get(sym, 0) + 1
        if _stop_loss_confirmation[sym] >= _sl_confirm_ticks():
            _stop_loss_confirmation.pop(sym, None)
            # BE-move stop hit (faded back to BEP after arming) vs a real loss
            # at the original stop — distinct labels so R stats separate them.
            # H1 fix 4: stop_price is now promoted to the BEP once be_moved, so
            # compare against the PRESERVED original ATR stop (orig_stop_price)
            # to tell a BEP scratch (price above the original stop) from a true
            # stop-out (price at/below it).
            _ref_stop = pos.get("orig_stop_price")
            if _ref_stop is None:
                _ref_stop = stop_price
            be_hit = (pos.get("be_moved") and _ref_stop
                      and price > float(_ref_stop))
            return ("breakeven-stop" if be_hit else "stop-loss"), crossed
        return None, crossed
    _stop_loss_confirmation.pop(sym, None)

    # ── P2 profit-ratchet (exit ordering: hard → protective → RATCHET → tp) ───
    # Reached only when price is above the protective stop and below tp_price —
    # the green-but-pre-target region. Locks gains via an ATR trail / give-back
    # cap, and only ever exits in profit (min_profit_usdt floor applies; a
    # sub-floor pullback holds and defers to the protective stop above).
    if _evaluate_ratchet(pos, sym, price, entry, now, cfg):
        return "profit-ratchet", crossed
    return None, crossed


# ── Process 1: realtime monitor (called on every WebSocket tick) ──────────────

def realtime_monitor(prices: Dict[str, float]):
    """
    Synchronous — called inside data_collector's async WebSocket loop on EVERY
    trade event (~100 ms per coin). MUST NOT do any blocking I/O.

    Sell exits and buy entries are dispatched to _sell_executor (non-blocking
    O(1) submit) so the event loop is never stalled.  The _sell_monitor_loop
    daemon thread acts as a 5-second fallback in case a tick is missed.
    """
    now = time.time()

    try:
        if _thread_health:
            _thread_health.heartbeat("realtime_monitor")
    except Exception:
        pass

    # ── Real-time sell check — fires within ~100 ms of price crossing threshold ──
    # Reads the pre-built symbol→position index (no lock needed for dict reads).
    pos_index = _pos_by_symbol   # local ref; atomically replaced on each mutation
    for sym, price in prices.items():
        pos = pos_index.get(sym)
        if pos is None or price <= 0:
            continue
        # Stamp freshness for held symbols — lets the pre-sell gate trust this
        # WS tick instead of re-quoting via REST (3s latency + veto risk).
        _last_ws_price_ts[sym] = now
        # Rate-limit retries: stop-loss retries immediately, take-profit waits 5s.
        last_fail = _sell_last_failed_ts.get(sym, 0)
        if last_fail:
            _cooldown = (_SELL_RETRY_COOLDOWN_LOSS
                         if _sell_last_failed_reason.get(sym, "") in ("stop-loss", "hard-stop-loss", "force-sell")
                         else _SELL_RETRY_COOLDOWN_PROFIT)
            if (now - last_fail) < _cooldown:
                continue
        # ── Phase 2 §2.1-2.3: shared exit decision (A1 parity with the sell
        # monitor). Per-position ATR stop / TP / hard SL / BE-move / trail when
        # geometry is stored; legacy global-mult path otherwise.
        with _selling_lock:
            already = sym in _selling
        if already:
            continue
        sell_reason, target_crossed = _evaluate_exit_decision(pos, sym, price, now)
        # Record first-seen crossing time — distinct from _sell_trigger_ts (which is
        # set when we actually call .submit()). The gap between these two timestamps
        # reveals event-loop starvation: if target_crossed_to_trigger_ms >> 0, sells
        # are being delayed by blocking work elsewhere on the event loop thread.
        if (target_crossed or sell_reason) and pos.get("_sell_target_crossed_ts") is None:
            pos["_sell_target_crossed_ts"] = now
        if sell_reason:
            with _selling_lock:
                if sym in _selling:
                    continue
                _selling.add(sym)
                _selling_ts[sym] = time.time()
                pos["_sell_trigger_ts"] = time.time()
                pos["_sell_reason"] = sell_reason
            _sell_executor.submit(_execute_sell, pos, price, sell_reason)

    # ── Inline signal refresh — throttled to every 30 s per coin ─────────────
    for sym, price in prices.items():
        if price <= 0:
            continue
        if now - _tick_signal_ts.get(sym, 0) >= _TICK_REFRESH_SEC:
            _tick_signal_ts[sym] = now
            _inline_refresh_from_ticks(sym, price)

    # ── Buy check dispatch — §3.1c: ticks NO LONGER trigger buy checks. ──────
    # Entry evaluation moved to 5m kline closes (on_kline5m_close) plus the
    # veto-recheck heartbeat (entries.eval_heartbeat_sec). Exits above keep
    # running on every tick (plus the 0.25 s sell-monitor loop) — untouched.
    # Legacy escape hatch: entries.tick_entries=true restores the old
    # per-tick dispatch (single-flight, background thread, same as before).
    _log_entry_timing_mode_once()
    if _entries_cfg()["tick_entries"]:
        _dispatch_buy_check(dict(prices))


# ── Sell monitor — daemon thread, independent of asyncio ─────────────────────

_sell_diag_ts: float      = 0.0
_sell_monitor_heartbeat: float = 0.0   # updated every loop — 0 means not started
_sell_monitor_thread: Optional[threading.Thread] = None

# REST price fallback cache — populated when WebSocket is geo-blocked on Railway
_rest_px: Dict[str, float] = {}
_rest_px_ts: float = 0.0
# Per-symbol freshness of GENUINE REST quotes (not gap-fills from signal cache
# or WS carry-forward). Only fresh entries may override live WS prices in the
# sell monitor — an hours-old REST price must never mask a live WS price.
_rest_px_sym_ts: Dict[str, float] = {}
_REST_PX_FRESH_SEC = 10.0
_stale_px_warn_ts: Dict[str, float] = {}  # throttle "no fresh price" warning per symbol
_REST_PX_TTL = 5.0   # refetch REST prices every 5 s when WebSocket is down
_sell_monitor_last_rest_ts: float = 0.0   # rate-limits sell-monitor REST refresh to 2s


def _fetch_batch_prices(symbols: list, source: str = "batch_prices",
                        critical: bool = True) -> Dict[str, float]:
    """Batch ticker price fetch via _binance_request (source-tagged, circuit-broken).
    Uses single-symbol endpoint (weight=1) when only one symbol requested,
    batch endpoint (weight=4) otherwise. critical=True by default: nearly all
    callers are on the exit path (held watchdog, pre-sell fresh quote,
    position restore) and must not be shed by the rate-limit budget."""
    import urllib.parse as _up2
    if not symbols:
        return {}
    symbols = list(symbols)
    _bl = _get_blimits()
    _w_single = int(getattr(_bl, "WEIGHT_TICKER_PRICE", 2) or 2) if _bl else 2
    _w_batch  = int(getattr(_bl, "WEIGHT_TICKER_PRICE_BATCH", 4) or 4) if _bl else 4
    if len(symbols) == 1:
        # Single-symbol endpoint (weight 2) vs batch endpoint (weight 4)
        url = f"https://data-api.binance.vision/api/v3/ticker/price?symbol={symbols[0]}"
        ok, data, _, _ = _binance_request(url, timeout=3.0, source=source,
                                          weight=_w_single, critical=critical)
        if not ok or not isinstance(data, dict):
            return {}
        try:
            px = float(data.get("price", 0) or 0)
            return {data["symbol"]: px} if px > 0 and data.get("symbol") else {}
        except Exception:
            return {}
    _syms_json = json.dumps(symbols, separators=(',', ':'))
    _encoded   = _up2.quote(_syms_json, safe='')
    url = f"https://data-api.binance.vision/api/v3/ticker/price?symbols={_encoded}"
    ok, data, _, _ = _binance_request(url, timeout=3.0, source=source,
                                      weight=_w_batch, critical=critical)
    if not ok or not isinstance(data, list):
        return {}
    result: Dict[str, float] = {}
    for entry in data:
        s = entry.get("symbol", "")
        try:
            px = float(entry.get("price", 0) or 0)
        except (TypeError, ValueError):
            continue
        if s and px > 0:
            result[s] = px
    return result


def _fetch_rest_prices(symbols: list) -> Dict[str, float]:
    """
    Batch-fetch current prices via REST.
    Tries public CDN first (rarely geo-blocked from Railway), then API mirrors.
    If the multi-symbol batch endpoint fails, falls back to individual symbol
    fetches so a single invalid/delisted coin can't block all prices.
    Timeout 2 s per URL — fast-fail so we never block sell checks.
    """
    import urllib.request as _ur
    import urllib.parse as _up
    if not symbols:
        return {}
    result: Dict[str, float] = {}

    def _parse_response(data) -> Dict[str, float]:
        out: Dict[str, float] = {}
        if isinstance(data, list):
            for item in data:
                s = item.get("symbol", "")
                p = float(item.get("price", 0) or 0)
                if s and p > 0:
                    out[s] = p
        elif isinstance(data, dict) and data.get("symbol"):
            p = float(data.get("price", 0) or 0)
            if p > 0:
                out[data["symbol"]] = p
        return out

    syms_param = _up.quote(json.dumps(symbols))
    for base in _KLINE_BASES[:3]:   # try at most 3 endpoints; fail fast
        try:
            url = f"{base}/api/v3/ticker/price?symbols={syms_param}"
            req = _ur.Request(url, headers={"User-Agent": "TradingBot/1.0"})
            _t0 = time.time()
            with _ur.urlopen(req, timeout=2) as resp:
                _body = resp.read()
                _hdrs = dict(resp.headers)
            _record_rest_health(_hdrs, (time.time() - _t0) * 1000)
            data = json.loads(_body)
            result = _parse_response(data)
            if result:
                return result
        except Exception as _e:
            _record_rest_error(str(_e))
            continue

    # Batch endpoint failed on all bases — try individual fetches (slower but
    # more reliable: a single delisted coin won't break all others).
    if not result and len(symbols) <= 20:  # cap individual fetches to avoid flooding
        for sym in symbols:
            for base in _KLINE_BASES[:2]:
                try:
                    url = f"{base}/api/v3/ticker/price?symbol={sym}"
                    req = _ur.Request(url, headers={"User-Agent": "TradingBot/1.0"})
                    _t0b = time.time()
                    with _ur.urlopen(req, timeout=2) as resp:
                        _body = resp.read()
                        _hdrs = dict(resp.headers)
                    _record_rest_health(_hdrs, (time.time() - _t0b) * 1000)
                    data = json.loads(_body)
                    parsed = _parse_response(data)
                    if parsed:
                        result.update(parsed)
                        break   # got price for this sym — next sym
                except Exception as _e:
                    _record_rest_error(str(_e))
                    continue

    return result


_price_refresher_thread: Optional[threading.Thread] = None
_rest_fail_log_ts: float = 0.0   # throttle REST-failure warning to once per 60 s
_price_refresher_heartbeat: float = 0.0  # updated each loop iteration
_price_refresher_hb_log_ts: float = 0.0  # throttle heartbeat log to once per 60 s

def _price_refresher_loop():
    """
    Dedicated background thread — fetches REST prices for all open positions
    every 2 s and writes them to _rest_px.  Runs independently of the sell
    monitor so network I/O NEVER delays a sell check.

    ALWAYS updates _last_ws_price_ts for every successfully fetched price so
    that price_age_sec in /api/sell-monitor reflects REST freshness, not WS age.
    """
    import data_collector as _dc
    global _rest_px, _rest_px_ts, _rest_fail_log_ts
    global _price_refresher_heartbeat, _price_refresher_hb_log_ts

    try:
        database.log_activity("Price refresher thread started", "info")
    except Exception:
        pass

    while True:
        _price_refresher_heartbeat = time.time()
        try:
            with _positions_lock:
                snap = list(_positions)
            if snap:
                all_syms = list({p["symbol"] for p in snap})
                fetched = _fetch_batch_prices(all_syms)
                if fetched:
                    _now_rf = time.time()
                    _rest_px.update(fetched)
                    for _s_rf in fetched:
                        _rest_px_sym_ts[_s_rf] = _now_rf
                    _rest_px_ts = _now_rf
                    for s, p in fetched.items():
                        # Always inject into _dc.prices and update timestamp so
                        # price_age_sec reflects REST freshness for low-WS-volume coins.
                        _dc.prices[s] = p
                        _last_ws_price_ts[s] = _now_rf

                    # 60s heartbeat log — confirms refresher is alive and working
                    _now_hb = time.time()
                    if _now_hb - _price_refresher_hb_log_ts >= 60.0:
                        _price_refresher_hb_log_ts = _now_hb
                        try:
                            database.log_activity(
                                f"[PriceRefresher] OK — fetched {len(fetched)}/{len(all_syms)} symbols: "
                                + ", ".join(f"{s}={v:.6f}" for s, v in list(fetched.items())[:5]),
                                "info"
                            )
                        except Exception:
                            pass
                else:
                    # REST unavailable — log once per minute so logs show it
                    now_rf = time.time()
                    if now_rf - _rest_fail_log_ts >= 60.0:
                        _rest_fail_log_ts = now_rf
                        missing = [p["symbol"] for p in snap if _rest_px.get(p["symbol"], 0) <= 0]
                        try:
                            database.log_activity(
                                f"Price refresher: REST fetch returned empty for {all_syms}; "
                                f"symbols with no price: {missing}", "warn"
                            )
                        except Exception:
                            pass

                    # Fill gaps from signal cache
                    with _signal_cache_lock:
                        for pos in snap:
                            s = pos["symbol"]
                            if _rest_px.get(s, 0) <= 0:
                                sc = _signal_cache.get(s)
                                if sc and sc.get("price", 0) > 0:
                                    _rest_px[s] = sc["price"]
                    # Last resort: carry forward any WS price we already have
                    for pos in snap:
                        s = pos["symbol"]
                        if _rest_px.get(s, 0) <= 0:
                            ws_p = _dc.prices.get(s, 0)
                            if ws_p > 0:
                                _rest_px[s] = ws_p
        except Exception:
            pass
        time.sleep(10.0)


_held_refresher_thread: Optional[threading.Thread] = None
_held_refresher_hb_log_ts: float = 0.0


def _held_price_ages(held_syms: list) -> Dict[str, float]:
    """Freshest WS price age per held symbol — best of data_collector's
    last_price_ts (wired defensively; the map may not exist yet) and the
    engine's own _last_ws_price_ts. Symbols with no timestamp map to inf."""
    now_ts = time.time()
    dc_ts: dict = {}
    try:
        import data_collector as _dc_ag
        dc_ts = getattr(_dc_ag, "last_price_ts", None) or {}
    except Exception:
        dc_ts = {}
    ages: Dict[str, float] = {}
    for s in held_syms:
        try:
            t1 = float(dc_ts.get(s, 0) or 0)
        except Exception:
            t1 = 0.0
        t2 = float(_last_ws_price_ts.get(s, 0) or 0)
        freshest = max(t1, t2)
        ages[s] = (now_ts - freshest) if freshest > 0 else float("inf")
    return ages


_HELD_SEED_FRESH_SEC: float = 30.0    # a held price older than this counts as unpriced at boot
_HELD_SEED_RETRIES: int = 3           # bounded retries for a transient empty REST batch
_HELD_SEED_BACKOFF: float = 0.5       # base backoff (× attempt) between retries


def _held_unpriced_symbols() -> list:
    """R2/I1 — list of held symbols that do NOT currently have a fresh price.
    A held position is 'priced' only when a positive price exists (engine _rest_px
    or data_collector.prices) AND its freshest age is within _HELD_SEED_FRESH_SEC.
    Empty list ⇒ every open position has a fresh price. Never raises."""
    try:
        with _positions_lock:
            held = list({p.get("symbol") for p in _positions if p.get("symbol")})
    except Exception:
        return []
    if not held:
        return []
    try:
        import data_collector as _dc_u
        dc_prices = getattr(_dc_u, "prices", None) or {}
    except Exception:
        dc_prices = {}
    ages = _held_price_ages(held)
    out: list = []
    for s in held:
        try:
            px = float(_rest_px.get(s, 0) or 0)
        except Exception:
            px = 0.0
        if px <= 0:
            try:
                px = float(dc_prices.get(s, 0) or 0)
            except Exception:
                px = 0.0
        if px <= 0 or ages.get(s, float("inf")) > _HELD_SEED_FRESH_SEC:
            out.append(s)
    return out


def _seed_held_prices(reason: str = "boot", retries: int = _HELD_SEED_RETRIES) -> list:
    """R2/I1 — seed fresh REST prices for every open (held) position, with a
    bounded retry so a transient empty batch at boot doesn't leave positions
    blind. ONE batched /ticker/price call per attempt for the still-unpriced
    held symbols; writes results into _rest_px / _rest_px_sym_ts /
    _last_ws_price_ts / data_collector.prices so the exit path can price stops.

    Returns the list of held symbols STILL unpriced after all retries (empty on
    full success). Logs a CRITICAL if any remain. Never raises — a failure
    degrades (returns the unpriced list) rather than crashing boot."""
    try:
        with _positions_lock:
            held = list({p.get("symbol") for p in _positions if p.get("symbol")})
    except Exception:
        return []
    if not held:
        return []
    try:
        import data_collector as _dc_seed
        dc_prices = getattr(_dc_seed, "prices", None)
    except Exception:
        dc_prices = None

    attempt = 0
    while True:
        attempt += 1
        target = _held_unpriced_symbols()
        if not target:
            return []
        try:
            fetched = _fetch_batch_prices(target, source="held_seed") or {}
        except Exception:
            fetched = {}
        if fetched:
            now_ts = time.time()
            for s, px in fetched.items():
                try:
                    if float(px) <= 0:
                        continue
                except Exception:
                    continue
                _rest_px[s] = px
                _rest_px_sym_ts[s] = now_ts
                _last_ws_price_ts[s] = now_ts
                if dc_prices is not None:
                    try:
                        dc_prices[s] = px
                    except Exception:
                        pass
        still = _held_unpriced_symbols()
        if not still:
            return []
        if attempt > retries:
            # Exhausted bounded retries — held positions remain blind. Log LOUD;
            # the held watchdog keeps retrying on its normal cadence.
            try:
                database.log_activity(
                    f"[HeldSeed] CRITICAL — {len(still)} held positions UNPRICED "
                    f"after {attempt} attempts ({reason}): {still}", "error")
            except Exception:
                pass
            try:
                log_diag_issue(
                    "price_feed", "error",
                    f"Held-price seed failed — {len(still)} positions unpriced",
                    detail=f"{still} ({reason})")
            except Exception:
                pass
            return still
        try:
            time.sleep(_HELD_SEED_BACKOFF * attempt)
        except Exception:
            pass


def _watchdog_fires_24h() -> int:
    """Rolling count of watchdog REST fires in the last 24h (prunes in place)."""
    cutoff = time.time() - 86400.0
    with _watchdog_lock:
        while _watchdog_fire_ts and _watchdog_fire_ts[0] < cutoff:
            _watchdog_fire_ts.popleft()
        return len(_watchdog_fire_ts)


def _watchdog_cycle() -> dict:
    """One held-position watchdog evaluation (C §6.3). Testable single pass.

    Returns {"held": [...], "stale": [...], "fired": bool, "fetched": int}.
    fired=True means ONE batched REST call was made for ALL held symbols
    (never per-symbol loops); fired=False means every held symbol's freshest
    WS price age was within _WATCHDOG_STALE_SEC — no REST at all."""
    global _held_max_price_age_sec, _watchdog_alert_log_ts, _held_refresher_hb_log_ts

    with _positions_lock:
        held_syms = list({p.get("symbol") for p in _positions if p.get("symbol")})
    if not held_syms:
        _held_max_price_age_sec = 0.0
        return {"held": [], "stale": [], "fired": False, "fetched": 0}

    ages = _held_price_ages(held_syms)
    stale = [s for s, a in ages.items() if a > _WATCHDOG_STALE_SEC]
    fired = False
    fetched_n = 0

    if stale:
        # ONE batched call for ALL held symbols (weight 4, critical=True) —
        # routed through _binance_request → binance_limits header recording.
        fetched = _fetch_batch_prices(held_syms, source="held_watchdog")
        # R2 — a transient empty batch (common at boot) must NOT leave held
        # positions blind for a whole cycle: retry a few times with short
        # backoff before giving up. If still empty the caller logs + the loop
        # keeps retrying on the normal watchdog cadence.
        _wd_attempt = 0
        while not fetched and _wd_attempt < _HELD_SEED_RETRIES:
            _wd_attempt += 1
            try:
                time.sleep(_HELD_SEED_BACKOFF * _wd_attempt)
            except Exception:
                pass
            fetched = _fetch_batch_prices(held_syms, source="held_watchdog")
        fired = True
        with _watchdog_lock:
            _watchdog_fire_ts.append(time.time())
        now_ts = time.time()
        if fetched:
            fetched_n = len(fetched)
            try:
                import data_collector as _dc_hr
                dc_prices = getattr(_dc_hr, "prices", None)
            except Exception:
                dc_prices = None
            for s, px in fetched.items():
                if dc_prices is not None:
                    try:
                        dc_prices[s] = px
                    except Exception:
                        pass
                _last_ws_price_ts[s] = now_ts
                _rest_px[s] = px
                _rest_px_sym_ts[s] = now_ts

            # 60s heartbeat log
            if now_ts - _held_refresher_hb_log_ts >= 60.0:
                _held_refresher_hb_log_ts = now_ts
                try:
                    database.log_activity(
                        f"[HeldWatchdog] fired (stale: {stale[:5]}) — "
                        f"{fetched_n}/{len(held_syms)} symbols refreshed: "
                        + ", ".join(f"{s}={v:.6f}" for s, v in list(fetched.items())[:5]),
                        "info"
                    )
                except Exception:
                    pass
        # Re-measure after the refresh attempt for health + alerting
        ages = _held_price_ages(held_syms)

    worst = max(ages.values()) if ages else 0.0
    _held_max_price_age_sec = 9999.0 if worst == float("inf") else round(worst, 2)

    # Health alert: a held symbol's price is >5s stale even after the
    # watchdog's refresh attempt — the exit path is flying blind.
    if _held_max_price_age_sec > _WATCHDOG_ALERT_SEC:
        _now_al = time.time()
        if _now_al - _watchdog_alert_log_ts >= 60.0:
            _watchdog_alert_log_ts = _now_al
            _worst_syms = sorted(
                (s for s, a in ages.items() if a > _WATCHDOG_ALERT_SEC),
                key=lambda s: -ages[s],
            )[:5]
            log_diag_issue(
                "price_feed", "error",
                f"Held-position price age > {_WATCHDOG_ALERT_SEC:.0f}s "
                f"(max {min(_held_max_price_age_sec, 9999):.1f}s) — exits may be delayed",
                detail=f"symbols: {_worst_syms}",
            )

    return {"held": held_syms, "stale": stale, "fired": fired, "fetched": fetched_n}


def _held_position_price_refresher():
    """C §6.3 — held-position exit WATCHDOG (was: unconditional 2s REST poll).

    Every 2s cycle: check each held symbol's freshest WS price age
    (data_collector.last_price_ts, fallback _last_ws_price_ts). ONLY if any
    held symbol's age exceeds _WATCHDOG_STALE_SEC (3s) does it fetch ALL held
    symbols in ONE batched /api/v3/ticker/price?symbols=[...] call
    (critical=True — exits depend on it). Never per-symbol loops.

    Unchanged guarantee: stale prices never trigger stop-loss — the sell
    monitor's _stale_px_syms skip rule is untouched."""
    consecutive_errors = 0

    try:
        database.log_activity(
            "Held-position exit watchdog started (2s cycle; REST only when a "
            "held symbol's WS price age exceeds 3s)", "info")
    except Exception:
        pass

    while True:
        try:
            if _thread_health:
                _thread_health.heartbeat("held_price_refresher")
        except Exception:
            pass
        try:
            result = _watchdog_cycle()
            if not result["held"]:
                time.sleep(5.0)
                continue
            if result["fired"]:
                if result["fetched"] > 0:
                    consecutive_errors = 0
                else:
                    consecutive_errors += 1
                    if consecutive_errors <= 3 or consecutive_errors % 30 == 0:
                        try:
                            database.log_activity(
                                f"[HeldWatchdog] REST fetch returned empty for {result['held']} "
                                f"(attempt {consecutive_errors})", "warn"
                            )
                        except Exception:
                            pass
        except Exception as _e:
            consecutive_errors += 1
            if consecutive_errors <= 3 or consecutive_errors % 30 == 0:
                try:
                    database.log_activity(
                        f"[HeldWatchdog] {type(_e).__name__}: {_e} ({consecutive_errors} consecutive errors)",
                        "warn"
                    )
                except Exception:
                    pass
                log_diag_issue(
                    "price_refresher", "error",
                    f"Held watchdog failed ({consecutive_errors} consecutive)",
                    detail=f"{type(_e).__name__}: {_e}",
                )
        # 2s base cadence (backs off to 30s under consecutive REST errors).
        time.sleep(max(2.0, min(30.0, 2.0 + consecutive_errors * 1.0)))


def start_held_position_refresher():
    """Idempotent — starts the held-position REST refresher if not already running."""
    global _held_refresher_thread
    if _held_refresher_thread is not None and _held_refresher_thread.is_alive():
        return
    _held_refresher_thread = threading.Thread(
        target=_held_position_price_refresher,
        name="held-price-refresher",
        daemon=True
    )
    _held_refresher_thread.start()


# ── Auto-recycle capital ───────────────────────────────────────────────────────
_capital_recycler_thread: Optional[threading.Thread] = None
_AUTO_RECYCLE_AGE_HOURS = 24.0
_AUTO_RECYCLE_GAP_PCT   = 3.0


def _capital_recycler_loop():
    """Every 30 min, force-sell positions that have been underwater >24h with >3% gap.
    Only runs if 'auto_recycle_enabled' is true in strategy.json (default OFF)."""
    while True:
        try:
            if _thread_health:
                _thread_health.heartbeat("capital_recycler")
            time.sleep(1800)
            strategy = _load_strategy()
            if not bool(strategy.get("auto_recycle_enabled", False)):
                continue
            age_h_thresh = float(strategy.get("auto_recycle_age_hours", _AUTO_RECYCLE_AGE_HOURS))
            gap_thresh   = float(strategy.get("auto_recycle_gap_pct",   _AUTO_RECYCLE_GAP_PCT))
            with _positions_lock:
                snap = list(_positions)
            now = time.time()
            for pos in snap:
                sym = pos.get("symbol")
                if not sym:
                    continue
                with _selling_lock:
                    if sym in _selling:
                        continue
                opened_ts = pos.get("opened_at_ts", 0)
                if opened_ts <= 0:
                    continue
                age_h = (now - opened_ts) / 3600
                if age_h < age_h_thresh:
                    continue
                real_bep = compute_real_breakeven_price(pos)
                if real_bep <= 0:
                    continue
                import data_collector as _dc_rc
                cur_price = _dc_rc.prices.get(sym, 0)
                if cur_price <= 0:
                    cur_price = pos.get("current_price") or 0
                if cur_price <= 0:
                    continue
                gap_pct = (real_bep - cur_price) / cur_price * 100
                if gap_pct < gap_thresh:
                    continue
                database.log_activity(
                    f"AUTO_RECYCLE {sym}: held {age_h:.1f}h, needs +{gap_pct:.2f}% to break even — force-selling to free capital",
                    "warn"
                )
                try:
                    log_diag_issue("auto_recycle", "info",
                        f"{sym}: held {age_h:.1f}h gap=+{gap_pct:.2f}% — recycling")
                except (NameError, Exception):
                    pass
                with _selling_lock:
                    if sym in _selling:
                        continue
                    _selling.add(sym)
                    _selling_ts[sym] = now
                import data_collector as _dc_rc2
                _sell_price = _dc_rc2.prices.get(sym, cur_price)
                _sell_executor.submit(_execute_sell, pos, _sell_price, "auto-recycle")
        except Exception as _re:
            try:
                log_diag_issue("auto_recycle", "warn", f"recycler loop error: {_re}")
            except (NameError, Exception):
                pass
            time.sleep(60)


def start_capital_recycler():
    global _capital_recycler_thread
    if _capital_recycler_thread is not None and _capital_recycler_thread.is_alive():
        return
    _capital_recycler_thread = threading.Thread(
        target=_capital_recycler_loop,
        name="capital_recycler",
        daemon=True,
    )
    _capital_recycler_thread.start()


# ── Phase 2 §2.4/§2.5 — exchange-side exit-order management ───────────────────
# Managers (exit_orders module) are LIVE-ONLY: paper mode and the live
# paper-fallback client keep pure local monitoring. Every entry point below is
# guarded on _managed_exits_active().

_managed_exit_thread: Optional[threading.Thread] = None
_managed_reconcile_last_ts: float = 0.0
_MANAGED_POLL_SEC = 2.0
_MANAGED_RECONCILE_SEC = 60.0


def _managed_exits_active() -> bool:
    """True only when exchange-side exit orders may be placed/polled:
    exit_orders importable, live mode, real Binance client (not the paper
    fallback)."""
    if exit_orders is None:
        return False
    try:
        if get_mode() != "live":
            return False
        return not connection.is_using_paper_fallback()
    except Exception:
        return False


def _current_price_for(sym: str) -> float:
    """Freshest known price for sym (WS first, REST cache fallback); 0 unknown."""
    px = 0.0
    try:
        import data_collector as _dc_me
        px = float(_dc_me.prices.get(sym, 0) or 0)
    except Exception:
        px = 0.0
    if px <= 0:
        px = float(_rest_px.get(sym, 0) or 0)
    return px


def _place_managed_exit(pos: dict) -> None:
    """Place the exchange-side exit for a freshly opened LIVE position.

    oco_enabled → SELL OCO (LIMIT_MAKER TP above + STOP_LOSS_LIMIT below);
    else maker_tp → post-only LIMIT_MAKER TP. Any rejection or failure is
    logged and the position falls back to FULL local monitoring — placement
    can never block or undo the position. Paper mode / paper fallback: no-op.
    Nothing new is stored on the position dict — exit_orders owns the state."""
    if not _managed_exits_active():
        return
    sym = pos.get("symbol") or ""
    tp = float(pos.get("tp_price") or 0)
    qty = _floor_qty(float(pos.get("quantity") or 0), sym)
    if not sym or tp <= 0 or qty <= 0:
        return
    cfg = _exit_cfg()
    try:
        if cfg.get("oco_enabled"):
            stop = float(pos.get("stop_price") or 0)
            if stop <= 0:
                database.log_activity(
                    f"[ManagedExit] {sym}: OCO skipped — no stop_price on position "
                    f"(legacy/disabled stop); local monitoring only", "warn")
                return
            resp = exit_orders.place_oco(
                sym, qty, _floor_price_tick(tp, sym), _floor_price_tick(stop, sym),
                float(cfg.get("oco_stop_limit_buffer_pct") or 0.5))
            if resp is None:
                database.log_activity(
                    f"[ManagedExit] {sym}: OCO rejected by engine (price through a leg) "
                    f"— local monitors will exit", "warn")
            else:
                database.log_activity(
                    f"[ManagedExit] {sym}: OCO placed — tp={_floor_price_tick(tp, sym):.8f} "
                    f"stop={_floor_price_tick(stop, sym):.8f} qty={qty}", "info")
        elif cfg.get("maker_tp"):
            # F1.4 CONFIRMED: the maker-TP rests at pos['tp_price'] (the ≥1.6R RR
            # target from _apply_entry_exit_geometry), NOT at a floor/BEP price —
            # so the managed exit and the local monitor both target the same TP.
            resp = exit_orders.place_maker_tp(sym, qty, _floor_price_tick(tp, sym))
            if isinstance(resp, dict) and resp.get("rejected"):
                # -2010 'would immediately match and take': price is already
                # through the target — the local monitors will exit immediately.
                database.log_activity(
                    f"[ManagedExit] {sym}: maker TP rejected ({resp.get('code')}: "
                    f"{resp.get('msg')}) — price already through target, local exit imminent",
                    "info")
            else:
                database.log_activity(
                    f"[ManagedExit] {sym}: maker TP resting @ {_floor_price_tick(tp, sym):.8f} "
                    f"qty={qty}", "info")
    except Exception as _pe:
        # NEVER block the position — full local monitoring covers it.
        log_diag_issue("managed_exit", "warn",
                       f"{sym}: exit-order placement failed — local monitoring only",
                       detail=f"{type(_pe).__name__}: {_pe}")


def _finalize_managed_exit(pos: dict, sym: str, exit_price: float, reason: str,
                           exit_label: str, maker_exit: bool) -> None:
    """Book a position close for an exchange-side exit that FILLED — WITHOUT
    placing any order (the exchange already sold the coins).

    exit_price: the managed order's price (tp_price for TP legs; ≈stop_trigger
    for the OCO stop leg — the STOP_LOSS_LIMIT fill can be marginally lower,
    accepted approximation per §2.5). Fees come from the fees model: maker for
    LIMIT_MAKER fills, taker for the stop leg. Mirrors _do_execute_sell's
    close bookkeeping (trade record → position removal → sync/learning)."""
    from datetime import timezone as _tz
    now_iso = datetime.now(_tz.utc).isoformat()
    qty = float(pos.get("quantity") or 0)
    entry_px = float(pos.get("entry_price") or 0)
    exit_price = float(exit_price or 0)
    try:
        fm = fees.get_fee_model(sym)
        fee_frac = fm.maker() if maker_exit else fm.taker()
    except Exception:
        fee_frac = _fee_rate_for(sym)
    quote = exit_price * qty
    sell_fee = quote * fee_frac
    usdt_returned = quote - sell_fee
    cost = qty * entry_px
    buy_fee = float(pos.get("buy_fee_usdt") or cost * _fee_rate_for(sym))
    net_profit = usdt_returned - cost - buy_fee

    buy_ts = pos.get("timestamp", now_iso)
    try:
        buy_dt = datetime.fromisoformat(str(buy_ts).replace("Z", "+00:00"))
        sell_dt = datetime.fromisoformat(now_iso)
        duration = int((sell_dt - buy_dt).total_seconds()) if buy_dt.tzinfo else 0
    except Exception:
        duration = 0
        buy_dt = datetime.now(_tz.utc)
    try:
        _opened_ts = float(pos.get("opened_at_ts") or 0)
        hold_sec = (time.time() - _opened_ts) if _opened_ts > 0 else (
            float(duration) if duration > 0 else None)
    except Exception:
        hold_sec = float(duration) if duration else None

    trade_record = {
        "coin":               sym,
        "mode":               "live",   # managers are live-only
        "entry_price":        entry_px,
        "exit_price":         exit_price,
        "quantity":           qty,
        "budget_usdt":        pos.get("budget_usdt"),
        "buy_fee":            buy_fee,
        "sell_fee":           sell_fee,
        "net_profit":         net_profit,
        "profitable":         1 if net_profit > 0 else 0,
        "duration_seconds":   duration,
        "entry_rsi":          pos.get("entry_rsi"),
        "entry_ma_position":  pos.get("entry_ma_position"),
        "entry_bb_position":  pos.get("entry_bb_position"),
        "entry_volume_trend": pos.get("entry_volume_trend"),
        "hour_of_day":        buy_dt.hour,
        "day_of_week":        buy_dt.weekday(),
        "timestamp_buy":      buy_ts,
        "timestamp_sell":     now_iso,
        "sell_reason":        reason,
        "signal_snapshot":    pos.get("signal_snapshot"),
        "intended_sell_price": exit_price,
        "sell_slippage_pct":  0.0,      # limit fill at the resting price
    }
    # F1.1: realized_r for the managed (exchange-side) close.
    _record_exit_r(sym, entry_px, exit_price, pos.get("sl_distance_pct"),
                   pos.get("planned_rr"), exit_label, pos=pos)
    # F7: managed exits must also carry an explicit label.
    if not exit_label:
        log_diag_issue("unlabeled_exit", "warn",
                       f"UNLABELED EXIT {sym}: managed close reason={reason!r} "
                       f"— defaulted to 'unknown'")
        exit_label = "unknown"
    try:
        database.log_trade(trade_record,
                           exit_label=exit_label,
                           slippage_bps=0.0,
                           entry_fee_usdt=buy_fee,
                           exit_fee_usdt=sell_fee,
                           hold_time_sec=hold_sec,
                           origin=pos.get("origin", "auto"))
    except Exception as _te_err:
        database.log_activity(f"log_trade error ({sym}, {reason}): {_te_err}", "warn")

    # Phase 4 §4.2b/§4.3 risk hooks — same as _do_execute_sell's close path.
    _note_trade_closed(sym, net_profit)

    # Remove position from DB + memory (exchange fill is the confirmation).
    if pos.get("id"):
        try:
            database.delete_position(pos["id"])
        except Exception:
            pass
    with _positions_lock:
        _positions[:] = [p for p in _positions if p.get("id") != pos.get("id")]
    _rebuild_pos_index()
    _pos_peaks.pop(sym, None)
    _stop_loss_confirmation.pop(sym, None)
    if exit_orders is not None:
        try:
            exit_orders.forget(sym)   # normally already forgotten by check_*
        except Exception:
            pass

    try:
        import supabase_sync
        supabase_sync.sync_sell_result_sync(trade_record, sym, _get_usdt_balance())
    except Exception as _se:
        try:
            database.log_activity(f"Supabase sync error after {reason} {sym}: {_se}", "error")
        except Exception:
            pass
    try:
        learning.learn_from_trade(trade_record)
    except Exception as _le:
        database.log_activity(f"learn_from_trade error ({sym}): {_le}", "warn")

    pnl_sign = "+" if net_profit >= 0 else ""
    msg = (f"[LIVE] SOLD {sym} @ ${exit_price:.4f} via exchange exit ({reason}) "
           f"· received {usdt_returned:.4f} USDT · P&L: {pnl_sign}{net_profit:.4f} USDT "
           f"(held {duration}s)")
    print(f"[{now_iso}] {msg}")
    try:
        database.log_activity(msg, "info")
    except Exception:
        pass


def _clear_managed_exit_before_sell(pos: dict, sym: str, reason: str) -> bool:
    """NO-DOUBLE-SELL enforcement (called from _do_execute_sell, _selling held).

    Returns True → no managed order remains, proceed with the local market
    sell. Returns False → ABORT the local sell because either:
      * the exchange order was already FILLED (-2011 race) — the position is
        finalized HERE from that fill (documented choice: we hold the _selling
        guard and have the managed entry's prices, so booking it immediately
        beats waiting a poll cycle), or
      * the cancel failed for an unknown reason — selling now could double-
        sell; the claim is released by the caller and the next tick retries.
    """
    managed = exit_orders.get_managed(sym)
    if not managed:
        return True
    cancelled = False
    try:
        if managed.get("kind") == "maker_tp":
            # False here means -2011: the order already filled/was cancelled.
            cancelled = exit_orders.cancel_maker_tp(sym)
        else:
            try:
                binance_direct.cancel_order_list(sym, managed["order_list_id"])
                cancelled = True
            except binance_direct.BinanceDirectError as _ce:
                if _ce.code != -2011:
                    raise
                cancelled = False
            exit_orders.forget(sym)
    except Exception as _cx:
        database.log_activity(
            f"[ManagedExit] {sym}: cancel before local sell failed "
            f"({type(_cx).__name__}: {_cx}) — sell aborted, will retry", "warn")
        _sell_last_failed_ts[sym] = time.time()
        _sell_last_failed_reason[sym] = reason
        return False
    if cancelled:
        database.log_activity(
            f"[ManagedExit] {sym}: exchange exit cancelled before local sell ({reason})",
            "info")
        return True

    # -2011: the exchange already closed the order. Re-check its final state
    # instead of selling (NO-DOUBLE-SELL contract step 3).
    try:
        if managed.get("kind") == "maker_tp":
            order = binance_direct.get_order(sym, managed["order_id"])
            if order.get("status") == "FILLED":
                _finalize_managed_exit(pos, sym, managed.get("tp_price"),
                                       "maker-tp", "tp", maker_exit=True)
                return False
        else:
            for oid, rsn, lbl, mk in (
                    (managed.get("tp_order_id"), "oco-tp", "tp", True),
                    (managed.get("sl_order_id"), "oco-sl", "sl", False)):
                if oid is None:
                    continue
                try:
                    leg = binance_direct.get_order(sym, oid)
                except binance_direct.BinanceDirectError as _le:
                    if _le.code == -2011:
                        continue
                    raise
                if leg.get("status") == "FILLED":
                    px = managed.get("tp_price") if mk else managed.get("stop_trigger")
                    _finalize_managed_exit(pos, sym, px, rsn, lbl, maker_exit=mk)
                    return False
    except Exception as _qe:
        database.log_activity(
            f"[ManagedExit] {sym}: post-cancel state check failed "
            f"({type(_qe).__name__}: {_qe}) — sell aborted, will retry", "warn")
        _sell_last_failed_ts[sym] = time.time()
        _sell_last_failed_reason[sym] = reason
        return False

    # Order gone but NOT filled (cancelled externally): the coins are still
    # ours — proceed with the local market sell.
    database.log_activity(
        f"[ManagedExit] {sym}: exchange exit was already cancelled externally — "
        f"proceeding with local sell ({reason})", "warn")
    return True


def _maybe_replace_oco_stop(pos: dict, sym: str, entry: dict, cfg: dict) -> None:
    """OCO trailing: when BE-move/trail logic has raised the effective stop
    above the resting OCO's trigger, cancel/replace the OCO with the new
    level. Throttling + minimum-improvement checks live in replace_oco_stop."""
    desired = max(float(pos.get("be_stop_price") or 0.0),
                  float(pos.get("tp_ratchet_floor") or 0.0))
    if pos.get("be_moved"):
        k_trail = float(cfg.get("k_trail") or 0.0)
        atr_ref = (pos.get("atr_pct_at_entry") or pos.get("sl_distance_pct")
                   or cfg.get("sl_min_pct") or 0.0)
        peak = float(pos.get("peak_price") or 0.0)
        if k_trail > 0 and peak > 0 and atr_ref:
            desired = max(desired, peak * (1.0 - k_trail * float(atr_ref) / 100.0))
    if desired <= float(entry.get("stop_trigger") or 0.0):
        return
    new_stop = _floor_price_tick(desired, sym)
    tick = _tick_size(sym) or 1e-8
    try:
        if exit_orders.replace_oco_stop(
                sym, new_stop, tick,
                float(cfg.get("oco_stop_limit_buffer_pct") or 0.5)):
            database.log_activity(
                f"[ManagedExit] {sym}: OCO stop raised to {new_stop:.8f}", "info")
    except Exception as _re:
        log_diag_issue("managed_exit", "warn",
                       f"{sym}: OCO stop replace failed: {type(_re).__name__}: {_re}")


def _managed_exit_poll_cycle() -> None:
    """One pass over all managed exits (2s cadence, live only).

    Per symbol: claim the _selling guard (skips symbols mid-sell — the local
    path's pre-sell cancel covers those), poll the manager, and act:
      maker_tp: filled → finalize as TP (no order); timeout_cancelled →
        normal market sell through the profit gate; gone → local monitoring.
      oco: filled_tp/filled_sl → finalize; skipped_rescue → immediate local
        market sell as stop-loss (gate-bypassing); gone → local monitoring;
        resting → trailing-stop replace when the local stop has risen."""
    if not _managed_exits_active():
        return
    managed = exit_orders.all_managed()
    if not managed:
        return
    cfg = _exit_cfg()
    pos_index = _pos_by_symbol
    for sym, entry in managed.items():
        pos = pos_index.get(sym)
        if pos is None:
            continue   # orphan — the reconcile pass cancels it
        price = _current_price_for(sym)
        with _selling_lock:
            if sym in _selling:
                continue
            _selling.add(sym)
            _selling_ts[sym] = time.time()
        release = True   # False when the claim is handed to _execute_sell
        try:
            if entry.get("kind") == "maker_tp":
                verdict = exit_orders.check_maker_tp(
                    sym, price, float(cfg.get("maker_tp_timeout_ms") or 1500))
                if verdict == "filled":
                    _finalize_managed_exit(pos, sym, entry.get("tp_price"),
                                           "maker-tp", "tp", maker_exit=True)
                elif verdict == "timeout_cancelled":
                    # Maker order cancelled after the touch-timeout — dispatch
                    # the NORMAL market sell (through the profit gate).
                    pos["_sell_trigger_ts"] = time.time()
                    pos["_sell_reason"] = "take-profit"
                    release = False
                    _sell_executor.submit(
                        _execute_sell, pos,
                        price or float(entry.get("tp_price") or 0), "take-profit")
                elif verdict == "gone":
                    database.log_activity(
                        f"[ManagedExit] {sym}: maker TP vanished externally — "
                        f"local monitoring continues", "warn")
            else:  # oco
                verdict = exit_orders.check_oco(
                    sym, price, float(cfg.get("oco_skip_rescue_sec") or 3.0))
                if verdict == "filled_tp":
                    _finalize_managed_exit(pos, sym, entry.get("tp_price"),
                                           "oco-tp", "tp", maker_exit=True)
                elif verdict == "filled_sl":
                    # Stop fill ≈ stop_trigger (accepted approximation).
                    _finalize_managed_exit(pos, sym, entry.get("stop_trigger"),
                                           "oco-sl", "sl", maker_exit=False)
                elif verdict == "skipped_rescue":
                    # A2 rescue: list cancelled because the stop leg was
                    # skipped — market-sell locally NOW. 'stop-loss' bypasses
                    # the profit gate (loss-side exit).
                    pos["_sell_trigger_ts"] = time.time()
                    pos["_sell_reason"] = "stop-loss"
                    release = False
                    _sell_executor.submit(
                        _execute_sell, pos,
                        price or float(entry.get("stop_trigger") or 0), "stop-loss")
                elif verdict == "gone":
                    database.log_activity(
                        f"[ManagedExit] {sym}: OCO vanished externally — "
                        f"local monitoring continues", "warn")
                elif verdict == "resting":
                    _maybe_replace_oco_stop(pos, sym, entry, cfg)
        except Exception as _me:
            log_diag_issue("managed_exit", "warn",
                           f"{sym}: managed-exit poll error",
                           detail=f"{type(_me).__name__}: {_me}")
        finally:
            if release:
                with _selling_lock:
                    _selling.discard(sym)
                    _selling_ts.pop(sym, None)


def _managed_exit_reconcile() -> None:
    """~60s cross-check of the managed table vs positions + open orders.

    managed-but-no-position → cancel the orphan order + forget;
    position-but-order-vanished → log only; the next check_* poll classifies
    it (filled → finalize, gone → forget) and local monitors keep covering."""
    if not _managed_exits_active():
        return
    if not exit_orders.all_managed():
        return
    with _positions_lock:
        snap = [{"symbol": p.get("symbol")} for p in _positions if p.get("symbol")]
    pos_syms = {p["symbol"] for p in snap}
    try:
        stale = exit_orders.reconcile(snap, binance_direct.get_open_orders)
    except Exception as _rce:
        log_diag_issue("managed_exit", "warn",
                       f"managed-exit reconcile failed: {type(_rce).__name__}: {_rce}")
        return
    for sym in stale:
        entry = exit_orders.get_managed(sym)
        if not entry:
            continue
        if sym not in pos_syms:
            try:
                if entry.get("kind") == "maker_tp":
                    exit_orders.cancel_maker_tp(sym)
                else:
                    try:
                        binance_direct.cancel_order_list(sym, entry["order_list_id"])
                    except binance_direct.BinanceDirectError as _oe:
                        if _oe.code != -2011:
                            raise
                    exit_orders.forget(sym)
                database.log_activity(
                    f"[ManagedExit] {sym}: orphan exchange exit cancelled "
                    f"(no local position)", "warn")
            except Exception as _oce:
                log_diag_issue("managed_exit", "warn",
                               f"{sym}: orphan exit-order cancel failed",
                               detail=f"{type(_oce).__name__}: {_oce}")
        else:
            database.log_activity(
                f"[ManagedExit] {sym}: managed order missing from open orders — "
                f"next poll classifies it (filled/cancelled); local monitors active",
                "warn")


def _managed_exit_loop():
    """Daemon loop: manager polling every 2s + reconcile every ~60s."""
    global _managed_reconcile_last_ts
    while True:
        try:
            if _thread_health:
                _thread_health.heartbeat("managed_exit_poller")
        except Exception:
            pass
        try:
            _managed_exit_poll_cycle()
            now = time.time()
            if now - _managed_reconcile_last_ts >= _MANAGED_RECONCILE_SEC:
                _managed_reconcile_last_ts = now
                _managed_exit_reconcile()
        except Exception as _mle:
            try:
                log_diag_issue("managed_exit", "warn",
                               f"managed-exit loop error: {type(_mle).__name__}: {_mle}")
            except Exception:
                pass
        time.sleep(_MANAGED_POLL_SEC)


def start_managed_exit_poller():
    """Idempotent — starts the managed-exit poll/reconcile daemon."""
    global _managed_exit_thread
    if exit_orders is None:
        return
    if _managed_exit_thread is not None and _managed_exit_thread.is_alive():
        return
    _managed_exit_thread = threading.Thread(
        target=_managed_exit_loop,
        name="managed-exit-poller",
        daemon=True,
    )
    _managed_exit_thread.start()


# ── Phantom position detector ──────────────────────────────────────────────────
_phantom_checker_thread: Optional[threading.Thread] = None


def _phantom_check_loop():
    """Every 300s (live mode only), compare DB positions to Binance balances.
    Logs mismatches to activity_log and inserts unresolved phantom_alerts rows."""
    import sqlite3 as _sq_ph
    while True:
        try:
            time.sleep(300)
            mode = get_mode()
            if mode != "live":
                continue
            with _positions_lock:
                snap = list(_positions)
            if not snap:
                continue
            try:
                acc = _acct()  # geo-block-safe direct transport
                balances = {b["asset"]: float(b.get("free", 0)) + float(b.get("locked", 0))
                            for b in acc.get("balances", [])}
            except Exception as _be:
                log_diag_issue("phantom_detector", "warn", f"get_account failed: {_be}")
                continue
            now_iso = datetime.now(timezone.utc).isoformat()
            for pos in snap:
                sym = pos.get("symbol", "")
                if not sym:
                    continue
                db_qty = float(pos.get("quantity", 0))
                # Strip only trailing USDT to get base asset
                base = sym[:-4] if sym.endswith("USDT") else sym
                binance_qty = balances.get(base, 0.0)
                # Only flag if Binance shows <5% of what DB expects and db_qty > dust
                if db_qty <= 1e-8:
                    continue
                if binance_qty >= db_qty * 0.05:
                    continue
                msg = (f"[PHANTOM] {sym}: DB has qty={db_qty:.6f} but Binance has {binance_qty:.6f}")
                try:
                    database.log_activity(msg, "warn")
                except Exception:
                    pass
                try:
                    conn = _sq_ph.connect(database.DB_PATH)
                    # Avoid duplicate unresolved alerts for same symbol
                    existing = conn.execute(
                        "SELECT id FROM phantom_alerts WHERE symbol=? AND resolved=0", (sym,)
                    ).fetchone()
                    if not existing:
                        conn.execute(
                            "INSERT INTO phantom_alerts (timestamp, symbol, db_qty, binance_qty, resolved) VALUES (?,?,?,?,0)",
                            (now_iso, sym, db_qty, binance_qty)
                        )
                        conn.commit()
                    conn.close()
                except Exception:
                    pass
        except Exception as _pe:
            try:
                log_diag_issue("phantom_detector", "warn", f"phantom loop error: {_pe}")
            except Exception:
                pass


def start_phantom_checker():
    """Idempotent — starts the phantom position detector if not already running."""
    global _phantom_checker_thread
    if _phantom_checker_thread is not None and _phantom_checker_thread.is_alive():
        return
    _phantom_checker_thread = threading.Thread(
        target=_phantom_check_loop,
        name="phantom-checker",
        daemon=True,
    )
    _phantom_checker_thread.start()


def _sell_monitor_loop():
    """
    Fallback daemon thread — wakes every 0.5 s and checks sell conditions.
    Never does any network I/O — REST prices are fetched by _price_refresher_loop
    (a separate daemon thread) and written to _rest_px.  Decoupling means sell
    checks are NEVER delayed by a slow HTTP request.

    Price priority: REST (_rest_px, refreshed every 2 s) > WebSocket (_dc.prices).
    Both are applied before the sell check so the freshest price is always used.
    """
    import data_collector as _dc
    global _sell_diag_ts, _sell_monitor_heartbeat

    try:
        database.log_activity("Sell monitor thread started", "info")
    except Exception:
        pass

    while True:
        _sell_monitor_heartbeat = time.time()
        try:
            if _thread_health:
                _thread_health.heartbeat("sell_monitor")
        except Exception:
            pass
        try:
            with _positions_lock:
                snap = list(_positions)

            # REST price refresh is owned by _held_position_price_refresher
            # (dedicated 2s thread). It was previously fetched inline here,
            # blocking this 0.25s trigger loop for up to 3s whenever the REST
            # endpoint was slow — exactly when fast selling matters most.

            # ── Watchdog: force-clear _selling entries stuck > 45 s ──────────
            # 45s > worst-case legit sell latency (gate REST 3s + balance clamp
            # + exchange-info refresh + order, ~25s of stacked timeouts).
            # Clearing earlier let a second worker submit a DUPLICATE sell
            # while the first was still placing the order.
            now_wd = time.time()
            with _selling_lock:
                stuck = [s for s, t in list(_selling_ts.items()) if now_wd - t > 45]
            for s in stuck:
                with _selling_lock:
                    _selling.discard(s)
                    _selling_ts.pop(s, None)
                try:
                    database.log_activity(
                        f"Sell monitor: force-cleared stuck guard for {s} (>45 s)", "warn"
                    )
                except Exception:
                    pass

            # Always refresh TP/SL multipliers — settings can change at any time.
            _refresh_risk_params()

            # Build price dict: start with WebSocket, then override with REST —
            # but ONLY with genuinely fresh REST quotes (<10s). An old REST
            # price (endpoint down, gap-fill from signal cache) must never
            # mask a live WS price: that both missed real triggers and fired
            # phantom ones.
            _mrg_now = time.time()
            prices = dict(_dc.prices)
            for sym2, p2 in _rest_px.items():
                if p2 > 0 and (_mrg_now - _rest_px_sym_ts.get(sym2, 0)) < _REST_PX_FRESH_SEC:
                    prices[sym2] = p2

            # Signal-cache is the final fallback — fills gaps when both WS and
            # REST are unavailable for a symbol (e.g. first 30 s after redeploy).
            # These prices can be 60 s+ stale, so they are used for DIAGNOSTICS
            # ONLY: symbols in _stale_px_syms are skipped by the trigger loop
            # below (better to retry in 250 ms than to stop-loss on a stale price).
            _stale_px_syms: set = set()
            with _signal_cache_lock:
                sc_snap = dict(_signal_cache)
            for pos in snap:
                s = pos["symbol"]
                if prices.get(s, 0) <= 0:
                    sc_p = sc_snap.get(s, {}).get("price", 0)
                    if sc_p and sc_p > 0:
                        prices[s] = sc_p
                        _stale_px_syms.add(s)

            # Diagnostic log every 60 s — shows ACTUAL sell target, not just breakeven
            now_t = time.time()
            if now_t - _sell_diag_ts >= 60.0:
                _sell_diag_ts = now_t
                lines = []
                no_price_syms = []
                for p in snap:
                    sym    = p["symbol"]
                    price  = prices.get(sym, 0.0)
                    entry  = p["entry_price"]
                    if price <= 0:
                        no_price_syms.append(sym)
                        lines.append(f"{sym} NO_PRICE(entry={entry:.4f})")
                        continue
                    _bep_m_diag = p.get("breakeven_mult_at_buy") or _get_breakeven_mult(entry, sym)
                    _bep_diag   = entry * _bep_m_diag
                    if p.get("tp_price"):  # Phase 2 per-position TP (§2.2)
                        actual = max(float(p["tp_price"]), _bep_diag)
                    else:
                        actual = max(_bep_diag, entry * _user_tp_mult) if _take_profit_enabled else _bep_diag  # real sell threshold
                    pct    = ((price - entry) / entry * 100) if entry else 0
                    gap_pct  = ((actual - price) / price * 100) if price > 0 and actual > price else 0.0
                    qty_held = p.get("quantity", 0)
                    budget   = p.get("budget_usdt", 0)
                    _diag_fee_frac = _fee_rate_for(sym)
                    buy_fee  = float(p.get("buy_fee_usdt") or budget * _diag_fee_frac)
                    gross_now = price * qty_held
                    est_profit = gross_now * (1 - _diag_fee_frac) - budget - buy_fee
                    lines.append(
                        f"{sym} entry={entry:.4f} cur={price:.4f}({pct:+.3f}%) "
                        f"target={actual:.4f} ({'SELL' if price >= actual else f'need +{gap_pct:.3f}%'}) "
                        f"est=${est_profit:+.3f}"
                    )
                msg = f"Sell monitor: {len(snap)} open — " + " | ".join(lines)
                if no_price_syms:
                    msg += f" | WARN: no price for {no_price_syms}"
                database.log_activity(msg, "info")

            now_monitor = time.time()
            for pos in snap:
                sym   = pos["symbol"]
                price = prices.get(sym, 0.0)

                if price <= 0:
                    continue
                if sym in _stale_px_syms:
                    # Only a stale signal-cache price is available — never make a
                    # stop-loss/take-profit decision on it. Warn (throttled) and
                    # retry next cycle when the refresher has a fresh price.
                    if now_monitor - _stale_px_warn_ts.get(sym, 0) >= 60.0:
                        _stale_px_warn_ts[sym] = now_monitor
                        try:
                            database.log_activity(
                                f"Sell monitor: no fresh price for {sym} — skipping sell "
                                f"checks this cycle (signal-cache price may be stale)", "warn"
                            )
                        except Exception:
                            pass
                    continue
                with _selling_lock:
                    if sym in _selling:
                        continue
                # Rate-limit retries: stop-loss retries immediately, take-profit waits 5s.
                last_fail = _sell_last_failed_ts.get(sym, 0)
                if last_fail:
                    _cooldown = (_SELL_RETRY_COOLDOWN_LOSS
                                 if _sell_last_failed_reason.get(sym, "") in ("stop-loss", "hard-stop-loss", "force-sell")
                                 else _SELL_RETRY_COOLDOWN_PROFIT)
                    if (now_monitor - last_fail) < _cooldown:
                        continue
                entry  = pos["entry_price"]
                real_target3 = compute_real_breakeven_price(pos)
                if real_target3 <= 0:
                    _bep_m3 = pos.get("breakeven_mult_at_buy") or _get_breakeven_mult(entry, sym)
                    real_target3 = entry * _bep_m3
                # Display target for the trace: per-position tp_price (§2.2)
                # when stored, else the legacy global-mult target.
                if pos.get("tp_price"):
                    target = max(float(pos["tp_price"]), real_target3)
                elif _take_profit_enabled:
                    target = max(real_target3, entry * _user_tp_mult)
                else:
                    target = real_target3

                # SELL_TRACE — per symbol, throttled to 1 per 60 s
                _tr_now = now_monitor
                if _tr_now - _sell_trace_log_ts.get(sym, 0) >= 60.0:
                    _sell_trace_log_ts[sym] = _tr_now
                    _loss_cd = _loss_cooldown.get(sym, 0)
                    _cd_rem  = max(0.0, _loss_cd - _tr_now)
                    database.log_activity(
                        f"[SELL_TRACE] {sym}: entry={entry:.6f} cur={price:.6f} "
                        f"target={target:.6f} (bep={real_target3:.6f}) "
                        f"above_target={price >= target} "
                        f"cooldown={_cd_rem:.0f}s",
                        "info"
                    )

                # ── Phase 2 §2.1-2.3: SAME shared exit decision as
                # realtime_monitor (A1 parity — the two monitors can never
                # disagree on trigger geometry).
                sell_reason3, _crossed3 = _evaluate_exit_decision(
                    pos, sym, price, now_monitor)

                if sell_reason3:
                    with _selling_lock:
                        if sym in _selling:
                            continue
                        _selling.add(sym)
                        _selling_ts[sym] = time.time()
                        pos["_sell_trigger_ts"] = time.time()
                        if pos.get("_sell_target_crossed_ts") is None:
                            pos["_sell_target_crossed_ts"] = time.time()
                    _sell_executor.submit(_execute_sell, pos, price, sell_reason3)

        except Exception as exc:
            try:
                database.log_activity(f"Sell monitor error: {exc}", "error")
            except Exception:
                pass
            log_diag_issue(
                "sell_monitor", "error",
                f"Monitor loop exception: {type(exc).__name__}",
                detail=str(exc),
            )
        time.sleep(0.25)  # 250ms cycle — halves worst-case sell delay


async def position_guardian():
    """
    Watchdog coroutine — starts the sell monitor and price refresher threads
    and restarts them if they ever die (belt-and-suspenders).
    """
    global _sell_monitor_thread, _price_refresher_thread, _held_refresher_thread
    while True:
        # §3.1c(ii) — veto-recheck heartbeat (restarted here if it ever dies).
        try:
            start_entry_heartbeat()
        except Exception:
            pass
        # O5.1 — fast re-check / confirm-then-fire loop (restarted if it dies).
        try:
            start_fast_recheck()
        except Exception:
            pass
        if not (_sell_monitor_thread and _sell_monitor_thread.is_alive()):
            _sell_monitor_thread = threading.Thread(
                target=_sell_monitor_loop, name="sell-monitor", daemon=True
            )
            _sell_monitor_thread.start()
        if not (_price_refresher_thread and _price_refresher_thread.is_alive()):
            _price_refresher_thread = threading.Thread(
                target=_price_refresher_loop, name="price-refresher", daemon=True
            )
            _price_refresher_thread.start()
        if not (_held_refresher_thread and _held_refresher_thread.is_alive()):
            _held_refresher_thread = threading.Thread(
                target=_held_position_price_refresher, name="held-price-refresher", daemon=True
            )
            _held_refresher_thread.start()
        await asyncio.sleep(5.0)


# ── C §6.2d/§6.5 — cache freshness + engine health contract ──────────────────

def stale_signal_syms(max_age_sec: float = _STALE_SIGNAL_SEC) -> list:
    """Symbols whose signal-cache entry is older than max_age_sec (default 180s
    ≈ every active symbol should be at most ~1 candle old in ws-first mode).
    Judged against the active universe when known (a symbol with NO entry is
    stale too); falls back to cache keys before the first scan pass."""
    now = time.time()
    with _signal_cache_lock:
        cache_ts = {s: float((e or {}).get("ts", 0) or 0) for s, e in _signal_cache.items()}
    universe = _active_universe or set(cache_ts)
    return sorted(s for s in universe if now - cache_ts.get(s, 0.0) > max_age_sec)


def get_engine_health() -> dict:
    """C §6.5 contract — engine health snapshot for control_api/thread_health."""
    stale = stale_signal_syms()
    _signal_scanner_health["stale_signal_count"] = len(stale)
    with _positions_lock:
        held_n = len(_positions)
    return {
        "scanner":                dict(_signal_scanner_health),
        "held_max_price_age_sec": _held_max_price_age_sec,
        "stale_signal_syms":      stale[:10],
        "watchdog_fires_24h":     _watchdog_fires_24h(),
        "held_symbols":           held_n,
    }


# ── C §6.2c — ws-first scanner maintenance (no REST) ─────────────────────────

def _refresh_symbol_from_db(sym: str) -> bool:
    """Rebuild a full cache entry for one symbol from DB candles (no REST).
    Used by the ws-first maintenance pass while the WS buffer is still thin."""
    try:
        db_rows = database.get_candles(sym, config.CANDLE_TIMEFRAME, limit=120)
        if len(db_rows) < 16:
            return False
        candles = [
            {"high":   float(r.get("high") or r["close"]),
             "low":    float(r.get("low")  or r["close"]),
             "close":  float(r["close"]),
             "volume": float(r.get("volume") or 0.0)}
            for r in db_rows
        ]
        return _rebuild_full_entry(sym, candles)
    except Exception:
        return False


def _ws_first_maintenance():
    """C §6.2c — the scanner-loop pass when legacy_rest_scan is OFF (default).
    Makes NO REST calls. Responsibilities:
      1. universe telemetry (approved coins),
      2. DB-fallback cache refresh for symbols whose WS buffers are still thin
         AND whose cache entry has gone stale,
      3. _signal_scanner_health freshness stats (mode/stale counts).
    Runs on an executor thread — the DB reads must stay off the event loop."""
    global _active_universe, _last_empty_universe_warn_ts
    strategy = _load_strategy()
    approved = [
        c["symbol"] for c in (strategy or {}).get("approved_coins", [])
        if c.get("approved")
    ]
    _active_universe = set(approved)
    _signal_scanner_health["universe_size"] = len(approved)
    if not approved:
        if time.time() - _last_empty_universe_warn_ts >= 600.0:
            _last_empty_universe_warn_ts = time.time()
            log.warning("[SignalScanner] ws-first pass: approved-coin universe is EMPTY, no buys possible")
            try:
                database.log_activity(
                    "Signal scan: 0 approved symbols — universe is empty, the bot cannot buy anything",
                    "warn",
                )
            except Exception:
                pass
        _signal_scanner_health["stale_signal_count"] = 0
        return

    try:
        import data_collector as _dc_mt
        ws_bufs = getattr(_dc_mt, "ws_candles", None) or {}
    except Exception:
        ws_bufs = {}

    now = time.time()
    refreshed_db = 0
    for sym in approved:
        try:
            buf_len = len(ws_bufs.get(sym) or [])
        except Exception:
            buf_len = 0
        if buf_len >= 16:
            continue  # kline-close path owns this symbol's freshness
        with _signal_cache_lock:
            entry_ts = float((_signal_cache.get(sym) or {}).get("ts", 0) or 0)
        if now - entry_ts <= _STALE_SIGNAL_SEC:
            continue
        if _refresh_symbol_from_db(sym):
            refreshed_db += 1

    if refreshed_db:
        _signal_scanner_health["db_fallback_refreshes"] = (
            _signal_scanner_health.get("db_fallback_refreshes", 0) + refreshed_db
        )
    _signal_scanner_health["stale_signal_count"] = len(stale_signal_syms())


# ── Process 2: signal scanner (async, refreshes cache every SCAN_INTERVAL_SEC) ─

async def signal_scanner(prices: dict):
    """
    Async coroutine — runs every SCAN_INTERVAL_SEC (60 s).

    C §6.2c — two modes (strategy.json data.legacy_rest_scan, default OFF):
      ws-first (default): kline-close events own signal freshness; this loop
        only (1) triggers _check_buys_from_cache periodically (poll-based buy
        backup), (2) DB-refreshes symbols whose WS buffers are thin (no REST),
        (3) updates _signal_scanner_health freshness stats.
      legacy: the old full REST refresh pass (emergency fallback, one release).
    The buy trigger stays: buys MUST fire even when WebSocket is slow.
    """
    global _last_logged_scan_mode
    _signal_scanner_health["interval_sec"] = float(config.SCAN_INTERVAL_SEC)
    # C §6.1b — adaptive interval: starts at the configured value; stretched
    # when a pass runs long, decayed back when passes get fast again.
    _base_interval = float(config.SCAN_INTERVAL_SEC)
    _effective_interval = _base_interval
    _signal_scanner_health["effective_interval_sec"] = _effective_interval
    while True:
        _t0_scan = time.time()
        try:
            _strategy_sc = _load_strategy()
            _legacy = bool(((_strategy_sc or {}).get("data") or {}).get("legacy_rest_scan", False))
            _mode = "legacy" if _legacy else "ws-first"
            _signal_scanner_health["mode"] = _mode
            if _mode != _last_logged_scan_mode:
                _last_logged_scan_mode = _mode
                log.info("[SignalScanner] Signal engine mode: %s (data.legacy_rest_scan=%s)",
                         _mode, _legacy)
                try:
                    database.log_activity(f"Signal engine mode: {_mode}", "info")
                except Exception:
                    pass
            loop = asyncio.get_running_loop()
            _t_maint = time.perf_counter()
            if _legacy:
                await _refresh_signal_cache()
            else:
                await loop.run_in_executor(None, _ws_first_maintenance)
            _maint_ms = round((time.perf_counter() - _t_maint) * 1000.0, 1)
            # Trigger buy checks right after refreshing — don't wait for WebSocket
            _t_bc = time.perf_counter()
            await loop.run_in_executor(None, _check_buys_from_cache, dict(prices))
            _bc_ms = round((time.perf_counter() - _t_bc) * 1000.0, 1)
            # Per-stage scan breakdown (diagnosis): maintenance + buy-check, and the
            # WolfScore sub-stage (captured inside _check_buys_from_cache). gate_loop
            # ≈ buy_check − wolfscore. Surfaced in _signal_scanner_health["stage_ms"].
            try:
                _wolf_ms = float(_scan_stage_ms.get("wolfscore_ms", 0.0))
                _scan_stage_ms["maint_ms"]      = _maint_ms
                _scan_stage_ms["buy_check_ms"]  = _bc_ms
                _scan_stage_ms["gate_loop_ms"]  = round(max(0.0, _bc_ms - _wolf_ms), 1)
                _scan_stage_ms["total_ms"]      = round(_maint_ms + _bc_ms, 1)
                _scan_stage_ms["updated_ts"]    = time.time()
                _signal_scanner_health["stage_ms"] = dict(_scan_stage_ms)
            except Exception:
                pass
        except Exception as e:
            print(f"[SignalScanner] Unexpected error: {e}")
            log_diag_issue(
                "signal_scanner", "error",
                f"Scan iteration failed: {type(e).__name__}",
                detail=str(e),
            )
        finally:
            _duration_sec = time.time() - _t0_scan
            _signal_scanner_health["last_refresh_ts"]  = time.time()
            _signal_scanner_health["last_duration_ms"] = round(_duration_sec * 1000, 1)
            _signal_scanner_health["scans_completed"] += 1

            # C §6.1b — duration telemetry + adaptive interval.
            # Slow pass (>0.8× current interval): next sleep = duration × 1.5,
            # capped at 120s, floored at SCAN_INTERVAL_SEC. Fast pass
            # (<0.5× SCAN_INTERVAL_SEC): decay back toward SCAN_INTERVAL_SEC.
            # Cap lowered 600→120s: the poll-based buy backup is chained to this
            # interval, so a runaway stretch was leaving entries un-polled for
            # minutes (the "signal engine cooldown"). WS 5m-close buys fire on the
            # dedicated _buy_check_executor regardless, but the poll backstop must
            # not lag by minutes.
            if _duration_sec > 0.8 * _effective_interval:
                _new_interval = min(120.0, max(_base_interval, _duration_sec * 1.5))
                log.warning(
                    "[SignalScanner] Slow scan pass: duration=%.1fs > 0.8×interval "
                    "(%.1fs) — stretching next interval to %.1fs",
                    _duration_sec, _effective_interval, _new_interval,
                )
                _effective_interval = _new_interval
            elif (_duration_sec < 0.5 * _base_interval
                  and _effective_interval > _base_interval):
                # Halve the stretch each fast pass until back at the base.
                _effective_interval = max(
                    _base_interval,
                    _base_interval + (_effective_interval - _base_interval) * 0.5,
                )
                if _effective_interval - _base_interval < 1.0:
                    _effective_interval = _base_interval
            _signal_scanner_health["effective_interval_sec"] = round(_effective_interval, 1)

        await asyncio.sleep(_effective_interval)



_KLINE_BASES = [
    # CDN first — not geo-blocked, works when api.binance.com returns 451
    "https://data-api.binance.vision",
    "https://api.binance.com",
    "https://api1.binance.com",
    "https://api2.binance.com",
    "https://api3.binance.com",
    "https://api4.binance.com",
]


async def _fetch_klines(session, sym: str) -> list:
    """Try each Binance base URL to work around regional geo-blocks (HTTP 451)."""
    import aiohttp
    last_exc: Exception = RuntimeError("No Binance bases configured")
    for base in _KLINE_BASES:
        url = f"{base}/api/v3/klines?symbol={sym}&interval=1m&limit=50"
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=3)) as resp:
                try:
                    _limits_record_headers(dict(resp.headers))  # C §6.4 weight accounting
                except Exception:
                    pass
                resp.raise_for_status()
                return await resp.json()
        except Exception as e:
            last_exc = e
    raise last_exc


async def _refresh_signal_cache():
    """
    Refresh the in-memory signal cache concurrently (asyncio.gather).
    Falls back to DB candles if REST is geo-blocked.
    Never executes trades.

    C §6.1a — single-flight skip-not-queue: if a refresh is already in flight,
    this invocation is SKIPPED (counted in scan_skipped_overlap). Nothing may
    ever queue a second concurrent refresh behind a running one.
    """
    if not _signal_refresh_inflight.acquire(blocking=False):
        _signal_scanner_health["scan_skipped_overlap"] += 1
        log.warning(
            "[SignalScanner] Refresh already in flight — skipping this "
            "invocation (skipped %d total)",
            _signal_scanner_health["scan_skipped_overlap"],
        )
        return
    try:
        await _refresh_signal_cache_locked()
    finally:
        _signal_refresh_inflight.release()


async def _refresh_signal_cache_locked():
    """Body of the signal-cache refresh. Only ever runs under the single-flight
    guard in _refresh_signal_cache — do not call directly."""
    import aiohttp
    global _last_empty_universe_warn_ts, _active_universe
    _pass_t0 = time.time()
    strategy = _load_strategy()
    if not strategy:
        return

    approved_coins = [
        c["symbol"]
        for c in strategy.get("approved_coins", [])
        if c.get("approved")
    ]
    # C §6.1e — universe telemetry + logging (warn when empty, throttled 10 min)
    _active_universe = set(approved_coins)
    _signal_scanner_health["universe_size"] = len(approved_coins)
    if not approved_coins:
        if time.time() - _last_empty_universe_warn_ts >= 600.0:
            _last_empty_universe_warn_ts = time.time()
            log.warning("[SignalScanner] Signal scan: scanning 0 symbols — approved-coin universe is EMPTY, no buys possible")
            try:
                database.log_activity(
                    "Signal scan: 0 approved symbols — universe is empty, the bot cannot buy anything",
                    "warn",
                )
            except Exception:
                pass
        return
    log.info("Signal scan: scanning %d symbols", len(approved_coins))

    async def _refresh_one(session, sym: str) -> bool:
        import data_collector as _dc
        MIN = 16          # matches _dc._MIN_CANDLES — enough for RSI to fire
        closes = volumes = candles = None
        raw = None

        # 1. Try Binance REST (fastest, most data — includes full OHLC for ATR)
        # C §6.1c — hard budget: the multi-base loop inside _fetch_klines can
        # stack 6 bases × 3s; bound EACH attempt to 5s total and allow exactly
        # ONE retry, so a symbol can never hold the pass for more than ~10s.
        try:
            for _attempt in (1, 2):
                try:
                    raw = await asyncio.wait_for(
                        _fetch_klines(session, sym),
                        timeout=_SCAN_SYMBOL_FETCH_TIMEOUT_SEC,
                    )
                    break
                except Exception:
                    if _attempt == 2:
                        raise
            closes  = [float(k[4]) for k in raw]
            volumes = [float(k[5]) for k in raw]
            candles = [
                {"high": float(k[2]), "low": float(k[3]),
                 "close": float(k[4]), "volume": float(k[5])}
                for k in raw
            ]
        except Exception:
            raw = None

        # 2. Fall back to DB candles (populated by download_history / kline saves)
        if not closes or len(closes) < MIN:
            db_rows = database.get_candles(sym, config.CANDLE_TIMEFRAME, limit=60)
            if len(db_rows) >= MIN:
                closes  = [float(c["close"])  for c in db_rows]
                volumes = [float(c["volume"]) for c in db_rows]
                candles = [
                    {"high":   float(c.get("high") or c["close"]),
                     "low":    float(c.get("low")  or c["close"]),
                     "close":  float(c["close"]),
                     "volume": float(c["volume"])}
                    for c in db_rows
                ]

        # 3. Fall back to in-memory WebSocket candle buffer (no REST needed)
        #    This kicks in when Binance REST is geo-blocked from Railway's servers.
        #    Fills up at 1 candle/minute; RSI fires after 16 minutes.
        if not closes or len(closes) < MIN:
            buf = _dc.ws_candles.get(sym, [])
            if len(buf) >= MIN:
                closes  = [float(r[4]) for r in buf]
                volumes = [float(r[5]) for r in buf]
                candles = [
                    {"high": float(r[2]), "low": float(r[3]),
                     "close": float(r[4]), "volume": float(r[5])}
                    for r in buf
                ]

        # 4. Time-sampled price buffer — meaningful RSI after 8 min; no OHLC.
        if not closes or len(closes) < MIN:
            samples = list(_dc.price_samples.get(sym, []))
            if len(samples) >= _dc._MIN_SAMPLES:
                closes  = samples
                volumes = [1.0] * len(samples)
                candles = [{"high": c, "low": c, "close": c, "volume": 1.0} for c in samples]

        # 5. Raw price-tick buffer — available in seconds but RSI quality is poor.
        #    Used only as last resort (first 8 minutes of uptime).
        if not closes or len(closes) < MIN:
            ticks = list(_dc.price_ticks.get(sym, []))
            if len(ticks) >= _dc._MIN_TICKS:
                closes  = ticks
                volumes = [1.0] * len(ticks)
                candles = [{"high": c, "low": c, "close": c, "volume": 1.0} for c in ticks]

        if not (closes and len(closes) >= MIN and candles):
            return False

        # Evaluate 6 signals (ATR accurate only when real OHLC is available)
        signals = evaluate_signals(candles)
        score   = sum(signals.values())
        rsi_list    = indicators.calc_rsi(closes, 14)
        rsi_display = rsi_list[-2] if rsi_list[-2] is not None else 0.0

        # BB veto — computed on the last completed candle (index -2)
        bb_u, bb_m, _ = indicators.calc_bollinger(closes)
        bb_ok = indicators.bb_buy_allowed(closes[-2], bb_u[-2], bb_m[-2])

        # 5m timeframe veto — cached 180 s to avoid repeated REST calls
        with _signal_cache_lock:
            prev = dict(_signal_cache.get(sym, {}))
        _5m_age = time.time() - prev.get("5m_ts", 0)
        if _5m_age >= 180:
            try:
                candles_5m = await asyncio.to_thread(_dc.fetch_5m_candles, sym)
                if not candles_5m or len(candles_5m) < 21:
                    # Cold start: the WS 5m buffer needs ~105 min to refill
                    # after a restart. Derive 5m candles from stored 1m
                    # history so the veto can evaluate real trend data.
                    try:
                        _db_1m = await asyncio.to_thread(
                            database.get_candles, sym, config.CANDLE_TIMEFRAME, 130
                        )
                        _derived_5m = indicators.aggregate_candles(_db_1m, group=5)
                        if len(_derived_5m) >= 21:
                            candles_5m = _derived_5m
                    except Exception:
                        pass
                if not candles_5m or len(candles_5m) < 21:
                    # Still not enough data — this is data availability, not a
                    # downtrend. Stay NEUTRAL (pass) during warmup instead of
                    # vetoing every coin '5m downtrend' for up to ~105 min
                    # after each restart.
                    five_m_ok = True
                    global _warned_5m_warmup
                    if not _warned_5m_warmup:
                        _warned_5m_warmup = True
                        _rem_s = max(0, 21 - len(candles_5m or [])) * 300
                        database.log_activity(
                            f"5m trend backfilling (~{_rem_s}s remaining) — "
                            f"treated neutral until buffer ready", "warn"
                        )
                else:
                    five_m_ok = indicators.is_5m_bullish(candles_5m)
                _5m_ts = time.time()
            except Exception:
                five_m_ok  = prev.get("5m_ok", True)
                _5m_ts     = prev.get("5m_ts", 0)
        else:
            five_m_ok = prev.get("5m_ok", True)
            _5m_ts    = prev.get("5m_ts", 0)

        # Phase 2: extra fields for new signal registry
        # low_24h — minimum low over last 24h from DB candles (no new REST call).
        # C §6.1d — this 1440-row read is the heaviest per-scan cost; cache the
        # value per symbol for 10 minutes and reuse within the window.
        _l24_cached = _low24h_cache.get(sym)
        if _l24_cached is not None and (time.time() - _l24_cached[1]) < _LOW24H_CACHE_TTL_SEC:
            low_24h = _l24_cached[0]
        else:
            try:
                _db_rows_24h = database.get_candles(sym, config.CANDLE_TIMEFRAME, limit=1440)
                if len(_db_rows_24h) >= 60:
                    low_24h = min(float(r.get("low") or r["close"]) for r in _db_rows_24h)
                else:
                    low_24h = min(c["low"] for c in candles) if candles else None
            except Exception:
                low_24h = None
            if low_24h is not None:
                _low24h_cache[sym] = (low_24h, time.time())

        # klines_1m — last 15 candles with OHLCV (R1 reversal + M4 micro-pullback)
        try:
            if raw and len(raw) >= 2:
                klines_1m = [
                    {"open": float(k[1]), "high": float(k[2]),
                     "low": float(k[3]), "close": float(k[4]), "volume": float(k[5])}
                    for k in raw[-15:]
                ]
            elif candles and len(candles) >= 2:
                klines_1m = candles[-15:]
            else:
                klines_1m = prev.get("klines_1m", [])
        except Exception:
            klines_1m = prev.get("klines_1m", [])

        # stoch_rsi_val — Stochastic RSI from existing closes data
        try:
            stoch_rsi_val = indicators.calc_stoch_rsi(closes)
        except Exception:
            stoch_rsi_val = None

        with _signal_cache_lock:
            _signal_cache[sym] = {
                "signals":       signals,
                "score":         score,
                "price":         closes[-1],
                "rsi_val":       rsi_display,
                "bb_ok":         bb_ok,
                "5m_ok":         five_m_ok,
                "5m_ts":         _5m_ts,
                "ts":            time.time(),
                "low_24h":       low_24h,
                "klines_1m":     klines_1m,
                "stoch_rsi_val": stoch_rsi_val,
            }
        return True

    results = []
    aborted_remaining = 0
    async with aiohttp.ClientSession() as session:
        batch_size = 5
        for i in range(0, len(approved_coins), batch_size):
            # C §6.1c — whole-pass ceiling: check elapsed at each batch boundary
            # and abort cleanly. Un-refreshed symbols keep their previous cache
            # entries (the cache is never cleared, only overwritten).
            _elapsed = time.time() - _pass_t0
            if _elapsed > _SCAN_PASS_BUDGET_SEC:
                aborted_remaining = len(approved_coins) - i
                log.warning(
                    "[SignalScanner] Pass exceeded %.0fs budget (%.1fs elapsed) — "
                    "aborting with %d/%d symbols un-refreshed (they keep their "
                    "previous cache entries)",
                    _SCAN_PASS_BUDGET_SEC, _elapsed,
                    aborted_remaining, len(approved_coins),
                )
                break
            batch = approved_coins[i:i + batch_size]
            batch_results = await asyncio.gather(
                *[_refresh_one(session, sym) for sym in batch],
                return_exceptions=True,
            )
            results.extend(batch_results)
            if i + batch_size < len(approved_coins):
                await asyncio.sleep(1.0)

    updated = sum(1 for r in results if r is True)
    with _signal_cache_lock:
        ready = sum(1 for v in _signal_cache.values() if v["score"] >= config.MIN_SIGNALS_TO_BUY)
    _abort_note = (f", ABORTED at {_SCAN_PASS_BUDGET_SEC:.0f}s budget with "
                   f"{aborted_remaining} un-refreshed" if aborted_remaining else "")
    database.log_activity(
        f"Signal scan: scanning {len(approved_coins)} symbols — "
        f"{updated}/{len(approved_coins)} updated, "
        f"{ready} at score≥{config.MIN_SIGNALS_TO_BUY}{_abort_note}",
        "info",
    )

# ── Phase 3 §3.1 — wire the 5m kline-close callback ──────────────────────────
# Registered at import time (control_api imports this module during startup,
# before the WebSocket feed launches) so no control_api change is needed.
# data_collector also falls back to on_kline5m_close via a guarded import if
# this registration never ran (e.g. direct data_collector usage in tests).
try:
    import data_collector as _dc_reg5m
    if hasattr(_dc_reg5m, "register_kline5m_callback"):
        _dc_reg5m.register_kline5m_callback(on_kline5m_close)
except Exception:
    pass
