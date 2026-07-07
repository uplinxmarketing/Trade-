"""
Data collector — historical REST download + live WebSocket price/kline feed.
No auth required for any endpoint in any mode.

§6.2/§6.3/§6.4 additions:
- Every REST call is gated through binance_limits (weight budget, 429/418
  back-pressure) with a guarded import so old deploys degrade to no-ops.
- 1m/5m candle buffers hold 120/60 closed candles, REST-backfilled at startup
  and gap-repaired per-symbol on missing kline ranges and after reconnects.
- Market streams are sharded across WS connections (≤300 streams each) plus a
  DEDICATED connection for held-symbol exit feeds (@trade + @bookTicker).
- Watchlist changes are diffed and applied via live SUBSCRIBE/UNSUBSCRIBE
  frames (paced, batched); large diffs fall back to close-and-reconnect.
- Connections are rotated make-before-break at ~23h (Binance drops at 24h).
- get_data_health() feeds the §6.5 health panel.
"""

import asyncio
import itertools
import json
import random
import threading
import time
import urllib.error
import urllib.request
from collections import deque
from datetime import datetime
from typing import Dict, Callable, List, Optional, Tuple

import config
import database
import indicators
from connection import client

# Shared Binance limit-compliance state (§6.4). Guarded import: the module may
# be absent in an old deploy — every use goes through the _limits_* no-op
# fallbacks below.
try:
    import binance_limits
except Exception:
    binance_limits = None

# Shared symbol-status source (G2). Guarded import: an old deploy may lack the
# get_tradeable_symbols()/symbol_status() helpers — every use fails OPEN (no
# filtering) via _get_tradeable_symbols() returning None.
try:
    import exchange_info
except Exception:
    exchange_info = None

# Shared live prices dict — imported by trade_engine and strategy_engine
prices: Dict[str, float] = {}

# Timestamp of the last WebSocket message — watchdog/health checks read this.
last_ws_message_ts: float = 0.0

# ── WebSocket health counters — zero-cost, written on existing events ─────────
_ws_health: Dict = {
    "connected":          False,
    "last_message_ts":    0.0,
    "messages_received":  0,
    "connect_count":      0,
    "disconnect_count":   0,
    "last_connect_ts":    0.0,
    "last_disconnect_ts": 0.0,
    "subscribed_coins":   0,
    "resubscribe_count":  0,
}

# In-memory rolling candle buffer — filled by WebSocket kline-close events and
# REST backfill/gap-repair. Primary data source for signal computation when
# Binance REST is geo-blocked. Holds up to 120 closed 1m candles per coin;
# once 16+ have accumulated, RSI signals fire and buys can happen.
ws_candles: Dict[str, list] = {}
_WS_CANDLE_MAX = 120  # closed 1m candles to keep per coin (§6.2)
_MIN_CANDLES   = 16   # minimum candles needed for at least RSI signal to work

# In-memory 5-minute candle buffer — filled via WebSocket @kline_5m subscription.
# Eliminates REST calls for 5m veto checks after the first 5 candles arrive.
ws_candles_5m: Dict[str, list] = {}
_WS_5M_CANDLE_MAX = 60   # 5 hours of 5m candles (§6.2)
_MIN_CANDLES_5M   = 21   # enough for EMA21 to be meaningful

# In-memory 15-minute candle buffer — filled via WebSocket @kline_15m + REST
# backfill (I1.3). trade_engine's T2_ema50_15m_slope needs EMA50 over 15m
# (>=50 candles); the engine reads this via fetch_15m_candles() (preferred) or
# the raw ws_candles_15m buffer directly. Backfilling 15m klines (limit 60)
# makes T2 usable within the startup backfill window instead of waiting hours
# for the 5m buffer to grow enough 15m aggregate candles.
ws_candles_15m: Dict[str, list] = {}
_WS_15M_CANDLE_MAX = 60   # 15 hours of 15m candles — >=50 for EMA50 (§3.1/T2)
_MIN_CANDLES_15M   = 50   # EMA50 over 15m needs >=50 candles to be meaningful

# Guards ws_candles / ws_candles_5m mutation: WS event loop appends while
# backfill/gap-repair executor threads merge. Merges REPLACE the per-symbol
# list atomically so lock-free readers (trade_engine, futures_engine) never
# see a half-built list.
_buffers_lock = threading.Lock()

# Rolling price-tick buffer — filled by WebSocket @trade events (held symbols'
# dedicated feed) and by @miniTicker 1s roll-ups for everything else.
price_ticks: Dict[str, deque] = {}
_TICK_MAX  = 50
_MIN_TICKS = 15

# Time-sampled price buffer — one close price recorded per 1 second.
# After 16 seconds: 16 samples → RSI(14) computes on meaningful price variation.
price_samples: Dict[str, list] = {}       # one price per _SAMPLE_INTERVAL
_price_sample_ts: Dict[str, float] = {}   # last sample time per coin
_SAMPLE_INTERVAL = 1.0    # seconds between samples
_SAMPLE_MAX      = 300    # keep 5 minutes of samples (300 × 1s)
_MIN_SAMPLES     = 16     # 16 seconds needed before RSI fires

# ── Held-symbol freshness (§6.3) ──────────────────────────────────────────────
# Per-symbol timestamp of the freshest WS price event (trade / bookTicker /
# miniTicker). trade_engine's REST price watchdog consumes this via
# held_price_age(); we only expose accurate data here.
last_price_ts: Dict[str, float] = {}

# Best bid/ask from @bookTicker (held-symbol dedicated feed):
# {symbol: {"bid": float, "bid_qty": float, "ask": float, "ask_qty": float, "ts": float}}
book_ticker: Dict[str, dict] = {}

# Callbacks registered by main.py to avoid circular imports
_price_callback: Optional[Callable[[Dict[str, float]], None]] = None
_kline_callback: Optional[Callable[[str, list, list], None]]  = None
# Phase 3 §3.1 — invoked whenever a 5m kline CLOSES for a symbol:
# cb(symbol, closed_row, buf_snapshot) where closed_row is
# [open_time, o, h, l, c, v] and buf_snapshot is a copy of the full
# ws_candles_5m buffer after the append. Always called from an executor
# thread (same offload pattern as the 1m _persist_and_signal path).
_kline5m_callback: Optional[Callable[[str, list, list], None]] = None


def register_price_callback(cb: Callable[[Dict[str, float]], None]):
    global _price_callback
    _price_callback = cb


def register_kline_callback(cb: Callable[[str, list, list], None]):
    """Wire kline-close events into trade_engine.update_coin_signals."""
    global _kline_callback
    _kline_callback = cb


def register_kline5m_callback(cb: Callable[[str, list, list], None]):
    """§3.1: wire 5m kline-CLOSE events into trade_engine's 5m entry
    evaluation (on_kline5m_close). The 1m callback stays registered for
    exits / cache freshness — this is entry-timing only."""
    global _kline5m_callback
    _kline5m_callback = cb


def _dispatch_kline5m(sym: str, closed: list, buf_snapshot: list):
    """Run the registered 5m-close callback on a worker thread (executor).
    Falls back to trade_engine.on_kline5m_close when no callback was
    registered (guarded — trade_engine may be absent in stripped deploys)."""
    try:
        cb = _kline5m_callback
        if cb is None:
            try:
                import trade_engine as _te_5m
                cb = getattr(_te_5m, "on_kline5m_close", None)
            except Exception:
                cb = None
        if cb is not None:
            cb(sym, closed, buf_snapshot)
    except Exception as e:
        print(f"[DataCollector] 5m close callback error ({sym}): {e}")


# ── Dynamic watchlist ─────────────────────────────────────────────────────────
# The WS stream set is rebuilt from the persisted watchlist (strategy.json
# approved_coins — the same file /api/coins writes). Changes are DIFFED against
# the current subscriptions and applied via live SUBSCRIBE/UNSUBSCRIBE frames
# (debounced to at most once per 5 s); a ~60 s poll of the persisted list is a
# self-healing fallback even if control_api never calls refresh_watchlist().

_reconnect_requested = threading.Event()
_WATCHLIST_POLL_SEC  = 60

# Hook for trade_engine: positions changed → resync the dedicated held-symbol
# exit-feed connection immediately (a 5 s poll is the fallback).
_positions_changed = threading.Event()
_HELD_POLL_SEC = 5.0


def refresh_watchlist():
    """Signal the WS manager to re-read the persisted watchlist and diff its
    subscriptions. Thread-safe (just sets an Event); safe to call at any time,
    including before the WS loop has started."""
    _reconnect_requested.set()
    print("[DataCollector] Watchlist refresh requested — subscriptions will be diffed")


def notify_positions_changed():
    """trade_engine calls this whenever _positions changes so the dedicated
    held-symbol WS connection (@trade + @bookTicker) resubscribes right away."""
    _positions_changed.set()


# ── G2/H3: dead/renamed-ticker exclusion ─────────────────────────────────────
# Delisted or renamed symbols (AGIX, EOS, FTM, LRC, MATIC, OCEAN, MKR) never
# subscribe. Filter the stream universe through exchange_info's tradeable set so
# they are DROPPED (with a one-time WARN each) instead of being auto-resubscribed
# forever by the F5 reconcile. The tradeable set is cached locally with a 1 h TTL
# so the reconcile checks excluded symbols at most once per hour.
#
# H3: the live-tradeable tier fails OPEN (unavailable set → no filtering), which
# on the VPS left the dead tickers in the universe forever because the
# exchangeInfo fetch is unreliable there. So exchange_info.KNOWN_DELISTED is now
# dropped UNCONDITIONALLY by _filter_tradeable, even with no live fetch. TON is
# NOT in that set: it is re-checked individually against the live tradeable set,
# and if TON (TONUSDT) is TRADING its subscription failure is a SEPARATE real
# bug ('ws_subscribe_failed'), not a delisting.

_tradeable_cache: Dict = {"symbols": None, "ts": 0.0}
_TRADEABLE_TTL_SEC = 3600.0                 # 1 h — excluded symbols re-checked ≤1/h
_tradeable_lock = threading.Lock()

# sym -> non-TRADING status (e.g. "BREAK", "DELISTED", "UNKNOWN"); exposed via
# get_data_health() as excluded_symbols.
_excluded_symbols: Dict[str, str] = {}
_excluded_warned: set = set()               # syms already WARN-logged (one-time)
_excluded_lock = threading.Lock()


def _get_tradeable_symbols() -> Optional[set]:
    """Cached tradeable-symbol set from exchange_info (1 h TTL). Returns None
    when exchange_info / get_tradeable_symbols is unavailable so callers fail
    OPEN. On a transient fetch error the last good set is reused rather than
    dropping all filtering."""
    if exchange_info is None:
        return None
    fn = getattr(exchange_info, "get_tradeable_symbols", None)
    if fn is None:
        return None
    now = time.time()
    with _tradeable_lock:
        cached = _tradeable_cache["symbols"]
        if cached is not None and now - _tradeable_cache["ts"] < _TRADEABLE_TTL_SEC:
            return cached
    try:
        syms = fn()
        if syms:
            syms = set(syms)
            with _tradeable_lock:
                _tradeable_cache["symbols"] = syms
                _tradeable_cache["ts"] = now
            return syms
    except Exception:
        pass
    # Empty/error: reuse the stale set if we have one, else fail open (None).
    with _tradeable_lock:
        return _tradeable_cache["symbols"]


def _symbol_status(sym: str) -> str:
    """Best-effort non-TRADING status string for a dropped symbol. Returns
    'DELISTED' for exchange_info.KNOWN_DELISTED even with no live exchangeInfo
    (symbol_status handles that hardcoded verdict itself)."""
    fn = getattr(exchange_info, "symbol_status", None) if exchange_info else None
    if fn is not None:
        try:
            st = fn(sym)
            if st:
                return str(st)
        except Exception:
            pass
    return "UNKNOWN"


def _known_delisted() -> set:
    """H3: the hardcoded delisted/renamed set from exchange_info, or an empty
    set when exchange_info (or the attribute) is unavailable. This is the ONLY
    part of the filter that does not fail open — these tickers are dropped even
    when a live tradeable set can't be fetched."""
    if exchange_info is None:
        return set()
    try:
        return set(getattr(exchange_info, "KNOWN_DELISTED", None) or set())
    except Exception:
        return set()


def _filter_tradeable(coins: list) -> list:
    """G2/H3: drop non-TRADING (delisted/renamed) symbols from the stream
    universe.

    H3 change — two tiers:
      1. exchange_info.KNOWN_DELISTED is dropped UNCONDITIONALLY (status
         'DELISTED'), even when the live tradeable set is None/empty because
         exchangeInfo is unreachable. This is what finally removes the 8 dead
         tickers on the VPS where the fetch fails and the filter used to fail
         open, keeping them forever.
      2. When a live tradeable set IS available, any OTHER non-TRADING symbol
         is also dropped (the original G2 fail-open behaviour for the unknowns).

    Dropped symbols + their status are recorded in _excluded_symbols and
    WARN-logged exactly ONCE each (not once per reconcile pass)."""
    tradeable = _get_tradeable_symbols()   # None/empty when exchangeInfo is down
    delisted = _known_delisted()
    kept: list = []
    dropped: list = []
    for c in coins:
        if c in delisted:
            dropped.append(c)                       # tier 1 — always
        elif tradeable and c not in tradeable:
            dropped.append(c)                       # tier 2 — only when known
        else:
            kept.append(c)
    to_warn: list = []
    restored: list = []
    with _excluded_lock:
        # I4.2 — a previously-excluded symbol that is TRADING again (e.g. TON
        # coming back from a BREAK halt) is cleared so the next reconcile
        # re-subscribes it, and the restore is logged. KNOWN_DELISTED entries
        # never "relist" this way — they stay dropped until the hardcoded set
        # is edited, so they are skipped here.
        for c in coins:
            if c in delisted:
                continue
            if tradeable and c in tradeable:
                prev = _excluded_symbols.pop(c, None)
                if prev is not None:
                    _excluded_warned.discard(c)
                    restored.append((c, prev))
        for c in dropped:
            if c not in _excluded_symbols:
                _excluded_symbols[c] = _symbol_status(c)
            if c not in _excluded_warned:
                _excluded_warned.add(c)
                to_warn.append((c, _excluded_symbols[c]))
    for c, status in to_warn:
        print(f"[DataCollector] Excluding non-tradeable symbol {c} "
              f"(status={status}) — dropped from stream universe")
        try:
            import trade_engine as _te_ex
            _te_ex.log_diag_issue(
                "data", "warn",
                f"{c} excluded from market streams (status={status})")
        except Exception:
            pass
    for c, prev in restored:
        print(f"[DataCollector] Restoring symbol {c} — status flipped "
              f"{prev}→TRADING; re-added to stream universe")
        try:
            import trade_engine as _te_rs
            _te_rs.log_diag_issue(
                "data", "info",
                f"{c} restored to market streams (was {prev}, now TRADING)")
        except Exception:
            pass
    return kept


def get_excluded_symbols() -> Dict[str, str]:
    """H3: public {symbol: status} map of symbols dropped from the stream
    universe (delisted/renamed/non-TRADING). The control_api health layer MUST
    subtract these from the engine's stale-signal count and from any coverage/
    uncovered accounting that is computed over the RAW approved list — they are
    intentionally not covered, so they must not turn health yellow.

    Populated lazily by _filter_tradeable during watchlist loads; call
    _load_persisted_watchlist() first if you need it warm on a cold process."""
    with _excluded_lock:
        return dict(_excluded_symbols)


def _bookticker_universe_enabled() -> bool:
    """G1a: strategy.json entries.bookticker_universe (default False). When
    True, @bookTicker is streamed for ALL covered symbols (not just held) so
    entry pricing can read streamed book quotes; when False, only held symbols
    get @bookTicker (dedicated exit feed) and the engine's on-demand REST
    covers entry pricing. Defensive read — any error → default False."""
    try:
        with open(config.STRATEGY_FILE) as f:
            s = json.load(f)
        ent = s.get("entries")
        if isinstance(ent, dict):
            return bool(ent.get("bookticker_universe", False))
    except Exception:
        pass
    return False


def _load_persisted_watchlist() -> list:
    """Symbols the WS feed should stream: the user's approved coins from
    strategy.json (written by /api/coins), union any symbols with open
    positions (held coins must keep streaming prices for the sell path),
    falling back to config.WATCHED_COINS when nothing is persisted.

    G2: the approved/base universe is filtered through exchange_info's
    tradeable set (fail-open) so delisted/renamed symbols never enter the
    stream list. Held-position symbols are unioned AFTER the filter so an open
    position always keeps its price feed even if the symbol is non-TRADING."""
    coins: list = []
    try:
        with open(config.STRATEGY_FILE) as f:
            s = json.load(f)
        coins = [
            str(c.get("symbol", "")).upper()
            for c in s.get("approved_coins", [])
            if c.get("approved") and str(c.get("symbol", "")).upper().endswith("USDT")
        ]
    except Exception:
        coins = []
    if not coins:
        coins = list(config.WATCHED_COINS)

    # G2: drop delisted/renamed (non-TRADING) symbols before anything else.
    coins = _filter_tradeable(coins)

    # Union with open-position symbols (guarded — trade_engine may not be
    # importable yet during early startup).
    try:
        import trade_engine as _te
        with _te._positions_lock:
            pos_syms = [str(p.get("symbol", "")).upper() for p in _te._positions]
        for sym in pos_syms:
            if sym and sym not in coins:
                coins.append(sym)
    except Exception:
        pass

    # De-dupe preserving order
    seen: set = set()
    out: list = []
    for c in coins:
        if c and c not in seen:
            seen.add(c)
            out.append(c)
    return out


# ── Binance limits gate (§6.4) — guarded no-op fallbacks ─────────────────────

class _RestDeferred(Exception):
    """Raised when the limits gate stayed closed for the whole polite-wait
    window — the caller should skip this fetch and try again later."""


class _RestLimited(Exception):
    """Raised on HTTP 429/418 after the event has been reported to
    binance_limits. Callers must NOT retry through this."""

    def __init__(self, code: int):
        self.code = code
        super().__init__(f"Binance rate limit response HTTP {code}")


_W_KLINES  = getattr(binance_limits, "WEIGHT_KLINES", 2) if binance_limits else 2
_W_EXINFO  = getattr(binance_limits, "WEIGHT_EXCHANGE_INFO", 20) if binance_limits else 20


def _limits_acquire(weight: int, critical: bool = False, defer_sec: float = 0.0) -> None:
    """Politely wait (≤ defer_sec) for the limits gate to open, recording the
    spend when it does. Raises _RestDeferred if the window closes on us.
    No-op when binance_limits is absent (old deploys)."""
    if binance_limits is None:
        return
    deadline = time.time() + max(0.0, defer_sec)
    while True:
        try:
            if binance_limits.spend(weight, critical):
                return
        except Exception:
            return  # never let a limits-module bug block data collection
        if time.time() >= deadline:
            raise _RestDeferred(f"limits gate closed (weight {weight})")
        time.sleep(0.5)


def _limits_record(headers) -> None:
    if binance_limits is None:
        return
    try:
        binance_limits.record_response_headers(headers)
    except Exception:
        pass


def _limits_on_429(retry_after) -> None:
    if binance_limits is None:
        return
    try:
        binance_limits.on_429(retry_after)
    except Exception:
        pass


def _limits_on_418(retry_after) -> None:
    if binance_limits is None:
        return
    try:
        binance_limits.on_418(retry_after)
    except Exception:
        pass


def _retry_after_sec(headers) -> Optional[float]:
    try:
        v = headers.get("Retry-After") if headers is not None else None
        return float(v) if v is not None else None
    except Exception:
        return None


def _urlopen_json(url: str, timeout: float = 15):
    """One HTTP GET with full limits telemetry: response headers are recorded
    on success AND on error; 429/418 are reported and re-raised as
    _RestLimited so callers never hammer through back-pressure."""
    req = urllib.request.Request(url, headers={"User-Agent": "WolfBot/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            _limits_record(getattr(resp, "headers", None))
            return json.loads(resp.read())
    except urllib.error.HTTPError as e:
        _limits_record(getattr(e, "headers", None))
        if e.code == 429:
            _limits_on_429(_retry_after_sec(getattr(e, "headers", None)))
            raise _RestLimited(429) from e
        if e.code == 418:
            _limits_on_418(_retry_after_sec(getattr(e, "headers", None)))
            raise _RestLimited(418) from e
        raise


# ── REST historical download ────────────────────────────────────────────

_BINANCE_BASES = [
    # Cloudflare CDN — often not geo-blocked even when api.binance.com is
    "https://data-api.binance.vision",
    "https://api.binance.com",
    "https://api1.binance.com",
    "https://api2.binance.com",
    "https://api3.binance.com",
    "https://api4.binance.com",
]


def _fetch_klines_rest(symbol: str, interval: str, limit: int = 500,
                       start_ms: Optional[int] = None, end_ms: Optional[int] = None,
                       critical: bool = False, defer_sec: float = 10.0):
    """Try each Binance base URL in order; return first successful response.
    Rate-limit gated (weight 2); optional startTime/endTime for gap repair.
    On 429/418 the base-fallback loop ABORTS immediately (shared IP budget)."""
    _limits_acquire(_W_KLINES, critical=critical, defer_sec=defer_sec)
    qs = f"symbol={symbol}&interval={interval}&limit={int(limit)}"
    if start_ms is not None:
        qs += f"&startTime={int(start_ms)}"
    if end_ms is not None:
        qs += f"&endTime={int(end_ms)}"
    last_err = None
    for base in _BINANCE_BASES:
        url = f"{base}/api/v3/klines?{qs}"
        try:
            return _urlopen_json(url, timeout=15)
        except _RestLimited:
            raise  # 429/418 already reported — never try the other bases
        except Exception as e:
            last_err = e
    raise last_err


def fetch_5m_candles(symbol: str, limit: int = 30) -> list:
    """Fetch 5-minute candles for multi-timeframe confirmation.
    Prefers the WebSocket buffer (ws_candles_5m) to avoid REST calls.
    Falls back to REST only when the buffer has fewer than _MIN_CANDLES_5M candles.
    Returns a list of dicts with open/high/low/close/volume keys.
    """
    buf = ws_candles_5m.get(symbol, [])
    if len(buf) >= _MIN_CANDLES_5M:
        return [
            {
                "open_time": int(k[0]),
                "open":      float(k[1]),
                "high":      float(k[2]),
                "low":       float(k[3]),
                "close":     float(k[4]),
                "volume":    float(k[5]),
            }
            for k in buf[-limit:]
        ]

    try:
        raw = _fetch_klines_rest(symbol, "5m", limit=limit, defer_sec=2.0)
        return [
            {
                "open_time": int(k[0]),
                "open":      float(k[1]),
                "high":      float(k[2]),
                "low":       float(k[3]),
                "close":     float(k[4]),
                "volume":    float(k[5]),
            }
            for k in raw
        ]
    except Exception:
        return []


def fetch_15m_candles(symbol: str, limit: int = 60) -> list:
    """I1.3 — 15-minute candles for T2_ema50_15m_slope (EMA50 needs >=50).
    Prefers the WebSocket/backfill buffer (ws_candles_15m) to avoid REST calls;
    falls back to REST only when the buffer has fewer than _MIN_CANDLES_15M
    candles. Returns a list of dicts with open/high/low/close/volume keys —
    same shape as fetch_5m_candles so trade_engine can read either uniformly.

    trade_engine's T2 should read this (or the raw ws_candles_15m buffer)
    instead of aggregating the 60×5m buffer (which only yields ~20 15m candles,
    below the 50 EMA50 needs)."""
    buf = ws_candles_15m.get(symbol, [])
    if len(buf) >= _MIN_CANDLES_15M:
        return [
            {
                "open_time": int(k[0]),
                "open":      float(k[1]),
                "high":      float(k[2]),
                "low":       float(k[3]),
                "close":     float(k[4]),
                "volume":    float(k[5]),
            }
            for k in buf[-limit:]
        ]

    try:
        raw = _fetch_klines_rest(symbol, "15m", limit=limit, defer_sec=2.0)
        return [
            {
                "open_time": int(k[0]),
                "open":      float(k[1]),
                "high":      float(k[2]),
                "low":       float(k[3]),
                "close":     float(k[4]),
                "volume":    float(k[5]),
            }
            for k in raw
        ]
    except Exception:
        return []


def backfill_ready(symbol: str) -> bool:
    """I1.4 — True once `symbol` has enough buffered history for the HARD entry
    readiness gate: >=_MIN_CANDLES (16) 1m candles for RSI AND >=_MIN_CANDLES_5M
    (21) 5m candles for the 5m veto. trade_engine should EXCLUDE not-ready
    symbols from ENTRY evaluation (not score them zero).

    15m is deliberately NOT part of this gate: T2_ema50_15m_slope is only
    SCORED, so a symbol missing 15m history means "T2 can't contribute", not
    "symbol unready". Use fetch_15m_candles()/ws_candles_15m separately to know
    whether T2 has data."""
    return (len(ws_candles.get(symbol, [])) >= _MIN_CANDLES
            and len(ws_candles_5m.get(symbol, [])) >= _MIN_CANDLES_5M)


def _compute_and_save(symbol: str, raw_klines: list, save_all: bool = False):
    """Compute indicators and persist candles.

    save_all=True:  used by the bulk REST history loader — every row is new
    save_all=False (default): used by the WebSocket close handler, which passes
        50 historical DB rows + 1 new row. Only the new row needs to be
        persisted; the older 50 are already in the DB and re-saving them holds
        the global SQLite lock for ~50 ms per coin per minute, which serialised
        the sell monitor and other DB consumers."""
    if not raw_klines:
        return

    parsed = []
    for k in raw_klines:
        parsed.append({
            "open_time": int(k[0]),
            "open":      float(k[1]),
            "high":      float(k[2]),
            "low":       float(k[3]),
            "close":     float(k[4]),
            "volume":    float(k[5]),
        })

    closes  = [c["close"]  for c in parsed]
    volumes = [c["volume"] for c in parsed]

    ma20_list    = indicators.calc_ma(closes, period=20)
    rsi14_list   = indicators.calc_rsi(closes, period=14)
    bb_u, bb_m, bb_l = indicators.calc_bollinger(closes)
    vol_ma_list  = indicators.calc_volume_ma(volumes, period=20)

    last_idx = len(parsed) - 1
    for i, row in enumerate(parsed):
        price  = row["close"]
        ma20   = ma20_list[i]
        bb_upper = bb_u[i]
        bb_mid   = bb_m[i]
        bb_lower = bb_l[i]
        vol_ma   = vol_ma_list[i]

        row["ma20"]       = ma20
        row["rsi14"]      = rsi14_list[i]
        row["bb_upper"]   = bb_upper
        row["bb_mid"]     = bb_mid
        row["bb_lower"]   = bb_lower
        row["volume_ma20"] = vol_ma
        row["ma_position"]  = indicators.classify_ma_position(price, ma20)
        row["bb_position"]  = indicators.classify_bb_position(price, bb_upper, bb_mid, bb_lower)
        row["volume_trend"] = indicators.classify_volume_trend(volumes[:i+1]) if i >= 6 else "flat"

        # Skip writes for the historical rows — they're already in the DB.
        if save_all or i == last_idx:
            database.save_candle(symbol, config.CANDLE_TIMEFRAME, row)


_bootstrap_5m_done = False
_bootstrap_5m_lock = threading.Lock()


def _bootstrap_5m_from_db():
    """Cold-start seed for ws_candles_5m: synthesize 5m candles from the 1m
    candles already persisted in SQLite. Without this, ws_candles_5m starts
    empty on every restart and the 5m veto blocks all buys for ~105 minutes
    (21 × 5m) until enough live WS candles accumulate.

    Each aligned bucket of 5 consecutive 1m candles becomes one 5m candle:
    o = first open, h = max high, l = min low, c = last close, v = sum volume.
    Only complete, fully-elapsed buckets are used; the most recent
    _WS_5M_CANDLE_MAX per coin are kept. Runs once per process.

    This stays the FIRST (free) source; the REST startup backfill fills the
    remainder up to the buffer targets afterwards."""
    global _bootstrap_5m_done
    with _bootstrap_5m_lock:
        if _bootstrap_5m_done:
            return
        _bootstrap_5m_done = True

    if config.CANDLE_TIMEFRAME != "1m":
        return  # can only synthesize 5m candles from 1m history

    seeded = 0
    total = 0
    now_ms = int(time.time() * 1000)
    try:
        coins = _load_persisted_watchlist()
    except Exception:
        coins = list(config.WATCHED_COINS)

    for sym in coins:
        try:
            if len(ws_candles_5m.get(sym, [])) >= _MIN_CANDLES_5M:
                continue  # live buffer already usable — don't touch it
            rows = database.get_candles(sym, "1m", limit=5 * (_WS_5M_CANDLE_MAX + 2))
            if len(rows) < 5:
                continue

            buckets: Dict[int, list] = {}
            for r in rows:
                try:
                    ot = int(r["open_time"])
                except Exception:
                    continue
                buckets.setdefault(ot - (ot % 300_000), []).append(r)

            synth = []
            for start in sorted(buckets):
                if start + 300_000 > now_ms:
                    continue  # 5m window not fully elapsed — candle not closed
                group = sorted(buckets[start], key=lambda g: int(g["open_time"]))
                if len(group) != 5:
                    continue  # gap in 1m history — skip incomplete bucket
                if [int(g["open_time"]) for g in group] != [start + i * 60_000 for i in range(5)]:
                    continue  # misaligned rows
                synth.append([
                    start,
                    float(group[0]["open"]),
                    max(float(g["high"]) for g in group),
                    min(float(g["low"]) for g in group),
                    float(group[-1]["close"]),
                    sum(float(g["volume"]) for g in group),
                ])
            if not synth:
                continue

            # Merge with anything the live WS already collected (WS entries win)
            with _buffers_lock:
                buf = ws_candles_5m.setdefault(sym, [])
                have = {int(k[0]) for k in buf}
                merged = [k for k in synth if int(k[0]) not in have] + list(buf)
                merged.sort(key=lambda k: int(k[0]))
                ws_candles_5m[sym] = merged[-_WS_5M_CANDLE_MAX:]
                total += len(ws_candles_5m[sym])
            seeded += 1
        except Exception as e:
            print(f"[DataCollector] 5m bootstrap error ({sym}): {e}")

    if seeded:
        print(f"[DataCollector] 5m bootstrap: seeded {seeded} coins "
              f"({total} synthesized 5m candles) from stored 1m history")
    else:
        print("[DataCollector] 5m bootstrap: no usable 1m history — 5m buffers start empty")


def download_history():
    """Download 1000 hourly candles per coin on first run."""
    # Seed 5m buffers from stored 1m candles first — this must run even when
    # the candle table is already populated (i.e. after every restart).
    try:
        _bootstrap_5m_from_db()
    except Exception as e:
        print(f"[DataCollector] 5m bootstrap failed: {e}")

    if not database.candles_table_empty():
        print("[DataCollector] Candle history already loaded — skipping download.")
        return

    print(f"[DataCollector] Downloading {config.CANDLE_LOOKBACK * 20} candles per coin…")
    for coin in _load_persisted_watchlist():
        try:
            raw = _fetch_klines_rest(coin, config.CANDLE_TIMEFRAME, limit=1000, defer_sec=30.0)
            _compute_and_save(coin, raw, save_all=True)
            _kline_store_save(coin, config.CANDLE_TIMEFRAME, raw)  # Phase 1 store
            print(f"  {coin}: {len(raw)} candles saved")
            time.sleep(0.3)  # be polite to Binance rate limits
        except Exception as e:
            print(f"  {coin}: download failed — {e}")
    print("[DataCollector] History download complete.")


# ── Buffer merge / gap repair / backfill (§6.2, §6.4) ────────────────────────

def _buffer_meta(interval: str) -> Tuple[Dict[str, list], int, int]:
    """(buffer dict, interval ms, max length) for an interval string."""
    if interval == "5m":
        return ws_candles_5m, 300_000, _WS_5M_CANDLE_MAX
    if interval == "15m":
        return ws_candles_15m, 900_000, _WS_15M_CANDLE_MAX
    return ws_candles, 60_000, _WS_CANDLE_MAX


def _append_closed_candle(sym: str, interval: str, closed: list):
    """Append one WS closed candle to its buffer, detecting gaps.
    Returns (appended, gap, snapshot):
      appended — False when the candle is a duplicate/out-of-order
                 (reconnect-overlap re-delivery); buffer untouched.
      gap      — (start_ms, end_ms) of the MISSING range when
                 open_time != last.open_time + interval_ms, else None.
      snapshot — copy of the buffer after the operation (signal source)."""
    buf_map, step, maxlen = _buffer_meta(interval)
    with _buffers_lock:
        buf = buf_map.setdefault(sym, [])
        last = int(buf[-1][0]) if buf else None
        ot = int(closed[0])
        if last is not None and ot <= last:
            return False, None, list(buf)
        gap = None
        if last is not None and ot != last + step:
            gap = (last + step, ot - 1)
        buf.append(closed)
        while len(buf) > maxlen:
            buf.pop(0)
        return True, gap, list(buf)


def _merge_candles(sym: str, interval: str, rest_rows: list) -> int:
    """Merge REST kline rows into the WS buffer without duplicates, keyed by
    open_time — existing (WS) entries win. Only fully-CLOSED candles are
    merged. The per-symbol list is REPLACED atomically for lock-free readers.
    Returns the number of candles added."""
    if not rest_rows:
        return 0
    buf_map, step, maxlen = _buffer_meta(interval)
    now_ms = int(time.time() * 1000)
    parsed = []
    for r in rest_rows:
        try:
            ot = int(r[0])
            # Row 6 is the candle close time when present (raw REST shape);
            # otherwise infer closure from open_time + interval.
            close_ms = int(r[6]) if len(r) > 6 else ot + step - 1
            if close_ms >= now_ms:
                continue  # in-progress candle — never buffer it
            parsed.append([ot, float(r[1]), float(r[2]), float(r[3]),
                           float(r[4]), float(r[5])])
        except Exception:
            continue
    if not parsed:
        return 0
    with _buffers_lock:
        buf = buf_map.setdefault(sym, [])
        have = {int(k[0]) for k in buf}
        add = [row for row in parsed if row[0] not in have]
        if not add:
            return 0
        merged = list(buf) + add
        merged.sort(key=lambda k: int(k[0]))
        buf_map[sym] = merged[-maxlen:]
        return len(add)


# ── Phase 1: durable kline store (database.klines table) ─────────────────────
# Every CLOSED 1m/5m kline (WS close events, backfill, gap repair) also flows
# into database.save_klines so backtests have a persistent history. WS-event
# rows are BUFFERED and flushed on an executor thread (~30 s or 50 rows per
# interval) — never on the WS event loop. All writes are guarded: if
# save_klines is missing (old database.py), everything degrades to a no-op.

_kline_store_lock = threading.Lock()
_kline_store_buf: Dict[str, list] = {}            # interval -> [(sym, row), ...]
_kline_store_last_flush: Dict[str, float] = {}    # interval -> last flush ts
_KLINE_FLUSH_SEC  = 30.0
_KLINE_FLUSH_ROWS = 50


def _kline_store_available() -> bool:
    return getattr(database, "save_klines", None) is not None


def _kline_store_buffer(sym: str, interval: str, row: list) -> bool:
    """Buffer one closed candle for the kline store. Cheap (list append under
    a lock) — safe to call from the WS event loop. Returns True when a flush
    is due; the CALLER must then schedule _kline_store_flush on an executor."""
    if not _kline_store_available():
        return False
    now = time.time()
    with _kline_store_lock:
        buf = _kline_store_buf.setdefault(interval, [])
        buf.append((sym, list(row)))
        last = _kline_store_last_flush.setdefault(interval, now)
        return len(buf) >= _KLINE_FLUSH_ROWS or now - last >= _KLINE_FLUSH_SEC


def _kline_store_flush(interval: str):
    """BLOCKING — executor/worker threads only. Drain the buffer for one
    interval and write it to the kline store, grouped per symbol."""
    fn = getattr(database, "save_klines", None)
    if fn is None:
        return
    with _kline_store_lock:
        rows = _kline_store_buf.pop(interval, [])
        _kline_store_last_flush[interval] = time.time()
    if not rows:
        return
    by_sym: Dict[str, list] = {}
    for sym, row in rows:
        by_sym.setdefault(sym, []).append(row)
    for sym, rws in by_sym.items():
        try:
            fn(sym, interval, rws)
        except Exception as e:
            print(f"[DataCollector] kline store flush failed ({sym} {interval}): {e}")


def _kline_store_save(sym: str, interval: str, raw_rows: list):
    """BLOCKING direct write for backfill / gap-repair fetches (those already
    run on executor threads). Skips in-progress (unclosed) candles."""
    fn = getattr(database, "save_klines", None)
    if fn is None or not raw_rows:
        return
    try:
        _, step, _ = _buffer_meta(interval)
        now_ms = int(time.time() * 1000)
        closed_rows = []
        for r in raw_rows:
            try:
                close_ms = int(r[6]) if len(r) > 6 else int(r[0]) + step - 1
                if close_ms < now_ms:
                    closed_rows.append(r)
            except Exception:
                continue
        if closed_rows:
            fn(sym, interval, closed_rows)
    except Exception as e:
        print(f"[DataCollector] kline store save failed ({sym} {interval}): {e}")


# Rolling operational counters (health panel)
_counters_lock = threading.Lock()
_gap_repairs: deque = deque()        # ts of each gap repair (24 h window)
_connect_attempts: deque = deque()   # ts of each WS connect attempt (5 min window)
_last_reconnect_alarm_ts = 0.0
_backfill_last_duration = 0.0
_manager_started_ts = 0.0

# I1.1 — startup-backfill completion telemetry (exposed via get_data_health()).
# _backfill_complete flips True after the first full startup sweep reaches its
# targets (or exhausts retries); _backfill_pct is the last sweep's
# symbols-filled ratio ×100. Read by the health panel so the UI can show the
# bot is "fully seeing" instead of guessing from buffer-fill percentages.
_backfill_complete = False
_backfill_pct = 0.0
_backfill_phase = "pending"   # pending → held → universe → complete
_backfill_last_stats: Dict = {}

_RECONNECT_ALARM_THRESHOLD = 30      # attempts per 5 min → ALARM (§6.4 item g)


def _record_gap_repair():
    now = time.time()
    with _counters_lock:
        _gap_repairs.append(now)
        while _gap_repairs and _gap_repairs[0] < now - 86_400:
            _gap_repairs.popleft()


def _record_connect_attempt():
    global _last_reconnect_alarm_ts
    now = time.time()
    with _counters_lock:
        _connect_attempts.append(now)
        while _connect_attempts and _connect_attempts[0] < now - 300:
            _connect_attempts.popleft()
        n = len(_connect_attempts)
        alarm = n > _RECONNECT_ALARM_THRESHOLD and now - _last_reconnect_alarm_ts > 60
        if alarm:
            _last_reconnect_alarm_ts = now
    if alarm:
        print(f"[DataCollector] ALARM: {n} WebSocket connection attempts in the "
              f"last 5 minutes — connectivity is flapping")
        try:
            import trade_engine as _te
            _te.log_diag_issue("websocket", "error",
                               f"ALARM: {n} WS connect attempts in 5 min (flapping)")
        except Exception:
            pass


# In-flight guard so one symbol/interval never runs concurrent repairs.
_repair_lock = threading.Lock()
_repair_inflight: set = set()


def _gap_repair_range(sym: str, interval: str, start_ms: int, end_ms: int,
                      reason: str = "kline-gap"):
    """Fetch ONLY the missing kline range for ONLY this symbol through the
    limits path and merge it into the buffer (WS entries win). Blocking —
    runs on an executor thread. Counted in gap_repairs_24h."""
    key = (sym, interval)
    with _repair_lock:
        if key in _repair_inflight:
            return
        _repair_inflight.add(key)
    try:
        _, step, maxlen = _buffer_meta(interval)
        n = int((end_ms - start_ms) // step) + 1
        n = max(1, min(n, maxlen, 1000))
        raw = _fetch_klines_rest(sym, interval, limit=n,
                                 start_ms=start_ms, end_ms=end_ms, defer_sec=15.0)
        added = _merge_candles(sym, interval, raw)
        _kline_store_save(sym, interval, raw)   # Phase 1: durable kline store
        _record_gap_repair()
        print(f"[DataCollector] Gap repair ({reason}) {sym} {interval}: "
              f"fetched {len(raw)}, merged {added} candle(s)")
        # Keep the SQLite candle history whole too (parity with the old
        # post-disconnect gap fill, which persisted refetched 1m rows).
        if interval == config.CANDLE_TIMEFRAME and raw:
            try:
                _compute_and_save(sym, raw, save_all=True)
            except Exception:
                pass
    except _RestDeferred:
        print(f"[DataCollector] Gap repair deferred for {sym} {interval} — limits gate closed")
    except Exception as e:
        print(f"[DataCollector] Gap repair failed for {sym} {interval}: {e}")
    finally:
        with _repair_lock:
            _repair_inflight.discard(key)


def _gap_repair_tail(symbols: list, reason: str):
    """After a reconnect (or make-before-break rotation): for every symbol
    whose buffer tail is ≥2 intervals old, fetch just the missing tail range.
    Blocking — runs on an executor thread."""
    for sym in symbols:
        for interval in (config.CANDLE_TIMEFRAME, "5m", "15m"):
            try:
                buf_map, step, _maxlen = _buffer_meta(interval)
                buf = buf_map.get(sym)
                if not buf:
                    continue  # empty buffers are the backfill's job
                last = int(buf[-1][0])
                now_ms = int(time.time() * 1000)
                if now_ms - last >= 2 * step:
                    _gap_repair_range(sym, interval, last + step, now_ms, reason=reason)
            except Exception as e:
                print(f"[DataCollector] Tail repair error ({sym} {interval}): {e}")


def _spawn_gap_repair(symbols, reason: str):
    """Schedule tail gap repair on the executor (call from the WS event loop)."""
    try:
        asyncio.get_running_loop().run_in_executor(
            None, _gap_repair_tail, sorted(symbols), reason)
    except Exception as e:
        print(f"[DataCollector] Could not schedule gap repair: {e}")


_BACKFILL_CONCURRENCY = 10
_backfill_running = threading.Event()
_bg_tasks: set = set()   # keep task refs so they aren't GC'd


def _backfill_symbol(sym: str) -> int:
    """Blocking single-symbol backfill through the rate-limited path.
    Fills the 1m buffer to 120, the 5m buffer to 60, and the 15m buffer to 60
    closed candles (I1.3 — 15m feeds T2_ema50_15m_slope)."""
    merged = 0
    try:
        if len(ws_candles.get(sym, [])) < _WS_CANDLE_MAX:
            raw = _fetch_klines_rest(sym, config.CANDLE_TIMEFRAME,
                                     limit=_WS_CANDLE_MAX, defer_sec=20.0)
            merged += _merge_candles(sym, config.CANDLE_TIMEFRAME, raw)
            _kline_store_save(sym, config.CANDLE_TIMEFRAME, raw)  # Phase 1 store
        if len(ws_candles_5m.get(sym, [])) < _WS_5M_CANDLE_MAX:
            raw = _fetch_klines_rest(sym, "5m", limit=_WS_5M_CANDLE_MAX, defer_sec=20.0)
            merged += _merge_candles(sym, "5m", raw)
            _kline_store_save(sym, "5m", raw)                     # Phase 1 store
        if len(ws_candles_15m.get(sym, [])) < _WS_15M_CANDLE_MAX:
            raw = _fetch_klines_rest(sym, "15m", limit=_WS_15M_CANDLE_MAX, defer_sec=20.0)
            merged += _merge_candles(sym, "15m", raw)
            _kline_store_save(sym, "15m", raw)                    # Phase 1 store
    except _RestDeferred:
        pass  # budget closed — the next backfill trigger will finish the job
    except Exception as e:
        print(f"[DataCollector] Backfill error ({sym}): {e}")
    return merged


def _avg_buffer_len(coins: list, buf_map: Dict[str, list]) -> float:
    """Mean candle count across `coins` for one buffer map (0.0 when empty)."""
    if not coins:
        return 0.0
    return round(sum(len(buf_map.get(c, [])) for c in coins) / len(coins), 1)


def _backfill_targets_met(coins: list) -> bool:
    """True once every symbol has reached the backfill_ready hard gate
    (1m>=16 AND 5m>=21). 15m is scored-only, so it isn't part of completion."""
    return all(backfill_ready(c) for c in coins)


async def _backfill_buffers(coins: list, reason: str, is_startup: bool = False):
    """Startup / new-coin REST backfill (§6.2 b + I1): fill 1m buffers to 120,
    5m buffers to 60, and 15m buffers to 60 closed candles, ≤10 symbols in
    flight, all through the binance_limits gate.

    I1.1 — HELD-position symbols are backfilled FIRST (priority ahead of the
    rest of the universe) so their sell monitors and exits see history within
    seconds of boot. The startup sweep RETRIES symbols that missed their target
    (rate-limit gate closed / transient fetch error) with backoff, and only
    then declares completion — flipping _backfill_complete and recording a
    clear completion line + get_data_health() telemetry."""
    global _backfill_last_duration, _backfill_complete, _backfill_pct
    global _backfill_phase, _backfill_last_stats
    coins = [c for c in coins if c]
    if not coins:
        return
    if _backfill_running.is_set():
        print("[DataCollector] Backfill already running — skipping duplicate sweep")
        return
    _backfill_running.set()
    if is_startup:
        _backfill_complete = False
        _backfill_phase = "universe"
    t0 = time.time()

    # I1.2 — held symbols first, then the rest of the universe (de-duped,
    # order-preserving). A held symbol always gets priority in the sweep queue.
    held = set(_held_symbols())
    ordered = [c for c in coins if c in held] + [c for c in coins if c not in held]

    async def sweep(pending: list) -> list:
        """One concurrent pass over `pending`; returns symbols still short of
        their 1m/5m targets afterward (retry candidates)."""
        sem = asyncio.Semaphore(_BACKFILL_CONCURRENCY)

        async def one(sym: str):
            async with sem:
                await asyncio.to_thread(_backfill_symbol, sym)

        await asyncio.gather(*(one(c) for c in pending), return_exceptions=True)
        return [c for c in pending if not backfill_ready(c)]

    # I1.1 — retry with backoff so a closed rate-limit gate / transient
    # exchange failure doesn't leave the bot half-blind (the bundle showed the
    # sweep never finishing). Startup gets more attempts than incremental adds.
    max_attempts = 4 if is_startup else 1
    pending = list(ordered)
    try:
        for attempt in range(1, max_attempts + 1):
            still = await sweep(pending)
            if not still or attempt == max_attempts:
                pending = still
                break
            delay = min(2.0 * attempt, 8.0)
            print(f"[DataCollector] Backfill ({reason}) attempt {attempt}: "
                  f"{len(still)} symbol(s) short of target — retrying in {delay:.0f}s")
            await asyncio.sleep(delay)
            pending = still
    finally:
        _backfill_running.clear()

    dur = time.time() - t0
    _backfill_last_duration = dur
    ready = [c for c in ordered if backfill_ready(c)]
    pct = round(100.0 * len(ready) / len(ordered), 1) if ordered else 0.0
    avg1 = _avg_buffer_len(ordered, ws_candles)
    avg5 = _avg_buffer_len(ordered, ws_candles_5m)
    avg15 = _avg_buffer_len(ordered, ws_candles_15m)
    _backfill_last_stats = {
        "reason": reason, "symbols": len(ordered), "ready": len(ready),
        "avg_1m": avg1, "avg_5m": avg5, "avg_15m": avg15,
        "duration_sec": round(dur, 1), "ts": time.time(),
    }
    if is_startup:
        _backfill_pct = pct
        _backfill_complete = True
        _backfill_phase = "complete"
    print(f"[DataCollector] backfill complete ({reason}): {len(ready)}/{len(ordered)} "
          f"symbols ready, 1m avg {avg1} candles, 5m avg {avg5}, 15m avg {avg15}, "
          f"took {dur:.1f}s ({pct:.0f}% ready)")
    if pending:
        print(f"[DataCollector] Backfill ({reason}): {len(pending)} symbol(s) still "
              f"short after {max_attempts} attempt(s): {', '.join(sorted(pending)[:20])}")


def _spawn_backfill(coins: list, reason: str, is_startup: bool = False):
    """Fire-and-forget backfill task on the running WS event loop."""
    try:
        task = asyncio.get_running_loop().create_task(
            _backfill_buffers(list(coins), reason, is_startup=is_startup))
        _bg_tasks.add(task)
        task.add_done_callback(_bg_tasks.discard)
    except Exception as e:
        print(f"[DataCollector] Could not schedule backfill: {e}")


def _batch_ticker_prices(symbols: list) -> Dict[str, float]:
    """Blocking batched /ticker/price fetch through the limits gate. Returns
    {symbol: price}. Used to seed held-position prices instantly at boot."""
    import urllib.parse as _up
    out: Dict[str, float] = {}
    syms = [s for s in symbols if s]
    if not syms:
        return out
    try:
        _limits_acquire(_W_KLINES * 2, critical=True, defer_sec=5.0)
    except _RestDeferred:
        pass  # spend anyway — held-symbol pricing is exit-critical
    if len(syms) == 1:
        qs = f"symbol={syms[0]}"
    else:
        qs = "symbols=" + _up.quote(json.dumps(syms, separators=(',', ':')), safe='')
    for base in _BINANCE_BASES:
        try:
            data = _urlopen_json(f"{base}/api/v3/ticker/price?{qs}", timeout=8)
            rows = data if isinstance(data, list) else [data]
            for r in rows:
                try:
                    px = float(r.get("price", 0) or 0)
                    s = r.get("symbol", "")
                    if s and px > 0:
                        out[s] = px
                except Exception:
                    continue
            if out:
                return out
        except _RestLimited:
            break
        except Exception:
            continue
    return out


def _batch_book_tickers(symbols: list) -> Dict[str, dict]:
    """Blocking batched /ticker/bookTicker fetch through the limits gate.
    Returns {symbol: {bid, bid_qty, ask, ask_qty}} — seeds book_ticker for
    held symbols so the sell path has best bid/ask within seconds of boot."""
    import urllib.parse as _up
    out: Dict[str, dict] = {}
    syms = [s for s in symbols if s]
    if not syms:
        return out
    try:
        _limits_acquire(_W_KLINES * 2, critical=True, defer_sec=5.0)
    except _RestDeferred:
        pass
    if len(syms) == 1:
        qs = f"symbol={syms[0]}"
    else:
        qs = "symbols=" + _up.quote(json.dumps(syms, separators=(',', ':')), safe='')
    for base in _BINANCE_BASES:
        try:
            data = _urlopen_json(f"{base}/api/v3/ticker/bookTicker?{qs}", timeout=8)
            rows = data if isinstance(data, list) else [data]
            for r in rows:
                try:
                    s = r.get("symbol", "")
                    if not s:
                        continue
                    out[s] = {
                        "bid":     float(r.get("bidPrice", 0) or 0),
                        "bid_qty": float(r.get("bidQty", 0) or 0),
                        "ask":     float(r.get("askPrice", 0) or 0),
                        "ask_qty": float(r.get("askQty", 0) or 0),
                    }
                except Exception:
                    continue
            if out:
                return out
        except _RestLimited:
            break
        except Exception:
            continue
    return out


def _seed_held_prices() -> int:
    """I1.2 — immediately seed prices/book_ticker/last_price_ts for every
    HELD-position symbol at boot (before the WS feed warms up) so their sell
    monitors have data within seconds instead of being briefly unprotected.
    Blocking — call via asyncio.to_thread from the WS loop. Returns the number
    of held symbols seeded."""
    held = _held_symbols()
    if not held:
        return 0
    now = time.time()
    px_map = _batch_ticker_prices(held)
    bt_map = _batch_book_tickers(held)
    seeded = 0
    for sym in held:
        px = px_map.get(sym)
        bt = bt_map.get(sym)
        # Fall back to bookTicker mid when /ticker/price missed the symbol.
        if (not px or px <= 0) and bt and bt.get("bid") and bt.get("ask"):
            px = (bt["bid"] + bt["ask"]) / 2.0
        if px and px > 0:
            prices[sym] = px
            last_price_ts[sym] = now
            try:
                client.update_price(sym, px)
            except Exception:
                pass
            try:
                import trade_engine as _te_seed
                _te_seed._last_ws_price_ts[sym] = now
            except Exception:
                pass
            seeded += 1
        if bt:
            bt2 = dict(bt)
            bt2["ts"] = now
            book_ticker[sym] = bt2
            last_price_ts[sym] = now
    if seeded:
        print(f"[DataCollector] Seeded fresh boot prices for {seeded}/{len(held)} "
              f"held symbol(s): {', '.join(sorted(held))}")
    return seeded


# ── Live WebSocket ────────────────────────────────────────────────────────────

# Market-data WS host: data-stream.binance.vision (geo-block workaround —
# same reason REST uses data-api.binance.vision). Do NOT change this host.
_WS_BASE = "wss://data-stream.binance.vision"

# §6.4: cap streams per connection well below Binance's 1024 combined-stream
# limit; shard the coin list across connections instead.
_MAX_STREAMS_PER_CONN = 300

# Streams every watched coin gets on the sharded market connections.
# I1.3 — @kline_15m added so T2_ema50_15m_slope stays fresh after the startup
# 15m backfill (adds 1 stream/symbol → 4 suffixes → 75 coins/shard under the
# 300-stream cap; the shard splitter absorbs any overflow into a 2nd conn).
_MARKET_SUFFIXES = (f"kline_{config.CANDLE_TIMEFRAME}", "kline_5m", "kline_15m", "miniTicker")

# §6.4 control-message pacing: live SUBSCRIBE/UNSUBSCRIBE frames are used for
# small diffs; larger diffs fall back to the proven close-and-reconnect path.
_LIVE_DIFF_MAX_STREAMS = 20
_SUBS_DEBOUNCE_SEC = 5.0          # watchlist diffs applied at most once per 5 s
_CONTROL_SEND_MIN_INTERVAL = 0.3  # ≥300 ms between outgoing frames → ≤3.3 msg/s,
                                  # comfortably under Binance's 5 msg/s cap even
                                  # counting the pong frames the websockets lib
                                  # sends automatically for server pings.
_CONTROL_BATCH_MAX = 50           # stream names per SUBSCRIBE/UNSUBSCRIBE payload

_ROTATE_AFTER_SEC = 23 * 3600     # make-before-break before Binance's 24 h drop
_ROTATE_CONFIRM_TIMEOUT = 45.0    # replacement must produce a message in this window


def _market_suffixes() -> tuple:
    """Per-coin market-shard streams. G1a: when entries.bookticker_universe is
    ON, every covered symbol also gets @bookTicker (+1 stream/symbol) so entry
    pricing can use streamed book quotes instead of on-demand REST."""
    if _bookticker_universe_enabled():
        return _MARKET_SUFFIXES + ("bookTicker",)
    return _MARKET_SUFFIXES


def _market_streams_for(coins: list) -> list:
    return [f"{c.lower()}@{sfx}" for c in coins for sfx in _market_suffixes()]


def _stream_symbol(stream: str) -> str:
    return stream.split("@", 1)[0].upper()


def _shard_coins(coins: list) -> List[list]:
    """Split the coin list into shards whose stream count fits one connection."""
    per_shard = max(1, _MAX_STREAMS_PER_CONN // max(1, len(_market_suffixes())))
    return [coins[i:i + per_shard] for i in range(0, len(coins), per_shard)]


def _build_ws_url(streams: list) -> str:
    """Combined-stream URL for one connection's stream list."""
    return f"{_WS_BASE}/stream?streams={'/'.join(streams)}"


# Cache exchangeInfo verification so rapid resubscribes/reconnects don't
# hammer the endpoint; keyed by the exact coin set (a watchlist change busts
# the key), TTL 24 h per the exchangeInfo caching guidance (weight 20).
_verify_cache: Dict = {"key": None, "ok": None, "ts": 0.0}
_VERIFY_TTL_SEC = 86_400


async def _verify_symbols(coins: list) -> list:
    """Return only coins that Binance confirms exist as USDT pairs."""
    key = frozenset(coins)
    if _verify_cache["key"] == key and time.time() - _verify_cache["ts"] < _VERIFY_TTL_SEC:
        return list(_verify_cache["ok"])
    try:
        def _fetch():
            _limits_acquire(_W_EXINFO, critical=False, defer_sec=10.0)
            url = "https://data-api.binance.vision/api/v3/exchangeInfo?permissions=SPOT"
            return _urlopen_json(url, timeout=15)
        data = await asyncio.to_thread(_fetch)
        valid = {s["symbol"] for s in data.get("symbols", []) if s["status"] == "TRADING"}
        ok    = [c for c in coins if c in valid]
        bad   = [c for c in coins if c not in valid]
        if bad:
            print(f"[DataCollector] Dropping invalid symbols: {bad}")
        _verify_cache["key"] = key
        _verify_cache["ok"]  = list(ok)
        _verify_cache["ts"]  = time.time()
        return ok
    except Exception as e:
        print(f"[DataCollector] Symbol verification failed ({e}) — using full coin list")
        return coins


def _persist_and_signal(sym: str, closed: list, buf_snapshot: list):
    """DB read + indicator recompute + save + signal callback for one closed
    1m candle. Runs on a worker thread (run_in_executor) so the blocking
    SQLite work never stalls the WebSocket event loop — all watched coins
    close their candle in the same second, and inline processing froze
    @trade ticks (and the realtime sell path) for the whole burst."""
    try:
        # ── Persist to DB (best-effort, not required)
        existing = database.get_candles(sym, config.CANDLE_TIMEFRAME, limit=50)
        db_raw = [
            [row["open_time"], row["open"], row["high"],
             row["low"], row["close"], row["volume"]]
            for row in existing
        ]
        all_raw = db_raw + [closed]
        _compute_and_save(sym, all_raw)

        # ── Signal update: prefer DB+new, fall back to WS buffer
        signal_src = all_raw if len(all_raw) >= _MIN_CANDLES else buf_snapshot
        if _kline_callback and len(signal_src) >= _MIN_CANDLES:
            closes  = [float(r[4]) for r in signal_src]
            volumes = [float(r[5]) for r in signal_src]
            _kline_callback(sym, closes, volumes)
    except Exception as e:
        print(f"[DataCollector] Candle persist/signal error ({sym}): {e}")


def _record_sample_and_ticks(symbol: str, price: float, now_ts: float):
    """Shared tick/sample bookkeeping for @trade and @miniTicker events.
    (Non-held coins no longer stream @trade — the miniTicker 1 s roll-up
    feeds price_ticks/price_samples for them instead.)"""
    ticks = price_ticks.setdefault(symbol, deque(maxlen=_TICK_MAX))
    ticks.append(price)
    if now_ts - _price_sample_ts.get(symbol, 0) >= _SAMPLE_INTERVAL:
        _price_sample_ts[symbol] = now_ts
        buf = price_samples.setdefault(symbol, [])
        buf.append(price)
        if len(buf) > _SAMPLE_MAX:
            buf.pop(0)


def _handle_ws_message(raw: str):
    """Shared message handler for ALL WS connections (market shards + the
    dedicated held-symbol exit feed). Must only be called from the WS loop."""
    msg = json.loads(raw)
    if "result" in msg and "id" in msg and "data" not in msg:
        return  # SUBSCRIBE/UNSUBSCRIBE ack — nothing to do
    stream = str(msg.get("stream", ""))
    data = msg.get("data", msg)
    evt = data.get("e")
    now_ts = time.time()

    if evt == "trade":
        symbol = data["s"]
        price  = float(data["p"])
        prices[symbol] = price
        last_price_ts[symbol] = now_ts
        _record_sample_and_ticks(symbol, price, now_ts)
        client.update_price(symbol, price)
        try:
            import trade_engine as _te_tr
            _te_tr._last_ws_price_ts[symbol] = now_ts
        except Exception:
            pass
        if _price_callback:
            _price_callback(dict(prices))

    elif evt == "kline":
        k = data["k"]
        if k.get("x"):   # candle closed
            sym      = k["s"]
            interval = k.get("i", "1m")
            closed = [
                int(k["t"]),   float(k["o"]), float(k["h"]),
                float(k["l"]), float(k["c"]), float(k["v"]),
            ]
            appended, gap, snapshot = _append_closed_candle(sym, interval, closed)
            if appended:
                # Phase 1: persist every closed 1m/5m kline to the durable
                # kline store. Buffering is O(1) here; the actual SQLite
                # write is flushed on an executor thread (~30 s / 50 rows).
                try:
                    if _kline_store_buffer(sym, interval, closed):
                        asyncio.get_running_loop().run_in_executor(
                            None, _kline_store_flush, interval)
                except Exception:
                    pass
            if gap is not None:
                # §6.2 c: fetch ONLY the missing range for ONLY this symbol.
                asyncio.get_running_loop().run_in_executor(
                    None, _gap_repair_range, sym, interval, gap[0], gap[1], "kline-gap")
            if interval == config.CANDLE_TIMEFRAME and appended:
                # ── Persist + signal on a worker thread so the blocking
                # SQLite work never stalls this loop. (Duplicates from
                # reconnect overlap are skipped — already persisted.)
                # Only the 1m stream drives the signal recompute; 5m has its
                # own path below and 15m only feeds the ws_candles_15m buffer
                # (buffered above) for T2 — no per-close callback needed.
                asyncio.get_running_loop().run_in_executor(
                    None, _persist_and_signal, sym, closed, snapshot)
            elif interval == "5m" and appended:
                # ── Phase 3 §3.1: entry-signal evaluation happens on 5m
                # closes. Same executor-offload pattern as the 1m path —
                # the callback does DB reads + indicator math and must
                # never run on this event loop.
                asyncio.get_running_loop().run_in_executor(
                    None, _dispatch_kline5m, sym, closed, snapshot)

    elif evt == "24hrMiniTicker":
        symbol = data["s"]
        close_price = float(data["c"])
        # miniTicker arrives every ~1s per coin — it is the price feed for all
        # non-held coins (held coins additionally stream @trade on the
        # dedicated exit connection).
        prices[symbol] = close_price
        last_price_ts[symbol] = now_ts
        _record_sample_and_ticks(symbol, close_price, now_ts)
        client.update_price(symbol, close_price)
        try:
            import trade_engine as _te_mt
            _te_mt._last_ws_price_ts[symbol] = now_ts
            # For HELD positions, evaluate sell triggers on this tick too —
            # low-trade-volume coins may only get miniTicker updates, and
            # waiting for another coin's @trade event (or the 0.25s fallback
            # loop) delays the exit.
            if _price_callback and symbol in _te_mt._pos_by_symbol:
                _price_callback(dict(prices))
        except Exception:
            pass

    elif stream.lower().endswith("@bookticker") or (
            evt is None and "b" in data and "a" in data and "s" in data):
        # @bookTicker payloads carry no "e" field — detect via stream name.
        symbol = data.get("s")
        if symbol:
            book_ticker[symbol] = {
                "bid":     float(data["b"]),
                "bid_qty": float(data["B"]),
                "ask":     float(data["a"]),
                "ask_qty": float(data["A"]),
                "ts":      now_ts,
            }
            last_price_ts[symbol] = now_ts
            try:
                import trade_engine as _te_bt
                _te_bt._last_ws_price_ts[symbol] = now_ts
            except Exception:
                pass


class _WSConnection:
    """One WebSocket connection (market shard or the held-symbol exit feed).
    Owns its reconnect loop with jittered exponential backoff (1 s → 60 s cap)
    and supports live SUBSCRIBE/UNSUBSCRIBE frames with paced, batched sends.
    All connections feed the same _handle_ws_message handler."""

    def __init__(self, conn_id: int, streams: list, kind: str = "market"):
        self.id = conn_id
        self.kind = kind
        self.streams: list = list(streams)
        self.ws = None
        self.connected_since: float = 0.0
        self.last_message_ts: float = 0.0
        self.first_message = asyncio.Event()
        self._stop = False
        self._task: Optional[asyncio.Task] = None
        self._send_lock = asyncio.Lock()
        self._last_send_ts = 0.0
        self._sub_id = 0
        self._sec_counts: deque = deque(maxlen=15)   # [epoch_sec, msg_count]
        self._rotate_retry_at = 0.0

    # ── Introspection ────────────────────────────────────────────────────
    def symbols(self) -> set:
        return {_stream_symbol(s) for s in self.streams}

    def _count_msg(self):
        now_s = int(time.time())
        if self._sec_counts and self._sec_counts[-1][0] == now_s:
            self._sec_counts[-1][1] += 1
        else:
            self._sec_counts.append([now_s, 1])

    def msgs_per_sec(self) -> float:
        now_s = int(time.time())
        total = sum(c for t, c in self._sec_counts if now_s - 10 <= t < now_s)
        return round(total / 10.0, 2)

    # ── Lifecycle ────────────────────────────────────────────────────────
    def start(self):
        self._task = asyncio.get_running_loop().create_task(self.run())

    async def stop(self):
        self._stop = True
        ws = self.ws
        if ws is not None:
            try:
                await ws.close()
            except Exception:
                pass
        t = self._task
        if t is not None and not t.done():
            try:
                await asyncio.wait_for(t, timeout=5.0)  # cancels t on timeout
            except Exception:
                pass

    async def force_reconnect(self):
        """Close the socket so run() reconnects with the (updated) stream
        list — the close-and-reconnect fallback for live-frame failures."""
        ws = self.ws
        if ws is not None:
            try:
                await ws.close()
            except Exception:
                pass

    # ── Live subscription management (§6.4 control-message pacing) ───────
    async def _send_control(self, method: str, streams: list):
        """Batched (≤50 names/payload) and paced (≥300 ms apart, ≤5 msg/s per
        connection incl. the auto-pongs websockets sends for server pings)."""
        ws = self.ws
        if ws is None:
            raise RuntimeError("not connected")
        async with self._send_lock:
            for i in range(0, len(streams), _CONTROL_BATCH_MAX):
                chunk = streams[i:i + _CONTROL_BATCH_MAX]
                wait = self._last_send_ts + _CONTROL_SEND_MIN_INTERVAL - time.time()
                if wait > 0:
                    await asyncio.sleep(wait)
                self._sub_id += 1
                await ws.send(json.dumps(
                    {"method": method, "params": list(chunk), "id": self._sub_id}))
                self._last_send_ts = time.time()

    async def add_streams(self, streams: list) -> bool:
        new = [s for s in streams if s not in self.streams]
        self.streams.extend(new)
        if not new or self.ws is None:
            return True  # offline: picked up on the next (re)connect
        try:
            await self._send_control("SUBSCRIBE", new)
            return True
        except Exception as e:
            print(f"[DataCollector] WS#{self.id} live SUBSCRIBE failed ({e}) — "
                  f"falling back to reconnect")
            await self.force_reconnect()
            return False

    async def remove_streams(self, streams: list) -> bool:
        gone = [s for s in streams if s in self.streams]
        if not gone:
            return True
        gone_set = set(gone)
        self.streams = [s for s in self.streams if s not in gone_set]
        if self.ws is None:
            return True
        try:
            await self._send_control("UNSUBSCRIBE", gone)
            return True
        except Exception as e:
            print(f"[DataCollector] WS#{self.id} live UNSUBSCRIBE failed ({e}) — "
                  f"falling back to reconnect")
            await self.force_reconnect()
            return False

    # ── Connection loop ──────────────────────────────────────────────────
    async def run(self):
        global last_ws_message_ts
        import websockets
        backoff = 1.0
        had_connection = False
        while not self._stop:
            if not self.streams:
                await asyncio.sleep(1.0)
                continue
            _record_connect_attempt()
            url = _build_ws_url(self.streams)
            try:
                async with websockets.connect(
                        url, ping_interval=20, ping_timeout=30, open_timeout=10) as ws:
                    self.ws = ws
                    self.connected_since = time.time()
                    backoff = 1.0
                    _ws_health["connect_count"]   += 1
                    _ws_health["last_connect_ts"]  = self.connected_since
                    print(f"[DataCollector] WS#{self.id} ({self.kind}) connected ✓ "
                          f"({len(self.streams)} streams)")
                    if had_connection:
                        # §6.2 c: gap repair for all of this shard's symbols
                        # after every reconnect.
                        _spawn_gap_repair(self.symbols(), f"ws#{self.id}-reconnect")
                        try:
                            import trade_engine as _te_dc
                            _te_dc.log_diag_issue(
                                "websocket", "info",
                                f"WS#{self.id} reconnected ({len(self.streams)} streams)")
                        except Exception:
                            pass
                    had_connection = True

                    async for raw in ws:
                        if self._stop:
                            break
                        self._count_msg()
                        self.last_message_ts = time.time()
                        if not self.first_message.is_set():
                            self.first_message.set()
                        _ws_health["messages_received"] += 1
                        _ws_health["last_message_ts"]    = self.last_message_ts
                        last_ws_message_ts               = self.last_message_ts
                        try:
                            _handle_ws_message(raw)
                        except Exception as e:
                            print(f"[DataCollector] Message error: {e}")

                # Clean close: watchlist resubscribe, rotation, or Binance's
                # scheduled 24 h disconnect (close frame) — reconnect quickly
                # with the current stream list; gap repair runs on reconnect.
                self.ws = None
                self.connected_since = 0.0
                _ws_health["last_disconnect_ts"] = time.time()
                if self._stop:
                    break
                print(f"[DataCollector] WS#{self.id} closed — reconnecting with "
                      f"{len(self.streams)} streams…")
                await asyncio.sleep(1.0)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.ws = None
                self.connected_since = 0.0
                _ws_health["disconnect_count"]   += 1
                _ws_health["last_disconnect_ts"]  = time.time()
                if self._stop:
                    break
                # §6.4 g: jittered exponential backoff, 1 s → 60 s cap.
                delay = min(backoff, 60.0) * random.uniform(0.7, 1.3)
                print(f"[DataCollector] WS#{self.id} disconnected: {e}")
                print(f"[DataCollector] WS#{self.id} reconnecting in {delay:.1f}s…")
                try:
                    import trade_engine as _te_dc
                    _te_dc.log_diag_issue(
                        "websocket", "warn",
                        f"WS#{self.id} disconnected, reconnect "
                        f"#{_ws_health['disconnect_count']}",
                        detail=f"{type(e).__name__}: {e} — backoff {delay:.1f}s")
                except Exception:
                    pass
                await asyncio.sleep(delay)
                backoff = min(backoff * 2.0, 60.0)
        self.ws = None
        self.connected_since = 0.0


# ── Connection manager state ─────────────────────────────────────────────────
_conn_seq = itertools.count(1)
_connections: List[_WSConnection] = []      # sharded market-data connections
_held_conn: Optional[_WSConnection] = None  # dedicated held-symbol exit feed
_current_coins: list = []                   # coins the market shards cover


def _update_aggregate_health():
    conns = list(_connections) + ([_held_conn] if _held_conn is not None else [])
    _ws_health["connected"] = any(c.ws is not None for c in conns)
    _ws_health["subscribed_coins"] = len({s for c in _connections for s in c.symbols()})


async def _rebuild_market_connections(coins: list):
    """Close-and-reconnect path: tear down all market shards and rebuild them
    for `coins`. Used at startup and as the fallback for large watchlist diffs."""
    for c in list(_connections):
        try:
            await c.stop()
        except Exception:
            pass
    del _connections[:]
    shards = _shard_coins(list(coins))
    for shard in shards:
        conn = _WSConnection(next(_conn_seq), _market_streams_for(shard))
        _connections.append(conn)
        conn.start()
    _current_coins[:] = list(coins)
    print(f"[DataCollector] Subscribing {len(coins)} coins across "
          f"{len(_connections)} WS connection(s) "
          f"(≤{_MAX_STREAMS_PER_CONN} streams each)")


async def _apply_watchlist_diff(new_coins: list):
    """§6.4 e: DIFF the desired coin set against current subscriptions.
    Small diffs (≤{_LIVE_DIFF_MAX_STREAMS} streams) are applied via live
    SUBSCRIBE/UNSUBSCRIBE frames; larger diffs fall back to the proven
    close-and-reconnect rebuild. Never resubscribe-all on a single toggle."""
    old = set(_current_coins)
    new = set(new_coins)
    added   = [c for c in new_coins if c not in old]
    removed = sorted(old - new)
    if not added and not removed:
        return
    _ws_health["resubscribe_count"] = _ws_health.get("resubscribe_count", 0) + 1
    diff_streams = len(_market_suffixes()) * (len(added) + len(removed))
    print(f"[DataCollector] Watchlist diff: +{len(added)}/-{len(removed)} coins "
          f"({diff_streams} streams)")

    if diff_streams > _LIVE_DIFF_MAX_STREAMS or not _connections:
        # Documented choice: large diffs (or a cold start) use full reconnect.
        await _rebuild_market_connections(list(new_coins))
        if added:
            _spawn_backfill(added, "watchlist-rebuild")
        return

    removed_set = set(removed)
    for conn in list(_connections):
        rem = [s for s in conn.streams if _stream_symbol(s) in removed_set]
        if rem:
            await conn.remove_streams(rem)
    for conn in [c for c in list(_connections) if not c.streams]:
        await conn.stop()
        _connections.remove(conn)

    for sym in added:
        streams = _market_streams_for([sym])
        target = None
        for conn in _connections:
            if len(conn.streams) + len(streams) <= _MAX_STREAMS_PER_CONN:
                target = conn
                break
        if target is None:
            conn = _WSConnection(next(_conn_seq), streams)
            _connections.append(conn)
            conn.start()
        else:
            await target.add_streams(streams)

    _current_coins[:] = list(new_coins)
    if added:
        _spawn_backfill(added, "watchlist-add")


# F5 — coverage reconciliation state. Symbols in the canonical watchlist that
# still had no active market stream on the PREVIOUS reconcile pass; a symbol
# uncovered on two consecutive passes is WARN-logged (persistently uncovered).
_reconcile_uncovered_prev: set = set()


async def _reconcile_coverage() -> None:
    """§F5 — every ~60 s, reconcile actually-subscribed symbols against the
    canonical watchlist (_load_persisted_watchlist) and auto-subscribe any
    that are missing (a silent SUBSCRIBE-frame drop or lost stream leaves a
    watchlist symbol uncovered even though _current_coins claims it). Symbols
    that stay uncovered across consecutive passes are WARN-logged by name."""
    global _reconcile_uncovered_prev
    try:
        watchlist = await asyncio.to_thread(_load_persisted_watchlist)
    except Exception:
        watchlist = list(_current_coins)
    covered = {s for c in _connections for s in c.symbols()}
    uncovered = [s for s in watchlist if s not in covered]
    if not uncovered:
        _reconcile_uncovered_prev = set()
        return
    # Auto-subscribe the uncovered symbols via the same add path the live
    # watchlist diff uses (verify first so delisted symbols are skipped).
    try:
        active = await _verify_symbols(uncovered)
    except Exception:
        active = []
    for sym in active:
        try:
            streams = _market_streams_for([sym])
            target = None
            for conn in _connections:
                if len(conn.streams) + len(streams) <= _MAX_STREAMS_PER_CONN:
                    target = conn
                    break
            if target is None:
                conn = _WSConnection(next(_conn_seq), streams)
                _connections.append(conn)
                conn.start()
            else:
                await target.add_streams(streams)
            if sym not in _current_coins:
                _current_coins.append(sym)
        except Exception as e:
            print(f"[DataCollector] Coverage re-subscribe failed for {sym}: {e}")
    if active:
        _ws_health["resubscribe_count"] = _ws_health.get("resubscribe_count", 0) + 1
        _spawn_backfill(list(active), "coverage-reconcile")
        print(f"[DataCollector] Coverage reconcile: re-subscribed "
              f"{len(active)}/{len(uncovered)} uncovered watchlist symbol(s)")
    # WARN on symbols that were uncovered on the PREVIOUS pass too — a transient
    # gap self-heals in one pass; a persistent one needs a human/diag flag.
    still = sorted(s for s in uncovered if s in _reconcile_uncovered_prev)
    if still:
        print(f"[DataCollector] WARN: {len(still)} watchlist symbol(s) remain "
              f"uncovered after reconcile: {', '.join(still)}")
        try:
            import trade_engine as _te_rc
            _te_rc.log_diag_issue(
                "data", "warn",
                f"{len(still)} watchlist symbol(s) uncovered by market streams",
                detail=", ".join(still))
        except Exception:
            pass
    _reconcile_uncovered_prev = set(uncovered)


def _held_symbols() -> list:
    """Symbols with open positions (guarded trade_engine access)."""
    try:
        import trade_engine as _te
        with _te._positions_lock:
            syms = {str(p.get("symbol", "")).upper() for p in _te._positions}
        return sorted(s for s in syms if s)
    except Exception:
        return []


async def _sync_held_connection():
    """§6.4 d: DEDICATED connection for held-symbol exit feeds (@trade +
    @bookTicker) so market chatter can't queue ahead of exit ticks. Rebuilt
    whenever the position set changes (notify_positions_changed() hook, with
    a 5 s poll as fallback)."""
    global _held_conn
    syms = await asyncio.to_thread(_held_symbols)
    desired = ([f"{s.lower()}@trade" for s in syms] +
               [f"{s.lower()}@bookTicker" for s in syms])
    desired_set = set(desired)

    if _held_conn is None:
        if not desired:
            return
        _held_conn = _WSConnection(next(_conn_seq), desired, kind="held")
        _held_conn.start()
        print(f"[DataCollector] Dedicated exit-feed connection started for "
              f"{len(syms)} held symbol(s)")
        return

    current = set(_held_conn.streams)
    if current == desired_set:
        return
    if not desired:
        await _held_conn.stop()
        _held_conn = None
        print("[DataCollector] No open positions — dedicated exit-feed connection closed")
        return

    add = [s for s in desired if s not in current]
    rem = [s for s in current if s not in desired_set]
    if len(add) + len(rem) <= _LIVE_DIFF_MAX_STREAMS and _held_conn.ws is not None:
        if rem:
            await _held_conn.remove_streams(rem)
        if add:
            await _held_conn.add_streams(add)
    else:
        _held_conn.streams = list(desired)
        await _held_conn.force_reconnect()
    print(f"[DataCollector] Exit-feed subscriptions updated "
          f"({len(syms)} held symbol(s))")


async def _rotate_connection(old: _WSConnection) -> _WSConnection:
    """§6.4 f: 23 h make-before-break. Open a replacement with the same
    streams, wait for its first message, close the old connection, then run
    gap repair for its symbols. On failure, keep the old one and retry later
    (Binance's own 24 h disconnect is handled by the normal reconnect path)."""
    print(f"[DataCollector] WS#{old.id} approaching Binance's 24h limit — "
          f"opening replacement (make-before-break)")
    repl = _WSConnection(next(_conn_seq), list(old.streams), kind=old.kind)
    repl.start()
    try:
        await asyncio.wait_for(repl.first_message.wait(), timeout=_ROTATE_CONFIRM_TIMEOUT)
    except (asyncio.TimeoutError, Exception):
        print(f"[DataCollector] Replacement WS#{repl.id} produced no messages — "
              f"keeping WS#{old.id}, will retry in 10 min")
        await repl.stop()
        old._rotate_retry_at = time.time() + 600
        return old
    await old.stop()
    print(f"[DataCollector] WS#{old.id} → WS#{repl.id} rotation complete")
    _spawn_gap_repair(repl.symbols(), f"ws#{repl.id}-rotation")
    return repl


async def _rotate_aged_connections():
    """Rotate at most ONE aged connection per invocation (spreads connect load)."""
    global _held_conn
    now = time.time()
    for i, conn in enumerate(list(_connections)):
        if (conn.connected_since and now - conn.connected_since >= _ROTATE_AFTER_SEC
                and now >= conn._rotate_retry_at):
            repl = await _rotate_connection(conn)
            if repl is not conn:
                try:
                    _connections[_connections.index(conn)] = repl
                except ValueError:
                    _connections.append(repl)
            return
    hc = _held_conn
    if (hc is not None and hc.connected_since
            and now - hc.connected_since >= _ROTATE_AFTER_SEC
            and now >= hc._rotate_retry_at):
        repl = await _rotate_connection(hc)
        if repl is not hc:
            _held_conn = repl


def _revive_dead_connections():
    """A connection task should never finish on its own — restart it if it does."""
    for conn in list(_connections) + ([_held_conn] if _held_conn is not None else []):
        t = conn._task
        if t is not None and t.done() and not conn._stop:
            try:
                exc = t.exception()
            except Exception:
                exc = None
            print(f"[DataCollector] FATAL: WS#{conn.id} task died "
                  f"({exc!r}) — restarting it")
            conn.start()


async def _start_websocket_loop():
    """
    WebSocket manager: builds sharded market connections (+ the dedicated
    held-symbol exit feed), REST-backfills the candle buffers, then supervises
    — watchlist diffs (debounced 5 s), held-position resyncs (5 s), 23 h
    make-before-break rotation, and dead-task revival.
    """
    global _manager_started_ts
    import websockets  # fail fast (and loudly, via the supervised runner) if missing
    _ = websockets
    _manager_started_ts = time.time()

    # Cold-start: seed 5m buffers from stored 1m candles (idempotent — also
    # attempted from download_history) so the 5m veto works right away. This
    # stays the first, free source; REST backfill below fills the rest.
    try:
        await asyncio.to_thread(_bootstrap_5m_from_db)
    except Exception as e:
        print(f"[DataCollector] 5m bootstrap failed: {e}")

    _reconnect_requested.clear()

    # I1.2 — seed fresh prices/book quotes for HELD symbols BEFORE anything
    # else so their sell monitors are never left unprotected at boot (the
    # H1-era gap). Blocking REST, but only for the (few) held symbols.
    try:
        await asyncio.to_thread(_seed_held_prices)
    except Exception as e:
        print(f"[DataCollector] Held-price boot seed error: {e}")

    try:
        desired = await asyncio.to_thread(_load_persisted_watchlist)
    except Exception:
        desired = list(config.WATCHED_COINS)
    active = await _verify_symbols(desired)
    await _rebuild_market_connections(active)
    # §6.2 b + I1 — fill 1m→120 / 5m→60 / 15m→60, held symbols first, retried
    # until the readiness targets are met; flips _backfill_complete when done.
    _spawn_backfill(list(active), "startup", is_startup=True)
    try:
        await _sync_held_connection()
    except Exception as e:
        print(f"[DataCollector] Held-feed sync error: {e}")

    last_watch_poll   = time.time()
    last_held_check   = time.time()
    last_rotate_check = time.time()
    last_reconcile    = time.time()
    last_subs_apply   = 0.0
    watch_change_pending = False
    # G1a — track the bookTicker-universe flag; a flip rebuilds all market
    # shards ONCE (adds/removes @bookTicker for every covered symbol) rather
    # than churning per-symbol. Captured from the flag the initial build used.
    bt_universe_state = _bookticker_universe_enabled()
    bt_flip_pending = False

    while True:
        await asyncio.sleep(1.0)
        now = time.time()
        _update_aggregate_health()
        _revive_dead_connections()

        # ── Dedicated held-symbol feed: hook event or 5 s poll
        if _positions_changed.is_set() or now - last_held_check >= _HELD_POLL_SEC:
            _positions_changed.clear()
            last_held_check = now
            try:
                await _sync_held_connection()
            except Exception as e:
                print(f"[DataCollector] Held-feed sync error: {e}")

        # ── Watchlist change detection: explicit flag every tick; the ~60 s
        # poll of the persisted list is the self-healing fallback.
        if _reconnect_requested.is_set():
            _reconnect_requested.clear()
            watch_change_pending = True
        elif now - last_watch_poll >= _WATCHLIST_POLL_SEC:
            last_watch_poll = now
            try:
                cur = await asyncio.to_thread(_load_persisted_watchlist)
                if set(cur) != set(_current_coins):
                    watch_change_pending = True
            except Exception:
                pass

        # ── G1a: bookTicker-universe flag flip detection (checked every tick;
        # applied via the same debounce so a flip is a single reconnect).
        cur_bt = _bookticker_universe_enabled()
        if cur_bt != bt_universe_state:
            bt_flip_pending = True

        # ── Debounced diff application: at most once per 5 s (§6.4 e)
        if (watch_change_pending or bt_flip_pending) and now - last_subs_apply >= _SUBS_DEBOUNCE_SEC:
            last_subs_apply = now
            try:
                desired = await asyncio.to_thread(_load_persisted_watchlist)
                active = await _verify_symbols(desired)
                if bt_flip_pending:
                    # A flag flip changes the per-coin suffix set for EVERY
                    # covered symbol — the coin-set diff can't express that, so
                    # rebuild all shards once with the new suffix set. This also
                    # absorbs any pending watchlist change in the same reconnect.
                    bt_flip_pending = False
                    watch_change_pending = False
                    bt_universe_state = cur_bt
                    await _rebuild_market_connections(active)
                    print(f"[DataCollector] bookTicker-universe "
                          f"{'ENABLED' if cur_bt else 'disabled'} — rebuilt market "
                          f"streams for {len(active)} coin(s)")
                elif watch_change_pending:
                    watch_change_pending = False
                    await _apply_watchlist_diff(active)
            except Exception as e:
                print(f"[DataCollector] Watchlist/bookTicker apply error: {e}")

        # ── 23 h make-before-break rotation (checked once a minute)
        if now - last_rotate_check >= 60.0:
            last_rotate_check = now
            try:
                await _rotate_aged_connections()
            except Exception as e:
                print(f"[DataCollector] Rotation error: {e}")

        # ── F5 — coverage reconciliation (watchlist vs actually-subscribed),
        # once a minute; auto-subscribes any uncovered symbols and WARNs on
        # ones that stay uncovered.
        if now - last_reconcile >= 60.0:
            last_reconcile = now
            try:
                await _reconcile_coverage()
            except Exception as e:
                print(f"[DataCollector] Coverage reconcile error: {e}")


# ── Health / freshness exports (§6.3, §6.5) ──────────────────────────────────

def held_price_age() -> Dict[str, Optional[float]]:
    """Per held symbol: age (seconds) of the freshest WS price event
    (trade/bookTicker/miniTicker). None = no WS price seen this session.
    trade_engine's REST price watchdog consumes this."""
    now = time.time()
    out: Dict[str, Optional[float]] = {}
    for sym in _held_symbols():
        ts = last_price_ts.get(sym, 0.0)
        out[sym] = round(now - ts, 3) if ts else None
    return out


def get_data_health() -> dict:
    """Cheap snapshot for the §6.5 health panel — counters are maintained
    incrementally; only the top-5 candle-age scan is computed here (O(coins))."""
    now = time.time()
    conns_out = []
    total_rate = 0.0
    for c in list(_connections) + ([_held_conn] if _held_conn is not None else []):
        rate = c.msgs_per_sec()
        total_rate += rate
        conns_out.append({
            "id": c.id,
            "kind": c.kind,
            "streams": len(c.streams),
            "connected_since": c.connected_since or None,
            "msgs_per_sec": rate,
            # websockets auto-pongs server pings but doesn't cheaply expose
            # their receipt time — last-message age is the documented proxy.
            "last_ping_age_sec": (round(now - c.last_message_ts, 1)
                                  if c.last_message_ts else None),
        })

    subscribed = sorted({s for c in _connections for s in c.symbols()})
    # F5 — coverage vs the canonical watchlist. "subscribed_coins" previously
    # conflated distinct-symbols with stream count; expose them separately so
    # health can tell "69 in universe / 61 covered / 138 streams" truthfully.
    try:
        _universe = _load_persisted_watchlist()
    except Exception:
        _universe = list(_current_coins)
    _universe_set = set(_universe)
    _covered = set(subscribed)
    _uncovered = sorted(_universe_set - _covered)
    with _excluded_lock:
        _excluded_snapshot = dict(_excluded_symbols)
    # I4.3 — split excluded symbols by reason so the UI can badge "delisted →
    # successor" (permanent, KNOWN_DELISTED) vs "halted (BREAK)" (temporary,
    # auto-restored when it returns to TRADING) differently.
    _excluded_delisted = sorted(s for s, st in _excluded_snapshot.items()
                                if str(st).upper() == "DELISTED")
    _excluded_break = sorted(s for s, st in _excluded_snapshot.items()
                             if str(st).upper() != "DELISTED")
    _total_streams = sum(len(c.streams) for c in _connections)
    ages = []
    for sym in subscribed:
        buf = ws_candles.get(sym)
        if buf:
            # Age relative to the expected close of the last buffered 1m candle.
            age = now - (int(buf[-1][0]) / 1000.0 + 60.0)
        else:
            # Never received a candle — age is how long we've been waiting.
            age = now - (_manager_started_ts or now)
        ages.append((sym, round(max(0.0, age), 1)))
    ages.sort(key=lambda t: t[1], reverse=True)

    fill1 = [min(1.0, len(ws_candles.get(s, [])) / _WS_CANDLE_MAX) for s in subscribed]
    fill5 = [min(1.0, len(ws_candles_5m.get(s, [])) / _WS_5M_CANDLE_MAX) for s in subscribed]

    with _counters_lock:
        while _gap_repairs and _gap_repairs[0] < now - 86_400:
            _gap_repairs.popleft()
        while _connect_attempts and _connect_attempts[0] < now - 300:
            _connect_attempts.popleft()
        gap_24h  = len(_gap_repairs)
        recon_5m = len(_connect_attempts)

    return {
        "connections":                conns_out,
        "total_msgs_per_sec":         round(total_rate, 2),
        "worst_candle_ages":          ages[:5],
        "buffer_fill_pct_1m":         round(100.0 * sum(fill1) / len(fill1), 1) if fill1 else 0.0,
        "buffer_fill_pct_5m":         round(100.0 * sum(fill5) / len(fill5), 1) if fill5 else 0.0,
        "backfill_last_duration_sec": round(_backfill_last_duration, 2),
        # I1.1 — startup-backfill completion signal for the health panel.
        "backfill_complete":          _backfill_complete,
        "backfill_pct":               _backfill_pct,
        "backfill_phase":             _backfill_phase,
        "backfill_stats":             dict(_backfill_last_stats),
        # I1.3 — mean 15m buffer depth (T2_ema50_15m_slope needs >=50).
        "buffer_fill_pct_15m":        round(
            100.0 * sum(min(1.0, len(ws_candles_15m.get(s, [])) / _WS_15M_CANDLE_MAX)
                        for s in subscribed) / len(subscribed), 1) if subscribed else 0.0,
        "gap_repairs_24h":            gap_24h,
        "reconnect_attempts_5min":    recon_5m,
        "resubscribe_count":          _ws_health.get("resubscribe_count", 0),
        "subscribed_coins":           len(subscribed),
        # F5 — distinct-symbol coverage vs universe, and raw stream count.
        "symbols_covered":            len(_covered),
        # G2 — expected_symbols is the count of VALID (tradeable) watchlist
        # symbols: _load_persisted_watchlist already drops non-TRADING ones, so
        # covering every valid symbol reads as 100% (health green). Delisted/
        # renamed symbols are reported separately via excluded_symbols.
        "expected_symbols":           len(_universe_set),
        "streams":                    _total_streams,
        "uncovered_symbols":          _uncovered[:50],
        "uncovered_count":            len(_uncovered),
        # G2 — {sym: non-TRADING status} dropped from the stream universe.
        "excluded_symbols":           _excluded_snapshot,
        "excluded_count":             len(_excluded_snapshot),
        # I4.3 — split by reason: permanent delisted (KNOWN_DELISTED, badge as
        # "delisted → successor") vs temporary halted (BREAK/other, auto-restored
        # when the symbol returns to TRADING).
        "excluded_delisted":          _excluded_delisted,
        "excluded_break":             _excluded_break,
    }


# ── Thread bootstrap ─────────────────────────────────────────────────────────

_ws_thread: Optional[threading.Thread] = None


def _ws_thread_runner():
    """Supervised runner for the websocket feed. Any failure — including a
    startup import error ('import websockets' missing after a partial update)
    — is logged loudly and the loop is restarted with backoff, instead of the
    thread dying silently and freezing all live prices."""
    import traceback
    delay = 5
    while True:
        try:
            loop = asyncio.new_event_loop()
            try:
                loop.run_until_complete(_start_websocket_loop())
            finally:
                loop.close()
            print("[DataCollector] FATAL: websocket loop exited unexpectedly "
                  f"— restarting in {delay}s")
        except Exception as e:
            print(f"[DataCollector] FATAL: websocket feed crashed: {type(e).__name__}: {e} "
                  f"— restarting in {delay}s")
            traceback.print_exc()
            try:
                import trade_engine as _te_ws
                _te_ws.log_diag_issue(
                    "websocket", "error",
                    f"websocket-feed thread crashed: {type(e).__name__}: {e}",
                    detail=f"restarting in {delay}s",
                )
            except Exception:
                pass
        _ws_health["connected"] = False
        time.sleep(delay)
        delay = min(delay * 2, 300)


async def start_websocket():
    """Run WebSocket loop in a dedicated thread — never blocks the uvicorn asyncio event loop."""
    global _ws_thread
    if _ws_thread is not None and _ws_thread.is_alive():
        return
    _ws_thread = threading.Thread(
        target=_ws_thread_runner,
        name="websocket-feed",
        daemon=True
    )
    _ws_thread.start()
