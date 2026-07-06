"""
Data collector — historical REST download + live WebSocket price/kline feed.
No auth required for any endpoint in any mode.
"""

import asyncio
import json
import threading
import time
import urllib.request
from collections import deque
from datetime import datetime
from typing import Dict, Callable, Optional

import config
import database
import indicators
from connection import client

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

# In-memory rolling candle buffer — filled by WebSocket kline-close events.
# Used as the primary data source for signal computation when Binance REST
# is geo-blocked from Railway's servers.  Holds up to 60 closed 1m candles
# per coin; once 16+ have accumulated, RSI signals fire and buys can happen.
ws_candles: Dict[str, list] = {}
_WS_CANDLE_MAX = 60   # candles to keep per coin
_MIN_CANDLES   = 16   # minimum candles needed for at least RSI signal to work

# In-memory 5-minute candle buffer — filled via WebSocket @kline_5m subscription.
# Eliminates REST calls for 5m veto checks after the first 5 candles arrive.
ws_candles_5m: Dict[str, list] = {}
_WS_5M_CANDLE_MAX = 30   # 150 minutes of 5m candles
_MIN_CANDLES_5M   = 21   # enough for EMA21 to be meaningful

# Rolling price-tick buffer — filled by every WebSocket @trade event.
# Used as immediate fallback (available in seconds) but RSI quality is poor
# because all ticks come from the same second (prices nearly identical → RSI≈50).
price_ticks: Dict[str, deque] = {}
_TICK_MAX  = 50
_MIN_TICKS = 15

# Time-sampled price buffer — one close price recorded per 1 second.
# After 16 seconds: 16 samples → RSI(14) computes on meaningful price variation.
# This is the primary signal source when Binance REST is geo-blocked.
price_samples: Dict[str, list] = {}       # one price per _SAMPLE_INTERVAL
_price_sample_ts: Dict[str, float] = {}   # last sample time per coin
_SAMPLE_INTERVAL = 1.0    # seconds between samples
_SAMPLE_MAX      = 300    # keep 5 minutes of samples (300 × 1s)
_MIN_SAMPLES     = 16     # 16 seconds needed before RSI fires

# Callbacks registered by main.py to avoid circular imports
_price_callback: Optional[Callable[[Dict[str, float]], None]] = None
_kline_callback: Optional[Callable[[str, list, list], None]]  = None


def register_price_callback(cb: Callable[[Dict[str, float]], None]):
    global _price_callback
    _price_callback = cb


def register_kline_callback(cb: Callable[[str, list, list], None]):
    """Wire kline-close events into trade_engine.update_coin_signals."""
    global _kline_callback
    _kline_callback = cb


# ── Dynamic watchlist ─────────────────────────────────────────────────────────
# The WS stream list is rebuilt on every (re)connect from the persisted
# watchlist (strategy.json approved_coins — the same file /api/coins writes)
# so dashboard changes actually re-subscribe. A watcher task also polls the
# persisted list every _WATCHLIST_POLL_SEC as a self-healing fallback, so the
# fix works even if control_api never calls refresh_watchlist().

_reconnect_requested = threading.Event()
_WATCHLIST_POLL_SEC  = 60

# Binance combined streams allow ~1024 streams per connection. Klines (1m+5m)
# are essential for signals and are always kept; miniTicker / @trade are
# dropped first when the coin list would blow the budget.
_MAX_COMBINED_STREAMS = 1000


def refresh_watchlist():
    """Signal the WS loop to reconnect and rebuild its stream list from the
    persisted watchlist. Thread-safe (just sets an Event); safe to call at any
    time, including before the WS loop has started. control_api's /api/coins
    handler can call this right after writing approved_coins; even without
    that hook the WS loop self-heals via its ~60s watchlist poll."""
    _reconnect_requested.set()
    print("[DataCollector] Watchlist refresh requested — WebSocket will resubscribe")


def _load_persisted_watchlist() -> list:
    """Symbols the WS feed should stream: the user's approved coins from
    strategy.json (written by /api/coins), union any symbols with open
    positions (held coins must keep streaming prices for the sell path),
    falling back to config.WATCHED_COINS when nothing is persisted."""
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


def _fetch_klines_rest(symbol: str, interval: str, limit: int = 500):
    """Try each Binance base URL in order; return first successful response."""
    last_err = None
    for base in _BINANCE_BASES:
        url = f"{base}/api/v3/klines?symbol={symbol}&interval={interval}&limit={limit}"
        try:
            with urllib.request.urlopen(url, timeout=15) as resp:
                return json.loads(resp.read())
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

    for base in _BINANCE_BASES:
        url = f"{base}/api/v3/klines?symbol={symbol}&interval=5m&limit={limit}"
        try:
            with urllib.request.urlopen(url, timeout=5) as resp:
                raw = json.loads(resp.read())
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
            continue
    return []


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
    _WS_5M_CANDLE_MAX per coin are kept. Runs once per process."""
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
            buf = ws_candles_5m.setdefault(sym, [])
            have = {int(k[0]) for k in buf}
            merged = [k for k in synth if int(k[0]) not in have] + list(buf)
            merged.sort(key=lambda k: int(k[0]))
            ws_candles_5m[sym] = merged[-_WS_5M_CANDLE_MAX:]
            seeded += 1
            total += len(ws_candles_5m[sym])
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
            raw = _fetch_klines_rest(coin, config.CANDLE_TIMEFRAME, limit=1000)
            _compute_and_save(coin, raw, save_all=True)
            print(f"  {coin}: {len(raw)} candles saved")
            time.sleep(0.3)  # be polite to Binance rate limits
        except Exception as e:
            print(f"  {coin}: download failed — {e}")
    print("[DataCollector] History download complete.")


# ── Live WebSocket ─────────────────────────────────────────────────────────────────────────────

def _build_ws_url(coins: list) -> str:
    """Compose the combined-stream URL, respecting Binance's per-connection
    stream budget. Klines (1m + 5m) are essential for signal computation and
    are always included for every coin; miniTicker (1s price roll-up, feeds
    the sell path) is next; per-trade tick streams are optional and dropped
    first when the watchlist is large."""
    n = len(coins)
    suffixes = [f"kline_{config.CANDLE_TIMEFRAME}", "kline_5m"]   # 2 per coin, always
    if n * 3 <= _MAX_COMBINED_STREAMS:
        suffixes.append("miniTicker")                             # 3 per coin
    if n * 4 <= _MAX_COMBINED_STREAMS:
        suffixes.append("trade")                                  # 4 per coin
    dropped = [s for s in ("trade", "miniTicker") if s not in suffixes]
    if dropped:
        print(f"[DataCollector] {n} coins would exceed the stream budget "
              f"({_MAX_COMBINED_STREAMS}) — dropping {dropped} streams, klines kept for all")
    streams = "/".join(
        f"{coin.lower()}@{suffix}" for coin in coins for suffix in suffixes
    )
    return f"wss://data-stream.binance.vision/stream?streams={streams}"


# Cache exchangeInfo verification so rapid resubscribes/reconnects don't
# hammer the endpoint; keyed by the exact coin set.
_verify_cache: Dict = {"key": None, "ok": None, "ts": 0.0}
_VERIFY_TTL_SEC = 600


async def _verify_symbols(coins: list) -> list:
    """Return only coins that Binance confirms exist as USDT pairs."""
    key = frozenset(coins)
    if _verify_cache["key"] == key and time.time() - _verify_cache["ts"] < _VERIFY_TTL_SEC:
        return list(_verify_cache["ok"])
    try:
        def _fetch():
            url = "https://data-api.binance.vision/api/v3/exchangeInfo?permissions=SPOT"
            with urllib.request.urlopen(url, timeout=15) as r:
                return json.loads(r.read())
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


async def _watchlist_watcher(ws, desired_set: set):
    """Runs alongside an open WS connection. Closes the socket — so the
    reconnect loop rebuilds the stream list — when refresh_watchlist() was
    called or the persisted watchlist (strategy.json approved_coins ∪ open
    positions) no longer matches what this connection subscribed to. The
    ~60s poll is a self-healing fallback that keeps /api/coins changes
    effective even if control_api never calls refresh_watchlist()."""
    try:
        while True:
            # Check the explicit refresh flag every second for responsiveness;
            # fall back to re-reading the persisted list every poll interval.
            for _ in range(_WATCHLIST_POLL_SEC):
                await asyncio.sleep(1.0)
                if _reconnect_requested.is_set():
                    break
            changed = _reconnect_requested.is_set()
            if not changed:
                try:
                    current = set(await asyncio.to_thread(_load_persisted_watchlist))
                    changed = current != desired_set
                except Exception:
                    changed = False
            if changed:
                _ws_health["resubscribe_count"] = _ws_health.get("resubscribe_count", 0) + 1
                print("[DataCollector] Watchlist changed — closing WebSocket to resubscribe")
                try:
                    await ws.close()
                except Exception:
                    pass
                return
    except asyncio.CancelledError:
        pass
    except Exception as e:
        print(f"[DataCollector] Watchlist watcher error: {e}")


async def _start_websocket_loop():
    """
    Async WebSocket loop with exponential-backoff reconnect.
    On each trade event: update prices, call trade_engine via callback.
    On closed kline: update candle DB.
    The stream list is rebuilt from the persisted watchlist on every
    (re)connect, so /api/coins changes take effect without a restart.
    """
    global last_ws_message_ts
    import websockets
    backoff = 1
    loop = asyncio.get_running_loop()

    # Cold-start: seed 5m buffers from stored 1m candles (idempotent — also
    # attempted from download_history) so the 5m veto works right away.
    try:
        await asyncio.to_thread(_bootstrap_5m_from_db)
    except Exception as e:
        print(f"[DataCollector] 5m bootstrap failed: {e}")

    while True:
        # Rebuild the stream list on every (re)connect: persisted watchlist
        # (/api/coins → strategy.json approved_coins) ∪ open-position symbols,
        # falling back to config.WATCHED_COINS when nothing is persisted.
        _reconnect_requested.clear()
        try:
            desired = await asyncio.to_thread(_load_persisted_watchlist)
        except Exception:
            desired = list(config.WATCHED_COINS)
        desired_set = set(desired)

        # Drop symbols Binance doesn't trade (invalid/delisted) — cached 10 min
        active_coins = await _verify_symbols(desired)
        if len(active_coins) * 2 > _MAX_COMBINED_STREAMS:
            keep = _MAX_COMBINED_STREAMS // 2
            print(f"[DataCollector] Watchlist too large ({len(active_coins)} coins) — "
                  f"subscribing the first {keep}")
            active_coins = active_coins[:keep]

        url = _build_ws_url(active_coins)
        print(f"[DataCollector] Connecting WebSocket ({len(active_coins)} coins)…")
        watcher = None
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=30, open_timeout=10) as ws:
                backoff = 1  # reset on successful connect
                _ws_health["connected"]        = True
                _ws_health["connect_count"]   += 1
                _ws_health["last_connect_ts"]  = time.time()
                _ws_health["subscribed_coins"] = len(active_coins)
                watcher = loop.create_task(_watchlist_watcher(ws, desired_set))
                print("[DataCollector] WebSocket connected ✓")
                if _ws_health["disconnect_count"] > 0:
                    try:
                        import trade_engine as _te_dc
                        _te_dc.log_diag_issue(
                            "websocket", "info",
                            f"WebSocket reconnected (subscribed {len(active_coins)} symbols)",
                        )
                    except Exception:
                        pass

                async for raw in ws:
                    _ws_health["messages_received"] += 1
                    _ws_health["last_message_ts"]    = time.time()
                    last_ws_message_ts               = _ws_health["last_message_ts"]
                    try:
                        msg  = json.loads(raw)
                        data = msg.get("data", msg)
                        evt  = data.get("e")

                        if evt == "trade":
                            symbol = data["s"]
                            price  = float(data["p"])
                            prices[symbol] = price
                            # Raw ticks (fast, but too uniform for quality RSI)
                            ticks = price_ticks.setdefault(symbol, deque(maxlen=_TICK_MAX))
                            ticks.append(price)
                            # Time-sampled prices (one per 1s → quality RSI after 16s)
                            now_ts = time.time()
                            if now_ts - _price_sample_ts.get(symbol, 0) >= _SAMPLE_INTERVAL:
                                _price_sample_ts[symbol] = now_ts
                                buf = price_samples.setdefault(symbol, [])
                                buf.append(price)
                                if len(buf) > _SAMPLE_MAX:
                                    buf.pop(0)
                            client.update_price(symbol, price)
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

                                # ── Route 5m candles to separate buffer
                                if interval == "5m":
                                    buf5 = ws_candles_5m.setdefault(sym, [])
                                    buf5.append(closed)
                                    if len(buf5) > _WS_5M_CANDLE_MAX:
                                        buf5.pop(0)
                                    continue

                                # ── Update in-memory candle buffer (primary signal source)
                                buf = ws_candles.setdefault(sym, [])
                                buf.append(closed)
                                if len(buf) > _WS_CANDLE_MAX:
                                    buf.pop(0)

                                # ── Persist + signal on a worker thread so the
                                # blocking SQLite work never stalls this loop.
                                loop.run_in_executor(
                                    None, _persist_and_signal, sym, closed, list(buf)
                                )

                        elif evt == "24hrMiniTicker":
                            symbol = data["s"]
                            close_price = float(data["c"])
                            # miniTicker arrives every ~1s per coin — update price
                            # only when no @trade event has updated it more recently
                            # (trade events are sub-second; miniTicker is a 1s roll-up).
                            prices[symbol] = close_price
                            client.update_price(symbol, close_price)
                            try:
                                import trade_engine as _te_mt
                                _te_mt._last_ws_price_ts[symbol] = time.time()
                                # For HELD positions, evaluate sell triggers on this
                                # tick too — low-trade-volume coins may only get
                                # miniTicker updates, and waiting for another coin's
                                # @trade event (or the 0.25s fallback loop) delays
                                # the exit.
                                if _price_callback and symbol in _te_mt._pos_by_symbol:
                                    _price_callback(dict(prices))
                            except Exception:
                                pass

                    except Exception as e:
                        print(f"[DataCollector] Message error: {e}")

            # Graceful close (e.g. watchlist resubscribe) — loop reconnects
            # immediately with a freshly rebuilt stream list, no backoff.
            _ws_health["connected"]          = False
            _ws_health["last_disconnect_ts"] = time.time()
            print("[DataCollector] WebSocket closed — rebuilding stream list…")

        except Exception as e:
            _ws_health["connected"]            = False
            _ws_health["disconnect_count"]    += 1
            _ws_health["last_disconnect_ts"]   = time.time()
            print(f"[DataCollector] WebSocket disconnected: {e}")
            print(f"[DataCollector] Reconnecting in {backoff}s…")
            try:
                import trade_engine as _te_dc
                _te_dc.log_diag_issue(
                    "websocket", "warn",
                    f"WebSocket disconnected, reconnect #{_ws_health['disconnect_count']}",
                    detail=f"{type(e).__name__}: {e} — backoff {backoff}s",
                )
            except Exception:
                pass

            # Fill gap: re-fetch last 10 candles via REST for active coins only.
            # Run in a thread so the blocking urlopen never freezes the event loop.
            def _gap_fill():
                for coin in active_coins:
                    try:
                        raw = _fetch_klines_rest(coin, config.CANDLE_TIMEFRAME, limit=10)
                        _compute_and_save(coin, raw, save_all=True)
                    except Exception:
                        pass
            try:
                await asyncio.to_thread(_gap_fill)
            except Exception:
                pass

            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)
        finally:
            # Never leak watcher tasks across reconnects.
            if watcher is not None:
                watcher.cancel()


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
