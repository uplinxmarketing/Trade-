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

# In-memory rolling candle buffer — filled by WebSocket kline-close events.
# Used as the primary data source for signal computation when Binance REST
# is geo-blocked from Railway's servers.  Holds up to 60 closed 1m candles
# per coin; once 16+ have accumulated, RSI signals fire and buys can happen.
ws_candles: Dict[str, list] = {}
_WS_CANDLE_MAX = 60   # candles to keep per coin
_MIN_CANDLES   = 16   # minimum candles needed for at least RSI signal to work

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
    Returns a list of dicts with open/high/low/close/volume keys.
    Tries each Binance base URL in order; returns [] on total failure.
    """
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


def download_history():
    """Download 1000 hourly candles per coin on first run."""
    if not database.candles_table_empty():
        print("[DataCollector] Candle history already loaded — skipping download.")
        return

    print(f"[DataCollector] Downloading {config.CANDLE_LOOKBACK * 20} candles per coin…")
    for coin in config.WATCHED_COINS:
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
    streams = "/".join(
        f"{coin.lower()}@trade/{coin.lower()}@kline_{config.CANDLE_TIMEFRAME}"
        for coin in coins
    )
    return f"wss://stream.binance.com:9443/stream?streams={streams}"


async def _verify_symbols(coins: list) -> list:
    """Return only coins that Binance confirms exist as USDT pairs."""
    try:
        def _fetch():
            url = "https://api.binance.com/api/v3/exchangeInfo?permissions=SPOT"
            with urllib.request.urlopen(url, timeout=15) as r:
                return json.loads(r.read())
        data = await asyncio.to_thread(_fetch)
        valid = {s["symbol"] for s in data.get("symbols", []) if s["status"] == "TRADING"}
        ok    = [c for c in coins if c in valid]
        bad   = [c for c in coins if c not in valid]
        if bad:
            print(f"[DataCollector] Dropping invalid symbols: {bad}")
        return ok
    except Exception as e:
        print(f"[DataCollector] Symbol verification failed ({e}) — using full coin list")
        return coins


async def _start_websocket_loop():
    """
    Async WebSocket loop with exponential-backoff reconnect.
    On each trade event: update prices, call trade_engine via callback.
    On closed kline: update candle DB.
    """
    import websockets
    backoff = 1

    # Verify symbols once on startup so invalid coins don't break the connection
    active_coins = await _verify_symbols(config.WATCHED_COINS)

    while True:
        url = _build_ws_url(active_coins)
        print(f"[DataCollector] Connecting WebSocket ({len(active_coins)} coins)…")
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=30, open_timeout=10) as ws:
                backoff = 1  # reset on successful connect
                print("[DataCollector] WebSocket connected ✓")

                async for raw in ws:
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
                                sym = k["s"]
                                closed = [
                                    int(k["t"]),   float(k["o"]), float(k["h"]),
                                    float(k["l"]), float(k["c"]), float(k["v"]),
                                ]

                                # ── Update in-memory candle buffer (primary signal source)
                                buf = ws_candles.setdefault(sym, [])
                                buf.append(closed)
                                if len(buf) > _WS_CANDLE_MAX:
                                    buf.pop(0)

                                # ── Also persist to DB (best-effort, not required)
                                existing = database.get_candles(sym, config.CANDLE_TIMEFRAME, limit=50)
                                db_raw = [
                                    [row["open_time"], row["open"], row["high"],
                                     row["low"], row["close"], row["volume"]]
                                    for row in existing
                                ]
                                all_raw = db_raw + [closed]
                                _compute_and_save(sym, all_raw)

                                # ── Signal update: prefer DB+new, fall back to WS buffer
                                signal_src = all_raw if len(all_raw) >= _MIN_CANDLES else buf
                                if _kline_callback and len(signal_src) >= _MIN_CANDLES:
                                    closes  = [float(r[4]) for r in signal_src]
                                    volumes = [float(r[5]) for r in signal_src]
                                    _kline_callback(sym, closes, volumes)

                    except Exception as e:
                        print(f"[DataCollector] Message error: {e}")

        except Exception as e:
            print(f"[DataCollector] WebSocket disconnected: {e}")
            print(f"[DataCollector] Reconnecting in {backoff}s…")

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


async def start_websocket():
    """Run WebSocket loop in a dedicated thread — never blocks the uvicorn asyncio event loop."""
    loop = asyncio.new_event_loop()
    t = threading.Thread(
        target=lambda: loop.run_until_complete(_start_websocket_loop()),
        name="websocket-feed",
        daemon=True
    )
    t.start()
