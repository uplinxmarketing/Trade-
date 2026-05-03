"""
Data collector — historical REST download + live WebSocket price/kline feed.
No auth required for any endpoint in any mode.
"""

import asyncio
import json
import time
import urllib.request
from datetime import datetime
from typing import Dict, Callable, Optional

import config
import database
import indicators
from connection import client

# Shared live prices dict — imported by trade_engine and strategy_engine
prices: Dict[str, float] = {}

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


# ── REST historical download ─────────────────────────────────────────────────

_BINANCE_BASES = [
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


def _compute_and_save(symbol: str, raw_klines: list):
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
            _compute_and_save(coin, raw)
            print(f"  {coin}: {len(raw)} candles saved")
            time.sleep(0.3)  # be polite to Binance rate limits
        except Exception as e:
            print(f"  {coin}: download failed — {e}")
    print("[DataCollector] History download complete.")


# ── Live WebSocket ───────────────────────────────────────────────────────────

def _build_ws_url(coins: list) -> str:
    streams = "/".join(
        f"{coin.lower()}@trade/{coin.lower()}@kline_{config.CANDLE_TIMEFRAME}"
        for coin in coins
    )
    return f"wss://stream.binance.com:9443/stream?streams={streams}"


async def _verify_symbols(coins: list) -> list:
    """Return only coins that Binance confirms exist as USDT pairs."""
    try:
        loop = asyncio.get_event_loop()
        def _fetch():
            url = "https://api.binance.com/api/v3/exchangeInfo?permissions=SPOT"
            with urllib.request.urlopen(url, timeout=10) as r:
                return json.loads(r.read())
        data = await loop.run_in_executor(None, _fetch)
        valid = {s["symbol"] for s in data.get("symbols", []) if s["status"] == "TRADING"}
        ok    = [c for c in coins if c in valid]
        bad   = [c for c in coins if c not in valid]
        if bad:
            print(f"[DataCollector] Dropping invalid symbols: {bad}")
        return ok
    except Exception as e:
        print(f"[DataCollector] Symbol verification failed ({e}) — using full list")
        return coins


async def start_websocket():
    """
    Async WebSocket loop with exponential-backoff reconnect.
    On each trade event: update prices, call trade_engine via callback.
    On closed kline: update candle DB.
    """
    import websockets
    backoff = 2

    # Verify symbols once on startup so invalid coins don't break the connection
    active_coins = await _verify_symbols(config.WATCHED_COINS)

    while True:
        url = _build_ws_url(active_coins)
        print(f"[DataCollector] Connecting WebSocket ({len(active_coins)} coins)…")
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=30) as ws:
                backoff = 2  # reset on successful connect
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
                            client.update_price(symbol, price)
                            if _price_callback:
                                _price_callback(dict(prices))

                        elif evt == "kline":
                            k = data["k"]
                            if k.get("x"):   # candle closed
                                sym = k["s"]
                                new_row = {
                                    "open_time": int(k["t"]),
                                    "open":   float(k["o"]),
                                    "high":   float(k["h"]),
                                    "low":    float(k["l"]),
                                    "close":  float(k["c"]),
                                    "volume": float(k["v"]),
                                }
                                # Re-fetch last 50 candles and recalculate indicators
                                existing = database.get_candles(sym, config.CANDLE_TIMEFRAME, limit=50)
                                all_raw = [
                                    [row["open_time"], row["open"], row["high"],
                                     row["low"], row["close"], row["volume"]]
                                    for row in existing
                                ] + [[
                                    new_row["open_time"], new_row["open"], new_row["high"],
                                    new_row["low"], new_row["close"], new_row["volume"]
                                ]]
                                _compute_and_save(sym, all_raw)

                                # Notify trade_engine so signal cache is updated
                                # immediately on kline close — enables real-time buys.
                                if _kline_callback and len(all_raw) >= 27:
                                    closes  = [float(r[4]) for r in all_raw]
                                    volumes = [float(r[5]) for r in all_raw]
                                    _kline_callback(sym, closes, volumes)

                    except Exception as e:
                        print(f"[DataCollector] Message error: {e}")

        except Exception as e:
            print(f"[DataCollector] WebSocket disconnected: {e}")
            print(f"[DataCollector] Reconnecting in {backoff}s…")

            # Fill gap: re-fetch last 10 candles via REST for active coins only
            try:
                for coin in active_coins:
                    raw = _fetch_klines_rest(coin, config.CANDLE_TIMEFRAME, limit=10)
                    _compute_and_save(coin, raw)
            except Exception:
                pass

            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 30)
