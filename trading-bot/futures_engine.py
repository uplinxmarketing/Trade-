"""
Futures paper-trading engine — two async coroutines:

  Loop A  mark_price_loop()   — WebSocket feed from Binance fstream
  Loop B  signal_scanner_loop() — scores coins every 60 s, opens/closes positions

Signal system (6 signals, scored -1/0/+1 each):
  1. trend  — close vs EMA20
  2. rsi    — RSI above/below 50 with overbought/oversold guards
  3. macd   — MACD histogram direction
  4. volume — volume vs 20-bar average, confirmed by candle direction
  5. obv    — OBV slope (last vs previous)
  6. funding — funding rate sign (shorts pay longs → +1, longs pay shorts → -1)

LONG  when total score >= +FUTURES_MIN_SIGNALS
SHORT when total score <= -FUTURES_MIN_SIGNALS
"""

import asyncio
import json
import math
import threading
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import aiohttp

import config
import database
from futures_paper_client import FuturesPaperClient
from indicators import calc_ema, calc_rsi, calc_macd, calc_obv

# ── Module-level state ────────────────────────────────────────────────────────

_client: Optional[FuturesPaperClient] = None
_client_lock = threading.Lock()

_mark_prices: Dict[str, float] = {}          # symbol → latest mark price
_funding_rates: Dict[str, float] = {}        # symbol → latest funding rate
_mark_lock = threading.Lock()

_futures_signal_cache: Dict[str, dict] = {}  # symbol → signal snapshot
_futures_signal_lock = threading.Lock()

_futures_active = True    # controlled by /api/futures/start|pause
_futures_settings = {
    "leverage":         config.FUTURES_LEVERAGE,
    "budget_usdt":      config.FUTURES_BUDGET_USDT,
    "take_profit_pct":  config.FUTURES_TAKE_PROFIT_PCT,
    "stop_loss_pct":    config.FUTURES_STOP_LOSS_PCT,
    "min_signals":      config.FUTURES_MIN_SIGNALS,
    "max_positions":    config.FUTURES_MAX_POSITIONS,
}
_settings_lock = threading.Lock()

FAPI_BASE  = "https://fapi.binance.com"
FSTREAM_WS = "wss://fstream.binance.com/ws/!markPrice@arr@1s"


# ── Init ──────────────────────────────────────────────────────────────────────

def init_futures_engine():
    global _client
    with _client_lock:
        if _client is None:
            _client = FuturesPaperClient(
                starting_usdt=float(config.FUTURES_STARTING_USDT)
            )


# ── Public helpers ────────────────────────────────────────────────────────────

def get_futures_status() -> dict:
    with _client_lock:
        if _client is None:
            return {"running": False, "balance": 0.0, "equity": 0.0,
                    "positions": 0, "active": False}
        bal    = _client.get_balance()
        equity = _client.get_equity()
        n_pos  = _client.open_position_count()

    trades = database.get_recent_futures_trades(1000)
    total_pnl  = sum(t.get("net_profit") or 0.0 for t in trades)
    wins       = sum(1 for t in trades if (t.get("net_profit") or 0) > 0)
    win_rate   = (wins / len(trades) * 100) if trades else 0.0

    return {
        "running":    _futures_active,
        "balance":    round(bal, 4),
        "equity":     round(equity, 4),
        "positions":  n_pos,
        "total_pnl":  round(total_pnl, 4),
        "win_rate":   round(win_rate, 1),
        "trade_count": len(trades),
    }


def get_futures_positions() -> List[dict]:
    with _client_lock:
        if _client is None:
            return []
        positions = _client.get_open_positions()

    result = []
    for p in positions:
        mp     = _get_mark(p["symbol"])
        upnl   = _upnl(p, mp)
        result.append({**p, "mark_price": mp, "unrealized_pnl": round(upnl, 4)})
    return result


def get_futures_signals() -> List[dict]:
    with _futures_signal_lock:
        return list(_futures_signal_cache.values())


def update_futures_settings(patch: dict):
    with _settings_lock:
        _futures_settings.update(patch)


def set_futures_active(active: bool):
    global _futures_active
    _futures_active = active


def reset_futures_wallet(starting_usdt: float = None):
    with _client_lock:
        if _client is None:
            return
        _client.reset(starting_usdt or config.FUTURES_STARTING_USDT)
    with _futures_signal_lock:
        _futures_signal_cache.clear()


# ── Internal ──────────────────────────────────────────────────────────────────

def _get_mark(symbol: str) -> float:
    with _mark_lock:
        return _mark_prices.get(symbol, 0.0)


def _upnl(pos: dict, mark_price: float) -> float:
    if mark_price <= 0:
        return 0.0
    qty = pos["quantity"]
    ep  = pos["entry_price"]
    if pos["direction"] == "LONG":
        return (mark_price - ep) * qty
    return (ep - mark_price) * qty


# ── Signal scoring ────────────────────────────────────────────────────────────

def _score_signals(
    closes: List[float],
    volumes: List[float],
    mark_price: float,
    funding_rate: float,
) -> Tuple[int, dict]:
    """Return (net_score, signals_dict).

    net_score > 0 is bullish, < 0 is bearish.
    Each signal contributes -1, 0, or +1.
    """
    if len(closes) < 26:
        return 0, {}

    signals: dict = {}

    # 1. Trend — EMA20
    emas = calc_ema(closes, 20)
    if emas:
        ema20 = emas[-1]
        if closes[-1] > ema20 * 1.001:
            signals["trend"] = 1
        elif closes[-1] < ema20 * 0.999:
            signals["trend"] = -1
        else:
            signals["trend"] = 0
    else:
        signals["trend"] = 0

    # 2. RSI
    rsi_vals = calc_rsi(closes, 14)
    rsi = rsi_vals[-1] if rsi_vals else 50.0
    if rsi > 55 and rsi < 75:
        signals["rsi"] = 1
    elif rsi < 45 and rsi > 25:
        signals["rsi"] = -1
    else:
        signals["rsi"] = 0

    # 3. MACD histogram
    try:
        macd_line, signal_line, histogram = calc_macd(closes)
        if histogram and len(histogram) >= 2:
            if histogram[-1] > 0 and histogram[-1] > histogram[-2]:
                signals["macd"] = 1
            elif histogram[-1] < 0 and histogram[-1] < histogram[-2]:
                signals["macd"] = -1
            else:
                signals["macd"] = 0
        else:
            signals["macd"] = 0
    except Exception:
        signals["macd"] = 0

    # 4. Volume with directional confirmation
    if len(volumes) >= 20:
        vol_ma = sum(volumes[-20:]) / 20
        if vol_ma > 0 and volumes[-1] > vol_ma * 1.1:
            # Confirm direction via candle body
            signals["volume"] = 1 if closes[-1] > closes[-2] else -1
        else:
            signals["volume"] = 0
    else:
        signals["volume"] = 0

    # 5. OBV slope
    try:
        obv_vals = calc_obv(closes, volumes)
        if obv_vals and len(obv_vals) >= 3:
            obv_slope = obv_vals[-1] - obv_vals[-3]
            if obv_slope > 0:
                signals["obv"] = 1
            elif obv_slope < 0:
                signals["obv"] = -1
            else:
                signals["obv"] = 0
        else:
            signals["obv"] = 0
    except Exception:
        signals["obv"] = 0

    # 6. Funding rate — negative means shorts pay longs (bullish for LONG)
    if funding_rate < -0.0001:
        signals["funding"] = 1
    elif funding_rate > 0.0001:
        signals["funding"] = -1
    else:
        signals["funding"] = 0

    net_score = sum(signals.values())
    return net_score, signals


# ── REST kline fetch ──────────────────────────────────────────────────────────

async def _fetch_klines(session: aiohttp.ClientSession, symbol: str, limit: int = 50) -> dict:
    """Fetch 1m klines + current funding rate for a futures symbol."""
    url_klines  = f"{FAPI_BASE}/fapi/v1/klines"
    url_funding = f"{FAPI_BASE}/fapi/v1/fundingRate"

    closes = []
    volumes = []
    funding_rate = 0.0

    try:
        async with session.get(
            url_klines,
            params={"symbol": symbol, "interval": "1m", "limit": limit},
            timeout=aiohttp.ClientTimeout(total=8),
        ) as resp:
            if resp.status == 200:
                data = await resp.json()
                closes  = [float(row[4]) for row in data]   # close
                volumes = [float(row[5]) for row in data]   # base volume
    except Exception as e:
        print(f"[FuturesEngine] klines fetch error {symbol}: {e}")

    try:
        async with session.get(
            url_funding,
            params={"symbol": symbol, "limit": 1},
            timeout=aiohttp.ClientTimeout(total=5),
        ) as resp:
            if resp.status == 200:
                data = await resp.json()
                if data:
                    funding_rate = float(data[-1].get("fundingRate", 0))
    except Exception:
        pass

    return {"closes": closes, "volumes": volumes, "funding_rate": funding_rate}


# ── Loop A — mark price WebSocket ─────────────────────────────────────────────

async def mark_price_loop():
    """Maintain a live mark price + funding rate dict from Binance futures WS."""
    global _mark_prices, _funding_rates

    watched = set(config.FUTURES_WATCHED_COINS)
    backoff = 2

    while True:
        try:
            import websockets
            async with websockets.connect(
                FSTREAM_WS, ping_interval=20, ping_timeout=30
            ) as ws:
                backoff = 2
                print("[FuturesEngine] Loop A: mark price WS connected")
                async for raw in ws:
                    try:
                        msgs = json.loads(raw)
                        if not isinstance(msgs, list):
                            msgs = [msgs]
                        updates: Dict[str, float] = {}
                        frates: Dict[str, float]  = {}
                        for m in msgs:
                            sym = m.get("s", "")
                            if sym not in watched:
                                continue
                            mp = float(m.get("p", 0) or m.get("mp", 0) or 0)
                            fr = float(m.get("r", 0) or 0)
                            if mp > 0:
                                updates[sym] = mp
                            frates[sym] = fr

                        with _mark_lock:
                            _mark_prices.update(updates)
                            _funding_rates.update(frates)

                        # Forward to futures client for TP/SL checks
                        with _client_lock:
                            if _client:
                                for sym, mp in updates.items():
                                    _client.update_mark_price(sym, mp)

                    except Exception:
                        pass

        except Exception as exc:
            print(f"[FuturesEngine] Loop A WS error: {exc} — reconnect in {backoff}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


# ── Loop B — signal scanner + position manager ────────────────────────────────

async def signal_scanner_loop():
    """Score every watched coin every FUTURES_SCAN_INTERVAL_SEC seconds.

    Opens LONG/SHORT positions when score threshold is met.
    TP/SL checks are done inline here too (belt-and-suspenders alongside
    the mark-price callback in Loop A).
    """
    await asyncio.sleep(15)   # let Loop A warm up first

    while True:
        try:
            await _run_scan()
        except Exception as exc:
            print(f"[FuturesEngine] Loop B error: {exc}")

        with _settings_lock:
            interval = _futures_settings.get(
                "scan_interval", config.FUTURES_SCAN_INTERVAL_SEC
            )
        await asyncio.sleep(interval)


async def _run_scan():
    global _futures_active

    if not _futures_active:
        return

    with _client_lock:
        client = _client
    if client is None:
        return

    # TP/SL check first
    closed = client.check_positions()
    for t in closed:
        database.log_activity(
            f"[Futures] CLOSE {t['direction']} {t['symbol']} "
            f"pnl={t['net_profit']:+.4f} USDT", "info"
        )

    with _settings_lock:
        settings = dict(_futures_settings)

    min_sig   = int(settings.get("min_signals", config.FUTURES_MIN_SIGNALS))
    max_pos   = int(settings.get("max_positions", config.FUTURES_MAX_POSITIONS))
    leverage  = int(settings.get("leverage", config.FUTURES_LEVERAGE))
    budget    = float(settings.get("budget_usdt", config.FUTURES_BUDGET_USDT))
    tp_pct    = float(settings.get("take_profit_pct", config.FUTURES_TAKE_PROFIT_PCT))
    sl_pct    = float(settings.get("stop_loss_pct", config.FUTURES_STOP_LOSS_PCT))

    watched = config.FUTURES_WATCHED_COINS

    async with aiohttp.ClientSession() as session:
        for symbol in watched:
            try:
                await _scan_symbol(
                    session, client, symbol,
                    min_sig, max_pos, leverage, budget, tp_pct, sl_pct,
                )
            except Exception as exc:
                print(f"[FuturesEngine] scan error {symbol}: {exc}")
            await asyncio.sleep(0.2)   # small rate-limit pause between symbols


async def _scan_symbol(
    session, client, symbol: str,
    min_sig: int, max_pos: int,
    leverage: int, budget: float,
    tp_pct: float, sl_pct: float,
):
    data = await _fetch_klines(session, symbol)
    closes   = data["closes"]
    volumes  = data["volumes"]
    funding  = data["funding_rate"]

    if len(closes) < 26:
        return

    with _mark_lock:
        mark_price   = _mark_prices.get(symbol, closes[-1] if closes else 0)
        funding_rate = _funding_rates.get(symbol, funding)

    if mark_price <= 0:
        return

    score, signals = _score_signals(closes, volumes, mark_price, funding_rate)

    snapshot = {
        "symbol":       symbol,
        "mark_price":   mark_price,
        "score":        score,
        "funding_rate": round(funding_rate * 100, 4),
        "signals":      signals,
        "timestamp":    datetime.now(timezone.utc).isoformat(),
    }
    with _futures_signal_lock:
        _futures_signal_cache[symbol] = snapshot

    n_open = client.open_position_count()

    # Try to open LONG
    if score >= min_sig and n_open < max_pos:
        if not client.has_open_position(symbol, "LONG"):
            pos = client.open_position(
                symbol, "LONG", mark_price, budget, leverage, tp_pct, sl_pct,
            )
            if pos:
                database.log_activity(
                    f"[Futures] OPEN LONG {symbol} @ {mark_price:.4f} "
                    f"score={score} margin={budget:.0f} lev={leverage}x", "info"
                )

    # Try to open SHORT
    elif score <= -min_sig and n_open < max_pos:
        if not client.has_open_position(symbol, "SHORT"):
            pos = client.open_position(
                symbol, "SHORT", mark_price, budget, leverage, tp_pct, sl_pct,
            )
            if pos:
                database.log_activity(
                    f"[Futures] OPEN SHORT {symbol} @ {mark_price:.4f} "
                    f"score={score} margin={budget:.0f} lev={leverage}x", "info"
                )
