"""
Futures paper-trading engine — v5.8.3

Two async coroutines:
  Loop A  mark_price_loop()     — WebSocket mark-price feed (best-effort)
  Loop B  signal_scanner_loop() — scores all coins every 60 s, opens/closes positions

Klines are fetched from the spot REST endpoint (api.binance.com/api/v3/klines)
which is the same format as the futures endpoint and is reliably reachable from
Railway. When the fstream WebSocket hasn't connected yet, the closing price from
the most recent 1m candle is used as the mark price so TP/SL checks still fire.

Signal system (6 signals, scored -1 / 0 / +1 each):
  1. trend   — close vs EMA20
  2. rsi     — RSI direction (60–80 = bull, 20–40 = bear)
  3. macd    — MACD histogram slope
  4. volume  — volume above 20-bar average AND confirmed by candle direction
  5. obv     — OBV 3-bar slope (computed inline)
  6. funding — funding rate sign (from fstream WS when available)

LONG  when total score >= +min_signals
SHORT when total score <= -min_signals
"""

import asyncio
import json
import threading
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import aiohttp

import config
import database
from futures_paper_client import FuturesPaperClient
from indicators import calc_ema, calc_rsi, calc_macd

# ── Module-level state ────────────────────────────────────────────────────────

_client: Optional[FuturesPaperClient] = None
_client_lock = threading.Lock()

_mark_prices: Dict[str, float]    = {}
_funding_rates: Dict[str, float]  = {}
_mark_lock = threading.Lock()

_futures_signal_cache: Dict[str, dict] = {}
_futures_signal_lock = threading.Lock()

_futures_active = True
_futures_settings: dict = {
    "leverage":          config.FUTURES_LEVERAGE,
    "budget_usdt":       config.FUTURES_BUDGET_USDT,
    "budget_mode":       "fixed",   # "fixed" | "percent"
    "budget_pct":        10.0,      # % of free balance when mode="percent"
    "take_profit_pct":   config.FUTURES_TAKE_PROFIT_PCT,
    "stop_loss_pct":     config.FUTURES_STOP_LOSS_PCT,
    "stop_loss_enabled": True,
    "min_signals":       2,         # of 6 — lowered for better trade frequency
    "max_positions":     config.FUTURES_MAX_POSITIONS,
}
_settings_lock = threading.Lock()

# Spot REST endpoint — identical kline format to fapi, reliably reachable
SPOT_API_BASE = "https://api.binance.com"
FSTREAM_WS    = "wss://fstream.binance.com/ws/!markPrice@arr@1s"


# ── Init ──────────────────────────────────────────────────────────────────────

def init_futures_engine():
    global _client
    with _client_lock:
        if _client is None:
            _client = FuturesPaperClient(
                starting_usdt=float(config.FUTURES_STARTING_USDT)
            )

    # Restore persisted settings so budget/leverage/etc survive Railway restarts
    saved_json = database.get_setting("futures_settings")
    if saved_json:
        try:
            saved = json.loads(saved_json)
            with _settings_lock:
                _futures_settings.update(saved)
            print(f"[FuturesEngine] Settings restored from DB: {saved}")
        except Exception as exc:
            print(f"[FuturesEngine] Settings restore error: {exc}")


# ── Public helpers ────────────────────────────────────────────────────────────

def get_futures_status() -> dict:
    with _client_lock:
        if _client is None:
            return {"running": False, "balance": 0.0, "equity": 0.0,
                    "positions": 0, "active": False}
        bal    = _client.get_balance()
        equity = _client.get_equity()
        n_pos  = _client.open_position_count()

    trades    = database.get_recent_futures_trades(1000)
    total_pnl = sum(t.get("net_profit") or 0.0 for t in trades)
    wins      = sum(1 for t in trades if (t.get("net_profit") or 0) > 0)
    win_rate  = (wins / len(trades) * 100) if trades else 0.0

    with _settings_lock:
        settings = dict(_futures_settings)

    return {
        "running":           _futures_active,
        "balance":           round(bal, 4),
        "equity":            round(equity, 4),
        "positions":         n_pos,
        "total_pnl":         round(total_pnl, 4),
        "win_rate":          round(win_rate, 1),
        "trade_count":       len(trades),
        "stop_loss_enabled": settings.get("stop_loss_enabled", True),
        "budget_mode":       settings.get("budget_mode", "fixed"),
        "budget_pct":        settings.get("budget_pct", 10.0),
    }


def get_futures_positions() -> List[dict]:
    with _client_lock:
        if _client is None:
            return []
        positions = _client.get_open_positions()

    result = []
    for p in positions:
        mp = _get_mark(p["symbol"])
        if mp <= 0:
            mp = p["entry_price"]   # fallback when WS not yet connected
        upnl = _upnl(p, mp)
        result.append({**p, "mark_price": mp, "unrealized_pnl": round(upnl, 4)})
    return result


def get_futures_signals() -> List[dict]:
    with _futures_signal_lock:
        return list(_futures_signal_cache.values())


def update_futures_settings(patch: dict):
    with _settings_lock:
        _futures_settings.update(patch)
        snap = dict(_futures_settings)
    # Persist immediately so changes survive restarts
    database.save_setting("futures_settings", json.dumps(snap))


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


# ── Internal helpers ──────────────────────────────────────────────────────────

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


def _calc_budget(settings: dict, balance: float) -> float:
    """Compute per-trade margin based on budget_mode setting."""
    if settings.get("budget_mode") == "percent":
        pct = float(settings.get("budget_pct", 10.0))
        val = balance * (pct / 100.0)
        return max(5.0, min(val, balance))
    return float(settings.get("budget_usdt", config.FUTURES_BUDGET_USDT))


# ── Signal scoring ─────────────────────────────────────────────────────────────

def _score_signals(
    closes: List[float],
    volumes: List[float],
    funding_rate: float,
) -> Tuple[int, dict]:
    if len(closes) < 26 or len(volumes) < 20:
        return 0, {}

    signals: dict = {}

    # 1. Trend: close vs EMA20
    emas  = calc_ema(closes, 20)
    ema20 = emas[-1] if emas else None
    if ema20 and ema20 > 0:
        ratio = closes[-1] / ema20
        if ratio > 1.0010:
            signals["trend"] = 1
        elif ratio < 0.9990:
            signals["trend"] = -1
        else:
            signals["trend"] = 0
    else:
        signals["trend"] = 0

    # 2. RSI
    rsi_vals = calc_rsi(closes, 14)
    rsi = next((v for v in reversed(rsi_vals) if v is not None), 50.0)
    if rsi > 60 and rsi < 80:
        signals["rsi"] = 1
    elif rsi < 40 and rsi > 20:
        signals["rsi"] = -1
    else:
        signals["rsi"] = 0

    # 3. MACD histogram slope
    try:
        _, _, histogram = calc_macd(closes)
        valid_h = [v for v in histogram if v is not None]
        if len(valid_h) >= 2:
            if valid_h[-1] > 0 and valid_h[-1] > valid_h[-2]:
                signals["macd"] = 1
            elif valid_h[-1] < 0 and valid_h[-1] < valid_h[-2]:
                signals["macd"] = -1
            else:
                signals["macd"] = 0
        else:
            signals["macd"] = 0
    except Exception:
        signals["macd"] = 0

    # 4. Volume + candle direction (5% above 20-bar average)
    vol_ma = sum(volumes[-20:]) / 20
    if vol_ma > 0 and volumes[-1] > vol_ma * 1.05:
        signals["volume"] = 1 if closes[-1] >= closes[-2] else -1
    else:
        signals["volume"] = 0

    # 5. OBV 3-bar slope (inline — avoids indicators.calc_obv dict requirement)
    obv = 0.0
    obv_series = [0.0]
    for i in range(1, len(closes)):
        if closes[i] > closes[i - 1]:
            obv += volumes[i]
        elif closes[i] < closes[i - 1]:
            obv -= volumes[i]
        obv_series.append(obv)

    if len(obv_series) >= 4:
        slope = obv_series[-1] - obv_series[-4]
        signals["obv"] = 1 if slope > 0 else (-1 if slope < 0 else 0)
    else:
        signals["obv"] = 0

    # 6. Funding rate (from fstream WS — 0 when WS not connected)
    if funding_rate < -0.00005:
        signals["funding"] = 1
    elif funding_rate > 0.00005:
        signals["funding"] = -1
    else:
        signals["funding"] = 0

    net_score = sum(signals.values())
    return net_score, signals


# ── REST kline fetch (spot API — reliable from Railway) ──────────────────────

async def _fetch_klines(
    session: aiohttp.ClientSession, symbol: str, limit: int = 60
) -> dict:
    """Fetch 1m candles using the spot REST endpoint (same format as fapi)."""
    closes, volumes = [], []
    try:
        async with session.get(
            f"{SPOT_API_BASE}/api/v3/klines",
            params={"symbol": symbol, "interval": "1m", "limit": limit},
            timeout=aiohttp.ClientTimeout(total=8),
        ) as resp:
            if resp.status == 200:
                data    = await resp.json()
                closes  = [float(row[4]) for row in data]
                volumes = [float(row[5]) for row in data]
            else:
                print(f"[FuturesEngine] klines {symbol}: HTTP {resp.status}")
    except Exception as exc:
        print(f"[FuturesEngine] klines error {symbol}: {exc}")

    return {"closes": closes, "volumes": volumes}


# ── Loop A — mark price WebSocket (best-effort) ───────────────────────────────

async def mark_price_loop():
    """Maintain live mark prices + funding rates from Binance fstream.

    The scanner loop works without this — it falls back to kline close prices.
    This loop improves TP/SL precision when it can connect.
    """
    watched = set(config.FUTURES_WATCHED_COINS)
    backoff = 2

    while True:
        try:
            import websockets
            async with websockets.connect(
                FSTREAM_WS, ping_interval=20, ping_timeout=30
            ) as ws:
                backoff = 2
                print("[FuturesEngine] Loop A: fstream WS connected")
                async for raw in ws:
                    try:
                        msgs = json.loads(raw)
                        if not isinstance(msgs, list):
                            msgs = [msgs]
                        with _mark_lock:
                            for m in msgs:
                                sym = m.get("s", "")
                                if sym not in watched:
                                    continue
                                mp = float(m.get("p") or m.get("mp") or 0)
                                fr = float(m.get("r") or 0)
                                if mp > 0:
                                    _mark_prices[sym] = mp
                                _funding_rates[sym] = fr
                        # Push to client for continuous TP/SL monitoring
                        with _client_lock:
                            if _client:
                                with _mark_lock:
                                    for sym in watched:
                                        mp = _mark_prices.get(sym, 0)
                                        if mp > 0:
                                            _client.update_mark_price(sym, mp)
                    except Exception:
                        pass
        except Exception as exc:
            print(f"[FuturesEngine] Loop A reconnect in {backoff}s: {exc}")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


# ── Loop B — signal scanner + position manager ────────────────────────────────

async def signal_scanner_loop():
    """Score every watched coin every FUTURES_SCAN_INTERVAL_SEC seconds."""
    await asyncio.sleep(15)   # give server time to fully start

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
    if not _futures_active:
        return

    with _client_lock:
        client = _client
    if client is None:
        return

    with _settings_lock:
        settings   = dict(_futures_settings)
        sl_enabled = settings.get("stop_loss_enabled", True)

    min_sig  = int(settings.get("min_signals",       2))
    max_pos  = int(settings.get("max_positions",     config.FUTURES_MAX_POSITIONS))
    leverage = int(settings.get("leverage",          config.FUTURES_LEVERAGE))
    tp_pct   = float(settings.get("take_profit_pct", config.FUTURES_TAKE_PROFIT_PCT))
    sl_pct   = float(settings.get("stop_loss_pct",   config.FUTURES_STOP_LOSS_PCT))
    balance  = client.get_balance()
    budget   = _calc_budget(settings, balance)

    # ── TP/SL/Liquidation check first (uses mark prices updated by scanner) ──
    closed = client.check_positions(sl_enabled=sl_enabled)
    for t in closed:
        database.log_activity(
            f"[Futures] CLOSE {t['direction']} {t['symbol']} "
            f"@ {t['exit_price']:.4f} pnl={t['net_profit']:+.4f} USDT", "info"
        )

    # ── Scan all coins ────────────────────────────────────────────────────────
    async with aiohttp.ClientSession() as session:
        for symbol in config.FUTURES_WATCHED_COINS:
            try:
                await _scan_symbol(
                    session, client, symbol,
                    min_sig, max_pos, leverage, budget,
                    tp_pct, sl_pct, sl_enabled,
                )
            except Exception as exc:
                print(f"[FuturesEngine] scan error {symbol}: {exc}")
            await asyncio.sleep(0.1)   # rate-limit REST calls


async def _scan_symbol(
    session,
    client: FuturesPaperClient,
    symbol: str,
    min_sig: int,
    max_pos: int,
    leverage: int,
    budget: float,
    tp_pct: float,
    sl_pct: float,
    sl_enabled: bool,
):
    data    = await _fetch_klines(session, symbol)
    closes  = data["closes"]
    volumes = data["volumes"]

    if len(closes) < 26:
        return

    # Mark price: prefer live fstream, fall back to latest kline close.
    # Always push into the client so check_positions() has a non-zero price.
    with _mark_lock:
        mark_price   = _mark_prices.get(symbol, 0.0)
        funding_rate = _funding_rates.get(symbol, 0.0)

    if mark_price <= 0:
        mark_price = closes[-1]

    client.update_mark_price(symbol, mark_price)

    score, signals = _score_signals(closes, volumes, funding_rate)

    with _futures_signal_lock:
        _futures_signal_cache[symbol] = {
            "symbol":       symbol,
            "mark_price":   mark_price,
            "score":        score,
            "funding_rate": round(funding_rate * 100, 6),
            "signals":      signals,
            "timestamp":    datetime.now(timezone.utc).isoformat(),
        }

    n_open = client.open_position_count()

    if score >= min_sig and n_open < max_pos:
        if not client.has_open_position(symbol, "LONG"):
            pos = client.open_position(
                symbol, "LONG", mark_price, budget, leverage,
                tp_pct, sl_pct, sl_enabled,
            )
            if pos:
                database.log_activity(
                    f"[Futures] OPEN LONG {symbol} @ {mark_price:.4f} "
                    f"score={score}/{min_sig} margin={budget:.0f} lev={leverage}x "
                    f"TP={pos['take_profit']:.4f}", "info",
                )

    elif score <= -min_sig and n_open < max_pos:
        if not client.has_open_position(symbol, "SHORT"):
            pos = client.open_position(
                symbol, "SHORT", mark_price, budget, leverage,
                tp_pct, sl_pct, sl_enabled,
            )
            if pos:
                database.log_activity(
                    f"[Futures] OPEN SHORT {symbol} @ {mark_price:.4f} "
                    f"score={score}/{min_sig} margin={budget:.0f} lev={leverage}x "
                    f"TP={pos['take_profit']:.4f}", "info",
                )
