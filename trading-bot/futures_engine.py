"""
Futures paper-trading engine — two async coroutines:

  Loop A  mark_price_loop()     — WebSocket mark price feed from Binance fstream
  Loop B  signal_scanner_loop() — scores coins every 60 s, opens/closes positions

Signal system (6 signals, scored -1 / 0 / +1 each):
  1. trend   — close vs EMA20 (with 0.1 % dead-band to avoid noise)
  2. rsi     — RSI direction relative to 50, guards for OB/OS extremes
  3. macd    — MACD histogram slope (rising positive or falling negative)
  4. volume  — volume above 20-bar average AND confirmed by candle direction
  5. obv     — OBV 3-bar slope (computed inline — indicators.calc_obv uses dicts)
  6. funding — funding rate sign (negative = shorts paying = LONG-favourable)

LONG  when total score >= +min_signals
SHORT when total score <= -min_signals
"""

import asyncio
import json
import math
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

_mark_prices: Dict[str, float] = {}
_funding_rates: Dict[str, float] = {}
_mark_lock = threading.Lock()

_futures_signal_cache: Dict[str, dict] = {}
_futures_signal_lock = threading.Lock()

_futures_active = True
_futures_settings: dict = {
    "leverage":          config.FUTURES_LEVERAGE,
    "budget_usdt":       config.FUTURES_BUDGET_USDT,
    "take_profit_pct":   config.FUTURES_TAKE_PROFIT_PCT,
    "stop_loss_pct":     config.FUTURES_STOP_LOSS_PCT,
    "stop_loss_enabled": True,
    "min_signals":       config.FUTURES_MIN_SIGNALS,
    "max_positions":     config.FUTURES_MAX_POSITIONS,
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

    trades    = database.get_recent_futures_trades(1000)
    total_pnl = sum(t.get("net_profit") or 0.0 for t in trades)
    wins      = sum(1 for t in trades if (t.get("net_profit") or 0) > 0)
    win_rate  = (wins / len(trades) * 100) if trades else 0.0

    with _settings_lock:
        sl_enabled = _futures_settings.get("stop_loss_enabled", True)

    return {
        "running":            _futures_active,
        "balance":            round(bal, 4),
        "equity":             round(equity, 4),
        "positions":          n_pos,
        "total_pnl":          round(total_pnl, 4),
        "win_rate":           round(win_rate, 1),
        "trade_count":        len(trades),
        "stop_loss_enabled":  sl_enabled,
    }


def get_futures_positions() -> List[dict]:
    with _client_lock:
        if _client is None:
            return []
        positions = _client.get_open_positions()

    result = []
    for p in positions:
        mp   = _get_mark(p["symbol"])
        upnl = _upnl(p, mp)
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


# ── Signal scoring ─────────────────────────────────────────────────────────────

def _score_signals(
    closes: List[float],
    volumes: List[float],
    funding_rate: float,
) -> Tuple[int, dict]:
    """Return (net_score, signals_dict).

    Each signal contributes -1 (bearish), 0 (neutral), or +1 (bullish).
    net_score > 0 → bullish bias,  net_score < 0 → bearish bias.

    Key fixes vs. v1:
      - OBV computed inline (indicators.calc_obv takes candle dicts, not lists)
      - MACD None values filtered before comparison
      - RSI neutral band widened to 40–60 so more signals fire
    """
    if len(closes) < 26 or len(volumes) < 20:
        return 0, {}

    signals: dict = {}

    # ── 1. Trend: close vs EMA20 ──────────────────────────────────────────────
    emas = calc_ema(closes, 20)
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

    # ── 2. RSI ────────────────────────────────────────────────────────────────
    rsi_vals = calc_rsi(closes, 14)
    rsi = next((v for v in reversed(rsi_vals) if v is not None), 50.0)
    if rsi > 60 and rsi < 80:       # clearly bullish, not yet overbought
        signals["rsi"] = 1
    elif rsi < 40 and rsi > 20:     # clearly bearish, not yet oversold
        signals["rsi"] = -1
    else:
        signals["rsi"] = 0          # neutral zone 40–60 or extreme OB/OS

    # ── 3. MACD histogram ────────────────────────────────────────────────────
    try:
        _, _, histogram = calc_macd(closes)
        # histogram contains None for early values — get last two valid ones
        valid_h = [v for v in histogram if v is not None]
        if len(valid_h) >= 2:
            if valid_h[-1] > 0 and valid_h[-1] > valid_h[-2]:
                signals["macd"] = 1    # rising positive histogram
            elif valid_h[-1] < 0 and valid_h[-1] < valid_h[-2]:
                signals["macd"] = -1   # falling negative histogram
            else:
                signals["macd"] = 0
        else:
            signals["macd"] = 0
    except Exception:
        signals["macd"] = 0

    # ── 4. Volume + candle direction ─────────────────────────────────────────
    vol_ma = sum(volumes[-20:]) / 20
    if vol_ma > 0 and volumes[-1] > vol_ma * 1.05:   # 5 % above avg
        signals["volume"] = 1 if closes[-1] >= closes[-2] else -1
    else:
        signals["volume"] = 0

    # ── 5. OBV slope (computed inline — avoids calc_obv's dict requirement) ──
    obv = 0.0
    obv_series = [0.0]
    for i in range(1, len(closes)):
        if closes[i] > closes[i - 1]:
            obv += volumes[i]
        elif closes[i] < closes[i - 1]:
            obv -= volumes[i]
        obv_series.append(obv)

    if len(obv_series) >= 4:
        slope = obv_series[-1] - obv_series[-4]   # 3-bar slope
        if slope > 0:
            signals["obv"] = 1
        elif slope < 0:
            signals["obv"] = -1
        else:
            signals["obv"] = 0
    else:
        signals["obv"] = 0

    # ── 6. Funding rate ───────────────────────────────────────────────────────
    if funding_rate < -0.00005:     # shorts paying longs → LONG-friendly
        signals["funding"] = 1
    elif funding_rate > 0.00005:    # longs paying shorts → SHORT-friendly
        signals["funding"] = -1
    else:
        signals["funding"] = 0

    net_score = sum(signals.values())
    return net_score, signals


# ── REST kline + funding fetch ────────────────────────────────────────────────

async def _fetch_klines(
    session: aiohttp.ClientSession, symbol: str, limit: int = 60
) -> dict:
    """Fetch 1m klines + latest funding rate for one futures symbol."""
    closes, volumes, funding_rate = [], [], 0.0

    try:
        async with session.get(
            f"{FAPI_BASE}/fapi/v1/klines",
            params={"symbol": symbol, "interval": "1m", "limit": limit},
            timeout=aiohttp.ClientTimeout(total=8),
        ) as resp:
            if resp.status == 200:
                data = await resp.json()
                closes  = [float(row[4]) for row in data]
                volumes = [float(row[5]) for row in data]
    except Exception as e:
        print(f"[FuturesEngine] klines error {symbol}: {e}")

    try:
        async with session.get(
            f"{FAPI_BASE}/fapi/v1/fundingRate",
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
    """Maintain a live mark price + funding rate dict from Binance fstream."""
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
                        # Forward to client for TP/SL monitoring
                        with _client_lock:
                            if _client:
                                with _mark_lock:
                                    for m in msgs:
                                        sym = m.get("s", "")
                                        mp  = _mark_prices.get(sym, 0)
                                        if mp > 0:
                                            _client.update_mark_price(sym, mp)
                    except Exception:
                        pass
        except Exception as exc:
            print(f"[FuturesEngine] Loop A error: {exc} — reconnect in {backoff}s")
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


# ── Loop B — signal scanner + position manager ────────────────────────────────

async def signal_scanner_loop():
    """Score every watched coin every FUTURES_SCAN_INTERVAL_SEC seconds."""
    await asyncio.sleep(20)   # let Loop A warm up

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
        settings      = dict(_futures_settings)
        sl_enabled    = settings.get("stop_loss_enabled", True)

    # ── TP/SL/Liquidation check ────────────────────────────────────────────────
    closed = client.check_positions(sl_enabled=sl_enabled)
    for t in closed:
        database.log_activity(
            f"[Futures] CLOSE {t['direction']} {t['symbol']} "
            f"@ {t['exit_price']:.4f} pnl={t['net_profit']:+.4f} USDT", "info"
        )

    min_sig  = int(settings.get("min_signals",  config.FUTURES_MIN_SIGNALS))
    max_pos  = int(settings.get("max_positions", config.FUTURES_MAX_POSITIONS))
    leverage = int(settings.get("leverage",      config.FUTURES_LEVERAGE))
    budget   = float(settings.get("budget_usdt", config.FUTURES_BUDGET_USDT))
    tp_pct   = float(settings.get("take_profit_pct", config.FUTURES_TAKE_PROFIT_PCT))
    sl_pct   = float(settings.get("stop_loss_pct",   config.FUTURES_STOP_LOSS_PCT))

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
            await asyncio.sleep(0.15)


async def _scan_symbol(
    session, client, symbol: str,
    min_sig: int, max_pos: int,
    leverage: int, budget: float,
    tp_pct: float, sl_pct: float,
    sl_enabled: bool,
):
    data    = await _fetch_klines(session, symbol)
    closes  = data["closes"]
    volumes = data["volumes"]

    if len(closes) < 26:
        return

    with _mark_lock:
        mark_price   = _mark_prices.get(symbol, closes[-1])
        funding_rate = _funding_rates.get(symbol, data["funding_rate"])

    score, signals = _score_signals(closes, volumes, funding_rate)

    snapshot = {
        "symbol":       symbol,
        "mark_price":   mark_price,
        "score":        score,
        "funding_rate": round(funding_rate * 100, 6),
        "signals":      signals,
        "timestamp":    datetime.now(timezone.utc).isoformat(),
    }
    with _futures_signal_lock:
        _futures_signal_cache[symbol] = snapshot

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
                    f"TP={pos['take_profit']:.4f} SL={'OFF' if not sl_enabled else pos['stop_loss']:.4f}",
                    "info",
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
                    f"TP={pos['take_profit']:.4f} SL={'OFF' if not sl_enabled else pos['stop_loss']:.4f}",
                    "info",
                )
