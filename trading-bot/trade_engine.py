"""
Trade execution engine — unified real-time architecture.

realtime_monitor:  Called on every WebSocket trade tick (~100 ms).
                   Handles both stop-loss/take-profit exits AND buy entry.
                   Buys read from an in-memory signal cache (no I/O per tick).

signal_scanner:    Async coroutine, runs every SCAN_INTERVAL_SEC (60 s).
                   Only refreshes the signal cache from REST / DB.
                   Does NOT execute trades — buy execution is in realtime_monitor.

update_coin_signals: Called by data_collector on every 1-minute kline close
                   (WebSocket-driven). Keeps the signal cache fresh between
                   REST refreshes.
"""

import asyncio
import json
import os
import time
import math
import threading
from datetime import datetime
from typing import Dict, List, Optional

import aiohttp

import config
import database
import indicators
import learning
from connection import client, get_mode

_positions: List[dict] = []
_positions_lock = threading.Lock()

_strategy_mtime: float = 0.0
_strategy_cache: dict = {}

_fee_rate = config.FEE_RATE_BNB if config.BNB_FEE_MODE else config.FEE_RATE_STANDARD
_breakeven_mult = 1.0 + _fee_rate + _fee_rate   # 1.0015 with BNB, 1.002 without

_cooldowns: dict = {}  # symbol -> unix timestamp when cooldown expires

# ── Real-time signal cache — updated on every kline close ────────────────────
# Maps symbol → {"signals": dict, "score": int, "price": float}
_signal_cache: Dict[str, dict] = {}
_signal_cache_lock = threading.Lock()
_last_buy_check: float = 0.0   # throttle: don't call get_account() on every tick


# ── Balance guard + budget helpers ───────────────────────────────────────────

def can_execute_buy(coin_cfg: dict, client) -> tuple[bool, str]:
    """
    Final pre-buy safety guard.
    Reads the shared _positions list directly (never a copy) so that every
    position appended since bot start is visible here.
    """
    budget = coin_cfg.get("budget_usdt", config.BUDGET_FIXED_USDT)
    try:
        account = client.get_account()
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
        total_open     = len(_positions)

    if len(open_this_coin) >= max_concurrent:
        return False, f"Max concurrent for {sym} reached ({max_concurrent})"

    if total_open >= config.MAX_OPEN_POSITIONS:
        return False, "Global max positions reached"

    return True, ""


def get_budget_for_coin(symbol: str, free_usdt: float) -> float:
    """Return trade size in USDT based on BUDGET_MODE (config defaults or strategy.json overrides)."""
    strategy = _load_strategy()
    mode = strategy.get("budget_mode", config.BUDGET_MODE)

    if mode == "fixed":
        return float(strategy.get("budget_fixed_usdt", config.BUDGET_FIXED_USDT))

    elif mode == "percent":
        pct = float(strategy.get("budget_pct_of_free", config.BUDGET_PCT_OF_FREE))
        return round(free_usdt * (pct / 100), 2)

    elif mode == "capped":
        cap = float(strategy.get("budget_total_cap_usdt", config.BUDGET_TOTAL_CAP_USDT))
        with _positions_lock:
            total_in_positions = sum(p["budget_usdt"] for p in _positions)
        remaining_cap = cap - total_in_positions
        per_trade = cap / config.MAX_OPEN_POSITIONS
        return min(per_trade, max(0.0, remaining_cap))

    elif mode == "per_coin":
        per_coin = strategy.get("budget_per_coin", config.BUDGET_PER_COIN)
        return float(per_coin.get(symbol, config.BUDGET_FIXED_USDT))

    return config.BUDGET_FIXED_USDT


# ── Cooldown helpers ──────────────────────────────────────────────────────────

def _set_cooldown(symbol: str):
    _cooldowns[symbol] = time.time() + config.COOLDOWN_AFTER_LOSS


def _in_cooldown(symbol: str) -> bool:
    exp = _cooldowns.get(symbol, 0)
    return time.time() < exp


# ── DB / startup helpers ──────────────────────────────────────────────────────

def load_positions_from_db():
    """Called on startup — restores open positions from SQLite."""
    global _positions
    rows = database.load_positions()
    with _positions_lock:
        _positions = list(rows)
    print(f"[TradeEngine] Loaded {len(_positions)} open position(s) from DB.")


def get_open_positions() -> List[dict]:
    with _positions_lock:
        return list(_positions)


# ── Strategy loader ───────────────────────────────────────────────────────────

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


# ── Account helpers ───────────────────────────────────────────────────────────

def _get_usdt_balance() -> float:
    try:
        acc = client.get_account()
        for b in acc["balances"]:
            if b["asset"] == "USDT":
                return float(b["free"])
    except Exception:
        pass
    return 0.0


def _floor_qty(qty: float, decimals: int = 6) -> float:
    """Floor quantity to avoid Binance LOT_SIZE precision errors."""
    factor = 10 ** decimals
    return math.floor(qty * factor) / factor


# ── Indicator helpers (derive from candle dict) ───────────────────────────────

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


# ── Signal evaluation (Bug 2/3: extracted function, explicit 4-key dict) ─────

def evaluate_signals(closes: list, volumes: list) -> dict:
    """
    Evaluate all four technical signals on the last COMPLETED candle (index -2).
    Returns exactly {"trend": bool, "rsi": bool, "macd": bool, "volume": bool}.
    RSI bounds are config.RSI_BUY_MIN and config.RSI_BUY_MAX — never hardcoded.
    """
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
    # RSI_BUY_MIN <= rsi <= RSI_BUY_MAX — both constants from config, never hardcoded
    rsi = bool(
        rsi_vals[-2] is not None
        and config.RSI_BUY_MIN <= rsi_vals[-2] <= config.RSI_BUY_MAX
    )
    macd = bool(
        histo[-2] is not None
        and histo[-3] is not None
        and histo[-2] > 0
        and histo[-2] > histo[-3]
    )
    volume = bool(
        volumes[-2] is not None
        and vol_ma[-2] is not None
        and volumes[-2] > vol_ma[-2] * config.VOLUME_RATIO_MIN
    )

    return {"trend": trend, "rsi": rsi, "macd": macd, "volume": volume}


def update_coin_signals(symbol: str, closes: list, volumes: list):
    """
    Update the in-memory signal cache for one coin.
    Called by data_collector on every 1-minute kline close (WebSocket-driven)
    and by signal_scanner every SCAN_INTERVAL_SEC as a REST-backup.
    Requires at least 27 candles; silently skips if fewer.
    """
    if len(closes) < 27:
        return
    try:
        signals = evaluate_signals(closes, volumes)
        score   = sum(signals.values())
        with _signal_cache_lock:
            _signal_cache[symbol] = {
                "signals": signals,
                "score":   score,
                "price":   closes[-1],
            }
    except Exception as e:
        print(f"[TradeEngine] Signal cache error {symbol}: {e}")


# ── Shared sell execution (used by both realtime_monitor and signal_scanner) ──

def _execute_sell(pos: dict, price: float, reason: str):
    """Execute a market sell for pos at price. Logs trade, cleans up state."""
    sym  = pos["symbol"]
    qty  = _floor_qty(pos["quantity"])
    mode = get_mode()
    now  = datetime.utcnow().isoformat()

    if qty <= 0 or price <= 0:
        return

    try:
        result = client.order_market_sell(symbol=sym, quantity=qty)
    except Exception as e:
        print(f"[TradeEngine] SELL failed {sym}: {e}")
        return

    fill_price = float(result["fills"][0]["price"]) if result.get("fills") else price
    sell_fee   = float(result["fills"][0]["commission"]) * fill_price if result.get("fills") else (qty * fill_price * _fee_rate)
    buy_fee    = pos["budget_usdt"] * _fee_rate
    gross      = qty * fill_price
    net_profit = gross - sell_fee - pos["budget_usdt"] - buy_fee

    buy_ts  = pos.get("timestamp", now)
    sell_ts = now
    try:
        buy_dt  = datetime.fromisoformat(buy_ts)
        sell_dt = datetime.fromisoformat(sell_ts)
        duration = int((sell_dt - buy_dt).total_seconds())
    except Exception:
        duration = 0
        buy_dt   = datetime.utcnow()

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
    }
    database.log_trade(trade_record)
    learning.learn_from_trade(trade_record)

    if pos.get("id"):
        database.delete_position(pos["id"])
    with _positions_lock:
        _positions[:] = [p for p in _positions if p.get("id") != pos.get("id")]

    usdt_received = gross - sell_fee
    pnl_sign      = "+" if net_profit >= 0 else ""
    print(
        f"[{sell_ts}] SELL {sym} @ {fill_price:.4f} USDT "
        f"· {usdt_received:.4f} USDT · P&L: {pnl_sign}{net_profit:.4f} USDT"
        f"  ({reason}, held {duration}s)"
    )


# ── Real-time buy check from signal cache (called from realtime_monitor) ──────

def _check_buys_from_cache(prices: Dict[str, float]):
    """
    Real-time buy executor. Reads the pre-computed signal cache — zero I/O per
    call except when a buy is actually about to fire. Throttled to at most once
    every 3 s to avoid hammering get_account() on every WebSocket tick.
    """
    global _last_buy_check

    # Fast pre-check: any coin signalling BUY? (no lock needed for scalar read)
    with _signal_cache_lock:
        any_ready = any(v["score"] >= config.MIN_SIGNALS_TO_BUY for v in _signal_cache.values())
    if not any_ready:
        return

    # Throttle: don't call get_account() faster than every 3 s
    now = time.time()
    if now - _last_buy_check < 3.0:
        return
    _last_buy_check = now

    strategy = _load_strategy()
    if not strategy.get("trading_active", True):
        return

    approved = {
        c["symbol"]: c
        for c in strategy.get("approved_coins", [])
        if c.get("approved")
    }
    global_max = strategy.get("global_max_positions", config.MAX_OPEN_POSITIONS)

    with _positions_lock:
        if len(_positions) >= global_max:
            return

    usdt_balance = _get_usdt_balance()
    ts_now = datetime.utcnow().isoformat()
    mode   = get_mode()

    with _signal_cache_lock:
        cache_snapshot = dict(_signal_cache)

    for sym, cached in cache_snapshot.items():
        if sym not in approved:
            continue
        if cached["score"] < config.MIN_SIGNALS_TO_BUY:
            continue
        if _in_cooldown(sym):
            continue

        with _positions_lock:
            already_held = any(p["symbol"] == sym for p in _positions)
            global_open  = len(_positions)

        if already_held or global_open >= global_max:
            continue

        budget = get_budget_for_coin(sym, usdt_balance)
        if budget <= 0:
            continue

        price = prices.get(sym) or cached["price"]
        if not price:
            continue

        client.update_price(sym, price)

        buy_cfg = {**approved[sym], "symbol": sym, "budget_usdt": budget}
        allowed, reason = can_execute_buy(buy_cfg, client)
        if not allowed:
            database.log_activity(f"{sym}: buy skipped — {reason}", "info")
            continue

        try:
            result = client.order_market_buy(symbol=sym, quoteOrderQty=budget)
        except Exception as e:
            print(f"[RealtimeBuy] BUY failed {sym}: {e}")
            database.log_activity(f"{sym}: BUY failed — {e}", "error")
            continue

        fill       = result.get("fills", [{}])[0]
        fill_price = float(fill.get("price", price))
        qty        = float(result.get("executedQty", 0))
        if qty <= 0:
            continue

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

        pos_record = {
            "symbol":             sym,
            "entry_price":        fill_price,
            "quantity":           qty,
            "budget_usdt":        budget,
            "timestamp":          ts_now,
            "mode":               mode,
            "entry_rsi":          entry_rsi,
            "entry_ma_position":  entry_ma,
            "entry_bb_position":  entry_bb,
            "entry_volume_trend": entry_vol,
        }
        pos_id = database.save_position(pos_record)
        pos_record["id"] = pos_id

        with _positions_lock:
            _positions.append(pos_record)

        usdt_balance -= budget + budget * _fee_rate
        global_open  += 1

        score     = cached["score"]
        breakeven = fill_price * _breakeven_mult
        msg = (
            f"BOUGHT {sym} @ ${fill_price:.4f} "
            f"| qty={qty:.6f} | BEP=${breakeven:.4f} "
            f"| signals={score}/4"
        )
        print(f"[RealtimeBuy] {msg}")
        database.log_activity(msg, "info")

        if global_open >= global_max:
            break


# ── Process 1: realtime monitor (called on every WebSocket tick) ──────────────

def realtime_monitor(prices: Dict[str, float]):
    """
    Synchronous. Called by data_collector on EVERY WebSocket 'trade' event.
    Handles both exits (take-profit / stop-loss) and entries (via signal cache).
    Both are purely in-memory — no kline fetches on the hot path.
    """
    with _positions_lock:
        snapshot = list(_positions)

    for pos in snapshot:
        sym   = pos["symbol"]
        price = prices.get(sym, 0.0)
        if price <= 0:
            continue

        entry     = pos["entry_price"]
        breakeven = entry * _breakeven_mult
        stop_loss = entry * (1 - config.STOP_LOSS_PCT)

        if price > breakeven:
            _execute_sell(pos, price, "take-profit")
        elif price <= stop_loss:
            _execute_sell(pos, price, "stop-loss")
            _set_cooldown(sym)

    # Real-time buy check — throttled, reads from signal cache only
    _check_buys_from_cache(prices)


# ── Process 2: signal scanner (async, runs every SCAN_INTERVAL_SEC) ──────────

async def signal_scanner(prices: dict):
    """
    Async coroutine — runs every SCAN_INTERVAL_SEC (60 s).
    Only refreshes the signal cache from REST / DB.
    Actual buy execution happens in realtime_monitor via _check_buys_from_cache.
    """
    while True:
        try:
            await _refresh_signal_cache()
        except Exception as e:
            print(f"[SignalScanner] Unexpected error: {e}")

        await asyncio.sleep(config.SCAN_INTERVAL_SEC)


_KLINE_BASES = [
    "https://api.binance.com",
    "https://api1.binance.com",
    "https://api2.binance.com",
    "https://api3.binance.com",
    "https://api4.binance.com",
]


async def _fetch_klines(session: aiohttp.ClientSession, sym: str) -> list:
    """Try each Binance base URL to work around regional geo-blocks (HTTP 451)."""
    last_exc: Exception = RuntimeError("No Binance bases configured")
    for base in _KLINE_BASES:
        url = f"{base}/api/v3/klines?symbol={sym}&interval=1m&limit=50"
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as resp:
                resp.raise_for_status()
                return await resp.json()
        except Exception as e:
            last_exc = e
    raise last_exc


async def _refresh_signal_cache():
    """
    Refresh the in-memory signal cache from REST klines (or DB fallback).
    Called every SCAN_INTERVAL_SEC as a backup to WebSocket-driven updates.
    Never executes trades — buy execution is in realtime_monitor.
    """
    strategy = _load_strategy()
    if not strategy:
        return

    approved_coins = [
        c["symbol"]
        for c in strategy.get("approved_coins", [])
        if c.get("approved")
    ]
    if not approved_coins:
        return

    db_ready = sum(
        1 for s in approved_coins
        if len(database.get_candles(s, config.CANDLE_TIMEFRAME, limit=27)) >= 27
    )
    database.log_activity(
        f"Signal refresh — {len(approved_coins)} coins | candles ready: {db_ready}/{len(approved_coins)}",
        "info",
    )

    updated = 0
    async with aiohttp.ClientSession() as session:
        for sym in approved_coins:
            closes = volumes = None
            try:
                raw = await _fetch_klines(session, sym)
                closes  = [float(k[4]) for k in raw]
                volumes = [float(k[5]) for k in raw]
            except Exception:
                db_rows = database.get_candles(sym, config.CANDLE_TIMEFRAME, limit=50)
                if len(db_rows) >= 27:
                    closes  = [float(c["close"])  for c in db_rows]
                    volumes = [float(c["volume"]) for c in db_rows]

            if closes and len(closes) >= 27:
                update_coin_signals(sym, closes, volumes)
                updated += 1

    with _signal_cache_lock:
        ready = sum(1 for v in _signal_cache.values() if v["score"] >= config.MIN_SIGNALS_TO_BUY)
    print(f"[SignalScanner] Cache refreshed {updated}/{len(approved_coins)} coins — {ready} signalling BUY")
