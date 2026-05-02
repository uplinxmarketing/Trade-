"""
Trade execution engine — two-speed architecture.

Process 1 (realtime_monitor):  Called on every WebSocket trade tick.
                                 Only handles stop-loss / take-profit exits.

Process 2 (signal_scanner):    Async coroutine, runs every SCAN_INTERVAL_SEC.
                                 Reads fresh 1m klines, evaluates 4 signals,
                                 and opens new positions when enough are bullish.
"""

import asyncio
import json
import os
import time
import math
import threading
import urllib.request
from datetime import datetime
from typing import Dict, List, Optional

import config
import database
import indicators
import learning
from connection import client, get_mode

_positions: List[dict] = []       # {id, symbol, entry_price, quantity, budget_usdt, timestamp, entry_rsi, entry_ma_position, entry_bb_position, entry_volume_trend}
_positions_lock = threading.Lock()

_strategy_mtime: float = 0.0
_strategy_cache: dict = {}

_fee_rate = config.FEE_RATE_BNB if config.BNB_FEE_MODE else config.FEE_RATE_STANDARD
_breakeven_mult = 1.0 + _fee_rate + _fee_rate   # 1.0015 with BNB, 1.002 without

_cooldowns: dict = {}  # symbol -> unix timestamp when cooldown expires


# ── Balance guard + budget helpers ───────────────────────────────────────────

def can_execute_buy(coin_cfg: dict, positions: list, client) -> tuple[bool, str]:
    """Final pre-buy safety guard. Fetches live account balance to verify funds."""
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
    open_this_coin = [p for p in positions if p["symbol"] == sym]
    max_concurrent = int(coin_cfg.get("max_concurrent", 1))
    if len(open_this_coin) >= max_concurrent:
        return False, f"Max concurrent for {sym} reached ({max_concurrent})"

    if len(positions) >= config.MAX_OPEN_POSITIONS:
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

    profit_sign = "+" if net_profit >= 0 else ""
    print(
        f"[TradeEngine] SOLD ({reason}) {sym} @ ${fill_price:.4f} "
        f"| net {profit_sign}${net_profit:.4f} | held {duration}s"
    )


# ── Process 1: realtime monitor (called on every WebSocket tick) ──────────────

def realtime_monitor(prices: Dict[str, float]):
    """
    Synchronous. Called on every WebSocket trade tick.
    Handles take-profit and stop-loss exits only — no new buys.
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


# ── Process 2: signal scanner (async, runs every SCAN_INTERVAL_SEC) ──────────

async def signal_scanner(prices: dict):
    """
    Async coroutine. Loops forever with asyncio.sleep(SCAN_INTERVAL_SEC).
    Fetches fresh 1m klines, evaluates 4 technical signals, and opens
    new positions when MIN_SIGNALS_TO_BUY or more are bullish.
    """
    while True:
        try:
            await _run_signal_scan(prices)
        except Exception as e:
            print(f"[SignalScanner] Unexpected error in scan loop: {e}")

        await asyncio.sleep(config.SCAN_INTERVAL_SEC)


async def _run_signal_scan(prices: dict):
    strategy = _load_strategy()
    if not strategy:
        return
    if not strategy.get("trading_active", True):
        return

    approved_coins = {
        c["symbol"]: c
        for c in strategy.get("approved_coins", [])
        if c.get("approved")
    }
    global_max = strategy.get("global_max_positions", config.MAX_OPEN_POSITIONS)
    mode       = get_mode()
    now        = datetime.utcnow().isoformat()

    with _positions_lock:
        global_open = len(_positions)

    if global_open >= global_max:
        return

    usdt_balance = _get_usdt_balance()

    for sym, coin_cfg in approved_coins.items():
        # Gate checks
        if _in_cooldown(sym):
            print(f"[SignalScanner] {sym} is in cooldown — skipping.")
            continue

        with _positions_lock:
            already_held = any(p["symbol"] == sym for p in _positions)
        if already_held:
            continue

        with _positions_lock:
            global_open = len(_positions)
        if global_open >= global_max:
            break

        budget = get_budget_for_coin(sym, usdt_balance)
        if budget <= 0:
            continue

        # Fetch 1m klines via REST
        try:
            url = (
                f"https://api.binance.com/api/v3/klines"
                f"?symbol={sym}&interval=1m&limit=50"
            )
            with urllib.request.urlopen(url, timeout=8) as resp:
                raw_klines = json.loads(resp.read())
        except Exception as e:
            print(f"[SignalScanner] {sym}: kline fetch failed — {e}")
            continue

        try:
            closes  = [float(k[4]) for k in raw_klines]
            volumes = [float(k[5]) for k in raw_klines]

            if len(closes) < 27:   # need at least slow EMA period + a couple candles
                print(f"[SignalScanner] {sym}: not enough candles ({len(closes)}) — skipping.")
                continue

            ema9      = indicators.calc_ema(closes, 9)
            ema21     = indicators.calc_ema(closes, 21)
            rsi       = indicators.calc_rsi(closes, 14)
            _, _, histogram = indicators.calc_macd(closes)
            vol_ma    = indicators.calc_volume_ma(volumes, 20)

            # Use index -2 (last completed candle, not the still-open one)
            trend_bullish = (
                ema9[-2] is not None
                and ema21[-2] is not None
                and ema9[-2] > ema21[-2]
            )
            rsi_bullish = (
                rsi[-2] is not None
                and config.RSI_BUY_MIN <= rsi[-2] <= config.RSI_BUY_MAX
            )
            macd_bullish = (
                histogram[-2] is not None
                and histogram[-3] is not None
                and histogram[-2] > 0
                and histogram[-2] > histogram[-3]
            )
            vol_bullish = (
                volumes[-2] is not None
                and vol_ma[-2] is not None
                and volumes[-2] > vol_ma[-2] * config.VOLUME_RATIO_MIN
            )

            bullish_count = sum([trend_bullish, rsi_bullish, macd_bullish, vol_bullish])

            print(
                f"[SignalScanner] {sym} signals — "
                f"trend={trend_bullish} rsi={rsi_bullish} "
                f"macd={macd_bullish} vol={vol_bullish} "
                f"→ {bullish_count}/4 bullish"
            )

            if bullish_count < config.MIN_SIGNALS_TO_BUY:
                continue

        except Exception as e:
            print(f"[SignalScanner] {sym}: indicator error — {e}")
            continue

        # ── Execute buy ───────────────────────────────────────────────────────
        price = prices.get(sym, closes[-1])

        # Gather entry indicators from latest candle
        entry_rsi = entry_ma = entry_bb = entry_vol = None
        try:
            candles = database.get_candles(sym, config.CANDLE_TIMEFRAME, limit=1)
            if candles:
                last = candles[-1]
                entry_rsi = last.get("rsi14")
                entry_ma  = last.get("ma_position") or _derive_ma_pos(price, last.get("ma20"))
                entry_bb  = last.get("bb_position") or _derive_bb_pos(price, last)
                entry_vol = last.get("volume_trend")
        except Exception:
            pass

        # Final pre-buy guard (live balance check + concurrent cap + global cap)
        with _positions_lock:
            snapshot_now = list(_positions)
        buy_cfg = {**coin_cfg, "symbol": sym, "budget_usdt": budget}
        allowed, reason = can_execute_buy(buy_cfg, snapshot_now, client)
        if not allowed:
            print(f"[SKIP] {sym}: {reason}")
            continue

        try:
            result = client.order_market_buy(symbol=sym, quoteOrderQty=budget)
        except Exception as e:
            print(f"[SignalScanner] BUY failed {sym}: {e}")
            continue

        fill = result.get("fills", [{}])[0]
        fill_price = float(fill.get("price", price))
        qty        = float(result.get("executedQty", 0))

        if qty <= 0:
            continue

        pos_record = {
            "symbol":              sym,
            "entry_price":         fill_price,
            "quantity":            qty,
            "budget_usdt":         budget,
            "timestamp":           now,
            "mode":                mode,
            "entry_rsi":           entry_rsi,
            "entry_ma_position":   entry_ma,
            "entry_bb_position":   entry_bb,
            "entry_volume_trend":  entry_vol,
        }
        pos_id = database.save_position(pos_record)
        pos_record["id"] = pos_id

        with _positions_lock:
            _positions.append(pos_record)

        usdt_balance -= budget + budget * _fee_rate
        global_open  += 1

        breakeven = fill_price * _breakeven_mult
        print(
            f"[SignalScanner] BOUGHT {sym} @ ${fill_price:.4f} "
            f"| qty={qty:.6f} | BEP=${breakeven:.4f} "
            f"| signals={bullish_count}/4"
        )
