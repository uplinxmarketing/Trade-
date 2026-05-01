"""
Trade execution engine.
- Reads strategy.json every tick (cheap file stat, full parse only when changed)
- Never calls Claude — only executes what strategy.json says
- Sells the instant price exceeds breakeven
- Never sells at a loss
"""

import json
import os
import time
import math
import threading
from datetime import datetime
from typing import Dict, List, Optional

import config
import database
import learning
from connection import client, get_mode

_positions: List[dict] = []       # {id, symbol, entry_price, quantity, budget_usdt, timestamp, entry_rsi, entry_ma_position, entry_bb_position, entry_volume_trend}
_positions_lock = threading.Lock()

_strategy_mtime: float = 0.0
_strategy_cache: dict = {}

_fee_rate = config.FEE_RATE_BNB if config.BNB_FEE_MODE else config.FEE_RATE_STANDARD
_breakeven_mult = 1.0 + _fee_rate + _fee_rate   # 1.0015 with BNB, 1.002 without


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


def trade_loop(prices: Dict[str, float]):
    """
    Called on every price tick by data_collector.
    Checks exits first, then opens new positions if strategy approves.
    """
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

    # ── 1. Check exits ────────────────────────────────────────────────────────
    to_close = []
    with _positions_lock:
        for pos in _positions:
            sym   = pos["symbol"]
            price = prices.get(sym, 0.0)
            if price <= 0:
                continue
            breakeven = pos["entry_price"] * _breakeven_mult
            if price > breakeven:
                to_close.append(dict(pos))

    for pos in to_close:
        sym      = pos["symbol"]
        qty      = _floor_qty(pos["quantity"])
        price    = prices.get(sym, 0.0)
        if qty <= 0 or price <= 0:
            continue

        try:
            result = client.order_market_sell(symbol=sym, quantity=qty)
        except Exception as e:
            print(f"[TradeEngine] SELL failed {sym}: {e}")
            continue

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
            "hour_of_day":        buy_dt.hour if buy_ts else 0,
            "day_of_week":        buy_dt.weekday() if buy_ts else 0,
            "timestamp_buy":      buy_ts,
            "timestamp_sell":     sell_ts,
        }
        database.log_trade(trade_record)
        learning.learn_from_trade(trade_record)

        # Remove from memory and DB
        if pos.get("id"):
            database.delete_position(pos["id"])
        with _positions_lock:
            _positions[:] = [p for p in _positions if p.get("id") != pos.get("id")]

        profit_sign = "+" if net_profit >= 0 else ""
        print(f"[TradeEngine] SOLD {sym} @ ${fill_price:.4f} | net {profit_sign}${net_profit:.4f} | held {duration}s")

    # ── 2. Open new positions ─────────────────────────────────────────────────
    with _positions_lock:
        current_open = list(_positions)

    global_open = len(current_open)
    if global_open >= global_max:
        return

    usdt_balance = _get_usdt_balance()

    for sym, coin_cfg in approved_coins.items():
        price = prices.get(sym, 0.0)
        if price <= 0:
            continue

        budget   = float(coin_cfg.get("budget_usdt", config.BUDGET_PER_TRADE_USDT))
        max_conc = int(coin_cfg.get("max_concurrent", 2))

        with _positions_lock:
            coin_open = sum(1 for p in _positions if p["symbol"] == sym)

        if coin_open >= max_conc:
            continue
        if global_open >= global_max:
            break
        if usdt_balance < budget:
            continue

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

        try:
            result = client.order_market_buy(symbol=sym, quoteOrderQty=budget)
        except Exception as e:
            print(f"[TradeEngine] BUY failed {sym}: {e}")
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
        print(f"[TradeEngine] BOUGHT {sym} @ ${fill_price:.4f} | qty={qty:.6f} | BEP=${breakeven:.4f}")


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
