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
_breakeven_mult = 1.0 + _fee_rate + _fee_rate

_cooldowns: dict = {}

# ── Real-time signal cache — updated on every kline close ────────────────────
_signal_cache: Dict[str, dict] = {}
_signal_cache_lock = threading.Lock()
_last_buy_check: float = 0.0


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
        # Fast path: read directly from PaperClient's in-memory balance dict
        if hasattr(client, "_balances"):
            with client._lock:
                return float(client._balances.get("USDT", 0.0))
        acc = client.get_account()
        for b in acc["balances"]:
            if b["asset"] == "USDT":
                return float(b["free"])
    except Exception:
        pass
    return float(os.getenv("STARTING_PAPER_USDT", "10000.0"))


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
    # MACD: histogram positive (matches frontend).
    # Previously required histo[-2] > histo[-3] (rising), which silently blocked
    # buys whenever histogram was positive but flattening.
    macd = bool(
        histo[-2] is not None
        and histo[-2] > 0
    )
    # Volume: current candle must exceed VOLUME_RATIO_MIN × 20-candle average.
    # No OR fallback — volume counts toward 3-of-4 only if the ratio threshold is met.
    volume = bool(
        volumes[-2] is not None
        and vol_ma[-2] is not None
        and vol_ma[-2] > 0
        and volumes[-2] >= vol_ma[-2] * config.VOLUME_RATIO_MIN
    )

    return {"trend": trend, "rsi": rsi, "macd": macd, "volume": volume}


def update_coin_signals(symbol: str, closes: list, volumes: list):
    """Update the signal cache on every kline close (WebSocket-driven)."""
    if len(closes) < 16:  # 16 = minimum for RSI-14 to produce a valid value at [-2]
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
    from datetime import timezone as _tz
    sym  = pos["symbol"]
    qty  = _floor_qty(pos["quantity"])
    mode = get_mode()
    now  = datetime.now(_tz.utc).isoformat()  # always UTC with timezone info

    if qty <= 0 or price <= 0:
        return

    # Make sure PaperClient uses the latest known price before the sell
    try:
        client.update_price(sym, price)
    except Exception:
        pass

    try:
        result = client.order_market_sell(symbol=sym, quantity=qty)
    except Exception as e:
        print(f"[TradeEngine] SELL failed {sym}: {e}")
        return

    fill_price = float(result["fills"][0]["price"]) if result.get("fills") else price
    # commission from PaperClient is already in USDT (fee = gross * fee_rate).
    # Do NOT multiply by fill_price again — that was double-counting.
    sell_fee = float(result["fills"][0]["commission"]) if result.get("fills") else (qty * fill_price * _fee_rate)
    buy_fee  = pos["budget_usdt"] * _fee_rate
    gross    = qty * fill_price
    net_profit = gross - sell_fee - pos["budget_usdt"] - buy_fee

    buy_ts  = pos.get("timestamp", now)
    sell_ts = now
    try:
        buy_dt  = datetime.fromisoformat(buy_ts.replace("Z", "+00:00"))
        sell_dt = datetime.fromisoformat(sell_ts.replace("Z", "+00:00"))
        # Both are timezone-aware; subtraction works directly
        if buy_dt.tzinfo and sell_dt.tzinfo:
            duration = int((sell_dt - buy_dt).total_seconds())
        else:
            duration = 0
    except Exception:
        duration = 0
        buy_dt   = datetime.now(_tz.utc)

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
        cache_size = len(_signal_cache)

    if not any_ready:
        if cache_size == 0:
            database.log_activity("Buy check: signal cache empty — waiting for first scan", "info")
        else:
            # Log scores so the user can see why no coin qualified
            with _signal_cache_lock:
                scores = {s: v["score"] for s, v in _signal_cache.items()}
            top = sorted(scores.items(), key=lambda x: -x[1])[:5]
            summary = " | ".join(f"{s}={sc}" for s, sc in top)
            database.log_activity(
                f"Buy check: cache={cache_size} coins, none at score≥{config.MIN_SIGNALS_TO_BUY} "
                f"(top5: {summary})", "info"
            )
        return

    # Throttle: don't call get_account() faster than every 3 s
    now = time.time()
    if now - _last_buy_check < 3.0:
        return
    _last_buy_check = now

    strategy = _load_strategy()
    if not strategy.get("trading_active", True):
        database.log_activity("Buy check: trading_active=False — bot is paused", "warn")
        return

    approved = {
        c["symbol"]: c
        for c in strategy.get("approved_coins", [])
        if c.get("approved")
    }
    if not approved:
        database.log_activity("Buy check: no approved coins in strategy.json", "warn")
        return

    global_max = strategy.get("global_max_positions", config.MAX_OPEN_POSITIONS)

    with _positions_lock:
        open_count = len(_positions)
    if open_count >= global_max:
        database.log_activity(
            f"Buy check: at max positions ({open_count}/{global_max})", "info"
        )
        return

    from datetime import timezone as _tz
    usdt_balance = _get_usdt_balance()
    ts_now = datetime.now(_tz.utc).isoformat()
    mode   = get_mode()

    # Log a readable snapshot so the activity log always shows what's happening
    with _signal_cache_lock:
        cache_snapshot = dict(_signal_cache)
    ready_syms = [s for s, v in cache_snapshot.items() if v["score"] >= config.MIN_SIGNALS_TO_BUY and s in approved]
    database.log_activity(
        f"Buy scan: USDT={usdt_balance:.2f} | {len(ready_syms)} coin(s) ready: "
        + (", ".join(f"{s}(score={cache_snapshot[s]['score']})" for s in ready_syms[:6]) or "none"),
        "info"
    )

    for sym, cached in cache_snapshot.items():
        if sym not in approved:
            continue
        if cached["score"] < config.MIN_SIGNALS_TO_BUY:
            continue
        if _in_cooldown(sym):
            database.log_activity(f"{sym}: buy skipped — in cooldown", "info")
            continue

        with _positions_lock:
            already_held = any(p["symbol"] == sym for p in _positions)
            global_open  = len(_positions)

        if already_held:
            continue
        if global_open >= global_max:
            break

        budget = get_budget_for_coin(sym, usdt_balance)
        if budget <= 0:
            database.log_activity(f"{sym}: buy skipped — budget=0 (mode={mode}, usdt={usdt_balance:.2f})", "warn")
            continue

        price = prices.get(sym) or cached["price"]
        if not price:
            database.log_activity(f"{sym}: buy skipped — no price available", "warn")
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
    Handles both stop-loss/take-profit exits AND buy entry (via signal cache).
    """
    with _positions_lock:
        snapshot = list(_positions)

    for pos in snapshot:
        sym   = pos["symbol"]
        price = prices.get(sym, 0.0)
        if price <= 0:
            continue

        entry       = pos["entry_price"]
        take_profit = entry * (1.0 + config.TAKE_PROFIT_PCT)
        stop_loss   = entry * (1.0 - config.STOP_LOSS_PCT)

        if price >= take_profit:
            _execute_sell(pos, price, "take-profit")
        elif price <= stop_loss:
            _execute_sell(pos, price, "stop-loss")
            _set_cooldown(sym)

    # Real-time buy check — reads signal cache, zero network I/O
    _check_buys_from_cache(prices)


# ── Position guardian: REST-based sell check when WebSocket prices are stale ──

async def position_guardian():
    """
    Polls open positions every 30 s via REST.
    Acts as a backstop when the WebSocket is disconnected or a symbol's
    price never arrives in the prices dict (e.g. right after startup).
    """
    while True:
        await asyncio.sleep(30)
        with _positions_lock:
            snap = list(_positions)
        if not snap:
            continue
        loop = asyncio.get_running_loop()
        for pos in snap:
            sym = pos["symbol"]
            try:
                def _fetch(s=sym):
                    return client.get_symbol_ticker(symbol=s)
                ticker = await loop.run_in_executor(None, _fetch)
                price  = float(ticker.get("price", 0))
                if price <= 0:
                    continue
                entry       = pos["entry_price"]
                take_profit = entry * (1.0 + config.TAKE_PROFIT_PCT)
                stop_loss   = entry * (1.0 - config.STOP_LOSS_PCT)
                if price >= take_profit:
                    _execute_sell(pos, price, "take-profit-guardian")
                elif price <= stop_loss:
                    _execute_sell(pos, price, "stop-loss-guardian")
                    _set_cooldown(sym)
            except Exception as e:
                print(f"[Guardian] {sym} price check failed: {e}")


# ── Process 2: signal scanner (async, refreshes cache every SCAN_INTERVAL_SEC) ─

async def signal_scanner(prices: dict):
    """
    Async coroutine — runs every SCAN_INTERVAL_SEC (60 s).
    Refreshes the signal cache from REST, then immediately attempts buys.
    This is the primary buy trigger — WebSocket callbacks are a fast-path
    supplement, but buys MUST fire even when WebSocket is slow or disconnected.
    """
    while True:
        try:
            await _refresh_signal_cache()
            # Trigger buy checks right after refreshing — don't wait for WebSocket
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, _check_buys_from_cache, dict(prices))
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


async def _fetch_klines(session, sym: str) -> list:
    """Try each Binance base URL to work around regional geo-blocks (HTTP 451)."""
    import aiohttp
    last_exc: Exception = RuntimeError("No Binance bases configured")
    for base in _KLINE_BASES:
        url = f"{base}/api/v3/klines?symbol={sym}&interval=1m&limit=50"
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=3)) as resp:
                resp.raise_for_status()
                return await resp.json()
        except Exception as e:
            last_exc = e
    raise last_exc


async def _refresh_signal_cache():
    """
    Refresh the in-memory signal cache concurrently (asyncio.gather).
    Falls back to DB candles if REST is geo-blocked.
    Never executes trades.
    """
    import aiohttp
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

    async def _refresh_one(session, sym: str) -> bool:
        import data_collector as _dc
        MIN = 16          # matches _dc._MIN_CANDLES — enough for RSI to fire
        closes = volumes = None

        # 1. Try Binance REST (fastest, most data)
        try:
            raw     = await _fetch_klines(session, sym)
            closes  = [float(k[4]) for k in raw]
            volumes = [float(k[5]) for k in raw]
        except Exception:
            pass

        # 2. Fall back to DB candles (populated by download_history / kline saves)
        if not closes or len(closes) < MIN:
            db_rows = database.get_candles(sym, config.CANDLE_TIMEFRAME, limit=60)
            if len(db_rows) >= MIN:
                closes  = [float(c["close"])  for c in db_rows]
                volumes = [float(c["volume"]) for c in db_rows]

        # 3. Fall back to in-memory WebSocket candle buffer (no REST needed)
        #    This kicks in when Binance REST is geo-blocked from Railway's servers.
        #    Fills up at 1 candle/minute; RSI fires after 16 minutes.
        if not closes or len(closes) < MIN:
            buf = _dc.ws_candles.get(sym, [])
            if len(buf) >= MIN:
                closes  = [float(r[4]) for r in buf]
                volumes = [float(r[5]) for r in buf]

        # 4. Fall back to trade-price tick buffer — available within seconds of
        #    WebSocket connect, no candle-close wait required.
        #    Volumes are uniform (no trade-volume in @trade events) so volume
        #    signal won't fire, but RSI / EMA / MACD all produce valid values.
        if not closes or len(closes) < MIN:
            ticks = list(_dc.price_ticks.get(sym, []))
            if len(ticks) >= _dc._MIN_TICKS:
                closes  = ticks
                volumes = [1.0] * len(ticks)

        if closes and len(closes) >= MIN:
            update_coin_signals(sym, closes, volumes)
            return True
        return False

    async with aiohttp.ClientSession() as session:
        results = await asyncio.gather(
            *[_refresh_one(session, sym) for sym in approved_coins],
            return_exceptions=True,
        )

    updated = sum(1 for r in results if r is True)
    with _signal_cache_lock:
        ready = sum(1 for v in _signal_cache.values() if v["score"] >= config.MIN_SIGNALS_TO_BUY)
    database.log_activity(
        f"Signal cache refreshed: {updated}/{len(approved_coins)} coins updated, "
        f"{ready} at score≥{config.MIN_SIGNALS_TO_BUY}",
        "info",
    )
