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
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
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
# Minimum multiplier needed to cover fees (breakeven floor).
_breakeven_mult = 1.0 + _fee_rate + _fee_rate + 0.0002

# Configurable exit multipliers — refreshed from strategy.json every buy/sell cycle.
# _take_profit_mult: price must reach entry * this to trigger a sell (>=_breakeven_mult).
# _stop_loss_mult:   price must fall to entry * this to trigger a stop-loss sell.
_take_profit_mult: float = _breakeven_mult   # default: break-even
_stop_loss_mult:   float = 1.0 - 0.02        # default: -2% stop loss

# New exit-mode flags (strategy.json controlled)
_take_profit_enabled: bool = True    # False → exit at breakeven (fees covered) only
_smart_hold_enabled:  bool = False   # True  → hold if signals still bullish; trail then exit
_trailing_stop_pct:   float = 0.5    # % drop from peak that triggers smart-hold exit

# Per-position high-water mark for smart hold (no lock needed — only written in sell thread)
_pos_peaks: Dict[str, float] = {}

_cooldowns: dict = {}

# ── Position index — rebuilt on every WebSocket tick for O(1) sell lookups ───
_pos_by_symbol: Dict[str, dict] = {}

# ── In-progress sell guard — prevents double-sells from monitor + guardian ────
_selling: set = set()
_selling_lock = threading.Lock()
_selling_ts: Dict[str, float] = {}   # when each sym was added — for watchdog

# ── Sell executor — parallel sells so 10 simultaneous exits never queue up ───
# Each position gets its own worker thread; _selling guard prevents duplicates.
_sell_executor = ThreadPoolExecutor(max_workers=12, thread_name_prefix="sell-worker")

# ── Real-time signal cache — updated on every kline close ────────────────────
_signal_cache: Dict[str, dict] = {}
_signal_cache_lock = threading.Lock()
_last_buy_check: float = 0.0
_last_no_signal_log: float = 0.0   # throttle "no coins ready" log to once per 60 s

# Per-coin timestamp of last inline tick-driven signal refresh
_tick_signal_ts: Dict[str, float] = {}
_TICK_REFRESH_SEC = 30.0  # recompute at most every 30 s per coin from price ticks


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

    if len(open_this_coin) >= max_concurrent:
        return False, f"Max concurrent for {sym} reached ({max_concurrent})"

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

    elif mode == "coin_pct":
        coin_pct = strategy.get("budget_coin_pct", {})
        pct = float(coin_pct.get(symbol, 5.0))
        return round(free_usdt * (pct / 100), 2)

    return config.BUDGET_FIXED_USDT


# ── Cooldown helpers ──────────────────────────────────────────────────────────

def _refresh_risk_params():
    """Read stop_loss_enabled/pct, take_profit_pct and new exit flags from strategy.json."""
    global _take_profit_mult, _stop_loss_mult, _take_profit_enabled, _smart_hold_enabled, _trailing_stop_pct
    strategy = _load_strategy()
    tp_pct = float(strategy.get("take_profit_pct", 0.5))   # e.g. 0.5 → 0.5%
    sl_pct = float(strategy.get("stop_loss_pct",   2.0))   # e.g. 2.0 → 2.0%
    sl_on  = bool(strategy.get("stop_loss_enabled", True))
    _take_profit_enabled = bool(strategy.get("take_profit_enabled", True))
    _smart_hold_enabled  = bool(strategy.get("smart_hold_enabled",  False))
    _trailing_stop_pct   = float(strategy.get("trailing_stop_pct",  0.5))
    tp_mult = 1.0 + (tp_pct / 100.0)
    # When TP is disabled → exit exactly at breakeven (fees covered, no extra target).
    # When TP is enabled  → exit at max(breakeven, entry × (1 + tp_pct/100)).
    _take_profit_mult = (_breakeven_mult if not _take_profit_enabled
                         else max(_breakeven_mult, tp_mult))
    # Stop loss: set to 0.0 when disabled so the check (price <= 0.0) never fires
    _stop_loss_mult   = (1.0 - sl_pct / 100.0) if sl_on else 0.0


def _set_cooldown(symbol: str):
    _cooldowns[symbol] = time.time() + config.COOLDOWN_AFTER_LOSS


def _in_cooldown(symbol: str) -> bool:
    exp = _cooldowns.get(symbol, 0)
    return time.time() < exp


# ── DB / startup helpers ──────────────────────────────────────────────────────

def _rebuild_pos_index():
    """Rebuild O(1) symbol→position lookup. Call whenever _positions changes."""
    global _pos_by_symbol
    with _positions_lock:
        _pos_by_symbol = {p["symbol"]: p for p in _positions}


def _apply_coin_restore(coins: list):
    """Write a list of coin symbols back into strategy.json approved_coins."""
    if not (coins and isinstance(coins, list) and len(coins) > 0):
        return
    try:
        if os.path.exists(config.STRATEGY_FILE):
            with open(config.STRATEGY_FILE) as f:
                strat = json.load(f)
            existing = {c["symbol"]: c for c in strat.get("approved_coins", [])}
            strat["approved_coins"] = [
                {
                    "symbol":         sym,
                    "approved":       True,
                    "budget_usdt":    existing.get(sym, {}).get("budget_usdt", config.BUDGET_PER_TRADE_USDT),
                    "max_concurrent": existing.get(sym, {}).get("max_concurrent", 3),
                    "confidence":     existing.get(sym, {}).get("confidence", 0.5),
                    "reason":         "Restored from Supabase",
                }
                for sym in coins
            ]
            strat["updated_at"] = datetime.now(timezone.utc).isoformat()
            with open(config.STRATEGY_FILE, "w") as f:
                json.dump(strat, f, indent=2)
            print(f"[TradeEngine] Restored {len(coins)} coins from Supabase → strategy.json.")
    except Exception as ce:
        print(f"[TradeEngine] Coin restore to strategy.json failed: {ce}")


def load_positions_from_db():
    """
    Called on startup.  Restores open positions from SQLite when available,
    or from Supabase when SQLite is empty (fresh Railway deploy / no volume).
    Coins and balance are ALWAYS restored from Supabase so the user's
    watchlist and wallet survive every redeploy regardless of SQLite state.
    """
    global _positions
    rows = database.load_positions()

    # Always fetch from Supabase so coins + balance are current even when
    # SQLite still has stale data from a previous deploy.
    supa_ok = False
    try:
        import supabase_sync
        restored = supabase_sync.restore_from_supabase()
        supa_ok = bool(restored)

        # ── Coins: ALWAYS restore — user selection must survive every deploy ──
        _apply_coin_restore(restored.get("selected_coins"))

        # ── Positions: only restore when SQLite is empty (avoids duplicates) ──
        if not rows and restored.get("positions"):
            for pos in restored["positions"]:
                pos_id = database.save_position(pos)
                pos["id"] = pos_id
            rows = database.load_positions()
            n_pos = len(rows)
            print(f"[TradeEngine] Restored {n_pos} position(s) from Supabase.")
            database.log_activity(
                f"Restored {n_pos} open position(s) from Supabase after redeploy", "info"
            )
        elif rows:
            database.log_activity(
                f"Startup: {len(rows)} position(s) loaded from local DB (Supabase backup intact)", "info"
            )
        else:
            database.log_activity(
                "Startup: no positions in local DB or Supabase — fresh wallet", "info"
            )

        # ── Trade history: restore when SQLite trades table is empty ────────────
        # Rebuilds win-rate, P&L stats and trade list from bot_trade_history.
        try:
            if not database.get_recent_trades(limit=1) and restored.get("trades"):
                imported = 0
                for trade in restored["trades"]:
                    try:
                        database.log_trade(trade)
                        imported += 1
                    except Exception:
                        pass
                if imported:
                    print(f"[TradeEngine] Imported {imported} trade(s) from Supabase history.")
                    database.log_activity(
                        f"Restored {imported} trade record(s) from Supabase history", "info"
                    )
            elif database.get_recent_trades(limit=1):
                database.log_activity(
                    f"Trade history intact in local DB — no restore needed", "info"
                )
        except Exception as te:
            print(f"[TradeEngine] Trade history restore failed (non-fatal): {te}")

        # ── Balance: restore when SQLite paper-state is empty or zero ──────────
        if restored.get("usdt_balance") is not None:
            usdt = restored["usdt_balance"]
            if hasattr(client, "_balances"):
                with client._lock:
                    current = client._balances.get("USDT", 0.0)
                # Only overwrite if local balance looks wrong (zero or default)
                if current <= 0 or not rows:
                    with client._lock:
                        client._balances["USDT"] = usdt
                        snapshot = dict(client._balances)
                    database.save_paper_state(snapshot)
                    print(f"[TradeEngine] Restored USDT balance from Supabase: {usdt:.2f}")
                    database.log_activity(
                        f"Restored USDT balance from Supabase: {usdt:.2f} USDT", "info"
                    )

    except Exception as e:
        print(f"[TradeEngine] Supabase restore failed (non-fatal): {e}")
        database.log_activity(f"Supabase restore failed — data may not survive redeploy: {e}", "warn")

    with _positions_lock:
        _positions = list(rows)
    _rebuild_pos_index()
    print(f"[TradeEngine] Loaded {len(_positions)} open position(s) from DB.")

    # Push current state to Supabase immediately so the next redeploy can restore it.
    # This runs even when SQLite had data (not just after a restore) so Supabase
    # always has an up-to-date snapshot regardless of how the data got into SQLite.
    try:
        import supabase_sync
        usdt = _get_usdt_balance()
        supabase_sync.sync_all(list(rows), usdt)
        database.log_activity(
            f"Supabase snapshot pushed — {len(rows)} position(s), {usdt:.2f} USDT backed up", "info"
        )
    except Exception as e:
        database.log_activity(f"Supabase snapshot push failed: {e}", "warn")

    # Sync PaperClient coin balances from the restored positions.
    # After a restart, PaperClient loads from saved paper_state, but if that
    # state is out of sync with the positions table (e.g. a partial crash),
    # sells will fail with "Insufficient balance". Self-heal by crediting the
    # exact quantity that each open position holds.
    if hasattr(client, "_balances") and _positions:
        changed = False
        for pos in _positions:
            sym  = pos["symbol"]
            coin = sym[:-4]  # strip USDT
            qty  = float(pos.get("quantity", 0))
            if qty <= 0:
                continue
            with client._lock:
                current = client._balances.get(coin, 0.0)
                if current < qty * 0.99:
                    client._balances[coin] = qty
                    changed = True
                    print(f"[TradeEngine] Synced paper balance: {coin}={qty:.8f} (was {current:.8f})")
        if changed:
            database.save_paper_state(dict(client._balances))


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
    """Execute a market sell for pos at price. Logs trade, cleans up state.
    Caller MUST have already added pos['symbol'] to _selling before submitting
    to the executor — this function just verifies and executes."""
    from datetime import timezone as _tz
    sym  = pos["symbol"]
    qty  = _floor_qty(pos["quantity"])
    mode = get_mode()
    now  = datetime.now(_tz.utc).isoformat()

    if qty <= 0 or price <= 0:
        with _selling_lock:
            _selling.discard(sym)
            _selling_ts.pop(sym, None)
        return

    # Verify position still exists — another executor task may have already sold it
    # (e.g. force-sell came in while we were queued).
    with _positions_lock:
        pos_id = pos.get("id")
        if pos_id and not any(p.get("id") == pos_id for p in _positions):
            with _selling_lock:
                _selling.discard(sym)
                _selling_ts.pop(sym, None)
            return  # already sold

    try:
        _do_execute_sell(pos, sym, qty, price, reason, mode, now)
    finally:
        with _selling_lock:
            _selling.discard(sym)
            _selling_ts.pop(sym, None)


def _do_execute_sell(pos: dict, sym: str, qty: float, price: float, reason: str, mode: str, now: str):
    """Inner sell logic — called only when the _selling guard is held."""
    from datetime import timezone as _tz

    # Ensure PaperClient price is current before the sell
    try:
        client.update_price(sym, price)
    except Exception:
        pass

    # Paper mode: self-heal coin balance if it went missing after a restart.
    # open position record is the source of truth for what we own.
    if hasattr(client, "_balances"):
        coin = sym[:-4]
        with client._lock:
            current_coin_bal = client._balances.get(coin, 0.0)
            if current_coin_bal < qty * 0.99:
                client._balances[coin] = qty
                snapshot = dict(client._balances)
            else:
                snapshot = None
        if snapshot is not None:
            database.save_paper_state(snapshot)
            database.log_activity(
                f"[PaperWallet] Auto-credited {qty:.8f} {coin} "
                f"(had {current_coin_bal:.8f}) — balance was out of sync after restart",
                "warn",
            )

    try:
        # For paper mode: pass the trigger price directly so any concurrent
        # WebSocket/REST update to _prices cannot cause the sell to execute
        # at the wrong (potentially lower) price between now and order execution.
        if hasattr(client, "_balances"):
            result = client.order_market_sell(symbol=sym, quantity=qty, price=price)
        else:
            result = client.order_market_sell(symbol=sym, quantity=qty)
    except Exception as e:
        msg = f"SELL failed {sym} ({reason}): {e}"
        print(f"[TradeEngine] {msg}")
        database.log_activity(msg, "error")
        return

    # Read fill data directly from PaperClient's order response — no recalculation.
    # cummulativeQuoteQty = gross - sell_fee = USDT actually returned to wallet.
    fill          = result.get("fills", [{}])[0]
    fill_price    = float(fill.get("price", price))
    sell_fee      = float(fill.get("commission", 0))
    usdt_returned = float(result.get("cummulativeQuoteQty", 0))  # gross - sell_fee
    budget        = pos["budget_usdt"]
    buy_fee       = budget * _fee_rate
    # Single authoritative formula — must match wallet formula in PaperClient exactly:
    #   net_profit = usdt_returned - budget - buy_fee
    #             = (qty × sell_price - sell_fee) - budget - (budget × fee_rate)
    net_profit    = usdt_returned - budget - buy_fee

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
    # Position cleanup runs in finally so it always executes even if logging raises.
    # The sell already completed — we must never leave a ghost position in memory.
    try:
        database.log_trade(trade_record)
        try:
            import supabase_sync
            # Parallel sync to Supabase — all 3 calls run concurrently, max 4 s total.
            # Synchronous here (not background) so data is never lost on Railway restart.
            usdt_now = _get_usdt_balance()
            supabase_sync.sync_sell_result_sync(trade_record, sym, usdt_now)
        except Exception as se:
            err_msg = f"Supabase sync error after selling {sym}: {se}"
            print(f"[TradeEngine] {err_msg}")
            try:
                database.log_activity(err_msg, "error")
            except Exception:
                pass
        try:
            learning.learn_from_trade(trade_record)
        except Exception as le:
            database.log_activity(f"learn_from_trade error ({sym}): {le}", "warn")
    except Exception as te:
        database.log_activity(f"log_trade error ({sym}): {te}", "warn")
    finally:
        if pos.get("id"):
            try:
                database.delete_position(pos["id"])
            except Exception:
                pass
        with _positions_lock:
            _positions[:] = [p for p in _positions if p.get("id") != pos.get("id")]
        _rebuild_pos_index()
        _pos_peaks.pop(sym, None)   # clean up smart-hold peak tracker

    pnl_sign = "+" if net_profit >= 0 else ""
    sell_msg = (
        f"SOLD {sym} @ ${fill_price:.4f} "
        f"· received {usdt_returned:.4f} USDT · P&L: {pnl_sign}{net_profit:.4f} USDT"
        f"  ({reason}, held {duration}s)"
    )
    print(f"[{sell_ts}] {sell_msg}")
    try:
        database.log_activity(sell_msg, "info")
    except Exception:
        pass


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
        global _last_no_signal_log
        now_ns = time.time()
        if now_ns - _last_no_signal_log >= 60.0:
            _last_no_signal_log = now_ns
            if cache_size == 0:
                import data_collector as _diag_dc
                n_ticks   = sum(1 for v in _diag_dc.price_ticks.values()   if len(v) >= _diag_dc._MIN_TICKS)
                n_samples = sum(1 for v in _diag_dc.price_samples.values() if len(v) >= _diag_dc._MIN_SAMPLES)
                n_ws      = sum(1 for v in _diag_dc.ws_candles.values()    if len(v) >= _diag_dc._MIN_CANDLES)
                database.log_activity(
                    f"Signal cache empty — data sources: ticks={n_ticks}, samples={n_samples}, ws_candles={n_ws}", "info"
                )
            else:
                with _signal_cache_lock:
                    snap = dict(_signal_cache)
                top = sorted(snap.items(), key=lambda x: -x[1]["score"])[:5]
                detail = " | ".join(
                    f"{s}:score={v['score']} RSI={v['signals'].get('rsi',False)} trend={v['signals'].get('trend',False)}"
                    for s, v in top
                )
                database.log_activity(
                    f"Buy check: {cache_size} coins, none at score≥{config.MIN_SIGNALS_TO_BUY} — top5: {detail}", "info"
                )
        return

    # Throttle: don't call get_account() faster than every 1 s
    now = time.time()
    if now - _last_buy_check < 1.0:
        return
    _last_buy_check = now

    strategy = _load_strategy()
    if not strategy.get("trading_active", True):
        database.log_activity("Buy check: trading_active=False — bot is paused", "warn")
        return

    # Enforce max_positions (configurable via /api/settings, default 10)
    max_pos = int(strategy.get("max_positions", 10))
    with _positions_lock:
        n_open = len(_positions)
    if n_open >= max_pos:
        return  # silently skip — already at capacity

    # Refresh configurable risk params (take profit %, stop loss %) from strategy.json
    _refresh_risk_params()

    # Enforce configurable min_signals threshold (overrides config.MIN_SIGNALS_TO_BUY)
    min_sigs = int(strategy.get("min_signals", config.MIN_SIGNALS_TO_BUY))

    approved = {
        c["symbol"]: c
        for c in strategy.get("approved_coins", [])
        if c.get("approved")
    }
    if not approved:
        database.log_activity("Buy check: no approved coins in strategy.json", "warn")
        return

    from datetime import timezone as _tz
    usdt_balance = _get_usdt_balance()
    ts_now = datetime.now(_tz.utc).isoformat()
    mode   = get_mode()

    # Log a readable snapshot so the activity log always shows what's happening
    with _signal_cache_lock:
        cache_snapshot = dict(_signal_cache)
    ready_syms = [s for s, v in cache_snapshot.items() if v["score"] >= min_sigs and s in approved]
    database.log_activity(
        f"Buy scan: USDT={usdt_balance:.2f} | {len(ready_syms)} coin(s) ready (min {min_sigs}/4 signals): "
        + (", ".join(f"{s}(score={cache_snapshot[s]['score']})" for s in ready_syms[:6]) or "none"),
        "info"
    )

    for sym, cached in cache_snapshot.items():
        if sym not in approved:
            continue
        if cached["score"] < min_sigs:
            continue
        if _in_cooldown(sym):
            database.log_activity(f"{sym}: buy skipped — in cooldown", "info")
            continue

        with _positions_lock:
            already_held = any(p["symbol"] == sym for p in _positions)

        if already_held:
            continue

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

        exit_target = round(fill_price * _take_profit_mult, 8)
        pos_record = {
            "symbol":             sym,
            "entry_price":        fill_price,
            "exit_target":        exit_target,
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
        _rebuild_pos_index()

        try:
            import supabase_sync
            # Parallel sync to Supabase — both calls run concurrently, max 4 s total.
            supabase_sync.sync_buy_result_sync(
                pos_record, usdt_balance - budget - budget * _fee_rate
            )
        except Exception as _be:
            try:
                database.log_activity(f"Supabase sync error after buying {sym}: {_be}", "error")
            except Exception:
                pass

        usdt_balance -= budget + budget * _fee_rate

        score     = cached["score"]
        msg = (
            f"BOUGHT {sym} @ ${fill_price:.4f} "
            f"| qty={qty:.6f} | EXIT TARGET=${exit_target:.4f} "
            f"| signals={score}/4"
        )
        print(f"[RealtimeBuy] {msg}")
        database.log_activity(msg, "info")


# ── Inline tick-driven signal refresh ────────────────────────────────────────

def _inline_refresh_from_ticks(sym: str, price: float):
    """
    Recompute signals for sym using the best available price series — no I/O.
    Preference order: time-sampled prices (quality RSI) → raw ticks (last resort).
    """
    import data_collector as _dc

    # Prefer 30-second sampled prices — spans real time, gives meaningful RSI
    closes = list(_dc.price_samples.get(sym, []))
    if len(closes) >= _dc._MIN_SAMPLES:
        closes = closes[:]   # copy
        closes[-1] = price   # inject latest price
    else:
        # Fall back to raw ticks (available in seconds but RSI less reliable)
        closes = list(_dc.price_ticks.get(sym, []))
        if len(closes) < _dc._MIN_TICKS:
            return
        closes[-1] = price

    vols = [1.0] * len(closes)
    update_coin_signals(sym, closes, vols)


# ── Process 1: realtime monitor (called on every WebSocket tick) ──────────────

def realtime_monitor(prices: Dict[str, float]):
    """
    Synchronous — called inside data_collector's async WebSocket loop on EVERY
    trade event (~100 ms per coin). MUST NOT do any blocking I/O.

    Sell exits and buy entries are dispatched to _sell_executor (non-blocking
    O(1) submit) so the event loop is never stalled.  The _sell_monitor_loop
    daemon thread acts as a 5-second fallback in case a tick is missed.
    """
    now = time.time()

    # ── Real-time sell check — fires within ~100 ms of price crossing threshold ──
    # Reads the pre-built symbol→position index (no lock needed for dict reads).
    pos_index = _pos_by_symbol   # local ref; atomically replaced on each mutation
    for sym, price in prices.items():
        pos = pos_index.get(sym)
        if pos is None or price <= 0:
            continue
        entry  = pos["entry_price"]
        # Use configurable take-profit multiplier (refreshed from strategy.json every buy cycle).
        # Must be at least _breakeven_mult so we never sell at a loss via take-profit.
        target = entry * _take_profit_mult
        stop   = entry * _stop_loss_mult

        if price >= target:
            with _selling_lock:
                already = sym in _selling
            if not already:
                if _smart_hold_enabled:
                    _pos_peaks[sym] = max(_pos_peaks.get(sym, target), price)
                    peak       = _pos_peaks[sym]
                    trail_stop = peak * (1.0 - _trailing_stop_pct / 100.0)
                    sell_reason: Optional[str] = None
                    if price <= trail_stop:
                        sell_reason = "smart-hold-trail"
                    else:
                        with _signal_cache_lock:
                            score = _signal_cache.get(sym, {}).get("score", 0)
                        if score < 3:
                            sell_reason = "take-profit"
                    if sell_reason:
                        with _selling_lock:
                            if sym not in _selling:
                                _selling.add(sym)
                                _selling_ts[sym] = time.time()
                        _sell_executor.submit(_execute_sell, pos, price, sell_reason)
                else:
                    with _selling_lock:
                        if sym not in _selling:
                            _selling.add(sym)
                            _selling_ts[sym] = time.time()
                    _sell_executor.submit(_execute_sell, pos, price, "take-profit")
        elif _stop_loss_mult < 1.0 and price <= stop:
            with _selling_lock:
                if sym in _selling:
                    continue
                _selling.add(sym)
                _selling_ts[sym] = time.time()
            _sell_executor.submit(_execute_sell, pos, price, "stop-loss")

    # ── Inline signal refresh — throttled to every 30 s per coin ─────────────
    for sym, price in prices.items():
        if price <= 0:
            continue
        if now - _tick_signal_ts.get(sym, 0) >= _TICK_REFRESH_SEC:
            _tick_signal_ts[sym] = now
            _inline_refresh_from_ticks(sym, price)

    # ── Real-time buy check — throttled to 1 s ────────────────────────────────
    _check_buys_from_cache(prices)


# ── Sell monitor — daemon thread, independent of asyncio ─────────────────────

_sell_diag_ts: float      = 0.0
_sell_monitor_heartbeat: float = 0.0   # updated every loop — 0 means not started
_sell_monitor_thread: Optional[threading.Thread] = None

# REST price fallback cache — populated when WebSocket is geo-blocked on Railway
_rest_px: Dict[str, float] = {}
_rest_px_ts: float = 0.0
_REST_PX_TTL = 2.0   # refetch REST prices every 2 s when WebSocket is down


def _fetch_rest_prices(symbols: list) -> Dict[str, float]:
    """
    Batch-fetch current prices via REST when WebSocket is unavailable.
    Uses /api/v3/ticker/price?symbols=[...] — only fetches needed symbols.
    Tries the public CDN first (rarely geo-blocked), then API fallbacks.
    Returns {} on complete failure.
    """
    import urllib.request as _ur
    import urllib.parse as _up
    result: Dict[str, float] = {}
    syms_param = _up.quote(json.dumps(symbols))
    for base in _KLINE_BASES:
        try:
            url = f"{base}/api/v3/ticker/price?symbols={syms_param}"
            req = _ur.Request(url, headers={"User-Agent": "TradingBot/1.0"})
            with _ur.urlopen(req, timeout=5) as resp:
                data = json.loads(resp.read())
            if isinstance(data, list):
                for item in data:
                    s = item.get("symbol", "")
                    p = float(item.get("price", 0) or 0)
                    if s and p > 0:
                        result[s] = p
            elif isinstance(data, dict) and data.get("symbol"):
                p = float(data.get("price", 0) or 0)
                if p > 0:
                    result[data["symbol"]] = p
            if result:
                return result
        except Exception:
            continue
    return result


def _sell_monitor_loop():
    """
    Fallback daemon thread — wakes every 5 s and catches any positions that
    the realtime_monitor missed (e.g. while WebSocket was reconnecting).
    Primary sell path is realtime_monitor which fires within ~100 ms of each tick.

    CRITICAL: When Railway's WebSocket to Binance is geo-blocked, _dc.prices is
    always empty → realtime_monitor never fires → this loop is the only sell path.
    The fallback price chain:
      1. WebSocket prices (_dc.prices) — accurate, updated every ~100 ms
      2. Signal cache  — in-memory last kline close, refreshed every 60 s by REST
      3. REST batch ticker fetch — accurate, ~200 ms latency, rate-limited to 8 s
    """
    import data_collector as _dc
    global _sell_diag_ts, _sell_monitor_heartbeat, _rest_px, _rest_px_ts

    try:
        database.log_activity("Sell monitor thread started", "info")
    except Exception:
        pass

    while True:
        _sell_monitor_heartbeat = time.time()   # update at START so alive-check is always fresh
        try:
            with _positions_lock:
                snap = list(_positions)

            # ── Watchdog: force-clear _selling entries stuck > 90 s ──────────
            # Prevents positions from showing "SELLING NOW" forever when the
            # executor thread hangs or crashes without clearing the guard.
            now_wd = time.time()
            with _selling_lock:
                stuck = [s for s, t in list(_selling_ts.items()) if now_wd - t > 90]
            for s in stuck:
                with _selling_lock:
                    _selling.discard(s)
                    _selling_ts.pop(s, None)
                try:
                    database.log_activity(
                        f"Sell monitor: force-cleared stuck guard for {s} (>90 s)", "warn"
                    )
                except Exception:
                    pass

            prices = dict(_dc.prices)

            # ── ALWAYS refresh REST prices for ALL open positions ─────────────
            # CRITICAL: _dc.prices may contain STALE prices from a WebSocket
            # that connected briefly then was geo-blocked by Railway/Binance.
            # A stale price below the exit target means the sell condition is
            # never met even when the real market price has crossed the target.
            # REST prices are fetched every 4 s and ALWAYS override WS prices.
            if snap:
                now_t2 = time.time()
                if (now_t2 - _rest_px_ts) >= _REST_PX_TTL:
                    all_syms = list({p["symbol"] for p in snap})
                    fetched = _fetch_rest_prices(all_syms)
                    if fetched:
                        _rest_px.update(fetched)
                        _rest_px_ts = now_t2
                        stale = [
                            s for s in fetched
                            if s in prices and prices[s] > 0
                            and abs(fetched[s] - prices[s]) / max(fetched[s], 1) > 0.001
                        ]
                        if stale or not prices:
                            database.log_activity(
                                f"Sell monitor REST refresh: {len(fetched)} prices"
                                + (f" | WS stale override: {stale}" if stale else ""), "info"
                            )
                    else:
                        # REST failed — fall back to signal cache for missing symbols
                        with _signal_cache_lock:
                            for pos2 in snap:
                                s2 = pos2["symbol"]
                                if not prices.get(s2):
                                    sc = _signal_cache.get(s2)
                                    if sc and sc.get("price", 0) > 0:
                                        prices[s2] = sc["price"]

            # Apply cached REST prices — these ALWAYS override stale WS prices
            for sym2, p2 in _rest_px.items():
                prices[sym2] = p2

            # Diagnostic log every 60 s
            now_t = time.time()
            if now_t - _sell_diag_ts >= 60.0:
                _sell_diag_ts = now_t
                lines = []
                for p in snap:
                    sym   = p["symbol"]
                    price = prices.get(sym, 0.0)
                    entry = p["entry_price"]
                    bep   = entry * _breakeven_mult
                    pct   = ((price - entry) / entry * 100) if entry else 0
                    lines.append(
                        f"{sym} entry={entry:.4f} cur={price:.4f}({pct:+.3f}%) "
                        f"target={bep:.4f}({'SELL' if price >= bep else 'hold'})"
                    )
                database.log_activity(
                    f"Sell monitor fallback: {len(snap)} open — " + " | ".join(lines), "info"
                )

            for pos in snap:
                sym   = pos["symbol"]
                price = prices.get(sym, 0.0)
                if price <= 0:
                    continue
                with _selling_lock:
                    if sym in _selling:
                        continue
                entry  = pos["entry_price"]
                target = entry * _take_profit_mult
                stop   = entry * _stop_loss_mult
                if price >= target:
                    if _smart_hold_enabled:
                        _pos_peaks[sym] = max(_pos_peaks.get(sym, target), price)
                        peak       = _pos_peaks[sym]
                        trail_stop = peak * (1.0 - _trailing_stop_pct / 100.0)
                        sell_reason2: Optional[str] = None
                        if price <= trail_stop:
                            sell_reason2 = "smart-hold-trail"
                        else:
                            with _signal_cache_lock:
                                score2 = _signal_cache.get(sym, {}).get("score", 0)
                            if score2 < 3:
                                sell_reason2 = "take-profit"
                        if sell_reason2:
                            with _selling_lock:
                                if sym not in _selling:
                                    _selling.add(sym)
                                    _selling_ts[sym] = time.time()
                            _sell_executor.submit(_execute_sell, pos, price, sell_reason2)
                    else:
                        with _selling_lock:
                            if sym not in _selling:
                                _selling.add(sym)
                                _selling_ts[sym] = time.time()
                        _sell_executor.submit(_execute_sell, pos, price, "take-profit")
                elif _stop_loss_mult < 1.0 and price <= stop:
                    with _selling_lock:
                        if sym in _selling:
                            continue
                        _selling.add(sym)
                        _selling_ts[sym] = time.time()
                    _sell_executor.submit(_execute_sell, pos, price, "stop-loss")

        except Exception as exc:
            try:
                database.log_activity(f"Sell monitor error: {exc}", "error")
            except Exception:
                pass
        # Sleep shorter when positions are open so we catch exits faster.
        # 1 s with open positions; 5 s when idle to avoid unnecessary REST calls.
        time.sleep(1.0 if snap else 5.0)


async def position_guardian():
    """
    Watchdog coroutine — starts the sell monitor thread and restarts it if
    it ever dies (which should never happen, but belt-and-suspenders).
    """
    global _sell_monitor_thread
    while True:
        alive = (
            _sell_monitor_thread is not None
            and _sell_monitor_thread.is_alive()
        )
        if not alive:
            _sell_monitor_thread = threading.Thread(
                target=_sell_monitor_loop, name="sell-monitor", daemon=True
            )
            _sell_monitor_thread.start()
        await asyncio.sleep(5.0)   # check every 5 s


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
    # Binance public CDN — served via Cloudflare, often accessible when api.binance.com is blocked
    "https://data-api.binance.vision",
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

        # 4. Time-sampled price buffer (one price per 30 s) — meaningful RSI after 8 min.
        #    Much better than raw ticks because prices span real time, not one second.
        if not closes or len(closes) < MIN:
            samples = list(_dc.price_samples.get(sym, []))
            if len(samples) >= _dc._MIN_SAMPLES:
                closes  = samples
                volumes = [1.0] * len(samples)

        # 5. Raw price-tick buffer — available in seconds but RSI quality is poor.
        #    Used only as last resort (first 8 minutes of uptime).
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
