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

def _fills_fee_usdt(fills: list, fallback_usdt: float) -> tuple:
    """
    Sum commissions across ALL fills and convert to USDT.

    Binance splits a single market order into multiple fills at different price
    levels. Each fill carries its own commission amount and asset.

    commissionAsset is usually:
      "BNB"  — BNB-fee-discount mode (fee deducted from BNB balance, NOT USDT)
      "USDT" — standard mode for sell orders (deducted from quote proceeds)
      <coin> — rare; base coin deducted from quantity received

    Returns (fee_in_usdt, commission_asset_of_first_fill).
    Falls back to (fallback_usdt, "estimated") when conversion is impossible.
    """
    if not fills:
        return fallback_usdt, "estimated"
    try:
        from data_collector import prices as _px
    except Exception:
        return fallback_usdt, "estimated"

    total = 0.0
    first_asset = fills[0].get("commissionAsset", "estimated")
    for f in fills:
        amount = float(f.get("commission") or 0)
        asset  = f.get("commissionAsset", "")
        if asset in ("USDT", "BUSD", "USDC"):
            total += amount
        elif asset:
            px = _px.get(asset + "USDT", 0)
            if px:
                total += amount * px
            else:
                # Price unavailable — fall back to estimate for entire order
                return fallback_usdt, "estimated"
    return total, first_asset
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

# ── Bad-symbol blacklist — populated when Binance returns -1013 (market closed)
# Prevents repeated buy attempts and rate-limits sell retries on closed markets.
_bad_symbols: set = set()
_sell_last_failed_ts: Dict[str, float] = {}  # last sell failure time per symbol
_sell_last_failed_reason: Dict[str, str] = {}  # reason that triggered the failed sell
_SELL_RETRY_COOLDOWN_PROFIT = 5.0   # take-profit: 5s is enough to break retry loop
_SELL_RETRY_COOLDOWN_LOSS   = 0.0   # stop-loss / force-sell: retry immediately

# ── Sell executor — parallel sells so 10 simultaneous exits never queue up ───
# Each position gets its own worker thread; _selling guard prevents duplicates.
_sell_executor = ThreadPoolExecutor(max_workers=12, thread_name_prefix="sell-worker")

# ── Real-time signal cache — updated on every kline close ────────────────────
_signal_cache: Dict[str, dict] = {}
_signal_cache_lock = threading.Lock()
_last_buy_check: float = 0.0
_last_no_signal_log: float = 0.0   # throttle "no coins ready" log to once per 60 s
_last_buy_scan_log: float = 0.0    # throttle "Buy scan: ..." to once per 30 s
_last_at_capacity_log: float = 0.0 # throttle "at max capacity" log to once per 60 s

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
    """Return trade size in USDT based on BUDGET_MODE (config defaults or strategy.json overrides).

    If bot_allocation_usdt > 0, the bot is restricted to that USDT cap across all
    concurrent positions — works identically in paper and live mode. The
    "effective free" balance for budget math becomes:
        min(free_usdt, allocation - sum_of_open_position_usdt)
    """
    strategy = _load_strategy()
    mode = strategy.get("budget_mode", config.BUDGET_MODE)
    reinvest = bool(strategy.get("reinvest_profits", False))
    allocation = float(strategy.get("bot_allocation_usdt", config.BOT_ALLOCATION_USDT))

    # Apply allocation cap: subtract value already locked in open positions.
    if allocation > 0:
        with _positions_lock:
            in_positions = sum(p.get("budget_usdt", 0.0) for p in _positions)
        effective_free = max(0.0, min(free_usdt, allocation - in_positions))
    else:
        effective_free = free_usdt

    # Authoritative starting balance: DB setting (written at wallet reset) takes priority
    # over strategy.json so a stale initial_balance_usdt never inflates reinvest scaling.
    initial_from_strategy = float(strategy.get("initial_balance_usdt", 0))
    if initial_from_strategy > 0:
        initial = initial_from_strategy
    else:
        starting_str = database.get_setting("paper_starting_balance")
        initial = float(starting_str) if starting_str else free_usdt

    if mode == "fixed":
        base = float(strategy.get("budget_fixed_usdt", config.BUDGET_FIXED_USDT))

    elif mode == "percent":
        pct = float(strategy.get("budget_pct_of_free", config.BUDGET_PCT_OF_FREE))
        # percent mode already scales with balance — reinvest has no extra effect
        return round(min(effective_free * (pct / 100), effective_free * 0.9), 2)

    elif mode == "capped":
        cap = float(strategy.get("budget_total_cap_usdt", config.BUDGET_TOTAL_CAP_USDT))
        with _positions_lock:
            total_in_positions = sum(p["budget_usdt"] for p in _positions)
        remaining_cap = cap - total_in_positions
        per_trade = cap / config.MAX_OPEN_POSITIONS
        base = min(per_trade, max(0.0, remaining_cap))

    elif mode == "per_coin":
        per_coin = strategy.get("budget_per_coin", config.BUDGET_PER_COIN)
        base = float(per_coin.get(symbol, config.BUDGET_FIXED_USDT))

    elif mode == "coin_pct":
        coin_pct = strategy.get("budget_coin_pct", {})
        pct = float(coin_pct.get(symbol, 5.0))
        # coin_pct already scales with balance — no extra reinvest scaling needed
        return round(min(effective_free * (pct / 100), effective_free * 0.9), 2)

    else:
        base = config.BUDGET_FIXED_USDT

    # Reinvest profits: scale budget proportionally to balance growth.
    # e.g. started 10 000 USDT, now have 12 000 → budget × 1.2 (20% more per trade).
    # Clamped to [0.5, 2.0] — tighter ceiling prevents runaway budget on small initial values.
    if reinvest and initial > 0 and mode in ("fixed", "per_coin", "capped"):
        scale = max(0.5, min(2.0, free_usdt / initial))
        base = base * scale

    # Hard cap: single trade never exceeds 40% of effective free USDT (prevents wallet wipeout)
    return round(min(base, effective_free * 0.4), 2)


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
            tmp = config.STRATEGY_FILE + ".tmp"
            with open(tmp, "w") as f:
                json.dump(strat, f, indent=2)
                f.flush()
                try:
                    os.fsync(f.fileno())
                except Exception:
                    pass
            os.replace(tmp, config.STRATEGY_FILE)
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
                # Restore from Supabase when:
                #   a) local balance is zero / negative
                #   b) no open positions in SQLite (fresh deploy / no volume)
                #   c) balance is at the ENV starting default AND Supabase has a
                #      meaningfully different value (paper_state was never persisted)
                _starting_usdt = float(os.getenv("STARTING_PAPER_USDT", "10000.0"))
                _at_default    = abs(current - _starting_usdt) < 0.01
                _supa_differs  = abs(usdt - current) > 1.0
                if current <= 0 or not rows or (_at_default and _supa_differs):
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


def _floor_qty(qty: float, decimals: int = 8) -> float:
    """Floor quantity to avoid Binance LOT_SIZE precision errors.

    Default is 8 decimal places — matches PaperClient's buy precision so the
    sell quantity is never truncated below what was actually purchased.
    (The old default of 6 dp caused a double-floor: paper buys stored 8 dp,
    sells re-floored to 6 dp, silently discarding ~$0.04–$0.09 per BTC trade
    and turning every profitable +0.5% exit into a ~−0.0099 USDT loss.)
    For live Binance use the symbol-specific LOT_SIZE stepSize instead.
    """
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


# ── Signal evaluation — 6-signal dict ────────────────────────────────────────

def evaluate_signals(candles: list) -> dict:
    """
    Evaluate all six technical signals on the last COMPLETED candle (index -2).
    Returns {"trend", "rsi", "macd", "volume", "obv", "atr"} — all booleans.

    candles: list of dicts with keys close, volume, high, low.
    When high/low are unavailable (tick data), pass high=low=close — ATR will
    return None → atr_is_tradeable returns False → atr signal = False.
    """
    closes  = [c["close"]  for c in candles]
    volumes = [c["volume"] for c in candles]

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
    rsi = bool(
        rsi_vals[-2] is not None
        and config.RSI_BUY_MIN <= rsi_vals[-2] <= config.RSI_BUY_MAX
    )
    # MACD: histogram positive AND rising (confirms momentum, not just a positive reading).
    macd = bool(
        histo[-2] is not None
        and histo[-3] is not None
        and histo[-2] > 0
        and histo[-2] > histo[-3]
    )
    volume = bool(
        volumes[-2] is not None
        and vol_ma[-2] is not None
        and vol_ma[-2] > 0
        and volumes[-2] >= vol_ma[-2] * config.VOLUME_RATIO_MIN
    )
    obv = indicators.obv_is_bullish(candles)
    atr = indicators.atr_is_tradeable(
        indicators.calc_atr(candles, config.ATR_PERIOD),
        candles[-2]["close"],
        config.ATR_MIN_PCT,
        config.ATR_MAX_PCT,
    )

    return {"trend": trend, "rsi": rsi, "macd": macd, "volume": volume, "obv": obv, "atr": atr}


def update_coin_signals(symbol: str, closes: list, volumes: list):
    """Update the signal cache on every kline close (WebSocket-driven).

    Builds minimal candle dicts (high=low=close) so OBV works correctly.
    ATR signal will be False (atr_is_tradeable returns False when ATR=0) —
    that is safe: the REST scan (_refresh_one) sets accurate ATR/BB/5m values
    every 60 s and they are preserved here via the prev cache entry.
    """
    if len(closes) < 16:  # minimum for RSI-14 to produce a valid value at [-2]
        return
    try:
        candles = [
            {"high": c, "low": c, "close": c, "volume": v}
            for c, v in zip(closes, volumes)
        ]
        signals = evaluate_signals(candles)
        score   = sum(signals.values())
        rsi_list    = indicators.calc_rsi(closes, 14)
        rsi_display = rsi_list[-2] if rsi_list[-2] is not None else 0.0

        with _signal_cache_lock:
            prev = _signal_cache.get(symbol, {})
            _signal_cache[symbol] = {
                "signals":  signals,
                "score":    score,
                "price":    closes[-1],
                "rsi_val":  rsi_display,
                "bb_ok":    prev.get("bb_ok",  True),  # preserved from last REST scan
                "5m_ok":    prev.get("5m_ok",  True),  # preserved from last REST scan
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
        # Always clear the smart-hold peak — even on sell failure — so that a
        # later position re-opened on the same symbol doesn't inherit a stale
        # high-water mark and instantly trip its trailing stop.
        _pos_peaks.pop(sym, None)


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
    if mode != "live" and hasattr(client, "_balances"):
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

    _is_paper = (mode != "live")
    try:
        # Paper mode: pass the trigger price directly so concurrent WebSocket/REST
        # updates cannot cause the sell to execute at the wrong price.
        if _is_paper:
            result = client.order_market_sell(symbol=sym, quantity=qty, price=price)
        else:
            result = client.order_market_sell(symbol=sym, quantity=qty)
    except Exception as e:
        err_str = str(e)
        msg = f"SELL failed {sym} ({reason}): {e}"
        print(f"[TradeEngine] {msg}")
        database.log_activity(msg, "error")
        _sell_last_failed_ts[sym] = time.time()
        _sell_last_failed_reason[sym] = reason
        if "-1013" in err_str or "Market is closed" in err_str:
            _bad_symbols.add(sym)
            database.log_activity(
                f"{sym}: blacklisted — market closed/delisted; position will be force-closed", "warn"
            )
            # Force-remove the position so we stop retrying a delisted coin.
            with _positions_lock:
                before = len(_positions)
                _positions[:] = [p for p in _positions if p.get("symbol") != sym]
                if len(_positions) < before:
                    database.log_activity(f"{sym}: position force-closed (market closed)", "warn")
            _rebuild_pos_index()  # keep O(1) index consistent with _positions
        return

    # ── Fee-aware fill parsing ─────────────────────────────────────────────────
    # Market orders can split across multiple fills at different price levels.
    # Each fill has its own commission amount and asset.
    # Wrap in try/except so an unexpected Binance response format never causes
    # a silent position ghost (sell executed on Binance, position stays in bot).
    try:
        fills      = result.get("fills", [])
        fill_price = float(fills[0].get("price", price)) if fills else price
        raw_quote  = float(result.get("cummulativeQuoteQty") or 0)
        budget     = pos["budget_usdt"]

        if mode == "live":
            sell_fee, fee_asset = _fills_fee_usdt(fills, raw_quote * _fee_rate)
            if fee_asset in ("USDT", "BUSD", "USDC"):
                usdt_returned = raw_quote
            else:
                usdt_returned = raw_quote - sell_fee
            buy_fee = float(pos.get("buy_fee_usdt") or budget * _fee_rate)
        else:
            sell_fee      = sum(float(f.get("commission") or 0) for f in fills)
            usdt_returned = raw_quote
            buy_fee       = budget * _fee_rate

        net_profit = usdt_returned - budget - buy_fee
    except Exception as parse_err:
        # Sell DID execute on Binance — fall back to estimates so trade is
        # still recorded and position is properly cleaned up.
        database.log_activity(
            f"SELL {sym} ({reason}): fill parse error ({parse_err}) — using estimates", "warn"
        )
        fill_price    = price
        raw_quote     = price * pos.get("quantity", 0)
        budget        = pos.get("budget_usdt", 0)
        sell_fee      = raw_quote * _fee_rate
        buy_fee       = float(pos.get("buy_fee_usdt") or budget * _fee_rate)
        usdt_returned = raw_quote - sell_fee
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

    # Always refresh risk params first — even when at capacity, so the sell monitor
    # uses current TP/SL settings rather than stale cached values.
    _refresh_risk_params()

    # Enforce max_positions (configurable via /api/settings, default 10)
    max_pos = int(strategy.get("max_positions", 10))
    with _positions_lock:
        n_open = len(_positions)
    if n_open >= max_pos:
        # Periodic heartbeat so the user sees the bot is still alive at capacity.
        # Without this, no buy logs are emitted while at max_positions and the
        # UI looks like it has been paused for no reason.
        global _last_at_capacity_log
        _now_cap = time.time()
        if _now_cap - _last_at_capacity_log >= 60.0:
            _last_at_capacity_log = _now_cap
            database.log_activity(
                f"At max positions ({n_open}/{max_pos}) — bot active, "
                f"waiting for an exit before opening new trades",
                "info",
            )
        return  # already at capacity — buys resume automatically after a sell

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

    # Build the cache snapshot but only emit the activity log every 30 s — these
    # writes hold the global DB lock and previously fired on every WS-tick scan,
    # serializing the sell monitor and signal scanner behind them.
    with _signal_cache_lock:
        cache_snapshot = dict(_signal_cache)
    ready_syms = [s for s, v in cache_snapshot.items() if v["score"] >= min_sigs and s in approved]
    global _last_buy_scan_log
    _now_ts = time.time()
    if ready_syms or (_now_ts - _last_buy_scan_log) >= 30.0:
        _last_buy_scan_log = _now_ts
        database.log_activity(
            f"Buy scan: USDT={usdt_balance:.2f} | {len(ready_syms)} coin(s) ready (min {min_sigs}/6 signals): "
            + (", ".join(f"{s}(score={cache_snapshot[s]['score']})" for s in ready_syms[:6]) or "none"),
            "info"
        )

    for sym, cached in cache_snapshot.items():
        # Re-check capacity before every individual buy — the pre-loop check only
        # guards the entry; without this, all ready coins buy in sequence and blow
        # past the configured max_positions limit.
        with _positions_lock:
            if len(_positions) >= max_pos:
                break

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

        # ── Hard veto checks: BB position and 5m trend ────────────────────────
        # Both are populated by the REST scan (_refresh_one) every 60 s and
        # preserved across kline-close updates.  Default True = not blocking.
        sigs    = cached.get("signals", {})
        bb_ok   = cached.get("bb_ok",  True)
        five_ok = cached.get("5m_ok",  True)
        score   = cached["score"]
        rsi_v   = cached.get("rsi_val", 0.0)
        sig_str = (
            f"EMA{'↑' if sigs.get('trend')  else '↓'} "
            f"RSI:{rsi_v:.0f} "
            f"MACD{'+'  if sigs.get('macd')   else '-'} "
            f"Vol{'✓'   if sigs.get('volume') else '✗'} "
            f"OBV{'✓'   if sigs.get('obv')    else '✗'} "
            f"ATR{'✓'   if sigs.get('atr')    else '✗'}"
        )
        bb_str  = "BB:PASS" if bb_ok   else "BB:FAIL"
        m5_str  = "5m:PASS" if five_ok else "5m:FAIL"
        if not bb_ok:
            database.log_activity(
                f"[SKIP] {sym}: price near upper Bollinger Band | "
                f"{sig_str} | {bb_str} | SKIP(upper band)", "info"
            )
            continue
        if not five_ok:
            database.log_activity(
                f"[SKIP] {sym}: 5m downtrend | "
                f"{sig_str} | {bb_str} {m5_str} | SKIP(5m downtrend)", "info"
            )
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

        if sym in _bad_symbols:
            database.log_activity(f"{sym}: buy skipped — market closed/delisted (blacklisted this session)", "warn")
            continue

        # Binance minimum notional is $10 for most spot pairs — reject early
        # so we don't waste an API call and get a cryptic -1013 error.
        if mode == "live" and budget < 10.0:
            database.log_activity(
                f"{sym}: buy skipped — budget ${budget:.2f} < $10 Binance minimum notional "
                f"(increase trade size in Settings)", "warn"
            )
            continue

        try:
            result = client.order_market_buy(symbol=sym, quoteOrderQty=budget)
        except Exception as e:
            err_str = str(e)
            print(f"[RealtimeBuy] BUY failed {sym}: {e}")
            database.log_activity(f"{sym}: BUY failed — {e}", "error")
            if "-1013" in err_str or "Market is closed" in err_str:
                _bad_symbols.add(sym)
                database.log_activity(f"{sym}: blacklisted — market closed/delisted on Binance", "warn")
            continue

        buy_fills  = result.get("fills", [])
        fill_price = float(buy_fills[0].get("price", price)) if buy_fills else price
        qty        = float(result.get("executedQty", 0))
        if qty <= 0:
            continue

        # Compute actual buy fee in USDT across all fills.
        # In live BNB-fee mode, commission is in BNB — convert using live price.
        # Stored on the position so _do_execute_sell can use the real value.
        if mode == "live":
            buy_fee_usdt, _ = _fills_fee_usdt(buy_fills, budget * _fee_rate)
        else:
            buy_fee_usdt = budget * _fee_rate

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
        # Fresh position — clear any stale smart-hold peak left over from a prior
        # sell on this symbol so the trailing stop starts from this entry alone.
        _pos_peaks.pop(sym, None)
        pos_record = {
            "symbol":             sym,
            "entry_price":        fill_price,
            "exit_target":        exit_target,
            "quantity":           qty,
            "budget_usdt":        budget,
            "buy_fee_usdt":       buy_fee_usdt,
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
                pos_record, usdt_balance - budget - buy_fee_usdt
            )
        except Exception as _be:
            try:
                database.log_activity(f"Supabase sync error after buying {sym}: {_be}", "error")
            except Exception:
                pass

        usdt_balance -= budget + buy_fee_usdt

        msg = (
            f"BOUGHT {sym} @ ${fill_price:.4f} "
            f"| qty={qty:.6f} | EXIT TARGET=${exit_target:.4f} "
            f"| {sig_str} | {bb_str} {m5_str} | count:{score}/6"
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
        # Rate-limit retries: stop-loss retries immediately, take-profit waits 5s.
        last_fail = _sell_last_failed_ts.get(sym, 0)
        if last_fail:
            _cooldown = (_SELL_RETRY_COOLDOWN_LOSS
                         if _sell_last_failed_reason.get(sym, "") in ("stop-loss", "force-sell")
                         else _SELL_RETRY_COOLDOWN_PROFIT)
            if (now - last_fail) < _cooldown:
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
    Batch-fetch current prices via REST.
    Tries public CDN first (rarely geo-blocked from Railway), then API mirrors.
    If the multi-symbol batch endpoint fails, falls back to individual symbol
    fetches so a single invalid/delisted coin can't block all prices.
    Timeout 2 s per URL — fast-fail so we never block sell checks.
    """
    import urllib.request as _ur
    import urllib.parse as _up
    if not symbols:
        return {}
    result: Dict[str, float] = {}

    def _parse_response(data) -> Dict[str, float]:
        out: Dict[str, float] = {}
        if isinstance(data, list):
            for item in data:
                s = item.get("symbol", "")
                p = float(item.get("price", 0) or 0)
                if s and p > 0:
                    out[s] = p
        elif isinstance(data, dict) and data.get("symbol"):
            p = float(data.get("price", 0) or 0)
            if p > 0:
                out[data["symbol"]] = p
        return out

    syms_param = _up.quote(json.dumps(symbols))
    for base in _KLINE_BASES[:3]:   # try at most 3 endpoints; fail fast
        try:
            url = f"{base}/api/v3/ticker/price?symbols={syms_param}"
            req = _ur.Request(url, headers={"User-Agent": "TradingBot/1.0"})
            with _ur.urlopen(req, timeout=2) as resp:
                data = json.loads(resp.read())
            result = _parse_response(data)
            if result:
                return result
        except Exception:
            continue

    # Batch endpoint failed on all bases — try individual fetches (slower but
    # more reliable: a single delisted coin won't break all others).
    if not result and len(symbols) <= 20:  # cap individual fetches to avoid flooding
        for sym in symbols:
            for base in _KLINE_BASES[:2]:
                try:
                    url = f"{base}/api/v3/ticker/price?symbol={sym}"
                    req = _ur.Request(url, headers={"User-Agent": "TradingBot/1.0"})
                    with _ur.urlopen(req, timeout=2) as resp:
                        data = json.loads(resp.read())
                    parsed = _parse_response(data)
                    if parsed:
                        result.update(parsed)
                        break   # got price for this sym — next sym
                except Exception:
                    continue

    return result


_price_refresher_thread: Optional[threading.Thread] = None
_rest_fail_log_ts: float = 0.0   # throttle REST-failure warning to once per 60 s

def _price_refresher_loop():
    """
    Dedicated background thread — fetches REST prices for all open positions
    every 2 s and writes them to _rest_px.  Runs independently of the sell
    monitor so network I/O NEVER delays a sell check.
    """
    import data_collector as _dc
    global _rest_px, _rest_px_ts, _rest_fail_log_ts

    while True:
        try:
            with _positions_lock:
                snap = list(_positions)
            if snap:
                all_syms = list({p["symbol"] for p in snap})
                fetched = _fetch_rest_prices(all_syms)
                if fetched:
                    _rest_px.update(fetched)
                    _rest_px_ts = time.time()
                    # Inject into _dc.prices for position symbols that aren't in the
                    # backend WebSocket subscription — ensures realtime_monitor and
                    # the sell monitor both have prices for all held coins.
                    for s, p in fetched.items():
                        if s not in _dc.prices or _dc.prices[s] <= 0:
                            _dc.prices[s] = p
                else:
                    # REST unavailable — log once per minute so Railway logs show it
                    now_rf = time.time()
                    if now_rf - _rest_fail_log_ts >= 60.0:
                        _rest_fail_log_ts = now_rf
                        missing = [p["symbol"] for p in snap if _rest_px.get(p["symbol"], 0) <= 0]
                        try:
                            database.log_activity(
                                f"Price refresher: REST fetch returned empty for {all_syms}; "
                                f"symbols with no price: {missing}", "warn"
                            )
                        except Exception:
                            pass

                    # Fill gaps from signal cache
                    with _signal_cache_lock:
                        for pos in snap:
                            s = pos["symbol"]
                            if _rest_px.get(s, 0) <= 0:
                                sc = _signal_cache.get(s)
                                if sc and sc.get("price", 0) > 0:
                                    _rest_px[s] = sc["price"]
                    # Last resort: carry forward any WS price we already have
                    for pos in snap:
                        s = pos["symbol"]
                        if _rest_px.get(s, 0) <= 0:
                            ws_p = _dc.prices.get(s, 0)
                            if ws_p > 0:
                                _rest_px[s] = ws_p
        except Exception:
            pass
        time.sleep(2.0)


def _sell_monitor_loop():
    """
    Fallback daemon thread — wakes every 0.5 s and checks sell conditions.
    Never does any network I/O — REST prices are fetched by _price_refresher_loop
    (a separate daemon thread) and written to _rest_px.  Decoupling means sell
    checks are NEVER delayed by a slow HTTP request.

    Price priority: REST (_rest_px, refreshed every 2 s) > WebSocket (_dc.prices).
    Both are applied before the sell check so the freshest price is always used.
    """
    import data_collector as _dc
    global _sell_diag_ts, _sell_monitor_heartbeat

    try:
        database.log_activity("Sell monitor thread started", "info")
    except Exception:
        pass

    while True:
        _sell_monitor_heartbeat = time.time()
        try:
            with _positions_lock:
                snap = list(_positions)

            # ── Watchdog: force-clear _selling entries stuck > 90 s ──────────
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

            # Always refresh TP/SL multipliers — settings can change at any time.
            _refresh_risk_params()

            # Build price dict: start with WebSocket, then override with REST.
            # _rest_px is maintained by _price_refresher_loop — no I/O here.
            prices = dict(_dc.prices)
            for sym2, p2 in _rest_px.items():
                if p2 > 0:
                    prices[sym2] = p2   # REST always overrides stale WS prices

            # Signal-cache is the final fallback — fills gaps when both WS and
            # REST are unavailable for a symbol (e.g. first 30 s after redeploy).
            with _signal_cache_lock:
                sc_snap = dict(_signal_cache)
            for pos in snap:
                s = pos["symbol"]
                if prices.get(s, 0) <= 0:
                    sc_p = sc_snap.get(s, {}).get("price", 0)
                    if sc_p and sc_p > 0:
                        prices[s] = sc_p

            # Diagnostic log every 60 s — shows ACTUAL sell target, not just breakeven
            now_t = time.time()
            if now_t - _sell_diag_ts >= 60.0:
                _sell_diag_ts = now_t
                lines = []
                no_price_syms = []
                for p in snap:
                    sym    = p["symbol"]
                    price  = prices.get(sym, 0.0)
                    entry  = p["entry_price"]
                    if price <= 0:
                        no_price_syms.append(sym)
                        lines.append(f"{sym} NO_PRICE(entry={entry:.4f})")
                        continue
                    actual = entry * _take_profit_mult   # real sell threshold
                    pct    = ((price - entry) / entry * 100) if entry else 0
                    lines.append(
                        f"{sym} entry={entry:.4f} cur={price:.4f}({pct:+.3f}%) "
                        f"target={actual:.4f}({'SELL' if price >= actual else 'hold'})"
                    )
                msg = f"Sell monitor: {len(snap)} open — " + " | ".join(lines)
                if no_price_syms:
                    msg += f" | WARN: no price for {no_price_syms}"
                database.log_activity(msg, "info")

            now_monitor = time.time()
            for pos in snap:
                sym   = pos["symbol"]
                price = prices.get(sym, 0.0)
                if price <= 0:
                    continue
                with _selling_lock:
                    if sym in _selling:
                        continue
                # Rate-limit retries: stop-loss retries immediately, take-profit waits 5s.
                last_fail = _sell_last_failed_ts.get(sym, 0)
                if last_fail:
                    _cooldown = (_SELL_RETRY_COOLDOWN_LOSS
                                 if _sell_last_failed_reason.get(sym, "") in ("stop-loss", "force-sell")
                                 else _SELL_RETRY_COOLDOWN_PROFIT)
                    if (now_monitor - last_fail) < _cooldown:
                        continue
                entry  = pos["entry_price"]
                target = entry * _take_profit_mult
                stop   = entry * _stop_loss_mult

                sell_reason3: Optional[str] = None

                if price >= target:
                    if _smart_hold_enabled:
                        _pos_peaks[sym] = max(_pos_peaks.get(sym, target), price)
                        peak       = _pos_peaks[sym]
                        trail_stop = peak * (1.0 - _trailing_stop_pct / 100.0)
                        if price <= trail_stop:
                            sell_reason3 = "smart-hold-trail"
                        else:
                            with _signal_cache_lock:
                                score2 = _signal_cache.get(sym, {}).get("score", 0)
                            if score2 < 3:
                                sell_reason3 = "take-profit"
                    else:
                        sell_reason3 = "take-profit"
                elif _stop_loss_mult < 1.0 and price <= stop:
                    sell_reason3 = "stop-loss"

                if sell_reason3:
                    with _selling_lock:
                        if sym in _selling:
                            continue
                        _selling.add(sym)
                        _selling_ts[sym] = time.time()
                    _sell_executor.submit(_execute_sell, pos, price, sell_reason3)

        except Exception as exc:
            try:
                database.log_activity(f"Sell monitor error: {exc}", "error")
            except Exception:
                pass
        # No blocking I/O here — 0.5 s gives ~0.5 s worst-case exit delay.
        time.sleep(0.5)


async def position_guardian():
    """
    Watchdog coroutine — starts the sell monitor and price refresher threads
    and restarts them if they ever die (belt-and-suspenders).
    """
    global _sell_monitor_thread, _price_refresher_thread
    while True:
        if not (_sell_monitor_thread and _sell_monitor_thread.is_alive()):
            _sell_monitor_thread = threading.Thread(
                target=_sell_monitor_loop, name="sell-monitor", daemon=True
            )
            _sell_monitor_thread.start()
        if not (_price_refresher_thread and _price_refresher_thread.is_alive()):
            _price_refresher_thread = threading.Thread(
                target=_price_refresher_loop, name="price-refresher", daemon=True
            )
            _price_refresher_thread.start()
        await asyncio.sleep(5.0)


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
        closes = volumes = candles = None

        # 1. Try Binance REST (fastest, most data — includes full OHLC for ATR)
        try:
            raw     = await _fetch_klines(session, sym)
            closes  = [float(k[4]) for k in raw]
            volumes = [float(k[5]) for k in raw]
            candles = [
                {"high": float(k[2]), "low": float(k[3]),
                 "close": float(k[4]), "volume": float(k[5])}
                for k in raw
            ]
        except Exception:
            pass

        # 2. Fall back to DB candles (populated by download_history / kline saves)
        if not closes or len(closes) < MIN:
            db_rows = database.get_candles(sym, config.CANDLE_TIMEFRAME, limit=60)
            if len(db_rows) >= MIN:
                closes  = [float(c["close"])  for c in db_rows]
                volumes = [float(c["volume"]) for c in db_rows]
                candles = [
                    {"high":   float(c.get("high") or c["close"]),
                     "low":    float(c.get("low")  or c["close"]),
                     "close":  float(c["close"]),
                     "volume": float(c["volume"])}
                    for c in db_rows
                ]

        # 3. Fall back to in-memory WebSocket candle buffer (no REST needed)
        #    This kicks in when Binance REST is geo-blocked from Railway's servers.
        #    Fills up at 1 candle/minute; RSI fires after 16 minutes.
        if not closes or len(closes) < MIN:
            buf = _dc.ws_candles.get(sym, [])
            if len(buf) >= MIN:
                closes  = [float(r[4]) for r in buf]
                volumes = [float(r[5]) for r in buf]
                candles = [
                    {"high": float(r[2]), "low": float(r[3]),
                     "close": float(r[4]), "volume": float(r[5])}
                    for r in buf
                ]

        # 4. Time-sampled price buffer — meaningful RSI after 8 min; no OHLC.
        if not closes or len(closes) < MIN:
            samples = list(_dc.price_samples.get(sym, []))
            if len(samples) >= _dc._MIN_SAMPLES:
                closes  = samples
                volumes = [1.0] * len(samples)
                candles = [{"high": c, "low": c, "close": c, "volume": 1.0} for c in samples]

        # 5. Raw price-tick buffer — available in seconds but RSI quality is poor.
        #    Used only as last resort (first 8 minutes of uptime).
        if not closes or len(closes) < MIN:
            ticks = list(_dc.price_ticks.get(sym, []))
            if len(ticks) >= _dc._MIN_TICKS:
                closes  = ticks
                volumes = [1.0] * len(ticks)
                candles = [{"high": c, "low": c, "close": c, "volume": 1.0} for c in ticks]

        if not (closes and len(closes) >= MIN and candles):
            return False

        # Evaluate 6 signals (ATR accurate only when real OHLC is available)
        signals = evaluate_signals(candles)
        score   = sum(signals.values())
        rsi_list    = indicators.calc_rsi(closes, 14)
        rsi_display = rsi_list[-2] if rsi_list[-2] is not None else 0.0

        # BB veto — computed on the last completed candle (index -2)
        bb_u, bb_m, _ = indicators.calc_bollinger(closes)
        bb_ok = indicators.bb_buy_allowed(closes[-2], bb_u[-2], bb_m[-2])

        # 5m timeframe veto — fetched once per coin per 60 s scan cycle
        try:
            candles_5m = await asyncio.to_thread(_dc.fetch_5m_candles, sym)
            five_m_ok  = indicators.is_5m_bullish(candles_5m)
        except Exception:
            five_m_ok  = True  # don't block trades if 5m fetch fails

        with _signal_cache_lock:
            _signal_cache[sym] = {
                "signals":  signals,
                "score":    score,
                "price":    closes[-1],
                "rsi_val":  rsi_display,
                "bb_ok":    bb_ok,
                "5m_ok":    five_m_ok,
            }
        return True

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
