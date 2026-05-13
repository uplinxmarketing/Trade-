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

_fee_rate = config.FEE_RATE  # 0.1% standard Binance spot
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
_breakeven_mult = 1.0 / (0.999 ** 2)  # exact: entry / (0.999²) ≈ entry × 1.002003

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
_SELL_RETRY_COOLDOWN_PROFIT = 2.0   # take-profit: 2s breaks retry loop without delaying exit
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


# ── Balance guard + budget helpers ──────────────────────────────────────────────

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

    # In fixed/per_coin mode return the full configured amount.
    # _check_balance / can_execute_buy will reject the buy if free USDT is insufficient —
    # that gives a clean "have X, need Y" message rather than silently trading a partial amount.
    if mode in ("fixed", "per_coin"):
        return round(base, 2)
    # For capped mode cap to 90% so a tiny buffer remains for fees.
    return round(min(base, effective_free * 0.9), 2)


# ── Cooldown helpers ───────────────────────────────────────────────────────

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


# ── DB / startup helpers ─────────────────────────────────────────────────────

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
    # Filter by current mode so live positions never load into a paper session
    # and vice versa — cross-loading caused paper client to "sell" live positions.
    _current_mode = get_mode()
    rows = database.load_positions(mode=_current_mode)

    # ── Startup migration: fix positions saved with wrong/missing mode tag ──────
    # Positions created before the mode-isolation fix default to mode='paper'
    # even when bought in live mode. If we're in live mode and find no positions
    # but the unfiltered table has rows, migrate them rather than losing them.
    if not rows:
        all_rows = database.load_positions()  # unfiltered
        if all_rows:
            database.migrate_positions_mode(_current_mode)
            rows = database.load_positions(mode=_current_mode)
            if rows:
                database.log_activity(
                    f"Startup: migrated {len(rows)} position(s) to mode='{_current_mode}' "
                    f"(were stored with wrong/missing mode tag)", "warn"
                )

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
            rows = database.load_positions(mode=_current_mode)
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

        # ── Trade history: restore when SQLite trades table is empty ──────────────────
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

        # ── Balance: restore when SQLite paper-state is empty or zero ────────────
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

    # Live mode: scan Binance Spot for coins the bot has no position for.
    # AUTO-RECOVER orphaned positions so real money is never invisible to the bot.
    # Positions are lost when the DB is wiped (container restart without a volume)
    # or when Supabase restore fails. We rebuild records from Binance balances so
    # the sell monitor immediately tracks them and can exit at take-profit/stop-loss.
    if get_mode() == "live":
        try:
            acc = client.get_account()
            tracked_symbols = {p["symbol"] for p in _positions}
            recovered_syms = []
            now_ts = datetime.now(timezone.utc).isoformat()

            for b in acc["balances"]:
                asset  = b["asset"]
                free   = float(b["free"])
                locked = float(b["locked"])
                total  = free + locked
                if asset in ("USDT", "BNB", "BUSD", "USDC") or total <= 0:
                    continue
                sym = asset + "USDT"
                if sym in tracked_symbols:
                    continue

                # Fetch current price — try cached REST price first, then live ticker
                price = _rest_px.get(sym, 0) or 0
                if price <= 0:
                    try:
                        ticker = client.get_symbol_ticker(symbol=sym)
                        price  = float(ticker.get("price", 0) or 0)
                    except Exception:
                        pass

                # Use only free balance — locked qty can't be market-sold and
                # would cause -2010 "insufficient balance" on Binance.
                sell_qty = free if free > 0 else total
                value = sell_qty * price if price > 0 else 0

                if price > 0 and value > 1.0:
                    # Recreate position using current price as estimated entry.
                    # Entry is unknown after a DB wipe — current price is the safest
                    # default: the bot will monitor from here and exit on TP/SL.
                    exit_target = round(price * _breakeven_mult, 8)
                    pos_record = {
                        "symbol":       sym,
                        "entry_price":  price,
                        "exit_target":  exit_target,
                        "quantity":     sell_qty,
                        "budget_usdt":  round(value, 2),
                        "buy_fee_usdt": round(value * _fee_rate, 6),
                        "timestamp":    now_ts,
                        "mode":         "live",
                    }
                    pos_id = database.save_position(pos_record)
                    pos_record["id"] = pos_id
                    with _positions_lock:
                        _positions.append(pos_record)
                    tracked_symbols.add(sym)
                    recovered_syms.append(sym)
                    msg = (
                        f"AUTO-RECOVERED orphaned live position: {sym} "
                        f"qty={total:.8f} @ ${price:.4f} (~${value:.2f} USDT) — "
                        f"entry price is ESTIMATED (current price used). "
                        f"Adjust stop-loss in dashboard if needed."
                    )
                    print(f"[TradeEngine] {msg}")
                    database.log_activity(msg, "warn")
                else:
                    msg = (
                        f"⚠ ORPHANED COIN: {asset} qty={total:.8f} on Binance "
                        f"but no price available — sell manually via Binance app."
                    )
                    print(f"[TradeEngine] {msg}")
                    database.log_activity(msg, "warn")

            if recovered_syms:
                _rebuild_pos_index()
                database.log_activity(
                    f"Recovered {len(recovered_syms)} orphaned live position(s) "
                    f"from Binance balances: {', '.join(recovered_syms)}", "warn"
                )
        except Exception as e:
            database.log_activity(f"Orphan scan/recovery failed (non-fatal): {e}", "warn")

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


# ── Strategy loader ─────────────────────────────────────────────────────────────

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


# ── Account helpers ─────────────────────────────────────────────────────────────

def _get_usdt_balance() -> float:
    try:
        if get_mode() != "live":
            # Paper mode: read directly from PaperClient's in-memory balance dict
            if hasattr(client, "_balances"):
                with client._lock:
                    return float(client._balances.get("USDT", 0.0))
            return float(os.getenv("STARTING_PAPER_USDT", "10000.0"))
        # Live mode: query Binance spot wallet — use free balance only (tradeable)
        acc = client.get_account()
        for b in acc["balances"]:
            if b["asset"] == "USDT":
                return float(b["free"])
    except Exception as e:
        database.log_activity(f"get_usdt_balance error: {e}", "warn")
    return 0.0


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


# ── Indicator helpers (derive from candle dict) ────────────────────────────────

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


# ── Signal evaluation — 6-signal dict ──────────────────────────────────────────────

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

        # -1013: market closed/delisted — blacklist and force-close
        # -2010: insufficient balance — ghost position (paper position carried into live),
        #        bot doesn't actually own the coin; force-close so retries stop
        is_ghost = "-2010" in err_str or "insufficient balance" in err_str.lower()
        is_closed = "-1013" in err_str or "Market is closed" in err_str
        if is_closed:
            _bad_symbols.add(sym)
        if is_closed or is_ghost:
            reason_label = "market closed/delisted" if is_closed else "ghost position (no coin balance on Binance)"
            database.log_activity(
                f"{sym}: force-closing position — {reason_label}", "warn"
            )
            with _positions_lock:
                before = len(_positions)
                _positions[:] = [p for p in _positions if p.get("symbol") != sym]
                if len(_positions) < before:
                    # Remove from DB too so it doesn't come back on restart
                    pos_id = pos.get("id")
                    if pos_id:
                        try:
                            database.delete_position(pos_id)
                        except Exception:
                            pass
                    database.log_activity(f"{sym}: position removed from records", "warn")
            _rebuild_pos_index()
        return

    # ── Fee-aware fill parsing ────────────────────────────────────────────────────
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
    # Remove position from memory and DB immediately — sell is confirmed on Binance.
    # Supabase sync and learning run after so a slow network never delays cleanup.
    if pos.get("id"):
        try: