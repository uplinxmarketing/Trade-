"""
Supabase sync — fire-and-forget background writes so trade data survives
Railway redeploys entirely in code with no infrastructure changes.

All writes are dispatched to a ThreadPoolExecutor; the calling code never
blocks.  On startup, restore_from_supabase() is called to recover open
positions and USDT balance when SQLite is empty (i.e. after a fresh deploy).

Schema reality (from types.ts):
  bot_trade_history : user_session, symbol, side, price, quantity, pnl, reason, bot_id(FK/null), created_at
  paper_portfolio   : user_session, symbol, quantity, avg_entry_price, updated_at, bot_id(FK/null), id
  bot_config        : user_session, current_balance, initial_balance, is_running, mode,
                      selected_coins, min_profit_percent, stop_loss_percent, updated_at, id

IMPORTANT: bot_id is a FK to trading_bots.id — never send a freeform string.
           Leave it NULL (omit the key entirely) so the constraint is never violated.
"""

import json
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Optional
from urllib import request
from urllib.error import HTTPError

# ── Credentials ───────────────────────────────────────────────────────────────
# MUST match the frontend Supabase project (src/integrations/supabase/client.ts).
# The bot and the UI share the same Supabase project so trades/positions are
# visible in the UI and survive Railway redeploys.
SUPABASE_URL = (
    os.getenv("SUPABASE_URL")
    or os.getenv("VITE_SUPABASE_URL")
    or "https://vrmnmedrwsddxovqcuns.supabase.co"
)
SUPABASE_KEY = (
    os.getenv("SUPABASE_ANON_KEY")
    or os.getenv("VITE_SUPABASE_PUBLISHABLE_KEY")
    or "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InZybW5tZWRyd3NkZHhvdnFjdW5zIiwicm9sZSI6ImFub24iLCJpYXQiOjE3Nzc3MjYwNTIsImV4cCI6MjA5MzMwMjA1Mn0.sV_iJu0CYQwQ9aQGWgFfusqH92QhPKgThOWrCmKKXJo"
)
SESSION = "railway_bot"

_enabled = bool(SUPABASE_URL and SUPABASE_KEY)
_pool = ThreadPoolExecutor(max_workers=2, thread_name_prefix="supa-sync")


# ── Low-level helpers ─────────────────────────────────────────────────────────

def _hdrs(prefer: str = "") -> dict:
    h = {
        "apikey":        SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type":  "application/json",
    }
    if prefer:
        h["Prefer"] = prefer
    return h


def _post(table: str, payload: dict) -> bool:
    """INSERT one row. Returns True on success."""
    url  = f"{SUPABASE_URL}/rest/v1/{table}"
    data = json.dumps(payload).encode()
    req  = request.Request(url, data=data, headers=_hdrs("return=minimal"), method="POST")
    try:
        with request.urlopen(req, timeout=8):
            return True
    except HTTPError as e:
        print(f"[SupaSync] POST {table} HTTP {e.code}: {e.read().decode('utf-8','replace')[:300]}")
        return False
    except Exception as e:
        print(f"[SupaSync] POST {table} error: {e}")
        return False


def _patch(table: str, query: str, payload: dict) -> int:
    """PATCH rows matching query. Returns count of updated rows (-1 on error)."""
    url  = f"{SUPABASE_URL}/rest/v1/{table}?{query}"
    data = json.dumps(payload).encode()
    req  = request.Request(url, data=data, headers=_hdrs("return=representation"), method="PATCH")
    try:
        with request.urlopen(req, timeout=8) as resp:
            body = json.loads(resp.read() or b"[]")
            return len(body) if isinstance(body, list) else 0
    except HTTPError as e:
        print(f"[SupaSync] PATCH {table} HTTP {e.code}: {e.read().decode('utf-8','replace')[:300]}")
        return -1
    except Exception as e:
        print(f"[SupaSync] PATCH {table} error: {e}")
        return -1


def _delete(table: str, query: str):
    url = f"{SUPABASE_URL}/rest/v1/{table}?{query}"
    req = request.Request(url, headers=_hdrs("return=minimal"), method="DELETE")
    try:
        with request.urlopen(req, timeout=8):
            pass
    except HTTPError as e:
        print(f"[SupaSync] DELETE {table} HTTP {e.code}: {e.read().decode('utf-8','replace')[:300]}")
    except Exception as e:
        print(f"[SupaSync] DELETE {table} error: {e}")


def _get(table: str, query: str) -> Optional[list]:
    url = f"{SUPABASE_URL}/rest/v1/{table}?{query}"
    req = request.Request(url, headers=_hdrs(), method="GET")
    try:
        with request.urlopen(req, timeout=8) as resp:
            return json.loads(resp.read())
    except HTTPError as e:
        print(f"[SupaSync] GET {table} HTTP {e.code}: {e.read().decode('utf-8','replace')[:300]}")
        return None
    except Exception as e:
        print(f"[SupaSync] GET {table} error: {e}")
        return None


def _bg(fn, *args):
    if _enabled:
        _pool.submit(fn, *args)


# ── Upsert helpers (PATCH first, INSERT if no row exists) ─────────────────────

def _upsert_portfolio(pos: dict):
    """Update an existing paper_portfolio row, or insert if it doesn't exist."""
    sym = pos.get("symbol", "")
    now = datetime.now(timezone.utc).isoformat()
    payload = {
        "quantity":        pos.get("quantity", 0),
        "avg_entry_price": pos.get("entry_price", 0),
        "updated_at":      now,
    }
    n = _patch("paper_portfolio",
               f"user_session=eq.{SESSION}&symbol=eq.{sym}", payload)
    if n == 0:
        # Row doesn't exist yet — insert
        _post("paper_portfolio", {
            "user_session":    SESSION,
            "symbol":          sym,
            "quantity":        pos.get("quantity", 0),
            "avg_entry_price": pos.get("entry_price", 0),
            "updated_at":      now,
        })


def _upsert_config(usdt: float):
    """Update the bot_config row for this session, or insert if missing."""
    now = datetime.now(timezone.utc).isoformat()
    payload = {
        "current_balance": usdt,
        "updated_at":      now,
    }
    n = _patch("bot_config", f"user_session=eq.{SESSION}", payload)
    if n == 0:
        _post("bot_config", {
            "user_session":    SESSION,
            "current_balance": usdt,
            "initial_balance": usdt,
            "is_running":      True,
            "mode":            "paper",
            "selected_coins":  [],
            "updated_at":      now,
        })


# ── Public API ────────────────────────────────────────────────────────────────

def sync_trade(trade: dict):
    """Mirror a completed trade (buy + sell rows) to bot_trade_history."""
    _bg(_sync_trade_impl, trade)


def _sync_trade_impl(trade: dict):
    now = datetime.now(timezone.utc).isoformat()
    _post("bot_trade_history", {
        "user_session": SESSION,
        "symbol":       trade.get("coin", ""),
        "side":         "buy",
        "price":        trade.get("entry_price", 0),
        "quantity":     trade.get("quantity", 0),
        "pnl":          None,
        "reason":       f"entry | {trade.get('mode','paper')}",
        "created_at":   trade.get("timestamp_buy") or now,
    })
    _post("bot_trade_history", {
        "user_session": SESSION,
        "symbol":       trade.get("coin", ""),
        "side":         "sell",
        "price":        trade.get("exit_price", 0),
        "quantity":     trade.get("quantity", 0),
        "pnl":          trade.get("net_profit", 0),
        "reason":       f"duration={trade.get('duration_seconds',0)}s | {trade.get('mode','paper')}",
        "created_at":   trade.get("timestamp_sell") or now,
    })


def sync_position_open(pos: dict):
    """Upsert an open position into paper_portfolio."""
    _bg(_upsert_portfolio, pos)


def sync_position_close(symbol: str):
    """Remove a closed position from paper_portfolio."""
    _bg(_delete, "paper_portfolio",
        f"user_session=eq.{SESSION}&symbol=eq.{symbol}")


def sync_balance(usdt: float):
    """Persist the current USDT balance to bot_config."""
    _bg(_upsert_config, usdt)


def sync_selected_coins(coins: list):    """Persist the bot's selected coin list to bot_config."""
    _bg(_sync_coins_impl, list(coins))


def _sync_coins_impl(coins: list):
    now = datetime.now(timezone.utc).isoformat()
    n = _patch("bot_config", f"user_session=eq.{SESSION}",
               {"selected_coins": coins, "updated_at": now})
    if n == 0:
        _post("bot_config", {
            "user_session":    SESSION,
            "current_balance": 0,
            "initial_balance": 0,
            "is_running":      True,
            "mode":            "paper",
            "selected_coins":  coins,
            "updated_at":      now,
        })


def sync_sell_result_sync(trade: dict, symbol: str, usdt: float):
    """
    Write sell result to Supabase synchronously using parallel threads.
    All three calls run at once — total blocking time ≤ 4 s.
    Call from the sell executor thread to ensure data survives Railway redeploys.
    """
    if not _enabled:
        return
    from concurrent.futures import ThreadPoolExecutor as _TPE
    with _TPE(max_workers=3) as _ex:
        f1 = _ex.submit(_sync_trade_impl, trade)
        f2 = _ex.submit(_delete, "paper_portfolio",
                        f"user_session=eq.{SESSION}&symbol=eq.{symbol}")
        f3 = _ex.submit(_upsert_config, usdt)
        for f in (f1, f2, f3):
            try:
                f.result(timeout=4)
            except Exception:
                pass


def sync_buy_result_sync(pos: dict, usdt: float):
    """
    Write buy result to Supabase synchronously using parallel threads.
    Both calls run at once — total blocking time ≤ 4 s.
    """
    if not _enabled:
        return
    from concurrent.futures import ThreadPoolExecutor as _TPE
    with _TPE(max_workers=2) as _ex:
        f1 = _ex.submit(_upsert_portfolio, pos)
        f2 = _ex.submit(_upsert_config, usdt)
        for f in (f1, f2):
            try:
                f.result(timeout=4)
            except Exception:
                pass


def sync_all(positions: list, usdt: float):
    """
    Full state push — call on startup so Supabase always has current data
    even when SQLite was restored from a previous state.  Deletes stale
    portfolio rows first (positions that were closed between restarts).
    """
    _bg(_sync_all_impl, list(positions), usdt)


def _sync_all_impl(positions: list, usdt: float):
    # Remove stale portfolio rows for this session
    _delete("paper_portfolio", f"user_session=eq.{SESSION}")
    # Push each current open position
    for pos in positions:
        _upsert_portfolio(pos)
    # Sync balance
    _upsert_config(usdt)
    print(f"[SupaSync] Full sync: {len(positions)} position(s), balance={usdt:.2f}")


def restore_from_supabase() -> dict:
    """
    Called on startup when SQLite is empty.  Fetches last known open
    positions, USDT balance, selected coins, AND completed trade history
    from Supabase so nothing is lost across Railway redeploys.

    Returns {"positions": [...], "usdt_balance": float|None,
             "selected_coins": [...], "trades": [...]}.
    Returns {} on any network error so callers treat it as a no-op.
    """
    if not _enabled:
        return {}

    result: dict = {"positions": [], "usdt_balance": None, "trades": []}

    try:
        rows = _get("paper_portfolio",
                    f"user_session=eq.{SESSION}&select=symbol,quantity,avg_entry_price,updated_at")
        if rows:
            # fee_rate hard-coded to match trade_engine default (BNB 0.075%)
            _fee = 0.00075
            _bep_mult = 1.0 + _fee + _fee
            for r in rows:
                qty   = float(r.get("quantity") or 0)
                price = float(r.get("avg_entry_price") or 0)
                if qty <= 0 or price <= 0:
                    continue
                result["positions"].append({
                    "symbol":      r["symbol"],
                    "entry_price": price,
                    "exit_target": round(price * _bep_mult, 8),
                    "quantity":    qty,
                    "budget_usdt": round(qty * price, 4),
                    "timestamp":   r.get("updated_at") or datetime.now(timezone.utc).isoformat(),
                    "mode":        "paper",
                })
    except Exception as e:
        print(f"[SupaSync] restore positions error: {e}")

    try:
        rows = _get("bot_config",
                    f"user_session=eq.{SESSION}&select=current_balance,selected_coins&order=updated_at.desc&limit=1")
        if rows:
            if rows[0].get("current_balance"):
                result["usdt_balance"] = float(rows[0]["current_balance"])
            if rows[0].get("selected_coins"):
                result["selected_coins"] = rows[0]["selected_coins"]
    except Exception as e:
        print(f"[SupaSync] restore balance error: {e}")

    # ── Trade history restore ─────────────────────────────────────────────────
    # Reconstruct completed trade records from bot_trade_history so the SQLite
    # trades table (and therefore win-rate, P&L stats) survive Railway redeploys.
    try:
        import re as _re
        from datetime import timedelta as _td

        sell_rows = _get(
            "bot_trade_history",
            f"user_session=eq.{SESSION}&side=eq.sell"
            f"&select=symbol,price,quantity,pnl,reason,created_at"
            f"&order=created_at.desc&limit=500",
        ) or []
        buy_rows = _get(
            "bot_trade_history",
            f"user_session=eq.{SESSION}&side=eq.buy"
            f"&select=symbol,price,reason,created_at"
            f"&order=created_at.desc&limit=500",
        ) or []

        # Index buy rows: symbol → [(datetime, entry_price), ...]
        buy_idx: dict = {}
        for br in buy_rows:
            sym = br.get("symbol", "")
            try:
                ts = datetime.fromisoformat(br["created_at"].replace("Z", "+00:00"))
                buy_idx.setdefault(sym, []).append((ts, float(br.get("price") or 0)))
            except Exception:
                pass

        _fee = 0.00075
        trades = []
        for sr in sell_rows:
            sym = sr.get("symbol", "")
            if not sym:
                continue
            exit_price  = float(sr.get("price")    or 0)
            quantity    = float(sr.get("quantity")  or 0)
            net_profit  = float(sr.get("pnl")       or 0)
            ts_sell_raw = sr.get("created_at", "")
            reason      = sr.get("reason", "")
            if exit_price <= 0 or quantity <= 0:
                continue

            # Parse duration from reason string "duration=NNNs | paper"
            duration = 0
            m = _re.search(r"duration=(\d+)s", reason)
            if m:
                duration = int(m.group(1))

            try:
                sell_dt = datetime.fromisoformat(ts_sell_raw.replace("Z", "+00:00"))
            except Exception:
                continue

            # Match closest buy row by expected buy timestamp
            entry_price = exit_price  # fallback if no match
            ts_buy_raw  = (sell_dt - _td(seconds=max(duration, 1))).isoformat()
            if sym in buy_idx:
                expected_buy = sell_dt - _td(seconds=max(duration, 1))
                best = min(buy_idx[sym],
                           key=lambda x: abs((x[0] - expected_buy).total_seconds()))
                tolerance = max(duration, 60) + 120
                if abs((best[0] - expected_buy).total_seconds()) < tolerance:
                    entry_price = best[1]
                    ts_buy_raw  = best[0].isoformat()

            mode = "live" if "live" in reason else "paper"
            budget_usdt = round(entry_price * quantity, 4)
            trades.append({
                "coin":               sym,
                "mode":               mode,
                "entry_price":        entry_price,
                "exit_price":         exit_price,
                "quantity":           quantity,
                "budget_usdt":        budget_usdt,
                "buy_fee":            round(budget_usdt * _fee, 8),
                "sell_fee":           round(exit_price * quantity * _fee, 8),
                "net_profit":         net_profit,
                "profitable":         1 if net_profit > 0 else 0,
                "duration_seconds":   duration,
                "entry_rsi":          None,
                "entry_ma_position":  None,
                "entry_bb_position":  None,
                "entry_volume_trend": None,
                "hour_of_day":        sell_dt.hour,
                "day_of_week":        sell_dt.weekday(),
                "timestamp_buy":      ts_buy_raw,
                "timestamp_sell":     ts_sell_raw,
            })
        result["trades"] = trades
    except Exception as e:
        print(f"[SupaSync] restore trades error: {e}")

    n   = len(result["positions"])
    t   = len(result["trades"])
    bal = result["usdt_balance"]
    if n or t or bal is not None:
        print(f"[SupaSync] Restored from Supabase: {n} position(s), {t} trade(s), balance={bal}")
    else:
        print("[SupaSync] Nothing to restore from Supabase (first run or no data)")

    return result
