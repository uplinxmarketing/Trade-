"""
Supabase sync — fire-and-forget background writes so trade data survives
Railway redeploys entirely in code with no infrastructure changes.

All writes are dispatched to a ThreadPoolExecutor; the calling code never
blocks.  On startup, restore_from_supabase() is called to recover open
positions and USDT balance from the last known good Supabase state when
SQLite is empty (i.e. after a fresh Railway deploy).
"""

import json
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Optional
from urllib import request
from urllib.error import HTTPError

# ── Credentials ───────────────────────────────────────────────────────────────
# Railway Supabase connector injects SUPABASE_URL + SUPABASE_ANON_KEY.
# Fall back to VITE_ prefixed vars (same values used by the React frontend)
# and finally to the hardcoded project values.
SUPABASE_URL = (
    os.getenv("SUPABASE_URL")
    or os.getenv("VITE_SUPABASE_URL")
    or "https://hkwirofdkgdamqnlcjqf.supabase.co"
)
SUPABASE_KEY = (
    os.getenv("SUPABASE_ANON_KEY")
    or os.getenv("VITE_SUPABASE_PUBLISHABLE_KEY")
    or "sb_publishable_6p26q62HNaU7pqv9k9jb_w_8S0ixNrP"
)

# Separate session tag from the browser frontend ("default") so rows never collide.
SESSION = "railway_bot"

_enabled = bool(SUPABASE_URL and SUPABASE_KEY)
_pool = ThreadPoolExecutor(max_workers=2, thread_name_prefix="supa-sync")


# ── Low-level REST helpers ────────────────────────────────────────────────────

def _hdrs(prefer: str = "") -> dict:
    h = {
        "apikey":        SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type":  "application/json",
    }
    if prefer:
        h["Prefer"] = prefer
    return h


def _post(table: str, payload: dict, upsert: bool = False):
    prefer = "resolution=merge-duplicates,return=minimal" if upsert else "return=minimal"
    url  = f"{SUPABASE_URL}/rest/v1/{table}"
    data = json.dumps(payload).encode()
    req  = request.Request(url, data=data, headers=_hdrs(prefer), method="POST")
    try:
        with request.urlopen(req, timeout=8):
            pass
    except HTTPError as e:
        print(f"[SupaSync] POST {table} HTTP {e.code}: {e.read().decode('utf-8','replace')[:200]}")
    except Exception as e:
        print(f"[SupaSync] POST {table} error: {e}")


def _delete(table: str, query: str):
    url = f"{SUPABASE_URL}/rest/v1/{table}?{query}"
    req = request.Request(url, headers=_hdrs("return=minimal"), method="DELETE")
    try:
        with request.urlopen(req, timeout=8):
            pass
    except HTTPError as e:
        print(f"[SupaSync] DELETE {table} HTTP {e.code}: {e.read().decode('utf-8','replace')[:200]}")
    except Exception as e:
        print(f"[SupaSync] DELETE {table} error: {e}")


def _get(table: str, query: str) -> Optional[list]:
    url = f"{SUPABASE_URL}/rest/v1/{table}?{query}"
    req = request.Request(url, headers=_hdrs(), method="GET")
    try:
        with request.urlopen(req, timeout=8) as resp:
            return json.loads(resp.read())
    except HTTPError as e:
        print(f"[SupaSync] GET {table} HTTP {e.code}: {e.read().decode('utf-8','replace')[:200]}")
        return None
    except Exception as e:
        print(f"[SupaSync] GET {table} error: {e}")
        return None


def _bg(fn, *args):
    if _enabled:
        _pool.submit(fn, *args)


# ── Public API ────────────────────────────────────────────────────────────────

def sync_trade(trade: dict):
    """Mirror a completed trade (both buy + sell rows) to bot_trade_history."""
    _bg(_sync_trade_impl, trade)


def _sync_trade_impl(trade: dict):
    now = datetime.now(timezone.utc).isoformat()
    # Buy row
    _post("bot_trade_history", {
        "user_session": SESSION,
        "symbol":       trade.get("coin", ""),
        "side":         "buy",
        "price":        trade.get("entry_price", 0),
        "quantity":     trade.get("quantity", 0),
        "pnl":          None,
        "reason":       f"entry | {trade.get('mode','paper')}",
        "bot_id":       SESSION,
        "created_at":   trade.get("timestamp_buy") or now,
    })
    # Sell row
    _post("bot_trade_history", {
        "user_session": SESSION,
        "symbol":       trade.get("coin", ""),
        "side":         "sell",
        "price":        trade.get("exit_price", 0),
        "quantity":     trade.get("quantity", 0),
        "pnl":          trade.get("net_profit", 0),
        "reason":       f"duration={trade.get('duration_seconds',0)}s | {trade.get('mode','paper')}",
        "bot_id":       SESSION,
        "created_at":   trade.get("timestamp_sell") or now,
    })


def sync_position_open(pos: dict):
    """Upsert an open position into paper_portfolio."""
    _bg(_sync_position_open_impl, pos)


def _sync_position_open_impl(pos: dict):
    _post("paper_portfolio", {
        "user_session":    SESSION,
        "symbol":          pos.get("symbol", ""),
        "quantity":        pos.get("quantity", 0),
        "avg_entry_price": pos.get("entry_price", 0),
        "updated_at":      datetime.now(timezone.utc).isoformat(),
        "bot_id":          SESSION,
    }, upsert=True)


def sync_position_close(symbol: str):
    """Remove a closed position from paper_portfolio."""
    _bg(_delete, "paper_portfolio",
        f"user_session=eq.{SESSION}&symbol=eq.{symbol}")


def sync_balance(usdt: float):
    """Persist the current USDT balance to bot_config."""
    _bg(_sync_balance_impl, usdt)


def sync_selected_coins(coins: list):
    """Persist the bot's selected coin list to bot_config."""
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
    Called on startup when SQLite is empty.  Fetches the last known open
    positions and USDT balance from Supabase so a fresh Railway deploy picks
    up exactly where the previous one left off.

    Returns {"positions": [...], "usdt_balance": float|None}.
    Returns {} on any network error so callers can treat it as a no-op.
    """
    if not _enabled:
        return {}

    result: dict = {"positions": [], "usdt_balance": None}

    try:
        rows = _get("paper_portfolio",
                    f"user_session=eq.{SESSION}&select=symbol,quantity,avg_entry_price,updated_at")
        if rows:
            for r in rows:
                qty   = float(r.get("quantity") or 0)
                price = float(r.get("avg_entry_price") or 0)
                if qty <= 0 or price <= 0:
                    continue
                result["positions"].append({
                    "symbol":      r["symbol"],
                    "entry_price": price,
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

    n = len(result["positions"])
    bal = result["usdt_balance"]
    if n or bal is not None:
        print(f"[SupaSync] Restored from Supabase: {n} position(s), balance={bal}")

    return result
