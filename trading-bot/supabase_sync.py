"""Supabase sync DISABLED — this module is a stub and performs NO cloud backup.

All state lives ONLY in local SQLite. Every function below is a no-op that
returns a falsy value; none of them must ever be treated as a successful
backup or restore.
"""

print("[SupabaseSync] WARNING: Supabase sync disabled — no cloud backup. "
      "All persistence is the local SQLite file only.")


def restore_from_supabase():
    """No-op: nothing to restore. Returns an empty dict."""
    return {}


def sync_all(positions, usdt):
    """No-op: nothing is backed up."""
    return False


def sync_buy_result_sync(pos, usdt):
    """No-op: nothing is backed up."""
    return False


def sync_sell_result_sync(trade, sym, usdt):
    """No-op: nothing is backed up."""
    return False


def sync_positions(positions):
    """No-op: nothing is backed up."""
    return False


def sync_balance(usdt):
    """No-op: nothing is backed up."""
    return False


def sync_selected_coins(coins):
    """No-op: nothing is backed up. (Called by control_api/strategy_engine.)"""
    return False
