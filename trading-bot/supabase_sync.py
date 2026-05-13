"""Supabase sync disabled — all state lives in local SQLite on the VPS."""

def restore_from_supabase():
    return {}

def sync_all(positions, usdt):
    pass

def sync_buy_result_sync(pos, usdt):
    pass

def sync_sell_result_sync(trade, sym, usdt):
    pass

def sync_positions(positions):
    pass

def sync_balance(usdt):
    pass
