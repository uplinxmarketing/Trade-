"""Replay historical candles at accelerated speed to build the pattern library."""

import time
from typing import Optional

import config
import database
import trade_engine
from connection import client


def run_backtest(symbol: str, speed_multiplier: int = 60):
    """Replay all candles for one symbol from DB."""
    candles = database.get_candles(symbol, config.CANDLE_TIMEFRAME, limit=10000)
    if not candles:
        print(f"[Backtest] No candles for {symbol} — run download_history() first.")
        return

    print(f"[Backtest] Replaying {len(candles)} candles for {symbol} at {speed_multiplier}× speed…")
    start_balance = None
    trades_before = len(database.get_recent_trades(limit=9999))

    for i, candle in enumerate(candles):
        price = candle.get("close", 0)
        if price <= 0:
            continue
        client.update_price(symbol, price)
        trade_engine.trade_loop({symbol: price})
        time.sleep(1 / speed_multiplier)

        if i % 100 == 0:
            print(f"  {symbol}: {i}/{len(candles)} candles — price=${price:.4f}")

    trades_after = len(database.get_recent_trades(limit=9999))
    n_new = trades_after - trades_before
    trades = database.get_recent_trades(limit=n_new) if n_new > 0 else []
    wins = sum(1 for t in trades if t.get("profitable"))
    win_rate = wins / len(trades) if trades else 0
    net = sum((t.get("net_profit") or 0) for t in trades)

    print(f"[Backtest] {symbol} complete: {n_new} trades | win_rate={win_rate:.1%} | net=${net:.2f}")


def run_full_backtest(speed_multiplier: int = 120):
    """Run backtest for all coins sequentially."""
    print(f"[Backtest] Full backtest at {speed_multiplier}×…")
    for coin in config.WATCHED_COINS:
        run_backtest(coin, speed_multiplier)
    print("[Backtest] All coins done.")
