"""
Trading bot entry point.
Start order: DB → strategy.json → control API → history download → WebSocket + strategy loop
"""

import asyncio
import os
import sys

# Ensure CWD is the trading-bot directory regardless of where the process
# was launched from (e.g. Railway runs from repo root: python trading-bot/main.py)
os.chdir(os.path.dirname(os.path.abspath(__file__)))

# Add trading-bot dir to sys.path so sibling imports work from any CWD
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv

load_dotenv()


def _ensure_env():
    """Create .env from .env.example if missing."""
    if not os.path.exists(".env"):
        if os.path.exists(".env.example"):
            import shutil
            shutil.copy(".env.example", ".env")
            print("[Main] Created .env from .env.example — using paper mode defaults.")
        else:
            # Write minimal .env
            with open(".env", "w") as f:
                f.write("MODE=paper\nSTARTING_PAPER_USDT=10000.0\n")
            print("[Main] Created minimal .env — paper mode, $10,000 USDT.")


def main():
    _ensure_env()

    # 1. Initialise database FIRST — before any other import that touches SQLite.
    #    Importing data_collector triggers connection.py → PaperClient → load_paper_state(),
    #    which will crash with "no such table" if init_db() hasn't run yet.
    import database
    database.init_db()

    import data_collector
    import trade_engine
    import strategy_engine
    import control_api
    from connection import get_mode

    # 2. Always regenerate strategy.json so config.py changes take effect.
    #    trading_active defaults to True (always active after restart).
    strategy_engine.write_default_strategy()

    # 3. Restore open positions from DB (crash recovery)
    trade_engine.load_positions_from_db()

    # 4. Start FastAPI dashboard on port 8000
    control_api.start_control_api()

    # 5. Download candle history (skipped if already loaded)
    data_collector.download_history()

    # 6. Register callbacks
    #    price callback  → real-time exits AND buy-cache checks on every tick
    #    kline callback  → update signal cache on every 1-minute candle close
    data_collector.register_price_callback(trade_engine.realtime_monitor)
    data_collector.register_kline_callback(trade_engine.update_coin_signals)

    print("\n[Main] All systems ready. Starting live feeds…\n")

    # 7. Run WebSocket feed and strategy loop concurrently
    asyncio.run(_run_loops())


async def _run_loops():
    import data_collector
    import strategy_engine
    import trade_engine

    await asyncio.gather(
        data_collector.start_websocket(),
        strategy_engine.strategy_loop(),
        trade_engine.signal_scanner(data_collector.prices),
    )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[Main] Stopped by user.")
        sys.exit(0)
