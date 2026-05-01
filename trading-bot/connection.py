"""
Mode-aware client factory.
All other files import `client` from here — they never check MODE themselves.
"""

import os
from dotenv import load_dotenv

load_dotenv()
MODE = os.getenv("MODE", "paper").lower()


def _build_client():
    if MODE == "live":
        from binance.client import Client as BinanceClient
        c = BinanceClient(
            os.getenv("BINANCE_API_KEY"),
            os.getenv("BINANCE_API_SECRET"),
        )
        # Attach no-op update_price so data_collector works in every mode
        c.update_price = lambda symbol, price: None
        return c

    elif MODE == "testnet":
        from binance.client import Client as BinanceClient
        c = BinanceClient(
            os.getenv("TESTNET_API_KEY"),
            os.getenv("TESTNET_API_SECRET"),
            testnet=True,
        )
        c.update_price = lambda symbol, price: None
        return c

    else:  # paper (default)
        from paper_client import PaperClient
        return PaperClient(
            starting_usdt=float(os.getenv("STARTING_PAPER_USDT", "10000.0")),
            fee_rate=0.00075,
        )


client = _build_client()


def get_mode() -> str:
    return MODE
