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
        api_key    = (os.getenv("BINANCE_API_KEY")    or "").strip()
        api_secret = (os.getenv("BINANCE_API_SECRET") or "").strip()
        if not api_key or not api_secret:
            raise EnvironmentError(
                "MODE=live requires BINANCE_API_KEY and BINANCE_API_SECRET. "
                "Set them as Railway environment variables or switch back to MODE=paper."
            )
        from binance.client import Client as BinanceClient
        c = BinanceClient(api_key, api_secret)
        try:
            c.ping()
        except Exception as exc:
            raise EnvironmentError(f"Binance API connection failed: {exc}") from exc
        # Attach no-op update_price so data_collector works in every mode
        c.update_price = lambda symbol, price: None
        return c

    elif MODE == "testnet":
        api_key    = (os.getenv("TESTNET_API_KEY")    or "").strip()
        api_secret = (os.getenv("TESTNET_API_SECRET") or "").strip()
        if not api_key or not api_secret:
            raise EnvironmentError(
                "MODE=testnet requires TESTNET_API_KEY and TESTNET_API_SECRET."
            )
        from binance.client import Client as BinanceClient
        c = BinanceClient(api_key, api_secret, testnet=True)
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
