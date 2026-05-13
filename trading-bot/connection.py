"""
Mode-aware client factory.
All other files import `client` from here — they never check MODE themselves.
"""

import os
import pathlib
from dotenv import load_dotenv

# Use absolute path so the correct .env is found regardless of systemd WorkingDirectory.
_ENV_PATH = pathlib.Path(__file__).parent / ".env"

# override=True ensures .env values always win over inherited process environment.
# Without this, os.execv() passes the OLD MODE to the restarted process and
# load_dotenv() silently skips the updated value — bot stays in paper mode forever.
load_dotenv(_ENV_PATH, override=True)

# _CONFIGURED_MODE is the authoritative mode from .env — it NEVER changes at runtime.
# A Binance connection failure must not alter this: get_mode() always returns what
# the user set, so /api/status, position tagging, and the frontend all stay consistent.
_CONFIGURED_MODE: str = os.getenv("MODE", "paper").lower()

_live_error: str = ""   # set if live connection failed; readable via get_live_error()
_using_paper_fallback: bool = False  # True when live was configured but connection failed


def _build_client():
    global _live_error, _using_paper_fallback

    if _CONFIGURED_MODE == "live":
        api_key    = (os.getenv("BINANCE_API_KEY")    or "").strip()
        api_secret = (os.getenv("BINANCE_API_SECRET") or "").strip()
        if not api_key or not api_secret:
            _live_error = "MODE=live but BINANCE_API_KEY / BINANCE_API_SECRET are empty"
            _using_paper_fallback = True
            print(f"[Connection] {_live_error} — falling back to paper client")
        else:
            try:
                from binance.client import Client as BinanceClient
                c = BinanceClient(api_key, api_secret,
                                  requests_params={"timeout": 10})
                c.ping()
                c.update_price = lambda symbol, price: None
                print("[Connection] Live Binance connection established ✓")
                return c
            except Exception as exc:
                _live_error = f"Binance API connection failed: {exc}"
                _using_paper_fallback = True
                print(f"[Connection] {_live_error} — using paper client for this session")

    elif _CONFIGURED_MODE == "testnet":
        api_key    = (os.getenv("TESTNET_API_KEY")    or "").strip()
        api_secret = (os.getenv("TESTNET_API_SECRET") or "").strip()
        if not api_key or not api_secret:
            print("[Connection] MODE=testnet but no testnet keys — falling back to paper")
            _using_paper_fallback = True
        else:
            try:
                from binance.client import Client as BinanceClient
                c = BinanceClient(api_key, api_secret, testnet=True,
                                  requests_params={"timeout": 10})
                c.update_price = lambda symbol, price: None
                return c
            except Exception as exc:
                _live_error = f"Testnet connection failed: {exc}"
                _using_paper_fallback = True
                print(f"[Connection] {_live_error} — falling back to paper")

    # Paper mode (configured or fallback)
    from paper_client import PaperClient
    return PaperClient(
        starting_usdt=float(os.getenv("STARTING_PAPER_USDT", "10000.0")),
        fee_rate=0.001,
    )


client = _build_client()


def get_mode() -> str:
    """Return the configured mode from .env. Never changes due to connection failures."""
    return _CONFIGURED_MODE


def get_live_error() -> str:
    return _live_error


def is_using_paper_fallback() -> bool:
    """True when MODE=live but Binance connection failed — using paper client temporarily."""
    return _using_paper_fallback
