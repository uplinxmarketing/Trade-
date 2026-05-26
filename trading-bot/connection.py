"""
Mode-aware client factory.
All other files import `client` from here — they never check MODE themselves.
"""

import os
import pathlib
from dotenv import load_dotenv

# Use absolute path so the correct .env is found regardless of systemd WorkingDirectory.
_ENV_PATH = pathlib.Path(__file__).parent / ".env"

# Load .env as base layer (override=False so systemd EnvironmentFile wins if set).
load_dotenv(_ENV_PATH, override=False)

# Override with values stored in the SQLite database — these are written every time
# the user sets a mode or API keys via the frontend, so they survive git operations,
# reboots, and .env loss. DB is the authoritative persistent store; .env is the fallback.
try:
    import database as _db_conn
    for _k, _env_k in (
        ("env_MODE",               "MODE"),
        ("env_BINANCE_API_KEY",    "BINANCE_API_KEY"),
        ("env_BINANCE_API_SECRET", "BINANCE_API_SECRET"),
    ):
        _v = _db_conn.get_setting(_k)
        if _v:
            os.environ[_env_k] = _v
except Exception:
    pass  # DB not ready yet — fall back to .env / systemd environment

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
                # The BinanceClient constructor calls self.ping() which is
                # geo-blocked on datacenter IPs (APIError code=0). Monkeypatch
                # ping to a no-op for construction, then restore it so real
                # trading calls are unaffected.
                _real_ping = BinanceClient.ping
                BinanceClient.ping = lambda self: {}
                try:
                    c = BinanceClient(api_key, api_secret,
                                      requests_params={"timeout": 10})
                finally:
                    BinanceClient.ping = _real_ping
                c.update_price = lambda symbol, price: None
                print("[Connection] Live Binance client initialised ✓")
                return c
            except Exception as exc:
                _live_error = f"Binance client init failed: {exc}"
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


def _live_reconnect_loop():
    """Retry live Binance connection every 60 s when on paper fallback."""
    global client, _live_error, _using_paper_fallback
    import time as _t
    while True:
        _t.sleep(60)
        if _CONFIGURED_MODE != "live" or not _using_paper_fallback:
            continue
        api_key    = (os.getenv("BINANCE_API_KEY")    or "").strip()
        api_secret = (os.getenv("BINANCE_API_SECRET") or "").strip()
        if not api_key or not api_secret:
            continue
        try:
            from binance.client import Client as BinanceClient
            _real_ping = BinanceClient.ping
            BinanceClient.ping = lambda self: {}
            try:
                c = BinanceClient(api_key, api_secret, requests_params={"timeout": 10})
            finally:
                BinanceClient.ping = _real_ping
            c.update_price = lambda symbol, price: None
            client = c
            _live_error = ""
            _using_paper_fallback = False
            print("[Connection] Auto-reconnect: live Binance client restored ✓")
        except Exception as exc:
            print(f"[Connection] Auto-reconnect failed: {exc}")


if _CONFIGURED_MODE == "live" and _using_paper_fallback:
    import threading as _th
    _th.Thread(target=_live_reconnect_loop, name="binance-reconnect", daemon=True).start()


def get_mode() -> str:
    """Return the configured mode from .env. Never changes due to connection failures."""
    return _CONFIGURED_MODE


def get_live_error() -> str:
    return _live_error


def is_using_paper_fallback() -> bool:
    """True when MODE=live but Binance connection failed — using paper client temporarily."""
    return _using_paper_fallback
