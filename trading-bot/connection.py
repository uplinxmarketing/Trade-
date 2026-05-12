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
MODE = os.getenv("MODE", "paper").lower()

_live_error: str = ""   # set if live connection failed; readable via get_live_error()


def _build_client():
    global MODE, _live_error

    if MODE == "live":
        api_key    = (os.getenv("BINANCE_API_KEY")    or "").strip()
        api_secret = (os.getenv("BINANCE_API_SECRET") or "").strip()
        if not api_key or not api_secret:
            _live_error = "MODE=live but BINANCE_API_KEY / BINANCE_API_SECRET are empty — falling back to paper"
            print(f"[Connection] {_live_error}")
            MODE = "paper"
        else:
            try:
                from binance.client import Client as BinanceClient
                c = BinanceClient(api_key, api_secret)
                c.ping()
                c.update_price = lambda symbol, price: None
                print("[Connection] Live Binance connection established ✓")
                return c
            except Exception as exc:
                _live_error = f"Binance API connection failed: {exc}"
                print(f"[Connection] {_live_error} — falling back to paper mode")
                MODE = "paper"
                # Revert .env so next restart doesn't retry with broken credentials
                try:
                    _revert_to_paper()
                except Exception:
                    pass

    elif MODE == "testnet":
        api_key    = (os.getenv("TESTNET_API_KEY")    or "").strip()
        api_secret = (os.getenv("TESTNET_API_SECRET") or "").strip()
        if not api_key or not api_secret:
            print("[Connection] MODE=testnet but no testnet keys — falling back to paper")
            MODE = "paper"
        else:
            try:
                from binance.client import Client as BinanceClient
                c = BinanceClient(api_key, api_secret, testnet=True)
                c.update_price = lambda symbol, price: None
                return c
            except Exception as exc:
                print(f"[Connection] Testnet connection failed: {exc} — falling back to paper")
                MODE = "paper"

    # Paper mode (default or fallback)
    from paper_client import PaperClient
    return PaperClient(
        starting_usdt=float(os.getenv("STARTING_PAPER_USDT", "10000.0")),
        fee_rate=0.00075,
    )


def _revert_to_paper():
    env_path = _ENV_PATH
    if not os.path.exists(env_path):
        return
    with open(env_path) as f:
        lines = f.readlines()
    for i, line in enumerate(lines):
        if line.startswith("MODE="):
            lines[i] = "MODE=paper\n"
            break
    with open(env_path, "w") as f:
        f.writelines(lines)
    os.environ["MODE"] = "paper"


client = _build_client()


def get_mode() -> str:
    return MODE


def get_live_error() -> str:
    return _live_error
