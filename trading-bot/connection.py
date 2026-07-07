"""
Mode-aware client factory.
All other files import `client` from here — they never check MODE themselves.
"""

import os
import pathlib
from dotenv import load_dotenv

# Use absolute path so the correct .env is found regardless of systemd WorkingDirectory.
_ENV_PATH = pathlib.Path(__file__).parent / ".env"

# Precedence: real process environment (systemd EnvironmentFile / Railway vars)
# > SQLite settings (written by the frontend) > .env file.
# Snapshot which keys the REAL environment defines BEFORE load_dotenv runs, so
# a DB override can never defeat an operator-set variable (e.g. MODE=paper set
# in the deployment environment to force the bot out of live mode).
_DB_ENV_KEYS = (
    ("env_MODE",               "MODE"),
    ("env_BINANCE_API_KEY",    "BINANCE_API_KEY"),
    ("env_BINANCE_API_SECRET", "BINANCE_API_SECRET"),
)
_real_env_has = {_env_k: _env_k in os.environ for _, _env_k in _DB_ENV_KEYS}

# Load .env as base layer (override=False so the real environment wins if set).
load_dotenv(_ENV_PATH, override=False)

# Fill from the SQLite database — written every time the user sets a mode or
# API keys via the frontend, so they survive git operations, reboots, and .env
# loss. DB values may override .env values (DB is the frontend-written store;
# .env is the fallback) but NEVER a variable set in the real process environment.
try:
    import database as _db_conn
    for _k, _env_k in _DB_ENV_KEYS:
        if _real_env_has.get(_env_k):
            continue  # real environment wins — never override it from the DB
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
        starting_usdt=_paper_starting_usdt(),
        fee_rate=_paper_fee_rate(),
    )


_PAPER_DEFAULT_STARTING_USDT = 10000.0


def _paper_starting_usdt() -> float:
    """Starting balance for a FRESH paper wallet (Phase 1 §1.6, mirror-live sizing).

    Precedence:
      1. STARTING_PAPER_USDT env var, when explicitly set to something OTHER
         than the legacy 10000 default (operator override always wins).
      2. strategy.json bot_allocation_usdt when > 0 — paper sizing mirrors the
         live allocation cap.
      3. 10000 legacy default.

    Only affects NEW paper states: PaperClient uses starting_usdt exclusively
    when database.load_paper_state() has no saved balances, so an existing
    wallet is never reset on upgrade."""
    env_raw = os.getenv("STARTING_PAPER_USDT")
    env_val = None
    if env_raw is not None:
        try:
            env_val = float(env_raw)
        except (TypeError, ValueError):
            env_val = None
    if env_val is not None and abs(env_val - _PAPER_DEFAULT_STARTING_USDT) > 1e-9:
        return env_val  # explicitly configured to a non-default value — wins
    try:
        import json as _json
        import config as _config
        with open(_config.STRATEGY_FILE, "r") as _f:
            _strategy = _json.load(_f)
        alloc = float(_strategy.get("bot_allocation_usdt", 0) or 0)
        if alloc > 0:
            return alloc
    except Exception:
        pass
    return env_val if env_val is not None else _PAPER_DEFAULT_STARTING_USDT


def _paper_fee_rate() -> float:
    """Fallback taker fee for PaperClient — from fees.FeeModel (spec §1.1).
    PaperClient re-reads the FeeModel per fill; this is only its last-resort
    constructor fallback, so it too must come from the model, not a literal."""
    try:
        import fees as _fees
        return _fees.get_fee_model().taker()
    except Exception:
        return 0.001  # legacy constant — only when the fees module is broken


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
