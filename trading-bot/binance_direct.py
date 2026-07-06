"""
Direct Binance REST transport — urllib + HMAC.

python-binance goes through the `requests` library to api.binance.com, which is
geo-blocked on datacenter IPs (HTTP 451 / APIError code=0). Plain urllib with a
custom User-Agent is not blocked (proven by control_api._fetch_account_direct,
which powers /api/wallet). This module is the transport ALL live-mode
authenticated calls must use: account reads and order placement.

Responses are the raw Binance REST payloads — identical shape to what
python-binance returns (fills / executedQty / cummulativeQuoteQty etc.), so
existing parsing code keeps working.
"""

import hashlib
import hmac
import json
import os
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

BASE = "https://api.binance.com"
_RECV_WINDOW_MS = 10_000
_TIME_RESYNC_S = 1800  # re-sync clock offset every 30 min


class BinanceDirectError(Exception):
    """Raised for HTTP errors and Binance error payloads ({"code": -NNNN, "msg": ...})."""

    def __init__(self, code, msg):
        self.code = code
        self.msg = msg
        super().__init__(f"Binance error {code}: {msg}")


_time_offset_ms = 0
_time_synced_at = 0.0
_sync_lock = threading.Lock()


def _sync_time() -> None:
    """Track offset between local clock and Binance server time (avoids -1021)."""
    global _time_offset_ms, _time_synced_at
    with _sync_lock:
        if time.time() - _time_synced_at < _TIME_RESYNC_S:
            return
        try:
            req = urllib.request.Request(
                f"{BASE}/api/v3/time", headers={"User-Agent": "WolfBot/1.0"})
            with urllib.request.urlopen(req, timeout=5) as r:
                server_ms = json.loads(r.read())["serverTime"]
            _time_offset_ms = server_ms - int(time.time() * 1000)
            _time_synced_at = time.time()
        except Exception:
            # Tolerate failure: recvWindow absorbs small drift. Marking synced
            # prevents a tight retry loop; next window retries naturally.
            _time_synced_at = time.time()


def signed_request(method: str, path: str, params: dict | None = None,
                   timeout: float = 10) -> dict:
    api_key = (os.getenv("BINANCE_API_KEY") or "").strip()
    api_secret = (os.getenv("BINANCE_API_SECRET") or "").strip()
    if not api_key or not api_secret:
        raise BinanceDirectError(-1, "BINANCE_API_KEY / BINANCE_API_SECRET not configured")
    _sync_time()
    p = dict(params or {})
    p["timestamp"] = int(time.time() * 1000) + _time_offset_ms
    p["recvWindow"] = _RECV_WINDOW_MS
    qs = urllib.parse.urlencode(p)
    sig = hmac.new(api_secret.encode(), qs.encode(), hashlib.sha256).hexdigest()
    qs = f"{qs}&signature={sig}"

    url = f"{BASE}{path}"
    data = None
    if method in ("POST", "PUT", "DELETE"):
        data = qs.encode()
    else:
        url = f"{url}?{qs}"

    req = urllib.request.Request(
        url, data=data, method=method,
        headers={
            "X-MBX-APIKEY": api_key,
            "User-Agent": "WolfBot/1.0",
            "Content-Type": "application/x-www-form-urlencoded",
        })
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            body = json.loads(r.read())
    except urllib.error.HTTPError as e:
        try:
            err = json.loads(e.read())
        except Exception:
            raise BinanceDirectError(e.code, f"HTTP {e.code}: {e.reason}") from e
        raise BinanceDirectError(err.get("code", e.code), err.get("msg", e.reason)) from e

    # Binance also returns error payloads with HTTP 200
    if isinstance(body, dict) and body.get("code", 0) < 0:
        raise BinanceDirectError(body["code"], body.get("msg", ""))
    return body


def _fmt(n) -> str:
    """Decimal string without scientific notation or trailing zeros."""
    s = f"{float(n):.8f}".rstrip("0").rstrip(".")
    return s if s else "0"


def get_account() -> dict:
    """GET /api/v3/account — same payload shape as python-binance get_account()."""
    return signed_request("GET", "/api/v3/account")


def order_market_buy(symbol: str, quote_order_qty: float) -> dict:
    """Market buy spending `quote_order_qty` USDT. Returns full order payload with fills."""
    return signed_request("POST", "/api/v3/order", {
        "symbol": symbol,
        "side": "BUY",
        "type": "MARKET",
        "quoteOrderQty": _fmt(quote_order_qty),
        "newOrderRespType": "FULL",
    })


def order_market_sell(symbol: str, quantity: float) -> dict:
    """Market sell `quantity` of the base asset. Returns full order payload with fills."""
    return signed_request("POST", "/api/v3/order", {
        "symbol": symbol,
        "side": "SELL",
        "type": "MARKET",
        "quantity": _fmt(quantity),
        "newOrderRespType": "FULL",
    })


def test_connectivity() -> tuple[bool, str]:
    """Cheap signed smoke test. Returns (ok, error_message)."""
    try:
        get_account()
        return True, ""
    except BinanceDirectError as e:
        return False, f"{e.code}: {e.msg}"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"
