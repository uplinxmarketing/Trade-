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

# Optional rate-limit telemetry (§6.4). Guarded so this module stays usable
# standalone (tests, scripts) without binance_limits on the path.
try:
    import binance_limits as _limits
except ImportError:  # pragma: no cover
    _limits = None

BASE = "https://api.binance.com"
# Public (unsigned) market-data host. api.binance.com is geo-blocked (HTTP 451)
# from this datacenter for public GETs the same way it is for signed calls, so
# public helpers use data-api.binance.vision — the geo-block-safe host every
# other public REST reader in the codebase already uses (data_collector,
# backfill, futures_engine, exchange_info, control_api).
PUBLIC_BASE = "https://data-api.binance.vision"
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
            if _limits is not None:
                _limits.record_response_headers(r.headers)
            body = json.loads(r.read())
    except urllib.error.HTTPError as e:
        if _limits is not None:
            # Error responses carry rate-limit headers too — record them, and
            # feed 429/418 into the shared back-pressure state before raising.
            _limits.record_response_headers(e.headers)
            if e.code == 429:
                _limits.on_429(_retry_after(e.headers))
            elif e.code == 418:
                _limits.on_418(_retry_after(e.headers))
        try:
            err = json.loads(e.read())
        except Exception:
            raise BinanceDirectError(e.code, f"HTTP {e.code}: {e.reason}") from e
        raise BinanceDirectError(err.get("code", e.code), err.get("msg", e.reason)) from e

    # Binance also returns error payloads with HTTP 200
    if isinstance(body, dict) and body.get("code", 0) < 0:
        raise BinanceDirectError(body["code"], body.get("msg", ""))
    return body


def _retry_after(headers):
    """Parse Retry-After seconds off an error response; None when absent."""
    if headers is None:
        return None
    try:
        v = headers.get("Retry-After")
        return float(v) if v is not None else None
    except (TypeError, ValueError):
        return None


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


def order_limit_maker(symbol: str, side: str, quantity: float, price: float) -> dict:
    """Post-only limit order (POST /api/v3/order type=LIMIT_MAKER).

    LIMIT_MAKER is REJECTED by the engine (code -2010, 'Order would immediately
    match and take.') instead of matching as a taker — that rejection is the
    point: it guarantees maker fees or nothing. Callers must expect and handle
    the -2010 BinanceDirectError. Returns the full order payload on acceptance.
    """
    return signed_request("POST", "/api/v3/order", {
        "symbol": symbol,
        "side": side,
        "type": "LIMIT_MAKER",
        "quantity": _fmt(quantity),
        "price": _fmt(price),
        "newOrderRespType": "FULL",
    })


def get_book_ticker(symbol: str, timeout: float = 5) -> dict:
    """GET /api/v3/ticker/bookTicker?symbol=X — PUBLIC best bid/ask (weight 2).

    Unsigned market-data read over the geo-block-safe data-api.binance.vision
    host (no API key, no HMAC — a plain urllib GET like the other public
    helpers). Returns the raw Binance payload:
        {"symbol", "bidPrice", "bidQty", "askPrice", "askQty"}  (values are strings)

    Rate limiting is guarded so this module stays usable standalone: the call is
    gated through binance_limits.can_spend(2, critical=True) when the module is
    importable (never blocks when accounting is absent), and actual usage is fed
    back via record_response_headers so the weight-2 spend is accounted for.
    Raises BinanceDirectError on HTTP/Binance errors (including a synthetic -1
    when a rate-limit gate defers the call)."""
    if _limits is not None:
        try:
            if not _limits.can_spend(2, critical=True):
                raise BinanceDirectError(-1, "book ticker deferred — REST rate-limited")
        except BinanceDirectError:
            raise
        except Exception:
            pass  # accounting failure must never block a market-data read
    sym = urllib.parse.quote(str(symbol).strip().upper())
    url = f"{PUBLIC_BASE}/api/v3/ticker/bookTicker?symbol={sym}"
    req = urllib.request.Request(url, headers={"User-Agent": "WolfBot/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            if _limits is not None:
                try:
                    _limits.record_response_headers(r.headers)
                except Exception:
                    pass
            body = json.loads(r.read())
    except urllib.error.HTTPError as e:
        if _limits is not None:
            try:
                _limits.record_response_headers(e.headers)
                if e.code == 429:
                    _limits.on_429(_retry_after(e.headers))
                elif e.code == 418:
                    _limits.on_418(_retry_after(e.headers))
            except Exception:
                pass
        try:
            err = json.loads(e.read())
        except Exception:
            raise BinanceDirectError(e.code, f"HTTP {e.code}: {e.reason}") from e
        raise BinanceDirectError(err.get("code", e.code), err.get("msg", e.reason)) from e
    if isinstance(body, dict) and body.get("code", 0) < 0:
        raise BinanceDirectError(body["code"], body.get("msg", ""))
    return body


def get_order(symbol: str, order_id: int) -> dict:
    """GET /api/v3/order — query a single order's status (weight 4)."""
    return signed_request("GET", "/api/v3/order", {
        "symbol": symbol,
        "orderId": int(order_id),
    })


def cancel_order(symbol: str, order_id: int) -> dict:
    """DELETE /api/v3/order — cancel a resting order.

    Raises BinanceDirectError -2011 ('Unknown order sent.') when the order is
    already filled/cancelled — callers racing a fill should tolerate that code.
    """
    return signed_request("DELETE", "/api/v3/order", {
        "symbol": symbol,
        "orderId": int(order_id),
    })


def get_open_orders(symbol: str | None = None) -> list:
    """GET /api/v3/openOrders.

    Weight: 6 WITH a symbol, 80 WITHOUT one — always pass `symbol` unless a
    full-account sweep is genuinely required (e.g. one-off reconciliation).
    """
    params = {"symbol": symbol} if symbol else {}
    return signed_request("GET", "/api/v3/openOrders", params)


def order_oco_sell(symbol: str, quantity: float, tp_price: float,
                   stop_trigger: float, stop_limit_price: float) -> dict:
    """One-Cancels-the-Other SELL: take-profit LIMIT_MAKER above, stop-loss-limit below.

    Uses the modern POST /api/v3/orderList/oco endpoint with aboveType/belowType
    leg params. For a SELL OCO the 'above' leg must price above market and the
    'below' leg below market:
      - above: LIMIT_MAKER at tp_price (maker-only take profit)
      - below: STOP_LOSS_LIMIT, stopPrice=stop_trigger, price=stop_limit_price
        (limit set below the trigger so it fills through fast moves), GTC.

    Legacy fallback (deprecated /api/v3/order/oco) param mapping, should the
    modern endpoint ever be unavailable:
      price=tp_price, stopPrice=stop_trigger, stopLimitPrice=stop_limit_price,
      stopLimitTimeInForce=GTC (no aboveType/belowType; sides are implicit).

    Returns the order-list payload: orderListId + orders[] (one per leg).
    """
    return signed_request("POST", "/api/v3/orderList/oco", {
        "symbol": symbol,
        "side": "SELL",
        "quantity": _fmt(quantity),
        "aboveType": "LIMIT_MAKER",
        "abovePrice": _fmt(tp_price),
        "belowType": "STOP_LOSS_LIMIT",
        "belowStopPrice": _fmt(stop_trigger),
        "belowPrice": _fmt(stop_limit_price),
        "belowTimeInForce": "GTC",
        "newOrderRespType": "FULL",
    })


def cancel_order_list(symbol: str, order_list_id: int) -> dict:
    """DELETE /api/v3/orderList — cancel an entire OCO (both legs).

    Raises BinanceDirectError -2011 when the list is already executed/cancelled.
    """
    return signed_request("DELETE", "/api/v3/orderList", {
        "symbol": symbol,
        "orderListId": int(order_list_id),
    })


def get_order_list(order_list_id: int) -> dict:
    """GET /api/v3/orderList — query an OCO's status (listOrderStatus:
    EXECUTING / ALL_DONE / REJECT; listStatusType tells why)."""
    return signed_request("GET", "/api/v3/orderList", {
        "orderListId": int(order_list_id),
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
