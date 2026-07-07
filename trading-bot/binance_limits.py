"""
binance_limits.py — shared, thread-safe Binance REST limit-compliance state (§6.4).

Every module that talks to Binance REST (data_collector background fetches,
trade_engine order/account calls, control_api probes) funnels its budget
decisions through this module so the whole process presents ONE coordinated
client to Binance's per-IP weight limit and honours 429/418 back-pressure.

Usage pattern (public REST via urllib):

    import binance_limits
    if not binance_limits.spend(binance_limits.WEIGHT_KLINES):   # gate + record
        return  # defer politely — budget closed
    resp = urllib.request.urlopen(req)
    binance_limits.record_response_headers(resp.headers)
    # on HTTP 429: binance_limits.on_429(retry_after_seconds_or_None)
    # on HTTP 418: binance_limits.on_418(retry_after_seconds_or_None)

Design notes:
- The server-reported ``X-MBX-USED-WEIGHT-1M`` header is the source of truth
  for total IP weight. The module ADDITIONALLY self-tracks *background*
  (non-critical) spend in a rolling 60 s window so data work stays inside its
  own sub-budget even when headers are missing or stale.
- ``critical=True`` (order placement / account reads) is only ever blocked by
  a 418 IP ban — never by weight budgeting or a 429 half-rate window's gate?
  No: a 429 pause blocks everything until it expires (Binance told us to stop),
  but the post-pause half-rate window only throttles non-critical calls.
- 418 = auto-ban for continuing after 429. ALL REST halts (critical included)
  until the ban expires; callers must never auto-retry through a ban.
"""

import random
import threading
import time

# ── Limit constants ───────────────────────────────────────────────────────────
# Weights per https://developers.binance.com/docs/binance-spot-api-docs/rest-api/limits
# and the per-endpoint tables in
# https://developers.binance.com/docs/binance-spot-api-docs/rest-api/market-data-endpoints

IP_WEIGHT_LIMIT_1M = 6000        # REQUEST_WEIGHT: 6000 per minute per IP (all endpoints combined)
WEIGHT_KLINES = 2                # GET /api/v3/klines — weight 2 (limit ≤ 1000)
WEIGHT_TICKER_PRICE = 2          # GET /api/v3/ticker/price?symbol=X — weight 2 (single symbol)
WEIGHT_TICKER_PRICE_BATCH = 4    # GET /api/v3/ticker/price with symbols=[...] or omitted — weight 4
WEIGHT_TICKER_24HR_ALL = 80      # GET /api/v3/ticker/24hr with NO symbol — weight 80. NEVER call this form.
WEIGHT_EXCHANGE_INFO = 20        # GET /api/v3/exchangeInfo — weight 20; cache the response ~24 h.

# Order-rate limits (ORDERS: ~100 per 10 s and 200 000 per day) are PER-ACCOUNT
# and tracked separately from request weight. X-MBX-ORDER-COUNT-10S is recorded
# here for visibility only — weight gating must never block order placement.

BACKGROUND_WEIGHT_CEILING = 4500   # non-critical work stops once server-reported 1m weight ≥ this
                                   # (leaves ≥1500 headroom for orders/account/watchdog calls)
BACKGROUND_SPEND_LIMIT_60S = 3000  # self-tracked non-critical spend budget per rolling 60 s
WEIGHT_STALE_SEC = 90              # server weight older than this is treated as unknown (→ 0)
HALF_RATE_SEC = 60.0               # post-429 recovery window (every other non-critical call denied)
DEFAULT_BAN_SEC = 300.0            # 418 with no Retry-After header → assume 5 min ban


# ── Internal state (guarded by _lock) ────────────────────────────────────────
_lock = threading.Lock()

_used_weight: int = 0
_used_weight_ts: float = 0.0

_order_count_10s: int = 0
_order_count_ts: float = 0.0

# Rolling 60 s window of (timestamp, weight) for NON-critical spends.
_bg_spend: list = []

_rest_paused_until: float = 0.0    # 429 → everything paused until here
_half_rate_until: float = 0.0      # post-429 → non-critical throttled until here
_banned_until: float = 0.0         # 418 → EVERYTHING halted until here
_half_rate_toggle: bool = False    # alternating gate used inside the half-rate window

_last_429_ts: float = 0.0
_last_418_ts: float = 0.0


def _header_get(headers, name: str):
    """Case-insensitive header lookup that works for plain dicts AND
    email.message.Message (what urllib puts on resp.headers / HTTPError.headers,
    whose .get() is already case-insensitive)."""
    if headers is None:
        return None
    try:
        v = headers.get(name)
        if v is not None:
            return v
    except Exception:
        pass
    low = name.lower()
    try:
        for k in headers.keys():
            if str(k).lower() == low:
                try:
                    return headers[k]
                except Exception:
                    return None
    except Exception:
        pass
    return None


def record_response_headers(headers) -> None:
    """Parse rate-limit telemetry off any Binance REST response (success OR
    error — HTTPError.headers works too). Cheap; call after every request."""
    global _used_weight, _used_weight_ts, _order_count_10s, _order_count_ts
    if headers is None:
        return
    now = time.time()
    w = _header_get(headers, "X-MBX-USED-WEIGHT-1M")
    oc = _header_get(headers, "X-MBX-ORDER-COUNT-10S")
    if w is None and oc is None:
        return
    with _lock:
        if w is not None:
            try:
                _used_weight = int(str(w).strip())
                _used_weight_ts = now
            except (TypeError, ValueError):
                pass
        if oc is not None:
            try:
                _order_count_10s = int(str(oc).strip())
                _order_count_ts = now
            except (TypeError, ValueError):
                pass


def _used_weight_locked(now: float) -> int:
    if _used_weight_ts and now - _used_weight_ts <= WEIGHT_STALE_SEC:
        return _used_weight
    return 0  # unknown / stale — self-tracked bg window still protects us


def used_weight() -> int:
    """Last server-reported 1-minute IP weight; 0 when unknown or stale (>90 s)."""
    with _lock:
        return _used_weight_locked(time.time())


def _prune_bg_locked(now: float) -> int:
    global _bg_spend
    cutoff = now - 60.0
    if _bg_spend and _bg_spend[0][0] < cutoff:
        _bg_spend = [e for e in _bg_spend if e[0] >= cutoff]
    return sum(w for _, w in _bg_spend)


def _gate_locked(weight: int, critical: bool, record: bool, now: float) -> bool:
    """Single decision point shared by can_spend() and spend().
    NOTE: inside the post-429 half-rate window this flips an alternating
    toggle, i.e. the gate is deliberately stateful for non-critical calls."""
    global _half_rate_toggle
    if now < _banned_until:
        return False                     # 418 IP ban — nothing goes out, critical included
    if now < _rest_paused_until:
        return False                     # 429 pause — everything waits
    if critical:
        return True                      # orders/account: only a ban stops these
    if now < _half_rate_until:
        _half_rate_toggle = not _half_rate_toggle
        if not _half_rate_toggle:
            return False                 # every other non-critical call denied
    if _used_weight_locked(now) >= BACKGROUND_WEIGHT_CEILING:
        return False
    bg = _prune_bg_locked(now)
    if bg + max(0, int(weight)) > BACKGROUND_SPEND_LIMIT_60S:
        return False
    if record:
        _bg_spend.append((now, max(0, int(weight))))
    return True


def can_spend(weight: int, critical: bool = False) -> bool:
    """True when a REST call of `weight` may go out right now.
    Non-critical calls are denied while paused/half-rate/over-budget;
    critical calls are denied only during a 418 ban or an active 429 pause."""
    with _lock:
        return _gate_locked(weight, critical, record=False, now=time.time())


def spend(weight: int, critical: bool = False) -> bool:
    """can_spend() + record the intent: on True, non-critical weight is charged
    to the rolling 60 s background window. Call this (not can_spend) right
    before actually issuing the request."""
    with _lock:
        return _gate_locked(weight, critical, record=not critical, now=time.time())


def on_429(retry_after_sec=None) -> None:
    """HTTP 429 Too Many Requests: pause ALL REST until now + Retry-After +
    jitter(0–2 s); after resuming, run half-rate for 60 s (non-critical only)."""
    global _rest_paused_until, _half_rate_until, _last_429_ts
    now = time.time()
    try:
        ra = max(0.0, float(retry_after_sec)) if retry_after_sec is not None else 0.0
    except (TypeError, ValueError):
        ra = 0.0
    with _lock:
        _last_429_ts = now
        paused_until = now + ra + random.uniform(0.0, 2.0)
        _rest_paused_until = max(_rest_paused_until, paused_until)
        _half_rate_until = max(_half_rate_until, _rest_paused_until + HALF_RATE_SEC)
        pu, hr = _rest_paused_until, _half_rate_until
    print(f"[BinanceLimits] 429 received — ALL REST paused for "
          f"{pu - now:.1f}s, then half-rate until +{hr - now:.0f}s")


def on_418(retry_after_sec=None) -> None:
    """HTTP 418: the IP is auto-banned for ignoring 429s. HALT all REST —
    critical included — until now + Retry-After (default 300 s). Callers must
    NEVER auto-retry through a ban."""
    global _banned_until, _last_418_ts
    now = time.time()
    try:
        ra = max(0.0, float(retry_after_sec)) if retry_after_sec is not None else DEFAULT_BAN_SEC
    except (TypeError, ValueError):
        ra = DEFAULT_BAN_SEC
    with _lock:
        _last_418_ts = now
        _banned_until = max(_banned_until, now + ra)
        bu = _banned_until
    print("=" * 78)
    print(f"[BinanceLimits] *** HTTP 418 — IP BANNED BY BINANCE for {bu - now:.0f}s ***")
    print("[BinanceLimits] *** ALL REST (orders included) is HALTED. Do not retry. ***")
    print("=" * 78)


def get_limits_health() -> dict:
    """Snapshot for the health panel (§6.5). All timestamps are epoch seconds
    (0.0 = never/inactive)."""
    now = time.time()
    with _lock:
        return {
            "used_weight":         _used_weight_locked(now),
            "used_weight_ts":      _used_weight_ts,
            "background_spend_60s": _prune_bg_locked(now),
            "rest_paused_until":   _rest_paused_until,
            "half_rate_until":     _half_rate_until,
            "banned_until":        _banned_until,
            "order_count_10s":     _order_count_10s,
            "last_429_ts":         _last_429_ts,
            "last_418_ts":         _last_418_ts,
        }
