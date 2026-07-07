"""
Trade execution engine — unified real-time architecture.

realtime_monitor:  Called on every WebSocket trade tick (~100 ms).
                   Handles both stop-loss/take-profit exits AND buy entry.
                   Buys read from an in-memory signal cache (no I/O per tick).

signal_scanner:    Async coroutine, runs every SCAN_INTERVAL_SEC (60 s).
                   ws-first mode (default): poll-based buy backup + DB refresh
                   for thin-WS-buffer symbols + health stats — NO REST.
                   legacy mode (strategy data.legacy_rest_scan=true): the old
                   full REST refresh pass (emergency fallback).
                   Does NOT execute trades — buy execution is in realtime_monitor.

update_coin_signals: Called by data_collector on every 1-minute kline close
                   (WebSocket-driven, runs on a worker thread). C §6.2: rebuilds
                   the FULL signal-cache entry from the ws_candles buffers —
                   real-ATR signals, bb_ok, 5m_ok, low_24h, klines_1m,
                   stoch_rsi_val — merge-preserving keys set by other paths.
"""

import asyncio
import json
import json as _json
import logging
import os
import time
import math
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

import config
import database
import indicators
import learning
import connection
from connection import get_mode
import binance_direct

try:
    import thread_health as _thread_health
except Exception:
    _thread_health = None

log = logging.getLogger(__name__)


# ── C §6.4 — Binance rate-limit accounting (binance_limits module) ────────────
# Wired DEFENSIVELY: the module may land after this file (parallel delivery),
# so import lazily and degrade to no-ops when unavailable. can_spend defaults
# to True (never block trading because accounting is missing).
_blimits = None
_blimits_import_failed = False


def _get_blimits():
    global _blimits, _blimits_import_failed
    if _blimits is None and not _blimits_import_failed:
        try:
            import binance_limits as _bl
            _blimits = _bl
        except Exception:
            _blimits_import_failed = True
    return _blimits


def _limits_can_spend(weight: int, critical: bool) -> bool:
    """Gate a REST call through binance_limits right before issuing it.
    Prefers spend() (gate + charge the background budget, per that module's
    contract) and falls back to can_spend(); allows on any failure."""
    bl = _get_blimits()
    fn = (getattr(bl, "spend", None) or getattr(bl, "can_spend", None)) if bl else None
    if fn is None:
        return True
    try:
        return bool(fn(weight, critical=critical))
    except TypeError:
        try:
            return bool(fn(weight, critical))
        except Exception:
            return True
    except Exception:
        return True


def _limits_record_headers(headers: dict) -> None:
    bl = _get_blimits()
    fn = getattr(bl, "record_response_headers", None) if bl else None
    if fn is None or not headers:
        return
    try:
        fn(headers)
    except Exception:
        pass


def _retry_after_sec(headers: Optional[dict], default: float) -> float:
    try:
        for k, v in (headers or {}).items():
            if str(k).lower() == "retry-after":
                return max(1.0, float(v))
    except Exception:
        pass
    return default


def _limits_on_429(headers: Optional[dict] = None) -> None:
    """Forward a 429 to binance_limits.on_429(retry_after_sec)."""
    bl = _get_blimits()
    fn = getattr(bl, "on_429", None) if bl else None
    if fn is None:
        return
    ra = _retry_after_sec(headers, 0.0)
    try:
        fn(ra if ra > 0 else None)
    except TypeError:
        try:
            fn()
        except Exception:
            pass
    except Exception:
        pass


def _limits_on_418(headers: Optional[dict] = None) -> None:
    """Forward a 418 to binance_limits.on_418(retry_after_sec)."""
    bl = _get_blimits()
    fn = getattr(bl, "on_418", None) if bl else None
    if fn is None:
        return
    ra = _retry_after_sec(headers, 0.0)
    try:
        fn(ra if ra > 0 else None)
    except TypeError:
        try:
            fn()
        except Exception:
            pass
    except Exception:
        pass


# ── Client transport helpers ─────────────────────────────────────────────────
# Always resolve the client through the connection module at call time so the
# auto-reconnect loop's rebinding (connection.client = new_client) takes effect
# here — a `from connection import client` snapshot would hold the stale
# PaperClient forever after a live reconnect.
#
# In LIVE mode all signed calls (account reads, order placement) go through
# binance_direct (urllib + HMAC): python-binance uses the `requests` library,
# which is geo-blocked on datacenter IPs (HTTP 451 / APIError code=0).
# Paper and testnet modes keep using the connection client unchanged.

def _client():
    """Current client — never cache the returned object across reconnects."""
    return connection.client


def _acct() -> dict:
    """Account snapshot. Live mode uses the geo-block-safe direct transport."""
    if get_mode() == "live":
        return binance_direct.get_account()
    return connection.client.get_account()


def _market_buy(symbol: str, quote_order_qty: float) -> dict:
    """Market buy via the mode-appropriate transport.

    Live mode: geo-block-safe signed call. If live mode is running on the
    paper fallback client (Binance connection failed at boot), buys are
    BLOCKED entirely — never silently 'trade' the paper client while the
    records would be stamped mode='live'.
    """
    if get_mode() == "live":
        if connection.is_using_paper_fallback():
            raise RuntimeError(
                "live mode is in paper fallback (Binance client unavailable) — buy blocked"
            )
        return binance_direct.order_market_buy(symbol, quote_order_qty)
    return connection.client.order_market_buy(symbol=symbol, quoteOrderQty=quote_order_qty)


def _market_sell(symbol: str, quantity: float) -> dict:
    """Live-mode market sell via the geo-block-safe direct transport."""
    if get_mode() == "live":
        return binance_direct.order_market_sell(symbol, quantity)
    return connection.client.order_market_sell(symbol=symbol, quantity=quantity)


def _log_order_intent(action: str, symbol: str, qty: float, intended_price: float):
    try:
        database.log_activity(
            f"[ORDER_SEND] {action} {symbol} qty={qty:.6f} "
            f"intended_price={intended_price:.6f} "
            f"intended_notional=${qty * intended_price:.4f}",
            "info"
        )
    except Exception:
        pass


def _log_order_result(action: str, symbol: str, intended_qty: float, intended_price: float, result: dict):
    try:
        fills = result.get("fills", [])
        filled_qty = float(result.get("executedQty") or sum(float(f.get("qty", 0)) for f in fills))
        actual_quote = float(result.get("cummulativeQuoteQty") or 0)
        actual_avg = actual_quote / filled_qty if filled_qty > 0 else 0
        slippage = ((actual_avg - intended_price) / intended_price * 100) if intended_price > 0 else 0
        database.log_activity(
            f"[ORDER_REPLY] {action} {symbol} status={result.get('status')} "
            f"filled={filled_qty:.6f} (intended {intended_qty:.6f}) "
            f"avg_price={actual_avg:.6f} (intended {intended_price:.6f}) "
            f"slippage={slippage:+.3f}% quote=${actual_quote:.4f}",
            "info"
        )
    except Exception:
        pass


# Phase 1: Signal registry — shadow mode only (use_new_signal_engine=False by default).
# Falls back gracefully if the file is absent so a partial deploy can't break the bot.
try:
    from signal_registry import evaluate_buy_decision as _sr_evaluate_buy_decision
    from signal_registry import SIGNAL_REGISTRY, list_all_signal_ids
    SIGNAL_REGISTRY_AVAILABLE = True
except Exception as _sr_import_err:
    SIGNAL_REGISTRY_AVAILABLE = False

_positions: List[dict] = []
_positions_lock = threading.Lock()

_strategy_mtime: float = 0.0
_strategy_cache: dict = {}

_fee_rate = config.FEE_RATE  # 0.1% standard Binance spot
# Minimum multiplier needed to cover fees (breakeven floor).

def _fills_fee_usdt(fills: list, fallback_usdt: float) -> tuple:
    """
    Sum commissions across ALL fills and convert to USDT.

    Binance splits a single market order into multiple fills at different price
    levels. Each fill carries its own commission amount and asset.

    commissionAsset is usually:
      "BNB"  — BNB-fee-discount mode (fee deducted from BNB balance, NOT USDT)
      "USDT" — standard mode for sell orders (deducted from quote proceeds)
      <coin> — rare; base coin deducted from quantity received

    Returns (fee_in_usdt, commission_asset_of_first_fill).
    Falls back to (fallback_usdt, "estimated") when conversion is impossible.
    """
    if not fills:
        return fallback_usdt, "estimated"
    try:
        from data_collector import prices as _px
    except Exception:
        return fallback_usdt, "estimated"

    total = 0.0
    first_asset = fills[0].get("commissionAsset", "estimated")
    for f in fills:
        amount = float(f.get("commission") or 0)
        asset  = f.get("commissionAsset", "")
        if asset in ("USDT", "BUSD", "USDC"):
            total += amount
        elif asset:
            px = _px.get(asset + "USDT", 0)
            if px:
                total += amount * px
            else:
                # Price unavailable — fall back to estimate for entire order
                return fallback_usdt, "estimated"
    return total, first_asset
_FEE_FLOOR = 1.0 / (0.999 ** 2)  # ~1.002003 — exact cost of two 0.1% Binance fees

# Per-coin override buffers: {"SHIBUSDT": 0.30, ...} — populated from strategy.json
_BUFFER_OVERRIDES: Dict[str, float] = {}

def _get_breakeven_mult(entry_price: float, symbol: str = "") -> float:
    """Adaptive sell threshold multiplier based on coin price tier.

    Tier  | Price range    | Buffer  | Total required gain
    ------|----------------|---------|--------------------
    ultra | >= $1 000      | 0.08%   | ~0.28%
    high  | $10 - $1 000   | 0.10%   | ~0.30%
    mid   | $0.10 - $10    | 0.15%   | ~0.35%
    sub   | $0.001 - $0.10 | 0.20%   | ~0.40%
    micro | < $0.001       | 0.30%   | ~0.50%

    Per-coin overrides in strategy.json key "slippage_buffer_overrides" take priority.
    """
    if symbol and symbol in _BUFFER_OVERRIDES:
        buf = float(_BUFFER_OVERRIDES[symbol])
    elif entry_price >= 1000.0:
        buf = 0.08
    elif entry_price >= 10.0:
        buf = 0.10
    elif entry_price >= 0.1:
        buf = 0.15
    elif entry_price >= 0.001:
        buf = 0.20
    else:
        buf = 0.30
    return _FEE_FLOOR * (1.0 + buf / 100.0)

def _min_profit_usdt() -> float:
    """Minimum net profit (USDT) any take-profit sell must clear.

    Read from strategy.json exits.min_profit_usdt (default 0.01), clamped to
    >= 0.001. _load_strategy() is mtime-cached, so edits hot-reload without a
    restart.

    PARITY RULE (A1): this SAME value MUST be used by the sell trigger
    (compute_real_breakeven_price), the profit gate (_profitable_sell_check),
    and any (future) breakeven-move stop. If they ever resolve different
    numbers, the trigger fires sells that the gate then vetoes forever.
    """
    try:
        raw = _load_strategy().get("exits", {}).get("min_profit_usdt", 0.01)
        return max(0.001, float(raw))
    except Exception:
        return 0.01


def compute_real_breakeven_price(pos: dict, min_profit: Optional[float] = None) -> float:
    """Return the REAL price at which selling pos would net at least min_profit USDT.

    min_profit=None (the default — use it everywhere) resolves via
    _min_profit_usdt() so trigger, gate and future breakeven-move stop all read
    the same config value and can never disagree. Only pass an explicit value
    in tests.

    Uses ACTUAL deployed capital (qty * entry_price), not the requested budget.
    This matters when lot-step rounding reduces the actual quantity below what
    the budget would have bought — the position only needs to recover what was
    actually spent, plus fees, plus min_profit.

    Accounts for quantity rounding loss, buy fee already paid, and sell fee.
    Used as the sell trigger in both realtime_monitor and _sell_monitor_loop so
    trigger and profit gate use identical math and never disagree.
    """
    try:
        if min_profit is None:
            min_profit = _min_profit_usdt()
        entry_price = float(pos.get("entry_price") or pos.get("avg_entry_price") or 0)
        buy_fee     = float(pos.get("buy_fee_usdt") or 0)
        qty         = float(pos.get("quantity", 0))
        if qty <= 0 or entry_price <= 0:
            return 0.0
        actual_cost = qty * entry_price + buy_fee
        # qty * price * (1 - 0.001) >= actual_cost + min_profit
        return (actual_cost + min_profit) / (qty * (1.0 - 0.001))
    except Exception:
        return 0.0


# Configurable exit multipliers — refreshed from strategy.json every buy/sell cycle.
# _user_tp_mult:    raw user TP target (1 + tp_pct/100); compared to per-position breakeven.
# _take_profit_mult: kept for UI/diagnostic exports; updated by _refresh_risk_params.
# _stop_loss_mult:   price must fall to entry * this to trigger a stop-loss sell.
_user_tp_mult:    float = 1.001              # 0.1% above entry; updated by _refresh_risk_params
_take_profit_mult: float = _FEE_FLOOR * 1.0010  # kept for legacy compat; updated each cycle
_stop_loss_mult:   float = 1.0 - 0.02           # default: -2% stop loss

_stop_loss_confirmation: Dict[str, int] = {}
_STOP_LOSS_CONFIRMATION_TICKS = 2

# Post-loss cooldown: after a confirmed slippage-loss fill, skip re-buying that coin for 30 min
_loss_cooldown: Dict[str, float] = {}
_LOSS_COOLDOWN_SEC = 1800  # 30 minutes

# Abort-log throttle — SELL ABORTED fires every 250ms per stuck position; cap to 1/min per symbol
_last_abort_log_ts: Dict[str, float] = {}
_ABORT_LOG_THROTTLE_SEC = 60.0

# Minimum hold time after a buy — prevents race-condition sells within seconds of entry
_MIN_HOLD_SEC = 10.0

# Per-symbol throttle for SELL_TRACE diagnostic log (1 per 60s per symbol)
_sell_trace_log_ts: Dict[str, float] = {}

# ── Shadow-eval log dedupe (B Step 5) ─────────────────────────────────────────
# The shadow disagreement/failure logs fire on every buy-loop pass — for a
# persistently disagreeing symbol that's one diag row every few seconds.
# Dedupe per (symbol, reason): identical entries within 15 min are only
# counted; the next emitted line carries "(×N in last 15m)".
_shadow_log_dedupe: Dict[Tuple[str, str], dict] = {}
_SHADOW_DEDUPE_WINDOW_SEC = 900.0  # 15 minutes


def _log_shadow_dedup(symbol: str, reason: str, message: str) -> None:
    """Emit a signal_shadow diag line, deduped per (symbol, reason) per 15 min."""
    key = (symbol, reason)
    now = time.time()
    ent = _shadow_log_dedupe.get(key)
    if ent and (now - ent["last_ts"]) < _SHADOW_DEDUPE_WINDOW_SEC:
        ent["suppressed"] += 1
        return
    suffix = ""
    if ent and ent["suppressed"] > 0:
        suffix = f" (×{ent['suppressed'] + 1} in last 15m)"
    _shadow_log_dedupe[key] = {"last_ts": now, "suppressed": 0}
    log_diag_issue("signal_shadow", "warn", message + suffix)

# WebSocket price freshness — updated by data_collector on every @trade or @miniTicker event.
# Used by the pre-sell check to decide whether a REST re-fetch is needed.
_last_ws_price_ts: Dict[str, float] = {}

# ── Binance REST health — updated by _fetch_rest_prices (zero extra calls) ────
_binance_health: Dict = {
    "last_rest_ok_ts":      0.0,
    "last_rest_latency_ms": 0.0,
    "used_weight_1m":       0,
    "used_weight_pct":      0.0,
    "rest_error_count":     0,
    "last_error_ts":        0.0,
    "last_error_msg":       "",
    # Claude API counters (separate from Binance REST errors)
    "claude_error_count":   0,
    "claude_last_error_ts": 0.0,
    "claude_last_error_msg": "",
    "claude_disabled_until": 0.0,
}
_binance_health_lock = threading.Lock()

# ── Circuit breaker — prevents runaway 400s from burning Binance weight ───────
_consecutive_400_count: int = 0
_circuit_breaker_until: float = 0.0

# ── Per-source exponential backoff — dampens timeout cascades ─────────────────
# Keyed by the `source` tag passed to _binance_request (e.g. "batch_prices",
# "btc_klines"). On consecutive failures the caller is skipped for an
# increasing window: 2s → 4 → 8 → 16 → 32 → 60s (cap). First success resets.
_rest_backoff: dict = {}
_REST_BACKOFF_BASE_SEC = 2.0
_REST_BACKOFF_MAX_SEC  = 60.0
_REST_BACKOFF_LOCK     = threading.Lock()


def _backoff_should_skip(source: str) -> bool:
    with _REST_BACKOFF_LOCK:
        s = _rest_backoff.get(source)
        return bool(s and s["failures"] > 0 and time.time() < s["next_retry_ts"])


def _backoff_record_failure(source: str) -> None:
    with _REST_BACKOFF_LOCK:
        s = _rest_backoff.setdefault(source, {"failures": 0, "next_retry_ts": 0.0})
        s["failures"] += 1
        delay = min(_REST_BACKOFF_BASE_SEC * (2 ** (s["failures"] - 1)), _REST_BACKOFF_MAX_SEC)
        s["next_retry_ts"] = time.time() + delay


def _backoff_record_success(source: str) -> None:
    with _REST_BACKOFF_LOCK:
        if source in _rest_backoff:
            _rest_backoff[source] = {"failures": 0, "next_retry_ts": 0.0}


def _check_circuit_breaker() -> bool:
    return time.time() < _circuit_breaker_until


def _trip_circuit_breaker() -> None:
    global _consecutive_400_count, _circuit_breaker_until
    _consecutive_400_count += 1
    if _consecutive_400_count >= 50:
        _circuit_breaker_until = time.time() + 60.0
        _consecutive_400_count = 0
        try:
            log_diag_issue(
                "binance", "warn",
                "Circuit breaker tripped — pausing REST 60s after 50 consecutive 400s",
            )
        except Exception:
            pass


def _reset_circuit_breaker() -> None:
    global _consecutive_400_count
    _consecutive_400_count = 0


def _binance_request(url: str, timeout: float = 3.0, source: str = "unknown",
                     weight: int = 1, critical: bool = False):
    """Unified Binance REST caller that captures FULL error context.

    Returns (success, data, headers, latency_ms).
    On failure records the exact URL + Binance JSON body in the diag log so we
    can see which call site fails and what Binance is actually complaining about.

    C §6.4: every call is gated by binance_limits.can_spend(weight, critical)
    (no-op allow when the module is absent), response headers are routed into
    binance_limits.record_response_headers, 429 → on_429 + existing per-source
    backoff, 418 → on_418 + immediate circuit-break (IP ban — stop everything).
    """
    import urllib.request as _ur_b
    import urllib.error as _ue_b
    import traceback as _tb_b
    global _circuit_breaker_until

    if _check_circuit_breaker():
        return (False, None, {}, 0.0)
    if _backoff_should_skip(source):
        return (False, None, {}, 0.0)
    if not _limits_can_spend(weight, critical):
        # Budget refused for a non-critical background call — skip gracefully;
        # callers already handle (False, ...) via their cache/WS/DB fallbacks.
        return (False, None, {}, 0.0)

    t0 = time.time()
    try:
        req = _ur_b.Request(url, headers={"User-Agent": "WolfBot/1.0"})
        with _ur_b.urlopen(req, timeout=timeout) as r:
            body = r.read()
            hdrs = dict(r.headers)
        latency_ms = (time.time() - t0) * 1000
        try:
            _record_rest_health(hdrs, latency_ms)
        except Exception:
            pass
        _limits_record_headers(hdrs)
        _reset_circuit_breaker()
        _backoff_record_success(source)
        return (True, json.loads(body), hdrs, latency_ms)
    except _ue_b.HTTPError as he:
        latency_ms = (time.time() - t0) * 1000
        try:
            body_text = he.read().decode("utf-8", errors="replace")[:600]
        except Exception:
            body_text = "<unreadable>"
        try:
            err_hdrs = dict(he.headers) if he.headers else {}
            _record_rest_health(err_hdrs, latency_ms)
        except Exception:
            err_hdrs = {}
        _limits_record_headers(err_hdrs)
        if he.code == 400:
            _trip_circuit_breaker()
        elif he.code == 429:
            # Rate-limited: inform the limits module + existing per-source backoff.
            _limits_on_429(err_hdrs)
            _backoff_record_failure(source)
        elif he.code == 418:
            # IP auto-ban: inform limits module and circuit-break ALL REST for
            # the Retry-After window (min 120s) — continuing would extend the ban.
            _limits_on_418(err_hdrs)
            _circuit_breaker_until = max(
                _circuit_breaker_until,
                time.time() + _retry_after_sec(err_hdrs, 120.0),
            )
            try:
                log_diag_issue(
                    "binance", "error",
                    "HTTP 418 (IP ban) — circuit-breaking all REST calls",
                    detail=f"source={source} retry_after={_retry_after_sec(err_hdrs, 120.0):.0f}s",
                )
            except Exception:
                pass
        try:
            _record_rest_error(
                f"[{source}] HTTP {he.code}: {he.reason}",
                url=url, response_body=body_text,
            )
        except TypeError:
            try:
                _record_rest_error(f"[{source}] HTTP {he.code} | {url[:200]} | {body_text[:200]}")
            except Exception:
                pass
        return (False, None, {}, latency_ms)
    except Exception as e:
        latency_ms = (time.time() - t0) * 1000
        tb = _tb_b.format_exc()[-400:]
        _backoff_record_failure(source)
        try:
            _record_rest_error(f"[{source}] {type(e).__name__}: {e}", url=url, response_body=tb)
        except TypeError:
            try:
                _record_rest_error(f"[{source}] {type(e).__name__}: {e} | {url[:200]}")
            except Exception:
                pass
        return (False, None, {}, latency_ms)


# ── In-memory diagnostic ring buffer — last 100 issues, never written to DB ───
from collections import deque as _deque

_diag_log: "_deque[dict]" = _deque(maxlen=100)
_diag_log_lock = threading.Lock()


def log_diag_issue(source: str, severity: str, message: str, detail: str = "") -> None:
    """Append an entry to the in-memory diagnostic ring buffer.

    source:   'binance' | 'websocket' | 'signal_scanner' | 'sell_monitor' |
              'price_refresher' | 'force_sell' | 'startup' | 'system'
    severity: 'error' | 'warn' | 'info'
    """
    try:
        entry = {
            "ts":       time.time(),
            "iso":      datetime.now(timezone.utc).isoformat(),
            "source":   source,
            "severity": severity,
            "message":  str(message)[:500],
            "detail":   str(detail)[:1000] if detail else "",
        }
        with _diag_log_lock:
            _diag_log.append(entry)
    except Exception:
        pass


def get_diag_log(limit: int = 50, since_ts: float = 0.0, severity_filter: str = "") -> list:
    """Return entries from the ring buffer, newest first."""
    try:
        with _diag_log_lock:
            entries = list(_diag_log)
        entries.reverse()
        if since_ts:
            entries = [e for e in entries if e["ts"] > since_ts]
        if severity_filter:
            entries = [e for e in entries if e["severity"] == severity_filter]
        return entries[:limit]
    except Exception:
        return []


def clear_diag_log() -> int:
    """Clear the ring buffer. Returns count cleared."""
    try:
        with _diag_log_lock:
            n = len(_diag_log)
            _diag_log.clear()
        return n
    except Exception:
        return 0


def _record_rest_health(response_headers: dict, latency_ms: float):
    """Record health metrics from a Binance REST response. Zero cost — headers are free."""
    try:
        with _binance_health_lock:
            _binance_health["last_rest_ok_ts"]      = time.time()
            _binance_health["last_rest_latency_ms"] = round(latency_ms, 1)
            w = (response_headers.get("x-mbx-used-weight-1m")
                 or response_headers.get("X-MBX-USED-WEIGHT-1M"))
            if w:
                used = int(w)
                _binance_health["used_weight_1m"]  = used
                _binance_health["used_weight_pct"] = round(used / 6000.0 * 100.0, 1)
    except Exception:
        pass


_last_binance_err_log_ts: Dict[str, float] = {}
_BINANCE_ERR_LOG_THROTTLE_SEC = 30.0


def _record_rest_error(err_msg: str, url: str = "", response_body: str = "") -> None:
    try:
        with _binance_health_lock:
            _binance_health["rest_error_count"] += 1
            _binance_health["last_error_ts"]     = time.time()
            _binance_health["last_error_msg"]    = str(err_msg)[:200]
        # Throttle ring-buffer writes — same error type at most once per 30s
        err_key = str(err_msg)[:80]
        now = time.time()
        if now - _last_binance_err_log_ts.get(err_key, 0) >= _BINANCE_ERR_LOG_THROTTLE_SEC:
            _last_binance_err_log_ts[err_key] = now
            detail_parts = []
            if url:
                detail_parts.append(f"url={url[:300]}")
            if response_body:
                detail_parts.append(f"response={response_body[:300]}")
            log_diag_issue(
                "binance", "error",
                f"REST: {str(err_msg)[:150]} (×{_binance_health['rest_error_count']} since start)",
                detail=" | ".join(detail_parts) if detail_parts else "",
            )
    except Exception:
        pass


# ── Claude API error tracking (separate from Binance REST errors) ─────────────
_claude_consecutive_401s: int = 0
_CLAUDE_DISABLE_SECS = 86400  # 24 h after 3 consecutive 401s


def _record_claude_error(err_msg: str, is_auth_error: bool = False) -> None:
    """Record a Claude API error. After 3 consecutive auth failures, disable Claude for 24 h."""
    global _claude_consecutive_401s
    try:
        now = time.time()
        with _binance_health_lock:
            _binance_health["claude_error_count"]   += 1
            _binance_health["claude_last_error_ts"]  = now
            _binance_health["claude_last_error_msg"] = str(err_msg)[:200]

        if is_auth_error:
            _claude_consecutive_401s += 1
            if _claude_consecutive_401s >= 3:
                with _binance_health_lock:
                    _binance_health["claude_disabled_until"] = now + _CLAUDE_DISABLE_SECS
                log_diag_issue(
                    "claude", "error",
                    f"Claude API disabled for 24h — 3 consecutive auth failures",
                    detail=str(err_msg)[:300],
                )
            else:
                log_diag_issue(
                    "claude", "error",
                    f"Claude API auth error ({_claude_consecutive_401s}/3 before disable)",
                    detail=str(err_msg)[:300],
                )
        else:
            _claude_consecutive_401s = 0  # reset streak on non-auth errors
            log_diag_issue(
                "claude", "error",
                f"Claude API error (×{_binance_health['claude_error_count']} since start)",
                detail=str(err_msg)[:300],
            )
    except Exception:
        pass


def is_claude_disabled() -> bool:
    """Return True if Claude has been rate-disabled due to repeated auth failures."""
    with _binance_health_lock:
        return time.time() < _binance_health["claude_disabled_until"]


def reset_claude_errors() -> None:
    """Reset Claude error counters and re-enable Claude (called after key is fixed)."""
    global _claude_consecutive_401s
    _claude_consecutive_401s = 0
    with _binance_health_lock:
        _binance_health["claude_error_count"]    = 0
        _binance_health["claude_last_error_ts"]  = 0.0
        _binance_health["claude_last_error_msg"] = ""
        _binance_health["claude_disabled_until"] = 0.0


# ── Signal scanner health ──────────────────────────────────────────────────────
_signal_scanner_health: Dict = {
    "last_refresh_ts":  0.0,
    "last_duration_ms": 0.0,
    "scans_completed":  0,
    "interval_sec":     float(30),  # will be updated at runtime from config.SCAN_INTERVAL_SEC
    # C §6.1 scan stabilizers
    "scan_skipped_overlap":   0,      # refreshes skipped because one was already in flight
    "effective_interval_sec": float(30),  # adaptive sleep (>= SCAN_INTERVAL_SEC, <= 600)
    "universe_size":          0,      # symbols in the last scan pass
    # C §6.2/§6.5 — WS-first signal engine
    "mode":                  "ws-first",  # "ws-first" | "legacy" (strategy data.legacy_rest_scan)
    "stale_signal_count":    0,           # symbols whose cache entry is older than 180s
    "last_event_refresh_ts": 0.0,         # last kline-close (event-driven) full cache rebuild
    "db_fallback_refreshes": 0,           # thin-WS-buffer symbols refreshed from DB (no REST)
}

# C §6.2d — cache freshness: entries older than this are "stale" (~1 candle + margin)
_STALE_SIGNAL_SEC = 180.0

# Active symbol universe (approved coins) — set by both scanner modes; used by
# stale_signal_syms() so freshness is judged against symbols we SHOULD track.
_active_universe: set = set()

# One-per-change mode logging ("ws-first" vs "legacy")
_last_logged_scan_mode: str = ""

# C §6.3 — held-position exit watchdog state
_held_max_price_age_sec: float = 0.0          # freshest-price age of the worst held symbol
_watchdog_fire_ts: "_deque[float]" = _deque(maxlen=50000)  # REST-fire timestamps (24h window)
_watchdog_lock = threading.Lock()
_WATCHDOG_STALE_SEC = 3.0                     # any held age above this → ONE batched REST fetch
_WATCHDOG_ALERT_SEC = 5.0                     # any held age above this → price_feed diag error
_watchdog_alert_log_ts: float = 0.0           # 60s throttle for the price_feed alert

# C §6.1a — single-flight guard: only ONE _refresh_signal_cache may run at a
# time. Overlapping invocations are SKIPPED (never queued) via
# acquire(blocking=False); a second concurrent refresh must never stack up.
_signal_refresh_inflight = threading.Lock()

# C §6.1c — hard budgets for a refresh pass
_SCAN_SYMBOL_FETCH_TIMEOUT_SEC = 5.0    # per-symbol REST fetch ceiling (one retry)
_SCAN_PASS_BUDGET_SEC          = 120.0  # whole-pass ceiling, checked at batch boundaries

# C §6.1d — low_24h cache: {sym: (value, ts)}; reused within 10 minutes so the
# 1440-row DB read (heaviest per-scan cost) runs at most once per symbol per window.
_low24h_cache: Dict[str, Tuple[Optional[float], float]] = {}
_LOW24H_CACHE_TTL_SEC = 600.0

# Throttle for the "universe is empty" warning (once per 10 min, not every pass)
_last_empty_universe_warn_ts: float = 0.0

# New exit-mode flags (strategy.json controlled)
_take_profit_enabled: bool = True    # False → exit at breakeven (fees covered) only
_smart_hold_enabled:  bool = False   # True  → hold if signals still bullish; trail then exit
_trailing_stop_pct:   float = 0.5    # % drop from peak that triggers smart-hold exit

# Per-position high-water mark for smart hold (no lock needed — only written in sell thread)
_pos_peaks: Dict[str, float] = {}

_cooldowns: dict = {}

# ── Position index — rebuilt on every WebSocket tick for O(1) sell lookups ───
_pos_by_symbol: Dict[str, dict] = {}

# ── In-progress sell guard — prevents double-sells from monitor + guardian ────
_selling: set = set()
_selling_lock = threading.Lock()
_selling_ts: Dict[str, float] = {}   # when each sym was added — for watchdog

# ── In-progress BUY guard — mirrors _selling pattern to prevent double-buys ──
# Without this, two concurrent buy paths (scanner iteration overlap, force-buy
# racing scheduled buy, retry-after-failure) can fire two buys for the same
# symbol within milliseconds. Confirmed by duplicate DOT and IMX entries in DB.
_buying: set = set()
_buying_lock = threading.Lock()
_buying_ts: Dict[str, float] = {}
_BUYING_TIMEOUT_SEC = 30.0   # auto-release if a buy attempt crashes silently

# ── Bad-symbol blacklist — populated when Binance returns -1013 (market closed)
# Prevents repeated buy attempts and rate-limits sell retries on closed markets.
_bad_symbols: set = set()
_sell_last_failed_ts: Dict[str, float] = {}  # last sell failure time per symbol
_sell_last_failed_reason: Dict[str, str] = {}  # reason that triggered the failed sell
_ghost_check_fails: Dict[str, int] = {}  # consecutive -2010 check_failed count per symbol
_SELL_RETRY_COOLDOWN_PROFIT = 0.5   # take-profit: 0.5s breaks retry loop without delaying exit
_SELL_RETRY_COOLDOWN_LOSS   = 0.0   # stop-loss / force-sell: retry immediately

# ── Sell executor — parallel sells so 10 simultaneous exits never queue up ───
# Each position gets its own worker thread; _selling guard prevents duplicates.
_sell_executor = ThreadPoolExecutor(max_workers=12, thread_name_prefix="sell-worker")

# ── Buy-check executor — keeps REST calls off the event loop thread ───────────
# _check_buys_from_cache calls _get_usdt_balance() which is a blocking Binance
# REST call in live mode. Running it inline on the WS event loop starves sell
# triggers during WS reconnect bursts (observed: 21s trigger-to-noticed gap).
# 2 workers is plenty — buy checks are throttled to 0.1s internally.
_buy_check_executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="buy-check")
_buy_check_in_flight: bool = False
_buy_check_lock = threading.Lock()

# ── Real-time signal cache — updated on every kline close ────────────────────
_signal_cache: Dict[str, dict] = {}
_signal_cache_lock = threading.Lock()
_last_buy_check: float = 0.0
_last_no_signal_log: float = 0.0   # throttle "no coins ready" log to once per 60 s
_last_buy_scan_log: float = 0.0    # throttle "Buy scan: ..." to once per 30 s
_last_at_capacity_log: float = 0.0 # throttle "at max capacity" log to once per 60 s
_last_fallback_block_log: float = 0.0  # throttle "paper fallback — buys blocked" to once per 60 s
_last_paused_log: float = 0.0          # throttle "bot is paused" warn to once per 60 s
_last_fresh_nodata_log: float = 0.0    # throttle "fresh re-check has no data" warn to once per 60 s
_last_reversal_nodata_log: float = 0.0 # throttle "reversal check has no data" warn to once per 60 s
_warned_5m_warmup: bool = False        # one-shot "5m veto neutral during warmup" startup log

# Buy stagger — prevents mass simultaneous buys on stale cache signals
_last_buy_ts: float = 0.0
_buys_this_scan: int = 0
_BUY_STAGGER_SEC  = 15.0  # minimum seconds between consecutive buys
_MAX_BUYS_PER_SCAN = 2    # max buys per scan cycle

# Per-coin timestamp of last inline tick-driven signal refresh
_tick_signal_ts: Dict[str, float] = {}
_TICK_REFRESH_SEC = 1.0   # recompute at most every 1 s per coin from price ticks


# ── Balance guard + budget helpers ──────────────────────────────────────────────

def can_execute_buy(coin_cfg: dict, client) -> tuple[bool, str]:
    """
    Final pre-buy safety guard.
    Reads the shared _positions list directly (never a copy) so that every
    position appended since bot start is visible here.
    """
    budget = coin_cfg.get("budget_usdt", config.BUDGET_FIXED_USDT)
    try:
        # Geo-block-safe in live mode; param `client` kept for signature compat.
        account = _acct()
        balances = {b["asset"]: float(b["free"]) for b in account["balances"]}
        usdt_free = balances.get("USDT", 0.0)
    except Exception as e:
        return False, f"Cannot fetch account: {e}"

    if usdt_free < budget * 1.002:
        return False, f"Insufficient USDT: have {usdt_free:.2f}, need {budget * 1.002:.2f}"

    sym = coin_cfg["symbol"]
    max_concurrent = int(coin_cfg.get("max_concurrent", 1))

    # Read the single shared mutable list — never a copy — so appends from
    # earlier in the same scan loop are always visible here.
    with _positions_lock:
        open_this_coin = [p for p in _positions if p["symbol"] == sym]

    if len(open_this_coin) >= max_concurrent:
        return False, f"Max concurrent for {sym} reached ({max_concurrent})"

    return True, ""


def get_budget_for_coin(symbol: str, free_usdt: float) -> float:
    """Return trade size in USDT based on BUDGET_MODE (config defaults or strategy.json overrides).

    If bot_allocation_usdt > 0, the bot is restricted to that USDT cap across all
    concurrent positions — works identically in paper and live mode. The
    "effective free" balance for budget math becomes:
        min(free_usdt, allocation - sum_of_open_position_usdt)
    """
    strategy = _load_strategy()
    mode = strategy.get("budget_mode", config.BUDGET_MODE)
    reinvest = bool(strategy.get("reinvest_profits", False))
    allocation = float(strategy.get("bot_allocation_usdt", config.BOT_ALLOCATION_USDT))

    # Apply allocation cap: subtract value already locked in open positions.
    if allocation > 0:
        with _positions_lock:
            in_positions = sum(p.get("budget_usdt", 0.0) for p in _positions)
        effective_free = max(0.0, min(free_usdt, allocation - in_positions))
    else:
        effective_free = free_usdt

    # Authoritative starting balance: DB setting (written at wallet reset) takes priority
    # over strategy.json so a stale initial_balance_usdt never inflates reinvest scaling.
    initial_from_strategy = float(strategy.get("initial_balance_usdt", 0))
    if initial_from_strategy > 0:
        initial = initial_from_strategy
    else:
        starting_str = database.get_setting("paper_starting_balance")
        initial = float(starting_str) if starting_str else free_usdt

    if mode == "fixed":
        base = float(strategy.get("budget_fixed_usdt", config.BUDGET_FIXED_USDT))

    elif mode == "percent":
        pct = float(strategy.get("budget_pct_of_free", config.BUDGET_PCT_OF_FREE))
        # percent mode already scales with balance — reinvest has no extra effect
        return round(min(effective_free * (pct / 100), effective_free * 0.9), 2)

    elif mode == "capped":
        cap = float(strategy.get("budget_total_cap_usdt", config.BUDGET_TOTAL_CAP_USDT))
        with _positions_lock:
            total_in_positions = sum(p["budget_usdt"] for p in _positions)
        remaining_cap = cap - total_in_positions
        per_trade = cap / config.MAX_OPEN_POSITIONS
        base = min(per_trade, max(0.0, remaining_cap))

    elif mode == "per_coin":
        per_coin = strategy.get("budget_per_coin", config.BUDGET_PER_COIN)
        base = float(per_coin.get(symbol, config.BUDGET_FIXED_USDT))

    elif mode == "coin_pct":
        coin_pct = strategy.get("budget_coin_pct", {})
        pct = float(coin_pct.get(symbol, 5.0))
        # coin_pct already scales with balance — no extra reinvest scaling needed
        return round(min(effective_free * (pct / 100), effective_free * 0.9), 2)

    else:
        base = config.BUDGET_FIXED_USDT

    # Reinvest profits: scale budget proportionally to balance growth.
    # e.g. started 10 000 USDT, now have 12 000 → budget × 1.2 (20% more per trade).
    # Clamped to [0.5, 2.0] — tighter ceiling prevents runaway budget on small initial values.
    if reinvest and initial > 0 and mode in ("fixed", "per_coin", "capped"):
        scale = max(0.5, min(2.0, free_usdt / initial))
        base = base * scale

    # In fixed/per_coin mode return the full configured amount (when no allocation
    # cap is set) — can_execute_buy will reject the buy if free USDT is insufficient,
    # giving a clean "have X, need Y" message rather than silently trading a partial amount.
    # When bot_allocation_usdt IS set, enforce it here too: without this clamp the
    # early return bypassed the cap entirely and the bot could spend the whole wallet.
    if mode in ("fixed", "per_coin"):
        if allocation > 0:
            return round(min(base, effective_free), 2)
        return round(base, 2)
    # For capped mode cap to 90% so a tiny buffer remains for fees.
    return round(min(base, effective_free * 0.9), 2)


# ── Cooldown helpers ────────────────────────────────────────────────────

def _refresh_risk_params():
    """Read stop_loss_enabled/pct, take_profit_pct and new exit flags from strategy.json."""
    global _user_tp_mult, _take_profit_mult, _stop_loss_mult, _take_profit_enabled, _smart_hold_enabled, _trailing_stop_pct, _BUFFER_OVERRIDES
    strategy = _load_strategy()
    tp_pct = float(strategy.get("take_profit_pct", 0.1))   # e.g. 0.5 → 0.5%
    sl_pct = float(strategy.get("stop_loss_pct",   0.4))   # default 0.4% from BEP
    sl_on  = bool(strategy.get("stop_loss_enabled", True))  # always on by default
    _take_profit_enabled = bool(strategy.get("take_profit_enabled", True))
    _smart_hold_enabled  = bool(strategy.get("smart_hold_enabled",  False))
    _trailing_stop_pct   = float(strategy.get("trailing_stop_pct",  0.5))

    # Per-coin buffer overrides from strategy.json (e.g. {"SHIBUSDT": 0.30, "BTCUSDT": 0.08})
    overrides = strategy.get("slippage_buffer_overrides", {})
    if isinstance(overrides, dict):
        _BUFFER_OVERRIDES = {k: float(v) for k, v in overrides.items()}

    # User TP raw multiplier — per-position breakeven is computed by _get_breakeven_mult
    _user_tp_mult = 1.0 + (tp_pct / 100.0)

    # _take_profit_mult kept for legacy exports/diagnostics — use a mid-range reference price
    _take_profit_mult = (_FEE_FLOOR * 1.0010 if not _take_profit_enabled
                         else max(_FEE_FLOOR * 1.0010, _user_tp_mult))
    # Stop loss: set to 0.0 when disabled so the check (price <= 0.0) never fires
    _stop_loss_mult   = (1.0 - sl_pct / 100.0) if sl_on else 0.0


def _profitable_sell_check(pos: dict, price: float, force_fresh: bool = False) -> bool:
    """Return True only when selling at `price` nets at least min_profit + small slippage cushion.

    Uses ACTUAL deployed capital (qty * entry_price) — same basis as the trigger
    in compute_real_breakeven_price — so the trigger and the safety gate cannot
    disagree on what 'profitable' means.

    When force_fresh=True, does a single-symbol REST fetch and overrides the
    passed price with the freshest available.

    Keeps a 0.05% slippage cushion (reduced from 0.15%) — calibrated for the
    liquid coins the bot trades. Any take-profit sell that passes this check
    should never result in a net loss.
    """
    symbol = pos.get("symbol", "")
    # Only re-fetch when the triggering price is NOT fresh. A WS tick that just
    # crossed the target (<2s old) must be honored as-is: re-quoting via REST
    # adds up to 3s latency and can veto the sell if a sub-second spike already
    # retraced — exactly the exit the user asked for.
    _px_age = time.time() - _last_ws_price_ts.get(symbol, 0)
    if force_fresh and symbol and _px_age >= 2.0:
        try:
            # Own source tag: gate-fetch failures must not escalate the shared
            # batch_prices backoff and starve the price refreshers.
            _fresh = _fetch_batch_prices([symbol], source="presell_fresh")
            _fp = _fresh.get(symbol, 0) if _fresh else 0
            if _fp > 0:
                price = _fp
                _rest_px[symbol] = _fp
                _rest_px_sym_ts[symbol] = time.time()
                _last_ws_price_ts[symbol] = time.time()
                try:
                    import data_collector as _dc_psc
                    _dc_psc.prices[symbol] = _fp
                except Exception:
                    pass
        except Exception:
            pass  # fall through to use passed price

    entry   = float(pos.get("entry_price") or pos.get("avg_entry_price") or 0)
    qty     = float(pos.get("quantity", 0))
    buy_fee = float(pos.get("buy_fee_usdt") or 0)
    if entry <= 0 or qty <= 0 or price <= 0:
        return False
    # Use ACTUAL deployed capital (matches compute_real_breakeven_price)
    actual_cost   = qty * entry + buy_fee
    gross_quote   = price * qty
    est_sell_fee  = gross_quote * _fee_rate
    net_returned  = gross_quote - est_sell_fee
    # GATE == TRIGGER: this check must verify exactly the same threshold the
    # sell trigger fires at (compute_real_breakeven_price with the default
    # min_profit resolved via _min_profit_usdt()). The SAME config value MUST
    # be used in the BEP trigger, this profit gate, and any (future)
    # breakeven-move stop — see _min_profit_usdt.
    # The gate previously demanded max(min_profit+slippage, cost*tp_pct) NET —
    # i.e. more profit than the price at the exit target delivers after fees —
    # so every take-profit sell was vetoed AT the target and only executed
    # 0.05-0.25% higher (or never, if price retreated). The trigger already
    # guarantees price >= max(breakeven, entry*tp); the gate only needs to
    # confirm the sell nets a real profit, not re-add its own margin on top.
    min_profit = _min_profit_usdt()
    estimated_profit = net_returned - actual_cost
    return estimated_profit >= min_profit


def _set_cooldown(symbol: str):
    _cooldowns[symbol] = time.time() + config.COOLDOWN_AFTER_LOSS


def _in_cooldown(symbol: str) -> bool:
    exp = _cooldowns.get(symbol, 0)
    return time.time() < exp


# ── DB / startup helpers ─────────────────────────────────────────────────

def _rebuild_pos_index():
    """Rebuild O(1) symbol→position lookup. Call whenever _positions changes."""
    global _pos_by_symbol
    with _positions_lock:
        _pos_by_symbol = {p["symbol"]: p for p in _positions}


def _apply_coin_restore(coins: list):
    """Write a list of coin symbols back into strategy.json approved_coins."""
    if not (coins and isinstance(coins, list) and len(coins) > 0):
        return
    try:
        if os.path.exists(config.STRATEGY_FILE):
            with open(config.STRATEGY_FILE) as f:
                strat = json.load(f)
            existing = {c["symbol"]: c for c in strat.get("approved_coins", [])}
            strat["approved_coins"] = [
                {
                    "symbol":         sym,
                    "approved":       True,
                    "budget_usdt":    existing.get(sym, {}).get("budget_usdt", config.BUDGET_PER_TRADE_USDT),
                    "max_concurrent": existing.get(sym, {}).get("max_concurrent", 3),
                    "confidence":     existing.get(sym, {}).get("confidence", 0.5),
                    "reason":         "Restored from Supabase",
                }
                for sym in coins
            ]
            strat["updated_at"] = datetime.now(timezone.utc).isoformat()
            tmp = config.STRATEGY_FILE + ".tmp"
            with open(tmp, "w") as f:
                json.dump(strat, f, indent=2)
                f.flush()
                try:
                    os.fsync(f.fileno())
                except Exception:
                    pass
            os.replace(tmp, config.STRATEGY_FILE)
            print(f"[TradeEngine] Restored {len(coins)} coins from Supabase → strategy.json.")
    except Exception as ce:
        print(f"[TradeEngine] Coin restore to strategy.json failed: {ce}")


# Symbols/assets already warned about by the orphan scanner — log once per
# process, not on every startup loop.
_orphan_warned: set = set()


def _has_unclosed_bot_buy(sym: str) -> bool:
    """True when the DB records a bot LIVE BUY of `sym` with no later closing sell.

    The trades table only stores completed round-trips (a row is written at sell
    time), so the buy-time evidence lives in activity_log: every live buy logs
    "[LIVE] BOUGHT {sym} ..." and every close logs "[LIVE] SOLD {sym} ..." (or a
    force-close message). Read-only query — never adopt a wallet holding as a
    sellable position unless the bot itself bought it.
    """
    try:
        import sqlite3 as _sq_ob
        conn = _sq_ob.connect(database.DB_PATH)
        try:
            buy_id = conn.execute(
                "SELECT MAX(id) FROM activity_log WHERE message LIKE ?",
                (f"[LIVE] BOUGHT {sym} %",),
            ).fetchone()[0]
            if buy_id is None:
                return False
            sell_id = conn.execute(
                "SELECT MAX(id) FROM activity_log WHERE message LIKE ? "
                "OR message LIKE ? OR message LIKE ?",
                (f"[LIVE] SOLD {sym} %",
                 f"{sym}: force-closing position%",
                 f"{sym}: position removed from records%"),
            ).fetchone()[0]
        finally:
            conn.close()
        return sell_id is None or buy_id > sell_id
    except Exception:
        return False  # uncertainty must never fabricate a sellable position


def load_positions_from_db():
    """
    Called on startup.  Restores open positions from SQLite when available,
    or from Supabase when SQLite is empty (fresh Railway deploy / no volume).
    Coins and balance are ALWAYS restored from Supabase so the user's
    watchlist and wallet survive every redeploy regardless of SQLite state.
    """
    global _positions
    # Filter by current mode so live positions never load into a paper session
    # and vice versa — cross-loading caused paper client to "sell" live positions.
    _current_mode = get_mode()
    rows = database.load_positions(mode=_current_mode)

    # ── Startup migration: fix positions saved with wrong/missing mode tag ──────
    # Positions created before the mode-isolation fix default to mode='paper'
    # even when bought in live mode. If we're in live mode and find no positions
    # but the unfiltered table has rows, migrate them rather than losing them.
    if not rows:
        all_rows = database.load_positions()  # unfiltered
        if all_rows:
            database.migrate_positions_mode(_current_mode)
            rows = database.load_positions(mode=_current_mode)
            if rows:
                database.log_activity(
                    f"Startup: migrated {len(rows)} position(s) to mode='{_current_mode}' "
                    f"(were stored with wrong/missing mode tag)", "warn"
                )

    # Always fetch from Supabase so coins + balance are current even when
    # SQLite still has stale data from a previous deploy.
    supa_ok = False
    try:
        import concurrent.futures as _cf
        import supabase_sync
        with _cf.ThreadPoolExecutor(max_workers=1) as _ex:
            _fut = _ex.submit(supabase_sync.restore_from_supabase)
            try:
                restored = _fut.result(timeout=5)
            except _cf.TimeoutError:
                raise Exception("Supabase restore timed out after 5s")
        supa_ok = bool(restored)

        # ── Coins: ALWAYS restore — user selection must survive every deploy ──
        _apply_coin_restore(restored.get("selected_coins"))

        # ── Positions: only restore when SQLite is empty (avoids duplicates) ──
        if not rows and restored.get("positions"):
            for pos in restored["positions"]:
                pos_id = database.save_position(pos)
                pos["id"] = pos_id
            rows = database.load_positions(mode=_current_mode)
            n_pos = len(rows)
            print(f"[TradeEngine] Restored {n_pos} position(s) from Supabase.")
            database.log_activity(
                f"Restored {n_pos} open position(s) from Supabase after redeploy", "info"
            )
        elif rows:
            database.log_activity(
                f"Startup: {len(rows)} position(s) loaded from local DB (Supabase backup intact)", "info"
            )
        else:
            database.log_activity(
                "Startup: no positions in local DB or Supabase — fresh wallet", "info"
            )

        # ── Trade history: restore when SQLite trades table is empty ────────────
        # Rebuilds win-rate, P&L stats and trade list from bot_trade_history.
        try:
            if not database.get_recent_trades(limit=1) and restored.get("trades"):
                imported = 0
                for trade in restored["trades"]:
                    try:
                        database.log_trade(trade)
                        imported += 1
                    except Exception:
                        pass
                if imported:
                    print(f"[TradeEngine] Imported {imported} trade(s) from Supabase history.")
                    database.log_activity(
                        f"Restored {imported} trade record(s) from Supabase history", "info"
                    )
            elif database.get_recent_trades(limit=1):
                database.log_activity(
                    f"Trade history intact in local DB — no restore needed", "info"
                )
        except Exception as te:
            print(f"[TradeEngine] Trade history restore failed (non-fatal): {te}")

        # ── Balance: restore when SQLite paper-state is empty or zero ──────────
        if restored.get("usdt_balance") is not None:
            usdt = restored["usdt_balance"]
            _pc = _client()
            if hasattr(_pc, "_balances"):
                with _pc._lock:
                    current = _pc._balances.get("USDT", 0.0)
                # Restore from Supabase when:
                #   a) local balance is zero / negative
                #   b) no open positions in SQLite (fresh deploy / no volume)
                #   c) balance is at the ENV starting default AND Supabase has a
                #      meaningfully different value (paper_state was never persisted)
                _starting_usdt = float(os.getenv("STARTING_PAPER_USDT", "10000.0"))
                _at_default    = abs(current - _starting_usdt) < 0.01
                _supa_differs  = abs(usdt - current) > 1.0
                if current <= 0 or not rows or (_at_default and _supa_differs):
                    with _pc._lock:
                        _pc._balances["USDT"] = usdt
                        snapshot = dict(_pc._balances)
                    database.save_paper_state(snapshot)
                    print(f"[TradeEngine] Restored USDT balance from Supabase: {usdt:.2f}")
                    database.log_activity(
                        f"Restored USDT balance from Supabase: {usdt:.2f} USDT", "info"
                    )

    except Exception as e:
        print(f"[TradeEngine] Supabase restore failed (non-fatal): {e}")
        database.log_activity(f"Supabase restore failed — data may not survive redeploy: {e}", "warn")

    with _positions_lock:
        _positions = list(rows)
    _rebuild_pos_index()
    print(f"[TradeEngine] Loaded {len(_positions)} open position(s) from DB.")

    # Push current state to Supabase immediately so the next redeploy can restore it.
    # This runs even when SQLite had data (not just after a restore) so Supabase
    # always has an up-to-date snapshot regardless of how the data got into SQLite.
    try:
        import supabase_sync
        usdt = _get_usdt_balance()
        supabase_sync.sync_all(list(rows), usdt)
        database.log_activity(
            f"Supabase snapshot pushed — {len(rows)} position(s), {usdt:.2f} USDT backed up", "info"
        )
    except Exception as e:
        database.log_activity(f"Supabase snapshot push failed: {e}", "warn")

    # Live mode: scan Binance Spot for coins the bot has no position for.
    # AUTO-RECOVER orphaned positions so real money is never invisible to the bot.
    # Positions are lost when the DB is wiped (container restart without a volume)
    # or when Supabase restore fails. We rebuild records from Binance balances so
    # the sell monitor immediately tracks them and can exit at take-profit/stop-loss.
    if get_mode() == "live":
        try:
            acc = _acct()  # geo-block-safe direct transport
            tracked_symbols = {p["symbol"] for p in _positions}
            recovered_syms = []
            now_ts = datetime.now(timezone.utc).isoformat()

            for b in acc["balances"]:
                asset  = b["asset"]
                free   = float(b["free"])
                locked = float(b["locked"])
                total  = free + locked
                # Skip quote/stable/fiat assets — no <asset>USDT pair exists for
                # them (e.g. USDUSDT → -1121 Invalid symbol) or they aren't
                # something the bot should ever auto-sell.
                if asset in ("USDT", "BNB", "BUSD", "USDC", "USD", "FDUSD", "TUSD",
                             "DAI", "USDP", "AEUR", "EUR", "GBP", "TRY", "BRL",
                             "AUD", "UAH", "RUB", "NGN", "ZAR", "PLN", "ARS",
                             "JPY", "MXN", "CZK", "COP") or total <= 0:
                    continue
                # Binance Earn wrappers (LDPYTH etc.) are not tradeable spot
                # assets — never adopt or warn about them.
                if asset.startswith("LD"):
                    continue
                # Binance Earn wrappers (LDPYTH etc.) are not tradeable spot
                # assets — never adopt or warn about them.
                if asset.startswith("LD"):
                    continue
                sym = asset + "USDT"
                if sym in tracked_symbols:
                    continue

                # Fetch current price — try cached REST price first, then the
                # public data-api ticker (geo-block-safe, no signed call needed)
                price = _rest_px.get(sym, 0) or 0
                if price <= 0:
                    try:
                        price = float(_fetch_batch_prices([sym]).get(sym, 0) or 0)
                    except Exception:
                        pass

                # Use only free balance — locked qty can't be market-sold and
                # would cause -2010 "insufficient balance" on Binance.
                sell_qty = free if free > 0 else total
                value = sell_qty * price if price > 0 else 0

                # Dust below Binance's ~$10 min notional can't be sold anyway —
                # skip it (log once per process, not every startup/loop).
                if price > 0 and value < 10.0:
                    if value >= 1.0 and asset not in _orphan_warned:
                        _orphan_warned.add(asset)
                        database.log_activity(
                            f"Orphan scan: skipping {asset} (~${value:.2f}) — below $10 "
                            f"min notional, unsellable dust", "info"
                        )
                    continue

                # ONLY adopt holdings the bot itself bought (recorded live BUY
                # with no closing sell). Anything else is the user's personal
                # holding — fabricating a position here would let the sell
                # monitor liquidate the user's own coins.
                if not _has_unclosed_bot_buy(sym):
                    if asset not in _orphan_warned:
                        _orphan_warned.add(asset)
                        database.log_activity(
                            f"Orphan scan: {asset} (~${value:.2f}) held on Binance but has "
                            f"no recorded bot buy — leaving untouched (not a bot position)",
                            "info"
                        )
                    continue

                if price > 0 and value > 1.0:
                    # Recreate position using current price as estimated entry.
                    # Entry is unknown after a DB wipe — current price is the safest
                    # default: the bot will monitor from here and exit on TP/SL.
                    exit_target = round(price * _get_breakeven_mult(price, sym), 8)
                    pos_record = {
                        "symbol":       sym,
                        "entry_price":  price,
                        "exit_target":  exit_target,
                        "quantity":     sell_qty,
                        "budget_usdt":  round(value, 2),
                        "buy_fee_usdt": round(value * _fee_rate, 6),
                        "timestamp":    now_ts,
                        "mode":         "live",
                    }
                    pos_id = database.save_position(pos_record)
                    pos_record["id"] = pos_id
                    with _positions_lock:
                        _positions.append(pos_record)
                    tracked_symbols.add(sym)
                    recovered_syms.append(sym)
                    msg = (
                        f"AUTO-RECOVERED orphaned live position: {sym} "
                        f"qty={total:.8f} @ ${price:.4f} (~${value:.2f} USDT) — "
                        f"entry price is ESTIMATED (current price used). "
                        f"Adjust stop-loss in dashboard if needed."
                    )
                    print(f"[TradeEngine] {msg}")
                    database.log_activity(msg, "warn")
                elif asset not in _orphan_warned:
                    _orphan_warned.add(asset)
                    msg = (
                        f"⚠ ORPHANED COIN: {asset} qty={total:.8f} on Binance "
                        f"but no price available — sell manually via Binance app."
                    )
                    print(f"[TradeEngine] {msg}")
                    database.log_activity(msg, "warn")

            if recovered_syms:
                _rebuild_pos_index()
                database.log_activity(
                    f"Recovered {len(recovered_syms)} orphaned live position(s) "
                    f"from Binance balances: {', '.join(recovered_syms)}", "warn"
                )
        except Exception as e:
            database.log_activity(f"Orphan scan/recovery failed (non-fatal): {e}", "warn")

    # Sync PaperClient coin balances from the restored positions.
    # After a restart, PaperClient loads from saved paper_state, but if that
    # state is out of sync with the positions table (e.g. a partial crash),
    # sells will fail with "Insufficient balance". Self-heal by crediting the
    # exact quantity that each open position holds.
    _pc2 = _client()
    if hasattr(_pc2, "_balances") and _positions:
        changed = False
        for pos in _positions:
            sym  = pos["symbol"]
            coin = sym[:-4]  # strip USDT
            qty  = float(pos.get("quantity", 0))
            if qty <= 0:
                continue
            with _pc2._lock:
                current = _pc2._balances.get(coin, 0.0)
                if current < qty * 0.99:
                    _pc2._balances[coin] = qty
                    changed = True
                    print(f"[TradeEngine] Synced paper balance: {coin}={qty:.8f} (was {current:.8f})")
        if changed:
            database.save_paper_state(dict(_pc2._balances))


def get_open_positions() -> List[dict]:
    with _positions_lock:
        return list(_positions)


# ── exchangeInfo cache warm-up ─────────────────────────────────────────
try:
    from exchange_info import get_symbol_filters as _warmup_gsf
    _warmup_gsf("BTCUSDT")  # warms the exchangeInfo cache at startup
    del _warmup_gsf
except Exception:
    pass


# ── Strategy loader ────────────────────────────────────────────────────

def _load_strategy() -> dict:
    global _strategy_mtime, _strategy_cache
    path = config.STRATEGY_FILE
    if not os.path.exists(path):
        return {}
    try:
        mtime = os.path.getmtime(path)
        if mtime != _strategy_mtime:
            with open(path, "r") as f:
                _strategy_cache = json.load(f)
            _strategy_mtime = mtime
    except Exception:
        pass
    return _strategy_cache


# ── Account helpers ────────────────────────────────────────────────────

def _get_usdt_balance() -> float:
    try:
        if get_mode() != "live":
            # Paper mode: read directly from PaperClient's in-memory balance dict
            _pc = _client()
            if hasattr(_pc, "_balances"):
                with _pc._lock:
                    return float(_pc._balances.get("USDT", 0.0))
            return float(os.getenv("STARTING_PAPER_USDT", "10000.0"))
        # Live mode: query Binance spot wallet — use free balance only (tradeable).
        # Geo-block-safe direct transport (python-binance requests are blocked).
        acc = _acct()
        for b in acc["balances"]:
            if b["asset"] == "USDT":
                return float(b["free"])
    except Exception as e:
        database.log_activity(f"get_usdt_balance error: {e}", "warn")
    return 0.0


def _get_actual_balance(symbol: str) -> float:
    """Return free+locked balance for the base asset of symbol, or 0.0 on error."""
    try:
        base = symbol.replace("USDT", "").replace("usdt", "")
        acc = _acct()
        for b in acc.get("balances", []):
            if b["asset"] == base:
                return float(b.get("free", 0)) + float(b.get("locked", 0))
    except Exception as e:
        log_diag_issue("ghost_check", "warn", f"_get_actual_balance failed for {symbol}: {e}")
    return 0.0


def _is_truly_ghost_position(symbol: str, expected_qty: float) -> Tuple[bool, str]:
    """Verify whether a -2010 sell failure means the position is truly gone.

    Returns (is_ghost, reason). Only returns is_ghost=True when the asset
    balance on Binance is genuinely zero — not when the bot just has the wrong
    quantity on record.  Errors during the check return (False, 'check_failed')
    so uncertainty never causes an accidental force-close.
    """
    try:
        base = symbol.replace("USDT", "").replace("usdt", "")
        acc = _acct()
        for b in acc.get("balances", []):
            if b["asset"] == base:
                total = float(b.get("free", 0)) + float(b.get("locked", 0))
                if total <= 1e-8:
                    return True, "zero_balance"
                if total < expected_qty * 0.95:
                    return False, f"qty_mismatch (have {total:.6f}, expected {expected_qty:.6f})"
                return False, "balance_ok"
        # Asset absent from balance list → genuinely not owned
        return True, "asset_not_in_account"
    except Exception as e:
        log_diag_issue("ghost_check", "error", f"Balance check failed for {symbol}: {e}")
        return False, "check_failed"


_lot_step_cache: Dict[str, float] = {}

# ── Buy-rejection instrumentation — pure observation, no logic changes ────────
from collections import defaultdict

_rejection_counts: dict = defaultdict(int)
_rejection_examples: dict = {}
_rejection_lock = threading.Lock()
_rejection_reset_ts: float = time.time()

def _record_rejection(symbol: str, score, reason: str, detail: str = ""):
    # NOTE: no score filter here — engine-path rejections carry the ENGINE
    # score (often 0-2) and systematic gates (pre-gate, min-notional, vetoes)
    # must be visible in diagnostics regardless of the legacy score.
    try:
        with _rejection_lock:
            _rejection_counts[reason] += 1
            ex_list = _rejection_examples.setdefault(reason, [])
            ex_list.append({
                "ts": time.time(),
                "symbol": symbol,
                "score": int(score or 0),
                "detail": str(detail)[:200],
            })
            if len(ex_list) > 5:
                ex_list.pop(0)
    except Exception:
        pass
    try:
        database.record_buy_rejection(symbol, reason, str(detail)[:500] if detail else None, int(score or 0), None)
    except Exception:
        pass

def get_rejection_stats() -> dict:
    with _rejection_lock:
        return {
            "counts":   dict(_rejection_counts),
            "examples": {k: list(v) for k, v in _rejection_examples.items()},
            "reset_ts": _rejection_reset_ts,
        }


def clear_rejection_stats() -> int:
    global _rejection_reset_ts
    with _rejection_lock:
        n = sum(_rejection_counts.values())
        _rejection_counts.clear()
        _rejection_examples.clear()
        _rejection_reset_ts = time.time()
        return n


def evaluate_buy_gates(sym: str) -> dict:
    """Mirror of the per-symbol gate chain in _check_buys_from_cache, WITHOUT
    executing anything. Used by /api/signals-summary so the frontend only shows
    a coin as a buy candidate when the bot would ACTUALLY buy it right now —
    a coin that passes the signal score but fails a veto (5m downtrend, BB top,
    cooldown, macro gate, paused bot…) must not display as 'BUY'.

    Returns {"ready": bool, "blockers": [str, ...]} — blockers lists every gate
    currently failing (empty when ready). Transient gates the buy loop retries
    on its own (buy stagger, per-scan cap) are intentionally excluded.
    """
    blockers: list = []
    strategy = _load_strategy()

    if not strategy.get("trading_active", True):
        blockers.append("bot_paused")
    if get_mode() == "live" and connection.is_using_paper_fallback():
        blockers.append("live_connection_down")

    max_pos = int(strategy.get("max_positions", 10))
    with _positions_lock:
        n_open = len(_positions)
        already_held = any(p["symbol"] == sym for p in _positions)
    if n_open >= max_pos:
        blockers.append(f"max_positions ({n_open}/{max_pos})")
    if already_held:
        blockers.append("already_held")

    approved = {c["symbol"] for c in strategy.get("approved_coins", []) if c.get("approved")}
    if approved and sym not in approved:
        blockers.append("not_in_approved_coins")

    if _in_cooldown(sym):
        blockers.append("cooldown_after_loss")
    if _loss_cooldown.get(sym, 0) > time.time():
        blockers.append("slippage_loss_cooldown")

    if bool(strategy.get("macro_gate_enabled", True)):
        _btc = get_btc_state()
        if _btc and _btc.get("regime") == "bearish":
            blockers.append("btc_bearish_macro_gate")

    with _signal_cache_lock:
        cached = _signal_cache.get(sym)
    if not cached:
        blockers.append("no_signal_data")
        return {"ready": False, "blockers": blockers}

    min_sigs = int(strategy.get("min_signals", config.MIN_SIGNALS_TO_BUY))
    signal_engine_active = (
        SIGNAL_REGISTRY_AVAILABLE
        and bool(strategy.get("signal_engine", {}).get("enabled", False))
    )
    if signal_engine_active:
        # Mirror the fixed pipeline: when the engine is enabled it is the sole
        # signal authority — no legacy-score blocker. Surface the engine's own
        # decision so the UI matches what the buy loop would actually do.
        try:
            _sig_data = {
                **cached.get("signals", {}),
                "rsi_value":       cached.get("rsi_val", 0.0),
                "current_price":   cached.get("price", 0.0),
                "low_24h":         cached.get("low_24h"),
                "klines_1m":       cached.get("klines_1m", []),
                "stoch_rsi_value": cached.get("stoch_rsi_val"),
            }
            _dec = _sr_evaluate_buy_decision(sym, _sig_data, strategy)
            if not _dec.get("allowed"):
                blockers.append(f"engine:{_dec.get('reason', 'blocked')}")
        except Exception:
            pass
    else:
        if cached.get("score", 0) < min_sigs:
            blockers.append(f"score {cached.get('score', 0)}/{min_sigs}")
        if bool(strategy.get("mandatory_signals_enabled", True)):
            sigs = cached.get("signals", {})
            rsi_v = cached.get("rsi_val", 0.0)
            # Single source of truth: nested signal_thresholds first, root
            # key as fallback — matches the buy loop's legacy mandatory gate.
            rsi_threshold = float(
                strategy.get("signal_thresholds", {}).get(
                    "rsi_buy_threshold", strategy.get("rsi_buy_threshold", 40.0)
                )
            )
            if not sigs.get("trend", False):
                blockers.append("mandatory_ema_down")
            if rsi_v <= 0 or rsi_v >= rsi_threshold:
                blockers.append(f"rsi {rsi_v:.0f} >= {rsi_threshold:.0f}")

    if not cached.get("bb_ok", True):
        blockers.append("price_at_upper_bollinger")
    if not cached.get("5m_ok", True):
        blockers.append("5m_downtrend")

    return {"ready": not blockers, "blockers": blockers}


# ── BTC market regime filter ───────────────────────────────────────────────────
_market_regime_cache: dict = {"ts": 0.0, "regime": "unknown", "details": {}}
_MARKET_REGIME_TTL_SEC = 120.0

def _fetch_btc_1h_klines() -> Optional[List[dict]]:
    # C §6.4 — non-critical background fetch: when can_spend refuses, callers
    # (get_market_regime / get_btc_state) fall back to their cached values.
    url = "https://data-api.binance.vision/api/v3/klines?symbol=BTCUSDT&interval=1h&limit=24"
    ok, data, _, _ = _binance_request(url, timeout=4.0, source="btc_klines",
                                      weight=2, critical=False)
    if not ok or not isinstance(data, list):
        return None
    try:
        return [{"open": float(k[1]), "high": float(k[2]), "low": float(k[3]),
                 "close": float(k[4]), "volume": float(k[5])} for k in data]
    except Exception:
        return None

def _ema_calc(values: List[float], period: int) -> float:
    if not values or period <= 0:
        return 0.0
    k = 2.0 / (period + 1.0)
    ema_val = values[0]
    for v in values[1:]:
        ema_val = v * k + ema_val * (1 - k)
    return ema_val

def get_market_regime() -> dict:
    """Returns regime: 'bullish'|'choppy'|'bearish'|'unknown'. Cached 2 min."""
    now = time.time()
    if (now - _market_regime_cache["ts"]) < _MARKET_REGIME_TTL_SEC:
        return _market_regime_cache
    klines = _fetch_btc_1h_klines()
    if not klines or len(klines) < 12:
        _market_regime_cache.update({"ts": now, "regime": "unknown", "details": {"error": "no_data"}})
        return _market_regime_cache
    closes = [k["close"] for k in klines]
    ema_8  = _ema_calc(closes, 8)
    ema_24 = _ema_calc(closes, 24)
    pct_24h = (closes[-1] - closes[0]) / closes[0] * 100 if closes[0] > 0 else 0
    pct_4h  = (closes[-1] - closes[-4]) / closes[-4] * 100 if closes[-4] > 0 else 0
    if ema_8 > ema_24 and pct_24h > 0.5:
        regime = "bullish"
    elif ema_8 < ema_24 and pct_24h < -2.0:
        regime = "bearish"
    elif pct_4h < -1.5:
        regime = "bearish"
    else:
        regime = "choppy"
    details = {"btc_price": closes[-1], "ema_8": round(ema_8, 2), "ema_24": round(ema_24, 2), "pct_24h": round(pct_24h, 3), "pct_4h": round(pct_4h, 3), "ts": now}
    _market_regime_cache.update({"ts": now, "regime": regime, "details": details})
    return _market_regime_cache


# ── BTC macro gate state — simpler thresholds, 5-min TTL ─────────────────────
# Used by the pre-loop macro gate in _check_buys_from_cache.
# Bearish = pct_24h < -3% OR pct_4h < -2% (clear dump, not just chop).
# Called from _buy_check_executor thread — NEVER from event loop.
_btc_state_cache: dict = {"ts": 0.0, "data": None}
_BTC_STATE_TTL_SEC = 300.0

def get_btc_state() -> Optional[dict]:
    """BTC macro state for macro gate. 5-min cache. Must run on buy-check thread."""
    now = time.time()
    if _btc_state_cache["data"] and (now - _btc_state_cache["ts"]) < _BTC_STATE_TTL_SEC:
        return _btc_state_cache["data"]
    klines = _fetch_btc_1h_klines()
    if not klines or len(klines) < 12:
        return _btc_state_cache.get("data")
    closes = [k["close"] for k in klines]
    ema_8  = _ema_calc(closes, 8)
    ema_24 = _ema_calc(closes, 24)
    pct_24h = (closes[-1] - closes[0]) / closes[0] * 100 if closes[0] > 0 else 0
    pct_4h  = (closes[-1] - closes[-4]) / closes[-4] * 100 if closes[-4] > 0 else 0
    if pct_24h < -3.0 or pct_4h < -2.0:
        regime = "bearish"
    elif ema_8 > ema_24 and pct_24h > 1.0:
        regime = "bullish"
    else:
        regime = "choppy"
    data = {
        "price":   closes[-1],
        "ema_8":   round(ema_8, 2),
        "ema_24":  round(ema_24, 2),
        "pct_4h":  round(pct_4h, 3),
        "pct_24h": round(pct_24h, 3),
        "regime":  regime,
    }
    _btc_state_cache["ts"]   = now
    _btc_state_cache["data"] = data
    return data


# ── Reversal confirmation ──────────────────────────────────────────────────────
_reversal_cache: dict = {}
_REVERSAL_TTL_SEC = 30.0

def _fetch_1m_klines_rc(symbol: str, limit: int = 6) -> Optional[List[dict]]:
    """Fetch 1m klines for reversal confirmation — tries every Binance base
    (same fallback list as the scanner) instead of a single hardcoded host."""
    for _base in _KLINE_BASES:
        url = f"{_base}/api/v3/klines?symbol={symbol}&interval=1m&limit={limit}"
        _src = f"reversal_klines_{_base.split('//', 1)[-1].split('.', 1)[0]}"
        # C §6.4 — non-critical: WS closed-candle buffer is the fallback
        ok, data, _, _ = _binance_request(url, timeout=3.0, source=_src,
                                          weight=2, critical=False)
        if not ok or not isinstance(data, list) or len(data) < limit:
            continue
        try:
            return [{"open": float(k[1]), "high": float(k[2]), "low": float(k[3]),
                     "close": float(k[4]), "volume": float(k[5])} for k in data]
        except Exception:
            continue
    return None


def _reversal_candles(symbol: str) -> Optional[List[dict]]:
    """Return the last 5 COMPLETED 1m candles for reversal confirmation.

    REST returns the in-progress candle last, so fetch 6 and drop it. When
    REST fails, fall back to the WebSocket closed-candle buffer (which holds
    only completed candles). Returns None when no source has enough data.
    """
    klines = _fetch_1m_klines_rc(symbol, limit=6)
    if klines and len(klines) >= 6:
        return klines[:-1]  # drop the in-progress candle
    try:
        import data_collector as _dc_rc
        buf = list(_dc_rc.ws_candles.get(symbol, []))[-6:]
        if len(buf) >= 5:
            return [{"open": float(r[1]), "high": float(r[2]), "low": float(r[3]),
                     "close": float(r[4]), "volume": float(r[5])} for r in buf[-5:]]
    except Exception:
        pass
    return None


def is_reversal_confirmed(symbol: str) -> tuple:
    """Returns (confirmed: bool, reason: str).

    Evaluates the last COMPLETED 1m candle (green + volume surge vs the prior
    4 completed candles) — never the in-progress candle, whose partial volume
    made confirmation near-impossible at arbitrary buy moments. When no candle
    data is available from any source (REST + WS), PASSES with a throttled
    warning: the pre-buy fresh re-check has already validated momentum, and
    data unavailability alone must never permanently block buying."""
    global _last_reversal_nodata_log
    now = time.time()
    cached_rc = _reversal_cache.get(symbol)
    if cached_rc and (now - cached_rc["ts"]) < _REVERSAL_TTL_SEC:
        return cached_rc["confirmed"], cached_rc.get("reason", "cached")
    candles = _reversal_candles(symbol)
    if not candles or len(candles) < 5:
        if now - _last_reversal_nodata_log >= 60.0:
            _last_reversal_nodata_log = now
            try:
                database.log_activity(
                    f"{symbol}: reversal confirmation has no candle data (REST+WS) — "
                    f"passing through (fresh re-check already validated momentum)", "warn"
                )
            except Exception:
                pass
        _reversal_cache[symbol] = {"ts": now, "confirmed": True, "reason": "no_data_pass"}
        return True, "no_data_pass"
    last = candles[-1]          # last COMPLETED candle
    prev = candles[:-1]         # the 4 completed candles before it
    if last["close"] <= last["open"]:
        _reversal_cache[symbol] = {"ts": now, "confirmed": False, "reason": "last_candle_red"}
        return False, "last_candle_red"
    avg_prev_vol = sum(c["volume"] for c in prev) / 4.0
    if avg_prev_vol > 0 and last["volume"] < avg_prev_vol * 1.15:
        _reversal_cache[symbol] = {"ts": now, "confirmed": False, "reason": "weak_volume"}
        return False, "weak_volume"
    recent_low = min(c["low"] for c in prev)
    if last["close"] <= recent_low:
        _reversal_cache[symbol] = {"ts": now, "confirmed": False, "reason": "still_below_lows"}
        return False, "still_below_lows"
    _reversal_cache[symbol] = {"ts": now, "confirmed": True, "reason": "ok"}
    return True, "ok"


# ── Pre-buy fresh kline fetch — multi-source, never fail-closed ───────────────

def _fetch_fresh_1m_candles(sym: str, limit: int = 50, min_candles: int = 36) -> list:
    """Fetch fresh 1m OHLCV candles for the pre-buy re-check.

    limit=50 gives MACD parity with the 60s scan (histogram[-2] needs >=36
    closes — the old limit=30 made the macd signal structurally impossible).

    Source order: every Binance REST base in _KLINE_BASES (per-base backoff
    keys so one dead host doesn't blind the others), then the in-memory
    WebSocket closed-candle buffer, then DB candles.

    Returns a list of candle dicts (open/high/low/close/volume) with at least
    ``min_candles`` entries, or [] when NO source has enough data. Callers
    must treat [] as "no fresh data available" and proceed on the cached
    signal decision — never as a failed signal check.
    """
    import urllib.parse as _up_fr
    qs = _up_fr.urlencode({"symbol": sym, "interval": "1m", "limit": limit})
    for _base in _KLINE_BASES:
        _src = f"klines_pre_buy_{_base.split('//', 1)[-1].split('.', 1)[0]}"
        # C §6.4 — non-critical: WS buffer + DB candles are the fallbacks
        ok, raw, _, _ = _binance_request(f"{_base}/api/v3/klines?{qs}",
                                         timeout=2.0, source=_src,
                                         weight=2, critical=False)
        if not ok or not isinstance(raw, list) or len(raw) < min_candles:
            continue
        try:
            return [{"open": float(k[1]), "high": float(k[2]), "low": float(k[3]),
                     "close": float(k[4]), "volume": float(k[5])} for k in raw]
        except Exception:
            continue
    try:
        import data_collector as _dc_fr
        buf = list(_dc_fr.ws_candles.get(sym, []))
        if len(buf) >= min_candles:
            return [{"open": float(r[1]), "high": float(r[2]), "low": float(r[3]),
                     "close": float(r[4]), "volume": float(r[5])} for r in buf[-limit:]]
    except Exception:
        pass
    try:
        rows = database.get_candles(sym, config.CANDLE_TIMEFRAME, limit=limit)
        if len(rows) >= min_candles:
            return [{"open":   float(r.get("open") or r["close"]),
                     "high":   float(r.get("high") or r["close"]),
                     "low":    float(r.get("low") or r["close"]),
                     "close":  float(r["close"]),
                     "volume": float(r.get("volume") or 0.0)} for r in rows]
    except Exception:
        pass
    return []


def _floor_qty(qty: float, symbol: str = "", decimals: int = 8) -> float:
    """Floor quantity to the symbol's LOT_SIZE stepSize to avoid Binance -1111 errors.
    Falls back to flat 8 decimal places if symbol is unknown or API call fails."""
    if symbol:
        step = _lot_step_cache.get(symbol)
        if step is None:
            try:
                info = _client().get_symbol_info(symbol)
                for f in (info or {}).get("filters", []):
                    if f.get("filterType") == "LOT_SIZE":
                        step = float(f["stepSize"])
                        break
            except Exception:
                step = 0.0
            # Cache only successful lookups — a transient failure (timeout,
            # geo-block, paper client) must not disable LOT_SIZE rounding for
            # this symbol permanently; retry on the next call instead.
            if step and step > 0:
                _lot_step_cache[symbol] = step
        if step and step > 0:
            factor = 1.0 / step
            return math.floor(qty * factor) / factor
    factor = 10 ** decimals
    return math.floor(qty * factor) / factor


# ── Indicator helpers (derive from candle dict) ─────────────────────────────

def _derive_ma_pos(price: float, ma20: Optional[float]) -> Optional[str]:
    if ma20 is None or ma20 == 0:
        return None
    diff = abs(price - ma20) / ma20
    if diff <= 0.001:
        return "at"
    return "above" if price > ma20 else "below"


def _derive_bb_pos(price: float, candle: dict) -> Optional[str]:
    upper = candle.get("bb_upper")
    lower = candle.get("bb_lower")
    mid   = candle.get("bb_mid")
    if upper is None or lower is None or mid is None:
        return None
    bw = upper - lower
    if bw == 0:
        return "mid_zone"
    near = 0.01 * bw
    if price > upper:
        return "above_upper"
    if price >= upper - near:
        return "near_upper"
    if price <= lower:
        return "below_lower"
    if price <= lower + near:
        return "near_lower"
    return "mid_zone"


# ── Signal evaluation — 6-signal dict ────────────────────────────────────

def evaluate_signals(candles: list) -> dict:
    """
    Evaluate all six technical signals on the last COMPLETED candle (index -2).
    Returns {"trend", "rsi", "macd", "volume", "obv", "atr"} — all booleans.

    candles: list of dicts with keys close, volume, high, low.
    When high/low are unavailable (tick data), pass high=low=close — ATR will
    return None → atr_is_tradeable returns False → atr signal = False.

    Short/empty input returns all-False instead of raising (the [-2]/[-3]
    indexing below needs at least 3 candles) — callers must decide whether
    thin data means "skip the check", never rely on an IndexError.
    """
    if not candles or len(candles) < 3:
        return {"trend": False, "rsi": False, "macd": False,
                "volume": False, "obv": False, "atr": False}
    closes  = [c["close"]  for c in candles]
    volumes = [c["volume"] for c in candles]

    ema9        = indicators.calc_ema(closes, 9)
    ema21       = indicators.calc_ema(closes, 21)
    rsi_vals    = indicators.calc_rsi(closes, 14)
    _, _, histo = indicators.calc_macd(closes)
    vol_ma      = indicators.calc_volume_ma(volumes, 20)

    trend = bool(
        ema9[-2]  is not None
        and ema21[-2] is not None
        and ema9[-2] > ema21[-2]
    )
    rsi = bool(
        rsi_vals[-2] is not None
        and config.RSI_BUY_MIN <= rsi_vals[-2] <= config.RSI_BUY_MAX
    )
    # MACD: histogram positive AND rising (confirms momentum, not just a positive reading).
    macd = bool(
        histo[-2] is not None
        and histo[-3] is not None
        and histo[-2] > 0
        and histo[-2] > histo[-3]
    )
    volume = bool(
        volumes[-2] is not None
        and vol_ma[-2] is not None
        and vol_ma[-2] > 0
        and volumes[-2] >= vol_ma[-2] * config.VOLUME_RATIO_MIN
    )
    obv = indicators.obv_is_bullish(candles)
    atr = indicators.atr_is_tradeable(
        indicators.calc_atr(candles, config.ATR_PERIOD),
        candles[-2]["close"],
        config.ATR_MIN_PCT,
        config.ATR_MAX_PCT,
    )

    return {"trend": trend, "rsi": rsi, "macd": macd, "volume": volume, "obv": obv, "atr": atr}


# ── C §6.2 — WS-first full cache rebuild helpers ─────────────────────────────

def _row_to_candle(r) -> Optional[dict]:
    """Normalize one buffer row (raw kline list OR dict) into a candle dict.
    Defensive against the data_collector buffer format changing shape."""
    try:
        if isinstance(r, dict):
            c = float(r["close"])
            out = {
                "open":   float(r.get("open",  c) or c),
                "high":   float(r.get("high",  c) or c),
                "low":    float(r.get("low",   c) or c),
                "close":  c,
                "volume": float(r.get("volume", 0) or 0.0),
            }
            if r.get("open_time") is not None:
                out["open_time"] = int(r["open_time"])
            return out
        # Raw kline row: [open_time, open, high, low, close, volume, ...]
        return {
            "open_time": int(r[0]),
            "open":      float(r[1]),
            "high":      float(r[2]),
            "low":       float(r[3]),
            "close":     float(r[4]),
            "volume":    float(r[5]),
        }
    except Exception:
        return None


def _ws_buffer_candles(symbol: str, min_candles: int = 16) -> list:
    """Real-OHLCV candle dicts from data_collector.ws_candles (120×1m buffer).
    Returns [] when the buffer is missing/thin/malformed — callers fall back."""
    try:
        import data_collector as _dc_wb
        buf = list((getattr(_dc_wb, "ws_candles", None) or {}).get(symbol) or [])
    except Exception:
        return []
    if len(buf) < min_candles:
        return []
    out = [c for c in (_row_to_candle(r) for r in buf) if c is not None]
    return out if len(out) >= min_candles else []


def _five_m_state(symbol: str, prev: dict) -> tuple:
    """(5m_ok, 5m_ts) for the WS-first rebuild.

    Source order: ws_candles_5m (≥21 candles, reuse is_5m_bullish) → 5m derived
    from the deep 1m WS buffer (120×1m → 24×5m) → warmup-neutral rule: preserve
    a previously computed value if one exists, else neutral (True) so data
    availability alone never vetoes buys after a restart."""
    global _warned_5m_warmup
    candles_5m: list = []
    try:
        import data_collector as _dc_5m
        buf5 = list((getattr(_dc_5m, "ws_candles_5m", None) or {}).get(symbol) or [])
        candles_5m = [c for c in (_row_to_candle(r) for r in buf5) if c is not None]
    except Exception:
        candles_5m = []
    if len(candles_5m) < 21:
        try:
            one_m = _ws_buffer_candles(symbol, min_candles=105)
            if one_m:
                derived = indicators.aggregate_candles(one_m, group=5)
                if len(derived) >= 21:
                    candles_5m = derived
        except Exception:
            pass
    if len(candles_5m) >= 21:
        try:
            return bool(indicators.is_5m_bullish(candles_5m)), time.time()
        except Exception:
            pass
    # Warmup-neutral (kept from the REST scan): not enough 5m data anywhere.
    prev_ts = prev.get("5m_ts", 0) or 0
    if prev_ts:
        return prev.get("5m_ok", True), prev_ts
    if not _warned_5m_warmup:
        _warned_5m_warmup = True
        try:
            database.log_activity(
                "5m veto warmup: <21 five-minute candles available "
                "(WS buffer refilling) — treating 5m trend as neutral "
                "until data accumulates", "warn"
            )
        except Exception:
            pass
    return True, 0


def _low_24h_for(symbol: str, candles: list) -> Optional[float]:
    """24h low: existing 10-min cache → min(DB 1440-row low, WS-buffer low).
    The WS buffer covers the freshest ~2h (rows may not be persisted yet);
    the DB read supplies the true 24h span."""
    _l24 = _low24h_cache.get(symbol)
    if _l24 is not None and (time.time() - _l24[1]) < _LOW24H_CACHE_TTL_SEC:
        return _l24[0]
    lows: list = []
    try:
        _db_rows_24h = database.get_candles(symbol, config.CANDLE_TIMEFRAME, limit=1440)
        if len(_db_rows_24h) >= 60:
            lows.append(min(float(r.get("low") or r["close"]) for r in _db_rows_24h))
    except Exception:
        pass
    try:
        if candles:
            lows.append(min(c["low"] for c in candles))
    except Exception:
        pass
    low_24h = min(lows) if lows else None
    if low_24h is not None:
        _low24h_cache[symbol] = (low_24h, time.time())
    return low_24h


def _rebuild_full_entry(symbol: str, candles: list) -> bool:
    """C §6.2a — rebuild the FULL signal-cache entry from real-OHLCV candles:
    6 signals with REAL high/low (accurate ATR), RSI display value, bb_ok,
    stoch_rsi_val, klines_1m (last 15 real OHLCV), low_24h, 5m_ok.

    Merge-preserve semantics: keys set by other paths are never dropped.
    Runs on a worker thread (kline handler dispatches via run_in_executor).
    Returns True when the entry was stored."""
    if not candles or len(candles) < 16:
        return False
    try:
        closes  = [c["close"]  for c in candles]
        volumes = [c["volume"] for c in candles]
        signals = evaluate_signals(candles)   # real high/low → ATR accurate
        score   = sum(signals.values())
        rsi_list    = indicators.calc_rsi(closes, 14)
        rsi_display = rsi_list[-2] if rsi_list[-2] is not None else 0.0
        # BB veto on the last completed candle (same semantics as the REST scan)
        bb_u, bb_m, _ = indicators.calc_bollinger(closes)
        bb_ok = indicators.bb_buy_allowed(closes[-2], bb_u[-2], bb_m[-2])
        try:
            stoch_rsi_val = indicators.calc_stoch_rsi(closes)
        except Exception:
            stoch_rsi_val = None

        with _signal_cache_lock:
            prev = dict(_signal_cache.get(symbol, {}))
        five_m_ok, five_m_ts = _five_m_state(symbol, prev)
        low_24h = _low_24h_for(symbol, candles)

        with _signal_cache_lock:
            entry = dict(_signal_cache.get(symbol, {}))  # re-read under write lock
            entry.update({
                "signals":       signals,
                "score":         score,
                "price":         closes[-1],
                "rsi_val":       rsi_display,
                "bb_ok":         bb_ok,
                "5m_ok":         five_m_ok,
                "5m_ts":         five_m_ts,
                "ts":            time.time(),
                "low_24h":       low_24h,
                "klines_1m":     [dict(c) for c in candles[-15:]],
                "stoch_rsi_val": stoch_rsi_val,
            })
            _signal_cache[symbol] = entry
        _signal_scanner_health["last_event_refresh_ts"] = time.time()
        return True
    except Exception as e:
        print(f"[TradeEngine] Full entry rebuild error {symbol}: {e}")
        return False


def update_coin_signals(symbol: str, closes: list, volumes: list):
    """Update the signal cache on every kline close (WebSocket-driven).

    C §6.2a — WS-first: when the data_collector ws_candles buffer has real
    OHLCV depth, rebuild the FULL cache entry from it (accurate ATR, bb_ok,
    5m_ok, low_24h, klines_1m, stoch_rsi_val) — no REST needed. Called on a
    worker thread (data_collector's kline handler runs _persist_and_signal
    via run_in_executor), so the DB reads here never touch the event loop.

    Fallback (thin WS buffer, e.g. right after restart): the legacy minimal
    path below — fake candles (high=low=close) so OBV works; ATR/BB/5m values
    from the last full refresh are preserved via the prev cache entry.
    """
    if len(closes) < 16:  # minimum for RSI-14 to produce a valid value at [-2]
        return
    try:
        _full = _ws_buffer_candles(symbol)
        if _full and _rebuild_full_entry(symbol, _full):
            return
    except Exception as e:
        print(f"[TradeEngine] WS-first rebuild error {symbol}: {e}")
    try:
        candles = [
            {"high": c, "low": c, "close": c, "volume": v}
            for c, v in zip(closes, volumes)
        ]
        signals = evaluate_signals(candles)
        # ATR requires real high/low data — tick candles have high=low=close so ATR=0.
        # Preserve the last REST-computed ATR value instead of overwriting with False.
        with _signal_cache_lock:
            prev = _signal_cache.get(symbol, {})
        signals["atr"] = prev.get("signals", {}).get("atr", False)
        score   = sum(signals.values())
        rsi_list    = indicators.calc_rsi(closes, 14)
        rsi_display = rsi_list[-2] if rsi_list[-2] is not None else 0.0

        with _signal_cache_lock:
            # MERGE into the previous entry instead of replacing it — the old
            # replace dropped klines_1m/low_24h/stoch_rsi_val/5m_ts written by
            # the REST scan, starving M4_micro_pullback (mandatory!), R1 and
            # M2 of data every time a 1m kline closed, and defeating the 180s
            # 5m-veto cache (5m_ts loss forced a REST refetch every scan).
            prev  = _signal_cache.get(symbol, {})  # re-read under the write lock
            entry = dict(prev)
            entry.update({
                "signals":  signals,
                "score":    score,
                "price":    closes[-1],
                "rsi_val":  rsi_display,
                "bb_ok":    prev.get("bb_ok",  False),  # preserved from last REST scan
                "5m_ok":    prev.get("5m_ok",  False),  # preserved from last REST scan
                "ts":       time.time(),
            })
            # Append the newly closed candle to klines_1m so R1/M4 keep seeing
            # fresh data between REST scans (cap at 60 — more than the engine
            # needs). Skip the append when the REST scan already recorded it.
            try:
                _k1        = list(entry.get("klines_1m") or [])
                _new_close = float(closes[-1])
                _new_vol   = float(volumes[-1]) if volumes else 0.0
                _last_k    = _k1[-1] if _k1 else None
                _is_dup = bool(
                    _last_k
                    and abs(float(_last_k.get("close", 0)) - _new_close) < 1e-12
                    and abs(float(_last_k.get("volume", 0)) - _new_vol) < 1e-12
                )
                if not _is_dup:
                    _open = float(closes[-2]) if len(closes) >= 2 else _new_close
                    _k1.append({
                        "open":   _open,
                        "high":   max(_open, _new_close),
                        "low":    min(_open, _new_close),
                        "close":  _new_close,
                        "volume": _new_vol,
                    })
                entry["klines_1m"] = _k1[-60:]
            except Exception:
                pass
            _signal_cache[symbol] = entry
    except Exception as e:
        print(f"[TradeEngine] Signal cache error {symbol}: {e}")


# ── Shared sell execution (used by both realtime_monitor and signal_scanner) ──

def _execute_sell(pos: dict, price: float, reason: str):
    """Execute a market sell for pos at price. Logs trade, cleans up state.
    Caller MUST have already added pos['symbol'] to _selling before submitting
    to the executor — this function just verifies and executes."""
    from datetime import timezone as _tz
    pos["_sell_picked_up_ts"] = time.time()
    sym  = pos["symbol"]
    try:
        database.log_activity(
            f"[SELL_START] {sym} ({reason}): raw_qty={pos.get('quantity')} price={price}",
            "info"
        )
    except Exception:
        pass
    qty  = _floor_qty(pos["quantity"], pos["symbol"])
    mode = get_mode()
    now  = datetime.now(_tz.utc).isoformat()

    if qty <= 0 or price <= 0:
        database.log_activity(
            f"[SELL_EARLY_RETURN] {sym} ({reason}): qty={qty} price={price} — invalid, skipping",
            "warn"
        )
        with _selling_lock:
            _selling.discard(sym)
            _selling_ts.pop(sym, None)
        return

    # ── Lot-step rounding (toggleable via strategy.use_lot_step_rounding) ───
    _strategy_ls = _load_strategy()
    if _strategy_ls.get("use_lot_step_rounding", True):
        try:
            from exchange_info import compute_sell_quantity
            _ls_qty, _ls_leftover, _ls_reason = compute_sell_quantity(sym, qty, current_price=price)
            if _ls_qty <= 0:
                database.log_activity(
                    f"[SELL_SKIPPED] {sym} ({reason}): lot_step — {_ls_reason} (raw_qty={qty})",
                    "warn"
                )
                with _selling_lock:
                    _selling.discard(sym)
                    _selling_ts.pop(sym, None)
                return
            if _ls_leftover > 0.0:
                log.info("%s: lot_step rounding sells %s (leftover %s)", sym, _ls_qty, _ls_leftover)
            qty = _ls_qty
        except Exception as _ls_exc:
            log.warning("%s: lot_step rounding failed (%s), using raw qty", sym, _ls_exc)

    # ── Clamp to actual Binance free balance (prevents APIError -2010) ─────
    if get_mode() == "live":
        try:
            _asset = sym[:-4]  # e.g. "XRP" from "XRPUSDT"
            _acc = _acct()  # geo-block-safe direct transport in live mode
            _free = next(
                (float(b["free"]) for b in _acc.get("balances", []) if b["asset"] == _asset),
                None
            )
            if _free is not None:
                if abs(qty - _free) > qty * 0.001:  # > 0.1% mismatch — log it
                    log.warning(
                        "%s: qty mismatch — db=%s, binance_free=%s, using %s for sell (%s)",
                        sym, qty, _free, min(qty, _free), reason
                    )
                qty = min(qty, _free)
                # Re-apply lot-step rounding after balance clamp
                if _strategy_ls.get("use_lot_step_rounding", True):
                    try:
                        from exchange_info import compute_sell_quantity
                        _clamped_qty, _, _cr = compute_sell_quantity(sym, qty)
                        if _clamped_qty <= 0:
                            database.log_activity(
                                f"[SELL_SKIPPED] {sym} ({reason}): after balance clamp — {_cr}",
                                "warn"
                            )
                            with _selling_lock:
                                _selling.discard(sym)
                                _selling_ts.pop(sym, None)
                            return
                        qty = _clamped_qty
                    except Exception:
                        pass
        except Exception as _bal_exc:
            log.warning("%s: failed to fetch Binance balance for sell clamp: %s", sym, _bal_exc)

    # Verify position still exists — another executor task may have already sold it
    # (e.g. force-sell came in while we were queued).
    with _positions_lock:
        pos_id = pos.get("id")
        if pos_id and not any(p.get("id") == pos_id for p in _positions):
            database.log_activity(
                f"[SELL_EARLY_RETURN] {sym} ({reason}): position id={pos_id} already removed — skipping duplicate sell",
                "warn"
            )
            with _selling_lock:
                _selling.discard(sym)
                _selling_ts.pop(sym, None)
            return  # already sold

    try:
        _do_execute_sell(pos, sym, qty, price, reason, mode, now)
    except Exception as _exc_sell:
        database.log_activity(
            f"[SELL_EXCEPTION] {sym} ({reason}): {type(_exc_sell).__name__}: {_exc_sell}",
            "error"
        )
        log_diag_issue(
            "force_sell" if reason == "force-sell" else "sell_monitor",
            "error",
            f"Sell failed: {sym} ({reason}) — {type(_exc_sell).__name__}",
            detail=str(_exc_sell),
        )
        # Force-sell / manual: if we hit an exception, the user explicitly wanted the
        # position gone. Remove from records so retries stop rather than leaving a ghost.
        if reason in ("force-sell", "manual", "user-initiated"):
            # Log a best-effort trade record before deleting so audit trail is preserved.
            try:
                _fs_qty    = float(pos.get("quantity", 0))
                _fs_entry  = float(pos.get("entry_price", 0))
                _fs_cost   = _fs_qty * _fs_entry
                _fs_quote  = price * _fs_qty
                _fs_sfee   = _fs_quote * _fee_rate
                _fs_bfee   = float(pos.get("buy_fee_usdt") or _fs_cost * _fee_rate)
                _fs_profit = _fs_quote - _fs_sfee - _fs_cost - _fs_bfee
                database.log_trade({
                    "coin":             sym,
                    "mode":             mode,
                    "entry_price":      _fs_entry,
                    "exit_price":       price,
                    "quantity":         _fs_qty,
                    "budget_usdt":      pos.get("budget_usdt"),
                    "buy_fee":          _fs_bfee,
                    "sell_fee":         _fs_sfee,
                    "net_profit":       _fs_profit,
                    "profitable":       1 if _fs_profit > 0 else 0,
                    "entry_rsi":        pos.get("entry_rsi"),
                    "timestamp_buy":    pos.get("timestamp"),
                    "timestamp_sell":   now,
                    "sell_reason":      f"force-sell-failed: {str(_exc_sell)[:200]}",
                    "signal_snapshot":  pos.get("signal_snapshot"),
                })
                database.log_activity(
                    f"[FORCE-SELL-FAIL] {sym}: trade record logged with estimated P&L "
                    f"(est_profit={_fs_profit:.4f})",
                    "warn"
                )
            except Exception as _log_err:
                database.log_activity(
                    f"[FORCE-SELL-FAIL] {sym}: also failed to log trade record: {_log_err}",
                    "error"
                )
            with _positions_lock:
                _positions[:] = [p for p in _positions if p.get("id") != pos.get("id")]
            if pos.get("id"):
                try:
                    database.delete_position(pos["id"])
                except Exception:
                    pass
            _rebuild_pos_index()
            database.log_activity(
                f"[FORCE_CLEAN] {sym}: force-sell raised exception — position removed from records",
                "warn"
            )
    finally:
        with _selling_lock:
            _selling.discard(sym)
            _selling_ts.pop(sym, None)
        # Always clear the smart-hold peak — even on sell failure — so that a
        # later position re-opened on the same symbol doesn't inherit a stale
        # high-water mark and instantly trip its trailing stop.
        _pos_peaks.pop(sym, None)


def _do_execute_sell(pos: dict, sym: str, qty: float, price: float, reason: str, mode: str, now: str):
    """Inner sell logic — called only when the _selling guard is held."""
    from datetime import timezone as _tz

    # Ensure PaperClient price is current before the sell
    try:
        _client().update_price(sym, price)
    except Exception:
        pass

    # Paper mode: self-heal coin balance if it went missing after a restart.
    # open position record is the source of truth for what we own.
    _pc = _client()
    if mode != "live" and hasattr(_pc, "_balances"):
        coin = sym[:-4]
        with _pc._lock:
            current_coin_bal = _pc._balances.get(coin, 0.0)
            if current_coin_bal < qty * 0.99:
                _pc._balances[coin] = qty
                snapshot = dict(_pc._balances)
            else:
                snapshot = None
        if snapshot is not None:
            database.save_paper_state(snapshot)
            database.log_activity(
                f"[PaperWallet] Auto-credited {qty:.8f} {coin} "
                f"(had {current_coin_bal:.8f}) — balance was out of sync after restart",
                "warn",
            )

    _is_paper = (mode != "live")
    pos["_sell_gate_start_ts"] = time.time()
    # FINAL SAFETY GATE — never lose money on a take-profit sell.
    # Stop-loss and force-sell bypass this check (they're meant to fire even at a loss).
    # auto-recycle bypasses too: the recycler explicitly frees capital from positions
    # >3% underwater — the profit gate would veto every one of its sells otherwise.
    if reason not in ("stop-loss", "force-sell", "manual", "user-initiated", "auto-recycle"):
        # force_fresh=True: _profitable_sell_check fetches a fresh REST price for this
        # symbol RIGHT NOW before evaluating. This prevents a stale price from passing
        # the check while the actual Binance fill comes back at a lower level.
        if not _profitable_sell_check(pos, price, force_fresh=True):
            # Price is not profitable even at the freshest REST quote.
            # Abort — position stays open, next tick re-evaluates.
            _now_abort = time.time()
            if _now_abort - _last_abort_log_ts.get(sym, 0) >= _ABORT_LOG_THROTTLE_SEC:
                _last_abort_log_ts[sym] = _now_abort
                _fresh_now = _rest_px.get(sym, 0) or price
                database.log_activity(
                    f"SELL ABORTED {sym} ({reason}): freshest REST price ${_fresh_now:.6f} "
                    f"would not net the {_min_profit_usdt():.4f} USDT minimum profit. "
                    f"Position held — will retry on next favourable tick (log throttled 60s).",
                    "warn"
                )
            _sell_last_failed_ts[sym] = time.time()
            _sell_last_failed_reason[sym] = reason
            return  # exit before placing order
        # If the gate fetched a fresh REST quote, use it as the paper-mode
        # execution price. Only when actually fresh — an old _rest_px entry
        # must not override the WS trigger price.
        if (time.time() - _rest_px_sym_ts.get(sym, 0)) < 5.0:
            price = _rest_px.get(sym, 0) or price

    pos["_sell_gate_done_ts"] = time.time()
    pos["_sell_binance_start_ts"] = time.time()
    try:
        # Paper mode: pass the trigger price directly so concurrent WebSocket/REST
        # updates cannot cause the sell to execute at the wrong price.
        _log_order_intent("SELL", sym, qty, price)
        if _is_paper:
            result = _client().order_market_sell(symbol=sym, quantity=qty, price=price)
        else:
            # Live mode: geo-block-safe direct signed transport
            result = _market_sell(sym, qty)
        _log_order_result("SELL", sym, qty, price, result)
        pos["_sell_binance_done_ts"] = time.time()
        _ghost_check_fails.pop(sym, None)  # reset -2010 backoff on success
    except Exception as e:
        pos["_sell_binance_done_ts"] = time.time()
        err_str = str(e)
        msg = f"SELL failed {sym} ({reason}): {e}"
        print(f"[TradeEngine] {msg}")
        database.log_activity(msg, "error")
        _sell_last_failed_ts[sym] = time.time()
        _sell_last_failed_reason[sym] = reason

        # -1013: market closed/delisted — blacklist and force-close
        # -2010: insufficient balance — could be ghost OR a quantity/lot-size
        #        mismatch on a real position. Verify with a balance check first.
        is_possible_ghost = "-2010" in err_str or "insufficient balance" in err_str.lower()
        is_closed = "-1013" in err_str or "Market is closed" in err_str
        if is_closed:
            _bad_symbols.add(sym)

        should_force_close = is_closed
        force_close_label = ""

        if is_possible_ghost and not is_closed:
            is_ghost, ghost_reason = _is_truly_ghost_position(sym, float(pos.get("quantity", 0)))
            if is_ghost:
                # Genuinely zero — safe to force-close
                database.log_activity(
                    f"[GHOST CONFIRMED] {sym}: balance={ghost_reason} — closing position tracker", "warn"
                )
                should_force_close = True
                force_close_label = f"ghost position ({ghost_reason})"
            else:
                # Real position — quantity mismatch or lot-size issue
                if ghost_reason == "check_failed":
                    # Exponential backoff: while the signed account endpoint is
                    # down, each 0.5s retry burns a live order attempt on the
                    # same -2010. Back off 1s→2s→4s… capped at 30s, and escalate
                    # a diag error after 5 consecutive failures.
                    n = _ghost_check_fails.get(sym, 0) + 1
                    _ghost_check_fails[sym] = n
                    _sell_last_failed_ts[sym] = time.time() + min(30.0, (2 ** min(n, 5)) * 0.5)
                    database.log_activity(
                        f"[GHOST CHECK FAILED] {sym}: -2010 but balance check inconclusive — "
                        f"retry #{n}, backing off {min(30.0, (2 ** min(n, 5)) * 0.5):.0f}s", "warn"
                    )
                    log_diag_issue("sell_monitor", "error" if n >= 5 else "warn",
                                   f"{sym}: -2010 balance check failed ×{n}, will retry", detail=err_str[:400])
                else:
                    actual_qty = _get_actual_balance(sym)
                    database.log_activity(
                        f"[GHOST FALSE ALARM] {sym}: {ghost_reason} — updating qty to {actual_qty:.6f}", "warn"
                    )
                    log_diag_issue("sell_monitor", "warn",
                                   f"{sym}: qty mismatch ({ghost_reason}), adjusted to {actual_qty:.6f}",
                                   detail=err_str[:400])
                    if actual_qty > 0:
                        with _positions_lock:
                            pos["quantity"] = actual_qty
                if reason in ("force-sell", "manual", "user-initiated"):
                    raise RuntimeError(
                        f"force-sell rejected by Binance (-2010): {err_str[:200]} — "
                        f"ghost_reason={ghost_reason}"
                    )
                return  # let sell retry next tick with corrected qty
        elif is_closed:
            force_close_label = "market closed/delisted"

        if should_force_close:
            database.log_activity(
                f"{sym}: force-closing position — {force_close_label}", "warn"
            )
            log_diag_issue(
                "sell_monitor", "warn",
                f"{sym}: force-closed — {force_close_label}",
                detail=err_str[:400],
            )
            with _positions_lock:
                before = len(_positions)
                _positions[:] = [p for p in _positions if p.get("symbol") != sym]
                if len(_positions) < before:
                    # Remove from DB too so it doesn't come back on restart
                    pos_id = pos.get("id")
                    if pos_id:
                        try:
                            database.delete_position(pos_id)
                        except Exception:
                            pass
                    database.log_activity(f"{sym}: position removed from records", "warn")
            _rebuild_pos_index()
        return

    # ── Fee-aware fill parsing ───────────────────────────────────────────────────
    # Market orders can split across multiple fills at different price levels.
    # Each fill has its own commission amount and asset.
    # Wrap in try/except so an unexpected Binance response format never causes
    # a silent position ghost (sell executed on Binance, position stays in bot).
    try:
        fills      = result.get("fills", [])
        raw_quote  = float(result.get("cummulativeQuoteQty") or 0)
        _exec_qty  = float(result.get("executedQty") or 0)
        # Exit price = VWAP across ALL fills (cummulativeQuoteQty / executedQty).
        # fills[0] is only the best-priced tranche of a multi-fill market order.
        # (Live only: PaperClient's cummulativeQuoteQty is already net of fee.)
        if mode == "live" and raw_quote > 0 and _exec_qty > 0:
            fill_price = raw_quote / _exec_qty
        elif fills:
            fill_price = float(fills[0].get("price", price))
        else:
            fill_price = price
        budget     = pos["budget_usdt"]
        # Use ACTUAL deployed capital as cost basis, not the requested budget —
        # and use the SAME quantity for cost basis as for proceeds. Lot-step
        # rounding / balance clamps can sell less than pos['quantity']; charging
        # the full recorded quantity against partial proceeds books a phantom loss.
        _pos_qty   = float(pos.get("quantity") or 0)
        _sold_qty  = _exec_qty if _exec_qty > 0 else qty
        _sold_frac = (_sold_qty / _pos_qty) if _pos_qty > 0 else 1.0
        actual_cost = _sold_qty * float(pos["entry_price"])

        if mode == "live":
            sell_fee, fee_asset = _fills_fee_usdt(fills, raw_quote * _fee_rate)
            # Binance sell-fee semantics: a stablecoin commission IS deducted from
            # the quote proceeds (received = cummulativeQuoteQty - commission);
            # a BNB commission comes out of the separate BNB balance, so the
            # full quote amount is received. ("estimated" fallback is treated as
            # quote-deducted — the conservative assumption.)
            if fee_asset in ("USDT", "BUSD", "USDC", "estimated"):
                usdt_returned = raw_quote - sell_fee
            else:
                usdt_returned = raw_quote
            buy_fee = float(pos.get("buy_fee_usdt") or _pos_qty * float(pos["entry_price"]) * _fee_rate)
            buy_fee *= _sold_frac  # charge only the sold portion's share of the buy fee
        else:
            sell_fee      = sum(float(f.get("commission") or 0) for f in fills)
            usdt_returned = raw_quote
            buy_fee       = actual_cost * _fee_rate

        net_profit = usdt_returned - actual_cost - buy_fee
    except Exception as parse_err:
        # Sell DID execute on Binance — fall back to estimates so trade is
        # still recorded and position is properly cleaned up.
        database.log_activity(
            f"SELL {sym} ({reason}): fill parse error ({parse_err}) — using estimates", "warn"
        )
        fill_price    = price
        raw_quote     = price * pos.get("quantity", 0)
        actual_cost   = float(pos.get("quantity", 0)) * float(pos.get("entry_price", 0))
        sell_fee      = raw_quote * _fee_rate
        buy_fee       = float(pos.get("buy_fee_usdt") or actual_cost * _fee_rate)
        usdt_returned = raw_quote - sell_fee
        net_profit    = usdt_returned - actual_cost - buy_fee

    # Post-fill integrity check: if a take-profit sell ended up net-negative,
    # classify the cause honestly rather than always blaming slippage.
    #
    # Genuine slippage: fill price materially worse than the trigger price.
    # Fee/rounding loss: fill matched the trigger but fees ate the margin —
    #   not a market execution problem, just a thin-margin edge case.
    #   These should NOT trigger the re-buy cooldown.
    if net_profit < 0 and reason == "take-profit":
        _trig_px  = price                     # the trigger price we checked against
        _fill_px  = fill_price                # what Binance actually filled at
        _slip_pct = ((_trig_px - _fill_px) / _trig_px * 100) if _trig_px > 0 else 0
        # Slippage threshold: 0.05% — smaller moves are fee/rounding artefacts
        _SLIP_THRESHOLD_PCT = 0.05
        if _slip_pct > _SLIP_THRESHOLD_PCT:
            reason = "slippage-loss"
            _loss_cooldown[sym] = time.time() + _LOSS_COOLDOWN_SEC
            database.log_activity(
                f"SLIPPAGE-LOSS {sym}: trigger=${_trig_px:.6f} fill=${_fill_px:.6f} "
                f"slip={_slip_pct:.3f}% net={net_profit:.4f} USDT "
                f"— relabeled 'slippage-loss', cooldown {_LOSS_COOLDOWN_SEC // 60}min", "warn"
            )
        else:
            reason = "below-breakeven"
            # No cooldown — this was a fee/rounding edge, not a bad fill.
            database.log_activity(
                f"BELOW-BREAKEVEN {sym}: trigger=${_trig_px:.6f} fill=${_fill_px:.6f} "
                f"slip={_slip_pct:.3f}% net={net_profit:.4f} USDT "
                f"— relabeled 'below-breakeven' (no cooldown)", "warn"
            )

    buy_ts  = pos.get("timestamp", now)
    sell_ts = now
    try:
        buy_dt  = datetime.fromisoformat(buy_ts.replace("Z", "+00:00"))
        sell_dt = datetime.fromisoformat(sell_ts.replace("Z", "+00:00"))
        # Both are timezone-aware; subtraction works directly
        if buy_dt.tzinfo and sell_dt.tzinfo:
            duration = int((sell_dt - buy_dt).total_seconds())
        else:
            duration = 0
    except Exception:
        duration = 0
        buy_dt   = datetime.now(_tz.utc)

    _intended_buy_price = pos.get("intended_buy_price")
    _pos_buy_slippage   = pos.get("buy_slippage_pct")
    _intended_sell_price = price
    _sell_slippage_pct = ((fill_price - price) / price * 100) if price > 0 else None
    trade_record = {
        "coin":               sym,
        "mode":               mode,
        "entry_price":        pos["entry_price"],
        "exit_price":         fill_price,
        "quantity":           qty,
        "budget_usdt":        pos["budget_usdt"],
        "buy_fee":            buy_fee,
        "sell_fee":           sell_fee,
        "net_profit":         net_profit,
        "profitable":         1 if net_profit > 0 else 0,
        "duration_seconds":   duration,
        "entry_rsi":          pos.get("entry_rsi"),
        "entry_ma_position":  pos.get("entry_ma_position"),
        "entry_bb_position":  pos.get("entry_bb_position"),
        "entry_volume_trend": pos.get("entry_volume_trend"),
        "hour_of_day":        buy_dt.hour,
        "day_of_week":        buy_dt.weekday(),
        "timestamp_buy":      buy_ts,
        "timestamp_sell":     sell_ts,
        "sell_reason":        reason,
        "signal_snapshot":    pos.get("signal_snapshot"),
        "intended_buy_price":  _intended_buy_price,
        "intended_sell_price": _intended_sell_price,
        "buy_slippage_pct":    _pos_buy_slippage,
        "sell_slippage_pct":   _sell_slippage_pct,
    }
    # ── Sell timing diagnostic ────────────────────────────────────────────────
    try:
        _t_done = time.time()
        pos["_sell_complete_ts"] = _t_done
        _trigger  = pos.get("_sell_trigger_ts", 0)
        _pickup   = pos.get("_sell_picked_up_ts", 0)
        _gate_s   = pos.get("_sell_gate_start_ts", 0)
        _gate_e   = pos.get("_sell_gate_done_ts", 0)
        _bin_s    = pos.get("_sell_binance_start_ts", 0)
        _bin_e    = pos.get("_sell_binance_done_ts", _t_done)
        _crossed = pos.get("_sell_target_crossed_ts", 0)
        if _trigger > 0:
            stages = {
                "target_crossed_to_trigger_ms": (_trigger - _crossed) * 1000 if _crossed and _crossed <= _trigger else 0,
                "trigger_to_pickup_ms": (_pickup - _trigger) * 1000 if _pickup else 0,
                "pickup_to_gate_ms":    (_gate_s - _pickup) * 1000 if _gate_s and _pickup else 0,
                "gate_ms":              (_gate_e - _gate_s) * 1000 if _gate_e and _gate_s else 0,
                "gate_to_binance_ms":   (_bin_s - _gate_e) * 1000 if _bin_s and _gate_e else 0,
                "binance_ms":           (_bin_e - _bin_s) * 1000 if _bin_e and _bin_s else 0,
                "total_ms":             (_t_done - _trigger) * 1000,
            }
            detail = " | ".join(f"{k}={v:.0f}" for k, v in stages.items() if v >= 1)
            if stages["total_ms"] > 2000:
                log_diag_issue("sell_timing", "warn",
                    f"{sym} sell slow: {stages['total_ms']:.0f}ms total ({reason})", detail=detail)
            else:
                log_diag_issue("sell_timing", "info",
                    f"{sym} sold in {stages['total_ms']:.0f}ms ({reason})", detail=detail)
    except Exception:
        pass

    # ── Sell latency for DB ───────────────────────────────────────────────────
    try:
        _fill_ts = time.time()
        _target_crossed_ts_val = pos.get("_sell_target_crossed_ts")
        _trigger_ts_val        = pos.get("_sell_trigger_ts")

        _target_to_trigger_ms: Optional[int] = None
        _trigger_to_filled_ms: Optional[int] = None
        _target_crossed_iso:   Optional[str] = None

        if _target_crossed_ts_val and _trigger_ts_val:
            _target_to_trigger_ms = int((_trigger_ts_val - _target_crossed_ts_val) * 1000)
        if _trigger_ts_val:
            _trigger_to_filled_ms = int((_fill_ts - _trigger_ts_val) * 1000)
        if _target_crossed_ts_val:
            from datetime import datetime as _dt2, timezone as _tz2
            _target_crossed_iso = _dt2.fromtimestamp(_target_crossed_ts_val, tz=_tz2.utc).isoformat()
    except Exception:
        _target_to_trigger_ms = None
        _trigger_to_filled_ms = None
        _target_crossed_iso   = None

    # Write the closed-trade record BEFORE deleting the position row — a crash
    # or DB error between the two must never lose the trade record. A leftover
    # position row is recoverable (the next sell attempt gets -2010 and the
    # ghost check cleans it up); a missing trade row is lost P&L history.
    try:
        database.log_trade(trade_record,
                           target_crossed_to_trigger_ms=_target_to_trigger_ms,
                           trigger_to_filled_ms=_trigger_to_filled_ms,
                           target_crossed_ts=_target_crossed_iso)
    except Exception as te:
        database.log_activity(f"log_trade error ({sym}): {te}", "warn")

    # Remove position from memory and DB — sell is confirmed on Binance and the
    # trade record is already written. Supabase sync and learning run after so
    # a slow network never delays cleanup.
    if pos.get("id"):
        try:
            database.delete_position(pos["id"])
        except Exception:
            pass
    with _positions_lock:
        _positions[:] = [p for p in _positions if p.get("id") != pos.get("id")]
    _rebuild_pos_index()
    _pos_peaks.pop(sym, None)

    try:
        import supabase_sync
        # Paper mode: read balance from memory (instant, no network call).
        # Live mode: skip the get_account() REST call — Supabase sync gets
        # an estimate from net_profit instead, avoiding a 10s blocking call
        # on the sell worker thread.
        _pc_sync = _client()
        if get_mode() != "live" and hasattr(_pc_sync, "_balances"):
            with _pc_sync._lock:
                usdt_now = float(_pc_sync._balances.get("USDT", 0.0))
        else:
            usdt_now = _get_usdt_balance()
        supabase_sync.sync_sell_result_sync(trade_record, sym, usdt_now)
    except Exception as se:
        err_msg = f"Supabase sync error after selling {sym}: {se}"
        print(f"[TradeEngine] {err_msg}")
        try:
            database.log_activity(err_msg, "error")
        except Exception:
            pass
    try:
        learning.learn_from_trade(trade_record)
    except Exception as le:
        database.log_activity(f"learn_from_trade error ({sym}): {le}", "warn")

    pnl_sign = "+" if net_profit >= 0 else ""
    mode_tag = "LIVE" if mode == "live" else "PAPER"
    sell_msg = (
        f"[{mode_tag}] SOLD {sym} @ ${fill_price:.4f} "
        f"· received {usdt_returned:.4f} USDT · P&L: {pnl_sign}{net_profit:.4f} USDT"
        f"  ({reason}, held {duration}s)"
    )
    print(f"[{sell_ts}] {sell_msg}")
    try:
        database.log_activity(sell_msg, "info")
    except Exception:
        pass


# ── Real-time buy check from signal cache (called from realtime_monitor) ──────

def _check_buys_from_cache(prices: Dict[str, float]):
    """
    Real-time buy executor. Reads the pre-computed signal cache — zero I/O per
    call except when a buy is actually about to fire. Throttled to at most once
    every 3 s to avoid hammering get_account() on every WebSocket tick.
    """
    global _last_buy_check, _buys_this_scan, _last_buy_ts
    _buys_this_scan = 0  # reset per-scan counter each invocation

    # Load strategy once up front (_load_strategy is mtime-cached — cheap).
    strategy = _load_strategy()
    _pre_min_sigs = int(strategy.get("min_signals", config.MIN_SIGNALS_TO_BUY))
    # Engine-aware pre-gate: when the signal engine is enabled the legacy
    # 6-signal score is NOT the buy criterion, so the any_ready shortcut must
    # not gate the engine's own per-coin evaluation.
    _pre_engine_active = (
        SIGNAL_REGISTRY_AVAILABLE
        and bool(strategy.get("signal_engine", {}).get("enabled", False))
    )

    # Paused bot: check BEFORE the any_ready pre-gate — previously this warn
    # sat below the pre-gate and was unreachable unless a coin coincidentally
    # passed the legacy score, so a silently-paused bot logged nothing.
    if not strategy.get("trading_active", True):
        global _last_paused_log
        _now_p = time.time()
        if _now_p - _last_paused_log >= 60.0:
            _last_paused_log = _now_p
            database.log_activity("Buy check: trading_active=False — bot is paused", "warn")
        return

    # Fast pre-check: any coin signalling BUY? (no lock needed for scalar read)
    # Use the runtime min_signals setting — the hardcoded config default would
    # silently defeat a user threshold set below it. Legacy path only: with the
    # signal engine active we proceed to the per-coin loop and let it decide.
    with _signal_cache_lock:
        any_ready = any(v["score"] >= _pre_min_sigs for v in _signal_cache.values())
        cache_size = len(_signal_cache)

    if cache_size == 0 or (not any_ready and not _pre_engine_active):
        global _last_no_signal_log
        now_ns = time.time()
        if now_ns - _last_no_signal_log >= 60.0:
            _last_no_signal_log = now_ns
            if cache_size == 0:
                import data_collector as _diag_dc
                n_ticks   = sum(1 for v in _diag_dc.price_ticks.values()   if len(v) >= _diag_dc._MIN_TICKS)
                n_samples = sum(1 for v in _diag_dc.price_samples.values() if len(v) >= _diag_dc._MIN_SAMPLES)
                n_ws      = sum(1 for v in _diag_dc.ws_candles.values()    if len(v) >= _diag_dc._MIN_CANDLES)
                database.log_activity(
                    f"Signal cache empty — data sources: ticks={n_ticks}, samples={n_samples}, ws_candles={n_ws}", "info"
                )
                _record_rejection("(all)", 0, "signal_cache_empty",
                                  f"ticks={n_ticks} samples={n_samples} ws_candles={n_ws}")
            else:
                with _signal_cache_lock:
                    snap = dict(_signal_cache)
                top = sorted(snap.items(), key=lambda x: -x[1]["score"])[:5]
                detail = " | ".join(
                    f"{s}:score={v['score']} RSI={v['signals'].get('rsi',False)} trend={v['signals'].get('trend',False)}"
                    for s, v in top
                )
                database.log_activity(
                    f"Buy check: {cache_size} coins, none at score≥{_pre_min_sigs} — top5: {detail}", "info"
                )
                if top:
                    _record_rejection(top[0][0], top[0][1].get("score", 0),
                                      "pre_gate_below_min_signals",
                                      f"none of {cache_size} coins at score>={_pre_min_sigs}")
        return

    # Throttle: don't call get_account() faster than every 1 s
    now = time.time()
    if now - _last_buy_check < 0.1:
        return
    _last_buy_check = now

    # Always refresh risk params first — even when at capacity, so the sell monitor
    # uses current TP/SL settings rather than stale cached values.
    _refresh_risk_params()

    # Enforce max_positions (configurable via /api/settings, default 10)
    max_pos = int(strategy.get("max_positions", 10))
    with _positions_lock:
        n_open = len(_positions)
    if n_open >= max_pos:
        # Periodic heartbeat so the user sees the bot is still alive at capacity.
        # Without this, no buy logs are emitted while at max_positions and the
        # UI looks like it has been paused for no reason.
        global _last_at_capacity_log
        _now_cap = time.time()
        if _now_cap - _last_at_capacity_log >= 60.0:
            _last_at_capacity_log = _now_cap
            database.log_activity(
                f"At max positions ({n_open}/{max_pos}) — bot active, "
                f"waiting for an exit before opening new trades",
                "info",
            )
        return  # already at capacity — buys resume automatically after a sell

    # Enforce configurable min_signals threshold (overrides config.MIN_SIGNALS_TO_BUY)
    min_sigs = int(strategy.get("min_signals", config.MIN_SIGNALS_TO_BUY))

    # Phase 2+3: new signal engine — active only when strategy["signal_engine"]["enabled"]=True
    signal_engine_active = (
        SIGNAL_REGISTRY_AVAILABLE
        and bool(strategy.get("signal_engine", {}).get("enabled", False))
    )

    approved = {
        c["symbol"]: c
        for c in strategy.get("approved_coins", [])
        if c.get("approved")
    }
    if not approved:
        database.log_activity("Buy check: no approved coins in strategy.json", "warn")
        return

    from datetime import timezone as _tz
    mode = get_mode()

    # HARD BLOCK: live mode running on the paper fallback client must never buy —
    # the fills would be simulated but recorded as mode='live'. Sells/monitoring
    # continue via the direct transport; buys resume when reconnect succeeds.
    if mode == "live" and connection.is_using_paper_fallback():
        global _last_fallback_block_log
        _now_fb = time.time()
        if _now_fb - _last_fallback_block_log >= 60.0:
            _last_fallback_block_log = _now_fb
            database.log_activity(
                "Buy check: live mode is in paper fallback (Binance client unavailable) "
                "— all buys blocked until the live connection is restored", "warn"
            )
        return

    usdt_balance = _get_usdt_balance()
    ts_now = datetime.now(_tz.utc).isoformat()

    # Build the cache snapshot but only emit the activity log every 30 s — these
    # writes hold the global DB lock and previously fired on every WS-tick scan,
    # serializing the sell monitor and signal scanner behind them.
    with _signal_cache_lock:
        cache_snapshot = dict(_signal_cache)
    ready_syms = [s for s, v in cache_snapshot.items() if v["score"] >= min_sigs and s in approved]
    global _last_buy_scan_log
    _now_ts = time.time()
    if ready_syms or (_now_ts - _last_buy_scan_log) >= 30.0:
        _last_buy_scan_log = _now_ts
        database.log_activity(
            f"Buy scan: USDT={usdt_balance:.2f} | {len(ready_syms)} coin(s) ready (min {min_sigs}/6 signals): "
            + (", ".join(f"{s}(score={cache_snapshot[s]['score']})" for s in ready_syms[:6]) or "none"),
            "info"
        )

    # ── BTC macro gate — checked once per scan, not per symbol ────────────────
    macro_gate_enabled = bool(strategy.get("macro_gate_enabled", True))
    if macro_gate_enabled:
        _btc = get_btc_state()
        if _btc and _btc.get("regime") == "bearish":
            database.log_activity(
                f"MACRO GATE: BTC bearish (24h={_btc['pct_24h']}% 4h={_btc['pct_4h']}%) — all buys paused",
                "warn"
            )
            for _s, _c in cache_snapshot.items():
                if _s in approved:
                    _record_rejection(_s, _c.get("score", 0), "btc_bearish_regime",
                                      f"pct_24h={_btc['pct_24h']} pct_4h={_btc['pct_4h']}")
            return

    # Read mandatory signal thresholds once (hot-reloadable from strategy.json).
    # Single source of truth for the RSI buy threshold: the nested
    # signal_thresholds key (what the UI writes and the engine's M1 reads)
    # first, then the legacy root key, then 40 — the old root-only read left
    # the legacy gate and the engine enforcing two different thresholds.
    mandatory_enabled = bool(strategy.get("mandatory_signals_enabled", True))
    rsi_threshold     = float(
        strategy.get("signal_thresholds", {}).get(
            "rsi_buy_threshold", strategy.get("rsi_buy_threshold", 40.0)
        )
    )

    for sym, cached in cache_snapshot.items():
        # Re-check capacity before every individual buy — the pre-loop check only
        # guards the entry; without this, all ready coins buy in sequence and blow
        # past the configured max_positions limit.
        with _positions_lock:
            if len(_positions) >= max_pos:
                break

        if sym not in approved:
            continue
        if not signal_engine_active and cached["score"] < min_sigs:
            _record_rejection(sym, cached["score"], "below_min_signals", f"needed {min_sigs}, got {cached['score']}")
            continue
        if _in_cooldown(sym):
            _record_rejection(sym, cached["score"], "cooldown")
            database.log_activity(f"{sym}: buy skipped — in cooldown", "info")
            continue
        if _loss_cooldown.get(sym, 0) > time.time():
            rem = int(_loss_cooldown[sym] - time.time())
            _record_rejection(sym, cached["score"], "loss_cooldown", f"{rem}s remaining")
            database.log_activity(f"{sym}: buy skipped — post-slippage-loss cooldown ({rem}s remaining)", "info")
            continue

        with _positions_lock:
            already_held = any(p["symbol"] == sym for p in _positions)

        if already_held:
            _record_rejection(sym, cached["score"], "already_held")
            continue

        # ── Extract signal snapshot for this symbol ────────────────────────────
        sigs    = cached.get("signals", {})
        bb_ok   = cached.get("bb_ok",  True)
        five_ok = cached.get("5m_ok",  True)
        score   = cached["score"]
        rsi_v   = cached.get("rsi_val", 0.0)
        sig_str = (
            f"EMA{'↑' if sigs.get('trend')  else '↓'} "
            f"RSI:{rsi_v:.0f} "
            f"MACD{'+'  if sigs.get('macd')   else '-'} "
            f"Vol{'✓'   if sigs.get('volume') else '✗'} "
            f"OBV{'✓'   if sigs.get('obv')    else '✗'} "
            f"ATR{'✓'   if sigs.get('atr')    else '✗'}"
        )
        bb_str  = "BB:PASS" if bb_ok   else "BB:FAIL"
        m5_str  = "5m:PASS" if five_ok else "5m:FAIL"

        # ── Buy decision: new signal engine OR legacy mandatory/score path ───────
        _buy_decision: dict = {}  # populated below for signal_snapshot capture
        if signal_engine_active:
            _sig_data = {
                **sigs,
                "rsi_value":      rsi_v,
                "current_price":  cached.get("price", 0.0),
                "low_24h":        cached.get("low_24h"),
                "klines_1m":      cached.get("klines_1m", []),
                "stoch_rsi_value": cached.get("stoch_rsi_val"),
            }
            _dec = _sr_evaluate_buy_decision(sym, _sig_data, strategy)
            _buy_decision = _dec
            if not _dec["allowed"]:
                # Record the ENGINE score — the legacy score is irrelevant on
                # this path and previously hid these rejections behind the
                # (now removed) score<3 filter.
                _record_rejection(sym, _dec.get("score", score), _dec["reason"],
                                  f"score={_dec['score']} fired={_dec['fired_signals']}")
                continue
            # Passed new engine — fall through to existing veto checks below
        else:
            # ── Legacy mandatory signal layer ──────────────────────────────────
            # Mandatory 1: EMA trend up (EMA9 > EMA21) — don't buy into a downtrend.
            # Mandatory 2: RSI below threshold — require a real dip, not mid-channel noise.
            if mandatory_enabled:
                if not sigs.get("trend", False):
                    _record_rejection(sym, score, "mandatory_ema_down", "EMA9 < EMA21")
                    continue
                if rsi_v <= 0 or rsi_v >= rsi_threshold:
                    _record_rejection(sym, score, "mandatory_rsi_too_high",
                                      f"rsi={rsi_v:.1f} threshold={rsi_threshold}")
                    continue

        # ── Hard veto checks: BB position and 5m trend ────────────────────────
        if not bb_ok:
            _record_rejection(sym, score, "bb_upper", sig_str)
            database.log_activity(
                f"[SKIP] {sym}: price near upper Bollinger Band | "
                f"{sig_str} | {bb_str} | SKIP(upper band)", "info"
            )
            continue
        if not five_ok:
            _record_rejection(sym, score, "5m_downtrend", sig_str)
            database.log_activity(
                f"[SKIP] {sym}: 5m downtrend | "
                f"{sig_str} | {bb_str} {m5_str} | SKIP(5m downtrend)", "info"
            )
            continue

        # ── 1m BB veto: skip when price is at or above the 1m upper band ──────
        # The 5m BB check (bb_ok above) catches medium-term tops; this catches
        # local 1m tops that the 5m hasn't reflected yet (e.g. INJUSDT case).
        try:
            candles_1m = database.get_candles(sym, config.CANDLE_TIMEFRAME, limit=1)
            if candles_1m:
                bb_pos_1m = candles_1m[-1].get("bb_position")
                if bb_pos_1m in ("above_upper", "at_upper"):
                    _record_rejection(sym, score, "bb_upper", f"1m {bb_pos_1m}")
                    database.log_activity(
                        f"[SKIP] {sym}: 1m BB {bb_pos_1m} — local top, wait for pullback | "
                        f"{sig_str} | SKIP(1m_top)", "info"
                    )
                    continue
        except Exception:
            pass

        # ── Stagger gate — max _MAX_BUYS_PER_SCAN buys per cycle ──────────────
        if _buys_this_scan >= _MAX_BUYS_PER_SCAN:
            database.log_activity(
                f"Buy scan: capped at {_MAX_BUYS_PER_SCAN} buys this cycle — "
                f"remaining coins evaluated next cycle", "info"
            )
            break
        if time.time() - _last_buy_ts < _BUY_STAGGER_SEC:
            continue  # this slot too soon — try next coin in case it's been longer

        # ── Falling knife filter ───────────────────────────────────────────────
        # Block buys when price has dropped >0.4% in the last 3 minutes of samples.
        # Prevents buying mid-crash where "RSI oversold" triggers but momentum is
        # still strongly negative.
        try:
            import data_collector as _dc_fk
            recent_closes = list(_dc_fk.price_samples.get(sym, []))
            if len(recent_closes) >= 180:
                price_now     = recent_closes[-1]
                price_3min    = recent_closes[-180]
                pct_3min      = (price_now - price_3min) / price_3min * 100 if price_3min > 0 else 0
                if pct_3min < -0.4:
                    _record_rejection(sym, score, "falling_knife", f"{pct_3min:.2f}% in 3min")
                    database.log_activity(
                        f"[SKIP] {sym}: falling knife — down {pct_3min:.2f}% in 3min | "
                        f"{sig_str} | SKIP(downward momentum)", "info"
                    )
                    continue
        except Exception:
            pass

        # ── Trend health gate ──────────────────────────────────────────────────
        # Block buys when price is below MA20 AND RSI is not deeply oversold (<35).
        # Also block when volume trend is decreasing (no buying pressure).
        try:
            _th_candles = database.get_candles(sym, config.CANDLE_TIMEFRAME, limit=1)
            if _th_candles:
                _last_c = _th_candles[-1]
                _ma_pos  = _last_c.get("ma_position") or _derive_ma_pos(price, _last_c.get("ma20"))
                _vol_tr  = _last_c.get("volume_trend")
                if _ma_pos == "below" and rsi_v > 35:
                    _record_rejection(sym, score, "trend_health", f"below MA20 RSI={rsi_v:.0f}")
                    database.log_activity(
                        f"[SKIP] {sym}: below MA20 with RSI {rsi_v:.0f} (not oversold) | "
                        f"{sig_str} | SKIP(downtrend)", "info"
                    )
                    continue
                if _vol_tr == "decreasing":
                    _record_rejection(sym, score, "volume_decreasing", f"vol_trend={_vol_tr}")
                    database.log_activity(
                        f"[SKIP] {sym}: volume decreasing — no buying pressure | "
                        f"{sig_str} | SKIP(weak volume)", "info"
                    )
                    continue
        except Exception:
            pass

        # ── Shadow evaluation — compare old vs new signal engine (Phase 1) ────────
        # Runs AFTER all veto/gate checks so it sees the same yes/no that the old
        # engine sees at the moment a buy would fire.  Logs any disagreement but
        # does NOT alter the buy decision — the old code path remains authoritative.
        if SIGNAL_REGISTRY_AVAILABLE and bool(strategy.get("shadow_evaluate_signals", True)):
            try:
                _shadow_data = dict(sigs)
                _shadow_data["rsi_value"] = rsi_v
                _new_dec = _sr_evaluate_buy_decision(sym, _shadow_data, strategy)
                _old_allowed = True  # reached this point → old engine approved
                if _old_allowed != _new_dec["allowed"]:
                    # B Step 5: deduped per (symbol, reason) — see _log_shadow_dedup
                    _log_shadow_dedup(
                        sym, str(_new_dec["reason"]),
                        f"{sym}: old=True new={_new_dec['allowed']} reason={_new_dec['reason']}",
                    )
            except Exception as _shadow_exc:
                _log_shadow_dedup(
                    sym, f"shadow_eval_failed:{type(_shadow_exc).__name__}",
                    f"Shadow eval failed for {sym}: {_shadow_exc}",
                )

        budget = get_budget_for_coin(sym, usdt_balance)
        if budget <= 0:
            _record_rejection(sym, score, "no_capital", f"usdt={usdt_balance:.2f}")
            database.log_activity(f"{sym}: buy skipped — budget=0 (mode={mode}, usdt={usdt_balance:.2f})", "warn")
            continue

        price = prices.get(sym) or cached["price"]
        if not price:
            _record_rejection(sym, score, "no_price")
            database.log_activity(f"{sym}: buy skipped — no price available", "warn")
            continue

        # Lot-step rounding guard: skip if rounding would waste >1% of capital.
        # Catches BTC/ETH-tier coins where a small budget gets floored to far fewer coins
        # than expected, creating a trapped position that needs a 5%+ move to break even.
        _ideal_qty_pre = budget / price
        _actual_qty_pre = _floor_qty(_ideal_qty_pre, sym)
        if _actual_qty_pre <= 0:
            _record_rejection(sym, score, "lot_step_loss", f"budget=${budget:.2f} price=${price:.4f} qty=0")
            database.log_activity(
                f"[SKIP] {sym}: lot-step too large for ${budget:.2f} at ${price:.4f} — "
                f"would receive 0 qty. Increase budget or remove this coin.", "warn"
            )
            continue
        _qty_loss_pct = (_ideal_qty_pre - _actual_qty_pre) / _ideal_qty_pre * 100
        if _qty_loss_pct > 1.0:
            _record_rejection(sym, score, "lot_step_loss", f"{_qty_loss_pct:.2f}% waste budget=${budget:.2f} price=${price:.4f}")
            database.log_activity(
                f"[SKIP] {sym}: lot-step rounding wastes {_qty_loss_pct:.2f}% of capital "
                f"(ideal={_ideal_qty_pre:.8f}, actual={_actual_qty_pre:.8f}, "
                f"step={_lot_step_cache.get(sym, '?')}). "
                f"Price ${price:.4f} too high for ${budget:.2f} budget — skipping.",
                "warn"
            )
            continue

        _client().update_price(sym, price)

        buy_cfg = {**approved[sym], "symbol": sym, "budget_usdt": budget}
        allowed, reason = can_execute_buy(buy_cfg, _client())
        if not allowed:
            database.log_activity(f"{sym}: buy skipped — {reason}", "info")
            continue

        if sym in _bad_symbols:
            database.log_activity(f"{sym}: buy skipped — market closed/delisted (blacklisted this session)", "warn")
            continue

        # Binance minimum notional is $10 for most spot pairs — reject early
        # so we don't waste an API call and get a cryptic -1013 error.
        if mode == "live" and budget < 10.0:
            _record_rejection(sym, score, "min_notional",
                              f"budget=${budget:.2f} < $10 Binance minimum")
            database.log_activity(
                f"{sym}: buy skipped — budget ${budget:.2f} < $10 Binance minimum notional "
                f"(increase trade size in Settings)", "warn"
            )
            continue

        # ── Atomic claim — BEFORE the slow REST gates below ─────────────────────
        # The fresh re-check + reversal confirmation take multiple seconds. If the
        # claim happened after them (as it used to), a concurrent scan could buy
        # this symbol inside that window, release the claim, and this thread would
        # then claim unopposed and buy it AGAIN (confirmed duplicate DOT/IMX rows).
        # Claim first; release on every failure path.
        with _buying_lock:
            _now_b = time.time()
            _stale_b = [s for s, ts in _buying_ts.items() if (_now_b - ts) > _BUYING_TIMEOUT_SEC]
            for _s in _stale_b:
                _buying.discard(_s)
                _buying_ts.pop(_s, None)
            if sym in _buying:
                database.log_activity(f"{sym}: buy skipped — concurrent buy already in flight", "info")
                _record_rejection(sym, score, "in_progress_buy")
                continue
            _buying.add(sym)
            _buying_ts[sym] = _now_b
            _other_buying = len(_buying) - 1  # in-flight claims on OTHER symbols

        def _release_buy_claim(_s=sym):
            with _buying_lock:
                _buying.discard(_s)
                _buying_ts.pop(_s, None)

        # Re-verify under the claim: another thread may have bought this symbol
        # or filled the last slot while this scan was mid-loop. Counting other
        # in-flight claims as reserved slots keeps concurrent scans from
        # collectively exceeding max_positions.
        with _positions_lock:
            _held_now = any(p["symbol"] == sym for p in _positions)
            _at_capacity_now = (len(_positions) + _other_buying) >= max_pos
        if _held_now:
            _release_buy_claim()
            _record_rejection(sym, score, "already_held")
            continue
        if _at_capacity_now:
            _release_buy_claim()
            break

        # ── Fresh signal re-check before committing ────────────────────────────
        # Cache can be 30-60s stale. Re-verify at execution time using fresh
        # candles (REST multi-base → WS buffer → DB, 50 klines for MACD parity
        # with the scan). When the signal engine approved this buy, re-verify
        # with the SAME engine — not the legacy min_signals score, which tests
        # the opposite (momentum-up) hypothesis of an engine dip entry. When
        # no data source is available, proceed on the cached decision (the
        # scanner refreshed it ≤60s ago) — data unavailability alone must
        # never fail-closed and block every buy.
        try:
            _fresh_candles = _fetch_fresh_1m_candles(sym)
            if not _fresh_candles:
                global _last_fresh_nodata_log
                _now_fr = time.time()
                if _now_fr - _last_fresh_nodata_log >= 60.0:
                    _last_fresh_nodata_log = _now_fr
                    database.log_activity(
                        f"{sym}: pre-buy fresh re-check has no data source (REST/WS/DB) — "
                        f"proceeding on cached signals "
                        f"(age={round(time.time() - cached.get('ts', 0), 1)}s)", "warn"
                    )
            else:
                _fresh_closes = [c["close"] for c in _fresh_candles]
                _fresh_sigs   = evaluate_signals(_fresh_candles)
                if signal_engine_active:
                    # Re-run the engine that approved the buy on the fresh data.
                    _fresh_rsi_list = indicators.calc_rsi(_fresh_closes, 14)
                    _fresh_rsi = _fresh_rsi_list[-2] if _fresh_rsi_list[-2] is not None else 0.0
                    try:
                        _fresh_stoch = indicators.calc_stoch_rsi(_fresh_closes)
                    except Exception:
                        _fresh_stoch = cached.get("stoch_rsi_val")
                    _fresh_data = {
                        **_fresh_sigs,
                        "rsi_value":       _fresh_rsi,
                        "current_price":   _fresh_closes[-1],
                        "low_24h":         cached.get("low_24h"),
                        "klines_1m":       _fresh_candles[-15:],
                        "stoch_rsi_value": _fresh_stoch,
                    }
                    _fresh_dec = _sr_evaluate_buy_decision(sym, _fresh_data, strategy)
                    if not _fresh_dec["allowed"]:
                        cache_age = round(time.time() - cached.get("ts", 0), 1)
                        database.log_activity(
                            f"[SKIP] {sym}: fresh engine re-check FAILED — "
                            f"{_fresh_dec['reason']} (cache age={cache_age}s)", "warn"
                        )
                        _record_rejection(sym, _fresh_dec.get("score", score), "stale_signals",
                                          f"fresh_engine={_fresh_dec['reason']} age={cache_age}s")
                        _release_buy_claim()
                        continue
                else:
                    _fresh_score = sum(_fresh_sigs.values())
                    if _fresh_score < min_sigs:
                        cache_age = round(time.time() - cached.get("ts", 0), 1)
                        database.log_activity(
                            f"[SKIP] {sym}: fresh re-check FAILED — score {_fresh_score}/6 < {min_sigs} "
                            f"(cache had {score}/6, age={cache_age}s)", "warn"
                        )
                        _record_rejection(sym, score, "stale_signals",
                                          f"fresh={_fresh_score} cache={score} age={cache_age}s")
                        _release_buy_claim()
                        continue
                _live_price = _fresh_closes[-1]
                if price > 0 and abs(_live_price - price) / price > 0.005:
                    database.log_activity(
                        f"[SKIP] {sym}: price moved {(_live_price - price)/price*100:.2f}% "
                        f"since cache (${price:.4f} → ${_live_price:.4f}) — skipping", "warn"
                    )
                    _release_buy_claim()
                    continue
                price = _live_price
                _client().update_price(sym, price)
        except Exception as _fresh_e:
            # Unexpected error (not data unavailability — that path never
            # raises). Proceed on the cached decision rather than reinstating
            # the old fail-closed skip that killed every buy.
            database.log_activity(
                f"{sym}: fresh re-check errored ({_fresh_e}) — proceeding on cached signals", "warn"
            )

        # ── Reversal confirmation ──────────────────────────────────────────────
        if bool(strategy.get("reversal_confirmation_enabled", True)):
            _rev_ok, _rev_reason = is_reversal_confirmed(sym)
            if not _rev_ok:
                _record_rejection(sym, score, "no_reversal_confirmed", _rev_reason)
                _release_buy_claim()
                continue

        try:
            _log_order_intent("BUY", sym, budget / max(price, 1e-12), price)
            # Live mode: geo-block-safe direct transport; raises in paper fallback.
            result = _market_buy(sym, budget)
            _log_order_result("BUY", sym, budget / max(price, 1e-12), price, result)
        except Exception as e:
            _release_buy_claim()
            err_str = str(e)
            print(f"[RealtimeBuy] BUY failed {sym}: {e}")
            database.log_activity(f"{sym}: BUY failed — {e}", "error")
            if "-1013" in err_str or "Market is closed" in err_str:
                _bad_symbols.add(sym)
                database.log_activity(f"{sym}: blacklisted — market closed/delisted on Binance", "warn")
            continue

        buy_fills  = result.get("fills", [])
        _exec_qty  = float(result.get("executedQty", 0) or 0)
        _cum_quote = float(result.get("cummulativeQuoteQty", 0) or 0)
        # Entry price = VWAP across ALL fills (cummulativeQuoteQty / executedQty).
        # fills[0] is only the best-priced tranche — using it understates the true
        # average cost on multi-fill market buys and corrupts breakeven math.
        if _exec_qty > 0 and _cum_quote > 0:
            fill_price = _cum_quote / _exec_qty
        elif buy_fills:
            fill_price = float(buy_fills[0].get("price", price))
        else:
            fill_price = price
        # Subtract base-asset commission from executedQty: without the BNB fee
        # discount Binance takes the fee in the bought coin, so the wallet is
        # credited executedQty − Σcommission. Recording the gross qty causes
        # -2010 "insufficient balance" on the eventual sell.
        _base_asset = sym[:-4] if sym.endswith("USDT") else sym
        _base_commission = sum(
            float(f.get("commission") or 0) for f in buy_fills
            if f.get("commissionAsset") == _base_asset
        )
        qty = max(0.0, _exec_qty - _base_commission)
        if qty <= 0:
            _release_buy_claim()
            continue

        # Compute actual buy fee in USDT across all fills.
        # In live BNB-fee mode, commission is in BNB — convert using live price.
        # Stored on the position so _do_execute_sell can use the real value.
        if mode == "live":
            buy_fee_usdt, _ = _fills_fee_usdt(buy_fills, budget * _fee_rate)
        else:
            buy_fee_usdt = budget * _fee_rate

        # Gather entry indicators from latest DB candle
        entry_rsi = entry_ma = entry_bb = entry_vol = None
        try:
            candles = database.get_candles(sym, config.CANDLE_TIMEFRAME, limit=1)
            if candles:
                last      = candles[-1]
                entry_rsi = last.get("rsi14")
                entry_ma  = last.get("ma_position") or _derive_ma_pos(fill_price, last.get("ma20"))
                entry_bb  = last.get("bb_position") or _derive_bb_pos(fill_price, last)
                entry_vol = last.get("volume_trend")
        except Exception:
            pass

        _bep_mult_buy = _get_breakeven_mult(fill_price, sym)
        if _take_profit_enabled:
            _eff_tp_mult = max(_bep_mult_buy, _user_tp_mult)
        else:
            _eff_tp_mult = _bep_mult_buy
        exit_target = round(fill_price * _eff_tp_mult, 8)
        _pos_peaks.pop(sym, None)
        _buy_slippage_pct = ((fill_price - price) / price * 100) if price > 0 else None
        pos_record = {
            "symbol":             sym,
            "entry_price":        fill_price,
            # B Step 4 — origin tag: this buy was decided by the automated
            # scanner (vs. a user-initiated/manual buy). Lives in the in-memory
            # position dict + activity log only: the positions and trades
            # tables are fixed-column INSERTs (database.save_position /
            # database.log_trade), so persisting it would need a schema
            # migration. save_position ignores unknown keys — safe to carry.
            "origin":             "auto",
            "exit_target":        exit_target,
            "breakeven_mult_at_buy": round(_bep_mult_buy, 8),
            "quantity":           qty,
            "budget_usdt":        budget,
            "buy_fee_usdt":       buy_fee_usdt,
            "opened_at_ts":       time.time(),  # for minimum-hold-time guard
            "timestamp":          ts_now,
            "mode":               mode,
            "entry_rsi":          entry_rsi,
            "entry_ma_position":  entry_ma,
            "entry_bb_position":  entry_bb,
            "entry_volume_trend": entry_vol,
            "intended_buy_price": price,
            "buy_slippage_pct":   _buy_slippage_pct,
            "buy_signals_snapshot": {
                "score":         score,
                "trend":         sigs.get("trend"),
                "rsi":           sigs.get("rsi"),
                "rsi_value":     rsi_v,
                "macd":          sigs.get("macd"),
                "volume":        sigs.get("volume"),
                "obv":           sigs.get("obv"),
                "atr":           sigs.get("atr"),
                "bb_ok":         bb_ok,
                "5m_ok":         five_ok,
                "cache_age_sec": round(time.time() - cached.get("ts", 0), 1),
            },
            "signal_snapshot": _json.dumps({
                "fired_signals":   _buy_decision.get("fired_signals", []),
                "score":           _buy_decision.get("score", 0),
                "mandatory_pass":  all(fired for _, fired in _buy_decision.get("mandatory_results", [])),
                "engine_enabled":  bool(strategy.get("signal_engine", {}).get("enabled", False)),
            }),
        }
        # Update stagger tracking
        _last_buy_ts = time.time()
        _buys_this_scan += 1
        pos_id = database.save_position(pos_record)
        pos_record["id"] = pos_id

        with _positions_lock:
            _positions.append(pos_record)
        _rebuild_pos_index()

        try:
            import supabase_sync
            # Parallel sync to Supabase — both calls run concurrently, max 4 s total.
            supabase_sync.sync_buy_result_sync(
                pos_record, usdt_balance - budget - buy_fee_usdt
            )
        except Exception as _be:
            try:
                database.log_activity(f"Supabase sync error after buying {sym}: {_be}", "error")
            except Exception:
                pass

        usdt_balance -= budget + buy_fee_usdt

        mode_tag  = "LIVE" if mode == "live" else "PAPER"
        cache_age = round(time.time() - cached.get("ts", 0), 1)
        _btc_now  = get_btc_state()
        _regime_tag = _btc_now.get("regime", "?") if _btc_now else "?"
        _rsi_thr_tag = f"<{rsi_threshold:.0f}" if mandatory_enabled else "any"
        msg = (
            f"[{mode_tag}] BOUGHT {sym} @ ${fill_price:.4f} "
            f"| MANDATORY[EMA↑ RSI:{rsi_v:.0f}{_rsi_thr_tag}] "
            f"| SCORED[MACD{'+'  if sigs.get('macd')   else '-'} "
            f"Vol{'✓'   if sigs.get('volume') else '✗'} "
            f"OBV{'✓'   if sigs.get('obv')    else '✗'} "
            f"ATR{'✓'   if sigs.get('atr')    else '✗'}] "
            f"| score:{score}/6 regime:{_regime_tag} | EXIT TARGET=${exit_target:.4f} "
            f"| qty={qty:.6f} | cache_age:{cache_age}s | origin:auto"
        )
        print(f"[RealtimeBuy] {msg}")
        database.log_activity(msg, "info")
        with _buying_lock:
            _buying.discard(sym)
            _buying_ts.pop(sym, None)


# ── Inline tick-driven signal refresh ────────────────────────────────────────────

def _inline_refresh_from_ticks(sym: str, price: float):
    """
    Recompute signals for sym using the best available price series — no I/O.
    Preference order: time-sampled prices (quality RSI) → raw ticks (last resort).
    """
    import data_collector as _dc

    # Prefer 30-second sampled prices — spans real time, gives meaningful RSI
    closes = list(_dc.price_samples.get(sym, []))
    if len(closes) >= _dc._MIN_SAMPLES:
        closes = closes[:]   # copy
        closes[-1] = price   # inject latest price
    else:
        # Fall back to raw ticks (available in seconds but RSI less reliable)
        closes = list(_dc.price_ticks.get(sym, []))
        if len(closes) < _dc._MIN_TICKS:
            return
        closes[-1] = price

    # Don't compute signals from fake volume — preserve existing signal entry
    # and only update price + RSI. Volume signal stays from last REST scan.
    import data_collector as _dc2
    with _signal_cache_lock:
        prev_entry = _signal_cache.get(sym)
    if prev_entry:
        with _signal_cache_lock:
            if sym in _signal_cache:
                _signal_cache[sym]["price"] = closes[-1]
                _signal_cache[sym]["ts"] = time.time()
        return
    vols = [1.0] * len(closes)
    update_coin_signals(sym, closes, vols)


# ── Process 1: realtime monitor (called on every WebSocket tick) ──────────────

def realtime_monitor(prices: Dict[str, float]):
    """
    Synchronous — called inside data_collector's async WebSocket loop on EVERY
    trade event (~100 ms per coin). MUST NOT do any blocking I/O.

    Sell exits and buy entries are dispatched to _sell_executor (non-blocking
    O(1) submit) so the event loop is never stalled.  The _sell_monitor_loop
    daemon thread acts as a 5-second fallback in case a tick is missed.
    """
    now = time.time()

    try:
        if _thread_health:
            _thread_health.heartbeat("realtime_monitor")
    except Exception:
        pass

    # ── Real-time sell check — fires within ~100 ms of price crossing threshold ──
    # Reads the pre-built symbol→position index (no lock needed for dict reads).
    pos_index = _pos_by_symbol   # local ref; atomically replaced on each mutation
    for sym, price in prices.items():
        pos = pos_index.get(sym)
        if pos is None or price <= 0:
            continue
        # Stamp freshness for held symbols — lets the pre-sell gate trust this
        # WS tick instead of re-quoting via REST (3s latency + veto risk).
        _last_ws_price_ts[sym] = now
        # Rate-limit retries: stop-loss retries immediately, take-profit waits 5s.
        last_fail = _sell_last_failed_ts.get(sym, 0)
        if last_fail:
            _cooldown = (_SELL_RETRY_COOLDOWN_LOSS
                         if _sell_last_failed_reason.get(sym, "") in ("stop-loss", "force-sell")
                         else _SELL_RETRY_COOLDOWN_PROFIT)
            if (now - last_fail) < _cooldown:
                continue
        # Minimum hold applies to STOP-LOSS only (prevents race-condition flips
        # right after the buy). Take-profit must never be delayed — an immediate
        # post-buy pump that reaches the target sells instantly.
        opened_ts = pos.get("opened_at_ts", 0)
        in_min_hold = opened_ts > 0 and (now - opened_ts) < _MIN_HOLD_SEC
        entry  = pos["entry_price"]
        # Real breakeven trigger: (budget + buy_fee + min_profit) / (qty × (1 - sell_fee)).
        # Only fires when a sell would actually net profit — no phantom triggers for
        # BTC/BNB where lot-step rounding raises the true break-even above simple BEP.
        # _profitable_sell_check in _do_execute_sell is the final safety net at execution.
        real_target = compute_real_breakeven_price(pos)
        if real_target <= 0:
            _bep_m = pos.get("breakeven_mult_at_buy") or _get_breakeven_mult(entry, sym)
            real_target = entry * _bep_m  # fallback for incomplete positions
        # Stop measured from ENTRY price — matches the frontend's "% below entry"
        # label. (Measuring from fee-inclusive BEP fired ~0.26% earlier than the
        # user's configured distance.)
        stop = entry * _stop_loss_mult
        # Take-profit trigger: the user's TP target, floored at real breakeven —
        # never fire at bare breakeven when the user asked for more.
        if _take_profit_enabled:
            target = max(real_target, entry * _user_tp_mult)
        else:
            target = real_target

        if price >= target:
            # Record first-seen crossing time — distinct from _sell_trigger_ts (which is
            # set when we actually call .submit()). The gap between these two timestamps
            # reveals event-loop starvation: if target_crossed_to_trigger_ms >> 0, sells
            # are being delayed by blocking work elsewhere on the event loop thread.
            if pos.get("_sell_target_crossed_ts") is None:
                pos["_sell_target_crossed_ts"] = now
            with _selling_lock:
                already = sym in _selling
            if not already:
                if _smart_hold_enabled and price >= target and target > real_target:
                    _pos_peaks[sym] = max(_pos_peaks.get(sym, target), price)
                    peak = _pos_peaks[sym]
                    # Trail is hard-floored at the exit target: smart-hold may ride
                    # the gain higher, but can never give back below the target.
                    trail_stop = max(peak * (1.0 - _trailing_stop_pct / 100.0), target)
                    sell_reason: Optional[str] = None
                    if price <= trail_stop:
                        sell_reason = "smart-hold-trail"
                    else:
                        with _signal_cache_lock:
                            _sc = _signal_cache.get(sym, {})
                            score = _sc.get("score", 0)
                            score_ts = _sc.get("ts", 0)
                        # A stale score must not hold a profitable position —
                        # only a FRESH bullish score may defer the sell.
                        if score < 3 or (now - score_ts) > 120:
                            sell_reason = "take-profit"
                    if sell_reason:
                        with _selling_lock:
                            if sym not in _selling:
                                _selling.add(sym)
                                _selling_ts[sym] = time.time()
                                pos["_sell_trigger_ts"] = time.time()
                                pos["_sell_reason"] = sell_reason
                                _sell_executor.submit(_execute_sell, pos, price, sell_reason)
                else:
                    with _selling_lock:
                        if sym not in _selling:
                            _selling.add(sym)
                            _selling_ts[sym] = time.time()
                            pos["_sell_trigger_ts"] = time.time()
                            pos["_sell_reason"] = "take-profit"
                            _sell_executor.submit(_execute_sell, pos, price, "take-profit")
        elif _stop_loss_mult < 1.0 and price <= stop and not in_min_hold:
            _stop_loss_confirmation[sym] = _stop_loss_confirmation.get(sym, 0) + 1
            if _stop_loss_confirmation[sym] >= _STOP_LOSS_CONFIRMATION_TICKS:
                _stop_loss_confirmation.pop(sym, None)
                if pos.get("_sell_target_crossed_ts") is None:
                    pos["_sell_target_crossed_ts"] = now
                with _selling_lock:
                    if sym in _selling:
                        continue
                    _selling.add(sym)
                    _selling_ts[sym] = time.time()
                    pos["_sell_trigger_ts"] = time.time()
                    pos["_sell_reason"] = "stop-loss"
                _sell_executor.submit(_execute_sell, pos, price, "stop-loss")
        else:
            _stop_loss_confirmation.pop(sym, None)

    # ── Inline signal refresh — throttled to every 30 s per coin ─────────────
    for sym, price in prices.items():
        if price <= 0:
            continue
        if now - _tick_signal_ts.get(sym, 0) >= _TICK_REFRESH_SEC:
            _tick_signal_ts[sym] = now
            _inline_refresh_from_ticks(sym, price)

    # ── Real-time buy check — dispatched to background thread, never blocks event loop ──
    # _check_buys_from_cache calls _get_usdt_balance() (blocking Binance REST in live
    # mode). Running it inline here would stall sell-trigger detection during WS reconnect
    # bursts. If a check is already in flight we skip — the in-flight check covers the same
    # coins, and during reconnect storms we'd otherwise queue 50 redundant REST calls.
    global _buy_check_in_flight
    with _buy_check_lock:
        if not _buy_check_in_flight:
            _buy_check_in_flight = True
            _prices_snap = dict(prices)
            def _run_buy_check():
                global _buy_check_in_flight
                try:
                    _check_buys_from_cache(_prices_snap)
                finally:
                    with _buy_check_lock:
                        _buy_check_in_flight = False
            _buy_check_executor.submit(_run_buy_check)


# ── Sell monitor — daemon thread, independent of asyncio ─────────────────────

_sell_diag_ts: float      = 0.0
_sell_monitor_heartbeat: float = 0.0   # updated every loop — 0 means not started
_sell_monitor_thread: Optional[threading.Thread] = None

# REST price fallback cache — populated when WebSocket is geo-blocked on Railway
_rest_px: Dict[str, float] = {}
_rest_px_ts: float = 0.0
# Per-symbol freshness of GENUINE REST quotes (not gap-fills from signal cache
# or WS carry-forward). Only fresh entries may override live WS prices in the
# sell monitor — an hours-old REST price must never mask a live WS price.
_rest_px_sym_ts: Dict[str, float] = {}
_REST_PX_FRESH_SEC = 10.0
_stale_px_warn_ts: Dict[str, float] = {}  # throttle "no fresh price" warning per symbol
_REST_PX_TTL = 5.0   # refetch REST prices every 5 s when WebSocket is down
_sell_monitor_last_rest_ts: float = 0.0   # rate-limits sell-monitor REST refresh to 2s


def _fetch_batch_prices(symbols: list, source: str = "batch_prices",
                        critical: bool = True) -> Dict[str, float]:
    """Batch ticker price fetch via _binance_request (source-tagged, circuit-broken).
    Uses single-symbol endpoint (weight=1) when only one symbol requested,
    batch endpoint (weight=4) otherwise. critical=True by default: nearly all
    callers are on the exit path (held watchdog, pre-sell fresh quote,
    position restore) and must not be shed by the rate-limit budget."""
    import urllib.parse as _up2
    if not symbols:
        return {}
    symbols = list(symbols)
    _bl = _get_blimits()
    _w_single = int(getattr(_bl, "WEIGHT_TICKER_PRICE", 2) or 2) if _bl else 2
    _w_batch  = int(getattr(_bl, "WEIGHT_TICKER_PRICE_BATCH", 4) or 4) if _bl else 4
    if len(symbols) == 1:
        # Single-symbol endpoint (weight 2) vs batch endpoint (weight 4)
        url = f"https://data-api.binance.vision/api/v3/ticker/price?symbol={symbols[0]}"
        ok, data, _, _ = _binance_request(url, timeout=3.0, source=source,
                                          weight=_w_single, critical=critical)
        if not ok or not isinstance(data, dict):
            return {}
        try:
            px = float(data.get("price", 0) or 0)
            return {data["symbol"]: px} if px > 0 and data.get("symbol") else {}
        except Exception:
            return {}
    _syms_json = json.dumps(symbols, separators=(',', ':'))
    _encoded   = _up2.quote(_syms_json, safe='')
    url = f"https://data-api.binance.vision/api/v3/ticker/price?symbols={_encoded}"
    ok, data, _, _ = _binance_request(url, timeout=3.0, source=source,
                                      weight=_w_batch, critical=critical)
    if not ok or not isinstance(data, list):
        return {}
    result: Dict[str, float] = {}
    for entry in data:
        s = entry.get("symbol", "")
        try:
            px = float(entry.get("price", 0) or 0)
        except (TypeError, ValueError):
            continue
        if s and px > 0:
            result[s] = px
    return result


def _fetch_rest_prices(symbols: list) -> Dict[str, float]:
    """
    Batch-fetch current prices via REST.
    Tries public CDN first (rarely geo-blocked from Railway), then API mirrors.
    If the multi-symbol batch endpoint fails, falls back to individual symbol
    fetches so a single invalid/delisted coin can't block all prices.
    Timeout 2 s per URL — fast-fail so we never block sell checks.
    """
    import urllib.request as _ur
    import urllib.parse as _up
    if not symbols:
        return {}
    result: Dict[str, float] = {}

    def _parse_response(data) -> Dict[str, float]:
        out: Dict[str, float] = {}
        if isinstance(data, list):
            for item in data:
                s = item.get("symbol", "")
                p = float(item.get("price", 0) or 0)
                if s and p > 0:
                    out[s] = p
        elif isinstance(data, dict) and data.get("symbol"):
            p = float(data.get("price", 0) or 0)
            if p > 0:
                out[data["symbol"]] = p
        return out

    syms_param = _up.quote(json.dumps(symbols))
    for base in _KLINE_BASES[:3]:   # try at most 3 endpoints; fail fast
        try:
            url = f"{base}/api/v3/ticker/price?symbols={syms_param}"
            req = _ur.Request(url, headers={"User-Agent": "TradingBot/1.0"})
            _t0 = time.time()
            with _ur.urlopen(req, timeout=2) as resp:
                _body = resp.read()
                _hdrs = dict(resp.headers)
            _record_rest_health(_hdrs, (time.time() - _t0) * 1000)
            data = json.loads(_body)
            result = _parse_response(data)
            if result:
                return result
        except Exception as _e:
            _record_rest_error(str(_e))
            continue

    # Batch endpoint failed on all bases — try individual fetches (slower but
    # more reliable: a single delisted coin won't break all others).
    if not result and len(symbols) <= 20:  # cap individual fetches to avoid flooding
        for sym in symbols:
            for base in _KLINE_BASES[:2]:
                try:
                    url = f"{base}/api/v3/ticker/price?symbol={sym}"
                    req = _ur.Request(url, headers={"User-Agent": "TradingBot/1.0"})
                    _t0b = time.time()
                    with _ur.urlopen(req, timeout=2) as resp:
                        _body = resp.read()
                        _hdrs = dict(resp.headers)
                    _record_rest_health(_hdrs, (time.time() - _t0b) * 1000)
                    data = json.loads(_body)
                    parsed = _parse_response(data)
                    if parsed:
                        result.update(parsed)
                        break   # got price for this sym — next sym
                except Exception as _e:
                    _record_rest_error(str(_e))
                    continue

    return result


_price_refresher_thread: Optional[threading.Thread] = None
_rest_fail_log_ts: float = 0.0   # throttle REST-failure warning to once per 60 s
_price_refresher_heartbeat: float = 0.0  # updated each loop iteration
_price_refresher_hb_log_ts: float = 0.0  # throttle heartbeat log to once per 60 s

def _price_refresher_loop():
    """
    Dedicated background thread — fetches REST prices for all open positions
    every 2 s and writes them to _rest_px.  Runs independently of the sell
    monitor so network I/O NEVER delays a sell check.

    ALWAYS updates _last_ws_price_ts for every successfully fetched price so
    that price_age_sec in /api/sell-monitor reflects REST freshness, not WS age.
    """
    import data_collector as _dc
    global _rest_px, _rest_px_ts, _rest_fail_log_ts
    global _price_refresher_heartbeat, _price_refresher_hb_log_ts

    try:
        database.log_activity("Price refresher thread started", "info")
    except Exception:
        pass

    while True:
        _price_refresher_heartbeat = time.time()
        try:
            with _positions_lock:
                snap = list(_positions)
            if snap:
                all_syms = list({p["symbol"] for p in snap})
                fetched = _fetch_batch_prices(all_syms)
                if fetched:
                    _now_rf = time.time()
                    _rest_px.update(fetched)
                    for _s_rf in fetched:
                        _rest_px_sym_ts[_s_rf] = _now_rf
                    _rest_px_ts = _now_rf
                    for s, p in fetched.items():
                        # Always inject into _dc.prices and update timestamp so
                        # price_age_sec reflects REST freshness for low-WS-volume coins.
                        _dc.prices[s] = p
                        _last_ws_price_ts[s] = _now_rf

                    # 60s heartbeat log — confirms refresher is alive and working
                    _now_hb = time.time()
                    if _now_hb - _price_refresher_hb_log_ts >= 60.0:
                        _price_refresher_hb_log_ts = _now_hb
                        try:
                            database.log_activity(
                                f"[PriceRefresher] OK — fetched {len(fetched)}/{len(all_syms)} symbols: "
                                + ", ".join(f"{s}={v:.6f}" for s, v in list(fetched.items())[:5]),
                                "info"
                            )
                        except Exception:
                            pass
                else:
                    # REST unavailable — log once per minute so logs show it
                    now_rf = time.time()
                    if now_rf - _rest_fail_log_ts >= 60.0:
                        _rest_fail_log_ts = now_rf
                        missing = [p["symbol"] for p in snap if _rest_px.get(p["symbol"], 0) <= 0]
                        try:
                            database.log_activity(
                                f"Price refresher: REST fetch returned empty for {all_syms}; "
                                f"symbols with no price: {missing}", "warn"
                            )
                        except Exception:
                            pass

                    # Fill gaps from signal cache
                    with _signal_cache_lock:
                        for pos in snap:
                            s = pos["symbol"]
                            if _rest_px.get(s, 0) <= 0:
                                sc = _signal_cache.get(s)
                                if sc and sc.get("price", 0) > 0:
                                    _rest_px[s] = sc["price"]
                    # Last resort: carry forward any WS price we already have
                    for pos in snap:
                        s = pos["symbol"]
                        if _rest_px.get(s, 0) <= 0:
                            ws_p = _dc.prices.get(s, 0)
                            if ws_p > 0:
                                _rest_px[s] = ws_p
        except Exception:
            pass
        time.sleep(10.0)


_held_refresher_thread: Optional[threading.Thread] = None
_held_refresher_hb_log_ts: float = 0.0


def _held_price_ages(held_syms: list) -> Dict[str, float]:
    """Freshest WS price age per held symbol — best of data_collector's
    last_price_ts (wired defensively; the map may not exist yet) and the
    engine's own _last_ws_price_ts. Symbols with no timestamp map to inf."""
    now_ts = time.time()
    dc_ts: dict = {}
    try:
        import data_collector as _dc_ag
        dc_ts = getattr(_dc_ag, "last_price_ts", None) or {}
    except Exception:
        dc_ts = {}
    ages: Dict[str, float] = {}
    for s in held_syms:
        try:
            t1 = float(dc_ts.get(s, 0) or 0)
        except Exception:
            t1 = 0.0
        t2 = float(_last_ws_price_ts.get(s, 0) or 0)
        freshest = max(t1, t2)
        ages[s] = (now_ts - freshest) if freshest > 0 else float("inf")
    return ages


def _watchdog_fires_24h() -> int:
    """Rolling count of watchdog REST fires in the last 24h (prunes in place)."""
    cutoff = time.time() - 86400.0
    with _watchdog_lock:
        while _watchdog_fire_ts and _watchdog_fire_ts[0] < cutoff:
            _watchdog_fire_ts.popleft()
        return len(_watchdog_fire_ts)


def _watchdog_cycle() -> dict:
    """One held-position watchdog evaluation (C §6.3). Testable single pass.

    Returns {"held": [...], "stale": [...], "fired": bool, "fetched": int}.
    fired=True means ONE batched REST call was made for ALL held symbols
    (never per-symbol loops); fired=False means every held symbol's freshest
    WS price age was within _WATCHDOG_STALE_SEC — no REST at all."""
    global _held_max_price_age_sec, _watchdog_alert_log_ts, _held_refresher_hb_log_ts

    with _positions_lock:
        held_syms = list({p.get("symbol") for p in _positions if p.get("symbol")})
    if not held_syms:
        _held_max_price_age_sec = 0.0
        return {"held": [], "stale": [], "fired": False, "fetched": 0}

    ages = _held_price_ages(held_syms)
    stale = [s for s, a in ages.items() if a > _WATCHDOG_STALE_SEC]
    fired = False
    fetched_n = 0

    if stale:
        # ONE batched call for ALL held symbols (weight 4, critical=True) —
        # routed through _binance_request → binance_limits header recording.
        fetched = _fetch_batch_prices(held_syms, source="held_watchdog")
        fired = True
        with _watchdog_lock:
            _watchdog_fire_ts.append(time.time())
        now_ts = time.time()
        if fetched:
            fetched_n = len(fetched)
            try:
                import data_collector as _dc_hr
                dc_prices = getattr(_dc_hr, "prices", None)
            except Exception:
                dc_prices = None
            for s, px in fetched.items():
                if dc_prices is not None:
                    try:
                        dc_prices[s] = px
                    except Exception:
                        pass
                _last_ws_price_ts[s] = now_ts
                _rest_px[s] = px
                _rest_px_sym_ts[s] = now_ts

            # 60s heartbeat log
            if now_ts - _held_refresher_hb_log_ts >= 60.0:
                _held_refresher_hb_log_ts = now_ts
                try:
                    database.log_activity(
                        f"[HeldWatchdog] fired (stale: {stale[:5]}) — "
                        f"{fetched_n}/{len(held_syms)} symbols refreshed: "
                        + ", ".join(f"{s}={v:.6f}" for s, v in list(fetched.items())[:5]),
                        "info"
                    )
                except Exception:
                    pass
        # Re-measure after the refresh attempt for health + alerting
        ages = _held_price_ages(held_syms)

    worst = max(ages.values()) if ages else 0.0
    _held_max_price_age_sec = 9999.0 if worst == float("inf") else round(worst, 2)

    # Health alert: a held symbol's price is >5s stale even after the
    # watchdog's refresh attempt — the exit path is flying blind.
    if _held_max_price_age_sec > _WATCHDOG_ALERT_SEC:
        _now_al = time.time()
        if _now_al - _watchdog_alert_log_ts >= 60.0:
            _watchdog_alert_log_ts = _now_al
            _worst_syms = sorted(
                (s for s, a in ages.items() if a > _WATCHDOG_ALERT_SEC),
                key=lambda s: -ages[s],
            )[:5]
            log_diag_issue(
                "price_feed", "error",
                f"Held-position price age > {_WATCHDOG_ALERT_SEC:.0f}s "
                f"(max {min(_held_max_price_age_sec, 9999):.1f}s) — exits may be delayed",
                detail=f"symbols: {_worst_syms}",
            )

    return {"held": held_syms, "stale": stale, "fired": fired, "fetched": fetched_n}


def _held_position_price_refresher():
    """C §6.3 — held-position exit WATCHDOG (was: unconditional 2s REST poll).

    Every 2s cycle: check each held symbol's freshest WS price age
    (data_collector.last_price_ts, fallback _last_ws_price_ts). ONLY if any
    held symbol's age exceeds _WATCHDOG_STALE_SEC (3s) does it fetch ALL held
    symbols in ONE batched /api/v3/ticker/price?symbols=[...] call
    (critical=True — exits depend on it). Never per-symbol loops.

    Unchanged guarantee: stale prices never trigger stop-loss — the sell
    monitor's _stale_px_syms skip rule is untouched."""
    consecutive_errors = 0

    try:
        database.log_activity(
            "Held-position exit watchdog started (2s cycle; REST only when a "
            "held symbol's WS price age exceeds 3s)", "info")
    except Exception:
        pass

    while True:
        try:
            if _thread_health:
                _thread_health.heartbeat("held_price_refresher")
        except Exception:
            pass
        try:
            result = _watchdog_cycle()
            if not result["held"]:
                time.sleep(5.0)
                continue
            if result["fired"]:
                if result["fetched"] > 0:
                    consecutive_errors = 0
                else:
                    consecutive_errors += 1
                    if consecutive_errors <= 3 or consecutive_errors % 30 == 0:
                        try:
                            database.log_activity(
                                f"[HeldWatchdog] REST fetch returned empty for {result['held']} "
                                f"(attempt {consecutive_errors})", "warn"
                            )
                        except Exception:
                            pass
        except Exception as _e:
            consecutive_errors += 1
            if consecutive_errors <= 3 or consecutive_errors % 30 == 0:
                try:
                    database.log_activity(
                        f"[HeldWatchdog] {type(_e).__name__}: {_e} ({consecutive_errors} consecutive errors)",
                        "warn"
                    )
                except Exception:
                    pass
                log_diag_issue(
                    "price_refresher", "error",
                    f"Held watchdog failed ({consecutive_errors} consecutive)",
                    detail=f"{type(_e).__name__}: {_e}",
                )
        # 2s base cadence (backs off to 30s under consecutive REST errors).
        time.sleep(max(2.0, min(30.0, 2.0 + consecutive_errors * 1.0)))


def start_held_position_refresher():
    """Idempotent — starts the held-position REST refresher if not already running."""
    global _held_refresher_thread
    if _held_refresher_thread is not None and _held_refresher_thread.is_alive():
        return
    _held_refresher_thread = threading.Thread(
        target=_held_position_price_refresher,
        name="held-price-refresher",
        daemon=True
    )
    _held_refresher_thread.start()


# ── Auto-recycle capital ───────────────────────────────────────────────────────
_capital_recycler_thread: Optional[threading.Thread] = None
_AUTO_RECYCLE_AGE_HOURS = 24.0
_AUTO_RECYCLE_GAP_PCT   = 3.0


def _capital_recycler_loop():
    """Every 30 min, force-sell positions that have been underwater >24h with >3% gap.
    Only runs if 'auto_recycle_enabled' is true in strategy.json (default OFF)."""
    while True:
        try:
            if _thread_health:
                _thread_health.heartbeat("capital_recycler")
            time.sleep(1800)
            strategy = _load_strategy()
            if not bool(strategy.get("auto_recycle_enabled", False)):
                continue
            age_h_thresh = float(strategy.get("auto_recycle_age_hours", _AUTO_RECYCLE_AGE_HOURS))
            gap_thresh   = float(strategy.get("auto_recycle_gap_pct",   _AUTO_RECYCLE_GAP_PCT))
            with _positions_lock:
                snap = list(_positions)
            now = time.time()
            for pos in snap:
                sym = pos.get("symbol")
                if not sym:
                    continue
                with _selling_lock:
                    if sym in _selling:
                        continue
                opened_ts = pos.get("opened_at_ts", 0)
                if opened_ts <= 0:
                    continue
                age_h = (now - opened_ts) / 3600
                if age_h < age_h_thresh:
                    continue
                real_bep = compute_real_breakeven_price(pos)
                if real_bep <= 0:
                    continue
                import data_collector as _dc_rc
                cur_price = _dc_rc.prices.get(sym, 0)
                if cur_price <= 0:
                    cur_price = pos.get("current_price") or 0
                if cur_price <= 0:
                    continue
                gap_pct = (real_bep - cur_price) / cur_price * 100
                if gap_pct < gap_thresh:
                    continue
                database.log_activity(
                    f"AUTO_RECYCLE {sym}: held {age_h:.1f}h, needs +{gap_pct:.2f}% to break even — force-selling to free capital",
                    "warn"
                )
                try:
                    log_diag_issue("auto_recycle", "info",
                        f"{sym}: held {age_h:.1f}h gap=+{gap_pct:.2f}% — recycling")
                except (NameError, Exception):
                    pass
                with _selling_lock:
                    if sym in _selling:
                        continue
                    _selling.add(sym)
                    _selling_ts[sym] = now
                import data_collector as _dc_rc2
                _sell_price = _dc_rc2.prices.get(sym, cur_price)
                _sell_executor.submit(_execute_sell, pos, _sell_price, "auto-recycle")
        except Exception as _re:
            try:
                log_diag_issue("auto_recycle", "warn", f"recycler loop error: {_re}")
            except (NameError, Exception):
                pass
            time.sleep(60)


def start_capital_recycler():
    global _capital_recycler_thread
    if _capital_recycler_thread is not None and _capital_recycler_thread.is_alive():
        return
    _capital_recycler_thread = threading.Thread(
        target=_capital_recycler_loop,
        name="capital_recycler",
        daemon=True,
    )
    _capital_recycler_thread.start()


# ── Phantom position detector ──────────────────────────────────────────────────
_phantom_checker_thread: Optional[threading.Thread] = None


def _phantom_check_loop():
    """Every 300s (live mode only), compare DB positions to Binance balances.
    Logs mismatches to activity_log and inserts unresolved phantom_alerts rows."""
    import sqlite3 as _sq_ph
    while True:
        try:
            time.sleep(300)
            mode = get_mode()
            if mode != "live":
                continue
            with _positions_lock:
                snap = list(_positions)
            if not snap:
                continue
            try:
                acc = _acct()  # geo-block-safe direct transport
                balances = {b["asset"]: float(b.get("free", 0)) + float(b.get("locked", 0))
                            for b in acc.get("balances", [])}
            except Exception as _be:
                log_diag_issue("phantom_detector", "warn", f"get_account failed: {_be}")
                continue
            now_iso = datetime.now(timezone.utc).isoformat()
            for pos in snap:
                sym = pos.get("symbol", "")
                if not sym:
                    continue
                db_qty = float(pos.get("quantity", 0))
                # Strip only trailing USDT to get base asset
                base = sym[:-4] if sym.endswith("USDT") else sym
                binance_qty = balances.get(base, 0.0)
                # Only flag if Binance shows <5% of what DB expects and db_qty > dust
                if db_qty <= 1e-8:
                    continue
                if binance_qty >= db_qty * 0.05:
                    continue
                msg = (f"[PHANTOM] {sym}: DB has qty={db_qty:.6f} but Binance has {binance_qty:.6f}")
                try:
                    database.log_activity(msg, "warn")
                except Exception:
                    pass
                try:
                    conn = _sq_ph.connect(database.DB_PATH)
                    # Avoid duplicate unresolved alerts for same symbol
                    existing = conn.execute(
                        "SELECT id FROM phantom_alerts WHERE symbol=? AND resolved=0", (sym,)
                    ).fetchone()
                    if not existing:
                        conn.execute(
                            "INSERT INTO phantom_alerts (timestamp, symbol, db_qty, binance_qty, resolved) VALUES (?,?,?,?,0)",
                            (now_iso, sym, db_qty, binance_qty)
                        )
                        conn.commit()
                    conn.close()
                except Exception:
                    pass
        except Exception as _pe:
            try:
                log_diag_issue("phantom_detector", "warn", f"phantom loop error: {_pe}")
            except Exception:
                pass


def start_phantom_checker():
    """Idempotent — starts the phantom position detector if not already running."""
    global _phantom_checker_thread
    if _phantom_checker_thread is not None and _phantom_checker_thread.is_alive():
        return
    _phantom_checker_thread = threading.Thread(
        target=_phantom_check_loop,
        name="phantom-checker",
        daemon=True,
    )
    _phantom_checker_thread.start()


def _sell_monitor_loop():
    """
    Fallback daemon thread — wakes every 0.5 s and checks sell conditions.
    Never does any network I/O — REST prices are fetched by _price_refresher_loop
    (a separate daemon thread) and written to _rest_px.  Decoupling means sell
    checks are NEVER delayed by a slow HTTP request.

    Price priority: REST (_rest_px, refreshed every 2 s) > WebSocket (_dc.prices).
    Both are applied before the sell check so the freshest price is always used.
    """
    import data_collector as _dc
    global _sell_diag_ts, _sell_monitor_heartbeat

    try:
        database.log_activity("Sell monitor thread started", "info")
    except Exception:
        pass

    while True:
        _sell_monitor_heartbeat = time.time()
        try:
            if _thread_health:
                _thread_health.heartbeat("sell_monitor")
        except Exception:
            pass
        try:
            with _positions_lock:
                snap = list(_positions)

            # REST price refresh is owned by _held_position_price_refresher
            # (dedicated 2s thread). It was previously fetched inline here,
            # blocking this 0.25s trigger loop for up to 3s whenever the REST
            # endpoint was slow — exactly when fast selling matters most.

            # ── Watchdog: force-clear _selling entries stuck > 45 s ──────────
            # 45s > worst-case legit sell latency (gate REST 3s + balance clamp
            # + exchange-info refresh + order, ~25s of stacked timeouts).
            # Clearing earlier let a second worker submit a DUPLICATE sell
            # while the first was still placing the order.
            now_wd = time.time()
            with _selling_lock:
                stuck = [s for s, t in list(_selling_ts.items()) if now_wd - t > 45]
            for s in stuck:
                with _selling_lock:
                    _selling.discard(s)
                    _selling_ts.pop(s, None)
                try:
                    database.log_activity(
                        f"Sell monitor: force-cleared stuck guard for {s} (>45 s)", "warn"
                    )
                except Exception:
                    pass

            # Always refresh TP/SL multipliers — settings can change at any time.
            _refresh_risk_params()

            # Build price dict: start with WebSocket, then override with REST —
            # but ONLY with genuinely fresh REST quotes (<10s). An old REST
            # price (endpoint down, gap-fill from signal cache) must never
            # mask a live WS price: that both missed real triggers and fired
            # phantom ones.
            _mrg_now = time.time()
            prices = dict(_dc.prices)
            for sym2, p2 in _rest_px.items():
                if p2 > 0 and (_mrg_now - _rest_px_sym_ts.get(sym2, 0)) < _REST_PX_FRESH_SEC:
                    prices[sym2] = p2

            # Signal-cache is the final fallback — fills gaps when both WS and
            # REST are unavailable for a symbol (e.g. first 30 s after redeploy).
            # These prices can be 60 s+ stale, so they are used for DIAGNOSTICS
            # ONLY: symbols in _stale_px_syms are skipped by the trigger loop
            # below (better to retry in 250 ms than to stop-loss on a stale price).
            _stale_px_syms: set = set()
            with _signal_cache_lock:
                sc_snap = dict(_signal_cache)
            for pos in snap:
                s = pos["symbol"]
                if prices.get(s, 0) <= 0:
                    sc_p = sc_snap.get(s, {}).get("price", 0)
                    if sc_p and sc_p > 0:
                        prices[s] = sc_p
                        _stale_px_syms.add(s)

            # Diagnostic log every 60 s — shows ACTUAL sell target, not just breakeven
            now_t = time.time()
            if now_t - _sell_diag_ts >= 60.0:
                _sell_diag_ts = now_t
                lines = []
                no_price_syms = []
                for p in snap:
                    sym    = p["symbol"]
                    price  = prices.get(sym, 0.0)
                    entry  = p["entry_price"]
                    if price <= 0:
                        no_price_syms.append(sym)
                        lines.append(f"{sym} NO_PRICE(entry={entry:.4f})")
                        continue
                    _bep_m_diag = p.get("breakeven_mult_at_buy") or _get_breakeven_mult(entry, sym)
                    _bep_diag   = entry * _bep_m_diag
                    actual = max(_bep_diag, entry * _user_tp_mult) if _take_profit_enabled else _bep_diag  # real sell threshold
                    pct    = ((price - entry) / entry * 100) if entry else 0
                    gap_pct  = ((actual - price) / price * 100) if price > 0 and actual > price else 0.0
                    qty_held = p.get("quantity", 0)
                    budget   = p.get("budget_usdt", 0)
                    buy_fee  = float(p.get("buy_fee_usdt") or budget * _fee_rate)
                    gross_now = price * qty_held
                    est_profit = gross_now * (1 - _fee_rate) - budget - buy_fee
                    lines.append(
                        f"{sym} entry={entry:.4f} cur={price:.4f}({pct:+.3f}%) "
                        f"target={actual:.4f} ({'SELL' if price >= actual else f'need +{gap_pct:.3f}%'}) "
                        f"est=${est_profit:+.3f}"
                    )
                msg = f"Sell monitor: {len(snap)} open — " + " | ".join(lines)
                if no_price_syms:
                    msg += f" | WARN: no price for {no_price_syms}"
                database.log_activity(msg, "info")

            now_monitor = time.time()
            for pos in snap:
                sym   = pos["symbol"]
                price = prices.get(sym, 0.0)

                if price <= 0:
                    continue
                if sym in _stale_px_syms:
                    # Only a stale signal-cache price is available — never make a
                    # stop-loss/take-profit decision on it. Warn (throttled) and
                    # retry next cycle when the refresher has a fresh price.
                    if now_monitor - _stale_px_warn_ts.get(sym, 0) >= 60.0:
                        _stale_px_warn_ts[sym] = now_monitor
                        try:
                            database.log_activity(
                                f"Sell monitor: no fresh price for {sym} — skipping sell "
                                f"checks this cycle (signal-cache price may be stale)", "warn"
                            )
                        except Exception:
                            pass
                    continue
                with _selling_lock:
                    if sym in _selling:
                        continue
                # Rate-limit retries: stop-loss retries immediately, take-profit waits 5s.
                last_fail = _sell_last_failed_ts.get(sym, 0)
                if last_fail:
                    _cooldown = (_SELL_RETRY_COOLDOWN_LOSS
                                 if _sell_last_failed_reason.get(sym, "") in ("stop-loss", "force-sell")
                                 else _SELL_RETRY_COOLDOWN_PROFIT)
                    if (now_monitor - last_fail) < _cooldown:
                        continue
                # Minimum hold applies to STOP-LOSS only — take-profit is never delayed
                opened_ts3 = pos.get("opened_at_ts", 0)
                in_min_hold3 = opened_ts3 > 0 and (now_monitor - opened_ts3) < _MIN_HOLD_SEC
                entry  = pos["entry_price"]
                real_target3 = compute_real_breakeven_price(pos)
                if real_target3 <= 0:
                    _bep_m3 = pos.get("breakeven_mult_at_buy") or _get_breakeven_mult(entry, sym)
                    real_target3 = entry * _bep_m3
                stop = entry * _stop_loss_mult  # % below ENTRY — matches frontend label
                # TP trigger = user target floored at real breakeven
                if _take_profit_enabled:
                    target = max(real_target3, entry * _user_tp_mult)
                else:
                    target = real_target3

                # SELL_TRACE — per symbol, throttled to 1 per 60 s
                _tr_now = now_monitor
                if _tr_now - _sell_trace_log_ts.get(sym, 0) >= 60.0:
                    _sell_trace_log_ts[sym] = _tr_now
                    _loss_cd = _loss_cooldown.get(sym, 0)
                    _cd_rem  = max(0.0, _loss_cd - _tr_now)
                    database.log_activity(
                        f"[SELL_TRACE] {sym}: entry={entry:.6f} cur={price:.6f} "
                        f"target={target:.6f} (bep={real_target3:.6f}) "
                        f"above_target={price >= target} "
                        f"cooldown={_cd_rem:.0f}s",
                        "info"
                    )

                sell_reason3: Optional[str] = None

                if price >= target:
                    if _smart_hold_enabled and price >= target and target > real_target3:
                        _pos_peaks[sym] = max(_pos_peaks.get(sym, target), price)
                        peak = _pos_peaks[sym]
                        # Trail hard-floored at the exit target (never give back below it)
                        trail_stop = max(peak * (1.0 - _trailing_stop_pct / 100.0), target)
                        if price <= trail_stop:
                            sell_reason3 = "smart-hold-trail"
                        else:
                            with _signal_cache_lock:
                                _sc2 = _signal_cache.get(sym, {})
                                score2 = _sc2.get("score", 0)
                                score2_ts = _sc2.get("ts", 0)
                            # Only a FRESH bullish score may defer a profitable sell
                            if score2 < 3 or (now_monitor - score2_ts) > 120:
                                sell_reason3 = "take-profit"
                    else:
                        sell_reason3 = "take-profit"
                elif _stop_loss_mult < 1.0 and price <= stop and not in_min_hold3:
                    _stop_loss_confirmation[sym] = _stop_loss_confirmation.get(sym, 0) + 1
                    if _stop_loss_confirmation[sym] >= _STOP_LOSS_CONFIRMATION_TICKS:
                        _stop_loss_confirmation.pop(sym, None)
                        sell_reason3 = "stop-loss"
                else:
                    _stop_loss_confirmation.pop(sym, None)

                if sell_reason3:
                    with _selling_lock:
                        if sym in _selling:
                            continue
                        _selling.add(sym)
                        _selling_ts[sym] = time.time()
                        pos["_sell_trigger_ts"] = time.time()
                        if pos.get("_sell_target_crossed_ts") is None:
                            pos["_sell_target_crossed_ts"] = time.time()
                    _sell_executor.submit(_execute_sell, pos, price, sell_reason3)

        except Exception as exc:
            try:
                database.log_activity(f"Sell monitor error: {exc}", "error")
            except Exception:
                pass
            log_diag_issue(
                "sell_monitor", "error",
                f"Monitor loop exception: {type(exc).__name__}",
                detail=str(exc),
            )
        time.sleep(0.25)  # 250ms cycle — halves worst-case sell delay


async def position_guardian():
    """
    Watchdog coroutine — starts the sell monitor and price refresher threads
    and restarts them if they ever die (belt-and-suspenders).
    """
    global _sell_monitor_thread, _price_refresher_thread, _held_refresher_thread
    while True:
        if not (_sell_monitor_thread and _sell_monitor_thread.is_alive()):
            _sell_monitor_thread = threading.Thread(
                target=_sell_monitor_loop, name="sell-monitor", daemon=True
            )
            _sell_monitor_thread.start()
        if not (_price_refresher_thread and _price_refresher_thread.is_alive()):
            _price_refresher_thread = threading.Thread(
                target=_price_refresher_loop, name="price-refresher", daemon=True
            )
            _price_refresher_thread.start()
        if not (_held_refresher_thread and _held_refresher_thread.is_alive()):
            _held_refresher_thread = threading.Thread(
                target=_held_position_price_refresher, name="held-price-refresher", daemon=True
            )
            _held_refresher_thread.start()
        await asyncio.sleep(5.0)


# ── C §6.2d/§6.5 — cache freshness + engine health contract ──────────────────

def stale_signal_syms(max_age_sec: float = _STALE_SIGNAL_SEC) -> list:
    """Symbols whose signal-cache entry is older than max_age_sec (default 180s
    ≈ every active symbol should be at most ~1 candle old in ws-first mode).
    Judged against the active universe when known (a symbol with NO entry is
    stale too); falls back to cache keys before the first scan pass."""
    now = time.time()
    with _signal_cache_lock:
        cache_ts = {s: float((e or {}).get("ts", 0) or 0) for s, e in _signal_cache.items()}
    universe = _active_universe or set(cache_ts)
    return sorted(s for s in universe if now - cache_ts.get(s, 0.0) > max_age_sec)


def get_engine_health() -> dict:
    """C §6.5 contract — engine health snapshot for control_api/thread_health."""
    stale = stale_signal_syms()
    _signal_scanner_health["stale_signal_count"] = len(stale)
    with _positions_lock:
        held_n = len(_positions)
    return {
        "scanner":                dict(_signal_scanner_health),
        "held_max_price_age_sec": _held_max_price_age_sec,
        "stale_signal_syms":      stale[:10],
        "watchdog_fires_24h":     _watchdog_fires_24h(),
        "held_symbols":           held_n,
    }


# ── C §6.2c — ws-first scanner maintenance (no REST) ─────────────────────────

def _refresh_symbol_from_db(sym: str) -> bool:
    """Rebuild a full cache entry for one symbol from DB candles (no REST).
    Used by the ws-first maintenance pass while the WS buffer is still thin."""
    try:
        db_rows = database.get_candles(sym, config.CANDLE_TIMEFRAME, limit=120)
        if len(db_rows) < 16:
            return False
        candles = [
            {"high":   float(r.get("high") or r["close"]),
             "low":    float(r.get("low")  or r["close"]),
             "close":  float(r["close"]),
             "volume": float(r.get("volume") or 0.0)}
            for r in db_rows
        ]
        return _rebuild_full_entry(sym, candles)
    except Exception:
        return False


def _ws_first_maintenance():
    """C §6.2c — the scanner-loop pass when legacy_rest_scan is OFF (default).
    Makes NO REST calls. Responsibilities:
      1. universe telemetry (approved coins),
      2. DB-fallback cache refresh for symbols whose WS buffers are still thin
         AND whose cache entry has gone stale,
      3. _signal_scanner_health freshness stats (mode/stale counts).
    Runs on an executor thread — the DB reads must stay off the event loop."""
    global _active_universe, _last_empty_universe_warn_ts
    strategy = _load_strategy()
    approved = [
        c["symbol"] for c in (strategy or {}).get("approved_coins", [])
        if c.get("approved")
    ]
    _active_universe = set(approved)
    _signal_scanner_health["universe_size"] = len(approved)
    if not approved:
        if time.time() - _last_empty_universe_warn_ts >= 600.0:
            _last_empty_universe_warn_ts = time.time()
            log.warning("[SignalScanner] ws-first pass: approved-coin universe is EMPTY, no buys possible")
            try:
                database.log_activity(
                    "Signal scan: 0 approved symbols — universe is empty, the bot cannot buy anything",
                    "warn",
                )
            except Exception:
                pass
        _signal_scanner_health["stale_signal_count"] = 0
        return

    try:
        import data_collector as _dc_mt
        ws_bufs = getattr(_dc_mt, "ws_candles", None) or {}
    except Exception:
        ws_bufs = {}

    now = time.time()
    refreshed_db = 0
    for sym in approved:
        try:
            buf_len = len(ws_bufs.get(sym) or [])
        except Exception:
            buf_len = 0
        if buf_len >= 16:
            continue  # kline-close path owns this symbol's freshness
        with _signal_cache_lock:
            entry_ts = float((_signal_cache.get(sym) or {}).get("ts", 0) or 0)
        if now - entry_ts <= _STALE_SIGNAL_SEC:
            continue
        if _refresh_symbol_from_db(sym):
            refreshed_db += 1

    if refreshed_db:
        _signal_scanner_health["db_fallback_refreshes"] = (
            _signal_scanner_health.get("db_fallback_refreshes", 0) + refreshed_db
        )
    _signal_scanner_health["stale_signal_count"] = len(stale_signal_syms())


# ── Process 2: signal scanner (async, refreshes cache every SCAN_INTERVAL_SEC) ─

async def signal_scanner(prices: dict):
    """
    Async coroutine — runs every SCAN_INTERVAL_SEC (60 s).

    C §6.2c — two modes (strategy.json data.legacy_rest_scan, default OFF):
      ws-first (default): kline-close events own signal freshness; this loop
        only (1) triggers _check_buys_from_cache periodically (poll-based buy
        backup), (2) DB-refreshes symbols whose WS buffers are thin (no REST),
        (3) updates _signal_scanner_health freshness stats.
      legacy: the old full REST refresh pass (emergency fallback, one release).
    The buy trigger stays: buys MUST fire even when WebSocket is slow.
    """
    global _last_logged_scan_mode
    _signal_scanner_health["interval_sec"] = float(config.SCAN_INTERVAL_SEC)
    # C §6.1b — adaptive interval: starts at the configured value; stretched
    # when a pass runs long, decayed back when passes get fast again.
    _base_interval = float(config.SCAN_INTERVAL_SEC)
    _effective_interval = _base_interval
    _signal_scanner_health["effective_interval_sec"] = _effective_interval
    while True:
        _t0_scan = time.time()
        try:
            _strategy_sc = _load_strategy()
            _legacy = bool(((_strategy_sc or {}).get("data") or {}).get("legacy_rest_scan", False))
            _mode = "legacy" if _legacy else "ws-first"
            _signal_scanner_health["mode"] = _mode
            if _mode != _last_logged_scan_mode:
                _last_logged_scan_mode = _mode
                log.info("[SignalScanner] Signal engine mode: %s (data.legacy_rest_scan=%s)",
                         _mode, _legacy)
                try:
                    database.log_activity(f"Signal engine mode: {_mode}", "info")
                except Exception:
                    pass
            loop = asyncio.get_running_loop()
            if _legacy:
                await _refresh_signal_cache()
            else:
                await loop.run_in_executor(None, _ws_first_maintenance)
            # Trigger buy checks right after refreshing — don't wait for WebSocket
            await loop.run_in_executor(None, _check_buys_from_cache, dict(prices))
        except Exception as e:
            print(f"[SignalScanner] Unexpected error: {e}")
            log_diag_issue(
                "signal_scanner", "error",
                f"Scan iteration failed: {type(e).__name__}",
                detail=str(e),
            )
        finally:
            _duration_sec = time.time() - _t0_scan
            _signal_scanner_health["last_refresh_ts"]  = time.time()
            _signal_scanner_health["last_duration_ms"] = round(_duration_sec * 1000, 1)
            _signal_scanner_health["scans_completed"] += 1

            # C §6.1b — duration telemetry + adaptive interval.
            # Slow pass (>0.8× current interval): next sleep = duration × 1.5,
            # capped at 600s, floored at SCAN_INTERVAL_SEC. Fast pass
            # (<0.5× SCAN_INTERVAL_SEC): decay back toward SCAN_INTERVAL_SEC.
            if _duration_sec > 0.8 * _effective_interval:
                _new_interval = min(600.0, max(_base_interval, _duration_sec * 1.5))
                log.warning(
                    "[SignalScanner] Slow scan pass: duration=%.1fs > 0.8×interval "
                    "(%.1fs) — stretching next interval to %.1fs",
                    _duration_sec, _effective_interval, _new_interval,
                )
                _effective_interval = _new_interval
            elif (_duration_sec < 0.5 * _base_interval
                  and _effective_interval > _base_interval):
                # Halve the stretch each fast pass until back at the base.
                _effective_interval = max(
                    _base_interval,
                    _base_interval + (_effective_interval - _base_interval) * 0.5,
                )
                if _effective_interval - _base_interval < 1.0:
                    _effective_interval = _base_interval
            _signal_scanner_health["effective_interval_sec"] = round(_effective_interval, 1)

        await asyncio.sleep(_effective_interval)



_KLINE_BASES = [
    # CDN first — not geo-blocked, works when api.binance.com returns 451
    "https://data-api.binance.vision",
    "https://api.binance.com",
    "https://api1.binance.com",
    "https://api2.binance.com",
    "https://api3.binance.com",
    "https://api4.binance.com",
]


async def _fetch_klines(session, sym: str) -> list:
    """Try each Binance base URL to work around regional geo-blocks (HTTP 451)."""
    import aiohttp
    last_exc: Exception = RuntimeError("No Binance bases configured")
    for base in _KLINE_BASES:
        url = f"{base}/api/v3/klines?symbol={sym}&interval=1m&limit=50"
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=3)) as resp:
                try:
                    _limits_record_headers(dict(resp.headers))  # C §6.4 weight accounting
                except Exception:
                    pass
                resp.raise_for_status()
                return await resp.json()
        except Exception as e:
            last_exc = e
    raise last_exc


async def _refresh_signal_cache():
    """
    Refresh the in-memory signal cache concurrently (asyncio.gather).
    Falls back to DB candles if REST is geo-blocked.
    Never executes trades.

    C §6.1a — single-flight skip-not-queue: if a refresh is already in flight,
    this invocation is SKIPPED (counted in scan_skipped_overlap). Nothing may
    ever queue a second concurrent refresh behind a running one.
    """
    if not _signal_refresh_inflight.acquire(blocking=False):
        _signal_scanner_health["scan_skipped_overlap"] += 1
        log.warning(
            "[SignalScanner] Refresh already in flight — skipping this "
            "invocation (skipped %d total)",
            _signal_scanner_health["scan_skipped_overlap"],
        )
        return
    try:
        await _refresh_signal_cache_locked()
    finally:
        _signal_refresh_inflight.release()


async def _refresh_signal_cache_locked():
    """Body of the signal-cache refresh. Only ever runs under the single-flight
    guard in _refresh_signal_cache — do not call directly."""
    import aiohttp
    global _last_empty_universe_warn_ts, _active_universe
    _pass_t0 = time.time()
    strategy = _load_strategy()
    if not strategy:
        return

    approved_coins = [
        c["symbol"]
        for c in strategy.get("approved_coins", [])
        if c.get("approved")
    ]
    # C §6.1e — universe telemetry + logging (warn when empty, throttled 10 min)
    _active_universe = set(approved_coins)
    _signal_scanner_health["universe_size"] = len(approved_coins)
    if not approved_coins:
        if time.time() - _last_empty_universe_warn_ts >= 600.0:
            _last_empty_universe_warn_ts = time.time()
            log.warning("[SignalScanner] Signal scan: scanning 0 symbols — approved-coin universe is EMPTY, no buys possible")
            try:
                database.log_activity(
                    "Signal scan: 0 approved symbols — universe is empty, the bot cannot buy anything",
                    "warn",
                )
            except Exception:
                pass
        return
    log.info("Signal scan: scanning %d symbols", len(approved_coins))

    async def _refresh_one(session, sym: str) -> bool:
        import data_collector as _dc
        MIN = 16          # matches _dc._MIN_CANDLES — enough for RSI to fire
        closes = volumes = candles = None
        raw = None

        # 1. Try Binance REST (fastest, most data — includes full OHLC for ATR)
        # C §6.1c — hard budget: the multi-base loop inside _fetch_klines can
        # stack 6 bases × 3s; bound EACH attempt to 5s total and allow exactly
        # ONE retry, so a symbol can never hold the pass for more than ~10s.
        try:
            for _attempt in (1, 2):
                try:
                    raw = await asyncio.wait_for(
                        _fetch_klines(session, sym),
                        timeout=_SCAN_SYMBOL_FETCH_TIMEOUT_SEC,
                    )
                    break
                except Exception:
                    if _attempt == 2:
                        raise
            closes  = [float(k[4]) for k in raw]
            volumes = [float(k[5]) for k in raw]
            candles = [
                {"high": float(k[2]), "low": float(k[3]),
                 "close": float(k[4]), "volume": float(k[5])}
                for k in raw
            ]
        except Exception:
            raw = None

        # 2. Fall back to DB candles (populated by download_history / kline saves)
        if not closes or len(closes) < MIN:
            db_rows = database.get_candles(sym, config.CANDLE_TIMEFRAME, limit=60)
            if len(db_rows) >= MIN:
                closes  = [float(c["close"])  for c in db_rows]
                volumes = [float(c["volume"]) for c in db_rows]
                candles = [
                    {"high":   float(c.get("high") or c["close"]),
                     "low":    float(c.get("low")  or c["close"]),
                     "close":  float(c["close"]),
                     "volume": float(c["volume"])}
                    for c in db_rows
                ]

        # 3. Fall back to in-memory WebSocket candle buffer (no REST needed)
        #    This kicks in when Binance REST is geo-blocked from Railway's servers.
        #    Fills up at 1 candle/minute; RSI fires after 16 minutes.
        if not closes or len(closes) < MIN:
            buf = _dc.ws_candles.get(sym, [])
            if len(buf) >= MIN:
                closes  = [float(r[4]) for r in buf]
                volumes = [float(r[5]) for r in buf]
                candles = [
                    {"high": float(r[2]), "low": float(r[3]),
                     "close": float(r[4]), "volume": float(r[5])}
                    for r in buf
                ]

        # 4. Time-sampled price buffer — meaningful RSI after 8 min; no OHLC.
        if not closes or len(closes) < MIN:
            samples = list(_dc.price_samples.get(sym, []))
            if len(samples) >= _dc._MIN_SAMPLES:
                closes  = samples
                volumes = [1.0] * len(samples)
                candles = [{"high": c, "low": c, "close": c, "volume": 1.0} for c in samples]

        # 5. Raw price-tick buffer — available in seconds but RSI quality is poor.
        #    Used only as last resort (first 8 minutes of uptime).
        if not closes or len(closes) < MIN:
            ticks = list(_dc.price_ticks.get(sym, []))
            if len(ticks) >= _dc._MIN_TICKS:
                closes  = ticks
                volumes = [1.0] * len(ticks)
                candles = [{"high": c, "low": c, "close": c, "volume": 1.0} for c in ticks]

        if not (closes and len(closes) >= MIN and candles):
            return False

        # Evaluate 6 signals (ATR accurate only when real OHLC is available)
        signals = evaluate_signals(candles)
        score   = sum(signals.values())
        rsi_list    = indicators.calc_rsi(closes, 14)
        rsi_display = rsi_list[-2] if rsi_list[-2] is not None else 0.0

        # BB veto — computed on the last completed candle (index -2)
        bb_u, bb_m, _ = indicators.calc_bollinger(closes)
        bb_ok = indicators.bb_buy_allowed(closes[-2], bb_u[-2], bb_m[-2])

        # 5m timeframe veto — cached 180 s to avoid repeated REST calls
        with _signal_cache_lock:
            prev = dict(_signal_cache.get(sym, {}))
        _5m_age = time.time() - prev.get("5m_ts", 0)
        if _5m_age >= 180:
            try:
                candles_5m = await asyncio.to_thread(_dc.fetch_5m_candles, sym)
                if not candles_5m or len(candles_5m) < 21:
                    # Cold start: the WS 5m buffer needs ~105 min to refill
                    # after a restart. Derive 5m candles from stored 1m
                    # history so the veto can evaluate real trend data.
                    try:
                        _db_1m = await asyncio.to_thread(
                            database.get_candles, sym, config.CANDLE_TIMEFRAME, 130
                        )
                        _derived_5m = indicators.aggregate_candles(_db_1m, group=5)
                        if len(_derived_5m) >= 21:
                            candles_5m = _derived_5m
                    except Exception:
                        pass
                if not candles_5m or len(candles_5m) < 21:
                    # Still not enough data — this is data availability, not a
                    # downtrend. Stay NEUTRAL (pass) during warmup instead of
                    # vetoing every coin '5m downtrend' for up to ~105 min
                    # after each restart.
                    five_m_ok = True
                    global _warned_5m_warmup
                    if not _warned_5m_warmup:
                        _warned_5m_warmup = True
                        database.log_activity(
                            "5m veto warmup: <21 five-minute candles available "
                            "(WS buffer refilling, REST/DB thin) — treating 5m "
                            "trend as neutral until data accumulates", "warn"
                        )
                else:
                    five_m_ok = indicators.is_5m_bullish(candles_5m)
                _5m_ts = time.time()
            except Exception:
                five_m_ok  = prev.get("5m_ok", True)
                _5m_ts     = prev.get("5m_ts", 0)
        else:
            five_m_ok = prev.get("5m_ok", True)
            _5m_ts    = prev.get("5m_ts", 0)

        # Phase 2: extra fields for new signal registry
        # low_24h — minimum low over last 24h from DB candles (no new REST call).
        # C §6.1d — this 1440-row read is the heaviest per-scan cost; cache the
        # value per symbol for 10 minutes and reuse within the window.
        _l24_cached = _low24h_cache.get(sym)
        if _l24_cached is not None and (time.time() - _l24_cached[1]) < _LOW24H_CACHE_TTL_SEC:
            low_24h = _l24_cached[0]
        else:
            try:
                _db_rows_24h = database.get_candles(sym, config.CANDLE_TIMEFRAME, limit=1440)
                if len(_db_rows_24h) >= 60:
                    low_24h = min(float(r.get("low") or r["close"]) for r in _db_rows_24h)
                else:
                    low_24h = min(c["low"] for c in candles) if candles else None
            except Exception:
                low_24h = None
            if low_24h is not None:
                _low24h_cache[sym] = (low_24h, time.time())

        # klines_1m — last 15 candles with OHLCV (R1 reversal + M4 micro-pullback)
        try:
            if raw and len(raw) >= 2:
                klines_1m = [
                    {"open": float(k[1]), "high": float(k[2]),
                     "low": float(k[3]), "close": float(k[4]), "volume": float(k[5])}
                    for k in raw[-15:]
                ]
            elif candles and len(candles) >= 2:
                klines_1m = candles[-15:]
            else:
                klines_1m = prev.get("klines_1m", [])
        except Exception:
            klines_1m = prev.get("klines_1m", [])

        # stoch_rsi_val — Stochastic RSI from existing closes data
        try:
            stoch_rsi_val = indicators.calc_stoch_rsi(closes)
        except Exception:
            stoch_rsi_val = None

        with _signal_cache_lock:
            _signal_cache[sym] = {
                "signals":       signals,
                "score":         score,
                "price":         closes[-1],
                "rsi_val":       rsi_display,
                "bb_ok":         bb_ok,
                "5m_ok":         five_m_ok,
                "5m_ts":         _5m_ts,
                "ts":            time.time(),
                "low_24h":       low_24h,
                "klines_1m":     klines_1m,
                "stoch_rsi_val": stoch_rsi_val,
            }
        return True

    results = []
    aborted_remaining = 0
    async with aiohttp.ClientSession() as session:
        batch_size = 5
        for i in range(0, len(approved_coins), batch_size):
            # C §6.1c — whole-pass ceiling: check elapsed at each batch boundary
            # and abort cleanly. Un-refreshed symbols keep their previous cache
            # entries (the cache is never cleared, only overwritten).
            _elapsed = time.time() - _pass_t0
            if _elapsed > _SCAN_PASS_BUDGET_SEC:
                aborted_remaining = len(approved_coins) - i
                log.warning(
                    "[SignalScanner] Pass exceeded %.0fs budget (%.1fs elapsed) — "
                    "aborting with %d/%d symbols un-refreshed (they keep their "
                    "previous cache entries)",
                    _SCAN_PASS_BUDGET_SEC, _elapsed,
                    aborted_remaining, len(approved_coins),
                )
                break
            batch = approved_coins[i:i + batch_size]
            batch_results = await asyncio.gather(
                *[_refresh_one(session, sym) for sym in batch],
                return_exceptions=True,
            )
            results.extend(batch_results)
            if i + batch_size < len(approved_coins):
                await asyncio.sleep(1.0)

    updated = sum(1 for r in results if r is True)
    with _signal_cache_lock:
        ready = sum(1 for v in _signal_cache.values() if v["score"] >= config.MIN_SIGNALS_TO_BUY)
    _abort_note = (f", ABORTED at {_SCAN_PASS_BUDGET_SEC:.0f}s budget with "
                   f"{aborted_remaining} un-refreshed" if aborted_remaining else "")
    database.log_activity(
        f"Signal scan: scanning {len(approved_coins)} symbols — "
        f"{updated}/{len(approved_coins)} updated, "
        f"{ready} at score≥{config.MIN_SIGNALS_TO_BUY}{_abort_note}",
        "info",
    )