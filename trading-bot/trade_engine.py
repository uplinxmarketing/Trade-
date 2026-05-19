"""
Trade execution engine — unified real-time architecture.

realtime_monitor:  Called on every WebSocket trade tick (~100 ms).
                   Handles both stop-loss/take-profit exits AND buy entry.
                   Buys read from an in-memory signal cache (no I/O per tick).

signal_scanner:    Async coroutine, runs every SCAN_INTERVAL_SEC (60 s).
                   Only refreshes the signal cache from REST / DB.
                   Does NOT execute trades — buy execution is in realtime_monitor.

update_coin_signals: Called by data_collector on every 1-minute kline close
                   (WebSocket-driven). Keeps the signal cache fresh between
                   REST refreshes.
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
from connection import client, get_mode

try:
    import thread_health as _thread_health
except Exception:
    _thread_health = None

log = logging.getLogger(__name__)

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

def compute_real_breakeven_price(pos: dict, min_profit: float = 0.003) -> float:
    """Return the REAL price at which selling pos would net at least min_profit USDT.

    Uses ACTUAL deployed capital (qty * entry_price), not the requested budget.
    This matters when lot-step rounding reduces the actual quantity below what
    the budget would have bought — the position only needs to recover what was
    actually spent, plus fees, plus min_profit.

    Accounts for quantity rounding loss, buy fee already paid, and sell fee.
    Used as the sell trigger in both realtime_monitor and _sell_monitor_loop so
    trigger and profit gate use identical math and never disagree.
    """
    try:
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


def _binance_request(url: str, timeout: float = 3.0, source: str = "unknown"):
    """Unified Binance REST caller that captures FULL error context.

    Returns (success, data, headers, latency_ms).
    On failure records the exact URL + Binance JSON body in the diag log so we
    can see which call site fails and what Binance is actually complaining about.
    """
    import urllib.request as _ur_b
    import urllib.error as _ue_b
    import traceback as _tb_b

    if _check_circuit_breaker():
        return (False, None, {}, 0.0)
    if _backoff_should_skip(source):
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
            pass
        if he.code == 400:
            _trip_circuit_breaker()
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
}

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
        account = client.get_account()
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

    # In fixed/per_coin mode return the full configured amount.
    # _check_balance / can_execute_buy will reject the buy if free USDT is insufficient —
    # that gives a clean "have X, need Y" message rather than silently trading a partial amount.
    if mode in ("fixed", "per_coin"):
        return round(base, 2)
    # For capped mode cap to 90% so a tiny buffer remains for fees.
    return round(min(base, effective_free * 0.9), 2)


# ── Cooldown helpers ────────────────────────────────────────────────────

def _refresh_risk_params():
    """Read stop_loss_enabled/pct, take_profit_pct and new exit flags from strategy.json."""
    global _user_tp_mult, _take_profit_mult, _stop_loss_mult, _take_profit_enabled, _smart_hold_enabled, _trailing_stop_pct, _BUFFER_OVERRIDES
    strategy = _load_strategy()
    tp_pct = float(strategy.get("take_profit_pct", 0.1))   # e.g. 0.5 → 0.5%
    sl_pct = float(strategy.get("stop_loss_pct",   2.0))   # e.g. 2.0 → 2.0%
    sl_on  = bool(strategy.get("stop_loss_enabled", True))
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
    if force_fresh and symbol:
        try:
            _fresh = _fetch_batch_prices([symbol])
            _fp = _fresh.get(symbol, 0) if _fresh else 0
            if _fp > 0:
                price = _fp
                _rest_px[symbol] = _fp
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
    # 0.05% slippage cushion — realistic for liquid coins, prevents zero/negative trades
    slippage_buf  = actual_cost * 0.0005
    strategy   = _load_strategy()
    tp_enabled = bool(strategy.get("take_profit_enabled", True))
    tp_pct     = float(strategy.get("take_profit_pct", 0.25))
    if tp_enabled:
        min_profit = max(0.003 + slippage_buf, actual_cost * (tp_pct / 100.0))
    else:
        min_profit = 0.003 + slippage_buf
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
            if hasattr(client, "_balances"):
                with client._lock:
                    current = client._balances.get("USDT", 0.0)
                # Restore from Supabase when:
                #   a) local balance is zero / negative
                #   b) no open positions in SQLite (fresh deploy / no volume)
                #   c) balance is at the ENV starting default AND Supabase has a
                #      meaningfully different value (paper_state was never persisted)
                _starting_usdt = float(os.getenv("STARTING_PAPER_USDT", "10000.0"))
                _at_default    = abs(current - _starting_usdt) < 0.01
                _supa_differs  = abs(usdt - current) > 1.0
                if current <= 0 or not rows or (_at_default and _supa_differs):
                    with client._lock:
                        client._balances["USDT"] = usdt
                        snapshot = dict(client._balances)
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
            acc = client.get_account()
            tracked_symbols = {p["symbol"] for p in _positions}
            recovered_syms = []
            now_ts = datetime.now(timezone.utc).isoformat()

            for b in acc["balances"]:
                asset  = b["asset"]
                free   = float(b["free"])
                locked = float(b["locked"])
                total  = free + locked
                if asset in ("USDT", "BNB", "BUSD", "USDC") or total <= 0:
                    continue
                sym = asset + "USDT"
                if sym in tracked_symbols:
                    continue

                # Fetch current price — try cached REST price first, then live ticker
                price = _rest_px.get(sym, 0) or 0
                if price <= 0:
                    try:
                        ticker = client.get_symbol_ticker(symbol=sym)
                        price  = float(ticker.get("price", 0) or 0)
                    except Exception:
                        pass

                # Use only free balance — locked qty can't be market-sold and
                # would cause -2010 "insufficient balance" on Binance.
                sell_qty = free if free > 0 else total
                value = sell_qty * price if price > 0 else 0

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
                else:
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
    if hasattr(client, "_balances") and _positions:
        changed = False
        for pos in _positions:
            sym  = pos["symbol"]
            coin = sym[:-4]  # strip USDT
            qty  = float(pos.get("quantity", 0))
            if qty <= 0:
                continue
            with client._lock:
                current = client._balances.get(coin, 0.0)
                if current < qty * 0.99:
                    client._balances[coin] = qty
                    changed = True
                    print(f"[TradeEngine] Synced paper balance: {coin}={qty:.8f} (was {current:.8f})")
        if changed:
            database.save_paper_state(dict(client._balances))


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
            if hasattr(client, "_balances"):
                with client._lock:
                    return float(client._balances.get("USDT", 0.0))
            return float(os.getenv("STARTING_PAPER_USDT", "10000.0"))
        # Live mode: query Binance spot wallet — use free balance only (tradeable)
        acc = client.get_account()
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
        acc = client.get_account()
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
        acc = client.get_account()
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
    try:
        if int(score or 0) < 3:
            return
    except Exception:
        return
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


# ── BTC market regime filter ───────────────────────────────────────────────────
_market_regime_cache: dict = {"ts": 0.0, "regime": "unknown", "details": {}}
_MARKET_REGIME_TTL_SEC = 120.0

def _fetch_btc_1h_klines() -> Optional[List[dict]]:
    url = "https://api.binance.com/api/v3/klines?symbol=BTCUSDT&interval=1h&limit=24"
    ok, data, _, _ = _binance_request(url, timeout=4.0, source="btc_klines")
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

def _fetch_1m_klines_rc(symbol: str, limit: int = 5) -> Optional[List[dict]]:
    url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval=1m&limit={limit}"
    ok, data, _, _ = _binance_request(url, timeout=3.0, source="reversal_klines")
    if not ok or not isinstance(data, list):
        return None
    try:
        return [{"open": float(k[1]), "high": float(k[2]), "low": float(k[3]),
                 "close": float(k[4]), "volume": float(k[5])} for k in data]
    except Exception:
        return None

def is_reversal_confirmed(symbol: str) -> tuple:
    """Returns (confirmed: bool, reason: str). Requires green candle + volume surge."""
    now = time.time()
    cached_rc = _reversal_cache.get(symbol)
    if cached_rc and (now - cached_rc["ts"]) < _REVERSAL_TTL_SEC:
        return cached_rc["confirmed"], cached_rc.get("reason", "cached")
    klines = _fetch_1m_klines_rc(symbol, limit=5)
    if not klines or len(klines) < 5:
        _reversal_cache[symbol] = {"ts": now, "confirmed": False, "reason": "no_data"}
        return False, "no_data"
    last = klines[-1]
    if last["close"] <= last["open"]:
        _reversal_cache[symbol] = {"ts": now, "confirmed": False, "reason": "last_candle_red"}
        return False, "last_candle_red"
    avg_prev_vol = sum(c["volume"] for c in klines[:-1]) / 4.0
    if avg_prev_vol > 0 and last["volume"] < avg_prev_vol * 1.15:
        _reversal_cache[symbol] = {"ts": now, "confirmed": False, "reason": "weak_volume"}
        return False, "weak_volume"
    recent_low = min(c["low"] for c in klines[:-1])
    if last["close"] <= recent_low:
        _reversal_cache[symbol] = {"ts": now, "confirmed": False, "reason": "still_below_lows"}
        return False, "still_below_lows"
    _reversal_cache[symbol] = {"ts": now, "confirmed": True, "reason": "ok"}
    return True, "ok"

def _floor_qty(qty: float, symbol: str = "", decimals: int = 8) -> float:
    """Floor quantity to the symbol's LOT_SIZE stepSize to avoid Binance -1111 errors.
    Falls back to flat 8 decimal places if symbol is unknown or API call fails."""
    if symbol:
        step = _lot_step_cache.get(symbol)
        if step is None:
            try:
                info = client.get_symbol_info(symbol)
                for f in (info or {}).get("filters", []):
                    if f.get("filterType") == "LOT_SIZE":
                        step = float(f["stepSize"])
                        break
            except Exception:
                step = 0.0
            _lot_step_cache[symbol] = step or 0.0
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
    """
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


def update_coin_signals(symbol: str, closes: list, volumes: list):
    """Update the signal cache on every kline close (WebSocket-driven).

    Builds minimal candle dicts (high=low=close) so OBV works correctly.
    ATR signal will be False (atr_is_tradeable returns False when ATR=0) —
    that is safe: the REST scan (_refresh_one) sets accurate ATR/BB/5m values
    every 60 s and they are preserved here via the prev cache entry.
    """
    if len(closes) < 16:  # minimum for RSI-14 to produce a valid value at [-2]
        return
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
            _signal_cache[symbol] = {
                "signals":  signals,
                "score":    score,
                "price":    closes[-1],
                "rsi_val":  rsi_display,
                "bb_ok":    prev.get("bb_ok",  False),  # preserved from last REST scan
                "5m_ok":    prev.get("5m_ok",  False),  # preserved from last REST scan
                "ts":       time.time(),
            }
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
            _acc = client.get_account()
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
        client.update_price(sym, price)
    except Exception:
        pass

    # Paper mode: self-heal coin balance if it went missing after a restart.
    # open position record is the source of truth for what we own.
    if mode != "live" and hasattr(client, "_balances"):
        coin = sym[:-4]
        with client._lock:
            current_coin_bal = client._balances.get(coin, 0.0)
            if current_coin_bal < qty * 0.99:
                client._balances[coin] = qty
                snapshot = dict(client._balances)
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
    if reason not in ("stop-loss", "force-sell", "manual", "user-initiated"):
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
                    f"would not net profit (incl. slippage buffer). "
                    f"Position held — will retry on next favourable tick (log throttled 60s).",
                    "warn"
                )
            _sell_last_failed_ts[sym] = time.time()
            _sell_last_failed_reason[sym] = reason
            return  # exit before placing order
        # _profitable_sell_check with force_fresh=True updated _rest_px[sym] — use it
        # as the paper-mode execution price so the fill reflects current market
        price = _rest_px.get(sym, 0) or price

    pos["_sell_gate_done_ts"] = time.time()
    pos["_sell_binance_start_ts"] = time.time()
    try:
        # Paper mode: pass the trigger price directly so concurrent WebSocket/REST
        # updates cannot cause the sell to execute at the wrong price.
        if _is_paper:
            result = client.order_market_sell(symbol=sym, quantity=qty, price=price)
        else:
            result = client.order_market_sell(symbol=sym, quantity=qty)
        pos["_sell_binance_done_ts"] = time.time()
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
                    database.log_activity(
                        f"[GHOST CHECK FAILED] {sym}: -2010 but balance check inconclusive — retrying next cycle", "warn"
                    )
                    log_diag_issue("sell_monitor", "warn",
                                   f"{sym}: -2010 balance check failed, will retry", detail=err_str[:400])
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
        fill_price = float(fills[0].get("price", price)) if fills else price
        raw_quote  = float(result.get("cummulativeQuoteQty") or 0)
        budget     = pos["budget_usdt"]
        # Use ACTUAL deployed capital (qty * entry_price) as cost basis, not the
        # requested budget. Binance buys often fill slightly under budget (lot-step
        # rounding) and the unspent change stays in USDT — subtracting full budget
        # from proceeds creates a phantom loss equal to the unspent amount.
        actual_cost = float(pos["quantity"]) * float(pos["entry_price"])

        if mode == "live":
            sell_fee, fee_asset = _fills_fee_usdt(fills, raw_quote * _fee_rate)
            if fee_asset in ("USDT", "BUSD", "USDC"):
                usdt_returned = raw_quote
            else:
                usdt_returned = raw_quote - sell_fee
            buy_fee = float(pos.get("buy_fee_usdt") or actual_cost * _fee_rate)
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

    # Post-fill integrity check: if a "take-profit" sell actually lost money
    # (market-order slippage filled below breakeven), relabel it so the trade
    # history is honest, and apply a 30-min cooldown to avoid re-buying immediately.
    if net_profit < 0 and reason == "take-profit":
        reason = "slippage-loss"
        _loss_cooldown[sym] = time.time() + _LOSS_COOLDOWN_SEC
        database.log_activity(
            f"SLIPPAGE-LOSS {sym}: fill at ${fill_price:.6f} returned ${net_profit:.4f} USDT "
            f"— relabeled 'slippage-loss', cooldown {_LOSS_COOLDOWN_SEC // 60}min", "warn"
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

    # Remove position from memory and DB immediately — sell is confirmed on Binance.
    # Supabase sync and learning run after so a slow network never delays cleanup.
    if pos.get("id"):
        try:
            database.delete_position(pos["id"])
        except Exception:
            pass
    with _positions_lock:
        _positions[:] = [p for p in _positions if p.get("id") != pos.get("id")]
    _rebuild_pos_index()
    _pos_peaks.pop(sym, None)

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

    try:
        database.log_trade(trade_record,
                           target_crossed_to_trigger_ms=_target_to_trigger_ms,
                           trigger_to_filled_ms=_trigger_to_filled_ms,
                           target_crossed_ts=_target_crossed_iso)
        try:
            import supabase_sync
            # Paper mode: read balance from memory (instant, no network call).
            # Live mode: skip the get_account() REST call — Supabase sync gets
            # an estimate from net_profit instead, avoiding a 10s blocking call
            # on the sell worker thread.
            if get_mode() != "live" and hasattr(client, "_balances"):
                with client._lock:
                    usdt_now = float(client._balances.get("USDT", 0.0))
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
    except Exception as te:
        database.log_activity(f"log_trade error ({sym}): {te}", "warn")

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

    # Fast pre-check: any coin signalling BUY? (no lock needed for scalar read)
    with _signal_cache_lock:
        any_ready = any(v["score"] >= config.MIN_SIGNALS_TO_BUY for v in _signal_cache.values())
        cache_size = len(_signal_cache)

    if not any_ready:
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
            else:
                with _signal_cache_lock:
                    snap = dict(_signal_cache)
                top = sorted(snap.items(), key=lambda x: -x[1]["score"])[:5]
                detail = " | ".join(
                    f"{s}:score={v['score']} RSI={v['signals'].get('rsi',False)} trend={v['signals'].get('trend',False)}"
                    for s, v in top
                )
                database.log_activity(
                    f"Buy check: {cache_size} coins, none at score≥{config.MIN_SIGNALS_TO_BUY} — top5: {detail}", "info"
                )
        return

    # Throttle: don't call get_account() faster than every 1 s
    now = time.time()
    if now - _last_buy_check < 0.1:
        return
    _last_buy_check = now

    strategy = _load_strategy()
    if not strategy.get("trading_active", True):
        database.log_activity("Buy check: trading_active=False — bot is paused", "warn")
        return

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
    usdt_balance = _get_usdt_balance()
    ts_now = datetime.now(_tz.utc).isoformat()
    mode   = get_mode()

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

    # Read mandatory signal thresholds once (hot-reloadable from strategy.json)
    mandatory_enabled = bool(strategy.get("mandatory_signals_enabled", True))
    rsi_threshold     = float(strategy.get("rsi_buy_threshold", 40.0))

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
            _record_rejection(sym, cached["score"], "loss_cooldown", f"{rem}s remaining")
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
                _record_rejection(sym, score, _dec["reason"],
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
                    log_diag_issue(
                        "signal_shadow", "warn",
                        f"{sym}: old=True new={_new_dec['allowed']} reason={_new_dec['reason']}",
                    )
            except Exception as _shadow_exc:
                log_diag_issue("signal_shadow", "warn", f"Shadow eval failed for {sym}: {_shadow_exc}")

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

        client.update_price(sym, price)

        buy_cfg = {**approved[sym], "symbol": sym, "budget_usdt": budget}
        allowed, reason = can_execute_buy(buy_cfg, client)
        if not allowed:
            database.log_activity(f"{sym}: buy skipped — {reason}", "info")
            continue

        if sym in _bad_symbols:
            database.log_activity(f"{sym}: buy skipped — market closed/delisted (blacklisted this session)", "warn")
            continue

        # Binance minimum notional is $10 for most spot pairs — reject early
        # so we don't waste an API call and get a cryptic -1013 error.
        if mode == "live" and budget < 10.0:
            database.log_activity(
                f"{sym}: buy skipped — budget ${budget:.2f} < $10 Binance minimum notional "
                f"(increase trade size in Settings)", "warn"
            )
            continue

        # ── Fresh signal re-check before committing ────────────────────────────
        # Cache can be 30-60s stale. A single REST kline fetch verifies signals
        # are still valid at execution time — eliminates stale-cache buys.
        try:
            import urllib.parse as _up_kb
            _fresh_url = (
                f"https://api.binance.com/api/v3/klines?"
                + _up_kb.urlencode({"symbol": sym, "interval": "1m", "limit": 30})
            )
            _ok_kb, _raw, _, _ = _binance_request(_fresh_url, timeout=2.0, source="klines_pre_buy")
            if not _ok_kb or not isinstance(_raw, list):
                _raw = []
            _fresh_closes  = [float(k[4]) for k in _raw]
            _fresh_volumes = [float(k[5]) for k in _raw]
            _fresh_candles = [
                {"high": float(k[2]), "low": float(k[3]),
                 "close": float(k[4]), "volume": float(k[5])}
                for k in _raw
            ]
            _fresh_sigs  = evaluate_signals(_fresh_candles)
            _fresh_score = sum(_fresh_sigs.values())
            if _fresh_score < min_sigs:
                cache_age = round(time.time() - cached.get("ts", 0), 1)
                database.log_activity(
                    f"[SKIP] {sym}: fresh re-check FAILED — score {_fresh_score}/6 < {min_sigs} "
                    f"(cache had {score}/6, age={cache_age}s)", "warn"
                )
                _record_rejection(sym, score, "stale_signals", f"fresh={_fresh_score} cache={score} age={cache_age}s")
                continue
            _live_price = _fresh_closes[-1]
            if price > 0 and abs(_live_price - price) / price > 0.005:
                database.log_activity(
                    f"[SKIP] {sym}: price moved {(_live_price - price)/price*100:.2f}% "
                    f"since cache (${price:.4f} → ${_live_price:.4f}) — skipping", "warn"
                )
                continue
            price = _live_price
            client.update_price(sym, price)
        except Exception as _fresh_e:
            database.log_activity(
                f"[SKIP] {sym}: fresh re-check failed ({_fresh_e}) — skipping for safety", "warn"
            )
            continue

        # ── Reversal confirmation ──────────────────────────────────────────────
        if bool(strategy.get("reversal_confirmation_enabled", True)):
            _rev_ok, _rev_reason = is_reversal_confirmed(sym)
            if not _rev_ok:
                _record_rejection(sym, score, "no_reversal_confirmed", _rev_reason)
                continue

        # Atomic claim — prevents concurrent buy for the same symbol
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

        try:
            result = client.order_market_buy(symbol=sym, quoteOrderQty=budget)
        except Exception as e:
            with _buying_lock:
                _buying.discard(sym)
                _buying_ts.pop(sym, None)
            err_str = str(e)
            print(f"[RealtimeBuy] BUY failed {sym}: {e}")
            database.log_activity(f"{sym}: BUY failed — {e}", "error")
            if "-1013" in err_str or "Market is closed" in err_str:
                _bad_symbols.add(sym)
                database.log_activity(f"{sym}: blacklisted — market closed/delisted on Binance", "warn")
            continue

        buy_fills  = result.get("fills", [])
        fill_price = float(buy_fills[0].get("price", price)) if buy_fills else price
        qty        = float(result.get("executedQty", 0))
        if qty <= 0:
            with _buying_lock:
                _buying.discard(sym)
                _buying_ts.pop(sym, None)
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
        pos_record = {
            "symbol":             sym,
            "entry_price":        fill_price,
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
            f"| qty={qty:.6f} | cache_age:{cache_age}s"
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
        # Rate-limit retries: stop-loss retries immediately, take-profit waits 5s.
        last_fail = _sell_last_failed_ts.get(sym, 0)
        if last_fail:
            _cooldown = (_SELL_RETRY_COOLDOWN_LOSS
                         if _sell_last_failed_reason.get(sym, "") in ("stop-loss", "force-sell")
                         else _SELL_RETRY_COOLDOWN_PROFIT)
            if (now - last_fail) < _cooldown:
                continue
        # Minimum hold: never sell within 10 s of buy — prevents race-condition flips
        opened_ts = pos.get("opened_at_ts", 0)
        if opened_ts > 0 and (now - opened_ts) < _MIN_HOLD_SEC:
            continue
        entry  = pos["entry_price"]
        # Real breakeven trigger: (budget + buy_fee + min_profit) / (qty × (1 - sell_fee)).
        # Only fires when a sell would actually net profit — no phantom triggers for
        # BTC/BNB where lot-step rounding raises the true break-even above simple BEP.
        # _profitable_sell_check in _do_execute_sell is the final safety net at execution.
        real_target = compute_real_breakeven_price(pos)
        if real_target <= 0:
            _bep_m = pos.get("breakeven_mult_at_buy") or _get_breakeven_mult(entry, sym)
            real_target = entry * _bep_m  # fallback for incomplete positions
        stop = entry * _stop_loss_mult
        if _take_profit_enabled:
            target = max(real_target, entry * _user_tp_mult)
        else:
            target = real_target

        if price >= real_target:
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
                    trail_stop = peak * (1.0 - _trailing_stop_pct / 100.0)
                    sell_reason: Optional[str] = None
                    if price <= trail_stop:
                        sell_reason = "smart-hold-trail"
                    else:
                        with _signal_cache_lock:
                            score = _signal_cache.get(sym, {}).get("score", 0)
                        if score < 3:
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
        elif _stop_loss_mult < 1.0 and price <= stop:
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
_REST_PX_TTL = 5.0   # refetch REST prices every 5 s when WebSocket is down
_sell_monitor_last_rest_ts: float = 0.0   # rate-limits sell-monitor REST refresh to 2s


def _fetch_batch_prices(symbols: list) -> Dict[str, float]:
    """Batch ticker price fetch via _binance_request (source-tagged, circuit-broken).
    Uses single-symbol endpoint (weight=1) when only one symbol requested,
    batch endpoint (weight=2) otherwise."""
    import urllib.parse as _up2
    if not symbols:
        return {}
    symbols = list(symbols)
    if len(symbols) == 1:
        # Single-symbol endpoint: weight=1 vs batch weight=2
        url = f"https://api.binance.com/api/v3/ticker/price?symbol={symbols[0]}"
        ok, data, _, _ = _binance_request(url, timeout=3.0, source="batch_prices")
        if not ok or not isinstance(data, dict):
            return {}
        try:
            px = float(data.get("price", 0) or 0)
            return {data["symbol"]: px} if px > 0 and data.get("symbol") else {}
        except Exception:
            return {}
    _syms_json = json.dumps(symbols, separators=(',', ':'))
    _encoded   = _up2.quote(_syms_json, safe='')
    url = f"https://api.binance.com/api/v3/ticker/price?symbols={_encoded}"
    ok, data, _, _ = _binance_request(url, timeout=3.0, source="batch_prices")
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


def _held_position_price_refresher():
    """Dedicated REST refresher for held (open) positions every 2 s.
    Critically important for low-WS-volume coins (ENJ, SUSHI, PENDLE, etc.)
    that rarely generate @trade events — without this they go minutes stale."""
    import data_collector as _dc_hr
    global _held_refresher_hb_log_ts
    consecutive_errors = 0

    try:
        database.log_activity("Held-position price refresher started (5s interval)", "info")
    except Exception:
        pass

    while True:
        try:
            if _thread_health:
                _thread_health.heartbeat("held_price_refresher")
        except Exception:
            pass
        try:
            with _positions_lock:
                held_syms = list({p.get("symbol") for p in _positions if p.get("symbol")})
            if not held_syms:
                time.sleep(5.0)
                continue

            fetched = _fetch_batch_prices(held_syms)
            now_ts = time.time()
            if fetched:
                for s, px in fetched.items():
                    _dc_hr.prices[s] = px
                    _last_ws_price_ts[s] = now_ts
                    _rest_px[s] = px
                consecutive_errors = 0

                # 60s heartbeat log
                if now_ts - _held_refresher_hb_log_ts >= 60.0:
                    _held_refresher_hb_log_ts = now_ts
                    try:
                        database.log_activity(
                            f"[HeldRefresher] OK — {len(fetched)}/{len(held_syms)} symbols refreshed: "
                            + ", ".join(f"{s}={v:.6f}" for s, v in list(fetched.items())[:5]),
                            "info"
                        )
                    except Exception:
                        pass
            else:
                consecutive_errors += 1
                if consecutive_errors <= 3 or consecutive_errors % 30 == 0:
                    try:
                        database.log_activity(
                            f"[HeldRefresher] REST fetch returned empty for {held_syms} "
                            f"(attempt {consecutive_errors})", "warn"
                        )
                    except Exception:
                        pass
        except Exception as _e:
            consecutive_errors += 1
            if consecutive_errors <= 3 or consecutive_errors % 30 == 0:
                try:
                    database.log_activity(
                        f"[HeldRefresher] {type(_e).__name__}: {_e} ({consecutive_errors} consecutive errors)",
                        "warn"
                    )
                except Exception:
                    pass
                log_diag_issue(
                    "price_refresher", "error",
                    f"Held refresher failed ({consecutive_errors} consecutive)",
                    detail=f"{type(_e).__name__}: {_e}",
                )
        time.sleep(max(5.0, min(30.0, 5.0 + consecutive_errors * 1.0)))


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

            # ── REST price refresh BEFORE trigger evaluation (rate-limited to 2s) ─
            # Fetch current prices for all held positions via a single batch REST
            # call so trigger decisions use prices ≤3s old regardless of WS activity.
            # Rate-limited to once every 2s so 0.25s iterations don't flood Binance.
            global _sell_monitor_last_rest_ts
            _sm_now_t = time.time()
            if snap and (_sm_now_t - _sell_monitor_last_rest_ts) >= 2.0:
                _sm_syms = list({p["symbol"] for p in snap})
                _sm_fetched = _fetch_batch_prices(_sm_syms)
                if _sm_fetched:
                    _sm_ts = time.time()
                    for _s, _p in _sm_fetched.items():
                        _dc.prices[_s]        = _p
                        _rest_px[_s]          = _p
                        _last_ws_price_ts[_s] = _sm_ts
                    _sell_monitor_last_rest_ts = _sm_ts

            # ── Watchdog: force-clear _selling entries stuck > 20 s ──────────
            now_wd = time.time()
            with _selling_lock:
                stuck = [s for s, t in list(_selling_ts.items()) if now_wd - t > 20]
            for s in stuck:
                with _selling_lock:
                    _selling.discard(s)
                    _selling_ts.pop(s, None)
                try:
                    database.log_activity(
                        f"Sell monitor: force-cleared stuck guard for {s} (>20 s)", "warn"
                    )
                except Exception:
                    pass

            # Always refresh TP/SL multipliers — settings can change at any time.
            _refresh_risk_params()

            # Build price dict: start with WebSocket, then override with REST.
            # _rest_px is maintained by _price_refresher_loop — no I/O here.
            prices = dict(_dc.prices)
            for sym2, p2 in _rest_px.items():
                if p2 > 0:
                    prices[sym2] = p2   # REST always overrides stale WS prices

            # Signal-cache is the final fallback — fills gaps when both WS and
            # REST are unavailable for a symbol (e.g. first 30 s after redeploy).
            with _signal_cache_lock:
                sc_snap = dict(_signal_cache)
            for pos in snap:
                s = pos["symbol"]
                if prices.get(s, 0) <= 0:
                    sc_p = sc_snap.get(s, {}).get("price", 0)
                    if sc_p and sc_p > 0:
                        prices[s] = sc_p

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
                # Minimum hold: never sell within 10 s of buy
                opened_ts3 = pos.get("opened_at_ts", 0)
                if opened_ts3 > 0 and (now_monitor - opened_ts3) < _MIN_HOLD_SEC:
                    continue
                entry  = pos["entry_price"]
                real_target3 = compute_real_breakeven_price(pos)
                if real_target3 <= 0:
                    _bep_m3 = pos.get("breakeven_mult_at_buy") or _get_breakeven_mult(entry, sym)
                    real_target3 = entry * _bep_m3
                stop = entry * _stop_loss_mult
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
                        f"real_target={real_target3:.6f} "
                        f"above_target={price >= real_target3} "
                        f"cooldown={_cd_rem:.0f}s",
                        "info"
                    )

                sell_reason3: Optional[str] = None

                if price >= real_target3:
                    if _smart_hold_enabled and price >= target and target > real_target3:
                        _pos_peaks[sym] = max(_pos_peaks.get(sym, target), price)
                        peak = _pos_peaks[sym]
                        trail_stop = peak * (1.0 - _trailing_stop_pct / 100.0)
                        if price <= trail_stop:
                            sell_reason3 = "smart-hold-trail"
                        else:
                            with _signal_cache_lock:
                                score2 = _signal_cache.get(sym, {}).get("score", 0)
                            if score2 < 3:
                                sell_reason3 = "take-profit"
                    else:
                        sell_reason3 = "take-profit"
                elif _stop_loss_mult < 1.0 and price <= stop:
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


# ── Process 2: signal scanner (async, refreshes cache every SCAN_INTERVAL_SEC) ─

async def signal_scanner(prices: dict):
    """
    Async coroutine — runs every SCAN_INTERVAL_SEC (60 s).
    Refreshes the signal cache from REST, then immediately attempts buys.
    This is the primary buy trigger — WebSocket callbacks are a fast-path
    supplement, but buys MUST fire even when WebSocket is slow or disconnected.
    """
    _signal_scanner_health["interval_sec"] = float(config.SCAN_INTERVAL_SEC)
    while True:
        _t0_scan = time.time()
        try:
            await _refresh_signal_cache()
            # Trigger buy checks right after refreshing — don't wait for WebSocket
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, _check_buys_from_cache, dict(prices))
        except Exception as e:
            print(f"[SignalScanner] Unexpected error: {e}")
            log_diag_issue(
                "signal_scanner", "error",
                f"Scan iteration failed: {type(e).__name__}",
                detail=str(e),
            )
        finally:
            _signal_scanner_health["last_refresh_ts"]  = time.time()
            _signal_scanner_health["last_duration_ms"] = round((time.time() - _t0_scan) * 1000, 1)
            _signal_scanner_health["scans_completed"] += 1

        await asyncio.sleep(config.SCAN_INTERVAL_SEC)



_KLINE_BASES = [
    # Direct Binance API first — most accurate, real-time prices (no CDN delay)
    "https://api.binance.com",
    "https://api1.binance.com",
    "https://api2.binance.com",
    "https://api3.binance.com",
    # CDN fallback — may serve slightly stale data but works when API is geo-blocked
    "https://data-api.binance.vision",
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
    """
    import aiohttp
    strategy = _load_strategy()
    if not strategy:
        return

    approved_coins = [
        c["symbol"]
        for c in strategy.get("approved_coins", [])
        if c.get("approved")
    ]
    if not approved_coins:
        return

    async def _refresh_one(session, sym: str) -> bool:
        import data_collector as _dc
        MIN = 16          # matches _dc._MIN_CANDLES — enough for RSI to fire
        closes = volumes = candles = None

        # 1. Try Binance REST (fastest, most data — includes full OHLC for ATR)
        try:
            raw     = await _fetch_klines(session, sym)
            closes  = [float(k[4]) for k in raw]
            volumes = [float(k[5]) for k in raw]
            candles = [
                {"high": float(k[2]), "low": float(k[3]),
                 "close": float(k[4]), "volume": float(k[5])}
                for k in raw
            ]
        except Exception:
            pass

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
                five_m_ok  = indicators.is_5m_bullish(candles_5m)
                _5m_ts     = time.time()
            except Exception:
                five_m_ok  = prev.get("5m_ok", True)
                _5m_ts     = prev.get("5m_ts", 0)
        else:
            five_m_ok = prev.get("5m_ok", True)
            _5m_ts    = prev.get("5m_ts", 0)

        # Phase 2: extra fields for new signal registry
        # low_24h — minimum low over last 24h from DB candles (no new REST call)
        try:
            _db_rows_24h = database.get_candles(sym, config.CANDLE_TIMEFRAME, limit=1440)
            if len(_db_rows_24h) >= 60:
                low_24h = min(float(r.get("low") or r["close"]) for r in _db_rows_24h)
            else:
                low_24h = min(c["low"] for c in candles) if candles else None
        except Exception:
            low_24h = None

        # klines_1m — last 5 candles with OHLCV for R1 reversal check
        try:
            if raw and len(raw) >= 5:
                klines_1m = [
                    {"open": float(k[1]), "high": float(k[2]),
                     "low": float(k[3]), "close": float(k[4]), "volume": float(k[5])}
                    for k in raw[-5:]
                ]
            elif candles and len(candles) >= 5:
                klines_1m = candles[-5:]
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
    async with aiohttp.ClientSession() as session:
        batch_size = 5
        for i in range(0, len(approved_coins), batch_size):
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
    database.log_activity(
        f"Signal cache refreshed: {updated}/{len(approved_coins)} coins updated, "
        f"{ready} at score≥{config.MIN_SIGNALS_TO_BUY}",
        "info",
    )