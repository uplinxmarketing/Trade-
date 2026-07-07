"""
exit_orders.py — exchange-side exit order managers (WolfBot v0.4 §2.4-2.5 + A2).

Self-contained: imports only binance_direct (transport) and stdlib. NO
trade_engine imports — everything trade-engine-specific (positions, prices,
timeouts, tick sizes) is passed in as parameters. A later integration agent
wires this into trade_engine's monitor loop.

Two exit styles are managed per symbol (at most ONE managed order per symbol):

  * maker_tp (§2.4): a post-only LIMIT_MAKER take-profit resting on the book.
    The caller polls check_maker_tp() with the current price; once price
    touches the TP and the order still hasn't filled within `timeout_ms`,
    the order is cancelled and the caller falls back to its market-sell path.

  * oco (§2.5 + A2): a SELL OCO — LIMIT_MAKER take-profit above, a
    STOP_LOSS_LIMIT below whose limit price sits `stop_limit_buffer_pct`
    UNDER the stop trigger so it fills through fast moves (A2). Includes
    skipped-leg rescue (price sits below the trigger but the stop never
    executes → cancel and let the caller market-sell) and throttled
    cancel/replace for trailing-stop updates.

════════════════════════════════════════════════════════════════════════════
NO-DOUBLE-SELL CONTRACT (integration layer MUST follow this):

  Any local market sell of a position that has a managed exchange-side exit
  risks selling the same coins twice (exchange fills the resting order while
  the local market sell is in flight). The integration layer MUST, inside its
  per-symbol _selling critical section and BEFORE issuing ANY local market
  sell:

    1. call get_managed(symbol);
    2. if an entry exists, cancel the exchange order FIRST
       (cancel_maker_tp(symbol) / cancel via cancel_order_list for OCO —
       check_* returning 'timeout_cancelled'/'skipped_rescue' has already
       done this) and only market-sell after the cancel returns;
    3. if the cancel reveals the order already FILLED (-2011 race), treat the
       position as closed by the exchange order and DO NOT market-sell.

  Symmetrically, reconcile() only REPORTS orphans — it never sells.
════════════════════════════════════════════════════════════════════════════

Public interface (the integration agent builds against exactly this):

    get_managed(symbol) -> dict | None          # copy of the managed entry
    all_managed() -> dict[str, dict]            # copy of the whole table
    forget(symbol) -> None

    place_maker_tp(symbol, qty, tp_price) -> dict | None
    check_maker_tp(symbol, current_price, timeout_ms, now=None) -> str
        # 'none' | 'resting' | 'filled' | 'timeout_cancelled' | 'gone'
    cancel_maker_tp(symbol) -> bool

    place_oco(symbol, qty, tp_price, stop_trigger, stop_limit_buffer_pct)
        -> dict | None
    check_oco(symbol, current_price, skip_rescue_sec, now=None) -> str
        # 'none' | 'resting' | 'filled_tp' | 'filled_sl'
        # | 'skipped_rescue' | 'gone'
    replace_oco_stop(symbol, new_stop_trigger, tick_size,
                     stop_limit_buffer_pct,
                     min_replace_interval_sec=5.0, now=None) -> bool

    reconcile(positions, get_open_orders_fn) -> list[str]

Error policy: BinanceDirectError always propagates with its original context,
EXCEPT where a specific code is deliberately tolerated (-2011 "Unknown order
sent." when cancelling an order that just filled/was cancelled, and -2010
LIMIT_MAKER placement rejection, which is an expected outcome, not an error).
"""

import threading
import time

import binance_direct
from binance_direct import BinanceDirectError

# Binance error codes handled specially (never swallowed silently elsewhere).
_REJECT_CODES = (-2010, -2021)   # would immediately match / immediately trigger
_UNKNOWN_ORDER = -2011           # cancel target already filled/cancelled

# Order-list statuses (GET /api/v3/orderList → listOrderStatus).
_LIST_EXECUTING = "EXECUTING"
_LIST_ALL_DONE = "ALL_DONE"

_TERMINAL_UNFILLED = ("CANCELED", "EXPIRED", "REJECTED", "EXPIRED_IN_MATCH")

# ── Managed-order state ───────────────────────────────────────────────────────
_lock = threading.Lock()
# {symbol: {kind: 'maker_tp'|'oco', order_id | order_list_id (+ tp_order_id /
#  sl_order_id for OCO), tp_price, stop_trigger, qty, placed_ts,
#  last_replace_ts, touched_ts (maker_tp) / below_since_ts (oco)}}
_managed: dict = {}


def get_managed(symbol: str):
    """Copy of the managed-exit entry for `symbol`, or None."""
    with _lock:
        e = _managed.get(symbol)
        return dict(e) if e else None


def all_managed() -> dict:
    """Copy of the whole managed table {symbol: entry}."""
    with _lock:
        return {s: dict(e) for s, e in _managed.items()}


def forget(symbol: str) -> None:
    """Drop local tracking for `symbol` (does NOT touch the exchange)."""
    with _lock:
        _managed.pop(symbol, None)


def _now(now):
    return time.time() if now is None else now


# ── Maker TP (§2.4) ──────────────────────────────────────────────────────────

def place_maker_tp(symbol: str, qty: float, tp_price: float):
    """Rest a post-only SELL LIMIT_MAKER take-profit at `tp_price`.

    The CALLER pre-floors `qty` to the symbol's lot-size step and `tp_price`
    to its tick size (trade_engine already owns exchange_info filters); this
    module only formats them for the wire.

    Returns:
      * the full order payload dict on acceptance (also registered in the
        managed table), or
      * {"rejected": True, "code": ..., "msg": ...} when the engine rejects
        with -2010 'would immediately match and take' — the caller falls
        through to its market-sell path. Nothing is registered.
    Any other BinanceDirectError propagates.
    """
    try:
        resp = binance_direct.order_limit_maker(symbol, "SELL", qty, tp_price)
    except BinanceDirectError as e:
        if e.code in _REJECT_CODES:
            return {"rejected": True, "code": e.code, "msg": e.msg}
        raise
    with _lock:
        _managed[symbol] = {
            "kind": "maker_tp",
            "order_id": resp["orderId"],
            "tp_price": float(tp_price),
            "stop_trigger": None,
            "qty": float(qty),
            "placed_ts": time.time(),
            "last_replace_ts": 0.0,
            "touched_ts": None,
        }
    return resp


def check_maker_tp(symbol: str, current_price: float, timeout_ms: float,
                   now: float | None = None) -> str:
    """Poll the managed maker TP for `symbol`.

    Returns:
      'none'              — no managed maker TP for this symbol
      'resting'           — still open on the book
      'filled'            — FILLED (entry forgotten; caller books the exit)
      'timeout_cancelled' — price touched tp_price but the order stayed
                            unfilled for > timeout_ms after first touch →
                            order cancelled here (entry forgotten; caller
                            market-sells through its profit gate)
      'gone'              — order vanished / cancelled externally (forgotten;
                            caller must reconcile the position itself)
    `now` is injectable for tests; defaults to time.time().
    """
    entry = get_managed(symbol)
    if not entry or entry["kind"] != "maker_tp":
        return "none"
    t = _now(now)

    try:
        order = binance_direct.get_order(symbol, entry["order_id"])
    except BinanceDirectError as e:
        if e.code == _UNKNOWN_ORDER:
            forget(symbol)
            return "gone"
        raise

    status = order.get("status", "")
    if status == "FILLED":
        forget(symbol)
        return "filled"
    if status in _TERMINAL_UNFILLED:
        forget(symbol)
        return "gone"

    # Record the first touch of the TP price; timeout counts from there.
    touched_ts = entry.get("touched_ts")
    if touched_ts is None and current_price >= entry["tp_price"]:
        touched_ts = t
        with _lock:
            live = _managed.get(symbol)
            if live is not None and live.get("touched_ts") is None:
                live["touched_ts"] = t

    if touched_ts is not None and (t - touched_ts) * 1000.0 > timeout_ms:
        try:
            binance_direct.cancel_order(symbol, entry["order_id"])
        except BinanceDirectError as e:
            if e.code != _UNKNOWN_ORDER:
                raise
            # Raced a fill/cancel: re-check to avoid a double sell.
            try:
                order = binance_direct.get_order(symbol, entry["order_id"])
            except BinanceDirectError:
                forget(symbol)
                return "gone"
            forget(symbol)
            return "filled" if order.get("status") == "FILLED" else "gone"
        forget(symbol)
        return "timeout_cancelled"

    return "resting"


def cancel_maker_tp(symbol: str) -> bool:
    """Cancel the managed maker TP and forget it.

    Returns True when the cancel went through; False when nothing was managed
    or the order was already filled/cancelled (-2011 tolerated — the CALLER
    must then check the position before any market sell; see the
    NO-DOUBLE-SELL contract). Other BinanceDirectError propagates (entry kept
    so a retry is possible).
    """
    entry = get_managed(symbol)
    if not entry or entry["kind"] != "maker_tp":
        return False
    try:
        binance_direct.cancel_order(symbol, entry["order_id"])
    except BinanceDirectError as e:
        if e.code != _UNKNOWN_ORDER:
            raise
        forget(symbol)
        return False
    forget(symbol)
    return True


# ── OCO (§2.5 + A2) ──────────────────────────────────────────────────────────

def place_oco(symbol: str, qty: float, tp_price: float, stop_trigger: float,
              stop_limit_buffer_pct: float):
    """Place a SELL OCO: LIMIT_MAKER TP at `tp_price`, STOP_LOSS_LIMIT below.

    stop_limit_price = stop_trigger × (1 − stop_limit_buffer_pct/100) — the
    limit sits under the trigger so the stop leg fills through fast moves
    instead of getting skipped (A2). Caller pre-floors qty/prices to the
    symbol's filters (same contract as place_maker_tp).

    Returns the order-list payload on acceptance (registered in the managed
    table with both leg order ids), or None when the engine rejects placement
    with -2010/-2021 (e.g. TP leg would immediately match) — the caller keeps
    its local exit logic. Other BinanceDirectError propagates.
    """
    stop_limit_price = stop_trigger * (1.0 - stop_limit_buffer_pct / 100.0)
    try:
        resp = binance_direct.order_oco_sell(
            symbol, qty, tp_price, stop_trigger, stop_limit_price)
    except BinanceDirectError as e:
        if e.code in _REJECT_CODES:
            return None
        raise

    tp_order_id = sl_order_id = None
    for rep in resp.get("orderReports", []) or resp.get("orders", []):
        if rep.get("type") == "LIMIT_MAKER":
            tp_order_id = rep.get("orderId")
        elif rep.get("type") == "STOP_LOSS_LIMIT":
            sl_order_id = rep.get("orderId")

    with _lock:
        _managed[symbol] = {
            "kind": "oco",
            "order_list_id": resp["orderListId"],
            "tp_order_id": tp_order_id,
            "sl_order_id": sl_order_id,
            "tp_price": float(tp_price),
            "stop_trigger": float(stop_trigger),
            "qty": float(qty),
            "placed_ts": time.time(),
            "last_replace_ts": 0.0,
            "below_since_ts": None,
        }
    return resp


def check_oco(symbol: str, current_price: float, skip_rescue_sec: float,
              now: float | None = None) -> str:
    """Poll the managed OCO for `symbol`.

    Returns:
      'none'           — no managed OCO for this symbol
      'resting'        — list still EXECUTING
      'filled_tp'      — TP leg FILLED (forgotten; caller books the win)
      'filled_sl'      — stop leg FILLED (forgotten; caller books the stop-out)
      'skipped_rescue' — price has sat below stop_trigger for
                         > skip_rescue_sec yet the list never executed (A2
                         skipped-leg rescue) → list cancelled here (forgotten;
                         caller market-sells locally)
      'gone'           — list vanished / cancelled externally / done with no
                         filled leg (forgotten; caller reconciles)
    """
    entry = get_managed(symbol)
    if not entry or entry["kind"] != "oco":
        return "none"
    t = _now(now)

    try:
        ol = binance_direct.get_order_list(entry["order_list_id"])
    except BinanceDirectError as e:
        if e.code == _UNKNOWN_ORDER:
            forget(symbol)
            return "gone"
        raise

    status = ol.get("listOrderStatus", "")
    if status == _LIST_ALL_DONE:
        # Which leg filled? Query the legs we recorded at placement.
        for oid, verdict in ((entry.get("tp_order_id"), "filled_tp"),
                             (entry.get("sl_order_id"), "filled_sl")):
            if oid is None:
                continue
            try:
                leg = binance_direct.get_order(symbol, oid)
            except BinanceDirectError as e:
                if e.code == _UNKNOWN_ORDER:
                    continue
                raise
            if leg.get("status") == "FILLED":
                forget(symbol)
                return verdict
        forget(symbol)
        return "gone"

    if status and status != _LIST_EXECUTING:
        forget(symbol)
        return "gone"

    # Skipped-leg rescue (A2): price is below the trigger but the stop leg
    # hasn't executed. Give it skip_rescue_sec from FIRST observation below,
    # then cancel the whole list so the caller can market-sell locally.
    below_since = entry.get("below_since_ts")
    if current_price < entry["stop_trigger"]:
        if below_since is None:
            below_since = t
            with _lock:
                live = _managed.get(symbol)
                if live is not None and live.get("below_since_ts") is None:
                    live["below_since_ts"] = t
        elif t - below_since > skip_rescue_sec:
            try:
                binance_direct.cancel_order_list(
                    symbol, entry["order_list_id"])
            except BinanceDirectError as e:
                if e.code != _UNKNOWN_ORDER:
                    raise
                # List completed while we decided — next poll classifies it.
                return "resting"
            forget(symbol)
            return "skipped_rescue"
    elif below_since is not None:
        with _lock:  # price recovered above the trigger — reset the clock
            live = _managed.get(symbol)
            if live is not None:
                live["below_since_ts"] = None

    return "resting"


def replace_oco_stop(symbol: str, new_stop_trigger: float, tick_size: float,
                     stop_limit_buffer_pct: float,
                     min_replace_interval_sec: float = 5.0,
                     now: float | None = None) -> bool:
    """Trailing-stop update: cancel the managed OCO and re-place it with a
    raised stop trigger (TP price and qty unchanged). Rate-limit aware (A2):

      * at most one replace per `min_replace_interval_sec` (default 5 s) per
        symbol — each replace costs a cancel + a place against the ORDERS
        account limit;
      * only replaces when the improvement (new − current trigger) is
        ≥ max(1 × tick_size, 0.05% of the current trigger) — micro-updates
        aren't worth the order-count spend.

    NOTE (A2): Binance spot stop orders support a native `trailingDelta`
    param, which would eliminate this cancel/replace churn entirely —
    evaluate it in paper mode before adopting.

    Returns True when the OCO was replaced. False when: nothing managed,
    throttled, improvement too small, or the list turned out to be already
    executed (-2011 on cancel — entry forgotten, caller's next check_oco
    poll... is gone, so it must reconcile the position). If the re-place
    after a successful cancel is REJECTED (-2010/-2021), the entry is
    forgotten and False is returned — the position is left UNPROTECTED and
    the caller must fall back to local exits immediately. Other
    BinanceDirectError propagates.
    """
    entry = get_managed(symbol)
    if not entry or entry["kind"] != "oco":
        return False
    t = _now(now)

    if t - entry.get("last_replace_ts", 0.0) < min_replace_interval_sec:
        return False
    improvement = new_stop_trigger - entry["stop_trigger"]
    if improvement < max(tick_size, entry["stop_trigger"] * 0.0005):
        return False

    try:
        binance_direct.cancel_order_list(symbol, entry["order_list_id"])
    except BinanceDirectError as e:
        if e.code != _UNKNOWN_ORDER:
            raise
        forget(symbol)  # already executed — caller reconciles the position
        return False

    forget(symbol)
    resp = place_oco(symbol, entry["qty"], entry["tp_price"],
                     new_stop_trigger, stop_limit_buffer_pct)
    if resp is None:
        return False  # rejected re-place: position now unprotected (see doc)
    with _lock:
        live = _managed.get(symbol)
        if live is not None:
            live["last_replace_ts"] = t
    return True


# ── Reconciliation (integration hook) ────────────────────────────────────────

def reconcile(positions: list, get_open_orders_fn) -> list:
    """Cross-check the managed table against live positions and open orders.

    Args:
      positions:          list of position dicts, each with a 'symbol' key
                          (the integration layer passes its own snapshot).
      get_open_orders_fn: callable(symbol) -> list of open-order dicts
                          (normally binance_direct.get_open_orders — injected
                          so callers control weight spend; ALWAYS per-symbol,
                          weight 6 vs 80).

    Returns the list of managed symbols that need caller cleanup, because:
      * the position no longer exists locally (managed order is an orphan —
        caller should cancel it and forget()), or
      * the managed order is no longer among the symbol's open orders
        (filled or cancelled externally — caller should query final state,
        book the exit if filled, then forget()).

    This function only REPORTS — it never cancels, sells, or forgets.
    Remember the NO-DOUBLE-SELL contract before acting on the output.
    """
    pos_symbols = {p.get("symbol") for p in positions}
    stale = []
    for symbol, entry in all_managed().items():
        if symbol not in pos_symbols:
            stale.append(symbol)
            continue
        open_orders = get_open_orders_fn(symbol)
        open_ids = {o.get("orderId") for o in open_orders}
        open_list_ids = {o.get("orderListId") for o in open_orders}
        if entry["kind"] == "maker_tp":
            if entry["order_id"] not in open_ids:
                stale.append(symbol)
        else:  # oco — present if either leg still rests
            if entry["order_list_id"] not in open_list_ids:
                stale.append(symbol)
    return stale
