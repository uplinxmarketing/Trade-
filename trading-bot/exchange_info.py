import urllib.request
import json
import math
import time
import logging
from decimal import Decimal, ROUND_DOWN
from threading import Lock

log = logging.getLogger(__name__)

_exchange_info_cache = {
    "last_fetched_ts": 0.0,
    "symbol_filters": {},
}
_exchange_info_lock = Lock()
_EXCHANGE_INFO_TTL_SEC = 3600


def _fetch_exchange_info():
    url = "https://api.binance.com/api/v3/exchangeInfo"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "WolfBot/1.0"})
        with urllib.request.urlopen(req, timeout=10.0) as r:
            data = json.loads(r.read())
    except Exception as e:
        log.warning("Failed to fetch exchangeInfo: %s", e)
        return None

    filters = {}
    for sym_info in data.get("symbols", []):
        symbol = sym_info.get("symbol")
        if not symbol:
            continue
        step_size = 0.0
        min_qty = 0.0
        min_notional = 0.0
        for f in sym_info.get("filters", []):
            ft = f.get("filterType")
            if ft == "LOT_SIZE":
                try:
                    step_size = float(f.get("stepSize", 0))
                    min_qty = float(f.get("minQty", 0))
                except (ValueError, TypeError):
                    pass
            elif ft in ("MIN_NOTIONAL", "NOTIONAL"):
                try:
                    val = f.get("minNotional") or f.get("notional")
                    if val:
                        min_notional = float(val)
                except (ValueError, TypeError):
                    pass
        if step_size > 0:
            filters[symbol] = {"step_size": step_size, "min_qty": min_qty, "min_notional": min_notional}
    return filters


def get_symbol_filters(symbol: str) -> dict:
    now = time.time()
    with _exchange_info_lock:
        age = now - _exchange_info_cache["last_fetched_ts"]
        if age > _EXCHANGE_INFO_TTL_SEC or not _exchange_info_cache["symbol_filters"]:
            new_filters = _fetch_exchange_info()
            if new_filters:
                _exchange_info_cache["symbol_filters"] = new_filters
                _exchange_info_cache["last_fetched_ts"] = now
                log.info("Refreshed exchangeInfo: %d symbols cached", len(new_filters))
        return _exchange_info_cache["symbol_filters"].get(symbol, {})


def round_quantity_to_step(quantity: float, step_size: float) -> float:
    if step_size <= 0 or quantity <= 0:
        return quantity
    # Use Decimal via str() to avoid float artifacts that cause Binance -1111.
    # Example: 6.14 / 0.01 in float gives 6.140000000000001; Decimal gives 6.14.
    q = Decimal(str(quantity))
    s = Decimal(str(step_size))
    multiples = (q / s).to_integral_value(rounding=ROUND_DOWN)
    return float((multiples * s).quantize(s))


def compute_sell_quantity(symbol: str, requested_qty: float):
    """Returns (sellable_qty, leftover_qty, reason_code)."""
    filters = get_symbol_filters(symbol)
    if not filters:
        log.warning("No exchangeInfo filters for %s, using raw quantity %s", symbol, requested_qty)
        return requested_qty, 0.0, "no_filters"

    step = filters.get("step_size", 0)
    min_qty = filters.get("min_qty", 0)

    if step <= 0:
        return requested_qty, 0.0, "no_step"

    rounded = round_quantity_to_step(requested_qty, step)
    leftover = requested_qty - rounded

    if rounded < min_qty:
        return 0.0, requested_qty, f"below_min_qty (rounded {rounded} < min {min_qty})"

    return rounded, leftover, "ok"
