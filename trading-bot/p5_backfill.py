"""p5_backfill.py — one-time ~50-day 5m history backfill for WolfScore-P5.

P5's features need up to 50 days of 5m bars per coin (BTC/breadth 50d SMA, 30d
high, 3d return) INCLUDING taker-buy volume. The durable kline store historically
held far less, so before P5 can score (rather than gate every coin 'warmup') the
history must be filled. This pulls 5m klines from Binance's public data mirror
(data-api.binance.vision — no key, no signature) and writes them via
database.save_klines, which now persists takerBuyBaseVolume (array index [9]).

Runs two ways:
  • Boot task: control_api kicks run_backfill(approved+BTC) in a background thread
    at the P5 go-live migration, so warmup clears progressively after deploy.
  • Standalone: `python3 p5_backfill.py` reads approved coins from strategy.json.

Bounded, throttled, and fully guarded — a fetch failure skips that coin, never
raises into the caller. Idempotent: save_klines is INSERT OR REPLACE.
"""

import json
import time
import urllib.parse
import urllib.request

import database

_BASE      = "https://data-api.binance.vision/api/v3/klines"
_INTERVAL  = "5m"
_LIMIT     = 1000            # max bars per request
_DEF_DAYS  = 51             # >50 so the 50d SMA / 30d high have full coverage
_BARS_PER_DAY = 288          # 5m bars


def _fetch(symbol: str, end_ms: int) -> list:
    q = urllib.parse.urlencode({"symbol": symbol, "interval": _INTERVAL,
                                "limit": _LIMIT, "endTime": int(end_ms)})
    req = urllib.request.Request(f"{_BASE}?{q}",
                                 headers={"User-Agent": "wolfbot-p5-backfill/1.0"})
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.loads(r.read().decode("utf-8"))


def backfill_symbol(symbol: str, days: int = _DEF_DAYS, throttle: float = 0.1,
                    now_ms: int = None) -> int:
    """Page 5m klines backwards until ~`days` of history is stored. Returns the
    number of bars written. now_ms lets the caller pass the wall clock (tests)."""
    need = days * _BARS_PER_DAY + 50
    end_ms = int(now_ms if now_ms is not None else time.time() * 1000)
    got = 0
    total = 0
    oldest = None
    while got < need:
        rows = None
        for attempt in range(3):
            try:
                rows = _fetch(symbol, end_ms)
                break
            except Exception:
                time.sleep(1.0 * (attempt + 1))
        if not rows:
            break
        try:
            total += database.save_klines(symbol, _INTERVAL, rows)
        except Exception:
            pass
        got += len(rows)
        first_open = int(rows[0][0])
        if oldest is not None and first_open >= oldest:
            break                      # no backward progress — stop
        oldest = first_open
        end_ms = first_open - 1
        if len(rows) < _LIMIT:
            break                      # reached the start of available history
        time.sleep(throttle)
    return total


def run_backfill(symbols, days: int = _DEF_DAYS, throttle: float = 0.1, log=None) -> dict:
    """Backfill each symbol (BTCUSDT is always included — macro/tilt need it).
    Guarded per-symbol. `log` is an optional callable(str)."""
    syms = []
    for s in list(symbols or []):
        if s and s not in syms:
            syms.append(s)
    if "BTCUSDT" not in syms:
        syms.insert(0, "BTCUSDT")
    ok = 0
    total = 0
    for s in syms:
        try:
            n = backfill_symbol(s, days=days, throttle=throttle)
            total += n
            ok += 1
            if log:
                log(f"[p5-backfill] {s}: +{n} bars")
        except Exception as e:
            if log:
                log(f"[p5-backfill] {s} FAILED: {type(e).__name__}: {e}")
    result = {"symbols": ok, "of": len(syms), "bars": total}
    if log:
        log(f"[p5-backfill] done: {result}")
    return result


def _approved_from_strategy() -> list:
    try:
        import config
        with open(config.STRATEGY_FILE, "r") as f:
            s = json.load(f)
        return [c["symbol"] for c in s.get("approved_coins", [])
                if isinstance(c, dict) and c.get("approved") and c.get("symbol")]
    except Exception:
        return []


if __name__ == "__main__":
    try:
        database.init_db()
    except Exception:
        pass
    syms = _approved_from_strategy()
    print(f"[p5-backfill] {len(syms)} approved coins (+BTC); pulling ~{_DEF_DAYS}d 5m…")
    res = run_backfill(syms, log=print)
    print(f"[p5-backfill] COMPLETE: {res}")
