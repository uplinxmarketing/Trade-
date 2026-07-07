#!/usr/bin/env python3
"""
backfill.py — historical kline backfill CLI (WolfBot v0.4 Phase 1, spec §1.2).

Downloads klines from Binance's public data endpoint
(https://data-api.binance.vision — no API key needed) and stores them in the
bot database via database.save_klines, so the backtester has months of 1m/5m
history to replay.

Usage:
    python backfill.py                                   # approved coins, 180d, 1m + 5m
    python backfill.py --symbols approved --days 180
    python backfill.py --symbols BTCUSDT,ETHUSDT --days 30 --interval 1m
    python backfill.py --interval 1m,5m --prune

Flags:
    --symbols   'approved' (default) → read approved coins from strategy.json
                approved_coins; or an explicit comma-separated list.
    --days      how far back to fill (default 180).
    --interval  '1m', '5m', a comma list, or 'both' (default: both 1m and 5m).
    --prune     after backfilling, delete klines older than strategy.json
                data.kline_retention_days (default 180).

Behaviour:
    * limit=1000 pagination walking startTime forward; polite 0.2s sleep
      between requests; 5s socket timeout; a few retries with backoff.
    * Resumable: kline_coverage() is consulted per (symbol, interval) and only
      the missing head (before first stored bar) and tail (after last stored
      bar) are fetched — re-running is cheap.
    * If binance_limits is importable its budget is honoured (non-critical
      spend; waits politely when the budget is closed) and response headers /
      429 / 418 are reported back to it.
"""

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
if _SCRIPT_DIR not in sys.path:
    sys.path.insert(0, _SCRIPT_DIR)

import database

try:
    import binance_limits
except Exception:                                  # optional integration
    binance_limits = None

BASE_URL = "https://data-api.binance.vision/api/v3/klines"
MAX_LIMIT = 1000            # Binance max rows per klines request
REQUEST_TIMEOUT_SEC = 5
SLEEP_BETWEEN_REQ_SEC = 0.2
MAX_FETCH_ATTEMPTS = 4
DEFAULT_INTERVALS = ("1m", "5m")

_UNIT_MS = {"s": 1_000, "m": 60_000, "h": 3_600_000,
            "d": 86_400_000, "w": 604_800_000}


def interval_ms(interval: str) -> int:
    """'1m' → 60000, '5m' → 300000, '1h' → 3600000, ..."""
    iv = str(interval).strip().lower()
    try:
        return int(iv[:-1]) * _UNIT_MS[iv[-1]]
    except (KeyError, ValueError, IndexError):
        raise ValueError(f"unsupported interval: {interval!r}")


def approved_symbols() -> list:
    """Approved coins from strategy.json approved_coins.
    Supports [{'symbol':..., 'approved': bool}, ...] (canonical) and plain
    string lists. Missing/unreadable file → []."""
    try:
        import config
        path = config.STRATEGY_FILE
    except Exception:
        path = os.path.join(_SCRIPT_DIR, "strategy.json")
    try:
        with open(path, "r") as f:
            s = json.load(f)
    except Exception:
        return []
    coins = s.get("approved_coins", []) if isinstance(s, dict) else []
    out = []
    for c in coins:
        if isinstance(c, str) and c.strip():
            out.append(c.strip().upper())
        elif isinstance(c, dict) and c.get("approved") and c.get("symbol"):
            out.append(str(c["symbol"]).strip().upper())
    # de-dup, preserve order
    seen = set()
    return [x for x in out if not (x in seen or seen.add(x))]


def _retry_after_sec(headers) -> "float | None":
    try:
        v = headers.get("Retry-After") if headers is not None else None
        return float(v) if v is not None else None
    except Exception:
        return None


def _budget_gate():
    """Honour the shared REST budget when binance_limits is available.
    Non-critical spend — waits politely (1s polls) while the budget is closed
    (429 pause / 418 ban / weight ceiling)."""
    if binance_limits is None:
        return
    waited = 0
    while not binance_limits.can_spend(binance_limits.WEIGHT_KLINES, critical=False):
        time.sleep(1.0)
        waited += 1
        if waited % 30 == 0:
            print("  ... REST budget closed, waiting politely "
                  f"({waited}s so far)")
    binance_limits.spend(binance_limits.WEIGHT_KLINES, critical=False)


def fetch_klines(symbol: str, interval: str, start_ms: int,
                 end_ms: "int | None" = None) -> "list | None":
    """One paginated GET /api/v3/klines call (limit=1000). Returns the parsed
    JSON list, or None after MAX_FETCH_ATTEMPTS failures."""
    params = {"symbol": symbol, "interval": interval,
              "startTime": int(start_ms), "limit": MAX_LIMIT}
    if end_ms is not None:
        params["endTime"] = int(end_ms)
    url = BASE_URL + "?" + urllib.parse.urlencode(params)

    for attempt in range(1, MAX_FETCH_ATTEMPTS + 1):
        _budget_gate()
        try:
            req = urllib.request.Request(
                url, headers={"User-Agent": "wolfbot-backfill/1.0"})
            with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT_SEC) as resp:
                if binance_limits is not None:
                    try:
                        binance_limits.record_response_headers(resp.headers)
                    except Exception:
                        pass
                data = json.loads(resp.read().decode("utf-8"))
                return data if isinstance(data, list) else []
        except urllib.error.HTTPError as e:
            retry_after = _retry_after_sec(getattr(e, "headers", None))
            if binance_limits is not None:
                try:
                    binance_limits.record_response_headers(getattr(e, "headers", None))
                except Exception:
                    pass
            if e.code == 429:
                print(f"  [{symbol} {interval}] HTTP 429 — backing off "
                      f"{retry_after or 10:.0f}s")
                if binance_limits is not None:
                    binance_limits.on_429(retry_after)
                time.sleep(retry_after if retry_after is not None else 10.0)
            elif e.code == 418:
                print(f"  [{symbol} {interval}] HTTP 418 (IP ban) — backing off "
                      f"{retry_after or 60:.0f}s")
                if binance_limits is not None:
                    binance_limits.on_418(retry_after)
                time.sleep(retry_after if retry_after is not None else 60.0)
            elif e.code in (400, 404):
                # bad symbol/params — retrying won't help
                print(f"  [{symbol} {interval}] HTTP {e.code} — skipping "
                      f"(bad symbol or params?)")
                return None
            else:
                time.sleep(min(2.0 ** attempt, 10.0))
        except Exception as e:
            print(f"  [{symbol} {interval}] fetch error "
                  f"(attempt {attempt}/{MAX_FETCH_ATTEMPTS}): {e}")
            time.sleep(min(2.0 ** attempt, 10.0))
    return None


def backfill_range(symbol: str, interval: str,
                   range_start_ms: int, range_end_ms: int) -> int:
    """Walk [range_start_ms, range_end_ms] with startTime pagination, saving
    each batch. Returns the number of bars written."""
    step = interval_ms(interval)
    cursor = int(range_start_ms)
    end = int(range_end_ms)
    total = 0
    while cursor <= end:
        batch = fetch_klines(symbol, interval, cursor, end)
        if batch is None:
            print(f"  [{symbol} {interval}] giving up on range at cursor={cursor}")
            break
        if not batch:
            break  # no data (symbol listed later, or fully caught up)
        rows = [r for r in batch if int(r[0]) <= end]
        total += database.save_klines(symbol, interval, rows)
        next_cursor = int(batch[-1][0]) + step
        if next_cursor <= cursor:      # safety: never loop in place
            break
        cursor = next_cursor
        if len(batch) < MAX_LIMIT:
            break                      # short page → range exhausted
        time.sleep(SLEEP_BETWEEN_REQ_SEC)
    return total


def backfill_symbol(symbol: str, interval: str, days: float) -> int:
    """Backfill one (symbol, interval) to cover the last `days` days.
    Resumable: only fetches before the first stored bar and after the last
    stored bar (kline_coverage)."""
    now_ms = int(time.time() * 1000)
    target_start = now_ms - int(days * 86_400_000)
    step = interval_ms(interval)

    cov = database.kline_coverage(symbol, interval)
    ranges = []
    if cov["count"] and cov["first_ms"] is not None:
        if target_start <= cov["first_ms"] - step:
            ranges.append((target_start, cov["first_ms"] - step))    # head gap
        if cov["last_ms"] + step <= now_ms:
            ranges.append((cov["last_ms"] + step, now_ms))           # tail gap
        if not ranges:
            print(f"[{symbol} {interval}] already covered "
                  f"({cov['count']} bars) — skipping")
            return 0
    else:
        ranges.append((target_start, now_ms))

    saved = 0
    for a, b in ranges:
        if a > b:
            continue
        saved += backfill_range(symbol, interval, a, b)

    cov2 = database.kline_coverage(symbol, interval)
    span_days = ((cov2["last_ms"] - cov2["first_ms"]) / 86_400_000
                 if cov2["count"] else 0.0)
    print(f"[{symbol} {interval}] +{saved} bars "
          f"(coverage: {cov2['count']} bars, ~{span_days:.1f} days)")
    return saved


def parse_intervals(spec: str) -> list:
    spec = (spec or "").strip().lower()
    if spec in ("", "both", "all", "default"):
        return list(DEFAULT_INTERVALS)
    out = []
    for part in spec.split(","):
        part = part.strip()
        if not part:
            continue
        interval_ms(part)  # validate — raises ValueError on garbage
        out.append(part)
    return out or list(DEFAULT_INTERVALS)


def main(argv=None) -> int:
    p = argparse.ArgumentParser(
        description="Backfill historical klines into the WolfBot database "
                    "(public Binance data endpoint, no API key).")
    p.add_argument("--symbols", default="approved",
                   help="'approved' (strategy.json approved_coins) or a "
                        "comma-separated list, e.g. BTCUSDT,ETHUSDT "
                        "(default: approved)")
    p.add_argument("--days", type=float, default=180,
                   help="days of history to cover (default: 180)")
    p.add_argument("--interval", default="both",
                   help="'1m', '5m', a comma list, or 'both' (default: both)")
    p.add_argument("--prune", action="store_true",
                   help="afterwards, prune klines older than strategy.json "
                        "data.kline_retention_days (default 180)")
    args = p.parse_args(argv)

    try:
        intervals = parse_intervals(args.interval)
    except ValueError as e:
        print(f"error: {e}")
        return 2

    if args.symbols.strip().lower() == "approved":
        symbols = approved_symbols()
        if not symbols:
            print("No approved coins found in strategy.json "
                  "(approved_coins) — pass --symbols SYM1,SYM2 instead.")
            return 1
    else:
        symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
        if not symbols:
            print("No symbols given.")
            return 1

    database.init_db()

    print(f"Backfilling {len(symbols)} symbol(s) × {intervals} "
          f"for {args.days:g} day(s)...")
    total = 0
    for i, sym in enumerate(symbols, 1):
        print(f"--- {sym} ({i}/{len(symbols)}) ---")
        for iv in intervals:
            try:
                total += backfill_symbol(sym, iv, args.days)
            except KeyboardInterrupt:
                print("\nInterrupted — progress so far is saved; "
                      "re-run to resume.")
                return 130
            except Exception as e:
                print(f"[{sym} {iv}] ERROR: {e}")

    print(f"Done. {total} bars written.")

    if args.prune:
        deleted = database.prune_klines_from_config()
        print(f"Pruned {deleted} kline row(s) past retention.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
