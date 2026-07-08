"""
lever_matrix.py — WolfBot v0.4 Part L3: the backtester quality-lever matrix.

Runs the LIVE strategy config through the existing deterministic backtester
(backtest.run_backtest) against each Tier-2 quality lever and produces a ranked
"edge report" so quality tuning is data-driven.

WHAT THIS IS
    A read-only, simulation-only analysis tool. It never places orders — it only
    replays STORED klines through backtest.run_backtest (whose determinism
    contract guarantees: same window + same klines + same config → identical
    stats). Each lever is a named transform over a DEEP COPY of the baseline
    (live) strategy dict; the baseline is never mutated.

DETERMINISM
    The only wall-clock use is choosing the [now - months, now] window — this is
    a live operator tool, not a workflow step, so a moving window is intended.
    Given a fixed window and fixed stored klines, the per-variant STATS are
    byte-identical across runs (the backtester guarantees this).

LEVERS (Tier-2, per spec)
    t2_mandatory      — signal role T2_ema50_15m_slope := "mandatory"   [supported]
    min_score_4       — entries.min_score + signal_engine.min_scored := 4 [supported]
    risk_off_stricter — regime.risk_off_pct_4h := -0.5 (stricter)        [supported]
    time_stop_30m     — exit flat theses after 30m below +0.5R           [UNSUPPORTED:
                        the current backtester has no time-stop parameter — see
                        backtest.py exit rules — so this lever is reported as
                        unsupported rather than silently skipped]
    tm1_hours         — TM1 trading-hours filter                         [UNSUPPORTED:
                        TM1_bad_hour reads the wall clock and is in
                        backtest._NON_REPLAYABLE_SIGNALS (excluded from replay
                        decisions), so it can't be exercised by the backtester]

CLI
    python lever_matrix.py --months 3 --symbols approved
    python lever_matrix.py --months 6 --symbols BTCUSDT,ETHUSDT
"""
from __future__ import annotations

import argparse
import copy
import json
import sys
import time
from typing import Callable, Dict, List, Optional

import database

# The backtester + config loaders. Imported defensively so a broken/absent
# dependency degrades to {"available": False, ...} instead of a hard crash.
try:
    import backtest as _backtest
except Exception as _exc:                       # pragma: no cover - import guard
    _backtest = None
    _BACKTEST_IMPORT_ERR = repr(_exc)
else:
    _BACKTEST_IMPORT_ERR = None

try:
    import backfill as _backfill
except Exception as _exc:                        # pragma: no cover - import guard
    _backfill = None
    _BACKFILL_IMPORT_ERR = repr(_exc)
else:
    _BACKFILL_IMPORT_ERR = None


# The regime/correlation guards in the backtester replay BTCUSDT klines, so BTC
# history is always backfilled alongside the traded universe (fallbacks are
# documented in backtest.py, but a covered BTC series makes the regime lever
# meaningful).
_BTC_SYMBOL = "BTCUSDT"

# Variants with fewer than this many trades get a low-confidence note.
_LOW_CONFIDENCE_TRADES = 100

# The stat keys backtest._compute_stats emits that we surface per variant.
_PRIMARY_METRIC = "expectancy_per_trade"


# ── Lever transforms ─────────────────────────────────────────────────────────
# Each transform receives a DEEP COPY of the base strategy dict and returns it
# mutated. run_lever_matrix always deep-copies before calling — transforms never
# see (or touch) the shared baseline.

def _ensure_dict(d: dict, key: str) -> dict:
    v = d.get(key)
    if not isinstance(v, dict):
        v = {}
        d[key] = v
    return v


def _t2_mandatory(s: dict) -> dict:
    """Promote the 15m EMA50 slope confirm to a mandatory gate."""
    se = _ensure_dict(s, "signal_engine")
    roles = _ensure_dict(se, "roles")
    roles["T2_ema50_15m_slope"] = "mandatory"
    return s


def _min_score_4(s: dict) -> dict:
    """Raise the minimum scored-signal threshold to 4 (both the entries.min_score
    field and its canonical signal_engine.min_scored alias)."""
    entries = _ensure_dict(s, "entries")
    entries["min_score"] = 4
    se = _ensure_dict(s, "signal_engine")
    se["min_scored"] = 4
    return s


def _risk_off_stricter(s: dict) -> dict:
    """Tighten the 4h risk-off regime threshold to -0.5% (from the -1.0 default)."""
    regime = _ensure_dict(s, "regime")
    regime["risk_off_pct_4h"] = -0.5
    return s


def _time_stop_30m(s: dict) -> dict:
    """Would enable a 30-minute time-stop for flat theses (below +0.5R). The
    current backtester exposes no time-stop parameter, so this is a no-op and the
    lever is flagged unsupported — it is reported, not silently skipped."""
    return s


def _tm1_hours(s: dict) -> dict:
    """Would enable the TM1 trading-hours filter. TM1_bad_hour reads the wall
    clock and is excluded from replay decisions (backtest._NON_REPLAYABLE_SIGNALS),
    so the backtester can't exercise it — reported as unsupported."""
    return s


LEVERS: List[dict] = [
    {
        "key": "t2_mandatory",
        "label": "T2 EMA50 15m slope = mandatory",
        "description": "Require the 15m EMA50 slope trend-confirm on every entry "
                       "(signal role T2_ema50_15m_slope := mandatory).",
        "transform": _t2_mandatory,
        "supported": True,
    },
    {
        "key": "min_score_4",
        "label": "min_score = 4",
        "description": "Raise entries.min_score (and signal_engine.min_scored) "
                       "from 3 to 4 — fewer, higher-conviction entries.",
        "transform": _min_score_4,
        "supported": True,
    },
    {
        "key": "risk_off_stricter",
        "label": "risk_off_pct_4h = -0.5 (stricter)",
        "description": "Tighten the BTC 4h risk-off regime threshold to -0.5% so "
                       "entries are vetoed sooner in a falling market.",
        "transform": _risk_off_stricter,
        "supported": True,
    },
    {
        "key": "time_stop_30m",
        "label": "time-stop 30m (flat theses)",
        "description": "Exit theses still below +0.5R after 30 minutes.",
        "transform": _time_stop_30m,
        "supported": False,
        "unsupported_reason": "The current backtester has no time-stop parameter "
                              "(see backtest.py exit rules) — cannot be simulated.",
    },
    {
        "key": "tm1_hours",
        "label": "TM1 trading-hours filter",
        "description": "Block entries during configured bad trading hours.",
        "transform": _tm1_hours,
        "supported": False,
        "unsupported_reason": "TM1_bad_hour reads the wall clock and is excluded "
                              "from replay decisions (backtest._NON_REPLAYABLE_"
                              "SIGNALS) — cannot be simulated.",
    },
]

# Sensible combinations of already-supported single levers.
_COMBOS: List[dict] = [
    {
        "key": "t2_mandatory+min_score_4",
        "label": "T2 mandatory + min_score 4",
        "description": "Both quality gates together: 15m slope required AND "
                       "min_score raised to 4.",
        "levers": ["t2_mandatory", "min_score_4"],
    },
]

_LEVERS_BY_KEY = {l["key"]: l for l in LEVERS}


# ── Baseline / universe resolution ───────────────────────────────────────────

def _load_baseline_strategy() -> dict:
    """The live strategy dict (raw strategy.json) — exactly what the live engine
    and the backtest CLI use as their config source."""
    return _backtest._load_strategy_file()


def _resolve_symbols(symbols: Optional[list], strategy: dict) -> List[str]:
    """Default to the live approved/valid universe (backfill's approved-coins
    loader, which reads strategy.json approved_coins the same way the live bot
    does). An explicit list overrides."""
    if symbols:
        return sorted({str(s).upper() for s in symbols if str(s).strip()})
    approved: List[str] = []
    try:
        if _backfill is not None:
            approved = list(_backfill.approved_symbols())
    except Exception:
        approved = []
    if not approved:
        # Fall back to the backtester's own approved-coins reader.
        try:
            approved = _backtest._approved_symbols(strategy)
        except Exception:
            approved = []
    return sorted({str(s).upper() for s in approved if str(s).strip()})


def _ensure_klines(symbols: List[str], months: float,
                   notes: List[str]) -> Dict[str, str]:
    """Backfill 1m + 5m klines for the window for each symbol (plus BTCUSDT for
    the regime/correlation guards). Network errors are caught PER SYMBOL and
    recorded, never aborting the whole run. Returns {symbol: error_repr} for any
    symbol that failed."""
    errors: Dict[str, str] = {}
    if _backfill is None:
        notes.append("backfill unavailable (%s) — relying on already-stored "
                     "klines; run backfill.py separately if coverage is short."
                     % _BACKFILL_IMPORT_ERR)
        return errors
    days = max(1.0, float(months) * 31.0)   # cover the window generously
    targets = list(dict.fromkeys(list(symbols) + [_BTC_SYMBOL]))
    for sym in targets:
        for interval in ("1m", "5m"):
            try:
                _backfill.backfill_symbol(sym, interval, days)
            except Exception as exc:
                errors[sym] = repr(exc)
                notes.append(f"backfill failed for {sym} {interval}: {exc!r} — "
                             "using whatever klines are already stored.")
    return errors


# ── Single-run helper ────────────────────────────────────────────────────────

def _stats_row(variant: str, label: str, description: str,
               result: dict) -> dict:
    """Project backtest.run_backtest's stats into the compact per-variant row."""
    stats = result.get("stats", {}) if isinstance(result, dict) else {}
    tc = int(stats.get("trades", 0) or 0)
    return {
        "variant": variant,
        "label": label,
        "description": description,
        "expectancy": stats.get("expectancy_per_trade", 0.0),
        "profit_factor": stats.get("profit_factor"),
        "max_drawdown": stats.get("max_drawdown", 0.0),
        "trade_count": tc,
        "win_rate": stats.get("win_rate", 0.0),
        "net_pnl": stats.get("net_pnl", 0.0),
        "confidence": ("low" if tc < _LOW_CONFIDENCE_TRADES else "ok"),
        "confidence_note": (
            f"only {tc} trades (< {_LOW_CONFIDENCE_TRADES}) — treat the edge as "
            "indicative, not conclusive." if tc < _LOW_CONFIDENCE_TRADES
            else f"{tc} trades — reasonable sample."),
    }


def _run_variant(variant: str, label: str, description: str,
                 strategy: dict, start_ms: int, end_ms: int,
                 symbols: List[str], notes: List[str]) -> Optional[dict]:
    """Run one backtest pass. Returns the compact stats row, or None on failure
    (recorded in notes)."""
    try:
        result = _backtest.run_backtest(start_ms, end_ms, symbols, strategy)
    except Exception as exc:
        notes.append(f"variant '{variant}' failed: {exc!r}")
        return None
    return _stats_row(variant, label, description, result)


def _rank_key(row: dict):
    """Primary metric expectancy, tie-break profit_factor (None sorts last).
    Higher is better → negate for ascending sort."""
    exp = row.get("expectancy") or 0.0
    pf = row.get("profit_factor")
    pf = pf if isinstance(pf, (int, float)) else float("-inf")
    return (-float(exp), -float(pf))


# ── Public entry point ───────────────────────────────────────────────────────

def run_lever_matrix(months: float = 3.0,
                     symbols: Optional[list] = None) -> dict:
    """Run the live config against each supported Tier-2 lever and rank the edge.

    Returns:
        {
          "available": True,
          "generated_ts": <ms>,
          "window": {"start_ms", "end_ms"},
          "window_months": float,
          "symbols_n": int,
          "symbols": [...],
          "baseline": {<stats row for the live config>},
          "variants": [<ranked stats rows incl. baseline, levers, combos>],
          "unsupported": [{"key","label","reason"}, ...],
          "notes": [...],
        }
    On unrecoverable failure returns {"error": "...", "available": False}.
    """
    if _backtest is None:
        return {"available": False,
                "error": f"backtest engine unavailable: {_BACKTEST_IMPORT_ERR}"}

    notes: List[str] = []

    try:
        months = float(months)
    except (TypeError, ValueError):
        months = 3.0
    if months <= 0:
        months = 3.0

    strategy = _load_baseline_strategy()
    if not isinstance(strategy, dict) or not strategy:
        notes.append("live strategy.json missing/empty — using an empty config "
                     "(backtester defaults apply).")
        strategy = strategy if isinstance(strategy, dict) else {}

    syms = _resolve_symbols(symbols, strategy)
    if not syms:
        return {"available": False,
                "error": "no symbols to backtest (empty approved universe and no "
                         "--symbols given)."}

    now_ms = int(time.time() * 1000)
    start_ms = now_ms - int(months * 30.0 * 86_400_000)
    end_ms = now_ms

    # Ensure klines exist for the window (one-click); failures are per-symbol.
    _ensure_klines(syms, months, notes)

    # Baseline (live config) — deep-copied so nothing downstream can mutate it.
    baseline_row = _run_variant(
        "baseline", "Baseline (live config)",
        "The current live strategy config, unmodified.",
        copy.deepcopy(strategy), start_ms, end_ms, syms, notes)
    if baseline_row is None:
        return {"available": False,
                "error": "baseline backtest failed — see notes.",
                "notes": notes}

    variants: List[dict] = [baseline_row]
    unsupported: List[dict] = []

    # Single supported levers.
    for lever in LEVERS:
        if not lever.get("supported"):
            unsupported.append({
                "key": lever["key"],
                "label": lever["label"],
                "reason": lever.get("unsupported_reason", "unsupported by the "
                          "current backtester."),
            })
            continue
        variant_strategy = lever["transform"](copy.deepcopy(strategy))
        row = _run_variant(lever["key"], lever["label"], lever["description"],
                           variant_strategy, start_ms, end_ms, syms, notes)
        if row is not None:
            variants.append(row)

    # Combinations of supported levers.
    for combo in _COMBOS:
        keys = combo["levers"]
        if any(not _LEVERS_BY_KEY.get(k, {}).get("supported") for k in keys):
            continue
        variant_strategy = copy.deepcopy(strategy)
        for k in keys:
            variant_strategy = _LEVERS_BY_KEY[k]["transform"](variant_strategy)
        row = _run_variant(combo["key"], combo["label"], combo["description"],
                           variant_strategy, start_ms, end_ms, syms, notes)
        if row is not None:
            variants.append(row)

    ranked = sorted(variants, key=_rank_key)
    for i, row in enumerate(ranked, 1):
        row["rank"] = i
        base_exp = baseline_row.get("expectancy") or 0.0
        row["edge_vs_baseline"] = round((row.get("expectancy") or 0.0)
                                        - float(base_exp), 8)

    report = {
        "available": True,
        "generated_ts": now_ms,
        "window": {"start_ms": start_ms, "end_ms": end_ms},
        "window_months": months,
        "primary_metric": _PRIMARY_METRIC,
        "symbols_n": len(syms),
        "symbols": syms,
        "baseline": baseline_row,
        "variants": ranked,
        "unsupported": unsupported,
        "notes": notes,
    }

    # Persist (best-effort; a DB failure must not lose the computed report).
    try:
        database.save_setting("backtest_lever_matrix_json",
                              json.dumps(report, default=str))
    except Exception as exc:
        notes.append(f"persist failed: {exc!r}")

    return report


# ── CLI ──────────────────────────────────────────────────────────────────────

def _parse_symbols_arg(raw: str) -> Optional[list]:
    raw = (raw or "").strip()
    if not raw or raw.lower() == "approved":
        return None
    return [s.strip().upper() for s in raw.split(",") if s.strip()]


def main(argv: Optional[List[str]] = None) -> int:
    p = argparse.ArgumentParser(
        description="WolfBot v0.4 L3 — backtester quality-lever matrix")
    p.add_argument("--months", type=float, default=3.0,
                   help="lookback window in months (default 3)")
    p.add_argument("--symbols", default="approved",
                   help="'approved' (live approved_coins) or a CSV list")
    args = p.parse_args(argv)

    report = run_lever_matrix(months=args.months,
                              symbols=_parse_symbols_arg(args.symbols))
    print(json.dumps(report, indent=2, default=str))
    return 0 if report.get("available") else 1


if __name__ == "__main__":
    sys.exit(main())
