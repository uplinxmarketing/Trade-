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
    loose_gate        — the O1 minimum-viable gate (min_score 2, T1 scored,
                        qv floor 3M, spread_max 0.30)                    [supported;
                        two knobs inert in replay — see O4 note below]
    manual_method_with_stop — near-filterless entry + ~+0.3% take-profit,
                        current ATR stop KEPT                            [supported]
    manual_method_no_stop   — same, but NO hard stop (losers held until TP
                        or end-of-window force-close → tail realized)    [supported]
    time_stop_30m     — exit flat theses after 30m below +0.5R           [UNSUPPORTED:
                        the current backtester has no time-stop parameter — see
                        backtest.py exit rules — so this lever is reported as
                        unsupported rather than silently skipped]
    tm1_hours         — TM1 trading-hours filter                         [UNSUPPORTED:
                        TM1_bad_hour reads the wall clock and is in
                        backtest._NON_REPLAYABLE_SIGNALS (excluded from replay
                        decisions), so it can't be exercised by the backtester]

O4 — LOOSE GATE + MANUAL-METHOD VARIANTS (judged on the SAME 3-month data)
    loose_gate replays the exact O1 minimum-viable gate the live preset applies
    so the matrix predicts what loosening does before/after it is turned on.
    HONEST CAVEAT: two of its four knobs are inert in this replay —
    entries.min_quote_volume_24h_usd is a LIVE-only entry filter (read in
    trade_engine, never in backtest) and signal_thresholds.spread_max_pct feeds
    E1_spread_too_wide, which is non-replayable (backtest._NON_REPLAYABLE_SIGNALS).
    The two that DO bite (min_score/min_scored := 2, T1_ema_short_long := scored)
    are replayed faithfully. The lever still runs; the inertness is surfaced as a
    per-run note, not hidden.

    The manual-method variants model how the operator historically traded:
    almost no entry filter, sell fast at a small fixed profit (~+0.3%), and HOLD
    losers until they recover (rarely stopped out). Both are modeled so the true
    cost of "hold until it recovers" (the rare deep loss) is MEASURED:
      * manual_method_with_stop — near-filterless entry (min_score 1, vetoes off,
        bb/5m structural gates off) + ~+0.3% take-profit via the non-legacy exits
        block (rr_ratio := 0 neutralizes the ATR-scaled R-multiple TP, so the TP
        collapses to its breakeven floor tp_buffer_pct := 0.3 ≈ +0.3% above BEP;
        k_trail := 0 sells immediately at target). The CURRENT ATR stop is KEPT
        (k_sl/sl_min_pct/sl_max_pct untouched → non-legacy sl_enabled stays true).
      * manual_method_no_stop — same entry + ~+0.3% target, but NO stop. The only
        way the backtester can disable the stop is the LEGACY exit mapping
        (drop the exits block + stop_loss_enabled := False → sl_enabled False,
        hard_sl None; take_profit_pct := 0.3 is the fixed-TP path). The backtester
        has NO N-bar time-exit parameter, so a held loser exits only at TP or at
        the end-of-window force-close (backtest_end) — which realizes the deep-loss
        tail in the stats. That terminal realization (not a literal time-stop) is
        why this variant is reported as supported; the caveat is noted per run.
    A true risk-multiple avg_win_R/avg_loss_R is not computable from run_backtest's
    trades (no per-trade stop distance is recorded), so those two fields are a
    return-per-entry-notional proxy (net_pnl / (qty×entry_px)) computed from the
    trades list — which is exactly what surfaces the manual tail (avg_loss_R goes
    sharply negative for no_stop as held losers close at backtest_end).

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


def _loose_gate(s: dict) -> dict:
    """The O1 minimum-viable ("loose") gate the live preset applies. Sets the
    four knobs exactly; note that in REPLAY only two bite (min_score/min_scored
    and the T1 role) — see the module docstring / the per-run note attached to
    this lever. Reported honestly, not silently."""
    entries = _ensure_dict(s, "entries")
    entries["min_score"] = 2
    entries["min_quote_volume_24h_usd"] = 3_000_000   # live-only filter (inert in replay)
    se = _ensure_dict(s, "signal_engine")
    se["min_scored"] = 2
    roles = _ensure_dict(se, "roles")
    roles["T1_ema_short_long"] = "scored"
    st = _ensure_dict(s, "signal_thresholds")
    st["spread_max_pct"] = 0.30                        # feeds E1 (non-replayable)
    return s


# Signals promoted to "scored" (near-filterless) and demoted to "off" (no veto)
# for the manual-method variants. E1/TM1 are handled by the backtester's
# non-replayable exclusion regardless; REGIME_risk_off is set off here but the
# backtester ALSO applies a hard risk_off macro gate that cannot be removed
# (documented — a genuinely protective gate baked into the sim).
_MANUAL_SCORED = (
    "T1_ema_short_long", "M3_macd_rising", "V1_volume_above_average",
    "V2_obv_rising", "M4_micro_pullback", "R1_reversal_confirmed",
    "T2_ema50_15m_slope",
)
_MANUAL_OFF = (
    "P2_bb_upper_touch_5m", "REGIME_risk_off", "X1_atr_untradeable",
    "E1_spread_too_wide",
)


def _manual_entry(s: dict) -> dict:
    """Shared 'almost no entry filter' side of the manual-method variants:
    min_score/min_scored := 1, every signal → scored, every veto → off, and the
    backtester's structural entry gates (bb-gate, 5m-veto) disabled so entries
    are near-filterless (falling-knife + the hard risk_off macro gate remain —
    genuinely protective and not role-removable)."""
    entries = _ensure_dict(s, "entries")
    entries["min_score"] = 1
    se = _ensure_dict(s, "signal_engine")
    se["min_scored"] = 1
    roles = _ensure_dict(se, "roles")
    for sig in _MANUAL_SCORED:
        roles[sig] = "scored"
    for sig in _MANUAL_OFF:
        roles[sig] = "off"
    bt = _ensure_dict(s, "backtest")
    bt["bb_gate_enabled"] = False
    bt["use_5m_veto"] = False
    return s


def _manual_method_with_stop(s: dict) -> dict:
    """Manual method WITH the current ATR stop kept. Near-filterless entry, a
    small ~+0.3% take-profit, and the existing ATR stop untouched.

    TP knob (non-legacy exits block, so sl_enabled stays true → ATR stop kept):
      * rr_ratio := 0        neutralizes the ATR-scaled R-multiple TP so the TP
                             collapses to its breakeven floor,
      * tp_buffer_pct := 0.3 sells ~+0.3% above breakeven (a genuine small profit),
      * k_trail := 0         sells immediately at target (no trailing ride)."""
    s = _manual_entry(s)
    exits = _ensure_dict(s, "exits")   # presence forces the non-legacy path
    exits["rr_ratio"] = 0.0
    exits["tp_buffer_pct"] = 0.3
    exits["k_trail"] = 0.0
    return s


def _manual_method_no_stop(s: dict) -> dict:
    """Manual method with NO stop — losers are HELD until they recover. Same
    near-filterless entry + ~+0.3% target, but the stop is disabled.

    The ONLY way the backtester can disable the stop is the LEGACY exit mapping
    (exit_config): no 'exits' block + legacy keys. stop_loss_enabled := False →
    sl_enabled False (no protective stop) and hard_sl None; take_profit_pct :=
    0.3 is the fixed-TP path (immediate sell at +0.3%). There is NO N-bar
    time-exit parameter, so a held loser exits only at TP or at the end-of-window
    force-close (backtest_end) — which REALIZES the deep-loss tail in the stats
    (that terminal realization, not a literal time-stop, is why this is
    supported)."""
    s = _manual_entry(s)
    s.pop("exits", None)                 # drop any exits block → legacy mapping
    s["stop_loss_enabled"] = False       # → sl_enabled False (no stop at all)
    s["take_profit_enabled"] = True
    s["take_profit_pct"] = 0.3           # fixed +0.3% target (legacy fixed-TP path)
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
        "key": "loose_gate",
        "label": "loose gate (O1 minimum-viable)",
        "description": "The exact O1 minimum-viable gate the live preset applies: "
                       "min_score/min_scored := 2, T1_ema_short_long := scored, "
                       "qv floor 3M, spread_max 0.30 — predicts the effect of "
                       "loosening before/after it is turned on.",
        "transform": _loose_gate,
        "supported": True,
        "run_note": "loose_gate: min_quote_volume_24h_usd (3M) is a LIVE-only entry "
                    "filter (unread by the backtester) and spread_max_pct (0.30) "
                    "feeds the non-replayable E1 veto — both are INERT in this "
                    "replay; only min_score/min_scored := 2 and T1 := scored bite.",
    },
    {
        "key": "manual_method_with_stop",
        "label": "manual method (+0.3% TP, ATR stop kept)",
        "description": "Operator's historical style: near-filterless entry "
                       "(min_score 1, vetoes off, bb/5m gates off), sell fast at "
                       "~+0.3% (rr_ratio 0 + tp_buffer_pct 0.3, k_trail 0), with "
                       "the CURRENT ATR stop kept.",
        "transform": _manual_method_with_stop,
        "supported": True,
        "run_note": "manual_method_with_stop: ~+0.3% TP is tp_buffer_pct := 0.3 "
                    "above breakeven (rr_ratio := 0 disables the ATR-scaled TP; "
                    "k_trail := 0 sells at target). ATR stop kept (non-legacy "
                    "sl_enabled). The hard risk_off macro gate still applies.",
    },
    {
        "key": "manual_method_no_stop",
        "label": "manual method (+0.3% TP, NO stop)",
        "description": "Same near-filterless entry + ~+0.3% target, but NO stop — "
                       "losers are HELD until they recover. Models the true cost "
                       "of 'hold until it recovers': the deep-loss tail is realized "
                       "at the end-of-window force-close.",
        "transform": _manual_method_no_stop,
        "supported": True,
        "run_note": "manual_method_no_stop: stop disabled via the LEGACY exit "
                    "mapping (stop_loss_enabled := False → sl_enabled False, "
                    "hard_sl None; take_profit_pct := 0.3). No N-bar time-exit "
                    "exists, so held losers realize at TP or the end-of-window "
                    "backtest_end force-close — measuring the deep-loss tail. "
                    "Expect few trades (capital locked in bagholds) → low "
                    "confidence, and a sharply negative avg_loss_R.",
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

def _r_metrics(result: dict) -> tuple:
    """avg_win_R / avg_loss_R from run_backtest's trades list.

    run_backtest does NOT record a per-trade stop distance, so a TRUE
    risk-multiple is not computable here; these are the closest proxy the trades
    list supports — the average net return per entry NOTIONAL
    (net_pnl / (qty × entry_px)) over winning / losing trades. It is exactly what
    surfaces the manual-method tail: avg_loss_R plunges for manual_method_no_stop
    when held losers finally close at the end-of-window force-close.
    Win/loss split mirrors backtest._compute_stats (net_pnl > 0 → win)."""
    trades = result.get("trades") if isinstance(result, dict) else None
    wins_r: List[float] = []
    losses_r: List[float] = []
    for t in (trades or []):
        try:
            notional = float(t["qty"]) * float(t["entry_px"])
            net = float(t["net_pnl"])
        except (KeyError, TypeError, ValueError):
            continue
        if notional <= 0:
            continue
        (wins_r if net > 0 else losses_r).append(net / notional)
    avg_win_R = round(sum(wins_r) / len(wins_r), 8) if wins_r else 0.0
    avg_loss_R = round(sum(losses_r) / len(losses_r), 8) if losses_r else 0.0
    return avg_win_R, avg_loss_R


def _stats_row(variant: str, label: str, description: str,
               result: dict) -> dict:
    """Project backtest.run_backtest's stats into the compact per-variant row."""
    stats = result.get("stats", {}) if isinstance(result, dict) else {}
    tc = int(stats.get("trades", 0) or 0)
    avg_win_R, avg_loss_R = _r_metrics(result)
    return {
        "variant": variant,
        "label": label,
        "description": description,
        "expectancy": stats.get("expectancy_per_trade", 0.0),
        "profit_factor": stats.get("profit_factor"),
        "max_drawdown": stats.get("max_drawdown", 0.0),
        "trades": tc,
        "trade_count": tc,
        "win_rate": stats.get("win_rate", 0.0),
        "avg_win_R": avg_win_R,
        "avg_loss_R": avg_loss_R,
        "avg_win": stats.get("avg_win", 0.0),
        "avg_loss": stats.get("avg_loss", 0.0),
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
            if lever.get("run_note"):
                notes.append(lever["run_note"])

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
