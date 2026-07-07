#!/usr/bin/env python3
"""WolfBot Part E — zero-restart effect tests.

Runs EIGHT live tests against a RUNNING bot (paper mode): each test changes a
frontend-settable value through the public API, verifies an ENGINE-VISIBLE
effect without any restart, then restores the original value.

Usage:
    python3 effect_tests.py --base http://127.0.0.1:8000

Exit code 0 = all 8 passed, 1 = any failure.

Probes used (all read the same hot config resolvers the live engine uses):
    GET /api/debug/exit-geometry   — backtest.exit_levels over current strategy
    GET /api/debug/budget          — trade_engine.get_budget_for_coin + slots
    GET /api/debug/evaluate-gates  — signal_registry.evaluate_buy_decision
    GET /api/risk/status           — trade_engine.get_risk_status
    GET /api/signal-registry       — resolved roles + thresholds
"""
from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.request

BASE = "http://127.0.0.1:8000"


def api(method: str, path: str, body: dict | None = None):
    """HTTP helper — returns (status_code, parsed_json)."""
    url = BASE + path
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=20) as r:
            return r.status, json.loads(r.read().decode() or "null")
    except urllib.error.HTTPError as e:
        # 4xx/5xx still carry a JSON body we want to inspect. Any other
        # failure (unreachable bot) propagates and aborts the run.
        try:
            return e.code, json.loads(e.read().decode() or "null")
        except Exception:
            return e.code, None


def get(path):
    return api("GET", path)


def put_strategy(patch: dict):
    code, body = api("PUT", "/api/strategy", {"config": patch})
    if code != 200 or not (body or {}).get("ok"):
        raise AssertionError(f"PUT /api/strategy {patch} -> {code}: {body}")
    return body


def approx(a, b, tol=1e-6):
    return abs(float(a) - float(b)) <= tol


def geometry(entry=100.0, budget=50.0, atr_pct=1.0):
    code, g = get(f"/api/debug/exit-geometry?symbol=BTCUSDT&entry={entry}"
                  f"&budget={budget}&atr_pct={atr_pct}")
    assert code == 200 and "error" not in (g or {}), f"geometry probe failed: {g}"
    return g


def current_exits() -> dict:
    code, doc = get("/api/strategy")
    assert code == 200, f"GET /api/strategy -> {code}"
    return dict(doc["config"]["exits"])


# ── Tests ─────────────────────────────────────────────────────────────────────

def test_1_rr_ratio_tp_buffer():
    """exits.rr_ratio / exits.tp_buffer_pct → next computed TP geometry."""
    orig = current_exits()
    try:
        put_strategy({"exits": {"rr_ratio": 1.6, "tp_buffer_pct": 0.05,
                                "k_sl": 1.0, "sl_min_pct": 0.2, "sl_max_pct": 5.0}})
        g1 = geometry()
        put_strategy({"exits": {"rr_ratio": 3.2, "tp_buffer_pct": 0.4}})
        g2 = geometry()
        assert approx(g1["tp_distance_pct"], 1.6 * g1["sl_distance_pct"], 1e-6), \
            f"baseline tp_dist {g1['tp_distance_pct']} != 1.6*sl_dist"
        assert approx(g2["tp_distance_pct"], 3.2 * g2["sl_distance_pct"], 1e-6), \
            f"tp_dist {g2['tp_distance_pct']} did not follow rr_ratio=3.2"
        assert g2["tp"] > g1["tp"], "tp price did not increase with rr_ratio"
        assert approx(g2["cfg"]["tp_buffer_pct"], 0.4), "tp_buffer_pct not hot-applied"
        return f"tp_dist {g1['tp_distance_pct']:.3f}%→{g2['tp_distance_pct']:.3f}% (rr 1.6→3.2), buffer 0.05→0.4"
    finally:
        put_strategy({"exits": {k: orig[k] for k in
                                ("rr_ratio", "tp_buffer_pct", "k_sl",
                                 "sl_min_pct", "sl_max_pct")}})


def test_2_k_sl_clamps():
    """exits.k_sl + sl_min/max clamps → probe stop distance changes."""
    orig = current_exits()
    try:
        put_strategy({"exits": {"k_sl": 1.0, "sl_min_pct": 0.2, "sl_max_pct": 5.0}})
        g1 = geometry(atr_pct=1.0)               # sl_dist = 1.0 * 1.0% = 1.0%
        put_strategy({"exits": {"k_sl": 2.5}})
        g2 = geometry(atr_pct=1.0)               # sl_dist = 2.5%
        put_strategy({"exits": {"sl_max_pct": 1.5}})
        g3 = geometry(atr_pct=1.0)               # clamped at 1.5%
        assert approx(g1["sl_distance_pct"], 1.0), f"baseline sl_dist {g1['sl_distance_pct']}"
        assert approx(g2["sl_distance_pct"], 2.5), f"k_sl=2.5 not applied: {g2['sl_distance_pct']}"
        assert approx(g3["sl_distance_pct"], 1.5), f"sl_max clamp not applied: {g3['sl_distance_pct']}"
        assert g2["sl"] < g1["sl"], "stop price did not widen with k_sl"
        return f"sl_dist 1.0%→2.5% (k_sl), clamped→1.5% (sl_max_pct)"
    finally:
        put_strategy({"exits": {k: orig[k] for k in ("k_sl", "sl_min_pct", "sl_max_pct")}})


def test_3_budget():
    """budget_fixed_usdt / sizing.bot_allocation_usdt → get_budget_for_coin probe."""
    _, cfg0 = get("/api/config")
    code, doc = get("/api/strategy")
    orig_sizing = dict(doc["config"]["sizing"])
    try:
        code, _ = api("POST", "/api/config",
                      {"budget_mode": "fixed", "budget_fixed_usdt": 23.0})
        assert code == 200
        _, b1 = get("/api/debug/budget?symbol=BTCUSDT&free=1000")
        code, _ = api("POST", "/api/config", {"budget_fixed_usdt": 46.0})
        assert code == 200
        _, b2 = get("/api/debug/budget?symbol=BTCUSDT&free=1000")
        assert b1.get("budget_usdt") and b2.get("budget_usdt"), f"budget probe failed: {b1} {b2}"
        ratio = b2["budget_usdt"] / b1["budget_usdt"]
        assert abs(ratio - 2.0) < 0.01, \
            f"budget did not double with budget_fixed_usdt 23→46 (got {b1['budget_usdt']}→{b2['budget_usdt']})"
        # v2 alias path: PUT sizing.bot_allocation_usdt must reach the engine's
        # allocation math (root key) with zero restart.
        put_strategy({"sizing": {"bot_allocation_usdt": 777.0}})
        _, b3 = get("/api/debug/budget?symbol=BTCUSDT&free=1000")
        assert approx(b3["slots"]["effective_allocation"], 777.0), \
            f"sizing.bot_allocation_usdt (v2) not engine-visible: {b3['slots']}"
        return (f"budget {b1['budget_usdt']}→{b2['budget_usdt']} USDT (fixed 23→46), "
                f"v2 allocation→{b3['slots']['effective_allocation']}")
    finally:
        api("POST", "/api/config", {
            "budget_mode": cfg0.get("budget_mode", "fixed"),
            "budget_fixed_usdt": cfg0.get("budget_fixed_usdt", 5.0),
            "bot_allocation_usdt": cfg0.get("bot_allocation_usdt", 0),
        })
        put_strategy({"sizing": {"bot_allocation_usdt": orig_sizing["bot_allocation_usdt"]}})


def test_4_max_positions():
    """sizing.max_positions → effective_slots in /api/risk/status (+ degraded)."""
    _, doc = get("/api/strategy")
    orig = dict(doc["config"]["sizing"])
    try:
        put_strategy({"sizing": {"max_positions": 3, "min_position_usdt": 10.0,
                                 "bot_allocation_usdt": 100.0}})
        _, rs = get("/api/risk/status")
        slots = rs["slots"]
        assert slots.get("effective_slots") == 3 and not slots.get("degraded"), \
            f"expected 3 undegraded slots, got {slots}"
        put_strategy({"sizing": {"min_position_usdt": 60.0}})
        _, rs2 = get("/api/risk/status")
        slots2 = rs2["slots"]
        assert slots2.get("effective_slots") == 1 and slots2.get("degraded") is True, \
            f"expected degraded 1 slot (floor(100/60)), got {slots2}"
        return f"slots 3 (ok) → 1 degraded (alloc 100 / min_pos 60), zero restart"
    finally:
        put_strategy({"sizing": {k: orig[k] for k in
                                 ("max_positions", "min_position_usdt",
                                  "bot_allocation_usdt")}})


def _signal_engine_state():
    _, d = get("/api/signal-engine/config")
    return d


def test_5_role_flip_veto():
    """Flip V1 to veto via PUT /api/signals/registry → gate outcome flips."""
    se0 = _signal_engine_state()
    was_active = bool(se0.get("active"))
    orig_cfg = se0.get("config") or {}
    if not was_active:
        # Engine gate must actually run for a role flip to matter — enable it
        # with the shipped defaults (restored below).
        d = se0.get("defaults") or {}
        code, resp = api("POST", "/api/signal-engine/config", {"signal_engine": {
            "enabled": True,
            "mandatory_signals": d.get("mandatory_signals", []),
            "scored_signals": d.get("scored_signals", []),
            "veto_signals": d.get("veto_signals", []),
            "min_scored": d.get("min_scored", 3),
        }})
        assert code == 200 and (resp or {}).get("ok"), f"enable engine failed: {resp}"
    try:
        _, g1 = get("/api/debug/evaluate-gates?symbol=BTCUSDT&rsi=30")
        assert g1.get("allowed") is True, f"baseline gate should allow: {g1}"
        code, resp = api("PUT", "/api/signals/registry",
                         {"roles": {"V1_volume_above_average": "veto"}})
        assert code == 200 and (resp or {}).get("ok"), f"role PUT failed: {resp}"
        _, reg = get("/api/signal-registry")
        role = next(s["role"] for s in reg["signals"]
                    if s["id"] == "V1_volume_above_average")
        assert role == "veto", f"registry does not show veto role: {role}"
        _, g2 = get("/api/debug/evaluate-gates?symbol=BTCUSDT&rsi=30")
        assert g2.get("allowed") is False and "veto_V1" in str(g2.get("reason")), \
            f"gate did not veto after role flip: {g2}"
        return f"gate allowed→blocked ({g2['reason']}) after V1→veto, no restart"
    finally:
        api("PUT", "/api/signals/registry",
            {"roles": {"V1_volume_above_average": "scored"}})
        if not was_active:
            if orig_cfg:
                api("POST", "/api/signal-engine/config", {"signal_engine": orig_cfg})
            else:
                # Block never existed — disable so the legacy path runs again.
                d = se0.get("defaults") or {}
                api("POST", "/api/signal-engine/config", {"signal_engine": {
                    "enabled": False,
                    "mandatory_signals": d.get("mandatory_signals", []),
                    "scored_signals": d.get("scored_signals", []),
                    "veto_signals": d.get("veto_signals", []),
                    "min_scored": d.get("min_scored", 3),
                }})


def test_6_rsi_threshold():
    """signal threshold change (rsi_buy_threshold) → registry + M1 evaluation."""
    _, reg0 = get("/api/signal-registry")
    orig_thr = (reg0.get("thresholds") or {}).get("rsi_buy_threshold", 40.0)
    try:
        code, resp = api("PUT", "/api/signals/registry",
                         {"thresholds": {"rsi_buy_threshold": 40.0}})
        assert code == 200 and (resp or {}).get("ok"), f"threshold PUT failed: {resp}"
        _, g1 = get("/api/debug/evaluate-gates?symbol=BTCUSDT&rsi=30")
        assert "M1_rsi_below_threshold" in g1.get("fired_signals", []), \
            f"M1 should fire at rsi=30 < 40: {g1}"
        # Nested (per-signal) shape — what the SignalsEditorPanel sends.
        code, resp = api("PUT", "/api/signals/registry",
                         {"thresholds": {"M1_rsi_below_threshold":
                                         {"rsi_buy_threshold": 25.0}}})
        assert code == 200 and (resp or {}).get("ok"), f"nested threshold PUT failed: {resp}"
        _, reg = get("/api/signal-registry")
        assert approx(reg["thresholds"]["rsi_buy_threshold"], 25.0), \
            f"registry does not show new bound: {reg['thresholds']}"
        m1 = next(s for s in reg["signals"] if s["id"] == "M1_rsi_below_threshold")
        assert approx(m1.get("thresholds", {}).get("rsi_buy_threshold"), 25.0), \
            f"per-signal thresholds not exposed: {m1}"
        _, g2 = get("/api/debug/evaluate-gates?symbol=BTCUSDT&rsi=30")
        assert "M1_rsi_below_threshold" not in g2.get("fired_signals", []), \
            f"M1 still fires at rsi=30 with threshold 25: {g2}"
        return "M1 fires @rsi30/thr40, silent @thr25; registry shows 25.0"
    finally:
        api("PUT", "/api/signals/registry",
            {"thresholds": {"rsi_buy_threshold": orig_thr}})


def test_7_min_profit_usdt():
    """exits.min_profit_usdt → exit-geometry probe BEP shifts."""
    orig = current_exits()
    try:
        put_strategy({"exits": {"min_profit_usdt": 0.01}})
        g1 = geometry(entry=100.0, budget=50.0)
        put_strategy({"exits": {"min_profit_usdt": 0.9}})
        g2 = geometry(entry=100.0, budget=50.0)
        qty, taker = g1["qty"], g1["taker_fee"]
        expected_shift = (0.9 - 0.01) / (qty * (1.0 - taker))
        actual_shift = g2["bep"] - g1["bep"]
        assert actual_shift > 0, "BEP did not move up with min_profit_usdt"
        assert abs(actual_shift - expected_shift) < 1e-6, \
            f"BEP shift {actual_shift} != expected {expected_shift}"
        return f"BEP {g1['bep']:.6f}→{g2['bep']:.6f} (min_profit 0.01→0.90 USDT)"
    finally:
        put_strategy({"exits": {"min_profit_usdt": orig["min_profit_usdt"]}})


def test_8_daily_loss_stop_pct():
    """risk.daily_loss_stop_pct → /api/risk/status daily.limit changes NOW."""
    _, doc = get("/api/strategy")
    orig = dict(doc["config"]["risk"])
    _, sdoc = get("/api/strategy")
    orig_alloc = sdoc["config"]["sizing"]["bot_allocation_usdt"]
    try:
        # Fix allocation so limit_usdt is deterministic.
        put_strategy({"sizing": {"bot_allocation_usdt": 200.0},
                      "risk": {"daily_loss_stop_pct": 2.0}})
        _, rs1 = get("/api/risk/status")
        d1 = rs1["daily"]
        assert approx(d1["limit_pct"], 2.0) and approx(d1["limit_usdt"], 4.0), \
            f"baseline daily limit wrong: {d1}"
        put_strategy({"risk": {"daily_loss_stop_pct": 7.5}})
        _, rs2 = get("/api/risk/status")
        d2 = rs2["daily"]
        assert approx(d2["limit_pct"], 7.5) and approx(d2["limit_usdt"], 15.0), \
            f"daily limit not hot-applied (30s-cache bug?): {d2}"
        return f"daily limit 2.0%/4.0 USDT → 7.5%/15.0 USDT immediately"
    finally:
        put_strategy({"sizing": {"bot_allocation_usdt": orig_alloc},
                      "risk": {"daily_loss_stop_pct": orig["daily_loss_stop_pct"]}})


TESTS = [
    ("1 exits.rr_ratio/tp_buffer → TP geometry",      test_1_rr_ratio_tp_buffer),
    ("2 exits.k_sl + clamps → stop distance",         test_2_k_sl_clamps),
    ("3 budget_fixed/sizing.allocation → budget",     test_3_budget),
    ("4 sizing.max_positions → effective_slots",      test_4_max_positions),
    ("5 signal role → veto flips gate outcome",       test_5_role_flip_veto),
    ("6 rsi_buy_threshold → registry + M1 eval",      test_6_rsi_threshold),
    ("7 exits.min_profit_usdt → BEP shift",           test_7_min_profit_usdt),
    ("8 risk.daily_loss_stop_pct → daily limit",      test_8_daily_loss_stop_pct),
]


def main() -> int:
    global BASE
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--base", default=BASE, help="bot base URL")
    args = ap.parse_args()
    BASE = args.base.rstrip("/")

    code, ping = get("/api/ping")
    if code != 200 or not (ping or {}).get("ok"):
        print(f"FATAL: bot not reachable at {BASE} ({code}: {ping})")
        return 1
    if ping.get("mode") == "live":
        print("FATAL: refusing to run effect tests against a LIVE bot")
        return 1

    failures = 0
    for name, fn in TESTS:
        try:
            detail = fn()
            print(f"PASS  {name}  — {detail}")
        except Exception as e:
            failures += 1
            print(f"FAIL  {name}  — {type(e).__name__}: {e}")
    total = len(TESTS)
    print(f"\n{total - failures}/{total} effect tests passed"
          + ("" if failures == 0 else f" ({failures} FAILED)"))
    return 0 if failures == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
