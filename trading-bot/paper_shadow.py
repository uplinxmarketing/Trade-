"""WolfBot v0.5 Part S3 — paper-shadow data generator (risk-free data flywheel).

WHAT THIS IS
============
A read-only shadow of the live bot that runs in ONE daemon thread on the SAME
real-time market data. On every eval cadence it:

  1. evaluates the SAME entry decision the live bot uses
     (``signal_registry.evaluate_buy_decision`` fed the SAME signal_data the live
     buy loop assembles) for every valid universe symbol NOT already in a PAPER
     position;
  2. OPENS a PAPER position at a MODELED fill (mid + half-spread + slippage, fees
     via ``fees.FeeModel``) — NO real order is ever placed, a SEPARATE paper
     ledger is kept entirely in this module;
  3. tracks each open paper position against the latest live price and evaluates
     the SAME exit MATH the live engine uses (hard-stop → protective/breakeven
     stop → ratchet → take-profit, same ordering) — computed from the SAME
     ``exits.*`` config numbers the live engine resolves, applied to the paper
     position's own price path;
  4. on every paper exit writes ONE labeled training sample via
     ``database.save_training_sample("paper_shadow", ...)``.

It is NOT slot-limited — it takes every qualifying setup (bounded only by a cap
on concurrent open paper positions) so it produces far more clean labeled
outcomes/day than the live bot.

ABSOLUTE SAFETY RULES (enforced by construction)
================================================
  * NEVER places a Binance order and NEVER touches any live position, live
    ``_ratchet_state``/``_stop_loss_confirmation`` dict, or any live-mutating
    trade_engine function. The only trade_engine calls made are PURE/read-only
    config + data resolvers:
      - ``trade_engine._load_strategy()``          (mtime-cached config read)
      - ``trade_engine._exit_cfg()``               (pure exits-config resolver)
      - ``trade_engine._ratchet_cfg()``            (pure ratchet-config resolver)
      - ``trade_engine._atr_pct_5m_at_entry()``    (read-only ATR from buffers/DB)
      - ``trade_engine.get_btc_regime()``          (read-only 60s-cached regime)
      - ``trade_engine._signal_cache`` snapshot    (copied under its lock)
    The exit GEOMETRY is REPLICATED here (paper keeps its OWN copy of the numbers
    resolved from those pure config readers and applies them to the paper
    position dict) — the live exit decision code is never invoked, so no live
    per-symbol exit state is ever mutated.
  * Every cycle is wrapped in try/except; a paper-shadow failure is logged and
    can NEVER affect the live bot (separate daemon thread, all exceptions caught).

SHADOW LAB — virtual budget + broad sampling (v0.5 S3 scale-up)
===============================================================
  * A VIRTUAL budget (``data.paper_shadow_budget_usdt``, default 10000) is
    deployed in fixed ``data.paper_shadow_position_usdt`` (default 11) tranches.
    The effective concurrent cap = ``min(paper_shadow_max_open, floor(budget /
    position))`` and hard-bounds the open ledger.
  * A symbol may hold UP TO ``data.paper_shadow_max_per_symbol`` (default 20)
    concurrent paper positions — so a coin can RE-ENTER and generate many
    outcomes/day, far more than the ~20 live trades. Each cycle every qualifying
    symbol opens ONE more tranche if under BOTH the per-symbol and the global
    (budget / concurrency) caps.
  * On every exit ONE labeled ``training_samples`` row AND one rich
    ``save_paper_trade`` row are written (guarded), the latter carrying
    wolfscore/regime (computed at entry) + the exit ladder branch that fired.

MEMORY DISCIPLINE (a leak was JUST fixed elsewhere — do not reintroduce one)
===========================================================================
  * Per-cycle evaluation is STATELESS: nothing about a rejected/evaluated symbol
    is retained.
  * Only OPEN paper positions persist in memory, in ONE BOUNDED dict hard-capped
    at the effective concurrent cap (never exceeds ``paper_shadow_max_open``).
    Even a 1000+ cap is just ≤ cap small dicts. New opens are refused (logged
    ONCE, not per-cycle) when the budget or the cap is reached; positions open
    longer than ``paper_shadow_max_hold_sec`` are force-closed (emitting their
    outcome) so nothing is retained forever.
  * DB writes are THROTTLED: at most ``_MAX_CLOSES_PER_CYCLE`` positions are
    closed (and written) per cycle, so a burst of simultaneous exits can NEVER
    stall the loop or hammer SQLite — the remainder stay in the bounded ledger
    and are drained on subsequent cycles.
  * Only OUTCOMES persist — to the DB (``training_samples`` + ``paper_trades``,
    hard-capped there). No growing in-memory history of any kind.
"""
from __future__ import annotations

import threading
import time
import logging
import random
from typing import Any, Dict, Optional, Tuple

log = logging.getLogger(__name__)

# ── Tunables (all overridable via strategy.json data.* block) ──────────────────
_DEFAULT_LOOP_SEC          = 8.0        # eval cadence (aligned to the live scanner)
_DEFAULT_POSITION_USDT     = 11.0       # fixed virtual notional per paper tranche
_DEFAULT_BUDGET_USDT       = 10000.0    # total virtual budget to deploy
_DEFAULT_MAX_OPEN          = 300        # hard cap on concurrent open paper positions
_DEFAULT_MAX_PER_SYMBOL    = 20         # concurrent paper positions allowed per symbol
_DEFAULT_SLIPPAGE_BPS      = 2.0        # modeled slippage each side (basis points)
_DEFAULT_MAX_HOLD_SEC      = 6 * 3600.0 # force-close + emit a stale paper position
_STATS_SAMPLE_LIMIT        = 5000       # bound get_paper_stats DB read
_MAX_CLOSES_PER_CYCLE      = 40         # DB-write throttle: exits emitted per cycle


def _num(v: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        if v is None:
            return default
        f = float(v)
        return f
    except (TypeError, ValueError):
        return default


class PaperShadow:
    """Singleton paper-shadow engine. One daemon thread; a bounded ledger of open
    paper positions; every exit emits a labeled training sample."""

    def __init__(self) -> None:
        self._thread: Optional[threading.Thread] = None
        self._stop_evt = threading.Event()
        self._lock = threading.Lock()
        # Bounded ledger — unique int id → paper position dict. ONE dict, hard-
        # capped at the effective concurrent cap; NEVER grows past it. Keyed by an
        # incrementing id (not symbol) so a symbol may re-enter up to max_per_symbol.
        self._open: Dict[int, dict] = {}
        self._sym_count: Dict[str, int] = {}   # symbol → live count in the ledger
        self._deployed: float = 0.0            # virtual budget currently deployed
        self._next_id: int = 0                 # monotonic position-id counter
        self._full_logged = False              # "cap/budget reached" logged ONCE
        self._started = False

    # ── config helpers (read-only, hot-reloadable) ────────────────────────────
    def _data_cfg(self) -> dict:
        try:
            import trade_engine
            raw = trade_engine._load_strategy().get("data")
            return raw if isinstance(raw, dict) else {}
        except Exception:
            return {}

    def _enabled(self) -> bool:
        # Gated on data.paper_shadow_enabled, default True.
        cfg = self._data_cfg()
        val = cfg.get("paper_shadow_enabled", True)
        try:
            return bool(val)
        except Exception:
            return True

    def _loop_sec(self) -> float:
        return max(3.0, _num(self._data_cfg().get("paper_shadow_loop_sec"),
                             _DEFAULT_LOOP_SEC) or _DEFAULT_LOOP_SEC)

    def _position_usdt(self) -> float:
        n = _num(self._data_cfg().get("paper_shadow_position_usdt"),
                 _DEFAULT_POSITION_USDT) or _DEFAULT_POSITION_USDT
        return n if n > 0 else _DEFAULT_POSITION_USDT

    def _budget(self) -> float:
        b = _num(self._data_cfg().get("paper_shadow_budget_usdt"),
                 _DEFAULT_BUDGET_USDT) or _DEFAULT_BUDGET_USDT
        return b if b > 0 else _DEFAULT_BUDGET_USDT

    def _max_per_symbol(self) -> int:
        m = _num(self._data_cfg().get("paper_shadow_max_per_symbol"),
                 _DEFAULT_MAX_PER_SYMBOL) or _DEFAULT_MAX_PER_SYMBOL
        return int(m) if m >= 1 else _DEFAULT_MAX_PER_SYMBOL

    def _effective_cap(self) -> int:
        """min(paper_shadow_max_open, floor(budget / position)) — the single hard
        bound on the open ledger. Never exceeds paper_shadow_max_open."""
        mo = _num(self._data_cfg().get("paper_shadow_max_open"), _DEFAULT_MAX_OPEN)
        max_open = int(mo) if (mo is not None and mo >= 1) else _DEFAULT_MAX_OPEN
        pos = self._position_usdt()
        by_budget = int(self._budget() // pos) if pos > 0 else max_open
        cap = min(max_open, by_budget)
        return cap if cap >= 1 else 1

    def _slippage_bps(self) -> float:
        s = _num(self._data_cfg().get("paper_shadow_slippage_bps"),
                 _DEFAULT_SLIPPAGE_BPS)
        return max(0.0, s if s is not None else _DEFAULT_SLIPPAGE_BPS)

    def _max_hold_sec(self) -> float:
        h = _num(self._data_cfg().get("paper_shadow_max_hold_sec"),
                 _DEFAULT_MAX_HOLD_SEC) or _DEFAULT_MAX_HOLD_SEC
        return h if h > 0 else _DEFAULT_MAX_HOLD_SEC

    # ── lifecycle ─────────────────────────────────────────────────────────────
    def start(self) -> bool:
        """Spawn the single daemon thread (idempotent). Returns True if the loop
        is running after the call. Never raises."""
        try:
            with self._lock:
                if not self._enabled():
                    log.info("[PaperShadow] disabled via data.paper_shadow_enabled — not starting")
                    return False
                if self._thread is not None and self._thread.is_alive():
                    return True
                self._stop_evt.clear()
                t = threading.Thread(target=self._run, name="paper-shadow", daemon=True)
                self._thread = t
                self._started = True
                t.start()
                log.info("[PaperShadow] started")
                return True
        except Exception as e:  # pragma: no cover — start must never break boot
            log.error("[PaperShadow] start failed: %s", e)
            return False

    def stop(self) -> None:
        """Signal the loop to exit (best-effort; the thread is a daemon)."""
        self._stop_evt.set()
        self._started = False

    def is_running(self) -> bool:
        t = self._thread
        return bool(t is not None and t.is_alive() and not self._stop_evt.is_set())

    # ── main loop ─────────────────────────────────────────────────────────────
    def _run(self) -> None:
        # Every iteration is fully guarded — a paper-shadow failure must NEVER
        # propagate to (or stall) anything else.
        while not self._stop_evt.is_set():
            t0 = time.time()
            try:
                if self._enabled():
                    self._cycle()
            except Exception as e:  # pragma: no cover — belt-and-braces
                log.warning("[PaperShadow] cycle error: %s", e)
            # Sleep the remainder of the cadence (interruptible on stop).
            elapsed = time.time() - t0
            self._stop_evt.wait(max(0.5, self._loop_sec() - elapsed))

    def _cycle(self) -> None:
        import trade_engine

        strategy = trade_engine._load_strategy() or {}
        approved = [
            c["symbol"] for c in strategy.get("approved_coins", [])
            if isinstance(c, dict) and c.get("approved") and c.get("symbol")
        ]
        if not approved:
            # Still manage/expire any open paper positions even with no universe.
            self._manage_open(strategy, set())
            return
        universe = set(approved)
        effective_cap = self._effective_cap()
        max_per_symbol = self._max_per_symbol()
        position_usdt = self._position_usdt()
        budget = self._budget()

        # Snapshot the SAME signal cache the live buy loop reads (under its lock).
        try:
            with trade_engine._signal_cache_lock:
                cache_snapshot = dict(trade_engine._signal_cache)
        except Exception:
            cache_snapshot = {}

        # 1) Manage existing open paper positions (exits + stale eviction) first,
        #    freeing ledger room + budget before evaluating new entries this cycle.
        self._manage_open(strategy, universe)

        # 2) Entry evaluation — STATELESS per symbol; only opens persist. Compute
        #    the regime tilt + cohort ONCE per cycle (O(universe)) for wolfscore.
        try:
            btc_regime = trade_engine.get_btc_regime()
        except Exception:
            btc_regime = None
        try:
            import ev_model
            tilt = ev_model.regime_tilt(trade_engine._btc_roc_1h_frac())
        except Exception:
            tilt = 0.0
        try:
            cohort = trade_engine._wolf_cohort_from(list(cache_snapshot.items()))
        except Exception:
            cohort = {}

        # Representativeness (Part-B constraint): evaluate candidates in a RANDOM
        # order each cycle. With a low shadow_max_open the ledger fills before the
        # whole universe is seen; a fixed order would then always open the same
        # first-listed coins and silently bias the training set. Shuffling makes the
        # opened subset a representative sample across the full score/regime range.
        # No score/regime FILTER is applied — the shadow stays unbiased by design.
        _eval_order = list(approved)
        random.shuffle(_eval_order)
        for sym in _eval_order:
            if self._stop_evt.is_set():
                return
            # Fast pre-check under lock — O(1). Refuse (log ONCE) when the global
            # budget/concurrency cap is reached; skip this symbol when at its
            # per-symbol cap. The definitive re-check happens at insert time.
            with self._lock:
                if len(self._open) >= effective_cap or \
                        (self._deployed + position_usdt) > budget:
                    if not self._full_logged:
                        log.info("[PaperShadow] cap reached — open=%d/%d deployed=%.0f/%.0f "
                                 "budget; pausing new opens", len(self._open),
                                 effective_cap, self._deployed, budget)
                        self._full_logged = True
                    break
                if self._sym_count.get(sym, 0) >= max_per_symbol:
                    continue  # this coin is saturated; others may still open
            cached = cache_snapshot.get(sym)
            if not isinstance(cached, dict):
                continue
            try:
                self._maybe_open(sym, cached, strategy, btc_regime,
                                 effective_cap, max_per_symbol, position_usdt,
                                 budget, cohort, tilt)
            except Exception as e:
                log.debug("[PaperShadow] open-eval %s failed: %s", sym, e)

    # ── entry ─────────────────────────────────────────────────────────────────
    def _build_signal_data(self, cached: dict, btc_regime: Any) -> dict:
        """Assemble the SAME signal_data dict the live buy loop feeds
        signal_registry (trade_engine buy loop §8529-8544)."""
        sigs = cached.get("signals", {}) or {}
        return {
            **sigs,
            "rsi_value":       cached.get("rsi_val", 0.0),
            "current_price":   cached.get("price", 0.0),
            "low_24h":         cached.get("low_24h"),
            "klines_1m":       cached.get("klines_1m", []),
            "stoch_rsi_value": cached.get("stoch_rsi_val"),
            "klines_5m":       cached.get("klines_5m", []),
            "ema50_15m_slope": cached.get("ema50_15m_slope"),
            "bb_position_5m":  cached.get("bb_position_5m"),
            "atr_pct":         cached.get("atr_pct"),
            "btc_regime":      btc_regime,
        }

    def _maybe_open(self, sym: str, cached: dict, strategy: dict,
                    btc_regime: Any, effective_cap: int, max_per_symbol: int,
                    position_usdt: float, budget: float,
                    cohort: dict, tilt: float) -> None:
        import signal_registry
        import ev_model

        sig_data = self._build_signal_data(cached, btc_regime)
        # SAME entry decision the live bot uses — reuse, don't reinvent.
        decision = signal_registry.evaluate_buy_decision(sym, sig_data, strategy)
        if not isinstance(decision, dict) or not decision.get("allowed"):
            return  # stateless: rejected setups leave no trace

        price = _num(self._live_price(sym), None) or _num(cached.get("price"), None)
        if not price or price <= 0:
            return

        pos = self._open_paper(sym, price, strategy, position_usdt)
        if pos is None:
            return
        try:
            pos["ev_score"] = ev_model.score(sig_data).get("probability")
        except Exception:
            pos["ev_score"] = None
        # WolfScore + regime AT ENTRY (guarded) — reuses the live compute_submetrics
        # + wolfscore path.
        wolf = None
        try:
            import trade_engine
            wolf = trade_engine._wolf_score_cached(sym, cached, cohort, tilt)
        except Exception:
            wolf = None
        if isinstance(wolf, dict):
            pos["wolfscore"] = wolf.get("pct")
            pos["regime"] = wolf.get("regime") or btc_regime
            # CRITICAL — the training sample MUST carry the WolfScore SUBMETRICS +
            # regime_tilt, exactly like the live path (trade_engine ev_submetrics/
            # ev_regime_tilt). Previously this stored ev_model.extract_features()
            # (the legacy flat EV feature names), which train_wolfscore cannot read
            # — feats.get('submetrics') was None and compute_submetrics() got the
            # wrong keys, so every paper sample degraded to all-zero submetrics and
            # the trained model was garbage (the "Retrain does nothing" bug).
            pos["ev_features"] = {
                "submetrics":  wolf.get("submetrics") or {},
                "regime_tilt": wolf.get("regime_tilt", 0.0),
            }
        else:
            pos["wolfscore"] = None
            pos["regime"] = btc_regime
            pos["ev_features"] = {}

        with self._lock:
            # Definitive cap re-check under lock (all counters are single-thread
            # written here, but keep the ledger strictly within every bound).
            if len(self._open) >= effective_cap:
                return
            if (self._deployed + position_usdt) > budget:
                return
            if self._sym_count.get(sym, 0) >= max_per_symbol:
                return
            pid = self._next_id
            self._next_id += 1
            self._open[pid] = pos
            self._sym_count[sym] = self._sym_count.get(sym, 0) + 1
            self._deployed += position_usdt

    def _open_paper(self, sym: str, mid: float, strategy: dict,
                    position_usdt: float) -> Optional[dict]:
        """Open a paper position at a MODELED fill and compute exit geometry from
        the SAME config numbers the live engine resolves. Places NO real order."""
        import trade_engine
        import fees

        fm = fees.get_fee_model(sym)
        taker = fm.taker()
        half_spread_frac = self._half_spread_frac(sym, mid)
        slip_frac = self._slippage_bps() / 10000.0

        # BUY fill = mid + half-spread + slippage (adverse for a taker buy).
        buy_fill = mid * (1.0 + half_spread_frac + slip_frac)
        if buy_fill <= 0:
            return None
        notional = position_usdt
        qty = notional / buy_fill
        buy_fee = qty * buy_fill * taker

        pos = {
            "symbol":     sym,
            "entry_price": buy_fill,
            "quantity":   qty,
            "buy_fee_usdt": buy_fee,
            "deployed_usdt": position_usdt,
            "opened_ts":  time.time(),
            "be_moved":   False,
            "trail_armed": False,
            "peak_price": buy_fill,
            "_sl_confirm": 0,
            "_ratchet":   {"armed": False, "peak_price": buy_fill, "peak_profit": 0.0},
        }
        # Exit geometry — paper keeps its OWN copy of the numbers, resolved from
        # the pure config readers the live engine uses (never mutates live state).
        self._apply_paper_geometry(pos, strategy, fm)
        return pos

    def _apply_paper_geometry(self, pos: dict, strategy: dict, fm) -> None:
        """Replicate trade_engine._apply_entry_exit_geometry math onto the PAPER
        position, using trade_engine._exit_cfg() (a pure config resolver). Keeps
        paper's own copy of every number — the live geometry function is never
        called with the paper position."""
        import trade_engine

        try:
            cfg = trade_engine._exit_cfg()
        except Exception:
            cfg = self._exit_cfg_fallback(strategy)
        pos["_exit_cfg"] = cfg

        sym = pos["symbol"]
        entry = float(pos["entry_price"])

        # ATR% at entry — read-only source ladder (identical to the live engine).
        atr_pct = None
        try:
            atr_pct, _src = trade_engine._atr_pct_5m_at_entry(sym, entry)
        except Exception:
            atr_pct = None
        pos["atr_pct_at_entry"] = round(atr_pct, 6) if atr_pct else None

        if cfg.get("legacy_mode"):
            sl_dist = cfg["sl_min_pct"] if cfg.get("sl_enabled") else None
        elif atr_pct is None:
            sl_dist = cfg["sl_min_pct"]
        else:
            sl_dist = min(max(cfg["k_sl"] * atr_pct, cfg["sl_min_pct"]),
                          cfg["sl_max_pct"])
        pos["sl_distance_pct"] = round(sl_dist, 6) if sl_dist is not None else None
        pos["stop_price"] = (entry * (1.0 - sl_dist / 100.0)) if sl_dist else None

        # Break-even price from the fee model (paper-local; self-contained).
        rt = fm.round_trip(entry_is_maker=False, exit_is_maker=False)
        bep = entry * (1.0 + rt)
        pos["bep"] = bep

        if cfg.get("rr_ratio"):
            tp_dist = cfg["rr_ratio"] * (sl_dist if sl_dist else cfg["sl_min_pct"])
        else:
            tp_dist = (cfg.get("tp_pct") or 0.0) if cfg.get("tp_enabled") else 0.0
        pos["tp_distance_pct"] = round(tp_dist, 6)
        tp = entry * (1.0 + tp_dist / 100.0)
        if bep > 0:
            tp = max(tp, bep * (1.0 + (cfg.get("tp_buffer_pct") or 0.0) / 100.0))
        pos["tp_price"] = tp
        pos["hard_sl_price"] = (entry * (1.0 - cfg["hard_sl_pct"] / 100.0)
                                if cfg.get("hard_sl_pct") else None)

    def _exit_cfg_fallback(self, strategy: dict) -> dict:
        """Minimal local copy of the exit-config defaults so paper geometry still
        resolves if trade_engine._exit_cfg() is unavailable. Mirrors
        trade_engine._exit_config_fallback defaults (non-legacy branch)."""
        exits = strategy.get("exits") if isinstance(strategy, dict) else None
        exits = exits if isinstance(exits, dict) else {}

        def _f(k, d):
            return _num(exits.get(k, d), d)

        return {
            "k_sl": _f("k_sl", 1.2), "sl_min_pct": _f("sl_min_pct", 0.5),
            "sl_max_pct": _f("sl_max_pct", 2.5), "hard_sl_pct": _f("hard_sl_pct", 3.0),
            "rr_ratio": _f("rr_ratio", 1.6), "tp_buffer_pct": _f("tp_buffer_pct", 0.05),
            "min_profit_usdt": _f("min_profit_usdt", 0.01),
            "breakeven_at_r": _f("breakeven_at_r", 1.2), "k_trail": _f("k_trail", 0.8),
            "min_hold_sec": _f("min_hold_sec", 10.0), "sl_confirm_ticks": int(_f("sl_confirm_ticks", 2)),
            "legacy_mode": False, "sl_enabled": True, "tp_enabled": True, "tp_pct": None,
        }

    # ── exits + management (read-only reuse of the exit MATH) ──────────────────
    def _manage_open(self, strategy: dict, universe: set) -> None:
        with self._lock:
            open_ids = list(self._open.keys())
        now = time.time()
        max_hold = self._max_hold_sec()
        closes = 0  # DB-write throttle: bound exits emitted (and written) per cycle
        for pid in open_ids:
            if self._stop_evt.is_set():
                return
            if closes >= _MAX_CLOSES_PER_CYCLE:
                # Burst guard — leave the rest in the bounded ledger; they exit on
                # a later cycle. Prevents a mass-exit from stalling the loop / DB.
                break
            with self._lock:
                pos = self._open.get(pid)
            if pos is None:
                continue
            sym = pos.get("symbol")
            try:
                price = _num(self._live_price(sym), None)
                if not price or price <= 0:
                    # No fresh price — expire only if stale past the hold cap.
                    if now - pos.get("opened_ts", now) > max_hold:
                        self._close_paper(pid, pos, pos.get("entry_price"),
                                          "time_stop", now)
                        closes += 1
                    continue
                reason = self._evaluate_paper_exit(pos, price, now)
                if reason is None and (now - pos.get("opened_ts", now) > max_hold):
                    reason = "time_stop"
                if reason is not None:
                    self._close_paper(pid, pos, price, reason, now)
                    closes += 1
            except Exception as e:
                log.debug("[PaperShadow] manage %s failed: %s", sym, e)

    def _evaluate_paper_exit(self, pos: dict, price: float,
                             now: float) -> Optional[str]:
        """Replicate the live exit MATH read-only on the PAPER position, SAME
        ordering as trade_engine._evaluate_exit_decision:
            hard-stop → BE-move/protective stop → ratchet → take-profit/trail.
        Uses the paper position's OWN copy of the config numbers and its OWN
        per-position state — no live exit state is ever touched."""
        import trade_engine

        cfg = pos.get("_exit_cfg") or self._exit_cfg_fallback({})
        entry = float(pos["entry_price"])
        sl_dist = pos.get("sl_distance_pct")
        opened_ts = pos.get("opened_ts", 0)
        in_min_hold = opened_ts > 0 and (now - opened_ts) < float(cfg.get("min_hold_sec", 10.0))

        # ── HARD SL — immediate, bypasses confirm ticks + min-hold ────────────
        hard_sl = pos.get("hard_sl_price")
        if hard_sl and price <= hard_sl:
            return "hard-stop-loss"

        bep = float(pos.get("bep") or 0.0)
        tp_price = float(pos.get("tp_price") or 0.0)
        tp_trigger = max(tp_price, bep)
        k_trail = float(cfg.get("k_trail") or 0.0)
        trailing_on = k_trail > 0
        stop_price = pos.get("stop_price")
        crossed = tp_trigger > 0 and price >= tp_trigger

        # ── BE-move: raise the protective stop to BEP at +breakeven_at_r × R ──
        if not pos.get("be_moved"):
            gain_pct = (price / entry - 1.0) * 100.0
            be_r = cfg.get("breakeven_at_r")
            if be_r is not None and sl_dist and gain_pct >= float(sl_dist) * float(be_r):
                pos["be_moved"] = True
                be_stop = max(float(stop_price or 0.0), bep)
                if pos.get("orig_stop_price") is None:
                    pos["orig_stop_price"] = stop_price
                pos["be_stop_price"] = be_stop
                pos["stop_price"] = be_stop
                stop_price = be_stop

        protective_stop = stop_price
        if pos.get("be_moved"):
            protective_stop = max(float(pos.get("be_stop_price") or 0.0),
                                  float(stop_price or 0.0))

        # ── Trailing arms only at/after tp_price (F1 decoupling) ──────────────
        if crossed:
            if not trailing_on:
                return "take-profit"
            pos["trail_armed"] = True

        if pos.get("trail_armed"):
            peak = pos["peak_price"] = max(float(pos.get("peak_price") or entry), price)
            atr_ref = (pos.get("atr_pct_at_entry") or sl_dist or cfg.get("sl_min_pct") or 0.0)
            trail_stop = peak * (1.0 - k_trail * float(atr_ref) / 100.0)
            floor = tp_trigger
            eff_trail = max(trail_stop, floor)
            if price <= eff_trail:
                return "trail" if trail_stop > floor else "take-profit"
            return None

        # ── Protective stop (confirm ticks + min-hold), below tp_price ────────
        if protective_stop and price <= protective_stop and not in_min_hold:
            pos["_sl_confirm"] = int(pos.get("_sl_confirm", 0)) + 1
            confirm_ticks = int(cfg.get("sl_confirm_ticks", 2) or 2)
            if pos["_sl_confirm"] >= confirm_ticks:
                ref_stop = pos.get("orig_stop_price")
                if ref_stop is None:
                    ref_stop = stop_price
                be_hit = (pos.get("be_moved") and ref_stop and price > float(ref_stop))
                return "breakeven-stop" if be_hit else "stop-loss"
            return None
        pos["_sl_confirm"] = 0

        # ── Profit-ratchet (paper-local state) — SAME numbers as live ─────────
        try:
            rc = trade_engine._ratchet_cfg()
        except Exception:
            rc = None
        if rc and self._evaluate_paper_ratchet(pos, price, entry, cfg, rc):
            return "profit-ratchet"
        return None

    def _evaluate_paper_ratchet(self, pos: dict, price: float, entry: float,
                                cfg: dict, rc: dict) -> bool:
        """Replicate trade_engine._evaluate_ratchet on paper-local state. Arms at
        +activate_r × 1R (position's own stop distance) or +activate_usdt; trails
        peak by k_atr × ATR(price units); give-back cap; only ever exits in
        profit (min_profit_usdt floor)."""
        if not rc.get("enabled"):
            pos["_ratchet"] = {"armed": False, "peak_price": price, "peak_profit": 0.0}
            return False

        profit = self._paper_unrealized_net(pos, price)
        if profit is None:
            return False

        qty = float(pos.get("quantity") or 0.0)
        sl_dist = pos.get("sl_distance_pct")
        r_usdt = (qty * entry * float(sl_dist) / 100.0
                  if (sl_dist and qty > 0 and entry > 0) else None)

        st = pos.get("_ratchet")
        if not isinstance(st, dict):
            st = {"armed": False, "peak_price": price, "peak_profit": profit}
            pos["_ratchet"] = st

        if not st.get("armed"):
            armed = False
            if r_usdt and r_usdt > 0 and profit >= rc["activate_r"] * r_usdt:
                armed = True
            elif profit >= rc["activate_usdt"]:
                armed = True
            if not armed:
                return False
            st["armed"] = True
            st["peak_price"] = price
            st["peak_profit"] = profit

        if price > st["peak_price"]:
            st["peak_price"] = price
        if profit > st["peak_profit"]:
            st["peak_profit"] = profit
        peak_price = st["peak_price"]
        peak_profit = st["peak_profit"]

        atr_pct = (pos.get("atr_pct_at_entry") or sl_dist or cfg.get("sl_min_pct") or 0.0)
        atr_price = entry * float(atr_pct) / 100.0 if atr_pct else 0.0
        ratchet_stop = peak_price - rc["k_atr"] * atr_price
        trail_hit = atr_price > 0 and price <= ratchet_stop

        giveback_floor = (1.0 - rc["giveback_pct"] / 100.0) * peak_profit
        giveback_hit = peak_profit > 0 and profit <= giveback_floor

        if not (trail_hit or giveback_hit):
            return False
        # Ratchet only ever exits IN PROFIT (min_profit_usdt floor).
        if profit < float(cfg.get("min_profit_usdt", 0.01) or 0.0):
            return False
        return True

    def _paper_unrealized_net(self, pos: dict, price: float) -> Optional[float]:
        """Net unrealized USDT profit if the paper position were market-sold NOW
        (deployed capital + taker exit fee) — mirrors _unrealized_net_profit."""
        import fees
        try:
            entry = float(pos.get("entry_price") or 0.0)
            qty = float(pos.get("quantity") or 0.0)
            buy_fee = float(pos.get("buy_fee_usdt") or 0.0)
            if entry <= 0 or qty <= 0 or price <= 0:
                return None
            taker = fees.get_fee_model(pos["symbol"]).taker()
            gross_quote = price * qty
            net_returned = gross_quote - gross_quote * taker
            return net_returned - (qty * entry + buy_fee)
        except Exception:
            return None

    @staticmethod
    def _exit_type(reason: str) -> str:
        """Map the exit-ladder branch that fired to a compact exit_type label:
        hard_stop / breakeven / ratchet / tp / time_stop."""
        r = str(reason or "")
        if "hard-stop" in r:
            return "hard_stop"
        if "breakeven" in r:
            return "breakeven"
        if "ratchet" in r:
            return "ratchet"
        if "take-profit" in r or "trail" in r:
            return "tp"
        if "stop-loss" in r:
            return "hard_stop"
        return "time_stop"  # time_stop / stale / anything else

    def _close_paper(self, pid: int, pos: dict, exit_mid: Optional[float],
                     reason: str, now: float) -> None:
        """Close a paper position at a MODELED sell fill, compute realized_r, emit
        ONE labeled training sample + ONE rich paper_trades row, and drop it from
        the bounded ledger (freeing its virtual budget + per-symbol slot)."""
        import fees
        import database

        sym = pos.get("symbol")
        # Remove from the ledger FIRST so a save failure can't leak the position;
        # release its virtual budget + per-symbol count under the same lock.
        with self._lock:
            if self._open.pop(pid, None) is not None:
                self._deployed = max(0.0, self._deployed
                                     - float(pos.get("deployed_usdt") or 0.0))
                c = self._sym_count.get(sym, 0) - 1
                if c > 0:
                    self._sym_count[sym] = c
                else:
                    self._sym_count.pop(sym, None)
                # Room again — allow the "cap reached" notice to re-log later.
                self._full_logged = False

        try:
            entry = float(pos.get("entry_price") or 0.0)
            qty = float(pos.get("quantity") or 0.0)
            buy_fee = float(pos.get("buy_fee_usdt") or 0.0)
            mid = _num(exit_mid, entry) or entry
            fm = fees.get_fee_model(sym)
            taker = fm.taker()
            half_spread_frac = self._half_spread_frac(sym, mid)
            slip_frac = self._slippage_bps() / 10000.0

            # SELL fill = mid − half-spread − slippage (adverse for a taker sell).
            sell_fill = mid * (1.0 - half_spread_frac - slip_frac)
            if sell_fill < 0:
                sell_fill = 0.0
            proceeds_gross = qty * sell_fill
            sell_fee = proceeds_gross * taker
            net_proceeds = proceeds_gross - sell_fee
            cost_basis = qty * entry + buy_fee
            pnl = net_proceeds - cost_basis

            # 1R risk in USDT = paper stop distance (falls back to sl_min_pct).
            sl_dist = pos.get("sl_distance_pct")
            cfg = pos.get("_exit_cfg") or {}
            if not sl_dist:
                sl_dist = cfg.get("sl_min_pct") or 0.5
            risk = qty * entry * float(sl_dist) / 100.0
            realized_r = (pnl / risk) if risk > 0 else 0.0

            label = 1 if realized_r > 0 else 0
            features = pos.get("ev_features") or {}
            exit_type = self._exit_type(reason)
            # 1) Training sample — unchanged contract (submetrics/regime_tilt live
            #    inside the features dict the EV path produced).
            database.save_training_sample("paper_shadow", sym, features,
                                          label, realized_r)
            # 2) Rich outcome row — guarded (a parallel agent adds save_paper_trade;
            #    tolerate its absence so this never breaks the paper loop).
            try:
                database.save_paper_trade({
                    "ts":         now,
                    "symbol":     sym,
                    "wolfscore":  pos.get("wolfscore"),
                    "regime":     pos.get("regime"),
                    "exit_type":  exit_type,
                    "entry_px":   entry,
                    "exit_px":    sell_fill,
                    "pnl":        pnl,
                    "realized_r": realized_r,
                    "hold_sec":   max(0.0, now - float(pos.get("opened_ts", now))),
                    "label":      label,
                })
            except AttributeError:
                pass  # save_paper_trade not present yet — training row still logged
            except Exception as e:
                log.debug("[PaperShadow] save_paper_trade %s failed: %s", sym, e)
            log.debug("[PaperShadow] EXIT %s type=%s reason=%s R=%.3f pnl=%.4f",
                      sym, exit_type, reason, realized_r, pnl)
        except Exception as e:
            log.debug("[PaperShadow] close %s failed: %s", sym, e)

    # ── market-data helpers (read-only) ───────────────────────────────────────
    def _live_price(self, sym: str) -> Optional[float]:
        try:
            import data_collector
            p = data_collector.prices.get(sym)
            return float(p) if p else None
        except Exception:
            return None

    def _half_spread_frac(self, sym: str, mid: float) -> float:
        """Half the modeled bid-ask spread as a FRACTION of mid. Reads the live
        book spread from the in-memory market snapshot; falls back to a small
        default when no quote is available."""
        try:
            import data_collector
            snap = data_collector.get_market_snapshot([sym]).get(sym) or {}
            spread_pct = _num(snap.get("spread_pct"), None)
            if spread_pct is not None and spread_pct >= 0:
                return (spread_pct / 100.0) / 2.0
        except Exception:
            pass
        # Fallback: a conservative default half-spread of ~1 bp.
        return 0.0001

    # ── stats (S3.4) — computed from the paper training rows, bounded ──────────
    def get_paper_stats(self) -> dict:
        """{open_positions, trades_today, expectancy, win_rate, avg_win_r,
        avg_loss_r, n_total} from the paper_shadow training rows. Bounded/cheap —
        reads at most _STATS_SAMPLE_LIMIT recent rows."""
        with self._lock:
            open_positions = len(self._open)
            deployed_budget = round(self._deployed, 2)
        try:
            effective_cap = self._effective_cap()
        except Exception:
            effective_cap = 0
        try:
            budget = self._budget()
        except Exception:
            budget = 0.0

        rows = []
        try:
            import database
            rows = database.get_training_samples(limit=_STATS_SAMPLE_LIMIT,
                                                 modes=["paper_shadow"])
        except Exception:
            rows = []

        n_total = len(rows)
        wins = []
        losses = []
        day_cutoff = time.time() - 86400.0
        trades_today = 0
        r_sum = 0.0
        r_n = 0
        for r in rows:
            rv = _num(r.get("realized_r"), None)
            if _num(r.get("ts"), 0.0) >= day_cutoff:
                trades_today += 1
            if rv is None:
                continue
            r_sum += rv
            r_n += 1
            if rv > 0:
                wins.append(rv)
            else:
                losses.append(rv)

        n_win = len(wins)
        n_loss = len(losses)
        win_rate = (n_win / (n_win + n_loss)) if (n_win + n_loss) > 0 else 0.0
        avg_win_r = (sum(wins) / n_win) if n_win else 0.0
        avg_loss_r = (sum(losses) / n_loss) if n_loss else 0.0
        expectancy = (r_sum / r_n) if r_n else 0.0

        return {
            "open_positions":  open_positions,
            "deployed_budget": deployed_budget,
            "effective_cap":   effective_cap,
            "budget":          budget,
            "trades_today":    trades_today,
            "expectancy":      round(expectancy, 4),
            "win_rate":        round(win_rate, 4),
            "avg_win_r":       round(avg_win_r, 4),
            "avg_loss_r":      round(avg_loss_r, 4),
            "n_total":         n_total,
        }

    def get_shadow_summary(self) -> dict:
        """Copy-paste-able results summary for control_api / UI. Delegates to
        database.get_paper_summary() (added by a parallel agent) — guarded so it
        returns {} rather than raising when that accessor isn't present yet."""
        try:
            import database
            summary = database.get_paper_summary()
            return summary if isinstance(summary, dict) else {}
        except Exception:
            return {}


# ── Module-level singleton + thin wrappers ────────────────────────────────────
_engine = PaperShadow()


def start() -> bool:
    return _engine.start()


def stop() -> None:
    _engine.stop()


def is_running() -> bool:
    return _engine.is_running()


def get_paper_stats() -> dict:
    return _engine.get_paper_stats()


def get_shadow_summary() -> dict:
    return _engine.get_shadow_summary()
