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

MEMORY DISCIPLINE (a leak was JUST fixed elsewhere — do not reintroduce one)
===========================================================================
  * Per-cycle evaluation is STATELESS: nothing about a rejected/evaluated symbol
    is retained.
  * Only OPEN paper positions persist in memory, in a BOUNDED dict capped at
    ``paper_shadow_max_open`` (default = universe size). New opens are refused
    when the cap is reached; positions open longer than ``paper_shadow_max_hold_sec``
    are force-closed (emitting their outcome) so nothing is retained forever.
  * Only OUTCOMES persist — to the DB (``training_samples``, hard-capped there).
    No growing in-memory history of any kind.
"""
from __future__ import annotations

import threading
import time
import logging
from typing import Any, Dict, Optional, Tuple

log = logging.getLogger(__name__)

# ── Tunables (all overridable via strategy.json data.* block) ──────────────────
_DEFAULT_LOOP_SEC          = 8.0        # eval cadence (aligned to the live scanner)
_DEFAULT_NOTIONAL_USDT     = 100.0      # fixed nominal notional per paper trade
_DEFAULT_SLIPPAGE_BPS      = 2.0        # modeled slippage each side (basis points)
_DEFAULT_MAX_HOLD_SEC      = 6 * 3600.0 # force-close + emit a stale paper position
_DEFAULT_MAX_OPEN_FALLBACK = 200        # cap when the universe size can't be read
_STATS_SAMPLE_LIMIT        = 5000       # bound get_paper_stats DB read


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
        # Bounded ledger — symbol → paper position dict. NEVER grows past the cap.
        self._open: Dict[str, dict] = {}
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

    def _notional(self) -> float:
        n = _num(self._data_cfg().get("paper_shadow_notional_usdt"),
                 _DEFAULT_NOTIONAL_USDT) or _DEFAULT_NOTIONAL_USDT
        return n if n > 0 else _DEFAULT_NOTIONAL_USDT

    def _slippage_bps(self) -> float:
        s = _num(self._data_cfg().get("paper_shadow_slippage_bps"),
                 _DEFAULT_SLIPPAGE_BPS)
        return max(0.0, s if s is not None else _DEFAULT_SLIPPAGE_BPS)

    def _max_hold_sec(self) -> float:
        h = _num(self._data_cfg().get("paper_shadow_max_hold_sec"),
                 _DEFAULT_MAX_HOLD_SEC) or _DEFAULT_MAX_HOLD_SEC
        return h if h > 0 else _DEFAULT_MAX_HOLD_SEC

    def _max_open(self, universe_size: int) -> int:
        # Default cap = universe size (≤ one paper position per symbol anyway,
        # since a symbol already in a paper position is skipped). An explicit
        # override may lower it further.
        override = self._data_cfg().get("paper_shadow_max_open")
        base = universe_size if universe_size > 0 else _DEFAULT_MAX_OPEN_FALLBACK
        ov = _num(override, None)
        if ov is not None and ov > 0:
            return int(min(base, ov)) if universe_size > 0 else int(ov)
        return int(base)

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
        max_open = self._max_open(len(universe))

        # Snapshot the SAME signal cache the live buy loop reads (under its lock).
        try:
            with trade_engine._signal_cache_lock:
                cache_snapshot = dict(trade_engine._signal_cache)
        except Exception:
            cache_snapshot = {}

        # 1) Manage existing open paper positions (exits + stale eviction) first,
        #    freeing ledger room before evaluating new entries this cycle.
        self._manage_open(strategy, universe)

        # 2) Entry evaluation — STATELESS per symbol; only opens persist.
        try:
            btc_regime = trade_engine.get_btc_regime()
        except Exception:
            btc_regime = None

        for sym in approved:
            if self._stop_evt.is_set():
                return
            with self._lock:
                if sym in self._open:
                    continue  # already in a paper position — one per symbol
                if len(self._open) >= max_open:
                    break     # ledger cap reached — refuse new opens (bounded memory)
            cached = cache_snapshot.get(sym)
            if not isinstance(cached, dict):
                continue
            try:
                self._maybe_open(sym, cached, strategy, btc_regime)
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
                    btc_regime: Any) -> None:
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

        pos = self._open_paper(sym, price, strategy)
        if pos is None:
            return
        # EV features + score from the SAME raw signals (interpretability parity).
        try:
            pos["ev_features"] = ev_model.extract_features(sig_data)
        except Exception:
            pos["ev_features"] = {}
        try:
            pos["ev_score"] = ev_model.score(sig_data).get("probability")
        except Exception:
            pos["ev_score"] = None

        with self._lock:
            # Re-check the cap under lock (another entry may have filled it).
            if sym in self._open:
                return
            self._open[sym] = pos

    def _open_paper(self, sym: str, mid: float, strategy: dict) -> Optional[dict]:
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
        notional = self._notional()
        qty = notional / buy_fill
        buy_fee = qty * buy_fill * taker

        pos = {
            "symbol":     sym,
            "entry_price": buy_fill,
            "quantity":   qty,
            "buy_fee_usdt": buy_fee,
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
            open_syms = list(self._open.keys())
        now = time.time()
        max_hold = self._max_hold_sec()
        for sym in open_syms:
            if self._stop_evt.is_set():
                return
            with self._lock:
                pos = self._open.get(sym)
            if pos is None:
                continue
            try:
                price = _num(self._live_price(sym), None)
                if not price or price <= 0:
                    # No fresh price — expire only if stale past the hold cap.
                    if now - pos.get("opened_ts", now) > max_hold:
                        self._close_paper(sym, pos, pos.get("entry_price"),
                                          "stale-no-price", now)
                    continue
                reason = self._evaluate_paper_exit(pos, price, now)
                if reason is None and (now - pos.get("opened_ts", now) > max_hold):
                    reason = "stale-timeout"
                if reason is not None:
                    self._close_paper(sym, pos, price, reason, now)
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

    def _close_paper(self, sym: str, pos: dict, exit_mid: Optional[float],
                     reason: str, now: float) -> None:
        """Close a paper position at a MODELED sell fill, compute realized_r, emit
        ONE labeled training sample, and drop it from the bounded ledger."""
        import fees
        import database

        # Remove from the ledger FIRST so a save failure can't leak the position.
        with self._lock:
            self._open.pop(sym, None)

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
            database.save_training_sample("paper_shadow", sym, features,
                                          label, realized_r)
            log.debug("[PaperShadow] EXIT %s reason=%s R=%.3f pnl=%.4f",
                      sym, reason, realized_r, pnl)
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
            "open_positions": open_positions,
            "trades_today":   trades_today,
            "expectancy":     round(expectancy, 4),
            "win_rate":       round(win_rate, 4),
            "avg_win_r":      round(avg_win_r, 4),
            "avg_loss_r":     round(avg_loss_r, 4),
            "n_total":        n_total,
        }


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
