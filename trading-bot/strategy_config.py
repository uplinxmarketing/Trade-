"""WolfBot v0.4 Phase 5 — the v2 strategy-config model.

Single source of truth for the typed v2 layout of strategy.json:

  * ``StrategyConfig``   — pydantic v2 model (every block ``extra='forbid'``)
    with the addendum-A4 defaults and ranges.
  * ``SCHEMA``           — flat per-field UI metadata keyed by dotted path
    (type/default/min/max/step/unit/section/label/help/read_only) so the
    frontend can auto-render the settings form from GET /api/strategy/schema.
  * ``validate_patch``   — deep-merge a partial patch onto the current v2
    view and validate the WHOLE result; returns (merged, {dotted: msg}).
  * ``migrate_to_v2``    — additive, never-crashing v1→v2 migration: adds
    missing blocks with defaults, copies obviously-1:1 legacy root keys, sets
    schema_version=2. Never deletes root keys (engine legacy fallbacks keep
    working). Returns (v2_dict, warnings).
  * ``current_v2_view``  — raw strategy.json dict → resolved v2-shaped view
    (defaults + legacy-key mapping + stored blocks) for GET /api/strategy.

This module is intentionally standalone (no engine imports) so it can be
unit-tested and imported from control_api without circular imports.
"""

from __future__ import annotations

import copy
from typing import Any, Dict, Literal, Optional, Tuple

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

SCHEMA_VERSION = 2

# ── Block models (addendum A4 defaults; extra keys rejected everywhere) ───────


class FeesConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    maker_pct:            float = Field(0.10, ge=0.0, le=1.0)
    taker_pct:            float = Field(0.10, ge=0.0, le=1.0)
    bnb_discount:         bool  = False
    auto_topup_bnb:       bool  = False
    per_symbol_overrides: Dict[str, Any] = Field(default_factory=dict)


class SizingConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    bot_allocation_usdt: float = Field(100.0, ge=0.0, le=1_000_000.0)
    max_positions:       int   = Field(9, ge=1, le=50)
    min_position_usdt:   float = Field(10.0, ge=1.0, le=1000.0)
    mode:                Literal["fixed", "percent", "capped", "per_coin", "coin_pct"] = "capped"
    reinvest_profits:    bool  = True


class EntriesConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    min_score:              int   = Field(3, ge=1, le=10)
    maker_first:            bool  = True
    chase_seconds:          float = Field(3.0, ge=1.0, le=30.0)
    max_reposts:            int   = Field(3, ge=0, le=10)
    taker_fallback:         bool  = False
    cooldown_after_sl_min:  float = Field(15.0, ge=0.0, le=240.0)
    prefer_fee_promo_pairs: bool  = False
    falling_knife_atr_mult: float = Field(1.0, ge=0.1, le=5.0)
    eval_heartbeat_sec:     float = Field(15.0, ge=5.0, le=120.0)
    tick_entries:           bool  = False
    max_lot_waste_pct:      float = Field(5.0, ge=0.0, le=50.0)
    maker_abandon_max:      int   = Field(3, ge=1, le=20)
    bookticker_universe:    bool  = False
    # L1 — when the maker chase is exhausted, fill as TAKER if the live spread is
    # ≤ this %. 0 disables the fallback (abandon as before). Cheap insurance
    # against ready signals starving on tight books; wide books still abandon.
    taker_fallback_max_spread_pct: float = Field(0.05, ge=0.0, le=5.0)
    # S3-6 — E1 spread veto is over-conservative for maker-first entries: posting at
    # the bid, you never PAY the spread, yet a wide spread was hard-vetoing top coins
    # (STRK/ILV/PEPE vetoed ~75% of the time). When maker_first is on AND taker
    # fallback is off, E1 uses this relaxed threshold instead of signal_thresholds.
    # spread_max_pct, so wide-book coins are still tradeable via maker posts.
    spread_max_pct_maker:   float = Field(0.5, ge=0.0, le=5.0)
    # L2 Tier 1 — skip an entry when (half-spread + recent avg exit slippage)
    # exceeds this % of the planned 1R stop distance (friction eats the edge).
    max_friction_of_stop:   float = Field(15.0, ge=0.0, le=100.0)
    # L2 Tier 1 — liquidity floor: skip symbols whose 24h quote volume (USDT) is
    # below this. Thin coins are where spread vetoes and slippage cluster.
    min_quote_volume_24h_usd: float = Field(20_000_000.0, ge=0.0, le=10_000_000_000.0)
    # O5.3 — a candidate must hold buy-ready across live ticks for this many
    # seconds, then fire immediately (confirm-then-fire). 0 = pure live/tick;
    # up to a full 300s candle = max discipline. Filters intra-candle flickers
    # without the minutes-long heartbeat lag. Never a switch to raw tick-buying —
    # the fresh re-check still gates; this is only how long confirmation takes.
    confirm_seconds:        float = Field(10.0, ge=0.0, le=300.0)
    # O5.2 — reason-specific candidacy cooldowns (minutes), replacing the flat
    # 30-min bench. A near-miss fresh re-check must not sideline a coin that may
    # qualify on the next tick; thin-liquidity/spread are stable so keep ~5 min.
    cooldown_recheck_fail_min: float = Field(1.0, ge=0.0, le=60.0)
    cooldown_thin_min:      float = Field(5.0, ge=0.0, le=240.0)
    cooldown_spread_min:    float = Field(5.0, ge=0.0, le=240.0)
    # Q1 — when slots are free and candidates are fresh-confirmed buy-ready, fire
    # the highest-scoring one immediately instead of holding it for confirm_seconds
    # (which was stranding ready coins while slots sat open). Marginal/subsequent
    # candidates still serve the confirm window.
    instant_fire_when_slots_free: bool = True
    # Part S — EV buy-scoring: rank ready coins by modeled win-probability and buy
    # the highest first (only reorders/gates coins already past vetoes+gates; never
    # overrides a block). min_win_probability is the smarter replacement for the
    # blunt min_score — buy only if modeled P(win) ≥ this. 0 = floor off (display
    # only); the floor is ignored anyway until the model is trained (S4 guardrail).
    ev_ranking_enabled:     bool  = True
    min_win_probability:    float = Field(0.0, ge=0.0, le=1.0)
    # Part S-2 — WolfScore adaptive buy floor (0-100 scale). A candidate must clear
    # BOTH the absolute floor AND the distribution rule (p75 of live scores, or
    # mean+k·stdev). In a downtrend where nothing decouples the best score fails the
    # floor → hold cash; in recovery scores rise back over it → re-engage. Both are
    # ignored (display-only) until the model is trained on ≥ ev_min_clean_trades.
    min_win_probability_floor: float = Field(55.0, ge=0.0, le=100.0)
    # S3-1 — default is now 'absolute': the buy floor is the static abs_floor (55,
    # the cliff the paper data revealed) regardless of the live distribution, so a
    # high scorer fires even in a strong field and sub-55 junk is cut in a weak one.
    # 'p75'/'meanstd' can only RAISE the bar above 55; 'off' disables it.
    ev_floor_mode:          Literal["absolute", "p75", "p60", "p90", "meanstd", "off"] = "absolute"
    ev_floor_meanstd_k:     float = Field(0.5, ge=0.0, le=3.0)
    # S3-2 — up-regime anti-chasing hard veto. In a clear uptrend (regime tilt>0.15)
    # block coins that have run too far from VWAP (WolfScore W ≤ threshold) — the
    # pump-chase that reverses (paper data: uptrend win-rate 15%). Toggle off to
    # rely purely on the learned wu_W weight once a model is trained.
    up_extension_veto:      bool  = True
    up_extension_w_thr:     float = Field(0.0, ge=-1.0, le=1.0)


class ExitsConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    k_sl:                      float = Field(1.2, ge=0.0, le=10.0)
    sl_min_pct:                float = Field(0.5, ge=0.1, le=10.0)
    sl_max_pct:                float = Field(2.5, ge=0.1, le=10.0)
    hard_sl_pct:               float = Field(3.0, ge=0.5, le=20.0)
    rr_ratio:                  Optional[float] = Field(1.6, ge=0.5, le=10.0)
    tp_buffer_pct:             float = Field(0.05, ge=0.0, le=1.0)
    min_profit_usdt:           float = Field(0.01, ge=0.001, le=1.0)
    breakeven_at_r:            Optional[float] = Field(1.2, ge=0.1, le=5.0)
    k_trail:                   float = Field(0.8, ge=0.0, le=5.0)
    smart_hold_score_gate:     bool  = False
    maker_tp:                  bool  = True
    maker_tp_timeout_ms:       float = Field(1500.0, ge=100.0, le=30000.0)
    oco_enabled:               bool  = False
    oco_stop_limit_buffer_pct: float = Field(0.5, ge=0.0, le=5.0)
    oco_skip_rescue_sec:       float = Field(3.0, ge=1.0, le=30.0)
    sl_confirm_ticks:          int   = Field(2, ge=1, le=10)
    min_hold_sec:              float = Field(10.0, ge=0.0, le=120.0)
    # P2 — ATR-based profit-ratchet trailing stop: the missing layer between the
    # breakeven move and the +rr_ratio target. Arms once a trade is meaningfully
    # green, then locks profit before it round-trips. All four are UI-tunable and
    # backtester levers; defaults are starting points, not claims.
    ratchet_enabled:           bool  = True
    # S3-7 — operator-approved R-multiple tuning (parameters only; exit LOGIC frozen).
    # Paper data: avg_win was only +0.49R vs the +1.6R target — the ratchet armed at
    # 0.4R and trailed tight (k_atr 0.6), grabbing the first small green. Arm later
    # (0.8R) and trail wider (1.0×ATR) so winners breathe toward the TP. Below
    # activation the fixed protective stop still holds (round-trip protection intact).
    ratchet_activate_r:        float = Field(0.8, ge=0.0, le=5.0)   # arm at ≥ this R of profit
    ratchet_activate_usdt:     float = Field(0.02, ge=0.0, le=100.0)  # OR ≥ this $ profit
    ratchet_k_atr:             float = Field(1.0, ge=0.05, le=5.0)  # trail = peak − k×ATR (per-coin)
    ratchet_giveback_pct:      float = Field(50.0, ge=1.0, le=100.0)  # exit if profit gives back ≥ this % of peak

    @model_validator(mode="after")
    def _check_sl_bounds(self):
        if self.sl_min_pct > self.sl_max_pct:
            raise ValueError(
                f"sl_min_pct ({self.sl_min_pct}) must be <= sl_max_pct ({self.sl_max_pct})")
        return self


class RegimeConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    enabled:           bool  = True
    refresh_sec:       float = Field(60.0, ge=1.0, le=3600.0)   # read-only (engine cache TTL)
    neutral_size_mult: float = Field(0.5, ge=0.0, le=1.0)
    # risk_off requires BOTH price<EMA50 (1h) AND pct_4h < this threshold (F2).
    risk_off_pct_4h:   float = Field(-1.0, ge=-20.0, le=0.0)
    # M1.2 — how neutral regime reduces risk. "size" halves the ticket (legacy;
    # can push a small-account ticket below minNotional → the $5.50 bug); "slots"
    # halves the number of concurrent NEW entries at FULL ticket (keeps notional
    # tradeable); "off" disables neutral scaling; "auto" (default) resolves to
    # "slots" when allocation/max_positions < 2×tradeable_min, else "size".
    neutral_scaling_mode: Literal["auto", "size", "slots", "off"] = "auto"


class RiskConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    daily_loss_stop_pct:          float = Field(2.0, ge=0.1, le=50.0)
    flatten_on_stop:              bool  = False
    max_consecutive_losses:       int   = Field(4, ge=1, le=50)
    max_avg_slippage_bps:         float = Field(15.0, ge=1.0, le=500.0)
    max_new_entries_when_btc_red: int   = Field(2, ge=0, le=50)


class DataConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    kline_retention_days: float = Field(180.0, ge=7.0, le=730.0)
    legacy_rest_scan:     bool  = False
    eval_retention_days:  int   = Field(14, ge=1, le=365)
    # I4 — three-tier auto-management of the approved_coins watchlist.
    auto_remove_delisted:        bool = True   # remove confirmed-delisted coins
    auto_replace_with_successor: bool = False  # also auto-add the renamed successor
    # R4 — memory-safety guardrail. When process RSS crosses the soft cap, warn
    # loudly (CRITICAL + UI banner) and gracefully self-restart (flush + clean
    # shutdown marker + exit 0) so an OOM kill becomes a clean restart. 0 disables.
    rss_soft_cap_mb:             float = Field(800.0, ge=0.0, le=8192.0)
    # R1 — enable tracemalloc + log the top allocations in the heartbeat so a leak
    # source is visible in the diagnostics bundle. Small overhead; leave on until
    # the leak is fixed, then it can be turned off.
    tracemalloc_enabled:         bool = True
    # Part S3 — run the paper-shadow evaluator in parallel with live: same signals
    # + universe, modeled fills (no real orders), reusing the live exit machinery
    # read-only, writing labeled outcomes (mode=paper_shadow) to the training set.
    # NOT slot-limited → far more clean data/day than live for the EV model.
    paper_shadow_enabled:        bool = True
    # Shadow-Lab scale (data scraper): a VIRTUAL budget the shadow can deploy so it
    # holds many concurrent positions and re-enters a coin, generating far more
    # labeled outcomes/day. Effective concurrent cap = min(max_open,
    # floor(budget/position)). Fake money only — never a real order. Memory stays
    # bounded (open positions capped; outcomes persist to a capped DB table).
    paper_shadow_budget_usdt:    float = Field(10000.0, ge=0.0, le=10_000_000.0)
    paper_shadow_position_usdt:  float = Field(11.0, ge=1.0, le=100_000.0)
    # S3-4 — default lowered 300→100: the win-rate/bucket signal is already clean
    # at n>1600, and 100 concurrent gives the same statistical picture at ~1/3 the
    # per-cycle CPU/memory load. Raise it for a faster flywheel if the box has room.
    paper_shadow_max_open:       int   = Field(100, ge=1, le=5000)
    paper_shadow_max_per_symbol: int   = Field(20, ge=1, le=500)
    # Shadow-Lab evaluator cadence (seconds between paper cycles). Higher = less
    # CPU (the flywheel manages up to max_open positions each cycle). Operator
    # throttle to trade data-rate for headroom on a busy box. Floored at 3s in
    # paper_shadow._loop_sec regardless of this value.
    paper_shadow_loop_sec:       float = Field(8.0, ge=3.0, le=120.0)
    # Part S4.3 — minimum clean labeled trades before the EV win-probability model
    # is allowed to gate real buys (below this the floor is display-only/advisory).
    ev_min_clean_trades:         int  = Field(300, ge=20, le=100000)


class StrategyConfig(BaseModel):
    """Full v2 layout. ``mode`` and ``schema_version`` are informational /
    read-only (validate_patch drops attempts to change them)."""
    model_config = ConfigDict(extra="forbid")
    schema_version: int = SCHEMA_VERSION
    mode:           Literal["paper", "live"] = "paper"
    fees:           FeesConfig    = Field(default_factory=FeesConfig)
    sizing:         SizingConfig  = Field(default_factory=SizingConfig)
    entries:        EntriesConfig = Field(default_factory=EntriesConfig)
    exits:          ExitsConfig   = Field(default_factory=ExitsConfig)
    regime:         RegimeConfig  = Field(default_factory=RegimeConfig)
    risk:           RiskConfig    = Field(default_factory=RiskConfig)
    data:           DataConfig    = Field(default_factory=DataConfig)


V2_BLOCKS = ("fees", "sizing", "entries", "exits", "regime", "risk", "data")

# Fields the API never lets a PUT change (informational only).
READ_ONLY_PATHS = frozenset({"schema_version", "mode", "regime.refresh_sec"})


def defaults() -> dict:
    """Fresh dict of the full A4 default config."""
    return StrategyConfig().model_dump()


# ── Per-field UI metadata (GET /api/strategy/schema) ─────────────────────────

SECTIONS = ["Fees", "Sizing", "Entries", "Exits", "Regime", "Risk", "Data"]

_D = defaults()


def _meta(path: str, typ: str, section: str, label: str, help_: str,
          minimum=None, maximum=None, step=None, unit=None,
          read_only: bool = False, nullable: bool = False,
          choices=None) -> dict:
    blk, _, key = path.partition(".")
    default = _D.get(blk, {}).get(key) if key else _D.get(blk)
    m: dict = {
        "type": typ, "default": default, "min": minimum, "max": maximum,
        "step": step, "unit": unit, "section": section, "label": label,
        "help": help_, "read_only": read_only,
    }
    if nullable:
        m["nullable"] = True
    if choices:
        m["choices"] = list(choices)
    return m


SCHEMA: Dict[str, dict] = {
    # ── Root ──────────────────────────────────────────────────────────────
    "schema_version": {"type": "int", "default": SCHEMA_VERSION, "min": 2, "max": 2,
                       "step": 1, "unit": None, "section": "Data",
                       "label": "Schema version",
                       "help": "Config schema version (managed automatically).",
                       "read_only": True},
    "mode": {"type": "enum", "default": "paper", "min": None, "max": None,
             "step": None, "unit": None, "section": "Data", "label": "Mode",
             "help": "Trading mode (paper/live) — set via the MODE env var, informational here.",
             "read_only": True, "choices": ["paper", "live"]},

    # ── Fees ──────────────────────────────────────────────────────────────
    "fees.maker_pct": _meta("fees.maker_pct", "float", "Fees", "Maker fee %",
                            "Exchange maker fee per fill.", 0.0, 1.0, 0.005, "%"),
    "fees.taker_pct": _meta("fees.taker_pct", "float", "Fees", "Taker fee %",
                            "Exchange taker fee per fill.", 0.0, 1.0, 0.005, "%"),
    "fees.bnb_discount": _meta("fees.bnb_discount", "bool", "Fees", "BNB fee discount",
                               "Pay fees in BNB for the 25% discount (needs BNB balance)."),
    "fees.auto_topup_bnb": _meta("fees.auto_topup_bnb", "bool", "Fees", "Auto top-up BNB",
                                 "Automatically buy a small BNB amount when the fee balance runs low."),
    "fees.per_symbol_overrides": _meta("fees.per_symbol_overrides", "object", "Fees",
                                       "Per-symbol fee overrides",
                                       "Symbol → {maker_pct, taker_pct} overrides for fee-promo pairs."),

    # ── Sizing ────────────────────────────────────────────────────────────
    "sizing.bot_allocation_usdt": _meta("sizing.bot_allocation_usdt", "float", "Sizing",
                                        "Bot allocation (USDT)",
                                        "Total capital the bot may deploy.", 0.0, 1_000_000.0, 1, "USDT"),
    "sizing.max_positions": _meta("sizing.max_positions", "int", "Sizing", "Max positions",
                                  "Maximum simultaneous open positions.", 1, 50, 1),
    "sizing.min_position_usdt": _meta("sizing.min_position_usdt", "float", "Sizing",
                                      "Min position (USDT)",
                                      "Skip entries smaller than this size.", 1.0, 1000.0, 1, "USDT"),
    "sizing.mode": _meta("sizing.mode", "enum", "Sizing", "Sizing mode",
                         "How per-trade budget is computed.",
                         choices=["fixed", "percent", "capped", "per_coin", "coin_pct"]),
    "sizing.reinvest_profits": _meta("sizing.reinvest_profits", "bool", "Sizing",
                                     "Reinvest profits",
                                     "Grow position sizing as realized profits accumulate."),

    # ── Entries ───────────────────────────────────────────────────────────
    "entries.min_score": _meta("entries.min_score", "int", "Entries", "Min signal score",
                               "Minimum scored signals required to enter. "
                               "Canonical key is signal_engine.min_scored (what the "
                               "signal registry reads); this field is a user-facing "
                               "alias and writing either updates both.", 1, 10, 1),
    "entries.maker_first": _meta("entries.maker_first", "bool", "Entries", "Maker-first entries",
                                 "Try a maker (post-only) entry before crossing the spread."),
    "entries.chase_seconds": _meta("entries.chase_seconds", "float", "Entries", "Chase seconds",
                                   "How long to chase the bid before reposting.", 1.0, 30.0, 1, "s"),
    "entries.max_reposts": _meta("entries.max_reposts", "int", "Entries", "Max reposts",
                                 "Maximum maker order reposts before giving up.", 0, 10, 1),
    "entries.taker_fallback": _meta("entries.taker_fallback", "bool", "Entries", "Taker fallback",
                                    "Fall back to a taker (market) entry when the maker chase fails."),
    "entries.cooldown_after_sl_min": _meta("entries.cooldown_after_sl_min", "float", "Entries",
                                           "Cooldown after SL (min)",
                                           "Per-symbol re-entry cooldown after a stop-loss.",
                                           0.0, 240.0, 5, "min"),
    "entries.prefer_fee_promo_pairs": _meta("entries.prefer_fee_promo_pairs", "bool", "Entries",
                                            "Prefer fee-promo pairs",
                                            "Rank zero/low-fee promo pairs first when slots are scarce."),
    "entries.falling_knife_atr_mult": _meta("entries.falling_knife_atr_mult", "float", "Entries",
                                            "Falling-knife ATR mult",
                                            "Block entries when the last drop exceeds this many ATRs.",
                                            0.1, 5.0, 0.1, "×ATR"),
    "entries.eval_heartbeat_sec": _meta("entries.eval_heartbeat_sec", "float", "Entries",
                                        "Eval heartbeat (s)",
                                        "Cadence of the entry-evaluation loop.", 5.0, 120.0, 1, "s"),
    "entries.tick_entries": _meta("entries.tick_entries", "bool", "Entries", "Tick-driven entries",
                                  "Also evaluate entries on every price tick (higher CPU)."),
    "entries.max_lot_waste_pct": _meta("entries.max_lot_waste_pct", "float", "Entries",
                                       "Max lot waste %",
                                       "Veto/flag an entry when LOT_SIZE step rounding would waste "
                                       "more than this % of the intended notional.",
                                       0.0, 50.0, 0.5, "%"),
    "entries.maker_abandon_max": _meta("entries.maker_abandon_max", "int", "Entries",
                                       "Maker abandon max",
                                       "Abandon the maker chase after this many unfilled reposts.",
                                       1, 20, 1),
    "entries.bookticker_universe": _meta("entries.bookticker_universe", "bool", "Entries",
                                         "Book-ticker universe",
                                         "Use the bookTicker (best bid/ask) stream across the whole "
                                         "universe for spread/price checks."),
    "entries.taker_fallback_max_spread_pct": _meta("entries.taker_fallback_max_spread_pct", "float",
                                         "Entries", "Taker fallback max spread",
                                         "After the maker chase is exhausted, fill as taker if the live "
                                         "spread is ≤ this %. 0 disables (abandon instead).",
                                         0.0, 5.0, 0.01, "%"),
    "entries.spread_max_pct_maker": _meta("entries.spread_max_pct_maker", "float", "Entries",
                                         "E1 spread veto (maker-first)",
                                         "Relaxed spread ceiling for the E1 veto when maker_first is on and "
                                         "taker fallback is off — you post at the bid so you never pay the "
                                         "spread. Wide-book top coins (STRK/ILV/PEPE) stay tradeable.",
                                         0.0, 5.0, 0.05, "%"),
    "entries.max_friction_of_stop": _meta("entries.max_friction_of_stop", "float", "Entries",
                                         "Max friction of stop",
                                         "Skip entry when (half-spread + recent avg exit slippage) exceeds "
                                         "this % of the planned 1R stop distance.",
                                         0.0, 100.0, 1.0, "%"),
    "entries.min_quote_volume_24h_usd": _meta("entries.min_quote_volume_24h_usd", "float", "Entries",
                                         "Min 24h quote volume",
                                         "Liquidity floor: skip symbols whose 24h quote volume (USDT) is "
                                         "below this.",
                                         0.0, 10_000_000_000.0, 1_000_000.0, "USDT"),
    "entries.confirm_seconds": _meta("entries.confirm_seconds", "float", "Entries",
                                         "Confirm seconds",
                                         "A buy-ready candidate must hold across live ticks for this many "
                                         "seconds, then fires immediately. 0 = live/tick, 300 = full candle.",
                                         0.0, 300.0, 1.0, "s"),
    "entries.cooldown_recheck_fail_min": _meta("entries.cooldown_recheck_fail_min", "float", "Entries",
                                         "Cooldown: re-check miss",
                                         "Candidacy cooldown after a fresh re-check just missed (keep near "
                                         "zero — the coin may qualify on the next tick).",
                                         0.0, 60.0, 0.5, "min"),
    "entries.cooldown_thin_min": _meta("entries.cooldown_thin_min", "float", "Entries",
                                         "Cooldown: thin liquidity",
                                         "Candidacy cooldown after a thin-volume skip (the condition is "
                                         "stable, so a modest bench is fine).",
                                         0.0, 240.0, 1.0, "min"),
    "entries.cooldown_spread_min": _meta("entries.cooldown_spread_min", "float", "Entries",
                                         "Cooldown: wide spread",
                                         "Candidacy cooldown after a wide-spread skip.",
                                         0.0, 240.0, 1.0, "min"),
    "entries.instant_fire_when_slots_free": _meta("entries.instant_fire_when_slots_free", "bool", "Entries",
                                         "Instant fire when slots free",
                                         "When slots are open and candidates are confirmed buy-ready, fire "
                                         "the highest-scoring one immediately instead of holding it for "
                                         "confirm_seconds."),
    "entries.ev_ranking_enabled": _meta("entries.ev_ranking_enabled", "bool", "Entries",
                                         "EV ranking (best-first)",
                                         "Rank ready coins by modeled win-probability and buy the highest "
                                         "first. Only reorders coins already past vetoes/gates."),
    "entries.min_win_probability": _meta("entries.min_win_probability", "float", "Entries",
                                         "Min win probability",
                                         "Only buy if modeled P(win) ≥ this (0-1). Smarter replacement for "
                                         "min_score. Ignored until the model is trained. 0 = floor off.",
                                         0.0, 1.0, 0.01, ""),
    "entries.min_win_probability_floor": _meta("entries.min_win_probability_floor", "float", "Entries",
                                         "WolfScore absolute floor",
                                         "Absolute WolfScore (0-100) a coin must clear to be bought — the "
                                         "safety net so nothing terrible is bought. Ignored until trained.",
                                         0.0, 100.0, 1.0, ""),
    "entries.ev_floor_mode": _meta("entries.ev_floor_mode", "enum", "Entries",
                                         "WolfScore floor distribution rule",
                                         "absolute = the static floor only (55, the paper-data cliff) — a high "
                                         "scorer fires even in a strong field. p75/meanstd can only RAISE the bar "
                                         "above 55. off disables the floor.",
                                         choices=["absolute", "p75", "p60", "p90", "meanstd", "off"]),
    "entries.ev_floor_meanstd_k": _meta("entries.ev_floor_meanstd_k", "float", "Entries",
                                         "WolfScore floor mean+k·stdev",
                                         "k for the mean+k·stdev distribution rule (when ev_floor_mode=meanstd).",
                                         0.0, 3.0, 0.1, ""),
    "entries.up_extension_veto": _meta("entries.up_extension_veto", "bool", "Entries",
                                         "Up-regime anti-chasing veto",
                                         "In a clear uptrend, hard-block coins that have run too far from VWAP "
                                         "(WolfScore W ≤ threshold) — the pump-chase that reverses (paper: uptrend "
                                         "win-rate 15%). Turn off to rely only on the learned wu_W weight."),
    "entries.up_extension_w_thr": _meta("entries.up_extension_w_thr", "float", "Entries",
                                         "Up-regime extension W threshold",
                                         "WolfScore W at/below which an up-regime coin is vetoed as extended. "
                                         "0 = at/above VWAP required; more negative = allow more extension.",
                                         -1.0, 1.0, 0.05, ""),

    # ── Exits ─────────────────────────────────────────────────────────────
    "exits.k_sl": _meta("exits.k_sl", "float", "Exits", "SL ATR multiple (k_sl)",
                        "Stop-loss distance in ATR multiples.", 0.0, 10.0, 0.1, "×ATR"),
    "exits.sl_min_pct": _meta("exits.sl_min_pct", "float", "Exits", "SL min %",
                              "Lower clamp on the ATR stop distance.", 0.1, 10.0, 0.05, "%"),
    "exits.sl_max_pct": _meta("exits.sl_max_pct", "float", "Exits", "SL max %",
                              "Upper clamp on the ATR stop distance (must be ≥ SL min %).",
                              0.1, 10.0, 0.05, "%"),
    "exits.hard_sl_pct": _meta("exits.hard_sl_pct", "float", "Exits", "Hard SL %",
                               "Absolute disaster stop, independent of ATR.", 0.5, 20.0, 0.1, "%"),
    "exits.rr_ratio": _meta("exits.rr_ratio", "float", "Exits", "Reward:risk ratio",
                            "Take-profit distance as a multiple of the stop distance (null = off).",
                            0.5, 10.0, 0.1, "R", nullable=True),
    "exits.tp_buffer_pct": _meta("exits.tp_buffer_pct", "float", "Exits", "TP buffer %",
                                 "Extra buffer added above breakeven for the TP.", 0.0, 1.0, 0.05, "%"),
    "exits.min_profit_usdt": _meta("exits.min_profit_usdt", "float", "Exits", "Min profit (USDT)",
                                   "Minimum net profit a TP must clear after fees.",
                                   0.001, 1.0, 0.005, "USDT"),
    "exits.breakeven_at_r": _meta("exits.breakeven_at_r", "float", "Exits", "Breakeven at R",
                                  "Move the stop to breakeven once this many R is reached (null = off).",
                                  0.1, 5.0, 0.1, "R", nullable=True),
    "exits.k_trail": _meta("exits.k_trail", "float", "Exits", "Trail ATR multiple (k_trail)",
                           "Trailing-stop distance in ATR multiples (0 = off).", 0.0, 5.0, 0.1, "×ATR"),
    "exits.smart_hold_score_gate": _meta("exits.smart_hold_score_gate", "bool", "Exits",
                                         "Smart-hold score gate",
                                         "Only smart-hold past TP while the entry score still passes."),
    "exits.maker_tp": _meta("exits.maker_tp", "bool", "Exits", "Maker TP",
                            "Place the take-profit as a maker limit order."),
    "exits.maker_tp_timeout_ms": _meta("exits.maker_tp_timeout_ms", "float", "Exits",
                                       "Maker TP timeout (ms)",
                                       "Cancel/replace the maker TP if unfilled after this long.",
                                       100.0, 30000.0, 100, "ms"),
    "exits.oco_enabled": _meta("exits.oco_enabled", "bool", "Exits", "OCO exits",
                               "Use exchange-side OCO (TP + stop) orders in live mode."),
    "exits.oco_stop_limit_buffer_pct": _meta("exits.oco_stop_limit_buffer_pct", "float", "Exits",
                                             "OCO stop-limit buffer %",
                                             "Distance between OCO stop trigger and its limit price.",
                                             0.0, 5.0, 0.05, "%"),
    "exits.oco_skip_rescue_sec": _meta("exits.oco_skip_rescue_sec", "float", "Exits",
                                       "OCO rescue delay (s)",
                                       "Grace period before the software rescue path takes over an OCO.",
                                       1.0, 30.0, 1, "s"),
    "exits.sl_confirm_ticks": _meta("exits.sl_confirm_ticks", "int", "Exits", "SL confirm ticks",
                                    "Consecutive ticks below the stop required to trigger it.", 1, 10, 1),
    "exits.min_hold_sec": _meta("exits.min_hold_sec", "float", "Exits", "Min hold (s)",
                                "Never exit (except hard SL) within this many seconds of entry.",
                                0.0, 120.0, 1, "s"),
    "exits.ratchet_enabled": _meta("exits.ratchet_enabled", "bool", "Exits", "Profit ratchet",
                                "Lock in profit before it round-trips: trail an ATR-based stop once a "
                                "trade is meaningfully green (sits between breakeven and the target)."),
    "exits.ratchet_activate_r": _meta("exits.ratchet_activate_r", "float", "Exits",
                                "Ratchet activate (R)",
                                "Arm the profit ratchet once unrealized profit reaches this many R. Higher "
                                "= let winners run further before trailing (S3-7: raised 0.4→0.8 so avg_win "
                                "moves toward the +1.6R target instead of exiting at +0.49R).",
                                0.0, 5.0, 0.1, "R"),
    "exits.ratchet_activate_usdt": _meta("exits.ratchet_activate_usdt", "float", "Exits",
                                "Ratchet activate ($)",
                                "OR arm the ratchet at this many USDT of unrealized profit (whichever first).",
                                0.0, 100.0, 0.01, "USDT"),
    "exits.ratchet_k_atr": _meta("exits.ratchet_k_atr", "float", "Exits", "Ratchet trail (×ATR)",
                                "Trailing distance = peak − this × ATR (per-coin, so majors trail tight "
                                "and volatile alts trail wide automatically). Wider = winners breathe "
                                "through pullbacks (S3-7: raised 0.6→1.0).",
                                0.05, 5.0, 0.05, "×ATR"),
    "exits.ratchet_giveback_pct": _meta("exits.ratchet_giveback_pct", "float", "Exits",
                                "Ratchet give-back cap (%)",
                                "Also exit if unrealized profit falls to this % of its peak (whichever "
                                "fires first with the ATR trail).",
                                1.0, 100.0, 1.0, "%"),

    # ── Regime ────────────────────────────────────────────────────────────
    "regime.enabled": _meta("regime.enabled", "bool", "Regime", "Regime filter",
                            "Gate/scale entries by the BTC market regime."),
    "regime.refresh_sec": _meta("regime.refresh_sec", "float", "Regime", "Refresh (s)",
                                "Regime cache refresh cadence (fixed engine TTL).",
                                1.0, 3600.0, 1, "s", read_only=True),
    "regime.neutral_size_mult": _meta("regime.neutral_size_mult", "float", "Regime",
                                      "Neutral size multiplier",
                                      "Position-size multiplier while the regime is neutral.",
                                      0.0, 1.0, 0.05, "×"),
    "regime.risk_off_pct_4h": _meta("regime.risk_off_pct_4h", "float", "Regime",
                                    "Risk-off 4h threshold",
                                    "risk_off requires BOTH price<EMA50(1h) AND BTC 4h move below this %.",
                                    -20.0, 0.0, 0.1, "%"),
    "regime.neutral_scaling_mode": _meta("regime.neutral_scaling_mode", "enum", "Regime",
                                    "Neutral scaling mode",
                                    "How neutral regime reduces risk: size (halve ticket), slots "
                                    "(halve concurrent new entries at full ticket), off, or auto "
                                    "(slots for small accounts, else size).",
                                    choices=["auto", "size", "slots", "off"]),

    # ── Risk ──────────────────────────────────────────────────────────────
    "risk.daily_loss_stop_pct": _meta("risk.daily_loss_stop_pct", "float", "Risk",
                                      "Daily loss stop %",
                                      "Halt new entries after losing this % of allocation in a day.",
                                      0.1, 50.0, 0.1, "%"),
    "risk.flatten_on_stop": _meta("risk.flatten_on_stop", "bool", "Risk", "Flatten on stop",
                                  "Also close open positions when the daily stop trips."),
    "risk.max_consecutive_losses": _meta("risk.max_consecutive_losses", "int", "Risk",
                                         "Max consecutive losses",
                                         "Pause entries after this many losses in a row.", 1, 50, 1),
    "risk.max_avg_slippage_bps": _meta("risk.max_avg_slippage_bps", "float", "Risk",
                                       "Max avg slippage (bps)",
                                       "Veto a symbol when its average slippage exceeds this.",
                                       1.0, 500.0, 1, "bps"),
    "risk.max_new_entries_when_btc_red": _meta("risk.max_new_entries_when_btc_red", "int", "Risk",
                                               "Max entries when BTC red",
                                               "Cap concurrent NEW entries while BTC is falling.",
                                               0, 50, 1),

    # ── Data ──────────────────────────────────────────────────────────────
    "data.kline_retention_days": _meta("data.kline_retention_days", "float", "Data",
                                       "Kline retention (days)",
                                       "How long raw klines are kept before pruning.",
                                       7.0, 730.0, 1, "days"),
    "data.legacy_rest_scan": _meta("data.legacy_rest_scan", "bool", "Data", "Legacy REST scan",
                                   "Keep the old REST polling scanner running alongside websockets."),
    "data.eval_retention_days": _meta("data.eval_retention_days", "int", "Data",
                                      "Eval retention (days)",
                                      "How long buy-rejection and entry-snapshot rows are kept "
                                      "before the daily maintenance loop prunes them.",
                                      1, 365, 1, "days"),
    "data.auto_remove_delisted": _meta("data.auto_remove_delisted", "bool", "Data",
                                       "Auto-remove delisted coins",
                                       "Automatically drop an approved coin from the watchlist once "
                                       "it is confirmed delisted (absent from exchangeInfo / known "
                                       "delisted) across two consecutive validation passes. A held "
                                       "coin is never removed while it has an open position or order."),
    "data.rss_soft_cap_mb": _meta("data.rss_soft_cap_mb", "float", "Data",
                                  "RSS soft cap (MB)",
                                  "When process memory crosses this, warn (CRITICAL + UI banner) and "
                                  "gracefully self-restart so an OOM kill becomes a clean restart. 0 disables.",
                                  0.0, 8192.0, 50.0, "MB"),
    "data.tracemalloc_enabled": _meta("data.tracemalloc_enabled", "bool", "Data",
                                  "Tracemalloc leak trace",
                                  "Enable tracemalloc and log the top memory allocations in the heartbeat "
                                  "so a leak source shows up in diagnostics."),
    "data.paper_shadow_enabled": _meta("data.paper_shadow_enabled", "bool", "Data",
                                  "Paper-shadow data engine",
                                  "Run the risk-free paper-shadow evaluator alongside live to generate "
                                  "labeled training data for the EV model (no real orders)."),
    "data.paper_shadow_budget_usdt": _meta("data.paper_shadow_budget_usdt", "float", "Data",
                                  "Shadow-Lab virtual budget",
                                  "Total fake budget the paper-shadow can deploy across concurrent shadow "
                                  "positions. Never a real order. Effective cap = min(max_open, budget/size).",
                                  0.0, 10_000_000.0, 100.0, "USDT"),
    "data.paper_shadow_position_usdt": _meta("data.paper_shadow_position_usdt", "float", "Data",
                                  "Shadow-Lab position size",
                                  "Fake notional per shadow position (matches the live $11 ticket by default).",
                                  1.0, 100_000.0, 1.0, "USDT"),
    "data.paper_shadow_max_open": _meta("data.paper_shadow_max_open", "int", "Data",
                                  "Shadow-Lab max open positions",
                                  "Hard cap on concurrent shadow positions (memory bound). 300-1000 gives a "
                                  "fast data flywheel; higher uses more RAM/CPU.",
                                  1, 5000, 50, ""),
    "data.paper_shadow_max_per_symbol": _meta("data.paper_shadow_max_per_symbol", "int", "Data",
                                  "Shadow-Lab max per symbol",
                                  "How many concurrent shadow positions one coin may hold (allows re-entry "
                                  "→ more outcomes per coin).",
                                  1, 500, 1, ""),
    "data.paper_shadow_loop_sec": _meta("data.paper_shadow_loop_sec", "float", "Data",
                                  "Shadow-Lab cycle cadence",
                                  "Seconds between paper-shadow cycles. Higher = less CPU (each cycle "
                                  "manages every open shadow position). Raise this for headroom on a "
                                  "busy box; lower it for a faster data flywheel. Floored at 3s.",
                                  3.0, 120.0, 1.0, "sec"),
    "data.ev_min_clean_trades": _meta("data.ev_min_clean_trades", "int", "Data",
                                  "EV model: min clean trades",
                                  "Minimum labeled trades before the win-probability model may gate real "
                                  "buys. Below this the floor is display-only.",
                                  20, 100000, 10, ""),
    "data.auto_replace_with_successor": _meta("data.auto_replace_with_successor", "bool", "Data",
                                              "Auto-add rename successor",
                                              "When a removed coin has a known rename successor that "
                                              "is TRADING (e.g. MATICUSDT→POLUSDT), also add the "
                                              "successor to the watchlist automatically. Off by "
                                              "default — the successor is only suggested."),
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _deep_merge(base: dict, patch: dict) -> dict:
    """Recursive dict merge — patch wins; nested dicts merge, scalars replace."""
    out = copy.deepcopy(base)
    for k, v in (patch or {}).items():
        if isinstance(v, dict) and isinstance(out.get(k), dict):
            out[k] = _deep_merge(out[k], v)
        else:
            out[k] = copy.deepcopy(v)
    return out


def _strip_read_only(patch: dict) -> dict:
    """Drop read-only paths from a patch so GET→edit→PUT round-trips never
    error on informational fields."""
    out = copy.deepcopy(patch or {})
    for path in READ_ONLY_PATHS:
        blk, _, key = path.partition(".")
        if not key:
            out.pop(blk, None)
        elif isinstance(out.get(blk), dict):
            out[blk].pop(key, None)
    return out


def _errors_by_path(exc: ValidationError) -> Dict[str, str]:
    errors: Dict[str, str] = {}
    for e in exc.errors():
        loc = ".".join(str(p) for p in e.get("loc", ()) if p != "__root__")
        errors[loc or "config"] = e.get("msg", "invalid value")
    return errors


# Legacy root key → (block, field). Copied only when the target block is being
# created by migration; the resolved view applies them as fallbacks too.
_LEGACY_MAP = {
    "max_positions":       ("sizing",  "max_positions"),
    "bot_allocation_usdt": ("sizing",  "bot_allocation_usdt"),
    "budget_mode":         ("sizing",  "mode"),
    "min_signals":         ("entries", "min_score"),
    "reinvest_profits":    ("sizing",  "reinvest_profits"),
    "kline_retention_days": ("data",   "kline_retention_days"),
    "legacy_rest_scan":    ("data",    "legacy_rest_scan"),
}


def _validate_single(block: str, field: str, value) -> bool:
    """True when `value` is acceptable for block.field per the v2 model."""
    try:
        StrategyConfig.model_validate({block: {field: value}})
        return True
    except Exception:
        return False


# ── Public API ────────────────────────────────────────────────────────────────

def current_v2_view(raw: dict, mode: Optional[str] = None) -> dict:
    """Raw strategy.json dict → resolved v2-shaped view: model defaults,
    overlaid with legacy root-key fallbacks, overlaid with any stored v2
    blocks (known keys only; unknown/garbage keys are dropped from the view
    but never touched in the file). Never raises."""
    view = defaults()
    if not isinstance(raw, dict):
        raw = {}
    # Legacy root keys as fallbacks (only when valid for the target field).
    for legacy_key, (blk, field) in _LEGACY_MAP.items():
        if legacy_key in raw and _validate_single(blk, field, raw[legacy_key]):
            view[blk][field] = raw[legacy_key]
    # Stored v2 blocks win (known keys only, values passed through as-is —
    # the resolved view mirrors what readers see, valid or not).
    for blk in V2_BLOCKS:
        stored = raw.get(blk)
        if isinstance(stored, dict):
            known = set(view[blk].keys())
            for k, v in stored.items():
                if k in known:
                    view[blk][k] = copy.deepcopy(v)
    view["schema_version"] = SCHEMA_VERSION
    if mode in ("paper", "live"):
        view["mode"] = mode
    elif raw.get("mode") in ("paper", "live"):
        view["mode"] = raw["mode"]
    return view


def validate_patch(current: dict, patch: dict) -> Tuple[dict, Dict[str, str]]:
    """Deep-merge `patch` (partial v2 dict) onto the current v2 view of the
    raw strategy dict `current`, validate the whole result via the model, and
    return (merged_v2_dict, errors). errors maps dotted field paths to
    messages; when non-empty the caller must write NOTHING. Read-only paths
    (mode, schema_version, regime.refresh_sec) are silently dropped from the
    patch. Never raises."""
    try:
        if not isinstance(patch, dict):
            return {}, {"config": "must be an object"}
        clean = _strip_read_only(patch)
        base = current_v2_view(current if isinstance(current, dict) else {})
        merged = _deep_merge(base, clean)
        try:
            model = StrategyConfig.model_validate(merged)
        except ValidationError as exc:
            return {}, _errors_by_path(exc)
        return model.model_dump(), {}
    except Exception as exc:  # absolute backstop — endpoint must not 500
        return {}, {"config": f"validation failed: {exc}"}


def migrate_to_v2(raw: dict) -> Tuple[dict, list]:
    """Additive v1→v2 migration. Returns (v2_dict, warnings).

    * Only ADDS blocks that are absent (defaults + obviously-1:1 legacy root
      keys). Blocks already present are left byte-for-byte untouched.
    * NEVER deletes root keys — engine legacy fallbacks keep working.
    * NEVER raises: garbage input → defaults + warnings.
    * Legacy stop_loss_pct / take_profit_pct are engine-mapped, NOT translated
      into ATR exit fields.
    Idempotent: a second run returns the input unchanged.
    """
    warnings: list = []
    try:
        if not isinstance(raw, dict):
            warnings.append(f"strategy root is {type(raw).__name__}, not an object — "
                            "starting from defaults")
            raw = {}
        out = copy.deepcopy(raw)
        d = defaults()
        for blk in V2_BLOCKS:
            existing = out.get(blk)
            if isinstance(existing, dict):
                continue  # user/engine block — never touched
            if existing is not None:
                warnings.append(f"'{blk}' existed but was not an object "
                                f"({type(existing).__name__}) — replaced with defaults")
            block = copy.deepcopy(d[blk])
            # Copy 1:1 legacy root keys into the freshly created block only.
            for legacy_key, (target_blk, field) in _LEGACY_MAP.items():
                if target_blk != blk or legacy_key not in out:
                    continue
                val = out.get(legacy_key)
                if _validate_single(blk, field, val):
                    block[field] = val
                else:
                    warnings.append(
                        f"legacy '{legacy_key}'={val!r} is invalid for "
                        f"{blk}.{field} — kept at root, block uses default")
            out[blk] = block
            warnings.append(f"added missing '{blk}' block")
        # F3 — explicit roles completeness. Ensure signal_engine.roles carries
        # an EXPLICIT role for EVERY registered signal (fill missing ones from
        # the registry's default role, including 'off'). This makes
        # active-but-implicit signals (P2_bb_upper_touch_5m / REGIME_risk_off)
        # and the default-off signals appear in the persisted dict. Idempotent:
        # a re-run adds nothing once every signal has a role.
        try:
            import signal_registry as _sr
            _se_present = isinstance(out.get("signal_engine"), dict)
            se = dict(out["signal_engine"]) if _se_present else dict(_sr.DEFAULT_SIGNAL_ENGINE)
            roles = dict(se["roles"]) if isinstance(se.get("roles"), dict) else {}
            added_roles = [sid for sid in _sr.SIGNAL_REGISTRY if sid not in roles]
            for sid in added_roles:
                roles[sid] = _sr._default_role_for(sid)
            if added_roles or not _se_present or se.get("roles") != roles:
                se["roles"] = roles
                out["signal_engine"] = se
                if not _se_present:
                    warnings.append("added 'signal_engine' block (defaults + explicit roles)")
                if added_roles:
                    warnings.append(
                        f"filled {len(added_roles)} explicit signal role(s): "
                        + ", ".join(sorted(added_roles)))
        except Exception as _rexc:
            warnings.append(f"roles completeness skipped: {_rexc}")
        if out.get("schema_version") != SCHEMA_VERSION:
            out["schema_version"] = SCHEMA_VERSION
            warnings.append(f"set schema_version={SCHEMA_VERSION}")
        return out, warnings
    except Exception as exc:
        # Absolute backstop: never break startup.
        try:
            base = dict(raw) if isinstance(raw, dict) else {}
        except Exception:
            base = {}
        safe = {**defaults(), **base}
        safe["schema_version"] = SCHEMA_VERSION
        return safe, warnings + [f"migration error — fell back to defaults: {exc}"]


def diff_views(old: dict, new: dict, prefix: str = "") -> Dict[str, list]:
    """Dotted-path diff of two nested dicts: {path: [old_value, new_value]}."""
    out: Dict[str, list] = {}
    keys = set(old or {}) | set(new or {})
    for k in sorted(keys, key=str):
        path = f"{prefix}.{k}" if prefix else str(k)
        ov, nv = (old or {}).get(k), (new or {}).get(k)
        if isinstance(ov, dict) and isinstance(nv, dict):
            out.update(diff_views(ov, nv, path))
        elif ov != nv:
            out[path] = [ov, nv]
    return out
