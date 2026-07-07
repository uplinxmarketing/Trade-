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
