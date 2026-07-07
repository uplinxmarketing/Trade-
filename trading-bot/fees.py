"""
fees.py — SINGLE SOURCE OF TRUTH for trading fees (WolfBot v0.4 Phase 1, spec §1.1).

Every piece of fee math in the bot MUST come from this module:

  * Break-even-price (BEP) math      → FeeModel.round_trip(...) (a FRACTION, e.g.
    BEP for a maker-entry/taker-exit long: entry_price * (1 + round_trip) — or the
    exact form entry / ((1 - taker_frac) * (1 - maker_frac)) built from .maker()
    and .taker()).
  * Budget / min-profit gate checks  → FeeModel.taker()/.maker() fractions.
  * PaperClient fill simulation      → get_fee_model(symbol).taker() etc.
  * The backtester                   → for_symbol(symbol, strategy=...) so a
    historical strategy dict can be replayed with its own fee config.

NO OTHER MODULE may hardcode 0.001 / 0.1% / FEE_RATE. Legacy call sites should
migrate to `get_fee_model(...)`; during the migration they may import
`DEFAULT_TAKER_FRACTION` from here instead of keeping their own literal.

Units convention (read carefully — this is the #1 source of fee bugs):
  * `maker_pct` / `taker_pct` are PERCENT values: 0.10 means 0.10%.
  * `.maker()` / `.taker()` / `.round_trip()` return FRACTIONS: 0.10% → 0.001.
  * `bnb_discount=True` multiplies both fractions by 0.75 (Binance's 25% BNB
    fee discount).

Configuration (strategy.json, all keys optional — missing/garbage → defaults):

    {
      "fees": {
        "maker_pct": 0.10,
        "taker_pct": 0.10,
        "bnb_discount": false,
        "per_symbol_overrides": {
          "BTCFDUSD": {"maker_pct": 0.0}          # merges onto the base model
        }
      }
    }

This module deliberately does NOT import trade_engine (or any other bot
module besides `config`) — it keeps its own tiny mtime-cached strategy.json
reader so it can be imported from anywhere (PaperClient, backtester, CLI
tools) without circular imports.
"""

import json
import math
import os
import threading
import time
from dataclasses import dataclass
from typing import Optional

# ── Defaults ──────────────────────────────────────────────────────────────────
DEFAULT_MAKER_PCT = 0.10          # percent — standard Binance spot maker fee
DEFAULT_TAKER_PCT = 0.10          # percent — standard Binance spot taker fee
BNB_DISCOUNT_MULT = 0.75          # 25% discount when paying fees in BNB
MAX_FEE_PCT = 1.0                 # sanity ceiling: no sane spot fee exceeds 1%

# Legacy escape hatch for call sites still migrating off hardcoded 0.001.
# Equals the default taker fee as a FRACTION (0.10% → 0.001).
DEFAULT_TAKER_FRACTION = 0.001


def _clamp_pct(value, default: float) -> float:
    """Coerce a config value to a sane fee percent in [0, MAX_FEE_PCT].

    Non-numeric / NaN / infinite garbage → `default`. Numeric values outside
    the sane range are clamped (so an accidental 5 becomes 1.0%, never 5%,
    and a negative becomes 0)."""
    try:
        v = float(value)
    except (TypeError, ValueError):
        return default
    if not math.isfinite(v):
        return default
    return min(max(v, 0.0), MAX_FEE_PCT)


@dataclass
class FeeModel:
    """Fee rates for one (symbol, strategy) context.

    Fields are PERCENT values (0.10 == 0.10%); the methods return FRACTIONS
    ready for arithmetic (0.10% → 0.001), with the BNB discount applied."""
    maker_pct: float = DEFAULT_MAKER_PCT   # percent, i.e. 0.10 == 0.10%
    taker_pct: float = DEFAULT_TAKER_PCT   # percent
    bnb_discount: bool = False             # True → multiply both by 0.75

    def _fraction(self, pct: float, default_pct: float) -> float:
        frac = _clamp_pct(pct, default_pct) / 100.0
        if self.bnb_discount:
            frac *= BNB_DISCOUNT_MULT
        return frac

    def maker(self) -> float:
        """Maker fee as a FRACTION (0.10% → 0.001), BNB discount applied."""
        return self._fraction(self.maker_pct, DEFAULT_MAKER_PCT)

    def taker(self) -> float:
        """Taker fee as a FRACTION (0.10% → 0.001), BNB discount applied."""
        return self._fraction(self.taker_pct, DEFAULT_TAKER_PCT)

    def round_trip(self, entry_is_maker: bool, exit_is_maker: bool) -> float:
        """Combined entry+exit fee as a FRACTION.

        e.g. defaults, taker/taker → 0.002. Use for BEP/min-profit math:
        a long is at break-even (to first order) once price has moved
        `entry_price * round_trip(...)` in its favour."""
        entry = self.maker() if entry_is_maker else self.taker()
        exit_ = self.maker() if exit_is_maker else self.taker()
        return entry + exit_


# ── strategy.json reader (own copy — must NOT import trade_engine) ────────────

def _strategy_file() -> Optional[str]:
    try:
        import config  # sibling module; only touches os/env — no import cycles
        return getattr(config, "STRATEGY_FILE", None)
    except Exception:
        return None


_MTIME_CHECK_INTERVAL = 1.0   # seconds between os.stat() checks
_cache_lock = threading.Lock()
_cache = {"checked_at": 0.0, "mtime": None, "data": {}}


def _load_strategy() -> dict:
    """mtime-cached strategy.json read. Missing/unreadable/garbage file → {}.

    The stat() is rechecked at most once per second; the JSON is re-parsed
    only when the mtime actually changed."""
    path = _strategy_file()
    if not path:
        return {}
    now = time.time()
    with _cache_lock:
        if (now - _cache["checked_at"]) < _MTIME_CHECK_INTERVAL:
            return _cache["data"]
        _cache["checked_at"] = now
        try:
            mtime = os.path.getmtime(path)
        except OSError:                      # missing file → defaults
            _cache["mtime"] = None
            _cache["data"] = {}
            return {}
        if mtime == _cache["mtime"]:
            return _cache["data"]
        try:
            with open(path, "r") as f:
                data = json.load(f)
            if not isinstance(data, dict):
                data = {}
        except Exception:                    # unreadable / invalid JSON → defaults
            data = {}
        _cache["mtime"] = mtime
        _cache["data"] = data
        return data


# ── Public constructors ───────────────────────────────────────────────────────

def for_symbol(symbol: Optional[str] = None, strategy: Optional[dict] = None) -> FeeModel:
    """Build the FeeModel for `symbol` from strategy config.

    `strategy` — pass an explicit strategy dict (e.g. the backtester replaying
    a historical config); None → read strategy.json (mtime-cached, 1s).

    Reads `fees.maker_pct`, `fees.taker_pct`, `fees.bnb_discount` and merges
    `fees.per_symbol_overrides[<SYMBOL>]` on top (symbol match is
    case-insensitive; an override may set any subset of the three keys —
    e.g. {"BTCFDUSD": {"maker_pct": 0.0}} for a zero-maker-fee pair).
    All values are sanity-clamped to [0, 1.0]% — garbage falls back to
    defaults (0.10% / 0.10% / no discount)."""
    if strategy is None:
        strategy = _load_strategy()
    fees = strategy.get("fees") if isinstance(strategy, dict) else None
    if not isinstance(fees, dict):
        fees = {}

    maker = _clamp_pct(fees.get("maker_pct", DEFAULT_MAKER_PCT), DEFAULT_MAKER_PCT)
    taker = _clamp_pct(fees.get("taker_pct", DEFAULT_TAKER_PCT), DEFAULT_TAKER_PCT)
    bnb = bool(fees.get("bnb_discount", False))

    if symbol:
        overrides = fees.get("per_symbol_overrides")
        if isinstance(overrides, dict):
            sym_u = str(symbol).strip().upper()
            override = None
            for k, v in overrides.items():
                if str(k).strip().upper() == sym_u:
                    override = v
                    break
            if isinstance(override, dict):
                if "maker_pct" in override:
                    maker = _clamp_pct(override.get("maker_pct"), maker)
                if "taker_pct" in override:
                    taker = _clamp_pct(override.get("taker_pct"), taker)
                if "bnb_discount" in override:
                    bnb = bool(override.get("bnb_discount"))

    return FeeModel(maker_pct=maker, taker_pct=taker, bnb_discount=bnb)


def get_fee_model(symbol: Optional[str] = None) -> FeeModel:
    """Module-level convenience: FeeModel from the live strategy.json.

    `get_fee_model()` → the base (symbol-independent) model;
    `get_fee_model("BTCFDUSD")` → base + per-symbol override."""
    return for_symbol(symbol)
