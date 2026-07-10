"""WolfBot v0.5 Part S — EV buy-scoring engine (win-probability per coin).

SCOPE: this module ONLY scores/ranks BUY candidates. It does not touch exits,
stops, sizing, or any safety layer. It converts a coin's raw signal picture into
a calibrated 0-100% win probability via logistic regression, decomposes every
score into per-signal contributions (interpretability is mandatory), and lets a
retraining job swap weight-versions without changing the framework.

Design notes:
- Dependency-light: pure-python logistic regression (no sklearn/numpy needed) so
  it can't add native-memory pressure or a heavy import.
- Ships UNTRAINED with transparent, hand-set interim weights — every score is
  honestly labelled trained/untrained and is reproducible from (inputs, version).
- Active model + versions persist in the DB settings KV (small JSON), hot-loaded
  with a short TTL so a UI activation takes effect without a restart.
- Features are all OPTIONAL: a missing signal contributes 0 (its standardized
  value is 0), so partial snapshots score gracefully.
"""
from __future__ import annotations

import json
import math
import time
from typing import Any, Dict, List, Optional, Tuple

try:
    import database
except Exception:  # pragma: no cover - database always present in the app
    database = None  # type: ignore

# ── Feature set ──────────────────────────────────────────────────────────────
# Each feature: canonical name → how to pull it from a raw signal dict. The raw
# dict is whatever the buy path already assembles for the entry snapshot; we read
# both raw magnitudes (preferred) and the boolean signals (fallback signal). Any
# absent key yields None → standardized to 0 (neutral, no contribution).
#
# (name, [aliases], kind)  kind: "num" (use magnitude) | "bool" (0/1)
FEATURES: List[Tuple[str, List[str], str]] = [
    ("rsi",            ["rsi_value", "rsi", "raw_rsi"],                 "num"),
    ("ema_gap_pct",    ["ema_gap_pct", "ema_gap", "ema_short_long_pct"],"num"),
    ("macd_hist",      ["macd_hist", "macd_histogram", "macd"],         "num"),
    ("vol_ratio",      ["vol_ratio", "volume_ratio", "volume_vs_avg"],  "num"),
    ("obv_slope",      ["obv_slope", "obv_value"],                      "num"),
    ("atr_pct",        ["atr_pct", "atr_percent", "atr"],               "num"),
    ("spread_pct",     ["spread_pct", "spread"],                        "num"),
    ("near_low_pct",   ["near_low_pct", "near_24h_low_pct"],            "num"),
    ("score",          ["score", "signal_score"],                      "num"),
    # Boolean signals (still informative even without magnitudes captured yet).
    ("b_trend",        ["trend"],                                       "bool"),
    ("b_rsi",          ["rsi_ok", "rsi_bool"],                          "bool"),
    ("b_macd",         ["macd_ok", "macd_bool"],                        "bool"),
    ("b_volume",       ["volume"],                                      "bool"),
    ("b_obv",          ["obv"],                                         "bool"),
    ("b_reversal",     ["reversal", "reversal_confirmed", "r1"],        "bool"),
    ("b_regime_riskoff", ["regime_risk_off", "risk_off"],              "bool"),
]
FEATURE_NAMES: List[str] = [f[0] for f in FEATURES]


def _num(v: Any) -> Optional[float]:
    try:
        if v is None or isinstance(v, bool):
            return float(v) if isinstance(v, bool) else None
        return float(v)
    except (TypeError, ValueError):
        return None


def extract_features(raw: Optional[dict]) -> Dict[str, Optional[float]]:
    """Pull the canonical feature vector from a raw signal dict. Also accepts a
    nested {'signals': {...}} / {'raw': {...}} shape. Missing → None."""
    if not isinstance(raw, dict):
        raw = {}
    # Flatten common nested containers so aliases resolve either way.
    merged: Dict[str, Any] = {}
    for container in (raw, raw.get("signals"), raw.get("raw"), raw.get("gates")):
        if isinstance(container, dict):
            for k, v in container.items():
                merged.setdefault(k, v)
    out: Dict[str, Optional[float]] = {}
    for name, aliases, kind in FEATURES:
        val: Optional[float] = None
        for a in aliases:
            if a in merged:
                if kind == "bool":
                    val = 1.0 if bool(merged[a]) else 0.0
                else:
                    val = _num(merged[a])
                if val is not None:
                    break
        out[name] = val
    return out


# ── Interim (untrained) model ────────────────────────────────────────────────
# Transparent, hand-set weights on STANDARDIZED features. norm = (mean, std) used
# to z-score each feature; for the interim model these are sensible priors so the
# score is a reasonable framework placeholder — NOT a validated probability.
# Positive coeff = raises P(win); negative = lowers it. Kept small & readable.
_INTERIM_NORM: Dict[str, Tuple[float, float]] = {
    "rsi":            (45.0, 12.0),
    "ema_gap_pct":    (0.0, 0.6),
    "macd_hist":      (0.0, 0.5),
    "vol_ratio":      (1.2, 0.6),
    "obv_slope":      (0.0, 1.0),
    "atr_pct":        (1.0, 0.6),
    "spread_pct":     (0.05, 0.05),
    "near_low_pct":   (3.0, 2.5),
    "score":          (3.0, 1.5),
    "b_trend":        (0.5, 0.5),
    "b_rsi":          (0.5, 0.5),
    "b_macd":         (0.5, 0.5),
    "b_volume":       (0.5, 0.5),
    "b_obv":          (0.5, 0.5),
    "b_reversal":     (0.3, 0.46),
    "b_regime_riskoff": (0.2, 0.4),
}
_INTERIM_COEFFS: Dict[str, float] = {
    "rsi":            -0.10,   # very high RSI = chasing; mild negative
    "ema_gap_pct":     0.25,   # trend strength
    "macd_hist":       0.20,
    "vol_ratio":       0.30,   # participation
    "obv_slope":       0.20,
    "atr_pct":         0.05,   # some volatility good, but weak prior
    "spread_pct":     -0.30,   # wide spread = worse fills/edge
    "near_low_pct":   -0.05,
    "score":           0.35,   # more signals agreeing
    "b_trend":         0.40,   # trend confirmation is the biggest single lever
    "b_rsi":           0.10,
    "b_macd":          0.15,
    "b_volume":        0.15,
    "b_obv":           0.12,
    "b_reversal":      0.25,
    "b_regime_riskoff": -0.45,  # risk-off regime hurts
}
INTERIM_MODEL: Dict[str, Any] = {
    "version":   "interim-v1",
    "trained":   False,
    "intercept": 0.0,           # 50% base rate when all features are neutral
    "coeffs":    _INTERIM_COEFFS,
    "norm":      _INTERIM_NORM,
    "n_trades":  0,
    "created":   "2026-07-10T00:00:00",
    "note":      "hand-set transparent priors — NOT a validated probability",
}

_ACTIVE_KEY = "ev_model_active"          # settings KV: active model JSON
_VERSIONS_KEY = "ev_model_versions"      # settings KV: list of saved versions (metadata)
_MIN_CLEAN_TRADES_DEFAULT = 300          # S4 guardrail before the floor can gate real buys

_cache: Dict[str, Any] = {"ts": 0.0, "model": None}
_CACHE_TTL = 10.0


def config_min_clean(default: int = _MIN_CLEAN_TRADES_DEFAULT) -> int:
    """S4.3 — the clean-trade guardrail is operator-configurable via
    data.ev_min_clean_trades (falls back to 300). Read defensively from the live
    strategy file so a UI change takes effect without a restart."""
    try:
        import config as _cfg
        import json as _j
        with open(_cfg.STRATEGY_FILE) as _f:
            data = (_j.load(_f) or {}).get("data") or {}
        v = int(data.get("ev_min_clean_trades", default))
        return max(20, v)
    except Exception:
        return int(default)


def _sigmoid(x: float) -> float:
    if x >= 0:
        z = math.exp(-x)
        return 1.0 / (1.0 + z)
    z = math.exp(x)
    return z / (1.0 + z)


def get_active_model() -> Dict[str, Any]:
    """The active weight-version (hot-loaded, 10s TTL). Falls back to the interim
    model when none has been activated or persistence is unavailable."""
    now = time.time()
    if _cache["model"] is not None and now - _cache["ts"] < _CACHE_TTL:
        return _cache["model"]
    model = None
    try:
        if database is not None:
            raw = database.get_setting(_ACTIVE_KEY)
            if raw:
                m = json.loads(raw)
                if isinstance(m, dict) and isinstance(m.get("coeffs"), dict):
                    model = m
    except Exception:
        model = None
    if model is None:
        model = INTERIM_MODEL
    _cache["model"] = model
    _cache["ts"] = now
    return model


def _z(name: str, val: Optional[float], norm: Dict[str, Any]) -> float:
    """Standardize a feature; missing → 0 (neutral). std<=0 → 0."""
    if val is None:
        return 0.0
    m, s = norm.get(name, (0.0, 1.0))
    try:
        s = float(s)
        if s <= 1e-9:
            return 0.0
        return (float(val) - float(m)) / s
    except Exception:
        return 0.0


def score(raw_or_features: Optional[dict], model: Optional[dict] = None) -> Dict[str, Any]:
    """Score a candidate. Accepts a raw signal dict OR a pre-extracted feature
    dict. Returns:
      {probability: 0-1, pct: 0-100, logit, trained: bool, version,
       contributions: {feature: logit_points}, top_reasons: [(feature, pts)...],
       n_trades}
    Every score is fully decomposed (interpretability is mandatory)."""
    m = model or get_active_model()
    coeffs: Dict[str, float] = m.get("coeffs", {}) or {}
    norm: Dict[str, Any] = m.get("norm", {}) or {}
    intercept = float(m.get("intercept", 0.0) or 0.0)

    # Accept either raw signals or an already-extracted feature vector.
    if raw_or_features and all(k in FEATURE_NAMES for k in raw_or_features.keys()):
        feats = {k: _num(v) if not isinstance(v, bool) else float(v)
                 for k, v in raw_or_features.items()}
    else:
        feats = extract_features(raw_or_features)

    logit = intercept
    contributions: Dict[str, float] = {}
    for name in FEATURE_NAMES:
        w = float(coeffs.get(name, 0.0) or 0.0)
        if w == 0.0:
            continue
        z = _z(name, feats.get(name), norm)
        pts = w * z
        if abs(pts) > 1e-9:
            contributions[name] = round(pts, 4)
            logit += pts
    p = _sigmoid(logit)
    top = sorted(contributions.items(), key=lambda kv: abs(kv[1]), reverse=True)[:6]
    return {
        "probability": round(p, 4),
        "pct":         round(p * 100.0, 1),
        "logit":       round(logit, 4),
        "trained":     bool(m.get("trained", False)),
        "version":     m.get("version", "interim-v1"),
        "n_trades":    int(m.get("n_trades", 0) or 0),
        "contributions": contributions,
        "top_reasons": [{"feature": k, "points": v} for k, v in top],
    }


# ── S4: training (pure-python logistic regression, dependency-light) ──────────
def _fit_logistic(X: List[List[float]], y: List[int],
                  epochs: int = 400, lr: float = 0.1, l2: float = 1e-3
                  ) -> Tuple[List[float], float]:
    """Batch gradient descent on standardized X. Returns (coeffs, intercept)."""
    n = len(X)
    d = len(X[0]) if n else 0
    w = [0.0] * d
    b = 0.0
    if n == 0 or d == 0:
        return w, b
    for _ in range(epochs):
        gw = [0.0] * d
        gb = 0.0
        for i in range(n):
            z = b + sum(w[j] * X[i][j] for j in range(d))
            p = _sigmoid(z)
            err = p - y[i]
            gb += err
            for j in range(d):
                gw[j] += err * X[i][j]
        b -= lr * (gb / n)
        for j in range(d):
            w[j] -= lr * (gw[j] / n + l2 * w[j])
    return w, b


def train(samples: List[dict], min_clean: Optional[int] = None
          ) -> Dict[str, Any]:
    """Fit a NEW weight-version on labeled samples. Does NOT activate it.
    Each sample: {"features": {...}} or {"raw": {...}} plus "label" in {0,1}
    (1 = win / positive realized R). Computes per-feature (mean,std) from the
    data, standardizes, fits, and reports in-sample + held-out calibration.
    Returns a model dict (same shape as INTERIM_MODEL) + a `report`."""
    rows: List[Tuple[Dict[str, Optional[float]], int]] = []
    for s in samples or []:
        if not isinstance(s, dict) or "label" not in s:
            continue
        raw = s.get("features") if isinstance(s.get("features"), dict) else s.get("raw")
        feats = raw if (isinstance(raw, dict) and all(k in FEATURE_NAMES for k in raw)) \
            else extract_features(raw)
        try:
            label = 1 if int(s["label"]) > 0 else 0
        except Exception:
            continue
        rows.append((feats, label))

    if min_clean is None:
        min_clean = config_min_clean()
    n = len(rows)
    if n < max(20, int(min_clean)):
        return {"ok": False, "trained": False, "n": n, "min_clean": int(min_clean),
                "error": f"insufficient clean samples ({n} < {min_clean})"}

    # Per-feature mean/std from the data.
    norm: Dict[str, Tuple[float, float]] = {}
    for name in FEATURE_NAMES:
        vals = [float(f.get(name)) for f, _ in rows if f.get(name) is not None]
        if len(vals) >= 2:
            mean = sum(vals) / len(vals)
            var = sum((v - mean) ** 2 for v in vals) / (len(vals) - 1)
            std = math.sqrt(var) if var > 1e-12 else 1.0
        else:
            mean, std = 0.0, 1.0
        norm[name] = (round(mean, 6), round(std, 6))

    def _vec(f: Dict[str, Optional[float]]) -> List[float]:
        return [_z(name, f.get(name), norm) for name in FEATURE_NAMES]

    # Held-out split (last 20% by insertion order = most recent).
    cut = int(n * 0.8)
    train_rows, test_rows = rows[:cut], rows[cut:]
    Xtr = [_vec(f) for f, _ in train_rows]
    ytr = [lbl for _, lbl in train_rows]
    w, b = _fit_logistic(Xtr, ytr)
    coeffs = {name: round(w[i], 6) for i, name in enumerate(FEATURE_NAMES)}

    def _calib(rws: List[Tuple[Dict[str, Optional[float]], int]]) -> Dict[str, Any]:
        if not rws:
            return {"n": 0}
        correct = 0
        pos = 0
        brier = 0.0
        for f, lbl in rws:
            z = b + sum(w[i] * _z(nm, f.get(nm), norm) for i, nm in enumerate(FEATURE_NAMES))
            p = _sigmoid(z)
            brier += (p - lbl) ** 2
            if (p >= 0.5) == (lbl == 1):
                correct += 1
            pos += lbl
        k = len(rws)
        return {"n": k, "accuracy": round(correct / k, 4),
                "win_rate": round(pos / k, 4), "brier": round(brier / k, 4)}

    model = {
        "version":   f"trained-{int(time.time())}",
        "trained":   True,
        "intercept": round(b, 6),
        "coeffs":    coeffs,
        "norm":      norm,
        "n_trades":  n,
        "created":   time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()),
        "note":      "fitted logistic regression on live+paper outcomes",
    }
    model["report"] = {
        "n_total": n, "n_train": len(train_rows), "n_test": len(test_rows),
        "in_sample": _calib(train_rows), "held_out": _calib(test_rows),
    }
    return {"ok": True, "trained": True, "model": model, "report": model["report"]}


# ── Version storage / activation (operator-reviewed, audit-logged) ────────────
def save_version(model: Dict[str, Any]) -> Optional[str]:
    """Persist a weight-version's metadata (not activated). Returns its version id."""
    if database is None or not isinstance(model, dict):
        return None
    try:
        vid = str(model.get("version") or f"v-{int(time.time())}")
        raw = database.get_setting(_VERSIONS_KEY)
        versions = json.loads(raw) if raw else []
        if not isinstance(versions, list):
            versions = []
        # Store the FULL model under its own key + metadata in the index.
        database.save_setting(f"ev_model_version::{vid}", json.dumps(model, default=str))
        meta = {"version": vid, "trained": bool(model.get("trained")),
                "n_trades": int(model.get("n_trades", 0) or 0),
                "created": model.get("created"), "report": model.get("report")}
        versions = [v for v in versions if v.get("version") != vid][:49] + [meta]
        database.save_setting(_VERSIONS_KEY, json.dumps(versions, default=str))
        return vid
    except Exception:
        return None


def list_versions() -> List[dict]:
    if database is None:
        return []
    try:
        raw = database.get_setting(_VERSIONS_KEY)
        versions = json.loads(raw) if raw else []
        return versions if isinstance(versions, list) else []
    except Exception:
        return []


def activate_version(version_id: str, actor: str = "operator") -> Dict[str, Any]:
    """Make a stored weight-version the ACTIVE model (hot). Audit-logged by the
    caller (control_api records config_history). Returns the activated model or
    an error dict. Never silently swaps — the caller gates this behind a UI action."""
    if database is None:
        return {"ok": False, "error": "no persistence"}
    try:
        if version_id in ("interim-v1", "interim"):
            database.save_setting(_ACTIVE_KEY, json.dumps(INTERIM_MODEL))
            _cache["ts"] = 0.0
            return {"ok": True, "active": "interim-v1"}
        raw = database.get_setting(f"ev_model_version::{version_id}")
        if not raw:
            return {"ok": False, "error": f"unknown version {version_id}"}
        model = json.loads(raw)
        database.save_setting(_ACTIVE_KEY, json.dumps(model, default=str))
        _cache["ts"] = 0.0  # invalidate cache → hot activation
        try:
            database.log_activity(f"EV model activated: {version_id} "
                                  f"(trained={model.get('trained')}, n={model.get('n_trades')})",
                                  "info")
        except Exception:
            pass
        return {"ok": True, "active": version_id, "trained": bool(model.get("trained"))}
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}


def active_weights() -> List[Dict[str, Any]]:
    """S4.4 — the active model's per-signal weights, sorted by magnitude, so the
    operator can see what the data actually values (and which signals are dead)."""
    m = get_active_model()
    coeffs = m.get("coeffs", {}) or {}
    rows = [{"feature": k, "weight": round(float(v), 4)} for k, v in coeffs.items()]
    rows.sort(key=lambda r: abs(r["weight"]), reverse=True)
    return rows


# ═════════════════════════════════════════════════════════════════════════════
# Part S-2 — WolfScore v3: spot-computable, regime-GATING buy-success score.
# 8 bounded sub-metrics (T,M,R,C,W,V,X,F) → regime-gated families → calibrated
# 0-100 probability. Weights are LEARNED (train_wolfscore) via the same versioned
# /activated flow; INTERIM weights are transparent priors until ≥300 clean trades.
# Scenario-tested: uptrend picks momentum, downtrend picks the decoupled coin.
# ═════════════════════════════════════════════════════════════════════════════
WOLF_SUBMETRICS = ["T", "M", "R", "C", "W", "V", "X", "F"]

# INTERIM_UNTRAINED weights (framework-run only; data overwrites them). F heaviest
# — cost is the top killer on $11 tickets; R highest in the defensive family
# because decoupling is the point of downtrend selection.
WOLF_INTERIM: Dict[str, Any] = {
    "version": "wolf-interim-v3",
    "kind":    "wolfscore_v3",
    "trained": False,
    "n_trades": 0,
    "created": "2026-07-10T12:00:00",
    "note":    "WolfScore v3 transparent priors — NOT a validated probability",
    "weights": {
        "b0":   -1.0,
        "wm_T":  1.6, "wm_M": 1.4,          # momentum family
        "wd_R":  1.8, "wd_W": 1.4,          # defensive family (R = decoupling)
        # S3-2 — up-regime anti-chasing. W (VWAP room) previously lived ONLY in the
        # defensive family, gated by (dn + 0.5·neutral), so in a strong uptrend it
        # was switched OFF — the bot had no brake on pump-chasing exactly where it
        # matters most (paper data: uptrend win-rate 15%). wu_W makes W count in the
        # UP regime: near VWAP (W→+1) rewarded, extended above VWAP (W<0) penalized.
        "wu_W":  1.5,
        "w_C":   1.1, "w_V": 0.7, "w_X": 0.5, "w_F": 3.5,
    },
}


def _clip(x: float, lo: float, hi: float) -> float:
    try:
        x = float(x)
    except (TypeError, ValueError):
        return 0.0
    return lo if x < lo else hi if x > hi else x


def regime_tilt(btc_roc_1h_frac: Optional[float]) -> float:
    """Flaw #1 fix: tanh(60 × ROC_frac) so a 1% BTC move gives ±0.54, not ~0.03.
    1h ROC (not 15m) to reduce noise. ∈ [-1, +1]. Missing → 0 (neutral)."""
    if btc_roc_1h_frac is None:
        return 0.0
    try:
        return math.tanh(60.0 * float(btc_roc_1h_frac))
    except Exception:
        return 0.0


def compute_submetrics(inp: Dict[str, Any], cohort: Optional[Dict[str, Any]] = None
                       ) -> Dict[str, float]:
    """The 8 bounded WolfScore sub-metrics, all spot-computable (streamed trades +
    1m/5m klines + miniTicker — NO OI / premium / L2 depth). Every input is
    optional; a missing feed degrades that sub-metric to 0/neutral, never raises.

    Expected inp keys (all optional):
      ema9, ema21, atr_pct, macd_hist, rolling_max_abs_hist_20, roc_15m,
      taker_buy_vol_5m, taker_sell_vol_5m, total_vol_5m, p_mid, vwap_15m,
      vol_5m, avg_vol_20, atr_target, atr_halfwidth, half_spread_pct,
      avg_slippage_pct, planned_stop_pct.
    cohort: {"median_roc_15m": float} — basket median of ROC_15m for R (decoupling).
    """
    g = inp.get
    cohort = cohort or {}
    eps = 1e-9

    # T — trend alignment: EMA gap normalized by the coin's own ATR%.
    ema9, ema21 = g("ema9"), g("ema21")
    atr_pct = g("atr_pct")
    T = 0.0
    try:
        if ema9 is not None and ema21 and float(ema21) != 0 and atr_pct:
            atr_norm = max(float(atr_pct), 0.05)  # avoid divide-by-tiny
            T = _clip(((float(ema9) - float(ema21)) / float(ema21) * 100.0) / atr_norm, -1, 1)
    except Exception:
        T = 0.0

    # M — momentum: macd_hist normalized by its rolling max |hist| (20).
    M = 0.0
    try:
        mh, rmax = g("macd_hist"), g("rolling_max_abs_hist_20")
        if mh is not None and rmax and float(rmax) > eps:
            M = _clip(float(mh) / float(rmax), -1, 1)
    except Exception:
        M = 0.0

    # R — cohort-relative strength (DECOUPLING): this coin's 15m ROC vs basket MEDIAN.
    R = 0.0
    try:
        roc = g("roc_15m")
        med = cohort.get("median_roc_15m")
        if roc is not None and med is not None:
            R = math.tanh(8.0 * (float(roc) - float(med)))
    except Exception:
        R = 0.0

    # C — CVD pressure: taker buy vs sell over 5m (real order-flow, single term).
    C = 0.0
    try:
        tb, ts, tot = g("taker_buy_vol_5m"), g("taker_sell_vol_5m"), g("total_vol_5m")
        denom = None
        if tot is not None and float(tot) > eps:
            denom = float(tot)
        elif tb is not None and ts is not None and (float(tb) + float(ts)) > eps:
            denom = float(tb) + float(ts)
        if denom and tb is not None and ts is not None:
            C = _clip((float(tb) - float(ts)) / (denom + eps), -1, 1)
    except Exception:
        C = 0.0

    # W — VWAP room (ANTI-CHASING): near VWAP → +1; far above → negative.
    W = 0.0
    try:
        pmid, vwap = g("p_mid"), g("vwap_15m")
        if pmid is not None and vwap and float(vwap) != 0:
            W = _clip(1.0 - 4.0 * abs(float(pmid) - float(vwap)) / float(vwap), -1, 1)
    except Exception:
        W = 0.0

    # V — volume confirmation: vol_5m / avg_vol_20, mapped to [0,1].
    V = 0.0
    try:
        v5, va = g("vol_5m"), g("avg_vol_20")
        if v5 is not None and va and float(va) > eps:
            V = _clip(float(v5) / float(va) - 1.0, 0, 2) / 2.0
    except Exception:
        V = 0.0

    # X — volatility fitness: tent peaking at the middle of the tradeable ATR band.
    X = 0.0
    try:
        if atr_pct is not None:
            tgt = float(g("atr_target") or 1.0)
            hw = float(g("atr_halfwidth") or 1.0) or 1.0
            X = _clip(1.0 - abs(float(atr_pct) - tgt) / hw, 0, 1)
    except Exception:
        X = 0.0

    # F — friction: (half-spread + recent avg slippage) / planned stop distance.
    F = 0.0
    try:
        hs = float(g("half_spread_pct") or 0.0)
        sl = float(g("avg_slippage_pct") or 0.0)
        stop = float(g("planned_stop_pct") or 0.0)
        if stop > eps:
            F = _clip((hs + sl) / stop, 0, 1)
    except Exception:
        F = 0.0

    return {"T": T, "M": M, "R": R, "C": C, "W": W, "V": V, "X": X, "F": F}


def get_active_wolf_model() -> Dict[str, Any]:
    """Active WolfScore weight-set (hot, 10s TTL). Falls back to the interim v3
    priors. Reuses the same DB-active model when it's a wolfscore_v3 kind."""
    m = get_active_model()
    if isinstance(m, dict) and m.get("kind") == "wolfscore_v3" and isinstance(m.get("weights"), dict):
        return m
    return WOLF_INTERIM


def wolfscore(sub: Dict[str, float], tilt: float,
              model: Optional[dict] = None,
              up_extension_veto: bool = True,
              up_extension_w_thr: float = 0.0) -> Dict[str, Any]:
    """Layer-2 regime-GATING score. Returns 0-100 calibrated probability + full
    decomposition. Flaw #2: F>0.5 is a HARD gate (score 0). Flaw #3: the regime
    tilt GATES which input families count (not just an additive dim), so downtrend
    flips selection toward the decoupled coin.

    S3-2 — up-regime anti-chasing:
      * wu_W term: W (VWAP room) now contributes in the UP regime, so near-VWAP
        coins score higher and extended-above-VWAP coins score lower in uptrends.
      * up_extension_veto (default ON): in a clear uptrend (tilt>0.15), a coin
        whose W has fallen at/below up_extension_w_thr (extended / broken vs VWAP)
        is HARD-gated. Operator-toggleable — pass up_extension_veto=False to rely
        purely on the learned wu_W weight once a model is trained on this regime."""
    m = model or get_active_wolf_model()
    w = m.get("weights", WOLF_INTERIM["weights"])
    T, M, R = sub.get("T", 0.0), sub.get("M", 0.0), sub.get("R", 0.0)
    C, W, V = sub.get("C", 0.0), sub.get("W", 0.0), sub.get("V", 0.0)
    X, F = sub.get("X", 0.0), sub.get("F", 0.0)

    # Flaw #2 — friction hard gate.
    if F > 0.5:
        return {"score": 0.0, "pct": 0.0, "z": None, "hard_gate": "friction",
                "regime_tilt": round(tilt, 4), "submetrics": sub,
                "families": {"momentum": 0.0, "defensive": 0.0, "base": 0.0, "residual": 0.0},
                "contributions": {}, "top_reasons": [{"feature": "F", "points": -999}],
                "trained": bool(m.get("trained", False)), "version": m.get("version", "wolf-interim-v3")}

    # S3-2 — extended-uptrend hard gate. In a clear uptrend, a coin that has run
    # too far from VWAP (W ≤ threshold) is the classic pump-chase that reverses
    # (paper data: uptrend win-rate 15%). Blocked here, before scoring.
    if up_extension_veto and tilt > 0.15 and W <= up_extension_w_thr:
        return {"score": 0.0, "pct": 0.0, "z": None, "hard_gate": "extended_uptrend",
                "regime_tilt": round(tilt, 4), "submetrics": sub,
                "families": {"momentum": 0.0, "defensive": 0.0, "base": 0.0, "residual": 0.0},
                "contributions": {}, "top_reasons": [{"feature": "W", "points": -999}],
                "trained": bool(m.get("trained", False)), "version": m.get("version", "wolf-interim-v3")}

    up = max(0.0, tilt)
    dn = max(0.0, -tilt)
    neutral = 1.0 - abs(tilt)

    mom_core = w.get("wm_T", 1.6) * T + w.get("wm_M", 1.4) * M
    def_core = w.get("wd_R", 1.8) * R + w.get("wd_W", 1.4) * W

    momentum_family = (up + 0.5 * neutral) * mom_core
    defensive_family = (dn + 0.5 * neutral) * def_core
    # S3-2 — W (VWAP room) counts in the up-regime too (anti-chasing brake).
    up_room = w.get("wu_W", 1.5) * W * up
    base = (w.get("w_C", 1.1) * C + w.get("w_V", 0.7) * V
            + w.get("w_X", 0.5) * X - w.get("w_F", 3.5) * F)
    residual = 0.3 * def_core * up - 0.3 * mom_core * dn

    z = w.get("b0", -1.0) + momentum_family + defensive_family + up_room + base + residual
    p = _sigmoid(z)

    contributions = {
        "T": round(w.get("wm_T", 1.6) * T * (up + 0.5 * neutral), 3),
        "M": round(w.get("wm_M", 1.4) * M * (up + 0.5 * neutral), 3),
        "R": round(w.get("wd_R", 1.8) * R * (dn + 0.5 * neutral), 3),
        "W": round(w.get("wd_W", 1.4) * W * (dn + 0.5 * neutral)
                   + w.get("wu_W", 1.5) * W * up, 3),
        "C": round(w.get("w_C", 1.1) * C, 3),
        "V": round(w.get("w_V", 0.7) * V, 3),
        "X": round(w.get("w_X", 0.5) * X, 3),
        "F": round(-w.get("w_F", 3.5) * F, 3),
    }
    top = sorted(contributions.items(), key=lambda kv: abs(kv[1]), reverse=True)[:6]
    return {
        "score": round(p * 100.0, 1), "pct": round(p * 100.0, 1),
        "probability": round(p, 4), "z": round(z, 4), "hard_gate": None,
        "regime_tilt": round(tilt, 4),
        "regime": "up" if tilt > 0.15 else "down" if tilt < -0.15 else "side",
        "submetrics": {k: round(v, 4) for k, v in sub.items()},
        "families": {"momentum": round(momentum_family, 3),
                     "defensive": round(defensive_family, 3),
                     "base": round(base, 3), "residual": round(residual, 3)},
        "contributions": contributions,
        "top_reasons": [{"feature": k, "points": v} for k, v in top],
        "trained": bool(m.get("trained", False)),
        "version": m.get("version", "wolf-interim-v3"),
        "n_trades": int(m.get("n_trades", 0) or 0),
    }


def _wolf_derived_features(sub: Dict[str, float], tilt: float) -> List[float]:
    """The regime-gating score is LINEAR in the 9 weights once each weight's gate
    is folded into a derived feature. Order matches WOLF_WEIGHT_ORDER."""
    up = max(0.0, tilt); dn = max(0.0, -tilt); neutral = 1.0 - abs(tilt)
    T, M = sub.get("T", 0.0), sub.get("M", 0.0)
    R, W = sub.get("R", 0.0), sub.get("W", 0.0)
    gm = up + 0.5 * neutral       # momentum-family gate
    gd = dn + 0.5 * neutral       # defensive-family gate
    return [
        T * (gm - 0.3 * dn),      # wm_T (also in −0.3·mom_core·dn)
        M * (gm - 0.3 * dn),      # wm_M
        R * (gd + 0.3 * up),      # wd_R (also in +0.3·def_core·up)
        W * (gd + 0.3 * up),      # wd_W
        W * up,                   # wu_W (S3-2 up-regime VWAP-room / anti-chasing)
        sub.get("C", 0.0),        # w_C
        sub.get("V", 0.0),        # w_V
        sub.get("X", 0.0),        # w_X
        -sub.get("F", 0.0),       # w_F (enters negatively)
    ]


WOLF_WEIGHT_ORDER = ["wm_T", "wm_M", "wd_R", "wd_W", "wu_W",
                     "w_C", "w_V", "w_X", "w_F"]


def train_wolfscore(samples: List[dict], min_clean: Optional[int] = None) -> Dict[str, Any]:
    """Fit the 9 WolfScore weights via logistic regression on stored (submetrics,
    regime_tilt, label) rows. Each sample: {"submetrics": {...}, "regime_tilt":
    float, "label": 0/1}. Friction-gated samples (F>0.5) are dropped (they'd be
    hard-rejected in production). Returns a wolfscore_v3 model version (NOT
    activated) + a held-out calibration report."""
    if min_clean is None:
        min_clean = config_min_clean()
    rows: List[Tuple[List[float], int]] = []
    for s in samples or []:
        if not isinstance(s, dict) or "label" not in s:
            continue
        sub = s.get("submetrics") if isinstance(s.get("submetrics"), dict) else None
        if sub is None:
            # tolerate rows that stored raw inputs → recompute submetrics
            sub = compute_submetrics(s.get("features") or s.get("raw") or {}, s.get("cohort"))
        if float(sub.get("F", 0.0)) > 0.5:
            continue
        tilt = float(s.get("regime_tilt", 0.0) or 0.0)
        try:
            label = 1 if int(s["label"]) > 0 else 0
        except Exception:
            continue
        rows.append((_wolf_derived_features(sub, tilt), label))

    n = len(rows)
    if n < max(20, int(min_clean)):
        return {"ok": False, "trained": False, "n": n, "min_clean": int(min_clean),
                "error": f"insufficient clean samples ({n} < {min_clean})"}
    cut = int(n * 0.8)
    Xtr = [x for x, _ in rows[:cut]]; ytr = [y for _, y in rows[:cut]]
    w, b = _fit_logistic(Xtr, ytr)
    weights = {"b0": round(b, 6)}
    for i, name in enumerate(WOLF_WEIGHT_ORDER):
        weights[name] = round(w[i], 6)

    def _calib(rws):
        if not rws:
            return {"n": 0}
        correct = pos = 0; brier = 0.0
        for x, lbl in rws:
            p = _sigmoid(b + sum(w[i] * x[i] for i in range(len(x))))
            brier += (p - lbl) ** 2
            if (p >= 0.5) == (lbl == 1):
                correct += 1
            pos += lbl
        k = len(rws)
        return {"n": k, "accuracy": round(correct / k, 4),
                "win_rate": round(pos / k, 4), "brier": round(brier / k, 4)}

    model = {
        "version": f"wolf-trained-{int(time.time())}", "kind": "wolfscore_v3",
        "trained": True, "weights": weights, "n_trades": n,
        "created": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()),
        "note": "fitted WolfScore v3 weights on live+paper outcomes",
        "report": {"n_total": n, "n_train": cut, "n_test": n - cut,
                   "in_sample": _calib(rows[:cut]), "held_out": _calib(rows[cut:])},
    }
    return {"ok": True, "trained": True, "model": model, "report": model["report"]}


def adaptive_floor(scores: List[float], abs_floor: float = 55.0,
                   mode: str = "p75", k: float = 0.5) -> Dict[str, Any]:
    """The buy floor is regime-aware / percentile-based, not a static number.
    A candidate must clear BOTH the absolute floor AND the distribution rule
    (p75, or mean+k·stdev). In a downtrend where nothing decouples, even the best
    score fails the absolute floor → hold cash; in recovery scores rise back over
    it → re-engage automatically. Returns the effective threshold."""
    vals = [float(s) for s in (scores or []) if s is not None]
    dist_thr = abs_floor
    if vals and mode not in ("absolute", "off"):
        # S3-1 — 'absolute'/'off' pin the threshold to the static abs_floor (55),
        # so an 80-scoring coin fires even when it is not top-quartile in a strong
        # field. The distribution modes below can only RAISE the bar above 55.
        if mode == "meanstd":
            mean = sum(vals) / len(vals)
            var = sum((v - mean) ** 2 for v in vals) / max(1, len(vals) - 1)
            dist_thr = mean + k * math.sqrt(var)
        else:  # percentile (default p75)
            try:
                pct = float(mode[1:]) / 100.0 if mode.startswith("p") else 0.75
            except Exception:
                pct = 0.75
            s = sorted(vals)
            idx = min(len(s) - 1, max(0, int(round(pct * (len(s) - 1)))))
            dist_thr = s[idx]
    threshold = max(float(abs_floor), float(dist_thr))
    return {"threshold": round(threshold, 2), "abs_floor": float(abs_floor),
            "dist_threshold": round(dist_thr, 2), "mode": mode, "n": len(vals)}


def model_status(min_clean: Optional[int] = None) -> Dict[str, Any]:
    """UI header: current model version, trained/untrained, progress toward the
    (configurable) clean-trade minimum below which the floor is display-only
    (S4.3), and the active per-signal learned weights (S4.4)."""
    if min_clean is None:
        min_clean = config_min_clean()
    m = get_active_model()
    n = int(m.get("n_trades", 0) or 0)
    return {
        "version": m.get("version", "interim-v1"),
        "trained": bool(m.get("trained", False)),
        "n_trades": n,
        "min_clean": int(min_clean),
        "floor_active": bool(m.get("trained", False) and n >= int(min_clean)),
        "note": m.get("note", ""),
        "weights": active_weights(),
    }
