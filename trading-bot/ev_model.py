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
                  epochs: int = 400, lr: float = 0.1, l2: float = 1e-3,
                  sample_weight: Optional[List[float]] = None
                  ) -> Tuple[List[float], float]:
    """Weighted batch gradient descent on standardized X. Returns (coeffs,
    intercept). sample_weight (S4-1.2) scales each row's gradient contribution so
    a +1.6R win teaches the model more than a +0.05R scratch — the score then
    reflects PROFITABILITY, not just hit-rate. Weights default to 1.0."""
    n = len(X)
    d = len(X[0]) if n else 0
    w = [0.0] * d
    b = 0.0
    if n == 0 or d == 0:
        return w, b
    if sample_weight and len(sample_weight) == n:
        sw = [max(0.0, float(s)) for s in sample_weight]
    else:
        sw = [1.0] * n
    wsum = sum(sw) or float(n)
    for _ in range(epochs):
        gw = [0.0] * d
        gb = 0.0
        for i in range(n):
            z = b + sum(w[j] * X[i][j] for j in range(d))
            p = _sigmoid(z)
            err = (p - y[i]) * sw[i]
            gb += err
            for j in range(d):
                gw[j] += err * X[i][j]
        b -= lr * (gb / wsum)
        for j in range(d):
            w[j] -= lr * (gw[j] / wsum + l2 * w[j])
    return w, b


def _realized_r_weight(realized_r: Optional[float],
                       floor: float = 0.2, cap: float = 3.0) -> float:
    """S4-1.2 — per-sample training weight from |realized_R|, clamped to [floor,
    cap] so scratches still count a little and a single 5R outlier can't dominate
    the fit. Missing R → 1.0 (neutral)."""
    if realized_r is None:
        return 1.0
    try:
        a = abs(float(realized_r))
    except (TypeError, ValueError):
        return 1.0
    return floor if a < floor else cap if a > cap else a


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


# ═══════════════════════════════════════════════════════════════════════════════
# WolfScore-MR (mean-reversion) — dip-quality buy score. Scores "how likely is this
# coin to bounce back above entry soon?" instead of v3's momentum (which structurally
# buys tops). Backtested 90 coins × 10d on real 5m data + the real exit engine:
# +$0.68/day, 92% win, PF 2.28, 0 disaster stops (v3: −$0.87/day, PF 0.42).
# v3 above is left FULLY INTACT; the engine selects via entries.buy_formula. The
# exit/sell engine is NOT touched by this.
# ═══════════════════════════════════════════════════════════════════════════════

WOLFSCORE_MR_VERSION = "wolf-mr-v1"
_MR_DIP_MIN_BASE = 0.7


def _mr_clip(x, lo=-1.0, hi=1.0):
    try:
        return max(lo, min(hi, float(x)))
    except (TypeError, ValueError):
        return 0.0


def compute_submetrics_mr(inp: Dict[str, Any], cohort: Optional[Dict[str, Any]] = None
                          ) -> Dict[str, float]:
    """MR 7 sub-metrics (O,E,K,Mk,Fr,Q,RS) from 5m klines (with taker volume) +
    cohort median + btc 1h roc + sma200. Each value clipped to its range. O (dip)
    and Q (quality) are REQUIRED — their gates are the thesis + the tail-risk
    protection for a no-stop strategy; a missing feed leaves them 0 so the gate
    fires safe. Never raises. Fr defaults to 1.0 (blocks) when ATR is unavailable."""
    import math as _m
    cohort = cohort or {}
    out: Dict[str, float] = {"O": 0.0, "E": 0.0, "K": 0.0, "Mk": 0.0,
                             "Fr": 1.0, "Q": 0.0, "RS": 0.0}
    try:
        closes = [float(c) for c in (inp.get("closes_5m") or []) if c is not None]
        vols   = [float(v) for v in (inp.get("volumes_5m") or []) if v is not None]
        takers = [float(t) for t in (inp.get("taker_5m") or []) if t is not None]
        price  = float(inp.get("price") or (closes[-1] if closes else 0.0))

        # O — dip depth (GATE): standardized distance below the 20-bar mean.
        if len(closes) >= 20 and price > 0:
            w = closes[-20:]
            sma20 = sum(w) / 20.0
            mean = sma20
            sd20 = (sum((x - mean) ** 2 for x in w) / 20.0) ** 0.5
            if sd20 > 0:
                out["O"] = _mr_clip((sma20 - price) / (2.0 * sd20))

        # E — exhaustion: 0.5·cvd_turn + 0.3·vol_fade + 0.6·lower_wick_frac.
        cvd_turn = 0.0
        if len(vols) >= 6 and len(takers) >= 6:
            cvd = [2.0 * takers[i] - vols[i] for i in range(len(vols))]
            if sum(cvd[-3:]) > sum(cvd[-6:-3]):
                cvd_turn = 1.0
            else:
                cvd_turn = -1.0
        vol_fade = 0.0
        if len(vols) >= 20:
            recent3 = sum(vols[-3:]) / 3.0
            avg20 = sum(vols[-20:]) / 20.0
            vol_fade = 1.0 if recent3 < avg20 else -0.5
        lower_wick = 0.0
        oh = inp.get("ohlc_last")   # (open, high, low, close) of the last candle
        if oh and len(oh) >= 4:
            o_, h_, l_, c_ = float(oh[0]), float(oh[1]), float(oh[2]), float(oh[3])
            rng = h_ - l_
            if rng > 0:
                lower_wick = (min(c_, o_) - l_) / rng
        out["E"] = _mr_clip(0.5 * cvd_turn + 0.3 * vol_fade + 0.6 * lower_wick)

        # K — knife guard (GATE at <−0.5): is the 3-bar drop ACCELERATING?
        roc3 = 0.0
        if len(closes) >= 7 and closes[-4] and closes[-7]:
            roc3 = closes[-1] / closes[-4] - 1.0
            roc_p3 = closes[-4] / closes[-7] - 1.0
            out["K"] = _mr_clip(-(roc3 - roc_p3) * 200.0)

        # Mk — market health (GATE at <−0.6): btc 12×5m ≈ 1h roc.
        btc = inp.get("btc_roc_1h")
        if btc is not None:
            out["Mk"] = _mr_clip(float(btc) * 150.0)

        # Fr — friction (GATE at >0.5): (2·fee + spread) / atr, all FRACTIONS.
        atr = inp.get("atr_frac")
        if atr and float(atr) > 0:
            fee = float(inp.get("fee_frac", 0.001))
            spread = float(inp.get("spread_frac", 0.0) or 0.0)
            out["Fr"] = min(1.0, (2.0 * fee + spread) / float(atr))

        # Q — coin quality (GATE at <−0.5): price vs the 200-bar mean.
        sma200 = inp.get("sma200")
        if sma200 and float(sma200) > 0 and price > 0:
            out["Q"] = _mr_clip((price / float(sma200) - 1.0) * 20.0)

        # RS — relative strength: coin 3-bar roc vs the cohort median 3-bar roc.
        med = cohort.get("median_roc_3bar")
        if med is not None:
            out["RS"] = _m.tanh(200.0 * (roc3 - float(med)))
    except Exception:
        pass
    return out


_MR_CALL_COUNT = 0   # §11-5: must stay 0 under P5 (MR must execute nowhere)


def get_mr_call_count() -> int:
    """Number of times wolfscore_mr has run since boot. Under buy_formula='p5' this
    MUST remain 0 — the operator's proof that the retired MR engine fires nowhere."""
    return _MR_CALL_COUNT


def wolfscore_mr(sub: Dict[str, float], tilt: float = 0.0) -> Dict[str, Any]:
    """MR score: 5 hard gates → regime-tuned dip_min/weights → z = b0 + min(1,O)·
    thesis → 100·sigmoid(z). O multiplies the thesis so no dip collapses the score
    regardless of coin quality. Returns the same decomposition shape as v3.
    DEPRECATED under P5 (rollback-only); _MR_CALL_COUNT tracks stray calls."""
    global _MR_CALL_COUNT
    _MR_CALL_COUNT += 1
    import math as _m
    O  = float(sub.get("O", 0.0)); E = float(sub.get("E", 0.0))
    K  = float(sub.get("K", 0.0)); Mk = float(sub.get("Mk", 0.0))
    Fr = float(sub.get("Fr", 1.0)); Q = float(sub.get("Q", 0.0))
    RS = float(sub.get("RS", 0.0))
    t = float(tilt or 0.0)
    regime = "up" if t > 0.15 else "down" if t < -0.15 else "side"

    dip_min = _MR_DIP_MIN_BASE
    wE, wK, wMk, wQ, wRS, wFr, b0 = 2.0, 1.5, 1.2, 0.8, 0.0, 3.0, -0.8
    if regime == "down":            # deeper, safer dip; reward coins fighting trend
        dip_min = _MR_DIP_MIN_BASE * 1.6
        wK, wMk, wRS, b0 = 2.5, 2.0, 1.5, -1.4
    elif regime == "up":            # shallower dips OK; exhaustion matters more
        dip_min = _MR_DIP_MIN_BASE * 0.7
        wE, b0 = 2.4, -0.5

    def _mr_zero(gate: str) -> Dict[str, Any]:
        return {"score": 0.0, "pct": 0.0, "hard_gate": gate, "z": None,
                "regime": regime, "regime_tilt": round(t, 4),
                "submetrics": {k: round(float(sub.get(k, 0.0)), 4)
                               for k in ("O", "E", "K", "Mk", "Fr", "Q", "RS")},
                "dip_min": round(dip_min, 3),
                "version": WOLFSCORE_MR_VERSION, "trained": False}

    # HARD GATES — no dip = no trade (O gates the whole thesis); knife / crash /
    # cost / dying-coin each return 0 immediately.
    if O < dip_min:  return _mr_zero("no_dip")
    if K < -0.5:     return _mr_zero("falling_knife")
    if Mk < -0.6:    return _mr_zero("market_crashing")
    if Fr > 0.5:     return _mr_zero("too_expensive")
    if Q < -0.5:     return _mr_zero("dying_coin")

    thesis = wE * E + wK * K + wMk * Mk + wQ * Q + wRS * RS - wFr * Fr
    _og = min(1.0, O)
    z = b0 + _og * thesis
    p = 1.0 / (1.0 + _m.exp(-max(-30.0, min(30.0, z))))
    contributions = {
        "E":  round(_og * wE * E, 3),  "K":  round(_og * wK * K, 3),
        "Mk": round(_og * wMk * Mk, 3), "Q":  round(_og * wQ * Q, 3),
        "RS": round(_og * wRS * RS, 3), "Fr": round(-_og * wFr * Fr, 3),
        "O":  round(O, 3),
    }
    top = sorted(contributions.items(), key=lambda kv: abs(kv[1]), reverse=True)[:6]
    return {
        "score": round(p * 100.0, 1), "pct": round(p * 100.0, 1),
        "probability": round(p, 4), "z": round(z, 4), "hard_gate": None,
        "regime": regime, "regime_tilt": round(t, 4), "dip_min": round(dip_min, 3),
        "submetrics": {k: round(float(sub.get(k, 0.0)), 4)
                       for k in ("O", "E", "K", "Mk", "Fr", "Q", "RS")},
        "contributions": contributions,
        "top_reasons": [{"feature": k, "points": v} for k, v in top],
        "version": WOLFSCORE_MR_VERSION, "trained": False,
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
    """Fit the 9 WolfScore weights via REALIZED-R-WEIGHTED logistic regression on
    stored (submetrics, regime_tilt, label, realized_r, ts) rows. Friction-gated
    samples (F>0.5) are dropped (they'd be hard-rejected in production).

    S4-1.2 — samples are weighted by |realized_R| so the model optimizes for
    PROFITABILITY, not hit-rate (a +0.05R scratch and a +1.6R win are both wins
    but must not count equally).
    S4-2 — the held-out split is TEMPORAL: rows are sorted oldest→newest and the
    model trains on the older 80%, validated on the most-recent 20% it never saw.
    The report includes reliability bins (predicted bucket → actual win-rate +
    avg realized R) so overfit is visible: if in-sample is great but held-out
    isn't calibrated, DON'T activate.

    Returns a wolfscore_v3 model version (NOT activated) + the report."""
    if min_clean is None:
        min_clean = config_min_clean()
    # (features, label, weight, realized_r, ts)
    rows: List[Tuple[List[float], int, float, Optional[float], float]] = []
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
        rr = s.get("realized_r")
        try:
            rr = float(rr) if rr is not None else None
        except (TypeError, ValueError):
            rr = None
        try:
            ts = float(s.get("ts") or 0.0)
        except (TypeError, ValueError):
            ts = 0.0
        rows.append((_wolf_derived_features(sub, tilt), label,
                     _realized_r_weight(rr), rr, ts))

    n = len(rows)
    if n < max(20, int(min_clean)):
        return {"ok": False, "trained": False, "n": n, "min_clean": int(min_clean),
                "error": f"insufficient clean samples ({n} < {min_clean})"}
    # S4-2 — chronological order (oldest first) for an honest temporal holdout.
    rows.sort(key=lambda r: r[4])
    cut = int(n * 0.8)
    train_rows, test_rows = rows[:cut], rows[cut:]
    Xtr = [r[0] for r in train_rows]
    ytr = [r[1] for r in train_rows]
    swtr = [r[2] for r in train_rows]
    w, b = _fit_logistic(Xtr, ytr, sample_weight=swtr)
    weights = {"b0": round(b, 6)}
    for i, name in enumerate(WOLF_WEIGHT_ORDER):
        weights[name] = round(w[i], 6)

    def _p(x):
        return _sigmoid(b + sum(w[i] * x[i] for i in range(len(x))))

    def _calib(rws):
        if not rws:
            return {"n": 0}
        correct = pos = 0
        brier = 0.0
        # reliability bins by predicted probability → actual win-rate + avg R.
        edges = [0.0, 0.4, 0.55, 0.7, 1.01]
        names = ["0-40", "40-55", "55-70", "70-100"]
        bins = {nm: {"n": 0, "wins": 0, "r_sum": 0.0, "r_n": 0} for nm in names}
        for x, lbl, _sw, rr, _ts in rws:
            p = _p(x)
            brier += (p - lbl) ** 2
            if (p >= 0.5) == (lbl == 1):
                correct += 1
            pos += lbl
            pc = p * 100.0
            for j in range(len(names)):
                if edges[j] * 100.0 <= pc < edges[j + 1] * 100.0:
                    bk = bins[names[j]]
                    bk["n"] += 1
                    bk["wins"] += lbl
                    if rr is not None:
                        bk["r_sum"] += rr
                        bk["r_n"] += 1
                    break
        k = len(rws)
        reliability = {}
        for nm, bk in bins.items():
            if bk["n"]:
                reliability[nm] = {
                    "n": bk["n"],
                    "actual_win_rate": round(bk["wins"] / bk["n"], 4),
                    "avg_r": round(bk["r_sum"] / bk["r_n"], 4) if bk["r_n"] else None,
                }
        return {"n": k, "accuracy": round(correct / k, 4),
                "win_rate": round(pos / k, 4), "brier": round(brier / k, 4),
                "reliability": reliability}

    # S4-1.4 — learned vs interim weight delta (which terms the data moved).
    interim = WOLF_INTERIM["weights"]
    delta = {nm: {"interim": interim.get(nm), "trained": weights.get(nm),
                  "delta": round((weights.get(nm, 0.0) or 0.0)
                                 - (interim.get(nm, 0.0) or 0.0), 4)}
             for nm in WOLF_WEIGHT_ORDER}

    model = {
        "version": f"wolf-trained-{int(time.time())}", "kind": "wolfscore_v3",
        "trained": True, "weights": weights, "n_trades": n,
        "created": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()),
        "note": "R-weighted WolfScore v3 fit on live+paper outcomes (temporal holdout)",
        "report": {"n_total": n, "n_train": cut, "n_test": n - cut,
                   "r_weighted": True, "holdout": "temporal(last 20%)",
                   "in_sample": _calib(train_rows), "held_out": _calib(test_rows),
                   "weights_vs_interim": delta},
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


# ═══════════════════════════════════════════════════════════════════════════
# WolfScore-P5 — calibrated-probability MLP buy score (wolf-p5-v1)
# ═══════════════════════════════════════════════════════════════════════════
# A pre-trained 63→24→1 MLP (two heads: P(win24), P(trap)) with Platt calibration,
# validated on 365d × 90 coins of real 5m Binance data (walk-forward + adversarial
# suite). This module ships the INFERENCE side only — training happens off-box on
# the sandbox PC (p5.py / p8.py) and produces wolf_p5_model.json, which is loaded
# here once at startup. Pure python (no numpy) to keep the live bot dependency-light.
#
# FEAT()/expand() below are a VERBATIM port of sandbox p5.py (the exact functions
# the model was trained on) — feature names, order, coefficients, and the 63-dim
# expansion (10 squares + 12 pairs) must match the artifact or every score is
# meaningless. compute_features_p5() is FEAT() re-parameterised to read per-coin
# arrays (assembled by trade_engine._wolf_inputs_p5) instead of the sandbox's
# global numpy arrays; the arithmetic is identical.
import os as _os_p5

P5_VERSION = "wolf-p5-v1"

# FN3 from p5.py — the 41 features, IN ORDER (model input dims 0..40).
P5_FN = ['dip', 'rsi5', 'rsi15', 'rsi1h', 'rsi4h', 'pos_rng', 'bb', 'tr15', 'tr1', 'tr4',
         'sl15', 'sl1', 'sl4', 'align', 'stack', 'volsurge', 'cvd', 'cvd_div', 'atrpn', 'atr_exp',
         'btc', 'rs', 'wick', 'redrun', 'vwapd', 'fr', 'ret24', 'ret3d', 'dhigh', 'bre', 'bslope', 'btcma',
         'ret15', 'ret1h', 'low1h', 'hl2', 'coil', 'codip', 'btc15', 'hsin', 'hcos']
P5_IX = {k: j for j, k in enumerate(P5_FN)}
# KEY (squares) and PAIRS from p5.py — the 10 + 12 = 22 interaction dims → 63 total.
P5_KEY = ['tr4', 'tr1', 'dip', 'align', 'stack', 'rsi5', 'cvd_div', 'bre', 'ret3d', 'dhigh']
P5_PAIRS = [('tr4', 'dip'), ('tr1', 'rsi5'), ('stack', 'dip'), ('align', 'volsurge'), ('btc', 'dip'),
            ('atrpn', 'fr'), ('bre', 'dip'), ('btcma', 'dip'), ('bre', 'atrpn'), ('ret3d', 'dip'),
            ('codip', 'dip'), ('low1h', 'volsurge')]

# Fees the model trained on (p5.py: FEE=0.001, SPREAD=0.0005, RT=2*FEE+SPREAD). The
# `fr` friction feature uses this exact round-trip constant — do NOT swap in the live
# per-coin fee here or the feature drifts from what the model saw.
_P5_FEE = 0.001
_P5_SPREAD = 0.0005
_P5_RT = 2 * _P5_FEE + _P5_SPREAD

_p5_model: Optional[Dict[str, Any]] = None
_p5_load_error: Optional[str] = None


def _p5_clip(x, a, b):
    return a if x < a else (b if x > b else x)


def _p5_mean(x):
    x = list(x)
    return sum(x) / len(x) if x else 0.0


def load_p5_model(path: Optional[str] = None) -> Dict[str, Any]:
    """Load + validate wolf_p5_model.json once. Validates version and that the
    feature list/order matches P5_FN exactly (a mismatch would silently corrupt
    every score). Raises on any structural problem — the caller decides whether a
    missing/invalid model disables the p5 formula."""
    global _p5_model, _p5_load_error
    if path is None:
        path = _os_p5.path.join(_os_p5.path.dirname(_os_p5.path.abspath(__file__)),
                                "wolf_p5_model.json")
    with open(path, "r") as f:
        m = json.load(f)
    if m.get("version") != P5_VERSION:
        raise ValueError(f"p5 model version {m.get('version')!r} != {P5_VERSION!r}")
    if list(m.get("features") or []) != P5_FN:
        raise ValueError("p5 model feature names/order do not match P5_FN")
    if list(m.get("squares") or []) != P5_KEY:
        raise ValueError("p5 model squares do not match P5_KEY")
    if [list(p) for p in (m.get("pairs") or [])] != [list(p) for p in P5_PAIRS]:
        raise ValueError("p5 model pairs do not match P5_PAIRS")
    for head in ("head_win24", "head_trap"):
        h = m.get(head) or {}
        W1 = h.get("W1") or []
        if len(W1) != 63 or (W1 and len(W1[0]) != len(h.get("b1") or [])):
            raise ValueError(f"p5 model {head} W1 shape invalid")
    _p5_model = m
    _p5_load_error = None
    return m


def ensure_p5_loaded(path: Optional[str] = None) -> bool:
    """Idempotent load. Returns True when a valid model is in memory, else records
    the error (p5 scoring then returns None → the buy path fails closed)."""
    global _p5_load_error
    if _p5_model is not None:
        return True
    try:
        load_p5_model(path)
        return True
    except Exception as e:
        _p5_load_error = f"{type(e).__name__}: {e}"
        return False


def p5_model_status() -> Dict[str, Any]:
    if _p5_model is None:
        return {"loaded": False, "version": None, "error": _p5_load_error}
    return {"loaded": True, "version": _p5_model.get("version"),
            "trained": _p5_model.get("trained"), "config": _p5_model.get("config", {}),
            "n_features": len(_p5_model.get("features", []))}


def p5_config() -> Dict[str, Any]:
    """The model's own config block (thresholds, gates, pacing) — the defaults the
    artifact was validated with. strategy.entries.p5_* override these live."""
    return (_p5_model or {}).get("config", {}) or {}


# ── FEAT helpers (verbatim from p5.py) ─────────────────────────────────────────
def _p5_rsi(cl, i, n=14):
    up = dn = 0.0
    for j in range(i - n, i):
        d = cl[j] - cl[j - 1]
        if d > 0:
            up += d
        else:
            dn -= d
    if up + dn == 0:
        return 0.0
    return (up / (up + dn)) * 2 - 1


def _p5_htf(cl, i, mult, n):
    out = []
    j = i
    for _ in range(n):
        if j - mult < 0:
            break
        out.append(cl[j - 1])
        j -= mult
    return out[::-1]


def compute_features_p5(a: Dict[str, Any]) -> Optional[Dict[str, float]]:
    """VERBATIM port of p5.py FEAT(), reading per-coin arrays from `a` instead of
    the sandbox globals. `a` carries:
      op,h,l,c,v,tb : per-coin OHLCV + taker-buy-volume lists (index space shared
                      with `btc`); i : index of the last CLOSED bar to score.
      btc : BTC close list; tsms : open-time ms of bar i; hi30 : 30d high at i;
      btc_sma576 : BTC 576-bar (2d) SMA at i; fv : first-valid index (warmup base);
      cx : (med, btilt, bre, bslope, codip) cohort/macro context for bar i.
    Returns the 41-feature dict (+ '_atrp' and '_dhigh_ratio' helpers) or None when
    the coin is inside its 320-bar warmup / lacks HTF depth. Never raises."""
    try:
        med, btilt, bre_, bslope, codip = a["cx"]
        cl = a["c"]; hi = a["h"]; lo = a["l"]; vo = a["v"]; tb = a["tb"]; op = a["op"]
        btc = a.get("btc") or cl   # only used as fallback when btc_last/3ago absent
        i = int(a["i"])
        fv = int(a.get("fv", 0) or 0)
        if i < fv + 320 or i < 320 or cl[i] == 0:
            return None
        px = cl[i]
        m20 = _p5_mean(cl[i - 20:i])
        sd20 = (_p5_mean([(x - m20) ** 2 for x in cl[i - 20:i]])) ** 0.5 or 1e-9
        h15 = _p5_htf(cl, i, 3, 30); h1 = _p5_htf(cl, i, 12, 26); h4 = _p5_htf(cl, i, 48, 13)
        if len(h15) < 25 or len(h1) < 21 or len(h4) < 9:
            return None
        ma15 = _p5_mean(h15[-20:]); ma1 = _p5_mean(h1[-20:]); ma4 = _p5_mean(h4[-8:])
        trs = [max(hi[j] - lo[j], abs(hi[j] - cl[j - 1]), abs(lo[j] - cl[j - 1])) for j in range(i - 14, i)]
        atr = _p5_mean(trs); atrp = atr / px if px else 0
        trs_o = [max(hi[j] - lo[j], abs(hi[j] - cl[j - 1]), abs(lo[j] - cl[j - 1])) for j in range(i - 40, i - 14)]
        atr_o = _p5_mean(trs_o) or 1e-9
        vs = _p5_mean(vo[i - 20:i]) or 1e-9; v5 = _p5_mean(vo[i - 5:i])
        cvd5 = sum(2 * tb[j] - vo[j] for j in range(i - 5, i)); tv5 = sum(vo[i - 5:i]) or 1e-9
        cvdp_ = sum(2 * tb[j] - vo[j] for j in range(i - 10, i - 5)); tvp = sum(vo[i - 10:i - 5]) or 1e-9
        ret5 = px / (cl[i - 5] or px) - 1
        cvdn = cvd5 / tv5; cvdp = cvdp_ / tvp
        hh = max(hi[i - 48:i]); ll = min(lo[i - 48:i]); rng = (hh - ll) or 1e-9
        vwv = sum(vo[i - 20:i]) or 1e-9
        vwap = sum(cl[j] * vo[j] for j in range(i - 20, i)) / vwv
        red = 0
        for j in range(i - 1, i - 7, -1):
            if cl[j] < op[j]:
                red += 1
            else:
                break
        r1 = (hi[i - 1] - lo[i - 1]) or 1e-9
        f: Dict[str, float] = {}
        f['dip'] = _p5_clip((m20 - px) / (2 * sd20), -1, 1); f['rsi5'] = _p5_rsi(cl, i, 14)
        f['rsi15'] = _p5_rsi(h15, len(h15), 14) if len(h15) > 15 else 0
        f['rsi1h'] = _p5_rsi(h1, len(h1), 14) if len(h1) > 15 else 0
        f['rsi4h'] = _p5_rsi(h4, len(h4), 8) if len(h4) > 9 else 0
        f['pos_rng'] = _p5_clip((px - ll) / rng * 2 - 1, -1, 1); f['bb'] = _p5_clip((px - m20) / (2 * sd20), -1, 1)
        f['tr15'] = _p5_clip((px / ma15 - 1) * 40, -1, 1); f['tr1'] = _p5_clip((px / ma1 - 1) * 25, -1, 1)
        f['tr4'] = _p5_clip((px / ma4 - 1) * 15, -1, 1)
        f['sl15'] = _p5_clip((h15[-1] / h15[-5] - 1) * 80, -1, 1) if h15[-5] else 0
        f['sl1'] = _p5_clip((h1[-1] / h1[-6] - 1) * 60, -1, 1) if h1[-6] else 0
        f['sl4'] = _p5_clip((h4[-1] / h4[-4] - 1) * 20, -1, 1) if h4[-4] else 0
        f['align'] = _p5_clip((f['tr15'] + f['tr1'] + f['tr4']) / 3, -1, 1)
        f['stack'] = _p5_clip((1 if f['tr1'] > 0 else -1) * 0.4 + (1 if f['tr4'] > 0 else -1) * 0.4 + (1 if f['sl4'] > 0 else -1) * 0.2, -1, 1)
        f['volsurge'] = _p5_clip(v5 / vs - 1, -1, 2) / 2; f['cvd'] = _p5_clip(cvdn, -1, 1)
        f['cvd_div'] = _p5_clip((cvdn - cvdp) * 2 - ret5 * 80, -1, 1)
        f['atrpn'] = _p5_clip(atrp * 150, 0, 1); f['atr_exp'] = _p5_clip(atr / atr_o - 1, -1, 1)
        f['btc'] = btilt
        f['rs'] = math.tanh(200 * ((cl[i - 1] / cl[i - 4] - 1) - med)) if cl[i - 4] else 0
        f['wick'] = _p5_clip(((min(cl[i - 1], op[i - 1]) - lo[i - 1]) / r1) * 2 - 1, -1, 1)
        f['redrun'] = _p5_clip(red / 3.0 - 1, -1, 1)
        f['vwapd'] = _p5_clip((px - vwap) / vwap * 100, -1, 1)
        f['fr'] = _p5_clip(_P5_RT / max(atrp, 1e-6), 0, 1); f['_atrp'] = atrp
        f['ret24'] = _p5_clip((px / cl[i - 288] - 1) * 8, -1, 1) if i >= 288 and cl[i - 288] else 0.0
        f['ret3d'] = _p5_clip((px / cl[i - 864] - 1) * 5, -1, 1) if i >= 864 and cl[i - 864] else 0.0
        _hi30 = float(a.get("hi30") or 0.0) or px
        f['dhigh'] = _p5_clip((px / _hi30 - 1) * 4, -1, 0)
        f['bre'] = bre_ * 2 - 1
        f['bslope'] = _p5_clip(bslope * 4, -1, 1)
        _btc_sma576 = float(a.get("btc_sma576") or 0.0) or px
        # BTC "current"/"3-bar-ago" close. In the sandbox all coins share one global
        # bar index so btc[i] is valid; LIVE, per-coin arrays are NOT index-aligned
        # with BTC's own array, so the caller passes btc_last/btc_3ago explicitly.
        # Fall back to btc[i]/btc[i-3] (aligned-index path, e.g. the port test).
        _btc_last = a.get("btc_last")
        _btc_3ago = a.get("btc_3ago")
        _btc_last = float(_btc_last) if _btc_last is not None else float(btc[i])
        _btc_3ago = float(_btc_3ago) if _btc_3ago is not None else float(btc[i - 3])
        f['btcma'] = _p5_clip((_btc_last / _btc_sma576 - 1) * 10, -1, 1)
        f['ret15'] = _p5_clip((px / cl[i - 3] - 1) * 100, -1, 1) if cl[i - 3] else 0.0
        f['ret1h'] = _p5_clip((px / cl[i - 12] - 1) * 60, -1, 1) if cl[i - 12] else 0.0
        l1 = min(lo[i - 12:i]) or 1e-9
        f['low1h'] = _p5_clip((px / l1 - 1) * 50, 0, 1)
        hl = sum(1 for j in range(i - 5, i) if lo[j] > lo[j - 1])
        f['hl2'] = hl / 5.0 * 2 - 1
        tr5 = _p5_mean(trs[-5:]); tr20 = _p5_mean(trs[-14:] + trs_o[-6:]) or 1e-9
        f['coil'] = _p5_clip(tr5 / tr20 - 1, -1, 1)
        f['codip'] = codip * 2 - 1
        f['btc15'] = _p5_clip((_btc_last / _btc_3ago - 1) * 150, -1, 1) if _btc_3ago else 0.0
        hr = (int(a.get("tsms") or 0) // 3600000) % 24
        f['hsin'] = math.sin(2 * math.pi * hr / 24.0)
        f['hcos'] = math.cos(2 * math.pi * hr / 24.0)
        f['_dhigh_ratio'] = (px / _hi30) if _hi30 else 1.0
        return f
    except Exception:
        return None


def _p5_expand(x: List[float]) -> List[float]:
    """p5.py expand(): 41 base features → 63 (append 10 squares of KEY, then 12
    products of PAIRS). Order is load-bearing (matches the trained W1 rows)."""
    e = list(x)
    for k in P5_KEY:
        e.append(x[P5_IX[k]] * x[P5_IX[k]])
    for aa, bb in P5_PAIRS:
        e.append(x[P5_IX[aa]] * x[P5_IX[bb]])
    return e


def _p5_sigmoid(z: float) -> float:
    if z < -30.0:
        z = -30.0
    elif z > 30.0:
        z = 30.0
    return 1.0 / (1.0 + math.exp(-z))


def _p5_head_prob(head: Dict[str, Any], e: List[float]) -> float:
    """Forward pass of one MLP head + Platt: relu(e·W1+b1)·W2+b2 → sigmoid → Platt.
    Mirrors p5.py mlp_prob() + platt_apply() exactly, in pure python."""
    W1 = head["W1"]; b1 = head["b1"]; W2 = head["W2"]; b2 = head["b2"]
    a_pl, b_pl = head["platt"]
    hid = len(b1)
    z = b2
    for j in range(hid):
        s = b1[j]
        for k in range(63):
            s += e[k] * W1[k][j]
        if s > 0.0:                      # relu
            z += s * W2[j]
    p_raw = _p5_sigmoid(z)
    p_raw = min(1.0 - 1e-6, max(1e-6, p_raw))
    logit = math.log(p_raw / (1.0 - p_raw))
    return _p5_sigmoid(a_pl * logit + b_pl)


def p5_infer(feat_vec: List[float]) -> Tuple[float, float, float]:
    """(p_win24, p_trap, ev) from a 41-feature vector. EV per p5.py: pW·win_mean +
    pT·trap_mean + max(0,1-pW-pT)·mid_mean, using the artifact's class_means."""
    e = _p5_expand(feat_vec)
    pw = _p5_head_prob(_p5_model["head_win24"], e)
    pt = _p5_head_prob(_p5_model["head_trap"], e)
    cm = _p5_model.get("class_means", {}) or {}
    pm = max(0.0, 1.0 - pw - pt)
    ev = pw * float(cm.get("win", 0.0)) + pt * float(cm.get("trap", 0.0)) + pm * float(cm.get("mid", 0.0))
    return pw, pt, ev


def wolfscore_p5(feat: Optional[Dict[str, float]], cfg: Optional[Dict[str, Any]] = None,
                 macro_bear: bool = False) -> Dict[str, Any]:
    """P5 publish payload for one coin (EvScorePanel-compatible). ALWAYS returns a
    real pct for any coin that produced features (no zeroing — unlike MR); `gated`
    is metadata that blocks BUYING, not the score display:
        gated ∈ {'warmup','universe','friction','macro_bear',''}
    Precedence: warmup > universe > friction > macro_bear. cfg overrides the model's
    universe_dhigh_min / friction_max. Never raises."""
    _mc = p5_config()
    _cfg = cfg or {}
    dhigh_min = float(_cfg.get("universe_dhigh_min", _mc.get("universe_dhigh_min", 0.70)))
    fr_max = float(_cfg.get("friction_max", _mc.get("friction_max", 0.60)))
    # NOTE: pct is ALWAYS numeric (never None) — the rest of the engine (tiered
    # scorer, aggregation, /api feeds) does float(pct) arithmetic and MR's gated
    # coins returned 0.0, not None. A None here threw 'float argument must be a
    # real number, not NoneType' on every scan → crash loop. Warmup/error → 0.0
    # (still gated + below threshold + trap/ev fail → never buyable).
    if feat is None:
        return {"pct": 0.0, "score": 0.0, "p_trap": 1.0, "ev": -1.0,
                "gated": "warmup", "hard_gate": "warmup", "version": P5_VERSION,
                "trained": True, "submetrics": {}}
    if _p5_model is None:
        return {"pct": 0.0, "score": 0.0, "p_trap": 1.0, "ev": -1.0,
                "gated": "warmup", "hard_gate": "warmup",
                "version": P5_VERSION, "trained": False, "submetrics": {},
                "note": "p5 model not loaded"}
    try:
        vec = [float(feat.get(k, 0.0)) for k in P5_FN]
        pw, pt, ev = p5_infer(vec)
        pct = 100.0 * pw
        gated = ""
        dr = feat.get("_dhigh_ratio")
        if dr is not None and float(dr) < dhigh_min:
            gated = "universe"
        elif float(feat.get("fr", 0.0)) > fr_max:
            gated = "friction"
        elif macro_bear:
            gated = "macro_bear"
        return {
            "pct": round(pct, 1), "score": round(pct, 1),
            "p_trap": round(pt, 4), "ev": round(ev, 6),
            "probability": round(pw, 4),
            "gated": gated, "hard_gate": (gated or None),
            "version": P5_VERSION, "trained": True,
            "submetrics": {
                "score": round(pct, 1), "p_trap": round(pt, 4), "ev": round(ev, 6),
                "dip": round(float(feat.get("dip", 0.0)), 4),
                "align": round(float(feat.get("align", 0.0)), 4),
                "bre": round(float(feat.get("bre", 0.0)), 4),
                "fr": round(float(feat.get("fr", 0.0)), 4),
                "dhigh": round(float(feat.get("dhigh", 0.0)), 4),
                "cvd_div": round(float(feat.get("cvd_div", 0.0)), 4),
                "rs": round(float(feat.get("rs", 0.0)), 4),
            },
            "_atrp": feat.get("_atrp"),
        }
    except Exception as e:
        return {"pct": 0.0, "score": 0.0, "p_trap": 1.0, "ev": -1.0,
                "gated": "warmup", "hard_gate": "warmup",
                "version": P5_VERSION, "trained": True, "submetrics": {},
                "note": f"p5 infer error: {type(e).__name__}: {e}"}


# ═══════════════════════════════════════════════════════════════════════════
# WolfScore-R — dual-head volume/quality ensemble (wolf-r-v1)
# ═══════════════════════════════════════════════════════════════════════════
# A 3-member ensemble; each member has two heads: head_win4h (P the sell engine
# closes this buy in profit within 4h) and head_freeze3d (P still underwater 3d
# later). pW = MEAN of the 3 win4h heads; pZ = MAX of the 3 freeze3d heads
# (worst-case veto). 26 features, feature windows reach 288 bars (warmup 620) —
# no deep aggregates. Ported VERBATIM from sandbox rlib.py (feats_coin v2=False,
# mlp_prob, platt_apply); numpy float32 so the live pipeline matches the sandbox
# reference to < 1e-6 (the equivalence gate). numpy is REQUIRED for this engine —
# if it (or the artifact) is missing, every coin gates 'model_missing' (fail loud).
try:
    import numpy as _rnp
    from numpy.lib.stride_tricks import sliding_window_view as _r_swv
    _R_NUMPY = True
except Exception:
    _rnp = None
    _R_NUMPY = False

R_VERSION = "wolf-r-v1"
R_WARMUP_BARS = 620
R_FEATURES = ['r5', 'r15', 'r1h', 'r4h', 'r24h', 'accel15', 'pullbk', 'lowsl1h', 'volr',
              'vburst', 'tb5', 'tb1h', 'tbshift', 'dollv', 'atrp', 'coil', 'rngexp', 'dhigh24',
              'pos24', 'rs1h', 'rs24h', 'breadth', 'btc15', 'btc1h', 'hsin', 'hcos']
_R_RT = 0.0020   # rlib RT_DEF — only used for the (kept) friction gate, not scoring

_r_model = None
_r_load_error = None


# ── rlib helpers (VERBATIM) ────────────────────────────────────────────────────
def _r_sh(x, k):
    o = _rnp.full(len(x), _rnp.nan, _rnp.float32); o[k:] = x[:-k]; return o


def _r_rmean(x, w):
    cs = _rnp.cumsum(_rnp.insert(x.astype(_rnp.float64), 0, 0.0))
    o = _rnp.full(len(x), _rnp.nan, _rnp.float32); o[w - 1:] = ((cs[w:] - cs[:-w]) / w).astype(_rnp.float32); return o


def _r_rsum(x, w):
    cs = _rnp.cumsum(_rnp.insert(x.astype(_rnp.float64), 0, 0.0))
    o = _rnp.full(len(x), _rnp.nan, _rnp.float32); o[w - 1:] = (cs[w:] - cs[:-w]).astype(_rnp.float32); return o


def _r_rmax(x, w):
    o = _rnp.full(len(x), _rnp.nan, _rnp.float32); o[w - 1:] = _r_swv(_rnp.ascontiguousarray(x), w).max(axis=1); return o


def _r_rmin(x, w):
    o = _rnp.full(len(x), _rnp.nan, _rnp.float32); o[w - 1:] = _r_swv(_rnp.ascontiguousarray(x), w).min(axis=1); return o


def load_r_model(path=None):
    """Load + validate wolf_r_model.json. Strict: version, 26-feature order, 3
    members × 2 heads with the exact shapes. Raises on any mismatch."""
    global _r_model, _r_load_error
    if not _R_NUMPY:
        raise RuntimeError("numpy unavailable — WolfScore-R requires numpy")
    if path is None:
        path = _os_p5.path.join(_os_p5.path.dirname(_os_p5.path.abspath(__file__)),
                                "wolf_r_model.json")
    with open(path, "r") as f:
        m = json.load(f)
    if m.get("version") != R_VERSION:
        raise ValueError(f"r model version {m.get('version')!r} != {R_VERSION!r}")
    if list(m.get("features") or []) != R_FEATURES:
        raise ValueError("r model feature names/order do not match R_FEATURES")
    mem = m.get("members") or []
    if len(mem) != 3:
        raise ValueError(f"r model expects 3 members, got {len(mem)}")
    hid = int(m.get("hidden", 16))
    for mi, member in enumerate(mem):
        for hk in ("head_win4h", "head_freeze3d"):
            h = member.get(hk) or {}
            for key, shp in (("mu", (26,)), ("sd", (26,)), ("W1", (26, hid)),
                             ("b1", (hid,)), ("W2", (hid,)), ("platt", (2,))):
                a = _rnp.asarray(h.get(key), dtype=_rnp.float32)
                if tuple(a.shape) != shp:
                    raise ValueError(f"r model member {mi} {hk}.{key} shape {a.shape} != {shp}")
        # pre-cast the arrays once for fast inference
        for hk in ("head_win4h", "head_freeze3d"):
            h = member[hk]
            h["_mu"] = _rnp.asarray(h["mu"], _rnp.float32)
            h["_sd"] = _rnp.asarray(h["sd"], _rnp.float32)
            h["_W1"] = _rnp.asarray(h["W1"], _rnp.float32)
            h["_b1"] = _rnp.asarray(h["b1"], _rnp.float32)
            h["_W2"] = _rnp.asarray(h["W2"], _rnp.float32)
            h["_b2"] = float(h["b2"])
            h["_platt"] = (float(h["platt"][0]), float(h["platt"][1]))
    _r_model = m
    _r_load_error = None
    return m


def ensure_r_loaded(path=None):
    global _r_load_error
    if _r_model is not None:
        return True
    if not _R_NUMPY:
        _r_load_error = "numpy unavailable"
        return False
    try:
        load_r_model(path)
        return True
    except Exception as e:
        _r_load_error = f"{type(e).__name__}: {e}"
        return False


def r_model_status():
    if _r_model is None:
        return {"loaded": False, "version": None, "error": _r_load_error, "numpy": _R_NUMPY}
    return {"loaded": True, "version": _r_model.get("version"), "numpy": _R_NUMPY,
            "hidden": _r_model.get("hidden"), "warmup_bars": _r_model.get("warmup_bars"),
            "members": len(_r_model.get("members", [])),
            "gate": _r_model.get("gate"), "pacing": _r_model.get("pacing")}


def compute_features_r(a, med1h, med24, breadth1h, btc15, btc1h, hs_last, hc_last):
    """VERBATIM port of rlib.feats_coin (v2=False, 26 features) → the LAST-bar
    26-vector for `a` (numpy (N,6) float32 [o,h,l,c,v,tb]). Cross-sectional
    (rs1h/rs24h/breadth) + BTC + hour come in as this-scan scalars (only the last
    row is read). Returns (x[26] cleaned, atr_last) or (None, None) if < warmup."""
    if not _R_NUMPY:
        return None, None
    try:
        a = _rnp.asarray(a, dtype=_rnp.float32)
        N = a.shape[0]
        if N < R_WARMUP_BARS:
            return None, None
        hi = a[:, 1]; lo = a[:, 2]; cl = a[:, 3]; vo = a[:, 4]; tb = a[:, 5]
        pc = _r_sh(cl, 1)
        tr = _rnp.maximum(hi - lo, _rnp.maximum(_rnp.abs(hi - pc), _rnp.abs(lo - pc)))
        tr = _rnp.where(_rnp.isfinite(tr), tr, hi - lo)
        atr = _r_rmean(tr, 14)
        v288 = _r_rmean(vo, 288)
        tbsh = _rnp.where(vo > 0, tb / _rnp.maximum(vo, 1e-12), _rnp.nan)
        tb1h = _r_rsum(tb, 12) / _rnp.maximum(_r_rsum(vo, 12), 1e-12)
        tbsh288 = _r_rmean(_rnp.nan_to_num(tbsh, nan=0.5), 288)
        mx12 = _r_rmax(hi, 12); mn12 = _r_rmin(lo, 12)
        mx288 = _r_rmax(hi, 288); mn288 = _r_rmin(lo, 288)
        lo6 = _r_rmin(lo, 6)
        F = _rnp.empty((N, 26), _rnp.float32)
        F[:, 0] = cl / _r_sh(cl, 1) - 1
        F[:, 1] = cl / _r_sh(cl, 3) - 1
        F[:, 2] = cl / _r_sh(cl, 12) - 1
        F[:, 3] = cl / _r_sh(cl, 48) - 1
        F[:, 4] = cl / _r_sh(cl, 288) - 1
        F[:, 5] = F[:, 1] - (_r_sh(cl, 3) / _r_sh(cl, 6) - 1)
        F[:, 6] = (cl - mx12) / _rnp.maximum(mx12 - mn12, 1e-9)
        F[:, 7] = (lo6 - _r_sh(lo6, 6)) / _rnp.maximum(atr, 1e-9)
        F[:, 8] = vo / _rnp.maximum(v288, 1e-9)
        F[:, 9] = _r_rsum(vo, 3) / _rnp.maximum(3 * v288, 1e-9)
        F[:, 10] = tbsh
        F[:, 11] = tb1h
        F[:, 12] = tb1h - tbsh288
        F[:, 13] = _rnp.log10(_rnp.maximum(v288 * cl, 1e-9))
        F[:, 14] = atr / _rnp.maximum(cl, 1e-9)
        F[:, 15] = atr / _rnp.maximum(_r_sh(atr, 288), 1e-9)
        F[:, 16] = (hi - lo) / _rnp.maximum(atr, 1e-9)
        F[:, 17] = cl / _rnp.maximum(mx288, 1e-9) - 1
        F[:, 18] = (cl - mn288) / _rnp.maximum(mx288 - mn288, 1e-9)
        F[:, 19] = F[:, 2] - _rnp.float32(med1h)
        F[:, 20] = F[:, 4] - _rnp.float32(med24)
        F[:, 21] = _rnp.float32(breadth1h)
        F[:, 22] = _rnp.float32(btc15)
        F[:, 23] = _rnp.float32(btc1h)
        F[:, 24] = _rnp.float32(hs_last)
        F[:, 25] = _rnp.float32(hc_last)
        x = F[-1]
        x = _rnp.nan_to_num(_rnp.clip(x, -1e6, 1e6), nan=0.0)
        _atr_last = float(atr[-1]) if _rnp.isfinite(atr[-1]) else 0.0
        return x, _atr_last
    except Exception:
        return None, None


def _r_mlp_prob(head, x2d):
    """rlib.mlp_prob VERBATIM (single-sample). x2d: (1,26) float32."""
    Xn = _rnp.clip((x2d - head["_mu"]) / head["_sd"], -6, 6)
    h1 = _rnp.tanh(Xn @ head["_W1"] + head["_b1"])
    z = _rnp.clip(h1 @ head["_W2"] + head["_b2"], -30, 30)
    return 1.0 / (1.0 + _rnp.exp(-z))


def _r_platt_apply(ab, p):
    """rlib.platt_apply VERBATIM."""
    pc = _rnp.clip(_rnp.asarray(p, dtype=_rnp.float64), 1e-6, 1 - 1e-6)
    zz = _rnp.log(pc / (1 - pc))
    return 1.0 / (1.0 + _rnp.exp(-(ab[0] * zz + ab[1])))


def r_infer(feat_vec):
    """(pW, pZ) from a 26-feature vector. pW = mean of the 3 win4h heads (×100);
    pZ = MAX of the 3 freeze3d heads (×100). Matches rlib exactly."""
    x2d = _rnp.asarray(feat_vec, dtype=_rnp.float32).reshape(1, 26)
    wins = []
    frzs = []
    for member in _r_model["members"]:
        hw = member["head_win4h"]; hf = member["head_freeze3d"]
        pw = _r_platt_apply(hw["_platt"], _r_mlp_prob(hw, x2d))
        pz = _r_platt_apply(hf["_platt"], _r_mlp_prob(hf, x2d))
        wins.append(float(pw.reshape(-1)[0]))
        frzs.append(float(pz.reshape(-1)[0]))
    pW = 100.0 * (sum(wins) / len(wins))
    pZ = 100.0 * max(frzs)
    return pW, pZ


def wolfscore_r(feat_vec, atr_last=None, cfg=None, dhigh_ratio=None, macro_bear=False):
    """WolfScore-R publish payload for one coin. `pct`/`score` = pW (0-100, the UI
    score). Also carries pZ and a gate reason. Gates (kept from P5 + the R gate,
    precedence): warmup > model_missing > universe > friction > macro_bear >
    pz_veto > below_thr > '' (eligible). pct is ALWAYS numeric (never None)."""
    _cfg = cfg or {}
    pw_min = float(_cfg.get("pw_min", 55.0))
    pz_max = float(_cfg.get("pz_max", 2.4))
    fr_max = float(_cfg.get("friction_max", 0.60))
    # friction_gate: when False, friction is still COMPUTED and reported in
    # submetrics but is NOT a hard gate. The sandbox (rlib.py) that produced the
    # volume baseline never hard-gates on friction — it subtracts the round-trip
    # cost from returns and lets the (net-of-cost-trained) pW/pZ model decide, so
    # a live friction gate double-counts the fee. Default True preserves the
    # legacy P5/v3 behavior; wolf-r-volume passes False.
    friction_gate = bool(_cfg.get("friction_gate", True))
    dhigh_min = float(_cfg.get("universe_dhigh_min", 0.70))
    if feat_vec is None:
        return {"pct": 0.0, "score": 0.0, "pw": 0.0, "pz": 100.0, "ev": None,
                "gated": "warmup", "hard_gate": "warmup", "version": R_VERSION,
                "trained": True, "submetrics": {}}
    if _r_model is None or not _R_NUMPY:
        return {"pct": 0.0, "score": 0.0, "pw": 0.0, "pz": 100.0,
                "gated": "model_missing", "hard_gate": "model_missing",
                "version": R_VERSION, "trained": False, "submetrics": {},
                "note": _r_load_error or "r model not loaded"}
    try:
        pW, pZ = r_infer(feat_vec)
        atrp = float(feat_vec[14]) if feat_vec is not None else 0.0   # F14 = atr/close
        fr = (_R_RT / max(atrp, 1e-6)) if atrp else 1.0
        gated = ""
        if dhigh_ratio is not None and float(dhigh_ratio) < dhigh_min:
            gated = "universe"
        elif friction_gate and fr > fr_max:
            gated = "friction"
        elif macro_bear:
            gated = "macro_bear"
        elif pZ > pz_max:
            gated = "pz_veto"
        elif pW < pw_min:
            gated = "below_thr"
        return {
            "pct": round(pW, 1), "score": round(pW, 1),
            "pw": round(pW, 2), "pz": round(pZ, 2),
            "gated": gated, "hard_gate": (gated or None),
            "version": R_VERSION, "trained": True,
            "submetrics": {
                "pW": round(pW, 2), "pZ": round(pZ, 2),
                "atrp": round(atrp, 5), "friction": round(fr, 3),
                "r1h": round(float(feat_vec[2]), 5), "r24h": round(float(feat_vec[4]), 5),
                "dhigh24": round(float(feat_vec[17]), 5), "rs1h": round(float(feat_vec[19]), 5),
                "vburst": round(float(feat_vec[9]), 4), "breadth": round(float(feat_vec[21]), 4),
            },
            "_atr_last": atr_last,
        }
    except Exception as e:
        return {"pct": 0.0, "score": 0.0, "pw": 0.0, "pz": 100.0,
                "gated": "model_missing", "hard_gate": "model_missing",
                "version": R_VERSION, "trained": True, "submetrics": {},
                "note": f"r infer error: {type(e).__name__}: {e}"}
