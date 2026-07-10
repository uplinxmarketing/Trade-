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


def train(samples: List[dict], min_clean: int = _MIN_CLEAN_TRADES_DEFAULT
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


def model_status(min_clean: int = _MIN_CLEAN_TRADES_DEFAULT) -> Dict[str, Any]:
    """UI header: current model version, trained/untrained, and progress toward
    the clean-trade minimum (below which the floor is display-only, S4.3)."""
    m = get_active_model()
    n = int(m.get("n_trades", 0) or 0)
    return {
        "version": m.get("version", "interim-v1"),
        "trained": bool(m.get("trained", False)),
        "n_trades": n,
        "min_clean": int(min_clean),
        "floor_active": bool(m.get("trained", False) and n >= int(min_clean)),
        "note": m.get("note", ""),
    }
