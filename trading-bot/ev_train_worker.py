"""ev_train_worker.py — self-contained EV training entry for a SEPARATE process.

Phase 5 (cold-work isolation): the WolfScore logistic fit is CPU-heavy and holds
the Python GIL for seconds. Running it in the trader process stalled live trading
during a retrain. This worker is the target of a spawn ProcessPoolExecutor so the
fit runs on its OWN process/GIL/core and only the thin result crosses back.

It imports ONLY ev_model + database (never control_api), so a spawn child stays
light and does NOT re-run the FastAPI boot. It does NOT modify WolfScore math —
it just orchestrates the existing ev_model.train_wolfscore + save_version, reading
samples from the DB and writing the new (un-activated) weight version back to it.
"""

import time


def run_training_job(run_id: str) -> dict:
    """Load training samples from the DB, fit a new WolfScore weight-version, save
    (do NOT activate) it. Returns a THIN result dict (no model blob) so it pickles
    cheaply back to the parent process. Fully guarded — never raises."""
    started = time.time()
    result: dict = {"ok": False, "run_id": run_id, "status": "failed",
                    "started_ts": started}
    try:
        import ev_model as _ev
        import database

        samples = []
        rows_usable = 0
        try:
            rows = database.get_training_samples(
                limit=100000, modes=["live", "paper_shadow"]) or []
            for r in rows:
                if not isinstance(r, dict):
                    continue
                feats = r.get("features") if isinstance(r.get("features"), dict) else {}
                _sub = feats.get("submetrics")
                if isinstance(_sub, dict) and _sub:
                    rows_usable += 1
                samples.append({
                    "submetrics":  _sub,
                    "regime_tilt": feats.get("regime_tilt", 0.0),
                    "features":    feats,
                    "cohort":      feats.get("cohort"),
                    "label":       r.get("label"),
                    "realized_r":  r.get("realized_r"),
                    "ts":          r.get("ts"),
                })
        except Exception as e:
            samples = []
            result["load_error"] = f"{type(e).__name__}: {e}"

        rows_loaded = len(samples)
        try:
            database.log_activity(
                f"EV retrain {run_id} (subprocess): loaded {rows_loaded} rows "
                f"({rows_usable} with WolfScore submetrics)", "info")
        except Exception:
            pass

        _trainer = getattr(_ev, "train_wolfscore", None) or getattr(_ev, "train")
        res = _trainer(samples)
        if isinstance(res, dict):
            result.update(res)
            result["run_id"] = run_id
            result["started_ts"] = started
            result["n_samples"] = rows_loaded
            result["rows_loaded"] = rows_loaded
            result["rows_usable"] = rows_usable
            result["status"] = "done" if res.get("ok") else "failed"
            if not res.get("ok") and res.get("error"):
                result["error"] = res["error"]
            if res.get("ok") and isinstance(res.get("model"), dict):
                try:
                    vid = _ev.save_version(res["model"])
                    result["saved_version"] = vid
                    database.log_activity(
                        f"EV retrain {run_id} (subprocess): trained {vid} on "
                        f"{rows_loaded} rows", "info")
                except Exception as e:
                    result["save_error"] = f"{type(e).__name__}: {e}"
                    result["status"] = "failed"
    except Exception as e:
        result = {"ok": False, "run_id": run_id, "status": "failed",
                  "started_ts": started, "error": f"{type(e).__name__}: {e}"}
    result.pop("model", None)   # keep the returned dict thin (no weights blob)
    result["finished_ts"] = time.time()
    return result
