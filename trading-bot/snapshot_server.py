"""snapshot_server.py — read-only dashboard API ("Process B") for the WolfBot
process split.

The live trader (control_api.py) tees its already-computed /api/all payload to a
JSON snapshot file when SNAPSHOT_WRITER_ENABLED=1. THIS process serves the
dashboard from that file and NOTHING else — it never opens the SQLite DB, never
calls Binance, never imports trade_engine. Point the browser's polling at this
process (a different port) and heavy dashboard traffic can no longer contend with
the trader's DB lock or Binance weight budget.

It is intentionally standalone: the trader does NOT import this module, and this
module does NOT import the trader. The only coupling is the snapshot file on disk.

Run:
    SNAPSHOT_WRITER_ENABLED=1  (on the trader process — so the file gets written)
    python -m uvicorn snapshot_server:app --host 0.0.0.0 --port 8090
or simply:
    python snapshot_server.py            # honours SNAPSHOT_SERVER_PORT (default 8090)

Env:
    SNAPSHOT_PATH          override the snapshot file location
    SNAPSHOT_SERVER_PORT   listen port when run directly (default 8090)
    SNAPSHOT_STALE_SEC     age (s) past which /api/all is flagged stale (default 15)
"""

import json
import os
import time

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# Resolve the data dir exactly like config/database do, without importing the
# heavy trader modules. config is light (stdlib only) and exposes _DATA_DIR.
try:
    import config as _config
    _DATA_DIR = _config._DATA_DIR
except Exception:
    _DATA_DIR = os.getenv("DATA_DIR") or "/opt/tradebot/data"

SNAPSHOT_PATH = os.getenv("SNAPSHOT_PATH") or os.path.join(_DATA_DIR, "dashboard_snapshot.json")
STALE_SEC = float(os.getenv("SNAPSHOT_STALE_SEC", "15"))

app = FastAPI(title="WolfBot Snapshot Server (read-only)")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

# Tiny in-process coalescing cache so a burst of polls reads the file once.
_cache = {"ts": 0.0, "snap": None}
_CACHE_TTL = 0.5


def _read_snapshot() -> dict:
    """Return the parsed snapshot ({'snapshot_ts':.., 'payload':..}) or {} on any
    problem. Coalesced to at most one disk read per _CACHE_TTL."""
    now = time.time()
    if _cache["snap"] is not None and (now - _cache["ts"]) < _CACHE_TTL:
        return _cache["snap"]
    try:
        with open(SNAPSHOT_PATH) as fh:
            snap = json.load(fh)
        if not isinstance(snap, dict):
            snap = {}
    except Exception:
        snap = {}
    _cache["ts"] = now
    _cache["snap"] = snap
    return snap


@app.get("/api/all")
def api_all():
    """Serve the trader's last /api/all payload from the snapshot file. Adds
    _snapshot metadata so the frontend can show a 'stale data' banner if the
    trader stopped writing (e.g. trader down)."""
    snap = _read_snapshot()
    payload = snap.get("payload") if isinstance(snap.get("payload"), dict) else None
    ts = float(snap.get("snapshot_ts") or 0.0)
    age = round(time.time() - ts, 2) if ts else None
    if payload is None:
        return {
            "status": {"running": False, "live_error": "snapshot_unavailable"},
            "positions": [], "trades": [], "activity": [], "signals": [],
            "_snapshot": {"available": False, "age_sec": age, "stale": True,
                          "path": SNAPSHOT_PATH},
        }
    out = dict(payload)
    out["_snapshot"] = {
        "available": True,
        "age_sec": age,
        "stale": (age is not None and age > STALE_SEC),
        "served_by": "snapshot_server",
    }
    return out


@app.get("/api/snapshot")
def api_snapshot():
    """Introspection: file presence + freshness, as seen by Process B."""
    snap = _read_snapshot()
    ts = float(snap.get("snapshot_ts") or 0.0)
    exists = os.path.exists(SNAPSHOT_PATH)
    return {
        "path": SNAPSHOT_PATH,
        "exists": exists,
        "size_bytes": (os.path.getsize(SNAPSHOT_PATH) if exists else None),
        "snapshot_ts": ts or None,
        "age_sec": (round(time.time() - ts, 2) if ts else None),
        "stale_threshold_sec": STALE_SEC,
        "served_by": "snapshot_server",
    }


@app.get("/health")
def health():
    snap = _read_snapshot()
    ts = float(snap.get("snapshot_ts") or 0.0)
    age = (time.time() - ts) if ts else None
    ok = age is not None and age <= STALE_SEC
    return {"ok": ok, "age_sec": (round(age, 2) if age is not None else None)}


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("SNAPSHOT_SERVER_PORT", "8090"))
    uvicorn.run(app, host="0.0.0.0", port=port)
