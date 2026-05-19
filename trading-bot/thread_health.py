import time
import threading

_heartbeats: dict = {}
_lock = threading.Lock()

def heartbeat(thread_name: str):
    with _lock:
        _heartbeats[thread_name] = time.time()

def get_health() -> dict:
    now = time.time()
    with _lock:
        snap = dict(_heartbeats)
    result = {}
    for name, ts in snap.items():
        ago = round(now - ts, 1)
        result[name] = {
            "last_beat_ago_seconds": ago,
            "alive": ago < 60,
            "status": "alive" if ago < 60 else ("warn" if ago < 300 else "stuck"),
        }
    return result
