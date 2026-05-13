#!/usr/bin/env python3
"""Run this once to restore trade_engine.py and control_api.py from compressed snapshots."""
import gzip, base64, os, sys

_FILES = {
    "trading-bot/trade_engine.py": '''__TRADE_ENGINE__''',
    "trading-bot/control_api.py":  '''__CONTROL_API__''',
}

root = os.path.dirname(os.path.abspath(__file__))
for rel, b64 in _FILES.items():
    dest = os.path.join(root, rel)
    content = gzip.decompress(base64.b64decode(b64.strip()))
    with open(dest, "wb") as f:
        f.write(content)
    lines = content.count(b"\n")
    print(f"Restored {rel} ({lines} lines)")

print("Done. Run: fuser -k 8000/tcp 2>/dev/null; sleep 1; cd trading-bot && python3 main.py")
