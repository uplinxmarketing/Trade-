#!/usr/bin/env python3
"""
Backfill net_profit for historical trade rows that used budget_usdt as cost basis.

Correct formula: net_profit = (exit_price - entry_price) * quantity - buy_fee - sell_fee
                 profitable  = 1 if net_profit > 0 else 0

Usage:
    python3 migrate_pnl.py           # dry-run: shows diff, writes nothing
    python3 migrate_pnl.py --commit  # applies changes after confirmation
"""

import sqlite3
import sys
import os

# Use the same resolved DB path as the running bot (database._resolve_data_dir)
# so this script can never open/create a different, empty database.
# An explicit DB_PATH env var still wins for one-off runs against a copy.
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import database

DB_PATH = os.getenv("DB_PATH") or database.DB_PATH
BACKUP_PATH = DB_PATH + ".backup_before_pnl_fix"
EPSILON = 0.0005   # rows within this tolerance are considered already correct

dry_run = "--commit" not in sys.argv

if not os.path.exists(DB_PATH):
    print(f"ERROR: database not found at {DB_PATH} — refusing to create an empty one.")
    sys.exit(1)

conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row

rows = conn.execute("""
    SELECT id, coin, timestamp_sell, entry_price, exit_price, quantity,
           buy_fee, sell_fee, net_profit, profitable, sell_reason
    FROM trades
    WHERE exit_price IS NOT NULL
      AND entry_price IS NOT NULL
      AND quantity IS NOT NULL
""").fetchall()

affected = []
for r in rows:
    try:
        correct = (float(r["exit_price"]) - float(r["entry_price"])) * float(r["quantity"]) \
                  - float(r["buy_fee"] or 0) - float(r["sell_fee"] or 0)
        stored  = float(r["net_profit"] or 0)
        if abs(correct - stored) > EPSILON:
            affected.append({
                "id":            r["id"],
                "coin":          r["coin"],
                "timestamp_sell":r["timestamp_sell"],
                "old_net":       stored,
                "new_net":       correct,
                "old_profitable":r["profitable"],
                "new_profitable":1 if correct > 0 else 0,
                "sell_reason":   r["sell_reason"],
            })
    except Exception as e:
        print(f"  [SKIP] id={r['id']} {r['coin']}: {e}")

print(f"\nRows scanned : {len(rows)}")
print(f"Rows affected: {len(affected)}")
print()

if not affected:
    print("Nothing to fix — all rows already correct.")
    conn.close()
    sys.exit(0)

old_total = sum(float(r["net_profit"] or 0) for r in rows)
new_total  = old_total + sum(a["new_net"] - a["old_net"] for a in affected)

print(f"{'ID':>6}  {'Coin':<12}  {'Timestamp':<24}  {'Old net':>10}  {'New net':>10}  {'Delta':>9}  {'Reason'}")
print("-" * 100)
for a in affected:
    delta = a["new_net"] - a["old_net"]
    print(f"{a['id']:>6}  {a['coin']:<12}  {a['timestamp_sell']:<24}  "
          f"{a['old_net']:>10.4f}  {a['new_net']:>10.4f}  {delta:>+9.4f}  {a['sell_reason'] or ''}")

print()
print(f"Current SUM(net_profit) : {old_total:+.4f} USDT")
print(f"Corrected SUM(net_profit): {new_total:+.4f} USDT")
print(f"Delta                    : {new_total - old_total:+.4f} USDT")
print()

if dry_run:
    print("DRY RUN — no changes written. Re-run with --commit to apply.")
    conn.close()
    sys.exit(0)

# Confirm
ans = input(f"Apply {len(affected)} updates? [yes/no] ").strip().lower()
if ans != "yes":
    print("Aborted.")
    conn.close()
    sys.exit(0)

# Backup first
import shutil
shutil.copy2(DB_PATH, BACKUP_PATH)
print(f"Backup written to {BACKUP_PATH}")

for a in affected:
    conn.execute(
        "UPDATE trades SET net_profit=?, profitable=? WHERE id=?",
        (a["new_net"], a["new_profitable"], a["id"])
    )
conn.commit()
print(f"Done — {len(affected)} rows updated.")

# Verify
total_after = conn.execute("SELECT COALESCE(SUM(net_profit),0) FROM trades WHERE exit_price IS NOT NULL").fetchone()[0]
print(f"New SUM(net_profit): {total_after:+.4f} USDT")
conn.close()
