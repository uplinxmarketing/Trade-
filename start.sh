#!/usr/bin/env bash
# TradeBot AI — Launch Script (Linux / Mac)
# Usage:  bash start.sh   OR   ./start.sh

set -euo pipefail
cd "$(dirname "$0")"

# ── Log setup ────────────────────────────────────────────────────────────────
LOG_DIR="logs"
mkdir -p "$LOG_DIR"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="$LOG_DIR/tradebot_$TIMESTAMP.log"

# Tee all output to both terminal and log file
exec > >(tee -a "$LOG_FILE") 2>&1

log()  { echo "[$(date '+%H:%M:%S')] $*"; }
err()  { echo "[$(date '+%H:%M:%S')] ERROR: $*" >&2; }
bail() { err "$*"; echo; echo "Full log saved to: $LOG_FILE"; exit 1; }

echo ""
echo "  ╔══════════════════════════════╗"
echo "  ║      TradeBot AI v2.1.0      ║"
echo "  ╚══════════════════════════════╝"
echo ""
log "Log file: $LOG_FILE"
echo ""

# ── Check Node.js ─────────────────────────────────────────────────────────────
if ! command -v node &>/dev/null; then
  bail "Node.js is not installed. Download from https://nodejs.org (v18+)"
fi

NODE_VER=$(node -e "process.stdout.write(process.versions.node)")
MAJOR=${NODE_VER%%.*}
if [ "$MAJOR" -lt 18 ]; then
  bail "Node.js v$NODE_VER is too old. Please install v18 or higher."
fi
log "Node.js $NODE_VER ✔"

# ── Check npm ────────────────────────────────────────────────────────────────
if ! command -v npm &>/dev/null; then
  bail "npm is not installed. It should come with Node.js."
fi
log "npm $(npm --version) ✔"

# ── Install dependencies ──────────────────────────────────────────────────────
if [ ! -d "node_modules" ] || [ ! -f "node_modules/.package-lock.json" ]; then
  log "Installing dependencies (first run only)..."
  npm install || bail "npm install failed. Check the log: $LOG_FILE"
  log "Dependencies installed ✔"
else
  log "Dependencies ready ✔"
fi

# ── Start dev server ──────────────────────────────────────────────────────────
echo ""
log "Starting TradeBot AI on http://localhost:8080 ..."
log "Browser will open automatically. Press Ctrl+C to stop."
echo ""

# Trap Ctrl+C cleanly
trap 'echo ""; log "Server stopped."; exit 0' INT TERM

npm run dev -- --open || bail "Dev server crashed. See log: $LOG_FILE"
