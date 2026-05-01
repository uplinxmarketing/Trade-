#!/usr/bin/env bash
# TradeBot AI — Mac double-click launcher
# Double-click in Finder. First time: right-click → Open.

set -euo pipefail
cd "$(dirname "$0")"

# ── Log setup ────────────────────────────────────────────────────────────────
LOG_DIR="logs"
mkdir -p "$LOG_DIR"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="$LOG_DIR/tradebot_$TIMESTAMP.log"
exec > >(tee -a "$LOG_FILE") 2>&1

log()  { echo "[$(date '+%H:%M:%S')] $*"; }
bail() {
  echo "[$(date '+%H:%M:%S')] ERROR: $*"
  osascript -e "display alert \"TradeBot AI Error\" message \"$*\n\nFull log: $LOG_FILE\""
  exit 1
}

echo ""
echo "  ╔══════════════════════════════╗"
echo "  ║      TradeBot AI v2.1.0      ║"
echo "  ╚══════════════════════════════╝"
echo ""
log "Log file: $LOG_FILE"

# ── Check Node.js ─────────────────────────────────────────────────────────────
if ! command -v node &>/dev/null; then
  bail "Node.js not found. Install from https://nodejs.org (v18+)"
fi

NODE_VER=$(node -e "process.stdout.write(process.versions.node)")
MAJOR=${NODE_VER%%.*}
if [ "$MAJOR" -lt 18 ]; then
  bail "Node.js v$NODE_VER is too old. Please install v18 or higher."
fi
log "Node.js $NODE_VER ✔"

# ── Install dependencies ──────────────────────────────────────────────────────
if [ ! -d "node_modules" ] || [ ! -f "node_modules/.package-lock.json" ]; then
  log "Installing dependencies (first run only)..."
  npm install || bail "npm install failed. See log: $LOG_FILE"
  log "Dependencies installed ✔"
else
  log "Dependencies ready ✔"
fi

echo ""
log "Starting TradeBot AI — browser will open automatically..."
log "Close this window to stop."
echo ""

trap 'log "Server stopped."; exit 0' INT TERM

npm run dev -- --open || bail "Dev server crashed. See log: $LOG_FILE"
