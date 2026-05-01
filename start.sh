#!/usr/bin/env bash
# TradeBot AI — Launch Script (Linux / Mac)
# Run:  bash start.sh   OR   ./start.sh

set -euo pipefail
cd "$(dirname "$0")"

# ── Log setup ────────────────────────────────────────────────────────────────
mkdir -p logs
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="logs/tradebot_$TIMESTAMP.log"
echo "TradeBot AI — Session $TIMESTAMP" > "$LOG_FILE"

# Tee all output to terminal AND log simultaneously
exec > >(tee -a "$LOG_FILE") 2>&1

log()  { echo "[$(date '+%H:%M:%S')] $*"; }
bail() { log "ERROR: $*"; echo; echo "  Log: $LOG_FILE"; exit 1; }

echo ""
echo "  ╔══════════════════════════════╗"
echo "  ║      TradeBot AI v2.5.0      ║"
echo "  ╚══════════════════════════════╝"
echo ""
log "Log: $LOG_FILE"
echo ""

# ── Check Node.js ─────────────────────────────────────────────────────────────
if ! command -v node &>/dev/null; then
  bail "Node.js not installed. Download from https://nodejs.org (v18+)"
fi
NODE_VER=$(node -e "process.stdout.write(process.versions.node)")
MAJOR=${NODE_VER%%.*}
if [ "$MAJOR" -lt 18 ]; then
  bail "Node.js v$NODE_VER is too old — need v18+. Download from https://nodejs.org"
fi
log "Node.js v$NODE_VER OK"

if ! command -v npm &>/dev/null; then
  bail "npm not found — reinstall Node.js from https://nodejs.org"
fi
log "npm $(npm --version) OK"

# ── Auto-create .env if missing ──────────────────────────────────────────────
if [ ! -f ".env" ]; then
  log "Creating .env file with Supabase credentials..."
  cat > .env << 'ENVEOF'
VITE_SUPABASE_URL=https://hkwirofdkgdamqnlcjqf.supabase.co
VITE_SUPABASE_PUBLISHABLE_KEY=sb_publishable_6p26q62HNaU7pqv9k9jb_w_8S0ixNrP
ENVEOF
  log ".env created OK"
else
  log ".env file found"
fi

# ── Auto-update from GitHub ──────────────────────────────────────────────────
if command -v git &>/dev/null && [ -d ".git" ]; then
  log "Checking for app updates (git pull)..."
  if git pull --ff-only origin main 2>&1; then
    log "App is up to date."
  else
    log "Auto-update skipped (offline or merge conflict — continuing with local version)."
  fi
else
  log "git not found or not a git repo — skipping auto-update."
fi

# ── Smart dependency check ────────────────────────────────────────────────────
# Only reinstall when package.json changes — stores its timestamp as a marker.
MARKER="node_modules/.install-marker"
NEED_INSTALL=0

if [ ! -d "node_modules" ]; then
  log "node_modules not found — installing..."
  NEED_INSTALL=1
elif [ ! -f "$MARKER" ]; then
  log "Install marker missing — reinstalling..."
  NEED_INSTALL=1
else
  PKG_TIME=$(date -r package.json +%s 2>/dev/null || stat -f %m package.json 2>/dev/null || echo "0")
  STORED_TIME=$(cat "$MARKER" 2>/dev/null || echo "")
  if [ "$PKG_TIME" != "$STORED_TIME" ]; then
    log "package.json changed — updating dependencies..."
    NEED_INSTALL=1
  else
    log "Dependencies up to date — skipping install."
  fi
fi

if [ "$NEED_INSTALL" = "1" ]; then
  echo ""
  echo "  ── Installing dependencies ──────────────────────────────────────"
  echo "  All output is live below. Errors will be visible here and in log."
  echo "  ─────────────────────────────────────────────────────────────────"
  echo ""
  # npm install output already tee'd to terminal+log via exec above
  if ! npm install; then
    bail "npm install failed — scroll up to see the error."
  fi
  if [ ! -d "node_modules/vite" ]; then
    bail "Install appeared to succeed but vite is missing. Try 'npm install' manually."
  fi
  # Save package.json timestamp so we skip install next time
  date -r package.json +%s 2>/dev/null || stat -f %m package.json 2>/dev/null > "$MARKER"
  # Ensure marker is written correctly on both Linux and Mac
  PKG_TIME=$(date -r package.json +%s 2>/dev/null || stat -f %m package.json 2>/dev/null || echo "0")
  echo "$PKG_TIME" > "$MARKER"
  log "Dependencies installed OK — marker saved."
  echo ""
fi

# ── Start dev server ──────────────────────────────────────────────────────────
echo ""
log "Starting TradeBot AI on http://localhost:8080 ..."
echo "  Browser will open automatically."
echo "  Keep this window open while using the app. Press Ctrl+C to stop."
echo ""
echo "  ── Server output ────────────────────────────────────────────────────"
echo ""

trap 'echo ""; log "Server stopped."; exit 0' INT TERM

# dev server output already tee'd via exec above
npm run dev -- --open || bail "Dev server crashed — scroll up to see the error."
