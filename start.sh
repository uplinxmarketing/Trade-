#!/usr/bin/env bash
# TradeBot AI — VPS Launch Script
# Run:  bash start.sh   OR   ./start.sh

set -euo pipefail
cd "$(dirname "$0")"

# ── Log setup ────────────────────────────────────────────────────────────────
mkdir -p logs
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="logs/tradebot_$TIMESTAMP.log"
echo "TradeBot AI — Session $TIMESTAMP" > "$LOG_FILE"
exec > >(tee -a "$LOG_FILE") 2>&1

log()  { echo "[$(date '+%H:%M:%S')] $*"; }
bail() { log "ERROR: $*"; echo; echo "  Log: $LOG_FILE"; exit 1; }

echo ""
echo "  ╔══════════════════════════════╗"
echo "  ║      TradeBot AI v5.12.37    ║"
echo "  ╚══════════════════════════════╝"
echo ""

# ── Auto-update from GitHub ──────────────────────────────────────────────────
if command -v git &>/dev/null && [ -d ".git" ]; then
  log "Pulling latest code from GitHub..."
  if git fetch origin main 2>&1 && git reset --hard origin/main 2>&1; then
    log "Code updated to latest."
  else
    log "Git update failed (offline?) — continuing with local version."
  fi
fi

# ── Check Node.js ─────────────────────────────────────────────────────────────
if ! command -v node &>/dev/null; then
  bail "Node.js not installed. Run: curl -fsSL https://deb.nodesource.com/setup_22.x | bash - && apt install nodejs"
fi
log "Node.js $(node --version) / npm $(npm --version)"

# ── Install JS dependencies if needed ────────────────────────────────────────
MARKER="node_modules/.install-marker"
PKG_TIME=$(date -r package.json +%s 2>/dev/null || echo "0")
STORED=$(cat "$MARKER" 2>/dev/null || echo "")
if [ ! -d "node_modules" ] || [ "$PKG_TIME" != "$STORED" ]; then
  log "Installing JS dependencies..."
  npm ci --prefer-offline 2>/dev/null || npm install || bail "npm install failed"
  echo "$PKG_TIME" > "$MARKER"
  log "JS dependencies OK"
fi

# ── Build React frontend ──────────────────────────────────────────────────────
log "Building frontend..."
npm run build || bail "npm run build failed"
log "Frontend built OK"

# Copy dist/ into trading-bot/dist/ so the Python bot can serve it
log "Copying dist/ into trading-bot/..."
rm -rf trading-bot/dist
cp -r dist trading-bot/dist
log "trading-bot/dist ready"

# ── Auto-create .env if missing ──────────────────────────────────────────────
if [ ! -f "trading-bot/.env" ]; then
  if [ -f ".env" ]; then
    cp .env trading-bot/.env
    log ".env copied to trading-bot/.env"
  else
    printf "MODE=paper\nSTARTING_PAPER_USDT=10000.0\n" > trading-bot/.env
    log "Created minimal trading-bot/.env (paper mode)"
  fi
fi

# ── Check Python ──────────────────────────────────────────────────────────────
PYTHON=$(command -v python3 || command -v python || true)
[ -z "$PYTHON" ] && bail "Python 3 not found — install with: apt install python3 python3-pip"
log "Python: $($PYTHON --version)"

# ── Install Python dependencies ───────────────────────────────────────────────
PY_MARKER="trading-bot/.py-install-marker"
REQ_TIME=$(date -r trading-bot/requirements.txt +%s 2>/dev/null || echo "0")
PY_STORED=$(cat "$PY_MARKER" 2>/dev/null || echo "")
if [ "$REQ_TIME" != "$PY_STORED" ]; then
  log "Installing Python dependencies..."
  $PYTHON -m pip install -r trading-bot/requirements.txt --quiet || bail "pip install failed"
  echo "$REQ_TIME" > "$PY_MARKER"
  log "Python dependencies OK"
else
  log "Python dependencies up to date"
fi

# ── Start bot ─────────────────────────────────────────────────────────────────
echo ""
log "Starting TradeBot AI bot on port ${PORT:-8000}..."
echo "  Dashboard: http://localhost:${PORT:-8000}"
echo "  Press Ctrl+C to stop."
echo ""

trap 'echo ""; log "Bot stopped."; exit 0' INT TERM

cd trading-bot
exec $PYTHON main.py
