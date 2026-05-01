#!/usr/bin/env bash
# TradeBot AI — Launch Script (Linux / Mac)
# Run with:  bash start.sh  OR  ./start.sh (after chmod +x start.sh)

set -e
cd "$(dirname "$0")"

echo ""
echo "  ╔══════════════════════════════╗"
echo "  ║      TradeBot AI v2.1.0      ║"
echo "  ╚══════════════════════════════╝"
echo ""

# Check Node is installed
if ! command -v node &>/dev/null; then
  echo "  ✖  Node.js is not installed."
  echo "     Download it from https://nodejs.org (v18 or higher)"
  exit 1
fi

NODE_VER=$(node -e "process.stdout.write(process.versions.node)")
echo "  ✔  Node.js $NODE_VER"

# Install dependencies if missing or outdated
if [ ! -d "node_modules" ]; then
  echo "  ⬇  Installing dependencies (first run only)..."
  npm install
  echo "  ✔  Dependencies installed"
else
  echo "  ✔  Dependencies ready"
fi

echo ""
echo "  ▶  Starting app — browser will open automatically..."
echo "     Press Ctrl+C to stop."
echo ""

# --open tells Vite to launch the browser automatically
npm run dev -- --open
