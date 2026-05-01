#!/usr/bin/env bash
# TradeBot AI — Mac double-click launcher
# Double-click this file in Finder to start the app.
# First time: right-click → Open (to bypass Gatekeeper), then just double-click after that.

set -e
cd "$(dirname "$0")"

echo ""
echo "  ╔══════════════════════════════╗"
echo "  ║      TradeBot AI v2.1.0      ║"
echo "  ╚══════════════════════════════╝"
echo ""

if ! command -v node &>/dev/null; then
  osascript -e 'display alert "Node.js not found" message "Please install Node.js from https://nodejs.org (v18 or higher) then try again."'
  exit 1
fi

if [ ! -d "node_modules" ]; then
  echo "  ⬇  Installing dependencies (first run only)..."
  npm install
fi

echo "  ▶  Starting — browser will open automatically..."
echo "     Close this window or press Ctrl+C to stop."
echo ""

npm run dev -- --open
