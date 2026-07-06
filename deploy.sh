#!/usr/bin/env bash
# One-command VPS deploy: pull latest main, ensure deps, restart, wait healthy.
# Usage: /opt/tradebot/deploy.sh
set -e
cd "$(dirname "$0")"

echo "── Pulling latest main…"
git pull origin main

# Rebuild the venv if its interpreter is missing/broken (e.g. clobbered by git)
if ! venv/bin/python -c "import fastapi" >/dev/null 2>&1; then
    echo "── venv broken or missing deps — rebuilding…"
    python3 -m venv venv --clear
    venv/bin/pip install -q --upgrade pip
    venv/bin/pip install -q -r trading-bot/requirements.txt
else
    venv/bin/pip install -q -r trading-bot/requirements.txt
fi

echo "── Restarting bot…"
systemctl restart tradebot

echo -n "── Waiting for bot to come up"
for i in $(seq 1 30); do
    if curl -sf http://localhost:8000/api/status >/dev/null 2>&1; then
        echo
        echo "✓ Bot is up:"
        curl -s http://localhost:8000/api/status | head -c 200
        echo
        exit 0
    fi
    echo -n "."
    sleep 2
done

echo
echo "✗ Bot did not come up within 60s. Check logs:"
echo "  journalctl -u tradebot -n 30 --no-pager"
exit 1
