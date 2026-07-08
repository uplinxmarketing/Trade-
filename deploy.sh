#!/usr/bin/env bash
# One-command VPS deploy: pull latest main, ensure deps, restart, wait healthy.
# Usage: /opt/tradebot/deploy.sh
set -e
cd "$(dirname "$0")"

echo "── Pulling latest main…"
# The build step below regenerates tracked artifacts (dist/, public/version.json),
# which leaves the working tree dirty and makes the next `git pull` abort with
# "local changes would be overwritten". Discard those regenerated artifacts before
# pulling so the pull always applies cleanly — they are rebuilt again below.
git checkout -- dist public/version.json 2>/dev/null || true
git pull origin main

# Rebuild the frontend bundle from source so the served dist/ always matches the
# committed code (avoids the white-screen trap where index.html references a
# hashed asset that never got committed). Best-effort: if npm is unavailable the
# committed dist/ is used as-is.
if command -v npm >/dev/null 2>&1; then
    echo "── Building frontend…"
    if npm ci --silent 2>/dev/null || npm install --silent; then
        if npm run build --silent; then
            echo "  ✓ frontend built"
        else
            echo "  ! frontend build failed — serving committed dist/ as-is"
        fi
    else
        echo "  ! npm install failed — serving committed dist/ as-is"
    fi
else
    echo "── npm not found — serving committed dist/ as-is"
fi

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
