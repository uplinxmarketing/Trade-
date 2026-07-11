#!/usr/bin/env bash
# One-command VPS deploy: pull latest main, ensure deps, restart, wait healthy.
# Usage: /opt/tradebot/deploy.sh
set -e
cd "$(dirname "$0")"

echo "── Fetching latest main…"
# This is a deploy MIRROR: the source of truth is GitHub `main`. `git pull` was
# fragile — the build step regenerates tracked artifacts (dist/, version.json) and
# any earlier non-fast-forward pull left a local merge commit, after which every
# pull aborts with "divergent branches / local changes would be overwritten".
# Fetch + hard-reset makes the working tree EXACTLY origin/main every time, so it
# can never diverge. Gitignored runtime state (data/bot.db, strategy.json, venv,
# node_modules) is untouched by reset.
git fetch origin main
git reset --hard origin/main
git clean -fd dist public 2>/dev/null || true

# Frontend: use the COMMITTED dist/ as-is. Every release builds dist/ and commits
# it (index.html + hashed assets always in sync), so rebuilding on the VPS added
# 30-60s of `npm ci` + `npm run build` for zero gain. Force a rebuild only when you
# deliberately changed src/ without committing dist/: DEPLOY_BUILD=1 ./deploy.sh
if [ "${DEPLOY_BUILD:-0}" = "1" ] && command -v npm >/dev/null 2>&1; then
    echo "── Building frontend (DEPLOY_BUILD=1)…"
    npm ci --silent 2>/dev/null || npm install --silent
    npm run build --silent && echo "  ✓ built" || echo "  ! build failed — serving committed dist/"
else
    echo "── Using committed dist/ (skip npm build; DEPLOY_BUILD=1 to force a rebuild)"
fi

# venv: only touch pip when the interpreter is broken OR requirements changed
# (hash guard). Skips the ~10s no-op pip install on every routine deploy.
_REQ=trading-bot/requirements.txt
_REQ_HASH=venv/.req_hash
if ! venv/bin/python -c "import fastapi" >/dev/null 2>&1; then
    echo "── venv broken — rebuilding…"
    python3 -m venv venv --clear
    venv/bin/pip install -q --upgrade pip
    venv/bin/pip install -q -r "$_REQ" && sha256sum "$_REQ" > "$_REQ_HASH" 2>/dev/null || true
elif ! sha256sum -c "$_REQ_HASH" >/dev/null 2>&1; then
    echo "── requirements changed — updating deps…"
    venv/bin/pip install -q -r "$_REQ" && sha256sum "$_REQ" > "$_REQ_HASH" 2>/dev/null || true
else
    echo "── deps unchanged — skipping pip"
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
