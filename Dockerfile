# ── Stage 1: Build React frontend ─────────────────────────────────────────────
FROM node:20-slim AS frontend
WORKDIR /app

# Cache npm dependencies separately (only busts when package.json changes)
COPY package*.json ./
RUN npm ci

# Railway auto-injects RAILWAY_GIT_COMMIT_SHA as a build arg on every push.
# Declaring it HERE (after npm ci, before the build) means:
#   - npm ci stays cached (fast rebuilds when only code changes)
#   - npm run build always re-runs when the SHA changes (every commit)
# Without this, Docker can reuse a stale build that has old version.json timestamps.
ARG RAILWAY_GIT_COMMIT_SHA=local
ENV RAILWAY_GIT_COMMIT_SHA=$RAILWAY_GIT_COMMIT_SHA

COPY . .
RUN npm run build

# ── Stage 2: Python trading bot + static file server ──────────────────────────
FROM python:3.11-slim
WORKDIR /app

# Python dependencies
COPY trading-bot/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Bot source
COPY trading-bot/ .

# React build output — served as static files by FastAPI
COPY --from=frontend /app/dist ./dist

# Railway injects PORT — bot reads it via os.environ.get("PORT", 8000)
EXPOSE 8000

CMD ["python3", "main.py"]
