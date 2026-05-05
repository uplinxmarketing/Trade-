# ── Stage 1: Build React frontend ─────────────────────────────────────────────
FROM node:20-slim AS frontend
WORKDIR /app

# Cache npm dependencies separately (only busts when package.json changes)
COPY package*.json ./
RUN npm ci

# CACHE_BUST changes on every push — guarantees Railway runs npm run build fresh
# even when it tries to serve a cached image layer.
ARG CACHE_BUST=2026-05-05T20:22:40Z
COPY .buildid ./
COPY . .
RUN echo "Cache bust: $CACHE_BUST" && npm run build

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
