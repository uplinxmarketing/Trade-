# ── Stage 1: Build React frontend ─────────────────────────────────────────────
FROM node:20-slim AS frontend
WORKDIR /app
COPY package*.json ./
RUN npm ci
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
