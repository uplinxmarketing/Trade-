FROM python:3.11-slim

WORKDIR /app

# Install dependencies first (cached layer)
COPY trading-bot/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy bot source
COPY trading-bot/ .

# Railway injects PORT — bot reads it via os.environ.get("PORT", 8000)
EXPOSE 8000

CMD ["python3", "main.py"]
