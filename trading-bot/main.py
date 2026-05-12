"""
Trading bot entry point.

Startup order:
  1. chdir to this file's directory
  2. Init DB (before any import that touches SQLite)
  3. start_control_api() — blocks on uvicorn; all bot logic launches via
     FastAPI lifespan once the HTTP server is ready
"""

import os
import sys

# Ensure CWD is trading-bot/ regardless of launch location
os.chdir(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from dotenv import load_dotenv
load_dotenv()


def _ensure_env():
    """Create .env from .env.example if missing."""
    if not os.path.exists(".env"):
        if os.path.exists(".env.example"):
            import shutil
            shutil.copy(".env.example", ".env")
            print("[Main] Created .env from .env.example — paper mode defaults.")
        else:
            with open(".env", "w") as f:
                f.write("MODE=paper\nSTARTING_PAPER_USDT=10000.0\n")
            print("[Main] Created minimal .env — paper mode, $10,000 USDT.")


def main():
    _ensure_env()

    # Init DB first — PaperClient constructor calls load_paper_state()
    # which will crash with "no such table" if init_db() hasn't run yet.
    import database
    database.init_db()

    # start_control_api() binds uvicorn on the main thread (blocking).
    # All remaining bot initialisation runs inside the FastAPI lifespan.
    import control_api
    control_api.start_control_api()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[Main] Stopped by user.")
        sys.exit(0)
