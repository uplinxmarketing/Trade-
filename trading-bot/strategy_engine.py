"""
Strategy engine — runs every DECISION_INTERVAL_SEC.
Calls Claude to decide which coins to approve.
Falls back to a permissive default if no API key.
Never executes trades — only writes strategy.json.
"""

import asyncio
import fcntl
import json
import os
from contextlib import contextmanager
from datetime import datetime, timezone

import config
import database
import learning
from data_collector import prices


def _load_strategy() -> dict:
    try:
        with open(config.STRATEGY_FILE) as f:
            return json.load(f)
    except Exception:
        return {}


@contextmanager
def _strategy_file_lock():
    """Exclusive file lock guarding strategy.json read-modify-write cycles.

    NOTE on lock domains: control_api._write_strategy_patch serializes its own
    writers on a module-private threading.Lock (_strategy_write_lock), which
    does NOT protect against this module's writes — a Claude strategy write
    could land between control_api's read and write (or vice versa) and
    silently revert a user settings change. This lockfile (fcntl.flock on
    strategy.json.lock) works across both threads and processes; control_api
    should adopt this same lockfile so all strategy.json writers share one
    lock domain.
    """
    lock_path = config.STRATEGY_FILE + ".lock"
    f = open(lock_path, "w")
    try:
        fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        yield
    finally:
        try:
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)
        except Exception:
            pass
        f.close()


def _atomic_write_strategy(data: dict):
    """Write strategy.json via temp+rename so concurrent readers never see a
    half-written or empty file (which would silently reset live trade settings)."""
    tmp = config.STRATEGY_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f, indent=2)
        f.flush()
        try:
            os.fsync(f.fileno())
        except Exception:
            pass
    os.replace(tmp, config.STRATEGY_FILE)


def _get_usdt_balance() -> float:
    from connection import client, get_mode
    try:
        if get_mode() != "live" and hasattr(client, "_balances"):
            with client._lock:
                return float(client._balances.get("USDT", 0.0))
        acc = client.get_account()
        for b in acc["balances"]:
            if b["asset"] == "USDT":
                return float(b["free"])
    except Exception:
        pass
    return float(os.getenv("STARTING_PAPER_USDT", "10000.0"))


def _build_market_data() -> dict:
    market_data = {}
    for coin in config.WATCHED_COINS:
        candles = database.get_candles(coin, config.CANDLE_TIMEFRAME, limit=50)
        if len(candles) < 2:
            continue
        last = candles[-1]
        prev = candles[-2]
        prev24 = candles[-25] if len(candles) >= 25 else candles[0]
        market_data[coin] = {
            "current_price":    last.get("close", 0),
            "rsi":              round(last.get("rsi14") or 50, 1),
            "ma_position":      last.get("ma_position", "unknown"),
            "bb_position":      last.get("bb_position", "unknown"),
            "volume_trend":     last.get("volume_trend", "flat"),
            "price_change_1h":  round((last["close"] - prev["close"]) / max(prev["close"], 1) * 100, 3) if prev.get("close") else 0,
            "price_change_24h": round((last["close"] - prev24["close"]) / max(prev24["close"], 1) * 100, 3) if prev24.get("close") else 0,
        }
    return market_data


def write_default_strategy():
    """
    Ensure a valid strategy.json exists.
    If the file is already present and contains approved_coins (i.e. the user's
    settings from a prior run, preserved on the Railway volume), keep it intact —
    only touch updated_at so nothing is lost across redeploys.
    Creates a fresh default only when the file is missing or corrupt.
    """
    # ── Preserve existing strategy across redeploys ────────────────────────────────────────────────────────────────────────────────────
    if os.path.exists(config.STRATEGY_FILE):
        try:
            # Read-modify-write under the shared file lock so a concurrent
            # settings write can't be lost between our read and our write.
            with _strategy_file_lock():
                with open(config.STRATEGY_FILE) as f:
                    existing = json.load(f)
                if existing.get("approved_coins"):
                    # Valid file — keep all user settings, just refresh the timestamp
                    existing["updated_at"] = datetime.now(timezone.utc).isoformat()
                    _atomic_write_strategy(existing)
            if existing.get("approved_coins"):
                n = len(existing["approved_coins"])
                active = existing.get("trading_active", True)
                print(f"[StrategyEngine] strategy.json preserved — {n} coins, active={active}.")
                # Sync coin list to Supabase so it survives Railway redeploys
                try:
                    import supabase_sync
                    syms = [c["symbol"] for c in existing["approved_coins"] if c.get("approved")]
                    if syms:
                        supabase_sync.sync_selected_coins(syms)
                except Exception:
                    pass
                return existing
        except Exception as e:
            print(f"[StrategyEngine] strategy.json unreadable ({e}) — writing defaults.")

    # ── First run or corrupt file — create defaults ──────────────────────────────────────────────────────────────────────────────────
    # Prefer the DB-persisted starting balance (written at wallet-reset time) so
    # initial_balance_usdt always matches what the paper client actually started with.
    starting_str  = database.get_setting("paper_starting_balance")
    starting_usdt = float(starting_str) if starting_str else float(os.getenv("STARTING_PAPER_USDT", "10000.0"))
    strategy = {
        "updated_at":           datetime.now(timezone.utc).isoformat(),
        "trading_active":       True,
        "pause_reason":         None,
        "initial_balance_usdt": starting_usdt,
        "min_signals":          config.MIN_SIGNALS_TO_BUY,
        "approved_coins": [
            {
                "symbol":         coin,
                "approved":       True,
                "budget_usdt":    config.BUDGET_PER_TRADE_USDT,
                "max_concurrent": 3,
                "confidence":     0.5,
                "reason":         "Default — all coins approved",
            }
            for coin in config.WATCHED_COINS
        ],
        "next_review_seconds":  config.DECISION_INTERVAL_SEC,
    }
    with _strategy_file_lock():
        _atomic_write_strategy(strategy)
    print(f"[StrategyEngine] strategy.json created — {len(config.WATCHED_COINS)} coins.")
    return strategy


def _call_claude(market_data: dict, recent_trades: list, win_rates: dict,
                 avg_durations: dict, patterns: list, open_positions: list,
                 usdt_balance: float) -> dict:
    import anthropic
    from trade_engine import _record_claude_error, is_claude_disabled

    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        return {}

    if is_claude_disabled():
        print("[StrategyEngine] Claude disabled due to repeated auth failures — skipping")
        return {}

    strategy_notes = _load_strategy().get("strategy_notes", "")
    notes_section = f"\n=== USER STRATEGY NOTES ===\n{strategy_notes}\n" if strategy_notes else ""

    prompt = f"""You are a disciplined spot trading analyst for a high-frequency bot.
Your job: decide which coins to approve for trading in the next 10 minutes.
The bot buys immediately on approved coins and sells the instant price exceeds breakeven (entry / 0.999² ≈ entry × 1.002003, covering both 0.1% fees).
It holds losing positions without selling until they recover.

=== MARKET DATA ===
{json.dumps(market_data, indent=2)}

=== RECENT TRADE PERFORMANCE ===
{json.dumps(recent_trades[-10:], indent=2)}

=== WIN RATES PER COIN ===
{json.dumps(win_rates, indent=2)}

=== AVERAGE TIME TO BREAKEVEN PER COIN (seconds) ===
{json.dumps(avg_durations, indent=2)}

=== LEARNED PATTERNS (min 3 occurrences) ===
{json.dumps(patterns[:15], indent=2)}

=== CURRENT STATE ===
USDT available:   {usdt_balance}
Open positions:   {len(open_positions)} (no cap — only limited by USDT balance)
Open details:     {json.dumps(open_positions, indent=2)}

=== DECISION RULES ===
- Approve coins where price is likely to move UP in the next 10-60 minutes
- NEVER approve a coin with RSI above {config.RSI_OVERBOUGHT} (overbought)
- NEVER approve a coin with 3+ consecutive declining volume candles (no momentum)
- Prefer coins where price is near MA20 support or lower Bollinger Band (bounce potential)
- Reduce budget or max_concurrent for coins with win_rate below 0.5
- Increase budget for coins with win_rate above 0.7 AND confidence pattern present
- If open positions already exist on a coin, factor in the tied-up USDT
- Set trading_active to false only if overall market is in sharp decline
{notes_section}
Respond ONLY with valid JSON matching this exact schema (no text outside JSON):

{{
  "updated_at": "<ISO timestamp>",
  "trading_active": true,
  "pause_reason": null,
  "approved_coins": [
    {{
      "symbol": "BTCUSDT",
      "approved": true,
      "budget_usdt": 100,
      "max_concurrent": 3,
      "confidence": 0.75,
      "reason": "one sentence"
    }}
  ],
  "next_review_seconds": {config.DECISION_INTERVAL_SEC}
}}"""

    try:
        aclient = anthropic.Anthropic(api_key=api_key)
        msg = aclient.messages.create(
            model=config.CLAUDE_MODEL,
            max_tokens=config.CLAUDE_MAX_TOKENS,
            messages=[{"role": "user", "content": prompt}],
        )
    except anthropic.AuthenticationError as auth_err:
        _record_claude_error(str(auth_err), is_auth_error=True)
        raise
    except Exception as api_err:
        _record_claude_error(str(api_err), is_auth_error=False)
        raise

    text = msg.content[0].text.strip()

    # Strip markdown code fences if present
    if text.startswith("```"):
        text = text.split("```")[1]
        if text.startswith("json"):
            text = text[4:]
    text = text.strip()

    return json.loads(text)


def run_strategy_once():
    """Run one strategy cycle. Writes strategy.json. Returns the strategy dict."""
    api_key = os.getenv("ANTHROPIC_API_KEY", "").strip()

    # Respect the claude_agent_enabled toggle — silently skip if disabled
    try:
        strategy_check = _load_strategy()
        claude_enabled = bool(strategy_check.get("claude_agent_enabled", True))
    except Exception:
        claude_enabled = True

    if not claude_enabled:
        return _load_strategy()  # keep existing strategy, no API call, no log

    if not api_key or api_key.startswith("#"):
        return write_default_strategy()

    print("[StrategyEngine] Running Claude analysis…")
    try:
        from trade_engine import get_open_positions
        market_data   = _build_market_data()
        recent_trades = database.get_recent_trades(limit=20)
        win_rates     = database.get_win_rate_per_coin()
        avg_durations = database.get_avg_duration_per_coin()
        patterns      = learning.get_pattern_summary(min_occurrences=3)
        open_pos      = get_open_positions()
        usdt_balance  = _get_usdt_balance()

        strategy = _call_claude(
            market_data, recent_trades, win_rates, avg_durations,
            patterns, open_pos, usdt_balance,
        )
        if not strategy or "approved_coins" not in strategy:
            raise ValueError("Claude returned invalid strategy")

        # Merge Claude's output INTO the current file rather than replacing it.
        # Claude's schema only produces the keys below; every other key in
        # strategy.json (protection toggles, budget/risk settings, notes —
        # e.g. bot_allocation_usdt, stop_loss_*, take_profit_*, max_positions,
        # min_signals, budget_*, macro_gate_enabled, claude_agent_enabled, …)
        # is user/API-owned and must survive every Claude write verbatim.
        # NOTE: trading_active is intentionally NOT overlaid from Claude — we
        # force it below so Claude can never silently stop the bot; an explicit
        # user stop (trading_active=False via /api/stop) is always honoured.
        _CLAUDE_OWNED_KEYS = ["approved_coins", "pause_reason", "next_review_seconds"]

        # Do the read-merge-write under the strategy file lock so a concurrent
        # settings write (control_api) can't be clobbered by this write.
        with _strategy_file_lock():
            _existing = _load_strategy()
            merged = dict(_existing)
            for key in _CLAUDE_OWNED_KEYS:
                if key in strategy:
                    merged[key] = strategy[key]

            # Preserve the user's coin selection — never let Claude overwrite it.
            # (Flag written when coins are saved via /api/strategy.)
            if _existing.get("user_selected_coins") and _existing.get("approved_coins"):
                merged["approved_coins"] = _existing["approved_coins"]
                merged["user_selected_coins"] = True

            # Honour an explicit user-initiated stop; ignore Claude's own value.
            merged["trading_active"] = False if _existing.get("trading_active") is False else True

            merged["updated_at"] = datetime.now(timezone.utc).isoformat()
            _atomic_write_strategy(merged)

        strategy = merged

        active  = strategy.get("trading_active", True)
        n_approved = sum(1 for c in strategy.get("approved_coins", []) if c.get("approved"))
        print(f"[StrategyEngine] Claude done — trading={active}, approved={n_approved} coins")

        # Persist selected coins to Supabase so they survive Railway redeploys
        try:
            import supabase_sync
            syms = [c["symbol"] for c in strategy.get("approved_coins", []) if c.get("approved")]
            if syms:
                supabase_sync.sync_selected_coins(syms)
        except Exception:
            pass

        return strategy

    except Exception as e:
        print(f"[StrategyEngine] Claude error: {e} — keeping existing strategy")
        # Preserve user coins/settings; only ensure trading stays active
        existing = write_default_strategy()
        if existing.get("trading_active") is False:
            existing["trading_active"] = True
        return existing


async def strategy_loop():
    """Runs strategy_once() every DECISION_INTERVAL_SEC."""
    # Wait before first run so the bot can fully start and the user's
    # trading_active setting (set in lifespan) is stable before Claude runs.
    await asyncio.sleep(60)
    while True:
        # run_strategy_once() may call the Anthropic API synchronously.
        # Run it in a thread so it never blocks the asyncio event loop
        # (which would starve the WebSocket and signal scanner).
        await asyncio.to_thread(run_strategy_once)
        interval = config.DECISION_INTERVAL_SEC
        print(f"[StrategyEngine] Next review in {interval}s")
        await asyncio.sleep(interval)
