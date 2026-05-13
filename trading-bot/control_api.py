"""
FastAPI control server — binds to $PORT (default 8000) on the main thread.
All trading-bot logic (DB init, history download, WebSocket feed, strategy
loop) starts in the FastAPI lifespan as async background tasks.

Endpoints
─────────
GET  /status          → live bot state (positions, balance, mode, signals)
GET  /trades          → recent closed trades
GET  /activity        → activity log
GET  /strategy        → current strategy.json
POST /strategy        → save strategy.json (full replace)
POST /start           → start bot (no-op if already running)
POST /stop            → graceful stop
POST /reset-wallet    → reset paper wallet to starting balance
POST /sell/{symbol}   → force-sell a specific position
POST /sell-all        → force-sell all open positions
GET  /health          → liveness probe (returns 200 OK)
"""

import asyncio
import json
import os
import time
import threading
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

import config
import database

# ── lazy imports ──────────────────────────────────────────────────────────────
# trade_engine and data_collector are imported lazily (inside request handlers
# or after the bot has started) so that a missing dependency at module-load time
# doesn't prevent the API server from booting.

_bot_task: asyncio.Task | None = None
_bot_lock = threading.Lock()


# ── Lifespan ──────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Start trading bot in background; shut it down cleanly on exit."""
    database.init_db()
    await _start_bot()
    yield
    await _stop_bot()


app = FastAPI(title="Trade Bot Control API", lifespan=lifespan)


# ── Bot lifecycle helpers ─────────────────────────────────────────────────────

async def _start_bot():
    global _bot_task
    with _bot_lock:
        if _bot_task and not _bot_task.done():
            return  # already running
        _bot_task = asyncio.create_task(_run_bot(), name="trade-bot")
        database.log_activity("Bot started", "info")


async def _stop_bot():
    global _bot_task
    with _bot_lock:
        task = _bot_task
    if task and not task.done():
        task.cancel()
        try:
            await asyncio.wait_for(asyncio.shield(task), timeout=5.0)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            pass
    database.log_activity("Bot stopped", "info")


async def _run_bot():
    """
    Main bot coroutine.  Runs all subsystems concurrently:
      • data_collector  — downloads history, then streams live WebSocket ticks
      • signal_scanner  — REST-based signal refresh every SCAN_INTERVAL_SEC
    trade_engine.realtime_monitor is called from data_collector on every tick.
    """
    import data_collector
    import trade_engine

    trade_engine.load_positions_from_db()

    await asyncio.gather(
        data_collector.run(),
        trade_engine.signal_scanner(),
    )


# ── Utility helpers ───────────────────────────────────────────────────────────

def _safe_float(val, default=0.0):
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def _format_position(pos: dict) -> dict:
    """Return a display-friendly copy of a position dict."""
    from connection import get_mode
    import data_collector

    sym          = pos.get("symbol", "")
    entry        = _safe_float(pos.get("entry_price"))
    qty          = _safe_float(pos.get("quantity"))
    budget       = _safe_float(pos.get("budget_usdt"))
    buy_fee      = _safe_float(pos.get("buy_fee_usdt"))
    current      = _safe_float(data_collector.prices.get(sym))
    target       = _safe_float(pos.get("exit_target", entry * config.FEE_RATE))
    current_val  = qty * current if current else 0.0
    unrealised   = current_val - budget - buy_fee if current else None

    return {
        "id":            pos.get("id"),
        "symbol":        sym,
        "entry_price":   entry,
        "exit_target":   target,
        "current_price": current,
        "quantity":      qty,
        "budget_usdt":   budget,
        "buy_fee_usdt":  buy_fee,
        "current_value": round(current_val, 4) if current else None,
        "unrealised_pnl": round(unrealised, 4) if unrealised is not None else None,
        "timestamp":     pos.get("timestamp"),
        "mode":          pos.get("mode", get_mode()),
    }


# ── GET /health ───────────────────────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok"}


# ── GET /status ───────────────────────────────────────────────────────────────

@app.get("/status")
async def get_status():
    from connection import get_mode, client
    import data_collector
    import trade_engine

    mode = get_mode()

    # Balance
    try:
        if mode == "live":
            acc   = client.get_account()
            usdt  = next(
                (float(b["free"]) for b in acc["balances"] if b["asset"] == "USDT"), 0.0
            )
        else:
            if hasattr(client, "_balances"):
                with client._lock:
                    usdt = float(client._balances.get("USDT", 0.0))
            else:
                usdt = float(os.getenv("STARTING_PAPER_USDT", "10000.0"))
    except Exception:
        usdt = 0.0

    # Open positions
    positions = [_format_position(p) for p in trade_engine.get_open_positions()]

    # Portfolio value
    total_invested = sum(_safe_float(p.get("budget_usdt")) for p in trade_engine.get_open_positions())
    total_current  = sum(_safe_float(p.get("current_value")) for p in positions if p.get("current_value"))
    portfolio_pnl  = total_current - total_invested if total_current else None

    # Signal cache snapshot
    with trade_engine._signal_cache_lock:
        signal_snapshot = {
            sym: {
                "score":   v.get("score", 0),
                "price":   v.get("price"),
                "rsi_val": v.get("rsi_val"),
                "signals": v.get("signals", {}),
                "bb_ok":   v.get("bb_ok", True),
                "5m_ok":   v.get("5m_ok", True),
            }
            for sym, v in trade_engine._signal_cache.items()
        }

    # Bot running?
    bot_running = bool(_bot_task and not _bot_task.done())

    # Recent trade stats
    recent_trades = database.get_recent_trades(limit=50)
    wins   = sum(1 for t in recent_trades if _safe_float(t.get("net_profit")) > 0)
    losses = sum(1 for t in recent_trades if _safe_float(t.get("net_profit")) <= 0)
    total_pnl = sum(_safe_float(t.get("net_profit")) for t in recent_trades)

    return {
        "mode":            mode,
        "bot_running":     bot_running,
        "usdt_balance":    round(usdt, 4),
        "open_positions":  positions,
        "position_count":  len(positions),
        "total_invested":  round(total_invested, 4),
        "portfolio_value": round(total_current, 4) if total_current else None,
        "portfolio_pnl":   round(portfolio_pnl, 4) if portfolio_pnl is not None else None,
        "signal_cache":    signal_snapshot,
        "trade_stats": {
            "wins":      wins,
            "losses":    losses,
            "total_pnl": round(total_pnl, 4),
            "win_rate":  round(wins / max(wins + losses, 1) * 100, 1),
        },
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


# ── GET /trades ───────────────────────────────────────────────────────────────

@app.get("/trades")
async def get_trades(limit: int = 50):
    trades = database.get_recent_trades(limit=min(limit, 200))
    return {"trades": trades, "count": len(trades)}


# ── GET /activity ─────────────────────────────────────────────────────────────

@app.get("/activity")
async def get_activity(limit: int = 100):
    rows = database.get_activity_log(limit=min(limit, 500))
    return {"activity": rows, "count": len(rows)}


# ── GET /strategy ─────────────────────────────────────────────────────────────

@app.get("/strategy")
async def get_strategy():
    path = config.STRATEGY_FILE
    if not os.path.exists(path):
        return {}
    try:
        with open(path) as f:
            return json.load(f)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not read strategy: {e}")


# ── POST /strategy ────────────────────────────────────────────────────────────

@app.post("/strategy")
async def save_strategy(request: Request):
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    body["updated_at"] = datetime.now(timezone.utc).isoformat()

    path = config.STRATEGY_FILE
    tmp  = path + ".tmp"
    try:
        with open(tmp, "w") as f:
            json.dump(body, f, indent=2)
            f.flush()
            try:
                os.fsync(f.fileno())
            except Exception:
                pass
        os.replace(tmp, path)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not save strategy: {e}")

    # Persist to Supabase immediately so the next deploy picks up the new config.
    try:
        import supabase_sync
        import trade_engine
        usdt = 0.0
        try:
            from connection import client, get_mode
            if get_mode() != "live" and hasattr(client, "_balances"):
                with client._lock:
                    usdt = float(client._balances.get("USDT", 0.0))
        except Exception:
            pass
        open_pos = trade_engine.get_open_positions()
        coins    = [c["symbol"] for c in body.get("approved_coins", []) if c.get("approved")]
        supabase_sync.sync_all(open_pos, usdt, selected_coins=coins)
    except Exception as e:
        database.log_activity(f"Supabase sync after strategy save failed: {e}", "warn")

    database.log_activity("Strategy updated via API", "info")
    return {"status": "saved", "updated_at": body["updated_at"]}


# ── POST /start ───────────────────────────────────────────────────────────────

@app.post("/start")
async def start_bot():
    await _start_bot()
    return {"status": "started"}


# ── POST /stop ────────────────────────────────────────────────────────────────

@app.post("/stop")
async def stop_bot():
    await _stop_bot()
    return {"status": "stopped"}


# ── POST /reset-wallet ────────────────────────────────────────────────────────

@app.post("/reset-wallet")
async def reset_wallet(request: Request):
    from connection import get_mode, client

    if get_mode() == "live":
        raise HTTPException(status_code=400, detail="Cannot reset wallet in live mode")

    try:
        body = await request.json()
    except Exception:
        body = {}

    starting = float(body.get("starting_usdt", os.getenv("STARTING_PAPER_USDT", "10000.0")))

    with client._lock:
        # Zero out all coin balances, reset USDT
        client._balances = {k: 0.0 for k in client._balances}
        client._balances["USDT"] = starting

    database.save_paper_state({"USDT": starting})
    database.set_setting("paper_starting_balance", str(starting))

    # Clear all open positions
    import trade_engine
    with trade_engine._positions_lock:
        for pos in trade_engine._positions:
            if pos.get("id"):
                try:
                    database.delete_position(pos["id"])
                except Exception:
                    pass
        trade_engine._positions.clear()
    trade_engine._rebuild_pos_index()

    # Sync reset state to Supabase
    try:
        import supabase_sync
        supabase_sync.sync_all([], starting)
    except Exception as e:
        database.log_activity(f"Supabase sync after wallet reset failed: {e}", "warn")

    database.log_activity(f"Paper wallet reset to {starting:.2f} USDT", "info")
    return {"status": "reset", "usdt_balance": starting}


# ── POST /sell/{symbol} ───────────────────────────────────────────────────────

@app.post("/sell/{symbol}")
async def force_sell(symbol: str):
    import trade_engine
    from data_collector import prices

    symbol = symbol.upper()
    positions = trade_engine.get_open_positions()
    target_pos = next((p for p in positions if p["symbol"] == symbol), None)

    if not target_pos:
        raise HTTPException(status_code=404, detail=f"No open position for {symbol}")

    price = prices.get(symbol, 0)
    if price <= 0:
        raise HTTPException(status_code=400, detail=f"No live price for {symbol}")

    sym = target_pos["symbol"]
    with trade_engine._selling_lock:
        if sym in trade_engine._selling:
            return {"status": "already selling", "symbol": symbol}
        trade_engine._selling.add(sym)
        trade_engine._selling_ts[sym] = time.time()

    trade_engine._sell_executor.submit(
        trade_engine._execute_sell, target_pos, price, "force-sell"
    )
    return {"status": "sell submitted", "symbol": symbol, "price": price}


# ── POST /sell-all ────────────────────────────────────────────────────────────

@app.post("/sell-all")
async def force_sell_all():
    import trade_engine
    from data_collector import prices

    positions = trade_engine.get_open_positions()
    if not positions:
        return {"status": "no open positions"}

    submitted = []
    skipped   = []

    for pos in positions:
        sym   = pos["symbol"]
        price = prices.get(sym, 0)
        if price <= 0:
            skipped.append(sym)
            continue
        with trade_engine._selling_lock:
            if sym in trade_engine._selling:
                skipped.append(sym)
                continue
            trade_engine._selling.add(sym)
            trade_engine._selling_ts[sym] = time.time()
        trade_engine._sell_executor.submit(
            trade_engine._execute_sell, pos, price, "force-sell-all"
        )
        submitted.append(sym)

    return {"status": "submitted", "submitted": submitted, "skipped": skipped}


# ── GET /coins ────────────────────────────────────────────────────────────────

@app.get("/coins")
async def get_coins():
    """Return approved coins from strategy.json with live signal data."""
    import trade_engine

    path = config.STRATEGY_FILE
    if not os.path.exists(path):
        return {"coins": []}

    try:
        with open(path) as f:
            strategy = json.load(f)
    except Exception:
        return {"coins": []}

    approved = strategy.get("approved_coins", [])

    with trade_engine._signal_cache_lock:
        cache = dict(trade_engine._signal_cache)

    result = []
    for coin in approved:
        sym  = coin.get("symbol", "")
        data = cache.get(sym, {})
        result.append({
            "symbol":   sym,
            "approved": coin.get("approved", False),
            "score":    data.get("score", 0),
            "signals":  data.get("signals", {}),
            "price":    data.get("price"),
            "rsi_val":  data.get("rsi_val"),
            "bb_ok":    data.get("bb_ok", True),
            "5m_ok":    data.get("5m_ok", True),
            "budget_usdt":    coin.get("budget_usdt"),
            "max_concurrent": coin.get("max_concurrent"),
            "confidence":     coin.get("confidence"),
        })

    return {"coins": result, "count": len(result)}


# ── POST /coins ───────────────────────────────────────────────────────────────

@app.post("/coins")
async def save_coins(request: Request):
    """Replace approved_coins in strategy.json."""
    try:
        coins = await request.json()
        if not isinstance(coins, list):
            raise ValueError("Expected a JSON array")
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Invalid JSON: {e}")

    path = config.STRATEGY_FILE
    try:
        if os.path.exists(path):
            with open(path) as f:
                strategy = json.load(f)
        else:
            strategy = {}
    except Exception:
        strategy = {}

    strategy["approved_coins"] = coins
    strategy["updated_at"]     = datetime.now(timezone.utc).isoformat()

    tmp = path + ".tmp"
    try:
        with open(tmp, "w") as f:
            json.dump(strategy, f, indent=2)
            f.flush()
            try:
                os.fsync(f.fileno())
            except Exception:
                pass
        os.replace(tmp, path)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not save coins: {e}")

    # Sync selected coins to Supabase
    try:
        import supabase_sync, trade_engine
        selected = [c["symbol"] for c in coins if c.get("approved")]
        usdt = 0.0
        try:
            from connection import client, get_mode
            if get_mode() != "live" and hasattr(client, "_balances"):
                with client._lock:
                    usdt = float(client._balances.get("USDT", 0.0))
        except Exception:
            pass
        supabase_sync.sync_all(trade_engine.get_open_positions(), usdt, selected_coins=selected)
    except Exception as e:
        database.log_activity(f"Supabase sync after coins save failed: {e}", "warn")

    database.log_activity(f"Approved coins updated via API: {len(coins)} coins", "info")
    return {"status": "saved", "count": len(coins)}


# ── GET /performance ──────────────────────────────────────────────────────────

@app.get("/performance")
async def get_performance():
    """Return aggregated performance stats."""
    trades = database.get_recent_trades(limit=500)
    if not trades:
        return {"message": "No trades yet"}

    total     = len(trades)
    wins      = [t for t in trades if _safe_float(t.get("net_profit")) > 0]
    losses    = [t for t in trades if _safe_float(t.get("net_profit")) <= 0]
    total_pnl = sum(_safe_float(t.get("net_profit")) for t in trades)
    avg_win   = sum(_safe_float(t.get("net_profit")) for t in wins)   / max(len(wins), 1)
    avg_loss  = sum(_safe_float(t.get("net_profit")) for t in losses) / max(len(losses), 1)
    avg_dur   = sum(_safe_float(t.get("duration_seconds")) for t in trades) / max(total, 1)

    # Per-coin breakdown
    by_coin: dict = {}
    for t in trades:
        coin = t.get("coin", "UNKNOWN")
        if coin not in by_coin:
            by_coin[coin] = {"trades": 0, "wins": 0, "pnl": 0.0}
        by_coin[coin]["trades"] += 1
        pnl = _safe_float(t.get("net_profit"))
        by_coin[coin]["pnl"] += pnl
        if pnl > 0:
            by_coin[coin]["wins"] += 1

    return {
        "total_trades":    total,
        "wins":            len(wins),
        "losses":          len(losses),
        "win_rate_pct":    round(len(wins) / max(total, 1) * 100, 1),
        "total_pnl":       round(total_pnl, 4),
        "avg_win_usdt":    round(avg_win, 4),
        "avg_loss_usdt":   round(avg_loss, 4),
        "avg_duration_sec": round(avg_dur, 1),
        "by_coin":         by_coin,
    }


# ── GET /balance ──────────────────────────────────────────────────────────────

@app.get("/balance")
async def get_balance():
    from connection import get_mode, client

    mode = get_mode()
    try:
        if mode == "live":
            acc  = client.get_account()
            bals = {b["asset"]: float(b["free"]) for b in acc["balances"] if float(b["free"]) > 0}
        else:
            with client._lock:
                bals = {k: v for k, v in client._balances.items() if v > 0}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Could not fetch balance: {e}")

    return {"mode": mode, "balances": bals, "usdt": bals.get("USDT", 0.0)}


# ── POST /sync-supabase ───────────────────────────────────────────────────────

@app.post("/sync-supabase")
async def sync_supabase():
    """Manually trigger a Supabase sync."""
    try:
        import supabase_sync, trade_engine
        from connection import get_mode, client

        mode = get_mode()
        usdt = 0.0
        if mode != "live" and hasattr(client, "_balances"):
            with client._lock:
                usdt = float(client._balances.get("USDT", 0.0))
        elif mode == "live":
            try:
                acc  = client.get_account()
                usdt = next(
                    (float(b["free"]) for b in acc["balances"] if b["asset"] == "USDT"), 0.0
                )
            except Exception:
                pass

        open_pos = trade_engine.get_open_positions()

        path = config.STRATEGY_FILE
        selected = []
        if os.path.exists(path):
            try:
                with open(path) as f:
                    strat = json.load(f)
                selected = [c["symbol"] for c in strat.get("approved_coins", []) if c.get("approved")]
            except Exception:
                pass

        result = supabase_sync.sync_all(open_pos, usdt, selected_coins=selected)
        database.log_activity("Manual Supabase sync triggered via API", "info")
        return {"status": "synced", "positions": len(open_pos), "usdt": usdt, "result": str(result)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Supabase sync failed: {e}")


# ── GET /learning ─────────────────────────────────────────────────────────────

@app.get("/learning")
async def get_learning():
    """Return the ML learning model's current parameter recommendations."""
    try:
        import learning
        params = learning.get_recommended_params()
        return {"recommended_params": params}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Learning module error: {e}")


# ── Serve React frontend ───────────────────────────────────────────────────────

_dist = Path(__file__).parent.parent / "frontend" / "dist"
if _dist.exists():
    from fastapi.staticfiles import StaticFiles as _SF
    from fastapi.responses import FileResponse as _FR

    @app.get("/{full_path:path}", include_in_schema=False)
    async def serve_spa(full_path: str):
        file_path = _dist / full_path
        if file_path.exists() and file_path.is_file():
            return _FR(file_path)
        return _FR(_dist / "index.html")

    app.mount("/assets", _SF(str(_dist / "assets"), name="assets"), )
else:
    print("[ControlAPI] No dist/ folder — API-only mode")

port = int(os.getenv("PORT", 8000))
print(f"[ControlAPI] Binding to 0.0.0.0:{port}")
uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")