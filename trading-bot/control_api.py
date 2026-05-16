"""
FastAPI control server — binds to $PORT (default 8000) on the main thread.
All trading-bot logic (DB init, history download, WebSocket feed, strategy
loop) starts in the FastAPI lifespan as async background tasks.
"""

import asyncio
import json
import os
import sys
import threading
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Optional

# Unique ID generated once per process start — changes on every restart so the
# browser can detect new deployments even when the version string is unchanged.
_DEPLOY_ID = str(uuid.uuid4())

# GitHub raw version URL — bot polls this to detect available updates.
_GITHUB_VERSION_URL = (
    "https://raw.githubusercontent.com/uplinxmarketing/Trade-/main/public/version.json"
)
_github_ver_cache: dict = {}
_github_ver_cache_ts: float = 0.0
_GITHUB_VER_TTL = 120  # re-fetch at most every 2 minutes

import uvicorn

import json as _json_v
import pathlib as _pl_v

def _read_frontend_version() -> dict:
    """Read version metadata from dist/version.json.
    Checks trading-bot/dist/ first, then the repo-root dist/, so the
    endpoint always reflects whatever the bot is actually serving."""
    for candidate in [
        _pl_v.Path(__file__).parent / "dist" / "version.json",
        _pl_v.Path(__file__).parent.parent / "dist" / "version.json",
    ]:
        try:
            if candidate.exists():
                return _json_v.loads(candidate.read_text())
        except Exception:
            pass
    return {"version": "unknown", "buildTime": "", "commit": ""}
from fastapi import FastAPI, Response, Body, Query, Request
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

import config
import database
from connection import get_mode, get_live_error, is_using_paper_fallback


# ── Lifespan: start the full trading bot after HTTP server is ready ───────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Runs once after uvicorn binds and is accepting connections.
    Railway's health-check will already be passing by the time this executes.
    """
    import data_collector
    import trade_engine
    import strategy_engine

    steps: list[str] = []
    try:
        # 1. DB (already done in main.py before uvicorn starts, but idempotent)
        database.init_db()
        steps.append("init_db OK")
        print(f"[ControlAPI] DATA DIRECTORY : {database._DATA_DIR}")
        print(f"[ControlAPI] DATABASE FILE  : {database.DB_PATH}")
        database.log_activity(f"Deploy started — DB: {database.DB_PATH}", "info")

        # 2. Ensure strategy.json exists (preserve user settings if file already present)
        strategy_engine.write_default_strategy()
        steps.append("strategy OK")

        # 2b. Record paper_starting_balance on first ever deploy (idempotent)
        if not database.get_setting("paper_starting_balance"):
            _starting = float(os.getenv("STARTING_PAPER_USDT", "10000.0"))
            database.save_setting("paper_starting_balance", str(_starting))

        # 3. Restore open positions + coins + balance from SQLite / Supabase
        trade_engine.load_positions_from_db()
        steps.append("positions OK")

        # 3b. Start REST price refresher for held positions (2s interval — critical for
        #     low-WS-volume coins that can go minutes stale without this)
        trade_engine.start_held_position_refresher()
        steps.append("held_price_refresher OK")
        trade_engine.start_capital_recycler()
        steps.append("capital_recycler OK")

        # 4. Apply startup defaults and auto-resume logic.
        #    stop_loss and smart_hold are ALWAYS forced OFF on every deploy —
        #    the user must explicitly enable them each session.
        #    trading_active is preserved so a running bot resumes after a redeploy.
        _s = _load_strategy()
        _auto_patch: dict = {
            "pause_reason": None,
        }
        if "trading_active" not in _s:
            # Brand-new deploy — don't auto-start, let user press Start
            _auto_patch["trading_active"] = False
        # else: preserve existing trading_active (resumes running bot after redeploy)
        if not _s.get("initial_balance_usdt"):
            _auto_patch["initial_balance_usdt"] = _get_usdt_balance() or float(os.getenv("STARTING_PAPER_USDT", "10000.0"))
        _write_strategy_patch(_auto_patch)
        steps.append(f"trading_active={'resume' if _s.get('trading_active') else 'off'}")

        # 5. History download — daemon thread, never blocks health-check
        threading.Thread(target=data_collector.download_history, daemon=True).start()
        steps.append("history_dl started")

        # 6. Register price/kline callbacks
        data_collector.register_price_callback(trade_engine.realtime_monitor)
        data_collector.register_kline_callback(trade_engine.update_coin_signals)
        steps.append("callbacks OK")

        # 7. Launch async tasks
        asyncio.create_task(data_collector.start_websocket())
        asyncio.create_task(strategy_engine.strategy_loop())
        asyncio.create_task(trade_engine.signal_scanner(data_collector.prices))
        asyncio.create_task(trade_engine.position_guardian())
        asyncio.create_task(_supabase_periodic_sync())
        steps.append("async tasks launched")

        # 8. Futures paper-trading agent (completely separate parallel process)
        if config.FUTURES_ENABLED:
            import futures_engine
            futures_engine.init_futures_engine()
            asyncio.create_task(futures_engine.mark_price_loop())
            asyncio.create_task(futures_engine.signal_scanner_loop())
            steps.append("futures tasks launched")

        msg = "Bot ready — " + " | ".join(steps)
        print(f"[ControlAPI] {msg}")
        database.log_activity(msg, "info")

    except Exception as exc:
        err = f"STARTUP ERROR at step {steps[-1] if steps else '?'}: {exc}"
        print(f"[ControlAPI] {err}")
        try:
            database.log_activity(err, "error")
        except Exception:
            pass
        # Do NOT re-raise — let uvicorn keep running so health-check passes
        # and the /api/activity endpoint can show the error.

    yield
    # Shutdown — daemon threads and tasks stop with the process


app = FastAPI(title="Trading Bot Control API", version="1.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# ── Periodic Supabase sync — ensures data survives Railway redeploys ──────────

async def _supabase_periodic_sync():
    """Every 2 minutes push current balance + open positions to Supabase."""
    import asyncio as _aio
    while True:
        await _aio.sleep(120)   # 2 minutes
        try:
            from trade_engine import get_open_positions
            import supabase_sync
            positions = get_open_positions()
            usdt = _get_usdt_balance()
            supabase_sync.sync_all(positions, usdt)
        except Exception as e:
            print(f"[PeriodicSync] Supabase sync error: {e}")


# ── Internal helpers ─────────────────────────────────────────────

def _load_strategy() -> dict:
    try:
        with open(config.STRATEGY_FILE) as f:
            return json.load(f)
    except Exception:
        return {}


_strategy_write_lock = threading.Lock()


def _write_strategy_patch(patch: dict):
    """Atomic merge-and-write to strategy.json (lock-protected against concurrent saves).

    Without atomicity, concurrent readers (sell monitor, signal scanner) may
    catch the file mid-truncate and json.load raises — which silently turns
    every default into the schema fallback (e.g. trading_active drops to True
    or take_profit_mult resets to breakeven mid-trade)."""
    with _strategy_write_lock:
        s = _load_strategy()
        s.update(patch)
        tmp_path = config.STRATEGY_FILE + ".tmp"
        with open(tmp_path, "w") as f:
            json.dump(s, f, indent=2)
            f.flush()
            try:
                os.fsync(f.fileno())
            except Exception:
                pass
        os.replace(tmp_path, config.STRATEGY_FILE)
    # Bust the /api/all response cache so a poll right after a write sees the
    # updated trading_active / settings / approved coins immediately.
    try:
        _API_ALL_CACHE["data"] = None
    except NameError:
        pass


def _enrich_position(pos: dict) -> dict:
    """Add hold_minutes and hold_human to a position dict in-place."""
    ts = pos.get("timestamp")
    if ts:
        try:
            ts_str = ts.replace("Z", "+00:00") if isinstance(ts, str) else None
            if ts_str:
                opened  = datetime.fromisoformat(ts_str)
                age_sec = (datetime.now(timezone.utc) - opened).total_seconds()
                pos["hold_minutes"] = round(age_sec / 60, 1)
                hours = int(age_sec // 3600)
                mins  = int((age_sec % 3600) // 60)
                pos["hold_human"] = f"{hours}h {mins}m" if hours > 0 else f"{mins}m"
        except Exception:
            pass
    return pos


def _get_positions():
    try:
        from trade_engine import get_open_positions, _rest_px, _signal_cache, _signal_cache_lock
        from data_collector import prices
        pos = get_open_positions()
        out = []
        for p in pos:
            sym    = p["symbol"]
            # Price priority chain — WebSocket first (sub-second), REST fallback (2 s)
            #   1. WebSocket prices dict (sub-second, most accurate for live mode)
            #   2. _rest_px  (REST refresh every 2 s — good fallback)
            #   3. Latest cached signal price (60 s old at worst)
            price  = prices.get(sym, 0) or _rest_px.get(sym, 0)
            if not price:
                with _signal_cache_lock:
                    sc_entry = _signal_cache.get(sym)
                if sc_entry and sc_entry.get("price", 0) > 0:
                    price = sc_entry["price"]
            # Final fallback: direct Binance REST ticker for coins not in WS stream
            if not price:
                try:
                    from connection import client
                    ticker = client.get_symbol_ticker(symbol=sym)
                    price = float(ticker.get("price", 0))
                except Exception:
                    pass
            entry  = p.get("entry_price", 0)
            qty    = p.get("quantity", 0)
            try:
                from trade_engine import _get_breakeven_mult as _gbm, _user_tp_mult as _utpm, _take_profit_enabled as _tpe
                _bep_m_pos = p.get("breakeven_mult_at_buy") or (_gbm(entry, p.get("symbol", "")) if entry else 1.002)
                _bep_pos   = entry * _bep_m_pos if entry else 0
                target     = p.get("exit_target") or (max(_bep_pos, entry * _utpm) if _tpe and entry else _bep_pos)
            except Exception:
                target = p.get("exit_target") or (entry * 1.003 if entry else 0)
                _bep_pos = target
            pnl    = (price - entry) * qty if price and entry else 0
            dist   = ((price - target) / target * 100) if target and price else 0
            row = _enrich_position({
                **p,
                "avg_entry_price": entry,
                "current_price":   price,
                "exit_target":     round(target, 8),
                "breakeven_price": round(_bep_pos, 6),
                "unrealized_pnl":  round(pnl, 4),
                "dist_to_exit_pct": round(dist, 4),
                "dist_to_bep_pct":  round(((price - _bep_pos) / _bep_pos * 100) if _bep_pos and price else 0, 4),
                "profitable":      price >= _bep_pos if price and _bep_pos else False,
            })
            try:
                import trade_engine as _te_rbep
                _real_bep = _te_rbep.compute_real_breakeven_price(p)
                if _real_bep > 0:
                    row["breakeven_price_real"] = round(_real_bep, 8)
                    # Override simple BEP with real BEP so UI reflects actual sell threshold
                    row["breakeven_price"] = round(_real_bep, 8)
                    row["profitable"] = bool(price >= _real_bep) if price else False
                    # ready_to_sell = price is above BOTH real_bep AND exit_target (bot will sell)
                    _exit_t_all = target  # already computed above
                    row["ready_to_sell"] = bool(price >= max(_real_bep, _exit_t_all)) if price else False
                    if price > 0:
                        _gap_pct = round((_real_bep - price) / price * 100, 4)
                        row["real_bep_gap_pct"]        = _gap_pct
                        row["real_bep_distance_usdt"]  = round((_real_bep - price) * float(p.get("quantity", 0)), 4)
                        row["is_trapped"]              = bool(_gap_pct > 2.0)
                        row["dist_to_bep_pct"]        = round((price - _real_bep) / _real_bep * 100, 4)
            except Exception:
                pass
            out.append(row)
        return out
    except Exception:
        return []


def _get_signal_snapshot() -> list:
    """Return a compact snapshot of the live signal cache for each watched coin."""
    try:
        from trade_engine import _signal_cache, _signal_cache_lock
        with _signal_cache_lock:
            snap = dict(_signal_cache)
        result = []
        for sym, entry in snap.items():
            sig  = entry.get("signals", {})
            result.append({
                "symbol":  sym,
                "price":   entry.get("price", 0),
                "score":   entry.get("score", 0),
                "rsi":     entry.get("rsi_val", 0),
                "bb_ok":   entry.get("bb_ok", True),
                "5m_ok":   entry.get("5m_ok", True),
                "trend":   bool(sig.get("trend")),
                "rsi_ok":  bool(sig.get("rsi")),
                "macd":    bool(sig.get("macd")),
                "volume":  bool(sig.get("volume")),
                "obv":     bool(sig.get("obv")),
                "atr":     bool(sig.get("atr")),
                "ts":      entry.get("ts", 0),
            })
        return result
    except Exception:
        return []


def _sell_monitor_alive() -> bool:
    try:
        import trade_engine as _te
        hb = _te._sell_monitor_heartbeat
        # Heartbeat is set at the START of each 5 s loop iteration.
        # Allow 15 s window (5 s sleep + up to 10 s for work) before calling it dead.
        return hb > 0 and (time.time() - hb) < 15.0
    except Exception:
        return False


# Cache Binance account balance — refreshed at most every 5 s so we don't
# hammer the REST API on every frontend poll.
_acct_cache: dict = {}
_acct_cache_ts: float = 0.0
_ACCT_CACHE_TTL = 20.0
_acct_cache_lock = threading.Lock()

def _get_cached_account() -> dict:
    """Return cached Binance account dict, refreshing at most every 5 s."""
    global _acct_cache, _acct_cache_ts
    now = time.time()
    with _acct_cache_lock:
        if now - _acct_cache_ts < _ACCT_CACHE_TTL and _acct_cache:
            return _acct_cache
    try:
        from connection import client
        acc = client.get_account()
        with _acct_cache_lock:
            _acct_cache = acc
            _acct_cache_ts = now
        return acc
    except Exception:
        with _acct_cache_lock:
            return _acct_cache  # return stale on error


def _get_usdt_balance() -> float:
    """Returns free USDT only — used for trade budget calculations."""
    try:
        from connection import get_mode
        if get_mode() != "live":
            from connection import client
            if hasattr(client, "_balances"):
                with client._lock:
                    return float(client._balances.get("USDT", 0.0))
        acc = _get_cached_account()
        for b in acc.get("balances", []):
            if b["asset"] == "USDT":
                return float(b["free"])
    except Exception:
        pass
    return float(os.getenv("STARTING_PAPER_USDT", "10000.0"))


def _get_usdt_display_balance() -> float:
    """Returns free+locked USDT — matches what Binance UI shows."""
    try:
        from connection import get_mode
        if get_mode() != "live":
            from connection import client
            if hasattr(client, "_balances"):
                with client._lock:
                    return float(client._balances.get("USDT", 0.0))
            return float(os.getenv("STARTING_PAPER_USDT", "10000.0"))
        acc = _get_cached_account()
        for b in acc.get("balances", []):
            if b["asset"] == "USDT":
                return float(b["free"]) + float(b["locked"])
    except Exception:
        pass
    return float(os.getenv("STARTING_PAPER_USDT", "10000.0"))


def _overall_win_rate() -> float:
    stats = database.get_trade_stats(mode=get_mode())
    total = stats["total"]
    return stats["wins"] / total if total else 0.0


def _get_initial_balance() -> float:
    """Return the appropriate starting balance baseline for the current mode.
    - Paper mode: use saved paper_starting_balance or STARTING_PAPER_USDT env var
    - Live mode:  use live_starting_balance (first-seen balance after going live);
                  snapshots current balance on first call so P&L is measured from
                  when live mode actually started, not the paper default of $10,000.
    """
    if get_mode() == "live":
        live_start = database.get_setting("live_starting_balance")
        if live_start:
            return float(live_start)
        # First time in live mode — snapshot current balance as the baseline
        try:
            from trade_engine import _get_usdt_balance as _teb, get_open_positions as _gop
            usdt = _teb()
            pos_value = sum(p.get("budget_usdt", 0) for p in _gop())
            baseline = usdt + pos_value
            if baseline > 0:
                database.save_setting("live_starting_balance", str(baseline))
                return baseline
        except Exception:
            pass
        return 0.0
    # Paper mode (unchanged)
    starting_str = database.get_setting("paper_starting_balance")
    if starting_str:
        return float(starting_str)
    return float(os.getenv("STARTING_PAPER_USDT", "10000.0"))


# ── Endpoints ─────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"ok": True, "mode": get_mode(), "ts": datetime.now(timezone.utc).isoformat()}


@app.get("/status")
def status():
    strategy = _load_strategy()
    return {
        "mode":           get_mode(),
        "live_error":     get_live_error() or None,
        "trading_active": strategy.get("trading_active", False),
        "pause_reason":   strategy.get("pause_reason"),
        "open_positions": _get_positions(),
        "usdt_balance":   _get_usdt_display_balance(),
        "trades_today":   database.get_trade_stats(mode=get_mode())["trades_today"],
        "win_rate":       round(_overall_win_rate(), 3),
        "strategy_updated_at": strategy.get("updated_at"),
    }


@app.get("/trades")
def trades():
    return database.get_recent_trades(limit=50)


@app.get("/patterns")
def patterns():
    return database.get_patterns(min_occurrences=1)


@app.post("/pause")
def pause():
    _write_strategy_patch({"trading_active": False, "pause_reason": "Paused via API"})
    return {"ok": True, "trading_active": False}


@app.post("/resume")
def resume():
    _write_strategy_patch({"trading_active": True, "pause_reason": None})
    return {"ok": True, "trading_active": True}


@app.post("/budget/{amount}")
def set_budget(amount: float):
    if amount < 1:
        return {"error": "Budget must be >= 1 USDT"}
    s = _load_strategy()
    for coin in s.get("approved_coins", []):
        coin["budget_usdt"] = amount
    with open(config.STRATEGY_FILE, "w") as f:
        json.dump(s, f, indent=2)
    return {"ok": True, "new_budget": amount}


def _config_response():
    strategy = _load_strategy()
    return {
        "budget_mode":           strategy.get("budget_mode",           config.BUDGET_MODE),
        "budget_fixed_usdt":     strategy.get("budget_fixed_usdt",     config.BUDGET_FIXED_USDT),
        "budget_pct_of_free":    strategy.get("budget_pct_of_free",    config.BUDGET_PCT_OF_FREE),
        "budget_total_cap_usdt": strategy.get("budget_total_cap_usdt", config.BUDGET_TOTAL_CAP_USDT),
        "budget_per_coin":       strategy.get("budget_per_coin",       config.BUDGET_PER_COIN),
        "budget_coin_pct":       strategy.get("budget_coin_pct",       {}),
        "bot_allocation_usdt":   strategy.get("bot_allocation_usdt",   config.BOT_ALLOCATION_USDT),
    }

def _config_patch(body: dict):
    allowed_keys = {
        "budget_mode", "budget_fixed_usdt", "budget_pct_of_free",
        "budget_total_cap_usdt", "budget_per_coin", "budget_coin_pct",
        "bot_allocation_usdt",
    }
    try:
        patch = {k: v for k, v in body.items() if k in allowed_keys}
        if not patch:
            return {"ok": False, "error": "No valid config keys provided"}
        _write_strategy_patch(patch)
        return {"ok": True, "updated": list(patch.keys()), "config": patch}
    except Exception as e:
        database.log_activity(f"Config save error: {e}", "error")
        return Response(
            content=json.dumps({"ok": False, "error": str(e)}),
            status_code=500, media_type="application/json"
        )

@app.get("/config")
def get_config(): return _config_response()

@app.get("/api/config")
def api_get_config(): return _config_response()

@app.post("/config")
def post_config(body: dict = Body(...)): return _config_patch(body)

@app.post("/api/config")
def api_post_config(body: dict = Body(...)): return _config_patch(body)


@app.post("/mode/{mode}")
def set_mode(mode: str):
    if mode not in ("paper", "testnet", "live"):
        return {"error": "mode must be paper | testnet | live"}
    return {
        "ok": True,
        "warning": f"Change MODE={mode} in .env then restart the bot.",
        "current_mode": get_mode(),
    }


# ── HTML Dashboard ─────────────────────────────────────────────────────────────────

DASHBOARD_HTML = """<!DOCTYPE html>
<html lang=\"en\">
<head>
<meta charset=\"UTF-8\">
<meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">
<title>Trading Bot Dashboard</title>
<style>
  :root { --gain:#22c55e; --loss:#ef4444; --warn:#f59e0b; --accent:#6366f1; }
  * { box-sizing:border-box; margin:0; padding:0; }
  body { background:#0f1117; color:#e2e8f0; font-family:'Segoe UI',system-ui,sans-serif; font-size:14px; }
  .banner { padding:10px 20px; font-weight:700; font-size:13px; text-align:center; letter-spacing:.5px; }
  .banner.paper   { background:#92400e; color:#fef3c7; }
  .banner.testnet { background:#1e3a8a; color:#bfdbfe; }
  .banner.live    { background:#7f1d1d; color:#fee2e2; }
  .container { max-width:1200px; margin:0 auto; padding:20px; }
  h2 { font-size:16px; font-weight:600; margin-bottom:12px; color:#94a3b8; text-transform:uppercase; letter-spacing:.8px; }
  .grid-4 { display:grid; grid-template-columns:repeat(4,1fr); gap:12px; margin-bottom:24px; }
  .card { background:#1e2130; border:1px solid #2d3348; border-radius:8px; padding:14px; }
  .card .label { font-size:10px; text-transform:uppercase; letter-spacing:.8px; color:#64748b; margin-bottom:4px; }
  .card .value { font-size:22px; font-weight:700; font-family:monospace; }
  .gain { color:var(--gain); } .loss { color:var(--loss); } .warn { color:var(--warn); } .accent { color:var(--accent); }
  table { width:100%; border-collapse:collapse; margin-bottom:24px; }
  th { text-align:left; padding:8px 10px; font-size:10px; text-transform:uppercase; letter-spacing:.8px;
       color:#64748b; border-bottom:1px solid #2d3348; white-space:nowrap; }
  td { padding:8px 10px; border-bottom:1px solid #1a1f30; font-size:13px; white-space:nowrap; }
  tr:hover td { background:#1a1f30; }
  .pill { display:inline-block; padding:2px 7px; border-radius:4px; font-size:11px; font-weight:600; }
  .pill-gain { background:rgba(34,197,94,.15); color:var(--gain); border:1px solid rgba(34,197,94,.3); }
  .pill-loss { background:rgba(239,68,68,.15); color:var(--loss); border:1px solid rgba(239,68,68,.3); }
  .pill-wait { background:rgba(245,158,11,.15); color:var(--warn); border:1px solid rgba(245,158,11,.3); }
  .pill-hold { background:rgba(99,102,241,.15); color:var(--accent); border:1px solid rgba(99,102,241,.3); }
  .progress-bar { width:100%; height:6px; background:#2d3348; border-radius:3px; overflow:hidden; }
  .progress-fill { height:100%; border-radius:3px; transition:width .3s; }
  .btn { padding:7px 16px; border:none; border-radius:5px; cursor:pointer; font-size:12px; font-weight:600; margin-right:6px; }
  .btn-pause  { background:#f59e0b; color:#000; }
  .btn-resume { background:var(--gain); color:#000; }
  .btn-refresh{ background:#2d3348; color:#e2e8f0; }
  .controls { margin-bottom:20px; display:flex; align-items:center; gap:8px; }
  .live-dot { width:8px; height:8px; border-radius:50%; background:var(--gain); animation:pulse 1.5s infinite; display:inline-block; }
  @keyframes pulse { 0%,100%{opacity:1} 50%{opacity:.4} }
  .section { margin-bottom:32px; }
  .mono { font-family:monospace; }
  .text-right { text-align:right; }
</style>
</head>
<body>
<div id=\"banner\" class=\"banner paper\">PAPER MODE — simulated trading only, no real money</div>

<div class=\"container\">
  <div class=\"controls\">
    <span class=\"live-dot\"></span>
    <span id=\"last-update\" style=\"color:#64748b;font-size:12px;\">Connecting…</span>
    <button class=\"btn btn-pause\"   onclick=\"pause()\">⏸ Pause</button>
    <button class=\"btn btn-resume\"  onclick=\"resume()\">▶ Resume</button>
    <button class=\"btn btn-refresh\" onclick=\"refresh()\">↻ Refresh</button>
  </div>

  <div class=\"grid-4\" id=\"metrics\">
    <div class=\"card\"><div class=\"label\">USDT Balance</div><div class=\"value\" id=\"m-balance\">—</div></div>
    <div class=\"card\"><div class=\"label\">Open Positions</div><div class=\"value accent\" id=\"m-open\">—</div></div>
    <div class=\"card\"><div class=\"label\">Trades Today</div><div class=\"value\" id=\"m-today\">—</div></div>
    <div class=\"card\"><div class=\"label\">Win Rate</div><div class=\"value\" id=\"m-winrate\">—</div></div>
  </div>

  <div class=\"section\">
    <h2>Open Positions</h2>
    <table id=\"positions-table\">
      <thead><tr>
        <th>Symbol</th><th>Entry $</th><th>Current $</th><th>BEP $</th>
        <th>Qty</th><th>Budget</th><th>Unrealised P&L</th><th>Dist to BEP</th><th>Status</th>
      </tr></thead>
      <tbody id=\"positions-body\"><tr><td colspan=\"9\" style=\"color:#64748b;text-align:center\">Loading…</td></tr></tbody>
    </table>
  </div>

  <div class=\"section\">
    <h2>Recent Trades</h2>
    <table id=\"trades-table\">
      <thead><tr>
        <th>Coin</th><th>Entry $</th><th>Exit $</th><th>Qty</th>
        <th>Budget</th><th>Buy Fee</th><th>Sell Fee</th>
        <th>Net P&L</th><th>Duration</th><th>Result</th>
      </tr></thead>
      <tbody id=\"trades-body\"><tr><td colspan=\"10\" style=\"color:#64748b;text-align:center\">Loading…</td></tr></tbody>
    </table>
  </div>

  <div class=\"section\">
    <h2>Learned Patterns</h2>
    <table id=\"patterns-table\">
      <thead><tr>
        <th>Coin</th><th>RSI Range</th><th>BB Position</th><th>Volume</th>
        <th>MA</th><th>Occurrences</th><th>Confidence</th><th>Avg Profit %</th>
      </tr></thead>
      <tbody id=\"patterns-body\"><tr><td colspan=\"8\" style=\"color:#64748b;text-align:center\">Loading…</td></tr></tbody>
    </table>
  </div>
</div>

<script>
const fmt = (n, dp=2) => (n == null ? '—' : Number(n).toLocaleString('en-US',{minimumFractionDigits:dp,maximumFractionDigits:dp}));
const fmtP = p => p>=1000 ? fmt(p,2) : p>=1 ? fmt(p,4) : fmt(p,6);

async function fetchStatus() {
  const r = await fetch('/status');
  return r.json();
}
async function fetchTrades() {
  const r = await fetch('/trades');
  return r.json();
}
async function fetchPatterns() {
  const r = await fetch('/patterns');
  return r.json();
}

function renderPositions(positions) {
  const tbody = document.getElementById('positions-body');
  if (!positions.length) {
    tbody.innerHTML = '<tr><td colspan=\"9\" style=\"color:#64748b;text-align:center\">No open positions</td></tr>';
    return;
  }
  tbody.innerHTML = positions.map(p => {
    const pnl = p.unrealized_pnl || 0;
    const dist = p.dist_to_bep_pct || 0;
    const isProfitable = p.profitable;
    const distPct = Math.min(100, Math.max(0, 50 + dist * 25));
    const barColor = isProfitable ? '#22c55e' : '#ef4444';
    const pill = isProfitable
      ? '<span class=\"pill pill-gain\">✅ Profitable</span>'
      : '<span class=\"pill pill-wait\">⏳ Waiting</span>';
    return `<tr>
      <td class=\"mono\">${p.symbol}</td>
      <td class=\"mono\">${fmtP(p.entry_price)}</td>
      <td class=\"mono ${isProfitable?'gain':'loss'}\">${fmtP(p.current_price)}</td>
      <td class=\"mono accent\">${fmtP(p.breakeven_price)}</td>
      <td class=\"mono\">${fmt(p.quantity,6)}</td>
      <td class=\"mono\">${fmt(p.budget_usdt,2)} USDT</td>
      <td class=\"mono ${pnl>=0?'gain':'loss'}\">${pnl>=0?'+':''}${fmt(pnl,4)} USDT</td>
      <td>
        <div class=\"progress-bar\" title=\"${fmt(dist,4)}% to BEP\">
          <div class=\"progress-fill\" style=\"width:${distPct}%;background:${barColor}\"></div>
        </div>
        <div style=\"font-size:10px;color:#64748b;margin-top:2px\">${dist>=0?'+':''}${fmt(dist,4)}%</div>
      </td>
      <td>${pill}</td>
    </tr>`;
  }).join('');
}

function renderTrades(trades) {
  const tbody = document.getElementById('trades-body');
  if (!trades.length) {
    tbody.innerHTML = '<tr><td colspan=\"10\" style=\"color:#64748b;text-align:center\">No completed trades yet</td></tr>';
    return;
  }
  tbody.innerHTML = trades.map(t => {
    const pnl = t.net_profit || 0;
    const dur = t.duration_seconds ? (t.duration_seconds >= 3600
      ? fmt(t.duration_seconds/3600,1)+'h'
      : Math.round(t.duration_seconds/60)+'m') : '—';
    const pill = t.profitable
      ? '<span class=\"pill pill-gain\">WIN</span>'
      : '<span class=\"pill pill-loss\">LOSS</span>';
    return `<tr>
      <td class=\"mono\">${t.coin}</td>
      <td class=\"mono\">${fmtP(t.entry_price)}</td>
      <td class=\"mono\">${fmtP(t.exit_price)}</td>
      <td class=\"mono\">${fmt(t.quantity,6)}</td>
      <td class=\"mono\">${fmt(t.budget_usdt,2)}</td>
      <td class=\"mono warn\">${fmt(t.buy_fee,4)}</td>
      <td class=\"mono warn\">${fmt(t.sell_fee,4)}</td>
      <td class=\"mono ${pnl>=0?'gain':'loss'}\">${pnl>=0?'+':''}${fmt(pnl,4)}</td>
      <td class=\"mono\">${dur}</td>
      <td>${pill}</td>
    </tr>`;
  }).join('');
}

function renderPatterns(patterns) {
  const tbody = document.getElementById('patterns-body');
  if (!patterns.length) {
    tbody.innerHTML = '<tr><td colspan=\"8\" style=\"color:#64748b;text-align:center\">No patterns yet — patterns build after 3+ trades</td></tr>';
    return;
  }
  tbody.innerHTML = patterns.map(p => {
    const conf = (p.confidence_score || 0) * 100;
    const col = conf >= 65 ? 'gain' : conf >= 40 ? 'warn' : 'loss';
    return `<tr>
      <td class=\"mono\">${p.coin}</td>
      <td class=\"mono\">${p.rsi_range||'—'}</td>
      <td>${p.bb_position||'—'}</td>
      <td>${p.volume_trend||'—'}</td>
      <td>${p.ma_position||'—'}</td>
      <td class=\"text-right mono\">${p.occurrence_count}</td>
      <td class=\"text-right mono ${col}\">${fmt(conf,1)}%</td>
      <td class=\"text-right mono ${(p.avg_profit_pct||0)>=0?'gain':'loss'}\">${(p.avg_profit_pct||0)>=0?'+':''}${fmt(p.avg_profit_pct,3)}%</td>
    </tr>`;
  }).join('');
}

async function refresh() {
  try {
    const [status, trades, patterns] = await Promise.all([fetchStatus(), fetchTrades(), fetchPatterns()]);
    const mode = status.mode || 'paper';
    const banner = document.getElementById('banner');
    banner.className = 'banner ' + mode;
    banner.textContent = mode === 'paper'   ? 'PAPER MODE — simulated trading only, no real money'
                       : mode === 'testnet' ? 'TESTNET — Binance testnet, fake funds'
                       :                     '⚠️ LIVE TRADING — REAL MONEY AT RISK';

    document.getElementById('m-balance').textContent  = '$' + fmt(status.usdt_balance,2) + (mode==='paper'?' (paper)':'');
    document.getElementById('m-open').textContent     = status.open_positions?.length ?? 0;
    document.getElementById('m-today').textContent    = status.trades_today ?? 0;
    const wr = (status.win_rate || 0) * 100;
    const wrEl = document.getElementById('m-winrate');
    wrEl.textContent = fmt(wr,1) + '%';
    wrEl.className = 'value ' + (wr >= 55 ? 'gain' : wr >= 40 ? 'warn' : 'loss');

    renderPositions(status.open_positions || []);
    renderTrades(trades);
    renderPatterns(patterns);

    document.getElementById('last-update').textContent =
      'Updated ' + new Date().toLocaleTimeString();
  } catch(e) {
    document.getElementById('last-update').textContent = 'Error: ' + e.message;
  }
}

async function pause()  { await fetch('/pause', {method:'POST'}); refresh(); }
async function resume() { await fetch('/resume',{method:'POST'}); refresh(); }

refresh();
setInterval(refresh, 5000);  // auto-refresh every 5s
</script>
</body>
</html>
"""


@app.get("/api/wallet")
def api_wallet():
    try:
        from connection import client
        from trade_engine import get_open_positions, _rest_px
        from data_collector import prices as live_prices

        acc = _get_cached_account()
        balances = [
            {
                "asset":  b["asset"],
                "free":   float(b["free"]),
                "locked": float(b["locked"]),
                "total":  float(b["free"]) + float(b["locked"]),
            }
            for b in acc.get("balances", [])
            if float(b["free"]) + float(b["locked"]) > 0
        ]
        # Trading uses only free USDT; display shows free+locked to match Binance UI
        usdt_free  = sum(b["free"]  for b in balances if b["asset"] == "USDT")
        usdt_total = sum(b["total"] for b in balances if b["asset"] == "USDT")

        # Total portfolio value = free USDT + mark-to-market value of open positions
        total_value = usdt_free
        for pos in get_open_positions():
            sym = pos["symbol"]
            px  = _rest_px.get(sym) or live_prices.get(sym) or pos["entry_price"]
            total_value += pos["quantity"] * px

        # Realized P&L: single source of truth — SQL SUM from trades table
        _mode        = get_mode()
        realized_pnl = database.get_realized_pnl(mode=_mode)

        # Session P&L: current total portfolio value minus the mode-appropriate starting balance
        starting_bal  = _get_initial_balance()
        session_pnl   = round(total_value - starting_bal, 4)

        _paper_fallback = is_using_paper_fallback()
        return {
            "balances":              balances,
            "total_usdt":            round(usdt_total, 4),
            "free_usdt":             round(usdt_free, 4),
            "total_value":           round(total_value, 4),
            "realized_pnl":          round(realized_pnl, 4),
            "session_pnl":           session_pnl,
            "starting_balance":      round(starting_bal, 4),
            "mode":                  _mode,
            "using_paper_fallback":  _paper_fallback,
            "is_paper_data":         _paper_fallback or _mode == "paper",
        }
    except Exception as e:
        return {"balances": [], "total_usdt": 0.0, "total_value": 0.0,
                "realized_pnl": 0.0, "session_pnl": 0.0, "mode": get_mode(), "error": str(e)}


@app.post("/api/wallet/reset_live_baseline")
def api_reset_live_baseline():
    """Snapshot current live balance as the new P&L starting baseline.
    Call this after switching to live mode so session P&L starts from your
    real balance instead of the $10,000 paper default."""
    if get_mode() != "live":
        return {"error": "Not in live mode"}
    try:
        from trade_engine import _get_usdt_balance as _teb, get_open_positions as _gop
        usdt = _teb()
        pos_value = sum(p.get("budget_usdt", 0) for p in _gop())
        baseline = usdt + pos_value
        if baseline <= 0:
            return {"error": "Cannot determine balance"}
        database.save_setting("live_starting_balance", str(baseline))
        return {"ok": True, "live_starting_balance": round(baseline, 4)}
    except Exception as e:
        return {"error": str(e)}


@app.get("/bot-dashboard", response_class=HTMLResponse)
def dashboard():
    """Python bot dashboard — accessible at /bot-dashboard when React app is at /."""
    return HTMLResponse(DASHBOARD_HTML)


# ── Start as daemon thread ───────────────────────────────────────────────────────────────

class ClaudeToggleRequest(BaseModel):
    enabled: bool


@app.post("/api/strategy/claude-toggle")
def api_claude_toggle(req: ClaudeToggleRequest):
    """Enable or disable the Claude AI strategy agent without editing files."""
    _write_strategy_patch({"claude_agent_enabled": bool(req.enabled)})
    return {"ok": True, "claude_agent_enabled": bool(req.enabled)}


@app.get("/api/strategy/claude-status")
def api_claude_status():
    """Return current Claude agent toggle state and whether a key is configured."""
    s = _load_strategy()
    api_key = os.getenv("ANTHROPIC_API_KEY", "").strip()
    key_ok = bool(api_key) and not api_key.startswith("#")
    return {
        "claude_agent_enabled": bool(s.get("claude_agent_enabled", True)),
        "api_key_configured":   key_ok,
    }


@app.get("/api/status")
def _get_market_health() -> dict:
    """Summarise current signal cache into a market-health verdict."""
    try:
        from trade_engine import _signal_cache, _signal_cache_lock
        with _signal_cache_lock:
            snap = dict(_signal_cache)
        total = len(snap)
        if total == 0:
            return {"verdict": "UNKNOWN", "explanation": "Signal cache empty"}
        healthy_5m      = sum(1 for s in snap.values() if s.get("5m_ok"))
        downtrend_pct   = round((total - healthy_5m) / total * 100, 1)
        avg_score       = round(sum(s.get("score", 0) for s in snap.values()) / total, 1)
        verdict = ("BEARISH" if downtrend_pct > 60 else "BULLISH" if downtrend_pct < 30 else "MIXED")
        explanation = (
            f"{downtrend_pct}% of coins in 5m downtrend — sells may be delayed"
            if downtrend_pct > 60 else
            f"Market mixed, avg score {avg_score}/6 — normal trading"
        )
        return {
            "downtrend_5m_pct": downtrend_pct,
            "avg_signal_score": avg_score,
            "coins_tracked":    total,
            "verdict":          verdict,
            "explanation":      explanation,
        }
    except Exception:
        return {}


def api_status():
    strategy = _load_strategy()
    # Use aggregated SQL so total/wins/losses/pnl/trades_today all cover the
    # same full dataset — not just the last 500 rows returned by get_recent_trades.
    stats      = database.get_trade_stats(mode=get_mode())
    all_stats  = database.get_trade_stats_all_modes()
    balance    = round(_get_usdt_balance(), 2)
    initial    = _get_initial_balance() or balance
    approved   = [c["symbol"] for c in strategy.get("approved_coins", []) if c.get("approved")]
    wins       = stats["wins"]
    total      = stats["total"]
    return {
        "running":                strategy.get("trading_active", False),
        "mode":                   get_mode(),
        "live_error":             get_live_error() or None,
        "using_paper_fallback":   is_using_paper_fallback(),
        "balance_usdt":        balance,
        "paper_balance":       balance,
        "initial_balance":     initial,
        "open_positions":      len(_get_positions()),
        "trades_today":        stats["trades_today"],
        "win_rate":            round(wins / total, 3) if total else 0.0,
        "wins":                wins,
        "losses":              stats["losses"],
        "total_trades":        total,
        "realized_pnl":        round(stats["realized_pnl"], 4),
        "today_realized_pnl":  round(stats["today_realized_pnl"], 4),
        "locked_profit":       round(stats["locked_profit"], 4),
        "total_fees":          round(stats["total_fees"], 4),
        "all_time_trades":     all_stats["total"],
        "all_time_realized_pnl": round(all_stats["realized_pnl"], 4),
        "all_time_win_rate":   all_stats["win_rate"],
        "watched_coins":       approved or config.WATCHED_COINS,
        "data_dir":            database._DATA_DIR,
        "db_path":             database.DB_PATH,
        "data_persistent":     database.is_data_persistent(),
        "sell_monitor_alive":  _sell_monitor_alive(),
        "market_health":       _get_market_health(),
    }


@app.get("/api/positions")
def api_positions():
    return {"positions": _get_positions()}


@app.get("/api/trades")
def api_trades():
    return {"trades": database.get_recent_trades(limit=200)}


@app.get("/api/stats")
def api_stats(
    date_from: Optional[str] = Query(None, alias="from"),
    date_to:   Optional[str] = Query(None, alias="to"),
):
    """
    Aggregated trade stats + filtered trade list for a date range.

    Query params (both optional, YYYY-MM-DD format):
      ?from=2026-05-01&to=2026-05-11
    When omitted, returns all-time stats.
    """
    mode   = get_mode()
    stats  = database.get_stats_for_range(mode=mode, date_from=date_from, date_to=date_to)
    trades = database.get_trades_for_range(mode=mode, date_from=date_from, date_to=date_to, limit=500)
    return {
        **stats,
        "date_from": date_from,
        "date_to":   date_to,
        "trades":    trades,
    }


@app.get("/api/stats/summary")
def api_stats_summary():
    """Single source of truth for portfolio metrics — today vs all-time."""
    import sqlite3 as _sqlite3
    conn = _sqlite3.connect(database.DB_PATH)
    conn.row_factory = _sqlite3.Row

    today = conn.execute("""
        SELECT
            COUNT(*) FILTER (WHERE side='SELL' AND pnl IS NOT NULL) AS closed_trades,
            COUNT(*) FILTER (WHERE side='SELL' AND pnl > 0) AS wins,
            COUNT(*) FILTER (WHERE side='SELL' AND pnl < 0) AS losses,
            ROUND(SUM(pnl), 4) AS net_pnl
        FROM trades
        WHERE DATE(created_at) = DATE('now')
    """).fetchone()

    alltime = conn.execute("""
        SELECT
            COUNT(*) FILTER (WHERE side='SELL' AND pnl IS NOT NULL) AS closed_trades,
            COUNT(*) FILTER (WHERE side='SELL' AND pnl > 0) AS wins,
            COUNT(*) FILTER (WHERE side='SELL' AND pnl < 0) AS losses,
            ROUND(SUM(pnl), 4) AS net_pnl,
            ROUND(AVG(pnl) FILTER (WHERE side='SELL' AND pnl > 0), 4) AS avg_win,
            ROUND(AVG(pnl) FILTER (WHERE side='SELL' AND pnl < 0), 4) AS avg_loss
        FROM trades
        WHERE pnl IS NOT NULL
    """).fetchone()
    conn.close()

    return {
        "today":    dict(today)   if today   else {},
        "all_time": dict(alltime) if alltime else {},
    }


class CoinsRequest(BaseModel):
    coins: list[str]


@app.post("/api/coins")
def api_set_coins(req: CoinsRequest):
    """Update the approved coin list in strategy.json without restarting."""
    valid = [c.upper() for c in req.coins if c.upper().endswith("USDT")]
    if not valid:
        return {"ok": False, "error": "No valid USDT pairs provided"}
    strategy = _load_strategy()
    existing = {c["symbol"]: c for c in strategy.get("approved_coins", [])}
    new_approved = []
    for sym in valid:
        cfg = existing.get(sym, {})
        new_approved.append({
            "symbol":         sym,
            "approved":       True,
            "budget_usdt":    cfg.get("budget_usdt", config.BUDGET_PER_TRADE_USDT),
            "max_concurrent": cfg.get("max_concurrent", 2),
            "confidence":     cfg.get("confidence", 0.5),
            "reason":         cfg.get("reason", "Updated via dashboard"),
        })
    _write_strategy_patch({"approved_coins": new_approved, "user_selected_coins": True})

    # Persist coin list to Supabase so it survives Railway redeploys
    try:
        import supabase_sync
        supabase_sync.sync_selected_coins(valid)
    except Exception:
        pass

    return {"ok": True, "coins": valid}


@app.get("/api/activity")
def api_activity():
    return {"entries": database.get_activity_log(limit=100)}


@app.post("/api/reset")
def api_reset():
    """Reset paper wallet: wipe all trades/positions and restore starting USDT balance."""
    starting_usdt = float(os.getenv("STARTING_PAPER_USDT", "10000.0"))
    try:
        # Reset in-memory PaperClient balance
        from connection import client as _client
        if hasattr(_client, "_balances"):
            with _client._lock:
                _client._balances = {"USDT": starting_usdt}
            _client._prices.clear()

        # Wipe DB trades + positions + activity log, set paper state
        database.reset_paper_wallet(starting_usdt)

        # Reload positions in trade engine (clears in-memory list)
        from trade_engine import load_positions_from_db
        load_positions_from_db()

        # Record starting balance as authoritative session anchor
        database.save_setting("paper_starting_balance", str(starting_usdt))

        # Reset initial_balance in strategy.json (kept for legacy compatibility)
        s = _load_strategy()
        s["initial_balance_usdt"] = starting_usdt
        with open(config.STRATEGY_FILE, "w") as f:
            json.dump(s, f, indent=2)

        database.log_activity(f"Paper wallet reset — {starting_usdt:.2f} USDT", "info")
        return {"ok": True, "balance_usdt": starting_usdt}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.post("/api/agent/start")
def api_agent_start():
    s = _load_strategy()
    if not s.get("initial_balance_usdt"):
        bal = _get_usdt_balance()
        _write_strategy_patch({"initial_balance_usdt": bal or float(os.getenv("STARTING_PAPER_USDT", "10000.0"))})
    _write_strategy_patch({"trading_active": True, "pause_reason": None})
    database.log_activity("Bot started via API", "info")
    return {"ok": True, "running": True}


@app.post("/api/agent/stop")
def api_agent_stop():
    _write_strategy_patch({"trading_active": False, "pause_reason": "Stopped via API"})
    return {"ok": True, "running": False}


class ForceBuyRequest(BaseModel):
    price: float = 0.0   # frontend sends its known WebSocket price


@app.post("/api/force-buy/{symbol}")
def api_force_buy(symbol: str, req: Optional[ForceBuyRequest] = None):
    """Force-buy a coin immediately regardless of current signals."""
    sym = symbol.upper()
    try:
        import trade_engine as _te
        from trade_engine import (get_budget_for_coin, _positions, _positions_lock,
                                    _get_breakeven_mult, _rebuild_pos_index)
        from connection import client as _client
        from data_collector import prices as live_prices

        # Use price hint from frontend; fall back to WebSocket cache if not provided
        hint_price = (req.price if req else 0) or 0
        price = hint_price or live_prices.get(sym, 0)
        if not price:
            return {"ok": False, "error": f"No live price for {sym} — WebSocket not yet connected"}

        usdt_balance = _get_usdt_balance()
        budget = get_budget_for_coin(sym, usdt_balance)
        if budget <= 0:
            return {"ok": False, "error": f"Budget 0 — balance: {usdt_balance:.2f} USDT"}

        with _positions_lock:
            already_held = any(p["symbol"] == sym for p in _positions)
        if already_held:
            return {"ok": False, "error": f"Already holding {sym}"}

        # Atomic claim — same guard as _check_buys_from_cache to prevent race with scanner
        with _te._buying_lock:
            _now_fb = time.time()
            _stale_fb = [s for s, ts in _te._buying_ts.items()
                         if (_now_fb - ts) > _te._BUYING_TIMEOUT_SEC]
            for _s in _stale_fb:
                _te._buying.discard(_s)
                _te._buying_ts.pop(_s, None)
            if sym in _te._buying:
                return {"ok": False, "error": f"Buy already in progress for {sym}"}
            _te._buying.add(sym)
            _te._buying_ts[sym] = _now_fb

        _client.update_price(sym, price)
        try:
            result = _client.order_market_buy(symbol=sym, quoteOrderQty=budget)
        except Exception as _buy_e:
            with _te._buying_lock:
                _te._buying.discard(sym)
                _te._buying_ts.pop(sym, None)
            raise _buy_e
        fill       = result.get("fills", [{}])[0]
        fill_price = float(fill.get("price", price))
        qty        = float(result.get("executedQty", 0))
        if qty <= 0:
            with _te._buying_lock:
                _te._buying.discard(sym)
                _te._buying_ts.pop(sym, None)
            return {"ok": False, "error": "Order returned 0 quantity"}

        _bep_m_fb = _get_breakeven_mult(fill_price, sym)
        exit_target = round(fill_price * _bep_m_fb, 8)
        pos = {
            "symbol": sym, "entry_price": fill_price, "quantity": qty,
            "budget_usdt": budget, "timestamp": datetime.now(timezone.utc).isoformat(),
            "mode": get_mode(), "exit_target": exit_target,
            "breakeven_mult_at_buy": round(_bep_m_fb, 8),
        }
        pos["id"] = database.save_position(pos)
        with _positions_lock:
            _positions.append(pos)
        _rebuild_pos_index()

        try:
            import supabase_sync
            supabase_sync.sync_buy_result_sync(pos, _get_usdt_balance())
        except Exception as _sbe:
            database.log_activity(f"Supabase sync error after force-buy {sym}: {_sbe}", "error")

        database.log_activity(f"Force buy: {sym} @ ${fill_price:.4f} | qty={qty:.6f} | budget={budget:.2f} USDT", "info")
        with _te._buying_lock:
            _te._buying.discard(sym)
            _te._buying_ts.pop(sym, None)
        return {"ok": True, "symbol": sym, "price": fill_price, "quantity": qty, "budget": budget}
    except Exception as e:
        return {"ok": False, "error": str(e)}


class ForceSellRequest(BaseModel):
    price: float = 0.0   # frontend sends its live WebSocket price


@app.post("/api/force-sell/{symbol}")
def api_force_sell(symbol: str, req: Optional[ForceSellRequest] = None):
    """Immediately sell an open position by symbol (case-insensitive).
    Returns immediately — the sell is dispatched to the background executor
    so this endpoint never blocks the HTTP response (prevents UI freeze).
    Accepts an optional price hint from the frontend so stale WebSocket
    prices on the server side never cause the sell to use the wrong price."""
    sym = symbol.upper()
    try:
        from trade_engine import get_open_positions, _execute_sell, _sell_executor
        from data_collector import prices as live_prices
        pos_list = get_open_positions()
        pos = next((p for p in pos_list if p["symbol"] == sym), None)
        if pos is None:
            return {"ok": False, "error": f"No open position for {sym}"}
        # Priority: frontend hint → server WebSocket cache → entry price (last resort)
        hint_price = (req.price if req else 0) or 0
        price = hint_price or live_prices.get(sym, 0) or pos.get("entry_price", 0)
        if not price:
            return {"ok": False, "error": f"No live price for {sym}"}
        from trade_engine import _get_breakeven_mult
        entry = pos.get("entry_price", 0)
        _bep_m_fs = pos.get("breakeven_mult_at_buy") or (_get_breakeven_mult(entry, sym) if entry else 1.002)
        breakeven_floor = round(entry * _bep_m_fs, 8) if entry else 0
        from trade_engine import _selling, _selling_lock, _selling_ts
        import time as _time
        with _selling_lock:
            if sym in _selling:
                return {"ok": False, "error": f"Sell already in progress for {sym}"}
            _selling.add(sym)
            _selling_ts[sym] = _time.time()
        # Dispatch to executor so this HTTP handler returns immediately.
        # _execute_sell handles all logging, DB cleanup, and _selling.discard in its finally.
        _sell_executor.submit(_execute_sell, pos, price, "force-sell")
        return {"ok": True, "symbol": sym, "price": price, "breakeven": breakeven_floor}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.post("/api/positions/force-remove/{symbol}")
def api_force_remove_position(symbol: str):
    """Remove a position from the bot's records WITHOUT placing a sell order.
    Use ONLY when the coin is already sold on Binance but the bot still tracks it
    (ghost position). This is a recovery tool — not a normal sell path."""
    from trade_engine import (
        _positions, _positions_lock, _rebuild_pos_index,
        _selling, _selling_lock, _selling_ts, _pos_peaks
    )
    sym = symbol.upper().strip()
    try:
        removed_ids = []
        with _positions_lock:
            for p in _positions:
                if p.get("symbol") == sym:
                    removed_ids.append(p.get("id"))
            before = len(_positions)
            _positions[:] = [p for p in _positions if p.get("symbol") != sym]
            removed_count = before - len(_positions)

        for pid in removed_ids:
            if pid:
                try:
                    database.delete_position(pid)
                except Exception:
                    pass

        # Clear any selling guard so future positions on same symbol aren't blocked
        with _selling_lock:
            _selling.discard(sym)
            _selling_ts.pop(sym, None)
        _pos_peaks.pop(sym, None)

        _rebuild_pos_index()

        if removed_count > 0:
            database.log_activity(
                f"[FORCE_REMOVE] {sym}: removed {removed_count} position(s) from records (no sell executed)",
                "warn"
            )
            return {"ok": True, "removed": removed_count, "symbol": sym}
        return {"ok": False, "error": f"No open position found for {sym}"}
    except Exception as e:
        return {"ok": False, "error": str(e)}


class ModeRequest(BaseModel):
    mode: str           # "paper" or "live"
    api_key: str = ""
    api_secret: str = ""


def _update_env_file(updates: dict):
    import pathlib
    env_path = pathlib.Path(__file__).parent / ".env"
    lines: list[str] = []
    if os.path.exists(env_path):
        with open(env_path) as f:
            lines = f.readlines()
    for key, value in updates.items():
        found = False
        for i, line in enumerate(lines):
            if line.startswith(f"{key}="):
                lines[i] = f"{key}={value}\n"
                found = True
                break
        if not found:
            lines.append(f"{key}={value}\n")
    with open(env_path, "w") as f:
        f.writelines(lines)
    # Update os.environ so os.execv() child process inherits the new values.
    # Without this, load_dotenv(override=True) still reads the old inherited env.
    for key, value in updates.items():
        os.environ[key] = str(value)


@app.post("/api/mode")
def api_set_mode(req: ModeRequest):
    if req.mode not in ("paper", "live"):
        return {"ok": False, "error": "mode must be paper or live"}

    # Guard: cannot switch live→paper while bot is actively trading.
    # Open positions would be orphaned (live coins on Binance, no bot record).
    # User must pause the bot first so they can review/close positions safely.
    if req.mode == "paper" and get_mode() == "live":
        strategy = _load_strategy()
        if strategy.get("trading_active", False):
            return {
                "ok": False,
                "error": (
                    "Bot is actively trading in live mode. "
                    "Pause the bot first to avoid orphaning open positions, "
                    "then switch to paper mode."
                ),
            }

    updates = {"MODE": req.mode}
    if req.mode == "live":
        if req.api_key:
            updates["BINANCE_API_KEY"] = req.api_key
        if req.api_secret:
            updates["BINANCE_API_SECRET"] = req.api_secret

    _update_env_file(updates)
    # Persist mode to DB as a second source of truth — survives git pull / .env loss.
    try:
        database.save_setting("trading_mode", req.mode)
    except Exception:
        pass
    # Stop trading before restart so no open orders are left dangling
    _write_strategy_patch({"trading_active": False})

    def _restart():
        time.sleep(0.8)
        os.execv(sys.executable, [sys.executable] + sys.argv)

    threading.Thread(target=_restart, daemon=True).start()
    return {"ok": True, "mode": req.mode, "restarting": True}


@app.get("/api/sell-monitor")
def api_sell_monitor():
    """Diagnostic: sell monitor thread status + per-position threshold check."""
    import trade_engine as _te
    from data_collector import prices as live_prices

    hb    = _te._sell_monitor_heartbeat
    alive = hb > 0 and (time.time() - hb) < 5.0
    age   = round(time.time() - hb, 1) if hb > 0 else None

    fee_rate = config.FEE_RATE

    try:
        ws_ts_map = _te._last_ws_price_ts
    except Exception:
        ws_ts_map = {}

    positions = _get_positions()
    checks = []
    now_t = time.time()
    for p in positions:
        sym   = p["symbol"]
        entry = p.get("entry_price", 0)
        price = live_prices.get(sym, 0)
        # Use the stored multiplier from buy time if available; else adaptive tier
        bep_m = p.get("breakeven_mult_at_buy") or (_te._get_breakeven_mult(entry, sym) if entry else 1.002)
        bep   = entry * bep_m if entry else 0
        sl_mult = _te._stop_loss_mult  # 0.0 when disabled
        sl      = entry * sl_mult if entry and sl_mult > 0 else 0
        sl_on   = sl_mult > 0 and sl_mult < 1.0
        pct   = ((price - entry) / entry * 100) if entry else 0
        ws_age = round(now_t - ws_ts_map.get(sym, 0), 2) if ws_ts_map.get(sym) else None
        checks.append({
            "symbol":          sym,
            "entry":           entry,
            "current":         price,
            "pct_from_entry":  round(pct, 4),
            "breakeven_price": round(bep, 6),
            "breakeven_mult":  round(bep_m, 6),
            "stop_loss":       round(sl, 6) if sl_on else None,
            "profitable":      price > bep if price and bep else False,
            "sl_hit":          (price <= sl) if (sl_on and price and sl) else False,
            "price_age_sec":   ws_age,
        })
        try:
            import trade_engine as _te_sm
            _real_bep_sm = _te_sm.compute_real_breakeven_price(p)
            if _real_bep_sm > 0:
                checks[-1]["breakeven_price_real"] = round(_real_bep_sm, 8)
                # Fix: profitable uses real BEP (matches /api/all) not simple BEP
                checks[-1]["profitable"] = bool(price >= _real_bep_sm) if price else False
                _exit_t = p.get("exit_target") or bep
                checks[-1]["ready_to_sell"] = bool(price >= max(_real_bep_sm, _exit_t)) if price else False
                if price > 0:
                    _gap_pct_sm = round((_real_bep_sm - price) / price * 100, 4)
                    checks[-1]["real_bep_gap_pct"]       = _gap_pct_sm
                    checks[-1]["real_bep_distance_usdt"] = round((_real_bep_sm - price) * float(p.get("quantity", 0)), 4)
                    checks[-1]["is_trapped"]             = bool(_gap_pct_sm > 2.0)
        except Exception:
            pass

    return {
        "sell_monitor_alive": alive,
        "heartbeat_age_sec":  age,
        "breakeven_pct":      round(fee_rate * 2 * 100, 4),
        "stop_loss_pct":      config.STOP_LOSS_PCT * 100,
        "sell_trigger":       "price >= entry × adaptive_breakeven_mult (tier-based)",
        "open_positions":     len(checks),
        "positions":          checks,
    }


@app.get("/api/sell-queue")
def api_sell_queue():
    """Show positions that have a sell trigger in-flight with per-stage timing."""
    import trade_engine as _te
    import time as _time
    now = _time.time()
    queued = []
    with _te._positions_lock:
        snap = list(_te._positions)
    for p in snap:
        trig = p.get("_sell_trigger_ts", 0)
        if trig > 0:
            queued.append({
                "symbol":        p.get("symbol"),
                "reason":        p.get("_sell_reason", ""),
                "stuck_seconds": round(now - trig, 1),
                "stage": (
                    "trigger"    if not p.get("_sell_picked_up_ts")   else
                    "queued"     if not p.get("_sell_gate_start_ts")  else
                    "gate"       if not p.get("_sell_gate_done_ts")   else
                    "binance"    if not p.get("_sell_binance_done_ts") else
                    "finalizing"
                ),
            })
    with _te._selling_lock:
        selling_set = list(_te._selling)
    return {
        "queued_count":   len(queued),
        "in_selling_set": selling_set,
        "items":          sorted(queued, key=lambda x: -x["stuck_seconds"]),
    }


@app.get("/api/positions/signal-analysis")
def api_positions_signal_analysis():
    """Per-position buy-signal snapshot vs post-buy price move."""
    import trade_engine as _te
    import sqlite3 as _sq3
    import json as _js
    import time as _time
    now = _time.time()
    out = []
    with _te._positions_lock:
        snap = list(_te._positions)
    for p in snap:
        sig = p.get("buy_signals_snapshot")
        if not sig:
            continue
        entry   = p.get("entry_price") or p.get("avg_entry_price", 0)
        current = p.get("current_price", 0) or _te._rest_px.get(p.get("symbol", ""), 0)
        pct = round((current - entry) / entry * 100, 3) if entry > 0 else 0
        out.append({
            "symbol":            p.get("symbol"),
            "status":            "open",
            "entry":             entry,
            "current":           current,
            "pct_since_buy":     pct,
            "age_min":           round((now - sig.get("ts", now)) / 60, 1) if sig.get("ts") else None,
            "signals_at_buy":    sig,
        })
    try:
        conn = _sq3.connect(database.DB_PATH)
        conn.row_factory = _sq3.Row
        rows = conn.execute("""
            SELECT symbol, entry_price, exit_price, pnl, buy_signals_snapshot, created_at
            FROM positions
            WHERE created_at > datetime('now','-1 day') AND exit_price IS NOT NULL
            ORDER BY id DESC LIMIT 30
        """).fetchall()
        conn.close()
        for r in rows:
            try:
                snap_raw = r["buy_signals_snapshot"]
                sig = (_js.loads(snap_raw) if isinstance(snap_raw, str) else snap_raw) if snap_raw else None
            except Exception:
                sig = None
            if not sig:
                continue
            entry = r["entry_price"] or 0
            exit_ = r["exit_price"] or 0
            pct = round((exit_ - entry) / entry * 100, 3) if entry > 0 else 0
            out.append({
                "symbol": r["symbol"], "status": "closed",
                "entry": entry, "exit": exit_,
                "pct": pct, "pnl": round(r["pnl"] or 0, 4),
                "signals_at_buy": sig,
            })
    except Exception:
        pass
    return {"positions": out, "count": len(out)}


@app.get("/api/signals/quality")
def api_signals_quality():
    """Group closed positions by signal characteristics to find which combos win."""
    import sqlite3 as _sq3
    import json as _js
    conn = _sq3.connect(database.DB_PATH)
    conn.row_factory = _sq3.Row
    try:
        rows = conn.execute("""
            SELECT entry_price, exit_price, pnl, buy_signals_snapshot
            FROM positions
            WHERE created_at > datetime('now','-7 days')
              AND exit_price IS NOT NULL AND buy_signals_snapshot IS NOT NULL
        """).fetchall()
    finally:
        conn.close()
    buckets: dict = {}
    for r in rows:
        try:
            snap_raw = r["buy_signals_snapshot"]
            snap = (_js.loads(snap_raw) if isinstance(snap_raw, str) else snap_raw) if snap_raw else None
        except Exception:
            continue
        if not snap:
            continue
        rsi_v   = snap.get("rsi_value") or snap.get("rsi", 50) or 50
        trend5m = snap.get("5m_ok") or snap.get("trend_5m_ok")
        knife   = snap.get("falling_knife", False)
        rsi_b   = ("rsi<30" if rsi_v < 30 else "rsi30-40" if rsi_v < 40 else
                   "rsi40-50" if rsi_v < 50 else "rsi50-60" if rsi_v < 60 else "rsi>60")
        trend_b = "trend5m=Y" if trend5m else ("trend5m=N" if trend5m is False else "trend5m=?")
        knife_b = "knife=Y" if knife else "knife=N"
        key = f"{rsi_b}|{trend_b}|{knife_b}"
        b = buckets.setdefault(key, {"trades": 0, "wins": 0, "total_pnl": 0.0})
        b["trades"] += 1
        if (r["pnl"] or 0) > 0:
            b["wins"] += 1
        b["total_pnl"] += (r["pnl"] or 0)
    summary = [
        {"characteristics": k, "trades": b["trades"],
         "win_rate_pct": round(100 * b["wins"] / b["trades"], 1),
         "total_pnl": round(b["total_pnl"], 4),
         "avg_pnl": round(b["total_pnl"] / b["trades"], 4)}
        for k, b in buckets.items() if b["trades"] >= 2
    ]
    summary.sort(key=lambda x: -x["avg_pnl"])
    return {"buckets": summary, "sample_size": sum(b["trades"] for b in buckets.values())}


@app.get("/api/proxy/binance/ticker/24hr")
async def api_proxy_ticker_24hr(symbols: str = None):
    """Chunked proxy for Binance 24hr ticker — avoids 400s from large symbol lists."""
    import urllib.request as _ur
    if not symbols:
        return JSONResponse(status_code=400, content={"error": "symbols required"})
    try:
        symbol_list = json.loads(symbols)
    except Exception:
        return JSONResponse(status_code=400, content={"error": "invalid symbols format"})
    if not isinstance(symbol_list, list):
        return JSONResponse(status_code=400, content={"error": "symbols must be a list"})

    CHUNK_SIZE = 20
    all_results: list = []
    for i in range(0, len(symbol_list), CHUNK_SIZE):
        chunk = symbol_list[i:i + CHUNK_SIZE]
        try:
            url = f"https://api.binance.com/api/v3/ticker/24hr?symbols={json.dumps(chunk)}"
            req = _ur.Request(url, headers={"User-Agent": "WolfBot/1.0"})
            with _ur.urlopen(req, timeout=5.0) as r:
                all_results.extend(json.loads(r.read()))
        except Exception:
            continue  # partial results preferred over full failure
    return all_results


@app.get("/api/signal-registry")
def api_signal_registry():
    """List all registered signals with their current role in the signal engine."""
    try:
        import signal_registry as _sr
        strategy      = _load_strategy()
        engine_cfg    = strategy.get("signal_engine", {})
        engine_active = bool(engine_cfg.get("enabled", False))

        mandatory_ids = engine_cfg.get("mandatory_signals", _sr.DEFAULT_SIGNAL_ENGINE["mandatory_signals"])
        scored_ids    = engine_cfg.get("scored_signals",    _sr.DEFAULT_SIGNAL_ENGINE["scored_signals"])
        veto_ids      = engine_cfg.get("veto_signals",      _sr.DEFAULT_SIGNAL_ENGINE["veto_signals"])

        signals_info = []
        for sig_id, sig_def in _sr.SIGNAL_REGISTRY.items():
            if sig_id in mandatory_ids:
                role = "mandatory"
            elif sig_id in scored_ids:
                role = "scored"
            elif sig_id in veto_ids:
                role = "veto"
            else:
                role = "disabled"
            signals_info.append({
                "id":          sig_id,
                "category":    sig_def.category,
                "description": sig_def.description,
                "role":        role,
            })

        return {
            "available":      True,
            "engine_enabled": engine_active,
            "total":          len(signals_info),
            "categories":     sorted({s["category"] for s in signals_info}),
            "min_scored":     int(engine_cfg.get("min_scored", _sr.DEFAULT_SIGNAL_ENGINE["min_scored"])),
            "thresholds":     strategy.get("signal_thresholds", _sr.DEFAULT_SIGNAL_THRESHOLDS),
            "signals":        signals_info,
        }
    except Exception as e:
        return {"available": False, "signals": [], "error": str(e)}


@app.get("/api/proxy/binance/{path:path}")
async def api_proxy_binance(path: str, request: Request):
    """Server-side proxy for Binance REST API — avoids browser CORS restrictions."""
    import urllib.request as _ur
    import urllib.error as _ue
    qs = str(request.query_params)
    url = f"https://api.binance.com/api/v3/{path}"
    if qs:
        url += "?" + qs
    req = _ur.Request(url, headers={"User-Agent": "WolfBot/1.0"})
    try:
        with _ur.urlopen(req, timeout=5.0) as r:
            body = r.read()
        return Response(content=body, media_type="application/json")
    except _ue.HTTPError as he:
        body = he.read()
        return Response(content=body, status_code=he.code, media_type="application/json")
    except Exception as e:
        return Response(content=json.dumps({"error": str(e)}), status_code=502, media_type="application/json")


@app.get("/api/diagnostics")
def api_diagnostics():
    """Comprehensive bot health snapshot. Zero extra Binance calls — all data is in-memory."""
    import trade_engine as _te
    import data_collector as _dc
    import threading as _thr

    now = time.time()

    bh = _te._binance_health
    last_rest_age = round(now - bh["last_rest_ok_ts"], 1) if bh["last_rest_ok_ts"] else None

    wh = _dc._ws_health
    last_msg_age = round(now - wh["last_message_ts"], 1) if wh["last_message_ts"] else None

    ssh = _te._signal_scanner_health
    last_scan_age = round(now - ssh["last_refresh_ts"], 1) if ssh["last_refresh_ts"] else None
    next_scan_in  = round(max(0.0, ssh["interval_sec"] - (last_scan_age or ssh["interval_sec"])), 1)
    scan_progress_pct = round(
        min(100.0, (last_scan_age or 0) / max(1, ssh["interval_sec"]) * 100), 1
    ) if last_scan_age else 0.0

    sm_hb      = getattr(_te, "_sell_monitor_heartbeat", 0)
    sm_hb_age  = round(now - sm_hb, 1) if sm_hb else None

    refresher  = getattr(_te, "_held_refresher_thread", None)
    ref_alive  = bool(refresher and refresher.is_alive())

    active_threads = [t.name for t in _thr.enumerate() if t.is_alive()]

    return {
        "server_time": now,
        "binance": {
            "rest_ok":            last_rest_age is not None and last_rest_age < 30,
            "last_rest_age_sec":  last_rest_age,
            "last_latency_ms":    bh["last_rest_latency_ms"],
            "used_weight_1m":     bh["used_weight_1m"],
            "used_weight_pct":    bh["used_weight_pct"],
            "weight_limit":       6000,
            "rest_error_count":   bh["rest_error_count"],
            "last_error_age_sec": round(now - bh["last_error_ts"], 1) if bh["last_error_ts"] else None,
            "last_error_msg":     bh["last_error_msg"],
        },
        "websocket": {
            "connected":            wh["connected"],
            "last_message_age_sec": last_msg_age,
            "messages_received":    wh["messages_received"],
            "connect_count":        wh["connect_count"],
            "disconnect_count":     wh["disconnect_count"],
            "subscribed_symbols":   len(_dc.prices),
        },
        "signal_scanner": {
            "last_refresh_age_sec":  last_scan_age,
            "next_refresh_in_sec":   next_scan_in,
            "scan_progress_pct":     scan_progress_pct,
            "interval_sec":          ssh["interval_sec"],
            "last_duration_ms":      ssh["last_duration_ms"],
            "scans_completed":       ssh["scans_completed"],
            "cached_signals_count":  len(_te._signal_cache),
        },
        "sell_monitor": {
            "alive":             sm_hb_age is not None and sm_hb_age < 15,
            "heartbeat_age_sec": sm_hb_age,
            "open_positions":    len(_te._positions),
            "in_progress_sells": len(_te._selling),
        },
        "buying": {
            "in_progress_count":   len(_te._buying),
            "in_progress_symbols": list(_te._buying),
        },
        "price_refresher": {
            "alive":       ref_alive,
            "thread_name": refresher.name if refresher else None,
        },
        "system": {
            "deploy_id":            _DEPLOY_ID,
            "active_threads_count": len(active_threads),
            "active_threads":       active_threads,
        },
        "issues": {
            "recent":         _te.get_diag_log(limit=25),
            "error_count":    len(_te.get_diag_log(limit=50, severity_filter="error")),
            "warn_count":     len(_te.get_diag_log(limit=50, severity_filter="warn")),
            "total_buffered": len(_te._diag_log),
        },
        "claude_api": {
            "error_count":      bh["claude_error_count"],
            "last_error_age_sec": round(now - bh["claude_last_error_ts"], 1) if bh["claude_last_error_ts"] else None,
            "last_error_msg":   bh["claude_last_error_msg"],
            "disabled":         now < bh["claude_disabled_until"],
            "disabled_until":   bh["claude_disabled_until"] if bh["claude_disabled_until"] > now else None,
        },
        "market_regime": (lambda b: {
            "regime":      b.get("regime", "unknown") if b else "unknown",
            "btc_price":   b.get("price")   if b else None,
            "pct_4h":      b.get("pct_4h")  if b else None,
            "pct_24h":     b.get("pct_24h") if b else None,
            "ema_8":       b.get("ema_8")   if b else None,
            "ema_24":      b.get("ema_24")  if b else None,
            "buys_paused": (b.get("regime") == "bearish") if b else False,
        })(_te.get_btc_state()),
    }


@app.get("/api/buy-rejections")
def api_buy_rejections():
    """Per-reason count of rejected buy candidates (score >= 3) since last reset."""
    import trade_engine as _te
    stats = _te.get_rejection_stats()
    total = sum(stats["counts"].values())
    sorted_by_count = sorted(stats["counts"].items(), key=lambda x: -x[1])
    return {
        "total_rejections": total,
        "by_reason": [
            {
                "reason": reason,
                "count": count,
                "pct_of_total": round(100 * count / total, 1) if total > 0 else 0,
                "examples": stats["examples"].get(reason, [])[-3:],
            }
            for reason, count in sorted_by_count
        ],
    }


@app.post("/api/buy-rejections/reset")
def api_buy_rejections_reset():
    """Clear the buy-rejection counters and return how many were cleared."""
    import trade_engine as _te
    n = _te.clear_rejection_stats()
    return {"ok": True, "cleared": n}


@app.post("/api/diagnostics/reset")
def api_diagnostics_reset():
    """Reset Binance REST and Claude API error counters."""
    import trade_engine as _te
    with _te._binance_health_lock:
        _te._binance_health["rest_error_count"] = 0
        _te._binance_health["last_error_ts"]    = 0.0
        _te._binance_health["last_error_msg"]   = ""
    _te.reset_claude_errors()
    return {"ok": True, "reset": True}


@app.get("/api/buy-rejections")
def api_buy_rejections():
    """Per-reason count of rejected buy candidates (score >= 3) since last reset."""
    import trade_engine as _te
    stats = _te.get_rejection_stats()
    total = sum(stats["counts"].values())
    sorted_reasons = sorted(stats["counts"].items(), key=lambda x: -x[1])
    return {
        "total_rejections": total,
        "since_last_reset_ts": getattr(_te, "_rejection_reset_ts", 0),
        "by_reason": [
            {
                "reason": reason,
                "count": count,
                "pct_of_total": round(100 * count / total, 1) if total > 0 else 0,
                "recent_examples": stats["examples"].get(reason, [])[-3:],
            }
            for reason, count in sorted_reasons
        ],
    }


@app.post("/api/buy-rejections/reset")
def api_buy_rejections_reset():
    import trade_engine as _te
    n = _te.clear_rejection_stats()
    return {"ok": True, "cleared": n}


@app.get("/api/diagnostics/log")
def api_diagnostics_log(limit: int = 50, since: float = 0.0, severity: str = ""):
    """Query the in-memory diagnostic ring buffer with optional filtering."""
    import trade_engine as _te
    return {
        "entries":        _te.get_diag_log(limit=limit, since_ts=since, severity_filter=severity),
        "total_buffered": len(_te._diag_log),
    }


@app.post("/api/diagnostics/log/clear")
def api_diagnostics_log_clear():
    """Clear the in-memory issue log. Doesn't affect bot operation."""
    import trade_engine as _te
    n = _te.clear_diag_log()
    return {"ok": True, "cleared": n}


@app.get("/api/diagnostics/log/text")
def api_diagnostics_log_text(limit: int = 50, severity: str = ""):
    """Plain-text diagnostic report — paste directly into chat or save to file."""
    from fastapi.responses import Response as _Resp
    import trade_engine as _te
    import data_collector as _dc
    from datetime import datetime as _dt, timezone as _tz

    entries = _te.get_diag_log(limit=limit, severity_filter=severity)
    try:
        bh = _te._binance_health
        wh = _dc._ws_health
        header = [
            f"=== WolfBot Diagnostic Report — {_dt.now(_tz.utc).isoformat()} ===",
            f"Deploy: {_DEPLOY_ID}",
            f"Binance REST: weight={bh.get('used_weight_1m',0)}/6000  "
            f"latency={bh.get('last_rest_latency_ms',0):.0f}ms  "
            f"errors={bh.get('rest_error_count',0)}",
            f"WebSocket: connected={wh.get('connected',False)}  "
            f"msgs={wh.get('messages_received',0)}  "
            f"disconnects={wh.get('disconnect_count',0)}",
            f"Open positions: {len(_te._positions)}  "
            f"In-progress sells: {len(_te._selling)}",
            f"--- Recent issues ({len(entries)}) ---",
        ]
    except Exception:
        header = ["=== WolfBot Diagnostic Report ==="]

    body = []
    for e in entries:
        body.append(f"[{e['iso']}] [{e['severity'].upper():5}] [{e['source']}] {e['message']}")
        if e.get("detail"):
            body.append(f"        ↳ {e['detail']}")

    return _Resp(content="\n".join(header + body), media_type="text/plain")


@app.get("/api/diagnostics/errors/summary")
def api_diagnostics_errors_summary():
    """Group recent diag errors by source tag + Binance error code.
    Shows which call site is failing most and what Binance is complaining about."""
    import trade_engine as _te
    import re as _re

    entries = _te.get_diag_log(limit=200, severity_filter="error")
    by_source: dict = {}
    by_binance_code: dict = {}
    examples: dict = {}

    for e in entries:
        msg    = e.get("message", "")
        detail = e.get("detail", "")

        m = _re.match(r'^REST:\s*\[(\w+)\]', msg) or _re.match(r'^\[(\w+)\]', msg)
        src = m.group(1) if m else "untagged"
        by_source[src] = by_source.get(src, 0) + 1

        mc = _re.search(r'"code":\s*(-?\d+)', detail)
        if mc:
            code = mc.group(1)
            by_binance_code[code] = by_binance_code.get(code, 0) + 1
            if code not in examples:
                examples[code] = detail[:400]

    return {
        "total_errors_in_buffer": len(entries),
        "by_source":      dict(sorted(by_source.items(), key=lambda x: -x[1])),
        "by_binance_code": dict(sorted(by_binance_code.items(), key=lambda x: -x[1])),
        "examples_by_code": examples,
    }


@app.get("/api/stats/daily")
def api_stats_daily(days: int = 7):
    """Daily trade summary — buys, sells, PnL, win rate per day."""
    import sqlite3 as _sq
    days = max(1, min(30, int(days)))
    conn = _sq.connect(database.DB_PATH)
    conn.row_factory = _sq.Row
    rows = conn.execute("""
        SELECT
            DATE(created_at)                                                          AS day,
            COUNT(*) FILTER (WHERE side='BUY')                                        AS buys,
            COUNT(*) FILTER (WHERE side='SELL')                                       AS sells,
            ROUND(SUM(pnl), 4)                                                        AS total_pnl,
            ROUND(AVG(pnl) FILTER (WHERE side='SELL'), 4)                             AS avg_pnl,
            ROUND(MIN(pnl) FILTER (WHERE side='SELL'), 4)                             AS worst_pnl,
            ROUND(MAX(pnl) FILTER (WHERE side='SELL'), 4)                             AS best_pnl,
            ROUND(
                100.0 * COUNT(*) FILTER (WHERE pnl > 0)
                / NULLIF(COUNT(*) FILTER (WHERE side='SELL'), 0),
            1)                                                                         AS win_rate
        FROM trades
        WHERE created_at > datetime('now', ?)
        GROUP BY day
        ORDER BY day DESC
    """, (f"-{days} days",)).fetchall()
    conn.close()
    return {"days": [dict(r) for r in rows]}


@app.get("/api/debug-refresher")
def api_debug_refresher():
    """Diagnostic: price refresher threads health + per-symbol REST price ages."""
    import trade_engine as _te
    from data_collector import prices as live_prices

    now_t = time.time()

    # Held-position refresher thread (primary, 2s interval)
    held_thread   = getattr(_te, "_held_refresher_thread", None)
    held_alive    = bool(held_thread and held_thread.is_alive())
    held_name     = held_thread.name if held_thread else None

    # Background _price_refresher_loop thread (secondary)
    ref_thread    = _te._price_refresher_thread
    ref_alive     = bool(ref_thread and ref_thread.is_alive())
    ref_hb        = _te._price_refresher_heartbeat
    ref_hb_age    = round(now_t - ref_hb, 1) if ref_hb > 0 else None

    # Sell monitor thread health
    sm_hb         = _te._sell_monitor_heartbeat
    sm_alive      = bool(sm_hb > 0 and (now_t - sm_hb) < 10.0)
    sm_hb_age     = round(now_t - sm_hb, 1) if sm_hb > 0 else None

    # Per-symbol price info for open positions
    try:
        ws_ts_map  = _te._last_ws_price_ts
        rest_px    = _te._rest_px
        rest_px_ts = _te._rest_px_ts
    except Exception:
        ws_ts_map  = {}
        rest_px    = {}
        rest_px_ts = 0.0

    held_symbols = [p.get("symbol") for p in _te._positions if p.get("symbol")]

    return {
        "refresher_alive":      held_alive,
        "thread_name":          held_name,
        "held_positions_count": len(held_symbols),
        "held_symbols":         held_symbols,
        "price_freshness": [
            {
                "symbol":      sym,
                "price":       round(live_prices.get(sym, 0), 8),
                "age_seconds": round(now_t - ws_ts_map[sym], 1) if sym in ws_ts_map else "never",
            }
            for sym in held_symbols
        ],
        "background_refresher": {
            "alive":         ref_alive,
            "heartbeat_age": ref_hb_age,
        },
        "sell_monitor": {
            "alive":         sm_alive,
            "heartbeat_age": sm_hb_age,
        },
        "rest_cache_age_sec": round(now_t - rest_px_ts, 1) if rest_px_ts > 0 else None,
    }


@app.get("/api/settings")
def api_get_settings():
    """Return current bot risk/strategy settings."""
    s = _load_strategy()
    return {
        "ok":                  True,
        "stop_loss_enabled":   s.get("stop_loss_enabled",   False),
        "stop_loss_pct":       s.get("stop_loss_pct",       2.0),
        "take_profit_enabled": s.get("take_profit_enabled", True),
        "take_profit_pct":     s.get("take_profit_pct",     0.1),
        "smart_hold_enabled":  s.get("smart_hold_enabled",  False),
        "trailing_stop_pct":   s.get("trailing_stop_pct",   0.5),
        "reinvest_profits":    s.get("reinvest_profits",    False),
        "max_positions":       s.get("max_positions",       20),
        "min_signals":         s.get("min_signals",         config.MIN_SIGNALS_TO_BUY),
        "strategy_notes":      s.get("strategy_notes",      ""),
        "budget_mode":         s.get("budget_mode",         config.BUDGET_MODE),
        "budget_fixed_usdt":   s.get("budget_fixed_usdt",   config.BUDGET_FIXED_USDT),
        "budget_pct_of_free":  s.get("budget_pct_of_free",  config.BUDGET_PCT_OF_FREE),
        "bot_allocation_usdt": s.get("bot_allocation_usdt", config.BOT_ALLOCATION_USDT),
    }


class SettingsRequest(BaseModel):
    stop_loss_enabled:   Optional[bool]  = None
    stop_loss_pct:       Optional[float] = None
    take_profit_enabled: Optional[bool]  = None
    take_profit_pct:     Optional[float] = None
    smart_hold_enabled:  Optional[bool]  = None
    trailing_stop_pct:   Optional[float] = None
    reinvest_profits:    Optional[bool]  = None
    max_positions:       Optional[int]   = None
    min_signals:         Optional[int]   = None
    strategy_notes:      Optional[str]   = None
    slippage_buffer_pct: Optional[float] = None  # 0.05–0.50%, default 0.10%


@app.post("/api/settings")
def api_save_settings(req: SettingsRequest):
    """Save bot risk/strategy settings into strategy.json."""
    try:
        patch: dict = {}
        if req.stop_loss_enabled   is not None: patch["stop_loss_enabled"]  = bool(req.stop_loss_enabled)
        if req.stop_loss_pct       is not None: patch["stop_loss_pct"]      = max(0.1, min(20.0, req.stop_loss_pct))
        if req.take_profit_enabled is not None: patch["take_profit_enabled"] = bool(req.take_profit_enabled)
        if req.take_profit_pct     is not None: patch["take_profit_pct"]    = max(0.0, min(50.0, req.take_profit_pct))
        if req.smart_hold_enabled  is not None: patch["smart_hold_enabled"] = bool(req.smart_hold_enabled)
        if req.trailing_stop_pct   is not None: patch["trailing_stop_pct"]  = max(0.1, min(10.0, req.trailing_stop_pct))
        if req.reinvest_profits    is not None: patch["reinvest_profits"]   = bool(req.reinvest_profits)
        if req.max_positions       is not None: patch["max_positions"]      = max(1,   min(100,  req.max_positions))
        if req.min_signals         is not None: patch["min_signals"]        = max(1,   min(6,    req.min_signals))
        if req.strategy_notes      is not None: patch["strategy_notes"]     = req.strategy_notes[:2000]
        if req.slippage_buffer_pct is not None: patch["slippage_buffer_pct"] = max(0.05, min(0.50, req.slippage_buffer_pct))
        if not patch:
            return {"ok": False, "error": "No valid settings provided"}
        _write_strategy_patch(patch)
        database.log_activity(
            "Settings updated: " + ", ".join(f"{k}={v}" for k, v in patch.items() if k != "strategy_notes"),
            "info"
        )
        return {"ok": True, **patch}
    except Exception as e:
        database.log_activity(f"Settings save error: {e}", "error")
        return Response(
            content=json.dumps({"ok": False, "error": str(e)}),
            status_code=500, media_type="application/json"
        )


@app.get("/api/ping")
def api_ping():
    return {"ok": True, "ts": datetime.now(timezone.utc).isoformat()}


@app.get("/api/buy-rejections")
def api_buy_rejections():
    import trade_engine as _te
    stats = _te.get_rejection_stats()
    total = sum(stats["counts"].values())
    sorted_reasons = sorted(stats["counts"].items(), key=lambda x: -x[1])
    return {
        "total_rejections": total,
        "since_reset_ts":   stats["reset_ts"],
        "since_reset_age_sec": round(time.time() - stats["reset_ts"], 1),
        "by_reason": [
            {
                "reason":          reason,
                "count":           count,
                "pct_of_total":    round(100 * count / total, 1) if total > 0 else 0,
                "recent_examples": stats["examples"].get(reason, [])[-3:],
            }
            for reason, count in sorted_reasons
        ],
    }


@app.post("/api/buy-rejections/reset")
def api_buy_rejections_reset():
    import trade_engine as _te
    n = _te.clear_rejection_stats()
    return {"ok": True, "cleared": n}


# Tiny response cache — coalesces overlapping polls (the frontend has both a 5 s
# and a 1 s interval; without this they each issue a full DB sweep, which holds
# the global SQLite lock and starves the sell monitor for ~50 ms each call).
_API_ALL_CACHE: dict = {"ts": 0.0, "data": None}
_API_ALL_TTL = 0.8   # seconds — slightly less than the 1 s fast-poll cadence


def _format_trades(raw: list) -> list:
    """Convert DB trade rows (coin/entry_price/exit_price/net_profit) to the
    frontend-expected format (symbol/side/price/pnl/created_at).

    The DB stores one row per completed trade (buy+sell pair).  The frontend
    wants two records per trade — a BUY entry and a SELL exit — so it can render
    the full trade history with correct PnL on the SELL row.
    """
    result = []
    for t in raw:
        sym = t.get("coin") or t.get("symbol") or ""
        if not sym:
            continue
        buy_ts  = t.get("timestamp_buy")  or t.get("created_at") or ""
        sell_ts = t.get("timestamp_sell") or t.get("created_at") or ""
        qty     = t.get("quantity", 0)
        budget  = t.get("budget_usdt", 0)
        # BUY leg
        result.append({
            "id":         f"{t.get('id','')}-buy",
            "symbol":     sym,
            "side":       "BUY",
            "price":      t.get("entry_price", 0),
            "quantity":   qty,
            "pnl":        None,
            "reason":     None,
            "created_at": buy_ts,
            "volume_usdt": budget,
        })
        # SELL leg — only if exit_price exists (completed trade)
        if t.get("exit_price"):
            result.append({
                "id":         f"{t.get('id','')}-sell",
                "symbol":     sym,
                "side":       "SELL",
                "price":      t.get("exit_price", 0),
                "quantity":   qty,
                "pnl":        t.get("net_profit"),
                "reason":     t.get("sell_reason") or "take-profit",
                "created_at": sell_ts,
                "volume_usdt": t.get("exit_price", 0) * qty if t.get("exit_price") else budget,
            })
    # Sort newest first
    result.sort(key=lambda x: x.get("created_at") or "", reverse=True)
    return result


def _append_fresh_prices(payload: dict) -> dict:
    """Inject per-symbol live prices from data_collector into every /api/all response.
    These bypass the cache so the frontend always gets sub-100ms-fresh prices even
    when the rest of the payload (positions, trades) is served from cache."""
    try:
        import data_collector as _dc_fp
        syms = {p.get("symbol") for p in payload.get("positions", []) if p.get("symbol")}
        if syms:
            fresh = {s: float(_dc_fp.prices[s]) for s in syms if s in _dc_fp.prices}
            return {**payload, "fresh_prices": fresh, "fresh_prices_ts": time.time()}
    except Exception:
        pass
    return payload


@app.get("/api/all")
def api_all():
    """Single endpoint returning status + positions + trades + activity.
    Reduces frontend from 4 concurrent fetches to 1, cutting Railway load 4x."""
    now_ts = time.time()
    cached = _API_ALL_CACHE.get("data")
    _ttl = 0.1 if _API_ALL_CACHE.get("has_positions") else 0.8
    if cached is not None and (now_ts - _API_ALL_CACHE["ts"]) < _ttl:
        return _append_fresh_prices(cached)

    strategy = _load_strategy()
    # Use aggregated SQL stats — covers ALL trades, not just the last 500.
    # get_recent_trades(limit=500) was causing total_trades/wins/pnl/trades_today to
    # describe different subsets (500 rows vs. full table) making them inconsistent.
    stats     = database.get_trade_stats(mode=get_mode())
    all_stats = database.get_trade_stats_all_modes()
    wins      = stats["wins"]
    total     = stats["total"]
    balance   = round(_get_usdt_display_balance(), 2)
    initial   = _get_initial_balance() or balance
    approved  = [c["symbol"] for c in strategy.get("approved_coins", []) if c.get("approved")]
    positions = _get_positions()
    _API_ALL_CACHE["has_positions"] = len(positions) > 0
    trades    = database.get_recent_trades(limit=200)   # for the trades list payload only
    payload = {
        "status": {
            "running":                strategy.get("trading_active", False),
            "mode":                   get_mode(),
            "live_error":             get_live_error() or None,
            "using_paper_fallback":   is_using_paper_fallback(),
            "balance_usdt":           balance,
            "paper_balance":      balance,
            "initial_balance":    initial,
            "open_positions":     len(positions),
            "trades_today":       stats["trades_today"],
            "win_rate":           round(wins / total, 3) if total else 0.0,
            "wins":               wins,
            "losses":             stats["losses"],
            "total_trades":       total,
            "realized_pnl":       round(stats["realized_pnl"], 4),
            "today_realized_pnl": round(stats["today_realized_pnl"], 4),
            "locked_profit":      round(stats["locked_profit"], 4),
            "total_fees":         round(stats["total_fees"], 4),
            "all_time_trades":    all_stats["total"],
            "all_time_realized_pnl": round(all_stats["realized_pnl"], 4),
            "all_time_win_rate":  all_stats["win_rate"],
            "watched_coins":      approved or config.WATCHED_COINS,
            "data_persistent": database.is_data_persistent(),
            "data_dir":        database._DATA_DIR,
            "stop_loss_enabled":   strategy.get("stop_loss_enabled",   False),
            "stop_loss_pct":       strategy.get("stop_loss_pct",       2.0),
            "take_profit_enabled": strategy.get("take_profit_enabled", True),
            "take_profit_pct":     strategy.get("take_profit_pct",     0.1),
            "smart_hold_enabled": strategy.get("smart_hold_enabled", False),
            "trailing_stop_pct":  strategy.get("trailing_stop_pct",  0.5),
            "reinvest_profits":   strategy.get("reinvest_profits",   False),
            "max_positions":      strategy.get("max_positions",       20),
            "min_signals":        strategy.get("min_signals",          config.MIN_SIGNALS_TO_BUY),
            "strategy_notes":     strategy.get("strategy_notes",      ""),
            "budget_mode":        strategy.get("budget_mode",         config.BUDGET_MODE),
            "budget_fixed_usdt":  strategy.get("budget_fixed_usdt",   config.BUDGET_FIXED_USDT),
            "bot_allocation_usdt": strategy.get("bot_allocation_usdt", config.BOT_ALLOCATION_USDT),
        },
        "positions":     positions,
        "trades":        _format_trades(trades[:200]),
        "activity":      database.get_activity_log(limit=100),
        "signals":       _get_signal_snapshot(),
        "market_health": _get_market_health(),
    }
    _API_ALL_CACHE["ts"]   = now_ts
    _API_ALL_CACHE["data"] = payload
    return _append_fresh_prices(payload)


@app.get("/api/backup/export")
def api_backup_export():
    """Download a JSON snapshot of strategy.json + all trade history."""
    import io
    strategy = _load_strategy()
    trades   = database.get_recent_trades(limit=100_000)
    payload  = {
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "strategy":    strategy,
        "trades":      trades,
    }
    body = json.dumps(payload, indent=2)
    from fastapi.responses import Response
    return Response(
        content=body,
        media_type="application/json",
        headers={"Content-Disposition": "attachment; filename=tradebot_backup.json"},
    )


class BackupImportRequest(BaseModel):
    strategy: Optional[dict] = None
    trades:   Optional[list] = None


@app.post("/api/backup/import")
def api_backup_import(req: BackupImportRequest):
    """Restore strategy and/or trade history from a previous export snapshot."""
    imported = {"strategy": False, "trades": 0}
    if req.strategy:
        try:
            _write_strategy_patch(req.strategy)
            imported["strategy"] = True
        except Exception as e:
            return {"ok": False, "error": f"strategy import failed: {e}"}
    if req.trades:
        try:
            count = database.import_trades(req.trades)
            imported["trades"] = count
        except Exception as e:
            return {"ok": False, "error": f"trades import failed: {e}"}
    return {"ok": True, "imported": imported}


@app.get("/api/debug")
def api_debug():
    """Diagnostic endpoint — returns full bot health including startup status."""
    import sys
    strategy = _load_strategy()
    approved = [c["symbol"] for c in strategy.get("approved_coins", []) if c.get("approved")]
    try:
        from trade_engine import get_open_positions, _sell_monitor_heartbeat, _FEE_FLOOR
        pos_count = len(get_open_positions())
        sm_alive  = (time.time() - _sell_monitor_heartbeat) < 10 if _sell_monitor_heartbeat else False
        bep_mult  = _FEE_FLOOR * 1.0010  # reference mid-price tier (high/$10-$1000)
    except Exception as e:
        pos_count = -1; sm_alive = False; bep_mult = 0
        database.log_activity(f"debug endpoint trade_engine error: {e}", "warn")
    try:
        from data_collector import prices as ws_prices
        ws_alive = len(ws_prices) > 0
        ws_count = len(ws_prices)
    except Exception:
        ws_alive = False; ws_count = 0
    last_logs = database.get_activity_log(limit=20)
    errors    = [e for e in last_logs if e.get("level") == "error"]
    return {
        "deploy_id":       _DEPLOY_ID,
        "python_version":  sys.version,
        "data_dir":        database._DATA_DIR,
        "db_path":         database.DB_PATH,
        "strategy_file":   config.STRATEGY_FILE,
        "trading_active":  strategy.get("trading_active", False),
        "approved_coins":  len(approved),
        "coin_list":       approved[:10],
        "open_positions":  pos_count,
        "sell_monitor_ok": sm_alive,
        "websocket_alive": ws_alive,
        "ws_prices_count": ws_count,
        "breakeven_mult":  round(bep_mult, 6),
        "recent_errors":   errors[:5],
        "startup_log":     [e for e in last_logs if "Deploy started" in e.get("message","") or "Bot ready" in e.get("message","") or "STARTUP ERROR" in e.get("message","")],
    }


@app.get("/api/debug-sell")
def api_debug_sell():
    """Sell-trigger diagnostic: shows real BEP, current price, and cooldowns for every open position."""
    try:
        import trade_engine as _te_ds
        import data_collector as _dc_ds
        positions = _te_ds.get_open_positions()
        rows = []
        _now = time.time()
        for p in positions:
            sym    = p.get("symbol", "")
            entry  = p.get("entry_price", 0)
            qty    = p.get("quantity", 0)
            budget = p.get("budget_usdt", 0)
            cur    = _dc_ds.prices.get(sym, 0) or _te_ds._rest_px.get(sym, 0)
            real_bep   = _te_ds.compute_real_breakeven_price(p)
            _bep_m     = p.get("breakeven_mult_at_buy") or _te_ds._get_breakeven_mult(entry, sym) if entry else 0
            simple_bep = entry * _bep_m if entry and _bep_m else 0
            cd         = _te_ds._loss_cooldown.get(sym, 0)
            rows.append({
                "symbol":          sym,
                "entry":           round(entry, 8),
                "quantity":        qty,
                "budget_usdt":     budget,
                "current_price":   round(cur, 8),
                "simple_bep":      round(simple_bep, 8),
                "real_bep":        round(real_bep, 8),
                "above_real_bep":  bool(cur >= real_bep) if (cur and real_bep) else False,
                "real_bep_gap_pct": round((real_bep - cur) / cur * 100, 4) if (cur and real_bep) else None,
                "loss_cooldown_remaining_s": round(max(0.0, cd - _now), 1),
                "take_profit_enabled": _te_ds._take_profit_enabled,
                "take_profit_mult":    round(_te_ds._user_tp_mult, 6),
                "opened_at_ts":    p.get("opened_at_ts", 0),
                "hold_sec":        round(_now - p.get("opened_at_ts", _now), 1),
            })
        return {
            "positions": rows,
            "_take_profit_enabled": _te_ds._take_profit_enabled,
            "_user_tp_mult":        round(_te_ds._user_tp_mult, 6),
            "_stop_loss_mult":      round(_te_ds._stop_loss_mult, 6),
        }
    except Exception as e:
        return {"error": str(e)}


class ChatRequest(BaseModel):
    messages: list[dict]
    apiKey: str


@app.post("/api/chat")
async def api_chat(req: ChatRequest):
    """Proxy streaming chat to Anthropic API, converting to OpenAI SSE format."""
    import aiohttp
    import json as _json

    if not req.apiKey:
        return Response(content="data: [DONE]\n\n", media_type="text/event-stream")

    anthropic_messages = [
        {"role": m["role"], "content": m["content"]}
        for m in req.messages
        if m.get("role") in ("user", "assistant") and m.get("content")
    ]

    async def generate():
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    "https://api.anthropic.com/v1/messages",
                    headers={
                        "x-api-key": req.apiKey,
                        "anthropic-version": "2023-06-01",
                        "content-type": "application/json",
                    },
                    json={
                        "model": "claude-haiku-4-5-20251001",
                        "max_tokens": 1024,
                        "stream": True,
                        "messages": anthropic_messages,
                    },
                ) as resp:
                    async for raw_line in resp.content:
                        line = raw_line.decode("utf-8").rstrip("\r\n")
                        if not line.startswith("data: "):
                            continue
                        data_str = line[6:]
                        if data_str == "[DONE]":
                            yield "data: [DONE]\n\n"
                            return
                        try:
                            event = _json.loads(data_str)
                            if event.get("type") == "content_block_delta":
                                text = event.get("delta", {}).get("text", "")
                                if text:
                                    chunk = _json.dumps({"choices": [{"delta": {"content": text}}]})
                                    yield f"data: {chunk}\n\n"
                            elif event.get("type") == "message_stop":
                                yield "data: [DONE]\n\n"
                                return
                        except Exception:
                            pass
        except Exception as exc:
            err = _json.dumps({"choices": [{"delta": {"content": f"\n\n[Error: {exc}]"}}]})
            yield f"data: {err}\n\n"
        yield "data: [DONE]\n\n"

    return StreamingResponse(generate(), media_type="text/event-stream")


@app.get("/api/version")
def api_version():
    return _read_frontend_version()


@app.get("/version.json")
def serve_version_json(response: Response):
    """
    Returns the latest version available on GitHub so the browser can detect
    when new code has been pushed. Falls back to the local dist/version.json
    when GitHub is unreachable.
    """
    import pathlib, json as _json, urllib.request as _req, time as _t
    global _github_ver_cache, _github_ver_cache_ts
    response.headers["Cache-Control"] = "no-store"

    now = _t.time()
    if now - _github_ver_cache_ts > _GITHUB_VER_TTL or not _github_ver_cache:
        try:
            url = _GITHUB_VERSION_URL + "?t=" + str(int(now))
            with _req.urlopen(url, timeout=4) as r:
                _github_ver_cache = _json.loads(r.read())
                _github_ver_cache_ts = now
        except Exception:
            pass  # keep stale cache or fall through to local

    if _github_ver_cache:
        data = dict(_github_ver_cache)
    else:
        # Fallback: local dist/version.json (try trading-bot/dist then project root dist)
        for candidate in [
            pathlib.Path(__file__).parent / "dist" / "version.json",
            pathlib.Path(__file__).parent.parent / "dist" / "version.json",
        ]:
            if candidate.exists():
                try:
                    data = _json.loads(candidate.read_text())
                    break
                except Exception:
                    pass
        else:
            data = {"version": "3.8.0", "buildTime": "unknown", "commit": "unknown"}

    data["deployId"] = _DEPLOY_ID
    return data


@app.post("/api/update")
def api_update():
    """Pull latest code from GitHub, rebuild the frontend, and restart the bot."""
    import pathlib, subprocess, threading, time as _t

    def _do_update():
        _t.sleep(0.6)  # let HTTP response reach the client first
        app_dir = pathlib.Path(__file__).parent.parent
        try:
            subprocess.run(["git", "fetch", "origin", "main"],
                           cwd=str(app_dir), check=True, timeout=30)
            subprocess.run(["git", "reset", "--hard", "origin/main"],
                           cwd=str(app_dir), check=True, timeout=30)
            subprocess.run(["npm", "run", "build"],
                           cwd=str(app_dir), check=True, timeout=300)
            print("[Update] Rebuild complete — restarting bot", flush=True)
        except Exception as exc:
            print(f"[Update] ERROR: {exc}", flush=True)
            return
        # Restart the current Python process in-place
        import os
        os.execv(sys.executable, [sys.executable] + sys.argv)

    threading.Thread(target=_do_update, daemon=True).start()
    return {"success": True, "message": "Update started — bot will restart in ~30 s"}


# ── Futures agent endpoints ────────────────────────────────────────────────────────
# All endpoints below are additive only — no existing route is modified.

@app.get("/api/futures/all")
def api_futures_all():
    """Single-call response combining status, positions, signals, and recent trades.

    The frontend polls this instead of making 4 separate requests, cutting
    network round-trips and keeping futures data consistent within a snapshot.
    """
    try:
        import futures_engine
        status    = futures_engine.get_futures_status()
        positions = futures_engine.get_futures_positions()
        signals   = futures_engine.get_futures_signals()
    except Exception as exc:
        status    = {"running": False, "balance": 0.0, "equity": 0.0,
                     "positions": 0, "total_pnl": 0.0, "win_rate": 0.0,
                     "trade_count": 0, "active": False}
        positions = []
        signals   = []

    try:
        trades = database.get_recent_futures_trades(30)
    except Exception:
        trades = []

    return {
        "status":    status,
        "positions": positions,
        "signals":   signals,
        "trades":    trades,
    }


@app.get("/api/futures/status")
def api_futures_status():
    try:
        import futures_engine
        return futures_engine.get_futures_status()
    except Exception as exc:
        return {"error": str(exc), "running": False, "balance": 0.0,
                "equity": 0.0, "positions": 0, "total_pnl": 0.0,
                "win_rate": 0.0, "trade_count": 0}


@app.get("/api/futures/positions")
def api_futures_positions():
    try:
        import futures_engine
        return {"positions": futures_engine.get_futures_positions()}
    except Exception as exc:
        return {"positions": [], "error": str(exc)}


@app.get("/api/futures/trades")
def api_futures_trades():
    try:
        trades = database.get_recent_futures_trades(50)
        return {"trades": trades}
    except Exception as exc:
        return {"trades": [], "error": str(exc)}


@app.get("/api/futures/signals")
def api_futures_signals():
    try:
        import futures_engine
        return {"signals": futures_engine.get_futures_signals()}
    except Exception as exc:
        return {"signals": [], "error": str(exc)}


@app.post("/api/futures/start")
def api_futures_start():
    try:
        import futures_engine
        futures_engine.set_futures_active(True)
        database.log_activity("[Futures] Trading started", "info")
        return {"success": True, "active": True}
    except Exception as exc:
        return {"success": False, "error": str(exc)}


@app.post("/api/futures/pause")
def api_futures_pause():
    try:
        import futures_engine
        futures_engine.set_futures_active(False)
        database.log_activity("[Futures] Trading paused", "info")
        return {"success": True, "active": False}
    except Exception as exc:
        return {"success": False, "error": str(exc)}


@app.post("/api/futures/settings")
def api_futures_settings(body: dict = Body(...)):
    try:
        import futures_engine
        allowed = {
            "leverage", "budget_usdt", "budget_mode", "budget_pct",
            "allocation_usdt", "take_profit_pct", "stop_loss_pct",
            "stop_loss_enabled", "min_signals", "max_positions",
        }
        patch = {k: v for k, v in body.items() if k in allowed}
        if "leverage" in patch:
            patch["leverage"] = max(1, min(20, int(patch["leverage"])))
        if "min_signals" in patch:
            patch["min_signals"] = max(1, min(6, int(patch["min_signals"])))
        if "max_positions" in patch:
            patch["max_positions"] = max(1, min(100, int(patch["max_positions"])))
        futures_engine.update_futures_settings(patch)
        return {"success": True, "settings": patch}
    except Exception as exc:
        return {"success": False, "error": str(exc)}


@app.post("/api/futures/close/{pos_id}")
def api_futures_close(pos_id: int):
    try:
        import futures_engine
        trade = futures_engine.close_position_by_id(pos_id)
        if trade is None:
            return {"success": False, "error": f"Position {pos_id} not found"}
        return {"success": True, "trade": trade}
    except Exception as exc:
        return {"success": False, "error": str(exc)}


@app.post("/api/futures/close_all")
def api_futures_close_all():
    try:
        import futures_engine
        trades = futures_engine.close_all_positions()
        return {"success": True, "closed": len(trades), "trades": trades}
    except Exception as exc:
        return {"success": False, "error": str(exc)}


@app.post("/api/futures/reset")
def api_futures_reset(body: dict = Body(...)):
    try:
        import futures_engine
        starting = float(body.get("starting_usdt", config.FUTURES_STARTING_USDT))
        futures_engine.reset_futures_wallet(starting)
        database.log_activity(f"[Futures] Wallet reset to {starting:.2f} USDT", "info")
        return {"success": True, "balance": starting}
    except Exception as exc:
        return {"success": False, "error": str(exc)}


def start_control_api():
    """Block the main thread on uvicorn — all bot logic starts via lifespan."""
    import pathlib

    # Mount React build INSIDE start_control_api so any failure (missing
    # aiofiles, missing dist/) is caught and logged — it never prevents the
    # HTTP server from binding and passing Railway's health check.
    # Check trading-bot/dist/ first, then repo-root dist/ as fallback.
    # Repo-root dist/ is what gets committed when building locally.
    _dist_candidates = [
        pathlib.Path(__file__).parent / "dist",
        pathlib.Path(__file__).parent.parent / "dist",
    ]
    dist = next((d for d in _dist_candidates if d.exists()), None)
    if dist:
        try:
            from fastapi.staticfiles import StaticFiles
            from fastapi.responses import FileResponse as _FR

            # Hashed assets (filename changes every build) — long cache is safe
            _assets_dir = dist / "assets"
            if _assets_dir.exists():
                app.mount("/assets", StaticFiles(directory=str(_assets_dir)), name="assets")

            # index.html must NEVER be cached — it references hashed bundles
            _index_path = dist / "index.html"

            @app.get("/")
            @app.get("/{full_path:path}")
            def _serve_spa(full_path: str = ""):
                if full_path.startswith("api/") or full_path.startswith("assets/"):
                    from fastapi.responses import Response as _Resp
                    return _Resp(status_code=404)
                if _index_path.exists():
                    return _FR(
                        str(_index_path),
                        media_type="text/html",
                        headers={
                            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
                            "Pragma":        "no-cache",
                            "Expires":       "0",
                        },
                    )
                from fastapi.responses import Response as _Resp
                return _Resp(status_code=404)

            print(f"[ControlAPI] Serving React build from {dist} (index.html: no-cache)")
        except Exception as e:
            print(f"[ControlAPI] WARNING: Could not mount static files: {e}")
            print("[ControlAPI] Continuing without static file serving — API-only mode")
    else:
        print("[ControlAPI] No dist/ folder — API-only mode")

    port = int(os.getenv("PORT", 8000))
    print(f"[ControlAPI] Binding to 0.0.0.0:{port}")
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
