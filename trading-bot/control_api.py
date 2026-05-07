"""
FastAPI control server — binds to $PORT on the main thread so Railway's
health-check can reach it immediately.  All trading-bot logic (DB init,
history download, WebSocket feed, strategy loop) starts in the FastAPI
lifespan as async background tasks — nothing blocks the HTTP server.
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

# Unique ID generated once per process start.  Railway restarts the process on
# every deploy, so this changes on every deployment — the browser can compare
# the stored value against the polled value to detect new deploys reliably.
_DEPLOY_ID = str(uuid.uuid4())

import uvicorn
from fastapi import FastAPI, Response, Body
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

import config
import database
from connection import get_mode


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

        # 4. Apply startup defaults and auto-resume logic.
        #    stop_loss and smart_hold are ALWAYS forced OFF on every deploy —
        #    the user must explicitly enable them each session.
        #    trading_active is preserved so a running bot resumes after a redeploy.
        _s = _load_strategy()
        _auto_patch: dict = {
            "pause_reason":       None,
            "stop_loss_enabled":  False,   # always OFF — user opts in each session
            "smart_hold_enabled": False,   # always OFF — user opts in each session
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


# ── Internal helpers ─────────────────────────────────────────────────────────

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


def _get_positions():
    try:
        from trade_engine import get_open_positions, _rest_px, _signal_cache, _signal_cache_lock
        from data_collector import prices
        pos = get_open_positions()
        out = []
        for p in pos:
            sym    = p["symbol"]
            # Price priority chain — fall through to the next source whenever
            # the previous one is missing or 0:
            #   1. _rest_px  (REST refresh every 2 s — usually freshest)
            #   2. WebSocket prices dict (sub-second updates when subscribed)
            #   3. Latest cached signal price (60 s old at worst)
            #   4. The position's entry price (so display never shows 0)
            # Without 3 and 4 the frontend was rendering Now == Entry every
            # time a single source briefly missed the symbol.
            price  = _rest_px.get(sym) or prices.get(sym, 0)
            if not price:
                with _signal_cache_lock:
                    sc_entry = _signal_cache.get(sym)
                if sc_entry and sc_entry.get("price", 0) > 0:
                    price = sc_entry["price"]
            # Do NOT fall back to entry_price here — returning entry_price as
            # current_price causes the frontend to show 0 P&L change even when
            # the WebSocket has a live price (the truthy entry_price short-circuits
            # the || chain in the UI before the WS lookup is ever evaluated).
            entry  = p.get("entry_price", 0)
            qty    = p.get("quantity", 0)
            target = p.get("exit_target") or (entry * (1 + config.FEE_RATE_BNB * 2) if config.BNB_FEE_MODE else entry * 1.002)
            pnl    = (price - entry) * qty if price and entry else 0
            dist   = ((price - target) / target * 100) if target and price else 0
            out.append({
                **p,
                "current_price":   price,
                "exit_target":     round(target, 8),
                "breakeven_price": round(target, 6),
                "unrealized_pnl":  round(pnl, 4),
                "dist_to_exit_pct": round(dist, 4),
                "dist_to_bep_pct":  round(dist, 4),   # alias — embedded dashboard reads this name
                "profitable":      price >= target if price and target else False,
            })
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


def _get_usdt_balance() -> float:
    try:
        from connection import client, get_mode
        if get_mode() != "live":
            # Fast path: read directly from PaperClient._balances
            if hasattr(client, "_balances"):
                with client._lock:
                    return float(client._balances.get("USDT", 0.0))
        acc = client.get_account()
        for b in acc["balances"]:
            if b["asset"] == "USDT":
                return float(b["free"])
    except Exception:
        pass
    return float(os.getenv("STARTING_PAPER_USDT", "10000.0"))


def _trades_today() -> int:
    trades = database.get_recent_trades(limit=200)
    today  = datetime.now(timezone.utc).date().isoformat()
    return sum(1 for t in trades if (t.get("timestamp_sell") or "").startswith(today))


def _overall_win_rate() -> float:
    trades = database.get_recent_trades(limit=500)
    closed = [t for t in trades if t.get("exit_price") is not None]
    if not closed:
        return 0.0
    return sum(1 for t in closed if t.get("profitable")) / len(closed)


# ── Endpoints ────────────────────────────────────────────────────────────────

@app.get("/health")
def health():
    return {"ok": True, "mode": get_mode(), "ts": datetime.now(timezone.utc).isoformat()}


@app.get("/status")
def status():
    strategy = _load_strategy()
    return {
        "mode":           get_mode(),
        "trading_active": strategy.get("trading_active", False),
        "pause_reason":   strategy.get("pause_reason"),
        "open_positions": _get_positions(),
        "usdt_balance":   _get_usdt_balance(),
        "trades_today":   _trades_today(),
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


# ── HTML Dashboard ───────────────────────────────────────────────────────────

DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
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
<div id="banner" class="banner paper">PAPER MODE — simulated trading only, no real money</div>

<div class="container">
  <div class="controls">
    <span class="live-dot"></span>
    <span id="last-update" style="color:#64748b;font-size:12px;">Connecting…</span>
    <button class="btn btn-pause"   onclick="pause()">⏸ Pause</button>
    <button class="btn btn-resume"  onclick="resume()">▶ Resume</button>
    <button class="btn btn-refresh" onclick="refresh()">↻ Refresh</button>
  </div>

  <div class="grid-4" id="metrics">
    <div class="card"><div class="label">USDT Balance</div><div class="value" id="m-balance">—</div></div>
    <div class="card"><div class="label">Open Positions</div><div class="value accent" id="m-open">—</div></div>
    <div class="card"><div class="label">Trades Today</div><div class="value" id="m-today">—</div></div>
    <div class="card"><div class="label">Win Rate</div><div class="value" id="m-winrate">—</div></div>
  </div>

  <div class="section">
    <h2>Open Positions</h2>
    <table id="positions-table">
      <thead><tr>
        <th>Symbol</th><th>Entry $</th><th>Current $</th><th>BEP $</th>
        <th>Qty</th><th>Budget</th><th>Unrealised P&L</th><th>Dist to BEP</th><th>Status</th>
      </tr></thead>
      <tbody id="positions-body"><tr><td colspan="9" style="color:#64748b;text-align:center">Loading…</td></tr></tbody>
    </table>
  </div>

  <div class="section">
    <h2>Recent Trades</h2>
    <table id="trades-table">
      <thead><tr>
        <th>Coin</th><th>Entry $</th><th>Exit $</th><th>Qty</th>
        <th>Budget</th><th>Buy Fee</th><th>Sell Fee</th>
        <th>Net P&L</th><th>Duration</th><th>Result</th>
      </tr></thead>
      <tbody id="trades-body"><tr><td colspan="10" style="color:#64748b;text-align:center">Loading…</td></tr></tbody>
    </table>
  </div>

  <div class="section">
    <h2>Learned Patterns</h2>
    <table id="patterns-table">
      <thead><tr>
        <th>Coin</th><th>RSI Range</th><th>BB Position</th><th>Volume</th>
        <th>MA</th><th>Occurrences</th><th>Confidence</th><th>Avg Profit %</th>
      </tr></thead>
      <tbody id="patterns-body"><tr><td colspan="8" style="color:#64748b;text-align:center">Loading…</td></tr></tbody>
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
    tbody.innerHTML = '<tr><td colspan="9" style="color:#64748b;text-align:center">No open positions</td></tr>';
    return;
  }
  tbody.innerHTML = positions.map(p => {
    const pnl = p.unrealized_pnl || 0;
    const dist = p.dist_to_bep_pct || 0;
    const isProfitable = p.profitable;
    const distPct = Math.min(100, Math.max(0, 50 + dist * 25));
    const barColor = isProfitable ? '#22c55e' : '#ef4444';
    const pill = isProfitable
      ? '<span class="pill pill-gain">✅ Profitable</span>'
      : '<span class="pill pill-wait">⏳ Waiting</span>';
    return \`<tr>
      <td class="mono">\${p.symbol}</td>
      <td class="mono">\${fmtP(p.entry_price)}</td>
      <td class="mono \${isProfitable?'gain':'loss'}">\${fmtP(p.current_price)}</td>
      <td class="mono accent">\${fmtP(p.breakeven_price)}</td>
      <td class="mono">\${fmt(p.quantity,6)}</td>
      <td class="mono">\${fmt(p.budget_usdt,2)} USDT</td>
      <td class="mono \${pnl>=0?'gain':'loss'}">\${pnl>=0?'+':''}\${fmt(pnl,4)} USDT</td>
      <td>
        <div class="progress-bar" title="\${fmt(dist,4)}% to BEP">
          <div class="progress-fill" style="width:\${distPct}%;background:\${barColor}"></div>
        </div>
        <div style="font-size:10px;color:#64748b;margin-top:2px">\${dist>=0?'+':''}\${fmt(dist,4)}%</div>
      </td>
      <td>\${pill}</td>
    </tr>\`;
  }).join('');
}

function renderTrades(trades) {
  const tbody = document.getElementById('trades-body');
  if (!trades.length) {
    tbody.innerHTML = '<tr><td colspan="10" style="color:#64748b;text-align:center">No completed trades yet</td></tr>';
    return;
  }
  tbody.innerHTML = trades.map(t => {
    const pnl = t.net_profit || 0;
    const dur = t.duration_seconds ? (t.duration_seconds >= 3600
      ? fmt(t.duration_seconds/3600,1)+'h'
      : Math.round(t.duration_seconds/60)+'m') : '—';
    const pill = t.profitable
      ? '<span class="pill pill-gain">WIN</span>'
      : '<span class="pill pill-loss">LOSS</span>';
    return \`<tr>
      <td class="mono">\${t.coin}</td>
      <td class="mono">\${fmtP(t.entry_price)}</td>
      <td class="mono">\${fmtP(t.exit_price)}</td>
      <td class="mono">\${fmt(t.quantity,6)}</td>
      <td class="mono">\${fmt(t.budget_usdt,2)}</td>
      <td class="mono warn">\${fmt(t.buy_fee,4)}</td>
      <td class="mono warn">\${fmt(t.sell_fee,4)}</td>
      <td class="mono \${pnl>=0?'gain':'loss'}">\${pnl>=0?'+':''}\${fmt(pnl,4)}</td>
      <td class="mono">\${dur}</td>
      <td>\${pill}</td>
    </tr>\`;
  }).join('');
}

function renderPatterns(patterns) {
  const tbody = document.getElementById('patterns-body');
  if (!patterns.length) {
    tbody.innerHTML = '<tr><td colspan="8" style="color:#64748b;text-align:center">No patterns yet — patterns build after 3+ trades</td></tr>';
    return;
  }
  tbody.innerHTML = patterns.map(p => {
    const conf = (p.confidence_score || 0) * 100;
    const col = conf >= 65 ? 'gain' : conf >= 40 ? 'warn' : 'loss';
    return \`<tr>
      <td class="mono">\${p.coin}</td>
      <td class="mono">\${p.rsi_range||'—'}</td>
      <td>\${p.bb_position||'—'}</td>
      <td>\${p.volume_trend||'—'}</td>
      <td>\${p.ma_position||'—'}</td>
      <td class="text-right mono">\${p.occurrence_count}</td>
      <td class="text-right mono \${col}">\${fmt(conf,1)}%</td>
      <td class="text-right mono \${(p.avg_profit_pct||0)>=0?'gain':'loss'}">\${(p.avg_profit_pct||0)>=0?'+':''}\${fmt(p.avg_profit_pct,3)}%</td>
    </tr>\`;
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

        acc = client.get_account()
        balances = [
            {"asset": b["asset"], "free": float(b["free"]), "locked": float(b["locked"])}
            for b in acc["balances"]
            if float(b["free"]) + float(b["locked"]) > 0
        ]
        usdt_free = sum(b["free"] for b in balances if b["asset"] == "USDT")

        # Total portfolio value = free USDT + mark-to-market value of open positions
        total_value = usdt_free
        for pos in get_open_positions():
            sym = pos["symbol"]
            px  = _rest_px.get(sym) or live_prices.get(sym) or pos["entry_price"]
            total_value += pos["quantity"] * px

        # Realized P&L: single source of truth — SQL SUM from trades table
        realized_pnl = database.get_realized_pnl(mode="paper")

        # Session P&L: current total portfolio value minus the balance at last reset
        starting_str  = database.get_setting("paper_starting_balance")
        starting_bal  = float(starting_str) if starting_str else float(os.getenv("STARTING_PAPER_USDT", "10000.0"))
        session_pnl   = round(total_value - starting_bal, 4)

        return {
            "balances":        balances,
            "total_usdt":      round(usdt_free, 4),
            "total_value":     round(total_value, 4),
            "realized_pnl":    round(realized_pnl, 4),
            "session_pnl":     session_pnl,
            "starting_balance": round(starting_bal, 4),
            "mode":            "paper",
        }
    except Exception as e:
        return {"balances": [], "total_usdt": 0.0, "total_value": 0.0,
                "realized_pnl": 0.0, "session_pnl": 0.0, "mode": "paper", "error": str(e)}


@app.get("/bot-dashboard", response_class=HTMLResponse)
def dashboard():
    """Python bot dashboard — accessible at /bot-dashboard when React app is at /."""
    return HTMLResponse(DASHBOARD_HTML)


# ── Start as daemon thread ────────────────────────────────────────────────────

@app.get("/api/status")
def api_status():
    strategy = _load_strategy()
    trades   = database.get_recent_trades(limit=500)
    sells    = [t for t in trades if t.get("exit_price") is not None]
    wins     = sum(1 for t in sells if (t.get("net_profit") or 0) > 0)
    # Realized P&L: single source of truth — SQL SUM directly from trades table
    realized = database.get_realized_pnl(mode="paper")
    initial  = float(strategy.get("initial_balance_usdt", 0))
    balance  = round(_get_usdt_balance(), 2)
    approved = [c["symbol"] for c in strategy.get("approved_coins", []) if c.get("approved")]
    return {
        "running":          strategy.get("trading_active", False),
        "mode":             get_mode(),
        "balance_usdt":     balance,
        "paper_balance":    balance,
        "initial_balance":  initial or balance,
        "open_positions":   len(_get_positions()),
        "trades_today":     database.get_trades_today_count(),
        "win_rate":         round(wins / len(sells), 3) if sells else 0.0,
        "wins":             wins,
        "losses":           len(sells) - wins,
        "total_trades":     len(sells),
        "realized_pnl":     round(realized, 4),
        "watched_coins":    approved or config.WATCHED_COINS,
        "data_dir":           database._DATA_DIR,
        "db_path":            database.DB_PATH,
        "data_persistent":    database._DATA_DIR == "/data",
        "sell_monitor_alive": _sell_monitor_alive(),
    }


@app.get("/api/positions")
def api_positions():
    return {"positions": _get_positions()}


@app.get("/api/trades")
def api_trades():
    return {"trades": database.get_recent_trades(limit=200)}


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
    _write_strategy_patch({"approved_coins": new_approved})

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
        from trade_engine import (get_budget_for_coin, _positions, _positions_lock,
                                    _breakeven_mult, _rebuild_pos_index)
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

        _client.update_price(sym, price)
        result = _client.order_market_buy(symbol=sym, quoteOrderQty=budget)
        fill       = result.get("fills", [{}])[0]
        fill_price = float(fill.get("price", price))
        qty        = float(result.get("executedQty", 0))
        if qty <= 0:
            return {"ok": False, "error": "Order returned 0 quantity"}

        exit_target = round(fill_price * _breakeven_mult, 8)
        pos = {
            "symbol": sym, "entry_price": fill_price, "quantity": qty,
            "budget_usdt": budget, "timestamp": datetime.now(timezone.utc).isoformat(),
            "mode": get_mode(), "exit_target": exit_target,
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
        return {"ok": True, "symbol": sym, "price": price}
    except Exception as e:
        return {"ok": False, "error": str(e)}


class ModeRequest(BaseModel):
    mode: str           # "paper" or "live"
    api_key: str = ""
    api_secret: str = ""


def _update_env_file(updates: dict):
    env_path = ".env"
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


@app.post("/api/mode")
def api_set_mode(req: ModeRequest):
    if req.mode not in ("paper", "live"):
        return {"ok": False, "error": "mode must be paper or live"}

    updates = {"MODE": req.mode}
    if req.mode == "live":
        if req.api_key:
            updates["BINANCE_API_KEY"] = req.api_key
        if req.api_secret:
            updates["BINANCE_API_SECRET"] = req.api_secret

    _update_env_file(updates)
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

    fee_rate = config.FEE_RATE_BNB if config.BNB_FEE_MODE else config.FEE_RATE_STANDARD
    bep_mult = 1.0 + fee_rate * 2

    positions = _get_positions()
    checks = []
    for p in positions:
        sym   = p["symbol"]
        entry = p.get("entry_price", 0)
        price = live_prices.get(sym, 0)
        bep   = entry * bep_mult if entry else 0
        sl    = entry * (1.0 - config.STOP_LOSS_PCT) if entry else 0
        pct   = ((price - entry) / entry * 100) if entry else 0
        checks.append({
            "symbol":          sym,
            "entry":           entry,
            "current":         price,
            "pct_from_entry":  round(pct, 4),
            "breakeven_price": round(bep, 6),
            "stop_loss":       round(sl, 6),
            "profitable":      price > bep if price and bep else False,
            "sl_hit":          price <= sl if price and sl else False,
        })

    return {
        "sell_monitor_alive": alive,
        "heartbeat_age_sec":  age,
        "breakeven_pct":      round(fee_rate * 2 * 100, 4),
        "stop_loss_pct":      config.STOP_LOSS_PCT * 100,
        "sell_trigger":       "price > entry × (1 + buy_fee + sell_fee)",
        "open_positions":     len(checks),
        "positions":          checks,
    }


@app.get("/api/settings")
def api_get_settings():
    """Return current bot risk/strategy settings."""
    s = _load_strategy()
    return {
        "ok":                 True,
        "stop_loss_enabled":  s.get("stop_loss_enabled",  True),
        "stop_loss_pct":      s.get("stop_loss_pct",      2.0),
        "take_profit_enabled":s.get("take_profit_enabled", True),
        "take_profit_pct":    s.get("take_profit_pct",    0.5),
        "smart_hold_enabled": s.get("smart_hold_enabled", False),
        "trailing_stop_pct":  s.get("trailing_stop_pct",  0.5),
        "reinvest_profits":   s.get("reinvest_profits",   False),
        "max_positions":      s.get("max_positions",       20),
        "min_signals":        s.get("min_signals",          config.MIN_SIGNALS_TO_BUY),
        "strategy_notes":     s.get("strategy_notes",      ""),
    }


class SettingsRequest(BaseModel):
    stop_loss_enabled:  Optional[bool]  = None
    stop_loss_pct:      Optional[float] = None
    take_profit_enabled:Optional[bool]  = None
    take_profit_pct:    Optional[float] = None
    smart_hold_enabled: Optional[bool]  = None
    trailing_stop_pct:  Optional[float] = None
    reinvest_profits:   Optional[bool]  = None
    max_positions:      Optional[int]   = None
    min_signals:        Optional[int]   = None
    strategy_notes:     Optional[str]   = None


@app.post("/api/settings")
def api_save_settings(req: SettingsRequest):
    """Save bot risk/strategy settings into strategy.json."""
    try:
        patch: dict = {}
        if req.stop_loss_enabled   is not None: patch["stop_loss_enabled"]  = bool(req.stop_loss_enabled)
        if req.stop_loss_pct       is not None: patch["stop_loss_pct"]      = max(0.1, min(20.0, req.stop_loss_pct))
        if req.take_profit_enabled is not None: patch["take_profit_enabled"] = bool(req.take_profit_enabled)
        if req.take_profit_pct     is not None: patch["take_profit_pct"]    = max(0.1, min(50.0, req.take_profit_pct))
        if req.smart_hold_enabled  is not None: patch["smart_hold_enabled"] = bool(req.smart_hold_enabled)
        if req.trailing_stop_pct   is not None: patch["trailing_stop_pct"]  = max(0.1, min(10.0, req.trailing_stop_pct))
        if req.reinvest_profits    is not None: patch["reinvest_profits"]   = bool(req.reinvest_profits)
        if req.max_positions       is not None: patch["max_positions"]      = max(1,   min(100,  req.max_positions))
        if req.min_signals         is not None: patch["min_signals"]        = max(1,   min(6,    req.min_signals))
        if req.strategy_notes      is not None: patch["strategy_notes"]     = req.strategy_notes[:2000]
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


# Tiny response cache — coalesces overlapping polls (the frontend has both a 5 s
# and a 1 s interval; without this they each issue a full DB sweep, which holds
# the global SQLite lock and starves the sell monitor for ~50 ms each call).
_API_ALL_CACHE: dict = {"ts": 0.0, "data": None}
_API_ALL_TTL = 0.8   # seconds — slightly less than the 1 s fast-poll cadence


@app.get("/api/all")
def api_all():
    """Single endpoint returning status + positions + trades + activity.
    Reduces frontend from 4 concurrent fetches to 1, cutting Railway load 4×."""
    now_ts = time.time()
    cached = _API_ALL_CACHE.get("data")
    if cached is not None and (now_ts - _API_ALL_CACHE["ts"]) < _API_ALL_TTL:
        return cached

    strategy = _load_strategy()
    # Single 500-row sweep; reuse the slice for the 200-row "trades" payload —
    # eliminates a second identical query that previously ran on every poll.
    trades   = database.get_recent_trades(limit=500)
    sells    = [t for t in trades if t.get("exit_price") is not None]
    wins     = sum(1 for t in sells if (t.get("net_profit") or 0) > 0)
    realized = database.get_realized_pnl(mode="paper")
    initial  = float(strategy.get("initial_balance_usdt", 0))
    balance  = round(_get_usdt_balance(), 2)
    approved = [c["symbol"] for c in strategy.get("approved_coins", []) if c.get("approved")]
    positions = _get_positions()  # also reused below — was called twice
    payload = {
        "status": {
            "running":         strategy.get("trading_active", False),
            "mode":            get_mode(),
            "balance_usdt":    balance,
            "paper_balance":   balance,
            "initial_balance": initial or balance,
            "open_positions":  len(positions),
            "trades_today":    database.get_trades_today_count(),
            "win_rate":        round(wins / len(sells), 3) if sells else 0.0,
            "wins":            wins,
            "losses":          len(sells) - wins,
            "total_trades":    len(sells),
            "realized_pnl":    round(realized, 4),
            "watched_coins":   approved or config.WATCHED_COINS,
            "data_persistent": database._DATA_DIR == "/data",
            "data_dir":        database._DATA_DIR,
            "stop_loss_enabled":  strategy.get("stop_loss_enabled",  True),
            "stop_loss_pct":      strategy.get("stop_loss_pct",      2.0),
            "take_profit_enabled":strategy.get("take_profit_enabled", True),
            "take_profit_pct":    strategy.get("take_profit_pct",    0.5),
            "smart_hold_enabled": strategy.get("smart_hold_enabled", False),
            "trailing_stop_pct":  strategy.get("trailing_stop_pct",  0.5),
            "reinvest_profits":   strategy.get("reinvest_profits",   False),
            "max_positions":      strategy.get("max_positions",       20),
            "min_signals":        strategy.get("min_signals",          config.MIN_SIGNALS_TO_BUY),
            "strategy_notes":     strategy.get("strategy_notes",      ""),
        },
        "positions": positions,
        "trades":    trades[:200],
        "activity":  database.get_activity_log(limit=100),
        "signals":   _get_signal_snapshot(),
    }
    _API_ALL_CACHE["ts"]   = now_ts
    _API_ALL_CACHE["data"] = payload
    return payload


@app.get("/api/debug")
def api_debug():
    """Diagnostic endpoint — returns full bot health including startup status."""
    import sys
    strategy = _load_strategy()
    approved = [c["symbol"] for c in strategy.get("approved_coins", []) if c.get("approved")]
    try:
        from trade_engine import get_open_positions, _sell_monitor_heartbeat, _breakeven_mult
        pos_count = len(get_open_positions())
        sm_alive  = (time.time() - _sell_monitor_heartbeat) < 10 if _sell_monitor_heartbeat else False
        bep_mult  = _breakeven_mult
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
    import pathlib, json as _json
    vf = pathlib.Path(__file__).parent / "dist" / "version.json"
    if vf.exists():
        try:
            return _json.loads(vf.read_text())
        except Exception:
            pass
    return {"version": "unknown"}


@app.get("/version.json")
def serve_version_json(response: Response):
    """
    Dynamic version endpoint — includes deployId (UUID generated at process
    start) so the browser can detect a new Railway deployment without relying
    on Docker build-cache or bundle fingerprint matching.
    """
    import pathlib, json as _json
    response.headers["Cache-Control"] = "no-store"
    vf = pathlib.Path(__file__).parent / "dist" / "version.json"
    try:
        data = _json.loads(vf.read_text())
    except Exception:
        data = {"version": "3.8.0", "buildTime": "unknown", "commit": "unknown"}
    data["deployId"] = _DEPLOY_ID
    return data


@app.post("/api/update")
def api_update():
    """Railway deployments are handled automatically — the client just needs to reload."""
    return {"success": False, "message": "Reload to pick up the latest build"}


# ── Futures agent endpoints ───────────────────────────────────────────────────
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
    dist = pathlib.Path(__file__).parent / "dist"
    if dist.exists():
        try:
            from fastapi.staticfiles import StaticFiles
            app.mount("/", StaticFiles(directory=str(dist), html=True), name="static")
            print(f"[ControlAPI] Serving React build from {dist}")
        except Exception as e:
            print(f"[ControlAPI] WARNING: Could not mount static files: {e}")
            print("[ControlAPI] Continuing without static file serving — API-only mode")
    else:
        print("[ControlAPI] No dist/ folder — API-only mode")

    port = int(os.getenv("PORT", 8000))
    print(f"[ControlAPI] Binding to 0.0.0.0:{port}")
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
