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
from fastapi import FastAPI, Response
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

    # 1. DB (already done in main.py before uvicorn starts, but idempotent)
    database.init_db()
    print(f"[ControlAPI] DATA DIRECTORY : {database._DATA_DIR}")
    print(f"[ControlAPI] DATABASE FILE  : {database.DB_PATH}")
    database.log_activity(f"Bot started — DB: {database.DB_PATH}", "info")

    # 2. Regenerate strategy.json
    strategy_engine.write_default_strategy()

    # 3. Restore open positions
    trade_engine.load_positions_from_db()

    # 4. History download runs in a background daemon thread — NOT awaited.
    #    Awaiting it blocks the lifespan yield for ~2 min (55 coins × REST calls),
    #    which causes Railway health-checks to time out and restart the deploy.
    threading.Thread(target=data_collector.download_history, daemon=True).start()

    # 5. Register callbacks
    data_collector.register_price_callback(trade_engine.realtime_monitor)
    data_collector.register_kline_callback(trade_engine.update_coin_signals)

    # 6. Launch WebSocket feed + strategy loop + signal scanner + guardian as async tasks
    asyncio.create_task(data_collector.start_websocket())
    asyncio.create_task(strategy_engine.strategy_loop())
    asyncio.create_task(trade_engine.signal_scanner(data_collector.prices))
    asyncio.create_task(trade_engine.position_guardian())  # REST backstop for sells

    print("[ControlAPI] All trading tasks started.")
    yield
    # Shutdown — daemon threads and tasks stop with the process


app = FastAPI(title="Trading Bot Control API", version="1.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# ── Internal helpers ─────────────────────────────────────────────────────────

def _load_strategy() -> dict:
    try:
        with open(config.STRATEGY_FILE) as f:
            return json.load(f)
    except Exception:
        return {}


def _write_strategy_patch(patch: dict):
    s = _load_strategy()
    s.update(patch)
    with open(config.STRATEGY_FILE, "w") as f:
        json.dump(s, f, indent=2)


def _get_positions():
    try:
        from trade_engine import get_open_positions
        from data_collector import prices
        pos = get_open_positions()
        out = []
        for p in pos:
            sym   = p["symbol"]
            price = prices.get(sym, 0)
            entry = p.get("entry_price", 0)
            qty   = p.get("quantity", 0)
            bep   = entry * (1 + config.FEE_RATE_BNB * 2) if config.BNB_FEE_MODE else entry * 1.002
            pnl   = (price - entry) * qty if price and entry else 0
            dist  = ((price - bep) / bep * 100) if bep and price else 0
            out.append({
                **p,
                "current_price":  price,
                "breakeven_price": round(bep, 6),
                "unrealized_pnl": round(pnl, 4),
                "dist_to_bep_pct": round(dist, 4),
                "profitable":      price > bep if price and bep else False,
            })
        return out
    except Exception:
        return []


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


@app.get("/config")
def get_config():
    strategy = _load_strategy()
    return {
        "budget_mode":           strategy.get("budget_mode",           config.BUDGET_MODE),
        "budget_fixed_usdt":     strategy.get("budget_fixed_usdt",     config.BUDGET_FIXED_USDT),
        "budget_pct_of_free":    strategy.get("budget_pct_of_free",    config.BUDGET_PCT_OF_FREE),
        "budget_total_cap_usdt": strategy.get("budget_total_cap_usdt", config.BUDGET_TOTAL_CAP_USDT),
        "budget_per_coin":       strategy.get("budget_per_coin",       config.BUDGET_PER_COIN),
    }


@app.post("/config")
def update_config(body: dict):
    allowed_keys = {
        "budget_mode", "budget_fixed_usdt", "budget_pct_of_free",
        "budget_total_cap_usdt", "budget_per_coin",
    }
    patch = {k: v for k, v in body.items() if k in allowed_keys}
    if not patch:
        return {"error": "No valid config keys provided"}
    _write_strategy_patch(patch)
    return {"ok": True, "updated": list(patch.keys()), "config": patch}


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
        acc = client.get_account()
        balances = [
            {"asset": b["asset"], "free": float(b["free"]), "locked": float(b["locked"])}
            for b in acc["balances"]
            if float(b["free"]) + float(b["locked"]) > 0
        ]
        total_usdt = sum(
            b["free"] for b in balances if b["asset"] == "USDT"
        )
        return {"balances": balances, "total_usdt": total_usdt, "mode": "paper"}
    except Exception as e:
        return {"balances": [], "total_usdt": 0.0, "mode": "paper", "error": str(e)}


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
    realized = sum(t.get("net_profit") or 0 for t in sells)
    initial  = float(strategy.get("initial_balance_usdt", 0))
    balance  = round(_get_usdt_balance(), 2)
    approved = [c["symbol"] for c in strategy.get("approved_coins", []) if c.get("approved")]
    return {
        "running":          strategy.get("trading_active", False),
        "mode":             get_mode(),
        "balance_usdt":     balance,
        "paper_balance":    balance,   # alias for frontend compatibility
        "initial_balance":  initial or balance,
        "open_positions":   len(_get_positions()),
        "trades_today":     database.get_trades_today_count(),
        "win_rate":         round(wins / len(sells), 3) if sells else 0.0,
        "wins":             wins,
        "losses":           len(sells) - wins,
        "total_trades":     len(sells),
        "realized_pnl":     round(realized, 4),
        "watched_coins":    approved or config.WATCHED_COINS,
        "data_dir":         database._DATA_DIR,
        "db_path":          database.DB_PATH,
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

        # Reset initial_balance in strategy.json
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
    import strategy_engine
    # Refresh approved coins from config before starting so stale strategy.json
    # never limits which coins are scanned.
    strategy_engine.write_default_strategy()
    s = _load_strategy()
    if not s.get("initial_balance_usdt"):
        bal = _get_usdt_balance()
        _write_strategy_patch({"initial_balance_usdt": bal or float(os.getenv("STARTING_PAPER_USDT", "10000.0"))})
    _write_strategy_patch({"trading_active": True, "pause_reason": None})
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
        from trade_engine import get_budget_for_coin, _positions, _positions_lock
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

        pos = {
            "symbol": sym, "entry_price": fill_price, "quantity": qty,
            "budget_usdt": budget, "timestamp": datetime.now(timezone.utc).isoformat(),
            "mode": get_mode(),
        }
        pos["id"] = database.save_position(pos)
        with _positions_lock:
            _positions.append(pos)

        database.log_activity(f"Force buy: {sym} @ ${fill_price:.4f} | qty={qty:.6f} | budget={budget:.2f} USDT", "info")
        return {"ok": True, "symbol": sym, "price": fill_price, "quantity": qty, "budget": budget}
    except Exception as e:
        return {"ok": False, "error": str(e)}


class ForceSellRequest(BaseModel):
    price: float = 0.0   # frontend sends its live WebSocket price


@app.post("/api/force-sell/{symbol}")
def api_force_sell(symbol: str, req: Optional[ForceSellRequest] = None):
    """Immediately sell an open position by symbol (case-insensitive).
    Accepts an optional price hint from the frontend so stale WebSocket
    prices on the server side never cause the sell to use the wrong price."""
    sym = symbol.upper()
    try:
        from trade_engine import get_open_positions, _execute_sell
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
        _execute_sell(pos, price, "force-sell")
        database.log_activity(f"Force sell: {sym} @ ${price:.4f} | qty={pos.get('quantity',0):.6f}", "info")
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


@app.get("/api/ping")
def api_ping():
    return {"ok": True, "ts": datetime.now(timezone.utc).isoformat()}


@app.get("/api/all")
def api_all():
    """Single endpoint returning status + positions + trades + activity.
    Reduces frontend from 4 concurrent fetches to 1, cutting Railway load 4×."""
    strategy = _load_strategy()
    trades   = database.get_recent_trades(limit=500)
    sells    = [t for t in trades if t.get("exit_price") is not None]
    wins     = sum(1 for t in sells if (t.get("net_profit") or 0) > 0)
    realized = sum(t.get("net_profit") or 0 for t in sells)
    initial  = float(strategy.get("initial_balance_usdt", 0))
    balance  = round(_get_usdt_balance(), 2)
    approved = [c["symbol"] for c in strategy.get("approved_coins", []) if c.get("approved")]
    return {
        "status": {
            "running":         strategy.get("trading_active", False),
            "mode":            get_mode(),
            "balance_usdt":    balance,
            "paper_balance":   balance,
            "initial_balance": initial or balance,
            "open_positions":  len(_get_positions()),
            "trades_today":    database.get_trades_today_count(),
            "win_rate":        round(wins / len(sells), 3) if sells else 0.0,
            "wins":            wins,
            "losses":          len(sells) - wins,
            "total_trades":    len(sells),
            "realized_pnl":    round(realized, 4),
            "watched_coins":   approved or config.WATCHED_COINS,
        },
        "positions": _get_positions(),
        "trades":    database.get_recent_trades(limit=200),
        "activity":  database.get_activity_log(limit=100),
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
