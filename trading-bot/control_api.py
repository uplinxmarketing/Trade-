"""
FastAPI control server — port 8000.
Serves REST endpoints + a live HTML dashboard for paper mode.
Runs as a daemon thread (non-blocking).
"""

import json
import os
import threading
import time
from datetime import datetime, timezone
from typing import Optional

import uvicorn
from fastapi import FastAPI, Response
from fastapi.responses import HTMLResponse

import config
import database
from connection import get_mode

app = FastAPI(title="Trading Bot Control API", version="1.0")

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
        from connection import client
        acc = client.get_account()
        for b in acc["balances"]:
            if b["asset"] == "USDT":
                return float(b["free"])
    except Exception:
        pass
    return 0.0


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


@app.get("/", response_class=HTMLResponse)
def dashboard():
    return HTMLResponse(DASHBOARD_HTML)


# ── Start as daemon thread ────────────────────────────────────────────────────

def start_control_api():
    t = threading.Thread(
        target=lambda: uvicorn.run(app, host="0.0.0.0", port=8000, log_level="error"),
        daemon=True,
    )
    t.start()
    print("[ControlAPI] Dashboard running at http://localhost:8000")
