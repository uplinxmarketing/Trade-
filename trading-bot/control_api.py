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
from datetime import datetime, timedelta, timezone
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
    The repo-root dist/ is the ONLY build location (committed on every release).
    trading-bot/dist/ was a stale committed copy that shadowed it — the update
    button kept 'installing' while the served version never changed."""
    for candidate in [
        _pl_v.Path(__file__).parent.parent / "dist" / "version.json",
        _pl_v.Path(__file__).parent / "dist" / "version.json",
    ]:
        try:
            if candidate.exists():
                return _json_v.loads(candidate.read_text())
        except Exception:
            pass
    return {"version": "unknown", "buildTime": "", "commit": ""}
from fastapi import FastAPI, Response, Body, Query, Request
from fastapi.responses import HTMLResponse, StreamingResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from pydantic import BaseModel

from concurrent.futures import TimeoutError as _ConcurrentTimeoutError

import config
import database
from connection import get_mode, get_live_error, is_using_paper_fallback


# ── Phase 5+6: DB migrations ─────────────────────────────────────────────────

import sqlite3 as _sqlite3_migrations

def _migrate_signal_snapshot_columns():
    try:
        conn = _sqlite3_migrations.connect(database.DB_PATH)
        cur = conn.cursor()
        for table in ("trades", "positions"):
            cur.execute(f"PRAGMA table_info({table})")
            cols = [c[1] for c in cur.fetchall()]
            if "signal_snapshot" not in cols:
                cur.execute(f"ALTER TABLE {table} ADD COLUMN signal_snapshot TEXT")
        conn.commit()
        conn.close()
    except Exception as e:
        import logging as _log_mg
        _log_mg.getLogger(__name__).warning("signal_snapshot migration failed: %s", e)

def _migrate_strategy_audit_table():
    try:
        conn = _sqlite3_migrations.connect(database.DB_PATH)
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS strategy_audit (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                field_key TEXT NOT NULL,
                old_value TEXT,
                new_value TEXT,
                source TEXT
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_strategy_audit_ts ON strategy_audit(timestamp)")
        conn.commit()
        conn.close()
    except Exception as e:
        import logging as _log_mg
        _log_mg.getLogger(__name__).warning("strategy_audit migration failed: %s", e)

def _migrate_alerts_table():
    try:
        conn = _sqlite3_migrations.connect(database.DB_PATH)
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                severity TEXT NOT NULL,
                category TEXT NOT NULL,
                message TEXT NOT NULL,
                metadata TEXT,
                acknowledged INTEGER DEFAULT 0
            )
        """)
        conn.commit()
        conn.close()
    except Exception as e:
        import logging as _log_mg
        _log_mg.getLogger(__name__).warning("alerts migration failed: %s", e)

# Run all migrations at module load time
_migrate_signal_snapshot_columns()
_migrate_strategy_audit_table()
_migrate_alerts_table()

# ── Strategy audit tracker ────────────────────────────────────────────────────

_last_strategy_snapshot: dict = {}
_strategy_audit_lock = threading.Lock()


def _flatten_dict(d: dict, prefix: str = "") -> dict:
    """Flatten nested dict to dot-notation keys."""
    out = {}
    for k, v in d.items():
        key = f"{prefix}.{k}" if prefix else k
        if isinstance(v, dict):
            out.update(_flatten_dict(v, key))
        else:
            out[key] = v
    return out


def _log_strategy_changes(new_strategy: dict, source: str) -> None:
    global _last_strategy_snapshot
    with _strategy_audit_lock:
        old_flat = _flatten_dict(_last_strategy_snapshot)
        new_flat = _flatten_dict(new_strategy)
        all_keys = set(old_flat) | set(new_flat)
        changes = []
        for key in all_keys:
            old_v = old_flat.get(key)
            new_v = new_flat.get(key)
            if old_v != new_v:
                changes.append((key, str(old_v) if old_v is not None else None,
                                str(new_v) if new_v is not None else None))
        if changes:
            now_ts = datetime.now(timezone.utc).isoformat()
            try:
                conn = _sqlite3_migrations.connect(database.DB_PATH)
                conn.executemany(
                    "INSERT INTO strategy_audit (timestamp, field_key, old_value, new_value, source) VALUES (?,?,?,?,?)",
                    [(now_ts, key, old_v, new_v, source) for key, old_v, new_v in changes]
                )
                conn.commit()
                conn.close()
            except Exception:
                pass
        _last_strategy_snapshot = dict(new_strategy)


# ── Lifespan: start the full trading bot after HTTP server is ready ───────────

# Keep strong references to the core background tasks — asyncio holds only weak
# refs, and a task that dies from an unhandled exception would otherwise vanish
# silently (no log, no restart, sell monitoring gone).
_bg_tasks: list = []


def _on_bg_task_done(task):
    """Done-callback: surface unhandled exceptions from core trading tasks."""
    try:
        if task.cancelled():
            return
        exc = task.exception()
    except Exception:
        return
    if exc is not None:
        import traceback
        tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        msg = f"BACKGROUND TASK DIED: {task.get_name()}: {exc}"
        print(f"[ControlAPI] {msg}\n{tb}")
        try:
            database.log_activity(msg, "error")
        except Exception:
            pass


def _spawn_bg_task(coro, name: str):
    t = asyncio.create_task(coro, name=name)
    t.add_done_callback(_on_bg_task_done)
    _bg_tasks.append(t)
    return t


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

        # 3+. Everything below involves network I/O (Binance account fetch,
        # orphan scan, Supabase) — it MUST NOT block the lifespan, or uvicorn
        # won't accept connections and nginx serves 502 for the whole init
        # (10-30 s on every deploy). Defer it to a background task; the API
        # starts serving immediately and endpoints tolerate partial state.
        async def _deferred_init():
            # Every step below is isolated in its OWN try/except.  Previously
            # steps 3-8 shared a single try-block: one exception in an early
            # step (position restore, balance fetch, …) silently prevented
            # callback registration AND the websocket/signal_scanner spawns,
            # so the signal cache stayed empty forever and zero automatic
            # buys could fire while the HTTP API looked perfectly healthy.
            # Now a failing step is logged loudly and init continues — the
            # critical buy pipeline (callbacks + task spawns) is GUARANTEED
            # to be reached regardless of earlier failures.
            def _step_failed(step_name: str, exc: Exception):
                err = (f"STARTUP ERROR at step {step_name}: {exc!r} — "
                       f"continuing with remaining init steps")
                print(f"[ControlAPI] {err}")
                try:
                    database.log_activity(err, "error")
                except Exception:
                    pass

            # Warm the Binance account cache immediately (live mode) so the
            # wallet panel's first request is served from cache instead of
            # waiting on a cold signed fetch.
            try:
                await asyncio.to_thread(_get_cached_account)
            except Exception as exc:
                _step_failed("account_cache_warm", exc)

            # 3. Restore open positions + coins + balance from SQLite / Supabase
            try:
                await asyncio.to_thread(trade_engine.load_positions_from_db)
                steps.append("positions OK")
            except Exception as exc:
                _step_failed("positions", exc)

            # 3b. Start REST price refresher for held positions (2s interval —
            #     critical for low-WS-volume coins that can go minutes stale)
            try:
                trade_engine.start_held_position_refresher()
                steps.append("held_price_refresher OK")
            except Exception as exc:
                _step_failed("held_price_refresher", exc)
            try:
                trade_engine.start_capital_recycler()
                steps.append("capital_recycler OK")
            except Exception as exc:
                _step_failed("capital_recycler", exc)
            try:
                trade_engine.start_phantom_checker()
                steps.append("phantom_checker OK")
            except Exception as exc:
                _step_failed("phantom_checker", exc)
            # Phase 2 §2.4/§2.5: exchange-side exit-order poll/reconcile daemon
            # (no-ops in paper mode; safe to always start).
            try:
                trade_engine.start_managed_exit_poller()
                steps.append("managed_exit_poller OK")
            except Exception as exc:
                _step_failed("managed_exit_poller", exc)

            # 4. Apply startup defaults and auto-resume logic.
            #    trading_active is preserved so a running bot resumes after a redeploy.
            try:
                _s = _load_strategy()
                _auto_patch: dict = {
                    "pause_reason": None,
                }
                if "trading_active" not in _s:
                    # Brand-new deploy — don't auto-start, let user press Start
                    _auto_patch["trading_active"] = False
                elif _s.get("resume_after_restart"):
                    # /api/update paused trading for the restart — the bot was
                    # RUNNING before the update, so resume it automatically.
                    _auto_patch["trading_active"] = True
                    _auto_patch["resume_after_restart"] = False
                    print("[ControlAPI] Resuming trading after update (was active before restart)")
                # else: preserve existing trading_active (resumes running bot after redeploy)
                if not _s.get("initial_balance_usdt"):
                    _bal = await asyncio.to_thread(_get_usdt_balance)
                    _auto_patch["initial_balance_usdt"] = _bal or float(os.getenv("STARTING_PAPER_USDT", "10000.0"))
                _write_strategy_patch(_auto_patch)
                steps.append(f"trading_active={'resume' if _s.get('trading_active') else 'off'}")
            except Exception as exc:
                _step_failed("auto_resume_patch", exc)

            # 4c. Phase 5 §5.1 — one-time v1→v2 strategy schema migration.
            #     Purely additive (missing blocks + schema_version); idempotent,
            #     so re-running on every deploy is a no-op after the first.
            try:
                import strategy_config as _scfg_mig
                _raw_mig = _load_strategy()
                _v2_mig, _warn_mig = _scfg_mig.migrate_to_v2(_raw_mig)
                _added_mig = {k: v for k, v in _v2_mig.items()
                              if _raw_mig.get(k) != v}
                if _added_mig:
                    _write_strategy_patch(_added_mig)
                    for _w in _warn_mig:
                        print(f"[ControlAPI] strategy v2 migration: {_w}")
                    try:
                        database.save_config_version(
                            "migration-v2",
                            _scfg_mig.diff_views(_raw_mig, _v2_mig),
                            _load_strategy())
                    except Exception:
                        pass
                    database.log_activity(
                        f"strategy.json migrated to schema v2 "
                        f"({len(_added_mig)} key(s) added)", "info")
                    steps.append("v2_migration applied")
                else:
                    steps.append("v2_migration noop")
            except Exception as exc:
                _step_failed("v2_migration", exc)

            # 5. History download — daemon thread, never blocks
            try:
                threading.Thread(target=data_collector.download_history, daemon=True).start()
                steps.append("history_dl started")
            except Exception as exc:
                _step_failed("history_dl", exc)

            # 6. Register price/kline callbacks — CRITICAL: without these the
            #    signal cache never fills and no automatic buy can ever fire.
            try:
                data_collector.register_price_callback(trade_engine.realtime_monitor)
                data_collector.register_kline_callback(trade_engine.update_coin_signals)
                steps.append("callbacks OK")
            except Exception as exc:
                _step_failed("callbacks", exc)

            # 7. Launch async tasks — CRITICAL: websocket + signal_scanner feed
            #    the signal cache. Each spawn is isolated so one failure can
            #    never stop the remaining tasks from launching.
            _spawned: list[str] = []
            for _task_name, _coro_factory in (
                ("websocket",         lambda: data_collector.start_websocket()),
                ("strategy_loop",     lambda: strategy_engine.strategy_loop()),
                ("signal_scanner",    lambda: trade_engine.signal_scanner(data_collector.prices)),
                ("position_guardian", lambda: trade_engine.position_guardian()),
                ("supabase_sync",     lambda: _supabase_periodic_sync()),
                ("anomaly_checker",   lambda: _anomaly_checker()),
            ):
                try:
                    _spawn_bg_task(_coro_factory(), _task_name)
                    _spawned.append(_task_name)
                except Exception as exc:
                    _step_failed(f"spawn_{_task_name}", exc)
            steps.append(f"async tasks launched ({len(_spawned)}/6: {', '.join(_spawned) or 'none'})")

            # 7b. Phase 1 daily maintenance: kline-store prune + nightly edge
            #     report. First pass ~60 s after startup, then every 24 h.
            try:
                _spawn_bg_task(_daily_maintenance_loop(), "daily_maintenance")
                steps.append("daily_maintenance OK")
            except Exception as exc:
                _step_failed("spawn_daily_maintenance", exc)

            # 8. Futures paper-trading agent (completely separate parallel process)
            try:
                if config.FUTURES_ENABLED:
                    import futures_engine
                    futures_engine.init_futures_engine()
                    _spawn_bg_task(futures_engine.mark_price_loop(), "futures_mark_price")
                    _spawn_bg_task(futures_engine.signal_scanner_loop(), "futures_signal_scanner")
                    steps.append("futures tasks launched")
            except Exception as exc:
                _step_failed("futures", exc)

            # 9. Log which BUY DECISION PATH is active, computed exactly the
            #    way trade_engine gates it (strategy.get("signal_engine", {})
            #    .get("enabled", False)) — so "engine looks on in the UI but
            #    legacy path is running" is visible straight from the logs.
            try:
                _sp = _load_strategy()
                _engine_on = bool(_sp.get("signal_engine", {}).get("enabled", False))
                _path_msg = (
                    "Buy decision path: SIGNAL ENGINE (strategy.json signal_engine.enabled=true)"
                    if _engine_on else
                    "Buy decision path: LEGACY 6-signal score (strategy.json signal_engine.enabled absent/false)"
                )
                print(f"[ControlAPI] {_path_msg}")
                database.log_activity(_path_msg, "info")
            except Exception as exc:
                _step_failed("decision_path_log", exc)

            msg = "Bot ready — " + " | ".join(steps)
            print(f"[ControlAPI] {msg}")
            try:
                database.log_activity(msg, "info")
            except Exception:
                pass

        _spawn_bg_task(_deferred_init(), "deferred_init")
        print("[ControlAPI] HTTP up — deferred init running in background")

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
app.add_middleware(GZipMiddleware, minimum_size=1000)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "PUT"],
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


async def _anomaly_checker():
    """Background task: check for trading anomalies every 300s and insert alerts."""
    import asyncio as _aio
    import sqlite3 as _sq_ac
    while True:
        await _aio.sleep(300)
        try:
            now_ts = datetime.now(timezone.utc).isoformat()
            conn = _sq_ac.connect(database.DB_PATH)
            conn.row_factory = _sq_ac.Row

            # 1. Win rate drop: compare last 4h vs prior 7 days
            try:
                r4h = conn.execute("""
                    SELECT COUNT(*) AS total,
                           SUM(CASE WHEN net_profit > 0 THEN 1 ELSE 0 END) AS wins
                    FROM trades WHERE timestamp_sell > datetime('now', '-4 hours')
                    AND exit_price IS NOT NULL
                """).fetchone()
                r7d = conn.execute("""
                    SELECT COUNT(*) AS total,
                           SUM(CASE WHEN net_profit > 0 THEN 1 ELSE 0 END) AS wins
                    FROM trades WHERE timestamp_sell > datetime('now', '-7 days')
                    AND timestamp_sell <= datetime('now', '-4 hours')
                """).fetchone()
                if r4h and r7d and r4h["total"] >= 3 and r7d["total"] >= 3:
                    wr4h = (r4h["wins"] or 0) / r4h["total"] * 100
                    wr7d = (r7d["wins"] or 0) / r7d["total"] * 100
                    if wr7d > 0 and (wr7d - wr4h) > 20:
                        msg = (f"Win rate drop detected: last 4h={wr4h:.1f}% vs prior 7d={wr7d:.1f}% "
                               f"(drop={wr7d - wr4h:.1f}%)")
                        conn.execute(
                            "INSERT INTO alerts (timestamp, severity, category, message, metadata) VALUES (?,?,?,?,?)",
                            (now_ts, "warn", "win_rate_drop", msg,
                             json.dumps({"wr_4h": round(wr4h, 1), "wr_7d": round(wr7d, 1)}))
                        )
            except Exception:
                pass

            # 2. Consecutive losses
            try:
                recent = conn.execute("""
                    SELECT net_profit FROM trades
                    ORDER BY id DESC LIMIT 10
                """).fetchall()
                consec = 0
                for row in recent:
                    if (row["net_profit"] or 0) <= 0:
                        consec += 1
                    else:
                        break
                if consec >= 5:
                    msg = f"Consecutive losing trades: {consec} in a row"
                    conn.execute(
                        "INSERT INTO alerts (timestamp, severity, category, message, metadata) VALUES (?,?,?,?,?)",
                        (now_ts, "warn", "consecutive_losses", msg,
                         json.dumps({"consecutive_losses": consec}))
                    )
            except Exception:
                pass

            conn.commit()
            conn.close()
        except Exception as e:
            print(f"[AnomalyChecker] error: {e}")


async def _daily_maintenance_loop():
    """Phase 1 housekeeping: prune the kline store per strategy.json retention
    and rebuild the nightly edge report. Runs once ~60 s after startup (so the
    edge report exists shortly after every deploy), then every 24 h. Each step
    is guarded separately and degrades to a no-op when the parallel Phase 1
    modules (database.prune_klines_from_config / attribution.run_nightly)
    haven't landed yet."""
    import asyncio as _aio
    await _aio.sleep(60)
    while True:
        try:
            _fn = getattr(database, "prune_klines_from_config", None)
            if _fn is not None:
                deleted = await _aio.to_thread(_fn)
                print(f"[Maintenance] kline prune: {deleted} row(s) deleted")
        except Exception as e:
            print(f"[Maintenance] kline prune failed: {e}")
        try:
            import attribution as _attr
            _fn = getattr(_attr, "run_nightly", None)
            if _fn is not None:
                await _aio.to_thread(_fn)
                print("[Maintenance] nightly edge report rebuilt")
        except Exception as e:
            print(f"[Maintenance] nightly attribution failed: {e}")
        # §3.6 — warn when any fees.per_symbol_overrides entry is >30 days
        # unreviewed ('_reviewed_ts' absent or old): Binance fee promos change
        # monthly, and a withdrawn promo silently left at maker_pct=0 corrupts
        # every BEP/min-profit calculation for that pair.
        try:
            import fees as _fees
            _pps = getattr(_fees, "promo_pairs_status", None)
            if _pps is not None:
                _stale_syms = sorted(s for s, e in _pps().items() if e.get("stale"))
                if _stale_syms:
                    database.log_activity(
                        f"Fee override review overdue (>30 days): "
                        f"{', '.join(_stale_syms)} — verify the Binance promo "
                        f"still applies and refresh '_reviewed_ts' in "
                        f"fees.per_symbol_overrides", "warn")
        except Exception as e:
            print(f"[Maintenance] fee-override staleness check failed: {e}")
        # Phase 4 §4.5 — BNB fee-balance health check: run once now, then on a
        # lighter hourly tick inside the 24 h maintenance window.
        await _bnb_health_check()
        for _hour_tick in range(23):
            await _aio.sleep(3600)
            await _bnb_health_check()
        await _aio.sleep(3600)


async def _bnb_health_check():
    """Phase 4 §4.5 — hourly BNB fee-balance check.

    Only acts when strategy.json fees.bnb_discount is true. Reads the engine's
    get_risk_status()['bnb'] snapshot; when the balance is low it logs a
    WARNING to the activity log, and when fees.auto_topup_bnb is enabled it
    calls trade_engine.maybe_topup_bnb() (which re-checks conditions
    internally) and logs the audit result. Fully defensive: no-ops when the
    engine hasn't landed these APIs yet."""
    import asyncio as _aio
    try:
        s = _load_strategy()
        fees_cfg = s.get("fees") if isinstance(s.get("fees"), dict) else {}
        if not fees_cfg.get("bnb_discount", False):
            return
        import trade_engine as _te
        _grs = getattr(_te, "get_risk_status", None)
        if not callable(_grs):
            return
        try:
            status = await _aio.to_thread(_grs)
        except Exception:
            return
        bnb = (status or {}).get("bnb") if isinstance(status, dict) else None
        if not isinstance(bnb, dict):
            return
        if bnb.get("low"):
            try:
                _usdt_val = float(bnb.get("bnb_usdt_value") or 0.0)
            except (TypeError, ValueError):
                _usdt_val = 0.0
            database.log_activity(
                f"BNB balance low (~{_usdt_val:.2f} USDT) — fee discount at risk",
                "warn")
        if fees_cfg.get("auto_topup_bnb", False):
            _topup = getattr(_te, "maybe_topup_bnb", None)
            if callable(_topup):
                try:
                    result = await _aio.to_thread(_topup)
                except Exception as exc:
                    database.log_activity(f"BNB auto top-up failed: {exc}", "error")
                    result = None
                if result:
                    try:
                        database.log_activity(
                            "BNB auto top-up: " + json.dumps(result, default=str),
                            "info")
                    except Exception:
                        pass
    except Exception as e:
        print(f"[Maintenance] BNB health check failed: {e}")


# ── Internal helpers ─────────────────────────────────────────────

def _load_strategy() -> dict:
    try:
        with open(config.STRATEGY_FILE) as f:
            s = json.load(f)
        _log_strategy_changes(s, "hot_reload")
        return s
    except Exception:
        return {}


_strategy_write_lock = threading.Lock()


# Part E — two-way alias sync between the v2 blocks (what the schema-driven UI
# writes) and the legacy root keys (what parts of the engine still read).
# Without this, a v2 edit like sizing.bot_allocation_usdt is written to the
# sizing block while get_budget_for_coin/effective_slots keep reading the stale
# root key — the classic "saves but changes nothing" dead field. Symmetrically,
# a legacy POST /api/settings max_positions write to the root key would be
# shadowed by an existing sizing block (which _sizing_cfg prefers).
#   v2 sizing.{max_positions,bot_allocation_usdt,mode,reinvest_profits}
#       ⇄ root {max_positions,bot_allocation_usdt,budget_mode,reinvest_profits}
#   v2 entries.min_score ⇄ root min_signals + signal_engine.min_scored
_SIZING_ROOT_ALIASES = (
    # (root key, sizing-block key)
    ("max_positions",       "max_positions"),
    ("bot_allocation_usdt", "bot_allocation_usdt"),
    ("budget_mode",         "mode"),
    ("reinvest_profits",    "reinvest_profits"),
)


def _apply_alias_sync(s: dict, patch: dict) -> None:
    """Mutate the merged strategy dict `s` so v2-block values and legacy root
    keys stay consistent after applying `patch`. Direction is decided by what
    the PATCH touched (v2 block touched → root follows; root touched → block
    follows when it exists). Never raises."""
    try:
        sizing_p = patch.get("sizing") if isinstance(patch.get("sizing"), dict) else None
        entries_p = patch.get("entries") if isinstance(patch.get("entries"), dict) else None
        # v2 → legacy root (engine reads these keys at the root)
        if sizing_p:
            for root_key, blk_key in _SIZING_ROOT_ALIASES:
                if blk_key in sizing_p:
                    s[root_key] = sizing_p[blk_key]
        if entries_p and "min_score" in entries_p:
            try:
                ms = int(entries_p["min_score"])
                s["min_signals"] = ms
                se = s.get("signal_engine")
                if isinstance(se, dict):
                    se["min_scored"] = ms
            except (TypeError, ValueError):
                pass
        # legacy root → v2 blocks (the resolved v2 view prefers stored blocks)
        if isinstance(s.get("sizing"), dict):
            for root_key, blk_key in _SIZING_ROOT_ALIASES:
                if root_key in patch:
                    s["sizing"][blk_key] = patch[root_key]
        if "min_signals" in patch:
            try:
                ms = int(patch["min_signals"])
                if isinstance(s.get("entries"), dict):
                    s["entries"]["min_score"] = ms
                se = s.get("signal_engine")
                if isinstance(se, dict) and "min_scored" not in patch.get(
                        "signal_engine", {}):
                    se["min_scored"] = ms
            except (TypeError, ValueError):
                pass
        # signal_engine.min_scored is the canonical key the registry reads; a
        # direct write to it must mirror back to the user-facing aliases so the
        # duplicate keys never diverge (F3 — collapse the min-score duplication).
        se_p = patch.get("signal_engine") if isinstance(patch.get("signal_engine"), dict) else None
        if se_p and "min_scored" in se_p and "min_score" not in (entries_p or {}):
            try:
                ms = int(se_p["min_scored"])
                s["min_signals"] = ms
                if isinstance(s.get("entries"), dict):
                    s["entries"]["min_score"] = ms
            except (TypeError, ValueError):
                pass
    except Exception:
        pass  # alias sync must never block a settings write


def _write_strategy_patch(patch: dict):
    """Atomic merge-and-write to strategy.json (lock-protected against concurrent saves).

    Without atomicity, concurrent readers (sell monitor, signal scanner) may
    catch the file mid-truncate and json.load raises — which silently turns
    every default into the schema fallback (e.g. trading_active drops to True
    or take_profit_mult resets to breakeven mid-trade)."""
    with _strategy_write_lock:
        s = _load_strategy()
        s.update(patch)
        _apply_alias_sync(s, patch)   # keep v2 blocks ⇄ legacy root keys consistent
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


def _flush_db_state():
    """Best-effort flush of any pending DB state before a process restart.

    database.py commits per-operation on short-lived connections, so there is
    normally nothing buffered — but if the module ever grows flush/commit/close
    helpers (or a WAL checkpoint), call them here so restarts never drop state."""
    for _fn_name in ("flush", "commit", "checkpoint", "close"):
        _fn = getattr(database, _fn_name, None)
        if callable(_fn):
            try:
                _fn()
            except Exception:
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
        # Phase 2 §2.4/§2.5 — managed exchange-side exit visibility (guarded:
        # exit_orders may be absent; the column just reads null then).
        try:
            import exit_orders as _eo_pos
            _managed_map = _eo_pos.all_managed()
        except Exception:
            _managed_map = {}
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
            # Fee-inclusive net profit if sold right now (conservative: uses standard 0.1% fee)
            _buy_fee_pos  = float(p.get("buy_fee_usdt") or 0)
            _sell_fee_est = (price * qty * 0.001) if price and qty else 0
            _net_pnl_now  = round(pnl - _buy_fee_pos - _sell_fee_est, 4) if price and entry else 0
            row = _enrich_position({
                **p,
                "avg_entry_price": entry,
                "current_price":   price,
                "exit_target":     round(target, 8),
                "breakeven_price": round(_bep_pos, 6),
                "unrealized_pnl":  round(pnl, 4),
                "net_profit_now":  _net_pnl_now,
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
                        # Recompute net_profit_now using real BEP-derived fee-inclusive P&L
                        row["net_profit_now"] = round(
                            (price - entry) * qty - _buy_fee_pos - _sell_fee_est, 4
                        )
            except Exception:
                pass
            # {managed_exit: 'maker_tp'|'oco'|None} — exchange-side exit order
            # currently managing this position's exit (live mode only).
            row["managed_exit"] = (_managed_map.get(sym) or {}).get("kind")
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

        # Load once for all coins
        strategy = _load_strategy()

        try:
            import signal_registry as _sr
            _registry_available = True
        except Exception:
            _registry_available = False

        # Signals to skip in snapshot (E1 makes a live REST call per coin)
        _SNAPSHOT_SKIP = {"E1_spread_too_wide"}

        result = []
        for sym, entry in snap.items():
            sig = entry.get("signals", {})

            # Legacy fields — kept for backward compatibility
            row = {
                "symbol": sym,
                "price":  entry.get("price", 0),
                "score":  entry.get("score", 0),
                "rsi":    entry.get("rsi_val", 0),
                "bb_ok":  entry.get("bb_ok", True),
                "5m_ok":  entry.get("5m_ok", True),
                "trend":  bool(sig.get("trend")),
                "rsi_ok": bool(sig.get("rsi")),
                "macd":   bool(sig.get("macd")),
                "volume": bool(sig.get("volume")),
                "obv":    bool(sig.get("obv")),
                "atr":    bool(sig.get("atr")),
                "ts":     entry.get("ts", 0),
            }

            if _registry_available:
                # Build the signal_data dict the registry expects
                signal_data = {
                    "trend":         sig.get("trend", False),
                    "rsi":           sig.get("rsi", False),
                    "macd":          sig.get("macd", False),
                    "volume":        sig.get("volume", False),
                    "obv":           sig.get("obv", False),
                    "atr":           sig.get("atr", False),
                    "rsi_value":     entry.get("rsi_val"),
                    "stoch_rsi_value": entry.get("stoch_rsi_val"),
                    "low_24h":       entry.get("low_24h"),
                    "current_price": entry.get("price"),
                    "klines_1m":     entry.get("klines_1m", []),
                }

                # Evaluate all signals, skipping REST-heavy veto signals
                signal_results: dict = {}
                for sig_id, sig_def in _sr.SIGNAL_REGISTRY.items():
                    if sig_id in _SNAPSHOT_SKIP:
                        signal_results[sig_id] = {"fired": False, "raw_value": "snapshot_skipped"}
                        continue
                    try:
                        fired, raw = sig_def.compute_fn(sym, signal_data, strategy)
                        signal_results[sig_id] = {"fired": bool(fired), "raw_value": raw}
                    except Exception:
                        signal_results[sig_id] = {"fired": False, "raw_value": None}

                # Buy decision (uses cached signal_results, no extra REST calls)
                try:
                    decision = _sr.evaluate_buy_decision(sym, signal_data, strategy)
                    row["buy_allowed"] = bool(decision.get("allowed", False))
                    row["buy_reason"]  = decision.get("reason", "")
                except Exception:
                    row["buy_allowed"] = False
                    row["buy_reason"]  = "eval_error"

                row["signal_results"] = signal_results

            result.append(row)
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
_acct_last_success_ts: float = 0.0   # last time a LIVE/paper fetch actually succeeded
_acct_fail_ts: float = 0.0           # negative-cache: last failed live fetch
_ACCT_CACHE_TTL = 20.0
_ACCT_FAIL_TTL = 5.0                 # don't hammer Binance while it's failing
_acct_cache_lock = threading.Lock()
_acct_refresh_lock = threading.Lock()  # single-flight guard for the network fetch

def _fetch_account_direct() -> dict:
    """Fetch /api/v3/account via binance_direct (urllib+HMAC with recvWindow and
    server-time sync), bypassing python-binance/requests which is geo-blocked on
    datacenter IPs. Returns {} on any failure."""
    if not os.getenv("BINANCE_API_KEY", "").strip() or not os.getenv("BINANCE_API_SECRET", "").strip():
        return {}
    import binance_direct
    try:
        return binance_direct.get_account()
    except binance_direct.BinanceDirectError as e:
        print(f"[Account] Binance API error {e.code}: {e.msg}")
        return {}
    except Exception as e:
        print(f"[Account] Direct fetch failed: {type(e).__name__}: {e}")
        return {}


def _get_cached_account() -> dict:
    """Return cached Binance account dict, refreshing at most every ACCT_CACHE_TTL s.

    Single-flight: only one thread performs the (slow) network fetch; concurrent
    callers get the stale cache. Failed live fetches are negative-cached for
    _ACCT_FAIL_TTL s so a broken connection doesn't stampede signed requests."""
    global _acct_cache, _acct_cache_ts, _acct_last_success_ts, _acct_fail_ts
    from connection import get_mode, client as _client
    now = time.time()
    with _acct_cache_lock:
        if now - _acct_cache_ts < _ACCT_CACHE_TTL and _acct_cache:
            return _acct_cache

    if get_mode() == "live":
        # Live mode: always use direct urllib+HMAC — python-binance/requests is geo-blocked
        with _acct_cache_lock:
            if now - _acct_fail_ts < _ACCT_FAIL_TTL:
                return _acct_cache  # recent failure — serve stale, don't hammer
        if not _acct_refresh_lock.acquire(blocking=False):
            # Another thread is already fetching — serve stale cache immediately
            with _acct_cache_lock:
                return _acct_cache
        try:
            # Re-check: the previous holder may have just refreshed the cache
            with _acct_cache_lock:
                if time.time() - _acct_cache_ts < _ACCT_CACHE_TTL and _acct_cache:
                    return _acct_cache
            acc = _fetch_account_direct()
            if acc.get("balances"):
                with _acct_cache_lock:
                    _acct_cache = acc
                    _acct_cache_ts = time.time()
                    _acct_last_success_ts = _acct_cache_ts
                return acc
            # Direct fetch failed — negative-cache and return stale rather than paper data
            with _acct_cache_lock:
                _acct_fail_ts = time.time()
                return _acct_cache
        finally:
            _acct_refresh_lock.release()
    else:
        # Paper / testnet mode: use the paper client
        try:
            acc = _client.get_account()
            with _acct_cache_lock:
                _acct_cache = acc
                _acct_cache_ts = now
                _acct_last_success_ts = now
            return acc
        except Exception:
            with _acct_cache_lock:
                return _acct_cache


def _get_usdt_balance() -> float:
    """Returns free USDT only — used for trade budget calculations."""
    from connection import get_mode, client as _client
    try:
        if get_mode() == "live":
            acc = _get_cached_account()
            for b in acc.get("balances", []):
                if b["asset"] == "USDT":
                    return float(b["free"])
            return 0.0  # live account fetch failed — don't fake $10k
        # Paper mode: read directly from PaperClient balance dict
        if hasattr(_client, "_balances"):
            with _client._lock:
                return float(_client._balances.get("USDT", 0.0))
        acc = _get_cached_account()
        for b in acc.get("balances", []):
            if b["asset"] == "USDT":
                return float(b["free"])
    except Exception:
        pass
    return float(os.getenv("STARTING_PAPER_USDT", "10000.0"))


def _get_usdt_display_balance() -> float:
    """Returns free+locked USDT — matches what Binance UI shows."""
    from connection import get_mode, client as _client
    try:
        if get_mode() == "live":
            acc = _get_cached_account()
            for b in acc.get("balances", []):
                if b["asset"] == "USDT":
                    return float(b["free"]) + float(b["locked"])
            return 0.0
        if hasattr(_client, "_balances"):
            with _client._lock:
                return float(_client._balances.get("USDT", 0.0))
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
    coins = s.get("approved_coins", [])
    for coin in coins:
        coin["budget_usdt"] = amount
    # Atomic, lock-protected write (tmp+rename) — a raw open(w) here can be
    # caught mid-truncate by the sell monitor / signal scanner readers.
    # Also set budget_fixed_usdt: get_budget_for_coin sizes trades from it,
    # not from the per-coin budget_usdt field.
    _write_strategy_patch({"approved_coins": coins, "budget_fixed_usdt": amount})
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

# F4 — Binance minimum-notional floor. A fixed/per-coin per-trade budget below
# max(sizing.min_position_usdt, this) is rejected at settings time (422) instead
# of being accepted and then failing forever at execution ("$5.50 < $10 min
# notional"). Percent/capped: validate the resulting per-trade size when
# computable, else warn.
MIN_NOTIONAL_FLOOR = 10.0


def _budget_floor(merged: dict) -> float:
    """Per-trade minimum = max(sizing.min_position_usdt, MIN_NOTIONAL_FLOOR)."""
    floor = MIN_NOTIONAL_FLOOR
    try:
        sizing = merged.get("sizing") if isinstance(merged.get("sizing"), dict) else {}
        mp = sizing.get("min_position_usdt")
        if mp is None:
            mp = merged.get("min_position_usdt")
        if mp is not None:
            floor = max(float(mp), MIN_NOTIONAL_FLOOR)
    except (TypeError, ValueError):
        floor = MIN_NOTIONAL_FLOOR
    return floor


def _effective_budget_mode(merged: dict) -> str:
    """sizing.mode (v2) wins over the legacy root budget_mode when present."""
    sizing = merged.get("sizing") if isinstance(merged.get("sizing"), dict) else {}
    if isinstance(sizing.get("mode"), str):
        return sizing["mode"]
    return merged.get("budget_mode", config.BUDGET_MODE)


def _validate_budget_floor(merged: dict):
    """Return (errors, warnings) for a merged (current+patch) strategy dict.
    errors block the write (fixed/per-coin per-trade budget below the floor);
    warnings are advisory (percent/capped resulting size below floor)."""
    errors: dict = {}
    warnings: list = []
    floor = _budget_floor(merged)
    mode = _effective_budget_mode(merged)
    if mode == "fixed":
        try:
            bf = float(merged.get("budget_fixed_usdt", config.BUDGET_FIXED_USDT))
            if bf < floor:
                errors["budget_fixed_usdt"] = (
                    f"fixed per-trade budget {bf:.2f} USDT is below the minimum "
                    f"{floor:.2f} USDT (Binance min notional {MIN_NOTIONAL_FLOOR:.0f}) "
                    f"— raise budget_fixed_usdt or lower sizing.min_position_usdt")
        except (TypeError, ValueError):
            errors["budget_fixed_usdt"] = "must be a number"
    elif mode == "per_coin":
        per_coin = merged.get("budget_per_coin", {})
        if isinstance(per_coin, dict):
            for sym, v in per_coin.items():
                try:
                    if float(v) < floor:
                        errors[f"budget_per_coin.{sym}"] = (
                            f"per-coin budget {float(v):.2f} USDT for {sym} is below "
                            f"the minimum {floor:.2f} USDT (Binance min notional "
                            f"{MIN_NOTIONAL_FLOOR:.0f})")
                except (TypeError, ValueError):
                    errors[f"budget_per_coin.{sym}"] = "must be a number"
    elif mode == "capped":
        try:
            cap = float(merged.get("budget_total_cap_usdt", config.BUDGET_TOTAL_CAP_USDT))
            max_pos = int(merged.get("max_positions", config.MAX_OPEN_POSITIONS) or 1)
            if max_pos > 0 and (cap / max_pos) < floor:
                warnings.append(
                    f"capped per-trade size ~= {cap / max_pos:.2f} USDT "
                    f"(cap {cap:.2f} / {max_pos} slots) is below the {floor:.2f} "
                    f"USDT min notional — trades may be rejected at execution")
        except (TypeError, ValueError):
            pass
    # percent / coin_pct scale with live balance — not statically computable.
    return errors, warnings


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
        # F4 — reject a sub-minimum fixed/per-coin budget (422) before writing.
        _merged_cfg = {**_load_strategy(), **patch}
        _berr, _bwarn = _validate_budget_floor(_merged_cfg)
        if _berr:
            return JSONResponse(status_code=422, content={"errors": _berr})
        _write_strategy_patch(patch)
        _resp = {"ok": True, "updated": list(patch.keys()), "config": patch}
        if _bwarn:
            _resp["warnings"] = _bwarn
        return _resp
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
        from trade_engine import get_open_positions, _rest_px
        from data_collector import prices as live_prices

        acc = _get_cached_account()
        # A stale cache still contains balances — only report the fetch as OK
        # when the last SUCCESSFUL refresh is recent (3x TTL grace window).
        _stale_age = (time.time() - _acct_last_success_ts) if _acct_last_success_ts > 0 else None
        _fetch_ok = bool(acc.get("balances")) and _stale_age is not None \
            and _stale_age < _ACCT_CACHE_TTL * 3

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
        open_pos_value = 0.0
        for pos in get_open_positions():
            sym = pos["symbol"]
            px  = _rest_px.get(sym) or live_prices.get(sym) or pos["entry_price"]
            open_pos_value += pos["quantity"] * px
        total_value = usdt_free + open_pos_value

        # Realized P&L: single source of truth — SQL SUM from trades table
        _mode        = get_mode()
        realized_pnl = database.get_realized_pnl(mode=_mode)
        try:
            _total_fees = float(database.get_trade_stats(mode=_mode).get("total_fees", 0.0))
        except Exception:
            _total_fees = 0.0

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
            "account_fetch_ok":      _fetch_ok,
            "account_stale_seconds": round(_stale_age, 1) if _stale_age is not None else None,
            "total_fees":            round(_total_fees, 4),
            "open_pos_value":        round(open_pos_value, 4),
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


@app.get("/api/market-health")
def api_market_health():
    """Market-health verdict — previously (incorrectly) shadowed /api/status."""
    return _get_market_health()


@app.get("/api/status")
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
    """Lightweight open positions snapshot."""
    try:
        return {"positions": _get_positions(), "ts": time.time()}
    except Exception as e:
        return {"positions": [], "ts": time.time(), "error": str(e)}


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

    # Real trades schema: closed rows have exit_price/net_profit/timestamp_sell
    # (there are no side/pnl/created_at columns).
    today = conn.execute("""
        SELECT
            COUNT(*) FILTER (WHERE net_profit IS NOT NULL) AS closed_trades,
            COUNT(*) FILTER (WHERE net_profit > 0) AS wins,
            COUNT(*) FILTER (WHERE net_profit <= 0) AS losses,
            ROUND(COALESCE(SUM(net_profit), 0.0), 4) AS net_pnl
        FROM trades
        WHERE exit_price IS NOT NULL AND DATE(timestamp_sell) = DATE('now')
    """).fetchone()

    alltime = conn.execute("""
        SELECT
            COUNT(*) FILTER (WHERE net_profit IS NOT NULL) AS closed_trades,
            COUNT(*) FILTER (WHERE net_profit > 0) AS wins,
            COUNT(*) FILTER (WHERE net_profit <= 0) AS losses,
            ROUND(COALESCE(SUM(net_profit), 0.0), 4) AS net_pnl,
            ROUND(AVG(net_profit) FILTER (WHERE net_profit > 0), 4) AS avg_win,
            ROUND(AVG(net_profit) FILTER (WHERE net_profit <= 0), 4) AS avg_loss
        FROM trades
        WHERE exit_price IS NOT NULL AND net_profit IS NOT NULL
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

    # Re-subscribe the WebSocket to the new coin list immediately — without
    # this, newly added coins stream no data until the 60s self-heal poll.
    try:
        import data_collector
        data_collector.refresh_watchlist()
    except Exception:
        pass

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
    price:  float = 0.0              # frontend sends its known WebSocket price
    budget: Optional[float] = None   # user-chosen USDT budget (AITradingAgent sends this)


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

        # Live mode on the paper-fallback client: REFUSE. Executing through
        # the in-memory PaperClient here would record a simulated fill as
        # mode='live' — a fake trade indistinguishable from a real one.
        # Paper mode is unaffected.
        if get_mode() == "live" and is_using_paper_fallback():
            return {"ok": False,
                    "error": ("live Binance connection is down — refusing to simulate a live order "
                              "(paper-fallback client is active; restore API keys/connectivity or "
                              "switch to paper mode)")}

        # Use price hint from frontend; fall back to WebSocket cache if not provided
        hint_price = (req.price if req else 0) or 0
        price = hint_price or live_prices.get(sym, 0)
        if not price:
            return {"ok": False, "error": f"No live price for {sym} — WebSocket not yet connected"}

        usdt_balance = _get_usdt_balance()
        # User-supplied budget from the UI takes priority over the strategy sizing
        req_budget = float(req.budget) if (req and req.budget) else 0.0
        budget = req_budget if req_budget > 0 else get_budget_for_coin(sym, usdt_balance)
        if budget <= 0:
            return {"ok": False, "error": f"Budget 0 — balance: {usdt_balance:.2f} USDT"}
        # Binance MARKET min-notional guard (mirrors trade_engine's live $10 check)
        if budget < 10.0:
            return {"ok": False, "error": f"Budget {budget:.2f} USDT below the $10 Binance minimum notional"}
        if budget > usdt_balance:
            return {"ok": False, "error": f"Budget {budget:.2f} exceeds free balance {usdt_balance:.2f} USDT"}

        # Force-buy intentionally overrides trading_active (manual action), but it
        # must still respect the max_positions cap.
        _strategy_fb = _load_strategy()
        _max_pos_fb = int(_strategy_fb.get("max_positions", 10))
        with _positions_lock:
            already_held = any(p["symbol"] == sym for p in _positions)
            open_count = len(_positions)
        if already_held:
            return {"ok": False, "error": f"Already holding {sym}"}
        if open_count >= _max_pos_fb:
            return {"ok": False,
                    "error": f"Max positions reached ({open_count}/{_max_pos_fb}) — close a position first"}

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

        try:
            if get_mode() == "live" and not is_using_paper_fallback():
                # Live mode: python-binance/requests is geo-blocked — route the
                # order through the direct urllib+HMAC transport.
                import binance_direct
                result = binance_direct.order_market_buy(sym, budget)
            else:
                _client.update_price(sym, price)
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
        try:
            _buy_fee_fb, _ = _te._fills_fee_usdt(result.get("fills", []), budget * _te._fee_rate)
        except Exception:
            _buy_fee_fb = budget * 0.001
        pos = {
            "symbol": sym, "entry_price": fill_price, "quantity": qty,
            "budget_usdt": budget, "timestamp": datetime.now(timezone.utc).isoformat(),
            "mode": get_mode(), "exit_target": exit_target,
            "breakeven_mult_at_buy": round(_bep_m_fb, 8),
            "buy_fee_usdt": round(_buy_fee_fb, 6),   # real BEP/profit-gate accounting
            "opened_at_ts": time.time(),             # minimum-hold-time guard
            "origin": "manual",                      # mirror of trade_engine's origin:"auto"
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

        database.log_activity(f"Force buy: {sym} @ ${fill_price:.4f} | qty={qty:.6f} | budget={budget:.2f} USDT | origin=manual", "info")
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
        # Wait for completion so the caller sees the actual outcome, not a silent fire-and-forget.
        future = _sell_executor.submit(_execute_sell, pos, price, "force-sell")
        try:
            future.result(timeout=15.0)
            return {"ok": True, "symbol": sym, "price": price, "breakeven": breakeven_floor, "completed": True}
        except _ConcurrentTimeoutError:
            return {"ok": True, "symbol": sym, "price": price, "breakeven": breakeven_floor,
                    "completed": False, "note": "sell submitted but not completed in 15s — check activity log"}
        except Exception as _fs_exc:
            with _selling_lock:
                _selling.discard(sym)
                _selling_ts.pop(sym, None)
            return {"ok": False, "symbol": sym, "error": f"{type(_fs_exc).__name__}: {_fs_exc}", "completed": False}
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
    # Mirror critical settings into the SQLite database so they survive git
    # operations (which don't touch the DB) even if the .env is lost.
    _persistent_keys = {"MODE", "BINANCE_API_KEY", "BINANCE_API_SECRET"}
    for key, value in updates.items():
        if key in _persistent_keys:
            try:
                database.save_setting(f"env_{key}", str(value))
            except Exception:
                pass
    # Update os.environ so the restarted process inherits fresh values.
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
        _flush_db_state()
        # Re-exec the current process in place so the bot survives even WITHOUT a
        # supervisor (bash start.sh / Procfile have no restart loop). Where systemd
        # or Railway (Restart=always) supervises us, an exec-based restart is
        # equivalent and they will also handle any crash-exit.
        try:
            os.execv(sys.executable, [sys.executable] + sys.argv)
        except Exception as _re:
            print(f"[ModeSwitch] execv failed ({_re}) — falling back to exit(1) for supervisor restart")
            os._exit(1)

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


_signals_summary_cache: dict = {"ts": 0.0, "data": None}
_SIGNALS_SUMMARY_TTL = 3.0

@app.get("/api/signals-summary")
def api_signals_summary(limit: int = 30):
    """Top N coins by score with full signal results. Cached 3s."""
    global _signals_summary_cache
    now = time.time()
    cached = _signals_summary_cache
    if cached["data"] and (now - cached["ts"]) < _SIGNALS_SUMMARY_TTL:
        data = cached["data"]
        return {"signals": data["signals"][:limit], "total_tracked": data["total_tracked"], "ts": data["ts"], "cached": True}

    try:
        from trade_engine import _signal_cache, _signal_cache_lock
        import signal_registry as _sr
        with _signal_cache_lock:
            snap = dict(_signal_cache)
        strategy = _load_strategy()
        _SNAPSHOT_SKIP = {"E1_spread_too_wide"}

        signals_list = []
        for sym, entry in snap.items():
            try:
                sig = entry.get("signals", {})
                signal_data = {
                    "trend": sig.get("trend", False),
                    "rsi": sig.get("rsi", False),
                    "macd": sig.get("macd", False),
                    "volume": sig.get("volume", False),
                    "obv": sig.get("obv", False),
                    "atr": sig.get("atr", False),
                    "rsi_value": entry.get("rsi_val"),
                    "stoch_rsi_value": entry.get("stoch_rsi_val"),
                    "low_24h": entry.get("low_24h"),
                    "current_price": entry.get("price"),
                    "klines_1m": entry.get("klines_1m", []),
                }
                signal_results = {}
                for sig_id, sig_def in _sr.SIGNAL_REGISTRY.items():
                    if sig_id in _SNAPSHOT_SKIP:
                        signal_results[sig_id] = {"fired": False, "raw_value": "snapshot_skipped"}
                        continue
                    try:
                        fired, raw = sig_def.compute_fn(sym, signal_data, strategy)
                        signal_results[sig_id] = {"fired": bool(fired), "raw_value": raw}
                    except Exception:
                        signal_results[sig_id] = {"fired": False, "raw_value": None}

                decision = _sr.evaluate_buy_decision(sym, signal_data, strategy)
                # Full gate chain — a coin only displays as a BUY candidate when
                # the bot would ACTUALLY buy it (vetoes, cooldowns, macro gate,
                # capacity, paused state all included). Signal score alone is
                # NOT a buy: showing it as one made the bot look broken.
                import trade_engine as _te_gates
                gates = _te_gates.evaluate_buy_gates(sym)
                sig_ok = bool(decision.get("allowed", False))
                signals_list.append({
                    "symbol": sym,
                    "score": entry.get("score", 0),
                    "price": entry.get("price"),
                    "signal_results": signal_results,
                    "buy_allowed": sig_ok and gates["ready"],
                    "buy_reason": (decision.get("reason", "") if not sig_ok
                                   else ("; ".join(gates["blockers"]) if gates["blockers"] else "ready")),
                    "gate_blockers": gates["blockers"],
                    "signal_engine_allowed": sig_ok,
                    # §3.6 — advisory flag: a zero-maker-fee FDUSD sibling of
                    # this USDT pair is configured (entries.prefer_fee_promo_pairs
                    # on + fees.per_symbol_overrides maker_pct=0). Never switches
                    # the traded symbol — the watchlist owns symbol choice.
                    "promo_pair_available": bool(
                        getattr(_te_gates, "promo_pair_available", lambda _s: False)(sym)),
                    "ts": entry.get("ts", 0),
                })
            except Exception:
                continue

        signals_list.sort(key=lambda x: (0 if x["buy_allowed"] else 1, -(x["score"] or 0)))
        result = {"signals": signals_list, "total_tracked": len(signals_list), "ts": now}
        _signals_summary_cache = {"ts": now, "data": result}
        return {"signals": signals_list[:limit], "total_tracked": len(signals_list), "ts": now, "cached": False}
    except Exception as e:
        return {"signals": [], "total_tracked": 0, "ts": now, "error": str(e)}


@app.get("/api/proxy/binance/ticker/24hr")
async def api_proxy_ticker_24hr(symbols: str = None, symbol: str = None):
    """Chunked proxy for Binance 24hr ticker — avoids 400s from large symbol lists.
    Also accepts a single `symbol` param (MarketStatsBar/OrderBookPanel use it)."""
    import urllib.request as _ur
    import urllib.parse as _up
    if symbol and not symbols:
        # Single-symbol form: forward as-is; response is a dict, not a list.
        try:
            url = f"https://data-api.binance.vision/api/v3/ticker/24hr?symbol={_up.quote(symbol.upper())}"
            req = _ur.Request(url, headers={"User-Agent": "WolfBot/1.0"})
            with _ur.urlopen(req, timeout=5.0) as r:
                return json.loads(r.read())
        except Exception as e:
            return JSONResponse(status_code=502, content={"error": f"ticker fetch failed: {e}"})
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
            url = f"https://data-api.binance.vision/api/v3/ticker/24hr?symbols={json.dumps(chunk)}"
            req = _ur.Request(url, headers={"User-Agent": "WolfBot/1.0"})
            with _ur.urlopen(req, timeout=5.0) as r:
                all_results.extend(json.loads(r.read()))
        except Exception:
            continue  # partial results preferred over full failure
    return all_results


# Which signal_thresholds keys tune which registered signal (used to expose
# per-signal editable thresholds to the SignalsEditorPanel; the engine itself
# reads the flat strategy.signal_thresholds map at evaluation time).
_SIGNAL_THRESHOLD_KEYS = {
    "M1_rsi_below_threshold": ("rsi_buy_threshold",),
    "M2_stoch_rsi_oversold":  ("stoch_rsi_threshold",),
    "M4_micro_pullback":      ("dip_pct", "lookback_candles"),
    "M5_rsi_zone":            ("rsi_zone_low", "rsi_zone_high"),
    "P1_near_24h_low":        ("near_low_pct",),
    "R1_reversal_confirmed":  ("reversal_volume_multiplier",),
    "E1_spread_too_wide":     ("spread_max_pct",),
    "TM1_bad_hour":           ("allowed_trading_hours_utc",),
}


def _effective_signal_thresholds(strategy: dict) -> dict:
    """Flat thresholds map the engine actually resolves: defaults overlaid with
    strategy.signal_thresholds, plus M1's rsi_buy_threshold (default 40.0,
    legacy root key fallback — same resolution order as signal_registry)."""
    import signal_registry as _sr
    out = dict(getattr(_sr, "DEFAULT_SIGNAL_THRESHOLDS", {}))
    out["rsi_buy_threshold"] = strategy.get("rsi_buy_threshold", 40.0)
    stored = strategy.get("signal_thresholds")
    if isinstance(stored, dict):
        out.update(stored)
    return out


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

        # Effective role resolution — mirrors the updated signal_registry:
        # the strategy.signal_engine.roles map wins, then the list-based
        # defaults above. Read defensively and validate roles; anything not in
        # {scored, mandatory, veto, off} is ignored (falls through to defaults).
        _roles_cfg = strategy.get("signal_engine", {}).get("roles", {})
        if not isinstance(_roles_cfg, dict):
            _roles_cfg = {}
        _VALID_ROLES = {"scored", "mandatory", "veto", "off"}

        signals_info = []
        for sig_id, sig_def in _sr.SIGNAL_REGISTRY.items():
            _cfg_role = _roles_cfg.get(sig_id)
            _cfg_role = _cfg_role.strip().lower() if isinstance(_cfg_role, str) else None
            if _cfg_role in _VALID_ROLES:
                # "off" is the roles-map spelling; the UI vocabulary is "disabled"
                role = "disabled" if _cfg_role == "off" else _cfg_role
            elif sig_id in mandatory_ids:
                role = "mandatory"
            elif sig_id in scored_ids:
                role = "scored"
            elif sig_id in veto_ids:
                role = "veto"
            else:
                role = "disabled"
            entry = {
                "id":          sig_id,
                "category":    sig_def.category,
                "description": sig_def.description,
                "role":        role,
                # F3 — True when the role came from an explicit signal_engine.roles
                # entry (post-migration every signal has one), False when resolved
                # from the list/registry defaults.
                "role_explicit": _cfg_role in _VALID_ROLES,
            }
            # Per-signal editable thresholds (resolved values) — the
            # SignalsEditorPanel renders threshold inputs from this.
            _th_keys = _SIGNAL_THRESHOLD_KEYS.get(sig_id)
            if _th_keys:
                _eff_th = _effective_signal_thresholds(strategy)
                entry["thresholds"] = {k: _eff_th.get(k) for k in _th_keys}
            signals_info.append(entry)

        return {
            "available":      True,
            "engine_enabled": engine_active,
            # Explicit decision-path flags (same computation trade_engine uses:
            # strategy.get("signal_engine", {}).get("enabled", False)). When
            # "persisted" is False the roles above come from code defaults,
            # NOT from saved config — the legacy path is what actually runs.
            "active":         engine_active,
            "persisted":      bool(engine_cfg),
            "decision_path":  "signal_engine" if engine_active else "legacy_6_signal",
            "total":          len(signals_info),
            "categories":     sorted({s["category"] for s in signals_info}),
            "min_scored":     int(engine_cfg.get("min_scored", _sr.DEFAULT_SIGNAL_ENGINE["min_scored"])),
            "thresholds":     _effective_signal_thresholds(strategy),
            "signals":        signals_info,
        }
    except Exception as e:
        return {"available": False, "signals": [], "error": str(e)}


# ── Phase 5+6: Diagnostic endpoints ──────────────────────────────────────────

@app.get("/api/signals/telemetry")
def api_signals_telemetry():
    """Per-signal telemetry (evaluated / fired / fire_rate) plus a scanner
    health snapshot. Guarded so a version skew between control_api and
    signal_registry/trade_engine returns {"error": ...} instead of a 500."""
    try:
        import signal_registry as _sr
        import trade_engine as _te
        if not hasattr(_sr, "get_signal_telemetry"):
            return {"error": "signal_registry.get_signal_telemetry() not available "
                             "(registry version skew — restart the bot after updating)"}
        # Defensive copy: the scanner thread mutates this dict live.
        scanner = dict(getattr(_te, "_signal_scanner_health", {}) or {})
        tel = _sr.get_signal_telemetry()  # {"window_hours": N, "signals": {...}}
        return {
            "signals":      tel.get("signals", tel),
            "scanner":      scanner,
            "window_hours": tel.get("window_hours", 24),
        }
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/diagnostics/signal-rates")
def api_diag_signal_rates(window_hours: float = 1.0):
    """Per-signal firing rate over a rolling window."""
    try:
        import signal_registry as _sr
        return {"signal_rates": _sr.get_signal_fire_rates(window_hours), "window_hours": window_hours}
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/diagnostics/coin-trace/{symbol}")
def api_diag_coin_trace(symbol: str, hours: float = 1.0):
    """Per-coin evaluation history trace."""
    try:
        import signal_registry as _sr
        return _sr.get_coin_trace(symbol.upper(), hours)
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/diagnostics/signal-win-rates")
def api_diag_signal_win_rates(days: int = 7):
    """Win rates grouped by signal_snapshot for completed trades."""
    import sqlite3 as _sq_swr
    import json as _js_swr
    days = max(1, min(90, int(days)))
    try:
        conn = _sq_swr.connect(database.DB_PATH)
        conn.row_factory = _sq_swr.Row
        rows = conn.execute("""
            SELECT signal_snapshot, net_profit
            FROM trades
            WHERE timestamp_sell > datetime('now', ?)
              AND signal_snapshot IS NOT NULL
        """, (f"-{days} days",)).fetchall()
        conn.close()
    except Exception as e:
        return {"error": str(e)}

    buckets: dict = {}
    for r in rows:
        try:
            snap = _js_swr.loads(r["signal_snapshot"])
            fired = tuple(sorted(snap.get("fired_signals", [])))
            score = snap.get("score", 0)
            engine = snap.get("engine_enabled", False)
            key = f"score={score}|engine={'on' if engine else 'off'}"
        except Exception:
            continue
        b = buckets.setdefault(key, {"trades": 0, "wins": 0, "total_pnl": 0.0})
        b["trades"] += 1
        if (r["net_profit"] or 0) > 0:
            b["wins"] += 1
        b["total_pnl"] += (r["net_profit"] or 0)

    summary = [
        {"key": k, "trades": b["trades"],
         "win_rate_pct": round(100 * b["wins"] / b["trades"], 1),
         "total_pnl": round(b["total_pnl"], 4),
         "avg_pnl": round(b["total_pnl"] / b["trades"], 4)}
        for k, b in buckets.items() if b["trades"] >= 1
    ]
    summary.sort(key=lambda x: -x["avg_pnl"])
    return {"days": days, "buckets": summary, "sample_size": sum(b["trades"] for b in buckets.values())}


@app.get("/api/diagnostics/sell-timing")
def api_diag_sell_timing(hours: int = 24):
    """Two-stage sell execution latency: target→trigger and trigger→fill."""
    import sqlite3 as _sq_st
    hours = max(1, min(168, int(hours)))
    try:
        conn = _sq_st.connect(database.DB_PATH)
        conn.row_factory = _sq_st.Row
        rows = conn.execute("""
            SELECT coin, target_crossed_to_trigger_ms, trigger_to_filled_ms,
                   sell_reason, timestamp_sell
            FROM trades
            WHERE timestamp_sell > datetime('now', ?)
        """, (f'-{hours} hours',)).fetchall()
        conn.close()
    except Exception as e:
        return {"error": str(e)}

    rows = [dict(r) for r in rows]

    def pct(values, p):
        s = sorted(v for v in values if v is not None)
        if not s:
            return None
        return s[min(int(len(s) * p / 100), len(s) - 1)]

    t2t = [r["target_crossed_to_trigger_ms"] for r in rows if r["target_crossed_to_trigger_ms"] is not None]
    t2f = [r["trigger_to_filled_ms"]         for r in rows if r["trigger_to_filled_ms"]         is not None]

    slow_t2t = sorted(
        [{"coin": r["coin"], "target_to_trigger_ms": r["target_crossed_to_trigger_ms"],
          "reason": r["sell_reason"], "ts": r["timestamp_sell"]}
         for r in rows if r["target_crossed_to_trigger_ms"] is not None and r["target_crossed_to_trigger_ms"] > 1000],
        key=lambda x: -x["target_to_trigger_ms"]
    )[:10]

    slow_t2f = sorted(
        [{"coin": r["coin"], "trigger_to_filled_ms": r["trigger_to_filled_ms"],
          "reason": r["sell_reason"], "ts": r["timestamp_sell"]}
         for r in rows if r["trigger_to_filled_ms"] is not None and r["trigger_to_filled_ms"] > 1000],
        key=lambda x: -x["trigger_to_filled_ms"]
    )[:10]

    return {
        "window_hours":            hours,
        "trades_count":            len(rows),
        "target_to_trigger_count": len(t2t),
        "trigger_to_filled_count": len(t2f),
        "target_to_trigger_ms": {
            "min": min(t2t) if t2t else None, "max": max(t2t) if t2t else None,
            "p50": pct(t2t, 50), "p90": pct(t2t, 90),
            "p95": pct(t2t, 95), "p99": pct(t2t, 99),
        },
        "trigger_to_filled_ms": {
            "min": min(t2f) if t2f else None, "max": max(t2f) if t2f else None,
            "p50": pct(t2f, 50), "p90": pct(t2f, 90),
            "p95": pct(t2f, 95), "p99": pct(t2f, 99),
        },
        "slow_target_to_trigger": slow_t2t,
        "slow_trigger_to_filled": slow_t2f,
    }


@app.get("/api/diagnostics/strategy-audit")
def api_diag_strategy_audit(limit: int = 50):
    """Recent strategy field changes."""
    import sqlite3 as _sq_sa
    limit = max(1, min(500, int(limit)))
    try:
        conn = _sq_sa.connect(database.DB_PATH)
        conn.row_factory = _sq_sa.Row
        rows = conn.execute("""
            SELECT id, timestamp, field_key, old_value, new_value, source
            FROM strategy_audit
            ORDER BY id DESC LIMIT ?
        """, (limit,)).fetchall()
        conn.close()
        return {"count": len(rows), "changes": [dict(r) for r in rows]}
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/alerts")
def api_alerts(limit: int = 50, only_unacknowledged: bool = False):
    """List recent alerts."""
    import sqlite3 as _sq_al
    limit = max(1, min(500, int(limit)))
    try:
        conn = _sq_al.connect(database.DB_PATH)
        conn.row_factory = _sq_al.Row
        where = "WHERE acknowledged = 0" if only_unacknowledged else ""
        rows = conn.execute(
            f"SELECT id, timestamp, severity, category, message, metadata, acknowledged "
            f"FROM alerts {where} ORDER BY id DESC LIMIT ?",
            (limit,)
        ).fetchall()
        conn.close()
        return {"count": len(rows), "alerts": [dict(r) for r in rows]}
    except Exception as e:
        return {"error": str(e)}


@app.post("/api/alerts/{alert_id}/acknowledge")
def api_alert_acknowledge(alert_id: int):
    """Acknowledge an alert by ID."""
    import sqlite3 as _sq_ack
    try:
        conn = _sq_ack.connect(database.DB_PATH)
        cur = conn.cursor()
        cur.execute("UPDATE alerts SET acknowledged = 1 WHERE id = ?", (alert_id,))
        conn.commit()
        affected = cur.rowcount
        conn.close()
        if affected == 0:
            return {"ok": False, "error": f"Alert {alert_id} not found"}
        return {"ok": True, "alert_id": alert_id}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/api/proxy/binance/{path:path}")
async def api_proxy_binance(path: str, request: Request):
    """Server-side proxy for Binance REST API — avoids browser CORS restrictions."""
    import urllib.request as _ur
    import urllib.error as _ue
    qs = str(request.query_params)
    url = f"https://data-api.binance.vision/api/v3/{path}"
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


# ── Part C §6.5: Data-health panel (GET /api/health/data) ───────────────────
# All three sources are wired DEFENSIVELY: the getter functions are being added
# by parallel work (data_collector.get_data_health, binance_limits.get_limits_health,
# trade_engine.get_engine_health). Until they land — or if any of them ever
# raises — the corresponding section degrades to {"available": false} and the
# endpoint keeps serving 200s.

# Module state for the "scan_skipped_overlap grew in the last 10 min" red rule.
_scan_skip_track = {
    "value": None,      # last observed scan_skipped_overlap counter
    "ts": 0.0,          # when it was observed
    "grew_ts": 0.0,     # last time we saw the counter increase
    "grew_from": None,
    "grew_to": None,
}
_scan_skip_lock = threading.Lock()
_SCAN_SKIP_RED_WINDOW_SEC = 600.0  # 10 minutes


def _health_num(v):
    """Best-effort float conversion; returns None instead of raising."""
    try:
        if v is None or isinstance(v, bool):
            return None
        return float(v)
    except Exception:
        return None


def _health_data_section() -> dict:
    """data_collector.get_data_health() → dict, or {"available": False}."""
    try:
        import data_collector as _dc
        fn = getattr(_dc, "get_data_health", None)
        if not callable(fn):
            return {"available": False}
        raw = fn()
        if not isinstance(raw, dict):
            return {"available": False}
        out = dict(raw)
        out["available"] = True
        return out
    except Exception:
        return {"available": False}


def _health_limits_section() -> dict:
    """binance_limits.get_limits_health() → dict, or {"available": False}."""
    try:
        import binance_limits as _bl
        fn = getattr(_bl, "get_limits_health", None)
        if not callable(fn):
            return {"available": False}
        raw = fn()
        if not isinstance(raw, dict):
            return {"available": False}
        out = dict(raw)
        out["available"] = True
        return out
    except Exception:
        return {"available": False}


def _health_engine_section() -> dict:
    """trade_engine.get_engine_health() → dict, or {"available": False}."""
    try:
        import trade_engine as _te
        fn = getattr(_te, "get_engine_health", None)
        if not callable(fn):
            return {"available": False}
        raw = fn()
        if not isinstance(raw, dict):
            return {"available": False}
        out = dict(raw)
        out["available"] = True
        return out
    except Exception:
        return {"available": False}


def _current_scan_skipped_overlap(engine: dict):
    """scan_skipped_overlap from engine health scanner section, falling back to
    trade_engine._signal_scanner_health on older engine versions."""
    try:
        sc = engine.get("scanner")
        if isinstance(sc, dict) and "scan_skipped_overlap" in sc:
            v = _health_num(sc.get("scan_skipped_overlap"))
            if v is not None:
                return v
    except Exception:
        pass
    try:
        import trade_engine as _te
        ssh = getattr(_te, "_signal_scanner_health", None)
        if isinstance(ssh, dict):
            return _health_num(ssh.get("scan_skipped_overlap"))
    except Exception:
        pass
    return None


def _compute_health_snapshot() -> dict:
    """Shared health computation for /api/health/data and /api/diagnostics.

    Returns {"status", "causes", "data", "limits", "engine", "ts"}.
    causes is EMPTY when green; each tripped red rule contributes one sentence.
    """
    now = time.time()
    data = _health_data_section()
    limits = _health_limits_section()
    engine = _health_engine_section()

    causes: list = []
    warns: list = []   # F5 — degraded/yellow tier (not red, but not healthy)

    # RED RULE 1: any active symbol's 1m candle age > 90s
    try:
        stale = []
        for item in (data.get("worst_candle_ages") or []):
            try:
                sym, age = item[0], float(item[1])
            except Exception:
                continue
            if age > 90:
                stale.append((str(sym), age))
        if stale:
            worst = ", ".join(f"{s} ({a:.0f}s)" for s, a in stale)
            causes.append(f"1m candle data is stale (>90s) for: {worst}.")
    except Exception:
        pass

    # RED RULE 2: used_weight > 4800
    try:
        uw = _health_num(limits.get("used_weight"))
        if uw is not None and uw > 4800:
            causes.append(
                f"Binance REST used weight is {uw:.0f}, above the 4800 red line (limit 6000)."
            )
    except Exception:
        pass

    # RED RULE 3: any held symbol's price age > 5s
    try:
        hpa = _health_num(engine.get("held_max_price_age_sec"))
        if hpa is not None and hpa > 5:
            held = engine.get("held_symbols") or []
            held_note = f" across {len(held)} held symbol(s)" if isinstance(held, (list, tuple)) and held else ""
            causes.append(
                f"Price feed for held positions is stale: max price age {hpa:.1f}s (>5s){held_note}."
            )
    except Exception:
        pass

    # RED RULE 4: scan_skipped_overlap grew within the last 10 minutes
    try:
        skip = _current_scan_skipped_overlap(engine)
        grew_ts = 0.0
        grew_from = grew_to = None
        with _scan_skip_lock:
            if skip is not None:
                prev = _scan_skip_track["value"]
                if prev is not None and skip > prev:
                    _scan_skip_track["grew_ts"] = now
                    _scan_skip_track["grew_from"] = prev
                    _scan_skip_track["grew_to"] = skip
                _scan_skip_track["value"] = skip
                _scan_skip_track["ts"] = now
            grew_ts = _scan_skip_track["grew_ts"]
            grew_from = _scan_skip_track["grew_from"]
            grew_to = _scan_skip_track["grew_to"]
        if grew_ts and (now - grew_ts) <= _SCAN_SKIP_RED_WINDOW_SEC:
            causes.append(
                f"Signal scans are overlapping: scan_skipped_overlap grew from "
                f"{grew_from:.0f} to {grew_to:.0f} within the last 10 minutes."
            )
    except Exception:
        pass

    # RED RULE 5: reconnect_attempts_5min > 30
    try:
        rc = _health_num(data.get("reconnect_attempts_5min"))
        if rc is not None and rc > 30:
            causes.append(
                f"WebSocket reconnect storm: {rc:.0f} reconnect attempts in the last 5 minutes (>30)."
            )
    except Exception:
        pass

    # RED RULE 6: banned_until / rest_paused_until in the future
    try:
        banned = _health_num(limits.get("banned_until"))
        if banned and banned > now:
            causes.append(
                f"Binance IP ban is active (HTTP 418): {banned - now:.0f}s remaining."
            )
        paused = _health_num(limits.get("rest_paused_until"))
        if paused and paused > now:
            causes.append(
                f"REST traffic is paused after rate limiting (HTTP 429): {paused - now:.0f}s remaining."
            )
    except Exception:
        pass

    # ── F5 — degraded (YELLOW) rules: not fatal, but health must not read green.
    # WARN RULE A: stale signals present (cache older than the scanner's
    # staleness threshold) — previously left health green while 6 symbols went
    # unscanned.
    try:
        sc = engine.get("scanner") if isinstance(engine.get("scanner"), dict) else {}
        stale_n = _health_num(sc.get("stale_signal_count"))
        if stale_n is not None and stale_n > 0:
            stale_syms = engine.get("stale_signal_syms") or []
            listed = (": " + ", ".join(str(s) for s in stale_syms[:10])) if stale_syms else ""
            warns.append(f"{stale_n:.0f} symbol(s) have stale signal data{listed}.")
    except Exception:
        pass

    # WARN RULE B: subscription coverage below the universe — some watchlist
    # symbols have no active market stream.
    try:
        covered = _health_num(data.get("symbols_covered"))
        expected = _health_num(data.get("expected_symbols"))
        if covered is not None and expected is not None and covered < expected:
            missing = data.get("uncovered_symbols") or []
            listed = (": " + ", ".join(str(s) for s in missing[:15])) if missing else ""
            warns.append(
                f"Subscription coverage {covered:.0f}/{expected:.0f} symbols — "
                f"{expected - covered:.0f} uncovered{listed}.")
    except Exception:
        pass

    status = "red" if causes else ("yellow" if warns else "green")
    return {
        "status": status,
        "causes": causes,
        "warns": warns,
        "data": data,
        "limits": limits,
        "engine": engine,
        "ts": now,
    }


def _limits_report_line() -> str:
    """One-line rate-limit state for the plain-text diagnostic report.
    Covers used weight plus any active 429 pause / 418 ban with remaining seconds."""
    lim = _health_limits_section()
    if not lim.get("available"):
        return "Rate limits: n/a (binance_limits not available)"
    now = time.time()
    parts = [f"Rate limits: used_weight={lim.get('used_weight')}"]
    oc = lim.get("order_count_10s")
    if oc is not None:
        parts.append(f"orders_10s={oc}")
    banned = _health_num(lim.get("banned_until"))
    if banned and banned > now:
        parts.append(f"BANNED (418) for {banned - now:.0f}s more")
    paused = _health_num(lim.get("rest_paused_until"))
    if paused and paused > now:
        parts.append(f"REST PAUSED (429) for {paused - now:.0f}s more")
    half = _health_num(lim.get("half_rate_until"))
    if half and half > now:
        parts.append(f"half-rate for {half - now:.0f}s more")
    last_429 = _health_num(lim.get("last_429_ts"))
    if last_429:
        parts.append(f"last 429 {now - last_429:.0f}s ago")
    last_418 = _health_num(lim.get("last_418_ts"))
    if last_418:
        parts.append(f"last 418 {now - last_418:.0f}s ago")
    return "  ".join(parts)


@app.get("/api/health/data")
def api_health_data():
    """Part C §6.5 health panel: data feed + rate limits + engine health with RED rules."""
    try:
        return _compute_health_snapshot()
    except Exception as e:
        # Last-resort backstop — this endpoint must never 500.
        return {
            "status": "red",
            "causes": [f"Health computation failed: {type(e).__name__}: {e}."],
            "data": {"available": False},
            "limits": {"available": False},
            "engine": {"available": False},
            "ts": time.time(),
        }


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

    ssh = dict(_te._signal_scanner_health)   # defensive copy — scanner thread mutates it live
    last_scan_age = round(now - ssh.get("last_refresh_ts", 0.0), 1) if ssh.get("last_refresh_ts") else None
    # Effective interval (actual pacing, incl. adaptive stretch) — falls back to
    # the configured base interval on older trade_engine versions.
    _eff_interval = float(ssh.get("effective_interval_sec") or ssh.get("interval_sec") or 0.0)
    next_scan_in  = round(max(0.0, _eff_interval - (last_scan_age or _eff_interval)), 1)
    scan_progress_pct = round(
        min(100.0, (last_scan_age or 0) / max(1.0, _eff_interval) * 100), 1
    ) if last_scan_age else 0.0
    _last_scan_dur_ms = float(ssh.get("last_duration_ms") or 0.0)
    # Overloaded = the last scan consumed >80% of the effective scan budget.
    _scanner_overloaded = bool(_eff_interval > 0 and _last_scan_dur_ms > 0.8 * _eff_interval * 1000)

    sm_hb      = getattr(_te, "_sell_monitor_heartbeat", 0)
    sm_hb_age  = round(now - sm_hb, 1) if sm_hb else None

    refresher  = getattr(_te, "_held_refresher_thread", None)
    ref_alive  = bool(refresher and refresher.is_alive())

    active_threads = [t.name for t in _thr.enumerate() if t.is_alive()]

    # Part C §6.5: compact health verdict (shared computation with /api/health/data)
    try:
        _hs = _compute_health_snapshot()
        _health_compact = {"status": _hs.get("status", "green"), "causes": _hs.get("causes", [])}
    except Exception:
        _health_compact = {"status": "green", "causes": []}

    return {
        "server_time": now,
        "health": _health_compact,
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
            # Old field names kept for the current UI — semantics now honest:
            # ages/progress are computed against the EFFECTIVE interval, and
            # last_duration_ms is the scan's own runtime (not runtime+sleep).
            "last_refresh_age_sec":  last_scan_age,
            "next_refresh_in_sec":   next_scan_in,
            "scan_progress_pct":     scan_progress_pct,
            "interval_sec":          ssh.get("interval_sec", 0),      # configured base interval
            "effective_interval_sec": _eff_interval,                  # actual pacing in force
            "last_duration_ms":      _last_scan_dur_ms,
            "scans_completed":       ssh.get("scans_completed", 0),
            "scan_skipped_overlap":  ssh.get("scan_skipped_overlap", 0),
            "universe_size":         ssh.get("universe_size", 0),
            "cached_signals_count":  len(_te._signal_cache),          # signal-cache size
            "overloaded":            _scanner_overloaded,
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


@app.get("/api/diagnostics/bundle")
def api_diagnostics_bundle():
    """One-click full diagnostic bundle — plaintext report aggregating version,
    health, limits, scanner, telemetry, gate blockers, risk, analytics and
    recent errors, formatted for pasting into a chat for remote diagnosis.
    Every section is independently guarded: a broken subsystem shows its error
    instead of killing the report (that would defeat the purpose)."""
    import io
    out = io.StringIO()
    now_iso = datetime.now(timezone.utc).isoformat()

    def section(title):
        out.write(f"\n===== {title} =====\n")

    def safe(fn, label=""):
        try:
            return fn()
        except Exception as e:
            out.write(f"  [{label or 'section'} unavailable: {type(e).__name__}: {e}]\n")
            return None

    out.write(f"WOLFBOT DIAGNOSTIC BUNDLE — {now_iso}\n")

    # -- Version / deploy --------------------------------------------------
    section("VERSION")
    def _ver():
        v = _read_frontend_version()
        out.write(f"  version={v.get('version')} buildTime={v.get('buildTime')} "
                  f"commit={v.get('commit')} deploy_id={_DEPLOY_ID[:8]}\n")
        out.write(f"  mode={get_mode()} paper_fallback={is_using_paper_fallback()} "
                  f"live_error={get_live_error() or 'none'}\n")
    safe(_ver, "version")

    # -- Health ------------------------------------------------------------
    section("HEALTH")
    def _health():
        snap = _compute_health_snapshot()
        out.write(f"  status={snap.get('status')}\n")
        for c in snap.get("causes", []):
            out.write(f"  CAUSE: {c}\n")
        for w in snap.get("warns", []):
            out.write(f"  WARN: {w}\n")
        d = snap.get("data") or {}
        if d.get("available"):
            out.write(f"  ws_connections={len(d.get('connections', []))} "
                      f"msgs/s={d.get('total_msgs_per_sec')} "
                      f"subscribed={d.get('subscribed_coins')} "
                      f"covered={d.get('symbols_covered')}/{d.get('expected_symbols')} "
                      f"streams={d.get('streams')} "
                      f"buffer_1m={d.get('buffer_fill_pct_1m')}% "
                      f"buffer_5m={d.get('buffer_fill_pct_5m')}% "
                      f"gap_repairs_24h={d.get('gap_repairs_24h')} "
                      f"reconnects_5min={d.get('reconnect_attempts_5min')}\n")
            wca = d.get("worst_candle_ages") or []
            if wca:
                out.write("  worst candle ages: "
                          + ", ".join(f"{sym}={age:.0f}s" for sym, age in wca[:5]) + "\n")
        lim = snap.get("limits") or {}
        if lim.get("available"):
            out.write(f"  used_weight={lim.get('used_weight')}/6000 "
                      f"bg_spend_60s={lim.get('background_spend_60s')} "
                      f"orders_10s={lim.get('order_count_10s')}\n")
        eng = snap.get("engine") or {}
        if eng.get("available"):
            sc = eng.get("scanner") or {}
            out.write(f"  scanner mode={sc.get('mode')} universe={sc.get('universe_size')} "
                      f"stale_signals={sc.get('stale_signal_count')} "
                      f"overlap_skips={sc.get('scan_skipped_overlap')} "
                      f"held_max_price_age={eng.get('held_max_price_age_sec')}s\n")
    safe(_health, "health")

    # -- Bot status ----------------------------------------------------------
    section("STATUS")
    def _stat():
        st = api_status()
        for k in ("running", "balance_usdt", "initial_balance", "open_positions",
                  "trades_today", "win_rate", "total_trades", "realized_pnl",
                  "watched_coins"):
            v = st.get(k)
            if isinstance(v, list):
                v = f"{len(v)} coins"
            out.write(f"  {k}={v}\n")
    safe(_stat, "status")

    # -- Risk / breakers -----------------------------------------------------
    section("RISK")
    def _risk():
        import trade_engine as _te
        rs = _te.get_risk_status()
        d = rs.get("daily", {})
        out.write(f"  daily: pnl_today={d.get('pnl_today')} limit={d.get('limit_usdt')} "
                  f"stopped={d.get('stopped')}\n")
        c = rs.get("consecutive", {})
        out.write(f"  consecutive_losses: {c.get('count')}/{c.get('limit')} "
                  f"paused_until={c.get('paused_until')}\n")
        sl = rs.get("slots", {})
        out.write(f"  slots: {sl.get('effective_slots')}/{sl.get('max_positions')} "
                  f"degraded={sl.get('degraded')} allocation={sl.get('effective_allocation')}\n")
        sv = rs.get("slippage_vetoes", {})
        if sv:
            out.write("  slippage vetoes: "
                      + ", ".join(f"{s}({v.get('avg_bps'):.0f}bps)" for s, v in sv.items()) + "\n")
        corr = rs.get("correlation", {})
        out.write(f"  correlation: entries_5min={corr.get('entries_5min')}/{corr.get('limit')} "
                  f"btc_5m_red={corr.get('btc_5m_red')}\n")
    safe(_risk, "risk")

    # -- Gate blockers distribution (why coins aren't being bought) ----------
    section("GATE BLOCKERS (current signals snapshot)")
    def _gates():
        res = api_signals_summary(limit=100)
        reasons: dict = {}
        allowed = 0
        for sig in res.get("signals", []):
            if sig.get("buy_allowed"):
                allowed += 1
            else:
                r = sig.get("buy_reason", "unknown")[:70]
                reasons[r] = reasons.get(r, 0) + 1
        out.write(f"  tracked={res.get('total_tracked')} buy_ready={allowed}\n")
        for r, n in sorted(reasons.items(), key=lambda x: -x[1])[:12]:
            out.write(f"  {n:>3}x {r}\n")
    safe(_gates, "gates")

    # -- Signal telemetry -----------------------------------------------------
    section("SIGNAL TELEMETRY (24h fire rates)")
    def _tel():
        import signal_registry as _sr
        tel = _sr.get_signal_telemetry()
        sigs = tel.get("signals", tel) or {}
        for sid, v in sorted(sigs.items()):
            ev, fi = v.get("evaluated", 0), v.get("fired", 0)
            rate = (fi / ev * 100) if ev else 0.0
            out.write(f"  {sid:<28} fired {fi}/{ev} ({rate:.1f}%)\n")
        if not sigs:
            out.write("  (no evaluations yet)\n")
    safe(_tel, "telemetry")

    # -- Analytics / expectancy (7d and 30d) ----------------------------------
    for days in (7, 30):
        section(f"ANALYTICS {days}d")
        def _an(days=days):
            ex = api_stats_expectancy(days=days)
            out.write(f"  trades={ex.get('trades')} win_rate={ex.get('win_rate')}% "
                      f"avg_win={ex.get('avg_win')} avg_loss={ex.get('avg_loss')} "
                      f"expectancy={ex.get('expectancy_per_trade')} "
                      f"profit_factor={ex.get('profit_factor')}\n")
            if ex.get("note"):
                out.write(f"  NOTE: {ex.get('note')}\n")
            else:
                out.write(f"  data_start={ex.get('data_start_ts')}\n")
            out.write(f"  total_fees={ex.get('total_fees')} "
                      f"fee_share={ex.get('fee_share_of_gross')} "
                      f"avg_hold={ex.get('avg_hold_time_sec')}s\n")
            labels = ex.get("exit_labels") or {}
            if labels:
                out.write("  exits: " + ", ".join(
                    f"{k}={v.get('count')}({v.get('net_pnl'):+.2f})"
                    for k, v in sorted(labels.items())) + "\n")
            per = ex.get("per_symbol") or []
            for row in per[:8]:
                out.write(f"    {row.get('symbol'):<12} trades={row.get('trades')} "
                          f"pnl={row.get('net_pnl'):+.2f} wr={row.get('win_rate')}%\n")
        safe(_an, f"analytics{days}")

    # -- Exit-R distribution (F1/F9) -------------------------------------------
    section("EXIT-R (planned vs realized)")
    def _exitr():
        er = api_diagnostics_exit_r()
        if not er.get("available"):
            out.write(f"  unavailable: {er.get('reason') or er.get('error')}\n")
            return
        dist = er.get("distribution") or er.get("buckets") or er.get("r_distribution")
        if isinstance(dist, dict):
            out.write("  distribution: " + ", ".join(
                f"{k}={v}" for k, v in dist.items()) + "\n")
        elif isinstance(dist, list):
            out.write("  distribution: " + ", ".join(str(x) for x in dist) + "\n")
        for k in ("planned_r_avg", "realized_r_avg", "planned_r_median",
                  "realized_r_median", "count", "n", "unlabeled_exits",
                  "unlabeled_count"):
            if k in er:
                out.write(f"  {k}={er.get(k)}\n")
    safe(_exitr, "exit_r")

    # -- Chronic spread (E1) vetoes (F10) --------------------------------------
    section("SPREAD-VETO STATS (E1, 24h)")
    def _veto():
        vs = api_diagnostics_veto_stats(hours=24.0)
        if vs.get("error"):
            out.write(f"  error: {vs.get('error')}\n")
            return
        syms = vs.get("symbols") or {}
        prune = vs.get("prune_candidates") or []
        out.write(f"  symbols_tracked={len(syms)} prune_candidates={len(prune)}\n")
        if prune:
            out.write("  PRUNE (E1>70%): " + ", ".join(
                f"{s}({syms[s]['e1_veto_pct']}% of {syms[s]['evals']})"
                for s in prune[:20]) + "\n")
        worst = sorted(syms.items(),
                       key=lambda kv: kv[1].get("e1_veto_pct", 0), reverse=True)[:8]
        for s, d in worst:
            out.write(f"    {s:<12} e1={d.get('e1_veto_pct')}% "
                      f"evals={d.get('evals')} vetoes={d.get('e1_vetoes')}\n")
    safe(_veto, "veto_stats")

    # -- Open positions --------------------------------------------------------
    section("OPEN POSITIONS")
    def _pos():
        import trade_engine as _te
        with _te._positions_lock:
            snap = list(_te._positions)
        if not snap:
            out.write("  none\n")
        for pth in snap:
            out.write(f"  {pth.get('symbol'):<12} qty={pth.get('quantity')} "
                      f"entry={pth.get('entry_price')} stop={pth.get('stop_price')} "
                      f"tp={pth.get('tp_price')} be_moved={pth.get('be_moved')} "
                      f"origin={pth.get('origin')} atr={pth.get('atr_pct_at_entry')}\n")
    safe(_pos, "positions")

    # -- Recent errors & warnings ----------------------------------------------
    section("RECENT ERRORS / WARNINGS (last 40)")
    def _errs():
        entries = database.get_activity_log(limit=400)
        picked = [e for e in entries
                  if str(e.get("severity", e.get("level", ""))).lower() in ("error", "warn", "warning")][:40]
        if not picked:
            out.write("  none\n")
        for e in picked:
            ts = e.get("timestamp", e.get("ts", ""))
            sev = str(e.get("severity", e.get("level", "?"))).upper()[:5]
            out.write(f"  [{ts}] {sev} {str(e.get('message', ''))[:200]}\n")
    safe(_errs, "errors")

    # -- Diag issues (structured) -----------------------------------------------
    section("DIAG ISSUES (recent)")
    def _diag():
        import trade_engine as _te
        issues = getattr(_te, "get_diag_issues", None)
        rows = issues(limit=25) if callable(issues) else []
        if not rows:
            out.write("  none\n")
        for r in rows[:25]:
            out.write(f"  [{r.get('ts', '')}] {r.get('severity', '?')} "
                      f"{r.get('source', '?')}: {str(r.get('message', ''))[:160]}\n")
    safe(_diag, "diag")

    # -- Config snapshot ----------------------------------------------------------
    section("CONFIG (resolved key settings)")
    def _cfg():
        import strategy_config as _scfg
        raw = _load_strategy()
        view = _scfg.current_v2_view(raw)
        out.write(f"  config_hash={database.config_hash(raw)} "
                  f"schema_version={view.get('schema_version')}\n")
        for block in ("sizing", "entries", "exits", "risk", "regime", "fees", "data"):
            b = view.get(block) or {}
            kv = " ".join(f"{k}={v}" for k, v in sorted(b.items())
                          if not isinstance(v, (dict, list)))
            out.write(f"  {block}: {kv}\n")
        se = raw.get("signal_engine", {})
        out.write(f"  signal_engine: enabled={se.get('enabled', False)} "
                  f"min_scored={se.get('min_scored')} roles={se.get('roles', {})}\n")
    safe(_cfg, "config")

    out.write("\n===== END OF BUNDLE =====\n")
    return Response(content=out.getvalue(), media_type="text/plain")


@app.get("/api/diagnostics/log/text")
def api_diagnostics_log_text(limit: int = 50, severity: str = ""):
    """Plain-text diagnostic report — paste directly into chat or save to file."""
    from fastapi.responses import Response as _Resp
    import trade_engine as _te
    import data_collector as _dc
    from datetime import datetime as _dt, timezone as _tz

    entries = _te.get_diag_log(limit=limit, severity_filter=severity)

    # Part D1: the old header printed a single unlabeled `latency=NNNms` with no
    # indication of which endpoint/host it measured. Probe the two distinct REST
    # targets separately and label each with host+endpoint+method.
    import urllib.request as _ur_diag

    def _fmt_probe(v):
        return v if isinstance(v, str) else f"{v:.0f}ms"

    # (a) public_data_ms — GET https://data-api.binance.vision/api/v3/ping
    try:
        _t0_pub = time.time()
        _preq = _ur_diag.Request("https://data-api.binance.vision/api/v3/ping",
                                 headers={"User-Agent": "WolfBot/1.0"})
        with _ur_diag.urlopen(_preq, timeout=5.0) as _pr:
            _pr.read()
        public_data_ms = (time.time() - _t0_pub) * 1000
    except Exception as _pub_e:
        public_data_ms = f"err ({type(_pub_e).__name__})"

    # (b) signed_api_ms — GET api.binance.com /api/v3/account via binance_direct
    #     (only meaningful in live mode with API keys configured)
    if get_mode() == "live" \
            and (os.getenv("BINANCE_API_KEY") or "").strip() \
            and (os.getenv("BINANCE_API_SECRET") or "").strip():
        try:
            import binance_direct as _bd_diag
            _t0_signed = time.time()
            _bd_diag.get_account()
            signed_api_ms = (time.time() - _t0_signed) * 1000
        except Exception as _signed_e:
            signed_api_ms = f"err ({type(_signed_e).__name__})"
    else:
        signed_api_ms = "n/a (paper)"

    try:
        bh = _te._binance_health
        wh = _dc._ws_health
        header = [
            f"=== WolfBot Diagnostic Report — {_dt.now(_tz.utc).isoformat()} ===",
            f"Deploy: {_DEPLOY_ID}",
            f"Binance REST: weight={bh.get('used_weight_1m',0)}/6000  "
            f"errors={bh.get('rest_error_count',0)}",
            f"REST latency: public data-api.binance.vision/ping={_fmt_probe(public_data_ms)} | "
            f"signed api.binance.com/account={_fmt_probe(signed_api_ms)}",
            f"WebSocket: connected={wh.get('connected',False)}  "
            f"msgs={wh.get('messages_received',0)}  "
            f"disconnects={wh.get('disconnect_count',0)}",
            f"Open positions: {len(_te._positions)}  "
            f"In-progress sells: {len(_te._selling)}",
            f"--- Recent issues ({len(entries)}) ---",
        ]
    except Exception:
        header = ["=== WolfBot Diagnostic Report ==="]

    # Part C §6.5: 429/418 visibility — rate-limit state line (used weight,
    # active pause/ban with remaining seconds). Defensive: skipped entirely if
    # binance_limits isn't wired yet.
    try:
        _lim_line = _limits_report_line()
        if header and header[-1].startswith("---"):
            header.insert(len(header) - 1, _lim_line)
        else:
            header.append(_lim_line)
    except Exception:
        pass

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


@app.get("/api/diagnostics/orphan-check")
def api_orphan_check(min_value_usdt: float = 10.0):
    # Default threshold = Binance's ~$10 min notional: holdings below it can't
    # be market-sold by the bot anyway, so alerting on $0.10 dust is pure noise.
    """Compare Binance balances to bot DB positions. Reports orphans and mismatches."""
    import sqlite3 as _sq

    # Get Binance balances using existing account cache helper
    try:
        acc = _get_cached_account()
        raw_balances = {b["asset"]: float(b["free"]) + float(b["locked"])
                        for b in acc.get("balances", [])
                        if float(b["free"]) + float(b["locked"]) > 0}
    except Exception as e:
        return {"error": f"Failed to fetch Binance balances: {e}"}

    # Get current prices
    prices = {}
    try:
        from trade_engine import _signal_cache, _signal_cache_lock
        with _signal_cache_lock:
            prices = {sym: entry.get("price", 0) for sym, entry in _signal_cache.items()}
    except Exception:
        pass

    # Get DB positions
    conn = _sq.connect(database.DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT symbol, quantity FROM positions")
    db_positions = {row[0]: float(row[1]) for row in cur.fetchall()}
    conn.close()

    stables = {"USDT", "BUSD", "USDC", "FDUSD", "TUSD", "DAI", "USDP", "BNB",
               "USD", "AEUR", "EUR", "GBP", "TRY", "BRL", "AUD", "UAH", "RUB",
               "NGN", "ZAR", "PLN", "ARS", "JPY", "MXN", "CZK", "COP"}
    issues = []

    for asset, qty in raw_balances.items():
        # LD* = Binance Earn wrappers — not tradeable spot assets
        if asset in stables or asset.startswith("LD") or qty <= 0:
            continue
        symbol = f"{asset}USDT"
        price = prices.get(symbol, 0)
        value_usdt = round(qty * price, 4) if price > 0 else None
        # Unknown price (value None) on the Binance side means an asset with no
        # USDT pair or one the bot never tracks — it's dust noise, skip it too.
        if value_usdt is None or value_usdt < min_value_usdt:
            continue
        db_qty = db_positions.get(symbol)
        if db_qty is None:
            issues.append({"type": "orphan_on_binance", "symbol": symbol,
                           "binance_qty": qty, "db_qty": None, "value_usdt": value_usdt})
        else:
            diff = abs(qty - db_qty)
            diff_pct = (diff / db_qty * 100) if db_qty > 0 else 100
            if diff_pct > 5.0 and (price or 0) * diff > min_value_usdt:
                issues.append({"type": "qty_mismatch", "symbol": symbol,
                               "binance_qty": qty, "db_qty": db_qty,
                               "diff_pct": round(diff_pct, 2), "value_usdt": value_usdt})

    for symbol, db_qty in db_positions.items():
        asset = symbol.replace("USDT", "")
        binance_qty = raw_balances.get(asset, 0.0)
        if binance_qty == 0 and db_qty > 0:
            price = prices.get(symbol, 0)
            value_usdt = round(db_qty * price, 4) if price > 0 else None
            if value_usdt is None or value_usdt > min_value_usdt:
                issues.append({"type": "orphan_in_db", "symbol": symbol,
                               "binance_qty": 0.0, "db_qty": db_qty, "value_usdt": value_usdt})

    total_value = sum((i.get("value_usdt") or 0) for i in issues)
    return {"issues_count": len(issues), "total_value_usdt": round(total_value, 4),
            "min_value_usdt_filter": min_value_usdt, "issues": issues}


@app.get("/api/reconcile")
def api_reconcile():
    """Reconcile bot positions against Binance balances (TopBar Reconcile button).

    Returns {ghosts: [...], mismatches: [...]} — reuses the orphan-check logic
    after forcing a fresh account fetch so results reflect current balances."""
    global _acct_cache_ts, _acct_fail_ts
    with _acct_cache_lock:
        _acct_cache_ts = 0.0   # bust the cache — force a fresh account fetch
        _acct_fail_ts = 0.0
    _get_cached_account()
    result = api_orphan_check()
    if result.get("error"):
        return {"error": result["error"], "ghosts": [], "mismatches": []}
    issues     = result.get("issues", [])
    ghosts     = [i for i in issues if i.get("type") in ("orphan_in_db", "orphan_on_binance")]
    mismatches = [i for i in issues if i.get("type") == "qty_mismatch"]
    if ghosts or mismatches:
        try:
            database.log_activity(
                f"Reconcile: {len(ghosts)} ghost(s), {len(mismatches)} qty mismatch(es): "
                + ", ".join(i.get("symbol", "?") for i in ghosts + mismatches), "warn")
        except Exception:
            pass
    return {"ok": True, "ghosts": ghosts, "mismatches": mismatches,
            "message": f"{len(ghosts)} ghost(s), {len(mismatches)} mismatch(es)"}


@app.get("/api/diagnostics/fill_quality")
def get_fill_quality(hours: int = 24):
    import sqlite3 as _sq
    from datetime import timedelta
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    try:
        conn = _sq.connect(database.DB_PATH)
        conn.row_factory = _sq.Row
        rows = conn.execute(
            "SELECT coin, buy_slippage_pct, sell_slippage_pct, intended_buy_price, entry_price, intended_sell_price, exit_price, quantity, timestamp_sell FROM trades WHERE timestamp_sell >= ? AND exit_price IS NOT NULL",
            (cutoff,)
        ).fetchall()
        conn.close()
        buy_slips  = [r["buy_slippage_pct"]  for r in rows if r["buy_slippage_pct"]  is not None]
        sell_slips = [r["sell_slippage_pct"] for r in rows if r["sell_slippage_pct"] is not None]

        def percentiles(vals):
            if not vals:
                return {"p50": None, "p90": None, "p99": None, "avg": None}
            s = sorted(vals)
            n = len(s)
            return {
                "p50": round(s[int(n * 0.5)], 4),
                "p90": round(s[int(n * 0.9)], 4),
                "p99": round(s[min(int(n * 0.99), n - 1)], 4),
                "avg": round(sum(s) / n, 4),
            }

        worst = sorted(
            [dict(r) for r in rows if r["sell_slippage_pct"] is not None],
            key=lambda x: abs(x["sell_slippage_pct"] or 0), reverse=True
        )[:10]
        return {
            "window_hours": hours,
            "trade_count": len(rows),
            "buy_slippage": percentiles(buy_slips),
            "sell_slippage": percentiles(sell_slips),
            "worst_fills": worst,
        }
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/diagnostics/phantoms")
def get_phantoms():
    import sqlite3 as _sq
    try:
        conn = _sq.connect(database.DB_PATH)
        conn.row_factory = _sq.Row
        rows = conn.execute(
            "SELECT id, timestamp, symbol, db_qty, binance_qty, resolved FROM phantom_alerts WHERE resolved=0 ORDER BY id DESC LIMIT 50"
        ).fetchall()
        conn.close()
        return {"phantoms": [dict(r) for r in rows], "count": len(rows)}
    except Exception as e:
        return {"error": str(e), "phantoms": [], "count": 0}


@app.post("/api/diagnostics/phantoms/{alert_id}/resolve")
def resolve_phantom(alert_id: int):
    import sqlite3 as _sq
    try:
        conn = _sq.connect(database.DB_PATH)
        conn.execute("UPDATE phantom_alerts SET resolved=1 WHERE id=?", (alert_id,))
        conn.commit()
        conn.close()
        return {"ok": True}
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/diagnostics/thread_health")
def get_thread_health():
    try:
        import thread_health as _th
        return _th.get_health()
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/diagnostics/buy_rejections")
def get_buy_rejections(hours: int = 1):
    from datetime import timedelta
    import sqlite3 as _sq
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    try:
        conn = _sq.connect(database.DB_PATH)
        conn.row_factory = _sq.Row
        total = conn.execute("SELECT COUNT(*) as c FROM buy_rejections WHERE timestamp >= ?", (cutoff,)).fetchone()["c"]
        by_reason_rows = conn.execute(
            "SELECT reason, COUNT(*) as cnt FROM buy_rejections WHERE timestamp >= ? GROUP BY reason ORDER BY cnt DESC LIMIT 20",
            (cutoff,)
        ).fetchall()
        by_coin_rows = conn.execute(
            "SELECT coin, COUNT(*) as rejections FROM buy_rejections WHERE timestamp >= ? GROUP BY coin ORDER BY rejections DESC LIMIT 20",
            (cutoff,)
        ).fetchall()
        recent = conn.execute(
            "SELECT timestamp, coin, reason, detail, score, rsi_value FROM buy_rejections WHERE timestamp >= ? ORDER BY id DESC LIMIT 50",
            (cutoff,)
        ).fetchall()
        conn.close()
        by_reason = [{"reason": r["reason"], "count": r["cnt"], "pct": round(r["cnt"]/total*100, 1) if total else 0} for r in by_reason_rows]
        by_coin = [{"coin": r["coin"], "rejections": r["rejections"]} for r in by_coin_rows]
        return {
            "window_hours": hours,
            "total_rejections": total,
            "by_reason": by_reason,
            "by_coin": by_coin,
            "recent": [dict(r) for r in recent],
        }
    except Exception as e:
        return {"error": str(e), "total_rejections": 0, "by_reason": [], "by_coin": [], "recent": []}


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


def _resolved_exit_cfg():
    """RESOLVED exits config from the engine (defaults + strategy.json merged).
    Defensive: any import/eval failure returns None instead of breaking
    /api/settings."""
    try:
        from trade_engine import _exit_cfg
        cfg = _exit_cfg()
        return dict(cfg) if isinstance(cfg, dict) else None
    except Exception:
        return None


def _resolved_entries_cfg():
    """RESOLVED entries config from the engine (defaults + strategy.json
    merged) — §3.1/§3.4/§3.5/§3.6 keys. None if the engine is unavailable."""
    try:
        from trade_engine import _entries_cfg
        cfg = _entries_cfg()
        return dict(cfg) if isinstance(cfg, dict) else None
    except Exception:
        return None


def _resolved_regime_cfg():
    """RESOLVED §3.3 regime config: neutral_size_mult (engine default 0.5)
    plus the fixed refresh cadence (the engine's 60 s regime cache TTL —
    informational/read-only, not accepted on POST)."""
    try:
        from trade_engine import _neutral_size_mult
        mult = float(_neutral_size_mult())
    except Exception:
        mult = None
    return {"neutral_size_mult": mult, "refresh_sec": 60}


# ── Phase 4 §4.1-§4.3 — risk breakers status / resume / settings blocks ──────

# Engine defaults (mirrored here so GET /api/settings can resolve the blocks
# even before the engine agent's _sizing_cfg/_risk_cfg helpers exist).
_SIZING_DEFAULTS = {
    "max_positions":     9,
    "min_position_usdt": 10,
}
_RISK_DEFAULTS = {
    "daily_loss_stop_pct":          2.0,
    "flatten_on_stop":              False,
    "max_consecutive_losses":       4,
    "max_avg_slippage_bps":         15,
    "max_new_entries_when_btc_red": 2,
}


def _resolved_block_cfg(block_name: str, defaults: dict, engine_fn_names: tuple):
    """RESOLVED config block: engine resolver when available, otherwise
    defaults merged with any strategy.json block of the same name."""
    for fn_name in engine_fn_names:
        try:
            import trade_engine as _te
            fn = getattr(_te, fn_name, None)
            if callable(fn):
                cfg = fn()
                if isinstance(cfg, dict):
                    return dict(cfg)
        except Exception:
            pass
    try:
        s = _load_strategy()
        blk = s.get(block_name) if isinstance(s.get(block_name), dict) else {}
        return {**defaults, **blk}
    except Exception:
        return dict(defaults)


def _resolved_sizing_cfg():
    return _resolved_block_cfg("sizing", _SIZING_DEFAULTS, ("_sizing_cfg", "sizing_cfg"))


def _resolved_risk_cfg():
    return _resolved_block_cfg("risk", _RISK_DEFAULTS, ("_risk_cfg", "risk_cfg"))


# Phase 4 — validation specs for the POST "sizing" and "risk" blocks
# (same (type, min, max) tuple contract as _ENTRIES_VALIDATION).
_SIZING_VALIDATION = {
    "max_positions":     ("int",   1, 50),
    "min_position_usdt": ("float", 1, 1000),
}
_RISK_VALIDATION = {
    "daily_loss_stop_pct":          ("float", 0.1, 50),
    "flatten_on_stop":              ("bool",  None, None),
    "max_consecutive_losses":       ("int",   1,   50),
    "max_avg_slippage_bps":         ("float", 1,   500),
    "max_new_entries_when_btc_red": ("int",   0,   50),
}

# Sections get_risk_status() is expected to return; each is surfaced as
# {"available": false} when the engine (or that section) is absent.
_RISK_STATUS_SECTIONS = ("daily", "consecutive", "slippage_vetoes",
                         "correlation", "slots", "bnb")


def _risk_status_payload() -> dict:
    """Assemble the /api/risk/status body. NEVER raises — every failure mode
    degrades to {"available": false} sections so the endpoint cannot 500."""
    out: dict = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "available": False,
    }
    status = None
    try:
        import trade_engine as _te
        _grs = getattr(_te, "get_risk_status", None)
        if callable(_grs):
            status = _grs()
    except Exception:
        status = None
    if not isinstance(status, dict):
        status = {}
    else:
        out["available"] = True
    for section in _RISK_STATUS_SECTIONS:
        val = status.get(section)
        out[section] = val if isinstance(val, dict) else {"available": False}
    return out


def _risk_compact_summary() -> dict:
    """Compact breaker-state summary carried on /api/all so the main poll can
    drive header badges without an extra request. Never raises."""
    out = {
        "daily_stopped":       False,
        "consec_paused":       False,
        "effective_slots":     None,
        "degraded":            False,
        "slippage_veto_count": 0,
    }
    try:
        import trade_engine as _te
        _grs = getattr(_te, "get_risk_status", None)
        status = _grs() if callable(_grs) else None
        if isinstance(status, dict):
            daily = status.get("daily") or {}
            if isinstance(daily, dict):
                out["daily_stopped"] = bool(daily.get("stopped"))
            consec = status.get("consecutive") or {}
            if isinstance(consec, dict):
                pu = consec.get("paused_until")
                try:
                    out["consec_paused"] = bool(pu) and float(pu) > time.time()
                except (TypeError, ValueError):
                    out["consec_paused"] = bool(pu)
            vetoes = status.get("slippage_vetoes")
            if isinstance(vetoes, dict):
                out["slippage_veto_count"] = len(vetoes)
            slots = status.get("slots") or {}
            if isinstance(slots, dict) and slots:
                out["effective_slots"] = slots.get("effective_slots")
                out["degraded"]        = bool(slots.get("degraded"))
        if out["effective_slots"] is None:
            _es = getattr(_te, "effective_slots", None)
            if callable(_es):
                slots = _es()
                if isinstance(slots, dict):
                    out["effective_slots"] = slots.get("effective_slots")
                    out["degraded"]        = bool(slots.get("degraded"))
    except Exception:
        pass
    return out


@app.get("/api/risk/status")
def api_risk_status():
    """Phase 4 §4.1 — full risk-breaker status. Never 500s: absent engine
    APIs degrade to {"available": false} sections."""
    return _risk_status_payload()


@app.post("/api/risk/resume")
def api_risk_resume(body: dict = Body(default={})):
    """Phase 4 §4.2 — clear an active daily loss stop. Requires an explicit
    {"confirm": true} body; returns the refreshed risk status."""
    if not isinstance(body, dict) or body.get("confirm") is not True:
        return JSONResponse(
            status_code=400,
            content={"ok": False,
                     "error": 'confirmation required — POST {"confirm": true}'})
    resumed = False
    err = None
    try:
        import trade_engine as _te
        _fn = getattr(_te, "resume_daily_stop", None)
        if callable(_fn):
            resumed = bool(_fn())
        else:
            err = "engine does not support resume_daily_stop yet"
    except Exception as exc:
        err = str(exc)
    if err:
        try:
            database.log_activity(f"Daily-stop resume failed: {err}", "error")
        except Exception:
            pass
        return {"ok": False, "error": err, "status": _risk_status_payload()}
    try:
        database.log_activity(
            "Daily loss stop manually resumed via API"
            + ("" if resumed else " (engine reported no active stop)"), "warn")
    except Exception:
        pass
    return {"ok": True, "resumed": resumed, "status": _risk_status_payload()}


# Phase 3 — validation spec for the POST "entries" block.
# Each entry: (type, min, max). type 'bool' ignores min/max; none are nullable.
_ENTRIES_VALIDATION = {
    "maker_first":            ("bool",  None, None),
    "chase_seconds":          ("float", 1.0,  30.0),
    "max_reposts":            ("int",   0,    10),
    "taker_fallback":         ("bool",  None, None),
    "cooldown_after_sl_min":  ("float", 0.0,  240.0),
    "falling_knife_atr_mult": ("float", 0.1,  5.0),
    "eval_heartbeat_sec":     ("float", 5.0,  120.0),
    "tick_entries":           ("bool",  None, None),
    "prefer_fee_promo_pairs": ("bool",  None, None),
}

# Phase 3 — validation spec for the POST "regime" block. "refresh_sec" is
# exposed on GET but read-only (fixed engine cache TTL) — it is silently
# dropped on POST so a GET→edit→POST round-trip never errors.
_REGIME_VALIDATION = {
    "neutral_size_mult": ("float", 0.0, 1.0),
}
_REGIME_READONLY_KEYS = {"refresh_sec"}


def _validate_typed_block(block: dict, spec: dict, block_name: str,
                          ignore_keys: frozenset = frozenset()):
    """Generic field-level validator for a POSTed settings sub-dict (same
    contract as _validate_exits_patch): returns (validated, errors); only keys
    that pass land in `validated`; keys in `ignore_keys` are dropped silently."""
    validated: dict = {}
    errors: dict = {}
    if not isinstance(block, dict):
        return {}, {block_name: "must be an object"}
    for key, val in block.items():
        if key in ignore_keys:
            continue
        spec_entry = spec.get(key)
        if spec_entry is None:
            errors[key] = f"unknown {block_name} key"
            continue
        typ, lo, hi = spec_entry
        if val is None:
            errors[key] = "must not be null"
            continue
        if typ == "bool":
            if isinstance(val, bool):
                validated[key] = val
            else:
                errors[key] = "must be a boolean"
            continue
        if isinstance(val, bool) or not isinstance(val, (int, float)):
            errors[key] = "must be a number"
            continue
        fv = float(val)
        if not (lo <= fv <= hi):
            errors[key] = f"must be between {lo} and {hi}"
            continue
        validated[key] = int(fv) if typ == "int" else fv
    return validated, errors


# Phase 2 §2.6 — validation spec for the POST "exits" block.
# Each entry: (type, min, max, nullable). type 'bool' ignores min/max.
_EXITS_VALIDATION = {
    "k_sl":                      ("float", 0.0,   10.0,    False),
    "sl_min_pct":                ("float", 0.1,   10.0,    False),
    "sl_max_pct":                ("float", 0.1,   10.0,    False),
    "hard_sl_pct":               ("float", 0.5,   20.0,    False),
    "rr_ratio":                  ("float", 0.5,   10.0,    True),
    "tp_buffer_pct":             ("float", 0.0,   1.0,     False),
    "min_profit_usdt":           ("float", 0.001, 1.0,     False),
    "breakeven_at_r":            ("float", 0.1,   5.0,     True),
    "k_trail":                   ("float", 0.0,   5.0,     False),
    "smart_hold_score_gate":     ("bool",  None,  None,    False),
    "maker_tp":                  ("bool",  None,  None,    False),
    "oco_enabled":               ("bool",  None,  None,    False),
    "maker_tp_timeout_ms":       ("float", 100.0, 30000.0, False),
    "oco_stop_limit_buffer_pct": ("float", 0.0,   5.0,     False),
    "oco_skip_rescue_sec":       ("float", 1.0,   30.0,    False),
    "sl_confirm_ticks":          ("int",   1,     10,      False),
    "min_hold_sec":              ("float", 0.0,   120.0,   False),
}


def _validate_exits_patch(exits: dict, current: dict):
    """Validate a POSTed exits dict. Returns (validated, errors) — errors is a
    {field: message} map; validated only holds keys that passed. The
    sl_min<=sl_max cross-check runs against the MERGED (current+new) values so
    updating one bound alone stays consistent."""
    validated: dict = {}
    errors: dict = {}
    if not isinstance(exits, dict):
        return {}, {"exits": "must be an object"}
    for key, val in exits.items():
        spec = _EXITS_VALIDATION.get(key)
        if spec is None:
            errors[key] = "unknown exits key"
            continue
        typ, lo, hi, nullable = spec
        if val is None:
            if nullable:
                validated[key] = None
            else:
                errors[key] = "must not be null"
            continue
        if typ == "bool":
            if isinstance(val, bool):
                validated[key] = val
            else:
                errors[key] = "must be a boolean"
            continue
        if isinstance(val, bool) or not isinstance(val, (int, float)):
            errors[key] = "must be a number"
            continue
        fv = float(val)
        if not (lo <= fv <= hi):
            errors[key] = f"must be between {lo} and {hi}"
            continue
        validated[key] = int(fv) if typ == "int" else fv
    # Cross-field: sl_min_pct <= sl_max_pct on the merged view.
    if not errors:
        merged = {**(current if isinstance(current, dict) else {}), **validated}
        try:
            _mn = float(merged.get("sl_min_pct", 0.5))
            _mx = float(merged.get("sl_max_pct", 2.5))
            if _mn > _mx:
                errors["sl_min_pct"] = (
                    f"sl_min_pct ({_mn}) must be <= sl_max_pct ({_mx})")
        except (TypeError, ValueError):
            pass
    return validated, errors


@app.get("/api/settings")
def api_get_settings():
    """Return current bot risk/strategy settings."""
    s = _load_strategy()
    return {
        "ok":                  True,
        # Phase 2 §2.6 — RESOLVED exits config (engine defaults merged with
        # any strategy.json "exits" block); None if the engine is unavailable.
        "exits":               _resolved_exit_cfg(),
        # Phase 3 — RESOLVED entries (§3.1/§3.4/§3.5/§3.6) and regime (§3.3)
        # configs; None when the engine is unavailable.
        "entries":             _resolved_entries_cfg(),
        "regime":              _resolved_regime_cfg(),
        # Phase 4 — RESOLVED sizing (§4/A3) and risk-breaker (§4.1-§4.3)
        # configs (engine resolvers when present, defaults+strategy.json else).
        "sizing":              _resolved_sizing_cfg(),
        "risk":                _resolved_risk_cfg(),
        # Defaults below MUST mirror the engine's actual fallbacks:
        # trade_engine._refresh_risk_params (sl_on=True, sl_pct=0.4, tp_pct=0.1,
        # smart_hold=False, trailing=0.5) and _check_buys_from_cache (max_positions=10).
        "stop_loss_enabled":   s.get("stop_loss_enabled",   True),
        "stop_loss_pct":       s.get("stop_loss_pct",       0.4),
        "take_profit_enabled": s.get("take_profit_enabled", True),
        "take_profit_pct":     s.get("take_profit_pct",     0.1),
        "smart_hold_enabled":  s.get("smart_hold_enabled",  False),
        "trailing_stop_pct":   s.get("trailing_stop_pct",   0.5),
        "reinvest_profits":    s.get("reinvest_profits",    False),
        "max_positions":       s.get("max_positions",       10),
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
    # F4 — budget fields (validated against the min-notional floor before write).
    budget_mode:         Optional[str]   = None
    budget_fixed_usdt:   Optional[float] = None
    budget_per_coin:     Optional[dict]  = None
    # Phase 2 §2.6 — optional exits block (validated by _validate_exits_patch;
    # a plain dict so unknown keys reach validation and get field-level errors
    # instead of being silently dropped by pydantic).
    exits:               Optional[dict]  = None
    # Phase 3 — optional entries (§3.5/§3.6 + §3.1/§3.4 knobs) and regime
    # (§3.3) blocks, validated by _validate_typed_block (same plain-dict
    # rationale as exits).
    entries:             Optional[dict]  = None
    regime:              Optional[dict]  = None
    # Phase 4 — optional sizing and risk blocks, validated by
    # _validate_typed_block (same plain-dict rationale as exits/entries).
    sizing:              Optional[dict]  = None
    risk:                Optional[dict]  = None


class _SignalEngineConfig(BaseModel):
    enabled:           bool
    mandatory_signals: list[str]
    scored_signals:    list[str]
    veto_signals:      list[str]
    min_scored:        int

class _SignalThresholdsUpdate(BaseModel):
    rsi_buy_threshold:             Optional[float] = None
    near_low_pct:                  Optional[float] = None
    reversal_volume_multiplier:    Optional[float] = None
    spread_max_pct:                Optional[float] = None
    allowed_trading_hours_utc:     Optional[str]   = None
    stoch_rsi_threshold:           Optional[float] = None

class SignalEngineUpdate(BaseModel):
    signal_engine:     Optional[_SignalEngineConfig]     = None
    signal_thresholds: Optional[_SignalThresholdsUpdate] = None
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
        # F4 — budget fields, floor-validated BEFORE the write (reject, never
        # accept-then-spam "$5.50 < $10 min notional" forever at execution).
        if req.budget_mode         is not None: patch["budget_mode"]        = req.budget_mode
        if req.budget_fixed_usdt   is not None: patch["budget_fixed_usdt"]  = float(req.budget_fixed_usdt)
        if req.budget_per_coin     is not None: patch["budget_per_coin"]    = req.budget_per_coin
        if any(k in patch for k in ("budget_mode", "budget_fixed_usdt", "budget_per_coin")):
            _merged_budget = {**_load_strategy(), **patch}
            _berr, _bwarn = _validate_budget_floor(_merged_budget)
            # Also flag an explicitly-set fixed size below the floor even when the
            # active mode isn't 'fixed' — storing a sub-notional value is the F4
            # footgun (accepted, then fails forever the moment fixed mode is used).
            if "budget_fixed_usdt" in patch and "budget_fixed_usdt" not in _berr:
                _fl = _budget_floor(_merged_budget)
                if float(patch["budget_fixed_usdt"]) < _fl:
                    _berr["budget_fixed_usdt"] = (
                        f"per-trade budget {float(patch['budget_fixed_usdt']):.2f} USDT is "
                        f"below the minimum {_fl:.2f} USDT (Binance min notional "
                        f"{MIN_NOTIONAL_FLOOR:.0f})")
            if _berr:
                return {"ok": False, "error": "invalid budget settings", "errors": _berr}
        # ── Phase 2 §2.6 — exits block (validated; field-level errors, no 500) ──
        if req.exits is not None:
            _s_cur = _load_strategy()
            _cur_exits = _s_cur.get("exits") if isinstance(_s_cur.get("exits"), dict) else {}
            _validated, _errors = _validate_exits_patch(req.exits, _cur_exits)
            if _errors:
                # Reject the whole request (nothing written) so a partial
                # legacy write can't ride along with a bad exits block.
                return {"ok": False, "error": "invalid exits settings", "errors": _errors}
            if _validated:
                patch["exits"] = {**_cur_exits, **_validated}
        # ── Phase 3 — entries block (validated; field-level errors, no 500) ──
        if req.entries is not None:
            _validated, _errors = _validate_typed_block(
                req.entries, _ENTRIES_VALIDATION, "entries")
            if _errors:
                return {"ok": False, "error": "invalid entries settings", "errors": _errors}
            if _validated:
                _s_cur = _load_strategy()
                _cur = _s_cur.get("entries") if isinstance(_s_cur.get("entries"), dict) else {}
                patch["entries"] = {**_cur, **_validated}
        # ── Phase 4 — sizing block (validated; field-level errors, no 500) ──
        if req.sizing is not None:
            _validated, _errors = _validate_typed_block(
                req.sizing, _SIZING_VALIDATION, "sizing")
            if _errors:
                return {"ok": False, "error": "invalid sizing settings", "errors": _errors}
            if _validated:
                _s_cur = _load_strategy()
                _cur = _s_cur.get("sizing") if isinstance(_s_cur.get("sizing"), dict) else {}
                patch["sizing"] = {**_cur, **_validated}
        # ── Phase 4 — risk block (validated; field-level errors, no 500) ──
        if req.risk is not None:
            _validated, _errors = _validate_typed_block(
                req.risk, _RISK_VALIDATION, "risk")
            if _errors:
                return {"ok": False, "error": "invalid risk settings", "errors": _errors}
            if _validated:
                _s_cur = _load_strategy()
                _cur = _s_cur.get("risk") if isinstance(_s_cur.get("risk"), dict) else {}
                patch["risk"] = {**_cur, **_validated}
        # ── Phase 3 — regime block (refresh_sec is read-only → dropped) ──
        if req.regime is not None:
            _validated, _errors = _validate_typed_block(
                req.regime, _REGIME_VALIDATION, "regime",
                ignore_keys=frozenset(_REGIME_READONLY_KEYS))
            if _errors:
                return {"ok": False, "error": "invalid regime settings", "errors": _errors}
            if _validated:
                _s_cur = _load_strategy()
                _cur = _s_cur.get("regime") if isinstance(_s_cur.get("regime"), dict) else {}
                patch["regime"] = {**_cur, **_validated}
        if not patch:
            return {"ok": False, "error": "No valid settings provided"}
        # F4 — reject a sub-minimum fixed/per-coin per-trade budget (422) before
        # writing, so an unfillable size can never be accepted then spam at exec.
        _merged_set = {**_load_strategy(), **patch}
        _berr, _bwarn = _validate_budget_floor(_merged_set)
        if _berr:
            return JSONResponse(status_code=422, content={"errors": _berr})
        _write_strategy_patch(patch)
        database.log_activity(
            "Settings updated: " + ", ".join(f"{k}={v}" for k, v in patch.items() if k != "strategy_notes"),
            "info"
        )
        _resp = {"ok": True, **patch}
        if _bwarn:
            _resp["warnings"] = _bwarn
        return _resp
    except Exception as e:
        database.log_activity(f"Settings save error: {e}", "error")
        return Response(
            content=json.dumps({"ok": False, "error": str(e)}),
            status_code=500, media_type="application/json"
        )


@app.get("/api/ping")
def api_ping():
    from connection import get_mode, is_using_paper_fallback
    return {
        "ok":   True,
        "ts":   datetime.now(timezone.utc).isoformat(),
        "mode": get_mode(),
        "using_paper_fallback": is_using_paper_fallback(),
    }


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
        # Phase 4 §4.4 — compact breaker-state summary so the main poll can
        # drive the "BUYS PAUSED" header badge without an extra request.
        "risk":          _risk_compact_summary(),
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
        "env":             {"MODE": get_mode()},  # frontend reads dbg.env.MODE
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


# ── Part E — zero-restart effect-test probes ─────────────────────────────────
# Tiny read-only endpoints that expose CONFIG-DERIVED engine values so the
# effect-test harness (trading-bot/effect_tests.py) can prove a settings edit
# hot-applied without waiting for a real trade. They call the SAME resolvers
# the live engine uses (_exit_cfg via backtest.exit_levels parity,
# get_budget_for_coin, signal_registry.evaluate_buy_decision).

@app.get("/api/debug/exit-geometry")
def api_debug_exit_geometry(symbol: str = "BTCUSDT", entry: float = 100.0,
                            budget: float = 50.0,
                            atr_pct: Optional[float] = None):
    """Exit geometry (sl/tp/hard_sl/bep + distances) for a HYPOTHETICAL entry
    at `entry` with `budget` USDT deployed, resolved from the CURRENT
    strategy.json through backtest.exit_levels — the same math
    _apply_entry_exit_geometry freezes onto real positions at entry time."""
    try:
        import fees as _fees
        from backtest import exit_levels as _exit_levels
        entry = float(entry)
        budget = float(budget)
        if entry <= 0 or budget <= 0:
            return JSONResponse(status_code=422,
                                content={"error": "entry and budget must be > 0"})
        fm = _fees.get_fee_model(symbol or None)
        taker = fm.taker()
        qty = budget / entry
        cost = budget + budget * taker   # deployed capital incl. entry fee
        lv = _exit_levels(entry, qty, cost, _load_strategy(), fm,
                          atr_pct=float(atr_pct) if atr_pct is not None else None)
        cfg = lv.pop("cfg", {}) or {}
        out = {
            "symbol": symbol, "entry": entry, "budget": budget,
            "qty": qty, "taker_fee": taker,
            **{k: lv[k] for k in ("tp", "sl", "hard_sl", "bep",
                                  "sl_distance_pct", "tp_distance_pct",
                                  "atr_pct", "atr_unavailable")},
            "cfg": {k: cfg.get(k) for k in (
                "k_sl", "sl_min_pct", "sl_max_pct", "hard_sl_pct", "rr_ratio",
                "tp_buffer_pct", "min_profit_usdt", "breakeven_at_r", "k_trail",
                "sl_confirm_ticks", "min_hold_sec", "legacy_mode")},
        }
        return out
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}


@app.get("/api/debug/budget")
def api_debug_budget(symbol: str = "BTCUSDT", free: float = 1000.0):
    """Per-trade budget the engine would allocate for `symbol` given `free`
    USDT — calls trade_engine.get_budget_for_coin (the REAL sizing path:
    budget_mode / allocation cap / reinvest / neutral-regime multiplier) plus
    the current effective_slots + sizing config."""
    try:
        import trade_engine as _te
        budget = _te.get_budget_for_coin(symbol, float(free))
        return {
            "symbol":       symbol,
            "free_usdt":    float(free),
            "budget_usdt":  budget,
            "sizing":       _te._sizing_cfg(),
            "slots":        _te.effective_slots(),
            "budget_mode":  _load_strategy().get("budget_mode", config.BUDGET_MODE),
            "btc_regime":   (_te.get_btc_regime() if hasattr(_te, "get_btc_regime") else None),
        }
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}


@app.get("/api/debug/evaluate-gates")
def api_debug_evaluate_gates(symbol: str = "BTCUSDT",
                             rsi: float = 30.0,
                             stoch_rsi: float = 20.0):
    """Run signal_registry.evaluate_buy_decision on a SYNTHETIC all-bullish
    snapshot under the CURRENT strategy.json — proves role/threshold edits
    (PUT /api/signals/registry) change the engine's gate outcome with zero
    restart. Synthetic data: every boolean signal true, price mid-range;
    rsi/stoch_rsi injectable to exercise M1/M2 thresholds."""
    try:
        import signal_registry as _sr
        strategy = _load_strategy()
        signal_data = {
            "trend": True, "rsi": True, "macd": True, "volume": True,
            "obv": True, "atr": True,
            "rsi_value": float(rsi), "stoch_rsi_value": float(stoch_rsi),
            "low_24h": 99.0, "current_price": 100.0,
            "klines_1m": [], "klines_5m": [],
            "bb_position_5m": "inside", "btc_regime": "risk_on",
            "ema50_15m_slope": 0.1,
        }
        # Synthetic evaluation must not poison the per-symbol result caches
        # the live scanner uses (R1/M4 cache by symbol for 10-20 s).
        probe_sym = f"__PROBE__{symbol}"
        dec = _sr.evaluate_buy_decision(probe_sym, signal_data, strategy)
        return {
            "symbol": symbol,
            "allowed": bool(dec.get("allowed")),
            "reason": dec.get("reason"),
            "score": dec.get("score"),
            "fired_signals": sorted(dec.get("fired_signals") or []),
            "veto_results": dec.get("veto_results"),
            "mandatory_results": dec.get("mandatory_results"),
            "min_scored_effective": int(
                (strategy.get("signal_engine") or {}).get(
                    "min_scored", _sr.DEFAULT_SIGNAL_ENGINE["min_scored"])
                if (strategy.get("signal_engine") or {}).get("enabled", False)
                else strategy.get("min_signals", 4)),
        }
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}


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


@app.get("/api/signal-engine/config")
def api_signal_engine_config_get():
    """Return the ACTUAL persisted signal engine configuration plus registry.

    Never dress up code defaults as saved config: DEFAULT_SIGNAL_ENGINE has
    enabled=True while trade_engine's gate defaults enabled=False when the
    strategy.json block is absent — echoing the defaults as "config" made the
    UI show an engine that was not actually running. "config" is now exactly
    what strategy.json holds ({} if never saved), "active" is computed the
    SAME way trade_engine gates buys, and the defaults are returned under a
    clearly-labelled separate key for UI prefill.
    """
    try:
        import signal_registry as _sr
        strategy   = _load_strategy()
        engine_cfg = strategy.get("signal_engine", {})
        # Mirror trade_engine's gate exactly: absent block/key => False =>
        # the LEGACY 6-signal path is what actually runs.
        engine_active = bool(strategy.get("signal_engine", {}).get("enabled", False))
        registry   = [
            {"id": sid, "category": sd.category, "description": sd.description}
            for sid, sd in _sr.SIGNAL_REGISTRY.items()
        ]
        return {
            "registry":   registry,
            "config":     engine_cfg,               # persisted state only — {} if never saved
            "active":     engine_active,            # what trade_engine actually enforces
            "persisted":  bool(engine_cfg),         # False => block absent from strategy.json
            "defaults":   _sr.DEFAULT_SIGNAL_ENGINE,  # code defaults, NOT saved config
            "decision_path": "signal_engine" if engine_active else "legacy_6_signal",
            "thresholds": strategy.get("signal_thresholds", _sr.DEFAULT_SIGNAL_THRESHOLDS),
        }
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.post("/api/signal-engine/config")
def api_signal_engine_config_post(update: SignalEngineUpdate):
    """Validate and save signal engine configuration to strategy.json."""
    try:
        import signal_registry as _sr
        valid_ids = set(_sr.SIGNAL_REGISTRY.keys())

        strategy_path = config.STRATEGY_FILE
        with open(strategy_path) as f:
            strategy = json.load(f)

        if update.signal_engine is not None:
            cfg = update.signal_engine.dict()
            # Signal IDs must exist in registry
            for role in ("mandatory_signals", "scored_signals", "veto_signals"):
                for sid in cfg.get(role, []):
                    if sid not in valid_ids:
                        return JSONResponse(status_code=400,
                                            content={"error": f"Unknown signal: {sid}", "role": role})
            # min_scored must not exceed scored count
            if cfg["min_scored"] > len(cfg["scored_signals"]):
                return JSONResponse(status_code=400,
                                    content={"error": "min_scored cannot exceed scored_signals count"})
            # No signal in multiple roles
            used = cfg["mandatory_signals"] + cfg["scored_signals"] + cfg["veto_signals"]
            if len(used) != len(set(used)):
                return JSONResponse(status_code=400,
                                    content={"error": "A signal cannot be in multiple roles"})
            strategy["signal_engine"] = cfg

        if update.signal_thresholds is not None:
            new_t = update.signal_thresholds.dict(exclude_none=True)
            if "rsi_buy_threshold" in new_t and not (10 <= new_t["rsi_buy_threshold"] <= 90):
                return JSONResponse(status_code=400, content={"error": "rsi_buy_threshold must be 10–90"})
            if "near_low_pct" in new_t and not (0.1 <= new_t["near_low_pct"] <= 20):
                return JSONResponse(status_code=400, content={"error": "near_low_pct must be 0.1–20"})
            if "spread_max_pct" in new_t and not (0.01 <= new_t["spread_max_pct"] <= 5):
                return JSONResponse(status_code=400, content={"error": "spread_max_pct must be 0.01–5"})
            existing_t = strategy.get("signal_thresholds", {})
            existing_t.update(new_t)
            strategy["signal_thresholds"] = existing_t

        tmp = strategy_path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(strategy, f, indent=2)
        os.replace(tmp, strategy_path)

        enabled = strategy.get("signal_engine", {}).get("enabled", False)
        database.log_activity(
            f"Signal engine config saved via UI: enabled={enabled}", "info"
        )
        return {
            "ok":         True,
            "config":     strategy.get("signal_engine"),
            "thresholds": strategy.get("signal_thresholds"),
        }
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e)})


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


@app.get("/api/update/check")
def api_update_check():
    """Server-side update check via pure git — the reliable signal.

    Compares the local checkout against origin/main using git itself (the same
    channel /api/update already uses to pull), so it works even when neither
    the browser nor the bot can reach raw.githubusercontent.com. Returns
    behind_count>0 when origin/main has commits the running code doesn't.
    """
    import subprocess, pathlib, shutil as _sh
    app_dir = pathlib.Path(__file__).parent.parent
    env = dict(os.environ)
    env["PATH"] = env.get("PATH", "") + ":/usr/local/bin:/usr/bin:/bin"
    git = _sh.which("git", path=env["PATH"]) or "git"
    def _git(*args, timeout=30):
        return subprocess.run([git, "-c", f"safe.directory={app_dir}", *args],
                              cwd=str(app_dir), env=env, capture_output=True,
                              text=True, timeout=timeout)
    try:
        _git("fetch", "origin", "main", timeout=45)
        behind = _git("rev-list", "--count", "HEAD..origin/main")
        local  = _git("rev-parse", "--short", "HEAD")
        remote = _git("rev-parse", "--short", "origin/main")
        subj   = _git("log", "-1", "--format=%s", "origin/main")
        n = int((behind.stdout or "0").strip() or "0")
        return {
            "ok": True,
            "update_available": n > 0,
            "behind_count": n,
            "local_commit": (local.stdout or "").strip(),
            "remote_commit": (remote.stdout or "").strip(),
            "latest_subject": (subj.stdout or "").strip(),
            "running_version": _read_frontend_version().get("version"),
        }
    except Exception as e:
        return {"ok": False, "error": f"{type(e).__name__}: {e}",
                "update_available": False}


@app.post("/api/update")
def api_update():
    """Pull latest code from GitHub, rebuild the frontend, and restart the bot."""
    import pathlib, subprocess, threading, time as _t

    def _do_update():
        _t.sleep(0.6)  # let HTTP response reach the client first
        # Pause trading so no new orders start mid-build/mid-restart.
        # resume_after_restart lets the deferred init restore the bot to its
        # pre-update running state — updates must not leave the bot stopped.
        was_active = bool(_load_strategy().get("trading_active", False))
        try:
            _write_strategy_patch({"trading_active": False,
                                   "pause_reason": "Updating bot",
                                   "resume_after_restart": was_active})
        except Exception:
            pass
        app_dir = pathlib.Path(__file__).parent.parent
        # systemd units often set PATH to just the venv (Environment=PATH=
        # /opt/tradebot/venv/bin), so bare "git"/"npm" are NOT findable from
        # this process — the updater silently failed on FileNotFoundError.
        # Build a PATH that includes the standard system dirs, and resolve
        # absolute binary paths via shutil.which as a belt-and-suspenders.
        import shutil as _sh
        _env = dict(os.environ)
        _env["PATH"] = _env.get("PATH", "") + ":/usr/local/bin:/usr/bin:/bin"
        _git = _sh.which("git", path=_env["PATH"]) or "git"
        try:
            # -c safe.directory covers root-owned checkouts ("dubious
            # ownership") when the service user differs from the repo owner.
            subprocess.run([_git, "-c", f"safe.directory={app_dir}",
                            "fetch", "origin", "main"],
                           cwd=str(app_dir), check=True, timeout=60,
                           env=_env, capture_output=True, text=True)
            subprocess.run([_git, "-c", f"safe.directory={app_dir}",
                            "reset", "--hard", "origin/main"],
                           cwd=str(app_dir), check=True, timeout=60,
                           env=_env, capture_output=True, text=True)
            print("[Update] Code updated to origin/main", flush=True)
        except Exception as exc:
            # Pull failed — nothing changed on disk; un-pause and bail.
            _detail = ""
            if isinstance(exc, subprocess.CalledProcessError):
                _detail = f" | stderr: {(exc.stderr or '')[:300]}"
            msg = f"UPDATE FAILED pulling code: {type(exc).__name__}: {exc}{_detail}"
            print(f"[Update] {msg}", flush=True)
            try:
                database.log_activity(msg, "error")
            except Exception:
                pass
            try:
                _write_strategy_patch({"trading_active": was_active,
                                       "pause_reason": None,
                                       "resume_after_restart": False})
            except Exception:
                pass
            return
        # The frontend build is COMMITTED in dist/ — npm is optional on the VPS.
        # A build failure must never abort the restart (that left the process
        # running old code while the repo had already been updated).
        try:
            _npm = _sh.which("npm", path=_env["PATH"])
            if _npm:
                subprocess.run([_npm, "run", "build"],
                               cwd=str(app_dir), check=True, timeout=300,
                               env=_env, capture_output=True, text=True)
                print("[Update] Rebuild complete", flush=True)
            else:
                print("[Update] npm not found — using committed dist/", flush=True)
        except Exception as exc:
            print(f"[Update] npm build skipped/failed ({exc}) — using committed dist/", flush=True)
        print("[Update] Restarting bot", flush=True)
        # Flush pending DB state, then restart the current Python process in-place
        _flush_db_state()
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


# ══════════════════════════════════════════════════════════════════════════
# WolfBot v0.4 Phase 1 — backtest jobs, expectancy, attribution, kline coverage
# All endpoints degrade to {"error": "..."} — never a 500 — and wire
# defensively into parallel-agent modules (backtest / attribution) via
# lazy import + getattr.
# ══════════════════════════════════════════════════════════════════════════

def _approved_symbols() -> list:
    """Approved USDT symbols from strategy.json (the /api/coins watchlist)."""
    try:
        s = _load_strategy()
        return [
            str(c.get("symbol", "")).upper()
            for c in s.get("approved_coins", [])
            if c.get("approved") and str(c.get("symbol", "")).upper()
        ]
    except Exception:
        return []


def _parse_ymd_utc_ms(s: str) -> int:
    """'YYYY-MM-DD' → epoch-ms at UTC midnight. Raises ValueError on junk."""
    dt = datetime.strptime(str(s).strip(), "%Y-%m-%d").replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


# ── Backtest job registry (single-flight, keep last 5) ──────────────────────

_backtest_jobs: dict = {}          # job_id -> job dict (insertion-ordered)
_backtest_jobs_lock = threading.Lock()
_BACKTEST_KEEP = 5


def _run_backtest_job(job_id: str, start_ms: int, end_ms: int,
                      symbols: list, strategy: dict):
    """Background THREAD worker — never runs on the event loop."""
    job = _backtest_jobs.get(job_id)
    if job is None:
        return
    try:
        import backtest as _bt
        run_fn = getattr(_bt, "run_backtest", None)
        if run_fn is None:
            raise RuntimeError("backtest.run_backtest not available yet")
        job["status"] = "running"

        def _progress(pct):
            try:
                job["progress_pct"] = max(0.0, min(100.0, round(float(pct), 1)))
            except Exception:
                pass

        try:
            result = run_fn(start_ms, end_ms, symbols, strategy, progress_cb=_progress)
        except TypeError:
            # older/parallel signature without progress_cb
            result = run_fn(start_ms, end_ms, symbols, strategy)
        job["result"] = result
        job["progress_pct"] = 100.0
        job["status"] = "done"
    except Exception as e:
        job["status"] = "error"
        job["error"] = f"{type(e).__name__}: {e}"
    finally:
        job["finished_ts"] = time.time()


@app.post("/api/backtest")
def api_backtest_start(body: dict = Body(default={})):
    """Start a backtest job. Body: {start: 'YYYY-MM-DD', end: 'YYYY-MM-DD',
    symbols: ['BTCUSDT', ...] | 'approved', config: optional strategy override}.
    Single-flight: 409 while another job is queued/running."""
    try:
        try:
            import backtest as _bt
            if getattr(_bt, "run_backtest", None) is None:
                return {"error": "backtest.run_backtest not available yet"}
        except ImportError:
            return {"error": "backtest module not available yet"}

        body = body or {}
        try:
            start_ms = _parse_ymd_utc_ms(body.get("start"))
            end_ms = _parse_ymd_utc_ms(body.get("end"))
        except Exception:
            return {"error": "start/end must be 'YYYY-MM-DD' strings"}
        if end_ms <= start_ms:
            return {"error": "end must be after start"}
        # end is inclusive: run through the end of that UTC day
        end_ms += 86_400_000 - 1

        symbols = body.get("symbols", "approved")
        if isinstance(symbols, str):
            symbols = _approved_symbols() if symbols.strip().lower() == "approved" \
                else [s.strip().upper() for s in symbols.split(",") if s.strip()]
        elif isinstance(symbols, list):
            symbols = [str(s).upper() for s in symbols if str(s).strip()]
        else:
            symbols = []
        if not symbols:
            return {"error": "no symbols to backtest (approve coins first or pass symbols)"}

        strategy = body.get("config") if isinstance(body.get("config"), dict) \
            else _load_strategy()

        with _backtest_jobs_lock:
            if any(j.get("status") in ("queued", "running")
                   for j in _backtest_jobs.values()):
                return JSONResponse({"error": "a backtest is already running"},
                                    status_code=409)
            job_id = uuid.uuid4().hex[:8]
            _backtest_jobs[job_id] = {
                "job_id":       job_id,
                "status":       "queued",
                "progress_pct": 0.0,
                "result":       None,
                "error":        None,
                "started_ts":   time.time(),
                "finished_ts":  None,
                "params": {"start": body.get("start"), "end": body.get("end"),
                           "symbols": symbols},
            }
            # keep only the last _BACKTEST_KEEP jobs (drop oldest finished)
            while len(_backtest_jobs) > _BACKTEST_KEEP:
                oldest = next(iter(_backtest_jobs))
                if oldest == job_id:
                    break
                _backtest_jobs.pop(oldest, None)

        threading.Thread(
            target=_run_backtest_job,
            args=(job_id, start_ms, end_ms, symbols, strategy),
            name=f"backtest-{job_id}", daemon=True,
        ).start()
        return {"job_id": job_id, "status": "queued", "symbols": symbols}
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}


@app.get("/api/backtest/{job_id}")
def api_backtest_status(job_id: str):
    """Status/result of a backtest job started via POST /api/backtest."""
    try:
        job = _backtest_jobs.get(job_id)
        if job is None:
            return {"error": f"unknown job_id '{job_id}'",
                    "known_jobs": list(_backtest_jobs.keys())}
        return job
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}


# ── Expectancy stats ─────────────────────────────────────────────────────────

@app.get("/api/stats/expectancy")
def api_stats_expectancy(days: int = 30, mode: Optional[str] = None):
    """Expectancy / fee / exit-label breakdown from the trades table.
    Default mode filter = current get_mode(); ?mode=paper|live|all overrides."""
    try:
        import sqlite3 as _sq
        days = max(1, min(3650, int(days)))
        try:
            m = (mode or get_mode() or "paper").strip().lower()
        except Exception:
            m = (mode or "all").strip().lower()
        cutoff = (datetime.now(timezone.utc) - timedelta(days=days)
                  ).strftime("%Y-%m-%dT%H:%M:%S")

        where = ["exit_price IS NOT NULL", "net_profit IS NOT NULL",
                 "timestamp_sell >= ?"]
        params: list = [cutoff]
        if m != "all":
            where.append("mode = ?")
            params.append(m)

        conn = _sq.connect(database.DB_PATH)
        conn.row_factory = _sq.Row
        try:
            rows = conn.execute(f"""
                SELECT coin, net_profit, buy_fee, sell_fee,
                       entry_fee_usdt, exit_fee_usdt,
                       hold_time_sec, duration_seconds, exit_label,
                       timestamp_sell
                FROM trades
                WHERE {' AND '.join(where)}
            """, params).fetchall()
            # F8 — earliest closed trade for THIS mode ignoring the window, so we
            # can tell whether the window is truncated by available history
            # (7d and 30d returning identical data = all trades since deploy).
            _hist_where = ["exit_price IS NOT NULL", "net_profit IS NOT NULL"]
            _hist_params: list = []
            if m != "all":
                _hist_where.append("mode = ?")
                _hist_params.append(m)
            _hist_row = conn.execute(
                f"SELECT MIN(timestamp_sell) AS first_ts FROM trades "
                f"WHERE {' AND '.join(_hist_where)}", _hist_params).fetchone()
            earliest_overall = _hist_row["first_ts"] if _hist_row else None
        finally:
            conn.close()

        n = len(rows)
        pnls, fees, gross_abs, holds = [], 0.0, 0.0, []
        per_symbol: dict = {}
        exit_labels: dict = {}
        data_start_ts = None   # F8 — earliest trade timestamp IN this window
        for r in rows:
            _ts = r["timestamp_sell"]
            if _ts and (data_start_ts is None or _ts < data_start_ts):
                data_start_ts = _ts
            pnl = float(r["net_profit"] or 0.0)
            # Phase 1 fee columns first, legacy buy_fee/sell_fee as fallback
            ef = r["entry_fee_usdt"] if r["entry_fee_usdt"] is not None else r["buy_fee"]
            xf = r["exit_fee_usdt"] if r["exit_fee_usdt"] is not None else r["sell_fee"]
            fee = float(ef or 0.0) + float(xf or 0.0)
            pnls.append(pnl)
            fees += fee
            gross_abs += abs(pnl + fee)
            ht = r["hold_time_sec"] if r["hold_time_sec"] is not None else r["duration_seconds"]
            if ht is not None:
                holds.append(float(ht))

            sym = r["coin"] or "?"
            ps = per_symbol.setdefault(sym, {"symbol": sym, "trades": 0,
                                             "net_pnl": 0.0, "wins": 0})
            ps["trades"] += 1
            ps["net_pnl"] += pnl
            if pnl > 0:
                ps["wins"] += 1

            label = r["exit_label"] if r["exit_label"] else "unlabeled"
            el = exit_labels.setdefault(label, {"count": 0, "net_pnl": 0.0})
            el["count"] += 1
            el["net_pnl"] += pnl

        wins = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p <= 0]
        gross_win = sum(wins)
        gross_loss = abs(sum(losses))

        sym_list = sorted(per_symbol.values(),
                          key=lambda d: abs(d["net_pnl"]), reverse=True)[:20]
        for d in sym_list:
            d["win_rate"] = round(100.0 * d["wins"] / d["trades"], 1) if d["trades"] else 0.0
            d["net_pnl"] = round(d["net_pnl"], 4)
            d.pop("wins", None)
        for el in exit_labels.values():
            el["net_pnl"] = round(el["net_pnl"], 4)

        # F8 — window is truncated by available history when the earliest trade
        # in the window equals the earliest trade overall AND it starts after the
        # requested cutoff. Surfaced so the frontend labels windows truthfully
        # instead of implying "N days" when only M days of data exist.
        truncated = bool(data_start_ts and data_start_ts > cutoff)
        note = None
        if truncated:
            note = f"data since {str(data_start_ts)[:10]} (window truncated by available history)"

        return {
            "days": days,
            "mode": m,
            "trades": n,
            "window_start_ts": cutoff,
            "data_start_ts": data_start_ts,
            "earliest_trade_ts": earliest_overall,
            "window_truncated": truncated,
            "note": note,
            "win_rate": round(100.0 * len(wins) / n, 1) if n else 0.0,
            "avg_win": round(gross_win / len(wins), 4) if wins else 0.0,
            "avg_loss": round(-gross_loss / len(losses), 4) if losses else 0.0,
            "expectancy_per_trade": round(sum(pnls) / n, 4) if n else 0.0,
            # None when there are no losses (undefined) — inf is not JSON-safe
            "profit_factor": (round(gross_win / gross_loss, 3)
                              if gross_loss > 0 else None),
            "total_fees": round(fees, 4),
            "fee_share_of_gross": (round(fees / gross_abs, 4)
                                   if gross_abs > 0 else None),
            "avg_hold_time_sec": (round(sum(holds) / len(holds), 1)
                                  if holds else None),
            "per_symbol": sym_list,
            "exit_labels": exit_labels,
        }
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}


@app.get("/api/diagnostics/veto-stats")
def api_diagnostics_veto_stats(hours: float = 24.0, min_evals: int = 20):
    """F10 — per-symbol chronic spread (E1) veto rate over a rolling window.

    Source: the buy_rejections table the engine already records (no new engine
    hooks). The signal-engine veto reason is 'veto_E1_spread_too_wide_fired';
    older/legacy spread rejections ('spread', '*spread_too_wide*') are folded in.
    evals = total recorded buy rejections for the symbol in the window (the
    available proxy for "evaluated then not entered"); e1_veto_pct = E1 vetoes /
    evals. prune_candidates = symbols E1-vetoed >70% of evals (>= min_evals)."""
    try:
        import sqlite3 as _sq
        hours = max(0.5, min(168.0, float(hours)))
        min_evals = max(1, int(min_evals))
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=hours)
                  ).strftime("%Y-%m-%dT%H:%M:%S")
        conn = _sq.connect(database.DB_PATH)
        conn.row_factory = _sq.Row
        try:
            rows = conn.execute("""
                SELECT coin,
                       COUNT(*) AS evals,
                       SUM(CASE WHEN reason LIKE 'veto_E1_spread_too_wide%'
                                  OR reason = 'spread'
                                  OR reason LIKE '%spread_too_wide%'
                                THEN 1 ELSE 0 END) AS e1
                FROM buy_rejections
                WHERE timestamp >= ? AND coin IS NOT NULL AND coin != '(all)'
                GROUP BY coin
            """, (cutoff,)).fetchall()
        finally:
            conn.close()
        symbols: dict = {}
        prune: list = []
        for r in rows:
            evals = int(r["evals"] or 0)
            e1 = int(r["e1"] or 0)
            if evals <= 0:
                continue
            pct = round(100.0 * e1 / evals, 1)
            symbols[r["coin"]] = {"e1_veto_pct": pct, "evals": evals, "e1_vetoes": e1}
            if pct > 70.0 and evals >= min_evals:
                prune.append(r["coin"])
        prune.sort(key=lambda s: symbols[s]["e1_veto_pct"], reverse=True)
        return {
            "window_hours": hours,
            "since": cutoff,
            "min_evals": min_evals,
            "source": "buy_rejections",
            "symbols": symbols,
            "prune_candidates": prune,
        }
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}",
                "symbols": {}, "prune_candidates": []}


@app.get("/api/diagnostics/exit-r")
def api_diagnostics_exit_r():
    """F1/F9 — planned vs realized R distribution from
    trade_engine.get_exit_r_stats(). Defensive: returns {available: false} when
    the engine helper isn't present yet (parallel rollout)."""
    try:
        import trade_engine as _te
        fn = getattr(_te, "get_exit_r_stats", None)
        if not callable(fn):
            return {"available": False,
                    "reason": "trade_engine.get_exit_r_stats not available"}
        data = fn()
        if not isinstance(data, dict):
            return {"available": False, "reason": "unexpected return type"}
        out = dict(data)
        out["available"] = True
        return out
    except Exception as e:
        return {"available": False, "error": f"{type(e).__name__}: {e}"}


@app.post("/api/risk/rebaseline")
def api_risk_rebaseline(body: dict = Body(...)):
    """F9 — rebase the risk engine's initial balance to current equity. Requires
    {confirm: true}. Calls trade_engine.rebaseline_initial_balance()."""
    try:
        if not isinstance(body, dict) or body.get("confirm") is not True:
            return JSONResponse(
                status_code=422,
                content={"errors": {"confirm": "must be true to rebaseline"}})
        import trade_engine as _te
        fn = getattr(_te, "rebaseline_initial_balance", None)
        if not callable(fn):
            return JSONResponse(
                status_code=503,
                content={"error": "trade_engine.rebaseline_initial_balance not available"})
        result = fn()
        try:
            database.log_activity("Risk initial balance rebaselined via API", "warn")
        except Exception:
            pass
        return {"ok": True,
                "result": result if isinstance(result, (dict, list, int, float, str)) else None}
    except Exception as e:
        return JSONResponse(status_code=500,
                            content={"ok": False, "error": f"{type(e).__name__}: {e}"})


# ── Attribution / edge report ────────────────────────────────────────────────

_edge_rebuild_lock = threading.Lock()


def _edge_rebuild_sync() -> dict:
    """Blocking rebuild via attribution.build_edge_report(days=7); persists
    the result to settings so the GET path serves it afterwards."""
    import attribution as _attr
    fn = getattr(_attr, "build_edge_report", None)
    if fn is None:
        raise RuntimeError("attribution.build_edge_report not available yet")
    report = fn(days=7)
    try:
        database.save_setting("edge_report_json", json.dumps(report, default=str))
        database.save_setting("edge_report_ts", str(time.time()))
    except Exception:
        pass
    return report


@app.get("/api/stats/attribution")
def api_stats_attribution(rebuild: int = 0):
    """Stored nightly edge report. ?rebuild=1 rebuilds synchronously in a
    thread (120 s timeout; 409 while another rebuild is in flight)."""
    try:
        if rebuild:
            try:
                import attribution as _attr
                if getattr(_attr, "build_edge_report", None) is None:
                    return {"error": "attribution.build_edge_report not available yet"}
            except ImportError:
                return {"error": "attribution module not available yet"}
            if not _edge_rebuild_lock.acquire(blocking=False):
                return JSONResponse({"error": "an edge report rebuild is already running"},
                                    status_code=409)
            import concurrent.futures as _cf
            ex = _cf.ThreadPoolExecutor(max_workers=1,
                                        thread_name_prefix="edge-rebuild")
            fut = ex.submit(_edge_rebuild_sync)
            fut.add_done_callback(lambda _f: _edge_rebuild_lock.release())
            ex.shutdown(wait=False)
            try:
                report = fut.result(timeout=120)
                return {"report": report, "ts": time.time(), "rebuilt": True}
            except _ConcurrentTimeoutError:
                return {"error": "edge report rebuild timed out after 120s "
                                 "(still finishing in the background)"}
            except Exception as e:
                return {"error": f"rebuild failed: {type(e).__name__}: {e}"}

        raw = None
        try:
            raw = database.get_setting("edge_report_json")
        except Exception:
            pass
        if not raw:
            return {"report": None, "ts": None, "rebuilt": False,
                    "error": "no edge report stored yet — nightly job has not run"}
        try:
            report = json.loads(raw)
        except Exception:
            return {"error": "stored edge report is not valid JSON"}
        ts = None
        try:
            ts_raw = database.get_setting("edge_report_ts")
            ts = float(ts_raw) if ts_raw else None
        except Exception:
            pass
        if ts is None and isinstance(report, dict):
            ts = report.get("ts") or report.get("generated_ts")
        return {"report": report, "ts": ts, "rebuilt": False}
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}


# ── Kline store coverage ─────────────────────────────────────────────────────

@app.get("/api/klines/coverage")
def api_klines_coverage(symbols: str = "approved"):
    """Per-symbol 1m + 5m kline-store coverage (backfill progress for the UI).
    ?symbols=approved (default) or a comma-separated symbol list."""
    try:
        cov_fn = getattr(database, "kline_coverage", None)
        if cov_fn is None:
            return {"error": "kline store not available yet"}
        if str(symbols).strip().lower() == "approved":
            syms = _approved_symbols()
        else:
            syms = [s.strip().upper() for s in str(symbols).split(",") if s.strip()]
        out = []
        for s in syms:
            try:
                out.append({"symbol": s,
                            "1m": cov_fn(s, "1m"),
                            "5m": cov_fn(s, "5m")})
            except Exception as e:
                out.append({"symbol": s, "error": str(e)})
        return {"symbols": out, "count": len(out), "ts": time.time()}
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}


# ── Phase 5 §5.1 + Phase 6 §6 — v2 strategy config API ───────────────────────
#
# Payload shapes (frontend contract):
#   GET  /api/strategy          → {config, schema_version, last_modified, config_hash, raw_keys}
#   PUT  /api/strategy          → body {config: <partial v2>, confirm?: "LIVE"}
#                                 200 {ok, applied, diff, config_hash, version}
#                                 422 {errors: {dotted.path: msg}} (file untouched)
#                                 409 {error} when live+active without confirm
#   GET  /api/strategy/schema   → {sections: [...], schema: {dotted: meta}, read_only: [...]}
#   GET  /api/strategy/history  → {history: [{version, ts, actor, diff, config_hash}], count}
#   POST /api/strategy/rollback → body {version, confirm?} → {ok, restored_version, version, config_hash}
#   GET  /api/strategy/ack      → {config_hash, schema_version, ts}
#   POST /api/strategy/preview  → body {config} → {n, reasons, totals} | {insufficient_data, n}
#   PUT  /api/signals/registry  → body {roles?, thresholds?, confirm?} → {ok, signal_engine, signal_thresholds, version, config_hash}

import strategy_config as _scfg


def _strategy_raw_file() -> dict:
    """Raw strategy.json without the hot-reload audit side effects."""
    try:
        with open(config.STRATEGY_FILE) as f:
            s = json.load(f)
        return s if isinstance(s, dict) else {}
    except Exception:
        return {}


def _strategy_v2_mode() -> str:
    return "live" if get_mode() == "live" else "paper"


def _strategy_live_guard(confirm) -> Optional[JSONResponse]:
    """409 guard: config writes while LIVE trading is actively running require
    an explicit confirm:'LIVE'. Never raises (guard failure = no guard)."""
    try:
        if get_mode() == "live" and bool(
                _strategy_raw_file().get("trading_active", False)):
            if confirm != "LIVE":
                return JSONResponse(
                    status_code=409,
                    content={"error": "live trading active — resend with confirm:'LIVE'"})
    except Exception:
        pass
    return None


def _strategy_get_payload() -> dict:
    raw = _strategy_raw_file()
    last_modified = None
    try:
        last_modified = datetime.fromtimestamp(
            os.path.getmtime(config.STRATEGY_FILE), tz=timezone.utc).isoformat()
    except Exception:
        pass
    return {
        "config":         _scfg.current_v2_view(raw, mode=_strategy_v2_mode()),
        "schema_version": _scfg.SCHEMA_VERSION,
        "last_modified":  last_modified,
        "config_hash":    database.config_hash(raw),
        "raw_keys":       sorted(str(k) for k in raw.keys()),
    }


@app.get("/api/strategy")
def api_get_strategy():
    """Resolved v2 view of strategy.json + file metadata."""
    try:
        return _strategy_get_payload()
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}


@app.get("/api/strategy/schema")
def api_get_strategy_schema():
    """Per-field UI metadata — the frontend auto-renders the form from this."""
    try:
        return {
            "sections":  list(_scfg.SECTIONS),
            "schema":    _scfg.SCHEMA,
            "read_only": sorted(_scfg.READ_ONLY_PATHS),
        }
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}


@app.put("/api/strategy")
def api_put_strategy(body: dict = Body(...)):
    """Validated partial update of the v2 blocks. All-or-nothing: any field
    error → 422 and the file is untouched."""
    try:
        if not isinstance(body, dict):
            return JSONResponse(status_code=422,
                                content={"errors": {"body": "must be an object"}})
        patch = body.get("config")
        if not isinstance(patch, dict) or not patch:
            return JSONResponse(
                status_code=422,
                content={"errors": {"config": "must be a non-empty object"}})
        guard = _strategy_live_guard(body.get("confirm"))
        if guard is not None:
            return guard

        raw = _strategy_raw_file()
        merged, errors = _scfg.validate_patch(raw, patch)
        if errors:
            return JSONResponse(status_code=422, content={"errors": errors})

        # F4 — budget floor guard: when the sizing block is touched (mode /
        # min_position_usdt), verify the fixed/per-coin per-trade budget still
        # clears the min-notional floor. Merge the new sizing block over the raw
        # root keys the engine actually reads for budget math.
        if isinstance(patch.get("sizing"), dict):
            _bmerged = {**raw}
            if isinstance(merged.get("sizing"), dict):
                _bmerged["sizing"] = merged["sizing"]
            _berr, _ = _validate_budget_floor(_bmerged)
            if _berr:
                return JSONResponse(status_code=422, content={"errors": _berr})

        old_view = _scfg.current_v2_view(raw, mode=_strategy_v2_mode())
        merged["mode"] = old_view.get("mode", merged.get("mode"))
        diff = _scfg.diff_views(old_view, merged)

        # Only the blocks the caller actually touched are written — untouched
        # blocks stay absent from the file so engine legacy fallbacks keep
        # applying until the user opts in.
        touched = [b for b in _scfg.V2_BLOCKS if isinstance(patch.get(b), dict)]
        file_patch = {b: merged[b] for b in touched}
        file_patch["schema_version"] = _scfg.SCHEMA_VERSION
        _write_strategy_patch(file_patch)

        new_raw = _strategy_raw_file()
        version = database.save_config_version("api", diff, new_raw)
        try:
            database.log_activity(
                "Strategy v2 updated via API: "
                + (", ".join(sorted(diff.keys())[:12]) or "no-op"), "info")
        except Exception:
            pass
        return {
            "ok":          True,
            "applied":     {b: merged[b] for b in touched},
            "diff":        diff,
            "config_hash": database.config_hash(new_raw),
            "version":     version,
        }
    except Exception as e:
        try:
            database.log_activity(f"Strategy v2 save error: {e}", "error")
        except Exception:
            pass
        return JSONResponse(status_code=422,
                            content={"errors": {"config": f"{type(e).__name__}: {e}"}})


@app.get("/api/strategy/history")
def api_strategy_history(limit: int = 20):
    """Config-change history (newest first, diffs only — full snapshots are
    fetched per-version by rollback)."""
    try:
        limit = max(1, min(int(limit), 200))
        history = database.get_config_history(limit=limit)
        return {"history": history, "count": len(history)}
    except Exception as e:
        return {"history": [], "count": 0, "error": f"{type(e).__name__}: {e}"}


@app.post("/api/strategy/rollback")
def api_strategy_rollback(body: dict = Body(...)):
    """Restore the FULL strategy.json content of a stored history version
    (atomic write under the shared strategy file lock)."""
    try:
        if not isinstance(body, dict):
            return JSONResponse(status_code=422,
                                content={"errors": {"body": "must be an object"}})
        try:
            version = int(body.get("version"))
        except (TypeError, ValueError):
            return JSONResponse(status_code=422,
                                content={"errors": {"version": "must be an integer"}})
        rec = database.get_config_version(version)
        if rec is None or not isinstance(rec.get("full"), dict) or not rec["full"]:
            return JSONResponse(status_code=404,
                                content={"error": f"unknown config version {version}"})
        guard = _strategy_live_guard(body.get("confirm"))
        if guard is not None:
            return guard

        full = rec["full"]
        old_raw = _strategy_raw_file()
        import strategy_engine as _se_rb
        # Both lock domains: control_api's writer lock (thread) AND the
        # cross-process strategy file lock, then the shared atomic writer.
        with _strategy_write_lock:
            with _se_rb._strategy_file_lock():
                _se_rb._atomic_write_strategy(full)
        try:
            _API_ALL_CACHE["data"] = None
        except Exception:
            pass

        diff = _scfg.diff_views(_scfg.current_v2_view(old_raw),
                                _scfg.current_v2_view(full))
        new_version = database.save_config_version(f"rollback(v{version})", diff, full)
        try:
            database.log_activity(
                f"strategy.json rolled back to config version {version}", "warn")
        except Exception:
            pass
        return {
            "ok":               True,
            "restored_version": version,
            "version":          new_version,
            "diff":             diff,
            "config_hash":      database.config_hash(full),
        }
    except Exception as e:
        return JSONResponse(status_code=422,
                            content={"errors": {"rollback": f"{type(e).__name__}: {e}"}})


@app.get("/api/strategy/ack")
def api_strategy_ack():
    """Hash of the file as THIS process reads it right now — the UI polls
    this after PUT to confirm the running bot sees the new config (all
    readers are mtime-cached on the same file, so same hash = applied)."""
    try:
        raw = _strategy_raw_file()
        return {
            "config_hash":    database.config_hash(raw),
            "schema_version": raw.get("schema_version"),
            "ts":             datetime.now(timezone.utc).isoformat(),
        }
    except Exception as e:
        return {"config_hash": None, "error": f"{type(e).__name__}: {e}"}


# ── Preview: re-evaluate stored entry snapshots under a candidate config ─────

_PREVIEW_SNAPSHOT_N = 200
_PREVIEW_MIN_N = 10
_PREVIEW_TIMEOUT_SEC = 30
_preview_lock = threading.Lock()


def _preview_signal_data(snap: dict) -> dict:
    """Rebuild the signal_registry signal_data dict from one stored entry
    snapshot (raw keys written by trade_engine._snapshot_raw_from_cache).
    Shape-tolerant: missing keys degrade to None/False."""
    raw = snap.get("raw") if isinstance(snap.get("raw"), dict) else {}
    sigs = raw.get("signals") if isinstance(raw.get("signals"), dict) else {}
    return {
        "trend":           bool(sigs.get("trend")),
        "rsi":             bool(sigs.get("rsi")),
        "macd":            bool(sigs.get("macd")),
        "volume":          bool(sigs.get("volume")),
        "obv":             bool(sigs.get("obv")),
        "atr":             bool(sigs.get("atr")),
        "rsi_value":       raw.get("rsi_val", raw.get("rsi_value")),
        "stoch_rsi_value": raw.get("stoch_rsi_val", raw.get("stoch_rsi_value")),
        "low_24h":         raw.get("low_24h"),
        "current_price":   raw.get("price", snap.get("price")),
        "klines_1m":       raw.get("klines_1m") or [],
    }


def _preview_candidate_strategy(current_raw: dict, patch: dict, merged: dict) -> dict:
    """Candidate raw strategy: current file + the patched v2 blocks, plus the
    legacy/engine keys evaluate_buy_decision actually reads (min_signals /
    signal_engine.min_scored mirror entries.min_score; signal_engine /
    signal_thresholds passthrough keys apply verbatim)."""
    import copy as _copy
    cand = _copy.deepcopy(current_raw) if isinstance(current_raw, dict) else {}
    for blk in _scfg.V2_BLOCKS:
        if isinstance(patch.get(blk), dict):
            cand[blk] = merged.get(blk, {})
    entries_patch = patch.get("entries")
    if isinstance(entries_patch, dict) and "min_score" in entries_patch:
        try:
            _ms = int(merged["entries"]["min_score"])
            cand["min_signals"] = _ms
            se = cand.get("signal_engine")
            if isinstance(se, dict):
                se = dict(se)
                se["min_scored"] = _ms
                cand["signal_engine"] = se
        except Exception:
            pass
    # Direct signal-engine experimentation (not part of the typed v2 model).
    for pk in ("signal_engine", "signal_thresholds"):
        if isinstance(patch.get(pk), dict):
            base = cand.get(pk) if isinstance(cand.get(pk), dict) else {}
            cand[pk] = {**base, **patch[pk]}
    return cand


def _preview_worker(snapshots: list, current: dict, candidate: dict) -> dict:
    import signal_registry as _sr
    reasons: dict = {}
    totals = {"current_allowed": 0, "candidate_allowed": 0,
              "would_allow": 0, "would_block": 0}
    evaluated = 0
    for snap in snapshots:
        sym = snap.get("symbol") or "?"
        try:
            sd = _preview_signal_data(snap)
        except Exception:
            continue
        row = {}
        for label, strat in (("current", current), ("candidate", candidate)):
            try:
                dec = _sr.evaluate_buy_decision(sym, sd, strat)
                allowed = bool(dec.get("allowed"))
                reason = "allowed" if allowed else str(dec.get("reason") or "blocked")
            except Exception as exc:
                allowed, reason = False, f"eval_error:{type(exc).__name__}"
            row[label] = (allowed, reason)
            bucket = reasons.setdefault(reason, {"current": 0, "candidate": 0})
            bucket[label] += 1
            if allowed:
                totals[f"{label}_allowed"] += 1
        evaluated += 1
        cur_ok, cand_ok = row["current"][0], row["candidate"][0]
        if not cur_ok and cand_ok:
            totals["would_allow"] += 1
        elif cur_ok and not cand_ok:
            totals["would_block"] += 1
    return {"n": evaluated, "reasons": reasons, "totals": totals}


@app.post("/api/strategy/preview")
def api_strategy_preview(body: dict = Body(...)):
    """Dry-run a candidate config against the last stored entry snapshots:
    per-reason counts under current vs candidate, plus would_allow /
    would_block totals. Single-flight (409) with a 30 s timeout."""
    try:
        if not isinstance(body, dict) or not isinstance(body.get("config"), dict) \
                or not body["config"]:
            return JSONResponse(
                status_code=422,
                content={"errors": {"config": "must be a non-empty object"}})
        patch = dict(body["config"])
        # signal_engine / signal_thresholds are passthrough experiment keys —
        # strip them before typed validation of the v2 blocks.
        v2_patch = {k: v for k, v in patch.items()
                    if k not in ("signal_engine", "signal_thresholds")}
        raw = _strategy_raw_file()
        merged: dict = {}
        if v2_patch:
            merged, errors = _scfg.validate_patch(raw, v2_patch)
            if errors:
                return JSONResponse(status_code=422, content={"errors": errors})

        snapshots = []
        try:
            snapshots = database.get_entry_snapshots(limit=_PREVIEW_SNAPSHOT_N)
        except Exception:
            snapshots = []
        if len(snapshots) < _PREVIEW_MIN_N:
            return {"insufficient_data": True, "n": len(snapshots),
                    "min_required": _PREVIEW_MIN_N}

        candidate = _preview_candidate_strategy(raw, patch, merged or
                                                _scfg.current_v2_view(raw))

        if not _preview_lock.acquire(blocking=False):
            return JSONResponse(status_code=409,
                                content={"error": "a preview is already running"})
        import concurrent.futures as _cf
        ex = _cf.ThreadPoolExecutor(max_workers=1, thread_name_prefix="cfg-preview")
        fut = ex.submit(_preview_worker, snapshots, raw, candidate)
        fut.add_done_callback(lambda _f: _preview_lock.release())
        ex.shutdown(wait=False)
        try:
            result = fut.result(timeout=_PREVIEW_TIMEOUT_SEC)
        except _ConcurrentTimeoutError:
            return {"error": f"preview timed out after {_PREVIEW_TIMEOUT_SEC}s "
                             "(still finishing in the background)"}
        except Exception as e:
            return {"error": f"preview failed: {type(e).__name__}: {e}"}
        result["snapshot_window"] = _PREVIEW_SNAPSHOT_N
        return result
    except Exception as e:
        return {"error": f"{type(e).__name__}: {e}"}


# ── PUT /api/signals/registry — roles + thresholds writer ────────────────────

_REGISTRY_VALID_ROLES = {"scored", "mandatory", "veto", "off"}


@app.put("/api/signals/registry")
def api_put_signals_registry(body: dict = Body(...)):
    """Write signal_engine.roles and/or signal_thresholds. Validated against
    the live SIGNAL_REGISTRY; 422 field errors, LIVE guard, history entry."""
    try:
        if not isinstance(body, dict):
            return JSONResponse(status_code=422,
                                content={"errors": {"body": "must be an object"}})
        roles = body.get("roles")
        thresholds = body.get("thresholds")
        if not isinstance(roles, dict):
            roles = None
        if not isinstance(thresholds, dict):
            thresholds = None
        if not roles and not thresholds:
            return JSONResponse(
                status_code=422,
                content={"errors": {"body": "provide roles and/or thresholds"}})

        import signal_registry as _sr
        errors: dict = {}
        clean_roles: dict = {}
        if roles:
            for sig_id, role in roles.items():
                if sig_id not in _sr.SIGNAL_REGISTRY:
                    errors[f"roles.{sig_id}"] = (
                        f"unknown signal id (known: {sorted(_sr.SIGNAL_REGISTRY)})")
                    continue
                norm = role.strip().lower() if isinstance(role, str) else None
                if norm == "disabled":   # UI vocabulary → roles-map spelling
                    norm = "off"
                if norm not in _REGISTRY_VALID_ROLES:
                    errors[f"roles.{sig_id}"] = "role must be scored|mandatory|veto|off"
                    continue
                clean_roles[sig_id] = norm
        clean_thresholds: dict = {}
        if thresholds:
            # rsi_buy_threshold is read from signal_thresholds by M1 but has
            # no entry in DEFAULT_SIGNAL_THRESHOLDS (legacy root default 40.0).
            known_th = {"rsi_buy_threshold": 40.0,
                        **getattr(_sr, "DEFAULT_SIGNAL_THRESHOLDS", {})}
            # Accept the SignalsEditorPanel's nested shape too:
            # {signal_id: {threshold_key: value}} — flatten to the flat map
            # the engine reads (unknown inner keys still 422 below).
            flat: dict = {}
            for key, val in thresholds.items():
                if key in _sr.SIGNAL_REGISTRY and isinstance(val, dict):
                    flat.update(val)
                else:
                    flat[key] = val
            thresholds = flat
            for key, val in thresholds.items():
                if key not in known_th:
                    errors[f"thresholds.{key}"] = (
                        f"unknown threshold (known: {sorted(known_th)})")
                    continue
                if isinstance(known_th.get(key), str):
                    if not isinstance(val, str):
                        errors[f"thresholds.{key}"] = "must be a string"
                        continue
                    clean_thresholds[key] = val
                    continue
                if isinstance(val, bool) or not isinstance(val, (int, float)):
                    errors[f"thresholds.{key}"] = "must be a number"
                    continue
                clean_thresholds[key] = float(val)
        if errors:
            return JSONResponse(status_code=422, content={"errors": errors})

        guard = _strategy_live_guard(body.get("confirm"))
        if guard is not None:
            return guard

        s = _strategy_raw_file()
        old_se = s.get("signal_engine") if isinstance(s.get("signal_engine"), dict) else {}
        old_th = s.get("signal_thresholds") if isinstance(s.get("signal_thresholds"), dict) else {}
        patch: dict = {}
        new_se, new_th = dict(old_se), dict(old_th)
        if clean_roles:
            merged_roles = dict(old_se.get("roles") or {})
            merged_roles.update(clean_roles)
            new_se = {**old_se, "roles": merged_roles}
            patch["signal_engine"] = new_se
        if clean_thresholds:
            new_th = {**old_th, **clean_thresholds}
            patch["signal_thresholds"] = new_th
        _write_strategy_patch(patch)

        new_raw = _strategy_raw_file()
        diff = _scfg.diff_views(
            {"signal_engine": old_se, "signal_thresholds": old_th},
            {"signal_engine": new_se, "signal_thresholds": new_th})
        version = database.save_config_version("api", diff, new_raw)
        try:
            database.log_activity(
                "Signal registry updated via API: "
                + (", ".join(sorted(diff.keys())[:12]) or "no-op"), "info")
        except Exception:
            pass
        return {
            "ok":                True,
            "signal_engine":     new_se,
            "signal_thresholds": new_th,
            "diff":              diff,
            "version":           version,
            "config_hash":       database.config_hash(new_raw),
        }
    except Exception as e:
        try:
            database.log_activity(f"Signal registry save error: {e}", "error")
        except Exception:
            pass
        return JSONResponse(status_code=422,
                            content={"errors": {"body": f"{type(e).__name__}: {e}"}})


def start_control_api():
    """Block the main thread on uvicorn — all bot logic starts via lifespan."""
    import pathlib

    # Mount React build INSIDE start_control_api so any failure (missing
    # aiofiles, missing dist/) is caught and logged — it never prevents the
    # HTTP server from binding and passing Railway's health check.
    # Repo-root dist/ is the committed build and MUST win — a leftover
    # trading-bot/dist/ from old deploys shadowed it and froze the UI at an
    # old version no matter how many updates were installed.
    _dist_candidates = [
        pathlib.Path(__file__).parent.parent / "dist",
        pathlib.Path(__file__).parent / "dist",
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
