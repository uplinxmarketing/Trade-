"""SQLite database layer — thread-safe, no async required."""

import os
import sqlite3
import threading
import json
from datetime import datetime, timezone
from typing import Optional, Dict, List, Any

# DATA_DIR stores the SQLite database and strategy.json.
# Default: sibling "data/" directory next to the trading-bot folder, which is
# outside the git checkout so git operations never touch it.
# Override with DATA_DIR env var for Railway / Docker volume mounts.
def _resolve_data_dir() -> str:
    _script_dir = os.path.dirname(os.path.abspath(__file__))
    _default    = os.path.join(_script_dir, "..", "data")  # /opt/tradebot/data
    candidate   = os.getenv("DATA_DIR", _default)
    try:
        os.makedirs(candidate, exist_ok=True)
        probe = os.path.join(candidate, ".write_probe")
        with open(probe, "w") as _f:
            _f.write("ok")
        os.remove(probe)
        return candidate
    except OSError:
        fallback = os.path.dirname(os.path.abspath(__file__))
        print(f"[DB] {candidate} not writable — falling back to {fallback}")
        return fallback

_DATA_DIR = _resolve_data_dir()
DB_PATH = os.path.join(_DATA_DIR, "bot.db")
_lock = threading.Lock()

import time as _time
_persistence_check: dict = {"ts": 0.0, "ok": None}

def is_data_persistent() -> bool:
    """Test whether _DATA_DIR is actually writable AND survives between calls.
    Cached for 60s — write probe is cheap but we don't need it every API call."""
    now = _time.time()
    if _persistence_check["ok"] is not None and (now - _persistence_check["ts"]) < 60:
        return _persistence_check["ok"]
    try:
        probe = os.path.join(_DATA_DIR, ".persistence_probe")
        with open(probe, "w") as f:
            f.write(str(now))
        with open(probe) as f:
            ok = f.read() == str(now)
        os.remove(probe)
        _persistence_check["ok"] = ok
    except Exception:
        _persistence_check["ok"] = False
    _persistence_check["ts"] = now
    return _persistence_check["ok"]


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with _lock:
        conn = _conn()
        c = conn.cursor()
        c.executescript("""
            CREATE TABLE IF NOT EXISTS candles (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                coin         TEXT NOT NULL,
                timeframe    TEXT NOT NULL,
                open_time    INTEGER NOT NULL,
                open         REAL,
                high         REAL,
                low          REAL,
                close        REAL,
                volume       REAL,
                ma20         REAL,
                rsi14        REAL,
                bb_upper     REAL,
                bb_lower     REAL,
                bb_mid       REAL,
                volume_ma20  REAL,
                ma_position  TEXT,
                bb_position  TEXT,
                volume_trend TEXT,
                saved_at     TEXT DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(coin, timeframe, open_time)
            );

            CREATE TABLE IF NOT EXISTS trades (
                id                 INTEGER PRIMARY KEY AUTOINCREMENT,
                coin               TEXT NOT NULL,
                mode               TEXT NOT NULL,
                entry_price        REAL,
                exit_price         REAL,
                quantity           REAL,
                budget_usdt        REAL,
                buy_fee            REAL,
                sell_fee           REAL,
                net_profit         REAL,
                profitable         INTEGER,
                duration_seconds   INTEGER,
                entry_rsi          REAL,
                entry_ma_position  TEXT,
                entry_bb_position  TEXT,
                entry_volume_trend TEXT,
                hour_of_day        INTEGER,
                day_of_week        INTEGER,
                timestamp_buy      TEXT,
                timestamp_sell     TEXT
            );

            CREATE TABLE IF NOT EXISTS patterns (
                id                 INTEGER PRIMARY KEY AUTOINCREMENT,
                coin               TEXT NOT NULL,
                pattern_key        TEXT NOT NULL UNIQUE,
                rsi_range          TEXT,
                bb_position        TEXT,
                volume_trend       TEXT,
                ma_position        TEXT,
                outcome_profitable INTEGER,
                avg_profit_pct     REAL,
                confidence_score   REAL,
                occurrence_count   INTEGER DEFAULT 1,
                last_seen          TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS decisions (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                action           TEXT,
                coin             TEXT,
                confidence       REAL,
                reason           TEXT,
                pattern_observed TEXT,
                was_correct      INTEGER,
                timestamp        TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS paper_state (
                id         INTEGER PRIMARY KEY DEFAULT 1,
                balances   TEXT,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS positions (
                id                 INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol             TEXT,
                entry_price        REAL,
                exit_target        REAL,
                quantity           REAL,
                budget_usdt        REAL,
                timestamp          TEXT,
                mode               TEXT,
                entry_rsi          REAL,
                entry_ma_position  TEXT,
                entry_bb_position  TEXT,
                entry_volume_trend TEXT
            );

            CREATE TABLE IF NOT EXISTS activity_log (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                message   TEXT NOT NULL,
                level     TEXT NOT NULL DEFAULT 'info',
                timestamp TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS settings (
                key        TEXT PRIMARY KEY,
                value      TEXT NOT NULL,
                updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS futures_state (
                id         INTEGER PRIMARY KEY DEFAULT 1,
                balances   TEXT,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            );

            CREATE TABLE IF NOT EXISTS futures_positions (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol            TEXT NOT NULL,
                direction         TEXT NOT NULL,
                entry_price       REAL NOT NULL,
                quantity          REAL NOT NULL,
                margin_usdt       REAL NOT NULL,
                leverage          INTEGER NOT NULL DEFAULT 5,
                take_profit       REAL,
                stop_loss         REAL,
                sl_enabled        INTEGER NOT NULL DEFAULT 1,
                liquidation_price REAL,
                funding_paid      REAL DEFAULT 0.0,
                timestamp         TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS futures_trades (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol           TEXT NOT NULL,
                direction        TEXT NOT NULL,
                entry_price      REAL NOT NULL,
                exit_price       REAL,
                quantity         REAL,
                margin_usdt      REAL,
                leverage         INTEGER,
                net_profit       REAL,
                profitable       INTEGER,
                funding_paid     REAL DEFAULT 0.0,
                duration_seconds INTEGER,
                timestamp_open   TEXT,
                timestamp_close  TEXT
            );

            CREATE TABLE IF NOT EXISTS buy_rejections (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp  TEXT NOT NULL,
                coin       TEXT NOT NULL,
                reason     TEXT NOT NULL,
                detail     TEXT,
                score      INTEGER,
                rsi_value  REAL
            );
            CREATE INDEX IF NOT EXISTS idx_buy_rejections_ts     ON buy_rejections(timestamp);
            CREATE INDEX IF NOT EXISTS idx_buy_rejections_reason ON buy_rejections(reason);

            CREATE TABLE IF NOT EXISTS phantom_alerts (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp   TEXT NOT NULL,
                symbol      TEXT NOT NULL,
                db_qty      REAL,
                binance_qty REAL,
                resolved    INTEGER DEFAULT 0
            );
        """)

        # ── Schema migrations for existing databases ─────────────────────────
        migrations = [
            "ALTER TABLE candles   ADD COLUMN ma_position  TEXT",
            "ALTER TABLE candles   ADD COLUMN bb_position  TEXT",
            "ALTER TABLE candles   ADD COLUMN volume_trend TEXT",
            "ALTER TABLE positions ADD COLUMN entry_rsi          REAL",
            "ALTER TABLE positions ADD COLUMN entry_ma_position  TEXT",
            "ALTER TABLE positions ADD COLUMN entry_bb_position  TEXT",
            "ALTER TABLE positions ADD COLUMN entry_volume_trend TEXT",
            "ALTER TABLE positions         ADD COLUMN exit_target  REAL",
            "ALTER TABLE futures_positions ADD COLUMN sl_enabled   INTEGER NOT NULL DEFAULT 1",
            "ALTER TABLE positions         ADD COLUMN buy_fee_usdt REAL",
            "ALTER TABLE futures_trades    ADD COLUMN buy_fee  REAL DEFAULT 0.0",
            "ALTER TABLE futures_trades    ADD COLUMN sell_fee REAL DEFAULT 0.0",
            # trades table — columns added after initial schema
            "ALTER TABLE trades ADD COLUMN entry_rsi          REAL",
            "ALTER TABLE trades ADD COLUMN entry_ma_position  TEXT",
            "ALTER TABLE trades ADD COLUMN entry_bb_position  TEXT",
            "ALTER TABLE trades ADD COLUMN entry_volume_trend TEXT",
            "ALTER TABLE trades ADD COLUMN hour_of_day        INTEGER",
            "ALTER TABLE trades ADD COLUMN day_of_week        INTEGER",
            "ALTER TABLE trades ADD COLUMN buy_fee            REAL",
            "ALTER TABLE trades ADD COLUMN sell_fee           REAL",
            "ALTER TABLE trades ADD COLUMN duration_seconds   INTEGER",
            "ALTER TABLE trades ADD COLUMN profitable         INTEGER",
            "ALTER TABLE trades ADD COLUMN sell_reason        TEXT",
            "ALTER TABLE trades ADD COLUMN signal_snapshot     TEXT",
            "ALTER TABLE trades ADD COLUMN target_crossed_to_trigger_ms INTEGER",
            "ALTER TABLE trades ADD COLUMN trigger_to_filled_ms         INTEGER",
            "ALTER TABLE trades ADD COLUMN target_crossed_ts            TEXT",
            # fill quality columns
            "ALTER TABLE trades ADD COLUMN intended_buy_price   REAL",
            "ALTER TABLE trades ADD COLUMN intended_sell_price  REAL",
            "ALTER TABLE trades ADD COLUMN buy_slippage_pct     REAL",
            "ALTER TABLE trades ADD COLUMN sell_slippage_pct    REAL",
            "ALTER TABLE positions ADD COLUMN intended_buy_price REAL",
            "ALTER TABLE positions ADD COLUMN buy_slippage_pct   REAL",
        ]
        for sql in migrations:
            try:
                c.execute(sql)
            except Exception:
                pass  # column already exists — SQLite has no IF NOT EXISTS for ALTER

        conn.commit()
        conn.close()
    print("Database initialised.")


# ── Candles ───────────────────────────────────────────────────────────────────

def save_candle(coin: str, timeframe: str, ohlcv: dict):
    with _lock:
        conn = _conn()
        conn.execute("""
            INSERT INTO candles
                (coin, timeframe, open_time, open, high, low, close, volume,
                 ma20, rsi14, bb_upper, bb_lower, bb_mid, volume_ma20,
                 ma_position, bb_position, volume_trend)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(coin, timeframe, open_time) DO UPDATE SET
                open=excluded.open, high=excluded.high, low=excluded.low,
                close=excluded.close, volume=excluded.volume,
                ma20=excluded.ma20, rsi14=excluded.rsi14,
                bb_upper=excluded.bb_upper, bb_lower=excluded.bb_lower,
                bb_mid=excluded.bb_mid, volume_ma20=excluded.volume_ma20,
                ma_position=excluded.ma_position,
                bb_position=excluded.bb_position,
                volume_trend=excluded.volume_trend
        """, (
            coin, timeframe,
            ohlcv.get("open_time", 0),
            ohlcv.get("open"), ohlcv.get("high"), ohlcv.get("low"),
            ohlcv.get("close"), ohlcv.get("volume"),
            ohlcv.get("ma20"), ohlcv.get("rsi14"),
            ohlcv.get("bb_upper"), ohlcv.get("bb_lower"), ohlcv.get("bb_mid"),
            ohlcv.get("volume_ma20"),
            ohlcv.get("ma_position"), ohlcv.get("bb_position"),
            ohlcv.get("volume_trend"),
        ))
        conn.commit()
        conn.close()


def get_candles(coin: str, timeframe: str, limit: int = 50) -> List[dict]:
    with _lock:
        conn = _conn()
        rows = conn.execute("""
            SELECT * FROM candles
            WHERE coin=? AND timeframe=?
            ORDER BY open_time DESC LIMIT ?
        """, (coin, timeframe, limit)).fetchall()
        conn.close()
    return [dict(r) for r in reversed(rows)]


def candles_table_empty() -> bool:
    with _lock:
        conn = _conn()
        row = conn.execute("SELECT COUNT(*) AS cnt FROM candles").fetchone()
        conn.close()
    return row["cnt"] == 0


# ── Trades ────────────────────────────────────────────────────────────────────

def log_trade(trade: dict, *,
              target_crossed_to_trigger_ms: Optional[int] = None,
              trigger_to_filled_ms: Optional[int] = None,
              target_crossed_ts: Optional[str] = None):
    with _lock:
        conn = _conn()
        conn.execute("""
            INSERT INTO trades
                (coin, mode, entry_price, exit_price, quantity, budget_usdt,
                 buy_fee, sell_fee, net_profit, profitable, duration_seconds,
                 entry_rsi, entry_ma_position, entry_bb_position,
                 entry_volume_trend, hour_of_day, day_of_week,
                 timestamp_buy, timestamp_sell, sell_reason, signal_snapshot,
                 target_crossed_to_trigger_ms, trigger_to_filled_ms, target_crossed_ts,
                 intended_buy_price, intended_sell_price, buy_slippage_pct, sell_slippage_pct)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            trade.get("coin"), trade.get("mode"), trade.get("entry_price"),
            trade.get("exit_price"), trade.get("quantity"), trade.get("budget_usdt"),
            trade.get("buy_fee"), trade.get("sell_fee"), trade.get("net_profit"),
            int(trade.get("profitable", 0)), trade.get("duration_seconds"),
            trade.get("entry_rsi"), trade.get("entry_ma_position"),
            trade.get("entry_bb_position"), trade.get("entry_volume_trend"),
            trade.get("hour_of_day"), trade.get("day_of_week"),
            trade.get("timestamp_buy"), trade.get("timestamp_sell"),
            trade.get("sell_reason"), trade.get("signal_snapshot"),
            target_crossed_to_trigger_ms, trigger_to_filled_ms, target_crossed_ts,
            trade.get("intended_buy_price"), trade.get("intended_sell_price"),
            trade.get("buy_slippage_pct"), trade.get("sell_slippage_pct"),
        ))
        conn.commit()
        conn.close()


def get_recent_trades(limit: int = 20) -> List[dict]:
    with _lock:
        conn = _conn()
        rows = conn.execute("""
            SELECT * FROM trades ORDER BY id DESC LIMIT ?
        """, (limit,)).fetchall()
        conn.close()
    return [dict(r) for r in rows]


def record_buy_rejection(coin: str, reason: str, detail: str = None, score: int = None, rsi_value: float = None):
    ts = datetime.now(timezone.utc).isoformat()
    with _lock:
        conn = _conn()
        conn.execute(
            "INSERT INTO buy_rejections (timestamp, coin, reason, detail, score, rsi_value) VALUES (?,?,?,?,?,?)",
            (ts, coin, reason, detail, score, rsi_value)
        )
        conn.execute("DELETE FROM buy_rejections WHERE id NOT IN (SELECT id FROM buy_rejections ORDER BY id DESC LIMIT 50000)")
        conn.commit()
        conn.close()


def get_trades_today_count() -> int:
    today = datetime.now(timezone.utc).date().isoformat()
    with _lock:
        conn = _conn()
        row = conn.execute("""
            SELECT COUNT(*) AS cnt FROM trades
            WHERE timestamp_sell LIKE ?
        """, (f"{today}%",)).fetchone()
        conn.close()
    return row["cnt"] if row else 0


def get_win_rate_per_coin() -> Dict[str, float]:
    with _lock:
        conn = _conn()
        rows = conn.execute("""
            SELECT coin,
                   CAST(SUM(profitable) AS REAL) / COUNT(*) AS win_rate
            FROM trades WHERE exit_price IS NOT NULL
            GROUP BY coin
        """).fetchall()
        conn.close()
    return {r["coin"]: round(r["win_rate"], 3) for r in rows}


def get_avg_duration_per_coin() -> Dict[str, float]:
    with _lock:
        conn = _conn()
        rows = conn.execute("""
            SELECT coin, AVG(duration_seconds) AS avg_dur
            FROM trades WHERE duration_seconds IS NOT NULL
            GROUP BY coin
        """).fetchall()
        conn.close()
    return {r["coin"]: round(r["avg_dur"], 1) for r in rows}


# ── Positions ─────────────────────────────────────────────────────────────────

def save_position(pos: dict) -> Optional[int]:
    with _lock:
        conn = _conn()
        conn.execute("""
            INSERT INTO positions
                (symbol, entry_price, exit_target, quantity, budget_usdt, timestamp, mode,
                 entry_rsi, entry_ma_position, entry_bb_position, entry_volume_trend,
                 buy_fee_usdt, signal_snapshot, intended_buy_price, buy_slippage_pct)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            pos["symbol"], pos["entry_price"], pos.get("exit_target"),
            pos["quantity"], pos["budget_usdt"], pos["timestamp"], pos.get("mode", "paper"),
            pos.get("entry_rsi"), pos.get("entry_ma_position"),
            pos.get("entry_bb_position"), pos.get("entry_volume_trend"),
            pos.get("buy_fee_usdt"), pos.get("signal_snapshot"),
            pos.get("intended_buy_price"), pos.get("buy_slippage_pct"),
        ))
        conn.commit()
        row = conn.execute("SELECT last_insert_rowid() AS id").fetchone()
        conn.close()
        return row["id"] if row else None


def delete_position(position_id: int):
    with _lock:
        conn = _conn()
        conn.execute("DELETE FROM positions WHERE id=?", (position_id,))
        conn.commit()
        conn.close()


def load_positions(mode: str = None) -> List[dict]:
    """Load open positions. Pass mode='live' or mode='paper' to avoid cross-contamination."""
    with _lock:
        conn = _conn()
        if mode:
            rows = conn.execute(
                "SELECT * FROM positions WHERE mode=?", (mode,)
            ).fetchall()
        else:
            rows = conn.execute("SELECT * FROM positions").fetchall()
        conn.close()
    return [dict(r) for r in rows]


def migrate_positions_mode(mode: str):
    """Set mode on all positions that have a wrong/missing mode tag.
    Called once on startup when the filtered query returns nothing but the
    unfiltered table has rows — happens when positions were saved before the
    mode-isolation fix (they default to 'paper' regardless of actual mode).
    """
    with _lock:
        conn = _conn()
        conn.execute(
            "UPDATE positions SET mode=? WHERE mode IS NULL OR mode != ?",
            (mode, mode)
        )
        conn.commit()
        conn.close()


# ── Paper state ───────────────────────────────────────────────────────────────

def save_paper_state(balances: dict):
    with _lock:
        conn = _conn()
        conn.execute("""
            INSERT INTO paper_state (id, balances, updated_at)
            VALUES (1, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(id) DO UPDATE SET
                balances=excluded.balances, updated_at=excluded.updated_at
        """, (json.dumps(balances),))
        conn.commit()
        conn.close()


def load_paper_state() -> Optional[dict]:
    with _lock:
        conn = _conn()
        try:
            row = conn.execute("SELECT balances FROM paper_state WHERE id=1").fetchone()
        except sqlite3.OperationalError:
            conn.close()
            return None
        conn.close()
    if row:
        try:
            return json.loads(row["balances"])
        except Exception:
            return None
    return None


def reset_paper_wallet(starting_usdt: float):
    """Wipe all trades, positions, and activity log. Restore starting USDT balance."""
    with _lock:
        conn = _conn()
        conn.execute("DELETE FROM trades")
        conn.execute("DELETE FROM positions")
        conn.execute("DELETE FROM activity_log")
        conn.execute("""
            INSERT INTO paper_state (id, balances, updated_at)
            VALUES (1, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(id) DO UPDATE SET
                balances=excluded.balances, updated_at=excluded.updated_at
        """, (json.dumps({"USDT": starting_usdt}),))
        # Record the starting balance so session_pnl can be computed as current_total - this value.
        conn.execute("""
            INSERT INTO settings (key, value, updated_at) VALUES ('paper_starting_balance', ?, CURRENT_TIMESTAMP)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
        """, (str(starting_usdt),))
        conn.commit()
        conn.close()


# ── Settings ──────────────────────────────────────────────────────────────────

def save_setting(key: str, value: str):
    with _lock:
        conn = _conn()
        conn.execute("""
            INSERT INTO settings (key, value, updated_at) VALUES (?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
        """, (key, value))
        conn.commit()
        conn.close()


def get_setting(key: str) -> Optional[str]:
    with _lock:
        conn = _conn()
        row = conn.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        conn.close()
    return row["value"] if row else None


def get_realized_pnl(mode: str = "paper") -> float:
    """Return SUM(net_profit) from trades table — the single source of truth for realized P&L."""
    with _lock:
        conn = _conn()
        row = conn.execute(
            "SELECT COALESCE(SUM(net_profit), 0.0) AS total FROM trades WHERE mode=?", (mode,)
        ).fetchone()
        conn.close()
    return float(row["total"]) if row else 0.0


def get_trade_stats(mode: str = "paper") -> dict:
    """Return total/wins/losses/realized_pnl from ALL closed trades (no LIMIT).

    Using len(get_recent_trades(limit=500)) for these counts is wrong when
    the bot has placed >500 trades — the win-rate and total-trades would
    describe only the last 500 rows while realized_pnl covers everything.
    This function uses a single aggregated SQL query so all four numbers
    are always consistent with each other.
    """
    today = datetime.now(timezone.utc).date().isoformat()
    with _lock:
        conn = _conn()
        row = conn.execute("""
            SELECT
                COUNT(*)                                            AS total,
                SUM(CASE WHEN net_profit >  0 THEN 1 ELSE 0 END)  AS wins,
                SUM(CASE WHEN net_profit <= 0 THEN 1 ELSE 0 END)  AS losses,
                COALESCE(SUM(net_profit), 0.0)                    AS realized_pnl,
                SUM(CASE WHEN timestamp_sell LIKE ? THEN 1 ELSE 0 END)          AS trades_today,
                COALESCE(SUM(CASE WHEN timestamp_sell LIKE ? THEN net_profit ELSE 0 END), 0.0)
                                                                   AS today_realized_pnl,
                COALESCE(SUM(CASE WHEN net_profit > 0 THEN net_profit ELSE 0 END), 0.0)
                                                                   AS locked_profit,
                COALESCE(SUM(COALESCE(buy_fee, 0) + COALESCE(sell_fee, 0)), 0.0)
                                                                   AS total_fees
            FROM trades
            WHERE exit_price IS NOT NULL AND mode = ?
        """, (f"{today}%", f"{today}%", mode)).fetchone()
        conn.close()
    if row:
        return {
            "total":               int(row["total"]              or 0),
            "wins":                int(row["wins"]               or 0),
            "losses":              int(row["losses"]             or 0),
            "realized_pnl":        float(row["realized_pnl"]     or 0.0),
            "trades_today":        int(row["trades_today"]       or 0),
            "today_realized_pnl":  float(row["today_realized_pnl"] or 0.0),
            "locked_profit":       float(row["locked_profit"]    or 0.0),
            "total_fees":          float(row["total_fees"]       or 0.0),
        }
    return {
        "total": 0, "wins": 0, "losses": 0, "realized_pnl": 0.0,
        "trades_today": 0, "today_realized_pnl": 0.0,
        "locked_profit": 0.0, "total_fees": 0.0,
    }

def get_trade_stats_all_modes() -> dict:
    """Aggregate closed-trade stats across ALL modes (paper + live combined)."""
    today = datetime.now(timezone.utc).date().isoformat()
    with _lock:
        conn = _conn()
        row = conn.execute("""
            SELECT
                COUNT(*)                                            AS total,
                SUM(CASE WHEN net_profit >  0 THEN 1 ELSE 0 END)  AS wins,
                SUM(CASE WHEN net_profit <= 0 THEN 1 ELSE 0 END)  AS losses,
                COALESCE(SUM(net_profit), 0.0)                    AS realized_pnl,
                SUM(CASE WHEN timestamp_sell LIKE ? THEN 1 ELSE 0 END)          AS trades_today,
                COALESCE(SUM(CASE WHEN timestamp_sell LIKE ? THEN net_profit ELSE 0 END), 0.0)
                                                                   AS today_realized_pnl,
                COALESCE(SUM(COALESCE(buy_fee, 0) + COALESCE(sell_fee, 0)), 0.0)
                                                                   AS total_fees
            FROM trades
            WHERE exit_price IS NOT NULL
        """, (f"{today}%", f"{today}%")).fetchone()
        conn.close()
    if row:
        total = int(row["total"] or 0)
        wins  = int(row["wins"]  or 0)
        return {
            "total":              total,
            "wins":               wins,
            "losses":             int(row["losses"]            or 0),
            "realized_pnl":       float(row["realized_pnl"]   or 0.0),
            "trades_today":       int(row["trades_today"]      or 0),
            "today_realized_pnl": float(row["today_realized_pnl"] or 0.0),
            "total_fees":         float(row["total_fees"]      or 0.0),
            "win_rate":           round(wins / total, 3) if total else 0.0,
        }
    return {
        "total": 0, "wins": 0, "losses": 0, "realized_pnl": 0.0,
        "trades_today": 0, "today_realized_pnl": 0.0,
        "total_fees": 0.0, "win_rate": 0.0,
    }


def get_stats_for_range(mode: str = "paper", date_from: str = None, date_to: str = None) -> dict:
    """Return aggregated trade stats for an optional date range.
    When both params are None, returns all-time stats (same as get_trade_stats totals).
    """
    parts = ["exit_price IS NOT NULL", "mode = ?"]
    params: list = [mode]
    if date_from:
        parts.append("DATE(timestamp_sell) >= ?")
        params.append(date_from)
    if date_to:
        parts.append("DATE(timestamp_sell) <= ?")
        params.append(date_to)
    where = " AND ".join(parts)

    with _lock:
        conn = _conn()
        row = conn.execute(f"""
            SELECT
                COUNT(*)                                            AS total,
                SUM(CASE WHEN net_profit >  0 THEN 1 ELSE 0 END)  AS wins,
                SUM(CASE WHEN net_profit <= 0 THEN 1 ELSE 0 END)  AS losses,
                COALESCE(SUM(net_profit), 0.0)                    AS realized_pnl,
                COALESCE(SUM(CASE WHEN net_profit > 0 THEN net_profit ELSE 0 END), 0.0)
                                                                   AS locked_profit,
                COALESCE(SUM(COALESCE(buy_fee, 0) + COALESCE(sell_fee, 0)), 0.0)
                                                                   AS total_fees
            FROM trades
            WHERE {where}
        """, params).fetchone()
        conn.close()

    if row:
        total = int(row["total"] or 0)
        wins  = int(row["wins"]  or 0)
        return {
            "total":         total,
            "wins":          wins,
            "losses":        int(row["losses"]       or 0),
            "realized_pnl":  float(row["realized_pnl"]  or 0.0),
            "locked_profit": float(row["locked_profit"] or 0.0),
            "total_fees":    float(row["total_fees"]    or 0.0),
            "win_rate":      round(wins / total, 3) if total else 0.0,
        }
    return {"total": 0, "wins": 0, "losses": 0, "realized_pnl": 0.0,
            "locked_profit": 0.0, "total_fees": 0.0, "win_rate": 0.0}


def get_trades_for_range(
    mode: str = "paper",
    date_from: str = None,
    date_to: str = None,
    limit: int = 500,
) -> List[dict]:
    """Return closed trades filtered by date range."""
    parts = ["exit_price IS NOT NULL", "mode = ?"]
    params: list = [mode]
    if date_from:
        parts.append("DATE(timestamp_sell) >= ?")
        params.append(date_from)
    if date_to:
        parts.append("DATE(timestamp_sell) <= ?")
        params.append(date_to)
    params.append(limit)
    where = " AND ".join(parts)

    with _lock:
        conn = _conn()
        rows = conn.execute(f"""
            SELECT * FROM trades WHERE {where}
            ORDER BY id DESC LIMIT ?
        """, params).fetchall()
        conn.close()
    return [dict(r) for r in rows]


def get_futures_trade_stats() -> dict:
    """Return aggregated stats for all futures trades."""
    with _lock:
        conn = _conn()
        row = conn.execute("""
            SELECT
                COUNT(*)                                           AS total,
                SUM(CASE WHEN net_profit > 0 THEN 1 ELSE 0 END)  AS wins,
                COALESCE(SUM(net_profit), 0.0)                   AS total_pnl,
                COALESCE(SUM(COALESCE(buy_fee, 0) + COALESCE(sell_fee, 0)), 0.0) AS total_fees
            FROM futures_trades
        """).fetchone()
        conn.close()
    if row:
        total = int(row["total"] or 0)
        wins  = int(row["wins"]  or 0)
        return {
            "total":      total,
            "wins":       wins,
            "total_pnl":  float(row["total_pnl"]  or 0.0),
            "total_fees": float(row["total_fees"] or 0.0),
            "win_rate":   round(wins / total * 100, 1) if total else 0.0,
        }
    return {"total": 0, "wins": 0, "total_pnl": 0.0, "total_fees": 0.0, "win_rate": 0.0}


# ── Activity log ──────────────────────────────────────────────────────────────

def log_activity(message: str, level: str = "info"):
    ts = datetime.now(timezone.utc).isoformat()
    with _lock:
        conn = _conn()
        conn.execute("""
            INSERT INTO activity_log (message, level, timestamp)
            VALUES (?, ?, ?)
        """, (message, level, ts))
        # Keep only the last 500 entries so the table never bloats
        conn.execute("""
            DELETE FROM activity_log
            WHERE id NOT IN (
                SELECT id FROM activity_log ORDER BY id DESC LIMIT 5000
            )
        """)
        conn.commit()
        conn.close()


def get_activity_log(limit: int = 100) -> List[dict]:
    with _lock:
        conn = _conn()
        rows = conn.execute("""
            SELECT id, message, level, timestamp
            FROM activity_log
            ORDER BY id DESC LIMIT ?
        """, (limit,)).fetchall()
        conn.close()
    return [dict(r) for r in rows]


# ── Patterns ──────────────────────────────────────────────────────────────────

def upsert_pattern(p: dict):
    with _lock:
        conn = _conn()
        existing = conn.execute(
            "SELECT * FROM patterns WHERE pattern_key=?", (p["pattern_key"],)
        ).fetchone()
        if existing:
            old_count = existing["occurrence_count"]
            new_count = old_count + 1
            profitable = 1 if p.get("outcome_profitable") else 0
            new_conf = (existing["confidence_score"] * old_count + profitable) / new_count
            new_profit = (
                ((existing["avg_profit_pct"] or 0) * old_count + (p.get("avg_profit_pct") or 0))
                / new_count
            )
            conn.execute("""
                UPDATE patterns SET
                    occurrence_count=?, confidence_score=?, avg_profit_pct=?,
                    outcome_profitable=?, last_seen=CURRENT_TIMESTAMP
                WHERE pattern_key=?
            """, (new_count, new_conf, new_profit, profitable, p["pattern_key"]))
        else:
            conn.execute("""
                INSERT INTO patterns
                    (coin, pattern_key, rsi_range, bb_position, volume_trend,
                     ma_position, outcome_profitable, avg_profit_pct, confidence_score)
                VALUES (?,?,?,?,?,?,?,?,?)
            """, (
                p.get("coin"), p["pattern_key"], p.get("rsi_range"),
                p.get("bb_position"), p.get("volume_trend"), p.get("ma_position"),
                int(p.get("outcome_profitable", 0)), p.get("avg_profit_pct", 0),
                1.0 if p.get("outcome_profitable") else 0.0,
            ))
        conn.commit()
        conn.close()


def get_patterns(min_occurrences: int = 3) -> List[dict]:
    with _lock:
        conn = _conn()
        rows = conn.execute("""
            SELECT * FROM patterns
            WHERE occurrence_count >= ?
            ORDER BY confidence_score DESC
        """, (min_occurrences,)).fetchall()
        conn.close()
    return [dict(r) for r in rows]


# ── Decisions ─────────────────────────────────────────────────────────────────

def log_decision(d: dict):
    with _lock:
        conn = _conn()
        conn.execute("""
            INSERT INTO decisions (action, coin, confidence, reason, pattern_observed)
            VALUES (?,?,?,?,?)
        """, (d.get("action"), d.get("coin"), d.get("confidence"),
              d.get("reason"), d.get("pattern_observed")))
        conn.commit()
        conn.close()


# ── Futures state ─────────────────────────────────────────────────────────────

def save_futures_state(balances: dict):
    with _lock:
        conn = _conn()
        conn.execute("""
            INSERT INTO futures_state (id, balances, updated_at)
            VALUES (1, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(id) DO UPDATE SET
                balances=excluded.balances, updated_at=excluded.updated_at
        """, (json.dumps(balances),))
        conn.commit()
        conn.close()


def load_futures_state() -> Optional[dict]:
    with _lock:
        conn = _conn()
        try:
            row = conn.execute("SELECT balances FROM futures_state WHERE id=1").fetchone()
        except sqlite3.OperationalError:
            conn.close()
            return None
        conn.close()
    if row:
        try:
            return json.loads(row["balances"])
        except Exception:
            return None
    return None


# ── Futures positions ─────────────────────────────────────────────────────────

def save_futures_position(pos: dict) -> Optional[int]:
    with _lock:
        conn = _conn()
        conn.execute("""
            INSERT INTO futures_positions
                (symbol, direction, entry_price, quantity, margin_usdt, leverage,
                 take_profit, stop_loss, sl_enabled, liquidation_price, funding_paid, timestamp)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            pos["symbol"], pos["direction"], pos["entry_price"],
            pos["quantity"], pos["margin_usdt"], pos.get("leverage", 5),
            pos.get("take_profit"), pos.get("stop_loss"),
            1 if pos.get("sl_enabled", True) else 0,
            pos.get("liquidation_price"), pos.get("funding_paid", 0.0),
            pos["timestamp"],
        ))
        conn.commit()
        row = conn.execute("SELECT last_insert_rowid() AS id").fetchone()
        conn.close()
        return row["id"] if row else None


def delete_futures_position(position_id: int):
    with _lock:
        conn = _conn()
        conn.execute("DELETE FROM futures_positions WHERE id=?", (position_id,))
        conn.commit()
        conn.close()


def load_futures_positions() -> List[dict]:
    with _lock:
        conn = _conn()
        rows = conn.execute("SELECT * FROM futures_positions").fetchall()
        conn.close()
    return [dict(r) for r in rows]


# ── Futures trades ────────────────────────────────────────────────────────────

def log_futures_trade(trade: dict):
    with _lock:
        conn = _conn()
        conn.execute("""
            INSERT INTO futures_trades
                (symbol, direction, entry_price, exit_price, quantity, margin_usdt,
                 leverage, net_profit, profitable, funding_paid, buy_fee, sell_fee,
                 duration_seconds, timestamp_open, timestamp_close)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            trade.get("symbol"), trade.get("direction"),
            trade.get("entry_price"), trade.get("exit_price"),
            trade.get("quantity"), trade.get("margin_usdt"),
            trade.get("leverage"), trade.get("net_profit"),
            int(trade.get("profitable", 0)), trade.get("funding_paid", 0.0),
            trade.get("buy_fee", 0.0), trade.get("sell_fee", 0.0),
            trade.get("duration_seconds"),
            trade.get("timestamp_open"), trade.get("timestamp_close"),
        ))
        conn.commit()
        conn.close()


def get_recent_futures_trades(limit: int = 20) -> List[dict]:
    with _lock:
        conn = _conn()
        rows = conn.execute("""
            SELECT * FROM futures_trades ORDER BY id DESC LIMIT ?
        """, (limit,)).fetchall()
        conn.close()
    return [dict(r) for r in rows]


def import_trades(trades: list) -> int:
    """Insert trade records from a backup export, skipping rows that already exist
    (matched on coin + timestamp_buy + mode). Returns count of rows inserted."""
    _COLS = (
        "coin", "mode", "entry_price", "exit_price", "quantity", "budget_usdt",
        "buy_fee", "sell_fee", "net_profit", "profitable", "duration_seconds",
        "entry_rsi", "entry_ma_position", "entry_bb_position", "entry_volume_trend",
        "hour_of_day", "day_of_week", "timestamp_buy", "timestamp_sell",
    )
    inserted = 0
    with _lock:
        conn = _conn()
        for t in trades:
            vals = tuple(t.get(c) for c in _COLS)
            try:
                conn.execute(
                    f"INSERT OR IGNORE INTO trades ({', '.join(_COLS)}) VALUES ({', '.join(['?']*len(_COLS))})",
                    vals,
                )
                if conn.execute("SELECT changes()").fetchone()[0]:
                    inserted += 1
            except Exception:
                pass
        conn.commit()
        conn.close()
    return inserted
