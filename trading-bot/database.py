"""SQLite database layer — thread-safe, no async required."""

import sqlite3
import threading
import json
from datetime import datetime
from typing import Optional, Dict, List, Any

DB_PATH = "bot.db"
_lock = threading.Lock()


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
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                coin        TEXT NOT NULL,
                timeframe   TEXT NOT NULL,
                open_time   INTEGER NOT NULL,
                open        REAL,
                high        REAL,
                low         REAL,
                close       REAL,
                volume      REAL,
                ma20        REAL,
                rsi14       REAL,
                bb_upper    REAL,
                bb_lower    REAL,
                bb_mid      REAL,
                volume_ma20 REAL,
                saved_at    TEXT DEFAULT CURRENT_TIMESTAMP,
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
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol      TEXT,
                entry_price REAL,
                quantity    REAL,
                budget_usdt REAL,
                timestamp   TEXT,
                mode        TEXT
            );
        """)
        conn.commit()
        conn.close()
    print("Database initialised.")


def save_candle(coin: str, timeframe: str, ohlcv: dict):
    with _lock:
        conn = _conn()
        c = conn.cursor()
        c.execute("""
            INSERT INTO candles
                (coin, timeframe, open_time, open, high, low, close, volume,
                 ma20, rsi14, bb_upper, bb_lower, bb_mid, volume_ma20)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(coin, timeframe, open_time) DO UPDATE SET
                open=excluded.open, high=excluded.high, low=excluded.low,
                close=excluded.close, volume=excluded.volume,
                ma20=excluded.ma20, rsi14=excluded.rsi14,
                bb_upper=excluded.bb_upper, bb_lower=excluded.bb_lower,
                bb_mid=excluded.bb_mid, volume_ma20=excluded.volume_ma20
        """, (
            coin, timeframe,
            ohlcv.get("open_time", 0),
            ohlcv.get("open"), ohlcv.get("high"), ohlcv.get("low"),
            ohlcv.get("close"), ohlcv.get("volume"),
            ohlcv.get("ma20"), ohlcv.get("rsi14"),
            ohlcv.get("bb_upper"), ohlcv.get("bb_lower"), ohlcv.get("bb_mid"),
            ohlcv.get("volume_ma20"),
        ))
        conn.commit()
        conn.close()


def get_candles(coin: str, timeframe: str, limit: int = 50) -> List[dict]:
    with _lock:
        conn = _conn()
        c = conn.cursor()
        rows = c.execute("""
            SELECT * FROM candles
            WHERE coin=? AND timeframe=?
            ORDER BY open_time DESC LIMIT ?
        """, (coin, timeframe, limit)).fetchall()
        conn.close()
    return [dict(r) for r in reversed(rows)]


def log_trade(trade: dict):
    with _lock:
        conn = _conn()
        c = conn.cursor()
        c.execute("""
            INSERT INTO trades
                (coin, mode, entry_price, exit_price, quantity, budget_usdt,
                 buy_fee, sell_fee, net_profit, profitable, duration_seconds,
                 entry_rsi, entry_ma_position, entry_bb_position,
                 entry_volume_trend, hour_of_day, day_of_week,
                 timestamp_buy, timestamp_sell)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            trade.get("coin"), trade.get("mode"), trade.get("entry_price"),
            trade.get("exit_price"), trade.get("quantity"), trade.get("budget_usdt"),
            trade.get("buy_fee"), trade.get("sell_fee"), trade.get("net_profit"),
            int(trade.get("profitable", 0)), trade.get("duration_seconds"),
            trade.get("entry_rsi"), trade.get("entry_ma_position"),
            trade.get("entry_bb_position"), trade.get("entry_volume_trend"),
            trade.get("hour_of_day"), trade.get("day_of_week"),
            trade.get("timestamp_buy"), trade.get("timestamp_sell"),
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
            new_profit = ((existing["avg_profit_pct"] or 0) * old_count + (p.get("avg_profit_pct") or 0)) / new_count
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


def save_paper_state(balances: dict):
    with _lock:
        conn = _conn()
        conn.execute("""
            INSERT INTO paper_state (id, balances, updated_at)
            VALUES (1, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(id) DO UPDATE SET balances=excluded.balances, updated_at=excluded.updated_at
        """, (json.dumps(balances),))
        conn.commit()
        conn.close()


def load_paper_state() -> Optional[dict]:
    with _lock:
        conn = _conn()
        row = conn.execute("SELECT balances FROM paper_state WHERE id=1").fetchone()
        conn.close()
    if row:
        try:
            return json.loads(row["balances"])
        except Exception:
            return None
    return None


# --- Position persistence ---

def save_position(pos: dict):
    with _lock:
        conn = _conn()
        conn.execute("""
            INSERT INTO positions (symbol, entry_price, quantity, budget_usdt, timestamp, mode)
            VALUES (?,?,?,?,?,?)
        """, (pos["symbol"], pos["entry_price"], pos["quantity"],
              pos["budget_usdt"], pos["timestamp"], pos.get("mode", "paper")))
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


def load_positions() -> List[dict]:
    with _lock:
        conn = _conn()
        rows = conn.execute("SELECT * FROM positions").fetchall()
        conn.close()
    return [dict(r) for r in rows]


def candles_table_empty() -> bool:
    with _lock:
        conn = _conn()
        row = conn.execute("SELECT COUNT(*) AS cnt FROM candles").fetchone()
        conn.close()
    return row["cnt"] == 0
