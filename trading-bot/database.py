"""SQLite database layer — thread-safe, no async required."""

import hashlib
import os
import sqlite3
import threading
import json
from datetime import datetime, timezone
from typing import Optional, Dict, List, Any

# DATA_DIR stores the SQLite database and strategy.json.
# Resolution order (never inside the git working tree unless nothing else works):
#   DATA_DIR env var → /opt/tradebot/data (if it already exists) → /data
#   (Railway volume, if it already exists) → ~/.wolfbot/data (created) →
#   script directory (last resort, with a loud warning — deploys/clones can
#   clobber a DB stored inside the checkout).
def _dir_writable(candidate: str, create: bool = True) -> bool:
    try:
        if create:
            os.makedirs(candidate, exist_ok=True)
        elif not os.path.isdir(candidate):
            return False
        probe = os.path.join(candidate, ".write_probe")
        with open(probe, "w") as _f:
            _f.write("ok")
        os.remove(probe)
        return True
    except OSError:
        return False


def _resolve_data_dir() -> str:
    _script_dir = os.path.dirname(os.path.abspath(__file__))

    env_dir = os.getenv("DATA_DIR")
    if env_dir and _dir_writable(env_dir, create=True):
        return env_dir

    # Pre-existing system locations — never created here (need root / a mount)
    for candidate in ("/opt/tradebot/data", "/data"):
        if _dir_writable(candidate, create=False):
            return candidate

    home_dir = os.path.join(os.path.expanduser("~"), ".wolfbot", "data")
    if _dir_writable(home_dir, create=True):
        return home_dir

    print("[Database] WARNING: falling back to a data dir INSIDE the git "
          f"checkout ({_script_dir}) — a redeploy, fresh clone, or 'git clean' "
          "can destroy the database. Set DATA_DIR to a persistent path.")
    return _script_dir

_DATA_DIR = _resolve_data_dir()
DB_PATH = os.path.join(_DATA_DIR, "bot.db")

# One-time migration: older builds stored bot.db at <repo>/data/bot.db (inside
# the git working tree). If that file exists and the new location is empty,
# COPY it over so history survives — the old file is left in place untouched.
def _migrate_legacy_db():
    _script_dir = os.path.dirname(os.path.abspath(__file__))
    legacy = os.path.abspath(os.path.join(_script_dir, "..", "data", "bot.db"))
    if legacy == os.path.abspath(DB_PATH):
        return
    if os.path.exists(legacy) and not os.path.exists(DB_PATH):
        try:
            import shutil
            shutil.copy2(legacy, DB_PATH)
            print(f"[Database] Migrated existing database: copied {legacy} → {DB_PATH} "
                  "(old file kept as-is)")
        except OSError as e:
            print(f"[Database] WARNING: could not migrate legacy DB {legacy} → {DB_PATH}: {e}")

_migrate_legacy_db()
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
    # G4.5 — concurrency/durability pragmas. journal_mode=WAL is persisted in
    # the DB header (a no-op after the first connection); busy_timeout and
    # synchronous are per-connection, so they are set on every connect. All are
    # best-effort: a read-only / locked DB must never break connection open.
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=5000")
        conn.execute("PRAGMA synchronous=NORMAL")
    except Exception:
        pass
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

            -- G4.5 — query indexes for the analytics/prune paths.
            CREATE INDEX IF NOT EXISTS idx_trades_timestamp_sell ON trades(timestamp_sell);
            CREATE INDEX IF NOT EXISTS idx_trades_coin           ON trades(coin);

            CREATE TABLE IF NOT EXISTS phantom_alerts (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp   TEXT NOT NULL,
                symbol      TEXT NOT NULL,
                db_qty      REAL,
                binance_qty REAL,
                resolved    INTEGER DEFAULT 0
            );

            -- Phase 1 measurement layer (spec §1.2): raw kline store used by
            -- the backtester / backfill CLI. open_time is Binance epoch-ms.
            CREATE TABLE IF NOT EXISTS klines (
                symbol    TEXT NOT NULL,
                interval  TEXT NOT NULL,
                open_time INTEGER NOT NULL,
                o         REAL,
                h         REAL,
                l         REAL,
                c         REAL,
                v         REAL,
                quote_v   REAL,
                PRIMARY KEY (symbol, interval, open_time)
            );

            -- Phase 1 measurement layer (spec §1.3): one row per evaluated
            -- entry opportunity (executed or not) with full JSON context.
            CREATE TABLE IF NOT EXISTS entry_snapshots (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                ts          REAL,
                symbol      TEXT,
                origin      TEXT,
                executed    INTEGER,
                price       REAL,
                raw_json    TEXT,
                gates_json  TEXT,
                config_hash TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_entry_snapshots_ts     ON entry_snapshots(ts);
            CREATE INDEX IF NOT EXISTS idx_entry_snapshots_symbol ON entry_snapshots(symbol);

            -- Part S — unified EV training-data store. Both live and
            -- paper-shadow entry paths write labeled outcomes here; the EV
            -- retrainer (ev_model.py) reads them. Hard-capped to 100k rows.
            CREATE TABLE IF NOT EXISTS training_samples (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                ts           REAL,
                mode         TEXT,
                symbol       TEXT,
                features_json TEXT,
                label        INTEGER,
                realized_r   REAL
            );
            CREATE INDEX IF NOT EXISTS idx_training_samples_ts   ON training_samples(ts);
            CREATE INDEX IF NOT EXISTS idx_training_samples_mode ON training_samples(mode);

            -- WolfBot Shadow-Lab — rich paper-shadow outcome store. One row per
            -- closed shadow trade; get_paper_summary aggregates it into the
            -- copy-paste "results summary". Hard-capped to the newest 200k rows.
            CREATE TABLE IF NOT EXISTS paper_trades (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                ts         REAL,
                symbol     TEXT,
                wolfscore  REAL,
                regime     TEXT,
                exit_type  TEXT,
                entry_px   REAL,
                exit_px    REAL,
                pnl        REAL,
                realized_r REAL,
                hold_sec   REAL,
                label      INTEGER
            );
            CREATE INDEX IF NOT EXISTS idx_paper_trades_ts     ON paper_trades(ts);
            CREATE INDEX IF NOT EXISTS idx_paper_trades_symbol ON paper_trades(symbol);
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
            # save_position INSERTs signal_snapshot but the base CREATE TABLE
            # never had the column — fresh databases failed on the first buy.
            "ALTER TABLE positions ADD COLUMN signal_snapshot    TEXT",
            # Phase 1 exit-label / fee-attribution columns (spec §1.3)
            "ALTER TABLE trades ADD COLUMN exit_label     TEXT",
            "ALTER TABLE trades ADD COLUMN slippage_bps   REAL",
            "ALTER TABLE trades ADD COLUMN entry_fee_usdt REAL",
            "ALTER TABLE trades ADD COLUMN exit_fee_usdt  REAL",
            "ALTER TABLE trades ADD COLUMN hold_time_sec  REAL",
            "ALTER TABLE trades ADD COLUMN origin         TEXT",
        ]
        for sql in migrations:
            try:
                c.execute(sql)
            except Exception:
                pass  # column already exists — SQLite has no IF NOT EXISTS for ALTER

        # ── Phase 5 §5.1 — config history (versioned strategy.json audit) ────
        try:
            c.execute("""
                CREATE TABLE IF NOT EXISTS config_history (
                    version     INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts          REAL,
                    actor       TEXT,
                    diff_json   TEXT,
                    full_json   TEXT,
                    config_hash TEXT
                )
            """)
            c.execute("CREATE INDEX IF NOT EXISTS idx_config_history_ts ON config_history(ts)")
        except Exception as e:
            print(f"[Database] WARNING: config_history table not created: {e}")

        # ── Trade dedupe index ────────────────────────────────────────────────
        # import_trades relies on INSERT OR IGNORE, which is a no-op without a
        # UNIQUE constraint. Clean up existing exact-duplicate rows first
        # (keep lowest id) so index creation cannot fail on legacy data.
        try:
            c.execute("""
                DELETE FROM trades WHERE id NOT IN (
                    SELECT MIN(id) FROM trades
                    GROUP BY coin, mode, entry_price, exit_price, quantity,
                             timestamp_buy, timestamp_sell
                )
            """)
            c.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_trades_dedupe
                ON trades(coin, mode, entry_price, exit_price, quantity,
                          timestamp_buy, timestamp_sell)
            """)
        except Exception as e:
            print(f"[Database] WARNING: trade dedupe index not created: {e}")

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


# ── Klines (Phase 1 measurement layer, spec §1.2) ─────────────────────────────

_KLINE_COLS = ("open_time", "o", "h", "l", "c", "v", "quote_v")


def _num(x) -> Optional[float]:
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def _kline_to_tuple(row) -> Optional[tuple]:
    """Normalise one kline into (open_time, o, h, l, c, v, quote_v).

    Accepts either a raw Binance kline array
    [open_time, open, high, low, close, volume, close_time, quote_volume, ...]
    or a dict with short keys {open_time, o, h, l, c, v, quote_v} (long-form
    aliases open/high/low/close/volume/quote_volume also accepted).
    Returns None for malformed rows."""
    try:
        if isinstance(row, dict):
            open_time = row.get("open_time", row.get("t"))
            o = row.get("o", row.get("open"))
            h = row.get("h", row.get("high"))
            l = row.get("l", row.get("low"))
            c = row.get("c", row.get("close"))
            v = row.get("v", row.get("volume"))
            quote_v = row.get("quote_v", row.get("quote_volume", row.get("q")))
        else:  # Binance kline array (list/tuple)
            open_time = row[0]
            o, h, l, c, v = row[1], row[2], row[3], row[4], row[5]
            quote_v = row[7] if len(row) > 7 else None
        return (int(open_time), _num(o), _num(h), _num(l), _num(c),
                _num(v), _num(quote_v))
    except (TypeError, ValueError, IndexError, KeyError):
        return None


def save_klines(symbol: str, interval: str, rows: list) -> int:
    """INSERT OR REPLACE klines for (symbol, interval). `rows` is a list of
    Binance kline arrays or dicts (see _kline_to_tuple). Duplicate open_times
    replace the stored row (PK symbol/interval/open_time). Returns the number
    of rows written; malformed rows are skipped."""
    tuples = []
    for r in rows or []:
        t = _kline_to_tuple(r)
        if t is not None:
            tuples.append((symbol, interval) + t)
    if not tuples:
        return 0
    with _lock:
        conn = _conn()
        conn.executemany("""
            INSERT OR REPLACE INTO klines
                (symbol, interval, open_time, o, h, l, c, v, quote_v)
            VALUES (?,?,?,?,?,?,?,?,?)
        """, tuples)
        conn.commit()
        conn.close()
    return len(tuples)


def get_klines(symbol: str, interval: str,
               start_ms: Optional[int] = None,
               end_ms: Optional[int] = None,
               limit: Optional[int] = None) -> List[dict]:
    """Return klines as dicts {open_time,o,h,l,c,v,quote_v} in ASCENDING
    open_time order. start_ms/end_ms are inclusive epoch-ms bounds. When
    `limit` is given, the MOST RECENT `limit` rows inside the range are
    returned (still ascending) — the useful shape for indicator warm-up."""
    where = ["symbol = ?", "interval = ?"]
    params: list = [symbol, interval]
    if start_ms is not None:
        where.append("open_time >= ?")
        params.append(int(start_ms))
    if end_ms is not None:
        where.append("open_time <= ?")
        params.append(int(end_ms))
    sql = f"SELECT open_time, o, h, l, c, v, quote_v FROM klines WHERE {' AND '.join(where)}"
    with _lock:
        conn = _conn()
        if limit is not None:
            rows = conn.execute(sql + " ORDER BY open_time DESC LIMIT ?",
                                params + [int(limit)]).fetchall()
            rows = list(reversed(rows))
        else:
            rows = conn.execute(sql + " ORDER BY open_time ASC", params).fetchall()
        conn.close()
    return [dict(r) for r in rows]


def prune_klines(retention_days: float) -> int:
    """Delete klines with open_time older than now - retention_days.
    Returns the number of rows deleted."""
    import time as _t
    cutoff_ms = int((_t.time() - float(retention_days) * 86400.0) * 1000)
    with _lock:
        conn = _conn()
        cur = conn.execute("DELETE FROM klines WHERE open_time < ?", (cutoff_ms,))
        deleted = cur.rowcount if cur.rowcount is not None and cur.rowcount >= 0 else 0
        conn.commit()
        conn.close()
    return deleted


def prune_klines_from_config() -> int:
    """Prune klines using strategy.json `data.kline_retention_days` (default
    180; top-level `kline_retention_days` accepted as fallback). Reads the
    file defensively — any problem means the 180-day default. A later agent
    schedules this; it is safe to call any time. Returns rows deleted."""
    days = 180.0
    try:
        import config as _cfg
        with open(_cfg.STRATEGY_FILE, "r") as f:
            s = json.load(f)
        raw = None
        if isinstance(s, dict):
            d = s.get("data")
            if isinstance(d, dict):
                raw = d.get("kline_retention_days")
            if raw is None:
                raw = s.get("kline_retention_days")
        if raw is not None:
            days = max(1.0, float(raw))
    except Exception:
        days = 180.0
    return prune_klines(days)


def prune_buy_rejections(retention_days: float) -> int:
    """Delete buy_rejections older than now - retention_days. The timestamp
    column is an ISO-8601 UTC string, so an ISO cutoff string compares
    correctly. Guarded/CREATE-safe: missing table → 0. Returns rows deleted."""
    import time as _t
    cutoff_iso = datetime.fromtimestamp(
        _t.time() - float(retention_days) * 86400.0, tz=timezone.utc).isoformat()
    try:
        with _lock:
            conn = _conn()
            try:
                conn.execute("""CREATE TABLE IF NOT EXISTS buy_rejections (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TEXT NOT NULL,
                    coin TEXT NOT NULL, reason TEXT NOT NULL, detail TEXT,
                    score INTEGER, rsi_value REAL)""")
                cur = conn.execute(
                    "DELETE FROM buy_rejections WHERE timestamp < ?", (cutoff_iso,))
                deleted = cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
                conn.commit()
            finally:
                conn.close()
        return deleted
    except Exception as e:
        print(f"[Database] prune_buy_rejections failed: {e}")
        return 0


def prune_entry_snapshots(retention_days: float) -> int:
    """Delete entry_snapshots older than now - retention_days. The ts column is
    an epoch-seconds float. Guarded/CREATE-safe. Returns rows deleted."""
    import time as _t
    cutoff = _t.time() - float(retention_days) * 86400.0
    try:
        with _lock:
            conn = _conn()
            try:
                conn.execute("""CREATE TABLE IF NOT EXISTS entry_snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL, symbol TEXT,
                    origin TEXT, executed INTEGER, price REAL, raw_json TEXT,
                    gates_json TEXT, config_hash TEXT)""")
                cur = conn.execute(
                    "DELETE FROM entry_snapshots WHERE ts < ?", (cutoff,))
                deleted = cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
                # Hard row cap (like buy_rejections) so the table can't balloon
                # between daily prunes under heavy rejection volume — this is what
                # a bulk read (edge report) would otherwise parse into memory.
                cur2 = conn.execute(
                    "DELETE FROM entry_snapshots WHERE id NOT IN "
                    "(SELECT id FROM entry_snapshots ORDER BY id DESC LIMIT 50000)")
                deleted += cur2.rowcount if cur2.rowcount and cur2.rowcount > 0 else 0
                conn.commit()
            finally:
                conn.close()
        return deleted
    except Exception as e:
        print(f"[Database] prune_entry_snapshots failed: {e}")
        return 0


def prune_evaluations_from_config() -> dict:
    """Prune buy_rejections + entry_snapshots using strategy.json
    `data.eval_retention_days` (default 14). Reads the file defensively — any
    problem means the 14-day default. Returns {buy_rejections, entry_snapshots}
    deleted counts. Safe to call any time (daily maintenance loop)."""
    days = 14.0
    try:
        import config as _cfg
        with open(_cfg.STRATEGY_FILE, "r") as f:
            s = json.load(f)
        raw = None
        if isinstance(s, dict):
            d = s.get("data")
            if isinstance(d, dict):
                raw = d.get("eval_retention_days")
        if raw is not None:
            days = max(1.0, float(raw))
    except Exception:
        days = 14.0
    return {
        "buy_rejections":  prune_buy_rejections(days),
        "entry_snapshots": prune_entry_snapshots(days),
    }


def kline_coverage(symbol: str, interval: str) -> dict:
    """Return {"first_ms", "last_ms", "count"} for stored (symbol, interval)
    klines. first_ms/last_ms are None when count == 0."""
    with _lock:
        conn = _conn()
        row = conn.execute("""
            SELECT MIN(open_time) AS first_ms,
                   MAX(open_time) AS last_ms,
                   COUNT(*)       AS cnt
            FROM klines WHERE symbol = ? AND interval = ?
        """, (symbol, interval)).fetchone()
        conn.close()
    if row and row["cnt"]:
        return {"first_ms": int(row["first_ms"]), "last_ms": int(row["last_ms"]),
                "count": int(row["cnt"])}
    return {"first_ms": None, "last_ms": None, "count": 0}


# ── Entry snapshots (Phase 1 measurement layer, spec §1.3) ────────────────────

def save_entry_snapshot(symbol: str, origin: str, executed, price,
                        raw: Optional[dict] = None,
                        gates: Optional[dict] = None,
                        config_hash: Optional[str] = None) -> Optional[int]:
    """Record one evaluated entry opportunity (whether or not it executed).

    `raw` — full signal/indicator context dict; `gates` — per-gate pass/fail
    dict. Both are JSON-serialised (non-serialisable values become str).
    `executed` is coerced to 0/1. Returns the new row id."""
    import time as _t
    ts = _t.time()
    try:
        raw_json = json.dumps(raw if raw is not None else {}, default=str)
    except Exception:
        raw_json = "{}"
    try:
        gates_json = json.dumps(gates if gates is not None else {}, default=str)
    except Exception:
        gates_json = "{}"
    with _lock:
        conn = _conn()
        cur = conn.execute("""
            INSERT INTO entry_snapshots
                (ts, symbol, origin, executed, price, raw_json, gates_json, config_hash)
            VALUES (?,?,?,?,?,?,?,?)
        """, (ts, symbol, origin, int(bool(executed)),
              _num(price), raw_json, gates_json, config_hash))
        conn.commit()
        row_id = cur.lastrowid
        conn.close()
    return row_id


def get_entry_snapshots(since_ts: Optional[float] = None,
                        symbol: Optional[str] = None,
                        executed: Optional[bool] = None,
                        limit: int = 500) -> List[dict]:
    """Return entry snapshots newest-first. `raw`/`gates` are parsed back to
    dicts (unparseable JSON → {}). `executed=None` returns both outcomes."""
    where = ["1=1"]
    params: list = []
    if since_ts is not None:
        where.append("ts >= ?")
        params.append(float(since_ts))
    if symbol is not None:
        where.append("symbol = ?")
        params.append(symbol)
    if executed is not None:
        where.append("executed = ?")
        params.append(int(bool(executed)))
    params.append(int(limit))
    with _lock:
        conn = _conn()
        rows = conn.execute(f"""
            SELECT id, ts, symbol, origin, executed, price,
                   raw_json, gates_json, config_hash
            FROM entry_snapshots
            WHERE {' AND '.join(where)}
            ORDER BY id DESC LIMIT ?
        """, params).fetchall()
        conn.close()
    out = []
    for r in rows:
        d = dict(r)
        for src, dst in (("raw_json", "raw"), ("gates_json", "gates")):
            try:
                d[dst] = json.loads(d.pop(src) or "{}")
            except Exception:
                d[dst] = {}
        out.append(d)
    return out


def config_hash(strategy: dict) -> str:
    """Deterministic 12-hex-char fingerprint of a strategy config dict —
    sha256 of the sort_keys JSON dump (non-serialisable values become str).
    Stored on entry_snapshots so measurements can be grouped by the exact
    config that produced them."""
    try:
        blob = json.dumps(strategy, sort_keys=True, default=str)
    except Exception:
        blob = repr(strategy)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:12]


# ── Trades ────────────────────────────────────────────────────────────────────

def log_trade(trade: dict, *,
              target_crossed_to_trigger_ms: Optional[int] = None,
              trigger_to_filled_ms: Optional[int] = None,
              target_crossed_ts: Optional[str] = None,
              exit_label: Optional[str] = None,
              slippage_bps: Optional[float] = None,
              entry_fee_usdt: Optional[float] = None,
              exit_fee_usdt: Optional[float] = None,
              hold_time_sec: Optional[float] = None,
              origin: Optional[str] = None):
    """Insert a closed trade.

    Backward compatible: existing callers pass only the `trade` dict (plus the
    latency kwargs). The Phase 1 measurement kwargs (exit_label, slippage_bps,
    entry_fee_usdt, exit_fee_usdt, hold_time_sec, origin) are optional; when a
    kwarg is None the same-named key from the `trade` dict is used, so callers
    may supply them either way. Unset values are stored as NULL.
    """
    # Phase 1 columns: explicit kwarg wins, else same-named trade-dict key.
    if exit_label is None:
        exit_label = trade.get("exit_label")
    if slippage_bps is None:
        slippage_bps = trade.get("slippage_bps")
    if entry_fee_usdt is None:
        entry_fee_usdt = trade.get("entry_fee_usdt")
    if exit_fee_usdt is None:
        exit_fee_usdt = trade.get("exit_fee_usdt")
    if hold_time_sec is None:
        hold_time_sec = trade.get("hold_time_sec")
    if origin is None:
        origin = trade.get("origin")
    # F7: no exit may be written silently unlabeled. A SELL/close with no label
    # is a bug in the calling path — default to 'unknown' so it is at least
    # visible in analytics, and log loudly so the leaking path can be found.
    if not exit_label and str(trade.get("side", "")).upper() != "BUY":
        exit_label = "unknown"
        try:
            _sym = trade.get("coin") or trade.get("symbol") or "?"
            log_activity(f"UNLABELED EXIT {_sym} reason={trade.get('sell_reason')} "
                         f"— labeled 'unknown' (F7 guard)", "warn")
        except Exception:
            pass
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
                 intended_buy_price, intended_sell_price, buy_slippage_pct, sell_slippage_pct,
                 exit_label, slippage_bps, entry_fee_usdt, exit_fee_usdt,
                 hold_time_sec, origin)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
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
            exit_label, slippage_bps, entry_fee_usdt, exit_fee_usdt,
            hold_time_sec, origin,
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
    """Set mode on positions that have a MISSING or unknown mode tag only.
    Called once on startup when the filtered query returns nothing but the
    unfiltered table has rows — happens when positions were saved before the
    mode-isolation fix. Rows already carrying a valid mode ('paper'/'live'/
    'testnet') are NEVER re-tagged — re-tagging opposite-mode rows would turn
    paper positions into live ones (real sells) and vice versa.
    """
    with _lock:
        conn = _conn()
        conn.execute(
            "UPDATE positions SET mode=? "
            "WHERE mode IS NULL OR mode NOT IN ('paper','live','testnet')",
            (mode,)
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
    """Wipe PAPER trades, PAPER positions, and the activity log.
    Live/testnet rows are never touched — this is a paper-wallet reset only."""
    with _lock:
        conn = _conn()
        conn.execute("DELETE FROM trades WHERE mode='paper'")
        conn.execute("DELETE FROM positions WHERE mode='paper'")
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


# ── Config history (Phase 5 §5.1) ─────────────────────────────────────────────

def save_config_version(actor: str, diff: dict, full: dict) -> Optional[int]:
    """Record one strategy.json change. `diff` is a dotted-path change map,
    `full` the complete post-change file content. Returns the new version
    number (autoincrement) or None on failure — history must never break a
    config write."""
    import time as _t
    try:
        diff_json = json.dumps(diff if diff is not None else {}, default=str)
    except Exception:
        diff_json = "{}"
    try:
        full_json = json.dumps(full if full is not None else {}, default=str)
    except Exception:
        full_json = "{}"
    try:
        chash = config_hash(full if isinstance(full, dict) else {})
    except Exception:
        chash = None
    try:
        with _lock:
            conn = _conn()
            cur = conn.execute("""
                INSERT INTO config_history (ts, actor, diff_json, full_json, config_hash)
                VALUES (?,?,?,?,?)
            """, (_t.time(), str(actor or "unknown"), diff_json, full_json, chash))
            conn.commit()
            version = cur.lastrowid
            conn.close()
        return version
    except Exception:
        return None


def get_config_history(limit: int = 50) -> List[dict]:
    """Newest-first config history (without full_json — fetch a single version
    for that). diff_json is parsed back to a dict."""
    try:
        with _lock:
            conn = _conn()
            rows = conn.execute("""
                SELECT version, ts, actor, diff_json, config_hash
                FROM config_history ORDER BY version DESC LIMIT ?
            """, (int(limit),)).fetchall()
            conn.close()
    except Exception:
        return []
    out = []
    for r in rows:
        d = dict(r)
        try:
            d["diff"] = json.loads(d.pop("diff_json") or "{}")
        except Exception:
            d["diff"] = {}
        out.append(d)
    return out


def get_config_version(version: int) -> Optional[dict]:
    """One config-history row including the parsed full config, or None."""
    try:
        with _lock:
            conn = _conn()
            row = conn.execute("""
                SELECT version, ts, actor, diff_json, full_json, config_hash
                FROM config_history WHERE version = ?
            """, (int(version),)).fetchone()
            conn.close()
    except Exception:
        return None
    if not row:
        return None
    d = dict(row)
    try:
        d["diff"] = json.loads(d.pop("diff_json") or "{}")
    except Exception:
        d["diff"] = {}
    try:
        d["full"] = json.loads(d.pop("full_json") or "{}")
    except Exception:
        d["full"] = {}
    return d


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


# ── Backups (N4) ──────────────────────────────────────────────────────────────

def _backups_dir() -> str:
    """The backups/ subdir next to DB_PATH (created on demand)."""
    d = os.path.join(os.path.dirname(os.path.abspath(DB_PATH)), "backups")
    os.makedirs(d, exist_ok=True)
    return d


def backup_db(keep: int = 7) -> dict:
    """Write a consistent on-disk backup of the SQLite DB at DB_PATH.

    Uses SQLite's online backup API (Connection.backup()) — NOT a raw file copy,
    which can capture a mid-write DB. The backup streams pages page-by-page while
    holding only a short-lived shared lock, so it is safe to run against the live
    DB. The result is verified by re-opening it read-only and running
    PRAGMA integrity_check; a corrupt backup is deleted and reported as an error.
    Retention keeps the newest `keep` files in backups/, deleting older ones.

    Returns {"ok", "path", "kept", "bytes", "error"} and never raises.
    """
    import time as _t
    dest_path = None
    try:
        keep = max(1, int(keep))
    except (TypeError, ValueError):
        keep = 7
    try:
        backups_dir = _backups_dir()
        stamp = _t.strftime("%Y%m%d-%H%M%S", _t.localtime())
        dest_path = os.path.join(backups_dir, f"bot.db.backup-{stamp}")

        # ── Write the backup via SQLite's safe online backup API ──────────────
        # Serialize against writers with the same _lock the rest of the module
        # uses; the backup itself streams pages and does not hold the write lock.
        with _lock:
            src = _conn()
            try:
                dst = sqlite3.connect(dest_path)
                try:
                    src.backup(dst)
                    # The backup copies the source header (journal_mode=WAL);
                    # force the backup file to a self-contained rollback DB so
                    # opening it never spawns -wal/-shm sidecars next to it.
                    try:
                        dst.execute("PRAGMA journal_mode=DELETE")
                    except Exception:
                        pass
                finally:
                    dst.close()
            finally:
                src.close()

        # ── Verify the backup is readable and internally consistent ───────────
        verify = sqlite3.connect(f"file:{dest_path}?mode=ro", uri=True)
        try:
            row = verify.execute("PRAGMA integrity_check").fetchone()
            ok = bool(row) and str(row[0]).lower() == "ok"
            if ok:
                # Cheap sanity read on a core table too.
                verify.execute("SELECT count(*) FROM trades").fetchone()
        finally:
            verify.close()

        if not ok:
            try:
                os.remove(dest_path)
            except OSError:
                pass
            return {"ok": False, "path": None, "kept": 0, "bytes": 0,
                    "error": "integrity_check failed on backup"}

        size = os.path.getsize(dest_path)

        # ── Retention: keep the newest `keep` backups, delete older ones ──────
        kept = 0
        try:
            existing = list_backups()  # newest-first
            for meta in existing[:keep]:
                kept += 1
            for meta in existing[keep:]:
                try:
                    os.remove(meta["path"])
                except OSError:
                    pass
        except Exception:
            kept = 1  # at least the file we just wrote

        return {"ok": True, "path": dest_path, "kept": kept,
                "bytes": int(size), "error": None}
    except Exception as e:
        if dest_path:
            try:
                os.remove(dest_path)
            except OSError:
                pass
        return {"ok": False, "path": None, "kept": 0, "bytes": 0,
                "error": f"{type(e).__name__}: {e}"}


# ── Training samples (Part S — unified EV training-data store) ────────────────
#
# The EV scoring model (ev_model.py) trains on labeled entry outcomes from BOTH
# live and paper-shadow trades. Both modes write here via save_training_sample;
# the retrainer reads via get_training_samples. All helpers are CREATE-safe,
# guarded, and never raise — a training-store failure must never break a trade.

# Rolling-window size: keep only the most recent N rows. The paper-shadow keeps
# improving (better selection → better outcomes), so a fixed recent window trains
# on the strategy's CURRENT behaviour and gets more accurate over time, while
# keeping the DB tiny and the whole UI fast. Enforced continuously on every insert
# and re-asserted at boot + in the daily maintenance loop.
_TRAINING_SAMPLES_CAP = 3000

_TRAINING_SAMPLES_DDL = """CREATE TABLE IF NOT EXISTS training_samples (
    id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL, mode TEXT, symbol TEXT,
    features_json TEXT, label INTEGER, realized_r REAL)"""


def save_training_sample(mode, symbol, features, label, realized_r, ts=None) -> None:
    """Store one labeled entry outcome for EV model training.

    `mode` is 'live' or 'paper_shadow'. `features` is a dict, JSON-serialised
    (non-serialisable values become str). `label` is 1 (win / positive realized
    R) or 0 (loss). `realized_r` is a float and may be None. `ts` defaults to
    time.time(). CREATE-safe, guarded, never raises. On write the table is
    hard-capped to the newest 100_000 rows so it can't grow unbounded."""
    import time as _t
    if ts is None:
        ts = _t.time()
    try:
        features_json = json.dumps(features if features is not None else {}, default=str)
    except Exception:
        features_json = "{}"
    try:
        with _lock:
            conn = _conn()
            try:
                conn.execute(_TRAINING_SAMPLES_DDL)
                conn.execute("""
                    INSERT INTO training_samples
                        (ts, mode, symbol, features_json, label, realized_r)
                    VALUES (?,?,?,?,?,?)
                """, (float(ts), mode, symbol, features_json,
                      int(bool(label)), _num(realized_r)))
                # Hard row cap (same pattern as buy_rejections/entry_snapshots).
                conn.execute(
                    "DELETE FROM training_samples WHERE id NOT IN "
                    "(SELECT id FROM training_samples ORDER BY id DESC LIMIT ?)",
                    (_TRAINING_SAMPLES_CAP,))
                conn.commit()
            finally:
                conn.close()
    except Exception as e:
        print(f"[Database] save_training_sample failed: {e}")


def get_training_samples(limit: int = 20000, modes=None) -> List[dict]:
    """Return training samples newest-first as
    [{ts, mode, symbol, features(dict), label, realized_r}].

    `features` is parsed from features_json (unparseable → {}). `modes` is an
    optional list to filter on (e.g. ['live'] or ['live','paper_shadow']).
    Bounded by `limit`. Guarded → [] on error."""
    where = ["1=1"]
    params: list = []
    if modes:
        placeholders = ",".join(["?"] * len(modes))
        where.append(f"mode IN ({placeholders})")
        params.extend(list(modes))
    params.append(int(limit))
    try:
        with _lock:
            conn = _conn()
            try:
                conn.execute(_TRAINING_SAMPLES_DDL)
                rows = conn.execute(f"""
                    SELECT ts, mode, symbol, features_json, label, realized_r
                    FROM training_samples
                    WHERE {' AND '.join(where)}
                    ORDER BY id DESC LIMIT ?
                """, params).fetchall()
            finally:
                conn.close()
    except Exception:
        return []
    out = []
    for r in rows:
        d = dict(r)
        try:
            d["features"] = json.loads(d.pop("features_json") or "{}")
        except Exception:
            d.pop("features_json", None)
            d["features"] = {}
        out.append(d)
    return out


def count_training_samples(modes=None) -> dict:
    """Return {"total", "live", "paper_shadow", "wins"} counts for the
    UI/guardrail. `modes` optionally restricts `total` to the given modes; the
    per-mode and wins counts are always reported. Guarded → zeros on error."""
    zeros = {"total": 0, "live": 0, "paper_shadow": 0, "wins": 0}
    where = ["1=1"]
    params: list = []
    if modes:
        placeholders = ",".join(["?"] * len(modes))
        where.append(f"mode IN ({placeholders})")
        params.extend(list(modes))
    try:
        with _lock:
            conn = _conn()
            try:
                conn.execute(_TRAINING_SAMPLES_DDL)
                row = conn.execute(f"""
                    SELECT
                        COUNT(*)                                                AS total,
                        SUM(CASE WHEN mode='live'         THEN 1 ELSE 0 END)   AS live,
                        SUM(CASE WHEN mode='paper_shadow' THEN 1 ELSE 0 END)   AS paper_shadow,
                        SUM(CASE WHEN label=1             THEN 1 ELSE 0 END)   AS wins
                    FROM training_samples
                    WHERE {' AND '.join(where)}
                """, params).fetchone()
            finally:
                conn.close()
    except Exception:
        return dict(zeros)
    if not row:
        return dict(zeros)
    return {
        "total":        int(row["total"]        or 0),
        "live":         int(row["live"]         or 0),
        "paper_shadow": int(row["paper_shadow"] or 0),
        "wins":         int(row["wins"]         or 0),
    }


def prune_training_samples_to_cap(cap: int = None) -> int:
    """Keep only the newest `cap` training_samples rows (default _TRAINING_SAMPLES_
    CAP); delete the rest. The write-path enforces this on every insert too — this
    is the explicit backstop the maintenance loop + boot call so an existing large
    table is shrunk immediately rather than only on the next insert. Returns rows
    deleted. Guarded/CREATE-safe."""
    n = int(cap) if cap is not None else _TRAINING_SAMPLES_CAP
    try:
        with _lock:
            conn = _conn()
            try:
                conn.execute(_TRAINING_SAMPLES_DDL)
                # Efficient id-threshold delete (uses the PK index): find the id of
                # the (n+1)th-newest row and drop everything at/below it. This is
                # O(log n) to locate + a ranged delete — vastly faster than
                # `NOT IN (subquery)`, which scanned the whole table and could hold
                # the DB lock long enough to make the backend unreachable at boot.
                row = conn.execute(
                    "SELECT id FROM training_samples ORDER BY id DESC LIMIT 1 OFFSET ?",
                    (n,)).fetchone()
                if row is None:
                    return 0  # fewer than n rows — nothing to trim
                cutoff_id = row[0]
                cur = conn.execute(
                    "DELETE FROM training_samples WHERE id <= ?", (cutoff_id,))
                deleted = cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
                conn.commit()
            finally:
                conn.close()
        return deleted
    except Exception as e:
        print(f"[Database] prune_training_samples_to_cap failed: {e}")
        return 0


def prune_training_samples(retention_days: float) -> int:
    """Delete training_samples with ts older than now - retention_days (ts is
    epoch seconds). CREATE-safe, guarded. Returns rows deleted (0 on error).
    The daily maintenance loop calls this."""
    import time as _t
    cutoff = _t.time() - float(retention_days) * 86400.0
    try:
        with _lock:
            conn = _conn()
            try:
                conn.execute(_TRAINING_SAMPLES_DDL)
                cur = conn.execute(
                    "DELETE FROM training_samples WHERE ts < ?", (cutoff,))
                deleted = cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
                conn.commit()
            finally:
                conn.close()
        return deleted
    except Exception as e:
        print(f"[Database] prune_training_samples failed: {e}")
        return 0


# ── Backups (N4) — list helper ────────────────────────────────────────────────

def list_backups() -> list:
    """Return existing backup files as {path, size, mtime} dicts, newest-first.
    Safe to call any time — a missing backups/ dir yields an empty list."""
    import re as _re
    _name_re = _re.compile(r"^bot\.db\.backup-\d{8}-\d{6}$")
    out = []
    try:
        backups_dir = os.path.join(
            os.path.dirname(os.path.abspath(DB_PATH)), "backups")
        if not os.path.isdir(backups_dir):
            return []
        for name in os.listdir(backups_dir):
            # Match only real backup files — never -wal/-shm/-journal sidecars.
            if not _name_re.match(name):
                continue
            path = os.path.join(backups_dir, name)
            try:
                st = os.stat(path)
            except OSError:
                continue
            if not os.path.isfile(path):
                continue
            out.append({"path": path, "size": int(st.st_size),
                        "mtime": st.st_mtime})
        out.sort(key=lambda d: d["mtime"], reverse=True)
    except Exception:
        return out
    return out


# ── Paper-shadow outcome store (WolfBot Shadow-Lab) ───────────────────────────
#
# The paper-shadow runs many concurrent shadow trades as a data scraper. Each
# closed shadow trade writes one detailed outcome row via save_paper_trade;
# get_paper_summary aggregates the table into the operator's copy-paste
# "results summary". All helpers are CREATE-safe, guarded, and never raise — a
# shadow-store failure must never break a shadow trade. The table is hard-capped
# to the newest 200_000 rows on write (same pattern as buy_rejections /
# training_samples / entry_snapshots).

# Rolling window (see _TRAINING_SAMPLES_CAP) — keep only the most recent N.
_PAPER_TRADES_CAP = 3000

_PAPER_TRADES_DDL = """CREATE TABLE IF NOT EXISTS paper_trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT, ts REAL, symbol TEXT, wolfscore REAL,
    regime TEXT, exit_type TEXT, entry_px REAL, exit_px REAL, pnl REAL,
    realized_r REAL, hold_sec REAL, label INTEGER)"""


def save_paper_trade(row: dict) -> None:
    """Store one closed paper-shadow trade outcome.

    `row` is a dict with keys ts, symbol, wolfscore, regime, exit_type,
    entry_px, exit_px, pnl, realized_r, hold_sec, label. Missing keys become
    NULL (numeric fields coerce via _num; `ts` defaults to time.time()).
    CREATE-safe, guarded, never raises. On write the table is hard-capped to
    the newest 200_000 rows so it can't grow unbounded under heavy scrape."""
    import time as _t
    row = row if isinstance(row, dict) else {}
    ts = row.get("ts")
    if ts is None:
        ts = _t.time()
    try:
        with _lock:
            conn = _conn()
            try:
                conn.execute(_PAPER_TRADES_DDL)
                conn.execute("""
                    INSERT INTO paper_trades
                        (ts, symbol, wolfscore, regime, exit_type, entry_px,
                         exit_px, pnl, realized_r, hold_sec, label)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    _num(ts), row.get("symbol"), _num(row.get("wolfscore")),
                    row.get("regime"), row.get("exit_type"),
                    _num(row.get("entry_px")), _num(row.get("exit_px")),
                    _num(row.get("pnl")), _num(row.get("realized_r")),
                    _num(row.get("hold_sec")),
                    int(bool(row.get("label"))) if row.get("label") is not None else None,
                ))
                # Hard row cap (same pattern as buy_rejections/training_samples).
                conn.execute(
                    "DELETE FROM paper_trades WHERE id NOT IN "
                    "(SELECT id FROM paper_trades ORDER BY id DESC LIMIT ?)",
                    (_PAPER_TRADES_CAP,))
                conn.commit()
            finally:
                conn.close()
    except Exception as e:
        print(f"[Database] save_paper_trade failed: {e}")


def _paper_score_bucket(score) -> Optional[str]:
    """Bucket a wolfscore into one of '0-40'/'40-55'/'55-70'/'70-100'.
    Returns None when the score is missing/non-numeric so it is excluded
    from the by_score_bucket breakdown."""
    s = _num(score)
    if s is None:
        return None
    if s < 40:
        return "0-40"
    if s < 55:
        return "40-55"
    if s < 70:
        return "55-70"
    return "70-100"


def _paper_regime_key(regime) -> Optional[str]:
    """Normalise a regime string into 'up'/'down'/'side' for the by_regime
    breakdown. Unknown/missing regimes are excluded (returns None)."""
    if regime is None:
        return None
    r = str(regime).strip().lower()
    if r in ("up", "bull", "bullish", "long"):
        return "up"
    if r in ("down", "bear", "bearish", "short"):
        return "down"
    if r in ("side", "sideways", "flat", "range", "ranging", "chop", "neutral"):
        return "side"
    return None


def get_paper_summary(hours: float = None, limit: int = 50000) -> dict:
    """Aggregate paper_trades into the copy-paste "results summary".

    When `hours` is given, only trades in the last `hours` are included.
    Bounded to the newest `limit` rows, pulled and aggregated in Python.
    A trade is a win when realized_r > 0 (or pnl > 0 when realized_r is NULL).
    Guarded → a zeros/empty-but-valid dict on error. Returns:
      {n, wins, losses, win_rate, expectancy_r, avg_win_r, avg_loss_r,
       profit_factor, total_pnl, avg_hold_sec, trades_per_hour,
       by_exit_type: {exit_type: {n, win_rate, avg_r}},
       by_regime:    {up/down/side: {n, win_rate, avg_r}},
       by_score_bucket: {'0-40'/'40-55'/'55-70'/'70-100': {n, win_rate, avg_r}},
       top_symbols: [{symbol, n, win_rate, avg_r, pnl} ...up to 15 by n],
       data_start_ts, generated_ts}"""
    import time as _t
    generated_ts = _t.time()
    empty = {
        "n": 0, "wins": 0, "losses": 0, "win_rate": 0.0,
        "expectancy_r": 0.0, "avg_win_r": 0.0, "avg_loss_r": 0.0,
        "profit_factor": 0.0, "total_pnl": 0.0, "avg_hold_sec": 0.0,
        "trades_per_hour": 0.0,
        "by_exit_type": {}, "by_regime": {}, "by_score_bucket": {},
        "top_symbols": [], "data_start_ts": None, "generated_ts": generated_ts,
    }
    try:
        where = ["1=1"]
        params: list = []
        if hours is not None:
            where.append("ts >= ?")
            params.append(generated_ts - float(hours) * 3600.0)
        params.append(int(limit))
        with _lock:
            conn = _conn()
            try:
                conn.execute(_PAPER_TRADES_DDL)
                rows = conn.execute(f"""
                    SELECT ts, symbol, wolfscore, regime, exit_type,
                           pnl, realized_r, hold_sec, label
                    FROM paper_trades
                    WHERE {' AND '.join(where)}
                    ORDER BY id DESC LIMIT ?
                """, params).fetchall()
            finally:
                conn.close()
    except Exception as e:
        print(f"[Database] get_paper_summary failed: {e}")
        return dict(empty)

    if not rows:
        return dict(empty)

    def _is_win(realized_r, pnl) -> bool:
        if realized_r is not None:
            return realized_r > 0
        return (pnl is not None) and (pnl > 0)

    def _grp():
        return {"n": 0, "wins": 0, "r_sum": 0.0, "r_cnt": 0, "pnl": 0.0}

    def _finalize(g: dict, with_pnl: bool = False) -> dict:
        out = {
            "n": g["n"],
            "win_rate": round(g["wins"] / g["n"], 4) if g["n"] else 0.0,
            "avg_r": round(g["r_sum"] / g["r_cnt"], 4) if g["r_cnt"] else 0.0,
        }
        if with_pnl:
            out["pnl"] = round(g["pnl"], 6)
        return out

    n = 0
    wins = 0
    r_sum = 0.0
    r_cnt = 0
    win_r_sum = 0.0
    win_r_cnt = 0
    loss_r_sum = 0.0
    loss_r_cnt = 0
    gross_win = 0.0
    gross_loss = 0.0  # magnitude of losing pnl
    total_pnl = 0.0
    hold_sum = 0.0
    hold_cnt = 0
    min_ts = None
    max_ts = None
    by_exit: Dict[str, dict] = {}
    by_regime: Dict[str, dict] = {}
    by_bucket: Dict[str, dict] = {}
    by_symbol: Dict[str, dict] = {}

    for r in rows:
        realized_r = r["realized_r"]
        pnl = r["pnl"]
        won = _is_win(realized_r, pnl)
        n += 1
        if won:
            wins += 1
        if realized_r is not None:
            r_sum += realized_r
            r_cnt += 1
            if realized_r > 0:
                win_r_sum += realized_r
                win_r_cnt += 1
            else:
                loss_r_sum += realized_r
                loss_r_cnt += 1
        if pnl is not None:
            total_pnl += pnl
            if pnl > 0:
                gross_win += pnl
            elif pnl < 0:
                gross_loss += -pnl
        if r["hold_sec"] is not None:
            hold_sum += r["hold_sec"]
            hold_cnt += 1
        ts = r["ts"]
        if ts is not None:
            min_ts = ts if min_ts is None else min(min_ts, ts)
            max_ts = ts if max_ts is None else max(max_ts, ts)

        def _accum(bucket_map: dict, key):
            if key is None:
                return
            g = bucket_map.get(key)
            if g is None:
                g = _grp()
                bucket_map[key] = g
            g["n"] += 1
            if won:
                g["wins"] += 1
            if realized_r is not None:
                g["r_sum"] += realized_r
                g["r_cnt"] += 1
            if pnl is not None:
                g["pnl"] += pnl

        et = r["exit_type"]
        _accum(by_exit, et if et is not None else "unknown")
        _accum(by_regime, _paper_regime_key(r["regime"]))
        _accum(by_bucket, _paper_score_bucket(r["wolfscore"]))
        _accum(by_symbol, r["symbol"] if r["symbol"] is not None else "?")

    losses = n - wins
    span_hours = None
    if min_ts is not None and max_ts is not None and max_ts > min_ts:
        span_hours = (max_ts - min_ts) / 3600.0
    trades_per_hour = round(n / span_hours, 3) if span_hours else 0.0
    profit_factor = round(gross_win / gross_loss, 4) if gross_loss > 0 else (
        float(gross_win) if gross_win > 0 else 0.0)

    top_symbols = sorted(
        ({"symbol": sym, **_finalize(g, with_pnl=True)}
         for sym, g in by_symbol.items()),
        key=lambda d: d["n"], reverse=True)[:15]

    return {
        "n": n,
        "wins": wins,
        "losses": losses,
        "win_rate": round(wins / n, 4) if n else 0.0,
        "expectancy_r": round(r_sum / r_cnt, 4) if r_cnt else 0.0,
        "avg_win_r": round(win_r_sum / win_r_cnt, 4) if win_r_cnt else 0.0,
        "avg_loss_r": round(loss_r_sum / loss_r_cnt, 4) if loss_r_cnt else 0.0,
        "profit_factor": profit_factor,
        "total_pnl": round(total_pnl, 6),
        "avg_hold_sec": round(hold_sum / hold_cnt, 2) if hold_cnt else 0.0,
        "trades_per_hour": trades_per_hour,
        "by_exit_type": {k: _finalize(v) for k, v in by_exit.items()},
        "by_regime": {k: _finalize(v) for k, v in by_regime.items()},
        "by_score_bucket": {k: _finalize(v) for k, v in by_bucket.items()},
        "top_symbols": top_symbols,
        "data_start_ts": min_ts,
        "generated_ts": generated_ts,
    }


def count_paper_trades() -> dict:
    """Return {"total", "wins", "open"} for the paper-shadow store. `open` is
    tracked in-memory by paper_shadow (not persisted here) so it is always
    None. A win is realized_r > 0, or pnl > 0 when realized_r is NULL.
    CREATE-safe, guarded → zeros on error."""
    try:
        with _lock:
            conn = _conn()
            try:
                conn.execute(_PAPER_TRADES_DDL)
                row = conn.execute("""
                    SELECT
                        COUNT(*) AS total,
                        SUM(CASE WHEN realized_r > 0
                                   OR (realized_r IS NULL AND pnl > 0)
                                 THEN 1 ELSE 0 END) AS wins
                    FROM paper_trades
                """).fetchone()
            finally:
                conn.close()
    except Exception:
        return {"total": 0, "wins": 0, "open": None}
    if not row:
        return {"total": 0, "wins": 0, "open": None}
    return {
        "total": int(row["total"] or 0),
        "wins": int(row["wins"] or 0),
        "open": None,
    }


def prune_paper_trades_to_cap(cap: int = None) -> int:
    """Keep only the newest `cap` paper_trades rows (default _PAPER_TRADES_CAP);
    delete the rest. Backstop for the write-path row cap. Guarded/CREATE-safe."""
    n = int(cap) if cap is not None else _PAPER_TRADES_CAP
    try:
        with _lock:
            conn = _conn()
            try:
                conn.execute(_PAPER_TRADES_DDL)
                # Efficient id-threshold delete (see prune_training_samples_to_cap).
                row = conn.execute(
                    "SELECT id FROM paper_trades ORDER BY id DESC LIMIT 1 OFFSET ?",
                    (n,)).fetchone()
                if row is None:
                    return 0
                cutoff_id = row[0]
                cur = conn.execute(
                    "DELETE FROM paper_trades WHERE id <= ?", (cutoff_id,))
                deleted = cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
                conn.commit()
            finally:
                conn.close()
        return deleted
    except Exception as e:
        print(f"[Database] prune_paper_trades_to_cap failed: {e}")
        return 0


def prune_paper_trades(retention_days: float) -> int:
    """Delete paper_trades with ts older than now - retention_days (ts is epoch
    seconds). CREATE-safe, guarded. Returns rows deleted (0 on error)."""
    import time as _t
    cutoff = _t.time() - float(retention_days) * 86400.0
    try:
        with _lock:
            conn = _conn()
            try:
                conn.execute(_PAPER_TRADES_DDL)
                cur = conn.execute(
                    "DELETE FROM paper_trades WHERE ts < ?", (cutoff,))
                deleted = cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
                conn.commit()
            finally:
                conn.close()
        return deleted
    except Exception as e:
        print(f"[Database] prune_paper_trades failed: {e}")
        return 0
