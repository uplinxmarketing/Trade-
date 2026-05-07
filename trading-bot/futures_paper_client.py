"""
FuturesPaperClient — simulates USDT-M perpetual futures trading.

Keeps a separate USDT pool in futures_state table.  Positions are persisted
in futures_positions; closed trades go to futures_trades.

Position P&L formula (isolated margin):
  LONG  pnl = (exit - entry) * qty - fees   (qty = margin * leverage / entry)
  SHORT pnl = (entry - exit) * qty - fees

Liquidation (simplified, no maintenance margin buffer):
  LONG  liq = entry * (1 - 1/leverage)
  SHORT liq = entry * (1 + 1/leverage)
"""

import math
import threading
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

import database

FEE_RATE = 0.0004   # typical Binance USDT-M taker fee


class FuturesPaperClient:
    def __init__(self, starting_usdt: float = 3000.0):
        self._lock = threading.Lock()
        self._mark_prices: Dict[str, float] = {}

        saved = database.load_futures_state()
        if saved is not None:
            self._usdt = float(saved.get("USDT", starting_usdt))
            # Backfill starting_usdt if missing from older DB rows
            if "starting_usdt" not in saved:
                saved["starting_usdt"] = starting_usdt
                database.save_futures_state(saved)
            self._starting_usdt = float(saved.get("starting_usdt", starting_usdt))
            print(f"[FuturesPaper] Restored — USDT: {self._usdt:.2f}")
        else:
            self._usdt = starting_usdt
            self._starting_usdt = starting_usdt
            database.save_futures_state({"USDT": self._usdt, "starting_usdt": starting_usdt})
            print(f"[FuturesPaper] Fresh start — USDT: {starting_usdt:.2f}")

        # In-memory position cache (mirrored from DB on start)
        self._positions: Dict[int, dict] = {}
        for p in database.load_futures_positions():
            self._positions[p["id"]] = p

    # ── Price feed ───────────────────────────────────────────────────────────

    def update_mark_price(self, symbol: str, price: float):
        with self._lock:
            self._mark_prices[symbol] = price

    def get_mark_price(self, symbol: str) -> float:
        with self._lock:
            return self._mark_prices.get(symbol, 0.0)

    # ── Balance ──────────────────────────────────────────────────────────────

    def get_balance(self) -> float:
        with self._lock:
            return self._usdt

    def get_starting_usdt(self) -> float:
        with self._lock:
            return self._starting_usdt

    def get_equity(self) -> float:
        """USDT + unrealized P&L of all open positions."""
        total_upnl = 0.0
        with self._lock:
            for pos in self._positions.values():
                mp = self._mark_prices.get(pos["symbol"], 0.0)
                if mp > 0:
                    total_upnl += self._calc_upnl(pos, mp)
            return self._usdt + total_upnl

    def _calc_upnl(self, pos: dict, mark_price: float) -> float:
        qty = pos["quantity"]
        ep  = pos["entry_price"]
        if pos["direction"] == "LONG":
            return (mark_price - ep) * qty
        else:
            return (ep - mark_price) * qty

    # ── Open position ─────────────────────────────────────────────────────────

    def open_position(
        self,
        symbol: str,
        direction: str,        # "LONG" or "SHORT"
        entry_price: float,
        margin_usdt: float,
        leverage: int,
        tp_pct: float,
        sl_pct: float,
        sl_enabled: bool = True,
    ) -> Optional[dict]:
        """Reserve margin and persist a new futures position. Returns position dict or None."""
        with self._lock:
            if self._usdt < margin_usdt:
                print(f"[FuturesPaper] Insufficient balance for {symbol}: "
                      f"need {margin_usdt:.2f}, have {self._usdt:.2f}")
                return None

            qty = math.floor((margin_usdt * leverage / entry_price) * 1e8) / 1e8
            if qty <= 0:
                return None

            fee = margin_usdt * leverage * FEE_RATE
            self._usdt -= (margin_usdt + fee)

            if direction == "LONG":
                take_profit       = entry_price * (1 + tp_pct)
                stop_loss         = entry_price * (1 - sl_pct) if sl_enabled else None
                liquidation_price = entry_price * (1 - 1 / leverage) * 0.99
            else:
                take_profit       = entry_price * (1 - tp_pct)
                stop_loss         = entry_price * (1 + sl_pct) if sl_enabled else None
                liquidation_price = entry_price * (1 + 1 / leverage) * 1.01

            ts = datetime.now(timezone.utc).isoformat()
            pos = {
                "symbol": symbol,
                "direction": direction,
                "entry_price": entry_price,
                "quantity": qty,
                "margin_usdt": margin_usdt,
                "leverage": leverage,
                "take_profit": take_profit,
                "stop_loss": stop_loss,
                "sl_enabled": sl_enabled,
                "liquidation_price": liquidation_price,
                "funding_paid": 0.0,
                "timestamp": ts,
            }

        pos_id = database.save_futures_position(pos)
        if pos_id is None:
            with self._lock:
                self._usdt += margin_usdt + fee   # rollback
            return None

        pos["id"] = pos_id
        with self._lock:
            self._positions[pos_id] = pos
            database.save_futures_state({"USDT": self._usdt, "starting_usdt": self._starting_usdt})

        sl_display = f"{stop_loss:.4f}" if stop_loss else "OFF"
        print(f"[FuturesPaper] OPEN {direction} {symbol} @ {entry_price:.4f} "
              f"qty={qty:.6f} margin={margin_usdt:.2f} lev={leverage}x "
              f"TP={take_profit:.4f} SL={sl_display}")
        return pos

    # ── Close position ────────────────────────────────────────────────────────

    def close_position(self, pos_id: int, exit_price: float, reason: str = "") -> Optional[dict]:
        """Close position, settle P&L, log the trade. Returns trade dict or None."""
        with self._lock:
            pos = self._positions.get(pos_id)
            if not pos:
                return None

            qty = pos["quantity"]
            ep  = pos["entry_price"]

            if pos["direction"] == "LONG":
                gross_pnl = (exit_price - ep) * qty
            else:
                gross_pnl = (ep - exit_price) * qty

            fee       = qty * exit_price * FEE_RATE
            net_pnl   = gross_pnl - fee

            margin_returned = pos["margin_usdt"] + net_pnl
            if margin_returned < 0:
                margin_returned = 0.0    # liquidated — lose entire margin

            self._usdt += margin_returned
            del self._positions[pos_id]

        database.delete_futures_position(pos_id)
        database.save_futures_state({"USDT": self._usdt, "starting_usdt": self._starting_usdt})

        ts_open  = pos["timestamp"]
        ts_close = datetime.now(timezone.utc).isoformat()
        try:
            dur = int((datetime.fromisoformat(ts_close) - datetime.fromisoformat(ts_open)).total_seconds())
        except Exception:
            dur = 0

        trade = {
            "symbol": pos["symbol"],
            "direction": pos["direction"],
            "entry_price": ep,
            "exit_price": exit_price,
            "quantity": qty,
            "margin_usdt": pos["margin_usdt"],
            "leverage": pos["leverage"],
            "net_profit": round(net_pnl, 6),
            "profitable": 1 if net_pnl > 0 else 0,
            "funding_paid": pos.get("funding_paid", 0.0),
            "duration_seconds": dur,
            "timestamp_open": ts_open,
            "timestamp_close": ts_close,
        }
        database.log_futures_trade(trade)

        print(f"[FuturesPaper] CLOSE {pos['direction']} {pos['symbol']} @ {exit_price:.4f} "
              f"pnl={net_pnl:+.4f} ({reason})")
        return trade

    # ── TP/SL/Liquidation check ───────────────────────────────────────────────

    def check_positions(self, sl_enabled: bool = True) -> List[dict]:
        """Check all open positions against current mark prices.

        sl_enabled overrides per-position sl_enabled flag so a settings
        change takes effect immediately without reopening positions.

        Returns list of trade dicts for any positions that were closed.
        """
        closed = []
        with self._lock:
            pos_snapshot = list(self._positions.values())

        for pos in pos_snapshot:
            mp = self.get_mark_price(pos["symbol"])
            if mp <= 0:
                continue

            # Respect per-position flag AND the live engine setting
            pos_sl_on = sl_enabled and pos.get("sl_enabled", True)
            sl_price  = pos.get("stop_loss")

            reason = None
            if pos["direction"] == "LONG":
                if mp >= pos["take_profit"]:
                    reason = "TP"
                elif mp <= pos["liquidation_price"]:
                    reason = "LIQ"
                elif pos_sl_on and sl_price and mp <= sl_price:
                    reason = "SL"
            else:  # SHORT
                if mp <= pos["take_profit"]:
                    reason = "TP"
                elif mp >= pos["liquidation_price"]:
                    reason = "LIQ"
                elif pos_sl_on and sl_price and mp >= sl_price:
                    reason = "SL"

            if reason:
                trade = self.close_position(pos["id"], mp, reason)
                if trade:
                    closed.append(trade)

        return closed

    # ── Queries ───────────────────────────────────────────────────────────────

    def get_open_positions(self) -> List[dict]:
        with self._lock:
            return list(self._positions.values())

    def has_open_position(self, symbol: str, direction: str) -> bool:
        with self._lock:
            return any(
                p["symbol"] == symbol and p["direction"] == direction
                for p in self._positions.values()
            )

    def open_position_count(self) -> int:
        with self._lock:
            return len(self._positions)

    # ── Reset ────────────────────────────────────────────────────────────────

    def reset(self, starting_usdt: float):
        with self._lock:
            self._usdt = starting_usdt
            self._starting_usdt = starting_usdt
            self._positions.clear()
            self._mark_prices.clear()

        # Wipe DB tables via the database module so the shared lock is respected
        with database._lock:
            conn = database._conn()
            conn.execute("DELETE FROM futures_positions")
            conn.execute("DELETE FROM futures_trades")
            conn.commit()
            conn.close()

        database.save_futures_state({"USDT": starting_usdt, "starting_usdt": starting_usdt})
        print(f"[FuturesPaper] Reset to {starting_usdt:.2f} USDT")
