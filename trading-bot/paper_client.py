"""
PaperClient — drop-in replacement for python-binance Client.
Return shapes are byte-for-byte identical to the real Binance API.

Phase 1 §1.6 — paper realism:
  * Fees come from fees.get_fee_model(symbol).taker() (strategy.json-driven,
    hot-reload, per-symbol overrides). The constructor's `fee_rate` param is
    kept only as a fallback for when the fees module is unavailable.
  * Fills carry slippage: buys fill at price*(1+slip), sells at price*(1-slip),
    where slip = half_spread + tier_bps/10000. The half-spread is read from
    data_collector.book_ticker (real WS bid/ask, guarded import, must be <30s
    fresh, capped at 50bps); otherwise 0. tier_bps defaults to 5bps —
    deterministic, documented, configurable later.
"""

import time
import math
import threading
import uuid
from typing import Dict, Optional
import database

# Default volume-tier slippage in basis points (§1.6). Applied on top of the
# observed half-spread. 5bps ~= typical market-order impact on liquid pairs.
DEFAULT_TIER_SLIPPAGE_BPS = 5.0
# Sanity cap on the half-spread component (a crossed/garbage book must not
# produce absurd fills).
MAX_HALF_SPREAD_FRACTION = 0.005  # 50 bps
# Book-ticker entries older than this are considered stale and ignored.
BOOK_TICKER_MAX_AGE_SEC = 30.0


class PaperClient:
    def __init__(self, starting_usdt: float = 10000.0, fee_rate: float = 0.001):
        # Fallback fee only — live fee comes from fees.get_fee_model(symbol).
        self._fee_rate = fee_rate
        self._prices: Dict[str, float] = {}
        self._lock = threading.Lock()

        # Load persisted state or start fresh
        saved = database.load_paper_state()
        if saved:
            self._balances: Dict[str, float] = saved
            print(f"[PaperClient] Restored state — USDT: {self._balances.get('USDT', 0):.2f}")
        else:
            self._balances = {"USDT": starting_usdt}
            database.save_paper_state(self._balances)
            print(f"[PaperClient] Fresh start — USDT: {starting_usdt:.2f}")

    # ── Fee + slippage helpers (Phase 1 §1.6) ────────────────────────────────

    def _fee_rate_for(self, symbol: str) -> float:
        """Taker fee FRACTION for symbol via fees.FeeModel; constructor value
        is the fallback when the fees module is unavailable/broken."""
        try:
            import fees as _fees
            return _fees.get_fee_model(symbol).taker()
        except Exception:
            return self._fee_rate

    def _slippage_frac(self, symbol: str) -> float:
        """Fill slippage as a FRACTION: half-spread (real WS book ticker when
        fresh, else 0) + tier bps. Deterministic given the current book."""
        half_spread = 0.0
        try:
            import data_collector as _dc  # guarded — may not be running (tests)
            bt = _dc.book_ticker.get(symbol)
            if bt and (time.time() - float(bt.get("ts", 0))) < BOOK_TICKER_MAX_AGE_SEC:
                bid = float(bt.get("bid", 0))
                ask = float(bt.get("ask", 0))
                mid = (bid + ask) / 2.0
                if bid > 0 and ask >= bid and mid > 0:
                    half_spread = min((ask - bid) / 2.0 / mid, MAX_HALF_SPREAD_FRACTION)
        except Exception:
            half_spread = 0.0
        return half_spread + DEFAULT_TIER_SLIPPAGE_BPS / 10000.0

    # ── Price management ──────────────────────────────────────────────────────────────────────────

    def update_price(self, symbol: str, price: float):
        with self._lock:
            self._prices[symbol] = price

    def _get_price(self, symbol: str) -> float:
        with self._lock:
            price = self._prices.get(symbol)
        if not price or price <= 0:
            raise ValueError(f"No price available for {symbol}. Is WebSocket running?")
        return price

    def _get_balance(self, asset: str) -> float:
        with self._lock:
            return self._balances.get(asset, 0.0)

    # ── Account ───────────────────────────────────────────────────────────────────────────────────

    def get_account(self) -> dict:
        with self._lock:
            balances = [
                {"asset": asset, "free": f"{bal:.8f}", "locked": "0.00000000"}
                for asset, bal in self._balances.items()
                if bal > 0
            ]
        return {
            "makerCommission": 15,
            "takerCommission": 15,
            "buyerCommission": 0,
            "sellerCommission": 0,
            "canTrade": True,
            "canWithdraw": True,
            "canDeposit": True,
            "balances": balances,
        }

    def get_symbol_ticker(self, symbol: str) -> dict:
        price = self._get_price(symbol)
        return {"symbol": symbol, "price": f"{price:.8f}"}

    def get_asset_balance(self, asset: str) -> dict:
        bal = self._get_balance(asset)
        return {"asset": asset, "free": f"{bal:.8f}", "locked": "0.00000000"}

    # ── Orders ──────────────────────────────────────────────────────────────────────────────────────

    def order_market_buy(self, symbol: str, quoteOrderQty: float) -> dict:
        mark = self._get_price(symbol)
        # §1.6: buys fill ABOVE the mark — half-spread + tier slippage.
        price = mark * (1.0 + self._slippage_frac(symbol))
        fee   = quoteOrderQty * self._fee_rate_for(symbol)
        total_cost = quoteOrderQty + fee
        coin  = symbol[:-4]  # strip USDT suffix
        qty   = math.floor((quoteOrderQty / price) * 1e8) / 1e8  # floor to 8 dp

        with self._lock:
            usdt_bal = self._balances.get("USDT", 0.0)
            if usdt_bal < total_cost:
                raise ValueError(
                    f"Insufficient paper USDT balance: need {total_cost:.4f}, have {usdt_bal:.4f}"
                )
            self._balances["USDT"] = usdt_bal - total_cost
            self._balances[coin]   = self._balances.get(coin, 0.0) + qty
            snapshot = dict(self._balances)

        # Single save after all balance changes are applied
        database.save_paper_state(snapshot)

        order_id = str(uuid.uuid4())[:12].replace("-", "")
        now_ms   = int(time.time() * 1000)

        return {
            "symbol": symbol,
            "orderId": order_id,
            "orderListId": -1,
            "clientOrderId": f"paper_{order_id}",
            "transactTime": now_ms,
            "price": "0.00000000",
            "origQty": f"{qty:.8f}",
            "executedQty": f"{qty:.8f}",
            "cummulativeQuoteQty": f"{quoteOrderQty:.8f}",
            "status": "FILLED",
            "timeInForce": "GTC",
            "type": "MARKET",
            "side": "BUY",
            "fills": [{
                "price": f"{price:.8f}",
                "qty": f"{qty:.8f}",
                "commission": f"{fee:.8f}",
                "commissionAsset": "BNB",
                "tradeId": 1,
            }],
        }

    def order_market_sell(self, symbol: str, quantity: float, price: float = 0) -> dict:
        # Use caller-supplied price when provided — prevents race conditions where
        # a concurrent update_price() call overwrites the trigger price before
        # order execution, causing sells to execute at the wrong (lower) price.
        if price <= 0:
            price = self._get_price(symbol)
        # §1.6: sells fill BELOW the trigger — half-spread + tier slippage.
        price = price * (1.0 - self._slippage_frac(symbol))
        coin  = symbol[:-4]  # strip USDT

        with self._lock:
            coin_bal = self._balances.get(coin, 0.0)
            if coin_bal < quantity * 0.9999:  # small tolerance for float precision
                raise ValueError(
                    f"Insufficient paper {coin} balance: need {quantity:.8f}, have {coin_bal:.8f}"
                )
            gross_usdt = quantity * price
            fee        = gross_usdt * self._fee_rate_for(symbol)
            net_usdt   = gross_usdt - fee

            new_coin = coin_bal - quantity
            if new_coin <= 0:
                self._balances.pop(coin, None)
            else:
                self._balances[coin] = new_coin
            self._balances["USDT"] = self._balances.get("USDT", 0.0) + net_usdt
            snapshot = dict(self._balances)

        # Single save after all balance changes are applied (was 2 saves before)
        database.save_paper_state(snapshot)

        order_id = str(uuid.uuid4())[:12].replace("-", "")
        now_ms   = int(time.time() * 1000)

        return {
            "symbol": symbol,
            "orderId": order_id,
            "orderListId": -1,
            "clientOrderId": f"paper_{order_id}",
            "transactTime": now_ms,
            "price": "0.00000000",
            "origQty": f"{quantity:.8f}",
            "executedQty": f"{quantity:.8f}",
            "cummulativeQuoteQty": f"{net_usdt:.8f}",
            "status": "FILLED",
            "timeInForce": "GTC",
            "type": "MARKET",
            "side": "SELL",
            "fills": [{
                "price": f"{price:.8f}",
                "qty": f"{quantity:.8f}",
                "commission": f"{fee:.8f}",
                "commissionAsset": "BNB",
                "tradeId": 1,
            }],
        }

    # ── No-ops for real client compatibility ──────────────────────────────────────────────────────────────────────────

    def ping(self) -> dict:
        return {}

    def get_server_time(self) -> dict:
        return {"serverTime": int(time.time() * 1000)}
