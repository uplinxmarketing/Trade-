"""
PaperClient — drop-in replacement for python-binance Client.
Return shapes are byte-for-byte identical to the real Binance API.
"""

import time
import math
import threading
import uuid
from typing import Dict, Optional
import database


class PaperClient:
    def __init__(self, starting_usdt: float = 10000.0, fee_rate: float = 0.00075):
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

    # ── Price management ────────────────────────────────────────────────────

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

    def _set_balance(self, asset: str, amount: float):
        with self._lock:
            self._balances[asset] = max(0.0, amount)
        database.save_paper_state(dict(self._balances))

    # ── Account ─────────────────────────────────────────────────────────────

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

    # ── Orders ──────────────────────────────────────────────────────────────

    def order_market_buy(self, symbol: str, quoteOrderQty: float) -> dict:
        price = self._get_price(symbol)
        fee   = quoteOrderQty * self._fee_rate
        total_cost = quoteOrderQty + fee
        usdt_bal   = self._get_balance("USDT")

        if usdt_bal < total_cost:
            raise ValueError(
                f"Insufficient paper USDT balance: need {total_cost:.4f}, have {usdt_bal:.4f}"
            )

        # Derive coin from symbol (strip USDT suffix)
        coin = symbol.replace("USDT", "").replace("BTC", "") if not symbol.endswith("USDT") else symbol[:-4]
        qty  = math.floor((quoteOrderQty / price) * 1e8) / 1e8  # floor to 8 dp

        self._set_balance("USDT", usdt_bal - total_cost)
        self._set_balance(coin, self._get_balance(coin) + qty)

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

    def order_market_sell(self, symbol: str, quantity: float) -> dict:
        price = self._get_price(symbol)
        coin  = symbol[:-4]  # strip USDT
        coin_bal = self._get_balance(coin)

        if coin_bal < quantity * 0.9999:  # small tolerance for float precision
            raise ValueError(
                f"Insufficient paper {coin} balance: need {quantity:.8f}, have {coin_bal:.8f}"
            )

        gross_usdt = quantity * price
        fee        = gross_usdt * self._fee_rate
        net_usdt   = gross_usdt - fee

        self._set_balance(coin, coin_bal - quantity)
        self._set_balance("USDT", self._get_balance("USDT") + net_usdt)

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

    # ── No-ops for real client compatibility ─────────────────────────────────

    def ping(self) -> dict:
        return {}

    def get_server_time(self) -> dict:
        return {"serverTime": int(time.time() * 1000)}
