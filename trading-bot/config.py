WATCHED_COINS = [
    "BTCUSDT",
    "ETHUSDT",
    "BNBUSDT",
    "SOLUSDT",
    "XRPUSDT",
]

BUDGET_PER_TRADE_USDT   = 100.0
MAX_OPEN_POSITIONS      = 5
DECISION_INTERVAL_SEC   = 600       # Claude strategy runs every 10 minutes
MIN_CLAUDE_CONFIDENCE   = 0.65
RSI_OVERBOUGHT          = 72
RSI_OVERSOLD            = 28
CANDLE_TIMEFRAME        = "1m"
CANDLE_LOOKBACK         = 50
BNB_FEE_MODE            = True
FEE_RATE_BNB            = 0.00075
FEE_RATE_STANDARD       = 0.001
CLAUDE_MODEL            = "claude-haiku-4-5-20251001"
CLAUDE_MAX_TOKENS       = 400
STRATEGY_FILE           = "strategy.json"

# ── Two-speed architecture constants ─────────────────────────────────────────
SCAN_INTERVAL_SEC    = 30      # signal scanner frequency (Process 2)
STOP_LOSS_PCT        = 0.015   # 1.5%
COOLDOWN_AFTER_LOSS  = 300     # 5 min cooldown after stop-loss
MIN_SIGNALS_TO_BUY   = 3       # out of 4 signals must be bullish
RSI_BUY_MIN          = 40
RSI_BUY_MAX          = 65
VOLUME_RATIO_MIN     = 1.5

# ── Budget allocation settings ────────────────────────────────────────────────
BUDGET_MODE           = "fixed"   # fixed | percent | capped | per_coin
BUDGET_FIXED_USDT     = 100.0
BUDGET_PCT_OF_FREE    = 10.0      # % of free USDT per trade
BUDGET_TOTAL_CAP_USDT = 500.0     # max total USDT deployed by bot at once
BUDGET_PER_COIN = {
    "BTCUSDT":  200.0,
    "ETHUSDT":  150.0,
    "SOLUSDT":  100.0,
    "BNBUSDT":  100.0,
    "DOGEUSDT": 50.0,
}
