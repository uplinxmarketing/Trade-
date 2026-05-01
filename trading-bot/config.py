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
CANDLE_TIMEFRAME        = "1h"
CANDLE_LOOKBACK         = 50
BNB_FEE_MODE            = True
FEE_RATE_BNB            = 0.00075
FEE_RATE_STANDARD       = 0.001
CLAUDE_MODEL            = "claude-haiku-4-5-20251001"
CLAUDE_MAX_TOKENS       = 400
STRATEGY_FILE           = "strategy.json"
