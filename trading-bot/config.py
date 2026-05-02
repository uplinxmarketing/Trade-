WATCHED_COINS = [
    "BTCUSDT",  "ETHUSDT",  "SOLUSDT",  "BNBUSDT",  "DOGEUSDT",
    "XRPUSDT",  "ADAUSDT",  "AVAXUSDT", "DOTUSDT",  "LINKUSDT",
    "MATICUSDT","UNIUSDT",  "LTCUSDT",  "ATOMUSDT", "SHIBUSDT",
    "ARBUSDT",  "OPUSDT",   "INJUSDT",  "FETUSDT",  "NEARUSDT",
    "TRXUSDT",  "TONUSDT",  "APTUSDT",  "SUIUSDT",  "PEPEUSDT",
    "WIFUSDT",  "BONKUSDT", "JUPUSDT",  "RENDERUSDT","TIAUSDT",
]

BUDGET_PER_TRADE_USDT   = 100.0
MAX_OPEN_POSITIONS      = 10
DECISION_INTERVAL_SEC   = 600       # Claude strategy runs every 10 minutes
MIN_CLAUDE_CONFIDENCE   = 0.65
RSI_OVERBOUGHT          = 75
RSI_OVERSOLD            = 25
CANDLE_TIMEFRAME        = "1m"
CANDLE_LOOKBACK         = 50
BNB_FEE_MODE            = True
FEE_RATE_BNB            = 0.00075
FEE_RATE_STANDARD       = 0.001
CLAUDE_MODEL            = "claude-haiku-4-5-20251001"
CLAUDE_MAX_TOKENS       = 400
STRATEGY_FILE           = "strategy.json"

# ── Two-speed architecture constants ─────────────────────────────────────────
SCAN_INTERVAL_SEC    = 60      # REST backup cache refresh (buys fire in realtime via WebSocket)
STOP_LOSS_PCT        = 0.015   # 1.5%
COOLDOWN_AFTER_LOSS  = 180     # 3 min cooldown after stop-loss
MIN_SIGNALS_TO_BUY   = 2       # out of 4 signals must be bullish
RSI_BUY_MIN          = 35
RSI_BUY_MAX          = 70
VOLUME_RATIO_MIN     = 1.05    # current volume just 5% above 20-candle average

# ── Budget allocation settings ────────────────────────────────────────────────
BUDGET_MODE           = "percent" # fixed | percent | capped | per_coin
BUDGET_FIXED_USDT     = 100.0
BUDGET_PCT_OF_FREE    = 5.0       # 5% of free USDT per trade
BUDGET_TOTAL_CAP_USDT = 1000.0
BUDGET_PER_COIN = {
    "BTCUSDT":  200.0,
    "ETHUSDT":  150.0,
    "SOLUSDT":  100.0,
    "BNBUSDT":  100.0,
    "DOGEUSDT": 50.0,
}
