WATCHED_COINS = [
    # Large-caps
    "BTCUSDT",  "ETHUSDT",  "SOLUSDT",  "BNBUSDT",  "XRPUSDT",
    "ADAUSDT",  "AVAXUSDT", "DOGEUSDT", "DOTUSDT",  "LINKUSDT",
    "POLUSDT",  "UNIUSDT",  "LTCUSDT",  "ATOMUSDT", "TRXUSDT",
    # Mid-caps / L2
    "ARBUSDT",  "OPUSDT",   "INJUSDT",  "FETUSDT",  "NEARUSDT",
    "TONUSDT",  "APTUSDT",  "SUIUSDT",  "SHIBUSDT", "RENDERUSDT",
    "TIAUSDT",  "SEIUSDT",  "STXUSDT",  "LDOUSDT",  "MKRUSDT",
    # Meme / high-vol
    "PEPEUSDT", "WIFUSDT",  "BONKUSDT", "JUPUSDT",  "FLOKIUSDT",
    "MEMEUSDT", "DOGSUSDT", "NEIROUSDT","BRETTUSDT","1000SATSUSDT",
    # DeFi / AI / Gaming
    "AAVEUSDT", "CRVUSDT",  "GRTUSDT",  "SNXUSDT",  "ENJUSDT",
    "SANDUSDT", "MANAUSDT", "AXSUSDT",  "GALAUSDT", "IMXUSDT",
    "ALGOUSDT", "VETUSDT",  "FILUSDT",  "ICPUSDT",  "HBARUSDT",
]

BUDGET_PER_TRADE_USDT   = 100.0
MAX_OPEN_POSITIONS      = 9999   # no hard cap — USDT balance is the only limit
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
import os as _os
def _data_dir() -> str:
    c = _os.getenv("DATA_DIR", "/data")
    try:
        _os.makedirs(c, exist_ok=True)
        p = _os.path.join(c, ".cfg_probe")
        open(p, "w").close(); _os.remove(p)
        return c
    except OSError:
        return _os.path.dirname(_os.path.abspath(__file__))
_DATA_DIR     = _data_dir()
STRATEGY_FILE = _os.path.join(_DATA_DIR, "strategy.json")

# ── Two-speed architecture constants ─────────────────────────────────────────
SCAN_INTERVAL_SEC    = 60      # REST backup cache refresh — WebSocket handles real-time
STOP_LOSS_PCT        = 0.005   # 0.5% stop-loss
COOLDOWN_AFTER_LOSS  = 180     # 3 min cooldown after stop-loss
MIN_SIGNALS_TO_BUY   = 2       # at least 2 of 4 signals must be bullish
RSI_BUY_MIN          = 20
RSI_BUY_MAX          = 80      # wide window — scalping works across most RSI zones
VOLUME_RATIO_MIN     = 1.1     # volume must be 1.1× the 20-candle average

# ── Budget allocation settings ────────────────────────────────────────────────
BUDGET_MODE           = "percent" # fixed | percent | capped | per_coin
BUDGET_FIXED_USDT     = 100.0
BUDGET_PCT_OF_FREE    = 5.0       # 5% of free USDT per trade
BUDGET_TOTAL_CAP_USDT = 1000.0
# Total USDT from the wallet that the bot is allowed to use (paper + live).
# 0 = unlimited (use the entire free USDT balance). Min enforced on the
# frontend = 5 USDT. The bot will never deploy more than this across all
# concurrent open positions, so the user can keep the rest of the wallet safe.
BOT_ALLOCATION_USDT   = 0.0
BUDGET_PER_COIN = {
    "BTCUSDT":  200.0,
    "ETHUSDT":  150.0,
    "SOLUSDT":  100.0,
    "BNBUSDT":  100.0,
    "DOGEUSDT": 50.0,
}
