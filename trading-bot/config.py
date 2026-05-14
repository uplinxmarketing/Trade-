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
    # High-volume additions (reaching 100 total)
    "WLDUSDT",  "PYTHUSDT", "JTOUSDT",  "STRKUSDT", "BOMEUSDT",
    "ORDIUSDT", "KASUSDT",  "CFXUSDT",  "MASKUSDT", "BANDUSDT",
    "1INCHUSDT","CHZUSDT",  "BATUSDT",  "MINAUSDT", "KAVAUSDT",
    "DYDXUSDT", "OCEANUSDT","ZRXUSDT",  "COMPUSDT", "ROSEUSDT",
    "KSMUSDT",  "CKBUSDT",  "JOEUSDT",  "OMUSDT",   "TURBOUSDT",
    "RDNTUSDT", "LRCUSDT",  "RVNUSDT",  "IOSTUSDT", "LPTUSDT",
    "YFIUSDT",  "AGIXUSDT", "SUSHIUSDT","PENDLEUSDT","RUNEUSDT",
    "MANTAUSDT","METISUSDT","THETAUSDT","ILVUSDT",  "QNTUSDT",
    "APEUSDT",  "ENAUSDT",  "GMXUSDT",  "ETCUSDT",  "XLMUSDT",
]

BUDGET_PER_TRADE_USDT   = 5.0
MAX_OPEN_POSITIONS      = 30
DECISION_INTERVAL_SEC   = 600       # Claude strategy runs every 10 minutes
MIN_CLAUDE_CONFIDENCE   = 0.65
RSI_OVERBOUGHT          = 75
RSI_OVERSOLD            = 25
CANDLE_TIMEFRAME        = "1m"
CANDLE_LOOKBACK         = 50
FEE_RATE                = 0.001    # 0.1% standard Binance spot (USDT, no BNB discount)
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

# ── Two-speed architecture constants ────────────────────────────────────────────
SCAN_INTERVAL_SEC    = 30      # REST backup cache refresh — WebSocket handles real-time
STOP_LOSS_PCT        = 0.005   # 0.5% stop-loss
COOLDOWN_AFTER_LOSS  = 30
MIN_SIGNALS_TO_BUY   = 4       # at least 4 of 6 signals must be bullish (raised from 3)
RSI_BUY_MIN          = 40      # was 45 — buy on dips; 40 catches oversold without being too aggressive
RSI_BUY_MAX          = 60      # was 65 — avoid overbought entries; blocks buys near RSI tops
VOLUME_RATIO_MIN     = 1.3     # volume must be 1.3× the 20-candle average

# ── ATR filter thresholds ────────────────────────────────────────────────────────────────────────────────────────────────────────
ATR_MIN_PCT = 0.0005   # ATR must be ≥ 0.05% of price
ATR_MAX_PCT = 0.015    # ATR must be ≤ 1.5% of price (too volatile → skip)
ATR_PERIOD  = 14       # standard ATR lookback period

# ── Budget allocation settings ──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
BUDGET_MODE           = "fixed"   # fixed | percent | capped | per_coin
BUDGET_FIXED_USDT     = 5.0       # $5 per trade
BUDGET_PCT_OF_FREE    = 5.0       # 5% of free USDT per trade (when mode=percent)
BUDGET_TOTAL_CAP_USDT = 110.0
# Total USDT the bot is allowed to use across all concurrent open positions.
# 0 = unlimited. Default $110 cap so the bot stays within the user's allocation.
BOT_ALLOCATION_USDT   = 0        # 0 = unlimited — don't cap concurrent positions
BUDGET_PER_COIN = {
    "BTCUSDT":  200.0,
    "ETHUSDT":  150.0,
    "SOLUSDT":  100.0,
    "BNBUSDT":  100.0,
    "DOGEUSDT": 50.0,
}

# ── Futures paper-trading agent ────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
FUTURES_ENABLED           = True
FUTURES_WATCHED_COINS     = [
    "BTCUSDT", "ETHUSDT",  "SOLUSDT",  "BNBUSDT",  "XRPUSDT",
    "ADAUSDT", "AVAXUSDT", "DOGEUSDT", "LINKUSDT",  "ARBUSDT",
    "OPUSDT",  "INJUSDT",  "NEARUSDT", "FETUSDT",   "TIAUSDT",
]
FUTURES_STARTING_USDT     = 3000.0   # paper wallet starting balance
FUTURES_BUDGET_USDT       = 100.0    # margin per position
FUTURES_LEVERAGE          = 5        # default leverage
FUTURES_TAKE_PROFIT_PCT   = 0.005    # 0.5 % price move from entry (= 2.5% ROE at 5x)
FUTURES_STOP_LOSS_PCT     = 0.003    # 0.3 % price move from entry
FUTURES_MIN_SIGNALS       = 3        # of 6 signals needed to open (3 fires more reliably)
FUTURES_SCAN_INTERVAL_SEC = 60       # signal scan frequency
FUTURES_MAX_POSITIONS     = 20       # max concurrent open positions
