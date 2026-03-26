# ═══════════════════════════════════════════════════════════════════════
#  config.py — ALL constants in one place
#  Only edit this file to tune the bot behaviour
# ═══════════════════════════════════════════════════════════════════════

API_TOKEN  = "t1yhvF3EeiN2hyK"
WS_URL     = "wss://ws.derivws.com/websockets/v3?app_id=1089"

SIGNAL_HOST = "localhost"
SIGNAL_PORT = 8765

TIMEFRAME  = 900   # 15-minute candles

# Martingale — hardcoded stakes from proven old executor
MAX_LEVELS       = 5
BASE_STAKE       = 1.0
MARTINGALE_STAKES = [1, 2.7, 6.3, 14.7, 34.3]   # level 1–5

# Daily circuit breaker
MAX_DAILY_LOSS = 50.0

# ADX thresholds (from proven old executor)
ADX_CAP_LIMIT   = 28   # above this → cap martingale to 3 levels
ADX_TREND_LIMIT = 40   # above this → trend-follow mode (keep direction, log only)

# How often background indicator cache refreshes (seconds)
INDICATOR_REFRESH_SECS = 55   # slightly under candle close so data is always fresh

# Candles fetched per symbol
CANDLE_FETCH_COUNT = 60

# Contract settings
CONTRACT_DURATION      = 15
CONTRACT_DURATION_UNIT = "m"

# Last-window early exit
LAST_WINDOW_SECS     = 10   # last 10s of contract — check floating profit

# Minimum payout ratio to trade a symbol (75% profit = 1.75 payout per $1 stake)
# Symbols below this cannot recover martingale losses — discard them
PAYOUT_MIN_RATIO     = 1.75

# Last-window early loss threshold
# If floating P&L ≤ this value in last LAST_WINDOW_SECS, declare LOSS early
# and place next martingale level immediately.
# Values above this threshold (small loss or profit) wait for natural close.
# Never declare early WIN — floating profit is unreliable in last seconds.
EARLY_LOSS_THRESHOLD = -1.0

# Streak threshold for post-WIN market preference
STREAK_PREFER_MIN    = 5   # prefer assets with streak > this after WIN

# ATR normalisation reference for 15-min forex
ATR_MIN = 0.0001
ATR_MAX = 0.0020

# ── Scanner ─────────────────────────────────────────────────────────────
STREAK_REQUIRED = 4   # consecutive same-colour candles before signal fires

# Forex symbols — shared by BOTH scanner and executor
SYMBOLS = [
    "frxAUDJPY", "frxEURAUD", "frxEURCAD", "frxAUDUSD", "frxEURCHF",
    "frxEURGBP", "frxEURJPY", "frxEURUSD", "frxGBPAUD", "frxGBPJPY",
    "frxGBPUSD", "frxUSDCAD", "frxUSDCHF", "frxUSDJPY", "frxAUDCAD",
    "frxAUDCHF", "frxAUDNZD", "frxEURNZD", "frxGBPCAD", "frxGBPCHF",
    "frxGBPNZD", "frxNZDUSD",
]
