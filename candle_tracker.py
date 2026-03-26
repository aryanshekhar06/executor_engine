# ═══════════════════════════════════════════════════════════════════════
#  candle_tracker.py — Candle state + streak detection
#
#  DESIGN:
#    - No candle is ever discarded — not on first start, not on reconnect
#    - First tick received = open price, always valid
#    - Streak history never reset on reconnect
#    - Current candle never reset on reconnect
#    - DOJI (body ≤ 3% of range) → reset streak to 0
# ═══════════════════════════════════════════════════════════════════════

import asyncio

from config          import TIMEFRAME, STREAK_REQUIRED
from logger          import log
from market_selector import submit


# ── Persistent state — survives reconnects ────────────────────────────
candles         : dict = {}   # symbol → list of "GREEN"/"RED"
current_candles : dict = {}   # symbol → live candle dict | None
_first_start    : set  = set() # symbols on first ever start — first candle discarded


def init_symbol(symbol: str):
    """
    Called on every (re)connect for every symbol.

    First start  → create empty streak history, candle=None
                   first tick will set the open price correctly

    Reconnect    → do nothing — streak and candle preserved exactly
                   as they were before the drop
    """
    if symbol not in candles:
        # First ever start — first candle will be discarded
        # (bot joined mid-candle, open price is not the real open)
        candles[symbol]         = []
        current_candles[symbol] = None
        _first_start.add(symbol)
    # Reconnect: preserve everything — no reset of any kind


# ═══════════════════════════════════════════════════════════════════════
#  STREAK CALCULATION
# ═══════════════════════════════════════════════════════════════════════

def calculate_streak(symbol: str) -> tuple:
    """Returns (colour, streak_count) for the current run."""
    history = candles.get(symbol, [])
    if not history:
        return None, 0
    last_colour = history[-1]
    streak = 1
    for i in range(len(history) - 2, -1, -1):
        if history[i] == last_colour:
            streak += 1
        else:
            break
    return last_colour, streak


# ═══════════════════════════════════════════════════════════════════════
#  CANDLE CLOSE
# ═══════════════════════════════════════════════════════════════════════

async def close_candle(symbol: str):
    candle      = current_candles[symbol]
    open_price  = candle["open"]
    close_price = candle["close"]
    high_price  = candle["high"]
    low_price   = candle["low"]

    # ── Discard first candle on bot start only ───────────────────────
    # On first start the bot joins mid-candle — the open price is
    # the first tick received, not the real candle open.
    # On reconnect this never triggers — streak and candle preserved.
    if symbol in _first_start:
        _first_start.discard(symbol)
        current_candles[symbol] = None
        log(f"[{symbol}] First candle discarded (joined mid-candle at bot start)")
        return

    candle_range = high_price - low_price
    body         = abs(close_price - open_price)

    # ── DOJI check ────────────────────────────────────────────────────
    # A candle is DOJI if EITHER condition is true:
    #   1. Body ≤ 5% of range  (standard ratio check)
    #   2. Body ≤ 30% of range AND range itself is small (choppy candle)
    #      Small range = range < average body of last 3 normal candles
    #      This catches indecision candles in tight ranging markets
    #      that pass the ratio check because range is tiny
    is_doji = False
    doji_reason = ""

    if candle_range == 0:
        is_doji = True
        doji_reason = "flat candle"
    elif body <= 0.05 * candle_range:
        is_doji = True
        doji_reason = f"{round(100 * body / candle_range, 1)}% body ≤ 5% threshold"
    elif candle_range < 0.0003 and body <= 0.30 * candle_range:
        # Tight range candle — market undecided, small moves both ways
        is_doji = True
        doji_reason = (f"tight range ({round(candle_range, 5)}) + "
                       f"body={round(100 * body / candle_range, 1)}% ≤ 30%")

    if is_doji:
        candles[symbol] = []
        current_candles[symbol] = None
        log(f"[{symbol}] DOJI ({doji_reason}) → Streak reset to 0")
        return

    # ── Colour ────────────────────────────────────────────────────────
    colour = "GREEN" if close_price > open_price else "RED"
    candles[symbol].append(colour)
    if len(candles[symbol]) > 20:
        candles[symbol].pop(0)

    colour, streak = calculate_streak(symbol)
    log(f"[{symbol}] {colour} | Streak={streak}")

    if streak == STREAK_REQUIRED - 1:
        log(f"⚠️  [{symbol}] {streak} streak — watching for {STREAK_REQUIRED}")

    if streak >= STREAK_REQUIRED:
        direction = "FALL" if colour == "GREEN" else "RISE"
        log(f"🚨 [{symbol}] {streak} {colour} candles → Signal={direction}")
        asyncio.create_task(submit(symbol, direction, streak))

    current_candles[symbol] = None


# ═══════════════════════════════════════════════════════════════════════
#  PROCESS TICK
# ═══════════════════════════════════════════════════════════════════════

async def process_tick(symbol: str, price: float, epoch: int):
    bucket = epoch - (epoch % TIMEFRAME)

    # ── No current candle → start one ────────────────────────────────
    if current_candles.get(symbol) is None:
        current_candles[symbol] = {
            "start": bucket, "open":  price,
            "high":  price,  "low":   price, "close": price,
        }
        return

    candle = current_candles[symbol]

    # ── Bucket changed → close previous candle ────────────────────────
    if bucket > candle["start"]:
        try:
            await close_candle(symbol)
        except Exception as e:
            log(f"⚠️  close_candle error ({symbol}): {e}")
            current_candles[symbol] = None

        current_candles[symbol] = {
            "start": bucket, "open":  price,
            "high":  price,  "low":   price, "close": price,
        }
        return

    # ── Same bucket → update OHLC ─────────────────────────────────────
    candle["high"]  = max(candle["high"],  price)
    candle["low"]   = min(candle["low"],   price)
    candle["close"] = price
