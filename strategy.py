# ═══════════════════════════════════════════════════════════════════════
#  strategy.py — Direction logic + stake ladder
#
#  REVERSAL (unchanged):
#    ADX 20–28 → scanner direction, 5 levels
#
#  TREND-FOLLOW (new strict rules):
#    ADX 28–40 → ALL of the following must be true:
#
#    BUY (RISE):
#      EMA9 > EMA50       price short-term above long-term trend
#      +DI  > -DI         bullish directional pressure
#      DI spread > 20     strong directional bias
#      Spread increasing  momentum building (not fading)
#      ADX rising         trend strengthening
#
#    SELL (FALL):
#      EMA9 < EMA50       price short-term below long-term trend
#      -DI  > +DI         bearish directional pressure
#      DI spread > 20     strong directional bias
#      Spread increasing  momentum building
#      ADX rising         trend strengthening
#
#    If ANY condition fails → REVERSAL (scanner direction)
#
#  ADX PERIOD : 14 candles × 15 min = 3.5 hours
#  EMA9  period: 9  candles × 15 min = 2.25 hours
#  EMA50 period: 50 candles × 15 min = 12.5 hours
# ═══════════════════════════════════════════════════════════════════════

from config import MARTINGALE_STAKES, MAX_LEVELS

ADX_NO_TRADE  = 20
ADX_REVERSAL  = 28
ADX_MAX_TRADE = 40
DI_SPREAD_MIN = 20   # minimum DI spread for trend-follow


def resolve_direction(signal_direction: str, adx: int,
                      plus_di: int = 0, minus_di: int = 0,
                      current_price: float = 0.0,
                      ema50: float = 0.0,
                      ema9: float = 0.0,
                      adx_prev: int = 0,
                      di_spread_prev: int = 0,
                      force_trade: bool = False,
                      fixed_max_levels: int = None) -> tuple:
    """
    Returns (trade_direction, max_levels, mode_label).
    Returns (None, 0, reason) only at signal time when ADX out of range.

    New parameters:
      ema9           : EMA9 value (faster trend confirmation)
      adx_prev       : ADX value of previous candle (for rising check)
      di_spread_prev : DI spread of previous candle (for increasing check)
    """
    di_spread = abs(plus_di - minus_di)

    # ── SIGNAL TIME: no-trade zones ───────────────────────────────────
    if not force_trade:
        if adx < ADX_NO_TRADE:
            return None, 0, f"NO TRADE | ADX={adx} < {ADX_NO_TRADE} — too weak"
        if adx > ADX_MAX_TRADE:
            return None, 0, f"NO TRADE | ADX={adx} > {ADX_MAX_TRADE} — too strong"

    # ── Max levels ────────────────────────────────────────────────────
    if fixed_max_levels is not None:
        max_levels = fixed_max_levels
    elif adx < ADX_REVERSAL:
        max_levels = MAX_LEVELS
    else:
        max_levels = 3

    # ── ZONE 1: ADX < 28 → REVERSAL ───────────────────────────────────
    if adx < ADX_REVERSAL:
        mode = (f"REVERSAL | ADX={adx} | "
                f"+DI={plus_di} -DI={minus_di} spread={di_spread} | "
                f"max {max_levels} levels")
        return signal_direction, max_levels, mode

    # ── ZONE 2: ADX 28-40 → TREND-FOLLOW with strict conditions ───────
    # Check all conditions — any failure → fall back to REVERSAL

    adx_rising    = adx > adx_prev if adx_prev > 0 else True
    spread_rising = di_spread > di_spread_prev if di_spread_prev > 0 else True

    buy_conditions = {
        "EMA9 > EMA50":    ema9 > ema50 if (ema9 > 0 and ema50 > 0) else False,
        "+DI > -DI":       plus_di > minus_di,
        "spread > 20":     di_spread > DI_SPREAD_MIN,
        "spread rising":   spread_rising,
        "ADX rising":      adx_rising,
    }

    sell_conditions = {
        "EMA9 < EMA50":    ema9 < ema50 if (ema9 > 0 and ema50 > 0) else False,
        "-DI > +DI":       minus_di > plus_di,
        "spread > 20":     di_spread > DI_SPREAD_MIN,
        "spread rising":   spread_rising,
        "ADX rising":      adx_rising,
    }

    buy_ok  = all(buy_conditions.values())
    sell_ok = all(sell_conditions.values())

    # Determine expected trend direction
    if buy_ok and signal_direction == "FALL":
        # Scanner says FALL (after green candles), trend confirms RISE
        # → TREND-FOLLOW → RISE
        failed = [k for k, v in buy_conditions.items() if not v]
        mode = (f"TREND-FOLLOW BUY | ADX={adx}↑ | "
                f"EMA9={round(ema9,5)} > EMA50={round(ema50,5)} | "
                f"+DI={plus_di} -DI={minus_di} spread={di_spread}↑ | "
                f"max {max_levels} levels")
        return "RISE", max_levels, mode

    elif sell_ok and signal_direction == "RISE":
        # Scanner says RISE (after red candles), trend confirms FALL
        # → TREND-FOLLOW → FALL
        mode = (f"TREND-FOLLOW SELL | ADX={adx}↑ | "
                f"EMA9={round(ema9,5)} < EMA50={round(ema50,5)} | "
                f"+DI={plus_di} -DI={minus_di} spread={di_spread}↑ | "
                f"max {max_levels} levels")
        return "FALL", max_levels, mode

    else:
        # Conditions not met → fall back to reversal
        if signal_direction == "FALL":
            failed = [k for k, v in buy_conditions.items() if not v]
        else:
            failed = [k for k, v in sell_conditions.items() if not v]

        mode = (f"REVERSAL (trend conditions failed: {', '.join(failed)}) | "
                f"ADX={adx} | +DI={plus_di} -DI={minus_di} spread={di_spread} | "
                f"max {max_levels} levels")
        return signal_direction, max_levels, mode


def get_stake(level: int) -> float:
    """Returns stake for the given 0-indexed level."""
    if level < len(MARTINGALE_STAKES):
        return MARTINGALE_STAKES[level]
    return MARTINGALE_STAKES[-1]