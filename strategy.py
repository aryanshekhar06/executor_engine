# ═══════════════════════════════════════════════════════════════════════
#  strategy.py — Direction logic + stake ladder
#
#  SIGNAL TIME (market_selector, force_trade=False):
#    ADX < 20         → NO TRADE — skip this market
#    20 ≤ ADX < 25    → REVERSAL, 5 levels
#    25 ≤ ADX ≤ 40    → TREND-FOLLOW with EMA50, 3 levels
#    ADX > 40         → NO TRADE — skip this market
#
#  MARTINGALE LEVELS (martingale, force_trade=True):
#    Max levels already fixed at signal time — never changes.
#    Only direction is re-determined from fresh ADX:
#    ADX < 25         → REVERSAL  (includes ADX < 20 fallback)
#    ADX ≥ 25         → TREND-FOLLOW with EMA50  (includes ADX > 40 fallback)
#
#  ADX PERIOD: 14 candles × 15 min = 3.5 hours
# ═══════════════════════════════════════════════════════════════════════





from config import MARTINGALE_STAKES, MAX_LEVELS

ADX_NO_TRADE  = 20
ADX_REVERSAL  = 25
ADX_MAX_TRADE = 40


def resolve_direction(signal_direction: str, adx: int,
                      plus_di: int = 0, minus_di: int = 0,
                      current_price: float = 0.0,
                      ema50: float = 0.0,
                      force_trade: bool = False,
                      fixed_max_levels: int = None) -> tuple:
    """
    Returns (trade_direction, max_levels, mode_label).
    Returns (None, 0, reason) only at signal time (force_trade=False)
    when ADX is out of the tradeable range.

    force_trade=False  → signal time (market_selector)
                         Returns None if ADX < 20 or ADX > 40
    force_trade=True   → martingale level placement
                         Never returns None
                         fixed_max_levels passed in to preserve
                         the level count decided at signal time

    Direction zones (always applied):
      ADX < 25   → REVERSAL (scanner direction)
      ADX ≥ 25   → TREND-FOLLOW confirmed by EMA50
    """
    di_spread  = abs(plus_di - minus_di)

    # ── SIGNAL TIME: check no-trade zones ─────────────────────────────
    if not force_trade:
        if adx < ADX_NO_TRADE:
            return None, 0, (f"NO TRADE | ADX={adx} < {ADX_NO_TRADE} — too weak/choppy")
        if adx > ADX_MAX_TRADE:
            return None, 0, (f"NO TRADE | ADX={adx} > {ADX_MAX_TRADE} — too strong")

    # ── Max levels ────────────────────────────────────────────────────
    # Martingale: use fixed value from signal time — never recalculate
    # Signal time: calculate from ADX
    if fixed_max_levels is not None:
        max_levels = fixed_max_levels
    elif adx < ADX_REVERSAL:
        max_levels = MAX_LEVELS   # 5 levels
    else:
        max_levels = 3            # 3 levels

    # ── Direction ─────────────────────────────────────────────────────
    # ADX < 25 (including < 20 fallback) → REVERSAL
    if adx < ADX_REVERSAL:
        mode = (f"REVERSAL | ADX={adx} | "
                f"+DI={plus_di} -DI={minus_di} spread={di_spread} | "
                f"max {max_levels} levels")
        return signal_direction, max_levels, mode

    # ADX ≥ 25 (including > 40 fallback) → TREND-FOLLOW with EMA50
    if ema50 == 0.0 or current_price == 0.0:
        mode = (f"REVERSAL (EMA50 unavailable | ADX={adx}) | "
                f"max {max_levels} levels")
        return signal_direction, max_levels, mode

    direction   = "RISE" if current_price > ema50 else "FALL"
    trend_label = (f"price={round(current_price, 5)} "
                   f"{'>' if current_price > ema50 else '<'} "
                   f"EMA50={round(ema50, 5)} → "
                   f"{'UPTREND' if current_price > ema50 else 'DOWNTREND'}")
    mode = (f"TREND-FOLLOW | ADX={adx} | {trend_label} | "
            f"+DI={plus_di} -DI={minus_di} spread={di_spread} | "
            f"max {max_levels} levels")
    return direction, max_levels, mode


def get_stake(level: int) -> float:
    """Returns stake for the given 0-indexed level."""
    if level < len(MARTINGALE_STAKES):
        return MARTINGALE_STAKES[level]
    return MARTINGALE_STAKES[-1]
