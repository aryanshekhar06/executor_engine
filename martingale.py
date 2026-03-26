# ═══════════════════════════════════════════════════════════════════════
#  martingale.py  (EXECUTOR SIDE)
#
#  DIRECTION LOGIC:
#    market_selector determines direction at signal time (L1).
#    At every subsequent level → fetch FRESH ADX/DI → re-determine.
#    Direction can change level to level as market conditions change.
#
#  FLOATING VALUE RULES (last LAST_WINDOW_SECS seconds):
#    floating < -0.5        → EARLY LOSS → place next level immediately
#    -0.5 ≤ floating ≤ 0.5  → WAIT       → let trade close naturally
#    floating > 0.5         → EARLY WIN  → lock profit, start next process
# ═══════════════════════════════════════════════════════════════════════

import asyncio
import json
import time
import websockets

from logger       import log, log_section
from trade_engine import place_trade, check_contract
from strategy     import resolve_direction, get_stake
from connection   import fetch_balance
import stats
from config       import (MAX_LEVELS, LAST_WINDOW_SECS, MAX_DAILY_LOSS,
                          TIMEFRAME, WS_URL, API_TOKEN)

# Floating thresholds
EARLY_LOSS_THRESHOLD = -0.5
EARLY_WIN_THRESHOLD  =  0.5


# ═══════════════════════════════════════════════════════════════════════
#  FRESH INDICATORS — self-contained WS, called at every level
# ═══════════════════════════════════════════════════════════════════════

def _calc_indicators(candles: list, period: int = 14) -> tuple:
    """
    Exact TradingView ADX (Wilder, period=14).
    Returns (adx, plus_di, minus_di, current_price, ema50, ema9, adx_prev, di_spread_prev)
    Always returns 8 values.
    """
    if len(candles) < period * 2:
        return 0, 0, 0, 0.0, 0.0, 0.0, 0, 0

    highs  = [float(c["high"])  for c in candles]
    lows   = [float(c["low"])   for c in candles]
    closes = [float(c["close"]) for c in candles]

    tr_list, pdm_list, mdm_list = [], [], []
    for i in range(1, len(candles)):
        h, l, ph, pl, pc = highs[i], lows[i], highs[i-1], lows[i-1], closes[i-1]
        tr   = max(h - l, abs(h - pc), abs(l - pc))
        up   = h  - ph
        down = pl - l
        pdm_list.append(up   if up   > down and up   > 0 else 0.0)
        mdm_list.append(down if down > up   and down > 0 else 0.0)
        tr_list.append(tr)

    atr  = sum(tr_list[:period])
    pdm_ = sum(pdm_list[:period])
    mdm_ = sum(mdm_list[:period])

    def _dx(a, p, m):
        if a == 0: return 0.0
        pdi = 100.0 * p / a
        mdi = 100.0 * m / a
        s   = pdi + mdi
        return 100.0 * abs(pdi - mdi) / s if s else 0.0

    dx_list = [_dx(atr, pdm_, mdm_)]

    # Store previous values for adx_prev and di_spread_prev
    prev_atr  = atr
    prev_pdm_ = pdm_
    prev_mdm_ = mdm_

    for i in range(period, len(tr_list)):
        prev_atr  = atr
        prev_pdm_ = pdm_
        prev_mdm_ = mdm_
        atr  = atr  - atr  / period + tr_list[i]
        pdm_ = pdm_ - pdm_ / period + pdm_list[i]
        mdm_ = mdm_ - mdm_ / period + mdm_list[i]
        dx_list.append(_dx(atr, pdm_, mdm_))

    if len(dx_list) < period:
        return 0, 0, 0, 0.0, 0.0, 0.0, 0, 0

    adx = sum(dx_list[:period]) / period
    for dx in dx_list[period:]:
        adx = (adx * (period - 1) + dx) / period

    # adx_prev: ADX without last data point
    adx_prev_val = sum(dx_list[:period]) / period
    for dx in dx_list[period:-1]:
        adx_prev_val = (adx_prev_val * (period - 1) + dx) / period

    plus_di  = int(round(100.0 * pdm_ / atr)) if atr else 0
    minus_di = int(round(100.0 * mdm_ / atr)) if atr else 0

    # Previous DI spread
    prev_plus_di  = int(round(100.0 * prev_pdm_ / prev_atr)) if prev_atr else 0
    prev_minus_di = int(round(100.0 * prev_mdm_ / prev_atr)) if prev_atr else 0
    di_spread_prev = abs(prev_plus_di - prev_minus_di)

    # EMA50
    ema_period = 50
    if len(closes) >= ema_period:
        ema50 = sum(closes[:ema_period]) / ema_period
        k50   = 2.0 / (ema_period + 1)
        for price in closes[ema_period:]:
            ema50 = (price - ema50) * k50 + ema50
        ema50 = round(ema50, 5)
    else:
        ema50 = round(sum(closes) / len(closes), 5)

    # EMA9
    ema9_period = 9
    if len(closes) >= ema9_period:
        ema9 = sum(closes[:ema9_period]) / ema9_period
        k9   = 2.0 / (ema9_period + 1)
        for price in closes[ema9_period:]:
            ema9 = (price - ema9) * k9 + ema9
        ema9 = round(ema9, 5)
    else:
        ema9 = round(closes[-1], 5)

    current_price = closes[-1]

    return (int(round(adx)), plus_di, minus_di, current_price,
            ema50, ema9, int(round(adx_prev_val)), di_spread_prev)


async def _fetch_fresh_indicators(symbol: str) -> tuple:
    """
    Opens own WS, fetches 30 candles, returns (adx, plus_di, minus_di).
    Called before every level placement for fresh direction decision.
    Returns (0, 0, 0) on failure — caller uses cached values as fallback.
    """
    ws = None
    try:
        ws = await websockets.connect(
            WS_URL, ping_interval=20, ping_timeout=30, open_timeout=20
        )
        await ws.send(json.dumps({"authorize": API_TOKEN}))
        auth = json.loads(await asyncio.wait_for(ws.recv(), timeout=10))
        if "error" in auth:
            return 0, 0, 0, 0.0, 0.0
        await ws.send(json.dumps({
            "ticks_history": symbol, "count": 30, "end": "latest",
            "style": "candles", "granularity": TIMEFRAME,
        }))
        raw  = await asyncio.wait_for(ws.recv(), timeout=10)
        data = json.loads(raw)
        if "error" in data:
            return 0, 0, 0, 0.0, 0.0, 0.0, 0, 0
        raw_candles = data.get("candles", [])
        # Exclude last candle — it is still forming (mid-candle)
        # TradingView uses closed candles only — this makes values match
        closed_candles = raw_candles[:-1] if len(raw_candles) > 1 else raw_candles
        return _calc_indicators(closed_candles)
    except Exception:
        return 0, 0, 0, 0.0, 0.0, 0.0, 0, 0
    finally:
        if ws:
            try:
                await ws.close()
            except Exception:
                pass


# ═══════════════════════════════════════════════════════════════════════
#  DAILY LOSS TRACKER
# ═══════════════════════════════════════════════════════════════════════

daily_loss     = 0.0
trading_halted = False


def record_loss(amount: float):
    global daily_loss, trading_halted
    daily_loss += abs(amount)
    log(f"📊 Daily loss: ${round(daily_loss, 2)} / ${MAX_DAILY_LOSS}")
    if daily_loss >= MAX_DAILY_LOSS:
        trading_halted = True
        log("🛑 DAILY LOSS LIMIT HIT — Trading halted")


def is_halted() -> bool:
    return trading_halted


# ═══════════════════════════════════════════════════════════════════════
#  TRADE FACTORY
# ═══════════════════════════════════════════════════════════════════════

def make_trade(signal: dict) -> dict:
    return {
        "symbol":           signal["symbol"],
        "signal_direction": signal.get("signal_direction", signal["direction"]),
        "direction":        signal["direction"],   # determined by market_selector
        "adx":              signal.get("adx",       0),
        "plus_di":          signal.get("plus_di",   0),
        "minus_di":         signal.get("minus_di",  0),
        "di_spread":        signal.get("di_spread", 0),
        "current_price":    signal.get("current_price", 0.0),
        "ema50":            signal.get("ema50", 0.0),
        "level":            0,
        "max_levels":       MAX_LEVELS,
        "cid":              None,
        "buy_price":        0.0,
        "total_staked":     0.0,
        "_check_fails":     0,
    }


# ═══════════════════════════════════════════════════════════════════════
#  PLACE LEVEL — fresh ADX at every level, direction re-determined
# ═══════════════════════════════════════════════════════════════════════

async def _do_place(ws, trade: dict, level: int) -> tuple:
    """
    Level 1 (index 0): uses market_selector's determined direction.
    Level 2+ (index 1+): fetches FRESH ADX/DI → re-determines direction.

    This means direction can change at every level as market evolves.
    Returns (cid, buy_price).
    """
    if level == 0:
        # L1 — use direction already determined by market_selector
        adx           = trade["adx"]
        plus_di       = trade["plus_di"]
        minus_di      = trade["minus_di"]
        current_price = trade.get("current_price", 0.0)
        ema50         = trade.get("ema50", 0.0)
        direction = trade["direction"]
        _, max_levels, mode = resolve_direction(
            trade["signal_direction"], adx, plus_di, minus_di,
            current_price=current_price, ema50=ema50,
            force_trade=True,
            fixed_max_levels=trade["max_levels"]
        )

    else:
        # L2+ — fetch fresh ADX/DI and re-determine direction
        log(f"🔄 Fetching fresh indicators for L{level + 1} ({trade['symbol']})...")
        adx, plus_di, minus_di, current_price, ema50 = await _fetch_fresh_indicators(trade["symbol"])

        if adx == 0 and plus_di == 0:
            log(f"⚠️  Fresh fetch failed — using cached ADX={trade['adx']}")
            adx, plus_di, minus_di = trade["adx"], trade["plus_di"], trade["minus_di"]
            current_price = trade.get("current_price", 0.0)
            ema50         = trade.get("ema50", 0.0)
        else:
            log(f"📊 Fresh → ADX={adx} | +DI={plus_di} | -DI={minus_di} "
                f"| Spread={abs(plus_di - minus_di)} | Price={current_price} | EMA50={ema50}")
            trade["adx"]           = adx
            trade["plus_di"]       = plus_di
            trade["minus_di"]      = minus_di
            trade["di_spread"]     = abs(plus_di - minus_di)
            trade["current_price"] = current_price
            trade["ema50"]         = ema50

        direction, max_levels, mode = resolve_direction(
            trade["signal_direction"], adx, plus_di, minus_di,
            current_price=current_price, ema50=ema50,
            force_trade=True
        )
        trade["max_levels"] = max_levels

        # ADX out of range at martingale level — use previous direction
        # The NO TRADE rule is for NEW entries only.
        # Once in a martingale cycle we must continue to recover losses.
        if direction is None:
            direction = trade["direction"]   # keep last known good direction
            log(f"⚠️  ADX out of range at L{level + 1} — "
                f"using previous direction={direction} to continue recovery")
        else:
            trade["direction"] = direction

    log_section(f"LEVEL {level + 1} / {trade['max_levels']} | {trade['symbol']}")
    log(f"   Direction    : {direction}")
    log(f"   ADX          : {adx} | +DI={plus_di} | -DI={minus_di} "
        f"| Spread={abs(plus_di - minus_di)}")
    log(f"   Mode         : {mode}")
    log(f"   Total staked : ${round(trade['total_staked'], 2)}")
    log(f"   Stake        : ${get_stake(level)}")

    stake = get_stake(level)
    cid, buy_price = await place_trade(ws, trade["symbol"], direction, stake)
    return cid, buy_price or stake


# ═══════════════════════════════════════════════════════════════════════
#  ADVANCE TO NEXT LEVEL
# ═══════════════════════════════════════════════════════════════════════

async def _advance_level(ws, trade: dict) -> str:
    """Increments level, places next trade immediately. Shared by LOSS and EARLY LOSS."""
    next_level = trade["level"] + 1

    if next_level >= trade["max_levels"]:
        log("💀 Max levels reached → Reset")
        record_loss(trade["total_staked"])
        stats.record_loss(trade["total_staked"], trade["level"])
        return "EXHAUSTED"

    log(f"   Next stake      : ${get_stake(next_level)}")
    log(f"   Need to recover : ${round(trade['total_staked'], 2)} in L{next_level + 1}")

    trade["level"]     = next_level
    trade["cid"]       = None
    trade["buy_price"] = 0.0

    return await process_trade(ws, trade)


# ═══════════════════════════════════════════════════════════════════════
#  PROCESS TRADE — called every second from engine loop
#  Returns: "OPEN" | "WIN" | "EXHAUSTED"
# ═══════════════════════════════════════════════════════════════════════

async def process_trade(ws, trade: dict) -> str:

    # ══ PLACE CURRENT LEVEL ════════════════════════════════════════════
    if trade["cid"] is None:
        level = trade["level"]

        if level >= trade["max_levels"]:
            log(f"💀 Max levels ({trade['max_levels']}) reached → Reset")
            record_loss(trade["total_staked"])
            return "EXHAUSTED"

        try:
            cid, buy_price = await _do_place(ws, trade, level)
        except (websockets.exceptions.ConnectionClosed,
                websockets.exceptions.WebSocketException):
            raise   # engine reconnects and retries

        if not cid:
            log("❌ Placement failed → Reset")
            record_loss(trade["total_staked"])
            return "EXHAUSTED"

        trade["cid"]          = cid
        trade["buy_price"]    = buy_price
        trade["total_staked"] += buy_price
        return "OPEN"

    # ══ MONITOR CONTRACT ═══════════════════════════════════════════════
    contract = await check_contract(ws, trade["cid"])

    if not contract:
        trade["_check_fails"] += 1
        if trade["_check_fails"] >= 5:
            log("⚠️  Contract unreachable 5x → clearing")
            return "EXHAUSTED"
        return "OPEN"
    trade["_check_fails"] = 0

    # ── CONTRACT CLOSED ───────────────────────────────────────────────
    if contract.get("is_sold"):
        profit      = float(contract.get("profit", 0))
        level_label = f"L{trade['level'] + 1}/{trade['max_levels']}"

        if profit > 0:
            bal = await fetch_balance(ws)
            log_section(f"✅ WIN  {level_label} | {trade['symbol']}")
            log(f"   Profit       : +${round(profit, 2)}")
            log(f"   Total staked : ${round(trade['total_staked'], 2)}")
            log(f"   Net gain     : +${round(profit - trade['buy_price'], 2)}")
            log(f"   Balance      : {bal}")
            stats.record_win(profit, trade["level"], trade["total_staked"], was_early=False)
            return "WIN"

        # Confirmed LOSS
        bal = await fetch_balance(ws)
        log_section(f"❌ LOSS  {level_label} | {trade['symbol']}")
        log(f"   Lost         : -${round(trade['buy_price'], 2)}")
        log(f"   Total staked : ${round(trade['total_staked'], 2)}")
        log(f"   Balance      : {bal}")
        return await _advance_level(ws, trade)

    # ── LAST WINDOW ───────────────────────────────────────────────────
    expiry_time = contract.get("expiry_time", 0)
    remaining   = expiry_time - int(time.time()) if expiry_time else 999

    if remaining <= LAST_WINDOW_SECS:
        floating = float(contract.get("profit", 0))
        log(f"Last {remaining}s | Floating=${round(floating, 2)}")

        # Zone 1: floating < -0.5 → EARLY LOSS
        if floating < EARLY_LOSS_THRESHOLD:
            bal = await fetch_balance(ws)
            log_section(f"⚡ EARLY LOSS  L{trade['level'] + 1}/{trade['max_levels']} "
                        f"| {trade['symbol']}")
            log(f"   Floating     : ${round(floating, 2)}  (< {EARLY_LOSS_THRESHOLD})")
            log(f"   Balance      : {bal}")
            stats.record_early_loss()
            return await _advance_level(ws, trade)

        # Zone 2: -0.5 ≤ floating ≤ 0.5 → wait
        elif floating <= EARLY_WIN_THRESHOLD:
            log(f"   Wait zone — letting trade close naturally")

        # Zone 3: floating > 0.5 → EARLY WIN
        else:
            bal = await fetch_balance(ws)
            log_section(f"✅ EARLY WIN  L{trade['level'] + 1}/{trade['max_levels']} "
                        f"| {trade['symbol']}")
            log(f"   Floating     : +${round(floating, 2)}  (> {EARLY_WIN_THRESHOLD})")
            log(f"   Balance      : {bal}")
            stats.record_win(profit, trade["level"], trade["total_staked"], was_early=False)
            return "WIN"

    return "OPEN"