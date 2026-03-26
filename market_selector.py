# ═══════════════════════════════════════════════════════════════════════
#  market_selector.py  (SCANNER SIDE)
#
#  Pipeline for every batch of simultaneous signals:
#
#    1. COLLECT — gather all signals fired in the same 3-second window
#    2. PAYOUT  — fetch payout ratio per symbol; discard < 1.75
#                 (martingale cannot recover losses at ≤75% payout)
#    3. SELECT  — prefer streak > STREAK_PREFER_MIN (5) first;
#                 if multiple qualify, use ADX closest to average;
#                 if none qualify, fall back to ADX method on all
#    4. SEND    — dispatch ONE enriched signal to executor
# ═══════════════════════════════════════════════════════════════════════

import asyncio
import json

from config        import TIMEFRAME, PAYOUT_MIN_RATIO, STREAK_PREFER_MIN
from connection    import open_temp_ws
from logger        import log
from signal_sender import send_payload
from strategy      import resolve_direction


COLLECTION_WINDOW = 3   # seconds to wait for more signals before deciding

# Internal state
_pending    : dict  = {}   # symbol → {direction, streak}
_timer_task         = None
_lock               = asyncio.Lock()


# ═══════════════════════════════════════════════════════════════════════
#  INDICATORS — ADX / +DI / -DI (Wilder's method)
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


async def _fetch_symbol_data(symbol: str, direction: str) -> dict:
    """
    Opens ONE temp WS per symbol.
    Fetches candles (for ADX/DI/EMA50) and a $1 proposal (for payout).
    Returns full data dict. Never raises.
    """
    contract_type = "PUT" if direction == "FALL" else "CALL"

    try:
        async with open_temp_ws() as ws:
            await ws.send(json.dumps({
                "ticks_history": symbol,
                "count":         60,
                "end":           "latest",
                "style":         "candles",
                "granularity":   TIMEFRAME,
            }))
            await ws.send(json.dumps({
                "proposal":      1,
                "amount":        1,
                "basis":         "stake",
                "contract_type": contract_type,
                "currency":      "USD",
                "duration":      15,
                "duration_unit": "m",
                "symbol":        symbol,
            }))

            candles_data = None
            payout_ratio = 0.0
            received     = 0

            deadline = asyncio.get_running_loop().time() + 15
            while received < 2:
                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    break
                raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
                msg = json.loads(raw)
                if "candles" in msg:
                    candles_data = msg["candles"]
                    received += 1
                elif "proposal" in msg:
                    p = msg["proposal"]
                    payout_ratio = float(p.get("payout", 0)) / 1.0
                    received += 1
                elif "error" in msg:
                    log(f"⚠️  {symbol}: API error — {msg['error']['message']}")
                    break

        adx, plus_di, minus_di, current_price, ema50, ema9, adx_prev, di_spread_prev = _calc_indicators(candles_data or [])
        return {
            "adx":            adx,
            "plus_di":        plus_di,
            "minus_di":       minus_di,
            "di_spread":      abs(plus_di - minus_di),
            "payout_ratio":   payout_ratio,
            "current_price":  current_price,
            "ema50":          ema50,
            "ema9":           ema9,
            "adx_prev":       adx_prev,
            "di_spread_prev": di_spread_prev,
        }

    except Exception as e:
        log(f"⚠️  market_selector: fetch failed ({symbol}): {e}")
        return {"adx": 0, "plus_di": 0, "minus_di": 0, "di_spread": 0,
                "payout_ratio": 0.0, "current_price": 0.0, "ema50": 0.0,
                "ema9": 0.0, "adx_prev": 0, "di_spread_prev": 0}



def _adx_select(pool: list) -> dict:
    """Pick symbol closest to average ADX in pool."""
    if len(pool) == 1:
        return pool[0]
    avg_adx = sum(s["adx"] for s in pool) / len(pool)
    return min(pool, key=lambda s: abs(s["adx"] - avg_adx))


async def _select_and_send():
    global _pending, _timer_task

    async with _lock:
        candidates = dict(_pending)
        _pending.clear()
        _timer_task = None

    if not candidates:
        return

    symbols = list(candidates.keys())
    log(f"🔍 Evaluating {len(symbols)} signal(s): {symbols}")

    # ── Fetch payout + indicators concurrently ────────────────────────
    tasks   = [_fetch_symbol_data(s, candidates[s]["direction"]) for s in symbols]
    results = await asyncio.gather(*tasks)

    # ── APPLY ALL FILTERS BEFORE SELECTION ───────────────────────────
    # Every symbol must pass ALL criteria to enter the pool.
    # Bad markets are removed here — selection only sees clean candidates.
    pool = []
    discarded = []

    log(f"   {'Symbol':<15} | {'Streak':>6} | {'Payout':>6} | {'ADX':>4} | "
        f"{'+DI':>4} | {'-DI':>4} | {'Spread':>6} | {'Status'}")
    log(f"   {'-'*15} | {'-'*6} | {'-'*6} | {'-'*4} | "
        f"{'-'*4} | {'-'*4} | {'-'*6} | {'-'*20}")

    for symbol, data in zip(symbols, results):
        signal_direction = candidates[symbol]["direction"]
        streak           = candidates[symbol]["streak"]
        adx              = data["adx"]
        plus_di          = data["plus_di"]
        minus_di         = data["minus_di"]
        di_spread        = data["di_spread"]
        payout           = data["payout_ratio"]
        current_price    = data.get("current_price", 0.0)
        ema50            = data.get("ema50", 0.0)
        ema9             = data.get("ema9", 0.0)
        adx_prev         = data.get("adx_prev", 0)
        di_spread_prev   = data.get("di_spread_prev", 0)

        # Check 1: Payout filter
        if payout > 0 and payout < PAYOUT_MIN_RATIO:
            reason = f"payout={payout:.2f} < {PAYOUT_MIN_RATIO}"
            log(f"   {symbol:<15} | {streak:>6} | {payout:>6.2f} | {adx:>4} | "
                f"{plus_di:>4} | {minus_di:>4} | {di_spread:>6} | 🚫 {reason}")
            discarded.append((symbol, reason))
            continue

        # Check 2: ADX/strategy filter — resolve direction now
        direction, max_levels, mode = resolve_direction(
            signal_direction, adx, plus_di, minus_di,
            current_price=current_price, ema50=ema50,
            ema9=ema9, adx_prev=adx_prev,
            di_spread_prev=di_spread_prev
        )

        if direction is None:
            # ADX < 20 or ADX > 40 — not tradeable
            reason = mode
            log(f"   {symbol:<15} | {streak:>6} | {payout:>6.2f} | {adx:>4} | "
                f"{plus_di:>4} | {minus_di:>4} | {di_spread:>6} | ⛔ {reason}")
            discarded.append((symbol, reason))
            continue

        # Passed all filters — add to tradeable pool
        log(f"   {symbol:<15} | {streak:>6} | {payout:>6.2f} | {adx:>4} | "
            f"{plus_di:>4} | {minus_di:>4} | {di_spread:>6} | ✅ {mode}")

        pool.append({
            "symbol":           symbol,
            "signal_direction": signal_direction,
            "direction":        direction,
            "streak":           streak,
            "adx":              adx,
            "plus_di":          plus_di,
            "minus_di":         minus_di,
            "di_spread":        di_spread,
            "payout_ratio":     payout,
            "current_price":    current_price,
            "ema50":            ema50,
            "ema9":             ema9,
            "adx_prev":         adx_prev,
            "di_spread_prev":   di_spread_prev,
            "max_levels":       max_levels,
        })

    if discarded:
        log(f"   Discarded {len(discarded)}/{len(symbols)}: "
            f"{[d[0] for d in discarded]}")

    if not pool:
        log("❌ No tradeable markets after all filters — skipping")
        return

    # ── SELECT BEST FROM CLEAN POOL ───────────────────────────────────
    # Prefer higher streaks first, then closest ADX to pool average
    high_streak = [s for s in pool if s["streak"] > STREAK_PREFER_MIN]
    select_from = high_streak if high_streak else pool
    best        = _adx_select(select_from)

    log(f"✅ Selected → {best['symbol']} | {best['direction']} | "
        f"Streak={best['streak']} | Payout={best['payout_ratio']:.2f} | "
        f"ADX={best['adx']} +DI={best['plus_di']} -DI={best['minus_di']}")
    log(f"   Max levels : {best['max_levels']}")

    payload = {
        "symbol":           best["symbol"],
        "signal_direction": best["signal_direction"],
        "direction":        best["direction"],
        "adx":              best["adx"],
        "plus_di":          best["plus_di"],
        "minus_di":         best["minus_di"],
        "di_spread":        best["di_spread"],
        "current_price":    best["current_price"],
        "ema50":            best["ema50"],
        "ema9":             best.get("ema9", 0.0),
        "adx_prev":         best.get("adx_prev", 0),
        "di_spread_prev":   best.get("di_spread_prev", 0),
    }
    await send_payload(payload)


# ═══════════════════════════════════════════════════════════════════════
#  PUBLIC API — called by candle_tracker
# ═══════════════════════════════════════════════════════════════════════

async def submit(symbol: str, direction: str, streak: int):
    """Called when a streak is detected. Adds to pool and (re)starts timer."""
    global _timer_task

    async with _lock:
        _pending[symbol] = {"direction": direction, "streak": streak}
        log(f"📥 market_selector: {symbol} | {direction} | streak={streak} "
            f"({len(_pending)} in pool)")

        if _timer_task and not _timer_task.done():
            _timer_task.cancel()

    _timer_task = asyncio.create_task(_wait_and_select())


async def _wait_and_select():
    try:
        await asyncio.sleep(COLLECTION_WINDOW)
        await _select_and_send()
    except asyncio.CancelledError:
        pass