# ═══════════════════════════════════════════════════════════════════════
#  indicators.py — ADX background cache
#
#  ADX CALCULATION (matches proven old executor exactly):
#    Simple average of (high - low) for the last 14 candles.
#    This is the volatility proxy the old executor used — not Wilder's ADX.
#    Range: any positive float (price units, e.g. 0.00082)
#
#  Public API:
#    start_cache(symbols)   : launches background refresh every 55s
#    get(symbol) -> dict    : {adx, updated_at}
#    get_best(signals) -> signal : signal whose ADX is closest to avg ADX
# ═══════════════════════════════════════════════════════════════════════

import asyncio
import json
import time

from config     import TIMEFRAME, CANDLE_FETCH_COUNT, INDICATOR_REFRESH_SECS
from connection import open_temp_ws
from logger     import log


# ── In-memory cache ─────────────────────────────────────────────────────
_cache: dict     = {}
_cache_lock       = asyncio.Lock()


# ═══════════════════════════════════════════════════════════════════════
#  CALCULATION — simple H-L average (same as old executor)
# ═══════════════════════════════════════════════════════════════════════

def _calc_adx(candles: list, period: int = 14) -> tuple:
    """
    Wilder's ADX — returns (adx, plus_di, minus_di) all as integers 0-100.

    adx      : trend strength (0=no trend, 100=max trend)
    plus_di  : bullish directional pressure
    minus_di : bearish directional pressure

    DI SPREAD = |plus_di - minus_di|
      High spread (>20) = strong directional bias even if ADX is low.
      This catches slow grinding trends that ADX alone misses.

    Example (EUR/AUD 6 green candles, ADX=26):
      plus_di=38, minus_di=12, spread=26 → strong bull bias → don't fade
    """
    if len(candles) < period * 2 + 1:
        return 0, 0, 0

    highs  = [float(c["high"])  for c in candles]
    lows   = [float(c["low"])   for c in candles]
    closes = [float(c["close"]) for c in candles]

    pdm_list, mdm_list, tr_list = [], [], []
    for i in range(1, len(candles)):
        h, l, pc = highs[i], lows[i], closes[i - 1]
        up   = h - highs[i - 1]
        down = lows[i - 1] - l
        pdm_list.append(up   if up   > down and up   > 0 else 0.0)
        mdm_list.append(down if down > up   and down > 0 else 0.0)
        tr_list.append(max(h - l, abs(h - pc), abs(l - pc)))

    # Wilder smoothing seed
    s_tr  = sum(tr_list[:period])
    s_pdm = sum(pdm_list[:period])
    s_mdm = sum(mdm_list[:period])

    dx_list = []
    for i in range(period, len(tr_list)):
        s_tr  = s_tr  - s_tr  / period + tr_list[i]
        s_pdm = s_pdm - s_pdm / period + pdm_list[i]
        s_mdm = s_mdm - s_mdm / period + mdm_list[i]
        if s_tr == 0:
            dx_list.append(0.0)
            continue
        pdi   = 100.0 * s_pdm / s_tr
        mdi   = 100.0 * s_mdm / s_tr
        denom = pdi + mdi
        dx_list.append(100.0 * abs(pdi - mdi) / denom if denom else 0.0)

    if not dx_list:
        return 0, 0, 0

    # Wilder smooth the DX list to get ADX
    adx_val = sum(dx_list[:period]) / period
    for dx in dx_list[period:]:
        adx_val = (adx_val * (period - 1) + dx) / period

    # Final +DI and -DI from last smoothed values
    plus_di  = int(round(100.0 * s_pdm / s_tr)) if s_tr else 0
    minus_di = int(round(100.0 * s_mdm / s_tr)) if s_tr else 0

    return int(round(adx_val)), plus_di, minus_di


# ═══════════════════════════════════════════════════════════════════════
#  FETCH ONE SYMBOL
# ═══════════════════════════════════════════════════════════════════════

async def _fetch_symbol(symbol: str) -> tuple:
    """Fetches candles and returns (adx, plus_di, minus_di). Never raises."""
    try:
        async with open_temp_ws() as ws:
            await ws.send(json.dumps({
                "ticks_history": symbol,
                "count":         30,
                "end":           "latest",
                "style":         "candles",
                "granularity":   TIMEFRAME,
            }))
            raw  = await asyncio.wait_for(ws.recv(), timeout=10)
            data = json.loads(raw)

        if "error" in data:
            return 0, 0, 0
        return _calc_adx(data.get("candles", []))

    except Exception as e:
        log(f"⚠️  indicators._fetch_symbol({symbol}): {e}")
        return 0, 0, 0


# ═══════════════════════════════════════════════════════════════════════
#  BACKGROUND REFRESH LOOP — batched to avoid rate limits
# ═══════════════════════════════════════════════════════════════════════

async def _refresh_loop(symbols: list):
    BATCH = 5   # max concurrent websockets per round

    while True:
        t0      = time.time()
        results = {}

        for i in range(0, len(symbols), BATCH):
            batch   = symbols[i:i + BATCH]
            tasks   = [_fetch_symbol(s) for s in batch]
            batch_r = await asyncio.gather(*tasks, return_exceptions=True)
            for symbol, result in zip(batch, batch_r):
                if not isinstance(result, Exception):
                    results[symbol] = result
            if i + BATCH < len(symbols):
                await asyncio.sleep(0.5)

        async with _cache_lock:
            for symbol, adx in results.items():
                _cache[symbol] = {
                    "adx":        adx,
                    "updated_at": int(time.time()),
                }

        elapsed = round(time.time() - t0, 1)
        log(f"📊 Indicators refreshed {len(results)}/{len(symbols)} symbols in {elapsed}s")

        await asyncio.sleep(INDICATOR_REFRESH_SECS)


def start_cache(symbols: list):
    """Launch background refresh. Call once from main.py."""
    return asyncio.create_task(_refresh_loop(symbols))


# ═══════════════════════════════════════════════════════════════════════
#  PUBLIC READ API
# ═══════════════════════════════════════════════════════════════════════

def get(symbol: str) -> dict:
    """Returns cached {adx, plus_di, minus_di, di_spread, updated_at}."""
    return _cache.get(symbol, {"adx": 0, "plus_di": 0, "minus_di": 0, "di_spread": 0, "updated_at": 0})


def get_best(signals: list) -> dict:
    """
    Returns the signal whose ADX is closest to the average ADX
    across all signals — same logic as old executor's select_market_after_reset.
    Always returns one signal.
    """
    if len(signals) == 1:
        return signals[0]

    scored = []
    for sig in signals:
        adx = get(sig["symbol"])["adx"]
        scored.append({**sig, "adx": adx})

    avg_adx = sum(s["adx"] for s in scored) / len(scored)
    best    = min(scored, key=lambda x: abs(x["adx"] - avg_adx))

    log(f"   Avg ADX = {round(avg_adx, 5)}")
    log(f"   {'Symbol':<15} | {'ADX':>8} | {'Δ avg':>8}")
    log(f"   {'-'*15} | {'-'*8} | {'-'*8}")
    for s in scored:
        marker = " ◀ SELECTED" if s["symbol"] == best["symbol"] else ""
        log(f"   {s['symbol']:<15} | {s['adx']:>8.5f} | "
            f"{abs(s['adx']-avg_adx):>8.5f}{marker}")

    return best
