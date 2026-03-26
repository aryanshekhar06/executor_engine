# ═══════════════════════════════════════════════════════════════════════
#  scanner.py — Tick scanner entry point
#
#  CONNECTION:
#    ping_interval=None — Deriv ignores WS protocol pings, causes faster drop
#    API {"ping":1} every 20s — real round-trip, keeps session alive
#    Proactive reconnect every 10 min — before natural ~13 min server drop
#
#  KEY FIX:
#    process_tick() and close_candle() errors are caught LOCALLY.
#    They must NEVER propagate to the outer except block — that block
#    is for genuine connection errors only. When candle logic errors
#    were propagating up, they triggered fake reconnects.
# ═══════════════════════════════════════════════════════════════════════

import asyncio
import json
import time

from config         import SYMBOLS, TIMEFRAME, STREAK_REQUIRED
from logger         import log, log_section
from connection     import connect, keepalive
from candle_tracker import init_symbol, process_tick

RECONNECT_INTERVAL = 600   # proactive reconnect every 10 min (drop happens at ~13 min)


# ═══════════════════════════════════════════════════════════════════════
#  KEEPALIVE — Deriv API level, NOT websocket level
# ═══════════════════════════════════════════════════════════════════════




# ═══════════════════════════════════════════════════════════════════════
#  LOAD + VALIDATE SYMBOLS
# ═══════════════════════════════════════════════════════════════════════

async def load_symbols() -> list:
    ws_init = await connect()
    try:
        await ws_init.send(json.dumps({"active_symbols": "brief", "product_type": "basic"}))
        data = json.loads(await asyncio.wait_for(ws_init.recv(), timeout=15))

        if data.get("error"):
            raise Exception("active_symbols error: " + data["error"]["message"])

        available = {s["symbol"].upper() for s in data["active_symbols"]
                     if "forex" in s.get("market", "").lower()}
        validated = [s for s in SYMBOLS if s.upper() in available]
        missing   = [s for s in SYMBOLS if s.upper() not in available]

        if missing:
            log(f"⚠️  Not found on Deriv: {missing}")
        log(f"✅ {len(validated)} / {len(SYMBOLS)} symbols validated")
        return validated

    finally:
        try:
            await ws_init.close()
        except Exception:
            pass


# ═══════════════════════════════════════════════════════════════════════
#  ONE SESSION
#  Returns on genuine connection drop only.
#  Candle state fully preserved — streaks survive reconnects.
# ═══════════════════════════════════════════════════════════════════════

async def run_session(symbols: list):
    ws = await connect()

    # Initialise only NEW symbols — existing candle state preserved
    for symbol in symbols:
        init_symbol(symbol)

    # Subscribe all symbols
    log(f"Subscribing to {len(symbols)} tick streams...")
    for symbol in symbols:
        await ws.send(json.dumps({"ticks": symbol, "subscribe": 1}))

    # Drain subscription confirmations
    confirmed = set()
    log("Waiting for subscription confirmations...")
    while len(confirmed) < len(symbols):
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=20)
            msg = json.loads(raw)

            if msg.get("error"):
                log(f"Sub error: {msg['error']['message']}")
                continue

            if msg.get("msg_type") == "tick" and msg.get("tick"):
                sym = msg["tick"]["symbol"]
                confirmed.add(sym)
                try:
                    await process_tick(sym, float(msg["tick"]["quote"]), msg["tick"]["epoch"])
                except Exception as e:
                    log(f"⚠️  Tick error during confirm ({sym}): {e}")

        except asyncio.TimeoutError:
            log("Confirmation timeout — continuing")
            break

    log(f"✅ {len(confirmed)} / {len(symbols)} live | Streaks preserved")
    log(f"Streak : {STREAK_REQUIRED} candles | TF : {TIMEFRAME}s")

    stop_ka       = asyncio.Event()
    session_start = asyncio.get_event_loop().time()
    asyncio.create_task(keepalive(ws, stop_ka))

    try:
        while True:

            # Proactive reconnect before server drops us (~13 min natural limit)
            elapsed = asyncio.get_event_loop().time() - session_start
            if elapsed >= RECONNECT_INTERVAL:
                log(f"🔄 Proactive reconnect after {int(elapsed)}s (streaks preserved)")
                return

            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=60)
                msg = json.loads(raw)

                if msg.get("msg_type") == "tick" and msg.get("tick"):
                    tick = msg["tick"]
                    try:
                        # ── Isolated: errors here must NOT cause a reconnect ──
                        await process_tick(
                            tick["symbol"],
                            float(tick["quote"]),
                            tick["epoch"],
                        )
                    except Exception as e:
                        log(f"⚠️  Tick error ({tick['symbol']}): {e}")

                elif msg.get("msg_type") == "ping":
                    pass   # API ping reply — ignore silently

                elif msg.get("error"):
                    log(f"Server: {msg['error']['message']}")

            except asyncio.TimeoutError:
                # 60s with no data at all — genuine network issue
                log("⚠️  No data for 60s — reconnecting (streaks preserved)")
                return

    except Exception as e:
        # Only genuine WebSocket / connection errors reach here
        log(f"⚠️  Connection lost: {e} — reconnecting (streaks preserved)")

    finally:
        stop_ka.set()
        try:
            await ws.close()
        except Exception:
            pass


# ═══════════════════════════════════════════════════════════════════════
#  HEARTBEAT
# ═══════════════════════════════════════════════════════════════════════

async def heartbeat():
    while True:
        await asyncio.sleep(60)
        log(f"SCANNER RUNNING | {time.strftime('%H:%M:%S')}")


# ═══════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════

async def main():
    log_section("SCANNER STARTED")
    log(f"   Symbols         : {len(SYMBOLS)}")
    log(f"   Streak required : {STREAK_REQUIRED} candles")
    log(f"   Timeframe       : {TIMEFRAME}s ({TIMEFRAME // 60} min)")
    log(f"   Signal target   : localhost:8765 (executor)")
    log(f"   Keepalive       : API ping/20s + proactive reconnect/10min")

    symbols = await load_symbols()
    asyncio.create_task(heartbeat())

    fail_count = 0
    while True:
        try:
            await run_session(symbols)
            fail_count = 0
        except Exception as e:
            log(f"Session error: {e}")
            fail_count += 1

        wait = min(15, 3 + max(0, fail_count - 1) * 4)
        log(f"Reconnecting in {wait}s...")
        await asyncio.sleep(wait)


asyncio.run(main())
