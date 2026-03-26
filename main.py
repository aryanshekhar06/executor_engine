# ═══════════════════════════════════════════════════════════════════════
#  main.py  (EXECUTOR SIDE)
#
#  One websocket (ws_global) only — pure execution, no selection.
#  Signal arrives pre-enriched from market_selector (scanner side).
#
#  Signal handling safety:
#    handle_signal() ONLY stores the signal.
#    ALL ws_global access happens inside engine() — never two
#    coroutines touching the same socket simultaneously.
# ═══════════════════════════════════════════════════════════════════════

import asyncio
import json

import websockets

from config       import SIGNAL_HOST, SIGNAL_PORT, MAX_DAILY_LOSS, SYMBOLS
from logger       import log, log_section
from connection   import connect_executor, fetch_balance
from trade_engine import check_contract
import martingale as mg
import stats


# ── Global state ──────────────────────────────────────────────────────
ws_global       = None
active_trade    = None
pending_signal  = None   # accepted signal, waiting for active_trade to clear
incoming_signal = None   # raw arrival, evaluated by engine on next tick
balance         = "0.00 USD"   # always a live string from fetch_balance()




# ═══════════════════════════════════════════════════════════════════════
#  SIGNAL SERVER  (TCP port 8765)
# ═══════════════════════════════════════════════════════════════════════

async def handle_signal(reader, writer):
    """
    Receives ONE signal from market_selector.
    Only stores it — engine loop evaluates on its own tick.
    ws_global is never touched here.
    """
    global incoming_signal
    try:
        data = await reader.read(2048)
        if not data:
            return
        signal = json.loads(data.decode())
        log(f"📩 SIGNAL → {signal['symbol']} | {signal['direction']} | "
            f"ADX={signal.get('adx', 0)} "
            f"+DI={signal.get('plus_di', 0)} "
            f"-DI={signal.get('minus_di', 0)}")
        incoming_signal = signal
    except Exception as e:
        log(f"Signal error: {e}")
    finally:
        writer.close()
        await writer.wait_closed()


async def signal_server():
    server = await asyncio.start_server(handle_signal, SIGNAL_HOST, SIGNAL_PORT)
    log(f"📡 Signal server on port {SIGNAL_PORT}")
    async with server:
        await server.serve_forever()


# ═══════════════════════════════════════════════════════════════════════
#  ENGINE LOOP
# ═══════════════════════════════════════════════════════════════════════

async def engine():
    global ws_global, active_trade, pending_signal, incoming_signal, balance

    while True:
        try:

            # ── Ensure connection ──────────────────────────────────────
            if not ws_global:
                ws_global = await connect_executor()
                balance   = await fetch_balance(ws_global)
                log(f"💰 Balance: ${balance}")

                # Verify active trade after reconnect
                if active_trade and active_trade.get("cid"):
                    log(f"🔍 Verifying CID={active_trade['cid']} after reconnect...")
                    contract = await check_contract(ws_global, active_trade["cid"])
                    if contract is None:
                        log("⚠️  Unreachable → clearing stale trade")
                        active_trade = None
                    elif contract.get("is_sold"):
                        profit = float(contract.get("profit", 0))
                        result = "WIN ✅" if profit > 0 else "LOSS ❌"
                        log(f"📋 Closed while offline | {result} | ${round(profit, 2)}")
                        if profit <= 0:
                            mg.record_loss(active_trade["total_staked"])
                        active_trade = None
                    else:
                        log("✅ Trade still open — resuming")

                elif active_trade and not active_trade.get("cid"):
                    log(f"🔄 Reconnected — retrying placement "
                        f"{active_trade['symbol']} L{active_trade['level'] + 1}")

            # ── Daily halt check ───────────────────────────────────────
            if mg.is_halted():
                await asyncio.sleep(60)
                continue

            # ══ EVALUATE INCOMING SIGNAL ══════════════════════════════
            # Done here (not in handle_signal) so ws_global is only ever
            # accessed from one coroutine at a time.
            if incoming_signal:
                signal          = incoming_signal
                incoming_signal = None

                if not active_trade:
                    # No trade running — accept directly
                    pending_signal = signal

                elif not active_trade.get("cid"):
                    # Placement in progress — ignore
                    log("⏭️  Signal ignored — placement in progress")

                else:
                    # Trade running — check floating P&L via ws_global
                    contract = await check_contract(ws_global, active_trade["cid"])

                    if contract is None:
                        log("⚠️  Could not check floating — signal ignored")

                    elif contract.get("is_sold"):
                        profit = float(contract.get("profit", 0))
                        log(f"   Contract already settled | P&L=${round(profit, 2)}")
                        if profit > 0:
                            log("✅ Settled as WIN → accepting new signal")
                            pending_signal = signal
                            active_trade   = None
                        else:
                            log(f"❌ Settled as LOSS → martingale continues on "
                                f"{active_trade['symbol']} L{active_trade['level'] + 1}")
                    else:
                        floating = float(contract.get("profit", 0))
                        log(f"   Floating P&L: ${round(floating, 2)}")
                        if floating > 0:
                            log("✅ Floating PROFIT → queued for after close")
                            pending_signal = signal
                        else:
                            log(f"❌ Floating LOSS → martingale continues on "
                                f"{active_trade['symbol']} L{active_trade['level'] + 1}")

            # ══ START NEW TRADE ════════════════════════════════════════
            if not active_trade and pending_signal:
                signal         = pending_signal
                pending_signal = None

                log_section("NEW TRADE")
                log(f"   Symbol    : {signal['symbol']}")
                log(f"   Direction : {signal['direction']}")
                adx    = signal.get("adx",       0)
                plus_di  = signal.get("plus_di",  0)
                minus_di = signal.get("minus_di", 0)
                spread = signal.get("di_spread",  0)
                log(f"   ADX       : {adx} | +DI={plus_di} | -DI={minus_di} | Spread={spread}")
                if adx > 40 or spread > 20:
                    log("   Trend     : STRONG | 3 levels max")
                elif adx > 28:
                    log("   Trend     : HIGH VOLATILITY | 3 levels max")
                else:
                    log("   Trend     : NORMAL | 5 levels max")
                active_trade = mg.make_trade(signal)
                log(f"🟢 Cycle started → {active_trade['symbol']}")

            # ══ PROCESS ACTIVE TRADE ══════════════════════════════════
            if active_trade:
                result = await mg.process_trade(ws_global, active_trade)

                if result == "WIN":
                    balance = await fetch_balance(ws_global)
                    log(f"💰 Balance: ${balance}")
                    active_trade = None
                    if pending_signal:
                        log("🔄 Picking up pending signal after WIN")

                elif result == "EXHAUSTED":
                    balance        = await fetch_balance(ws_global)
                    log(f"💰 Balance: ${balance}")
                    active_trade   = None
                    pending_signal = None

            await asyncio.sleep(1)

        except (websockets.exceptions.ConnectionClosed,
                websockets.exceptions.WebSocketException) as e:
            log(f"🔴 WS disconnected: {e} → Reconnecting")
            ws_global = None
            await asyncio.sleep(3)

        except Exception as e:
            log(f"⚠️  Engine error: {e}")
            import traceback; traceback.print_exc()
            await asyncio.sleep(3)


# ═══════════════════════════════════════════════════════════════════════
#  HEARTBEAT
# ═══════════════════════════════════════════════════════════════════════

async def heartbeat():
    # heartbeat NEVER touches ws_global — only reads the global balance
    # variable which is updated exclusively by the engine loop.
    while True:
        await asyncio.sleep(60)

        trade_info = (
            f"{active_trade['symbol']} "
            f"L{active_trade['level'] + 1}/{active_trade['max_levels']} "
            f"| CID={active_trade.get('cid', 'placing')} "
            f"| Staked=${round(active_trade['total_staked'], 2)}"
        ) if active_trade else "idle"

        status = "🛑 HALTED" if mg.is_halted() else "✅ ACTIVE"

        log_section("HEARTBEAT")
        log(f"   Status      : {status}")
        log(f"   Balance     : ${balance}")
        log(f"   Daily loss  : ${round(mg.daily_loss, 2)} / ${MAX_DAILY_LOSS}")
        log(f"   Trade       : {trade_info}")
        log(f"   Pending     : {pending_signal['symbol'] if pending_signal else 'none'}")
        for line in stats.summary():
            log(line)


# ═══════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ═══════════════════════════════════════════════════════════════════════

async def main():
    log_section("EXECUTOR STARTED")
    log(f"   Symbols    : {len(SYMBOLS)}")
    log(f"   Daily limit: ${MAX_DAILY_LOSS}")
    log(f"   Levels     : 5 → capped to 3 if ADX>28 or DI spread>20")
    log(f"   Stakes     : $1 → $2.7 → $6.3 → $14.7 → $34.3")
    log(f"   WS opens   : 1 (ws_global only)")
    log(f"   Selection  : Scanner (market_selector.py)")

    await asyncio.gather(
        signal_server(),
        engine(),
        heartbeat(),
    )


asyncio.run(main())