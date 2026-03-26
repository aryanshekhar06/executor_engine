# ═══════════════════════════════════════════════════════════════════════
#  signal_sender.py  (SCANNER SIDE)
#
#  Responsibility: Send ONE ready signal payload to the executor via TCP.
#  No logic, no selection, no indicator fetching — just transport.
#
#  Called by: market_selector.py → send_payload(signal_dict)
#
#  Payload format:
#    {
#      "symbol":    "frxEURJPY",
#      "direction": "FALL",
#      "adx":       35,
#      "plus_di":   28,
#      "minus_di":  12,
#      "di_spread": 16
#    }
# ═══════════════════════════════════════════════════════════════════════

import asyncio
import json

from config import SIGNAL_HOST, SIGNAL_PORT
from logger import log


async def send_payload(signal: dict):
    """
    Sends signal dict to executor on port 8765.
    Retries every 2s if executor is not ready.
    """
    payload = json.dumps(signal).encode()

    while True:
        try:
            reader, writer = await asyncio.open_connection(SIGNAL_HOST, SIGNAL_PORT)
            writer.write(payload)
            await writer.drain()
            writer.close()
            await writer.wait_closed()
            log(f"🚀 SIGNAL SENT → {signal['symbol']} | {signal['direction']} | "
                f"ADX={signal.get('adx', 0)} "
                f"+DI={signal.get('plus_di', 0)} "
                f"-DI={signal.get('minus_di', 0)}")
            return
        except Exception:
            log(f"⚠️  Executor not ready — retrying {signal['symbol']}...")
            await asyncio.sleep(2)
