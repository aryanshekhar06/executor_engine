# ═══════════════════════════════════════════════════════════════════════
#  trade_engine.py — Trade placement + contract monitoring
#
#  place_trade uses RAW recv (not safe_recv) so WebSocket errors
#  propagate naturally to the engine loop for reconnection.
#  safe_recv is only used in check_contract where None is acceptable.
# ═══════════════════════════════════════════════════════════════════════

import asyncio
import json
import websockets

from config     import CONTRACT_DURATION, CONTRACT_DURATION_UNIT
from connection import safe_recv
from logger     import log


async def _recv_expecting(ws, key: str, timeout: float = 10):
    """
    Receives messages until one contains `key`.
    Discards unrelated messages (ping replies etc).
    Raises WebSocketException on connection drop — never swallows it.
    Raises asyncio.TimeoutError if deadline exceeded.
    """
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            raise asyncio.TimeoutError(f"Timed out waiting for '{key}'")
        # Raw recv — WebSocket errors propagate up, never swallowed
        raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
        msg = json.loads(raw)
        if "error" in msg:
            raise RuntimeError(msg["error"].get("message", "API error"))
        if key in msg:
            return msg
        # Unrelated message (ping reply etc) — discard and loop


# ═══════════════════════════════════════════════════════════════════════
#  PLACE TRADE
# ═══════════════════════════════════════════════════════════════════════

async def place_trade(ws, symbol: str, direction: str, stake: float) -> tuple:
    """
    Returns (contract_id, buy_price) on success.
    Raises WebSocketException on connection drop → engine reconnects.
    Returns (None, None) on API/logic errors only.
    """
    log(f"🚀 PLACING | {symbol} | {direction} | ${stake}")
    contract_type = "PUT" if direction == "FALL" else "CALL"

    try:
        # ── Proposal ─────────────────────────────────────────────────
        await ws.send(json.dumps({
            "proposal":      1,
            "amount":        stake,
            "basis":         "stake",
            "contract_type": contract_type,
            "currency":      "USD",
            "duration":      CONTRACT_DURATION,
            "duration_unit": CONTRACT_DURATION_UNIT,
            "symbol":        symbol,
        }))
        res      = await _recv_expecting(ws, "proposal", timeout=10)
        proposal = res["proposal"]

        # ── Buy ───────────────────────────────────────────────────────
        await ws.send(json.dumps({
            "buy":   proposal["id"],
            "price": proposal["ask_price"],
        }))
        res       = await _recv_expecting(ws, "buy", timeout=10)
        cid       = res["buy"].get("contract_id")
        buy_price = float(res["buy"].get("buy_price", stake))

        if cid:
            log(f"✅ TRADE OPEN | CID={cid} | Paid=${buy_price}")
            return cid, buy_price

        log("❌ No contract_id in response")
        return None, None

    except (websockets.exceptions.ConnectionClosed,
            websockets.exceptions.WebSocketException):
        # Re-raise — engine loop catches this, resets ws_global, reconnects
        raise

    except asyncio.TimeoutError:
        log("❌ Placement timed out")
        return None, None

    except RuntimeError as e:
        log(f"❌ API error: {e}")
        return None, None

    except Exception as e:
        log(f"❌ place_trade error: {e}")
        return None, None


# ═══════════════════════════════════════════════════════════════════════
#  CHECK CONTRACT
# ═══════════════════════════════════════════════════════════════════════

async def check_contract(ws, cid: int) -> dict:
    """
    Polls contract status.
    Re-raises WebSocketException — engine reconnects and re-verifies
    instead of clearing a live trade due to a dropped connection.
    Returns None only on timeout or non-WS errors (safe to retry).
    """
    try:
        await ws.send(json.dumps({
            "proposal_open_contract": 1,
            "contract_id": cid,
        }))
        deadline = asyncio.get_running_loop().time() + 10
        while True:
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                return None
            # Raw recv — WS errors propagate, unrelated msgs discarded
            raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
            msg = json.loads(raw)
            if "proposal_open_contract" in msg:
                return msg["proposal_open_contract"]
            # Discard unrelated message (ping reply etc) and retry

    except (websockets.exceptions.ConnectionClosed,
            websockets.exceptions.WebSocketException):
        # Re-raise — engine will reconnect and re-verify trade
        raise

    except Exception:
        return None
