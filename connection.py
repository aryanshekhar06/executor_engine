# ═══════════════════════════════════════════════════════════════════════
#  connection.py — WebSocket connection helpers
#
#  TWO connection modes:
#
#  connect_scanner() — ping_interval=None + API ping every 20s + proactive 10min reconnect
#    Used by: scanner.py ONLY
#    Why: 22 symbols close simultaneously → burst of messages → WS-level
#         pong gets delayed → ping_timeout kills connection. Solved by
#         disabling WS pings and using Deriv API {"ping":1} instead.
#
#  connect_executor() — ping_interval=20, ping_timeout=30
#    Used by: main.py (ws_global) ONLY
#    Why: Executor is sequential, no burst traffic. WS-level pings are
#         stable and match the proven old executor settings exactly.
#         No manual keepalive needed — WS handles it at protocol level.
#
#  open_temp_ws() — ping_interval=20, ping_timeout=30
#    Used by: indicators.py (short-lived fetches)
#    Why: Short-lived connections, WS pings are fine here.
#
#  connect() — alias for connect_scanner() for backward compatibility
# ═══════════════════════════════════════════════════════════════════════

import asyncio
import json
import websockets
from contextlib import asynccontextmanager

from config import API_TOKEN, WS_URL
from logger import log


async def safe_recv(ws, timeout=10):
    """Receive one message. Returns parsed dict or None on any error."""
    try:
        raw = await asyncio.wait_for(ws.recv(), timeout=timeout)
        return json.loads(raw)
    except Exception:
        return None


# ═══════════════════════════════════════════════════════════════════════
#  SCANNER CONNECTION — dual keepalive: WS-level + Deriv API ping
# ═══════════════════════════════════════════════════════════════════════

async def connect_scanner() -> websockets.WebSocketClientProtocol:
    """
    For scanner.py only.

    KEEPALIVE STRATEGY:
      - ping_interval=None — Deriv's server does NOT respond to
        WebSocket protocol-level ping frames. Enabling them causes
        the library to close the connection after ping_timeout when
        it never receives a pong (drops in ~4 min instead of ~13 min).

      - Deriv API {"ping":1} every 20s via keepalive() task.
        The server responds with {"msg_type":"ping"} — a real
        application-level round trip that keeps the session alive.

      - Proactive reconnect every 10 minutes via scanner's main loop.
        The natural drop happens at ~13 min. Reconnecting at 10 min
        keeps streaks alive and avoids any disruption at candle close.
    """
    attempt = 0
    while True:
        attempt += 1
        try:
            log(f"🔌 Connecting... (attempt {attempt})")
            ws = await websockets.connect(
                WS_URL,
                ping_interval=None,   # Deriv ignores WS pings — use API ping
                ping_timeout=None,
                close_timeout=10,
                open_timeout=20,
                max_queue=256,        # buffer 22-symbol burst at candle boundary
            )
            await ws.send(json.dumps({"authorize": API_TOKEN}))
            auth = await safe_recv(ws, timeout=15)

            if auth and "error" not in auth:
                bal = auth.get("authorize", {}).get("balance", "?")
                cur = auth.get("authorize", {}).get("currency", "USD")
                log(f"✅ Connected | Balance: {bal} {cur}")
                return ws

            err = auth.get("error", {}).get("message", "no response") if auth else "no response"
            log(f"❌ Auth failed: {err}")
            await ws.close()

        except Exception as e:
            log(f"❌ Connection error: {e}")

        wait = min(30, attempt * 2)
        log(f"⏳ Retry in {wait}s...")
        await asyncio.sleep(wait)


# ── Alias for scanner backward compatibility ───────────────────────────
connect = connect_scanner


# ═══════════════════════════════════════════════════════════════════════
#  EXECUTOR CONNECTION — WS-level pings, no manual keepalive needed
#  Matches proven old executor settings exactly.
# ═══════════════════════════════════════════════════════════════════════

async def connect_executor() -> websockets.WebSocketClientProtocol:
    """
    For main.py (ws_global) only.
    Uses WS-level ping_interval=20 — same as the proven old executor.
    Sequential usage, no burst traffic, so pings are stable.
    No manual keepalive task needed.
    """
    attempt = 0
    while True:
        attempt += 1
        try:
            log(f"🔌 Connecting... (attempt {attempt})")
            ws = await websockets.connect(
                WS_URL,
                ping_interval=20,    # WS-level keepalive every 20s
                ping_timeout=30,     # generous timeout for reliability
                close_timeout=10,
                open_timeout=20,
            )
            await ws.send(json.dumps({"authorize": API_TOKEN}))
            auth = await safe_recv(ws, timeout=15)

            if auth and "error" not in auth:
                bal = auth.get("authorize", {}).get("balance", "?")
                cur = auth.get("authorize", {}).get("currency", "USD")
                log(f"✅ Connected | Balance: {bal} {cur}")
                return ws

            err = auth.get("error", {}).get("message", "no response") if auth else "no response"
            log(f"❌ Auth failed: {err}")
            await ws.close()

        except Exception as e:
            log(f"❌ Connection error: {e}")

        wait = min(30, attempt * 2)
        log(f"⏳ Retry in {wait}s...")
        await asyncio.sleep(wait)


# ═══════════════════════════════════════════════════════════════════════
#  MANUAL KEEPALIVE — for scanner only
# ═══════════════════════════════════════════════════════════════════════

async def keepalive(ws, stop_event: asyncio.Event):
    """
    Sends Deriv API {"ping":1} every 20s. For scanner use only.
    Works alongside WS-level pings (ping_interval=60) for dual keepalive.
    """
    while not stop_event.is_set():
        try:
            await asyncio.sleep(20)
            if not stop_event.is_set():
                await ws.send(json.dumps({"ping": 1}))
        except Exception:
            break


# ═══════════════════════════════════════════════════════════════════════
#  TEMP WS — for indicators (short-lived fetches)
# ═══════════════════════════════════════════════════════════════════════

@asynccontextmanager
async def open_temp_ws():
    """
    Short-lived authorized websocket for indicator fetches.
    WS-level pings are fine here — connections are brief.
    """
    ws = None
    try:
        ws = await websockets.connect(
            WS_URL,
            ping_interval=20,
            ping_timeout=30,
            open_timeout=20,
        )
        await ws.send(json.dumps({"authorize": API_TOKEN}))
        auth = json.loads(await asyncio.wait_for(ws.recv(), timeout=10))
        if "error" in auth:
            raise ConnectionError(f"Auth failed: {auth['error']['message']}")
        yield ws
    finally:
        if ws:
            try:
                await ws.close()
            except Exception:
                pass


# ═══════════════════════════════════════════════════════════════════════
#  BALANCE FETCH — always real, always from Deriv API
# ═══════════════════════════════════════════════════════════════════════

async def fetch_balance(ws) -> str:
    """
    Fetches live account balance from Deriv.
    Returns formatted string e.g. "9849.69 USD".
    Never returns a calculated value — always live API.
    Raises WebSocketException on connection drop (engine reconnects).
    """
    import asyncio, json as _json
    await ws.send(_json.dumps({"balance": 1, "subscribe": 0}))
    deadline = asyncio.get_running_loop().time() + 10
    while True:
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            return "unknown"
        raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
        msg = _json.loads(raw)
        if "balance" in msg:
            b   = msg["balance"]
            amt = float(b.get("balance",  0))
            cur = b.get("currency", "USD")
            return f"{amt:.2f} {cur}"
        if "error" in msg:
            return "unknown"
