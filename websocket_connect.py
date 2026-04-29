# websocket_connect.py

import asyncio
import json
import websockets
from event_bus import market_data_queue

# 🔒 DEFAULT WS URL (infrastructure-level)
DEFAULT_WS_URL = "ws://127.0.0.1:8765"


async def websocket_client(ws_url, api_key, instruments):
    """
    WebSocket Client
    ----------------
    • Uses hardcoded default WS URL
    • API key MUST come from env
    • Fails loudly on errors
    """

    # -------------------------------
    # Resolve WS URL
    # -------------------------------
    if not ws_url:
        ws_url = DEFAULT_WS_URL

    if not api_key:
        print("[WS][ERROR] API_KEY missing (check .env)", flush=True)
        return

    print(f"[WS] Connecting to WebSocket @ {ws_url}", flush=True)

    try:
        async with websockets.connect(ws_url) as ws:
            # -------------------------------
            # AUTH
            # -------------------------------
            auth_payload = {
                "action": "authenticate",
                "api_key": api_key
            }
            await ws.send(json.dumps(auth_payload))
            print("[WS] Authentication sent", flush=True)

            # -------------------------------
            # SUBSCRIBE
            # -------------------------------
            for inst in instruments:
                sub_payload = {
                    "action": "subscribe",
                    "exchange": inst["exchange"],
                    "symbol": inst["symbol"]
                }
                await ws.send(json.dumps(sub_payload))
                print(
                    f"[WS] Subscribed {inst['exchange']}:{inst['symbol']}",
                    flush=True
                )

            # -------------------------------
            # RECEIVE LOOP
            # -------------------------------
            async for message in ws:
                try:
                    data = json.loads(message)
                except json.JSONDecodeError:
                    continue

                if data.get("type") == "market_data":
                    await market_data_queue.put(data)

    except Exception as e:
        print(f"[WS][ERROR] WebSocket connection failed: {e}", flush=True)
