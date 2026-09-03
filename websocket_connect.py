import asyncio
import json
from typing import Awaitable, Callable, List, Optional

import websockets

import connection_log
from event_bus import market_data_queue, depth_data_queue
from market_time import now_kolkata

DEFAULT_WS_URL = "ws://127.0.0.1:8765"


async def websocket_client(
    ws_url: str | None,
    api_key: str,
    instruments: List[dict],
    mode: str,
    depth_levels: int = 5,
    conn_log_dir: Optional[str] = None,
    on_reconnect: Optional[Callable[[str, "datetime", "datetime"], Awaitable]] = None,
    background_tasks: Optional[set] = None,
):
    """
    WebSocket connection only:
    - authenticate
    - subscribe one mode per connection
    - forward incoming market_data packets to event_bus queue

    conn_log_dir: if set, DAY_STARTED / RECONNECTED / DISCONNECTED events are
    written to the connection log for this connection, tagged with this
    connection's own mode ("Quote" or "Depth") — both feeds pass this now,
    so BackfillManager can derive gap windows for either independently.

    on_reconnect: optional async callback, called (fire-and-forget, not
    awaited here — so a slow heal never blocks the live feed) as
    on_reconnect(mode_label, disconnect_dt, reconnect_dt) whenever this
    connection comes back up after a REAL mid-session drop (never on the
    day's first connect — there's nothing to heal then, since nothing
    was ever missed).

    background_tasks: shared set the fire-and-forget heal task gets
    added to (see below for why). Optional only so this module still
    works standalone/in tests without engine_runtime.py wiring one up;
    production always passes one.
    """
    if not ws_url:
        ws_url = DEFAULT_WS_URL

    if not api_key:
        raise RuntimeError("API_KEY missing")

    mode_label = str(mode).strip().title()
    print(f"[WS] Connecting to {ws_url} | mode={mode_label}", flush=True)

    last_disconnect_at = None  # set when DISCONNECTED fires below; used to
                                # compute the exact healed window on reconnect

    while True:
        try:
            async with websockets.connect(ws_url) as ws:
                await ws.send(json.dumps({"action": "authenticate", "api_key": api_key}))
                print(f"[WS] Authentication sent | mode={mode_label}", flush=True)

                if conn_log_dir:
                    now = now_kolkata()
                    is_reconnect = connection_log.has_event_today(
                        conn_log_dir, "DAY_STARTED", now, mode=mode_label
                    )
                    event = "RECONNECTED" if is_reconnect else "DAY_STARTED"
                    connection_log.log_event(conn_log_dir, event, now, mode=mode_label)

                    if is_reconnect and on_reconnect is not None and last_disconnect_at is not None:
                        # Fire-and-forget — the heal runs in the background
                        # via asyncio.to_thread (see engine_runtime.py), the
                        # live feed keeps flowing without waiting on it.
                        #
                        # Kept in `background_tasks` (and only removed once
                        # actually done) specifically so engine_runtime.py's
                        # shutdown sequence can find and wait for it. Before
                        # this, the task from asyncio.create_task() here had
                        # NO reference stored anywhere at all — shutdown had
                        # zero way to even know it existed, let alone wait
                        # for it, so a heal (and whatever candle-building/
                        # backfill work it kicks off) could keep running
                        # completely undetected well after "Shutdown
                        # complete" was already printed. This is the
                        # standard asyncio pattern for fire-and-forget tasks
                        # (see the "Important" note in the asyncio.create_task
                        # docs) — a bare asyncio.create_task() with nothing
                        # holding a reference is only ever safe from garbage
                        # collection by luck, and gives the caller no way to
                        # ever find it again.
                        heal_task = asyncio.create_task(on_reconnect(mode_label, last_disconnect_at, now))
                        if background_tasks is not None:
                            background_tasks.add(heal_task)
                            heal_task.add_done_callback(background_tasks.discard)
                    last_disconnect_at = None

                for inst in instruments:
                    payload = {
                        "action": "subscribe",
                        "exchange": inst["exchange"],
                        "symbol": inst["symbol"],
                        "mode": mode_label,
                    }
                    if mode_label == "Depth":
                        payload["depth"] = depth_levels
                    await ws.send(json.dumps(payload))

                # Log grouped subscription summary per exchange
                grouped = {}
                for inst in instruments:
                    ex = str(inst.get("exchange", "")).upper()
                    sym = str(inst.get("symbol", "")).upper()
                    if not ex or not sym:
                        continue
                    grouped.setdefault(ex, []).append(sym)

                for ex, symbols in grouped.items():
                    suffix = (
                        " (same universe as Quote)" if mode_label == "Depth" else ""
                    )
                    print(
                        f"[WS] Subscribed {mode_label} {ex}:{len(symbols)} symbols{suffix}",
                        flush=True,
                    )

                loop = asyncio.get_running_loop()
                last_rx_at = loop.time()

                # This connection is single-mode, so every tick it receives
                # goes to the same queue for the whole connection's lifetime.
                target_queue = depth_data_queue if mode_label == "Depth" else market_data_queue

                async def heartbeat():
                    while True:
                        await asyncio.sleep(60)
                        idle_sec = int(loop.time() - last_rx_at)
                        print(
                            f"[WS][HEARTBEAT] Connected | mode={mode_label} | idle={idle_sec}s | queue={target_queue.qsize()}",
                            flush=True,
                        )

                hb_task = asyncio.create_task(heartbeat())
                try:
                    async for message in ws:
                        last_rx_at = loop.time()
                        try:
                            data = json.loads(message)
                        except json.JSONDecodeError:
                            continue

                        if data.get("type") == "market_data":
                            data["_subscription_mode"] = mode_label
                            await target_queue.put(data)
                        elif data.get("status") == "error":
                            # A mid-session error — e.g. the broker session
                            # was invalidated after we'd already
                            # authenticated — is a real fault. Raising
                            # instead of just printing routes us into the
                            # except-Exception block below, which logs
                            # DISCONNECTED and retries in 2s, instead of
                            # leaving a socket open that looks "connected"
                            # while receiving nothing useful.
                            raise RuntimeError(
                                f"server error message | mode={mode_label} data={data}"
                            )
                finally:
                    hb_task.cancel()
                    await asyncio.gather(hb_task, return_exceptions=True)
        except asyncio.CancelledError:
            if conn_log_dir:
                connection_log.log_event(
                    conn_log_dir, "DISCONNECTED", now_kolkata(),
                    mode=mode_label, note="task cancelled (shutdown/session end)",
                )
            raise
        except Exception as exc:
            if conn_log_dir:
                last_disconnect_at = now_kolkata()
                connection_log.log_event(
                    conn_log_dir, "DISCONNECTED", last_disconnect_at,
                    mode=mode_label, note=str(exc),
                )
            print(f"[WS][ERROR] mode={mode_label} {exc}. Reconnecting in 2s...", flush=True)
            await asyncio.sleep(2)


async def run_market_data_feeds(
    api_key: str,
    instruments: List[dict],
    ws_url: str | None = None,
    conn_log_dir: Optional[str] = None,
    depth_levels: int = 5,
    on_reconnect: Optional[Callable[[str, "datetime", "datetime"], Awaitable]] = None,
    background_tasks: Optional[set] = None,
):
    """
    Single entry point that owns BOTH the Quote and Depth connections.

    - Quote connection: feeds market_data_queue, logs to conn_log_dir
      with mode="Quote".
    - Depth connection: feeds depth_data_queue, logs to conn_log_dir
      with mode="Depth" — same connection_log.db, distinguished by the
      `mode` column, so BackfillManager can derive gap windows for
      either feed independently (they can drop/reconnect at different
      times, unrelated to each other).

    on_reconnect: passed through to both connections unchanged — see
    websocket_client's docstring. Each connection calls it with its
    own mode_label, so a Quote reconnect only ever triggers a Quote
    heal and a Depth reconnect only ever triggers a Depth heal.

    background_tasks: shared set passed through to both connections
    unchanged, so a heal fired from either one lands in the SAME set
    engine_runtime.py watches at shutdown — see websocket_client's
    docstring for why this exists at all.

    Both connections retry independently forever (each has its own
    try/except + reconnect loop), so a Depth-side drop never affects
    the Quote side and vice versa. This coroutine itself only returns
    if both connections somehow return, which — by design — they don't.
    """
    await asyncio.gather(
        websocket_client(
            ws_url, api_key, instruments,
            mode="Quote",
            conn_log_dir=conn_log_dir,
            on_reconnect=on_reconnect,
            background_tasks=background_tasks,
        ),
        websocket_client(
            ws_url, api_key, instruments,
            mode="Depth",
            depth_levels=depth_levels,
            conn_log_dir=conn_log_dir,
            on_reconnect=on_reconnect,
            background_tasks=background_tasks,
        ),
    )
