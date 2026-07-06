# depth_store.py

import os
from collections import deque
from event_bus import depth_data_queue


class DepthStore:
    """
    DepthStore
    ──────────
    The Depth-side counterpart to OHLCCollector — same shape (owns its
    own run() loop consuming its own queue), but for order-book depth
    instead of candles.

    • Consumes depth_data_queue directly (websocket_connect.py routes
      Depth-mode ticks here, never through OHLCCollector/market_data_queue)
    • Keeps a rolling RAM_WINDOW_MINUTES (default 30) window of history
      per symbol — not just the single latest snapshot — pruned on
      every update. Disk (via DepthWriter) always keeps the full history;
      this only bounds what stays resident in memory.
    • Forwards every update to DepthWriter (SQLite-backed) for persistence

    NOTE: the exact field names OpenAlgo uses for bid/ask levels in a
    Depth packet weren't available when this was written, so
    `_process_tick()` tries a few common shapes and falls back to
    logging the raw keys (once) if none match. If you see a
    "[DEPTH][WARN] Unrecognized depth payload shape" line, paste the
    keys back and the field mapping can be corrected exactly.
    """

    def __init__(self, depth_writer=None):
        # symbol -> deque of snapshot dicts, oldest first
        self.depth_history  = {}
        self.depth_writer    = depth_writer
        self._warned_shape   = False
        self.ram_window_secs = int(os.getenv("RAM_WINDOW_MINUTES", "30")) * 60

    async def run(self):
        print("[DEPTH] Collector running", flush=True)
        while True:
            tick = await depth_data_queue.get()
            self._process_tick(tick)

    @staticmethod
    def _normalize_symbol(raw_symbol):
        if raw_symbol is None:
            return None
        s = str(raw_symbol).strip().upper()
        if ":" in s:
            return s.split(":")[-1]
        return s

    def _process_tick(self, tick):
        data       = tick.get("data", {})
        raw_symbol = data.get("symbol", tick.get("symbol"))
        symbol     = self._normalize_symbol(raw_symbol)
        if not symbol:
            return

        bids = data.get("bids") or data.get("buy") or (data.get("depth") or {}).get("buy") or []
        asks = data.get("asks") or data.get("sell") or (data.get("depth") or {}).get("sell") or []

        if not bids and not asks:
            if not self._warned_shape:
                print(
                    f"[DEPTH][WARN] Unrecognized depth payload shape, "
                    f"keys={list(data.keys())}",
                    flush=True,
                )
                self._warned_shape = True
            return

        ts_ms = data.get("ltt")
        if ts_ms is None:
            return  # can't window it without a timestamp — discard

        snapshot = {
            "bids":      bids,
            "asks":      asks,
            "ltp":       data.get("ltp"),
            "timestamp": ts_ms,
        }

        # ── RAM: rolling window, pruned by this tick's own timestamp ────
        history = self.depth_history.setdefault(symbol, deque())
        history.append(snapshot)
        cutoff_ms = ts_ms - self.ram_window_secs * 1000
        while history and history[0]["timestamp"] < cutoff_ms:
            history.popleft()

        # ── Disk: full history, no windowing ────────────────────────────
        if self.depth_writer is not None:
            self.depth_writer.enqueue(symbol, snapshot)

    # ─────────────────────────────────────────────
    # Read access for live consumers (e.g. slippage-aware execution)
    # ─────────────────────────────────────────────

    def get(self, symbol: str):
        """Most recent snapshot for symbol, or None."""
        history = self.depth_history.get(self._normalize_symbol(symbol))
        return history[-1] if history else None

    def get_history(self, symbol: str):
        """Full in-RAM rolling-window history (oldest first) for symbol."""
        return list(self.depth_history.get(self._normalize_symbol(symbol), []))

    def best_bid_ask(self, symbol: str):
        """Convenience accessor: (best_bid_level, best_ask_level) or (None, None)."""
        snap = self.get(symbol)
        if not snap:
            return None, None
        bid = snap["bids"][0] if snap["bids"] else None
        ask = snap["asks"][0] if snap["asks"] else None
        return bid, ask