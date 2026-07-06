import asyncio

# Central async queue for all Quote-mode ticks (consumed by OHLCCollector)
market_data_queue = asyncio.Queue(maxsize=5000)

# Dedicated queue for Depth-mode ticks (consumed by DepthStore) — kept
# separate from market_data_queue so OHLCCollector never has to know
# depth data exists.
depth_data_queue = asyncio.Queue(maxsize=5000)

# Breakout / strategy signals: SignalGenerator -> TradeExecutor
# Each item: {"symbol": str, "exchange": str, "side": "BUY"|"SELL", "price": float}
signal_queue = asyncio.Queue(maxsize=1000)