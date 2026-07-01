import asyncio

# Central async queue for all ticks
market_data_queue = asyncio.Queue(maxsize=5000)