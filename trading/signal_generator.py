import asyncio
from event_bus import signal_queue
from utils import load_symbols


class SignalGenerator:
    """
    SIGNAL GENERATOR (READ-ONLY, NO ORDERS)
    ----------------------------------------
    • Watches LTP vs. previous closed 1m candle (PH / PL)
    • Emits breakout signals onto signal_queue
    • Has NO knowledge of positions, brokers, or orders
    • Emits at most one signal per symbol per reference candle,
      so it doesn't spam the queue while price stays past the
      breakout level (dedup by candle timestamp)
    """

    def __init__(self, ohlc, symbols_csv: str = "symbols.csv"):
        self.ohlc = ohlc
        self.symbols = load_symbols(symbols_csv)

        # symbol -> timestamp of the last candle we already signaled for
        self._last_signaled_candle = {}

        print("[SIGNAL] Signal generator initialized", flush=True)

    async def run(self):
        print("[SIGNAL] Signal generator running", flush=True)

        while True:
            for instr in self.symbols:
                symbol = instr["symbol"]
                exchange = instr["exchange"]

                ltp = self.ohlc.latest_ltp.get(symbol)
                candles = self.ohlc.ohlc_1m.get(symbol)

                if ltp is None or not candles:
                    continue

                last_candle = candles[-1]
                candle_ts = last_candle.get("timestamp")
                ph = last_candle["high"]
                pl = last_candle["low"]

                side = None
                if ltp > ph:
                    side = "BUY"
                elif ltp < pl:
                    side = "SELL"

                if side is None:
                    continue

                # Already signaled for this exact reference candle? skip.
                if self._last_signaled_candle.get(symbol) == candle_ts:
                    continue

                self._last_signaled_candle[symbol] = candle_ts

                signal = {
                    "symbol": symbol,
                    "exchange": exchange,
                    "side": side,
                    "price": ltp,
                }

                print(
                    f"[SIGNAL] 📈 {side} {symbol} @ {ltp} "
                    f"(PH:{ph} PL:{pl})",
                    flush=True,
                )

                await signal_queue.put(signal)

            await asyncio.sleep(0.1)