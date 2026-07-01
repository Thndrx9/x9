import asyncio
import os
from dotenv import load_dotenv
from openalgo import api
from utils import load_symbols

load_dotenv()


class TradeExecutor:
    """
    RAM-DRIVEN EXECUTOR (FINAL, FULL VISIBILITY)
    -------------------------------------------
    • Live monitor: LTP / PH / PL (one line per symbol)
    • Breakout trading (prev candle)
    • ONE active position globally
    • Stoploss = 0.2%
    • Force square-off on shutdown using MARKET orders
    """

    STOPLOSS_PCT = 0.002  # 0.2%

    def __init__(self, ohlc, symbols_csv: str = "symbols.csv"):
        self.ohlc = ohlc
        self.symbols = load_symbols(symbols_csv)
        self.active_position = None

        self.client = api(
            api_key=os.getenv("OPENALGO_API_KEY"),
            host=os.getenv("OPENALGO_HOST", "http://127.0.0.1:5000")
        )

        print("[EXEC] Executor initialized (FULL MONITOR MODE)", flush=True)

    # =====================================================
    # MAIN LOOP
    # =====================================================

    async def run(self):
        print("[EXEC] Executor running (live monitor)", flush=True)

        while True:
            for instr in self.symbols:
                symbol = instr["symbol"]
                exchange = instr["exchange"]

                ltp = self.ohlc.latest_ltp.get(symbol)
                candles = self.ohlc.ohlc_1m.get(symbol)

                if ltp is None or not candles:
                    continue

                # ------------------------------
                # Trading logic
                # ------------------------------
                last_candle = candles[-1]
                ph = last_candle["high"]
                pl = last_candle["low"]

                if self.active_position:
                    self._check_stoploss()
                    continue

                if ltp > ph:
                    self._enter_trade(exchange, symbol, "BUY", ltp)

                elif ltp < pl:
                    self._enter_trade(exchange, symbol, "SELL", ltp)

            await asyncio.sleep(0.1)

    # =====================================================
    # =====================================================
    # ENTRY
    # =====================================================

    def _enter_trade(self, exchange, symbol, side, price):
        if self.active_position:
            return

        print(f"\n[EXEC] 🚀 ENTRY | {side} {symbol} @ {price}", flush=True)

        resp = self.client.placeorder(
            strategy="PythonBreakoutTest",
            symbol=symbol,
            exchange=exchange,
            action=side,
            price_type="MARKET",
            product="MIS",
            quantity=1
        )

        if resp.get("status") != "success":
            print("[EXEC] ❌ Order rejected", flush=True)
            return

        sl = (
            price * (1 - self.STOPLOSS_PCT)
            if side == "BUY"
            else price * (1 + self.STOPLOSS_PCT)
        )

        self.active_position = {
            "symbol": symbol,
            "exchange": exchange,
            "side": side,
            "sl": sl
        }

        print(f"[EXEC] ✅ POSITION OPEN | SL:{sl:.2f}", flush=True)

    # =====================================================
    # STOPLOSS
    # =====================================================

    def _check_stoploss(self):
        pos = self.active_position
        ltp = self.ohlc.latest_ltp.get(pos["symbol"])
        if ltp is None:
            return

        hit = (
            ltp <= pos["sl"]
            if pos["side"] == "BUY"
            else ltp >= pos["sl"]
        )

        if not hit:
            return

        exit_side = "SELL" if pos["side"] == "BUY" else "BUY"

        print(f"\n[EXEC] 🛑 STOPLOSS HIT | {pos['symbol']} @ {ltp}", flush=True)

        self.client.placeorder(
            strategy="PythonBreakoutTest",
            symbol=pos["symbol"],
            exchange=pos["exchange"],
            action=exit_side,
            price_type="MARKET",
            product="MIS",
            quantity=1
        )

        self.active_position = None

    # =====================================================
    # 🔥 ASYNC SHUTDOWN – FORCE SQUARE-OFF
    # =====================================================

    async def shutdown_async(self):
        print("[EXEC] SHUTDOWN: Force closing all positions", flush=True)

        try:
            raw = self.client.positionbook()
        except Exception as e:
            print(f"[EXEC] positionbook() failed: {e}", flush=True)
            return

        positions = raw.get("data", [])
        if not positions:
            print("[EXEC] No open positions found", flush=True)
            return

        for pos in positions:
            qty = pos.get("quantity", 0)
            if qty == 0:
                continue

            symbol = pos["symbol"]
            exchange = pos["exchange"]
            product = pos.get("product", "MIS")
            exit_side = "SELL" if qty > 0 else "BUY"

            print(
                f"[EXEC] FORCE CLOSE | {symbol} {exit_side} {abs(qty)}",
                flush=True
            )

            try:
                self.client.placeorder(
                    strategy="PythonBreakoutTest",
                    symbol=symbol,
                    exchange=exchange,
                    action=exit_side,
                    price_type="MARKET",
                    product=product,
                    quantity=abs(qty)
                )
            except Exception as e:
                print(f"[EXEC] Failed to close {symbol}: {e}", flush=True)

        self.active_position = None