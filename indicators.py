# indicators.py

import pandas as pd
from collections import defaultdict


class IndicatorEngine:
    """
    IndicatorEngine (RAM-only)
    ──────────────────────────
    • Computes indicators for ALL configured timeframes
    • TF list is read dynamically from ohlc.ohlc_data keys
    • Uses ONLY closed candles
    """

    def __init__(self, ohlc):
        self.ohlc       = ohlc
        self.indicators = defaultdict(dict)

    def update(self, symbol):
        # Iterate every TF that OHLCCollector was configured with
        for tf in self.ohlc.ohlc_data.keys():
            self._adx(symbol, tf, di_length=14, adx_smoothing=14)

    def get(self, symbol, timeframe, name, di_length, adx_smoothing):
        return self.indicators[symbol].get(
            (timeframe, name, di_length, adx_smoothing),
            {"ready": False},
        )

    def get_series(self, symbol, timeframe, name, di_length, adx_smoothing, lookback=3):
        data = self.get(symbol, timeframe, name, di_length, adx_smoothing)
        if not data or not data.get("ready"):
            return [None] * lookback

        series = data.get("series", [])
        if not series:
            return [None] * lookback

        values = [point["value"] for point in series if point.get("value") is not None]
        if not values:
            return [None] * lookback

        values = values[-lookback:]
        if len(values) < lookback:
            values = ([None] * (lookback - len(values))) + values
        return values

    def _adx(self, symbol, tf, di_length, adx_smoothing):
        # Fetch closed candles for this TF from the unified store
        candles = self.ohlc.ohlc_data.get(tf, {}).get(symbol)

        key = (tf, "ADX", di_length, adx_smoothing)

        if not candles or len(candles) < max(di_length, adx_smoothing) + 1:
            self.indicators[symbol][key] = {"ready": False}
            return

        df = pd.DataFrame(candles)

        last_ts = df.iloc[-1]["timestamp"]
        prev    = self.indicators[symbol].get(key)
        if prev and prev.get("timestamp") == last_ts:
            return  # nothing new — skip recalculation

        high, low, close = df["high"], df["low"], df["close"]

        plus_dm  = high.diff()
        minus_dm = low.diff().abs()
        plus_dm  = plus_dm.where((plus_dm > minus_dm) & (plus_dm > 0),  0.0)
        minus_dm = minus_dm.where((minus_dm > plus_dm) & (minus_dm > 0), 0.0)

        tr = pd.concat(
            [high - low, (high - close.shift()).abs(), (low - close.shift()).abs()],
            axis=1,
        ).max(axis=1)

        atr       = tr.rolling(di_length).mean()
        plus_di   = 100 * (plus_dm.rolling(di_length).mean() / atr)
        minus_di  = 100 * (minus_dm.rolling(di_length).mean() / atr)
        dx        = (abs(plus_di - minus_di) / (plus_di + minus_di)) * 100
        adx_series = dx.rolling(adx_smoothing).mean()
        adx       = adx_series.iloc[-1]

        series_payload = [
            {
                "timestamp": ts,
                "value":     float(value) if pd.notna(value) else None,
            }
            for ts, value in zip(df["timestamp"], adx_series)
        ]

        self.indicators[symbol][key] = {
            "value":     float(adx),
            "timestamp": last_ts,
            "ready":     True,
            "series":    series_payload,
        }