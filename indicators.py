# indicators.py

import os
import numpy as np
import pandas as pd
from collections import defaultdict

# ============================================================
# Config — every indicator's parameter set(s), fully manual /
# .env driven. Add or remove variants freely; nothing is
# hardcoded to a fixed "fast/default/slow" tier.
#
#   RSI_LENGTHS      comma-separated ints            e.g. "14,9,21"
#   SMA_RSI_PARAMS   semicolon-separated "rsi_len,sma_len" pairs
#                                                      e.g. "14,14;9,5"
#   ATR_LENGTHS      comma-separated ints            e.g. "14,7"
#   MACD_PARAMS      semicolon-separated "fast,slow,signal" triples
#                                                      e.g. "12,26,9;5,13,5"
#   STOCH_PARAMS     semicolon-separated "k_len,k_smooth,d_smooth" triples
#                                                      e.g. "14,1,3;5,3,3"
#   ADX_PARAMS       semicolon-separated "di_len,adx_smoothing" pairs
#                                                      e.g. "14,14"
#
# All formulas match TradingView's built-ins (ta.rsi, ta.atr, ta.macd,
# ta.stoch, ta.dmi/ta.adx) — see _rma() below for why that means Wilder's
# smoothing, not a plain rolling mean, for RSI/ATR/ADX.
# ============================================================

TICK_TF_KEY = "TICKS"  # pseudo-timeframe key for the raw tick buffer


def _parse_int_list(env_name, default):
    raw = os.getenv(env_name, default)
    out = []
    for part in raw.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            out.append(int(part))
        except ValueError:
            print(f"[INDICATORS][WARN] skipping malformed {env_name} value '{part}'", flush=True)
    return out


def _parse_tuple_list(env_name, default, arity):
    raw = os.getenv(env_name, default)
    out = []
    for group in raw.split(";"):
        group = group.strip()
        if not group:
            continue
        parts = [p.strip() for p in group.split(",") if p.strip()]
        if len(parts) != arity:
            print(
                f"[INDICATORS][WARN] skipping malformed {env_name} group '{group}' "
                f"— expected {arity} value(s)",
                flush=True,
            )
            continue
        try:
            out.append(tuple(int(p) for p in parts))
        except ValueError:
            print(f"[INDICATORS][WARN] skipping malformed {env_name} group '{group}'", flush=True)
    return out


def _rma(series: pd.Series, length: int) -> pd.Series:
    """
    Wilder's Moving Average (RMA), matching Pine Script's ta.rma() exactly:
        rma[length-1] = sma(src[0..length-1])          (seed)
        rma[i]        = (rma[i-1] * (length - 1) + src[i]) / length

    This is deliberately NOT pandas' .ewm(alpha=1/length, adjust=False) —
    that seeds from the raw first value instead of an SMA, which converges
    to the same place eventually but isn't bit-exact with TradingView,
    especially on a freshly warmed-up RAM candle/tick store like this one
    where "eventually" may not have happened yet. RSI, ATR, and DMI/ADX
    all use this same smoothing in TradingView.
    """
    values = series.to_numpy(dtype="float64")
    n = len(values)
    out = np.full(n, np.nan)

    if n < length:
        return pd.Series(out, index=series.index)

    seed = np.nanmean(values[:length])
    out[length - 1] = seed

    for i in range(length, n):
        prev = out[i - 1]
        if np.isnan(prev):
            continue
        out[i] = (prev * (length - 1) + values[i]) / length

    return pd.Series(out, index=series.index)


class IndicatorEngine:
    """
    IndicatorEngine (RAM-only)
    ──────────────────────────
    • Computes indicators for every configured candle timeframe PLUS a
      synthetic "TICKS" pseudo-timeframe built from OHLCCollector's raw
      tick RAM buffer (each tick treated as a zero-range OHLC bar —
      high=low=close=ltp — the standard approach when no per-tick range
      exists; ATR/Stochastic still work correctly off tick-to-tick moves).
    • Every indicator's parameter set(s) are config-driven (see module
      docstring above) — pick however many variants you want, nothing
      hardcoded.
    • Uses ONLY closed candles for candle timeframes (never the
      in-progress one); for TICKS, uses whatever's currently buffered.
    • Formulas match TradingView's built-ins exactly (see _rma() above).
    """

    def __init__(self, ohlc):
        self.ohlc       = ohlc
        self.indicators = defaultdict(dict)

        self.rsi_lengths  = _parse_int_list("RSI_LENGTHS", "14")
        self.sma_rsi_params = _parse_tuple_list("SMA_RSI_PARAMS", "14,14", arity=2)
        self.atr_lengths  = _parse_int_list("ATR_LENGTHS", "14")
        self.macd_params  = _parse_tuple_list("MACD_PARAMS", "12,26,9", arity=3)
        self.stoch_params = _parse_tuple_list("STOCH_PARAMS", "14,1,3", arity=3)
        self.adx_params   = _parse_tuple_list("ADX_PARAMS", "14,14", arity=2)

    # ─────────────────────────────────────────────
    # Public API
    # ─────────────────────────────────────────────

    def update(self, symbol):
        tf_list = list(self.ohlc.ohlc_data.keys()) + [TICK_TF_KEY]
        for tf in tf_list:
            df = self._get_candles_df(symbol, tf)
            if df is None or df.empty:
                continue
            self._adx_family(symbol, tf, df)
            self._rsi_family(symbol, tf, df)
            self._sma_rsi_family(symbol, tf, df)
            self._atr_family(symbol, tf, df)
            self._macd_family(symbol, tf, df)
            self._stoch_family(symbol, tf, df)

    def get(self, symbol, timeframe, name, *params):
        return self.indicators[symbol].get(
            (timeframe, name, *params),
            {"ready": False},
        )

    def get_series(self, symbol, timeframe, name, *params, lookback=3):
        """
        Flat-value lookback series — works for single-value indicators
        (ADX, RSI, SMA_RSI, ATR). For MACD/STOCH (multi-field series),
        read .get(...)["series"] directly instead.
        """
        data = self.get(symbol, timeframe, name, *params)
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

    # ─────────────────────────────────────────────
    # Candle source (real candles OR the tick pseudo-timeframe)
    # ─────────────────────────────────────────────

    def _get_candles_df(self, symbol, tf):
        if tf == TICK_TF_KEY:
            ticks = self.ohlc.get_recent_ticks(symbol)
            if not ticks:
                return None
            return pd.DataFrame([
                {
                    "timestamp": t["timestamp"],
                    "open":  t["ltp"],
                    "high":  t["ltp"],
                    "low":   t["ltp"],
                    "close": t["ltp"],
                    "volume": t.get("qty", 0),
                }
                for t in ticks
            ])

        candles = self.ohlc.ohlc_data.get(tf, {}).get(symbol)
        if not candles:
            return None
        return pd.DataFrame(candles)

    # ─────────────────────────────────────────────
    # Storage helper (single-value indicators)
    # ─────────────────────────────────────────────

    def _store(self, symbol, key, timestamps, value_series, last_ts):
        series_payload = [
            {"timestamp": ts, "value": float(v) if pd.notna(v) else None}
            for ts, v in zip(timestamps, value_series)
        ]
        last_value = value_series.iloc[-1]
        self.indicators[symbol][key] = {
            "value":     float(last_value) if pd.notna(last_value) else None,
            "timestamp": last_ts,
            "ready":     True,
            "series":    series_payload,
        }

    # ─────────────────────────────────────────────
    # ADX / DMI  — TradingView-exact (ta.dmi / ta.adx): Wilder's RMA
    # smoothing throughout, NOT a plain rolling mean.
    # ─────────────────────────────────────────────

    def _adx_family(self, symbol, tf, df):
        high, low, close = df["high"], df["low"], df["close"]
        last_ts = df.iloc[-1]["timestamp"]

        up   = high.diff()
        down = -low.diff()

        plus_dm  = pd.Series(np.where((up > down) & (up > 0), up, 0.0), index=df.index)
        minus_dm = pd.Series(np.where((down > up) & (down > 0), down, 0.0), index=df.index)

        tr = pd.concat(
            [high - low, (high - close.shift()).abs(), (low - close.shift()).abs()],
            axis=1,
        ).max(axis=1)

        for di_length, adx_smoothing in self.adx_params:
            key = (tf, "ADX", di_length, adx_smoothing)
            if len(df) < di_length + adx_smoothing:
                self.indicators[symbol][key] = {"ready": False}
                continue

            prev = self.indicators[symbol].get(key)
            if prev and prev.get("timestamp") == last_ts:
                continue

            atr_for_di = _rma(tr, di_length)
            plus_di  = 100 * (_rma(plus_dm, di_length)  / atr_for_di)
            minus_di = 100 * (_rma(minus_dm, di_length) / atr_for_di)

            di_sum = plus_di + minus_di
            dx = (100 * (plus_di - minus_di).abs() / di_sum.replace(0, np.nan)).fillna(0.0)
            adx_series = _rma(dx, adx_smoothing)

            self._store(symbol, key, df["timestamp"], adx_series, last_ts)
            # attach +DI/-DI too, since they're computed anyway
            self.indicators[symbol][key]["plus_di"]  = float(plus_di.iloc[-1])  if pd.notna(plus_di.iloc[-1])  else None
            self.indicators[symbol][key]["minus_di"] = float(minus_di.iloc[-1]) if pd.notna(minus_di.iloc[-1]) else None

    # ─────────────────────────────────────────────
    # RSI — TradingView-exact (ta.rsi): Wilder's RMA on gains/losses.
    # ─────────────────────────────────────────────

    @staticmethod
    def _rsi_series(close: pd.Series, length: int) -> pd.Series:
        delta = close.diff()
        gain  = delta.clip(lower=0)
        loss  = -delta.clip(upper=0)

        avg_gain = _rma(gain, length)
        avg_loss = _rma(loss, length)

        with np.errstate(divide="ignore", invalid="ignore"):
            rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))

        # avg_loss == 0 -> no losses in window -> RSI = 100, UNLESS
        # avg_gain is also 0 (totally flat price) -> RSI = 50 (convention
        # for the undefined 0/0 case; matches common practical behavior).
        rsi = np.where(avg_loss.to_numpy() == 0,
                        np.where(avg_gain.to_numpy() == 0, 50.0, 100.0),
                        rsi.to_numpy())
        return pd.Series(rsi, index=close.index)

    def _rsi_family(self, symbol, tf, df):
        close = df["close"]
        last_ts = df.iloc[-1]["timestamp"]

        for length in self.rsi_lengths:
            key = (tf, "RSI", length)
            if len(df) < length + 1:
                self.indicators[symbol][key] = {"ready": False}
                continue

            prev = self.indicators[symbol].get(key)
            if prev and prev.get("timestamp") == last_ts:
                continue

            rsi_series = self._rsi_series(close, length)
            self._store(symbol, key, df["timestamp"], rsi_series, last_ts)

    # ─────────────────────────────────────────────
    # SMA of RSI — simple moving average applied on top of the RSI series
    # above (separate rsi_length + sma_length, independently configurable).
    # ─────────────────────────────────────────────

    def _sma_rsi_family(self, symbol, tf, df):
        close = df["close"]
        last_ts = df.iloc[-1]["timestamp"]

        for rsi_length, sma_length in self.sma_rsi_params:
            key = (tf, "SMA_RSI", rsi_length, sma_length)
            if len(df) < rsi_length + sma_length:
                self.indicators[symbol][key] = {"ready": False}
                continue

            prev = self.indicators[symbol].get(key)
            if prev and prev.get("timestamp") == last_ts:
                continue

            rsi_series = self._rsi_series(close, rsi_length)
            sma_series = rsi_series.rolling(sma_length).mean()
            self._store(symbol, key, df["timestamp"], sma_series, last_ts)

    # ─────────────────────────────────────────────
    # ATR — TradingView-exact (ta.atr): Wilder's RMA of True Range.
    # ─────────────────────────────────────────────

    def _atr_family(self, symbol, tf, df):
        high, low, close = df["high"], df["low"], df["close"]
        last_ts = df.iloc[-1]["timestamp"]

        tr = pd.concat(
            [high - low, (high - close.shift()).abs(), (low - close.shift()).abs()],
            axis=1,
        ).max(axis=1)

        for length in self.atr_lengths:
            key = (tf, "ATR", length)
            if len(df) < length + 1:
                self.indicators[symbol][key] = {"ready": False}
                continue

            prev = self.indicators[symbol].get(key)
            if prev and prev.get("timestamp") == last_ts:
                continue

            atr_series = _rma(tr, length)
            self._store(symbol, key, df["timestamp"], atr_series, last_ts)

    # ─────────────────────────────────────────────
    # MACD — TradingView-exact (ta.macd): plain EMA (pandas .ewm with
    # adjust=False seeds from the first value, same as Pine's ta.ema —
    # no Wilder smoothing here, that's correct, MACD doesn't use RMA).
    # ─────────────────────────────────────────────

    def _macd_family(self, symbol, tf, df):
        close = df["close"]
        last_ts = df.iloc[-1]["timestamp"]

        for fast, slow, signal in self.macd_params:
            key = (tf, "MACD", fast, slow, signal)
            if len(df) < slow + signal:
                self.indicators[symbol][key] = {"ready": False}
                continue

            prev = self.indicators[symbol].get(key)
            if prev and prev.get("timestamp") == last_ts:
                continue

            ema_fast = close.ewm(span=fast, adjust=False).mean()
            ema_slow = close.ewm(span=slow, adjust=False).mean()
            macd_line = ema_fast - ema_slow
            signal_line = macd_line.ewm(span=signal, adjust=False).mean()
            histogram = macd_line - signal_line

            series_payload = [
                {
                    "timestamp": ts,
                    "macd":      float(m) if pd.notna(m) else None,
                    "signal":    float(s) if pd.notna(s) else None,
                    "histogram": float(h) if pd.notna(h) else None,
                }
                for ts, m, s, h in zip(df["timestamp"], macd_line, signal_line, histogram)
            ]

            last_macd, last_signal, last_hist = macd_line.iloc[-1], signal_line.iloc[-1], histogram.iloc[-1]
            self.indicators[symbol][key] = {
                "value":     float(last_macd)   if pd.notna(last_macd)   else None,  # macd line
                "signal":    float(last_signal) if pd.notna(last_signal) else None,
                "histogram": float(last_hist)   if pd.notna(last_hist)   else None,
                "timestamp": last_ts,
                "ready":     True,
                "series":    series_payload,
            }

    # ─────────────────────────────────────────────
    # Stochastic %K / %D — TradingView-exact (ta.stoch): raw %K over
    # highest-high/lowest-low(k_length), SMA-smoothed to %K, SMA'd again
    # to %D. Defaults (14, 1, 3) match TradingView's default Stochastic.
    # ─────────────────────────────────────────────

    def _stoch_family(self, symbol, tf, df):
        high, low, close = df["high"], df["low"], df["close"]
        last_ts = df.iloc[-1]["timestamp"]

        for k_length, k_smooth, d_smooth in self.stoch_params:
            key = (tf, "STOCH", k_length, k_smooth, d_smooth)
            if len(df) < k_length + max(k_smooth, 1) + d_smooth:
                self.indicators[symbol][key] = {"ready": False}
                continue

            prev = self.indicators[symbol].get(key)
            if prev and prev.get("timestamp") == last_ts:
                continue

            lowest_low   = low.rolling(k_length).min()
            highest_high = high.rolling(k_length).max()
            rng = (highest_high - lowest_low).replace(0, np.nan)

            raw_k = (100 * (close - lowest_low) / rng).fillna(50.0)  # flat range -> mid value
            k_line = raw_k.rolling(k_smooth).mean() if k_smooth > 1 else raw_k
            d_line = k_line.rolling(d_smooth).mean()

            series_payload = [
                {
                    "timestamp": ts,
                    "k": float(k) if pd.notna(k) else None,
                    "d": float(d) if pd.notna(d) else None,
                }
                for ts, k, d in zip(df["timestamp"], k_line, d_line)
            ]

            last_k, last_d = k_line.iloc[-1], d_line.iloc[-1]
            self.indicators[symbol][key] = {
                "k":         float(last_k) if pd.notna(last_k) else None,
                "d":         float(last_d) if pd.notna(last_d) else None,
                "timestamp": last_ts,
                "ready":     True,
                "series":    series_payload,
            }