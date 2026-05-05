"""
findata.trend_indicators
------------------------
Trend / momentum / volatility indicator pack via pandas-ta.

Computes the standard set a discretionary trader looks at — moving
averages, RSI, MACD, ADX, Bollinger Bands — and returns the latest
value for each plus a few derived booleans (golden cross? bullish MACD
cross? RSI overbought?). Designed for "what's the current technical
posture" reads, not for backtesting.
"""

from __future__ import annotations

from typing import Optional

import pandas as pd


def _classify_rsi(rsi: float) -> str:
    if rsi >= 70: return "overbought"
    if rsi >= 65: return "near_overbought"
    if rsi <= 30: return "oversold"
    if rsi <= 35: return "near_oversold"
    return "neutral"


def _classify_adx(adx: float) -> str:
    if adx >= 40: return "very_strong_trend"
    if adx >= 25: return "strong_trend"
    if adx >= 20: return "weak_trend"
    return "no_trend"


def compute_trend_indicators(
    ticker: str,
    window_days: int = 252,
) -> dict:
    """
    Snapshot of trend / momentum / volatility indicators on a ticker.

    Pulls fresh OHLC, runs the pandas-ta indicator pack, and returns
    the latest value for each plus pre-computed semantic flags so the
    consuming agent doesn't have to interpret the numbers itself.

    Parameters
    ----------
    ticker : str
        Yahoo Finance ticker symbol.
    window_days : int, default 252
        Lookback for the price series (long enough that 200-day SMA is
        meaningful — must be ≥ 220).

    Returns
    -------
    dict
        Hierarchical dict with sections:
          trend       — sma_20/50/200, ema_12/26, golden_cross_recent,
                        above_50d, above_200d
          momentum    — rsi_14 + state, macd, macd_signal,
                        macd_bullish_cross
          volatility  — bollinger upper/middle/lower, bb_width,
                        bb_state ("near_upper" | "near_lower" | "mid")
          trend_strength — adx_14 + state
          summary     — one-line plain-English of the current posture
    """
    import pandas_ta as ta
    from findata.equity_prices import get_equity_prices

    if not isinstance(ticker, str) or not ticker.strip():
        raise ValueError("ticker must be a non-empty string")
    window_days = max(220, min(2520, int(window_days)))

    end = pd.Timestamp.utcnow().normalize()
    # Pad the start so a 200-day SMA has full warmup before window edge.
    start = end - pd.Timedelta(days=int(window_days * 1.4))

    ticker_u = ticker.strip().upper()
    try:
        df = get_equity_prices(
            tickers=[ticker_u],
            start_date=start.strftime("%Y-%m-%d"),
            end_date=end.strftime("%Y-%m-%d"),
        )
    except Exception:
        df = pd.DataFrame()

    if df.empty:
        return {
            "ticker": ticker_u,
            "as_of": end.strftime("%Y-%m-%d"),
            "summary": f"No price data returned for {ticker_u}.",
        }

    # Normalise to flat ohlc.
    if isinstance(df.columns, pd.MultiIndex):
        ohlc = pd.DataFrame()
        for f in ("Open", "High", "Low", "Close"):
            try:
                ohlc[f.lower()] = df[f][ticker_u]
            except Exception:
                pass
    else:
        ohlc = df.rename(columns={c: str(c).lower() for c in df.columns})
        ohlc = ohlc[[c for c in ("open", "high", "low", "close") if c in ohlc.columns]]

    if "close" not in ohlc.columns or ohlc["close"].dropna().empty:
        return {"ticker": ticker_u, "as_of": end.strftime("%Y-%m-%d"),
                "summary": f"OHLC missing for {ticker_u}."}

    close = ohlc["close"].dropna()
    high = ohlc.get("high", close).dropna()
    low = ohlc.get("low", close).dropna()

    # ── pandas-ta calls — defensively wrapped, each indicator independent
    def _safe(fn, *args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except Exception:
            return None

    sma_20 = _safe(ta.sma, close, length=20)
    sma_50 = _safe(ta.sma, close, length=50)
    sma_200 = _safe(ta.sma, close, length=200)
    ema_12 = _safe(ta.ema, close, length=12)
    ema_26 = _safe(ta.ema, close, length=26)
    rsi_14 = _safe(ta.rsi, close, length=14)
    macd_df = _safe(ta.macd, close, fast=12, slow=26, signal=9)
    bb_df = _safe(ta.bbands, close, length=20, std=2)
    adx_df = _safe(ta.adx, high, low, close, length=14)

    last_close = float(close.iloc[-1])

    def _last(series) -> Optional[float]:
        if series is None or len(series) == 0:
            return None
        v = series.dropna()
        if v.empty:
            return None
        return float(v.iloc[-1])

    # Golden / death cross detection: did SMA_50 cross SMA_200 within
    # the last 30 days?
    golden_cross_recent = False
    death_cross_recent = False
    if sma_50 is not None and sma_200 is not None:
        s50 = sma_50.dropna()
        s200 = sma_200.dropna()
        joint = pd.concat([s50, s200], axis=1, join="inner")
        joint.columns = ["s50", "s200"]
        if len(joint) > 31:
            recent = joint.tail(30)
            diff = recent["s50"] - recent["s200"]
            sign_change = (diff.shift(1) < 0) & (diff > 0)
            golden_cross_recent = bool(sign_change.any())
            sign_change_down = (diff.shift(1) > 0) & (diff < 0)
            death_cross_recent = bool(sign_change_down.any())

    # MACD bullish-cross detection: did MACD cross above MACD_signal in
    # the last 5 days?
    macd_bullish_cross = False
    macd_val = None
    macd_signal_val = None
    if macd_df is not None and not macd_df.empty:
        macd_col = next((c for c in macd_df.columns if c.startswith("MACD_") and "h" not in c.lower()), None)
        signal_col = next((c for c in macd_df.columns if c.startswith("MACDs_")), None)
        if macd_col and signal_col:
            macd_val = _last(macd_df[macd_col])
            macd_signal_val = _last(macd_df[signal_col])
            recent = macd_df[[macd_col, signal_col]].dropna().tail(6)
            if len(recent) >= 2:
                diff = recent[macd_col] - recent[signal_col]
                xover = (diff.shift(1) < 0) & (diff > 0)
                macd_bullish_cross = bool(xover.any())

    bb_upper = bb_middle = bb_lower = None
    if bb_df is not None and not bb_df.empty:
        bb_upper = _last(next((bb_df[c] for c in bb_df.columns if c.startswith("BBU_")), None))
        bb_middle = _last(next((bb_df[c] for c in bb_df.columns if c.startswith("BBM_")), None))
        bb_lower = _last(next((bb_df[c] for c in bb_df.columns if c.startswith("BBL_")), None))
    bb_width = None
    bb_state = None
    if bb_upper and bb_lower and bb_middle:
        bb_width = (bb_upper - bb_lower) / bb_middle if bb_middle else None
        if last_close >= bb_upper * 0.995:
            bb_state = "near_upper"
        elif last_close <= bb_lower * 1.005:
            bb_state = "near_lower"
        else:
            bb_state = "mid"

    adx_val = None
    if adx_df is not None and not adx_df.empty:
        adx_val = _last(next((adx_df[c] for c in adx_df.columns if c.startswith("ADX_")), None))

    sma_50_val = _last(sma_50)
    sma_200_val = _last(sma_200)

    rsi_val = _last(rsi_14)
    rsi_state = _classify_rsi(rsi_val) if rsi_val is not None else None
    adx_state = _classify_adx(adx_val) if adx_val is not None else None

    summary_parts: list[str] = [f"{ticker_u} at ${last_close:.2f}."]
    if sma_50_val and sma_200_val:
        if last_close > sma_50_val and last_close > sma_200_val:
            summary_parts.append(
                f"Above 50-day (${sma_50_val:.2f}) and 200-day (${sma_200_val:.2f}) SMAs."
            )
        elif last_close < sma_50_val and last_close < sma_200_val:
            summary_parts.append(
                f"Below 50-day (${sma_50_val:.2f}) and 200-day (${sma_200_val:.2f}) SMAs."
            )
    if golden_cross_recent:
        summary_parts.append("Golden cross within last 30d.")
    if death_cross_recent:
        summary_parts.append("Death cross within last 30d.")
    if rsi_val is not None:
        summary_parts.append(f"RSI {rsi_val:.1f} ({rsi_state}).")
    if macd_bullish_cross:
        summary_parts.append("MACD bullish cross within last 5d.")
    if adx_val is not None:
        summary_parts.append(f"ADX {adx_val:.1f} ({adx_state}).")
    if bb_state:
        summary_parts.append(f"Bollinger: {bb_state}.")

    return {
        "ticker": ticker_u,
        "as_of": end.strftime("%Y-%m-%d"),
        "current_price": last_close,
        "trend": {
            "sma_20": _last(sma_20),
            "sma_50": sma_50_val,
            "sma_200": sma_200_val,
            "ema_12": _last(ema_12),
            "ema_26": _last(ema_26),
            "above_50d": (sma_50_val is not None and last_close > sma_50_val),
            "above_200d": (sma_200_val is not None and last_close > sma_200_val),
            "golden_cross_recent": golden_cross_recent,
            "death_cross_recent": death_cross_recent,
        },
        "momentum": {
            "rsi_14": rsi_val,
            "rsi_state": rsi_state,
            "macd": macd_val,
            "macd_signal": macd_signal_val,
            "macd_bullish_cross_5d": macd_bullish_cross,
        },
        "volatility": {
            "bb_upper": bb_upper,
            "bb_middle": bb_middle,
            "bb_lower": bb_lower,
            "bb_width": bb_width,
            "bb_state": bb_state,
        },
        "trend_strength": {
            "adx_14": adx_val,
            "adx_state": adx_state,
        },
        "summary": " ".join(summary_parts),
    }
