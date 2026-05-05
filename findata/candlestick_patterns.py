"""
findata.candlestick_patterns
----------------------------
Recognise classic candlestick patterns (hammer, engulfing, doji,
morning star, …) on a price history.

Wraps ``pandas-ta``'s ``cdl_pattern("all")`` which exposes ~60 patterns.
We post-process to:
  * collapse the wide one-column-per-pattern frame into a long list
    of {date, pattern, signal} events (most cells are 0, so the wide
    frame is mostly noise)
  * tag each event as ``bullish`` / ``bearish`` from the cell sign
    (pandas-ta uses +100 / -100 / 0)
  * generate a one-line summary string the agent can quote verbatim
"""

from __future__ import annotations

from typing import Optional

import pandas as pd


def detect_candlestick_patterns(
    ticker: str,
    lookback_days: int = 90,
    benchmark_ticker: Optional[str] = None,  # reserved for future filters
) -> dict:
    """
    Detect candlestick patterns over the last ``lookback_days`` of an
    asset's price history.

    Pulls OHLC from yfinance, runs pandas-ta's catalogue of pattern
    recognisers, returns the non-zero events as a list ordered most-
    recent first.

    Parameters
    ----------
    ticker : str
        Yahoo Finance ticker symbol.
    lookback_days : int, default 90
        Calendar-day window. Capped at 1825 (5 years).
    benchmark_ticker : str or None
        Reserved — currently unused. Future versions may filter pattern
        signals by relative strength vs a benchmark.

    Returns
    -------
    dict
        {
          "ticker": str,
          "lookback_days": int,
          "n_patterns_found": int,
          "patterns": [
            {"date": "YYYY-MM-DD", "pattern": str, "signal": "bullish"|"bearish",
             "close_at_pattern": float},
            ...
          ],
          "summary": str  # one-line — first thing an agent quotes
        }

    Examples
    --------
    >>> from findata.candlestick_patterns import detect_candlestick_patterns
    >>> r = detect_candlestick_patterns("AAPL", lookback_days=90)
    >>> r["summary"]
    >>> r["patterns"][:3]
    """
    import pandas_ta as ta
    from findata.equity_prices import get_equity_prices

    if not isinstance(ticker, str) or not ticker.strip():
        raise ValueError("ticker must be a non-empty string")
    lookback_days = max(20, min(1825, int(lookback_days)))

    end = pd.Timestamp.utcnow().normalize()
    # Pad the start window — pandas-ta needs a few weeks of history before
    # most patterns can be detected; the 1.5x pad ensures the requested
    # window is fully covered after warm-up.
    start = end - pd.Timedelta(days=int(lookback_days * 1.5))

    try:
        df = get_equity_prices(
            tickers=[ticker.strip().upper()],
            start_date=start.strftime("%Y-%m-%d"),
            end_date=end.strftime("%Y-%m-%d"),
        )
    except Exception:
        df = pd.DataFrame()

    if df.empty:
        return {
            "ticker": ticker.strip().upper(),
            "lookback_days": lookback_days,
            "n_patterns_found": 0,
            "patterns": [],
            "summary": f"No price data returned for {ticker}.",
        }

    # Normalise to a single-ticker OHLC frame regardless of yfinance's
    # MultiIndex shape. pandas-ta needs columns named open/high/low/close.
    if isinstance(df.columns, pd.MultiIndex):
        # Try to extract the asset's frame for each OHLC field
        ohlc = pd.DataFrame()
        for field in ("Open", "High", "Low", "Close"):
            try:
                ohlc[field.lower()] = df[field][ticker.strip().upper()]
            except Exception:
                pass
    else:
        ohlc = df.rename(columns={c: str(c).lower() for c in df.columns})
        ohlc = ohlc[[c for c in ("open", "high", "low", "close") if c in ohlc.columns]]

    if ohlc.empty or any(c not in ohlc.columns for c in ("open", "high", "low", "close")):
        return {
            "ticker": ticker.strip().upper(),
            "lookback_days": lookback_days,
            "n_patterns_found": 0,
            "patterns": [],
            "summary": f"OHLC data for {ticker} missing required columns.",
        }

    # Trim to the requested window (after using extra history for warm-up).
    cutoff = end - pd.Timedelta(days=lookback_days)
    if ohlc.index.tz is not None:
        cutoff = cutoff.tz_localize(ohlc.index.tz)

    # pandas-ta's cdl_pattern adds many 'CDL_xxx' columns inline.
    try:
        patterns_df = ohlc.ta.cdl_pattern(name="all")
    except Exception:
        # Some pandas-ta versions raise on cdl_pattern; fall back to a
        # smaller curated set that's robust across versions.
        patterns_df = pd.DataFrame(index=ohlc.index)
        for fn in ("cdl_doji", "cdl_inside", "cdl_z"):
            try:
                col = getattr(ohlc.ta, fn)()
                if col is not None:
                    patterns_df[fn.upper()] = col
            except Exception:
                pass

    if patterns_df is None or patterns_df.empty:
        return {
            "ticker": ticker.strip().upper(),
            "lookback_days": lookback_days,
            "n_patterns_found": 0,
            "patterns": [],
            "summary": f"No candlestick patterns recognised on {ticker} over {lookback_days}d.",
        }

    # Trim to window + collapse the wide dataframe into events.
    patterns_df = patterns_df.loc[patterns_df.index >= cutoff]
    events: list[dict] = []
    for date, row in patterns_df.iterrows():
        for col, val in row.items():
            if val is None or val == 0 or pd.isna(val):
                continue
            try:
                v = float(val)
            except (TypeError, ValueError):
                continue
            if v == 0:
                continue
            close_val = ohlc.loc[date, "close"] if date in ohlc.index else None
            events.append({
                "date": date.strftime("%Y-%m-%d"),
                "pattern": str(col).removeprefix("CDL_").lower(),
                "signal": "bullish" if v > 0 else "bearish",
                "close_at_pattern": float(close_val) if close_val is not None else None,
            })

    events.sort(key=lambda e: e["date"], reverse=True)

    bull_count = sum(1 for e in events if e["signal"] == "bullish")
    bear_count = sum(1 for e in events if e["signal"] == "bearish")
    most_recent = events[0] if events else None
    summary = (
        f"{ticker.strip().upper()} last {lookback_days}d: "
        f"{bull_count} bullish + {bear_count} bearish candlestick patterns. "
    )
    if most_recent:
        summary += (
            f"Most recent: {most_recent['signal']} {most_recent['pattern']} on "
            f"{most_recent['date']}"
        )
        if most_recent.get("close_at_pattern"):
            summary += f" (close ${most_recent['close_at_pattern']:.2f})."
        else:
            summary += "."

    return {
        "ticker": ticker.strip().upper(),
        "lookback_days": lookback_days,
        "n_patterns_found": len(events),
        "patterns": events,
        "summary": summary,
    }
