"""
findata.analyst_consensus
-------------------------
Wall-Street analyst consensus snapshot via yfinance.

Surfaces the fields a PM needs to ground a target-price decision: mean /
high / low target, current vs. target spread, recommendation key,
analyst count. All from yfinance.Ticker.info — no API key.
"""

from __future__ import annotations

from typing import List

import pandas as pd


# yfinance recommendationKey values, ordered most bullish to most bearish.
_REC_ORDER = ["strong_buy", "buy", "hold", "underperform", "sell"]


def get_analyst_consensus(tickers: List[str]) -> pd.DataFrame:
    """
    Fetch the current Wall-Street consensus on one or more equity tickers.

    Returns a DataFrame with the analyst-coverage snapshot:
      * mean / high / low / median target prices
      * current price + implied upside (target_mean / current_price - 1)
      * recommendation key (strong_buy / buy / hold / underperform / sell)
      * recommendation mean (1.0 = strong buy, 5.0 = sell)
      * number of analysts covering the name

    Use this to anchor the moderator's target-price field — without it,
    target prices tend to be suspiciously round numbers picked from the
    model's prior. With it, the agent can say "consensus target is $X
    (12% upside, 41 analysts), my target reflects a 5% premium because…"

    Parameters
    ----------
    tickers : list[str]
        Yahoo Finance ticker symbols, e.g. ``["AAPL"]``.

    Returns
    -------
    pandas.DataFrame
        Index: ticker.
        Columns: ``current_price``, ``target_mean``, ``target_median``,
        ``target_high``, ``target_low``, ``upside_pct``,
        ``recommendation_key``, ``recommendation_mean``, ``num_analysts``.

    Examples
    --------
    >>> from findata.analyst_consensus import get_analyst_consensus
    >>> df = get_analyst_consensus(["AAPL", "MSFT"])
    >>> df[["target_mean", "upside_pct", "recommendation_key", "num_analysts"]]
    """
    import yfinance as yf

    if not isinstance(tickers, (list, tuple)) or not tickers:
        raise ValueError("tickers must be a non-empty list of strings")

    rows: list[dict] = []
    for t in tickers:
        if not isinstance(t, str) or not t.strip():
            continue
        try:
            info = yf.Ticker(t).info or {}
        except Exception:
            info = {}
        current = info.get("currentPrice")
        target_mean = info.get("targetMeanPrice")
        upside = None
        try:
            if isinstance(current, (int, float)) and isinstance(target_mean, (int, float)) and current:
                upside = (target_mean / current) - 1.0
        except Exception:
            upside = None
        rows.append({
            "ticker": t.strip().upper(),
            "current_price":       current,
            "target_mean":         target_mean,
            "target_median":       info.get("targetMedianPrice"),
            "target_high":         info.get("targetHighPrice"),
            "target_low":          info.get("targetLowPrice"),
            "upside_pct":          upside,
            "recommendation_key":  info.get("recommendationKey"),
            "recommendation_mean": info.get("recommendationMean"),
            "num_analysts":        info.get("numberOfAnalystOpinions"),
        })

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.set_index("ticker")
    return df
