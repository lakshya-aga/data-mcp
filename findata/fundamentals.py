"""
findata.fundamentals
--------------------
Snapshot of company fundamentals via yfinance — valuation multiples,
growth, margins, cash position, sector classification.

One row per ticker. No history (use ``yf.Ticker(t).financials`` directly
if you need quarterly statement series). Designed for "what's the
current shape of this business?" queries — perfect ground-truth for
chat-style debate or recipe-builder agents.
"""

from __future__ import annotations

from typing import List

import pandas as pd


# Fields we surface, mapped to yfinance .info keys. Dict ordering is the
# column order in the returned DataFrame — most useful first.
_FIELDS: dict[str, str] = {
    # Identity
    "name":             "longName",
    "sector":           "sector",
    "industry":         "industry",
    "country":          "country",
    "currency":         "financialCurrency",
    "market_cap":       "marketCap",
    "shares_out":       "sharesOutstanding",
    # Valuation
    "trailing_pe":      "trailingPE",
    "forward_pe":       "forwardPE",
    "peg_ratio":        "pegRatio",
    "price_to_book":    "priceToBook",
    "price_to_sales":   "priceToSalesTrailing12Months",
    "ev_to_ebitda":     "enterpriseToEbitda",
    "ev_to_revenue":    "enterpriseToRevenue",
    # Profitability / margins
    "profit_margin":    "profitMargins",
    "operating_margin": "operatingMargins",
    "gross_margin":     "grossMargins",
    "roe":              "returnOnEquity",
    "roa":              "returnOnAssets",
    # Growth (TTM YoY)
    "revenue_growth":   "revenueGrowth",
    "earnings_growth":  "earningsGrowth",
    # Balance sheet snapshot
    "total_cash":       "totalCash",
    "total_debt":       "totalDebt",
    "free_cash_flow":   "freeCashflow",
    "operating_cash":   "operatingCashflow",
    # Income / dividend
    "dividend_yield":   "dividendYield",
    "payout_ratio":     "payoutRatio",
    "beta":             "beta",
    # Recent price context
    "current_price":    "currentPrice",
    "fifty_two_week_low":  "fiftyTwoWeekLow",
    "fifty_two_week_high": "fiftyTwoWeekHigh",
}


def get_equity_fundamentals(tickers: List[str]) -> pd.DataFrame:
    """
    Fetch a fundamentals snapshot for one or more equity tickers.

    Wraps ``yfinance.Ticker(t).info`` and projects a curated set of
    valuation, profitability, balance-sheet, and growth fields into a
    consistent DataFrame. yfinance's ``.info`` dict is large and noisy;
    this filters it down to the ~25 fields most useful for debate-style
    bull/bear research.

    Parameters
    ----------
    tickers : list[str]
        Yahoo Finance ticker symbols, e.g. ``["AAPL", "MSFT"]``.

    Returns
    -------
    pandas.DataFrame
        Index: ticker (string).
        Columns: ``name``, ``sector``, ``industry``, ``country``,
        ``currency``, ``market_cap``, ``shares_out``, ``trailing_pe``,
        ``forward_pe``, ``peg_ratio``, ``price_to_book``,
        ``price_to_sales``, ``ev_to_ebitda``, ``ev_to_revenue``,
        ``profit_margin``, ``operating_margin``, ``gross_margin``,
        ``roe``, ``roa``, ``revenue_growth``, ``earnings_growth``,
        ``total_cash``, ``total_debt``, ``free_cash_flow``,
        ``operating_cash``, ``dividend_yield``, ``payout_ratio``,
        ``beta``, ``current_price``, ``fifty_two_week_low``,
        ``fifty_two_week_high``.

        Missing fields render as NaN. yfinance failures yield an empty
        row for that ticker rather than raising.

    Examples
    --------
    >>> from findata.fundamentals import get_equity_fundamentals
    >>> df = get_equity_fundamentals(["AAPL", "MSFT", "NVDA"])
    >>> df[["forward_pe", "revenue_growth", "free_cash_flow"]]
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
        row = {"ticker": t.strip().upper()}
        for col, key in _FIELDS.items():
            row[col] = info.get(key)
        rows.append(row)

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.set_index("ticker")[list(_FIELDS.keys())]
    return df
