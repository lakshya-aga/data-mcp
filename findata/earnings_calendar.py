"""
findata.earnings_calendar
-------------------------
Past + upcoming earnings dates and consensus / actual EPS via yfinance.

Anchors the time-horizon field on a debate verdict. Without an earnings
calendar, agents pick "3-6 months" as a default. With it, they can
reason about whether a thesis is contingent on the next print or an
event further out.
"""

from __future__ import annotations

from typing import Optional

import pandas as pd


def get_earnings_calendar(
    ticker: str,
    days_back: int = 365,
    days_forward: int = 90,
) -> pd.DataFrame:
    """
    Fetch past + upcoming earnings rows for a single ticker.

    Returns one row per earnings event with EPS estimate vs. actual when
    the event has reported. yfinance's ``.earnings_dates`` attribute is
    the source — it returns up to ~4 quarters of history plus the next
    1-2 scheduled prints.

    Use this for time-horizon reasoning (does the thesis pay off before
    or after the next print?), surprise-history reasoning (this name
    has beaten EPS estimates 7 of the last 8 quarters), and event-window
    backtests.

    Parameters
    ----------
    ticker : str
        Yahoo Finance ticker symbol, e.g. ``"AAPL"``.
    days_back : int, default 365
        How far back to look for past earnings rows. Capped at 1825
        (5 years) but yfinance typically returns at most ~8 quarters.
    days_forward : int, default 90
        How far forward to surface upcoming scheduled earnings.

    Returns
    -------
    pandas.DataFrame
        Index: ``DatetimeIndex`` of earnings dates (sorted ascending).
        Columns: ``eps_estimate``, ``eps_actual``, ``surprise_pct``,
        ``is_past``, ``ticker``.

        ``is_past`` is True for events that have already reported,
        False for upcoming. ``surprise_pct`` is
        ``(actual − estimate) / |estimate|`` when both are present,
        else NaN.

    Examples
    --------
    >>> from findata.earnings_calendar import get_earnings_calendar
    >>> df = get_earnings_calendar("AAPL", days_back=730, days_forward=120)
    >>> df.tail(6)[["eps_estimate", "eps_actual", "surprise_pct", "is_past"]]
    """
    import yfinance as yf

    if not isinstance(ticker, str) or not ticker.strip():
        raise ValueError("ticker must be a non-empty string")
    days_back = max(1, min(1825, int(days_back)))
    days_forward = max(0, min(365, int(days_forward)))

    try:
        cal = yf.Ticker(ticker).earnings_dates
    except Exception:
        cal = None

    if cal is None or cal.empty:
        return pd.DataFrame(
            columns=["eps_estimate", "eps_actual", "surprise_pct", "is_past", "ticker"],
            index=pd.DatetimeIndex([], name="date"),
        )

    # yfinance column names are 'EPS Estimate', 'Reported EPS', 'Surprise(%)' —
    # normalise. Versions vary slightly.
    df = cal.copy()
    df.columns = [str(c).strip() for c in df.columns]
    rename = {
        "EPS Estimate":   "eps_estimate",
        "Reported EPS":   "eps_actual",
        "Surprise(%)":    "surprise_pct",
        "Surprise (%)":   "surprise_pct",
    }
    df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})
    for col in ("eps_estimate", "eps_actual", "surprise_pct"):
        if col not in df.columns:
            df[col] = pd.NA

    # Filter by date window. The index is timezone-aware in newer yfinance;
    # compare against the same tz.
    now = pd.Timestamp.utcnow()
    if df.index.tz is None:
        df.index = df.index.tz_localize("UTC")
    cutoff_back = now - pd.Timedelta(days=days_back)
    cutoff_forward = now + pd.Timedelta(days=days_forward)
    df = df.loc[(df.index >= cutoff_back) & (df.index <= cutoff_forward)].copy()

    df["is_past"] = df.index <= now
    df["ticker"] = ticker.strip().upper()
    df.index.name = "date"
    df = df.sort_index()
    return df[["eps_estimate", "eps_actual", "surprise_pct", "is_past", "ticker"]]
