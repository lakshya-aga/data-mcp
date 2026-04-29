"""
findata.stooq
-------------
Daily OHLCV data from Stooq (https://stooq.com), a free source that is often
cleaner than Yahoo Finance for international markets.
"""

from __future__ import annotations

from typing import List, Optional


def get_stooq_prices(
    tickers: List[str],
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    fields: Optional[List[str]] = None,
) -> "pd.DataFrame":
    """
    Fetch historical daily OHLCV bars from Stooq.

    Stooq exposes a free CSV endpoint for daily prices on global equities,
    ETFs, indices, FX pairs, and commodities. Tickers follow Stooq's own
    convention (e.g. ``"aapl.us"`` for Apple, ``"^spx"`` for the S&P 500
    index, ``"eurusd"`` for the euro/dollar pair, ``"cdr.pl"`` for CD
    Projekt on the Warsaw Stock Exchange).

    Parameters
    ----------
    tickers : list[str]
        One or more Stooq ticker symbols.
    start_date : str or None, optional
        Inclusive start date, ``"YYYY-MM-DD"``. ``None`` returns full history.
    end_date : str or None, optional
        Inclusive end date, ``"YYYY-MM-DD"``. ``None`` returns through latest.
    fields : list[str] or None, optional
        Columns to keep. Accepted values:
        ``"Open"``, ``"High"``, ``"Low"``, ``"Close"``, ``"Volume"``.
        ``None`` (default) returns all available columns.

    Returns
    -------
    pd.DataFrame
        Multiple tickers — ``pd.MultiIndex`` columns ``(field, ticker)`` with
        a ``DatetimeIndex``.
        Single ticker — flat column index (field names only) with a
        ``DatetimeIndex``.

    Raises
    ------
    ValueError
        If ``tickers`` is empty or no data is returned.
    ImportError
        If a required package is not installed.

    Examples
    --------
    >>> from findata.stooq import get_stooq_prices
    >>> df = get_stooq_prices(["aapl.us", "^spx"], start_date="2020-01-01")
    """
    try:
        import pandas as pd
    except ImportError as exc:
        raise ImportError(
            "pandas is required. Install: pip install pandas"
        ) from exc

    try:
        from pandas_datareader import data as pdr_data
    except ImportError as exc:
        raise ImportError(
            "pandas-datareader is required. Install: pip install pandas-datareader"
        ) from exc

    if not isinstance(tickers, (list, tuple)) or not tickers:
        raise ValueError("tickers must be a non-empty list of strings.")

    cleaned: List[str] = []
    for t in tickers:
        if not isinstance(t, str) or not t.strip():
            raise ValueError("Each ticker must be a non-empty string.")
        cleaned.append(t.strip())

    start_dt = pd.to_datetime(start_date) if start_date else None
    end_dt = pd.to_datetime(end_date) if end_date else None
    if start_dt is not None and end_dt is not None and start_dt > end_dt:
        raise ValueError("start_date must be on or before end_date.")

    valid_fields = {"Open", "High", "Low", "Close", "Volume"}
    if fields is not None:
        if not isinstance(fields, (list, tuple)) or not fields:
            raise ValueError("fields must be a non-empty list or None.")
        bad = [f for f in fields if f not in valid_fields]
        if bad:
            raise ValueError(
                f"Unsupported fields {bad}. Choose from: {sorted(valid_fields)}"
            )

    frames = {}
    for t in cleaned:
        try:
            df = pdr_data.DataReader(t, "stooq", start=start_dt, end=end_dt)
        except Exception as exc:
            raise ValueError(
                f"Failed to fetch Stooq data for ticker {t!r}."
            ) from exc

        if df is None or df.empty:
            raise ValueError(f"No data returned for ticker {t!r}.")

        df = df.sort_index()
        df.index = pd.to_datetime(df.index)
        df.index.name = "date"

        if fields:
            df = df[[c for c in fields if c in df.columns]]

        frames[t] = df

    if len(frames) == 1:
        return next(iter(frames.values()))

    combined = pd.concat(frames, axis=1)
    combined.columns = combined.columns.swaplevel(0, 1)
    combined = combined.sort_index(axis=1)
    return combined
