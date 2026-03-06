"""
findata.equity_prices
---------------------
Wrapper around yfinance for fetching historical OHLCV equity price data.
"""

from __future__ import annotations

from typing import List, Optional

import pandas as pd


def get_equity_prices(
    tickers: List[str],
    start_date: str,
    end_date: str,
    fields: Optional[List[str]] = None,
    frequency: str = "1d",
    auto_adjust: bool = True,
) -> pd.DataFrame:
    """
    Fetch historical OHLCV price data for one or more equity tickers.

    Wraps ``yfinance.download`` with normalised column handling and input
    validation.  Returns a consistently shaped DataFrame regardless of
    whether one or many tickers are requested.

    Parameters
    ----------
    tickers : list[str]
        Ticker symbols, e.g. ``["AAPL", "MSFT", "GOOG"]``.
    start_date : str
        Inclusive start date, ``"YYYY-MM-DD"``.
    end_date : str
        Inclusive end date, ``"YYYY-MM-DD"``.
    fields : list[str] or None, optional
        OHLCV columns to keep.  Accepted values:
        ``"Open"``, ``"High"``, ``"Low"``, ``"Close"``, ``"Volume"``.
        ``None`` (default) returns all columns.
    frequency : str, optional
        Bar size passed to yfinance ``interval``.
        Supported: ``"1d"`` (default), ``"5d"``, ``"1wk"``, ``"1mo"``, ``"3mo"``.
    auto_adjust : bool, optional
        Adjust prices for splits and dividends (default ``True``).

    Returns
    -------
    pd.DataFrame
        Multiple tickers — ``pd.MultiIndex`` columns ``(field, ticker)``
        with a ``DatetimeIndex``.
        Single ticker   — flat column index (field names only) with a
        ``DatetimeIndex``.

    Raises
    ------
    ValueError
        If ``tickers`` is empty or ``frequency`` is not a supported value.
    ImportError
        If ``yfinance`` is not installed.

    Examples
    --------
    >>> from findata.equity_prices import get_equity_prices

    >>> # Single ticker — daily close only
    >>> df = get_equity_prices(
    ...     tickers=["AAPL"],
    ...     start_date="2024-01-01",
    ...     end_date="2024-12-31",
    ...     fields=["Close"],
    ... )
    >>> df.head()

    >>> # Multiple tickers — all OHLCV
    >>> df = get_equity_prices(
    ...     tickers=["AAPL", "MSFT", "NVDA"],
    ...     start_date="2023-01-01",
    ...     end_date="2024-01-01",
    ... )
    >>> close = df["Close"]           # DataFrame: rows=dates, cols=tickers
    >>> aapl = df["Close"]["AAPL"]    # Series

    >>> # Weekly bars
    >>> df = get_equity_prices(
    ...     tickers=["SPY"],
    ...     start_date="2020-01-01",
    ...     end_date="2024-01-01",
    ...     fields=["Close"],
    ...     frequency="1wk",
    ... )
    """
    try:
        import yfinance as yf
    except ImportError as exc:
        raise ImportError(
            "yfinance is required.  Install: pip install yfinance"
        ) from exc

    if not tickers:
        raise ValueError("tickers must be a non-empty list.")

    _VALID_FREQ = {"1d", "5d", "1wk", "1mo", "3mo"}
    if frequency not in _VALID_FREQ:
        raise ValueError(
            f"frequency={frequency!r} is not supported. "
            f"Choose from: {sorted(_VALID_FREQ)}"
        )

    # yfinance >= 0.2.38 dropped group_by in favour of multi_level_index.
    # We request multi_level_index=True so multi-ticker downloads always return
    # a MultiIndex, then flatten for single-ticker calls below.
    raw: pd.DataFrame = yf.download(
        tickers=tickers,
        start=start_date,
        end=end_date,
        interval=frequency,
        auto_adjust=auto_adjust,
        progress=False,
        multi_level_index=True,
    )

    if isinstance(raw.columns, pd.MultiIndex):
        if fields:
            raw = raw.loc[:, raw.columns.get_level_values(0).isin(fields)]
    else:
        # Older yfinance or single-ticker with flat cols
        if fields:
            raw = raw[[c for c in fields if c in raw.columns]]

    # Flatten MultiIndex for a single ticker so callers get df["Close"] not df["Close"]["AAPL"]
    if len(tickers) == 1 and isinstance(raw.columns, pd.MultiIndex):
        raw.columns = raw.columns.get_level_values(0)

    return raw
