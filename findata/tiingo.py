"""
findata.tiingo
--------------
Tiingo REST API wrapper for daily and intraday equity bars. Requires a free
or paid API key (intraday/IEX endpoints are paid-only on most plans).
"""

from __future__ import annotations

import os
from typing import Optional


_DAILY_FREQS = {"daily", "weekly", "monthly", "annually"}
_INTRADAY_FREQS = {"1min", "5min", "15min", "30min", "1hour"}


def get_tiingo_prices(
    ticker: str,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    frequency: str = "daily",
    api_key: Optional[str] = None,
    timeout: int = 30,
) -> "pd.DataFrame":
    """
    Fetch historical bars for a Tiingo ticker.

    Routes to ``/tiingo/daily/{ticker}/prices`` for daily/weekly/monthly bars
    or ``/iex/{ticker}/prices`` for intraday bars.

    Parameters
    ----------
    ticker : str
        Equity / ETF symbol, e.g. ``"AAPL"``, ``"SPY"``.
    start_date : str or None, optional
        Inclusive lower bound, ``"YYYY-MM-DD"``.
    end_date : str or None, optional
        Inclusive upper bound, ``"YYYY-MM-DD"``.
    frequency : str, optional
        Daily-or-lower: ``"daily"`` (default), ``"weekly"``, ``"monthly"``,
        ``"annually"``. Intraday (IEX endpoint, paid plans):
        ``"1min"``, ``"5min"``, ``"15min"``, ``"30min"``, ``"1hour"``.
    api_key : str or None, optional
        Tiingo API key. Falls back to the ``TIINGO_API_KEY`` environment
        variable.
    timeout : int, optional
        Request timeout in seconds (default ``30``).

    Returns
    -------
    pd.DataFrame
        DatetimeIndex rows with the columns Tiingo returns. Daily endpoint
        columns include ``open``, ``high``, ``low``, ``close``, ``volume``,
        ``adjOpen``, ``adjHigh``, ``adjLow``, ``adjClose``, ``adjVolume``,
        ``divCash``, ``splitFactor``. Intraday endpoint columns include
        ``open``, ``high``, ``low``, ``close``, ``volume``.

    Raises
    ------
    ValueError
        If inputs are invalid or the API returns an error.
    ImportError
        If a required package is not installed.

    Examples
    --------
    >>> from findata.tiingo import get_tiingo_prices
    >>> df = get_tiingo_prices("AAPL", start_date="2024-01-01",
    ...                        end_date="2024-12-31", frequency="daily")
    """
    try:
        import pandas as pd
    except ImportError as exc:
        raise ImportError(
            "pandas is required. Install: pip install pandas"
        ) from exc

    try:
        import requests
    except ImportError as exc:
        raise ImportError(
            "requests is required. Install: pip install requests"
        ) from exc

    if not isinstance(ticker, str) or not ticker.strip():
        raise ValueError("ticker must be a non-empty string.")

    if frequency in _DAILY_FREQS:
        intraday = False
    elif frequency in _INTRADAY_FREQS:
        intraday = True
    else:
        raise ValueError(
            f"frequency={frequency!r} is not supported. Daily: "
            f"{sorted(_DAILY_FREQS)}; intraday: {sorted(_INTRADAY_FREQS)}."
        )

    if not isinstance(timeout, int) or timeout <= 0:
        raise ValueError("timeout must be a positive integer (seconds).")

    key = api_key or os.environ.get("TIINGO_API_KEY")
    if not key:
        raise ValueError(
            "Tiingo API key is required. Pass api_key= or set "
            "TIINGO_API_KEY in the environment."
        )

    if intraday:
        url = f"https://api.tiingo.com/iex/{ticker.upper()}/prices"
    else:
        url = f"https://api.tiingo.com/tiingo/daily/{ticker.upper()}/prices"

    params: dict = {"resampleFreq" if intraday else "resampleFreq": frequency}
    params["format"] = "json"
    if start_date:
        params["startDate"] = start_date
    if end_date:
        params["endDate"] = end_date

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Token {key}",
    }

    try:
        resp = requests.get(url, params=params, headers=headers, timeout=timeout)
        resp.raise_for_status()
    except Exception as exc:
        raise ValueError(
            f"Failed to fetch Tiingo data for {ticker!r}."
        ) from exc

    rows = resp.json()
    if not isinstance(rows, list) or not rows:
        raise ValueError(f"No data returned by Tiingo for {ticker!r}.")

    df = pd.DataFrame(rows)
    date_col = "date" if "date" in df.columns else None
    if date_col is None:
        raise ValueError(f"Tiingo response missing date column. Got: {df.columns.tolist()}")

    df[date_col] = pd.to_datetime(df[date_col])
    df = df.set_index(date_col).sort_index()
    df.index = df.index.tz_localize(None) if df.index.tz is not None else df.index
    df.index.name = "date"
    return df
