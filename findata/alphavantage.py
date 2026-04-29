"""
findata.alphavantage
--------------------
AlphaVantage REST API wrapper for daily and intraday equity bars.

The free tier is rate-limited (5 requests/minute, 25 requests/day at the
time of writing); use ``ALPHAVANTAGE_API_KEY`` from the environment or pass
``api_key=`` explicitly.
"""

from __future__ import annotations

import os
from typing import Optional


_DAILY_INTERVALS = {"1d", "1wk", "1mo"}
_INTRADAY_INTERVALS = {"1min", "5min", "15min", "30min", "60min"}
_VALID_INTERVALS = _DAILY_INTERVALS | _INTRADAY_INTERVALS


def get_alphavantage_prices(
    symbol: str,
    interval: str = "1d",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    adjusted: bool = True,
    output_size: str = "full",
    api_key: Optional[str] = None,
    timeout: int = 30,
) -> "pd.DataFrame":
    """
    Fetch OHLCV bars for an equity symbol from AlphaVantage.

    Routes to ``TIME_SERIES_DAILY[_ADJUSTED]``, ``TIME_SERIES_WEEKLY[_ADJUSTED]``,
    ``TIME_SERIES_MONTHLY[_ADJUSTED]``, or ``TIME_SERIES_INTRADAY`` based on
    ``interval``.

    Parameters
    ----------
    symbol : str
        Equity ticker, e.g. ``"AAPL"``, ``"MSFT"``, ``"VTI"``.
    interval : str, optional
        Bar size. Daily: ``"1d"`` (default), ``"1wk"``, ``"1mo"``. Intraday:
        ``"1min"``, ``"5min"``, ``"15min"``, ``"30min"``, ``"60min"``.
    start_date : str or None, optional
        Inclusive lower bound, ``"YYYY-MM-DD"``. Filtered client-side.
    end_date : str or None, optional
        Inclusive upper bound, ``"YYYY-MM-DD"``. Filtered client-side.
    adjusted : bool, optional
        For daily/weekly/monthly bars, request the split/dividend-adjusted
        series. Ignored for intraday. Default ``True``.
    output_size : str, optional
        ``"full"`` (default) or ``"compact"`` (last 100 rows). Applies to
        daily and intraday endpoints.
    api_key : str or None, optional
        AlphaVantage API key. Falls back to the ``ALPHAVANTAGE_API_KEY``
        environment variable.
    timeout : int, optional
        Request timeout in seconds (default ``30``).

    Returns
    -------
    pd.DataFrame
        DatetimeIndex rows with columns ``open``, ``high``, ``low``, ``close``,
        ``volume`` (and ``adjusted_close``, ``dividend``, ``split_coefficient``
        when adjusted daily/weekly/monthly is selected).

    Raises
    ------
    ValueError
        If inputs are invalid, the API returns an error, or no data is
        returned.
    ImportError
        If a required package is not installed.

    Examples
    --------
    >>> from findata.alphavantage import get_alphavantage_prices
    >>> df = get_alphavantage_prices("AAPL", interval="5min",
    ...                              start_date="2024-12-01", end_date="2024-12-31")
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

    if not isinstance(symbol, str) or not symbol.strip():
        raise ValueError("symbol must be a non-empty string.")
    if interval not in _VALID_INTERVALS:
        raise ValueError(
            f"interval={interval!r} is not supported. "
            f"Choose from daily {sorted(_DAILY_INTERVALS)} or intraday "
            f"{sorted(_INTRADAY_INTERVALS)}."
        )
    if output_size not in {"compact", "full"}:
        raise ValueError("output_size must be 'compact' or 'full'.")
    if not isinstance(timeout, int) or timeout <= 0:
        raise ValueError("timeout must be a positive integer (seconds).")

    key = api_key or os.environ.get("ALPHAVANTAGE_API_KEY")
    if not key:
        raise ValueError(
            "AlphaVantage API key is required. Pass api_key= or set "
            "ALPHAVANTAGE_API_KEY in the environment."
        )

    if interval in _INTRADAY_INTERVALS:
        function = "TIME_SERIES_INTRADAY"
        params = {
            "function": function,
            "symbol": symbol.upper(),
            "interval": interval,
            "outputsize": output_size,
            "adjusted": "true" if adjusted else "false",
            "datatype": "json",
            "apikey": key,
        }
        series_key = f"Time Series ({interval})"
    else:
        suffix = "_ADJUSTED" if adjusted else ""
        if interval == "1d":
            function = f"TIME_SERIES_DAILY{suffix}"
            series_key = "Time Series (Daily)"
        elif interval == "1wk":
            function = f"TIME_SERIES_WEEKLY{suffix}"
            series_key = (
                "Weekly Adjusted Time Series" if adjusted else "Weekly Time Series"
            )
        else:
            function = f"TIME_SERIES_MONTHLY{suffix}"
            series_key = (
                "Monthly Adjusted Time Series" if adjusted else "Monthly Time Series"
            )
        params = {
            "function": function,
            "symbol": symbol.upper(),
            "outputsize": output_size,
            "datatype": "json",
            "apikey": key,
        }

    url = "https://www.alphavantage.co/query"
    try:
        resp = requests.get(url, params=params, timeout=timeout)
        resp.raise_for_status()
    except Exception as exc:
        raise ValueError(
            f"Failed to fetch AlphaVantage data for {symbol!r}."
        ) from exc

    payload = resp.json()
    for err_key in ("Error Message", "Note", "Information"):
        if err_key in payload and series_key not in payload:
            raise ValueError(
                f"AlphaVantage returned {err_key}: {payload[err_key]}"
            )

    series = payload.get(series_key)
    if not series:
        raise ValueError(
            f"AlphaVantage returned no series under key {series_key!r}. "
            f"Response keys: {list(payload.keys())}"
        )

    df = pd.DataFrame.from_dict(series, orient="index")
    df.index = pd.to_datetime(df.index)
    df.index.name = "date"
    df = df.sort_index()

    rename = {
        "1. open": "open",
        "2. high": "high",
        "3. low": "low",
        "4. close": "close",
        "5. volume": "volume",
        "5. adjusted close": "adjusted_close",
        "6. volume": "volume",
        "7. dividend amount": "dividend",
        "8. split coefficient": "split_coefficient",
    }
    df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})
    df = df.astype(float, errors="ignore")
    if "volume" in df.columns:
        df["volume"] = df["volume"].astype(float)

    start_dt = pd.to_datetime(start_date) if start_date else None
    end_dt = pd.to_datetime(end_date) if end_date else None
    if start_dt is not None:
        df = df[df.index >= start_dt]
    if end_dt is not None:
        df = df[df.index <= end_dt]

    if df.empty:
        raise ValueError(f"No data returned for {symbol!r} in the requested range.")
    return df
