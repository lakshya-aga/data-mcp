"""
findata.polygon
---------------
Polygon.io REST API wrapper for aggregate (OHLCV) bars.
"""

from __future__ import annotations

import os
from typing import Optional


_VALID_TIMESPANS = {
    "minute", "hour", "day", "week", "month", "quarter", "year",
}


def get_polygon_aggregates(
    ticker: str,
    start_date: str,
    end_date: str,
    multiplier: int = 1,
    timespan: str = "day",
    adjusted: bool = True,
    limit: int = 50000,
    api_key: Optional[str] = None,
    timeout: int = 30,
) -> "pd.DataFrame":
    """
    Fetch aggregate bars from Polygon.io.

    Wraps ``GET /v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{from}/{to}``
    and pages through ``next_url`` so requests larger than Polygon's per-page
    cap return a complete history.

    Parameters
    ----------
    ticker : str
        Equity, ETF, FX, or crypto ticker (e.g. ``"AAPL"``, ``"X:BTCUSD"``,
        ``"C:EURUSD"``).
    start_date : str
        Inclusive lower bound, ``"YYYY-MM-DD"``.
    end_date : str
        Inclusive upper bound, ``"YYYY-MM-DD"``.
    multiplier : int, optional
        Size of the timespan multiplier (default ``1``).
    timespan : str, optional
        Bar timespan. Supported: ``"minute"``, ``"hour"``, ``"day"`` (default),
        ``"week"``, ``"month"``, ``"quarter"``, ``"year"``.
    adjusted : bool, optional
        Whether to adjust for splits (default ``True``).
    limit : int, optional
        Page size (default ``50000``, which is also Polygon's cap).
    api_key : str or None, optional
        Polygon API key. Falls back to the ``POLYGON_API_KEY`` environment
        variable.
    timeout : int, optional
        Request timeout in seconds (default ``30``).

    Returns
    -------
    pd.DataFrame
        DatetimeIndex (bar open time, UTC-naive) with columns:
        ``open``, ``high``, ``low``, ``close``, ``volume``, ``vwap``,
        ``transactions``.

    Raises
    ------
    ValueError
        If inputs are invalid or no data is returned.
    ImportError
        If a required package is not installed.

    Examples
    --------
    >>> from findata.polygon import get_polygon_aggregates
    >>> df = get_polygon_aggregates("AAPL", "2024-01-01", "2024-12-31",
    ...                             multiplier=1, timespan="day")
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
    if timespan not in _VALID_TIMESPANS:
        raise ValueError(
            f"timespan={timespan!r} is not supported. "
            f"Choose from: {sorted(_VALID_TIMESPANS)}"
        )
    if not isinstance(multiplier, int) or multiplier <= 0:
        raise ValueError("multiplier must be a positive integer.")
    if not isinstance(limit, int) or not (1 <= limit <= 50000):
        raise ValueError("limit must be an int between 1 and 50000.")
    if not isinstance(timeout, int) or timeout <= 0:
        raise ValueError("timeout must be a positive integer (seconds).")
    if not start_date or not end_date:
        raise ValueError("start_date and end_date are required.")
    if pd.to_datetime(start_date) > pd.to_datetime(end_date):
        raise ValueError("start_date must be on or before end_date.")

    key = api_key or os.environ.get("POLYGON_API_KEY")
    if not key:
        raise ValueError(
            "Polygon API key is required. Pass api_key= or set "
            "POLYGON_API_KEY in the environment."
        )

    base = "https://api.polygon.io"
    url: Optional[str] = (
        f"{base}/v2/aggs/ticker/{ticker.upper()}/range/{multiplier}/{timespan}/"
        f"{start_date}/{end_date}"
    )
    params: dict = {
        "adjusted": "true" if adjusted else "false",
        "sort": "asc",
        "limit": limit,
        "apiKey": key,
    }

    rows: list = []
    while url is not None:
        try:
            resp = requests.get(url, params=params, timeout=timeout)
            resp.raise_for_status()
        except Exception as exc:
            raise ValueError(
                f"Failed to fetch Polygon aggregates for {ticker!r}."
            ) from exc

        payload = resp.json()
        status = payload.get("status")
        if status not in ("OK", "DELAYED"):
            err = payload.get("error") or payload.get("message") or status
            raise ValueError(f"Polygon returned status={status!r}: {err}")

        rows.extend(payload.get("results") or [])

        next_url = payload.get("next_url")
        if next_url:
            url = next_url
            params = {"apiKey": key}
        else:
            url = None

    if not rows:
        raise ValueError(f"No aggregate data returned for {ticker!r}.")

    df = pd.DataFrame(rows)
    rename = {
        "t": "timestamp",
        "o": "open",
        "h": "high",
        "l": "low",
        "c": "close",
        "v": "volume",
        "vw": "vwap",
        "n": "transactions",
    }
    df = df.rename(columns={k: v for k, v in rename.items() if k in df.columns})
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True).dt.tz_convert(None)
    df = df.drop_duplicates(subset=["timestamp"]).set_index("timestamp").sort_index()
    df.index.name = "date"
    keep = [c for c in ["open", "high", "low", "close", "volume", "vwap", "transactions"] if c in df.columns]
    return df[keep]
