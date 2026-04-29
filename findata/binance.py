"""
findata.binance
---------------
Crypto OHLCV candles from the Binance public REST API. No API key required
for market data endpoints.
"""

from __future__ import annotations

from typing import Optional


_VALID_INTERVALS = {
    "1m", "3m", "5m", "15m", "30m",
    "1h", "2h", "4h", "6h", "8h", "12h",
    "1d", "3d", "1w", "1M",
}


def get_binance_klines(
    symbol: str,
    interval: str = "1d",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: int = 1000,
    timeout: int = 30,
) -> "pd.DataFrame":
    """
    Fetch OHLCV klines (candlesticks) from Binance.

    Wraps the ``GET /api/v3/klines`` public endpoint and pages through results
    automatically when a date range exceeds Binance's per-request limit
    (1000 candles).

    Parameters
    ----------
    symbol : str
        Trading pair, e.g. ``"BTCUSDT"``, ``"ETHUSDT"``. Binance uses no
        separator between base and quote.
    interval : str, optional
        Candle interval. Supported: ``"1m"``, ``"3m"``, ``"5m"``, ``"15m"``,
        ``"30m"``, ``"1h"``, ``"2h"``, ``"4h"``, ``"6h"``, ``"8h"``, ``"12h"``,
        ``"1d"`` (default), ``"3d"``, ``"1w"``, ``"1M"``.
    start_date : str or None, optional
        Inclusive lower bound, ``"YYYY-MM-DD"`` or any pandas-parseable
        timestamp.
    end_date : str or None, optional
        Inclusive upper bound, ``"YYYY-MM-DD"`` or any pandas-parseable
        timestamp.
    limit : int, optional
        Max candles per HTTP request (Binance hard cap is 1000). The function
        pages until ``end_date`` is reached.
    timeout : int, optional
        Request timeout in seconds (default ``30``).

    Returns
    -------
    pd.DataFrame
        DatetimeIndex (open time, UTC-naive) with columns:
        ``open``, ``high``, ``low``, ``close``, ``volume``,
        ``close_time``, ``quote_asset_volume``, ``num_trades``,
        ``taker_buy_base_volume``, ``taker_buy_quote_volume``.

    Raises
    ------
    ValueError
        If inputs are invalid or no data is returned.
    ImportError
        If a required package is not installed.

    Examples
    --------
    >>> from findata.binance import get_binance_klines
    >>> df = get_binance_klines("BTCUSDT", interval="1h",
    ...                         start_date="2024-01-01", end_date="2024-01-07")
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
        raise ValueError("symbol must be a non-empty string (e.g. 'BTCUSDT').")
    if interval not in _VALID_INTERVALS:
        raise ValueError(
            f"interval={interval!r} is not supported. "
            f"Choose from: {sorted(_VALID_INTERVALS)}"
        )
    if not isinstance(limit, int) or not (1 <= limit <= 1000):
        raise ValueError("limit must be an int between 1 and 1000.")
    if not isinstance(timeout, int) or timeout <= 0:
        raise ValueError("timeout must be a positive integer (seconds).")

    start_ms: Optional[int] = None
    end_ms: Optional[int] = None
    if start_date is not None:
        start_ms = int(pd.Timestamp(start_date, tz="UTC").timestamp() * 1000)
    if end_date is not None:
        end_ms = int(pd.Timestamp(end_date, tz="UTC").timestamp() * 1000)
    if start_ms is not None and end_ms is not None and start_ms > end_ms:
        raise ValueError("start_date must be on or before end_date.")

    url = "https://api.binance.com/api/v3/klines"
    rows: list = []
    cursor = start_ms

    while True:
        params: dict = {
            "symbol": symbol.upper(),
            "interval": interval,
            "limit": limit,
        }
        if cursor is not None:
            params["startTime"] = cursor
        if end_ms is not None:
            params["endTime"] = end_ms

        try:
            resp = requests.get(url, params=params, timeout=timeout)
            resp.raise_for_status()
        except Exception as exc:
            raise ValueError(
                f"Failed to fetch Binance klines for {symbol!r}."
            ) from exc

        batch = resp.json()
        if not isinstance(batch, list):
            raise ValueError(f"Unexpected response from Binance: {batch!r}")
        if not batch:
            break

        rows.extend(batch)

        if len(batch) < limit:
            break

        last_open = int(batch[-1][0])
        next_cursor = last_open + 1
        if cursor is not None and next_cursor <= cursor:
            break
        cursor = next_cursor
        if end_ms is not None and cursor > end_ms:
            break

    if not rows:
        raise ValueError(f"No kline data returned for {symbol!r}.")

    cols = [
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_asset_volume", "num_trades",
        "taker_buy_base_volume", "taker_buy_quote_volume", "ignore",
    ]
    df = pd.DataFrame(rows, columns=cols)
    df = df.drop(columns=["ignore"])

    numeric_cols = [
        "open", "high", "low", "close", "volume",
        "quote_asset_volume", "taker_buy_base_volume", "taker_buy_quote_volume",
    ]
    df[numeric_cols] = df[numeric_cols].astype(float)
    df["num_trades"] = df["num_trades"].astype("int64")

    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms", utc=True).dt.tz_convert(None)
    df["close_time"] = pd.to_datetime(df["close_time"], unit="ms", utc=True).dt.tz_convert(None)

    df = df.drop_duplicates(subset=["open_time"])
    df = df.set_index("open_time").sort_index()
    df.index.name = "date"
    return df
