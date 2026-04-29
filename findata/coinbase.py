"""
findata.coinbase
----------------
Crypto OHLCV candles from the Coinbase Exchange (Advanced Trade) public REST
API. No API key required for market data.
"""

from __future__ import annotations

from typing import Optional


_GRANULARITY_SECONDS = {
    "1m": 60,
    "5m": 300,
    "15m": 900,
    "1h": 3600,
    "6h": 21600,
    "1d": 86400,
}


def get_coinbase_candles(
    product_id: str,
    granularity: str = "1d",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    timeout: int = 30,
) -> "pd.DataFrame":
    """
    Fetch OHLCV candles from Coinbase Exchange.

    Wraps ``GET https://api.exchange.coinbase.com/products/{product_id}/candles``
    and pages through results so date ranges larger than Coinbase's 300-candle
    per-request cap return a complete history.

    Parameters
    ----------
    product_id : str
        Trading pair, e.g. ``"BTC-USD"``, ``"ETH-USD"``. Coinbase uses a hyphen
        between base and quote.
    granularity : str, optional
        Candle interval. Supported: ``"1m"``, ``"5m"``, ``"15m"``, ``"1h"``,
        ``"6h"``, ``"1d"`` (default).
    start_date : str or None, optional
        Inclusive lower bound, ``"YYYY-MM-DD"`` or any pandas-parseable
        timestamp. Required if ``end_date`` is given.
    end_date : str or None, optional
        Inclusive upper bound, ``"YYYY-MM-DD"`` or any pandas-parseable
        timestamp.
    timeout : int, optional
        Request timeout in seconds (default ``30``).

    Returns
    -------
    pd.DataFrame
        DatetimeIndex (candle open time, UTC-naive) with columns:
        ``low``, ``high``, ``open``, ``close``, ``volume``.

    Raises
    ------
    ValueError
        If inputs are invalid or no data is returned.
    ImportError
        If a required package is not installed.

    Examples
    --------
    >>> from findata.coinbase import get_coinbase_candles
    >>> df = get_coinbase_candles("BTC-USD", granularity="1h",
    ...                           start_date="2024-01-01", end_date="2024-01-07")
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

    if not isinstance(product_id, str) or not product_id.strip():
        raise ValueError("product_id must be a non-empty string (e.g. 'BTC-USD').")
    if granularity not in _GRANULARITY_SECONDS:
        raise ValueError(
            f"granularity={granularity!r} is not supported. "
            f"Choose from: {sorted(_GRANULARITY_SECONDS)}"
        )
    if not isinstance(timeout, int) or timeout <= 0:
        raise ValueError("timeout must be a positive integer (seconds).")

    gran_sec = _GRANULARITY_SECONDS[granularity]

    start_ts = pd.Timestamp(start_date, tz="UTC") if start_date else None
    end_ts = pd.Timestamp(end_date, tz="UTC") if end_date else None
    if start_ts is not None and end_ts is not None and start_ts > end_ts:
        raise ValueError("start_date must be on or before end_date.")

    url = f"https://api.exchange.coinbase.com/products/{product_id.upper()}/candles"

    if start_ts is None and end_ts is None:
        try:
            resp = requests.get(
                url,
                params={"granularity": gran_sec},
                headers={"Accept": "application/json"},
                timeout=timeout,
            )
            resp.raise_for_status()
        except Exception as exc:
            raise ValueError(
                f"Failed to fetch Coinbase candles for {product_id!r}."
            ) from exc
        rows = resp.json()
    else:
        if end_ts is None:
            end_ts = pd.Timestamp.utcnow().tz_convert("UTC")
        if start_ts is None:
            start_ts = end_ts - pd.Timedelta(seconds=gran_sec * 300)

        rows = []
        page_span = pd.Timedelta(seconds=gran_sec * 300)
        cursor = start_ts
        while cursor < end_ts:
            page_end = min(cursor + page_span, end_ts)
            params = {
                "granularity": gran_sec,
                "start": cursor.isoformat(),
                "end": page_end.isoformat(),
            }
            try:
                resp = requests.get(
                    url,
                    params=params,
                    headers={"Accept": "application/json"},
                    timeout=timeout,
                )
                resp.raise_for_status()
            except Exception as exc:
                raise ValueError(
                    f"Failed to fetch Coinbase candles for {product_id!r}."
                ) from exc

            batch = resp.json()
            if isinstance(batch, list):
                rows.extend(batch)
            cursor = page_end + pd.Timedelta(seconds=gran_sec)

    if not isinstance(rows, list) or not rows:
        raise ValueError(f"No candle data returned for {product_id!r}.")

    df = pd.DataFrame(
        rows,
        columns=["time", "low", "high", "open", "close", "volume"],
    )
    df = df.drop_duplicates(subset=["time"])
    df["time"] = pd.to_datetime(df["time"], unit="s", utc=True).dt.tz_convert(None)
    df = df.set_index("time").sort_index()
    df.index.name = "date"
    df = df.astype(float)

    if start_ts is not None:
        df = df[df.index >= start_ts.tz_convert(None)]
    if end_ts is not None:
        df = df[df.index <= end_ts.tz_convert(None)]

    if df.empty:
        raise ValueError(f"No candle data returned for {product_id!r}.")
    return df
