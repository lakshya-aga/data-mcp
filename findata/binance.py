"""
findata.binance
---------------
Binance public REST API wrapper for crypto OHLCV (klines) data.

The Spot Klines endpoint (``GET /api/v3/klines``) is anonymous — no API key
required for read access — and ships up to 1000 candles per request. We
paginate transparently when the user asks for a larger range.

Reference:
    https://developers.binance.com/docs/binance-spot-api-docs/rest-api/market-data-endpoints
"""

from __future__ import annotations


_VALID_INTERVALS = {
    "1s", "1m", "3m", "5m", "15m", "30m",
    "1h", "2h", "4h", "6h", "8h", "12h",
    "1d", "3d", "1w", "1M",
}


def get_binance_ohlcv(
    symbol: str,
    interval: str = "1d",
    start_date: str | None = None,
    end_date: str | None = None,
    limit: int = 1000,
    timeout: int = 30,
    base_url: str = "https://api.binance.com",
) -> "pd.DataFrame":
    """
    Fetch crypto OHLCV candles from Binance's public Spot Klines API.

    Parameters
    ----------
    symbol : str
        Trading pair in Binance format, e.g. ``"BTCUSDT"`` or ``"ETHUSDT"``.
    interval : str, optional
        Candle interval. One of ``1s, 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h,
        8h, 12h, 1d, 3d, 1w, 1M``. Default ``"1d"``.
    start_date : str or None, optional
        Inclusive start date ``"YYYY-MM-DD"`` (UTC). If ``None``, the API
        returns the most recent ``limit`` candles.
    end_date : str or None, optional
        Inclusive end date ``"YYYY-MM-DD"`` (UTC). If ``None``, the request
        runs up to "now".
    limit : int, optional
        Per-request page size, 1..1000 (default 1000). When ``start_date`` /
        ``end_date`` span more than this, pagination loops automatically.
    timeout : int, optional
        Per-request timeout in seconds (default 30).
    base_url : str, optional
        Override the API host. Defaults to ``api.binance.com``; use
        ``api.binance.us`` for the US-restricted endpoint.

    Returns
    -------
    pd.DataFrame
        DatetimeIndex (UTC, tz-naive, named ``date``) of candle close times,
        with columns ``open``, ``high``, ``low``, ``close``, ``volume``,
        ``quote_volume``, ``trades``. All numeric columns are float64
        except ``trades`` which is int64.

    Raises
    ------
    ValueError
        If inputs are invalid or the API returns no rows.
    ImportError
        If a required package is not installed.

    Examples
    --------
    >>> from findata.binance import get_binance_ohlcv
    >>> df = get_binance_ohlcv("BTCUSDT", interval="1d", limit=10)
    >>> df.tail()
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

    # ── input validation ────────────────────────────────────────────────
    if not isinstance(symbol, str) or not symbol.strip():
        raise ValueError("symbol must be a non-empty string (e.g. 'BTCUSDT').")
    symbol = symbol.strip().upper()

    if not isinstance(interval, str) or interval not in _VALID_INTERVALS:
        raise ValueError(
            f"interval must be one of {sorted(_VALID_INTERVALS)}; got {interval!r}."
        )

    if not isinstance(limit, int) or not (1 <= limit <= 1000):
        raise ValueError("limit must be an int in [1, 1000].")

    if not isinstance(timeout, int) or timeout <= 0:
        raise ValueError("timeout must be a positive integer (seconds).")

    def _to_ms(date_str: str | None) -> int | None:
        if date_str is None:
            return None
        ts = pd.Timestamp(date_str, tz="UTC")
        if pd.isna(ts):
            raise ValueError(f"could not parse date: {date_str!r}")
        return int(ts.value // 1_000_000)

    start_ms = _to_ms(start_date)
    end_ms = _to_ms(end_date)
    if start_ms is not None and end_ms is not None and end_ms < start_ms:
        raise ValueError("end_date must be >= start_date.")

    # ── pagination loop ────────────────────────────────────────────────
    url = f"{base_url.rstrip('/')}/api/v3/klines"
    rows: list[list] = []
    cursor = start_ms

    while True:
        params: dict[str, str | int] = {
            "symbol": symbol,
            "interval": interval,
            "limit": limit,
        }
        if cursor is not None:
            params["startTime"] = cursor
        if end_ms is not None:
            params["endTime"] = end_ms

        resp = requests.get(url, params=params, timeout=timeout)
        resp.raise_for_status()
        page = resp.json()
        if not isinstance(page, list):
            raise ValueError(f"unexpected Binance response: {page!r}")
        if not page:
            break
        rows.extend(page)

        # Continue paginating only if the user asked for a date range and
        # the page filled to the limit (i.e. there are likely more candles).
        if start_ms is None or len(page) < limit:
            break
        last_open_time = int(page[-1][0])
        next_cursor = last_open_time + 1
        if next_cursor == cursor:  # safety: avoid infinite loops
            break
        cursor = next_cursor

    if not rows:
        raise ValueError(
            f"no candles returned for symbol={symbol!r} interval={interval!r} "
            f"start={start_date!r} end={end_date!r}."
        )

    # ── shape the frame ────────────────────────────────────────────────
    # Klines columns per Binance docs:
    # [open_time, open, high, low, close, volume, close_time, quote_volume,
    #  trades, taker_buy_base, taker_buy_quote, ignore]
    df = pd.DataFrame(
        rows,
        columns=[
            "open_time", "open", "high", "low", "close", "volume",
            "close_time", "quote_volume", "trades",
            "taker_buy_base", "taker_buy_quote", "_ignore",
        ],
    )

    # Dedup overlapping pages (Binance can repeat the boundary candle).
    df = df.drop_duplicates(subset="open_time", keep="last")

    # Cast numerics; trades is integer count, the rest are quote-currency floats.
    for col in ["open", "high", "low", "close", "volume", "quote_volume",
                "taker_buy_base", "taker_buy_quote"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["trades"] = pd.to_numeric(df["trades"], errors="coerce").astype("Int64")

    df.index = pd.to_datetime(df["close_time"], unit="ms", utc=True).dt.tz_convert(None)
    df.index.name = "date"
    df.drop(
        columns=["open_time", "close_time", "_ignore",
                 "taker_buy_base", "taker_buy_quote"],
        inplace=True,
    )
    df.sort_index(inplace=True)
    return df
