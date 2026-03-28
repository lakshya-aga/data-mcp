"""
findata.coingecko
-----------------
CoinGecko public API wrapper for crypto OHLCV data.
"""

from __future__ import annotations

def get_coingecko_ohlcv(
    coin_id: str,
    vs_currency: str = "usd",
    days: int | str = 30,
    timeout: int = 30,
) -> "pd.DataFrame":
    """
    Fetch crypto OHLCV candles from the CoinGecko public API.

    Parameters
    ----------
    coin_id : str
        CoinGecko coin id, e.g. ``"bitcoin"`` or ``"ethereum"``.
    vs_currency : str, optional
        Quote currency, e.g. ``"usd"`` (default).
    days : int or str, optional
        Number of days of data to return (e.g. ``1``, ``7``, ``30``),
        or ``"max"`` for full history. Default is ``30``.
    timeout : int, optional
        Request timeout in seconds (default ``30``).

    Returns
    -------
    pd.DataFrame
        DatetimeIndex rows, columns: ``open``, ``high``, ``low``, ``close``,
        ``volume``. Timestamps are in UTC with timezone removed.

    Raises
    ------
    ValueError
        If inputs are invalid or no data is returned.
    ImportError
        If a required package is not installed.

    Examples
    --------
    >>> from findata.coingecko import get_coingecko_ohlcv
    >>> df = get_coingecko_ohlcv("bitcoin", vs_currency="usd", days=90)
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

    if not isinstance(coin_id, str) or not coin_id.strip():
        raise ValueError("coin_id must be a non-empty string (e.g. 'bitcoin').")

    if not isinstance(vs_currency, str) or not vs_currency.strip():
        raise ValueError("vs_currency must be a non-empty string (e.g. 'usd').")

    if isinstance(days, bool):
        raise ValueError("days must be a positive int or the string 'max'.")

    if isinstance(days, int):
        if days <= 0:
            raise ValueError("days must be a positive integer.")
    elif isinstance(days, str):
        days = days.strip().lower()
        if days != "max":
            raise ValueError("days must be a positive int or the string 'max'.")
    else:
        raise ValueError("days must be a positive int or the string 'max'.")

    if not isinstance(timeout, int) or timeout <= 0:
        raise ValueError("timeout must be a positive integer (seconds).")

    base_url = "https://api.coingecko.com/api/v3"
    ohlc_url = f"{base_url}/coins/{coin_id}/ohlc"
    chart_url = f"{base_url}/coins/{coin_id}/market_chart"

    params = {"vs_currency": vs_currency, "days": days}

    ohlc_resp = requests.get(ohlc_url, params=params, timeout=timeout)
    ohlc_resp.raise_for_status()
    ohlc_data = ohlc_resp.json()

    if not isinstance(ohlc_data, list) or not ohlc_data:
        raise ValueError("No OHLC data returned from CoinGecko for the request.")

    chart_resp = requests.get(chart_url, params=params, timeout=timeout)
    chart_resp.raise_for_status()
    chart_data = chart_resp.json()

    volumes = chart_data.get("total_volumes", []) if isinstance(chart_data, dict) else []
    volume_map = {int(ts): vol for ts, vol in volumes if isinstance(ts, (int, float))}

    df = pd.DataFrame(
        ohlc_data,
        columns=["timestamp", "open", "high", "low", "close"],
    )
    df["volume"] = df["timestamp"].map(volume_map)

    ts = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    df.index = ts.dt.tz_convert(None)
    df.index.name = "date"
    df.drop(columns=["timestamp"], inplace=True)

    return df
