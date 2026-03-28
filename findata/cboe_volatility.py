"""
findata.cboe_volatility
-----------------------
Yahoo Finance wrapper for CBOE volatility index data (e.g. VIX, VVIX).
"""

from __future__ import annotations

from typing import List, Optional, Sequence, TYPE_CHECKING

if TYPE_CHECKING:
    import pandas as pd


def get_cboe_volatility_indices(
    symbols: Optional[Sequence[str]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    fields: Optional[Sequence[str]] = None,
    interval: str = "1d",
    auto_adjust: bool = False,
) -> "pd.DataFrame":
    """
    Fetch historical CBOE volatility index data via Yahoo Finance.

    Parameters
    ----------
    symbols : sequence[str] or None, optional
        Yahoo Finance tickers for CBOE volatility indices, e.g.
        ``["^VIX", "^VVIX", "^VXN", "^VXD"]``. Defaults to ``["^VIX"]``.
    start_date : str or None, optional
        Inclusive start date, ``"YYYY-MM-DD"``. ``None`` lets Yahoo decide.
    end_date : str or None, optional
        Inclusive end date, ``"YYYY-MM-DD"``. ``None`` lets Yahoo decide.
    fields : sequence[str] or None, optional
        OHLCV columns to keep. Accepted values:
        ``"Open"``, ``"High"``, ``"Low"``, ``"Close"``, ``"Adj Close"``,
        ``"Volume"``. ``None`` (default) returns all columns.
    interval : str, optional
        Bar size passed to yfinance ``interval``.
        Supported: ``"1d"`` (default), ``"5d"``, ``"1wk"``, ``"1mo"``,
        ``"3mo"``.
    auto_adjust : bool, optional
        Adjust prices for splits and dividends (default ``False``).

    Returns
    -------
    pd.DataFrame
        Multiple symbols — ``pd.MultiIndex`` columns ``(field, symbol)``
        with a ``DatetimeIndex``.
        Single symbol     — flat column index (field names only) with a
        ``DatetimeIndex``.

    Raises
    ------
    ValueError
        If inputs are invalid or no data is returned.
    ImportError
        If a required package is not installed.

    Examples
    --------
    >>> from findata.cboe_volatility import get_cboe_volatility_indices
    >>> df = get_cboe_volatility_indices(
    ...     symbols=["^VIX", "^VVIX"],
    ...     start_date="2020-01-01",
    ...     end_date="2024-12-31",
    ... )
    """
    try:
        import pandas as pd
    except ImportError as exc:
        raise ImportError(
            "pandas is required. Install: pip install pandas"
        ) from exc

    try:
        import yfinance as yf
    except ImportError as exc:
        raise ImportError(
            "yfinance is required. Install: pip install yfinance"
        ) from exc

    if symbols is None:
        symbols_list = ["^VIX"]
    elif isinstance(symbols, str):
        symbols_list = [symbols]
    else:
        symbols_list = list(symbols)

    if not symbols_list:
        raise ValueError("symbols must be a non-empty sequence.")
    if any(not isinstance(sym, str) or not sym.strip() for sym in symbols_list):
        raise ValueError("symbols must contain non-empty strings only.")

    _VALID_INTERVALS = {"1d", "5d", "1wk", "1mo", "3mo"}
    if interval not in _VALID_INTERVALS:
        raise ValueError(
            f"interval={interval!r} is not supported. "
            f"Choose from: {sorted(_VALID_INTERVALS)}"
        )

    _VALID_FIELDS = {"Open", "High", "Low", "Close", "Adj Close", "Volume"}
    if fields is not None:
        fields_list = list(fields)
        if not fields_list:
            raise ValueError("fields must be a non-empty sequence when provided.")
        invalid = [f for f in fields_list if f not in _VALID_FIELDS]
        if invalid:
            raise ValueError(
                "fields contains unsupported values: "
                f"{invalid}. Supported: {sorted(_VALID_FIELDS)}"
            )
    else:
        fields_list = None

    if start_date is not None:
        try:
            start_ts = pd.to_datetime(start_date)
        except (TypeError, ValueError) as exc:
            raise ValueError("start_date must be a valid date string.") from exc
    else:
        start_ts = None

    if end_date is not None:
        try:
            end_ts = pd.to_datetime(end_date)
        except (TypeError, ValueError) as exc:
            raise ValueError("end_date must be a valid date string.") from exc
    else:
        end_ts = None

    if start_ts is not None and end_ts is not None and start_ts > end_ts:
        raise ValueError("start_date must be on or before end_date.")

    raw: pd.DataFrame = yf.download(
        tickers=symbols_list,
        start=start_date,
        end=end_date,
        interval=interval,
        auto_adjust=auto_adjust,
        progress=False,
        multi_level_index=True,
    )

    if raw.empty:
        raise ValueError(
            "No data returned. Check symbols and date range."
        )

    if fields_list is not None:
        raw = raw[fields_list]

    if raw.columns.nlevels == 2 and len(symbols_list) == 1:
        raw.columns = raw.columns.get_level_values(0)

    if not isinstance(raw.index, pd.DatetimeIndex):
        raw.index = pd.to_datetime(raw.index)

    if raw.index.tz is not None:
        raw.index = raw.index.tz_localize(None)

    return raw
