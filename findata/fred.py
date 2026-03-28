"""
findata.fred
------------
FRED macroeconomic time series via the fredapi client.
"""

from __future__ import annotations

from typing import List, Optional


def get_fred_series(
    series_ids: List[str],
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    api_key: Optional[str] = None,
) -> "pd.DataFrame":
    """
    Fetch one or more FRED time series.

    Parameters
    ----------
    series_ids : list of str
        One or more FRED series identifiers (e.g., "CPIAUCSL").
    start_date : str or None, optional
        Inclusive start date, "YYYY-MM-DD". "None" returns full history.
    end_date : str or None, optional
        Inclusive end date, "YYYY-MM-DD". "None" returns full history.
    api_key : str or None, optional
        FRED API key. If "None", fredapi will fall back to the
        ``FRED_API_KEY`` environment variable.

    Returns
    -------
    pd.DataFrame
        DatetimeIndex rows with one column per series id.

    Raises
    ------
    ValueError
        If inputs are invalid or data cannot be fetched.
    ImportError
        If a required package is not installed.

    Examples
    --------
    >>> from findata.fred import get_fred_series
    >>> df = get_fred_series(["CPIAUCSL", "UNRATE"], start_date="2015-01-01")
    """
    try:
        import pandas as pd
    except ImportError as exc:
        raise ImportError(
            "pandas is required. Install: pip install pandas"
        ) from exc

    try:
        from fredapi import Fred
    except ImportError as exc:
        raise ImportError(
            "fredapi is required. Install: pip install fredapi"
        ) from exc

    if not isinstance(series_ids, (list, tuple)) or not series_ids:
        raise ValueError("series_ids must be a non-empty list of strings.")

    cleaned_ids: List[str] = []
    for series_id in series_ids:
        if not isinstance(series_id, str) or not series_id.strip():
            raise ValueError("Each series_id must be a non-empty string.")
        cleaned_ids.append(series_id.strip())

    start_dt = None
    end_dt = None

    if start_date is not None:
        try:
            start_dt = pd.to_datetime(start_date)
        except Exception as exc:
            raise ValueError("start_date must be a valid date string.") from exc

    if end_date is not None:
        try:
            end_dt = pd.to_datetime(end_date)
        except Exception as exc:
            raise ValueError("end_date must be a valid date string.") from exc

    if start_dt is not None and end_dt is not None and start_dt > end_dt:
        raise ValueError("start_date must be on or before end_date.")

    try:
        fred = Fred(api_key=api_key)
    except Exception as exc:
        raise ValueError("Failed to initialize fredapi client.") from exc

    series_list = []
    for series_id in cleaned_ids:
        try:
            s = fred.get_series(
                series_id,
                observation_start=start_dt,
                observation_end=end_dt,
            )
        except Exception as exc:
            raise ValueError(
                f"Failed to fetch FRED series {series_id!r}."
            ) from exc

        if s is None or s.empty:
            raise ValueError(f"No data returned for series {series_id!r}.")

        if not isinstance(s.index, pd.DatetimeIndex):
            try:
                s.index = pd.to_datetime(s.index)
            except Exception as exc:
                raise ValueError(
                    f"Failed to parse dates for series {series_id!r}."
                ) from exc

        s.name = series_id
        series_list.append(s)

    df = pd.concat(series_list, axis=1)
    if df.empty:
        raise ValueError("No data returned for requested series.")

    return df
