"""
findata.fama_french
-------------------
Daily Fama-French factor returns from the Kenneth R. French Data Library.
"""

from __future__ import annotations

from typing import Optional


def get_fama_french_factors(
    factor_model: str = "3",
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    as_decimal: bool = True,
) -> "pd.DataFrame":
    """
    Fetch daily Fama-French factor returns (3- or 5-factor models).

    Parameters
    ----------
    factor_model : str, optional
        ``"3"`` for the 3-factor set or ``"5"`` for the 5-factor set.
    start_date : str or None, optional
        Inclusive start date, ``"YYYY-MM-DD"``. ``None`` returns all history.
    end_date : str or None, optional
        Inclusive end date, ``"YYYY-MM-DD"``. ``None`` returns all history.
    as_decimal : bool, optional
        If ``True`` (default), divide values by 100 to convert percentage
        returns to decimals.

    Returns
    -------
    pd.DataFrame
        DatetimeIndex rows with columns:
        3-factor: ``Mkt-RF``, ``SMB``, ``HML``, ``RF``.
        5-factor: ``Mkt-RF``, ``SMB``, ``HML``, ``RMW``, ``CMA``, ``RF``.

    Raises
    ------
    ValueError
        If ``factor_model`` is not ``"3"`` or ``"5"``, or if date inputs
        are invalid.
    ImportError
        If a required package is not installed.

    Examples
    --------
    >>> from findata.fama_french import get_fama_french_factors
    >>> df = get_fama_french_factors(factor_model="5", start_date="2020-01-01")
    """
    try:
        import pandas as pd
    except ImportError as exc:
        raise ImportError(
            "pandas is required. Install: pip install pandas"
        ) from exc

    try:
        from pandas_datareader import data as pdr_data
    except ImportError as exc:
        raise ImportError(
            "pandas-datareader is required. Install: pip install pandas-datareader"
        ) from exc

    model_key = str(factor_model).strip()
    dataset_map = {
        "3": "F-F_Research_Data_Factors_daily",
        "5": "F-F_Research_Data_5_Factors_2x3_daily",
    }
    if model_key not in dataset_map:
        raise ValueError(
            "factor_model must be '3' or '5'. "
            f"Received: {factor_model!r}"
        )

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
        raw = pdr_data.DataReader(dataset_map[model_key], "famafrench")
    except Exception as exc:
        raise ValueError(
            "Failed to download Fama-French data from the Ken French library."
        ) from exc

    if not isinstance(raw, dict) or 0 not in raw:
        raise ValueError(
            "Unexpected data format returned by pandas-datareader for famafrench."
        )

    df = raw[0].copy()
    if df.empty:
        raise ValueError("No data returned for the requested factor set.")

    try:
        df.index = pd.to_datetime(df.index, format="%Y%m%d")
    except Exception as exc:
        raise ValueError("Failed to parse dates in Fama-French data.") from exc

    df.index.name = "date"

    if start_dt is not None:
        df = df.loc[df.index >= start_dt]

    if end_dt is not None:
        df = df.loc[df.index <= end_dt]

    if as_decimal:
        df = df / 100.0

    return df
