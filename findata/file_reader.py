"""
findata.file_reader
-------------------
Load financial time-series data from local flat files.
Supports CSV, Parquet, and Excel.
"""

from __future__ import annotations

from pathlib import Path
from typing import List, Optional, Union

import pandas as pd


def get_file_data(
    filepath: Union[str, Path],
    tickers: Optional[List[str]] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    date_column: str = "date",
    ticker_column: str = "ticker",
    fields: Optional[List[str]] = None,
    file_format: Optional[str] = None,
) -> pd.DataFrame:
    """
    Load financial time-series data from a local flat file.

    Reads a file into a ``pd.DataFrame``, parses the date column into a
    ``DatetimeIndex``, and applies optional filters for tickers, date
    range, and field selection.

    Parameters
    ----------
    filepath : str | pathlib.Path
        Path to the data file.  Relative or absolute.
    tickers : list[str] or None, optional
        Keep only rows where ``ticker_column`` matches one of these values.
        ``None`` (default) returns all tickers.
    start_date : str or None, optional
        Inclusive start-date filter, ``"YYYY-MM-DD"``.
    end_date : str or None, optional
        Inclusive end-date filter, ``"YYYY-MM-DD"``.
    date_column : str, optional
        Name of the date column in the file (default ``"date"``).
    ticker_column : str, optional
        Name of the ticker/symbol column in the file (default ``"ticker"``).
    fields : list[str] or None, optional
        Data columns to return.  Index columns are always included.
        ``None`` returns all columns.
    file_format : str or None, optional
        Force format: ``"csv"``, ``"parquet"``, or ``"excel"``.
        Inferred from file extension when ``None``.

    Returns
    -------
    pd.DataFrame
        Filtered data with ``date_column`` parsed as a ``DatetimeIndex``.

    Raises
    ------
    FileNotFoundError
        If ``filepath`` does not exist.
    ValueError
        If the file format cannot be inferred or is not supported.

    Examples
    --------
    >>> from findata.file_reader import get_file_data

    >>> # Load an entire CSV
    >>> df = get_file_data("data/prices.csv")

    >>> # Parquet — filtered to two tickers and a date range
    >>> df = get_file_data(
    ...     "data/prices.parquet",
    ...     tickers=["AAPL", "MSFT"],
    ...     start_date="2023-01-01",
    ...     end_date="2023-12-31",
    ...     fields=["close", "volume"],
    ... )

    >>> # Excel with non-standard column names
    >>> df = get_file_data(
    ...     "data/export.xlsx",
    ...     date_column="Date",
    ...     ticker_column="Symbol",
    ...     file_format="excel",
    ... )
    """
    filepath = Path(filepath)
    if not filepath.exists():
        raise FileNotFoundError(f"File not found: {filepath}")

    # ---- infer format -------------------------------------------------- #
    if file_format is None:
        _EXT_MAP = {
            ".csv": "csv",
            ".parquet": "parquet",
            ".pq": "parquet",
            ".xlsx": "excel",
            ".xls": "excel",
        }
        suffix = filepath.suffix.lower()
        if suffix not in _EXT_MAP:
            raise ValueError(
                f"Cannot infer file format from extension {suffix!r}. "
                "Pass file_format='csv'|'parquet'|'excel' explicitly."
            )
        file_format = _EXT_MAP[suffix]

    # ---- load ---------------------------------------------------------- #
    # parse_dates is deprecated in pandas ≥ 2.0; use pd.to_datetime post-load.
    if file_format == "csv":
        df = pd.read_csv(filepath)
    elif file_format == "parquet":
        df = pd.read_parquet(filepath)
    elif file_format == "excel":
        df = pd.read_excel(filepath)
    else:
        raise ValueError(f"Unsupported file_format={file_format!r}.")

    # ---- parse date column --------------------------------------------- #
    if date_column in df.columns:
        df[date_column] = pd.to_datetime(df[date_column])

    # ---- filter by ticker ---------------------------------------------- #
    if ticker_column in df.columns and tickers is not None:
        df = df[df[ticker_column].isin(tickers)]

    # ---- set DatetimeIndex and filter by date range -------------------- #
    if date_column in df.columns:
        df = df.set_index(date_column).sort_index()
        if start_date:
            df = df[df.index >= pd.Timestamp(start_date)]
        if end_date:
            df = df[df.index <= pd.Timestamp(end_date)]

    # ---- column selection ---------------------------------------------- #
    if fields:
        keep = [c for c in fields if c in df.columns]
        if ticker_column in df.columns and ticker_column not in keep:
            keep = [ticker_column] + keep
        df = df[keep]

    return df
