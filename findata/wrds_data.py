"""
findata.wrds_data
-----------------
WRDS (Wharton Research Data Services) wrapper for CRSP, Compustat, and other
research databases. Free for users with university affiliations.

Authentication is handled by the ``wrds`` Python package via the user's
``~/.pgpass`` file, the ``WRDS_USERNAME`` / ``WRDS_PASSWORD`` environment
variables, or the interactive prompt on first connection.
"""

from __future__ import annotations

import os
from typing import Optional


def get_wrds_data(
    library: str,
    table: str,
    columns: Optional[list] = None,
    where: Optional[str] = None,
    date_column: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: Optional[int] = None,
    wrds_username: Optional[str] = None,
) -> "pd.DataFrame":
    """
    Query a CRSP / Compustat / WRDS table and return the result as a DataFrame.

    Thin wrapper around :py:meth:`wrds.Connection.raw_sql` that builds a safe
    parameterised SELECT against ``{library}.{table}`` and applies optional
    column projection, ``WHERE`` filter, date range, and ``LIMIT``.

    Parameters
    ----------
    library : str
        WRDS library / schema, e.g. ``"crsp"``, ``"comp"``, ``"crspq"``,
        ``"wrdsapps"``.
    table : str
        Table name within the library, e.g. ``"dsf"`` (CRSP daily stock file),
        ``"funda"`` (Compustat fundamentals annual), ``"msf"`` (CRSP monthly).
    columns : list[str] or None, optional
        Columns to project. ``None`` (default) selects ``*``.
    where : str or None, optional
        Raw SQL ``WHERE`` clause body (without the ``WHERE`` keyword), e.g.
        ``"permno in (10107, 14593)"``. Use this for filters that the helper
        date range can't express.
    date_column : str or None, optional
        Name of the date column to filter on (e.g. ``"date"`` for ``crsp.dsf``,
        ``"datadate"`` for ``comp.funda``). Required if ``start_date`` /
        ``end_date`` is given.
    start_date : str or None, optional
        Inclusive lower bound on ``date_column``, ``"YYYY-MM-DD"``.
    end_date : str or None, optional
        Inclusive upper bound on ``date_column``, ``"YYYY-MM-DD"``.
    limit : int or None, optional
        Optional ``LIMIT`` on the result set.
    wrds_username : str or None, optional
        Override the WRDS username. Falls back to the ``WRDS_USERNAME``
        environment variable, then to whatever the ``wrds`` package picks up.

    Returns
    -------
    pd.DataFrame
        Result rows. If ``date_column`` is provided, the DataFrame is indexed
        by that column (parsed as datetime) and sorted ascending.

    Raises
    ------
    ValueError
        If inputs are invalid or the query returns nothing.
    ImportError
        If the ``wrds`` package is not installed.

    Examples
    --------
    >>> from findata.wrds_data import get_wrds_data
    >>> # CRSP daily stock file for AAPL (permno=14593)
    >>> df = get_wrds_data(
    ...     library="crsp",
    ...     table="dsf",
    ...     columns=["permno", "date", "prc", "ret", "vol"],
    ...     where="permno = 14593",
    ...     date_column="date",
    ...     start_date="2020-01-01",
    ...     end_date="2024-12-31",
    ... )

    >>> # Compustat annual fundamentals for a list of GVKEYs
    >>> df = get_wrds_data(
    ...     library="comp",
    ...     table="funda",
    ...     columns=["gvkey", "datadate", "at", "lt", "ni", "revt"],
    ...     where="gvkey in ('001690', '012141') and indfmt='INDL'",
    ...     date_column="datadate",
    ...     start_date="2010-01-01",
    ... )
    """
    try:
        import pandas as pd
    except ImportError as exc:
        raise ImportError(
            "pandas is required. Install: pip install pandas"
        ) from exc

    try:
        import wrds
    except ImportError as exc:
        raise ImportError(
            "wrds is required. Install: pip install wrds"
        ) from exc

    if not isinstance(library, str) or not library.strip().isidentifier():
        raise ValueError("library must be a SQL-safe identifier (letters/_/digits).")
    if not isinstance(table, str) or not table.strip().isidentifier():
        raise ValueError("table must be a SQL-safe identifier.")

    cols_sql = "*"
    if columns is not None:
        if not isinstance(columns, (list, tuple)) or not columns:
            raise ValueError("columns must be a non-empty list or None.")
        for c in columns:
            if not isinstance(c, str) or not c.strip().isidentifier():
                raise ValueError(f"Column name {c!r} is not a SQL-safe identifier.")
        cols_sql = ", ".join(columns)

    if (start_date or end_date) and not date_column:
        raise ValueError(
            "date_column is required when start_date or end_date is given."
        )
    if date_column is not None and not date_column.strip().isidentifier():
        raise ValueError("date_column must be a SQL-safe identifier.")

    start_dt = pd.to_datetime(start_date) if start_date else None
    end_dt = pd.to_datetime(end_date) if end_date else None
    if start_dt is not None and end_dt is not None and start_dt > end_dt:
        raise ValueError("start_date must be on or before end_date.")

    if limit is not None and (not isinstance(limit, int) or limit <= 0):
        raise ValueError("limit must be a positive integer or None.")

    clauses: list = []
    params: dict = {}
    if where:
        clauses.append(f"({where})")
    if start_dt is not None:
        clauses.append(f"{date_column} >= %(_start)s")
        params["_start"] = start_dt.date()
    if end_dt is not None:
        clauses.append(f"{date_column} <= %(_end)s")
        params["_end"] = end_dt.date()

    sql = f"SELECT {cols_sql} FROM {library}.{table}"
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    if date_column is not None:
        sql += f" ORDER BY {date_column}"
    if limit is not None:
        sql += f" LIMIT {int(limit)}"

    username = wrds_username or os.environ.get("WRDS_USERNAME")
    try:
        if username:
            db = wrds.Connection(wrds_username=username)
        else:
            db = wrds.Connection()
    except Exception as exc:
        raise ValueError(
            "Failed to connect to WRDS. Check credentials in ~/.pgpass or the "
            "WRDS_USERNAME / WRDS_PASSWORD environment variables."
        ) from exc

    try:
        df = db.raw_sql(sql, params=params)
    except Exception as exc:
        raise ValueError(f"WRDS query failed: {sql!r}") from exc
    finally:
        try:
            db.close()
        except Exception:
            pass

    if df is None or df.empty:
        raise ValueError("WRDS query returned no rows.")

    if date_column is not None and date_column in df.columns:
        df[date_column] = pd.to_datetime(df[date_column])
        df = df.set_index(date_column).sort_index()
        df.index.name = date_column

    return df
