"""
findata.sp500_composition
-------------------------
Point-in-time S&P 500 index composition using the fja05680/sp500 GitHub dataset.

The dataset is cloned once to a local cache directory via git and read from
disk on every subsequent call.  No network request is made after the initial
clone unless you call ``refresh_sp500_cache()``.

Cache location (default): ``~/.cache/findata/sp500/``
Override via env var:      ``FINDATA_CACHE_DIR=/your/path``

Requires git to be available on the system PATH.
"""

from __future__ import annotations

import os
import subprocess
from datetime import date, datetime
from functools import lru_cache
from pathlib import Path
from typing import List, Union

import pandas as pd

_REPO_URL = "https://github.com/fja05680/sp500.git"
_CSV_FILENAME = "S&P 500 Historical Components & Changes.csv"

DateLike = Union[str, date, datetime]


def _cache_dir() -> Path:
    """Return the path to the local sp500 repo clone."""
    base = os.environ.get("FINDATA_CACHE_DIR", str(Path.home() / ".cache" / "findata"))
    return Path(base) / "sp500"


def _ensure_repo() -> Path:
    """
    Clone the sp500 repo into the local cache on first call.
    Subsequent calls return immediately if the clone already exists.

    Returns
    -------
    pathlib.Path
        Path to the root of the local repo clone.

    Raises
    ------
    RuntimeError
        If ``git clone`` fails (git not on PATH, no network, etc.).
    """
    repo_path = _cache_dir()
    if (repo_path / ".git").exists():
        return repo_path

    repo_path.mkdir(parents=True, exist_ok=True)
    result = subprocess.run(
        ["git", "clone", "--depth", "1", _REPO_URL, str(repo_path)],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Failed to clone sp500 repo.\n"
            f"stderr: {result.stderr.strip()}\n"
            "Ensure git is installed and https://github.com is reachable."
        )
    return repo_path


def refresh_sp500_cache() -> None:
    """
    Pull the latest changes from the upstream sp500 GitHub repo.

    Runs ``git pull --ff-only`` inside the local clone and clears the
    in-process CSV cache so the next call to ``get_sp500_composition``
    re-reads the updated file from disk.

    Raises
    ------
    RuntimeError
        If ``git pull`` fails (no network, diverged history, etc.).

    Examples
    --------
    >>> from findata.sp500_composition import refresh_sp500_cache
    >>> refresh_sp500_cache()
    """
    repo_path = _ensure_repo()
    result = subprocess.run(
        ["git", "-C", str(repo_path), "pull", "--ff-only"],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"git pull failed.\nstderr: {result.stderr.strip()}"
        )
    _load_csv.cache_clear()


@lru_cache(maxsize=1)
def _load_csv() -> pd.DataFrame:
    """
    Read and parse the composition CSV from the local git clone.

    The result is cached in memory via ``lru_cache``.  The cache is
    automatically cleared when ``refresh_sp500_cache()`` is called.

    Returns
    -------
    pd.DataFrame
        Columns: ``date`` (Timestamp), ``tickers`` (list[str]).
        Sorted ascending by date.
    """
    repo_path = _ensure_repo()
    csv_path = repo_path / _CSV_FILENAME
    if not csv_path.exists():
        raise FileNotFoundError(
            f"Expected CSV not found at {csv_path}.\n"
            "Try calling refresh_sp500_cache() or deleting the cache "
            f"directory ({_cache_dir()}) so it can be re-cloned."
        )
    df = pd.read_csv(csv_path)
    df.columns = [c.strip().lower() for c in df.columns]
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date").reset_index(drop=True)
    # tickers column is a space-separated string in the CSV
    df["tickers"] = df["tickers"].str.split()
    return df


def get_sp500_composition(
    as_of_date: DateLike,
    return_dataframe: bool = False,
) -> Union[List[str], pd.DataFrame]:
    """
    Return the point-in-time S&P 500 composition for a given date.

    Looks up the most recent index snapshot on or before ``as_of_date``
    from the locally cached fja05680/sp500 dataset.  The repo is cloned
    automatically on first use to ``~/.cache/findata/sp500/`` (override
    with the ``FINDATA_CACHE_DIR`` environment variable).

    Parameters
    ----------
    as_of_date : str | datetime.date | datetime.datetime
        Target date.  Accepts ``"YYYY-MM-DD"`` strings, ``datetime.date``,
        or ``datetime.datetime`` objects.
    return_dataframe : bool, optional
        ``False`` (default) — returns ``list[str]`` of ticker symbols.
        ``True``            — returns a ``pd.DataFrame`` with column
        ``"ticker"`` and index set to the snapshot date.

    Returns
    -------
    list[str] or pd.DataFrame
        S&P 500 member tickers as of ``as_of_date``.

    Raises
    ------
    ValueError
        If ``as_of_date`` predates the earliest snapshot in the dataset.
    RuntimeError
        If the local repo clone fails (git not installed, no network on
        first run).
    FileNotFoundError
        If the expected CSV is missing from the local cache.

    Notes
    -----
    The CSV is read once per process and held in memory.  To force a
    re-read after a ``refresh_sp500_cache()`` call the in-memory cache is
    cleared automatically by that function.

    The cache directory can be overridden at any time via the environment
    variable ``FINDATA_CACHE_DIR`` before the first call.

    Examples
    --------
    >>> from findata.sp500_composition import get_sp500_composition

    >>> # Current (latest available) membership
    >>> members = get_sp500_composition("2024-12-31")
    >>> len(members)    # ~503
    >>> members[:5]     # ['AAPL', 'MSFT', 'NVDA', ...]

    >>> # Historical point-in-time
    >>> members_2010 = get_sp500_composition("2010-06-30")
    >>> "GE" in members_2010    # True

    >>> # As a DataFrame (index = snapshot date, column = "ticker")
    >>> df = get_sp500_composition("2023-01-15", return_dataframe=True)

    >>> # Combine with equity prices — point-in-time universe
    >>> from findata.equity_prices import get_equity_prices
    >>> tickers = get_sp500_composition("2020-01-31")
    >>> prices = get_equity_prices(tickers, "2020-02-01", "2020-06-30", fields=["Close"])

    >>> # Keep the local cache current
    >>> from findata.sp500_composition import refresh_sp500_cache
    >>> refresh_sp500_cache()
    """
    ts = pd.Timestamp(as_of_date)
    df = _load_csv()

    mask = df["date"] <= ts
    if not mask.any():
        raise ValueError(
            f"as_of_date {ts.date()} is earlier than the first snapshot "
            f"in the dataset ({df['date'].iloc[0].date()})."
        )

    snapshot = df[mask].iloc[-1]
    tickers: List[str] = snapshot["tickers"]

    if return_dataframe:
        return pd.DataFrame(
            {"ticker": tickers},
            index=[snapshot["date"]] * len(tickers),
        )
    return tickers
