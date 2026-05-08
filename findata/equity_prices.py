"""
findata.equity_prices
---------------------
Wrapper around yfinance for fetching historical OHLCV equity price data.
"""

from __future__ import annotations

import logging
import re
from typing import Iterable, List, Optional, Union

import pandas as pd


# Historical-roster delisting suffix: ``XL-201809`` means the symbol XL was
# in the index until 2018-09. Some S&P 500 archive CSVs annotate every
# delisted name this way. yfinance has no idea what to do with it — strip
# it before sending. Conservative pattern: ONLY a trailing dash followed
# by exactly 6 digits, so legit hyphenated tickers (BRK-B, RDS-A, etc.)
# survive unchanged.
_DELISTING_SUFFIX_RE = re.compile(r"-(\d{6})$")


def _normalize_tickers(raw: Union[str, Iterable[str], None]) -> List[str]:
    """Coerce raw input into a clean, unique list of yfinance ticker symbols.

    Accepts:
      - ``"AAPL"``                       — single string
      - ``"AAPL,MSFT,GOOG"``             — comma-separated string
      - ``"AAPL MSFT GOOG"``             — whitespace-separated string
      - ``"AAPL, MSFT,  GOOG"``          — mixed delimiters / whitespace
      - ``["AAPL", "MSFT"]``             — list / tuple / iterable of strings
      - mixed nested strings (``["AAPL,MSFT", "GOOG"]``) — flattened too

    Strips trailing ``-YYYYMM`` historical-roster delisting suffixes and
    emits a single INFO log line per call when any were found, so a caller
    that passed a S&P 500 archive dump sees what was scrubbed without
    being flooded with one log line per ticker.

    De-dupes while preserving first-seen order. Returns ``[]`` for None /
    empty / all-blank input — the public function still raises ValueError
    on empty so the caller knows the request was malformed.
    """
    if raw is None:
        return []

    if isinstance(raw, str):
        tokens: list[str] = re.split(r"[\s,]+", raw)
    else:
        tokens = []
        for x in raw:
            if x is None:
                continue
            tokens.extend(re.split(r"[\s,]+", str(x)))

    out: list[str] = []
    seen: set[str] = set()
    stripped: list[tuple[str, str]] = []
    for t in tokens:
        t = t.strip().upper()
        if not t:
            continue
        m = _DELISTING_SUFFIX_RE.search(t)
        if m:
            cleaned = t[: m.start()]
            stripped.append((t, cleaned))
            t = cleaned
        if not t or t in seen:
            continue
        seen.add(t)
        out.append(t)

    if stripped:
        sample = ", ".join(f"{orig}→{clean}" for orig, clean in stripped[:5])
        more = f" (+{len(stripped) - 5} more)" if len(stripped) > 5 else ""
        logging.info(
            "equity_prices: stripped %d historical-roster delisting "
            "suffixes (-YYYYMM): %s%s",
            len(stripped), sample, more,
        )

    return out


def get_equity_prices(
    tickers: Union[List[str], str],
    start_date: str,
    end_date: str,
    fields: Optional[List[str]] = None,
    frequency: str = "1d",
    auto_adjust: bool = True,
) -> pd.DataFrame:
    """
    Fetch historical OHLCV price data for one or more equity tickers.

    Wraps ``yfinance.download`` with normalised column handling and input
    validation.  Returns a consistently shaped DataFrame regardless of
    whether one or many tickers are requested.

    Parameters
    ----------
    tickers : list[str]
        Ticker symbols, e.g. ``["AAPL", "MSFT", "GOOG"]``.
    start_date : str
        Inclusive start date, ``"YYYY-MM-DD"``.
    end_date : str
        Inclusive end date, ``"YYYY-MM-DD"``.
    fields : list[str] or None, optional
        OHLCV columns to keep.  Accepted values:
        ``"Open"``, ``"High"``, ``"Low"``, ``"Close"``, ``"Volume"``.
        ``None`` (default) returns all columns.
    frequency : str, optional
        Bar size passed to yfinance ``interval``.
        Supported: ``"1d"`` (default), ``"5d"``, ``"1wk"``, ``"1mo"``, ``"3mo"``.
    auto_adjust : bool, optional
        Adjust prices for splits and dividends (default ``True``).

    Returns
    -------
    pd.DataFrame
        Multiple tickers — ``pd.MultiIndex`` columns ``(field, ticker)``
        with a ``DatetimeIndex``.
        Single ticker   — flat column index (field names only) with a
        ``DatetimeIndex``.

    Raises
    ------
    ValueError
        If ``tickers`` is empty or ``frequency`` is not a supported value.
    ImportError
        If ``yfinance`` is not installed.

    Examples
    --------
    >>> from findata.equity_prices import get_equity_prices

    >>> # Single ticker — daily close only
    >>> df = get_equity_prices(
    ...     tickers=["AAPL"],
    ...     start_date="2024-01-01",
    ...     end_date="2024-12-31",
    ...     fields=["Close"],
    ... )
    >>> df.head()

    >>> # Multiple tickers — all OHLCV
    >>> df = get_equity_prices(
    ...     tickers=["AAPL", "MSFT", "NVDA"],
    ...     start_date="2023-01-01",
    ...     end_date="2024-01-01",
    ... )
    >>> close = df["Close"]           # DataFrame: rows=dates, cols=tickers
    >>> aapl = df["Close"]["AAPL"]    # Series

    >>> # Weekly bars
    >>> df = get_equity_prices(
    ...     tickers=["SPY"],
    ...     start_date="2020-01-01",
    ...     end_date="2024-01-01",
    ...     fields=["Close"],
    ...     frequency="1wk",
    ... )
    """
    try:
        import yfinance as yf
    except ImportError as exc:
        raise ImportError(
            "yfinance is required.  Install: pip install yfinance"
        ) from exc

    # Coerce/clean the input. The original signature was List[str] but
    # callers (and LLM-generated notebook code) routinely pass CSV strings
    # or S&P 500 archive dumps with -YYYYMM delisting suffixes. yfinance
    # treats a CSV string as ONE symbol and returns "No data found, symbol
    # may be delisted" for the entire blob, which is exactly the failure
    # mode that masked this bug for so long.
    tickers = _normalize_tickers(tickers)
    if not tickers:
        raise ValueError(
            "tickers must be a non-empty list (or comma/space-separated string)."
        )

    _VALID_FREQ = {"1d", "5d", "1wk", "1mo", "3mo"}
    if frequency not in _VALID_FREQ:
        raise ValueError(
            f"frequency={frequency!r} is not supported. "
            f"Choose from: {sorted(_VALID_FREQ)}"
        )

    # yfinance >= 0.2.38 dropped group_by in favour of multi_level_index.
    # We request multi_level_index=True so multi-ticker downloads always return
    # a MultiIndex, then flatten for single-ticker calls below.
    raw: pd.DataFrame = yf.download(
        tickers=tickers,
        start=start_date,
        end=end_date,
        interval=frequency,
        auto_adjust=auto_adjust,
        progress=False,
        multi_level_index=True,
    )

    if isinstance(raw.columns, pd.MultiIndex):
        if fields:
            raw = raw.loc[:, raw.columns.get_level_values(0).isin(fields)]
    else:
        # Older yfinance or single-ticker with flat cols
        if fields:
            raw = raw[[c for c in fields if c in raw.columns]]

    # Flatten MultiIndex for a single ticker so callers get df["Close"] not df["Close"]["AAPL"]
    if len(tickers) == 1 and isinstance(raw.columns, pd.MultiIndex):
        raw.columns = raw.columns.get_level_values(0)

    return raw
