"""
findata.bloomberg
-----------------
Wrapper around the Bloomberg Open API (blpapi) for fetching reference and
historical data.

STATUS: Stub — implement the blpapi session logic inside ``get_bloomberg_data``
        before use.

Install the Bloomberg Python SDK (requires a Bloomberg Terminal or B-PIPE):
    pip install blpapi --index-url https://bcms.bloomberg.com/pip/simple/
"""

from __future__ import annotations

from typing import Dict, List, Optional

import pandas as pd


def get_bloomberg_data(
    tickers: List[str],
    fields: List[str],
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    request_type: str = "HistoricalDataRequest",
    overrides: Optional[Dict[str, str]] = None,
    host: str = "localhost",
    port: int = 8194,
) -> pd.DataFrame:
    """
    Fetch data from Bloomberg via blpapi.

    Wraps a ``blpapi`` session to support both historical time-series and
    point-in-time reference data requests.  Normalises the blpapi response
    into a ``pd.DataFrame`` consistent with other findata functions.

    Parameters
    ----------
    tickers : list[str]
        Bloomberg security identifiers, e.g.
        ``["AAPL US Equity", "MSFT US Equity"]``.
    fields : list[str]
        Bloomberg field mnemonics, e.g.
        ``["PX_LAST", "VOLUME", "EQY_SH_OUT"]``.
    start_date : str or None, optional
        Inclusive start date ``"YYYY-MM-DD"``.
        Used for ``HistoricalDataRequest`` only.
    end_date : str or None, optional
        Inclusive end date ``"YYYY-MM-DD"``.
        Used for ``HistoricalDataRequest`` only.
    request_type : str, optional
        Bloomberg service request type:

        ``"HistoricalDataRequest"`` (default) — daily OHLCV / fundamentals.
        ``"ReferenceDataRequest"``             — current or overridden fields.
    overrides : dict[str, str] or None, optional
        Bloomberg field overrides, e.g.
        ``{"BEST_FPERIOD_OVERRIDE": "1BF"}`` for forward estimates.
    host : str, optional
        Bloomberg API host (default ``"localhost"``).
    port : int, optional
        Bloomberg API port (default ``8194``).

    Returns
    -------
    pd.DataFrame
        ``HistoricalDataRequest`` — ``MultiIndex`` columns ``(field, ticker)``
        with ``DatetimeIndex``.
        ``ReferenceDataRequest``  — index = tickers, columns = fields.

    Raises
    ------
    ImportError
        If ``blpapi`` is not installed.
    ConnectionError
        If a Bloomberg session cannot be opened at ``host``:``port``.
    NotImplementedError
        Until the session body is implemented (current state).

    Examples
    --------
    >>> from findata.bloomberg import get_bloomberg_data

    >>> # Historical daily prices
    >>> df = get_bloomberg_data(
    ...     tickers=["AAPL US Equity", "MSFT US Equity"],
    ...     fields=["PX_LAST", "VOLUME"],
    ...     start_date="2024-01-01",
    ...     end_date="2024-12-31",
    ... )
    >>> close = df["PX_LAST"]    # rows=dates, cols=tickers

    >>> # Reference data — current values
    >>> ref = get_bloomberg_data(
    ...     tickers=["AAPL US Equity"],
    ...     fields=["CUR_MKT_CAP", "GICS_SECTOR_NAME"],
    ...     request_type="ReferenceDataRequest",
    ... )

    >>> # Forward EPS estimate with period override
    >>> fwd = get_bloomberg_data(
    ...     tickers=["AAPL US Equity", "NVDA US Equity"],
    ...     fields=["BEST_EPS"],
    ...     request_type="ReferenceDataRequest",
    ...     overrides={"BEST_FPERIOD_OVERRIDE": "1BF"},
    ... )
    """
    try:
        import blpapi  # noqa: F401
    except ImportError as exc:
        raise ImportError(
            "blpapi is required.  "
            "Install: pip install blpapi "
            "--index-url https://bcms.bloomberg.com/pip/simple/"
        ) from exc

    # ------------------------------------------------------------------
    # TODO: implement blpapi session logic here.
    #
    # Skeleton:
    #   opts = blpapi.SessionOptions()
    #   opts.setServerHost(host)
    #   opts.setServerPort(port)
    #   session = blpapi.Session(opts)
    #   if not session.start():
    #       raise ConnectionError(f"Cannot connect to Bloomberg at {host}:{port}")
    #   session.openService("//blp/refdata")
    #   svc = session.getService("//blp/refdata")
    #   req = svc.createRequest(request_type)
    #   # populate tickers / fields / dates / overrides
    #   # send request, iterate events, parse Element tree → DataFrame
    # ------------------------------------------------------------------
    raise NotImplementedError(
        "get_bloomberg_data() is a stub.  "
        "Implement the blpapi session in findata/bloomberg.py."
    )
