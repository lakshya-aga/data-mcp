"""
findata.sec_edgar
-----------------
SEC EDGAR filings metadata via the public ``data.sec.gov`` JSON endpoints.

The SEC requires a descriptive ``User-Agent`` header on every request. Set the
``SEC_EDGAR_USER_AGENT`` environment variable (e.g.
``"Acme Research research@example.com"``) or pass ``user_agent`` explicitly.
"""

from __future__ import annotations

import os
from typing import Optional


def get_sec_edgar_filings(
    cik: str,
    form_types: Optional[list] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    user_agent: Optional[str] = None,
    timeout: int = 30,
) -> "pd.DataFrame":
    """
    Fetch the recent filings list for a company from SEC EDGAR.

    Hits ``https://data.sec.gov/submissions/CIK<cik>.json`` and returns the
    ``filings.recent`` table as a DataFrame indexed by filing date. EDGAR
    surfaces roughly the most recent ~1000 filings per issuer through this
    endpoint.

    Parameters
    ----------
    cik : str
        Central Index Key. Accepts integer-like strings or zero-padded forms
        (e.g. ``"320193"`` or ``"0000320193"`` for Apple).
    form_types : list[str] or None, optional
        Filter to specific filing form types (e.g. ``["10-K", "10-Q", "8-K"]``).
        ``None`` (default) returns all forms.
    start_date : str or None, optional
        Inclusive lower bound on ``filingDate``, ``"YYYY-MM-DD"``.
    end_date : str or None, optional
        Inclusive upper bound on ``filingDate``, ``"YYYY-MM-DD"``.
    user_agent : str or None, optional
        ``User-Agent`` header sent with the request. SEC requires a contact
        string. Falls back to the ``SEC_EDGAR_USER_AGENT`` environment
        variable.
    timeout : int, optional
        Request timeout in seconds (default ``30``).

    Returns
    -------
    pd.DataFrame
        DatetimeIndex (filing date) with columns including ``accessionNumber``,
        ``form``, ``primaryDocument``, ``primaryDocDescription``, ``reportDate``,
        ``size``, ``isXBRL``, ``isInlineXBRL``, plus a derived
        ``filing_url`` pointing at the primary document on EDGAR.

    Raises
    ------
    ValueError
        If inputs are invalid or no filings are returned.
    ImportError
        If a required package is not installed.

    Examples
    --------
    >>> from findata.sec_edgar import get_sec_edgar_filings
    >>> df = get_sec_edgar_filings(
    ...     "320193",
    ...     form_types=["10-K", "10-Q"],
    ...     start_date="2020-01-01",
    ...     user_agent="Acme Research research@example.com",
    ... )
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

    if not isinstance(cik, (str, int)) or not str(cik).strip():
        raise ValueError("cik must be a non-empty string or integer.")

    cik_str = str(cik).strip().lstrip("0") or "0"
    cik_padded = cik_str.zfill(10)

    ua = user_agent or os.environ.get("SEC_EDGAR_USER_AGENT")
    if not ua or not ua.strip():
        raise ValueError(
            "SEC EDGAR requires a descriptive User-Agent containing contact "
            "info. Pass user_agent= or set SEC_EDGAR_USER_AGENT."
        )

    if not isinstance(timeout, int) or timeout <= 0:
        raise ValueError("timeout must be a positive integer (seconds).")

    start_dt = pd.to_datetime(start_date) if start_date else None
    end_dt = pd.to_datetime(end_date) if end_date else None
    if start_dt is not None and end_dt is not None and start_dt > end_dt:
        raise ValueError("start_date must be on or before end_date.")

    url = f"https://data.sec.gov/submissions/CIK{cik_padded}.json"
    headers = {"User-Agent": ua, "Accept": "application/json"}

    try:
        resp = requests.get(url, headers=headers, timeout=timeout)
        resp.raise_for_status()
    except Exception as exc:
        raise ValueError(
            f"Failed to fetch EDGAR submissions for CIK {cik_padded!r}."
        ) from exc

    payload = resp.json()
    recent = payload.get("filings", {}).get("recent")
    if not recent or "accessionNumber" not in recent:
        raise ValueError(f"No filings returned for CIK {cik_padded!r}.")

    df = pd.DataFrame(recent)
    if df.empty:
        raise ValueError(f"No filings returned for CIK {cik_padded!r}.")

    df["filingDate"] = pd.to_datetime(df["filingDate"])
    if "reportDate" in df.columns:
        df["reportDate"] = pd.to_datetime(df["reportDate"], errors="coerce")

    if form_types:
        wanted = {f.upper() for f in form_types}
        df = df[df["form"].str.upper().isin(wanted)]

    if start_dt is not None:
        df = df[df["filingDate"] >= start_dt]
    if end_dt is not None:
        df = df[df["filingDate"] <= end_dt]

    if df.empty:
        raise ValueError("No filings matched the requested filters.")

    accession_no_dash = df["accessionNumber"].str.replace("-", "", regex=False)
    df["filing_url"] = (
        "https://www.sec.gov/Archives/edgar/data/"
        + cik_str
        + "/"
        + accession_no_dash
        + "/"
        + df.get("primaryDocument", "")
    )

    df = df.set_index("filingDate").sort_index()
    df.index.name = "filing_date"
    return df
