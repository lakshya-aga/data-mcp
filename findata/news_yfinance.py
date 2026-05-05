"""
findata.news_yfinance
---------------------
Recent news headlines for a US-listed ticker via yfinance.

yfinance returns a list of dicts whose schema has shifted across versions
(``providerPublishTime`` in older releases, nested ``content.pubDate`` in
newer ones). This wrapper normalises both into a stable DataFrame.
"""

from __future__ import annotations

from typing import Optional

import pandas as pd


def get_yfinance_news(
    ticker: str,
    max_records: int = 15,
) -> pd.DataFrame:
    """
    Fetch recent news headlines for a US-listed ticker via yfinance.

    Returns a DataFrame indexed by publication time (UTC, descending) with
    columns ``title``, ``publisher``, ``link``, ``summary``, ``ticker``.
    yfinance's news payload shape differs between versions; this wrapper
    flattens both into the same shape regardless.

    Best for: company-specific news on US equities (NYSE / NASDAQ) where
    Yahoo Finance has good coverage. For non-US equities, crypto, or
    industry-wide news, use ``get_gdelt_news`` instead.

    Parameters
    ----------
    ticker : str
        Yahoo Finance ticker symbol, e.g. ``"AAPL"``, ``"MSFT"``.
    max_records : int, default 15
        Maximum number of articles to return (capped at 30).

    Returns
    -------
    pandas.DataFrame
        Index: ``DatetimeIndex`` (UTC, sorted descending).
        Columns: ``title``, ``publisher``, ``link``, ``summary``, ``ticker``.
        Empty DataFrame with the right schema if yfinance returns nothing
        (delisted ticker, transient outage, etc.).

    Examples
    --------
    >>> from findata.news_yfinance import get_yfinance_news
    >>> df = get_yfinance_news("AAPL", max_records=10)
    >>> df[["title", "publisher"]].head()
    """
    import yfinance as yf

    if not isinstance(ticker, str) or not ticker.strip():
        raise ValueError("ticker must be a non-empty string")
    max_records = max(1, min(30, int(max_records)))

    try:
        items = yf.Ticker(ticker).news or []
    except Exception:
        items = []

    rows: list[dict] = []
    for it in items[:max_records]:
        # yfinance v0.2.50+ nests fields under ``content``; older versions
        # have them at top level. Try both.
        content = it.get("content") if isinstance(it, dict) else None
        if not isinstance(content, dict):
            content = {}

        title = it.get("title") or content.get("title")
        link = (
            it.get("link")
            or (content.get("canonicalUrl") or {}).get("url")
            or (content.get("clickThroughUrl") or {}).get("url")
        )
        publisher = (
            it.get("publisher")
            or (content.get("provider") or {}).get("displayName")
        )
        summary = it.get("summary") or content.get("summary")

        published = it.get("providerPublishTime") or content.get("pubDate")
        ts: Optional[pd.Timestamp]
        if isinstance(published, (int, float)):
            ts = pd.Timestamp(published, unit="s", tz="UTC")
        elif isinstance(published, str):
            try:
                ts = pd.Timestamp(published, tz="UTC")
            except Exception:
                ts = None
        else:
            ts = None

        rows.append({
            "published": ts,
            "title": title,
            "publisher": publisher,
            "link": link,
            "summary": summary,
            "ticker": ticker,
        })

    if not rows:
        return pd.DataFrame(
            columns=["title", "publisher", "link", "summary", "ticker"],
            index=pd.DatetimeIndex([], name="published", tz="UTC"),
        )

    df = pd.DataFrame(rows)
    df = df.set_index("published").sort_index(ascending=False)
    return df
