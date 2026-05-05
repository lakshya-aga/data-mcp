"""
findata.news_gdelt
------------------
Recent news from the GDELT global news monitor.

Two queries per call:
  * ``company_query`` — company-specific (ticker, name, products)
  * ``sector_query`` — industry / sector-wide (optional)

Returns articles with GDELT's average tone score per article (-100 = very
negative, +100 = very positive), plus URL, source, language, country.
GDELT is free, no API key, multi-language coverage. Best for broad
trawls of public-news view on a company or theme.

Note: GDELT's ArtList mode returns only the per-article *average* tone.
Richer V2Tone (positive / negative / polarity / activity-ref / self-ref
/ word-count) requires the GKG dataset — not surfaced here in v1; would
add a separate ``get_gdelt_gkg_tone`` if there's demand.
"""

from __future__ import annotations

from typing import Optional, Any

import pandas as pd


_GDELT_DOC_URL = "https://api.gdeltproject.org/api/v2/doc/doc"


def _fetch_gdelt_articles(
    query: str,
    days: int,
    max_records: int,
    sort: str = "DateDesc",
) -> list[dict[str, Any]]:
    """Hit GDELT's doc API and return its articles list (or [] on error)."""
    import requests

    params = {
        "query": query,
        "mode": "ArtList",
        "format": "JSON",
        "timespan": f"{int(days)}d",
        "maxrecords": str(int(max_records)),
        "sort": sort,
    }
    try:
        resp = requests.get(_GDELT_DOC_URL, params=params, timeout=12)
        resp.raise_for_status()
        data = resp.json()
    except Exception:
        return []
    return data.get("articles") or []


def _normalise_article(a: dict[str, Any], query: str, query_kind: str) -> dict[str, Any]:
    """Normalise a GDELT article dict into a stable record shape."""
    # GDELT seendate format is "YYYYMMDDTHHMMSSZ" — parse defensively.
    raw_date = a.get("seendate")
    seen: Optional[pd.Timestamp]
    if isinstance(raw_date, str):
        try:
            seen = pd.Timestamp(raw_date.replace("Z", ""), tz="UTC")
        except Exception:
            seen = None
    else:
        seen = None
    # Tone is a numeric string in GDELT's output; coerce.
    tone_raw = a.get("tone")
    try:
        tone = float(tone_raw) if tone_raw is not None else None
    except (TypeError, ValueError):
        tone = None

    return {
        "seendate": seen,
        "title": a.get("title"),
        "url": a.get("url"),
        "url_mobile": a.get("url_mobile"),
        "domain": a.get("domain"),
        "sourcecountry": a.get("sourcecountry"),
        "language": a.get("language"),
        "tone": tone,
        "socialimage": a.get("socialimage"),
        "query": query,
        "query_kind": query_kind,
    }


def get_gdelt_news(
    company_query: str,
    sector_query: Optional[str] = None,
    days: int = 7,
    max_records: int = 25,
) -> dict[str, pd.DataFrame]:
    """
    Fetch recent news from GDELT for a company AND (optionally) its sector.

    Two GDELT queries per call:
      * ``company_query`` (required) — narrow query about a specific company,
        e.g. ``"Apple Inc iPhone Services"`` or ``"NVIDIA AI chip"``.
      * ``sector_query`` (optional) — broader industry / sector query,
        e.g. ``"semiconductor industry"`` or ``"global EV demand"``.

    Each article carries GDELT's average tone score (a single number from
    roughly −100 = strongly negative to +100 = strongly positive), plus
    URL, source country, domain, and language.

    Parameters
    ----------
    company_query : str
        Required. GDELT-style query string for the company-specific search.
    sector_query : str or None, default None
        Optional broader industry/sector query. Skipped if None.
    days : int, default 7
        Lookback window in days. Capped at 30.
    max_records : int, default 25
        Max articles per query (1-75). Each query gets its own page so
        ``max_records=25`` and ``sector_query`` set returns up to 50 rows
        across both buckets.

    Returns
    -------
    dict[str, pandas.DataFrame]
        Always returns the keys ``"company"`` and ``"sector"``. The
        ``"sector"`` DataFrame is empty when ``sector_query`` is None.

        Each DataFrame columns:
            ``title``, ``url``, ``url_mobile``, ``domain``, ``sourcecountry``,
            ``language``, ``tone``, ``socialimage``, ``query``, ``query_kind``.
        Index: ``DatetimeIndex`` (UTC, descending) named ``seendate``.

    Examples
    --------
    >>> from findata.news_gdelt import get_gdelt_news
    >>> result = get_gdelt_news(
    ...     company_query="NVIDIA AI chip data center",
    ...     sector_query="semiconductor manufacturing",
    ...     days=14,
    ...     max_records=30,
    ... )
    >>> result["company"][["title", "tone", "domain"]].head()
    >>> result["sector"]["tone"].describe()  # sector sentiment summary

    Notes
    -----
    GDELT publishes only the per-article *average* tone in its doc API
    (ArtList mode). The richer V2Tone breakdown (positive / negative /
    polarity / activity-density / self-ref-density / word-count) lives
    in the GKG dataset and is not exposed here. If you need that, query
    GDELT GKG directly via ``http://data.gdeltproject.org/gdeltv2/``.

    No API key required — GDELT's doc endpoint is fully public.
    """
    if not isinstance(company_query, str) or not company_query.strip():
        raise ValueError("company_query must be a non-empty string")
    days = max(1, min(30, int(days)))
    max_records = max(1, min(75, int(max_records)))

    company_articles = _fetch_gdelt_articles(company_query, days, max_records)
    sector_articles = (
        _fetch_gdelt_articles(sector_query, days, max_records)
        if sector_query and sector_query.strip()
        else []
    )

    def _to_df(articles: list[dict[str, Any]], query: str, kind: str) -> pd.DataFrame:
        rows = [_normalise_article(a, query, kind) for a in articles]
        if not rows:
            return pd.DataFrame(
                columns=[
                    "title", "url", "url_mobile", "domain", "sourcecountry",
                    "language", "tone", "socialimage", "query", "query_kind",
                ],
                index=pd.DatetimeIndex([], name="seendate", tz="UTC"),
            )
        df = pd.DataFrame(rows)
        return df.set_index("seendate").sort_index(ascending=False)

    return {
        "company": _to_df(company_articles, company_query, "company"),
        "sector": _to_df(sector_articles, sector_query or "", "sector"),
    }
