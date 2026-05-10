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

# GDELT's public doc endpoint rate-limits to one request per ~5 seconds
# per IP. Each get_gdelt_news() call fires up to 4 requests (ArtList +
# ToneChart, twice if sector_query is set), so we space them out
# defensively. A single extra second is cheap insurance against the
# 429 → empty-articles silent failure that bit us when tone enrichment
# was added.
_GDELT_RATE_LIMIT_SECS = 7.0


def _fetch_gdelt_articles(
    query: str,
    days: int,
    max_records: int,
    sort: str = "DateDesc",
) -> list[dict[str, Any]]:
    """Hit GDELT's doc API and return its articles list (or [] on error).

    Note: GDELT's ``ArtList`` mode does NOT include the ``tone`` field
    despite earlier comments in this module claiming it did. Tone is
    fetched separately via ``_fetch_gdelt_tone_chart`` and joined onto
    each article in ``get_gdelt_news`` below.
    """
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


def _fetch_gdelt_tone_chart(query: str, days: int) -> dict[str, Any]:
    """Query GDELT's ``ToneChart`` mode to get tone bins + example URLs.

    Returns a dict::

        {
            "average_tone": float | None,   # weighted mean of bin centres
            "url_tone": {url: float, ...},  # per-URL tone for the top
                                            # articles GDELT highlights
            "n_articles_seen": int,         # total articles across all bins
        }

    The ``url_tone`` map is what lets us back-fill the per-article tone
    field that ``ArtList`` doesn't expose. Each ToneChart bin lists a
    handful of "toparts" — the most prominent articles in that bin —
    so this gives tone for the articles GDELT considers representative
    of the query, even though the broader ArtList still has many
    untagged entries (those fall back to ``average_tone``).
    """
    import requests

    params = {
        "query": query,
        "mode": "ToneChart",
        "format": "JSON",
        "timespan": f"{int(days)}d",
    }
    try:
        resp = requests.get(_GDELT_DOC_URL, params=params, timeout=12)
        resp.raise_for_status()
        data = resp.json()
    except Exception:
        return {"average_tone": None, "url_tone": {}, "n_articles_seen": 0}

    bins = data.get("tonechart") or []
    url_tone: dict[str, float] = {}
    weighted_sum = 0.0
    total_count = 0
    for b in bins:
        try:
            centre = float(b.get("bin"))
            count = int(b.get("count") or 0)
        except (TypeError, ValueError):
            continue
        weighted_sum += centre * count
        total_count += count
        for top in b.get("toparts") or []:
            url = top.get("url")
            if isinstance(url, str) and url:
                url_tone[url] = centre
    avg = (weighted_sum / total_count) if total_count > 0 else None
    return {
        "average_tone": avg,
        "url_tone": url_tone,
        "n_articles_seen": total_count,
    }


def _normalise_article(
    a: dict[str, Any],
    query: str,
    query_kind: str,
    *,
    url_tone: Optional[dict[str, float]] = None,
    fallback_tone: Optional[float] = None,
) -> dict[str, Any]:
    """Normalise a GDELT article dict into a stable record shape.

    Tone enrichment: ``ArtList`` doesn't ship tone, so ``get_gdelt_news``
    runs a second ``ToneChart`` query and threads the resulting
    ``url_tone`` map through. We look the article's URL up in that map
    first; for articles GDELT didn't surface as a ToneChart "topart"
    we fall back to the chart's overall ``average_tone``. This way
    every row has *some* tone signal — exact when GDELT highlighted the
    article, aggregate when it didn't — instead of a uniform null
    column that the panel was forced to default to "neutral".
    """
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

    url = a.get("url")
    tone: Optional[float] = None
    tone_source: str = "unavailable"
    if url and url_tone and url in url_tone:
        tone = float(url_tone[url])
        tone_source = "tone_chart_exact"
    elif fallback_tone is not None:
        tone = float(fallback_tone)
        tone_source = "tone_chart_aggregate"

    return {
        "seendate": seen,
        "title": a.get("title"),
        "url": url,
        "url_mobile": a.get("url_mobile"),
        "domain": a.get("domain"),
        "sourcecountry": a.get("sourcecountry"),
        "language": a.get("language"),
        "tone": tone,
        "tone_source": tone_source,
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
    import time

    if not isinstance(company_query, str) or not company_query.strip():
        raise ValueError("company_query must be a non-empty string")
    days = max(1, min(30, int(days)))
    max_records = max(1, min(75, int(max_records)))

    # Sequence: ArtList(company) → ToneChart(company) → ArtList(sector)
    # → ToneChart(sector). Sleep between calls — GDELT rate-limits the
    # public endpoint to one request per ~5 seconds and silently 429s
    # when exceeded (which surfaces as empty articles + a confused
    # analyst).
    company_articles = _fetch_gdelt_articles(company_query, days, max_records)
    time.sleep(_GDELT_RATE_LIMIT_SECS)
    company_tone = _fetch_gdelt_tone_chart(company_query, days)

    sector_articles: list[dict[str, Any]] = []
    sector_tone: dict[str, Any] = {"average_tone": None, "url_tone": {}, "n_articles_seen": 0}
    if sector_query and sector_query.strip():
        time.sleep(_GDELT_RATE_LIMIT_SECS)
        sector_articles = _fetch_gdelt_articles(sector_query, days, max_records)
        time.sleep(_GDELT_RATE_LIMIT_SECS)
        sector_tone = _fetch_gdelt_tone_chart(sector_query, days)

    def _to_df(
        articles: list[dict[str, Any]],
        query: str,
        kind: str,
        tone_data: dict[str, Any],
    ) -> pd.DataFrame:
        rows = [
            _normalise_article(
                a, query, kind,
                url_tone=tone_data.get("url_tone"),
                fallback_tone=tone_data.get("average_tone"),
            )
            for a in articles
        ]
        if not rows:
            return pd.DataFrame(
                columns=[
                    "title", "url", "url_mobile", "domain", "sourcecountry",
                    "language", "tone", "tone_source", "socialimage",
                    "query", "query_kind",
                ],
                index=pd.DatetimeIndex([], name="seendate", tz="UTC"),
            )
        df = pd.DataFrame(rows)
        return df.set_index("seendate").sort_index(ascending=False)

    return {
        "company": _to_df(company_articles, company_query, "company", company_tone),
        "sector": _to_df(sector_articles, sector_query or "", "sector", sector_tone),
        # Aggregate tone summary so callers can render an overall sentiment
        # number even when individual articles fell back to the average.
        "tone_summary": {
            "company_average_tone": company_tone.get("average_tone"),
            "company_n_articles_in_chart": company_tone.get("n_articles_seen", 0),
            "sector_average_tone": sector_tone.get("average_tone"),
            "sector_n_articles_in_chart": sector_tone.get("n_articles_seen", 0),
        },
    }
