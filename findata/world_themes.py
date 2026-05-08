"""
findata.world_themes
--------------------
Snapshot of the major macro / geopolitical narratives in the news right
now, queryable by theme. Sourced entirely from the GDELT v2.0 theme
taxonomy — every theme code is a real CAMEO/GKG entry, every article
returned has a real source URL the agent can quote and the user can
click through to verify.

The trading panel's macro analyst calls this once per run to surface
the ~10 narratives most likely to affect equities (rates, inflation,
oil, geopolitical conflict, sanctions, AI capex, etc.). The analyst
then weaves the matches it finds into its sector-specific reasoning.

Why this NOT a generic "RSS aggregator":
  - Every article carries GDELT's `tone` score (-10 to +10), so the
    agent can weight by sentiment polarity, not just count headlines.
  - GDELT classifies every article into multiple themes, so we get a
    structured "this sector is in the news right now" signal — not
    just keyword search.
  - Free + open + no API key required.
  - Every URL is a real publisher (Reuters, FT, Bloomberg, Indian
    newspapers, etc.) — the user can verify.

Reference: https://api.gdeltproject.org/api/v2/doc/doc
GKG theme taxonomy: https://blog.gdeltproject.org/the-gkg-themes-list/
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Optional

import pandas as pd


logger = logging.getLogger(__name__)


# Curated list of GDELT v2.0 themes that materially move equities.
# Each entry: (theme_code, label, what_it_signals).
#
# These are real GKG theme codes — confirmed against GDELT's published
# taxonomy. They're stable across years; GDELT rarely deprecates codes.
_CURATED_THEMES: list[tuple[str, str, str]] = [
    ("ECON_INTEREST_RATES", "Central-bank rate decisions",
     "Fed/ECB/BOE/RBI/PBoC rate moves + policy signals"),
    ("ECON_INFLATION", "Inflation regime",
     "CPI prints, wage data, supply-chain price pressure"),
    ("ECON_RECESSION", "Recession watch",
     "GDP revisions, leading indicators, yield-curve commentary"),
    ("ECON_QUANTITATIVEEASING", "Balance-sheet policy",
     "QE / QT signals, central-bank securities operations"),
    ("ECON_DEBT", "Sovereign + corporate debt stress",
     "Credit rating actions, debt-ceiling fights, EM debt crises"),
    ("ENV_OIL", "Oil markets",
     "OPEC decisions, supply disruptions, Hormuz/Russia/sanctions effects"),
    ("ENV_GAS", "Natural gas markets",
     "European gas storage, LNG supply, pipeline geopolitics"),
    ("MILITARY_USE_OF_WEAPONS", "Active armed conflict",
     "Hot-war news (Ukraine, Middle East, Taiwan Strait, etc.)"),
    ("SANCTIONS", "Sanctions regimes",
     "Imposition / lifting / enforcement of US/EU/UN sanctions"),
    ("TRADE_DISPUTE", "Trade frictions",
     "Tariffs, export controls, tech-decoupling moves"),
    ("WB_2454_BILATERAL_TRADE_RELATIONS", "Bilateral trade relations",
     "US-China, EU-UK, US-India trade-deal negotiations"),
    ("UNGP_FORESTS_RIVERS_OCEANS", "Climate physical risk",
     "Floods, droughts, cyclones affecting commodities + supply chains"),
]


# Default fetch params. Conservative so a panel run doesn't spam GDELT.
_DEFAULT_DAYS_BACK = 7
_DEFAULT_MAX_PER_THEME = 5


def get_world_themes(
    themes: Optional[list[str]] = None,
    days: int = _DEFAULT_DAYS_BACK,
    max_per_theme: int = _DEFAULT_MAX_PER_THEME,
) -> dict[str, Any]:
    """Snapshot the active major macro narratives via GDELT theme queries.

    Parameters
    ----------
    themes : list of GKG theme codes, optional
        If None, queries the curated list (12 themes by default). Pass a
        narrower list to drill into specific narratives.
    days : int, default 7
        Look-back window in days.
    max_per_theme : int, default 5
        Articles to surface per theme.

    Returns
    -------
    dict::

        {
          "as_of": "2026-05-09",
          "days_back": 7,
          "themes": [
            {
              "theme_code": "ENV_OIL",
              "theme_label": "Oil markets",
              "signals": "OPEC decisions, supply disruptions, …",
              "n_articles_fetched": 5,
              "tone_avg": -1.2,           # avg GDELT tone across articles
              "tone_min": -3.5,
              "tone_max": 0.4,
              "top_articles": [
                {"title": "...", "url": "...", "source": "...",
                 "ts": "2026-05-08T...", "tone": -1.8}
              ]
            },
            ...
          ],
          "errors": []                    # themes that failed to fetch
        }

    No mock fallback — if GDELT is unreachable, returns ``status='no_data'``
    with the upstream error so the agent knows the snapshot is empty.
    """
    requested = themes if themes is not None else [c for c, _, _ in _CURATED_THEMES]
    label_map = {c: (lbl, sig) for c, lbl, sig in _CURATED_THEMES}

    out_themes: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []

    for code in requested:
        label, signals = label_map.get(code, (code, ""))
        try:
            articles = _fetch_theme_articles(code, days=days, max_records=max_per_theme)
        except Exception as e:
            logger.warning("GDELT theme fetch failed for %s: %s", code, e)
            errors.append({"theme_code": code, "error": f"{type(e).__name__}: {e}"})
            continue

        # Tone summary
        tones = [a["tone"] for a in articles if isinstance(a.get("tone"), (int, float))]
        tone_avg = sum(tones) / len(tones) if tones else None
        tone_min = min(tones) if tones else None
        tone_max = max(tones) if tones else None

        out_themes.append({
            "theme_code": code,
            "theme_label": label,
            "signals": signals,
            "n_articles_fetched": len(articles),
            "tone_avg": round(tone_avg, 2) if tone_avg is not None else None,
            "tone_min": round(tone_min, 2) if tone_min is not None else None,
            "tone_max": round(tone_max, 2) if tone_max is not None else None,
            "top_articles": articles,
        })

    if not out_themes and errors:
        # Total upstream failure — surface to the agent.
        return {
            "status": "no_data",
            "as_of": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            "days_back": days,
            "themes": [],
            "errors": errors,
            "summary": "GDELT unreachable for all requested themes.",
        }

    return {
        "status": "ok",
        "as_of": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "days_back": days,
        "themes": out_themes,
        "errors": errors,
        "summary": _summarise_regime(out_themes),
    }


# ─── GDELT API call ──────────────────────────────────────────────────


_GDELT_DOC_API = "https://api.gdeltproject.org/api/v2/doc/doc"


def _fetch_theme_articles(theme_code: str, days: int, max_records: int) -> list[dict[str, Any]]:
    """Hit GDELT's doc API and normalise the article list.

    GDELT's query syntax for theme: ``theme:CODE``. Returns up to
    ``max_records`` most-recent articles. Each article carries a
    ``tone`` score from GDELT's GKG analysis (-10 = very negative,
    +10 = very positive, 0 = neutral).
    """
    import requests

    params = {
        "query": f"theme:{theme_code}",
        "mode": "ArtList",
        "format": "json",
        "maxrecords": str(max(1, min(50, int(max_records)))),
        "sort": "DateDesc",
        "timespan": f"{int(days)}d",
    }
    resp = requests.get(_GDELT_DOC_API, params=params, timeout=15)
    resp.raise_for_status()
    payload = resp.json()
    raw_articles = payload.get("articles") or []

    out: list[dict[str, Any]] = []
    for a in raw_articles:
        url = a.get("url")
        if not url:
            continue
        try:
            tone = float(a.get("tone")) if a.get("tone") is not None else None
        except (TypeError, ValueError):
            tone = None
        ts_raw = a.get("seendate") or a.get("published_date")
        ts = _normalise_gdelt_ts(ts_raw)
        out.append({
            "title": (a.get("title") or "").strip(),
            "url": url,
            "source": a.get("domain") or a.get("sourcecountry") or "",
            "language": a.get("language") or "",
            "ts": ts,
            "tone": round(tone, 2) if tone is not None else None,
        })
    return out


def _normalise_gdelt_ts(raw: Any) -> Optional[str]:
    """GDELT's seendate is YYYYMMDDTHHMMSSZ. Return ISO 8601 string."""
    if not raw or not isinstance(raw, str):
        return None
    try:
        dt = datetime.strptime(raw[:15], "%Y%m%dT%H%M%S")
        return dt.replace(tzinfo=timezone.utc).isoformat()
    except Exception:
        return raw  # leave as-is if parse fails


# ─── Regime summary ─────────────────────────────────────────────────


def _summarise_regime(themes: list[dict[str, Any]]) -> str:
    """One-line summary of which themes are most negative right now.

    Heuristic: themes with average tone < -2 (notably negative) are
    flagged as 'stressed'. Themes with tone > +1 are flagged as
    'positive'. Used to give the agent a fast scan path.
    """
    if not themes:
        return "No theme data."

    stressed = [t for t in themes if (t.get("tone_avg") or 0) < -2]
    positive = [t for t in themes if (t.get("tone_avg") or 0) > 1]

    bits: list[str] = []
    if stressed:
        labels = [t["theme_label"] for t in stressed[:4]]
        bits.append(f"stressed narratives — {', '.join(labels)}")
    if positive:
        labels = [t["theme_label"] for t in positive[:3]]
        bits.append(f"upbeat — {', '.join(labels)}")
    if not bits:
        bits.append("mixed-tone narratives across themes")

    return f"World themes ({len(themes)} fetched): " + " · ".join(bits) + "."


# ─── Convenience: themes most relevant to a sector ───────────────────


# Maps sector slugs (from sector_exposure.SECTOR_EXPOSURE) to the GDELT
# theme codes most directly relevant. Used by the macro analyst when
# querying targeted themes for a specific company.
SECTOR_TO_THEMES: dict[str, list[str]] = {
    "in_private_bank": ["ECON_INTEREST_RATES", "ECON_DEBT"],
    "in_psu_bank": ["ECON_INTEREST_RATES", "ECON_DEBT"],
    "in_nbfc": ["ECON_INTEREST_RATES", "ECON_DEBT"],
    "in_refining_oil_marketing": [
        "ENV_OIL", "MILITARY_USE_OF_WEAPONS", "SANCTIONS",
    ],
    "in_metals_mining": ["ECON_RECESSION", "TRADE_DISPUTE", "ENV_OIL"],
    "in_cement": ["ENV_OIL"],
    "in_it_services": [
        "ECON_RECESSION", "ECON_INTEREST_RATES",
        "WB_2454_BILATERAL_TRADE_RELATIONS",
    ],
    "in_auto_pv_2w": ["ENV_OIL", "ECON_INFLATION", "TRADE_DISPUTE"],
    "in_pharma": ["TRADE_DISPUTE", "WB_2454_BILATERAL_TRADE_RELATIONS"],
    "in_fmcg": ["ECON_INFLATION", "UNGP_FORESTS_RIVERS_OCEANS"],
    "in_telecom": ["ECON_INTEREST_RATES"],
    "in_power_gen_utility": ["ENV_OIL", "ENV_GAS", "UNGP_FORESTS_RIVERS_OCEANS"],
    "us_megacap_tech": [
        "ECON_INTEREST_RATES", "TRADE_DISPUTE",
        "WB_2454_BILATERAL_TRADE_RELATIONS",
    ],
    "us_banks": ["ECON_INTEREST_RATES", "ECON_DEBT", "ECON_RECESSION"],
    "us_energy_e_p": [
        "ENV_OIL", "MILITARY_USE_OF_WEAPONS", "SANCTIONS",
    ],
    "us_consumer_discretionary": ["ECON_INFLATION", "ECON_RECESSION"],
}
