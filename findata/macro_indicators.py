"""
findata.macro_indicators
------------------------
A curated bundle of macroeconomic indicators that actually move
equities, packaged as a single dict the agent can scan in one tool
call instead of stitching ten FRED calls together itself.

Why this exists separately from ``findata.fred``:
  - ``fred.get_fred_series`` is a low-level wrapper over fredapi —
    it expects the caller to know the series IDs.
  - The trading panel's Macro Analyst doesn't know FRED IDs (asking
    a 14b-class model to remember CPIAUCSL vs CPILFESL is wasted
    capacity) — it just wants "tell me what rates / inflation /
    credit / FX look like right now".
  - This module pre-bakes the right series, computes the changes
    over standard horizons, and labels them with English names.

Coverage right now is US-centric because that's what FRED carries.
For Indian equity (.NS) panels we surface USD/INR and a note that
local-CPI / repo-rate aren't covered — the analyst then leans on
news + global dollar context for the Indian macro view.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any, Optional


logger = logging.getLogger(__name__)


# Curated series. Each entry: (FRED id, human label, units).
# Grouped so the LLM-facing snapshot stays readable.
_INTEREST_RATES = [
    ("FEDFUNDS",  "Fed Funds rate",         "%"),
    ("DGS2",      "2Y Treasury yield",      "%"),
    ("DGS10",     "10Y Treasury yield",     "%"),
    ("DGS30",     "30Y Treasury yield",     "%"),
    ("T10Y2Y",    "10Y-2Y spread",          "%"),
    ("T10Y3M",    "10Y-3M spread",          "%"),
    ("MORTGAGE30US", "30Y mortgage rate",   "%"),
]

_INFLATION = [
    ("CPIAUCSL",  "Headline CPI level",     "index"),
    ("CPILFESL",  "Core CPI level",         "index"),
    ("PCEPI",     "PCE price index",        "index"),
    ("T5YIE",     "5Y breakeven inflation", "%"),
    ("T10YIE",    "10Y breakeven inflation","%"),
]

_GROWTH_LABOR = [
    ("UNRATE",    "Unemployment rate",      "%"),
    ("ICSA",      "Initial jobless claims", "thousands"),
    ("INDPRO",    "Industrial production",  "index"),
    ("UMCSENT",   "Consumer sentiment",     "index"),
]

_CREDIT_RISK = [
    ("BAMLC0A0CM",       "IG corporate spread",     "bps"),
    ("BAMLH0A0HYM2",     "HY corporate spread",     "bps"),
    ("BAMLC0A4CBBB",     "BBB corporate spread",    "bps"),
    ("VIXCLS",           "VIX",                     "index"),
]

_FX_COMMODITY = [
    ("DTWEXBGS",     "Broad dollar index",  "index"),
    ("DEXINUS",      "USD/INR",             "INR per USD"),
    ("DCOILWTICO",   "WTI crude oil",       "USD/bbl"),
    ("DCOILBRENTEU", "Brent crude oil",     "USD/bbl"),
    # Note: London Gold Fix (GOLDAMGBD228NLBM) was discontinued by FRED.
    # Skipping until we identify a stable replacement series; the macro
    # analyst can read gold context from news + the dollar index.
]


_ALL_SERIES = _INTEREST_RATES + _INFLATION + _GROWTH_LABOR + _CREDIT_RISK + _FX_COMMODITY


# Horizons over which to compute changes. The LLM reasons better about
# "rate moved +50bp over 90 days" than about a raw time series.
_HORIZONS_DAYS = (30, 90, 365)


def _pct_change(latest: float, prior: float) -> Optional[float]:
    if prior is None or latest is None or prior == 0:
        return None
    return (float(latest) / float(prior) - 1.0) * 100.0


def _abs_change(latest: float, prior: float) -> Optional[float]:
    if prior is None or latest is None:
        return None
    return float(latest) - float(prior)


def get_macro_snapshot(
    country: str = "US",
    lookback_days: int = 400,
    api_key: Optional[str] = None,
) -> dict[str, Any]:
    """Return a curated dict snapshot of macro indicators + recent changes.

    Parameters
    ----------
    country : str
        Currently only "US" is fully supported (FRED). Indian panels
        get the dollar-index + USD/INR slice plus a note that local
        repo-rate / CPI aren't on FRED.
    lookback_days : int
        Window pulled from FRED so 30/90/365-day changes can all be
        computed from one call. Default 400 covers 365d cleanly.
    api_key : str or None
        FRED API key override. Falls back to FRED_API_KEY env.

    Returns
    -------
    dict shaped as::

        {
          "as_of": "2026-05-08",
          "country": "US",
          "indicators": {
            "Fed Funds rate": {
              "series_id": "FEDFUNDS",
              "value": 5.33, "unit": "%",
              "changes": {
                "30d_abs":  -0.10,
                "90d_abs":  +0.25,
                "365d_abs": +1.00,
                "30d_pct":  ...,   # absent for series where pct doesn't make sense
              },
              "as_of": "2026-04-30",
            },
            ...
          },
          "groups": {
            "Interest rates": ["Fed Funds rate", "2Y Treasury yield", ...],
            "Inflation":      [...],
            "Growth & labor": [...],
            "Credit & risk":  [...],
            "FX & commodity": [...],
          },
          "errors": [...],   # series IDs that failed to load
          "summary": "Two-sentence English summary of regime"
        }

    On failure to reach FRED entirely, returns a dict with
    ``status: "no_data"`` and an explanatory ``summary``.
    """
    try:
        from .fred import get_fred_series
        import pandas as pd
    except ImportError as e:
        return {
            "status": "no_data",
            "country": country,
            "summary": f"FRED tooling unavailable: {e}",
            "indicators": {}, "groups": {}, "errors": [],
        }

    end = date.today()
    start = end - timedelta(days=int(lookback_days))

    # Per-series fetch with individual error capture. Tried bulk fetch
    # first but findata.fred.get_fred_series re-raises on the first
    # failed series — one discontinued/renamed series ID would kill the
    # whole snapshot. FRED occasionally retires series (London Gold Fix
    # was the latest example), so single-failure resilience is worth the
    # extra round-trips. fredapi enforces ~1 RPS internally so the wall-
    # clock cost is similar to the bulk path anyway.
    series_ids = [sid for sid, _, _ in _ALL_SERIES]
    df_columns: dict[str, "pd.Series"] = {}
    errors_by_series: dict[str, str] = {}
    for sid in series_ids:
        try:
            single = get_fred_series(
                [sid],
                start_date=start.strftime("%Y-%m-%d"),
                end_date=end.strftime("%Y-%m-%d"),
                api_key=api_key,
            )
            if sid in single.columns:
                df_columns[sid] = single[sid]
        except Exception as e:
            errors_by_series[sid] = f"{type(e).__name__}: {e}"
            logger.warning("FRED series %s failed: %s", sid, e)

    if not df_columns:
        # Total failure — likely API key not set or upstream down.
        first_err = next(iter(errors_by_series.values())) if errors_by_series else "no series fetched"
        return {
            "status": "no_data",
            "country": country,
            "summary": f"FRED fetch failed: {first_err}",
            "indicators": {}, "groups": {}, "errors": list(errors_by_series.keys()),
        }

    df = pd.DataFrame(df_columns)

    indicators: dict[str, dict[str, Any]] = {}
    errors: list[str] = list(errors_by_series.keys())

    for sid, label, unit in _ALL_SERIES:
        if sid not in df.columns:
            errors.append(sid)
            continue
        col = df[sid].dropna()
        if col.empty:
            errors.append(sid)
            continue
        latest_ts = col.index[-1]
        latest_val = float(col.iloc[-1])

        changes: dict[str, float] = {}
        for h_days in _HORIZONS_DAYS:
            target_ts = latest_ts - pd.Timedelta(days=h_days)
            # Use the last reading at or before target_ts (FRED is irregular).
            prior_slice = col.loc[:target_ts]
            if prior_slice.empty:
                continue
            prior_val = float(prior_slice.iloc[-1])
            changes[f"{h_days}d_abs"] = round(_abs_change(latest_val, prior_val), 4)
            # Percent changes only useful for level series, NOT for rates
            # already in % (a Fed Funds rate moving from 5% to 6% is +20%
            # which is meaningless — the user wants +1.00 percentage points).
            if unit not in ("%", "bps"):
                pct = _pct_change(latest_val, prior_val)
                if pct is not None:
                    changes[f"{h_days}d_pct"] = round(pct, 2)

        indicators[label] = {
            "series_id": sid,
            "value": round(latest_val, 4),
            "unit": unit,
            "changes": changes,
            "as_of": latest_ts.strftime("%Y-%m-%d"),
        }

    # Synthetic series: compute a couple of derived indicators inline.
    if "BAA10YM" in df.columns and "AAA10YM" in df.columns:
        try:
            baa = df["BAA10YM"].dropna()
            aaa = df["AAA10YM"].dropna()
            common = baa.index.intersection(aaa.index)
            if not common.empty:
                spread = (baa.loc[common] - aaa.loc[common]).dropna()
                latest_ts = spread.index[-1]
                latest_val = float(spread.iloc[-1])
                indicators["BAA-AAA credit spread"] = {
                    "series_id": "BAA10YM-AAA10YM",
                    "value": round(latest_val, 4),
                    "unit": "%",
                    "changes": {},
                    "as_of": latest_ts.strftime("%Y-%m-%d"),
                }
        except Exception:
            pass

    groups: dict[str, list[str]] = {
        "Interest rates":  [lbl for _, lbl, _ in _INTEREST_RATES],
        "Inflation":       [lbl for _, lbl, _ in _INFLATION],
        "Growth & labor":  [lbl for _, lbl, _ in _GROWTH_LABOR],
        "Credit & risk":   [lbl for _, lbl, _ in _CREDIT_RISK],
        "FX & commodity":  [lbl for _, lbl, _ in _FX_COMMODITY],
    }
    # Drop labels that didn't load so the agent doesn't get empty entries.
    groups = {g: [l for l in lbls if l in indicators] for g, lbls in groups.items()}

    summary = _summarise_regime(indicators, country)

    # Dedupe errors — a failed-fetch series would be appended twice
    # (once by the per-series try/except above, once by the empty-data
    # check inside the indicator loop).
    errors = list(dict.fromkeys(errors))

    out = {
        "status": "ok",
        "as_of": end.strftime("%Y-%m-%d"),
        "country": country,
        "indicators": indicators,
        "groups": groups,
        "errors": errors,
        "summary": summary,
    }

    if country.upper() in ("IN", "INDIA"):
        out["india_note"] = (
            "FRED doesn't carry RBI repo / Indian CPI / IIP. The dollar "
            "index + USD/INR + global crude in this snapshot are still "
            "the relevant cross-border drivers; for India-domestic macro "
            "the news analyst's GDELT pull is the substitute source."
        )

    return out


def _summarise_regime(indicators: dict[str, dict[str, Any]], country: str) -> str:
    """One- or two-sentence English summary of the macro regime so the
    LLM has a fast scan path. Pure heuristic — agent can disagree."""
    bits: list[str] = []
    fed = indicators.get("Fed Funds rate", {}).get("value")
    spread_10_2 = indicators.get("10Y-2Y spread", {}).get("value")
    cpi_breakeven = indicators.get("10Y breakeven inflation", {}).get("value")
    hy = indicators.get("HY corporate spread", {}).get("value")
    vix = indicators.get("VIX", {}).get("value")

    if fed is not None:
        bits.append(f"Fed Funds at {fed:.2f}%")
    if spread_10_2 is not None:
        if spread_10_2 < 0:
            bits.append(f"yield curve inverted ({spread_10_2:.2f}%)")
        elif spread_10_2 < 0.5:
            bits.append(f"yield curve flat ({spread_10_2:.2f}%)")
        else:
            bits.append(f"yield curve positively sloped ({spread_10_2:.2f}%)")
    if cpi_breakeven is not None:
        bits.append(f"10Y breakeven inflation {cpi_breakeven:.2f}%")
    if hy is not None:
        if hy > 600:
            bits.append(f"HY spreads stressed ({hy:.0f} bps)")
        elif hy < 300:
            bits.append(f"HY spreads tight ({hy:.0f} bps)")
        else:
            bits.append(f"HY spreads moderate ({hy:.0f} bps)")
    if vix is not None:
        if vix > 25:
            bits.append(f"VIX elevated ({vix:.1f})")
        elif vix < 15:
            bits.append(f"VIX subdued ({vix:.1f})")

    if not bits:
        return f"Macro snapshot for {country}: data unavailable."
    return f"Macro snapshot ({country}): " + " · ".join(bits) + "."


def get_yield_curve(api_key: Optional[str] = None) -> dict[str, Any]:
    """Snapshot of the US Treasury curve at the most recent date,
    plus the curve from one year ago for shape comparison.

    Returns::

        {
          "status": "ok",
          "today": [{"tenor": "3M", "yield": 5.32}, ...],
          "one_year_ago": [...],
          "as_of": "2026-05-08",
          "summary": "Curve inverted 3M-10Y, normalising at long end."
        }
    """
    try:
        from .fred import get_fred_series
        import pandas as pd
    except ImportError as e:
        return {"status": "no_data", "summary": f"FRED unavailable: {e}",
                "today": [], "one_year_ago": []}

    tenor_map = [
        ("3M", "DGS3MO"), ("6M", "DGS6MO"), ("1Y", "DGS1"),
        ("2Y", "DGS2"),  ("3Y", "DGS3"),  ("5Y", "DGS5"),
        ("7Y", "DGS7"),  ("10Y", "DGS10"), ("20Y", "DGS20"),
        ("30Y", "DGS30"),
    ]

    end = date.today()
    start = end - timedelta(days=400)
    try:
        df = get_fred_series(
            [sid for _, sid in tenor_map],
            start_date=start.strftime("%Y-%m-%d"),
            end_date=end.strftime("%Y-%m-%d"),
            api_key=api_key,
        )
    except Exception as e:
        return {"status": "no_data", "summary": f"FRED fetch failed: {e}",
                "today": [], "one_year_ago": []}

    today_curve: list[dict[str, Any]] = []
    year_ago_curve: list[dict[str, Any]] = []
    cutoff_year_ago = pd.Timestamp(end - timedelta(days=365))
    for tenor, sid in tenor_map:
        if sid not in df.columns:
            continue
        col = df[sid].dropna()
        if col.empty:
            continue
        today_curve.append({"tenor": tenor, "yield": round(float(col.iloc[-1]), 4)})
        prior = col.loc[:cutoff_year_ago]
        if not prior.empty:
            year_ago_curve.append({"tenor": tenor, "yield": round(float(prior.iloc[-1]), 4)})

    summary = _summarise_curve(today_curve)
    return {
        "status": "ok" if today_curve else "no_data",
        "as_of": end.strftime("%Y-%m-%d"),
        "today": today_curve,
        "one_year_ago": year_ago_curve,
        "summary": summary,
    }


def _summarise_curve(curve: list[dict[str, Any]]) -> str:
    if not curve:
        return "Yield curve unavailable."
    by_tenor = {pt["tenor"]: pt["yield"] for pt in curve}
    short = by_tenor.get("3M") or by_tenor.get("6M") or by_tenor.get("1Y")
    long = by_tenor.get("10Y") or by_tenor.get("30Y")
    if short is None or long is None:
        return "Yield curve partial."
    diff = long - short
    if diff < -0.25:
        shape = f"inverted ({diff:.2f}%)"
    elif diff < 0.25:
        shape = f"flat ({diff:.2f}%)"
    else:
        shape = f"upward-sloping ({diff:.2f}%)"
    return f"US Treasury curve {shape}; short rate {short:.2f}%, long rate {long:.2f}%."
