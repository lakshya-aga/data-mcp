"""
findata.support_resistance
--------------------------
Algorithmic support / resistance levels.

No off-the-shelf library is great here, so we hand-roll a two-stage
algorithm:

  1. ``scipy.signal.find_peaks`` over the price series to surface all
     local maxima (resistance candidates) and minima (support
     candidates) within the lookback window.
  2. Cluster those peak prices via 1D KMeans into ``n_levels`` bins.
     Each cluster's centroid is a candidate level; the touch-count
     within a tolerance band ranks them.

The output is a list of {price, type, touches, last_touch, strength}.
Designed for the bull/bear analysts to quote ("support at $185 with
4 touches in the last 90 days, last bounce 2026-04-19") rather than
to feed a backtest.
"""

from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd


def _classify_strength(touches: int) -> str:
    if touches >= 5: return "strong"
    if touches >= 3: return "moderate"
    return "weak"


def compute_support_resistance(
    ticker: str,
    lookback_days: int = 252,
    n_levels: int = 5,
    tolerance_pct: float = 0.5,
) -> dict:
    """
    Detect candidate support/resistance levels from price history.

    Parameters
    ----------
    ticker : str
        Yahoo Finance ticker symbol.
    lookback_days : int, default 252
        Calendar-day window. Capped at 1825 (5y).
    n_levels : int, default 5
        How many cluster centroids to surface (split between support
        and resistance based on which side of current price they sit).
        Capped at 12.
    tolerance_pct : float, default 0.5
        Band around each centroid (in %) used to count touches. 0.5 =
        a price within 0.5% of the centroid counts as a touch.

    Returns
    -------
    dict
        {
          "ticker": str,
          "current_price": float,
          "lookback_days": int,
          "levels": [
            {"price": float, "type": "support"|"resistance",
             "touches": int, "last_touch": "YYYY-MM-DD",
             "strength": "weak"|"moderate"|"strong"},
            ...
          ],
          "nearest_support": float | None,
          "nearest_resistance": float | None,
          "summary": str
        }
    """
    from scipy.signal import find_peaks
    from sklearn.cluster import KMeans

    from findata.equity_prices import get_equity_prices

    if not isinstance(ticker, str) or not ticker.strip():
        raise ValueError("ticker must be a non-empty string")
    lookback_days = max(30, min(1825, int(lookback_days)))
    n_levels = max(2, min(12, int(n_levels)))
    tolerance_pct = max(0.05, min(5.0, float(tolerance_pct)))

    end = pd.Timestamp.utcnow().normalize()
    start = end - pd.Timedelta(days=lookback_days)

    ticker_u = ticker.strip().upper()
    try:
        df = get_equity_prices(
            tickers=[ticker_u],
            start_date=start.strftime("%Y-%m-%d"),
            end_date=end.strftime("%Y-%m-%d"),
            fields=["High", "Low", "Close"],
        )
    except Exception:
        df = pd.DataFrame()

    if df.empty:
        return {
            "ticker": ticker_u, "current_price": None,
            "lookback_days": lookback_days, "levels": [],
            "nearest_support": None, "nearest_resistance": None,
            "summary": f"No price data returned for {ticker_u}.",
        }

    # Normalise to flat columns regardless of yfinance MultiIndex shape.
    def _series(field: str) -> pd.Series:
        if isinstance(df.columns, pd.MultiIndex):
            try:
                return df[field][ticker_u]
            except Exception:
                return pd.Series(dtype=float)
        return df.get(field, pd.Series(dtype=float))

    high = _series("High").dropna()
    low = _series("Low").dropna()
    close = _series("Close").dropna()
    if close.empty:
        return {
            "ticker": ticker_u, "current_price": None,
            "lookback_days": lookback_days, "levels": [],
            "nearest_support": None, "nearest_resistance": None,
            "summary": f"No usable Close prices for {ticker_u}.",
        }

    current_price = float(close.iloc[-1])
    # Distance metric — peaks must be at least N bars apart so we don't
    # cluster every adjacent bar's micro-wiggle as a separate level.
    distance = max(3, int(len(close) / 60))

    # Find peaks (resistance) and troughs (support) on the High/Low series.
    res_idx, _ = find_peaks(high.values, distance=distance)
    sup_idx, _ = find_peaks(-low.values, distance=distance)

    res_prices = high.iloc[res_idx].values if len(res_idx) else np.array([])
    sup_prices = low.iloc[sup_idx].values if len(sup_idx) else np.array([])

    candidate_prices = np.concatenate([res_prices, sup_prices])
    if len(candidate_prices) < 3:
        return {
            "ticker": ticker_u, "current_price": current_price,
            "lookback_days": lookback_days, "levels": [],
            "nearest_support": None, "nearest_resistance": None,
            "summary": f"Too few peaks/troughs to cluster levels on {ticker_u}.",
        }

    k = min(n_levels, max(2, len(candidate_prices) // 2))
    try:
        km = KMeans(n_clusters=k, n_init="auto", random_state=42)
        km.fit(candidate_prices.reshape(-1, 1))
        centroids = sorted({float(c) for c in km.cluster_centers_.ravel()})
    except Exception:
        # Cluster fit failed — fall back to evenly-spaced quantiles.
        centroids = list(np.quantile(candidate_prices, np.linspace(0, 1, k)))

    # For each centroid, count touches: any High/Low that came within
    # ±tolerance_pct of the centroid.
    levels: list[dict] = []
    for c in centroids:
        band = abs(c) * (tolerance_pct / 100.0)
        # A "touch" is a bar whose low ≤ c+band and high ≥ c-band.
        touched_mask = (low <= c + band) & (high >= c - band)
        touched_dates = touched_mask[touched_mask].index
        touches = int(len(touched_dates))
        last_touch = (
            touched_dates[-1].strftime("%Y-%m-%d") if touches > 0 else None
        )
        level_type = "resistance" if c > current_price else "support"
        levels.append({
            "price": round(float(c), 2),
            "type": level_type,
            "touches": touches,
            "last_touch": last_touch,
            "strength": _classify_strength(touches),
        })

    # Order by relevance: most touches first, then closest to current.
    levels.sort(
        key=lambda x: (-x["touches"], abs(x["price"] - current_price)),
    )

    supports = [lv for lv in levels if lv["type"] == "support"]
    resistances = [lv for lv in levels if lv["type"] == "resistance"]
    nearest_support = (
        max(supports, key=lambda lv: lv["price"])["price"]
        if supports else None
    )
    nearest_resistance = (
        min(resistances, key=lambda lv: lv["price"])["price"]
        if resistances else None
    )

    summary_parts = [
        f"{ticker_u} at ${current_price:.2f}.",
    ]
    if nearest_support is not None:
        s = next(lv for lv in supports if lv["price"] == nearest_support)
        summary_parts.append(
            f"Nearest support ${nearest_support:.2f} "
            f"({s['touches']} touches, {s['strength']})."
        )
    if nearest_resistance is not None:
        r = next(lv for lv in resistances if lv["price"] == nearest_resistance)
        summary_parts.append(
            f"Nearest resistance ${nearest_resistance:.2f} "
            f"({r['touches']} touches, {r['strength']})."
        )

    return {
        "ticker": ticker_u,
        "current_price": current_price,
        "lookback_days": lookback_days,
        "levels": levels,
        "nearest_support": nearest_support,
        "nearest_resistance": nearest_resistance,
        "summary": " ".join(summary_parts),
    }
