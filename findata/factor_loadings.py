"""
findata.factor_loadings
-----------------------
Run a Fama-French factor regression for a single ticker and return its
factor loadings + alpha + interpretation. Higher-level than
``findata.fama_french.get_fama_french_factors`` (which returns raw
factor return series) — this is what the trading-panel's Fundamentals
Analyst calls to answer "what kind of stock is this?" in one tool call.

Output is a structured dict with named loadings, t-stats, R², and a
plain-English interpretation table the analyst can paste into its
Risk Stats / Factor Exposures section without further reasoning.

Default: Fama-French 5-factor model (Mkt-RF / SMB / HML / RMW / CMA)
fit on daily returns over a 504-day rolling window. Pass
``factor_model="3"`` for the classic 3-factor model.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Any, Optional


logger = logging.getLogger(__name__)


# Plain-English read of each factor's sign + magnitude.
_FACTOR_INTERPRETATION: dict[str, dict[str, Any]] = {
    "Mkt-RF": {
        "label": "Market beta",
        "high": "more volatile than the market",
        "low": "defensive vs market",
        "neutral": "tracks market",
        "threshold_high": 1.1,
        "threshold_low": 0.9,
    },
    "SMB": {
        "label": "Size",
        "high": "small-cap tilt (returns track smaller stocks)",
        "low": "large-cap tilt (returns track larger stocks)",
        "neutral": "size-neutral",
        "threshold_high": 0.2,
        "threshold_low": -0.2,
    },
    "HML": {
        "label": "Value vs growth",
        "high": "value tilt (returns track high book-to-market)",
        "low": "growth tilt (returns track low book-to-market)",
        "neutral": "value/growth-neutral",
        "threshold_high": 0.2,
        "threshold_low": -0.2,
    },
    "RMW": {
        "label": "Profitability",
        "high": "quality tilt (high-profitability bias)",
        "low": "low-profitability bias",
        "neutral": "profitability-neutral",
        "threshold_high": 0.2,
        "threshold_low": -0.2,
    },
    "CMA": {
        "label": "Investment",
        "high": "conservative-investment tilt (low-capex bias)",
        "low": "aggressive-investment tilt (high-capex bias)",
        "neutral": "investment-neutral",
        "threshold_high": 0.2,
        "threshold_low": -0.2,
    },
}


# Ticker-suffix → Ken French region mapping. yfinance suffix conventions.
# When region="auto" the loadings function picks the best regional FF
# basket for the ticker. Falls back to US for symbols with no suffix.
_SUFFIX_TO_REGION: list[tuple[str, str]] = [
    (".NS", "AsiaPacificExJapan"),   # India NSE
    (".BO", "AsiaPacificExJapan"),   # India BSE
    (".HK", "AsiaPacificExJapan"),   # Hong Kong
    (".SS", "EmergingMarkets"),       # Shanghai
    (".SZ", "EmergingMarkets"),       # Shenzhen
    (".T",  "Japan"),                  # Tokyo
    (".JP", "Japan"),
    (".KS", "AsiaPacificExJapan"),    # Korea
    (".KQ", "AsiaPacificExJapan"),
    (".TW", "AsiaPacificExJapan"),    # Taiwan
    (".SI", "AsiaPacificExJapan"),    # Singapore
    (".AX", "AsiaPacificExJapan"),    # Australia
    (".NZ", "AsiaPacificExJapan"),    # New Zealand
    (".L",  "Europe"),                 # London
    (".PA", "Europe"),                 # Paris
    (".DE", "Europe"),                 # Frankfurt / Xetra
    (".AS", "Europe"),                 # Amsterdam
    (".MI", "Europe"),                 # Milan
    (".SW", "Europe"),                 # Swiss
    (".MC", "Europe"),                 # Madrid
    (".OL", "Europe"),                 # Oslo
    (".ST", "Europe"),                 # Stockholm
    (".CO", "Europe"),                 # Copenhagen
    (".HE", "Europe"),                 # Helsinki
    (".SA", "EmergingMarkets"),        # Sao Paulo
    (".MX", "EmergingMarkets"),        # Mexico
    (".JO", "EmergingMarkets"),        # Johannesburg
    (".IS", "EmergingMarkets"),        # Istanbul
    (".TO", "NorthAmerica"),           # Toronto (Canada)
    (".V",  "NorthAmerica"),           # TSX Venture
]


def auto_region_for_ticker(ticker: str) -> str:
    """Pick the Ken-French region most appropriate for a yfinance ticker.

    Returns the canonical region key. Defaults to ``"US"`` for symbols
    with no recognised suffix.
    """
    if not ticker:
        return "US"
    upper = ticker.strip().upper()
    for suffix, region in _SUFFIX_TO_REGION:
        if upper.endswith(suffix):
            return region
    return "US"


def get_factor_loadings(
    ticker: str,
    factor_model: str = "5",
    region: str = "auto",
    window_days: int = 504,
    annualisation_days: int = 252,
) -> dict[str, Any]:
    """Run a Fama-French regression and return the ticker's factor profile.

    Parameters
    ----------
    ticker : str
        Yahoo-format ticker.
    factor_model : "3" | "5", default "5"
        FF3 (Mkt/SMB/HML) or FF5 (+RMW + CMA).
    region : str, default ``"auto"``
        ``"auto"`` selects the appropriate Ken-French region from the
        ticker's suffix (.NS / .BO → AsiaPacificExJapan, .L / .PA →
        Europe, .T → Japan, .SS / .SZ → EmergingMarkets, etc.; default
        US for unsuffixed symbols). Or pass an explicit region name:
        US / Developed / DevelopedExUS / Europe / Japan /
        AsiaPacificExJapan / NorthAmerica / EmergingMarkets.
    window_days : int, default 504
        Rolling window of daily returns to fit on. ~2 trading years.
    annualisation_days : int
        Trading days/year for annualising alpha. Default 252.

    Returns
    -------
    dict::

        {
          "ticker": "AAPL", "factor_model": "FF5",
          "window_days": 504, "n_observations": 503,
          "as_of": "YYYY-MM-DD",
          "alpha_daily_pct": 0.012,        # daily intercept × 100
          "alpha_annual_pct": 3.04,        # annualised
          "alpha_t_stat": 1.42,
          "loadings": [
            {"factor": "Mkt-RF", "label": "Market beta",
             "beta": 1.15, "t_stat": 18.2, "interpretation": "..."},
            ...
          ],
          "r_squared": 0.78,
          "summary": "Apple is a large-cap growth name with a quality
                       tilt. Market beta 1.15 (modestly above 1.0).
                       Annual alpha +3.0%, t=1.42 — not statistically
                       significant.",
          "status": "ok",
        }

    On data failure (ticker missing prices, FF endpoint unreachable,
    etc.) returns ``{"status": "no_data", "summary": "...", ...}``
    with the underlying error.
    """
    # Lazy imports — keeps the rest of findata loadable on systems
    # without statsmodels installed.
    try:
        import numpy as np
        import pandas as pd
        import statsmodels.api as sm
        from .fama_french import get_fama_french_factors
        from .equity_prices import get_equity_prices
    except ImportError as e:
        return _empty(ticker, factor_model, window_days,
                      summary=f"factor-loadings unavailable: {e}")

    ticker_u = (ticker or "").strip().upper()
    if not ticker_u:
        raise ValueError("ticker must be non-empty")

    if factor_model not in ("3", "5"):
        raise ValueError(f"factor_model must be '3' or '5', got {factor_model!r}")

    # Resolve "auto" → suffix-derived region. Otherwise pass explicit
    # region through (validated by get_fama_french_factors).
    resolved_region = (
        auto_region_for_ticker(ticker_u) if region == "auto" else region
    )

    end = date.today()
    # Pad start by ~30% so weekend / holiday gaps don't starve the
    # window. We trim to the requested length after the FF + price
    # joins line up.
    start = end - timedelta(days=int(window_days * 1.5))

    # ── Fetch factors + price ──────────────────────────────────────
    try:
        factors = get_fama_french_factors(
            factor_model=factor_model,
            region=resolved_region,
            start_date=start.strftime("%Y-%m-%d"),
            end_date=end.strftime("%Y-%m-%d"),
            as_decimal=True,
        )
    except Exception as e:
        logger.warning("FF factors fetch failed for %s region=%s: %s",
                       ticker_u, resolved_region, e)
        return _empty(ticker_u, factor_model, window_days,
                      region=resolved_region,
                      summary=f"FF factors fetch failed (region={resolved_region}): {type(e).__name__}: {e}")

    try:
        prices = get_equity_prices(
            tickers=[ticker_u],
            start_date=start.strftime("%Y-%m-%d"),
            end_date=end.strftime("%Y-%m-%d"),
            fields=["Close"],
        )
    except Exception as e:
        logger.warning("price fetch failed for %s: %s", ticker_u, e)
        return _empty(ticker_u, factor_model, window_days,
                      region=resolved_region,
                      summary=f"price fetch failed: {type(e).__name__}: {e}")

    # Normalise price to a 1-D Close series.
    close = _extract_close(prices, ticker_u)
    if close is None or close.empty or len(close) < 60:
        return _empty(ticker_u, factor_model, window_days,
                      region=resolved_region,
                      summary=f"insufficient price history for {ticker_u} ({len(close) if close is not None else 0} obs)")

    # ── Compute excess returns + align ─────────────────────────────
    ticker_ret = close.pct_change().dropna()
    # Make both indexes naive ns-resolution so the join works regardless
    # of yfinance / FF dtype quirks.
    ticker_ret.index = pd.DatetimeIndex(ticker_ret.index).tz_localize(None)
    factors.index = pd.DatetimeIndex(factors.index).tz_localize(None)

    # The FF table has an "RF" column (risk-free rate, daily). Build
    # excess return = ticker_ret - RF. Then regress on the factor
    # columns excluding RF.
    if "RF" not in factors.columns:
        return _empty(ticker_u, factor_model, window_days,
                      region=resolved_region,
                      summary="FF table missing RF column")

    joined = pd.concat([ticker_ret.rename("ret"), factors], axis=1).dropna()
    if len(joined) < 60:
        return _empty(ticker_u, factor_model, window_days,
                      region=resolved_region,
                      summary=f"insufficient overlap between FF + price ({len(joined)} obs)")

    # Trim to the requested window (most-recent N observations).
    joined = joined.tail(int(window_days))

    excess_ret = joined["ret"] - joined["RF"]
    factor_cols = [c for c in joined.columns if c not in ("ret", "RF")]
    X = sm.add_constant(joined[factor_cols])
    y = excess_ret

    # ── OLS fit ─────────────────────────────────────────────────────
    try:
        model = sm.OLS(y, X).fit()
    except Exception as e:
        return _empty(ticker_u, factor_model, window_days,
                      region=resolved_region,
                      summary=f"OLS regression failed: {type(e).__name__}: {e}")

    # ── Extract + interpret ────────────────────────────────────────
    alpha_daily = float(model.params.get("const", 0.0))
    alpha_t = float(model.tvalues.get("const", 0.0))
    alpha_annual = (1 + alpha_daily) ** annualisation_days - 1

    loadings: list[dict[str, Any]] = []
    for factor in factor_cols:
        beta = float(model.params[factor])
        t = float(model.tvalues[factor])
        meta = _FACTOR_INTERPRETATION.get(factor, {})
        if not meta:
            interp = ""
        elif beta >= meta["threshold_high"]:
            interp = meta["high"]
        elif beta <= meta["threshold_low"]:
            interp = meta["low"]
        else:
            interp = meta["neutral"]
        loadings.append({
            "factor": factor,
            "label": meta.get("label", factor),
            "beta": round(beta, 3),
            "t_stat": round(t, 2),
            "interpretation": interp,
        })

    r_squared = float(model.rsquared)
    summary = _build_summary(
        ticker_u, factor_model, resolved_region, loadings,
        alpha_annual=alpha_annual * 100,
        alpha_t=alpha_t,
        r_squared=r_squared,
    )

    return {
        "ticker": ticker_u,
        "factor_model": f"FF{factor_model}-{resolved_region}",
        "region": resolved_region,
        "window_days": int(window_days),
        "n_observations": int(len(joined)),
        "as_of": end.strftime("%Y-%m-%d"),
        "alpha_daily_pct": round(alpha_daily * 100, 4),
        "alpha_annual_pct": round(alpha_annual * 100, 2),
        "alpha_t_stat": round(alpha_t, 2),
        "loadings": loadings,
        "r_squared": round(r_squared, 3),
        "summary": summary,
        "status": "ok",
        "disclaimer": _disclaimer_for_region(resolved_region),
    }


def _disclaimer_for_region(region: str) -> str:
    """Honest disclaimer about what each Ken French regional basket
    actually contains — used so the agent doesn't overclaim."""
    if region == "US":
        return "US Fama-French factors. Direct fit for US-listed equities."
    if region == "AsiaPacificExJapan":
        return (
            "Asia Pacific ex Japan FF basket — includes India + Hong Kong "
            "+ Singapore + Australia + Korea + Taiwan + Indonesia + "
            "Philippines + Malaysia + Thailand + New Zealand. India is the "
            "largest constituent, so this is the closest fit for .NS / .BO "
            "tickers among the Ken-French regional sets — but it remains "
            "regional exposure, not a pure-India factor model."
        )
    if region == "EmergingMarkets":
        return (
            "Emerging Markets FF basket — broad EM aggregate. Loadings "
            "describe exposure to EM-aggregate patterns, not local-market "
            "factors."
        )
    if region == "Japan":
        return "Japan-only FF factors. Direct fit for .T / .JP tickers."
    if region == "Europe":
        return "Europe-only FF factors. Direct fit for European listings."
    if region == "NorthAmerica":
        return "North America FF factors (US + Canada). Suitable for .TO / .V tickers."
    return f"Ken French {region} factor basket — regional exposure, not a pure single-country model."


# ── Helpers ────────────────────────────────────────────────────────


def _extract_close(prices, ticker_u: str):
    """Pull a 1-D Close series from a yfinance-shaped frame."""
    import pandas as pd
    if prices is None or prices.empty:
        return None
    if isinstance(prices.columns, pd.MultiIndex):
        try:
            return prices["Close"][ticker_u].astype(float).dropna()
        except Exception:
            return None
    if "Close" in prices.columns:
        return prices["Close"].astype(float).dropna()
    if "close" in prices.columns:
        return prices["close"].astype(float).dropna()
    return None


def _build_summary(
    ticker: str, factor_model: str, region: str, loadings: list[dict[str, Any]],
    alpha_annual: float, alpha_t: float, r_squared: float,
) -> str:
    """Compose a one-paragraph human-readable summary."""
    by_factor = {l["factor"]: l for l in loadings}
    bits: list[str] = []

    # Size
    if "SMB" in by_factor:
        b = by_factor["SMB"]["beta"]
        if b >= 0.2:
            bits.append("small-cap tilt")
        elif b <= -0.2:
            bits.append("large-cap tilt")
        else:
            bits.append("size-neutral")

    # Value/growth
    if "HML" in by_factor:
        b = by_factor["HML"]["beta"]
        if b >= 0.2:
            bits.append("value tilt")
        elif b <= -0.2:
            bits.append("growth tilt")
        else:
            bits.append("value/growth-neutral")

    # Quality (RMW only in FF5)
    if "RMW" in by_factor:
        b = by_factor["RMW"]["beta"]
        if b >= 0.2:
            bits.append("quality tilt")
        elif b <= -0.2:
            bits.append("low-profitability bias")

    # Market
    mkt_beta = by_factor.get("Mkt-RF", {}).get("beta", 1.0)

    # Alpha significance — |t| > 2 is the classic rule of thumb.
    if abs(alpha_t) > 2:
        alpha_phrase = (
            f"annual alpha {alpha_annual:+.1f}%, t={alpha_t:.2f} — "
            f"statistically significant"
        )
    else:
        alpha_phrase = (
            f"annual alpha {alpha_annual:+.1f}%, t={alpha_t:.2f} — "
            f"not statistically significant"
        )

    style = " · ".join(bits) if bits else "style-neutral"
    return (
        f"{ticker} {style}. Market beta {mkt_beta:.2f}. "
        f"{alpha_phrase}. R² = {r_squared:.2f} (FF{factor_model}-{region})."
    )


def _empty(
    ticker: str, factor_model: str, window_days: int,
    *, summary: str, region: str = "US",
) -> dict[str, Any]:
    return {
        "ticker": ticker,
        "factor_model": f"FF{factor_model}-{region}",
        "region": region,
        "window_days": int(window_days),
        "n_observations": 0,
        "as_of": "",
        "alpha_daily_pct": 0.0,
        "alpha_annual_pct": 0.0,
        "alpha_t_stat": 0.0,
        "loadings": [],
        "r_squared": 0.0,
        "summary": summary,
        "status": "no_data",
    }
