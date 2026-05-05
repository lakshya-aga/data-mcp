"""
findata.trend_regime
--------------------
Quantitative trending-vs-mean-reverting classification.

Two complementary readings:
  * **Hurst exponent** — H<0.5 = mean-reverting, H≈0.5 = random walk,
    H>0.5 = persistently trending. Computed via the simple R/S ratio
    over multiple lag windows (no external library).
  * **Linear regression slope** on log(prices) — the annualised drift
    + R². R² says how cleanly the slope fits.

Together they answer: "is the price action trending, and how cleanly?"
A high-Hurst + high-R² name is a momentum candidate; a low-Hurst +
low-R² name is a mean-reversion candidate.
"""

from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd


def _hurst_exponent(prices: pd.Series, max_lag: int = 100) -> Optional[float]:
    """R/S Hurst exponent on a price series. Returns None if the series
    is too short or pathological.
    """
    p = prices.dropna()
    if len(p) < 60:
        return None
    log_p = np.log(p.values)
    diffs = np.diff(log_p)
    if len(diffs) < 30:
        return None
    lags = range(2, min(max_lag, len(diffs) // 2))
    rs: list[tuple[int, float]] = []
    for lag in lags:
        chunked = np.array_split(diffs, max(1, len(diffs) // lag))
        scores = []
        for chunk in chunked:
            if len(chunk) < 2:
                continue
            mean = chunk.mean()
            cum = np.cumsum(chunk - mean)
            r = cum.max() - cum.min()
            s = chunk.std(ddof=0)
            if s > 0:
                scores.append(r / s)
        if scores:
            rs.append((lag, float(np.mean(scores))))
    if len(rs) < 4:
        return None
    log_lags = np.log([x[0] for x in rs])
    log_rs = np.log([x[1] for x in rs])
    if not np.isfinite(log_rs).all():
        return None
    slope, _ = np.polyfit(log_lags, log_rs, 1)
    return float(slope)


def _classify_regime(hurst: Optional[float]) -> str:
    if hurst is None:
        return "unknown"
    if hurst < 0.45: return "mean_reverting"
    if hurst > 0.55: return "trending"
    return "random_walk"


def compute_trend_regime(
    ticker: str,
    window_days: int = 252,
) -> dict:
    """
    Quantify whether a ticker is in a trending or mean-reverting regime.

    Parameters
    ----------
    ticker : str
        Yahoo Finance ticker symbol.
    window_days : int, default 252
        Lookback window. Capped at 2520 (10y), min 60.

    Returns
    -------
    dict
        {
          "ticker": str,
          "window_days": int,
          "n_obs": int,
          "hurst_exponent": float | None,
          "regime": "mean_reverting" | "trending" | "random_walk" | "unknown",
          "linear_slope_annualized": float | None,
          "slope_r2": float | None,
          "summary": str
        }
    """
    from findata.equity_prices import get_equity_prices

    if not isinstance(ticker, str) or not ticker.strip():
        raise ValueError("ticker must be a non-empty string")
    window_days = max(60, min(2520, int(window_days)))

    end = pd.Timestamp.utcnow().normalize()
    start = end - pd.Timedelta(days=int(window_days * 1.2))

    ticker_u = ticker.strip().upper()
    try:
        df = get_equity_prices(
            tickers=[ticker_u],
            start_date=start.strftime("%Y-%m-%d"),
            end_date=end.strftime("%Y-%m-%d"),
            fields=["Close"],
        )
    except Exception:
        df = pd.DataFrame()

    out: dict = {
        "ticker": ticker_u,
        "window_days": window_days,
        "n_obs": 0,
        "hurst_exponent": None,
        "regime": "unknown",
        "linear_slope_annualized": None,
        "slope_r2": None,
        "summary": f"No price data returned for {ticker_u}.",
    }

    if df.empty:
        return out

    if isinstance(df.columns, pd.MultiIndex):
        try:
            close = df["Close"][ticker_u].dropna()
        except Exception:
            return out
    else:
        close = df.get("Close", df.iloc[:, 0]).dropna()

    if len(close) < 60:
        out["summary"] = f"Too few observations on {ticker_u} for regime detection."
        return out

    out["n_obs"] = int(len(close))

    # Hurst
    hurst = _hurst_exponent(close)
    out["hurst_exponent"] = hurst
    out["regime"] = _classify_regime(hurst)

    # Linear regression on log prices: annualised slope + R²
    try:
        log_p = np.log(close.values)
        x = np.arange(len(log_p))
        slope, intercept = np.polyfit(x, log_p, 1)
        # Daily slope → annual
        out["linear_slope_annualized"] = float(slope * 252)
        # R² of fit
        y_hat = slope * x + intercept
        ss_res = np.sum((log_p - y_hat) ** 2)
        ss_tot = np.sum((log_p - log_p.mean()) ** 2)
        out["slope_r2"] = float(1.0 - ss_res / ss_tot) if ss_tot > 0 else None
    except Exception:
        pass

    # Summary
    parts = [f"{ticker_u} {window_days}d:"]
    if hurst is not None:
        parts.append(f"Hurst {hurst:.2f} ({out['regime']})")
    if out["linear_slope_annualized"] is not None:
        slope_pct = out["linear_slope_annualized"] * 100
        parts.append(f"linear drift {slope_pct:+.1f}%/yr")
    if out["slope_r2"] is not None:
        parts.append(f"R² {out['slope_r2']:.2f}")
    out["summary"] = ". ".join(parts) + "."
    return out
