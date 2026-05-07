"""
findata.arima_forecast
----------------------
Fit a SARIMA model on a price series via small-grid AIC search, then
generate a forward forecast that the debate agents can use as a
quantitative trend signal.

Why a manual grid + statsmodels (instead of pmdarima.auto_arima)?
  * pmdarima has a long history of binary-wheel mismatches with
    numpy / scipy versions; it breaks on every other release.
  * statsmodels.SARIMAX is in the pinned env already and stable.
  * A 36-cell grid (p,d,q ∈ {0,1,2} × seasonal {0, weekly}) is fast
    enough to run inside a debate turn (~3-8 seconds on a daily
    series) and covers the orders that actually matter for prices.

Returns a structured dict the agent can render as text. No charts —
the chart tool already renders price; this tool's job is the *number*
the agent quotes ("ARIMA(1,1,1) forecasts +2.3% over 20 trading days,
95% CI [-1.1%, +5.7%]").
"""

from __future__ import annotations

import logging
import warnings
from typing import Optional

import numpy as np
import pandas as pd


logger = logging.getLogger(__name__)


# Grid kept deliberately small. Each (p,d,q) combination requires a full
# MLE fit; doubling the grid quadruples runtime. These covers the orders
# that empirically matter for daily equity log-returns.
_P_GRID = (0, 1, 2)
_D_GRID = (0, 1)             # most equity series need d=1 (random walk)
_Q_GRID = (0, 1, 2)
# Seasonal options: none, or weekly (s=5 trading days).
# Add (1,0,1,5) and (0,1,1,5) — the two most common seasonal shapes.
_SEASONAL_OPTIONS = (
    (0, 0, 0, 0),            # non-seasonal
    (1, 0, 1, 5),            # weekly AR/MA
    (0, 1, 1, 5),            # weekly seasonal MA + diff
)


def fit_arima_forecast(
    ticker: str,
    *,
    lookback_days: int = 365,
    forecast_days: int = 20,
    use_log_prices: bool = True,
    max_grid_seconds: float = 30.0,
) -> dict:
    """
    Fit SARIMA via grid-search on close prices and forecast forward.

    Parameters
    ----------
    ticker : str
        Yahoo-format ticker (e.g. "TATATECH.NS", "AAPL").
    lookback_days : int, default 365
        Calendar-day window of history fed to the fitter. Capped 90-1825.
    forecast_days : int, default 20
        Number of trading days to forecast forward. Capped 1-60.
    use_log_prices : bool, default True
        Fit on log-prices then exponentiate the forecast — usually
        produces stabler residuals than fitting on raw price levels.
    max_grid_seconds : float
        Soft budget for grid-search. The fitter aborts the grid as soon
        as cumulative wall-clock exceeds this; the best model found so
        far is used.

    Returns
    -------
    dict
        {
          "ticker": "TATATECH.NS",
          "status": "ok" | "no_data" | "fit_failed",
          "n_observations": int,
          "best_order": [p, d, q],
          "best_seasonal_order": [P, D, Q, s],
          "aic": float,
          "bic": float,
          "in_sample_rmse": float,
          "last_close": float,
          "last_date": "YYYY-MM-DD",
          "forecast": [
              {"date": "YYYY-MM-DD", "mean": float,
               "lower_95": float, "upper_95": float}, ...
          ],
          "forecast_return_pct": float,       # mean[-1] / last_close - 1
          "forecast_return_lower_pct": float, # 95% CI lower
          "forecast_return_upper_pct": float, # 95% CI upper
          "signal": "bullish" | "bearish" | "neutral",
          "summary": "ARIMA(1,1,1) forecasts +2.3% over 20 trading days …",
          "n_grid_evaluated": int,
        }

    On failure (no data / fit blow-ups), ``status`` is non-"ok" and the
    numerical fields are filled with sentinels; ``summary`` carries a
    short human-readable reason.
    """
    import time
    from .equity_prices import get_equity_prices

    ticker_u = (ticker or "").strip().upper()
    if not ticker_u:
        raise ValueError("ticker must be a non-empty string")

    lookback_days = max(90, min(1825, int(lookback_days)))
    forecast_days = max(1, min(60, int(forecast_days)))

    end = pd.Timestamp.utcnow().normalize()
    start = end - pd.Timedelta(days=lookback_days)

    # ── Pull prices ─────────────────────────────────────────────────
    try:
        df = get_equity_prices(
            tickers=[ticker_u],
            start_date=start.strftime("%Y-%m-%d"),
            end_date=end.strftime("%Y-%m-%d"),
        )
    except Exception:
        df = pd.DataFrame()

    close = _extract_close(df, ticker_u)
    if close is None or close.empty or close.notna().sum() < 60:
        return _empty(
            ticker_u, lookback_days, forecast_days,
            status="no_data",
            summary=f"ARIMA skipped — fewer than 60 valid closes for {ticker_u}.",
        )

    close = close.dropna()
    target = np.log(close) if use_log_prices else close.copy()

    # ── Grid search ─────────────────────────────────────────────────
    from statsmodels.tsa.statespace.sarimax import SARIMAX

    best = None
    deadline = time.monotonic() + max_grid_seconds
    n_eval = 0

    for p in _P_GRID:
        for d in _D_GRID:
            for q in _Q_GRID:
                for seas in _SEASONAL_OPTIONS:
                    if time.monotonic() > deadline:
                        break
                    if p == 0 and q == 0 and seas == (0, 0, 0, 0):
                        continue  # degenerate, skip
                    try:
                        with warnings.catch_warnings():
                            warnings.simplefilter("ignore")
                            model = SARIMAX(
                                target,
                                order=(p, d, q),
                                seasonal_order=seas,
                                enforce_stationarity=False,
                                enforce_invertibility=False,
                            )
                            res = model.fit(disp=False, maxiter=50)
                        n_eval += 1
                        aic = float(res.aic)
                        if not np.isfinite(aic):
                            continue
                        if best is None or aic < best["aic"]:
                            best = {
                                "result": res,
                                "order": (p, d, q),
                                "seasonal": seas,
                                "aic": aic,
                                "bic": float(res.bic),
                            }
                    except Exception as e:
                        logger.debug(
                            "SARIMAX (%d,%d,%d)x%s failed for %s: %s",
                            p, d, q, seas, ticker_u, e,
                        )
                        continue
        if time.monotonic() > deadline:
            break

    if best is None:
        return _empty(
            ticker_u, lookback_days, forecast_days,
            status="fit_failed",
            summary=f"ARIMA grid produced no valid fit for {ticker_u}.",
            n_grid_evaluated=n_eval,
        )

    res = best["result"]

    # ── In-sample residual stats ────────────────────────────────────
    fitted = np.asarray(res.fittedvalues)
    actual = np.asarray(target)
    # Drop the burn-in slice the model couldn't predict.
    residual = actual[-len(fitted):] - fitted if len(fitted) <= len(actual) else (actual - fitted)
    in_sample_rmse_log = float(np.sqrt(np.nanmean(residual ** 2)))
    # Convert log-RMSE back to a price-level RMSE proxy when use_log_prices.
    in_sample_rmse = (
        float(np.exp(in_sample_rmse_log) - 1) if use_log_prices else in_sample_rmse_log
    )

    # ── Forecast ────────────────────────────────────────────────────
    try:
        fc = res.get_forecast(steps=forecast_days)
        mean = np.asarray(fc.predicted_mean)
        ci = np.asarray(fc.conf_int(alpha=0.05))
        lower, upper = ci[:, 0], ci[:, 1]
    except Exception as e:
        return _empty(
            ticker_u, lookback_days, forecast_days,
            status="fit_failed",
            summary=f"ARIMA forecast call failed: {type(e).__name__}: {e}",
            n_grid_evaluated=n_eval,
        )

    if use_log_prices:
        mean = np.exp(mean)
        lower = np.exp(lower)
        upper = np.exp(upper)

    # Build trading-day-spaced future index using business-day calendar.
    # NSE/NYSE differ but B-day is a reasonable proxy; the agent doesn't
    # rely on exact calendar matches.
    last_date = close.index[-1]
    future_idx = pd.bdate_range(
        start=last_date + pd.Timedelta(days=1), periods=forecast_days
    )

    last_close = float(close.iloc[-1])
    final_mean = float(mean[-1])
    final_lower = float(lower[-1])
    final_upper = float(upper[-1])

    ret_pct = (final_mean / last_close - 1.0) * 100.0
    ret_lo = (final_lower / last_close - 1.0) * 100.0
    ret_hi = (final_upper / last_close - 1.0) * 100.0

    # Signal: bullish if 95% CI lower bound is positive,
    #         bearish if 95% CI upper bound is negative,
    #         neutral otherwise (CI straddles zero).
    if ret_lo > 0:
        signal = "bullish"
    elif ret_hi < 0:
        signal = "bearish"
    else:
        signal = "neutral"

    p, d, q = best["order"]
    seas = best["seasonal"]
    seas_str = "" if seas == (0, 0, 0, 0) else f"×({seas[0]},{seas[1]},{seas[2]},{seas[3]})"
    summary = (
        f"ARIMA({p},{d},{q}){seas_str} forecasts {ret_pct:+.2f}% over "
        f"{forecast_days} trading days "
        f"(95% CI [{ret_lo:+.2f}%, {ret_hi:+.2f}%]). "
        f"Signal: {signal}. AIC={best['aic']:.1f}."
    )

    forecast_rows = [
        {
            "date": d.strftime("%Y-%m-%d"),
            "mean": float(m),
            "lower_95": float(l),
            "upper_95": float(u),
        }
        for d, m, l, u in zip(future_idx, mean, lower, upper)
    ]

    return {
        "ticker": ticker_u,
        "status": "ok",
        "n_observations": int(close.shape[0]),
        "best_order": [int(p), int(d), int(q)],
        "best_seasonal_order": [int(s) for s in seas],
        "aic": float(best["aic"]),
        "bic": float(best["bic"]),
        "in_sample_rmse": in_sample_rmse,
        "last_close": last_close,
        "last_date": last_date.strftime("%Y-%m-%d"),
        "forecast": forecast_rows,
        "forecast_return_pct": ret_pct,
        "forecast_return_lower_pct": ret_lo,
        "forecast_return_upper_pct": ret_hi,
        "signal": signal,
        "summary": summary,
        "n_grid_evaluated": n_eval,
    }


# ── helpers ────────────────────────────────────────────────────────


def _extract_close(df: pd.DataFrame, ticker_u: str) -> Optional[pd.Series]:
    """Pull a 1-D Close series out of the (possibly MultiIndex) frame."""
    if df is None or df.empty:
        return None
    if isinstance(df.columns, pd.MultiIndex):
        try:
            s = df["Close"][ticker_u]
            return pd.Series(s).astype(float)
        except Exception:
            return None
    if "Close" in df.columns:
        return df["Close"].astype(float)
    if "close" in df.columns:
        return df["close"].astype(float)
    return None


def _empty(
    ticker_u: str,
    lookback_days: int,
    forecast_days: int,
    *,
    status: str,
    summary: str,
    n_grid_evaluated: int = 0,
) -> dict:
    return {
        "ticker": ticker_u,
        "status": status,
        "n_observations": 0,
        "best_order": [0, 0, 0],
        "best_seasonal_order": [0, 0, 0, 0],
        "aic": float("nan"),
        "bic": float("nan"),
        "in_sample_rmse": float("nan"),
        "last_close": float("nan"),
        "last_date": "",
        "forecast": [],
        "forecast_return_pct": float("nan"),
        "forecast_return_lower_pct": float("nan"),
        "forecast_return_upper_pct": float("nan"),
        "signal": "neutral",
        "summary": summary,
        "n_grid_evaluated": n_grid_evaluated,
    }
