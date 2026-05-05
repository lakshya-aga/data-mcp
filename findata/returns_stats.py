"""
findata.returns_stats
---------------------
Quick risk / return statistics from a price history — annualised vol,
beta vs a benchmark, max drawdown, Sharpe.

Pure pandas / numpy on top of ``get_equity_prices``. The agents could
compute these inline from a price call, but they routinely either
forget the annualisation factor or compute beta against the wrong
window. Centralising here gives them a single grounded number.
"""

from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd


def compute_returns_stats(
    ticker: str,
    window_days: int = 252,
    benchmark: Optional[str] = "SPY",
    risk_free_rate: float = 0.0,
) -> pd.Series:
    """
    Annualised vol / beta / max drawdown / Sharpe over a rolling window.

    Pulls the most recent ``window_days`` of daily Close for the ticker
    (and optionally a benchmark), computes log returns, and returns a
    one-row Series with risk metrics. All metrics use the daily-data
    convention (annualisation factor = sqrt(252) for vol, *252 for
    return, etc.).

    Parameters
    ----------
    ticker : str
        Yahoo Finance ticker symbol.
    window_days : int, default 252
        Lookback window in calendar days. Capped at 2520 (10 years).
    benchmark : str or None, default "SPY"
        Benchmark ticker for beta calculation. Set to None to skip
        beta and use only the single-ticker stats.
    risk_free_rate : float, default 0.0
        Annual risk-free rate used in Sharpe. 0.0 by default to keep
        the number unambiguous; pass 0.045 for ~4.5% T-bill yield.

    Returns
    -------
    pandas.Series
        Index: ``ticker``, ``window_days``, ``n_obs``,
        ``annual_return``, ``annual_vol``, ``sharpe``, ``max_drawdown``,
        ``beta``, ``alpha_annual``, ``corr_to_benchmark``.

        ``beta`` / ``alpha_annual`` / ``corr_to_benchmark`` are NaN when
        ``benchmark`` is None or when the benchmark download fails.

    Examples
    --------
    >>> from findata.returns_stats import compute_returns_stats
    >>> stats = compute_returns_stats("AAPL", window_days=252, benchmark="SPY")
    >>> stats[["annual_return", "annual_vol", "sharpe", "max_drawdown", "beta"]]

    Notes
    -----
    Uses simple Close → log return; doesn't adjust for splits or
    dividends beyond yfinance's auto-adjust=True default. For survivor-
    bias-free studies, source from CRSP via ``findata.equity_prices``
    with a different fetcher.
    """
    from findata.equity_prices import get_equity_prices

    if not isinstance(ticker, str) or not ticker.strip():
        raise ValueError("ticker must be a non-empty string")
    window_days = max(20, min(2520, int(window_days)))

    end = pd.Timestamp.utcnow().normalize()
    # Pad the start by 30% so trading-day count comfortably exceeds the
    # requested window even with weekends + holidays.
    start = end - pd.Timedelta(days=int(window_days * 1.4))

    tickers = [ticker.strip().upper()]
    if benchmark:
        tickers.append(benchmark.strip().upper())

    try:
        df = get_equity_prices(
            tickers=tickers,
            start_date=start.strftime("%Y-%m-%d"),
            end_date=end.strftime("%Y-%m-%d"),
            fields=["Close"],
        )
    except Exception:
        df = pd.DataFrame()

    # Normalise the closes Series for the asset and (if any) benchmark.
    asset_t = ticker.strip().upper()
    bench_t = benchmark.strip().upper() if benchmark else None

    def _column(name: str) -> Optional[pd.Series]:
        if df.empty:
            return None
        if isinstance(df.columns, pd.MultiIndex):
            try:
                return df["Close"][name]
            except Exception:
                return None
        return df.get(name) or df.get("Close")

    asset_close = _column(asset_t)
    bench_close = _column(bench_t) if bench_t else None

    out: dict[str, float | str | int] = {
        "ticker": asset_t,
        "window_days": window_days,
        "n_obs": 0,
        "annual_return": float("nan"),
        "annual_vol":    float("nan"),
        "sharpe":        float("nan"),
        "max_drawdown":  float("nan"),
        "beta":          float("nan"),
        "alpha_annual":  float("nan"),
        "corr_to_benchmark": float("nan"),
    }

    if asset_close is None or asset_close.dropna().empty:
        return pd.Series(out)

    s = asset_close.dropna()
    log_r = np.log(s / s.shift(1)).dropna()
    out["n_obs"] = int(len(log_r))
    if len(log_r) < 5:
        return pd.Series(out)

    out["annual_return"] = float(log_r.mean() * 252)
    out["annual_vol"]    = float(log_r.std(ddof=1) * np.sqrt(252))
    if out["annual_vol"] > 0:
        out["sharpe"] = float((out["annual_return"] - risk_free_rate) / out["annual_vol"])

    # Max drawdown on the price-curve cumulative product.
    equity = (1 + log_r.expm1() if hasattr(log_r, "expm1") else (np.exp(log_r) - 1)).add(1).cumprod()
    peak = equity.cummax()
    dd = equity / peak - 1.0
    out["max_drawdown"] = float(dd.min()) if not dd.empty else float("nan")

    # Beta / alpha vs benchmark, when present.
    if bench_close is not None and not bench_close.dropna().empty:
        bench_log_r = np.log(bench_close.dropna() / bench_close.dropna().shift(1)).dropna()
        aligned = pd.concat([log_r, bench_log_r], axis=1, join="inner")
        aligned.columns = ["asset", "bench"]
        aligned = aligned.dropna()
        if len(aligned) >= 5 and aligned["bench"].std(ddof=1) > 0:
            cov = float(aligned["asset"].cov(aligned["bench"]))
            var = float(aligned["bench"].var(ddof=1))
            beta = cov / var if var > 0 else float("nan")
            out["beta"] = float(beta)
            out["corr_to_benchmark"] = float(aligned["asset"].corr(aligned["bench"]))
            # Annualised alpha = mean(asset_r - rf) - beta * mean(bench_r - rf)
            asset_excess = aligned["asset"].mean() * 252 - risk_free_rate
            bench_excess = aligned["bench"].mean() * 252 - risk_free_rate
            if not np.isnan(beta):
                out["alpha_annual"] = float(asset_excess - beta * bench_excess)

    return pd.Series(out)
