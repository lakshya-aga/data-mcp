"""
findata.ohlc_chart
------------------
Render an OHLC candlestick chart with optional support/resistance lines
and trend-indicator overlays. Returns the PNG as base64 + a markdown
``![alt](data:image/png;base64,...)`` snippet ready for inline embed
in chat / debate transcripts.

Built on mplfinance (matplotlib under the hood). Self-contained — no
external services, no auth.
"""

from __future__ import annotations

import base64
import io
from typing import Optional

import pandas as pd


def _normalise_dtindex(idx: pd.Index) -> pd.DatetimeIndex:
    """Force a DatetimeIndex to naive ns-resolution.

    yfinance hands back inconsistent dtypes:
      - datetime64[ns]                     — equity (.NS, US)
      - datetime64[s]                      — newer BTC-USD / crypto data
      - datetime64[ns, UTC]                — some crypto with tz
      - datetime64[s, UTC]                 — both axes wrong at once
    Comparing any of these against a Timestamp from a different (unit, tz)
    quadrant raises ``Invalid comparison`` in pandas 2.2+. Force everything
    to the same canonical frame so call sites don't have to guess.
    """
    if not isinstance(idx, pd.DatetimeIndex):
        idx = pd.DatetimeIndex(idx)
    if idx.tz is not None:
        idx = idx.tz_localize(None)
    if hasattr(idx, "as_unit"):
        idx = idx.as_unit("ns")
    elif idx.dtype != "datetime64[ns]":
        # Older pandas without as_unit — coerce via numpy.
        idx = pd.DatetimeIndex(idx.values.astype("datetime64[ns]"))
    return idx


def _normalise_timestamp(ts: pd.Timestamp) -> pd.Timestamp:
    """Force a Timestamp to naive ns-resolution. Mirror of _normalise_dtindex."""
    if not isinstance(ts, pd.Timestamp):
        ts = pd.Timestamp(ts)
    if ts.tz is not None:
        ts = ts.tz_localize(None)
    if hasattr(ts, "as_unit"):
        ts = ts.as_unit("ns")
    return ts


def plot_ohlc_chart(
    ticker: str,
    lookback_days: int = 252,
    with_sr: bool = True,
    with_indicators: bool = True,
    style: str = "yahoo",
    width_px: int = 1100,
    height_px: int = 650,
) -> dict:
    """
    Render an OHLC candlestick chart for a single ticker.

    Parameters
    ----------
    ticker : str
        Yahoo Finance ticker symbol.
    lookback_days : int, default 252
        Calendar-day window. Capped at 1825 (5y), min 30.
    with_sr : bool, default True
        Overlay algorithmic support/resistance horizontal lines.
        Requires the support_resistance module — falls back gracefully
        on failure.
    with_indicators : bool, default True
        Overlay 50-day + 200-day SMAs + an RSI subplot.
    style : str, default "yahoo"
        mplfinance style name. Common: "yahoo", "charles", "nightclouds",
        "blueskies".
    width_px / height_px : int
        PNG output dimensions. Defaults size well for inline chat embed.

    Returns
    -------
    dict
        {
          "ticker": str,
          "lookback_days": int,
          "title": str,
          "summary": str,
          "image_base64": "...",
          "markdown_image": "![title](data:image/png;base64,...)",
          "params": {...},
        }

        On failure (no data / mplfinance crash), returns the same shape
        with ``image_base64`` = "" and an explanatory ``summary``.
    """
    import mplfinance as mpf
    import matplotlib
    matplotlib.use("Agg")  # non-interactive backend; no Tk/Qt needed
    import matplotlib.pyplot as plt
    from findata.equity_prices import get_equity_prices

    if not isinstance(ticker, str) or not ticker.strip():
        raise ValueError("ticker must be a non-empty string")
    lookback_days = max(30, min(1825, int(lookback_days)))
    width_px = max(400, min(2000, int(width_px)))
    height_px = max(300, min(1200, int(height_px)))

    end = pd.Timestamp.utcnow().normalize()
    # Pad start so SMAs warm up by the leftmost visible bar.
    start = end - pd.Timedelta(days=int(lookback_days * 1.5))

    ticker_u = ticker.strip().upper()
    try:
        df = get_equity_prices(
            tickers=[ticker_u],
            start_date=start.strftime("%Y-%m-%d"),
            end_date=end.strftime("%Y-%m-%d"),
        )
    except Exception:
        df = pd.DataFrame()

    title = f"{ticker_u} · {lookback_days}d OHLC"
    if df.empty:
        # yfinance rate-limit + delisted tickers + ticker-not-found all
        # land here. Hand the agent a paste-ready italic fallback so
        # it can keep going with text-only analysis without inventing
        # an apologetic "unexpected error" message.
        msg = f"No price data returned for {ticker_u} (yfinance empty / rate-limited)."
        return {
            "ticker": ticker_u, "lookback_days": lookback_days, "title": title,
            "summary": msg,
            "image_base64": "",
            "markdown_image": f"*Chart unavailable for {ticker_u}: {msg}*",
            "chart_status": "no_data",
            "params": {"with_sr": with_sr, "with_indicators": with_indicators},
        }

    # Normalise to flat OHLC frame with capitalised columns (mplfinance
    # expects Open/High/Low/Close/Volume).
    if isinstance(df.columns, pd.MultiIndex):
        ohlc = pd.DataFrame()
        for f in ("Open", "High", "Low", "Close", "Volume"):
            try:
                ohlc[f] = df[f][ticker_u]
            except Exception:
                pass
    else:
        ohlc = df.copy()
        ohlc.columns = [str(c).title() for c in ohlc.columns]
    if "Close" not in ohlc.columns:
        msg = f"OHLC frame missing Close column for {ticker_u}."
        return {
            "ticker": ticker_u, "lookback_days": lookback_days, "title": title,
            "summary": msg,
            "image_base64": "",
            "markdown_image": f"*Chart unavailable for {ticker_u}: {msg}*",
            "chart_status": "schema_error",
            "params": {"with_sr": with_sr, "with_indicators": with_indicators},
        }

    # Trim to the visible window (after using padded data for SMA warmup).
    # Two axes of mismatch can break the comparison `index >= cutoff` in
    # modern pandas:
    #
    #   1. tz-awareness: pd.Timestamp.utcnow() is tz-aware (UTC) but
    #      yfinance returns naive indexes for .NS tickers and aware
    #      indexes for some crypto tickers (BTC-USD).
    #   2. resolution:   yfinance has migrated to datetime64[s] for some
    #      tickers (BTC-USD as of recent versions), but Timestamp objects
    #      default to datetime64[ns]. pandas 2.2+ raises
    #      "Invalid comparison between dtype=datetime64[s] and Timestamp"
    #      when the units differ.
    #
    # Belt-and-braces fix: force both the index AND the cutoff to a
    # canonical "naive nanosecond" reference frame before comparing. This
    # is idempotent on already-correct frames and survives whatever
    # yfinance throws at us.
    cutoff = end - pd.Timedelta(days=lookback_days)
    ohlc.index = _normalise_dtindex(ohlc.index)
    cutoff = _normalise_timestamp(cutoff)
    ohlc_visible = ohlc.loc[ohlc.index >= cutoff].copy()
    if ohlc_visible.empty:
        ohlc_visible = ohlc.tail(lookback_days)

    # ── Build overlay layers ──
    addplots = []
    summary_bits: list[str] = [title]

    if with_indicators:
        try:
            sma_50 = ohlc["Close"].rolling(50).mean()
            sma_200 = ohlc["Close"].rolling(200).mean()
            addplots.append(mpf.make_addplot(
                sma_50.reindex(ohlc_visible.index),
                color="#3b82f6", width=1.0, alpha=0.85,
            ))
            addplots.append(mpf.make_addplot(
                sma_200.reindex(ohlc_visible.index),
                color="#a855f7", width=1.0, alpha=0.85,
            ))
            summary_bits.append("50/200d SMA overlay.")
        except Exception:
            pass

    # RSI panel
    rsi_panel_kwargs: dict = {}
    if with_indicators:
        try:
            import pandas_ta as ta
            rsi = ta.rsi(ohlc["Close"], length=14)
            rsi_v = rsi.reindex(ohlc_visible.index)
            if rsi_v.notna().any():
                addplots.append(mpf.make_addplot(
                    rsi_v, panel=1, color="#06b6d4", width=1.0, ylabel="RSI",
                ))
                # Overbought/oversold reference lines via constant series
                addplots.append(mpf.make_addplot(
                    pd.Series(70, index=rsi_v.index), panel=1,
                    color="#ef4444", width=0.6, linestyle="--",
                ))
                addplots.append(mpf.make_addplot(
                    pd.Series(30, index=rsi_v.index), panel=1,
                    color="#22c55e", width=0.6, linestyle="--",
                ))
                rsi_panel_kwargs = {"num_panels": 2, "panel_ratios": (3, 1)}
                summary_bits.append("RSI(14) subplot.")
        except Exception:
            pass

    # Support/resistance horizontal lines
    sr_levels: list[dict] = []
    if with_sr:
        try:
            from findata.support_resistance import compute_support_resistance
            sr = compute_support_resistance(
                ticker_u, lookback_days=lookback_days, n_levels=5,
            )
            sr_levels = sr.get("levels") or []
        except Exception:
            sr_levels = []

    hlines_arg = None
    if sr_levels:
        prices = [lv["price"] for lv in sr_levels]
        colors = [
            "#ef4444" if lv["type"] == "resistance" else "#22c55e"
            for lv in sr_levels
        ]
        hlines_arg = dict(hlines=prices, colors=colors, linewidths=0.8, linestyle=":")
        summary_bits.append(f"{len(sr_levels)} S/R levels.")

    # ── Render to PNG buffer ──
    # Newer mplfinance (>=0.12.10b1 or so) rejects ``addplot=None``
    # outright — its validator wants dict / list / omitted-entirely.
    # Build the kwargs dict and OMIT addplot when empty rather than
    # passing None. Same for hlines and the RSI-panel split.
    buf = io.BytesIO()
    try:
        mpf_kwargs = dict(
            type="candle",
            style=style,
            volume=False,  # subtract from real estate; users can opt in later
            figsize=(width_px / 100, height_px / 100),
            tight_layout=True,
            savefig=dict(fname=buf, dpi=100, bbox_inches="tight"),
            show_nontrading=False,
            ylabel="Price",
            datetime_format="%Y-%m-%d",
        )
        if addplots:
            mpf_kwargs["addplot"] = addplots
        mpf_kwargs.update(rsi_panel_kwargs)
        if hlines_arg:
            mpf_kwargs["hlines"] = hlines_arg
        mpf.plot(ohlc_visible, **mpf_kwargs)
        plt.close("all")
    except Exception as exc:
        plt.close("all")
        msg = f"mplfinance render failed: {type(exc).__name__}: {exc}"
        return {
            "ticker": ticker_u, "lookback_days": lookback_days, "title": title,
            "summary": msg,
            "image_base64": "",
            "markdown_image": f"*Chart unavailable for {ticker_u}: {msg}*",
            "chart_status": "render_error",
            "params": {"with_sr": with_sr, "with_indicators": with_indicators},
        }

    raw = buf.getvalue()
    if not raw:
        msg = "Chart rendered empty buffer."
        return {
            "ticker": ticker_u, "lookback_days": lookback_days, "title": title,
            "summary": msg,
            "image_base64": "",
            "markdown_image": f"*Chart unavailable for {ticker_u}: {msg}*",
            "chart_status": "empty_buffer",
            "params": {"with_sr": with_sr, "with_indicators": with_indicators},
        }

    b64 = base64.b64encode(raw).decode("ascii")
    md_alt = title.replace("[", "(").replace("]", ")")
    md = f"![{md_alt}](data:image/png;base64,{b64})"

    return {
        "ticker": ticker_u,
        "lookback_days": lookback_days,
        "title": title,
        "summary": " ".join(summary_bits),
        "image_base64": b64,
        "markdown_image": md,
        "chart_status": "ok",
        "params": {
            "with_sr": with_sr,
            "with_indicators": with_indicators,
            "style": style,
            "width_px": width_px,
            "height_px": height_px,
            "n_sr_levels": len(sr_levels),
        },
    }
