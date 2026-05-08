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
    # Tz alignment matters here: pd.Timestamp.utcnow() produces a tz-aware
    # UTC Timestamp, but yfinance returns a NAIVE datetime64[ns] index for
    # .NS tickers. Comparing naive↔aware raises in modern pandas
    # ("Invalid comparison between dtype=datetime64[s] and Timestamp").
    # Normalise both sides to whichever convention the index uses.
    cutoff = end - pd.Timedelta(days=lookback_days)
    idx_tz = getattr(ohlc.index, "tz", None)
    if idx_tz is None:
        # Naive index — strip tz from cutoff if it has one.
        if getattr(cutoff, "tz", None) is not None:
            cutoff = cutoff.tz_localize(None)
    else:
        # Aware index — match its tz on cutoff.
        if getattr(cutoff, "tz", None) is None:
            cutoff = cutoff.tz_localize(idx_tz)
        else:
            cutoff = cutoff.tz_convert(idx_tz)
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
    buf = io.BytesIO()
    try:
        mpf_kwargs = dict(
            type="candle",
            style=style,
            volume=False,  # subtract from real estate; users can opt in later
            addplot=addplots if addplots else None,
            figsize=(width_px / 100, height_px / 100),
            tight_layout=True,
            savefig=dict(fname=buf, dpi=100, bbox_inches="tight"),
            show_nontrading=False,
            ylabel="Price",
            datetime_format="%Y-%m-%d",
        )
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
