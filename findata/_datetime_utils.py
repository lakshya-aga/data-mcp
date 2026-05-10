"""
findata._datetime_utils
-----------------------
Shared helpers for normalising heterogeneous datetime indexes + cutoffs
to a canonical "naive nanosecond" reference frame.

Why this exists: yfinance + FRED + various other upstreams hand back
DatetimeIndexes in any of four (tz × resolution) quadrants:

    datetime64[ns]                      — equity (.NS, US)
    datetime64[s]                       — newer BTC-USD / crypto
    datetime64[ns, UTC]                 — some crypto with tz
    datetime64[s, UTC]                  — both wrong at once

Comparing any of these against a ``Timestamp`` from a DIFFERENT
quadrant raises ``TypeError: Invalid comparison between dtype=...``
in pandas 2.2+. Every findata module that does temporal filtering
(``df.loc[df.index >= cutoff]``) hits this — first symptomatic in
``ohlc_chart`` (LTTS.NS, BTC-USD), then in
``candlestick_patterns``, then in others.

Centralising the normalisation here means: a) the fix is one-place,
b) future findata modules can't replicate the bug accidentally,
c) the test surface is single.
"""

from __future__ import annotations

import pandas as pd


def normalise_dtindex(idx) -> pd.DatetimeIndex:
    """Coerce a DatetimeIndex (or anything indexable that pd.DatetimeIndex
    can wrap) to **naive nanosecond resolution**.

    Idempotent on already-correct inputs. Handles every (tz × unit)
    combination yfinance / FRED / pandas-datareader emit.
    """
    if not isinstance(idx, pd.DatetimeIndex):
        idx = pd.DatetimeIndex(idx)
    if idx.tz is not None:
        idx = idx.tz_localize(None)
    if hasattr(idx, "as_unit"):
        idx = idx.as_unit("ns")
    elif idx.dtype != "datetime64[ns]":
        idx = pd.DatetimeIndex(idx.values.astype("datetime64[ns]"))
    return idx


def normalise_timestamp(ts) -> pd.Timestamp:
    """Coerce a scalar Timestamp / datetime / string to **naive
    nanosecond resolution**. Mirrors normalise_dtindex.
    """
    if not isinstance(ts, pd.Timestamp):
        ts = pd.Timestamp(ts)
    if ts.tz is not None:
        ts = ts.tz_localize(None)
    if hasattr(ts, "as_unit"):
        ts = ts.as_unit("ns")
    return ts


def align_for_comparison(idx, cutoff):
    """Convenience: return (normalised_idx, normalised_cutoff) ready for
    safe ``idx >= cutoff`` comparisons. Apply once at the top of any
    function that does index-based slicing on yfinance / FRED data."""
    return normalise_dtindex(idx), normalise_timestamp(cutoff)
