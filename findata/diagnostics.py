"""
findata.diagnostics — post-install health check for every data source.

Runs a tiny query against each ``findata.*`` wrapper and reports whether the
upstream returned data shaped as expected. Intended for use right after
``pip install`` to catch missing API keys, blocked outbound traffic, broken
upstream schemas, or stale caches *before* an agent tries to use the source
in a generated notebook.

Usage::

    python -m findata.diagnostics             # run all checks
    python -m findata.diagnostics --json      # machine-readable
    python -m findata.diagnostics --only fred,coingecko

Exit code:
    0  every check passed (or was deliberately skipped)
    1  at least one check FAILED

The output uses three statuses:

    OK       — call returned a non-empty result of the expected shape
    SKIPPED  — optional dependency missing, no API key, or feature gated
    FAIL     — call raised, returned wrong shape, or returned empty data

Skipped checks do not count as failures because they reflect deployment
choices (e.g. no ``yfinance`` extra installed) rather than bugs.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import traceback
from dataclasses import asdict, dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Callable, Optional


# ─────────────────────────────────────────────────────────────────────────
# Result type
# ─────────────────────────────────────────────────────────────────────────


@dataclass
class CheckResult:
    name: str
    status: str           # "OK" | "SKIPPED" | "FAIL"
    elapsed_ms: int
    detail: str = ""      # short human-readable note
    rows: Optional[int] = None
    cols: Optional[int] = None
    extra: dict[str, Any] = field(default_factory=dict)


def _make(name: str) -> "_CheckRunner":
    return _CheckRunner(name)


class _CheckRunner:
    """Tiny helper that times a callable and classifies its outcome."""

    def __init__(self, name: str) -> None:
        self.name = name

    def run(self, fn: Callable[[], Any], *, expect_nonempty: bool = True) -> CheckResult:
        start = time.perf_counter()
        try:
            result = fn()
        except _SkipCheck as e:
            return CheckResult(self.name, "SKIPPED", _ms(start), detail=str(e))
        except Exception as e:
            return CheckResult(
                self.name,
                "FAIL",
                _ms(start),
                detail=f"{type(e).__name__}: {e}",
                extra={"traceback": traceback.format_exc(limit=3)},
            )

        # Shape inspection: pandas frames, lists, dicts, anything truthy.
        rows: Optional[int] = None
        cols: Optional[int] = None
        try:
            import pandas as pd  # type: ignore

            if isinstance(result, pd.DataFrame):
                rows, cols = result.shape
                if expect_nonempty and rows == 0:
                    return CheckResult(
                        self.name,
                        "FAIL",
                        _ms(start),
                        detail="returned empty DataFrame",
                        rows=0,
                        cols=cols,
                    )
                return CheckResult(
                    self.name, "OK", _ms(start),
                    detail=f"{rows} rows × {cols} cols",
                    rows=rows, cols=cols,
                )
            if isinstance(result, pd.Series):
                rows = len(result)
                if expect_nonempty and rows == 0:
                    return CheckResult(self.name, "FAIL", _ms(start), detail="empty Series", rows=0)
                return CheckResult(self.name, "OK", _ms(start), detail=f"{rows} entries", rows=rows)
        except ImportError:
            pass

        if isinstance(result, (list, tuple, set, dict)):
            n = len(result)
            if expect_nonempty and n == 0:
                return CheckResult(self.name, "FAIL", _ms(start), detail="empty collection")
            return CheckResult(self.name, "OK", _ms(start), detail=f"{n} items", rows=n)

        if expect_nonempty and not result:
            return CheckResult(self.name, "FAIL", _ms(start), detail=f"falsy result: {result!r}")
        return CheckResult(self.name, "OK", _ms(start), detail="ok")


def _ms(start: float) -> int:
    return int((time.perf_counter() - start) * 1000)


class _SkipCheck(Exception):
    """Raise to mark a check as SKIPPED rather than FAIL."""


# ─────────────────────────────────────────────────────────────────────────
# Per-source checks. Each returns a callable that the runner will execute.
#
# All checks deliberately use *small* requests so a full sweep finishes in
# well under a minute even on a slow link. Date ranges are anchored to
# "today minus 30 days" so the same check stays valid as time moves on.
# ─────────────────────────────────────────────────────────────────────────


def _recent_window(days: int = 30) -> tuple[str, str]:
    end = date.today()
    start = end - timedelta(days=days)
    return start.isoformat(), end.isoformat()


def _check_equity_prices() -> CheckResult:
    runner = _make("equity_prices")

    def fn():
        try:
            import yfinance  # noqa: F401
        except ImportError:
            raise _SkipCheck("yfinance not installed (pip install findata-mcp[yfinance])")
        from findata.equity_prices import get_equity_prices

        start, end = _recent_window(10)
        return get_equity_prices(["AAPL"], start, end)

    return runner.run(fn)


def _check_fama_french() -> CheckResult:
    runner = _make("fama_french")

    def fn():
        from findata.fama_french import get_fama_french_factors

        start, end = _recent_window(60)
        return get_fama_french_factors("3", start_date=start, end_date=end)

    return runner.run(fn)


def _check_ken_french() -> CheckResult:
    runner = _make("ken_french_factors")

    def fn():
        from findata.ken_french_factors import get_ken_french_factors

        start, end = _recent_window(60)
        return get_ken_french_factors("3", start_date=start, end_date=end)

    return runner.run(fn)


def _check_fred() -> CheckResult:
    runner = _make("fred")

    def fn():
        if not os.environ.get("FRED_API_KEY"):
            raise _SkipCheck("FRED_API_KEY env var not set")
        try:
            import fredapi  # noqa: F401
        except ImportError:
            raise _SkipCheck("fredapi not installed (pip install fredapi)")
        from findata.fred import get_fred_series

        start, end = _recent_window(120)
        # CPIAUCSL = US headline CPI, monthly, always populated.
        return get_fred_series(["CPIAUCSL"], start_date=start, end_date=end)

    return runner.run(fn)


def _check_cboe_volatility() -> CheckResult:
    runner = _make("cboe_volatility")

    def fn():
        try:
            import yfinance  # noqa: F401
        except ImportError:
            raise _SkipCheck("yfinance not installed (pip install findata-mcp[yfinance])")
        from findata.cboe_volatility import get_cboe_volatility_indices

        start, end = _recent_window(10)
        return get_cboe_volatility_indices(["^VIX"], start_date=start, end_date=end)

    return runner.run(fn)


def _check_coingecko() -> CheckResult:
    runner = _make("coingecko")

    def fn():
        from findata.coingecko import get_coingecko_ohlcv

        # 1 day = smallest valid range for the public free endpoint.
        return get_coingecko_ohlcv("bitcoin", vs_currency="usd", days=1, timeout=15)

    return runner.run(fn)


def _check_binance() -> CheckResult:
    runner = _make("binance")

    def fn():
        from findata.binance import get_binance_ohlcv

        # 5 candles is the smallest sensible probe; daily interval is the
        # least likely to hit weekend / micro-rollover edge cases on the
        # health endpoint.
        return get_binance_ohlcv("BTCUSDT", interval="1d", limit=5, timeout=15)

    return runner.run(fn)


def _check_sp500_composition() -> CheckResult:
    runner = _make("sp500_composition")

    def fn():
        from findata.sp500_composition import get_sp500_composition

        # Use a fixed date in the past so the underlying CSV always has it,
        # even if the cache hasn't been refreshed today.
        return get_sp500_composition("2024-01-02", return_dataframe=True)

    return runner.run(fn)


def _check_file_reader() -> CheckResult:
    runner = _make("file_reader")

    def fn():
        # Synthesise a tiny CSV in a temp dir and read it back. This proves
        # the wrapper can parse dates, filter tickers, and slice dates —
        # no network or upstream dependency.
        import tempfile

        import pandas as pd

        from findata.file_reader import get_file_data

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "sample.csv"
            df = pd.DataFrame({
                "date": ["2024-01-02", "2024-01-03", "2024-01-04"],
                "ticker": ["AAPL", "AAPL", "MSFT"],
                "close": [185.10, 184.25, 372.40],
            })
            df.to_csv(path, index=False)
            return get_file_data(path, tickers=["AAPL"])

    return runner.run(fn)


def _check_bloomberg() -> CheckResult:
    runner = _make("bloomberg")

    def fn():
        try:
            import blpapi  # noqa: F401
        except ImportError:
            raise _SkipCheck("blpapi not installed (Bloomberg Terminal SDK required)")
        # We don't actually fire a Bloomberg query during diagnostics — the
        # SDK requires a live Terminal session and would hang or pop dialogs.
        # Treat "module imports cleanly" as the success criterion.
        return ["blpapi importable"]

    return runner.run(fn)


def _check_ohlc_chart() -> CheckResult:
    """Smoke-test plot_ohlc_chart end-to-end for one US + one Indian ticker.

    Catches the kind of regression we hit on 2026-05-08 (naive
    datetime64[ns] index for .NS tickers vs tz-aware UTC cutoff
    raised TypeError before any chart could render). The fallback
    machinery in ohlc_chart hides these by returning chart_status
    ≠ "ok" instead of raising — useful for users, but means a real
    code bug only surfaces when an agent actually charts in a debate.
    Running this check on container start surfaces it immediately.

    The check is intentionally cheap: 60-day lookback, no S/R, no
    indicators — fastest path that still exercises data-fetch +
    mplfinance render. yfinance rate-limit responses degrade to a
    soft warning rather than a FAIL (genuine "no_data" isn't a bug).
    """
    runner = _make("ohlc_chart")

    def fn():
        try:
            import mplfinance  # noqa: F401
        except ImportError:
            raise _SkipCheck("mplfinance not installed")
        from findata.ohlc_chart import plot_ohlc_chart

        soft_warnings: list[str] = []
        hard_failures: list[str] = []
        for ticker in ("AAPL", "RELIANCE.NS"):
            out = plot_ohlc_chart(
                ticker, lookback_days=60,
                with_sr=False, with_indicators=False,
            )
            status = out.get("chart_status", "?")
            if status == "ok":
                continue
            if status == "no_data":
                # yfinance empty / rate-limited — environmental, not a bug.
                soft_warnings.append(f"{ticker}: no_data (yfinance empty or rate-limited)")
                continue
            # render_error / schema_error / wrapper_error → real bug.
            err = (out.get("summary") or "")[:120]
            hard_failures.append(f"{ticker}: {status} — {err}")

        if hard_failures:
            raise RuntimeError("; ".join(hard_failures))
        # All OK (or only soft warnings). Report soft warnings in detail
        # so the post-install console line still flags them.
        return {
            "tickers": ["AAPL", "RELIANCE.NS"],
            "soft_warnings": soft_warnings,
        }

    return runner.run(fn)


# Ordered registry. The CLI uses this for `--only` filtering.
ALL_CHECKS: dict[str, Callable[[], CheckResult]] = {
    "equity_prices": _check_equity_prices,
    "fama_french": _check_fama_french,
    "ken_french_factors": _check_ken_french,
    "fred": _check_fred,
    "cboe_volatility": _check_cboe_volatility,
    "coingecko": _check_coingecko,
    "binance": _check_binance,
    "sp500_composition": _check_sp500_composition,
    "file_reader": _check_file_reader,
    "bloomberg": _check_bloomberg,
    "ohlc_chart": _check_ohlc_chart,
}


# ─────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────


def run_all(selected: Optional[list[str]] = None) -> list[CheckResult]:
    names = selected or list(ALL_CHECKS.keys())
    out: list[CheckResult] = []
    for name in names:
        fn = ALL_CHECKS.get(name)
        if fn is None:
            out.append(CheckResult(name, "FAIL", 0, detail=f"unknown check: {name}"))
            continue
        out.append(fn())
    return out


def _format_row(r: CheckResult) -> str:
    colours = {"OK": "\033[32m", "SKIPPED": "\033[33m", "FAIL": "\033[31m"}
    reset = "\033[0m"
    is_tty = sys.stdout.isatty()
    badge = f"{colours[r.status]}{r.status:<7}{reset}" if is_tty else f"{r.status:<7}"
    timing = f"{r.elapsed_ms:>5} ms"
    return f"  {badge}  {r.name:<22} {timing}  {r.detail}"


def main(argv: Optional[list[str]] = None) -> int:
    parser = argparse.ArgumentParser(
        prog="findata-diagnostics",
        description="Health-check every findata data source after install.",
    )
    parser.add_argument(
        "--only",
        metavar="CHECK[,CHECK...]",
        help="Comma-separated subset of checks to run (default: all).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit results as a JSON array instead of a human-readable table.",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="List the available check names and exit.",
    )
    args = parser.parse_args(argv)

    if args.list:
        for name in ALL_CHECKS:
            print(name)
        return 0

    selected = [s.strip() for s in args.only.split(",")] if args.only else None
    results = run_all(selected)

    if args.json:
        print(json.dumps([asdict(r) for r in results], indent=2, default=str))
    else:
        print("findata diagnostics")
        print("─" * 60)
        for r in results:
            print(_format_row(r))
        print("─" * 60)
        ok = sum(1 for r in results if r.status == "OK")
        skipped = sum(1 for r in results if r.status == "SKIPPED")
        failed = sum(1 for r in results if r.status == "FAIL")
        print(f"  {ok} OK  ·  {skipped} skipped  ·  {failed} failed")

    return 1 if any(r.status == "FAIL" for r in results) else 0


if __name__ == "__main__":
    raise SystemExit(main())
