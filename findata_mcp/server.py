#!/usr/bin/env python3
"""
findata_mcp.server
==================
MCP server for the **findata** library.

Audience : code-writing agents (not analysis agents).
Contract : every tool call returns documentation about our wrapper functions —
           signatures, parameter tables, return-type descriptions, and
           copy-paste code examples.  No live data is ever fetched.

Running
-------
    findata-mcp                       # console-script after pip install -e .
    python -m findata_mcp.server      # from source without installing

MCP client configuration
------------------------
Add to claude_desktop_config.json / .cursor/mcp.json:

    {
      "mcpServers": {
        "findata": {
          "command": "findata-mcp"
        }
      }
    }

Or, if running from source:

    {
      "mcpServers": {
        "findata": {
          "command": "python",
          "args": ["-m", "findata_mcp.server"]
        }
      }
    }
"""

from __future__ import annotations

import asyncio
import importlib
import inspect
import os
import re
import shutil
import sys
import textwrap
from pathlib import Path
from typing import Any, Dict, List

# ---------------------------------------------------------------------------
# Codex CLI integration
# ---------------------------------------------------------------------------

# Resolution order:
#   1. CODEX_CLI_PATH env var (explicit override)
#   2. `codex` on PATH (npm-installed inside the container)
#   3. macOS app bundle (local development default)
_CODEX_CLI = (
    os.environ.get("CODEX_CLI_PATH")
    or shutil.which("codex")
    or "/Applications/Codex.app/Contents/Resources/codex"
)
_REPO_ROOT = Path(__file__).resolve().parent.parent  # data-mcp/


def _extract_tags_from_text(*texts: str) -> List[str]:
    """Derive search tags from function name words and a summary string."""
    seen: set[str] = set()
    tags: List[str] = []
    for text in texts:
        for word in re.findall(r"[a-z]+", text.lower()):
            if len(word) > 2 and word not in seen:
                seen.add(word)
                tags.append(word)
    return tags


def _hot_reload_new_sources() -> List[str]:
    """
    Scan findata/ for modules not yet in the registry and register any
    get_* functions found in them.  Called after a successful Codex run so
    the new source is immediately discoverable without restarting the server.

    Returns the list of function names that were added.
    """
    findata_dir = _REPO_ROOT / "findata"
    added: List[str] = []

    for py_file in sorted(findata_dir.glob("*.py")):
        if py_file.name.startswith("_"):
            continue

        module_name = f"findata.{py_file.stem}"

        # Import fresh modules; skip ones that were already imported at
        # server start (they're already in the registry).
        if module_name in sys.modules:
            mod = sys.modules[module_name]
        else:
            try:
                mod = importlib.import_module(module_name)
            except Exception:
                # Broken import — Codex validation step should have caught
                # this, but be defensive and skip rather than crashing.
                continue

        for attr_name in dir(mod):
            if not attr_name.startswith("get_"):
                continue
            if attr_name in _REGISTRY_BY_NAME:
                continue
            fn = getattr(mod, attr_name)
            if not callable(fn):
                continue

            doc = inspect.getdoc(fn) or ""
            summary = doc.splitlines()[0] if doc else f"Wrapper function {attr_name}."

            entry: Dict[str, Any] = {
                "name": attr_name,
                "callable": fn,
                "module": module_name,
                "tags": _extract_tags_from_text(attr_name, summary),
                "stub": False,
                "install_requires": [],
                "summary": summary,
                "example": textwrap.dedent(f"""\
                    from {module_name} import {attr_name}

                    df = {attr_name}(...)
                """),
            }
            _REGISTRY.append(entry)
            _REGISTRY_BY_NAME[attr_name] = entry
            added.append(attr_name)

    return added


def _build_codex_prompt(request: str) -> str:
    """Return the full prompt sent to the Codex agent for a data-source request."""
    return textwrap.dedent(f"""\
        You are adding a new financial data wrapper to the findata library.

        ## Repository root
        {_REPO_ROOT}

        ## User request
        "{request}"

        ## Your tasks

        ### 1 — Create findata/<module_name>.py
        Pick a sensible snake_case module name and implement one public wrapper
        function (e.g. `get_fama_french_factors`).  Follow this exact style:

        ```python
        \"\"\"
        findata.<module_name>
        ---------------------
        One-line description of the data source.
        \"\"\"
        from __future__ import annotations
        from typing import List, Optional
        import pandas as pd

        def get_<something>(
            param1: ...,
            param2: ...,
        ) -> pd.DataFrame:
            \"\"\"
            One-line summary.

            Parameters
            ----------
            param1 : type
                Description.

            Returns
            -------
            pd.DataFrame
                DatetimeIndex rows, columns described here.

            Raises
            ------
            ImportError
                If a required package is not installed.

            Examples
            --------
            >>> from findata.<module_name> import get_<something>
            >>> df = get_<something>(...)
            \"\"\"
            try:
                import some_package
            except ImportError as exc:
                raise ImportError(
                    "some_package is required. Install: pip install some_package"
                ) from exc
            # ... implementation ...
            return df
        ```

        Rules:
        - Heavy dependencies (pandas, requests, etc.) are lazy-imported INSIDE the
          function body with a helpful ImportError message.
        - Always return a pd.DataFrame with a DatetimeIndex.
        - Validate inputs; raise ValueError with clear messages.
        - The implementation can use any approach: a Python library (e.g.
          pandas-datareader, yfinance), a public REST API, or HTML scraping — pick
          whichever is most reliable and requires the fewest exotic dependencies.

        ### 2 — Register in findata_mcp/server.py
        Add the import near the other findata imports:
            from findata.<module_name> import get_<something>

        Then add one dict to the `_REGISTRY` list:
        {{
            "name": "get_<something>",
            "callable": get_<something>,
            "module": "findata.<module_name>",
            "tags": [...relevant keywords an agent might search for...],
            "stub": False,
            "install_requires": [...pip packages...],
            "summary": "One sentence.",
            "example": textwrap.dedent(\"\"\"\\\\
                from findata.<module_name> import get_<something>

                df = get_<something>(...)
                # Explain what df looks like
            \"\"\"),
        }}

        ### 3 — Validate before finishing
        After writing both files, run this shell command from the repo root:

            cd {_REPO_ROOT} && python -c "
        import inspect
        from findata.<module_name> import <function_name>
        sig = inspect.signature(<function_name>)
        print('VALIDATION OK:', '<function_name>', sig)
        "

        Rules for validation:
        - If the command fails for any reason (ImportError, SyntaxError,
          missing dependency, etc.) fix the code and re-run until it passes.
        - If a required package is missing, install it first with pip, then
          re-run the check.
        - Do NOT call the function to fetch live data — only verify it
          imports cleanly and has the expected signature.
        - Only report success after this check exits with code 0.

        Only modify findata/<module_name>.py (new file) and
        findata_mcp/server.py (add import + registry entry).
        Do not touch any other files.
    """)


async def _run_codex_agent(request: str, timeout: int = 300) -> str:
    """Spawn `codex exec` to implement the requested data source."""
    prompt = _build_codex_prompt(request)
    try:
        proc = await asyncio.create_subprocess_exec(
            _CODEX_CLI, "exec",
            "--full-auto",
            "--skip-git-repo-check",
            "-C", str(_REPO_ROOT),
            prompt,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
    except FileNotFoundError:
        return (
            f"Codex CLI not found at {_CODEX_CLI!r}. "
            "Ensure Codex is installed at /Applications/Codex.app."
        )

    try:
        stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
    except asyncio.TimeoutError:
        proc.kill()
        return f"Codex agent timed out after {timeout}s."

    out = stdout.decode(errors="replace").strip()
    err = stderr.decode(errors="replace").strip()

    if proc.returncode != 0:
        detail = err or out or "(no output)"
        return (
            f"Codex exited with code {proc.returncode}.\n\n"
            f"Details:\n{detail}"
        )

    # Hot-reload: register any new findata modules into the live registry
    # so the new source is immediately searchable without a server restart.
    newly_registered = _hot_reload_new_sources()
    reload_note = (
        f"\n\nHot-reloaded into registry: {newly_registered}"
        if newly_registered
        else "\n\n(No new registry entries detected — server restart may be needed.)"
    )

    return (
        f"Codex agent completed (exit 0).\n\n"
        + (out if out else "(no stdout)")
        + reload_note
    )

try:
    from mcp.server import Server
    from mcp.server.stdio import stdio_server
    from mcp import types
except ImportError as exc:
    raise SystemExit(
        "The 'mcp' package is required.\n"
        "Install: pip install mcp\n"
        f"Original error: {exc}"
    ) from exc

# Import our wrapper functions for signature introspection.
# The MCP documents these — not the underlying yfinance / blpapi APIs.
from findata.equity_prices import get_equity_prices
from findata.sp500_composition import get_sp500_composition, refresh_sp500_cache
from findata.file_reader import get_file_data
from findata.bloomberg import get_bloomberg_data
from findata.fama_french import get_fama_french_factors
from findata.ken_french_factors import get_ken_french_factors
from findata.fred import get_fred_series
from findata.cboe_volatility import get_cboe_volatility_indices
from findata.coingecko import get_coingecko_ohlcv
from findata.binance import get_binance_ohlcv
from findata.news_yfinance import get_yfinance_news
from findata.news_gdelt import get_gdelt_news
from findata.fundamentals import get_equity_fundamentals
from findata.analyst_consensus import get_analyst_consensus
from findata.earnings_calendar import get_earnings_calendar
from findata.returns_stats import compute_returns_stats

# ---------------------------------------------------------------------------
# Registry
# One entry per public findata wrapper.  Add new data sources here.
# ---------------------------------------------------------------------------

_REGISTRY: List[Dict[str, Any]] = [
    {
        "name": "get_equity_prices",
        "callable": get_equity_prices,
        "module": "findata.equity_prices",
        "tags": [
            "equity", "prices", "ohlcv", "historical", "daily", "weekly",
            "monthly", "stocks", "close", "open", "high", "low", "volume",
            "time series", "market data", "yfinance", "bars",
        ],
        "stub": False,
        "install_requires": ["yfinance"],
        "summary": (
            "Fetch historical OHLCV bars for one or more equities. "
            "Wraps yfinance with normalised MultiIndex column output and input validation."
        ),
        "example": textwrap.dedent("""\
            from findata.equity_prices import get_equity_prices

            # Single ticker — daily close only
            df = get_equity_prices(
                tickers=["AAPL"],
                start_date="2024-01-01",
                end_date="2024-12-31",
                fields=["Close"],
            )
            # df: DatetimeIndex rows, flat column "Close"

            # Multiple tickers — all OHLCV fields
            df = get_equity_prices(
                tickers=["AAPL", "MSFT", "NVDA"],
                start_date="2023-01-01",
                end_date="2024-01-01",
            )
            close = df["Close"]           # DataFrame: rows=dates, cols=tickers
            aapl  = df["Close"]["AAPL"]   # Series

            # Weekly bars
            df = get_equity_prices(
                tickers=["SPY"],
                start_date="2020-01-01",
                end_date="2024-01-01",
                fields=["Close"],
                frequency="1wk",
            )
        """),
    },
    {
        "name": "get_cboe_volatility_indices",
        "callable": get_cboe_volatility_indices,
        "module": "findata.cboe_volatility",
        "tags": [
            "vix", "cboe", "volatility", "index", "indices", "implied",
            "variance", "vvix", "vxn", "vxd", "vix9d", "market", "fear",
            "yfinance", "historical", "ohlcv", "daily",
        ],
        "stub": False,
        "install_requires": ["yfinance", "pandas"],
        "summary": (
            "Fetch historical CBOE volatility index data (e.g. VIX, VVIX) "
            "via Yahoo Finance tickers."
        ),
        "example": textwrap.dedent("""\
            from findata.cboe_volatility import get_cboe_volatility_indices

            # VIX and VVIX daily history
            df = get_cboe_volatility_indices(
                symbols=["^VIX", "^VVIX"],
                start_date="2020-01-01",
                end_date="2024-12-31",
            )
            # df: DatetimeIndex rows, MultiIndex columns (field, symbol)
        """),
    },
    {
        "name": "get_coingecko_ohlcv",
        "callable": get_coingecko_ohlcv,
        "module": "findata.coingecko",
        "tags": [
            "crypto", "cryptocurrency", "coingecko", "bitcoin", "ethereum",
            "ohlcv", "candles", "bars", "prices", "volume", "market data",
            "historical", "spot", "usd", "public api",
        ],
        "stub": False,
        "install_requires": ["pandas", "requests"],
        "summary": "Fetch crypto OHLCV candles from the CoinGecko public API.",
        "example": textwrap.dedent("""\
            from findata.coingecko import get_coingecko_ohlcv

            # 90 days of BTC/USD OHLCV
            df = get_coingecko_ohlcv("bitcoin", vs_currency="usd", days=90)
            # df: DatetimeIndex rows, columns open/high/low/close/volume
        """),
    },
    {
        "name": "get_binance_ohlcv",
        "callable": get_binance_ohlcv,
        "module": "findata.binance",
        "tags": [
            "crypto", "cryptocurrency", "binance", "btc", "eth", "ohlcv",
            "candles", "klines", "bars", "prices", "volume", "market data",
            "historical", "spot", "usdt", "public api", "rest",
        ],
        "stub": False,
        "install_requires": ["pandas", "requests"],
        "summary": (
            "Fetch crypto OHLCV (klines) from Binance Spot's public REST API. "
            "No auth, paginated up to 1000 candles per request, supports "
            "1-second to 1-month intervals."
        ),
        "example": textwrap.dedent("""\
            from findata.binance import get_binance_ohlcv

            # 365 days of BTCUSDT daily candles
            df = get_binance_ohlcv("BTCUSDT", interval="1d",
                                   start_date="2024-01-01",
                                   end_date="2024-12-31")
            # df: DatetimeIndex (UTC, tz-naive) rows,
            #   columns open/high/low/close/volume/quote_volume/trades

            # Last 24 4-hour candles for ETHUSDT
            df = get_binance_ohlcv("ETHUSDT", interval="4h", limit=24)
        """),
    },
    # ─── News / sentiment ──────────────────────────────────────────
    {
        "name": "get_yfinance_news",
        "callable": get_yfinance_news,
        "module": "findata.news_yfinance",
        "tags": [
            "news", "headlines", "yfinance", "yahoo finance", "company news",
            "press", "media", "articles", "us equity", "ticker news",
        ],
        "stub": False,
        "install_requires": ["yfinance"],
        "summary": (
            "Fetch recent news headlines for a US-listed ticker via "
            "yfinance.Ticker(t).news. Returns a DataFrame indexed by "
            "publication time with title, publisher, link, and summary."
        ),
        "example": textwrap.dedent("""\
            from findata.news_yfinance import get_yfinance_news

            df = get_yfinance_news("AAPL", max_records=15)
            # DatetimeIndex desc; columns: title / publisher / link / summary / ticker
            df[["title", "publisher"]].head()
        """),
    },
    {
        "name": "get_gdelt_news",
        "callable": get_gdelt_news,
        "module": "findata.news_gdelt",
        "tags": [
            "news", "gdelt", "global", "sentiment", "tone", "articles",
            "company news", "industry news", "sector news", "media monitor",
            "multi-language", "free", "no api key",
        ],
        "stub": False,
        "install_requires": ["requests"],
        "summary": (
            "Fetch recent news from GDELT for a company AND (optionally) its "
            "sector. Returns a dict with two DataFrames ('company' and "
            "'sector'), each row is one article with GDELT's average tone "
            "score (-100..+100), domain, language, source country."
        ),
        "example": textwrap.dedent("""\
            from findata.news_gdelt import get_gdelt_news

            # Company-specific + sector context
            r = get_gdelt_news(
                company_query="NVIDIA AI chip data center",
                sector_query="semiconductor manufacturing",
                days=14,
                max_records=30,
            )
            r["company"][["title", "tone", "domain"]].head()
            r["sector"]["tone"].describe()  # sector sentiment summary

            # Company only (sector=None)
            r = get_gdelt_news("Apple iPhone Services revenue", days=7)
            r["company"][["title", "tone"]].head()
        """),
    },
    # ─── Fundamentals / analyst consensus / events ─────────────────
    {
        "name": "get_equity_fundamentals",
        "callable": get_equity_fundamentals,
        "module": "findata.fundamentals",
        "tags": [
            "fundamentals", "valuation", "pe", "pb", "ev/ebitda", "margins",
            "roe", "roa", "free cash flow", "balance sheet", "growth",
            "dividend", "yfinance", "snapshot", "ticker",
        ],
        "stub": False,
        "install_requires": ["yfinance"],
        "summary": (
            "Fundamentals snapshot per ticker — valuation multiples (P/E, "
            "P/B, EV/EBITDA), profitability (ROE, margins), growth, balance "
            "sheet (cash, debt, FCF), dividend yield, beta, 52-week range. "
            "One row per ticker via yfinance.Ticker(t).info."
        ),
        "example": textwrap.dedent("""\
            from findata.fundamentals import get_equity_fundamentals

            df = get_equity_fundamentals(["AAPL", "MSFT", "NVDA"])
            df[["forward_pe", "revenue_growth", "free_cash_flow", "roe"]]
        """),
    },
    {
        "name": "get_analyst_consensus",
        "callable": get_analyst_consensus,
        "module": "findata.analyst_consensus",
        "tags": [
            "analyst", "consensus", "target price", "recommendation",
            "wall street", "ratings", "buy hold sell", "yfinance",
            "coverage", "upside",
        ],
        "stub": False,
        "install_requires": ["yfinance"],
        "summary": (
            "Wall-Street consensus per ticker — mean/high/low target prices, "
            "current vs target upside %, recommendation key, recommendation "
            "mean (1=strong buy, 5=sell), number of analysts. Anchors any "
            "target-price reasoning in a debate or research note."
        ),
        "example": textwrap.dedent("""\
            from findata.analyst_consensus import get_analyst_consensus

            df = get_analyst_consensus(["AAPL", "MSFT"])
            df[["current_price", "target_mean", "upside_pct",
                "recommendation_key", "num_analysts"]]
        """),
    },
    {
        "name": "get_earnings_calendar",
        "callable": get_earnings_calendar,
        "module": "findata.earnings_calendar",
        "tags": [
            "earnings", "calendar", "eps", "estimate", "actual", "surprise",
            "results", "report date", "yfinance", "events", "quarterly",
        ],
        "stub": False,
        "install_requires": ["yfinance"],
        "summary": (
            "Past + upcoming earnings rows for a ticker. Each row is one "
            "earnings event with EPS estimate vs. actual (when reported) and "
            "surprise %. Anchors time-horizon reasoning — does the thesis "
            "depend on the next print?"
        ),
        "example": textwrap.dedent("""\
            from findata.earnings_calendar import get_earnings_calendar

            df = get_earnings_calendar("AAPL", days_back=730, days_forward=120)
            df[["eps_estimate", "eps_actual", "surprise_pct", "is_past"]].tail(8)
        """),
    },
    {
        "name": "compute_returns_stats",
        "callable": compute_returns_stats,
        "module": "findata.returns_stats",
        "tags": [
            "returns", "vol", "volatility", "beta", "alpha", "max drawdown",
            "sharpe", "risk", "stats", "annualised", "single ticker",
            "benchmark",
        ],
        "stub": False,
        "install_requires": ["yfinance", "numpy"],
        "summary": (
            "Annualised return / vol / Sharpe / max drawdown / beta vs a "
            "benchmark for a single ticker over a rolling window. Pure "
            "pandas/numpy on top of get_equity_prices."
        ),
        "example": textwrap.dedent("""\
            from findata.returns_stats import compute_returns_stats

            stats = compute_returns_stats(
                "AAPL", window_days=252, benchmark="SPY", risk_free_rate=0.045,
            )
            # pandas.Series with annual_return, annual_vol, sharpe,
            # max_drawdown, beta, alpha_annual, corr_to_benchmark
            stats[["annual_return", "annual_vol", "sharpe",
                   "max_drawdown", "beta"]]
        """),
    },
    {
        "name": "get_sp500_composition",
        "callable": get_sp500_composition,
        "module": "findata.sp500_composition",
        "tags": [
            "sp500", "s&p 500", "index", "composition", "constituents",
            "members", "point in time", "pit", "historical", "benchmark",
            "universe", "index membership",
        ],
        "stub": False,
        "install_requires": ["git (system)"],
        "summary": (
            "Return the point-in-time S&P 500 membership for any date. "
            "Clones fja05680/sp500 to ~/.cache/findata/sp500/ on first use "
            "and reads from disk thereafter."
        ),
        "example": textwrap.dedent("""\
            from findata.sp500_composition import get_sp500_composition, refresh_sp500_cache

            # List of ~503 tickers as of a given date
            members = get_sp500_composition("2024-12-31")
            # members -> ['AAPL', 'MSFT', 'NVDA', ...]

            # Historical point-in-time
            members_2010 = get_sp500_composition("2010-06-30")

            # As a DataFrame (index = snapshot date, column = "ticker")
            df = get_sp500_composition("2023-01-15", return_dataframe=True)

            # Combine: prices for index constituents at a point in time
            from findata.equity_prices import get_equity_prices
            tickers = get_sp500_composition("2020-01-31")
            prices  = get_equity_prices(tickers, "2020-02-01", "2020-06-30", fields=["Close"])

            # Pull the latest changes from the upstream repo
            refresh_sp500_cache()
        """),
    },
    {
        "name": "get_file_data",
        "callable": get_file_data,
        "module": "findata.file_reader",
        "tags": [
            "file", "csv", "parquet", "excel", "xlsx", "local", "flat file",
            "read", "load", "custom data", "proprietary", "vendor", "disk",
        ],
        "stub": False,
        "install_requires": ["pandas", "openpyxl (for Excel)", "pyarrow (for Parquet)"],
        "summary": (
            "Load financial time-series from a local CSV, Parquet, or Excel file. "
            "Handles date parsing, ticker filtering, and field selection in one call."
        ),
        "example": textwrap.dedent("""\
            from findata.file_reader import get_file_data

            # Load an entire CSV
            df = get_file_data("data/prices.csv")

            # Parquet — filtered to two tickers and a date range
            df = get_file_data(
                "data/prices.parquet",
                tickers=["AAPL", "MSFT"],
                start_date="2023-01-01",
                end_date="2023-12-31",
                fields=["close", "volume"],
            )

            # Excel with non-standard column names
            df = get_file_data(
                "data/export.xlsx",
                date_column="Date",
                ticker_column="Symbol",
                file_format="excel",
            )
        """),
    },
    {
        "name": "get_bloomberg_data",
        "callable": get_bloomberg_data,
        "module": "findata.bloomberg",
        "tags": [
            "bloomberg", "blp", "blpapi", "bbg", "terminal", "reference data",
            "historical", "fundamentals", "overrides", "b-pipe", "px_last",
        ],
        "stub": True,
        "install_requires": [
            "blpapi (pip install blpapi --index-url https://bcms.bloomberg.com/pip/simple/)"
        ],
        "summary": (
            "Fetch Bloomberg data via blpapi.  Supports HistoricalDataRequest "
            "and ReferenceDataRequest with optional field overrides.  "
            "STUB — implement the session body in findata/bloomberg.py before use."
        ),
        "example": textwrap.dedent("""\
            from findata.bloomberg import get_bloomberg_data

            # Historical daily prices
            df = get_bloomberg_data(
                tickers=["AAPL US Equity", "MSFT US Equity"],
                fields=["PX_LAST", "VOLUME"],
                start_date="2024-01-01",
                end_date="2024-12-31",
            )
            close = df["PX_LAST"]    # rows=dates, cols=tickers

            # Reference data — current values
            ref = get_bloomberg_data(
                tickers=["AAPL US Equity"],
                fields=["CUR_MKT_CAP", "GICS_SECTOR_NAME"],
                request_type="ReferenceDataRequest",
            )

            # Forward EPS with period override
            fwd = get_bloomberg_data(
                tickers=["AAPL US Equity", "NVDA US Equity"],
                fields=["BEST_EPS"],
                request_type="ReferenceDataRequest",
                overrides={"BEST_FPERIOD_OVERRIDE": "1BF"},
            )
            # NOTE: raises NotImplementedError until session logic is added.
        """),
    },
    {
        "name": "get_fama_french_factors",
        "callable": get_fama_french_factors,
        "module": "findata.fama_french",
        "tags": [
            "fama-french", "fama french", "ff3", "ff5", "factors",
            "three factor", "five factor", "ken french", "research data",
            "daily", "returns", "risk premia", "mkt-rf", "smb", "hml",
            "rmw", "cma", "rf",
        ],
        "stub": False,
        "install_requires": ["pandas-datareader"],
        "summary": (
            "Fetch daily Fama-French 3- or 5-factor returns from the "
            "Kenneth R. French Data Library."
        ),
        "example": textwrap.dedent("""\
            from findata.fama_french import get_fama_french_factors

            # 5-factor daily returns as decimals
            df = get_fama_french_factors(
                factor_model="5",
                start_date="2010-01-01",
                end_date="2020-12-31",
            )
            # df: DatetimeIndex rows with columns Mkt-RF, SMB, HML, RMW, CMA, RF
        """),
    },
    {
        "name": "get_ken_french_factors",
        "callable": get_ken_french_factors,
        "module": "findata.ken_french_factors",
        "tags": [
            "ken french", "kenneth french", "fama-french", "fama french",
            "ff3", "ff5", "factors", "three factor", "five factor",
            "daily", "returns", "risk premia", "mkt-rf", "smb", "hml",
            "rmw", "cma", "rf",
        ],
        "stub": False,
        "install_requires": ["pandas-datareader"],
        "summary": (
            "Fetch daily Fama-French 3- or 5-factor returns from the "
            "Kenneth R. French Data Library."
        ),
        "example": textwrap.dedent("""\
            from findata.ken_french_factors import get_ken_french_factors

            # 3-factor daily returns as decimals
            df = get_ken_french_factors(
                factor_model="3",
                start_date="2015-01-01",
                end_date="2020-12-31",
            )
            # df: DatetimeIndex rows with columns Mkt-RF, SMB, HML, RF
        """),
    },
    {
        "name": "get_fred_series",
        "callable": get_fred_series,
        "module": "findata.fred",
        "tags": [
            "fred", "federal reserve", "st louis fed", "macroeconomic",
            "macro", "economic", "time series", "cpi", "inflation",
            "unemployment", "rates", "interest rates", "gdp",
        ],
        "stub": False,
        "install_requires": ["fredapi"],
        "summary": "Fetch one or more FRED macroeconomic time series.",
        "example": textwrap.dedent("""\
            from findata.fred import get_fred_series

            df = get_fred_series(
                ["CPIAUCSL", "UNRATE"],
                start_date="2015-01-01",
                end_date="2024-12-31",
            )
            # df: DatetimeIndex rows, columns are CPIAUCSL and UNRATE
        """),
    },
]

# Build name-keyed index BEFORE call_tool references it
_REGISTRY_BY_NAME: Dict[str, Dict[str, Any]] = {e["name"]: e for e in _REGISTRY}


# ---------------------------------------------------------------------------
# Doc renderer
# Renders documentation about OUR wrapper — not the underlying library.
# The docstring is indented (not wrapped in a nested fence) to avoid
# broken markdown when the docstring itself contains backtick blocks.
# ---------------------------------------------------------------------------

def _render_doc(entry: Dict[str, Any]) -> str:
    fn = entry["callable"]
    sig = inspect.signature(fn)
    docstring = inspect.getdoc(fn) or "(no docstring)"
    # Indent each line of the docstring by 4 spaces so it renders as a
    # preformatted block without requiring nested fences.
    indented_doc = "\n".join("    " + line for line in docstring.splitlines())

    stub_banner = ""
    if entry["stub"]:
        stub_banner = (
            "\n> ⚠️  **STUB** — raises `NotImplementedError` until you "
            f"implement the session body in `{entry['module']}`.\n"
        )

    deps = ", ".join(f"`{d}`" for d in entry.get("install_requires", []))
    install_line = f"\n**Dependencies:** {deps}\n" if deps else ""

    return (
        f"## `{entry['name']}`\n\n"
        f"{entry['summary']}\n"
        f"{install_line}"
        f"{stub_banner}\n"
        f"**Import**\n"
        f"```python\n"
        f"from {entry['module']} import {entry['name']}\n"
        f"```\n\n"
        f"### Signature\n"
        f"```python\n"
        f"{entry['name']}{sig}\n"
        f"```\n\n"
        f"### Parameters & return type\n\n"
        f"{indented_doc}\n\n"
        f"### Example\n"
        f"```python\n"
        f"{entry['example'].rstrip()}\n"
        f"```\n"
    )


# ---------------------------------------------------------------------------
# Search scoring
# ---------------------------------------------------------------------------

def _score(entry: Dict[str, Any], query: str) -> int:
    q = query.lower()
    words = set(q.split())
    score = 0
    if q in entry["name"].lower():
        score += 12
    for tag in entry["tags"]:
        if tag in q or q in tag:
            score += 4
        if words & set(tag.split()):
            score += 2
    if words & set(entry["summary"].lower().split()):
        score += 1
    return score


# ---------------------------------------------------------------------------
# MCP server
# ---------------------------------------------------------------------------

server = Server("findata-mcp")


@server.list_tools()
async def list_tools() -> List[types.Tool]:
    return [
        types.Tool(
            name="search_tools",
            description=(
                "Search the findata library for the right wrapper function. "
                "Returns the function signature, full parameter docs, return type, "
                "and a ready-to-paste code example — all describing findata's own API. "
                "Designed for code-writing agents. "
                "Example queries: 'equity daily prices', 'sp500 constituents on a date', "
                "'load parquet file', 'bloomberg reference data'."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Natural-language description of the data you need.",
                    },
                    "top_k": {
                        "type": "integer",
                        "description": "Max results to return (default 2).",
                        "default": 2,
                    },
                },
                "required": ["query"],
            },
        ),
        types.Tool(
            name="get_tool_doc",
            description=(
                "Retrieve the complete documentation and code example for one specific "
                "findata function by exact name.  Use after search_tools to drill in."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "tool_name": {
                        "type": "string",
                        "description": (
                            "Exact function name: 'get_equity_prices', "
                            "'get_sp500_composition', 'get_file_data', "
                            "or 'get_bloomberg_data'."
                        ),
                    }
                },
                "required": ["tool_name"],
            },
        ),
        types.Tool(
            name="list_all_tools",
            description=(
                "List every findata wrapper function with a one-line summary and tags. "
                "Use to discover all available data sources."
            ),
            inputSchema={"type": "object", "properties": {}},
        ),
        types.Tool(
            name="request_data_source",
            description=(
                "Request a new financial data source to be added to the findata library. "
                "Describe the data you need in plain English and the Codex AI agent will "
                "automatically implement a wrapper function and register it in the MCP server. "
                "Example requests: 'add Fama-French factors', "
                "'add FRED macroeconomic series', 'add crypto prices from CoinGecko'. "
                "The new function will be immediately available via search_tools after the "
                "server is restarted. This tool runs the Codex CLI agent and may take up to "
                "5 minutes — returns a status message when done."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "description": {
                        "type": "string",
                        "description": (
                            "Natural-language description of the data source to add. "
                            "Be specific about what data you want, e.g. "
                            "'Fama-French 3-factor and 5-factor daily returns from Ken French data library'."
                        ),
                    },
                    "timeout_seconds": {
                        "type": "integer",
                        "description": "Max seconds to wait for Codex (default 300).",
                        "default": 300,
                    },
                },
                "required": ["description"],
            },
        ),
    ]


@server.call_tool()
async def call_tool(name: str, arguments: Dict[str, Any]) -> List[types.TextContent]:

    # ------------------------------------------------------------------ #
    # search_tools
    # ------------------------------------------------------------------ #
    if name == "search_tools":
        query: str = arguments.get("query", "").strip()
        top_k: int = int(arguments.get("top_k", 2))

        if not query:
            return [types.TextContent(type="text", text="Provide a non-empty query.")]

        ranked = sorted(_REGISTRY, key=lambda e: _score(e, query), reverse=True)
        top = ranked[:top_k]

        if _score(top[0], query) == 0:
            names = ", ".join(f"`{e['name']}`" for e in _REGISTRY)
            return [types.TextContent(
                type="text",
                text=(
                    f"No tools matched '{query}'.  "
                    f"Available functions: {names}.  "
                    "Try `list_all_tools` for the full catalogue."
                ),
            )]

        parts = [f"# findata tools matching: `{query}`\n"]
        for entry in top:
            parts.append(_render_doc(entry))
            parts.append("\n---\n")

        return [types.TextContent(type="text", text="\n".join(parts))]

    # ------------------------------------------------------------------ #
    # get_tool_doc
    # ------------------------------------------------------------------ #
    elif name == "get_tool_doc":
        tool_name: str = arguments.get("tool_name", "").strip()
        entry = _REGISTRY_BY_NAME.get(tool_name)
        if entry is None:
            available = ", ".join(f"`{n}`" for n in _REGISTRY_BY_NAME)
            return [types.TextContent(
                type="text",
                text=f"Unknown tool `{tool_name}`.  Available: {available}",
            )]
        return [types.TextContent(type="text", text=_render_doc(entry))]

    # ------------------------------------------------------------------ #
    # list_all_tools
    # ------------------------------------------------------------------ #
    elif name == "list_all_tools":
        lines = ["# findata — available wrapper functions\n"]
        for entry in _REGISTRY:
            stub_flag = "  *(stub — implement before use)*" if entry["stub"] else ""
            lines.append(
                f"### `{entry['name']}`{stub_flag}\n"
                f"- **Module:** `{entry['module']}`\n"
                f"- **Summary:** {entry['summary']}\n"
                f"- **Tags:** {', '.join(entry['tags'])}\n"
                f"- **Install:** {', '.join(entry.get('install_requires', []))}\n"
            )
        return [types.TextContent(type="text", text="\n".join(lines))]

    # ------------------------------------------------------------------ #
    # request_data_source
    # ------------------------------------------------------------------ #
    elif name == "request_data_source":
        description: str = arguments.get("description", "").strip()
        timeout: int = int(arguments.get("timeout_seconds", 300))

        if not description:
            return [types.TextContent(
                type="text",
                text="Provide a non-empty description of the data source you want added.",
            )]

        status_msg = (
            f"Starting Codex agent to implement: \"{description}\"\n"
            f"Working directory: {_REPO_ROOT}\n"
            f"Timeout: {timeout}s\n\n"
        )
        result = await _run_codex_agent(description, timeout=timeout)
        return [types.TextContent(type="text", text=status_msg + result)]

    return [types.TextContent(type="text", text=f"Unknown tool: {name}")]


# ---------------------------------------------------------------------------
# Entry points
# ---------------------------------------------------------------------------

# ── stdio (default — for Claude Desktop / CLI agents) ────────────────────────

async def _main_stdio() -> None:
    async with stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            server.create_initialization_options(),
        )


def _main_sync() -> None:
    """Synchronous wrapper — used by the ``findata-mcp`` console-script."""
    import asyncio
    asyncio.run(_main_stdio())


# ── SSE / HTTP (for web-based agents and OpenAI Agents SDK) ──────────────────

def run_sse_server(host: str = "0.0.0.0", port: int = 8000) -> None:
    """
    Run the MCP server over HTTP + SSE transport.

    The server exposes two endpoints:
        GET  /sse          — SSE stream (agent connects here first)
        POST /messages     — agent sends tool calls here

    Usage
    -----
        # From the command line:
        findata-mcp-sse
        findata-mcp-sse --host 127.0.0.1 --port 9000

        # Programmatically:
        from findata_mcp.server import run_sse_server
        run_sse_server(host="0.0.0.0", port=8000)

    Connecting from OpenAI Agents SDK
    ----------------------------------
        from agents.mcp import MCPServerSse
        mcp = MCPServerSse(url="http://localhost:8000/sse")

    Connecting with the raw MCP Python client
    ------------------------------------------
        from mcp.client.sse import sse_client
        from mcp import ClientSession
        async with sse_client("http://localhost:8000/sse") as (r, w):
            async with ClientSession(r, w) as session:
                await session.initialize()
                result = await session.call_tool("search_tools", {"query": "equity prices"})
    """
    try:
        import uvicorn
        from mcp.server.sse import SseServerTransport
        from starlette.applications import Starlette
        from starlette.routing import Mount, Route
    except ImportError as exc:
        raise SystemExit(
            "SSE transport requires extra packages.\n"
            "Install: pip install uvicorn starlette\n"
            f"Original error: {exc}"
        ) from exc

    sse_transport = SseServerTransport("/messages")

    async def handle_sse(request):
        async with sse_transport.connect_sse(
            request.scope, request.receive, request._send
        ) as (read_stream, write_stream):
            await server.run(
                read_stream,
                write_stream,
                server.create_initialization_options(),
            )

    starlette_app = Starlette(
        routes=[
            Route("/sse", endpoint=handle_sse),
            Mount("/messages", app=sse_transport.handle_post_message),
        ]
    )

    print(f"findata MCP server running on http://{host}:{port}")
    print(f"  SSE endpoint : http://{host}:{port}/sse")
    print(f"  Messages     : http://{host}:{port}/messages")
    uvicorn.run(starlette_app, host=host, port=port)


def _main_sse_cli() -> None:
    """Console-script entry point for ``findata-mcp-sse``."""
    import argparse
    parser = argparse.ArgumentParser(description="Run findata MCP server over SSE/HTTP")
    parser.add_argument("--host", default="0.0.0.0", help="Bind host (default: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=8000, help="Bind port (default: 8000)")
    args = parser.parse_args()
    run_sse_server(host=args.host, port=args.port)


if __name__ == "__main__":
    _main_sse_cli()
