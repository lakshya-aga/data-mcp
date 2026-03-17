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

import inspect
import json
import os
import subprocess
import textwrap
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

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
]

# Build name-keyed index BEFORE call_tool references it
_REGISTRY_BY_NAME: Dict[str, Dict[str, Any]] = {e["name"]: e for e in _REGISTRY}

_REQUESTS_DIR = Path(os.environ.get("FINDATA_TOOL_REQUESTS_DIR", ".tool_builder/requests"))
_DEFAULT_BUILDER_PROMPT = (
    "Implement the requested findata wrapper tool in this repository. "
    "Use the request spec file provided. Add/organize the module under findata/, "
    "update findata_mcp/server.py registry/docs/examples so the new tool is discoverable, "
    "and update README usage/docs accordingly."
)


def _create_tool_request(arguments: Dict[str, Any]) -> dict:
    """Persist a tool-build request and optionally spawn an external builder agent."""
    tool_name = arguments.get("tool_name", "").strip()
    module_path = arguments.get("module_path", "").strip()
    summary = arguments.get("summary", "").strip()
    code = arguments.get("code", "")

    if not tool_name or not module_path or not summary or not code:
        raise ValueError("tool_name, module_path, summary, and code are required")

    request_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
    _REQUESTS_DIR.mkdir(parents=True, exist_ok=True)
    request_file = _REQUESTS_DIR / f"{request_id}.json"

    payload = {
        "request_id": request_id,
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "tool_name": tool_name,
        "module_path": module_path,
        "summary": summary,
        "tags": arguments.get("tags", []),
        "install_requires": arguments.get("install_requires", []),
        "example": arguments.get("example", ""),
        "code": code,
        "notes": arguments.get("notes", ""),
    }
    request_file.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    builder_cmd = os.environ.get("FINDATA_TOOL_BUILDER_CMD", "").strip()
    spawned = False
    spawn_error = None

    if builder_cmd:
        # Supports placeholders: {request_file}, {request_id}
        cmd = builder_cmd.format(request_file=str(request_file), request_id=request_id)
        try:
            subprocess.Popen(cmd, shell=True, cwd=str(Path(__file__).resolve().parents[1]))
            spawned = True
        except Exception as exc:  # pragma: no cover
            spawn_error = str(exc)

    return {
        "request_id": request_id,
        "request_file": str(request_file),
        "builder_spawned": spawned,
        "builder_command": builder_cmd or None,
        "spawn_error": spawn_error,
    }


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
            name="request_tool_addition",
            description=(
                "Request a new findata wrapper by submitting proposed implementation code + metadata. "
                "The server stores a formal spec and can spawn an external tool-building agent "
                "(configured by FINDATA_TOOL_BUILDER_CMD) to implement and update docs/registry."
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "tool_name": {"type": "string", "description": "Public wrapper function name, e.g. get_fx_spot_prices"},
                    "module_path": {"type": "string", "description": "Module target, e.g. findata/fx.py"},
                    "summary": {"type": "string", "description": "One-line purpose of the wrapper"},
                    "code": {"type": "string", "description": "Proposed Python implementation code"},
                    "tags": {"type": "array", "items": {"type": "string"}},
                    "install_requires": {"type": "array", "items": {"type": "string"}},
                    "example": {"type": "string", "description": "Usage snippet for docs"},
                    "notes": {"type": "string", "description": "Additional implementation constraints"},
                },
                "required": ["tool_name", "module_path", "summary", "code"],
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
    # request_tool_addition
    # ------------------------------------------------------------------ #
    elif name == "request_tool_addition":
        try:
            request_info = _create_tool_request(arguments)
        except ValueError as exc:
            return [types.TextContent(type="text", text=f"Invalid request: {exc}")]

        spawn_status = (
            "✅ builder agent spawn requested"
            if request_info["builder_spawned"]
            else "ℹ️ request queued (no builder command configured)"
        )

        text = (
            f"# Tool addition request accepted\n\n"
            f"- Request ID: `{request_info['request_id']}`\n"
            f"- Request file: `{request_info['request_file']}`\n"
            f"- Status: {spawn_status}\n"
            f"- Builder command: `{request_info['builder_command'] or '(unset)'}`\n"
            f"\n"
            f"To auto-build, set env var `FINDATA_TOOL_BUILDER_CMD` with placeholders `{{request_file}}` and `{{request_id}}`.\n"
            f"Example:\n"
            f"`FINDATA_TOOL_BUILDER_CMD=\"openclaw sessions_spawn --runtime acp --agentId openai/gpt-5.1-codex --task '" + _DEFAULT_BUILDER_PROMPT + " Request: {request_file}'\"`\n"
        )
        if request_info["spawn_error"]:
            text += f"\nSpawn error: `{request_info['spawn_error']}`\n"

        return [types.TextContent(type="text", text=text)]

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


def run_streamable_server(host: str = "0.0.0.0", port: int = 8000, mount_path: str = "/mcp") -> None:
    """Run the MCP server over streamable HTTP transport (preferred for Agents SDK)."""
    try:
        import anyio
        import uvicorn
        from contextlib import asynccontextmanager
        from mcp.server.streamable_http import StreamableHTTPServerTransport
        from starlette.applications import Starlette
        from starlette.routing import Mount
    except ImportError as exc:
        raise SystemExit(
            "Streamable transport requires extra packages.\n"
            "Install: pip install uvicorn starlette anyio\n"
            f"Original error: {exc}"
        ) from exc

    transport = StreamableHTTPServerTransport(mcp_session_id=None)

    @asynccontextmanager
    async def lifespan(app):
        async with transport.connect() as (read_stream, write_stream):
            async with anyio.create_task_group() as tg:
                tg.start_soon(server.run, read_stream, write_stream, server.create_initialization_options())
                yield
                tg.cancel_scope.cancel()

    starlette_app = Starlette(routes=[Mount(mount_path, app=transport.handle_request)], lifespan=lifespan)

    print(f"findata MCP streamable server running on http://{host}:{port}{mount_path}")
    uvicorn.run(starlette_app, host=host, port=port)


def _main_sse_cli() -> None:
    """CLI entry point for network transports."""
    import argparse
    parser = argparse.ArgumentParser(description="Run findata MCP server over HTTP transport")
    parser.add_argument("--host", default="0.0.0.0", help="Bind host (default: 0.0.0.0)")
    parser.add_argument("--port", type=int, default=8000, help="Bind port (default: 8000)")
    parser.add_argument("--transport", choices=["streamable", "sse"], default="streamable")
    parser.add_argument("--mount-path", default="/mcp", help="Path for streamable endpoint")
    args = parser.parse_args()

    if args.transport == "sse":
        run_sse_server(host=args.host, port=args.port)
    else:
        run_streamable_server(host=args.host, port=args.port, mount_path=args.mount_path)


if __name__ == "__main__":
    _main_sse_cli()
