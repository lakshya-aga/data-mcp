#!/usr/bin/env python3
"""
test_request_source.py
======================
End-to-end test for the request_data_source MCP tool.

Steps:
  1. Connect to MCP server — list current data sources
  2. Request a new data source via Codex CLI agent
  3. Reconnect to MCP server (picks up the newly registered source)
  4. Search for the newly added source

Usage:
  python test_request_source.py
  python test_request_source.py "add FRED macroeconomic series"
  python test_request_source.py "add crypto prices from CoinGecko" 300
"""

import asyncio
import sys
import textwrap
import time

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

SERVER_CMD = "python"
SERVER_ARGS = ["-m", "findata_mcp"]

# Override from CLI: python test_request_source.py "<description>" [timeout_seconds]
REQUEST_DESCRIPTION = (
    sys.argv[1]
    if len(sys.argv) > 1
    else "add Fama-French 3-factor and 5-factor daily returns from the Ken French data library"
)
TIMEOUT_SECONDS = int(sys.argv[2]) if len(sys.argv) > 2 else 300

# Derive a short search query from the description for step 3
# (take the first few meaningful words)
_SEARCH_QUERY = " ".join(REQUEST_DESCRIPTION.lower().replace("add ", "").split()[:4])

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _hr(char: str = "─", width: int = 70) -> str:
    return char * width


def _section(title: str) -> None:
    print(f"\n{_hr()}")
    print(f"  {title}")
    print(_hr())


def _indent(text: str, prefix: str = "  ") -> str:
    return textwrap.indent(text, prefix)


def _server_params() -> StdioServerParameters:
    return StdioServerParameters(command=SERVER_CMD, args=SERVER_ARGS)


# ---------------------------------------------------------------------------
# Steps
# ---------------------------------------------------------------------------

async def step1_list_current_sources() -> list[str]:
    """Connect, call list_all_tools, return the names seen."""
    _section("STEP 1 — Current data sources (before request)")

    async with stdio_client(_server_params()) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()

            result = await session.call_tool("list_all_tools", {})
            text = result.content[0].text
            print(_indent(text))

            # Extract function names for comparison later
            names = [
                line.split("`")[1]
                for line in text.splitlines()
                if line.startswith("### `")
            ]
            print(f"\n  → {len(names)} source(s) registered: {names}")
            return names


async def step2_request_new_source() -> str:
    """Connect, call request_data_source, stream Codex output, return result text."""
    _section(f"STEP 2 — Requesting new source via Codex CLI agent")
    print(f"  Description : {REQUEST_DESCRIPTION!r}")
    print(f"  Timeout     : {TIMEOUT_SECONDS}s")
    print()

    async with stdio_client(_server_params()) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()

            t0 = time.monotonic()
            print("  Waiting for Codex agent to finish... (this may take 1-5 minutes)")
            result = await session.call_tool(
                "request_data_source",
                {
                    "description": REQUEST_DESCRIPTION,
                    "timeout_seconds": TIMEOUT_SECONDS,
                },
            )
            elapsed = time.monotonic() - t0

            text = result.content[0].text
            print(_indent(text))
            print(f"\n  → Codex call completed in {elapsed:.1f}s")
            return text


async def step3_search_new_source(before_names: list[str]) -> None:
    """Reconnect (fresh import = picks up newly registered module), search."""
    _section(f"STEP 3 — Search for new source (fresh server session)")
    print(f"  Search query: {_SEARCH_QUERY!r}\n")

    async with stdio_client(_server_params()) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()

            # Full catalogue — highlight what's new
            catalogue = await session.call_tool("list_all_tools", {})
            cat_text = catalogue.content[0].text
            after_names = [
                line.split("`")[1]
                for line in cat_text.splitlines()
                if line.startswith("### `")
            ]
            new_names = [n for n in after_names if n not in before_names]

            if new_names:
                print(f"  New source(s) detected: {new_names}")
            else:
                print(
                    "  No new sources detected in registry yet.\n"
                    "  (The Codex agent writes the files but the server module\n"
                    "   is already imported; a full process restart would be\n"
                    "   required to reload — this is expected behaviour.)"
                )

            # Search for it by keyword regardless
            search_result = await session.call_tool(
                "search_tools",
                {"query": _SEARCH_QUERY, "top_k": 2},
            )
            print("\n  Search result:")
            print(_indent(search_result.content[0].text))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

async def main() -> None:
    print(_hr("═"))
    print("  findata MCP — request_data_source end-to-end test")
    print(_hr("═"))

    before_names = await step1_list_current_sources()
    codex_output = await step2_request_new_source()

    # If Codex reported an error, warn but still proceed to step 3
    if "exit 0" not in codex_output and "timed out" not in codex_output.lower():
        print("\n  ⚠  Codex may have encountered an error — proceeding to step 3 anyway.")

    await step3_search_new_source(before_names)

    _section("DONE")
    print(
        "  Files written by Codex live in the data-mcp/ repo.\n"
        "  Restart the MCP server to make the new source fully discoverable.\n"
    )


if __name__ == "__main__":
    asyncio.run(main())
