"""
examples/connect_raw_sse.py
============================
Connect to the findata MCP server using the raw MCP Python client over SSE.
No agent SDK required — useful for testing or building your own agent loop.

Prerequisites
-------------
    pip install -e ".[sse,yfinance]"

Start the server first:
    findata-mcp-sse                    # http://localhost:8000
"""

import asyncio
from mcp.client.sse import sse_client
from mcp import ClientSession


async def main():
    async with sse_client("http://localhost:8000/sse") as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()

            # --- what tools are available? ---
            tools = await session.list_tools()
            print("Available MCP tools:")
            for t in tools.tools:
                print(f"  {t.name}: {t.description[:60]}...")

            print("\n" + "="*60 + "\n")

            # --- search by natural language ---
            result = await session.call_tool(
                "search_tools",
                {"query": "equity daily prices", "top_k": 1},
            )
            print("search_tools('equity daily prices'):\n")
            print(result.content[0].text)

            print("\n" + "="*60 + "\n")

            # --- get full doc for sp500 composition ---
            result = await session.call_tool(
                "get_tool_doc",
                {"tool_name": "get_sp500_composition"},
            )
            print("get_tool_doc('get_sp500_composition'):\n")
            print(result.content[0].text)

            print("\n" + "="*60 + "\n")

            # --- list everything ---
            result = await session.call_tool("list_all_tools", {})
            print("list_all_tools():\n")
            print(result.content[0].text)


if __name__ == "__main__":
    asyncio.run(main())