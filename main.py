import asyncio
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

async def main():
    server_params = StdioServerParameters(
        command="findata-mcp",   # or "python" with args=["-m", "findata_mcp.server"]
        args=[],
    )

    async with stdio_client(server_params) as (read, write):
        async with ClientSession(read, write) as session:
            await session.initialize()

            # --- discover what's available ---
            tools = await session.list_tools()
            print([t.name for t in tools.tools])
            # ['search_tools', 'get_tool_doc', 'list_all_tools']

            # --- search by natural language ---
            result = await session.call_tool(
                "search_tools",
                {"query": "equity daily prices", "top_k": 1},
            )
            print(result.content[0].text)
            # Returns: signature + docstring + code example for get_equity_prices()

            # --- get full doc for a specific function ---
            result = await session.call_tool(
                "get_tool_doc",
                {"tool_name": "get_sp500_composition"},
            )
            print(result.content[0].text)

            # --- list everything ---
            result = await session.call_tool("list_all_tools", {})
            print(result.content[0].text)

asyncio.run(main())