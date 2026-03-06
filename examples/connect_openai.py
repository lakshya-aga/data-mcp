"""
examples/connect_openai_agents.py
==================================
Connect the findata MCP server to the OpenAI Agents SDK.

Prerequisites
-------------
    pip install -e ".[sse,yfinance]"
    pip install openai-agents          # OpenAI Agents SDK

Then in a separate terminal start the MCP server:
    findata-mcp-sse                    # runs on http://localhost:8000

"""

import asyncio
from agents import Agent, Runner          # openai-agents
from agents.mcp import MCPServerSse       # MCP-over-SSE client built into the SDK


async def main():
    # -----------------------------------------------------------------
    # 1. Point the SDK at your running findata MCP server
    # -----------------------------------------------------------------
    mcp_server = MCPServerSse(
        url="http://localhost:8000/sse",
        # Optional: name shown in traces
        name="findata",
    )

    # -----------------------------------------------------------------
    # 2. Create an agent that has access to the MCP tools
    #    The agent will call search_tools / get_tool_doc automatically
    #    when it needs to figure out how to fetch financial data.
    # -----------------------------------------------------------------
    agent = Agent(
        name="FinAgent",
        instructions=(
            "You are a financial data coding assistant. "
            "When you need to fetch financial data, use the findata MCP tools "
            "to look up the correct function signature and write the code. "
            "Always use the findata library functions — do not use yfinance directly."
        ),
        mcp_servers=[mcp_server],
    )

    # -----------------------------------------------------------------
    # 3. Run a task
    # -----------------------------------------------------------------
    async with mcp_server:
        result = await Runner.run(
            agent,
            "Write Python code to fetch daily close prices for AAPL and MSFT "
            "for all of 2024, then compute the correlation between them.",
        )
        print(result.final_output)


if __name__ == "__main__":
    asyncio.run(main())