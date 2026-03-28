#!/usr/bin/env python3
"""
smoke_test.py — pull the findata-mcp container and run sample MCP requests.

Usage:
    python smoke_test.py
"""
import asyncio
import subprocess
import sys
import time

IMAGE = "ghcr.io/lakshya-aga/data-mcp:latest"
CONTAINER = "findata-mcp-test"
PORT = 8000
URL = f"http://localhost:{PORT}/sse"


def run(cmd, **kwargs):
    print(f"$ {cmd}")
    return subprocess.run(cmd, shell=True, check=True, **kwargs)


def pull_and_start():
    print("\n── Pull ─────────────────────────────────────────")
    run(f"docker pull {IMAGE}")

    print("\n── Start container ──────────────────────────────")
    run(f"docker rm -f {CONTAINER} 2>/dev/null || true")
    run(
        f"docker run -d --name {CONTAINER} -p {PORT}:{PORT} "
        f"-v ~/.codex:/root/.codex:ro "
        f"-v ~/.claude:/root/.claude:ro "
        f"{IMAGE}"
    )

    print("\n── Waiting for server to be ready ───────────────")
    deadline = time.time() + 30
    import urllib.request, urllib.error
    while time.time() < deadline:
        try:
            urllib.request.urlopen(f"http://localhost:{PORT}/sse", timeout=2)
            break
        except Exception:
            time.sleep(1)
    else:
        print("Server did not become ready in time.")
        run(f"docker logs {CONTAINER}")
        sys.exit(1)
    print("Server ready.")


async def requests():
    from mcp.client.sse import sse_client
    from mcp.client.session import ClientSession

    async with sse_client(URL) as (r, w):
        async with ClientSession(r, w) as s:
            await s.initialize()

            # 1. List all tools
            print("\n── list_all_tools ───────────────────────────────")
            res = await s.call_tool("list_all_tools", {})
            print(res.content[0].text[:800])

            # 2. Search
            print("\n── search_tools: 'equity daily prices' ──────────")
            res = await s.call_tool("search_tools", {"query": "equity daily prices", "top_k": 2})
            print(res.content[0].text[:800])

            # 3. Get full doc for a specific function
            print("\n── get_tool_doc: get_fama_french_factors ────────")
            res = await s.call_tool("get_tool_doc", {"function_name": "get_fama_french_factors"})
            print(res.content[0].text[:800])

            # 4. Search volatility
            print("\n── search_tools: 'volatility index vix' ─────────")
            res = await s.call_tool("search_tools", {"query": "volatility index vix", "top_k": 2})
            print(res.content[0].text[:800])

            print("\n── All requests passed ──────────────────────────")


if __name__ == "__main__":
    pull_and_start()
    try:
        asyncio.run(requests())
    finally:
        print(f"\n── Stopping container ───────────────────────────")
        subprocess.run(f"docker rm -f {CONTAINER}", shell=True)
