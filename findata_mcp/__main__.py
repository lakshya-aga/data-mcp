"""Allow `python -m findata_mcp` to start the stdio MCP server."""
from findata_mcp.server import _main_sync

_main_sync()
