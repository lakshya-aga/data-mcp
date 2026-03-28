#!/usr/bin/env bash
# entrypoint.sh
# -------------
# Runs OAuth setup for Codex CLI and Claude Code on first start,
# then hands off to the MCP server (CMD).
#
# Credentials are persisted via volume mounts so auth only runs once:
#   docker run -it \
#     -v findata-codex:/root/.codex \
#     -v findata-claude:/root/.claude \
#     ...
set -e

CODEX_AUTH_FILE="${HOME}/.codex/auth.json"
CLAUDE_AUTH_FILE="${HOME}/.claude/credentials.json"

# ---------------------------------------------------------------------------
# Codex CLI
# ---------------------------------------------------------------------------
if [ -z "$OPENAI_API_KEY" ] && [ ! -f "$CODEX_AUTH_FILE" ]; then
    echo ""
    echo "══════════════════════════════════════════════════"
    echo "  Codex CLI — OAuth login required"
    echo "══════════════════════════════════════════════════"
    echo "  A browser window will open (or copy the URL)."
    echo ""
    codex auth login
    echo ""
    echo "  Codex auth complete."
fi

# ---------------------------------------------------------------------------
# Claude Code
# ---------------------------------------------------------------------------
if [ -z "$ANTHROPIC_API_KEY" ] && [ ! -f "$CLAUDE_AUTH_FILE" ]; then
    echo ""
    echo "══════════════════════════════════════════════════"
    echo "  Claude Code — OAuth login required"
    echo "══════════════════════════════════════════════════"
    echo "  A browser window will open (or copy the URL)."
    echo ""
    claude auth login
    echo ""
    echo "  Claude auth complete."
fi

# ---------------------------------------------------------------------------
# Start MCP server
# ---------------------------------------------------------------------------
echo ""
echo "══════════════════════════════════════════════════"
echo "  findata-mcp starting on :8000"
echo "══════════════════════════════════════════════════"
echo ""

exec "$@"
