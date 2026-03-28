FROM python:3.11-slim

# System deps:
#   git  — get_sp500_composition clones a repo on first use
#   curl — NodeSource setup script
#   nodejs/npm — Codex CLI + Claude Code CLI
RUN apt-get update \
    && apt-get install -y --no-install-recommends git curl \
    && curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get install -y --no-install-recommends nodejs \
    && rm -rf /var/lib/apt/lists/*

# Install Codex CLI and Claude Code CLI globally
RUN npm install -g @openai/codex @anthropic-ai/claude-code

# Python package
WORKDIR /app
COPY pyproject.toml .
COPY findata/       findata/
COPY findata_mcp/   findata_mcp/

RUN pip install --no-cache-dir -e ".[all]" \
    && pip install --no-cache-dir \
        pandas-datareader \
        fredapi \
        requests

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 8000

ENTRYPOINT ["/entrypoint.sh"]
CMD ["findata-mcp-sse", "--host", "0.0.0.0", "--port", "8000"]
