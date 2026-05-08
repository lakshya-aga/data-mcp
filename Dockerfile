FROM python:3.12-slim

# Why 3.12 (was 3.11 historically): pandas-ta 0.4.67b0 / 0.4.71b0 — the
# only versions still available on PyPI as of 2026-05 — declare
# Requires-Python >=3.12. The older 0.3.14b0 release we used to pin
# was yanked from PyPI. On 3.11 the resolver finds zero candidates
# and the build fails with "No matching distribution found for
# pandas-ta". Bumping to 3.12 unblocks the pandas-ta install; every
# other findata dep (yfinance, fredapi, pandas-datareader,
# mplfinance, scipy, scikit-learn) already supports 3.12. Aligns
# with finagent + fruit-thrower + knowledge-mcp which are all 3.12.
#
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
