FROM python:3.11-slim

WORKDIR /app

# Install system deps (git is needed by get_sp500_composition on first use)
RUN apt-get update && apt-get install -y --no-install-recommends git \
    && rm -rf /var/lib/apt/lists/*

# Copy package files first so pip layer is cached separately from source changes
COPY pyproject.toml .
COPY findata/       findata/
COPY findata_mcp/   findata_mcp/

# Install the package + all transport/data-source extras
RUN pip install --no-cache-dir -e ".[all]" \
    && pip install --no-cache-dir \
        pandas-datareader \
        fredapi \
        requests

# findata-mcp-sse binds here
EXPOSE 8000

# SSE/HTTP transport — the right mode for containerised deployments.
# Agents connect via:  GET  http://<host>:8000/sse
#                      POST http://<host>:8000/messages
CMD ["findata-mcp-sse", "--host", "0.0.0.0", "--port", "8000"]
