# findata-mcp

A unified financial data library with an **MCP server** for **code-writing agents**.

When an agent queries the MCP (e.g. `"equity daily prices"`), it receives:
- The **findata wrapper function** signature
- Full parameter and return-type documentation
- A ready-to-paste code example calling our API

The MCP never fetches live data — it is a documentation server so agents
can write correct calls to the findata library.

---

## Project structure

```
findata_mcp/
├── findata/                         Data library
│   ├── equity_prices.py             get_equity_prices()           yfinance wrapper
│   ├── sp500_composition.py         get_sp500_composition()       fja05680/sp500 (local git clone)
│   ├── fama_french.py               get_fama_french_factors()     Ken French Data Library
│   ├── fred.py                      get_fred_series()             FRED macroeconomic series
│   ├── cboe_volatility.py           get_cboe_volatility_indices() VIX / VVIX
│   ├── coingecko.py                 get_coingecko_ohlcv()         CoinGecko public API
│   ├── file_reader.py               get_file_data()               CSV / Parquet / Excel
│   └── bloomberg.py                 get_bloomberg_data()          blpapi (stub)
├── findata_mcp/
│   └── server.py                    Tool registry + MCP handlers
├── Dockerfile
├── .github/workflows/docker.yml     GHCR build + push
├── pyproject.toml
└── README.md
```

---

## Installation

The recommended way to run findata-mcp is via Docker. The image is published to GHCR on every push to `main` and includes Codex CLI and Claude Code baked in.

### Prerequisites

- Docker
- Codex and/or Claude authenticated on your host machine

### 1. Authenticate on your host (one-time)

```bash
codex auth login    # opens browser → saves to ~/.codex/auth.json
claude auth login   # opens browser → saves to ~/.claude/credentials.json
```

### 2. Pull and run

```bash
docker compose up -d
```

That's it. `docker-compose.yml` mounts `~/.codex` and `~/.claude` read-only so the container inherits your auth with no interactive prompts.

Or with `docker run` directly:

```bash
docker run -d -p 8000:8000 \
  -v ~/.codex:/root/.codex:ro \
  -v ~/.claude:/root/.claude:ro \
  ghcr.io/lakshya-aga/data-mcp:latest
```

### Auth options

| Option | How |
|--------|-----|
| **Mount host auth (recommended)** | Login on host, mount `~/.codex` and `~/.claude` read-only |
| **API keys** | Pass `OPENAI_API_KEY` / `ANTHROPIC_API_KEY` as env vars — skips OAuth entirely |
| **Interactive OAuth** | Run with `-it` and no auth — container prompts you on first start |

```bash
# API keys (non-interactive)
docker run -d -p 8000:8000 \
  -e OPENAI_API_KEY=sk-... \
  -e ANTHROPIC_API_KEY=sk-ant-... \
  -e FRED_API_KEY=... \
  ghcr.io/lakshya-aga/data-mcp:latest
```

### Environment variables

| Variable | Description |
|---|---|
| `OPENAI_API_KEY` | Codex CLI auth — skips Codex OAuth if set |
| `ANTHROPIC_API_KEY` | Claude auth — skips Claude OAuth if set |
| `FRED_API_KEY` | Required for `get_fred_series`. Free at [fred.stlouisfed.org](https://fred.stlouisfed.org/docs/api/api_key.html) |
| `CODEX_CLI_PATH` | Override Codex binary path (defaults to `codex` on PATH) |

### Endpoints

| Endpoint | Purpose |
|---|---|
| `GET  /sse` | SSE stream — agents connect here first |
| `POST /messages` | Agent sends tool calls here |

### Connect Claude Desktop

```json
{
  "mcpServers": {
    "findata": {
      "url": "http://localhost:8000/sse"
    }
  }
}
```

### Connecting from an agent

```python
# OpenAI Agents SDK
from agents.mcp import MCPServerSse
mcp = MCPServerSse(url="http://localhost:8000/sse")

# Raw MCP Python client
from mcp.client.sse import sse_client
from mcp import ClientSession

async with sse_client("http://localhost:8000/sse") as (r, w):
    async with ClientSession(r, w) as session:
        await session.initialize()
        result = await session.call_tool("search_tools", {"query": "equity prices"})
```

### `request_data_source` — writing new wrappers

Codex is baked into the image. To let Codex write new wrapper files back to
disk, mount the repo source:

```bash
docker run -d -p 8000:8000 \
  -v ~/.codex:/root/.codex:ro \
  -v $(pwd):/app \
  ghcr.io/lakshya-aga/data-mcp:latest
```

New wrappers written by Codex are hot-reloaded into the live registry immediately — no restart needed.

---

## Local installation

```bash
# Core install (MCP server + CSV/Excel file reading)
pip install -e .

# Everything except Bloomberg
pip install -e ".[all]"

# With dev/test tools
pip install -e ".[dev]"
```

Copy `.env.example` to `.env` and fill in any keys you need.

---

## Running locally

```bash
# stdio transport (Claude Desktop / CLI agents)
findata-mcp

# SSE/HTTP transport (web-based agents, OpenAI Agents SDK)
findata-mcp-sse --host 0.0.0.0 --port 8000
```

---

## MCP tools

| Tool | Description |
|---|---|
| `search_tools` | Natural-language query → matching function docs + code examples |
| `get_tool_doc` | Full reference for one function by exact name |
| `list_all_tools` | All wrapper functions with summaries and tags |
| `request_data_source` | Ask Codex to implement and register a new data wrapper |

### `search_tools`
```json
{ "query": "fama french factors", "top_k": 2 }
```

### `request_data_source`
```json
{ "description": "add World Bank GDP indicators using wbdata" }
```
Codex writes `findata/<module>.py`, updates `server.py`, validates the import,
and hot-reloads the new function into the live registry.

---

## findata quick reference

### `get_equity_prices`
```python
from findata.equity_prices import get_equity_prices

df = get_equity_prices(
    tickers=["AAPL", "MSFT"],
    start_date="2024-01-01",
    end_date="2024-12-31",
    fields=["Close"],
    frequency="1d",         # 1d 5d 1wk 1mo 3mo
)
```

### `get_fama_french_factors`
```python
from findata.fama_french import get_fama_french_factors

df = get_fama_french_factors(factor_model="5", start_date="2010-01-01", end_date="2020-12-31")
# columns: Mkt-RF, SMB, HML, RMW, CMA, RF
```

### `get_fred_series`
```python
from findata.fred import get_fred_series

df = get_fred_series(["CPIAUCSL", "UNRATE"], start_date="2015-01-01", end_date="2024-12-31")
```

### `get_coingecko_ohlcv`
```python
from findata.coingecko import get_coingecko_ohlcv

df = get_coingecko_ohlcv("bitcoin", vs_currency="usd", days=90)
# columns: open, high, low, close, volume
```

### `get_cboe_volatility_indices`
```python
from findata.cboe_volatility import get_cboe_volatility_indices

df = get_cboe_volatility_indices(symbols=["^VIX", "^VVIX"], start_date="2020-01-01", end_date="2024-12-31")
```

### `get_sp500_composition`
```python
from findata.sp500_composition import get_sp500_composition

members = get_sp500_composition("2024-12-31")   # list[str], ~503 tickers
```

### `get_file_data`
```python
from findata.file_reader import get_file_data

df = get_file_data("data/prices.parquet", tickers=["AAPL"], start_date="2023-01-01", end_date="2023-12-31")
```

---

## Tests

```bash
pytest tests/ -v
```
