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
data-mcp/
├── findata/                         Data library
│   ├── equity_prices.py             get_equity_prices()           yfinance wrapper
│   ├── stooq.py                     get_stooq_prices()            Stooq (international markets)
│   ├── sp500_composition.py         get_sp500_composition()       fja05680/sp500 (local git clone)
│   ├── fama_french.py               get_fama_french_factors()     Ken French Data Library
│   ├── ken_french_factors.py        get_ken_french_factors()      Ken French Data Library
│   ├── fred.py                      get_fred_series()             FRED macroeconomic series
│   ├── cboe_volatility.py           get_cboe_volatility_indices() VIX / VVIX
│   ├── coingecko.py                 get_coingecko_ohlcv()         CoinGecko public API
│   ├── binance.py                   get_binance_klines()          Binance public REST API
│   ├── coinbase.py                  get_coinbase_candles()        Coinbase Exchange public REST API
│   ├── sec_edgar.py                 get_sec_edgar_filings()       SEC EDGAR submissions JSON
│   ├── alphavantage.py              get_alphavantage_prices()     AlphaVantage REST API
│   ├── tiingo.py                    get_tiingo_prices()           Tiingo REST API
│   ├── polygon.py                   get_polygon_aggregates()      Polygon.io REST API
│   ├── wrds_data.py                 get_wrds_data()               CRSP/Compustat via WRDS
│   ├── file_reader.py               get_file_data()               CSV / Parquet / Excel
│   └── bloomberg.py                 get_bloomberg_data()          blpapi (stub)
├── findata_mcp/
│   └── server.py                    Tool registry + MCP handlers
├── Dockerfile
├── docker-compose.yml
├── .github/workflows/docker.yml     GHCR build + push on every push to main
├── pyproject.toml
└── README.md
```

---

## Installation

The recommended way to run findata-mcp is via Docker. The image is published to GHCR on every push to `main` and includes Codex CLI baked in.

### Prerequisites

- Docker
- Codex authenticated on your host machine

### 1. Authenticate Codex (one-time)

```bash
codex auth login    # opens browser → saves to ~/.codex/auth.json
```

### 2. Pull and run

```bash
curl -O https://raw.githubusercontent.com/lakshya-aga/data-mcp/main/docker-compose.yml
docker compose up -d
```

`docker-compose.yml` mounts `~/.codex` read-only so the container inherits your Codex session with no interactive prompts. Named volumes keep generated files and data across restarts.

### 3. Verify

```bash
docker logs data-mcp-findata-mcp-1
# should show: findata-mcp starting on :8000
```

---

## Connecting to the server

### Claude Desktop

Add to `~/Library/Application Support/Claude/claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "findata": {
      "url": "http://localhost:8000/sse"
    }
  }
}
```

### Python (raw MCP client)

```python
import asyncio
from mcp.client.sse import sse_client
from mcp.client.session import ClientSession

async def main():
    async with sse_client("http://localhost:8000/sse") as (r, w):
        async with ClientSession(r, w) as s:
            await s.initialize()
            res = await s.call_tool("search_tools", {"query": "equity daily prices", "top_k": 3})
            print(res.content[0].text)

asyncio.run(main())
```

### OpenAI Agents SDK

```python
from agents.mcp import MCPServerSse
mcp = MCPServerSse(url="http://localhost:8000/sse")
```

---

## MCP tools

| Tool | Description |
|------|-------------|
| `search_tools` | Natural-language query → matching function docs + code examples |
| `get_tool_doc` | Full reference for one function by exact name |
| `list_all_tools` | All wrapper functions with summaries and tags |
| `request_data_source` | Ask Codex to implement and register a new data wrapper |

### search_tools

```python
res = await s.call_tool("search_tools", {"query": "fama french factors", "top_k": 2})
```

### get_tool_doc

```python
res = await s.call_tool("get_tool_doc", {"tool_name": "get_equity_prices"})
```

### request_data_source

```python
res = await s.call_tool("request_data_source", {
    "description": "get World Bank GDP per capita using the wbdata library"
})
```

Codex writes `findata/<module>.py`, updates `server.py`, and hot-reloads the new function into the live registry — no restart needed.

---

## Environment variables

| Variable | Description |
|----------|-------------|
| `OPENAI_API_KEY` | Codex auth — skips OAuth if set (alternative to host auth mount) |
| `FRED_API_KEY` | Required for `get_fred_series`. Free at [fred.stlouisfed.org](https://fred.stlouisfed.org/docs/api/api_key.html) |
| `ALPHAVANTAGE_API_KEY` | Required for `get_alphavantage_prices`. Free tier at [alphavantage.co](https://www.alphavantage.co/support/#api-key) |
| `TIINGO_API_KEY` | Required for `get_tiingo_prices`. Free tier at [tiingo.com](https://www.tiingo.com/account/api/token) |
| `POLYGON_API_KEY` | Required for `get_polygon_aggregates`. Free tier at [polygon.io](https://polygon.io/dashboard/api-keys) |
| `SEC_EDGAR_USER_AGENT` | Required for `get_sec_edgar_filings`. Descriptive contact string, e.g. `"Acme Research research@example.com"` |
| `WRDS_USERNAME` / `WRDS_PASSWORD` | Required for `get_wrds_data` if not using `~/.pgpass`. Free for university affiliates at [wrds.wharton.upenn.edu](https://wrds-www.wharton.upenn.edu/) |
| `CODEX_CLI_PATH` | Override Codex binary path (defaults to `codex` on PATH) |

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
