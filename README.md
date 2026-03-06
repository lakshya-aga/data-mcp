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
│   ├── __init__.py
│   ├── equity_prices.py             get_equity_prices()         yfinance wrapper
│   ├── sp500_composition.py         get_sp500_composition()     fja05680/sp500 (local git clone)
│   │                                refresh_sp500_cache()
│   ├── file_reader.py               get_file_data()             CSV / Parquet / Excel
│   └── bloomberg.py                 get_bloomberg_data()        blpapi (stub — implement me)
├── findata_mcp/                     MCP server package
│   ├── __init__.py
│   ├── __main__.py
│   └── server.py                    Tool registry + MCP handlers
├── tests/
│   └── test_findata.py
├── pyproject.toml
└── README.md
```

---

## Installation

```bash
cd findata_mcp

# Core install (MCP server + CSV/Excel file reading)
pip install -e .

# Add yfinance support
pip install -e ".[yfinance]"

# Add Parquet support
pip install -e ".[parquet]"

# Add Bloomberg support (requires Bloomberg Terminal or B-PIPE)
pip install -e ".[bloomberg]"

# Everything except Bloomberg
pip install -e ".[all]"

# With dev/test tools
pip install -e ".[dev]"
```

> **Note:** The S&P 500 composition tool requires `git` on your system PATH
> (not a Python package).  It clones the dataset on first use.

---

## Running the MCP server

```bash
# After pip install -e .
findata-mcp

# From source without installing
python -m findata_mcp.server
```

The server communicates over **stdio** (standard MCP transport).

---

## MCP client configuration

### Claude Desktop
`~/Library/Application Support/Claude/claude_desktop_config.json` (macOS)
`%APPDATA%\Claude\claude_desktop_config.json` (Windows)

```json
{
  "mcpServers": {
    "findata": {
      "command": "findata-mcp"
    }
  }
}
```

### Cursor / other clients
```json
{
  "mcpServers": {
    "findata": {
      "command": "python",
      "args": ["-m", "findata_mcp.server"]
    }
  }
}
```

---

## MCP tools

| Tool | Description |
|---|---|
| `search_tools` | Natural-language query → matching function docs + code examples |
| `get_tool_doc` | Full reference for one function by exact name |
| `list_all_tools` | All wrapper functions with summaries and tags |

### `search_tools`
```json
{ "query": "equity daily prices", "top_k": 2 }
```
Returns: our wrapper's signature, parameter docs, and a copy-paste example.

### `get_tool_doc`
```json
{ "tool_name": "get_equity_prices" }
```

### `list_all_tools`
```json
{}
```

---

## findata quick reference

### `get_equity_prices`
```python
from findata.equity_prices import get_equity_prices

df = get_equity_prices(
    tickers=["AAPL", "MSFT"],
    start_date="2024-01-01",
    end_date="2024-12-31",
    fields=["Close"],       # Open High Low Close Volume — default: all
    frequency="1d",         # 1d 5d 1wk 1mo 3mo
    auto_adjust=True,
)
# Single ticker  → flat columns: df["Close"]
# Multi ticker   → MultiIndex:   df["Close"]["AAPL"]
```

### `get_sp500_composition`
```python
from findata.sp500_composition import get_sp500_composition, refresh_sp500_cache

# Clones https://github.com/fja05680/sp500 to ~/.cache/findata/sp500/ on first call
members = get_sp500_composition("2024-12-31")           # list[str], ~503 tickers
df      = get_sp500_composition("2024-12-31", return_dataframe=True)

refresh_sp500_cache()   # git pull + clear in-memory cache

# Override cache location
import os
os.environ["FINDATA_CACHE_DIR"] = "/data/cache"
```

### `get_file_data`
```python
from findata.file_reader import get_file_data

df = get_file_data(
    "data/prices.parquet",
    tickers=["AAPL", "MSFT"],
    start_date="2023-01-01",
    end_date="2023-12-31",
    fields=["close", "volume"],
    date_column="date",       # override column names if needed
    ticker_column="ticker",
)
```

### `get_bloomberg_data` *(stub — implement session logic first)*
```python
from findata.bloomberg import get_bloomberg_data

df = get_bloomberg_data(
    tickers=["AAPL US Equity"],
    fields=["PX_LAST", "VOLUME"],
    start_date="2024-01-01",
    end_date="2024-12-31",
    request_type="HistoricalDataRequest",  # or "ReferenceDataRequest"
    overrides={"BEST_FPERIOD_OVERRIDE": "1BF"},   # optional
)
```

---

## Adding a new data source

1. Create `findata/your_source.py` with a well-documented wrapper function.
2. Add one entry to `_REGISTRY` in `findata_mcp/server.py`:

```python
{
    "name": "get_your_data",
    "callable": get_your_data,
    "module": "findata.your_source",
    "tags": ["keyword1", "keyword2"],
    "stub": False,
    "install_requires": ["your-package"],
    "summary": "One sentence describing what this wrapper fetches.",
    "example": "from findata.your_source import get_your_data\ndf = get_your_data(...)\n",
}
```

3. Restart the MCP server — the function is immediately discoverable.

---

## Tests

```bash
pytest tests/ -v
```
