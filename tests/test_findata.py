"""
tests/test_findata.py
---------------------
Run with:  pytest tests/ -v
"""
import sys
import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import patch, MagicMock


# ── equity_prices ─────────────────────────────────────────────────────────────

class TestGetEquityPrices:

    def test_raises_on_empty_tickers(self):
        from findata.equity_prices import get_equity_prices
        with pytest.raises(ValueError, match="non-empty"):
            get_equity_prices([], "2024-01-01", "2024-01-31")

    def test_raises_on_bad_frequency(self):
        from findata.equity_prices import get_equity_prices
        with pytest.raises(ValueError, match="frequency"):
            get_equity_prices(["AAPL"], "2024-01-01", "2024-01-31", frequency="bad")

    def test_single_ticker_returns_flat_columns(self):
        from findata.equity_prices import get_equity_prices
        mock_df = pd.DataFrame(
            {"Close": [185.0, 186.0]},
            index=pd.to_datetime(["2024-01-02", "2024-01-03"]),
        )
        with patch("yfinance.download", return_value=mock_df):
            result = get_equity_prices(["AAPL"], "2024-01-01", "2024-01-05", fields=["Close"])
        assert not isinstance(result.columns, pd.MultiIndex)
        assert "Close" in result.columns

    def test_multi_ticker_preserves_multiindex(self):
        from findata.equity_prices import get_equity_prices
        arrays = [["Close", "Close"], ["AAPL", "MSFT"]]
        mi = pd.MultiIndex.from_arrays(arrays)
        mock_df = pd.DataFrame(
            [[185.0, 374.0], [186.0, 375.0]],
            index=pd.to_datetime(["2024-01-02", "2024-01-03"]),
            columns=mi,
        )
        with patch("yfinance.download", return_value=mock_df):
            result = get_equity_prices(["AAPL", "MSFT"], "2024-01-01", "2024-01-05")
        assert isinstance(result.columns, pd.MultiIndex)

    def test_missing_yfinance_raises_import_error(self):
        from findata.equity_prices import get_equity_prices
        with patch.dict(sys.modules, {"yfinance": None}):
            with pytest.raises((ImportError, TypeError)):
                get_equity_prices(["AAPL"], "2024-01-01", "2024-01-05")


# ── binance ───────────────────────────────────────────────────────────────────


def _mock_binance_kline_row(open_time_ms: int, close: float = 60000.0):
    """Build one Binance klines row with the 12-element shape the API returns."""
    return [
        open_time_ms,                # open_time
        f"{close - 100:.2f}",        # open
        f"{close + 100:.2f}",        # high
        f"{close - 200:.2f}",        # low
        f"{close:.2f}",              # close
        "100.5",                     # volume
        open_time_ms + 86_400_000 - 1,  # close_time
        f"{close * 100.5:.2f}",      # quote_volume
        12345,                       # trades
        "50.0",                      # taker_buy_base
        f"{close * 50.0:.2f}",       # taker_buy_quote
        "0",                         # ignore
    ]


class TestGetBinanceOhlcv:

    def test_raises_on_empty_symbol(self):
        from findata.binance import get_binance_ohlcv
        with pytest.raises(ValueError, match="symbol"):
            get_binance_ohlcv("", interval="1d")

    def test_raises_on_bad_interval(self):
        from findata.binance import get_binance_ohlcv
        with pytest.raises(ValueError, match="interval"):
            get_binance_ohlcv("BTCUSDT", interval="bad")

    def test_raises_on_bad_limit(self):
        from findata.binance import get_binance_ohlcv
        with pytest.raises(ValueError, match="limit"):
            get_binance_ohlcv("BTCUSDT", limit=5000)

    def test_raises_on_inverted_range(self):
        from findata.binance import get_binance_ohlcv
        with pytest.raises(ValueError, match=r"end_date must be"):
            get_binance_ohlcv(
                "BTCUSDT", start_date="2024-12-31", end_date="2024-01-01",
            )

    def test_basic_shape_and_columns(self):
        from findata.binance import get_binance_ohlcv
        rows = [_mock_binance_kline_row(open_time_ms=1_700_000_000_000 + i * 86_400_000)
                for i in range(3)]
        with patch("requests.get") as mock_get:
            mock_get.return_value.json.return_value = rows
            mock_get.return_value.raise_for_status = lambda: None
            df = get_binance_ohlcv("BTCUSDT", interval="1d", limit=3)
        assert df.shape == (3, 7)
        assert set(["open", "high", "low", "close", "volume",
                    "quote_volume", "trades"]) == set(df.columns)
        assert df.index.name == "date"
        # close column round-trips as float, trades as Int64
        assert df["close"].dtype.kind == "f"
        assert str(df["trades"].dtype) == "Int64"

    def test_dedup_overlapping_pages(self):
        """Pagination boundary candle should not appear twice."""
        from findata.binance import get_binance_ohlcv
        page1 = [_mock_binance_kline_row(open_time_ms=1_700_000_000_000 + i * 86_400_000)
                 for i in range(2)]
        # Page 2 starts at the same boundary as page 1's last; the loader
        # should drop the duplicate and return 3 unique rows.
        page2 = [_mock_binance_kline_row(open_time_ms=1_700_000_000_000 + i * 86_400_000)
                 for i in range(1, 3)]
        responses = [page1, page2, []]
        with patch("requests.get") as mock_get:
            def fake_get(*_a, **_kw):
                m = MagicMock()
                m.json.return_value = responses.pop(0)
                m.raise_for_status = lambda: None
                return m
            mock_get.side_effect = fake_get
            df = get_binance_ohlcv(
                "BTCUSDT", interval="1d",
                start_date="2023-11-14", end_date="2023-11-30",
                limit=2,
            )
        # 3 unique open_times across the two overlapping pages.
        assert df.shape[0] == 3

    def test_raises_when_no_candles(self):
        from findata.binance import get_binance_ohlcv
        with patch("requests.get") as mock_get:
            mock_get.return_value.json.return_value = []
            mock_get.return_value.raise_for_status = lambda: None
            with pytest.raises(ValueError, match="no candles"):
                get_binance_ohlcv("FAKEUSDT", interval="1d", limit=10)


# ── sp500_composition ─────────────────────────────────────────────────────────

def _mock_csv_df():
    return pd.DataFrame({
        "date": pd.to_datetime(["2000-01-03", "2010-01-04", "2020-01-02"]),
        "tickers": [
            ["AAPL", "MSFT", "GE"],
            ["AAPL", "MSFT", "XOM"],
            ["AAPL", "MSFT", "AMZN", "GOOG"],
        ],
    })


class TestGetSp500Composition:

    def setup_method(self):
        # Clear lru_cache before each test so mocks take effect
        from findata.sp500_composition import _load_csv
        _load_csv.cache_clear()

    def test_returns_list_by_default(self):
        from findata.sp500_composition import get_sp500_composition
        with patch("findata.sp500_composition._load_csv", return_value=_mock_csv_df()):
            result = get_sp500_composition("2015-06-01")
        assert isinstance(result, list)
        assert "AAPL" in result

    def test_returns_dataframe_when_asked(self):
        from findata.sp500_composition import get_sp500_composition
        with patch("findata.sp500_composition._load_csv", return_value=_mock_csv_df()):
            result = get_sp500_composition("2015-06-01", return_dataframe=True)
        assert isinstance(result, pd.DataFrame)
        assert "ticker" in result.columns

    def test_point_in_time_correctness(self):
        """A date in 2005 should resolve to the 2000 snapshot."""
        from findata.sp500_composition import get_sp500_composition
        with patch("findata.sp500_composition._load_csv", return_value=_mock_csv_df()):
            result = get_sp500_composition("2005-01-01")
        assert set(result) == {"AAPL", "MSFT", "GE"}

    def test_2020_snapshot(self):
        """A date on or after 2020-01-02 should return the 2020 snapshot."""
        from findata.sp500_composition import get_sp500_composition
        with patch("findata.sp500_composition._load_csv", return_value=_mock_csv_df()):
            result = get_sp500_composition("2024-01-01")
        assert set(result) == {"AAPL", "MSFT", "AMZN", "GOOG"}

    def test_raises_before_earliest_date(self):
        from findata.sp500_composition import get_sp500_composition
        with patch("findata.sp500_composition._load_csv", return_value=_mock_csv_df()):
            with pytest.raises(ValueError, match="earlier than"):
                get_sp500_composition("1990-01-01")

    def test_accepts_date_object(self):
        from datetime import date
        from findata.sp500_composition import get_sp500_composition
        with patch("findata.sp500_composition._load_csv", return_value=_mock_csv_df()):
            result = get_sp500_composition(date(2005, 6, 1))
        assert isinstance(result, list)


# ── file_reader ───────────────────────────────────────────────────────────────

class TestGetFileData:

    def test_raises_on_missing_file(self, tmp_path):
        from findata.file_reader import get_file_data
        with pytest.raises(FileNotFoundError):
            get_file_data(tmp_path / "nope.csv")

    def test_loads_csv(self, tmp_path):
        from findata.file_reader import get_file_data
        p = tmp_path / "prices.csv"
        pd.DataFrame({
            "date": ["2024-01-02", "2024-01-03"],
            "ticker": ["AAPL", "AAPL"],
            "close": [185.0, 186.0],
        }).to_csv(p, index=False)
        df = get_file_data(p)
        assert len(df) == 2
        assert isinstance(df.index, pd.DatetimeIndex)

    def test_ticker_filter(self, tmp_path):
        from findata.file_reader import get_file_data
        p = tmp_path / "prices.csv"
        pd.DataFrame({
            "date": ["2024-01-02", "2024-01-03"],
            "ticker": ["AAPL", "MSFT"],
            "close": [185.0, 374.0],
        }).to_csv(p, index=False)
        df = get_file_data(p, tickers=["AAPL"])
        assert len(df) == 1

    def test_date_range_filter(self, tmp_path):
        from findata.file_reader import get_file_data
        p = tmp_path / "prices.csv"
        pd.DataFrame({
            "date": ["2024-01-02", "2024-01-03", "2024-01-04"],
            "ticker": ["AAPL"] * 3,
            "close": [185.0, 186.0, 187.0],
        }).to_csv(p, index=False)
        df = get_file_data(p, start_date="2024-01-03", end_date="2024-01-03")
        assert len(df) == 1

    def test_unknown_extension_raises(self, tmp_path):
        from findata.file_reader import get_file_data
        p = tmp_path / "data.xyz"
        p.write_text("x")
        with pytest.raises(ValueError, match="Cannot infer"):
            get_file_data(p)

    def test_explicit_format_override(self, tmp_path):
        from findata.file_reader import get_file_data
        # Save a CSV but name it .dat — use explicit format
        p = tmp_path / "prices.dat"
        pd.DataFrame({
            "date": ["2024-01-02"],
            "ticker": ["AAPL"],
            "close": [185.0],
        }).to_csv(p, index=False)
        df = get_file_data(p, file_format="csv")
        assert len(df) == 1


# ── bloomberg stub ────────────────────────────────────────────────────────────

class TestBloombergStub:

    def test_raises_not_implemented_with_blpapi_present(self):
        from findata.bloomberg import get_bloomberg_data
        with patch.dict(sys.modules, {"blpapi": MagicMock()}):
            with pytest.raises(NotImplementedError):
                get_bloomberg_data(
                    tickers=["AAPL US Equity"],
                    fields=["PX_LAST"],
                    start_date="2024-01-01",
                    end_date="2024-01-31",
                )

    def test_raises_import_error_without_blpapi(self):
        import importlib
        with patch.dict(sys.modules, {"blpapi": None}):
            import findata.bloomberg as bb
            importlib.reload(bb)
            with pytest.raises((ImportError, TypeError)):
                bb.get_bloomberg_data(
                    tickers=["AAPL US Equity"],
                    fields=["PX_LAST"],
                )


# ── MCP registry sanity ───────────────────────────────────────────────────────

def _stub_mcp():
    """Inject stub mcp modules so the server can be imported without the real package."""
    mcp_stub = MagicMock()
    mcp_stub.server.Server = MagicMock(return_value=MagicMock())
    sys.modules.setdefault("mcp", mcp_stub)
    sys.modules.setdefault("mcp.server", mcp_stub.server)
    sys.modules.setdefault("mcp.server.stdio", MagicMock())
    sys.modules.setdefault("mcp.types", MagicMock())


class TestMcpRegistry:

    def test_all_required_keys_present(self):
        _stub_mcp()
        from findata_mcp.server import _REGISTRY
        required = {"name", "callable", "module", "tags", "stub",
                    "install_requires", "summary", "example"}
        for entry in _REGISTRY:
            missing = required - entry.keys()
            assert not missing, f"{entry['name']} is missing keys: {missing}"

    def test_no_duplicate_names(self):
        _stub_mcp()
        from findata_mcp.server import _REGISTRY
        names = [e["name"] for e in _REGISTRY]
        assert len(names) == len(set(names)), "Duplicate names in _REGISTRY"

    def test_registry_by_name_index_complete(self):
        _stub_mcp()
        from findata_mcp.server import _REGISTRY, _REGISTRY_BY_NAME
        assert set(_REGISTRY_BY_NAME.keys()) == {e["name"] for e in _REGISTRY}

    def test_render_doc_returns_string(self):
        _stub_mcp()
        from findata_mcp.server import _REGISTRY, _render_doc
        for entry in _REGISTRY:
            doc = _render_doc(entry)
            assert isinstance(doc, str)
            assert entry["name"] in doc
            assert "```python" in doc

    def test_score_equity_query(self):
        _stub_mcp()
        from findata_mcp.server import _REGISTRY, _score
        eq_entry = next(e for e in _REGISTRY if e["name"] == "get_equity_prices")
        sp_entry = next(e for e in _REGISTRY if e["name"] == "get_sp500_composition")
        assert _score(eq_entry, "equity daily prices") > _score(sp_entry, "equity daily prices")

    def test_score_sp500_query(self):
        _stub_mcp()
        from findata_mcp.server import _REGISTRY, _score
        eq_entry = next(e for e in _REGISTRY if e["name"] == "get_equity_prices")
        sp_entry = next(e for e in _REGISTRY if e["name"] == "get_sp500_composition")
        assert _score(sp_entry, "sp500 constituents") > _score(eq_entry, "sp500 constituents")
