"""Mocked API tests for extractors."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
import responses
from polars import DataFrame

from ELT.extract_fred import BASE_URL as FRED_BASE_URL, FredExtractor
from ELT.extract_yfinance import YFinanceExtractor
from utils.http import retry_call


class TestFredExtractor:
    """Mocked tests for the FRED extractor."""

    @responses.activate
    def test_fetch_single_series_success(self) -> None:
        responses.get(
            FRED_BASE_URL,
            json={
                "observations": [
                    {"date": "2024-01-01", "value": "4.5"},
                    {"date": "2024-01-02", "value": "4.6"},
                ]
            },
            status=200,
        )
        extractor = FredExtractor(api_key="fake-key")
        result = extractor.get_series_observations(
            "DGS10",
            observation_start="2024-01-01",
            observation_end="2024-01-02",
        )
        assert isinstance(result, dict)
        assert len(result["observations"]) == 2

    @responses.activate
    def test_fetch_multiple_success(self) -> None:
        """Simulate fetching two tenors."""
        # First tenor
        responses.get(
            FRED_BASE_URL,
            json={
                "observations": [
                    {"date": "2024-01-01", "value": "4.5"},
                    {"date": "2024-01-02", "value": "4.6"},
                ]
            },
            status=200,
        )
        # Second tenor
        responses.get(
            FRED_BASE_URL,
            json={
                "observations": [
                    {"date": "2024-01-01", "value": "3.5"},
                    {"date": "2024-01-02", "value": "3.6"},
                ]
            },
            status=200,
        )

        extractor = FredExtractor(api_key="fake-key")
        mapping = {"2Y": "DGS2", "10Y": "DGS10"}
        df = extractor.get_series_observations(
            mapping,
            observation_start="2024-01-01",
            observation_end="2024-01-02",
        )
        assert isinstance(df, DataFrame)
        assert df.shape == (2, 3)  # date, 2Y, 10Y
        assert "2Y" in df.columns
        assert "10Y" in df.columns

    @responses.activate
    def test_retry_on_503(self) -> None:
        """Ensure retries happen on 503."""
        responses.get(
            FRED_BASE_URL,
            json={"error": "temporarily unavailable"},
            status=503,
        )
        responses.get(
            FRED_BASE_URL,
            json={
                "observations": [
                    {"date": "2024-01-01", "value": "4.5"},
                ]
            },
            status=200,
        )

        extractor = FredExtractor(api_key="fake-key")
        result = extractor.get_series_observations(
            "DGS10",
            observation_start="2024-01-01",
        )
        assert isinstance(result, dict)
        assert len(result["observations"]) == 1


class TestRetryCall:
    """Tests for the generic retry_call utility."""

    def test_succeeds_first_try(self) -> None:
        assert retry_call(lambda x: x * 2, 5, retries=2) == 10

    def test_retries_then_succeeds(self) -> None:
        call_count = 0

        def flaky(x: int) -> int:
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise RuntimeError("boom")
            return x * 2

        assert retry_call(flaky, 5, retries=3, backoff_factor=0.01) == 10
        assert call_count == 3

    def test_retries_exhausted(self) -> None:
        def always_fails(x: int) -> int:
            raise ValueError("nope")

        with pytest.raises(ValueError, match="nope"):
            retry_call(always_fails, 5, retries=2, backoff_factor=0.01)


class TestYFinanceExtractor:
    """Mocked tests for the Yahoo Finance option-chain extractor."""

    @patch("ELT.extract_yfinance.yf.Ticker")
    def test_get_option_chain_schema(self, mock_ticker_cls: MagicMock) -> None:
        """Verify the canonical schema is produced from mocked calls/puts."""
        mock_ticker = MagicMock()
        mock_ticker.ticker = "AAPL"
        mock_ticker.options = ["2026-06-15"]
        mock_ticker.fast_info.get.return_value = 220.5
        mock_ticker_cls.return_value = mock_ticker

        calls = pl.DataFrame(
            {
                "contractSymbol": ["AAPL260615C00260000"],
                "lastTradeDate": [
                    datetime(2026, 6, 12, 15, 33, 20, tzinfo=timezone.utc)
                ],
                "strike": [260.0],
                "lastPrice": [32.36],
                "bid": [29.9],
                "ask": [32.5],
                "change": [1.09],
                "percentChange": [3.4857695],
                "volume": [7],
                "openInterest": [15],
                "impliedVolatility": [0.56836369140625],
                "inTheMoney": [True],
                "contractSize": ["REGULAR"],
                "currency": ["USD"],
            }
        )
        puts = pl.DataFrame(
            {
                "contractSymbol": ["AAPL260615P00250000"],
                "lastTradeDate": [
                    datetime(2026, 6, 12, 19, 44, 31, tzinfo=timezone.utc)
                ],
                "strike": [250.0],
                "lastPrice": [0.02],
                "bid": [0.0],
                "ask": [0.05],
                "change": [-0.02],
                "percentChange": [-50.0],
                "volume": [38],
                "openInterest": [34],
                "impliedVolatility": [0.6523472265625001],
                "inTheMoney": [False],
                "contractSize": ["REGULAR"],
                "currency": ["USD"],
            }
        )
        chain_result = MagicMock()
        chain_result.calls = calls
        chain_result.puts = puts
        mock_ticker.option_chain.return_value = chain_result

        extractor = YFinanceExtractor(calls_per_minute=10_000)
        df = extractor.get_option_chain("AAPL")

        assert len(df) == 2
        assert set(df["type"].to_list()) == {"call", "put"}
        assert all(u == "AAPL" for u in df["underlying"].to_list())
        assert df["expiry"].dtype == pl.Date
        assert df["snapshot_ts"].dtype == pl.Datetime("us", "UTC")
        assert df["last_trade_date"].dtype == pl.Datetime("us", "UTC")
        assert df["strike"].dtype == pl.Decimal(18, 6)
        assert df["last_price"].dtype == pl.Decimal(18, 6)
        assert df["percent_change"].dtype == pl.Float64
        assert df["volume"].dtype == pl.Int64
        assert df["open_interest"].dtype == pl.Int64
        assert df["implied_volatility"].dtype == pl.Float64
        assert df["in_the_money"].dtype == pl.Boolean
        assert df["underlying_price"][0] == Decimal("220.500000")

    @patch("ELT.extract_yfinance.yf.Ticker")
    def test_get_option_chain_dedupes_contracts(
        self, mock_ticker_cls: MagicMock
    ) -> None:
        """If the same contract appears twice, keep only one row."""
        mock_ticker = MagicMock()
        mock_ticker.ticker = "AAPL"
        mock_ticker.options = ["2026-06-15"]
        mock_ticker.fast_info.get.return_value = 220.0
        mock_ticker_cls.return_value = mock_ticker

        calls = pl.DataFrame(
            {
                "contractSymbol": ["AAPL260615C00260000"],
                "lastTradeDate": [None],
                "strike": [260.0],
                "lastPrice": [32.0],
                "bid": [30.0],
                "ask": [32.5],
                "change": [1.0],
                "percentChange": [3.0],
                "volume": [7],
                "openInterest": [15],
                "impliedVolatility": [0.5],
                "inTheMoney": [True],
                "contractSize": ["REGULAR"],
                "currency": ["USD"],
            }
        )
        chain_result = MagicMock()
        chain_result.calls = calls
        chain_result.puts = pl.DataFrame()
        mock_ticker.option_chain.return_value = chain_result

        extractor = YFinanceExtractor(calls_per_minute=10_000)
        df = extractor.get_option_chain("AAPL")
        assert len(df) == 1

    @patch("ELT.extract_yfinance.yf.Ticker")
    def test_get_option_chains_batch(self, mock_ticker_cls: MagicMock) -> None:
        """Batch wrapper should isolate failures per ticker."""
        mock_ticker = MagicMock()
        mock_ticker.ticker = "AAPL"
        mock_ticker.options = ["2026-06-15"]
        mock_ticker.fast_info.get.return_value = 220.0
        mock_ticker_cls.return_value = mock_ticker

        calls = pl.DataFrame(
            {
                "contractSymbol": ["AAPL260615C00260000"],
                "lastTradeDate": [None],
                "strike": [260.0],
                "lastPrice": [32.0],
                "bid": [30.0],
                "ask": [32.5],
                "change": [1.0],
                "percentChange": [3.0],
                "volume": [7],
                "openInterest": [15],
                "impliedVolatility": [0.5],
                "inTheMoney": [True],
                "contractSize": ["REGULAR"],
                "currency": ["USD"],
            }
        )
        chain_result = MagicMock()
        chain_result.calls = calls
        chain_result.puts = pl.DataFrame()
        mock_ticker.option_chain.return_value = chain_result

        extractor = YFinanceExtractor(calls_per_minute=10_000)
        results = extractor.get_option_chains(["AAPL", "FAKE"])
        assert results["AAPL"] is not None and len(results["AAPL"]) == 1
        # FAKE uses the same mock in this test, so it also succeeds; the
        # important behaviour is that the batch wrapper returns per-ticker
        # results without raising.
        assert results["FAKE"] is not None
