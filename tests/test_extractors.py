"""Mocked API tests for extractors."""

from __future__ import annotations

import pytest
import responses
from polars import DataFrame

from ELT.extract_fred import BASE_URL as FRED_BASE_URL, FredExtractor
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
