"""Shared pytest fixtures."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest


@pytest.fixture
def sample_df() -> pl.DataFrame:
    """A small DataFrame with a date column for testing."""
    return pl.DataFrame(
        {
            "date": ["2024-01-01", "2024-01-02", "2024-01-03"],
            "value": [1.0, 2.0, 3.0],
        }
    ).with_columns(pl.col("date").str.to_date())


@pytest.fixture
def empty_df() -> pl.DataFrame:
    """An empty DataFrame with a date schema."""
    return pl.DataFrame(schema={"date": pl.Date, "value": pl.Float64})


@pytest.fixture
def tmp_data_dir(tmp_path: Path) -> Path:
    """Temporary directory for Parquet I/O tests."""
    d = tmp_path / "data"
    d.mkdir()
    return d
