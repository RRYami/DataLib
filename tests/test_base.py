"""Unit tests for shared base logic."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from ELT.base import ParquetSaver, merge_and_dedupe
from utils.results import SaveResult
from utils.validators import DataValidationError, run_validation_suite


class TestMergeAndDedupe:
    """Tests for the merge_and_dedupe helper."""

    def test_merge_no_overlap(self) -> None:
        existing = pl.DataFrame(
            {
                "date": ["2024-01-01", "2024-01-02"],
                "value": [1.0, 2.0],
                "last_fetched_at": ["2024-01-01T00:00:00", "2024-01-01T00:00:00"],
            }
        ).with_columns(
            pl.col("date").str.to_date(),
            pl.col("last_fetched_at").str.to_datetime(),
        )
        new = pl.DataFrame(
            {
                "date": ["2024-01-03", "2024-01-04"],
                "value": [3.0, 4.0],
                "last_fetched_at": ["2024-01-02T00:00:00", "2024-01-02T00:00:00"],
            }
        ).with_columns(
            pl.col("date").str.to_date(),
            pl.col("last_fetched_at").str.to_datetime(),
        )
        merged = merge_and_dedupe(existing, new, ["date"])
        assert len(merged) == 4
        assert merged["value"].to_list() == [1.0, 2.0, 3.0, 4.0]

    def test_dedupe_keeps_latest(self) -> None:
        existing = pl.DataFrame(
            {
                "date": ["2024-01-01"],
                "value": [1.0],
                "last_fetched_at": ["2024-01-01T00:00:00"],
            }
        ).with_columns(
            pl.col("date").str.to_date(),
            pl.col("last_fetched_at").str.to_datetime(),
        )
        new = pl.DataFrame(
            {
                "date": ["2024-01-01"],
                "value": [2.0],
                "last_fetched_at": ["2024-01-02T00:00:00"],
            }
        ).with_columns(
            pl.col("date").str.to_date(),
            pl.col("last_fetched_at").str.to_datetime(),
        )
        merged = merge_and_dedupe(existing, new, ["date"])
        assert len(merged) == 1
        assert merged["value"].item() == 2.0

    def test_empty_existing(self) -> None:
        existing = pl.DataFrame(
            schema={
                "date": pl.Date,
                "value": pl.Float64,
                "last_fetched_at": pl.Datetime,
            }
        )
        new = pl.DataFrame(
            {
                "date": ["2024-01-01"],
                "value": [1.0],
                "last_fetched_at": ["2024-01-01T00:00:00"],
            }
        ).with_columns(
            pl.col("date").str.to_date(),
            pl.col("last_fetched_at").str.to_datetime(),
        )
        merged = merge_and_dedupe(existing, new, ["date"])
        assert len(merged) == 1


class TestParquetSaverInternals:
    """Tests for ParquetSaver helper methods."""

    def test_determine_start_date_no_existing(self, tmp_data_dir: Path) -> None:
        saver = ParquetSaver(data_dir=tmp_data_dir)
        assert saver._determine_start_date(None, lookback_days=7) is None

    def test_determine_start_date_with_existing(self, tmp_data_dir: Path) -> None:
        saver = ParquetSaver(data_dir=tmp_data_dir)
        existing = pl.DataFrame(
            {
                "date": ["2024-01-10", "2024-01-15"],
                "value": [1.0, 2.0],
            }
        ).with_columns(pl.col("date").str.to_date())
        start = saver._determine_start_date(existing, lookback_days=7)
        assert start == "2024-01-08"

    def test_atomic_write(self, tmp_data_dir: Path, sample_df: pl.DataFrame) -> None:
        saver = ParquetSaver(data_dir=tmp_data_dir)
        path = tmp_data_dir / "test_atomic.parquet"
        saver._write_parquet(sample_df, path)
        assert path.exists()
        read_back = pl.read_parquet(path)
        assert read_back.shape == sample_df.shape

    def test_read_existing_missing(self, tmp_data_dir: Path) -> None:
        saver = ParquetSaver(data_dir=tmp_data_dir)
        assert saver._read_existing(tmp_data_dir / "nope.parquet") is None

    def test_sub_dir_creation(self, tmp_data_dir: Path) -> None:
        saver = ParquetSaver(data_dir=tmp_data_dir)
        sub = saver._sub_dir("foo")
        assert sub.exists()
        assert sub.is_dir()


class TestValidators:
    """Tests for the validation suite."""

    def test_not_empty_pass(self, sample_df: pl.DataFrame) -> None:
        run_validation_suite(sample_df, context="test")

    def test_not_empty_fail(self, empty_df: pl.DataFrame) -> None:
        with pytest.raises(DataValidationError, match="is empty"):
            run_validation_suite(empty_df, context="test")

    def test_missing_column(self, sample_df: pl.DataFrame) -> None:
        with pytest.raises(DataValidationError, match="missing required columns"):
            run_validation_suite(
                sample_df, required_columns=["date", "missing_col"], context="test"
            )

    def test_all_null_column(self) -> None:
        df = pl.DataFrame(
            {
                "date": ["2024-01-01", "2024-01-02"],
                "value": [None, None],
            }
        ).with_columns(pl.col("date").str.to_date())
        with pytest.raises(DataValidationError, match="all-null columns"):
            run_validation_suite(df, not_null_columns=["value"], context="test")

    def test_row_count_bounds(self, sample_df: pl.DataFrame) -> None:
        with pytest.raises(DataValidationError, match="too many rows"):
            run_validation_suite(
                sample_df, max_rows=2, context="test"
            )

    def test_date_range(self, sample_df: pl.DataFrame) -> None:
        with pytest.raises(DataValidationError, match="date range"):
            run_validation_suite(
                sample_df,
                min_date="2024-01-05",
                context="test",
            )


class TestSaveResult:
    """Tests for the SaveResult dataclass."""

    def test_defaults(self) -> None:
        r = SaveResult()
        assert r.ok is True
        assert r.total == 0

    def test_add_saved(self) -> None:
        r = SaveResult()
        r.add_saved("AAPL")
        assert r.saved == ["AAPL"]
        assert r.ok is True

    def test_add_failed(self) -> None:
        r = SaveResult()
        r.add_failed("BAD", "error")
        assert r.ok is False
        assert r.failed == [("BAD", "error")]

    def test_to_dict(self) -> None:
        r = SaveResult()
        r.add_saved("AAPL")
        r.add_failed("BAD", "err")
        d = r.to_dict()
        assert d["ok"] is False
        assert d["total"] == 2
        assert d["saved"] == ["AAPL"]
        assert d["failed"][0]["item"] == "BAD"
