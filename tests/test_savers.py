"""Integration tests for savers."""

from __future__ import annotations

from pathlib import Path

import polars as pl

from ELT.base import ParquetSaver


class DummyExtractor:
    """A stub extractor that returns canned data."""

    def __init__(self, rows: list[dict] | None = None) -> None:
        self.rows = rows or []

    def fetch(self, start_date: str | None = None) -> pl.DataFrame:
        return pl.DataFrame(self.rows).with_columns(
            pl.col("date").str.to_date()
        )


class TestParquetSaverIntegration:
    """End-to-end tests for the base saver."""

    def test_save_aggregate_full_cycle(self, tmp_path: Path) -> None:
        saver = ParquetSaver(data_dir=tmp_path)
        extractor = DummyExtractor(
            rows=[
                {"date": "2024-01-01", "value": 1.0, "last_fetched_at": "2024-01-01T00:00:00"},
                {"date": "2024-01-02", "value": 2.0, "last_fetched_at": "2024-01-01T00:00:00"},
            ]
        )
        ok = saver._save_aggregate(
            "test.parquet",
            extractor.fetch,
            dedupe_keys=["date"],
            lookback_days=7,
        )
        assert ok is True
        path = tmp_path / "test.parquet"
        assert path.exists()
        df = pl.read_parquet(path)
        assert len(df) == 2

    def test_save_aggregate_idempotent(self, tmp_path: Path) -> None:
        saver = ParquetSaver(data_dir=tmp_path)
        rows = [
            {"date": "2024-01-01", "value": 1.0, "last_fetched_at": "2024-01-01T00:00:00"},
        ]
        ok1 = saver._save_aggregate(
            "test.parquet",
            DummyExtractor(rows).fetch,
            dedupe_keys=["date"],
        )
        assert ok1 is True

        # Re-run with same data — should be a no-op after dedupe
        ok2 = saver._save_aggregate(
            "test.parquet",
            DummyExtractor(rows).fetch,
            dedupe_keys=["date"],
        )
        assert ok2 is True

        df = pl.read_parquet(tmp_path / "test.parquet")
        assert len(df) == 1

    def test_save_single_ticker(self, tmp_path: Path) -> None:
        saver = ParquetSaver(data_dir=tmp_path)
        rows = [
            {"date": "2024-01-01", "close": 100.0, "last_fetched_at": "2024-01-01T00:00:00"},
        ]
        ok = saver._save_single_ticker(
            "AAPL",
            lambda: DummyExtractor(rows).fetch(),
            "daily_bars",
            ["date"],
        )
        assert ok is True
        path = tmp_path / "daily_bars" / "AAPL.parquet"
        assert path.exists()

    def test_save_single_ticker_fetch_failure(self, tmp_path: Path) -> None:
        saver = ParquetSaver(data_dir=tmp_path)

        def bad_fetch() -> pl.DataFrame:
            raise RuntimeError("network down")

        ok = saver._save_single_ticker(
            "AAPL",
            bad_fetch,
            "daily_bars",
            ["date"],
        )
        assert ok is False
        assert not (tmp_path / "daily_bars" / "AAPL.parquet").exists()

    def test_validation_blocks_bad_data(self, tmp_path: Path) -> None:
        """Ensure _validate prevents writing all-null date columns."""
        saver = ParquetSaver(data_dir=tmp_path)
        bad_df = pl.DataFrame(
            {
                "date": [None, None],
                "value": [1.0, 2.0],
            }
        ).with_columns(pl.col("date").cast(pl.Date))

        def bad_fetch() -> pl.DataFrame:
            return bad_df

        ok = saver._save_single_ticker(
            "AAPL",
            bad_fetch,
            "daily_bars",
            ["date"],
        )
        assert ok is False

    def test_read_all_in_subdir(self, tmp_path: Path) -> None:
        saver = ParquetSaver(data_dir=tmp_path)
        for ticker in ["AAPL", "MSFT"]:
            rows = [
                {"date": "2024-01-01", "close": 100.0, "last_fetched_at": "2024-01-01T00:00:00"},
            ]
            saver._save_single_ticker(
                ticker,
                lambda r=rows: DummyExtractor(r).fetch(),
                "daily_bars",
                ["date"],
            )
        combined = saver._read_all_in_subdir("daily_bars")
        assert combined is not None
        assert len(combined) == 2

    def test_read_all_in_subdir_empty(self, tmp_path: Path) -> None:
        saver = ParquetSaver(data_dir=tmp_path)
        assert saver._read_all_in_subdir("empty") is None
