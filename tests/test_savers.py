"""Integration tests for savers."""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import polars as pl

from ELT.base import ParquetSaver
from ELT.save_yfinance import YFinanceSaver


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


class TestYFinanceSaver:
    """End-to-end tests for the Yahoo Finance option-chain saver."""

    def _sample_option_df(self, snapshot_ts: datetime) -> pl.DataFrame:
        return pl.DataFrame(
            {
                "underlying": ["AAPL", "AAPL"],
                "type": ["call", "put"],
                "expiry": [date(2026, 6, 15), date(2026, 6, 15)],
                "contract_symbol": ["AAPL260615C00260000", "AAPL260615P00250000"],
                "strike": [Decimal("260.000000"), Decimal("250.000000")],
                "currency": ["USD", "USD"],
                "last_price": [Decimal("32.360000"), Decimal("0.020000")],
                "bid": [Decimal("29.900000"), Decimal("0.000000")],
                "ask": [Decimal("32.500000"), Decimal("0.050000")],
                "change": [Decimal("1.090000"), Decimal("-0.020000")],
                "percent_change": [3.4857695, -50.0],
                "volume": [7, 38],
                "open_interest": [15, 34],
                "implied_volatility": [0.56836369140625, 0.6523472265625001],
                "in_the_money": [True, False],
                "contract_size": ["REGULAR", "REGULAR"],
                "last_trade_date": [datetime(2026, 6, 12, 15, 33, 20, tzinfo=timezone.utc), datetime(2026, 6, 12, 19, 44, 31, tzinfo=timezone.utc)],
                "underlying_price": [Decimal("220.500000"), Decimal("220.500000")],
                "snapshot_ts": [snapshot_ts, snapshot_ts],
                "snapshot_date": [snapshot_ts.date(), snapshot_ts.date()],
                "source": ["YFINANCE", "YFINANCE"],
                "last_fetched_at": [snapshot_ts, snapshot_ts],
            },
            schema={
                "underlying": pl.Utf8,
                "type": pl.Utf8,
                "expiry": pl.Date,
                "contract_symbol": pl.Utf8,
                "strike": pl.Decimal(18, 6),
                "currency": pl.Utf8,
                "last_price": pl.Decimal(18, 6),
                "bid": pl.Decimal(18, 6),
                "ask": pl.Decimal(18, 6),
                "change": pl.Decimal(18, 6),
                "percent_change": pl.Float64,
                "volume": pl.Int64,
                "open_interest": pl.Int64,
                "implied_volatility": pl.Float64,
                "in_the_money": pl.Boolean,
                "contract_size": pl.Utf8,
                "last_trade_date": pl.Datetime("us", "UTC"),
                "underlying_price": pl.Decimal(18, 6),
                "snapshot_ts": pl.Datetime("us", "UTC"),
                "snapshot_date": pl.Date,
                "source": pl.Utf8,
                "last_fetched_at": pl.Datetime("us", "UTC"),
            },
        )

    def test_save_option_chains_round_trip(self, tmp_path: Path) -> None:
        saver = YFinanceSaver(data_dir=tmp_path, calls_per_minute=10_000)
        snapshot_ts = datetime(2026, 6, 13, 12, 0, 0, tzinfo=timezone.utc)
        saver.extractor.get_option_chain = lambda ticker, expiries=None: self._sample_option_df(snapshot_ts)

        result = saver.save_option_chains(["AAPL"])

        assert result.ok
        assert "AAPL" in result.saved
        path = tmp_path / "options" / "AAPL.parquet"
        assert path.exists()

        df = pl.read_parquet(path)
        assert len(df) == 2
        assert df["strike"].dtype == pl.Decimal(18, 6)
        assert df["snapshot_ts"].dtype == pl.Datetime("us", "UTC")

    def test_save_option_chains_idempotent(self, tmp_path: Path) -> None:
        saver = YFinanceSaver(data_dir=tmp_path, calls_per_minute=10_000)
        snapshot_ts = datetime(2026, 6, 13, 12, 0, 0, tzinfo=timezone.utc)
        saver.extractor.get_option_chain = lambda ticker, expiries=None: self._sample_option_df(snapshot_ts)

        saver.save_option_chains(["AAPL"])
        result = saver.save_option_chains(["AAPL"])

        assert result.ok
        df = saver.read_option_chains("AAPL")
        assert df is not None
        assert len(df) == 2

    def test_read_all_option_chains(self, tmp_path: Path) -> None:
        saver = YFinanceSaver(data_dir=tmp_path, calls_per_minute=10_000)
        snapshot_ts = datetime(2026, 6, 13, 12, 0, 0, tzinfo=timezone.utc)
        saver.extractor.get_option_chain = lambda ticker, expiries=None: self._sample_option_df(snapshot_ts)

        saver.save_option_chains(["AAPL", "MSFT"])
        combined = saver.read_all_option_chains()

        assert combined is not None
        assert len(combined) == 4
