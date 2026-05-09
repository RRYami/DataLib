from __future__ import annotations

import os

import polars as pl

from ELT.base import ParquetSaver
from ELT.extract_ecb import EcbExtractor
from logger.logger import get_logger

logger = get_logger(__name__)


class EcbSaver(ParquetSaver):
    """
    Persist ECB data to Parquet files with idempotent, incremental updates.

    Directory layout
    ----------------
    ::

        data/ecb/
        ├── interest_rates.parquet
        ├── hicp.parquet
        └── monetary_aggregates.parquet
    """

    def __init__(
        self,
        data_dir: str | os.PathLike = "data/parquet/ecb",
    ):
        super().__init__(data_dir)
        self.extractor = EcbExtractor()

    def _save_series_group(
        self,
        filename: str,
        fetch_fn,
        dedupe_keys: list[str],
        lookback_days: int = 7,
    ) -> None:
        """Generic save helper for a group of related series."""
        path = self.data_dir / filename
        existing = self._read_existing(path)
        start_date = self._determine_start_date(existing, lookback_days)

        logger.info(
            f"Fetching {filename.replace('.parquet', '')} "
            f"(start={start_date or 'full history'}, lookback={lookback_days}d)"
        )

        new_df = fetch_fn(start_date)

        if new_df is None or new_df.is_empty():
            logger.warning(f"No data fetched for {filename}")
            return

        if existing is not None:
            from ELT.base import merge_and_dedupe
            merged = merge_and_dedupe(existing, new_df, dedupe_keys)
        else:
            merged = new_df

        merged = merged.sort(dedupe_keys)
        self._write_parquet(merged, path)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def save_interest_rates(self, lookback_days: int = 7) -> None:
        """Save / update ECB key interest rates."""
        self._save_series_group(
            "interest_rates.parquet",
            lambda start: self.extractor.get_interest_rates(start=start),
            ["date", "series_label"],
            lookback_days,
        )

    def save_hicp(self, lookback_days: int = 7) -> None:
        """Save / update HICP inflation data."""
        self._save_series_group(
            "hicp.parquet",
            lambda start: self.extractor.get_hicp(start=start),
            ["date", "series_label"],
            lookback_days,
        )

    def save_monetary_aggregates(self, lookback_days: int = 7) -> None:
        """Save / update monetary aggregates (M1, M3)."""
        self._save_series_group(
            "monetary_aggregates.parquet",
            lambda start: self.extractor.get_monetary_aggregates(start=start),
            ["date", "series_label"],
            lookback_days,
        )

    def save_all(self, lookback_days: int = 7) -> None:
        """Run all three saves."""
        self.save_interest_rates(lookback_days)
        self.save_hicp(lookback_days)
        self.save_monetary_aggregates(lookback_days)

    # ------------------------------------------------------------------
    # Read helpers
    # ------------------------------------------------------------------

    def read_interest_rates(self) -> pl.DataFrame | None:
        path = self.data_dir / "interest_rates.parquet"
        return self._read_existing(path)

    def read_hicp(self) -> pl.DataFrame | None:
        path = self.data_dir / "hicp.parquet"
        return self._read_existing(path)

    def read_monetary_aggregates(self) -> pl.DataFrame | None:
        path = self.data_dir / "monetary_aggregates.parquet"
        return self._read_existing(path)
