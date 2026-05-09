from __future__ import annotations

import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import polars as pl

from ELT.extract_ecb import EcbExtractor
from logger.logger import get_logger, setup_logging

setup_logging()
logger = get_logger(__name__)


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _merge_and_dedupe(
    existing: pl.DataFrame,
    new: pl.DataFrame,
    unique_keys: list[str],
) -> pl.DataFrame:
    """Concatenate and keep the most-recent ``last_fetched_at``."""
    combined = pl.concat([existing, new], how="diagonal_relaxed")
    combined = combined.sort([*unique_keys, "last_fetched_at"])
    return combined.unique(subset=unique_keys, keep="last")


class EcbSaver:
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
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.extractor = EcbExtractor()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _read_existing(self, filename: str) -> pl.DataFrame | None:
        path = self.data_dir / filename
        if not path.exists():
            return None
        try:
            return pl.read_parquet(path)
        except Exception as exc:
            logger.error(f"Failed to read existing {path}: {exc}")
            return None

    def _write_parquet(self, df: pl.DataFrame, filename: str) -> None:
        path = self.data_dir / filename
        try:
            df.write_parquet(path)
            logger.info(f"Wrote {len(df):,} rows to {path}")
        except Exception as exc:
            logger.error(f"Failed to write {path}: {exc}")
            raise

    def _determine_start_date(
        self,
        existing: pl.DataFrame | None,
        lookback_days: int,
    ) -> str | None:
        """Return a start date lookback_days before the latest observation."""
        if existing is None or existing.is_empty():
            return None
        max_date = existing["date"].max()
        assert isinstance(max_date, date)
        lookback = max_date - timedelta(days=lookback_days)
        return lookback.strftime("%Y-%m-%d")

    def _save_series_group(
        self,
        filename: str,
        fetch_fn,
        dedupe_keys: list[str],
        lookback_days: int = 7,
    ) -> None:
        """Generic save helper for a group of related series."""
        existing = self._read_existing(filename)
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
            merged = _merge_and_dedupe(existing, new_df, dedupe_keys)
        else:
            merged = new_df

        merged = merged.sort(dedupe_keys)
        self._write_parquet(merged, filename)

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
        return pl.read_parquet(path) if path.exists() else None

    def read_hicp(self) -> pl.DataFrame | None:
        path = self.data_dir / "hicp.parquet"
        return pl.read_parquet(path) if path.exists() else None

    def read_monetary_aggregates(self) -> pl.DataFrame | None:
        path = self.data_dir / "monetary_aggregates.parquet"
        return pl.read_parquet(path) if path.exists() else None
