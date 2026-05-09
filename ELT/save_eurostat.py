from __future__ import annotations

import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import polars as pl

from ELT.extract_eurostat import EurostatExtractor
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


class EurostatSaver:
    """
    Persist Eurostat data to Parquet files with idempotent, incremental updates.
    """

    def __init__(
        self,
        data_dir: str | os.PathLike = "data/parquet/eurostat",
    ):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.extractor = EurostatExtractor()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _sub_dir(self, name: str) -> Path:
        sub = self.data_dir / name
        sub.mkdir(parents=True, exist_ok=True)
        return sub

    def _read_existing(self, path: Path) -> pl.DataFrame | None:
        if not path.exists():
            return None
        try:
            return pl.read_parquet(path)
        except Exception as exc:
            logger.error(f"Failed to read existing {path}: {exc}")
            return None

    def _write_parquet(self, df: pl.DataFrame, path: Path) -> None:
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
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
        if existing is None or existing.is_empty():
            return None
        max_date = existing["date"].max()
        assert isinstance(max_date, date)
        lookback = max_date - timedelta(days=lookback_days)
        return lookback.strftime("%Y-%m")

    def _save_aggregate(
        self,
        filename: str,
        fetch_fn,
        dedupe_keys: list[str],
        lookback_days: int = 7,
    ) -> None:
        """Save a dataset that goes into a single file."""
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
            merged = _merge_and_dedupe(existing, new_df, dedupe_keys)
        else:
            merged = new_df

        merged = merged.sort(dedupe_keys)
        self._write_parquet(merged, path)

    def _save_per_country(
        self,
        countries: list[str],
        fetch_fn,
        sub_dir_name: str,
        dedupe_keys: list[str],
        lookback_days: int = 7,
    ) -> None:
        """Save a dataset where each country gets its own file."""
        for country in countries:
            path = self._sub_dir(sub_dir_name) / f"{country.upper()}.parquet"
            existing = self._read_existing(path)
            start_date = self._determine_start_date(existing, lookback_days)

            try:
                new_df = fetch_fn(country, start_date)
            except Exception as exc:
                logger.error(f"Failed to fetch {sub_dir_name} for {country}: {exc}")
                continue

            if new_df is None or new_df.is_empty():
                logger.warning(f"No data for {country} {sub_dir_name}")
                continue

            if existing is not None:
                merged = _merge_and_dedupe(existing, new_df, dedupe_keys)
            else:
                merged = new_df

            merged = merged.sort(dedupe_keys)
            self._write_parquet(merged, path)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def save_euro_area_yield_curve(
        self,
        curve_type: str = "PAR",
        lookback_days: int = 7,
    ) -> None:
        """Save / update the Euro area yield curve."""
        self._save_aggregate(
            "euro_area_yield_curve.parquet",
            lambda start: self.extractor.get_euro_area_yield_curve(
                curve_type=curve_type
            ),
            ["date", "maturity", "curve_type"],
            lookback_days,
        )

    def save_gov_bond_yields(
        self,
        countries: list[str] | None = None,
        lookback_days: int = 7,
    ) -> None:
        """Save / update 10Y government bond yields — one file per country."""
        countries = countries or [
            "BE",
            "DE",
            "IE",
            "ES",
            "FR",
            "IT",
            "NL",
            "AT",
            "PT",
            "FI",
        ]
        self._save_per_country(
            countries,
            lambda country, start: self.extractor.get_gov_bond_yields(
                countries=[country]
            ),
            "gov_bond_yields",
            ["date"],
            lookback_days,
        )

    def save_hicp(self, lookback_days: int = 7) -> None:
        """Save / update HICP by country."""
        self._save_aggregate(
            "hicp.parquet",
            lambda start: self.extractor.get_hicp(),
            ["date", "country"],
            lookback_days,
        )

    def save_gdp(self, lookback_days: int = 7) -> None:
        """Save / update quarterly GDP by country."""
        self._save_aggregate(
            "gdp.parquet",
            lambda start: self.extractor.get_gdp(),
            ["date", "country"],
            lookback_days,
        )

    def save_all(self, lookback_days: int = 7) -> None:
        """Run all saves."""
        self.save_euro_area_yield_curve(lookback_days=lookback_days)
        self.save_gov_bond_yields(lookback_days=lookback_days)
        self.save_hicp(lookback_days=lookback_days)
        self.save_gdp(lookback_days=lookback_days)

    # ------------------------------------------------------------------
    # Read helpers
    # ------------------------------------------------------------------

    def read_euro_area_yield_curve(self) -> pl.DataFrame | None:
        path = self.data_dir / "euro_area_yield_curve.parquet"
        return self._read_existing(path)

    def read_gov_bond_yields(self, country: str) -> pl.DataFrame | None:
        path = self._sub_dir("gov_bond_yields") / f"{country.upper()}.parquet"
        return self._read_existing(path)

    def read_hicp(self) -> pl.DataFrame | None:
        path = self.data_dir / "hicp.parquet"
        return self._read_existing(path)

    def read_gdp(self) -> pl.DataFrame | None:
        path = self.data_dir / "gdp.parquet"
        return self._read_existing(path)

    def read_all_gov_bond_yields(self) -> pl.DataFrame | None:
        """Read and concatenate all country bond-yield files."""
        sub = self._sub_dir("gov_bond_yields")
        files = sorted(sub.glob("*.parquet"))
        if not files:
            return None
        dfs = [pl.read_parquet(f) for f in files]
        return pl.concat(dfs, how="diagonal_relaxed")
