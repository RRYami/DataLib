from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import TypedDict

import polars as pl

from ELT.base import ParquetSaver, merge_and_dedupe
from ELT.extract_fred import (
    GSW_INSTANTANEOUS_FORWARDS,
    GSW_TERM_PREMIUMS_FORWARD,
    GSW_TERM_PREMIUMS_SPOT,
    GSW_ZERO_COUPON_YIELDS,
    TREASURY_CONSTANT_MATURITY,
    FredExtractor,
)
from logger.logger import get_logger

logger = get_logger(__name__)


class _ComponentConfig(TypedDict):
    mapping: dict[str, str]
    component_label: str


class _TermPremiumConfig(TypedDict):
    spot_mapping: dict[str, str]
    fwd_mapping: dict[str, str]


_COMPONENT_CONFIG: dict[str, _ComponentConfig] = {
    "treasury_constant_maturity": {
        "mapping": TREASURY_CONSTANT_MATURITY,
        "component_label": "constant_maturity",
    },
    "gsw_zero_coupon": {
        "mapping": GSW_ZERO_COUPON_YIELDS,
        "component_label": "zero_coupon",
    },
    "gsw_forward_rates": {
        "mapping": GSW_INSTANTANEOUS_FORWARDS,
        "component_label": "forward",
    },
}

_TERM_PREMIUM_CONFIG: dict[str, _TermPremiumConfig] = {
    "gsw_term_premiums": {
        "spot_mapping": GSW_TERM_PREMIUMS_SPOT,
        "fwd_mapping": GSW_TERM_PREMIUMS_FORWARD,
    }
}


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _wide_to_long(
    wide: pl.DataFrame,
    component_label: str,
    series_id_map: dict[str, str],
    batch_ts: datetime,
) -> pl.DataFrame:
    """
    Convert a wide DataFrame to long format and attach metadata columns.
    """
    if wide.is_empty() or len(wide.columns) <= 1:
        return pl.DataFrame(
            schema={
                "date": pl.Date,
                "tenor": pl.String,
                "value": pl.Float64,
                "fred_series_id": pl.String,
                "component": pl.String,
                "source": pl.String,
                "last_fetched_at": pl.Datetime(time_unit="us", time_zone="UTC"),
            }
        )

    long = wide.unpivot(
        index=["date"],
        variable_name="tenor",
        value_name="value",
    )

    long = long.with_columns(
        pl.col("tenor").replace_strict(series_id_map).alias("fred_series_id"),
        pl.lit(component_label).alias("component"),
        pl.lit("FRED").alias("source"),
        pl.lit(batch_ts).alias("last_fetched_at"),
    )

    return long


class FredSaver(ParquetSaver):
    """
    Persist FRED yield-curve data to Parquet files with idempotent,
    incremental updates.
    """

    def __init__(
        self,
        data_dir: str | os.PathLike = "data/parquet/yield_curve_usa",
        api_key: str | None = None,
    ):
        super().__init__(data_dir)
        self.extractor = FredExtractor(api_key=api_key)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _save_component(
        self,
        filename: str,
        mapping: dict[str, str],
        component_label: str,
        lookback_days: int,
    ) -> None:
        batch_ts = _now_utc()
        path = self.data_dir / filename
        existing = self._read_existing(path)
        start_date = self._determine_start_date(existing, lookback_days)

        logger.info(
            f"Fetching {component_label} "
            f"(start={start_date or 'full history'}, lookback={lookback_days}d)"
        )

        wide = self.extractor.get_series_observations(
            mapping,
            observation_start=start_date,
        )
        # We passed a dict, so the extractor returns a DataFrame.
        assert isinstance(wide, pl.DataFrame)

        new_long = _wide_to_long(wide, component_label, mapping, batch_ts)

        if existing is not None:
            merged = merge_and_dedupe(
                existing, new_long, ["date", "tenor", "fred_series_id"]
            )
        else:
            merged = new_long

        merged = merged.sort(["date", "tenor", "fred_series_id"])
        self._write_parquet(merged, path)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def save_treasury_constant_maturity(self, lookback_days: int = 7) -> None:
        """Save / update H.15 Treasury constant-maturity yields."""
        cfg = _COMPONENT_CONFIG["treasury_constant_maturity"]
        self._save_component(
            "treasury_constant_maturity.parquet",
            cfg["mapping"],
            cfg["component_label"],
            lookback_days,
        )

    def save_gsw_zero_coupon(self, lookback_days: int = 7) -> None:
        """Save / update GSW fitted zero-coupon yields."""
        cfg = _COMPONENT_CONFIG["gsw_zero_coupon"]
        self._save_component(
            "gsw_zero_coupon.parquet",
            cfg["mapping"],
            cfg["component_label"],
            lookback_days,
        )

    def save_gsw_forward_rates(self, lookback_days: int = 7) -> None:
        """Save / update GSW instantaneous forward rates."""
        cfg = _COMPONENT_CONFIG["gsw_forward_rates"]
        self._save_component(
            "gsw_forward_rates.parquet",
            cfg["mapping"],
            cfg["component_label"],
            lookback_days,
        )

    def save_gsw_term_premiums(self, lookback_days: int = 7) -> None:
        """
        Save / update GSW term premiums (spot + forward) into a single file.
        """
        batch_ts = _now_utc()
        filename = "gsw_term_premiums.parquet"
        path = self.data_dir / filename
        existing = self._read_existing(path)
        start_date = self._determine_start_date(existing, lookback_days)

        logger.info(
            f"Fetching term premiums "
            f"(start={start_date or 'full history'}, lookback={lookback_days}d)"
        )

        # Spot
        spot_wide = self.extractor.get_term_premiums(
            observation_start=start_date, forward=False
        )
        spot_long = _wide_to_long(
            spot_wide,
            "term_premium_spot",
            GSW_TERM_PREMIUMS_SPOT,
            batch_ts,
        )

        # Forward
        fwd_wide = self.extractor.get_term_premiums(
            observation_start=start_date, forward=True
        )
        fwd_long = _wide_to_long(
            fwd_wide,
            "term_premium_forward",
            GSW_TERM_PREMIUMS_FORWARD,
            batch_ts,
        )

        new_long = pl.concat([spot_long, fwd_long], how="diagonal_relaxed")

        if existing is not None:
            merged = merge_and_dedupe(
                existing, new_long, ["date", "tenor", "fred_series_id"]
            )
        else:
            merged = new_long

        merged = merged.sort(["date", "tenor", "fred_series_id"])
        self._write_parquet(merged, path)

    def save_all(self, lookback_days: int = 7) -> None:
        """Run all four save methods. Safe to call daily."""
        self.save_treasury_constant_maturity(lookback_days)
        self.save_gsw_zero_coupon(lookback_days)
        self.save_gsw_forward_rates(lookback_days)
        self.save_gsw_term_premiums(lookback_days)

    # ------------------------------------------------------------------
    # Read helpers (convenience)
    # ------------------------------------------------------------------

    def read_treasury_constant_maturity(
        self,
        wide: bool = True,
    ) -> pl.DataFrame | None:
        """Read the saved constant-maturity data."""
        path = self.data_dir / "treasury_constant_maturity.parquet"
        if not path.exists():
            return None
        df = pl.read_parquet(path)
        if not wide:
            return df
        return df.pivot(index="date", on="tenor", values="value")

    def read_gsw_zero_coupon(self, wide: bool = True) -> pl.DataFrame | None:
        path = self.data_dir / "gsw_zero_coupon.parquet"
        if not path.exists():
            return None
        df = pl.read_parquet(path)
        if not wide:
            return df
        return df.pivot(index="date", on="tenor", values="value")

    def read_gsw_forward_rates(self, wide: bool = True) -> pl.DataFrame | None:
        path = self.data_dir / "gsw_forward_rates.parquet"
        if not path.exists():
            return None
        df = pl.read_parquet(path)
        if not wide:
            return df
        return df.pivot(index="date", on="tenor", values="value")

    def read_gsw_term_premiums(self, wide: bool = True) -> pl.DataFrame | None:
        path = self.data_dir / "gsw_term_premiums.parquet"
        if not path.exists():
            return None
        df = pl.read_parquet(path)
        if not wide:
            return df
        return df.pivot(index="date", on="tenor", values="value")
