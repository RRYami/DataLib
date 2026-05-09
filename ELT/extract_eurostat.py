from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any

import eurostat
import polars as pl

from logger.logger import get_logger
from utils.http import retry_call

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Dataset codes
# ---------------------------------------------------------------------------
_EUROSTAT_DATASETS = {
    "euro_area_yield_curve": "IRT_EURYLD_M",
    "gov_bond_yields": "IRT_H_CGBY_M",
    "hicp": "PRC_HICP_MIDX",
    "gdp": "NAMQ_10_GDP",
}

# Euro area yield curve maturities
_EA_YIELD_MATURITIES = ["Y1", "Y2", "Y3", "Y5", "Y7", "Y10", "Y15", "Y20", "Y30"]

# EU countries available in IRT_H_CGBY_M
_EU_COUNTRIES = ["BE", "DE", "IE", "ES", "FR", "IT", "NL", "AT", "PT", "FI"]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _RateLimiter:
    """Enforce a minimum interval between API calls."""

    def __init__(self, calls_per_minute: int = 60) -> None:
        self.min_interval = 60.0 / calls_per_minute
        self.last_call: float | None = None

    def sleep_if_needed(self) -> None:
        if self.last_call is not None:
            elapsed = time.monotonic() - self.last_call
            if elapsed < self.min_interval:
                time.sleep(self.min_interval - elapsed)
        self.last_call = time.monotonic()


def _unpivot_eurostat_df(
    raw: Any,
    dataset_code: str,
    indicator_label: str,
    extra_meta: dict[str, str] | None = None,
) -> pl.DataFrame:
    """
    Convert a wide Eurostat pandas DataFrame into a tidy long Polars DataFrame.

    Eurostat returns years/periods as columns (e.g. ``1973-01``, ``2024-01``).
    We melt these into ``date`` / ``value`` columns and attach metadata.
    """
    import pandas as pd

    if raw is None or (isinstance(raw, pd.DataFrame) and raw.empty):
        return pl.DataFrame()

    if not isinstance(raw, pd.DataFrame):
        logger.warning(f"Unexpected type from eurostat: {type(raw)}")
        return pl.DataFrame()

    batch_ts = datetime.now(timezone.utc)

    # Identify the time-period columns (they look like "YYYY-MM" or "YYYY")
    id_cols = [c for c in raw.columns if not _looks_like_period(str(c))]
    period_cols = [c for c in raw.columns if _looks_like_period(str(c))]

    if not period_cols:
        logger.warning(
            f"No time-period columns found for {dataset_code}. "
            f"Columns: {list(raw.columns)}"
        )
        return pl.DataFrame()

    # Melt to long format
    long_pd = raw.melt(
        id_vars=id_cols,
        value_vars=period_cols,
        var_name="date",
        value_name="value",
    )

    df = pl.from_pandas(long_pd)

    # Parse date — try monthly first, then quarterly, then annual
    df = df.with_columns(pl.col("date").cast(pl.String))
    date_sample = df["date"].head(1).to_list()
    sample = str(date_sample[0]) if date_sample else ""

    if len(sample) == 7 and sample[4] == "-":  # YYYY-MM
        df = df.with_columns(pl.col("date").str.to_date("%Y-%m", strict=False))
    elif len(sample) == 6 and sample[4] == "Q":  # YYYY-QN
        df = df.with_columns(
            pl.col("date")
            .str.extract_groups(r"(\d{4})-Q(\d)")
            .struct.rename_fields(["year", "quarter"])
        )
        # Keep as string for quarterly, or map to month-start
        df = df.with_columns(
            pl.when(pl.col("date").str.contains(r"Q1"))
            .then(pl.col("date").str.slice(0, 4) + "-01-01")
            .when(pl.col("date").str.contains(r"Q2"))
            .then(pl.col("date").str.slice(0, 4) + "-04-01")
            .when(pl.col("date").str.contains(r"Q3"))
            .then(pl.col("date").str.slice(0, 4) + "-07-01")
            .when(pl.col("date").str.contains(r"Q4"))
            .then(pl.col("date").str.slice(0, 4) + "-10-01")
            .otherwise(None)
            .str.to_date("%Y-%m-%d", strict=False)
            .alias("date")
        )
    elif len(sample) == 4:  # YYYY
        df = df.with_columns(pl.col("date").str.to_date("%Y", strict=False))
    else:
        df = df.with_columns(pl.col("date").str.to_date("%Y-%m-%d", strict=False))

    # Drop null dates and values
    df = df.filter(pl.col("date").is_not_null(), pl.col("value").is_not_null())

    # Cast value
    df = df.with_columns(pl.col("value").cast(pl.Float64, strict=False))

    # Attach metadata
    meta = {"indicator": indicator_label, "source": "EUROSTAT", "last_fetched_at": batch_ts}
    if extra_meta:
        meta.update(extra_meta)

    for k, v in meta.items():
        df = df.with_columns(pl.lit(v).alias(k))

    return df.sort("date")


def _looks_like_period(s: str) -> bool:
    """Heuristic to detect Eurostat time-period column names."""
    if len(s) == 7 and s[4] == "-":
        return s[:4].isdigit() and s[5:7].isdigit()  # YYYY-MM
    if len(s) == 6 and s[4] == "Q" and s[5].isdigit():
        return s[:4].isdigit()  # YYYY-QN
    if len(s) == 4 and s.isdigit():
        return True  # YYYY
    return False


# ---------------------------------------------------------------------------
# Extractor
# ---------------------------------------------------------------------------


class EurostatExtractor:
    """
    Extract data from Eurostat into tidy Polars DataFrames.

    Wraps the ``eurostat`` package, converting wide-format output
    (time periods as columns) into long format.
    """

    def __init__(self, calls_per_minute: int = 60) -> None:
        self.rate_limiter = _RateLimiter(calls_per_minute)

    # -- low-level ---------------------------------------------------------

    def get_dataset(
        self,
        dataset_code: str,
        indicator_label: str,
        filters: dict[str, Any] | None = None,
        extra_meta: dict[str, str] | None = None,
    ) -> pl.DataFrame:
        """
        Fetch a Eurostat dataset and convert to tidy long format.

        Parameters
        ----------
        dataset_code
            Eurostat dataset code, e.g. ``"IRT_EURYLD_M"``.
        indicator_label
            Human-readable label stored in the ``indicator`` column.
        filters
            Dimension filters passed to ``eurostat.get_data_df()``.
        extra_meta
            Additional constant columns to attach.

        Returns
        -------
        polars.DataFrame
            Long-format frame with ``date, value`` plus metadata.
        """
        self.rate_limiter.sleep_if_needed()
        logger.info(f"Fetching Eurostat dataset: {dataset_code}")

        try:
            raw = retry_call(
                eurostat.get_data_df,
                dataset_code,
                retries=3,
                backoff_factor=1.0,
                filter_pars=filters or {},
            )
        except Exception as exc:
            logger.error(f"Failed to fetch {dataset_code}: {exc}")
            raise

        df = _unpivot_eurostat_df(raw, dataset_code, indicator_label, extra_meta)
        logger.info(f"Fetched {len(df)} observations for {dataset_code}")
        return df

    # -- euro area yield curve ---------------------------------------------

    def get_euro_area_yield_curve(
        self,
        curve_type: str = "PAR",
        bond_type: str = "CGB_EA",
        maturities: list[str] | None = None,
    ) -> pl.DataFrame:
        """
        Fetch the Euro area yield curve.

        Parameters
        ----------
        curve_type
            ``"PAR"`` (par yield), ``"SPOT_RT"`` (spot rate), or
            ``"INS_FWD"`` (instantaneous forward).
        bond_type
            ``"CGB_EA"`` (all euro area) or ``"CGB_EA_AAA"`` (AAA-rated).
        maturities
            Subset of maturities, e.g. ``["Y1", "Y5", "Y10"]``.
            ``None`` fetches the standard tenors.
        """
        maturities = maturities or ["Y1", "Y2", "Y3", "Y5", "Y10", "Y20", "Y30"]
        dataset = _EUROSTAT_DATASETS["euro_area_yield_curve"]

        dfs: list[pl.DataFrame] = []
        for maturity in maturities:
            try:
                df = self.get_dataset(
                    dataset,
                    indicator_label=f"euro_area_yield_curve_{curve_type.lower()}",
                    filters={
                        "geo": "EA",
                        "yld_curv": curve_type,
                        "bonds": bond_type,
                        "maturity": maturity,
                    },
                    extra_meta={"maturity": maturity, "curve_type": curve_type, "bond_type": bond_type},
                )
                if not df.is_empty():
                    dfs.append(df)
            except Exception as exc:
                logger.error(f"Failed to fetch {maturity}: {exc}")

        if not dfs:
            return pl.DataFrame()
        return pl.concat(dfs, how="diagonal_relaxed").sort("date")

    # -- government bond yields (per-country) ------------------------------

    def get_gov_bond_yields(
        self,
        countries: list[str] | None = None,
    ) -> pl.DataFrame:
        """
        Fetch 10-year central government bond yields by country.

        Parameters
        ----------
        countries
            List of 2-letter country codes.
            ``None`` fetches all available EU countries.
        """
        countries = countries or _EU_COUNTRIES
        dataset = _EUROSTAT_DATASETS["gov_bond_yields"]

        dfs: list[pl.DataFrame] = []
        for country in countries:
            try:
                df = self.get_dataset(
                    dataset,
                    indicator_label="gov_bond_yield_10y",
                    filters={"geo": country, "int_rt": "GBY_LT"},
                    extra_meta={"country": country, "maturity": "Y10"},
                )
                if not df.is_empty():
                    dfs.append(df)
            except Exception as exc:
                logger.error(f"Failed to fetch {country}: {exc}")

        if not dfs:
            return pl.DataFrame()
        return pl.concat(dfs, how="diagonal_relaxed").sort("date")

    # -- HICP --------------------------------------------------------------

    def get_hicp(
        self,
        countries: list[str] | None = None,
    ) -> pl.DataFrame:
        """
        Fetch HICP (all items, index) by country.

        Parameters
        ----------
        countries
            List of 2-letter country codes.
            ``None`` fetches EA + major EU economies.
        """
        countries = countries or ["EA", "DE", "FR", "IT", "ES", "NL"]
        dataset = _EUROSTAT_DATASETS["hicp"]

        dfs: list[pl.DataFrame] = []
        for country in countries:
            try:
                df = self.get_dataset(
                    dataset,
                    indicator_label="hicp_all_items",
                    filters={"geo": country, "coicop": "CP00"},
                    extra_meta={"country": country, "item": "all_items"},
                )
                if not df.is_empty():
                    dfs.append(df)
            except Exception as exc:
                logger.error(f"Failed to fetch HICP for {country}: {exc}")

        if not dfs:
            return pl.DataFrame()
        return pl.concat(dfs, how="diagonal_relaxed").sort("date")

    # -- GDP ---------------------------------------------------------------

    def get_gdp(
        self,
        countries: list[str] | None = None,
    ) -> pl.DataFrame:
        """
        Fetch quarterly GDP (volume, seasonally adjusted) by country.

        Parameters
        ----------
        countries
            List of 2-letter country codes.
            ``None`` fetches EA + major EU economies.
        """
        countries = countries or ["EA", "DE", "FR", "IT", "ES", "NL"]
        dataset = _EUROSTAT_DATASETS["gdp"]

        dfs: list[pl.DataFrame] = []
        for country in countries:
            try:
                df = self.get_dataset(
                    dataset,
                    indicator_label="gdp_volume_sa",
                    filters={
                        "geo": country,
                        "na_item": "B1GQ",
                        "unit": "CLV10_MNAC",
                        "s_adj": "SCA",
                    },
                    extra_meta={"country": country},
                )
                if not df.is_empty():
                    dfs.append(df)
            except Exception as exc:
                logger.error(f"Failed to fetch GDP for {country}: {exc}")

        if not dfs:
            return pl.DataFrame()
        return pl.concat(dfs, how="diagonal_relaxed").sort("date")
