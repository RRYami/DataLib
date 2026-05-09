from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any

import polars as pl
from ecbdata import ecbdata

from logger.logger import get_logger

logger = get_logger(__name__)

# ---------------------------------------------------------------------------
# Known working ECB series codes
# ---------------------------------------------------------------------------
_ECB_INTEREST_RATES = {
    "mro_fixed_rate": "FM.D.U2.EUR.4F.KR.MRR_FR.LEV",
    "mro_min_bid_rate": "FM.D.U2.EUR.4F.KR.MRR_RT.LEV",
    "deposit_facility": "FM.D.U2.EUR.4F.KR.DFR.LEV",
}

_ECB_INFLATION = {
    "hicp_all_items": "ICP.M.U2.Y.000000.3.INX",
    "hicp_energy": "ICP.M.U2.Y.XEF000.4.INX",
    "hicp_food": "ICP.M.U2.Y.XFD000.3.INX",
    "hicp_core": "ICP.M.U2.Y.XFN000.3.INX",
}

_ECB_MONEY = {
    "m3": "BSI.M.U2.Y.V.M30.X.1.U2.2300.Z01.E",
    "m1": "BSI.M.U2.Y.V.M10.X.1.U2.2300.Z01.E",
}


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


def _parse_ecb_df(raw: Any, series_id: str, series_label: str) -> pl.DataFrame:
    """
    Convert an ecbdata pandas DataFrame into a tidy Polars DataFrame.

    The raw frame from ``ecbdata`` contains many SDMX metadata columns.
    We keep only ``TIME_PERIOD``, ``OBS_VALUE``, plus add our own metadata.
    """
    import pandas as pd

    if raw is None or (isinstance(raw, pd.DataFrame) and raw.empty):
        return pl.DataFrame()

    if not isinstance(raw, pd.DataFrame):
        logger.warning(f"Unexpected type from ecbdata: {type(raw)}")
        return pl.DataFrame()

    # ecbdata always returns these two columns for the actual data
    if "TIME_PERIOD" not in raw.columns or "OBS_VALUE" not in raw.columns:
        logger.warning(
            f"Expected columns missing for {series_id}. Got: {list(raw.columns)}"
        )
        return pl.DataFrame()

    batch_ts = datetime.now(timezone.utc)

    _df = pl.from_pandas(raw[["TIME_PERIOD", "OBS_VALUE"]].copy())
    assert isinstance(_df, pl.DataFrame)
    df: pl.DataFrame = _df
    df = df.rename({"TIME_PERIOD": "date", "OBS_VALUE": "value"})

    # TIME_PERIOD can be daily (YYYY-MM-DD) or monthly (YYYY-MM)
    # Try daily first, fall back to monthly
    date_col: pl.Series = df.get_column("date")
    date_samples = date_col.head(3).to_list()
    sample = str(date_samples[0]) if date_samples else ""
    if len(sample) == 7 and sample[4] == "-":  # YYYY-MM
        df = df.with_columns(
            pl.col("date")
            .str.to_date("%Y-%m", strict=False)
            .dt.month_start()
            .alias("date")
        )
    else:
        df = df.with_columns(pl.col("date").str.to_date("%Y-%m-%d", strict=False))

    df = df.with_columns(
        pl.lit(series_label).alias("series_label"),
        pl.lit(series_id).alias("series_id"),
        pl.lit("ECB").alias("source"),
        pl.lit(batch_ts).alias("last_fetched_at"),
    )

    # Cast value to float, coercing any non-numeric to null
    df = df.with_columns(pl.col("value").cast(pl.Float64, strict=False))

    return df.sort("date")


# ---------------------------------------------------------------------------
# Extractor
# ---------------------------------------------------------------------------


class EcbExtractor:
    """
    Extract data from the European Central Bank (ECB) Data Portal.

    Uses the ``ecbdata`` package to query SDMX series.  No API key is
    required — ECB data is freely available.
    """

    def __init__(self, calls_per_minute: int = 60) -> None:
        self.rate_limiter = _RateLimiter(calls_per_minute)

    # -- low-level ---------------------------------------------------------

    def get_series(
        self,
        series_id: str,
        series_label: str | None = None,
        start: str | None = None,
        end: str | None = None,
    ) -> pl.DataFrame:
        """
        Fetch a single ECB series by its SDMX identifier.

        Parameters
        ----------
        series_id
            Full ECB series code, e.g.
            ``"ICP.M.U2.Y.000000.3.INX"``.
        series_label
            Human-readable label (stored in the ``series_label`` column).
        start, end
            Date bounds.  Format depends on series frequency:
            ``"YYYY-MM-DD"`` for daily, ``"YYYY-MM"`` for monthly.

        Returns
        -------
        polars.DataFrame
            Tidy long-format frame with columns
            ``date, value, series_label, series_id, source, last_fetched_at``.
        """
        self.rate_limiter.sleep_if_needed()
        label = series_label or series_id

        logger.info(f"Fetching ECB series: {series_id}")
        try:
            kwargs: dict[str, str] = {}
            if start is not None:
                kwargs["start"] = start
            if end is not None:
                kwargs["end"] = end
            raw = ecbdata.get_series(series_id, **kwargs)  # type: ignore[arg-type]
        except Exception as exc:
            logger.error(f"Failed to fetch {series_id}: {exc}")
            raise

        df = _parse_ecb_df(raw, series_id, label)
        logger.info(f"Fetched {len(df)} observations for {series_id}")
        return df

    def get_multiple_series(
        self,
        series_map: dict[str, str],
        start: str | None = None,
        end: str | None = None,
    ) -> dict[str, pl.DataFrame | None]:
        """
        Fetch multiple series.  Returns ``{label: DataFrame | None}``.
        """
        results: dict[str, pl.DataFrame | None] = {}
        for label, series_id in series_map.items():
            try:
                results[label] = self.get_series(series_id, label, start, end)
            except Exception as exc:
                logger.error(f"Failed to fetch {label} ({series_id}): {exc}")
                results[label] = None
        return results

    # -- interest rates ----------------------------------------------------

    def get_interest_rates(
        self,
        rates: list[str] | str = "ALL",
        start: str | None = None,
        end: str | None = None,
    ) -> pl.DataFrame:
        """
        Fetch ECB key interest rates.

        Parameters
        ----------
        rates
            ``"ALL"`` or a list of rate names:
            ``"mro_fixed_rate"``, ``"mro_min_bid_rate"``, ``"deposit_facility"``.
        """
        if rates == "ALL":
            rates = list(_ECB_INTEREST_RATES.keys())
        elif isinstance(rates, str):
            rates = [rates]

        series_map = {k: _ECB_INTEREST_RATES[k] for k in rates}
        results = self.get_multiple_series(series_map, start, end)

        dfs: list[pl.DataFrame] = []
        for label, df in results.items():
            if df is not None and not df.is_empty():
                dfs.append(df)

        if not dfs:
            return pl.DataFrame()
        return pl.concat(dfs, how="diagonal_relaxed").sort("date")

    # -- inflation ---------------------------------------------------------

    def get_hicp(
        self,
        items: list[str] | str = "ALL",
        start: str | None = None,
        end: str | None = None,
    ) -> pl.DataFrame:
        """
        Fetch Harmonised Index of Consumer Prices (HICP).

        Parameters
        ----------
        items
            ``"ALL"`` or a list of item names:
            ``"hicp_all_items"``, ``"hicp_energy"``, ``"hicp_food"``,
            ``"hicp_core"``.
        """
        if items == "ALL":
            items = list(_ECB_INFLATION.keys())
        elif isinstance(items, str):
            items = [items]

        series_map = {k: _ECB_INFLATION[k] for k in items}
        results = self.get_multiple_series(series_map, start, end)

        dfs: list[pl.DataFrame] = []
        for label, df in results.items():
            if df is not None and not df.is_empty():
                dfs.append(df)

        if not dfs:
            return pl.DataFrame()
        return pl.concat(dfs, how="diagonal_relaxed").sort("date")

    # -- money supply ------------------------------------------------------

    def get_monetary_aggregates(
        self,
        aggregates: list[str] | str = "ALL",
        start: str | None = None,
        end: str | None = None,
    ) -> pl.DataFrame:
        """
        Fetch monetary aggregates (M1, M3).

        Parameters
        ----------
        aggregates
            ``"ALL"`` or a list of names: ``"m1"``, ``"m3"``.
        """
        if aggregates == "ALL":
            aggregates = list(_ECB_MONEY.keys())
        elif isinstance(aggregates, str):
            aggregates = [aggregates]

        series_map = {k: _ECB_MONEY[k] for k in aggregates}
        results = self.get_multiple_series(series_map, start, end)

        dfs: list[pl.DataFrame] = []
        for label, df in results.items():
            if df is not None and not df.is_empty():
                dfs.append(df)

        if not dfs:
            return pl.DataFrame()
        return pl.concat(dfs, how="diagonal_relaxed").sort("date")
