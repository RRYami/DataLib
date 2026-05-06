from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Union

import polars as pl
import requests

from get_api_keys import get_api_key
from logger.logger import get_logger, setup_logging

setup_logging()
logger = get_logger(__name__)

BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

# ---------------------------------------------------------------------------
# Treasury Constant Maturity Yields (H.15) — market-quoted par yields
# ---------------------------------------------------------------------------
TREASURY_CONSTANT_MATURITY = {
    "1M": "DGS1MO",
    "3M": "DGS3MO",
    "6M": "DGS6MO",
    "1Y": "DGS1",
    "2Y": "DGS2",
    "3Y": "DGS3",
    "5Y": "DGS5",
    "7Y": "DGS7",
    "10Y": "DGS10",
    "20Y": "DGS20",
    "30Y": "DGS30",
}

# ---------------------------------------------------------------------------
# Gürkaynak-Sack-Wright (GSW) fitted zero-coupon yields (daily, 1990+)
# ---------------------------------------------------------------------------
GSW_ZERO_COUPON_YIELDS = {
    "1Y": "THREEFY1",
    "2Y": "THREEFY2",
    "3Y": "THREEFY3",
    "4Y": "THREEFY4",
    "5Y": "THREEFY5",
    "6Y": "THREEFY6",
    "7Y": "THREEFY7",
    "8Y": "THREEFY8",
    "9Y": "THREEFY9",
    "10Y": "THREEFY10",
}

# ---------------------------------------------------------------------------
# GSW instantaneous forward rates (daily, 1990+)
# ---------------------------------------------------------------------------
GSW_INSTANTANEOUS_FORWARDS = {
    "1Y": "THREEFF1",
    "2Y": "THREEFF2",
    "3Y": "THREEFF3",
    "4Y": "THREEFF4",
    "5Y": "THREEFF5",
    "6Y": "THREEFF6",
    "7Y": "THREEFF7",
    "8Y": "THREEFF8",
    "9Y": "THREEFF9",
    "10Y": "THREEFF10",
}

# ---------------------------------------------------------------------------
# GSW term premiums — spot (zero-coupon) and forward
# ---------------------------------------------------------------------------
GSW_TERM_PREMIUMS_SPOT = {
    "1Y": "THREEFYTP1",
    "2Y": "THREEFYTP2",
    "3Y": "THREEFYTP3",
    "4Y": "THREEFYTP4",
    "5Y": "THREEFYTP5",
    "6Y": "THREEFYTP6",
    "7Y": "THREEFYTP7",
    "8Y": "THREEFYTP8",
    "9Y": "THREEFYTP9",
    "10Y": "THREEFYTP10",
}

GSW_TERM_PREMIUMS_FORWARD = {
    "1Y": "THREEFFTP1",
    "2Y": "THREEFFTP2",
    "3Y": "THREEFFTP3",
    "4Y": "THREEFFTP4",
    "5Y": "THREEFFTP5",
    "6Y": "THREEFFTP6",
    "7Y": "THREEFFTP7",
    "8Y": "THREEFFTP8",
    "9Y": "THREEFFTP9",
    "10Y": "THREEFFTP10",
}

ALL_CURVE_COMPONENTS = {
    "constant_maturity": TREASURY_CONSTANT_MATURITY,
    "zero_coupon": GSW_ZERO_COUPON_YIELDS,
    "instantaneous_forward": GSW_INSTANTANEOUS_FORWARDS,
    "term_premium_spot": GSW_TERM_PREMIUMS_SPOT,
    "term_premium_forward": GSW_TERM_PREMIUMS_FORWARD,
}


def _build_params(
    api_key: str,
    series_id: str,
    observation_start: Optional[str] = None,
    observation_end: Optional[str] = None,
    frequency: str = "d",
    file_type: str = "json",
    limit: int = 100_000,
) -> dict:
    params = {
        "series_id": series_id,
        "api_key": api_key,
        "file_type": file_type,
        "frequency": frequency,
        "limit": limit,
    }
    if observation_start:
        params["observation_start"] = observation_start
    if observation_end:
        params["observation_end"] = observation_end
    return params


def _fetch_single_series(
    session: requests.Session,
    tenor: str,
    series_id: str,
    params: dict,
) -> pl.DataFrame:
    """
    Fetch a single FRED series and return a tidy Polars DataFrame
    with columns ``[date, <tenor>]``.
    """
    try:
        response = session.get(BASE_URL, params=params)
        response.raise_for_status()
        data = response.json()
    except requests.RequestException as e:
        logger.error(f"Failed to fetch {series_id} ({tenor}): {e}")
        raise

    observations = data.get("observations", [])
    if not observations:
        logger.warning(f"No observations returned for {series_id} ({tenor})")
        return pl.DataFrame(
            schema={
                "date": pl.Date,
                tenor: pl.Float64,
            }
        )

    return pl.DataFrame(observations).select(
        [
            pl.col("date").str.to_date("%Y-%m-%d"),
            pl.col("value").replace(".", None).cast(pl.Float64).alias(tenor),
        ]
    )


def _merge_series(dfs: list[pl.DataFrame]) -> pl.DataFrame:
    """
    Outer-join a list of single-series DataFrames on ``date``.
    """
    if not dfs:
        return pl.DataFrame(schema={"date": pl.Date})

    combined = dfs[0]
    for df in dfs[1:]:
        combined = combined.join(df, on="date", how="outer", coalesce=True)
    return combined.sort("date")


class FredExtractor:
    """
    A class to extract data from the FRED (Federal Reserve Economic Data) API.
    """

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or get_api_key("FRED_KEY")

    # ------------------------------------------------------------------
    # Low-level API
    # ------------------------------------------------------------------

    def get_series_observations(
        self,
        series_id: Union[dict, str],
        observation_start: Optional[str] = None,
        observation_end: Optional[str] = None,
        frequency: Optional[str] = "d",
        file_type: Optional[str] = "json",
        limit: Optional[int] = 100_000,
    ) -> Union[dict, pl.DataFrame]:
        """
        Fetch series observations from FRED API.

        Parameters
        ----------
        series_id
            A single FRED series ID (``str``) or a **dict** mapping
            human-readable tenor names → series IDs.
        observation_start, observation_end
            Date bounds in ``YYYY-MM-DD`` format.  ``None`` means unbounded.
        frequency
            Data frequency (``'d'``, ``'w'``, ``'m'``, etc.).
        file_type
            Response format — currently only ``'json'`` is supported for
            DataFrame output.
        limit
            Max observations per series.

        Returns
        -------
        dict
            When ``series_id`` is a single string.
        polars.DataFrame
            When ``series_id`` is a dict (wide format, outer-joined on date).
        """
        if isinstance(series_id, str):
            params = _build_params(
                self.api_key,
                series_id,
                observation_start,
                observation_end,
                frequency or "d",
                file_type or "json",
                limit or 100_000,
            )
            with requests.Session() as session:
                response = session.get(BASE_URL, params=params)
            response.raise_for_status()
            return response.json()

        if isinstance(series_id, dict):
            return self._fetch_multiple(
                series_id,
                observation_start,
                observation_end,
                frequency or "d",
                file_type or "json",
                limit or 100_000,
            )

        raise TypeError("series_id must be a str or a dict")

    def _fetch_multiple(
        self,
        series_map: dict[str, str],
        observation_start: Optional[str] = None,
        observation_end: Optional[str] = None,
        frequency: str = "d",
        file_type: str = "json",
        limit: int = 100_000,
    ) -> pl.DataFrame:
        """
        Parallel-fetch every series in *series_map* and outer-join on date.
        Missing series (due to API errors) are represented as all-null columns.
        """
        dfs: list[pl.DataFrame] = []
        failed_tenors: list[str] = []
        params_template = {
            "api_key": self.api_key,
            "file_type": file_type,
            "frequency": frequency,
            "limit": limit,
        }
        if observation_start:
            params_template["observation_start"] = observation_start
        if observation_end:
            params_template["observation_end"] = observation_end

        with requests.Session() as session:
            with ThreadPoolExecutor(max_workers=min(10, len(series_map))) as executor:
                futures = {
                    executor.submit(
                        _fetch_single_series,
                        session,
                        tenor,
                        sid,
                        {**params_template, "series_id": sid},
                    ): tenor
                    for tenor, sid in series_map.items()
                }
                for future in as_completed(futures):
                    tenor = futures[future]
                    try:
                        df = future.result()
                        dfs.append(df)
                    except Exception as e:
                        logger.error(f"Error processing {tenor}: {e}")
                        failed_tenors.append(tenor)

        result = _merge_series(dfs)
        # Add null columns for any tenors that failed so downstream code
        # can still reference them safely.
        for tenor in failed_tenors:
            if tenor not in result.columns:
                result = result.with_columns(pl.lit(None).cast(pl.Float64).alias(tenor))

        return result

    # ------------------------------------------------------------------
    # High-level convenience methods
    # ------------------------------------------------------------------

    def get_constant_maturity_yields(
        self,
        observation_start: Optional[str] = None,
        observation_end: Optional[str] = None,
        tenors: Optional[list[str]] = None,
    ) -> pl.DataFrame:
        """
        Fetch the standard H.15 Treasury constant-maturity yield curve.

        Parameters
        ----------
        tenors
            Subset of tenor keys to retrieve (e.g. ``["2Y", "10Y"]``).
            ``None`` returns the full curve.
        """
        mapping = TREASURY_CONSTANT_MATURITY
        if tenors is not None:
            mapping = {k: v for k, v in mapping.items() if k in tenors}
        return self._fetch_multiple(
            mapping,
            observation_start=observation_start,
            observation_end=observation_end,
        )

    def get_zero_coupon_yields(
        self,
        observation_start: Optional[str] = None,
        observation_end: Optional[str] = None,
        tenors: Optional[list[str]] = None,
    ) -> pl.DataFrame:
        """
        Fetch GSW fitted zero-coupon Treasury yields (``THREEFY*``).
        """
        mapping = GSW_ZERO_COUPON_YIELDS
        if tenors is not None:
            mapping = {k: v for k, v in mapping.items() if k in tenors}
        return self._fetch_multiple(
            mapping,
            observation_start=observation_start,
            observation_end=observation_end,
        )

    def get_instantaneous_forward_rates(
        self,
        observation_start: Optional[str] = None,
        observation_end: Optional[str] = None,
        tenors: Optional[list[str]] = None,
    ) -> pl.DataFrame:
        """
        Fetch GSW fitted instantaneous forward rates (``THREEFF*``).
        """
        mapping = GSW_INSTANTANEOUS_FORWARDS
        if tenors is not None:
            mapping = {k: v for k, v in mapping.items() if k in tenors}
        return self._fetch_multiple(
            mapping,
            observation_start=observation_start,
            observation_end=observation_end,
        )

    def get_term_premiums(
        self,
        observation_start: Optional[str] = None,
        observation_end: Optional[str] = None,
        tenors: Optional[list[str]] = None,
        forward: bool = False,
    ) -> pl.DataFrame:
        """
        Fetch GSW term premiums.

        Parameters
        ----------
        forward
            If ``True``, fetch forward term premiums (``THREEFFTP*``);
            otherwise fetch spot term premiums (``THREEFYTP*``).
        """
        mapping = GSW_TERM_PREMIUMS_FORWARD if forward else GSW_TERM_PREMIUMS_SPOT
        if tenors is not None:
            mapping = {k: v for k, v in mapping.items() if k in tenors}
        return self._fetch_multiple(
            mapping,
            observation_start=observation_start,
            observation_end=observation_end,
        )

    def get_full_curve(
        self,
        observation_start: Optional[str] = None,
        observation_end: Optional[str] = None,
        include_constant_maturity: bool = True,
        include_zero_coupon: bool = True,
        include_instantaneous_forwards: bool = True,
        include_term_premiums: bool = False,
        long_format: bool = False,
    ) -> pl.DataFrame:
        """
        Fetch every requested curve component and outer-join into a single
        wide DataFrame (or long format if requested).

        Column naming convention
        ------------------------
        * Constant maturity: ``1M``, ``3M``, … ``30Y``
        * Zero-coupon: ``ZC_1Y``, … ``ZC_10Y``
        * Instantaneous forwards: ``FWD_1Y``, … ``FWD_10Y``
        * Term premiums (spot): ``TP_1Y``, … ``TP_10Y``
        * Term premiums (forward): ``TPFWD_1Y``, … ``TPFWD_10Y``

        Parameters
        ----------
        long_format
            If ``True``, melt the wide DataFrame to long format with columns
            ``[date, tenor, value]``.
        """
        all_dfs: list[pl.DataFrame] = []

        if include_constant_maturity:
            df = self.get_constant_maturity_yields(observation_start, observation_end)
            all_dfs.append(df)

        if include_zero_coupon:
            df = self.get_zero_coupon_yields(observation_start, observation_end)
            if not df.is_empty():
                df = df.rename({k: f"ZC_{k}" for k in GSW_ZERO_COUPON_YIELDS.keys()})
                all_dfs.append(df)

        if include_instantaneous_forwards:
            df = self.get_instantaneous_forward_rates(
                observation_start, observation_end
            )
            if not df.is_empty():
                df = df.rename(
                    {k: f"FWD_{k}" for k in GSW_INSTANTANEOUS_FORWARDS.keys()}
                )
                all_dfs.append(df)

        if include_term_premiums:
            df_spot = self.get_term_premiums(
                observation_start, observation_end, forward=False
            )
            if not df_spot.is_empty():
                df_spot = df_spot.rename(
                    {k: f"TP_{k}" for k in GSW_TERM_PREMIUMS_SPOT.keys()}
                )
                all_dfs.append(df_spot)

            df_fwd = self.get_term_premiums(
                observation_start, observation_end, forward=True
            )
            if not df_fwd.is_empty():
                df_fwd = df_fwd.rename(
                    {k: f"TPFWD_{k}" for k in GSW_TERM_PREMIUMS_FORWARD.keys()}
                )
                all_dfs.append(df_fwd)

        wide = _merge_series(all_dfs)
        if not long_format:
            return wide

        return wide.unpivot(
            index=["date"],
            variable_name="tenor",
            value_name="value",
        ).sort(["date", "tenor"])
