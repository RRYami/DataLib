from __future__ import annotations

import os

import polars as pl

from ELT.base import ParquetSaver, merge_and_dedupe
from ELT.extract_polygon import PolygonExtractor
from logger.logger import get_logger
from utils.results import SaveResult

logger = get_logger(__name__)


class PolygonSaver(ParquetSaver):
    """
    Persist Polygon.io data to per-ticker Parquet files with idempotent,
    incremental updates.
    """

    def __init__(
        self,
        data_dir: str | os.PathLike = "data/parquet",
        api_key: str | None = None,
    ):
        super().__init__(data_dir)
        self.extractor = PolygonExtractor(api_key=api_key)

    # ------------------------------------------------------------------
    # Public API — per-ticker files
    # ------------------------------------------------------------------

    def save_daily_bars(
        self,
        tickers: list[str],
        start_date: str,
        end_date: str,
    ) -> SaveResult:
        """Save / update daily OHLCV bars — one file per ticker."""
        result = SaveResult()
        for ticker in tickers:
            ok = self._save_single_ticker(
                ticker,
                lambda t=ticker: self.extractor.get_daily_bars(t, start_date, end_date),
                "daily_bars",
                ["date"],
            )
            if ok:
                result.add_saved(ticker)
            else:
                result.add_failed(ticker, "fetch or validation failed")
        return result

    def save_ticker_details(self, tickers: list[str]) -> SaveResult:
        """Save / update company details — one file per ticker."""
        result = SaveResult()
        for ticker in tickers:
            ok = self._save_single_ticker(
                ticker,
                lambda t=ticker: self.extractor.get_ticker_details(t),
                "ticker_details",
                ["ticker"],
            )
            if ok:
                result.add_saved(ticker)
            else:
                result.add_failed(ticker, "fetch or validation failed")
        return result

    def save_daily_open_close(
        self,
        tickers: list[str],
        date: str,
    ) -> SaveResult:
        """Save / update open/close snapshots — one file per ticker."""
        result = SaveResult()
        for ticker in tickers:
            ok = self._save_single_ticker(
                ticker,
                lambda t=ticker: self.extractor.get_daily_open_close(t, date),
                "daily_open_close",
                ["date"],
            )
            if ok:
                result.add_saved(ticker)
            else:
                result.add_failed(ticker, "fetch or validation failed")
        return result

    # ------------------------------------------------------------------
    # Public API — aggregate files
    # ------------------------------------------------------------------

    def save_ticker_list(
        self,
        market: str = "stocks",
        limit: int = 2_500,
    ) -> SaveResult:
        """Save / update the full ticker list for a market."""
        result = SaveResult()
        path = self._sub_dir("ticker_list") / f"{market}.parquet"
        existing = self._read_existing(path)

        try:
            new_df = self.extractor.get_ticker_list(market=market, limit=limit)
        except Exception as exc:
            logger.error(f"Failed to fetch ticker list: {exc}")
            result.add_failed("ticker_list", str(exc))
            return result

        if new_df is None or new_df.is_empty():
            logger.warning("No ticker list data returned")
            result.add_skipped("ticker_list")
            return result

        if existing is not None:
            merged = merge_and_dedupe(existing, new_df, ["ticker"])
        else:
            merged = new_df

        merged = merged.sort("ticker")
        self._write_parquet(merged, path)
        result.add_saved("ticker_list")
        return result

    # ------------------------------------------------------------------
    # Batch operations
    # ------------------------------------------------------------------

    def save_all(
        self,
        tickers: list[str],
        start_date: str,
        end_date: str,
    ) -> SaveResult:
        """Run everything: ticker details + daily bars."""
        details_result = self.save_ticker_details(tickers)
        bars_result = self.save_daily_bars(tickers, start_date, end_date)
        combined = SaveResult()
        combined.saved = details_result.saved + bars_result.saved
        combined.failed = details_result.failed + bars_result.failed
        combined.skipped = details_result.skipped + bars_result.skipped
        return combined

    # ------------------------------------------------------------------
    # Read helpers (per-ticker)
    # ------------------------------------------------------------------

    def read_daily_bars(self, ticker: str) -> pl.DataFrame | None:
        path = self._sub_dir("daily_bars") / f"{ticker.upper()}.parquet"
        return self._read_existing(path)

    def read_ticker_details(self, ticker: str) -> pl.DataFrame | None:
        path = self._sub_dir("ticker_details") / f"{ticker.upper()}.parquet"
        return self._read_existing(path)

    def read_daily_open_close(self, ticker: str) -> pl.DataFrame | None:
        path = self._sub_dir("daily_open_close") / f"{ticker.upper()}.parquet"
        return self._read_existing(path)

    def read_ticker_list(self, market: str = "stocks") -> pl.DataFrame | None:
        path = self._sub_dir("ticker_list") / f"{market}.parquet"
        return self._read_existing(path)

    # ------------------------------------------------------------------
    # Read helpers (aggregate across tickers)
    # ------------------------------------------------------------------

    def read_all_daily_bars(self) -> pl.DataFrame | None:
        """Read and concatenate all daily-bars files."""
        return self._read_all_in_subdir("daily_bars")

    def read_all_ticker_details(self) -> pl.DataFrame | None:
        return self._read_all_in_subdir("ticker_details")

    def read_all_daily_open_close(self) -> pl.DataFrame | None:
        return self._read_all_in_subdir("daily_open_close")
