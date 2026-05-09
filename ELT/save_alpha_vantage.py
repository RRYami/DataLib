from __future__ import annotations

import os

import polars as pl

from ELT.base import ParquetSaver
from ELT.extract_alpha_vantage import AlphaVantageExtractor
from logger.logger import get_logger
from utils.results import SaveResult

logger = get_logger(__name__)


class AlphaVantageSaver(ParquetSaver):
    """
    Persist Alpha Vantage data to per-ticker Parquet files with idempotent,
    incremental updates.

    Each ticker gets its own file so you can add, remove, or refresh
    individual tickers without touching the rest.
    """

    def __init__(
        self,
        data_dir: str | os.PathLike = "data/parquet",
        api_key: str | None = None,
    ):
        super().__init__(data_dir)
        self.extractor = AlphaVantageExtractor(api_key=api_key)

    # ------------------------------------------------------------------
    # Public API — fundamentals (per-ticker files)
    # ------------------------------------------------------------------

    def save_income_statement(self, tickers: list[str]) -> SaveResult:
        """Save / update income statements — one file per ticker."""
        result = SaveResult()
        for ticker in tickers:
            ok = self._save_single_ticker(
                ticker,
                lambda t=ticker: self.extractor.get_statement(t, "income_statement"),
                "income_statement",
                ["fiscal_date_ending", "report_type"],
            )
            if ok:
                result.add_saved(ticker)
            else:
                result.add_failed(ticker, "fetch or validation failed")
        return result

    def save_balance_sheet(self, tickers: list[str]) -> SaveResult:
        """Save / update balance sheets — one file per ticker."""
        result = SaveResult()
        for ticker in tickers:
            ok = self._save_single_ticker(
                ticker,
                lambda t=ticker: self.extractor.get_statement(t, "balance_sheet"),
                "balance_sheet",
                ["fiscal_date_ending", "report_type"],
            )
            if ok:
                result.add_saved(ticker)
            else:
                result.add_failed(ticker, "fetch or validation failed")
        return result

    def save_cash_flow(self, tickers: list[str]) -> SaveResult:
        """Save / update cash flow statements — one file per ticker."""
        result = SaveResult()
        for ticker in tickers:
            ok = self._save_single_ticker(
                ticker,
                lambda t=ticker: self.extractor.get_statement(t, "cash_flow"),
                "cash_flow",
                ["fiscal_date_ending", "report_type"],
            )
            if ok:
                result.add_saved(ticker)
            else:
                result.add_failed(ticker, "fetch or validation failed")
        return result

    def save_earnings(self, tickers: list[str]) -> SaveResult:
        """Save / update earnings — one file per ticker."""
        result = SaveResult()
        for ticker in tickers:
            ok = self._save_single_ticker(
                ticker,
                lambda t=ticker: self.extractor.get_statement(t, "earnings"),
                "earnings",
                ["fiscal_date_ending", "report_type"],
            )
            if ok:
                result.add_saved(ticker)
            else:
                result.add_failed(ticker, "fetch or validation failed")
        return result

    def save_overview(self, tickers: list[str]) -> SaveResult:
        """Save / update company overviews — one file per ticker."""
        result = SaveResult()
        for ticker in tickers:
            ok = self._save_single_ticker(
                ticker,
                lambda t=ticker: self.extractor.get_overview(t),
                "overview",
                ["ticker"],
            )
            if ok:
                result.add_saved(ticker)
            else:
                result.add_failed(ticker, "fetch or validation failed")
        return result

    # ------------------------------------------------------------------
    # Public API — time series (per-ticker files)
    # ------------------------------------------------------------------

    def save_daily_adjusted(self, tickers: list[str]) -> SaveResult:
        """Save / update daily adjusted OHLCV — one file per ticker."""
        result = SaveResult()
        for ticker in tickers:
            ok = self._save_single_ticker(
                ticker,
                lambda t=ticker: self.extractor.get_time_series(t, "daily_adjusted"),
                "daily_adjusted",
                ["date"],
            )
            if ok:
                result.add_saved(ticker)
            else:
                result.add_failed(ticker, "fetch or validation failed")
        return result

    def save_weekly_adjusted(self, tickers: list[str]) -> SaveResult:
        """Save / update weekly adjusted OHLCV — one file per ticker."""
        result = SaveResult()
        for ticker in tickers:
            ok = self._save_single_ticker(
                ticker,
                lambda t=ticker: self.extractor.get_time_series(t, "weekly_adjusted"),
                "weekly_adjusted",
                ["date"],
            )
            if ok:
                result.add_saved(ticker)
            else:
                result.add_failed(ticker, "fetch or validation failed")
        return result

    def save_monthly_adjusted(self, tickers: list[str]) -> SaveResult:
        """Save / update monthly adjusted OHLCV — one file per ticker."""
        result = SaveResult()
        for ticker in tickers:
            ok = self._save_single_ticker(
                ticker,
                lambda t=ticker: self.extractor.get_time_series(t, "monthly_adjusted"),
                "monthly_adjusted",
                ["date"],
            )
            if ok:
                result.add_saved(ticker)
            else:
                result.add_failed(ticker, "fetch or validation failed")
        return result

    # ------------------------------------------------------------------
    # Batch operations
    # ------------------------------------------------------------------

    def save_all_fundamentals(self, tickers: list[str]) -> SaveResult:
        """Run all fundamentals + overview saves."""
        combined = SaveResult()
        for fn in [
            self.save_income_statement,
            self.save_balance_sheet,
            self.save_cash_flow,
            self.save_earnings,
            self.save_overview,
        ]:
            res = fn(tickers)
            combined.saved.extend(res.saved)
            combined.failed.extend(res.failed)
            combined.skipped.extend(res.skipped)
        return combined

    def save_all(self, tickers: list[str]) -> SaveResult:
        """Run everything: fundamentals + all time-series intervals."""
        combined = self.save_all_fundamentals(tickers)
        for fn in [
            self.save_daily_adjusted,
            self.save_weekly_adjusted,
            self.save_monthly_adjusted,
        ]:
            res = fn(tickers)
            combined.saved.extend(res.saved)
            combined.failed.extend(res.failed)
            combined.skipped.extend(res.skipped)
        return combined

    # ------------------------------------------------------------------
    # Read helpers (per-ticker)
    # ------------------------------------------------------------------

    def read_income_statement(self, ticker: str) -> pl.DataFrame | None:
        path = self._sub_dir("income_statement") / f"{ticker.upper()}.parquet"
        return self._read_existing(path)

    def read_balance_sheet(self, ticker: str) -> pl.DataFrame | None:
        path = self._sub_dir("balance_sheet") / f"{ticker.upper()}.parquet"
        return self._read_existing(path)

    def read_cash_flow(self, ticker: str) -> pl.DataFrame | None:
        path = self._sub_dir("cash_flow") / f"{ticker.upper()}.parquet"
        return self._read_existing(path)

    def read_earnings(self, ticker: str) -> pl.DataFrame | None:
        path = self._sub_dir("earnings") / f"{ticker.upper()}.parquet"
        return self._read_existing(path)

    def read_overview(self, ticker: str) -> pl.DataFrame | None:
        path = self._sub_dir("overview") / f"{ticker.upper()}.parquet"
        return self._read_existing(path)

    def read_daily_adjusted(self, ticker: str) -> pl.DataFrame | None:
        path = self._sub_dir("daily_adjusted") / f"{ticker.upper()}.parquet"
        return self._read_existing(path)

    def read_weekly_adjusted(self, ticker: str) -> pl.DataFrame | None:
        path = self._sub_dir("weekly_adjusted") / f"{ticker.upper()}.parquet"
        return self._read_existing(path)

    def read_monthly_adjusted(self, ticker: str) -> pl.DataFrame | None:
        path = self._sub_dir("monthly_adjusted") / f"{ticker.upper()}.parquet"
        return self._read_existing(path)

    # ------------------------------------------------------------------
    # Read helpers (aggregate across tickers)
    # ------------------------------------------------------------------

    def read_all_income_statements(self) -> pl.DataFrame | None:
        """Read and concatenate all income-statement files."""
        return self._read_all_in_subdir("income_statement")

    def read_all_balance_sheets(self) -> pl.DataFrame | None:
        return self._read_all_in_subdir("balance_sheet")

    def read_all_cash_flows(self) -> pl.DataFrame | None:
        return self._read_all_in_subdir("cash_flow")

    def read_all_earnings(self) -> pl.DataFrame | None:
        return self._read_all_in_subdir("earnings")

    def read_all_overviews(self) -> pl.DataFrame | None:
        return self._read_all_in_subdir("overview")

    def read_all_daily_adjusted(self) -> pl.DataFrame | None:
        return self._read_all_in_subdir("daily_adjusted")

    def read_all_weekly_adjusted(self) -> pl.DataFrame | None:
        return self._read_all_in_subdir("weekly_adjusted")

    def read_all_monthly_adjusted(self) -> pl.DataFrame | None:
        return self._read_all_in_subdir("monthly_adjusted")
