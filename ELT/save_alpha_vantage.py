from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

from ELT.extract_alpha_vantage import AlphaVantageExtractor
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
    """
    Concatenate *existing* and *new*, then keep the most-recent
    ``last_fetched_at`` for each unique key combination.
    """
    combined = pl.concat([existing, new], how="diagonal_relaxed")
    combined = combined.sort([*unique_keys, "last_fetched_at"])
    return combined.unique(subset=unique_keys, keep="last")


class AlphaVantageSaver:
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
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.extractor = AlphaVantageExtractor(api_key=api_key)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _sub_dir(self, name: str) -> Path:
        """Return (and create) a subdirectory for a data type."""
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

    def _save_single_ticker(
        self,
        ticker: str,
        fetch_fn,
        sub_dir_name: str,
        dedupe_keys: list[str],
    ) -> None:
        """
        Fetch one ticker, merge with existing file, and write back.

        Parameters
        ----------
        ticker
            Stock symbol.
        fetch_fn
            Callable that returns a ``pl.DataFrame``.
        sub_dir_name
            Sub-directory name (e.g. ``"income_statement"``).
        dedupe_keys
            Columns to use for deduplication (``last_fetched_at`` is
            appended automatically).
        """
        path = self._sub_dir(sub_dir_name) / f"{ticker.upper()}.parquet"
        existing = self._read_existing(path)

        try:
            new_df = fetch_fn()
        except Exception as exc:
            logger.error(f"Failed to fetch {sub_dir_name} for {ticker}: {exc}")
            return

        if new_df is None or new_df.is_empty():
            logger.warning(f"No data returned for {ticker} {sub_dir_name}")
            return

        if existing is not None:
            merged = _merge_and_dedupe(existing, new_df, dedupe_keys)
        else:
            merged = new_df

        merged = merged.sort(dedupe_keys)
        self._write_parquet(merged, path)

    # ------------------------------------------------------------------
    # Public API — fundamentals (per-ticker files)
    # ------------------------------------------------------------------

    def save_income_statement(self, tickers: list[str]) -> None:
        """Save / update income statements — one file per ticker."""
        for ticker in tickers:
            self._save_single_ticker(
                ticker,
                lambda t=ticker: self.extractor.get_statement(t, "income_statement"),
                "income_statement",
                ["fiscal_date_ending", "report_type"],
            )

    def save_balance_sheet(self, tickers: list[str]) -> None:
        """Save / update balance sheets — one file per ticker."""
        for ticker in tickers:
            self._save_single_ticker(
                ticker,
                lambda t=ticker: self.extractor.get_statement(t, "balance_sheet"),
                "balance_sheet",
                ["fiscal_date_ending", "report_type"],
            )

    def save_cash_flow(self, tickers: list[str]) -> None:
        """Save / update cash flow statements — one file per ticker."""
        for ticker in tickers:
            self._save_single_ticker(
                ticker,
                lambda t=ticker: self.extractor.get_statement(t, "cash_flow"),
                "cash_flow",
                ["fiscal_date_ending", "report_type"],
            )

    def save_earnings(self, tickers: list[str]) -> None:
        """Save / update earnings — one file per ticker."""
        for ticker in tickers:
            self._save_single_ticker(
                ticker,
                lambda t=ticker: self.extractor.get_statement(t, "earnings"),
                "earnings",
                ["fiscal_date_ending", "report_type"],
            )

    def save_overview(self, tickers: list[str]) -> None:
        """Save / update company overviews — one file per ticker."""
        for ticker in tickers:
            self._save_single_ticker(
                ticker,
                lambda t=ticker: self.extractor.get_overview(t),
                "overview",
                ["ticker"],
            )

    # ------------------------------------------------------------------
    # Public API — time series (per-ticker files)
    # ------------------------------------------------------------------

    def save_daily_adjusted(self, tickers: list[str]) -> None:
        """Save / update daily adjusted OHLCV — one file per ticker."""
        for ticker in tickers:
            self._save_single_ticker(
                ticker,
                lambda t=ticker: self.extractor.get_time_series(t, "daily_adjusted"),
                "daily_adjusted",
                ["date"],
            )

    def save_weekly_adjusted(self, tickers: list[str]) -> None:
        """Save / update weekly adjusted OHLCV — one file per ticker."""
        for ticker in tickers:
            self._save_single_ticker(
                ticker,
                lambda t=ticker: self.extractor.get_time_series(t, "weekly_adjusted"),
                "weekly_adjusted",
                ["date"],
            )

    def save_monthly_adjusted(self, tickers: list[str]) -> None:
        """Save / update monthly adjusted OHLCV — one file per ticker."""
        for ticker in tickers:
            self._save_single_ticker(
                ticker,
                lambda t=ticker: self.extractor.get_time_series(t, "monthly_adjusted"),
                "monthly_adjusted",
                ["date"],
            )

    # ------------------------------------------------------------------
    # Batch operations
    # ------------------------------------------------------------------

    def save_all_fundamentals(self, tickers: list[str]) -> None:
        """Run all fundamentals + overview saves."""
        self.save_income_statement(tickers)
        self.save_balance_sheet(tickers)
        self.save_cash_flow(tickers)
        self.save_earnings(tickers)
        self.save_overview(tickers)

    def save_all(self, tickers: list[str]) -> None:
        """Run everything: fundamentals + all time-series intervals."""
        self.save_all_fundamentals(tickers)
        self.save_daily_adjusted(tickers)
        self.save_weekly_adjusted(tickers)
        self.save_monthly_adjusted(tickers)

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

    def _read_all_in_subdir(self, sub_dir_name: str) -> pl.DataFrame | None:
        """Read every ``.parquet`` in a sub-directory and concatenate."""
        sub = self._sub_dir(sub_dir_name)
        files = sorted(sub.glob("*.parquet"))
        if not files:
            return None
        dfs = [pl.read_parquet(f) for f in files]
        return pl.concat(dfs, how="diagonal_relaxed")
