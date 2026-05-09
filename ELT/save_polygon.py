from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

from ELT.extract_polygon import PolygonExtractor
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


class PolygonSaver:
    """
    Persist Polygon.io data to per-ticker Parquet files with idempotent,
    incremental updates.
    """

    def __init__(
        self,
        data_dir: str | os.PathLike = "data/parquet",
        api_key: str | None = None,
    ):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.extractor = PolygonExtractor(api_key=api_key)

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
        Fetch one ticker, merge with its existing file, and write back.

        Parameters
        ----------
        ticker
            Stock symbol.
        fetch_fn
            Callable that returns a ``pl.DataFrame``.
        sub_dir_name
            Sub-directory name (e.g. ``"daily_bars"``).
        dedupe_keys
            Columns used for deduplication (``last_fetched_at`` is appended
            automatically in ``_merge_and_dedupe``).
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
    # Public API — per-ticker files
    # ------------------------------------------------------------------

    def save_daily_bars(
        self,
        tickers: list[str],
        start_date: str,
        end_date: str,
    ) -> None:
        """Save / update daily OHLCV bars — one file per ticker."""
        for ticker in tickers:
            self._save_single_ticker(
                ticker,
                lambda t=ticker: self.extractor.get_daily_bars(t, start_date, end_date),
                "daily_bars",
                ["date"],
            )

    def save_ticker_details(self, tickers: list[str]) -> None:
        """Save / update company details — one file per ticker."""
        for ticker in tickers:
            self._save_single_ticker(
                ticker,
                lambda t=ticker: self.extractor.get_ticker_details(t),
                "ticker_details",
                ["ticker"],
            )

    def save_daily_open_close(
        self,
        tickers: list[str],
        date: str,
    ) -> None:
        """Save / update open/close snapshots — one file per ticker."""
        for ticker in tickers:
            self._save_single_ticker(
                ticker,
                lambda t=ticker: self.extractor.get_daily_open_close(t, date),
                "daily_open_close",
                ["date"],
            )

    # ------------------------------------------------------------------
    # Public API — aggregate files
    # ------------------------------------------------------------------

    def save_ticker_list(
        self,
        market: str = "stocks",
        limit: int = 2_500,
    ) -> None:
        """Save / update the full ticker list for a market."""
        path = self._sub_dir("ticker_list") / f"{market}.parquet"
        existing = self._read_existing(path)

        try:
            new_df = self.extractor.get_ticker_list(market=market, limit=limit)
        except Exception as exc:
            logger.error(f"Failed to fetch ticker list: {exc}")
            return

        if new_df is None or new_df.is_empty():
            logger.warning("No ticker list data returned")
            return

        if existing is not None:
            merged = _merge_and_dedupe(existing, new_df, ["ticker"])
        else:
            merged = new_df

        merged = merged.sort("ticker")
        self._write_parquet(merged, path)

    # ------------------------------------------------------------------
    # Batch operations
    # ------------------------------------------------------------------

    def save_all(
        self,
        tickers: list[str],
        start_date: str,
        end_date: str,
    ) -> None:
        """Run everything: ticker details + daily bars."""
        self.save_ticker_details(tickers)
        self.save_daily_bars(tickers, start_date, end_date)

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

    def _read_all_in_subdir(self, sub_dir_name: str) -> pl.DataFrame | None:
        """Read every ``.parquet`` in a sub-directory and concatenate."""
        sub = self._sub_dir(sub_dir_name)
        files = sorted(sub.glob("*.parquet"))
        if not files:
            return None
        dfs = [pl.read_parquet(f) for f in files]
        return pl.concat(dfs, how="diagonal_relaxed")
