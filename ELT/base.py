"""Base saver with shared parquet I/O, deduplication, and incremental logic."""

from __future__ import annotations

import os
import tempfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import polars as pl

from logger.logger import get_logger

logger = get_logger(__name__)


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def merge_and_dedupe(
    existing: pl.DataFrame,
    new: pl.DataFrame,
    unique_keys: list[str],
) -> pl.DataFrame:
    """Concatenate *existing* and *new*, then keep the most-recent
    ``last_fetched_at`` for each unique key combination.
    """
    combined = pl.concat([existing, new], how="diagonal_relaxed")
    combined = combined.sort([*unique_keys, "last_fetched_at"])
    return combined.unique(subset=unique_keys, keep="last")


class ParquetSaver:
    """Base class for incremental, idempotent Parquet persistence.

    Subclasses provide:
    - ``data_dir`` (or accept it in ``__init__``)
    - An ``extractor`` instance
    - Public ``save_*`` methods that call ``_save_aggregate`` / ``_save_single_ticker``
    """

    def __init__(self, data_dir: str | os.PathLike = "data/parquet") -> None:
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Low-level I/O
    # ------------------------------------------------------------------

    def _read_existing(self, path: Path) -> pl.DataFrame | None:
        if not path.exists():
            return None
        try:
            return pl.read_parquet(path)
        except Exception as exc:
            logger.error(f"Failed to read existing {path}: {exc}")
            return None

    def _write_parquet(self, df: pl.DataFrame, path: Path) -> None:
        """Write *df* to *path* atomically via a temporary file."""
        path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_path = tempfile.mkstemp(
            suffix=".parquet.tmp", dir=path.parent, prefix=path.stem + "_"
        )
        os.close(fd)
        tmp = Path(tmp_path)
        try:
            df.write_parquet(tmp)
            os.replace(tmp, path)
            logger.info(f"Wrote {len(df):,} rows to {path}")
        except Exception as exc:
            logger.error(f"Failed to write {path}: {exc}")
            # Clean up temp file on failure
            if tmp.exists():
                tmp.unlink()
            raise

    def _sub_dir(self, name: str) -> Path:
        """Return (and create) a subdirectory for a data type."""
        sub = self.data_dir / name
        sub.mkdir(parents=True, exist_ok=True)
        return sub

    # ------------------------------------------------------------------
    # Incremental helpers
    # ------------------------------------------------------------------

    def _determine_start_date(
        self,
        existing: pl.DataFrame | None,
        lookback_days: int,
    ) -> str | None:
        """Return a start date ``lookback_days`` before the latest observation.

        If no data exists, return ``None`` (fetch full history).
        """
        if existing is None or existing.is_empty():
            return None
        max_date = existing["date"].max()
        # Polars .max() on a Date series returns date | None
        if max_date is None:
            return None
        assert isinstance(max_date, date)
        lookback = max_date - timedelta(days=lookback_days)
        return lookback.strftime("%Y-%m-%d")

    # ------------------------------------------------------------------
    # Generic save patterns
    # ------------------------------------------------------------------

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
            merged = merge_and_dedupe(existing, new_df, dedupe_keys)
        else:
            merged = new_df

        merged = merged.sort(dedupe_keys)
        self._write_parquet(merged, path)

    def _save_single_ticker(
        self,
        ticker: str,
        fetch_fn,
        sub_dir_name: str,
        dedupe_keys: list[str],
    ) -> None:
        """Fetch one ticker, merge with its existing file, and write back.

        Parameters
        ----------
        ticker
            Stock symbol.
        fetch_fn
            Callable that returns a ``pl.DataFrame``.
        sub_dir_name
            Sub-directory name (e.g. ``"daily_bars"``).
        dedupe_keys
            Columns used for deduplication.
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
            merged = merge_and_dedupe(existing, new_df, dedupe_keys)
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
                merged = merge_and_dedupe(existing, new_df, dedupe_keys)
            else:
                merged = new_df

            merged = merged.sort(dedupe_keys)
            self._write_parquet(merged, path)

    def _read_all_in_subdir(self, sub_dir_name: str) -> pl.DataFrame | None:
        """Read every ``.parquet`` in a sub-directory and concatenate."""
        sub = self._sub_dir(sub_dir_name)
        files = sorted(sub.glob("*.parquet"))
        if not files:
            return None
        dfs = [pl.read_parquet(f) for f in files]
        return pl.concat(dfs, how="diagonal_relaxed")
