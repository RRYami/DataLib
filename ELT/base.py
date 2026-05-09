"""Base saver with shared parquet I/O, deduplication, and incremental logic."""

from __future__ import annotations

import json
import os
import tempfile
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import polars as pl

from logger.logger import get_logger
from utils.validators import DataValidationError, run_validation_suite

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

    def _write_parquet(
        self,
        df: pl.DataFrame,
        path: Path,
        meta: dict[str, Any] | None = None,
    ) -> None:
        """Write *df* to *path* atomically via a temporary file.

        Also writes a ``{filename}.meta.json`` sidecar with row count,
        schema, and optional caller-provided metadata.
        """
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
            self._write_meta_sidecar(df, path, meta)
        except Exception as exc:
            logger.error(f"Failed to write {path}: {exc}")
            # Clean up temp file on failure
            if tmp.exists():
                tmp.unlink()
            raise

    def _write_meta_sidecar(
        self,
        df: pl.DataFrame,
        parquet_path: Path,
        extra_meta: dict[str, Any] | None = None,
    ) -> None:
        """Write a JSON sidecar next to the parquet file."""
        meta = {
            "parquet_file": str(parquet_path.name),
            "row_count": len(df),
            "column_count": len(df.columns),
            "columns": [
                {"name": c, "dtype": str(df.schema[c])} for c in df.columns
            ],
            "written_at": datetime.now(timezone.utc).isoformat(),
        }
        if extra_meta:
            meta.update(extra_meta)
        sidecar = parquet_path.with_suffix(".parquet.meta.json")
        try:
            with open(sidecar, "w", encoding="utf-8") as fh:
                json.dump(meta, fh, indent=2, default=str)
        except Exception as exc:
            logger.warning(f"Failed to write meta sidecar {sidecar}: {exc}")

    def _sub_dir(self, name: str) -> Path:
        """Return (and create) a subdirectory for a data type."""
        sub = self.data_dir / name
        sub.mkdir(parents=True, exist_ok=True)
        return sub

    # ------------------------------------------------------------------
    # Validation hook
    # ------------------------------------------------------------------

    def _validate(
        self,
        df: pl.DataFrame,
        context: str = "DataFrame",
    ) -> None:
        """Validate a DataFrame before it is persisted.

        Subclasses may override this to apply dataset-specific rules.
        The default implementation runs a lightweight suite:
        not-empty, required columns (``date``, ``value``), and no
        all-null critical columns.

        Raises
        ------
        DataValidationError
            If any check fails.
        """
        run_validation_suite(
            df,
            required_columns=["date"],
            not_null_columns=["date"],
            context=context,
        )

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
    ) -> bool:
        """Save a dataset that goes into a single file.

        Returns
        -------
        bool
            ``True`` if saved successfully.
        """
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
            return False

        if existing is not None:
            merged = merge_and_dedupe(existing, new_df, dedupe_keys)
        else:
            merged = new_df

        merged = merged.sort(dedupe_keys)
        try:
            self._validate(
                merged,
                context=f"{filename.replace('.parquet', '')} before write",
            )
        except DataValidationError as exc:
            logger.error(f"Validation failed for {filename}: {exc}")
            return False

        self._write_parquet(merged, path)
        return True

    def _save_single_ticker(
        self,
        ticker: str,
        fetch_fn,
        sub_dir_name: str,
        dedupe_keys: list[str],
    ) -> bool:
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

        Returns
        -------
        bool
            ``True`` if the ticker was saved successfully.
        """
        path = self._sub_dir(sub_dir_name) / f"{ticker.upper()}.parquet"
        existing = self._read_existing(path)

        try:
            new_df = fetch_fn()
        except Exception as exc:
            logger.error(f"Failed to fetch {sub_dir_name} for {ticker}: {exc}")
            return False

        if new_df is None or new_df.is_empty():
            logger.warning(f"No data returned for {ticker} {sub_dir_name}")
            return False

        if existing is not None:
            merged = merge_and_dedupe(existing, new_df, dedupe_keys)
        else:
            merged = new_df

        merged = merged.sort(dedupe_keys)
        try:
            self._validate(
                merged,
                context=f"{sub_dir_name}/{ticker.upper()} before write",
            )
        except DataValidationError as exc:
            logger.error(f"Validation failed for {ticker} {sub_dir_name}: {exc}")
            return False

        self._write_parquet(merged, path)
        return True

    def _save_per_country(
        self,
        countries: list[str],
        fetch_fn,
        sub_dir_name: str,
        dedupe_keys: list[str],
        lookback_days: int = 7,
    ) -> dict[str, bool]:
        """Save a dataset where each country gets its own file.

        Returns
        -------
        dict[str, bool]
            Mapping of country code → success status.
        """
        results: dict[str, bool] = {}
        for country in countries:
            path = self._sub_dir(sub_dir_name) / f"{country.upper()}.parquet"
            existing = self._read_existing(path)
            start_date = self._determine_start_date(existing, lookback_days)

            try:
                new_df = fetch_fn(country, start_date)
            except Exception as exc:
                logger.error(f"Failed to fetch {sub_dir_name} for {country}: {exc}")
                results[country] = False
                continue

            if new_df is None or new_df.is_empty():
                logger.warning(f"No data for {country} {sub_dir_name}")
                results[country] = False
                continue

            if existing is not None:
                merged = merge_and_dedupe(existing, new_df, dedupe_keys)
            else:
                merged = new_df

            merged = merged.sort(dedupe_keys)
            try:
                self._validate(
                    merged,
                    context=f"{sub_dir_name}/{country.upper()} before write",
                )
            except DataValidationError as exc:
                logger.error(f"Validation failed for {country} {sub_dir_name}: {exc}")
                results[country] = False
                continue

            self._write_parquet(merged, path)
            results[country] = True
        return results

    def _read_all_in_subdir(self, sub_dir_name: str) -> pl.DataFrame | None:
        """Read every ``.parquet`` in a sub-directory and concatenate."""
        sub = self._sub_dir(sub_dir_name)
        files = sorted(sub.glob("*.parquet"))
        if not files:
            return None
        dfs = [pl.read_parquet(f) for f in files]
        return pl.concat(dfs, how="diagonal_relaxed")
