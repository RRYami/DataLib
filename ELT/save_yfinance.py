from __future__ import annotations

import os

import polars as pl

from ELT.base import ParquetSaver
from ELT.extract_yfinance import YFinanceExtractor
from logger.logger import get_logger
from utils.results import SaveResult
from utils.validators import (
    DataValidationError,
    run_validation_suite,
    validate_required_columns,
)

logger = get_logger(__name__)


class YFinanceSaver(ParquetSaver):
    """
    Persist Yahoo Finance option-chain snapshots to per-ticker Parquet files.

    Directory layout
    ----------------
    ::

        data/parquet/options/
        ├── AAPL.parquet
        ├── MSFT.parquet
        └── ...

    Each file stores every historical snapshot for that underlying,
    one row per (contract_symbol, expiry, type, snapshot_ts).
    """

    DEFAULT_TICKERS = ["AAPL", "MSFT", "NVDA", "SPY"]

    def __init__(
        self,
        data_dir: str | os.PathLike = "data/parquet",
        calls_per_minute: int = 30,
    ) -> None:
        super().__init__(data_dir)
        self.extractor = YFinanceExtractor(calls_per_minute=calls_per_minute)

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def _validate(self, df: pl.DataFrame, context: str = "DataFrame") -> None:
        """Validate an option-chain DataFrame before persistence."""
        if df.is_empty():
            raise DataValidationError(f"{context} is empty (0 rows)")

        required = [
            "underlying",
            "type",
            "expiry",
            "contract_symbol",
            "strike",
            "snapshot_ts",
            "snapshot_date",
        ]
        validate_required_columns(df, required, context=context)

        # Ensure natural key columns are not entirely null
        validate_required_columns(df, required, context=context)

        run_validation_suite(
            df,
            required_columns=required,
            not_null_columns=[
                "underlying",
                "type",
                "expiry",
                "contract_symbol",
                "strike",
                "snapshot_ts",
                "snapshot_date",
            ],
            date_column="snapshot_date",
            context=context,
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def save_option_chains(
        self,
        tickers: list[str] | None = None,
        expiries: list[str] | None = None,
    ) -> SaveResult:
        """
        Fetch and save option-chain snapshots for one or more tickers.

        Parameters
        ----------
        tickers
            List of underlying tickers. Defaults to ``DEFAULT_TICKERS``.
        expiries
            Optional list of ``"YYYY-MM-DD"`` expiry strings. Defaults to
            all available expiries from yfinance.

        Returns
        -------
        SaveResult
            Structured summary of which tickers succeeded / failed.
        """
        tickers = tickers or self.DEFAULT_TICKERS
        result = SaveResult()

        for ticker in tickers:
            ok = self._save_single_ticker(
                ticker,
                lambda tk=ticker: self.extractor.get_option_chain(tk, expiries),
                "options",
                ["contract_symbol", "expiry", "type", "snapshot_ts"],
            )
            if ok:
                result.add_saved(ticker)
            else:
                result.add_failed(ticker, "fetch or validation failed")

        return result

    # ------------------------------------------------------------------
    # Read helpers
    # ------------------------------------------------------------------

    def read_option_chains(self, ticker: str) -> pl.DataFrame | None:
        """Read saved option-chain snapshots for a single underlying."""
        path = self._sub_dir("options") / f"{ticker.upper()}.parquet"
        return self._read_existing(path)

    def read_all_option_chains(self) -> pl.DataFrame | None:
        """Read and concatenate option-chain files for all saved tickers."""
        return self._read_all_in_subdir("options")
