"""Data validation gates for extracted and loaded DataFrames."""

from __future__ import annotations

from typing import Any

import polars as pl

from logger.logger import get_logger

logger = get_logger(__name__)


class DataValidationError(Exception):
    """Raised when a DataFrame fails a validation gate."""

    def __init__(self, message: str, failures: dict[str, Any] | None = None):
        super().__init__(message)
        self.failures = failures or {}


def validate_not_empty(df: pl.DataFrame, context: str = "DataFrame") -> None:
    """Ensure the DataFrame has at least one row."""
    if df.is_empty():
        raise DataValidationError(f"{context} is empty (0 rows)")


def validate_required_columns(
    df: pl.DataFrame,
    columns: list[str],
    context: str = "DataFrame",
) -> None:
    """Ensure all *columns* are present in *df*."""
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise DataValidationError(
            f"{context} missing required columns: {missing}",
            failures={"missing_columns": missing},
        )


def validate_no_all_null_columns(
    df: pl.DataFrame,
    columns: list[str] | None = None,
    context: str = "DataFrame",
) -> None:
    """Ensure none of the specified columns are entirely null.

    If *columns* is ``None``, checks every column in *df*.
    """
    cols_to_check = columns or df.columns
    all_null = [c for c in cols_to_check if df[c].null_count() == len(df)]
    if all_null:
        raise DataValidationError(
            f"{context} has all-null columns: {all_null}",
            failures={"all_null_columns": all_null},
        )


def validate_row_count_bounds(
    df: pl.DataFrame,
    min_rows: int = 1,
    max_rows: int | None = None,
    context: str = "DataFrame",
) -> None:
    """Ensure row count is within expected bounds."""
    n = len(df)
    if n < min_rows:
        raise DataValidationError(
            f"{context} has too few rows ({n} < {min_rows})",
            failures={"row_count": n, "min_rows": min_rows},
        )
    if max_rows is not None and n > max_rows:
        raise DataValidationError(
            f"{context} has too many rows ({n} > {max_rows})",
            failures={"row_count": n, "max_rows": max_rows},
        )


def validate_date_range(
    df: pl.DataFrame,
    date_column: str = "date",
    min_date: str | None = None,
    max_date: str | None = None,
    context: str = "DataFrame",
) -> None:
    """Ensure the date column falls within the expected range."""
    if date_column not in df.columns:
        raise DataValidationError(
            f"{context} missing date column '{date_column}'",
            failures={"missing_columns": [date_column]},
        )

    col = df[date_column]
    actual_min = col.min()
    actual_max = col.max()

    failures: dict[str, Any] = {}
    if min_date is not None and actual_min is not None:
        if str(actual_min) < min_date:
            failures["min_date"] = {"expected": min_date, "actual": str(actual_min)}
    if max_date is not None and actual_max is not None:
        if str(actual_max) > max_date:
            failures["max_date"] = {"expected": max_date, "actual": str(actual_max)}

    if failures:
        raise DataValidationError(
            f"{context} date range validation failed",
            failures=failures,
        )


def run_validation_suite(
    df: pl.DataFrame,
    required_columns: list[str] | None = None,
    not_null_columns: list[str] | None = None,
    min_rows: int = 1,
    max_rows: int | None = None,
    date_column: str | None = "date",
    min_date: str | None = None,
    max_date: str | None = None,
    context: str = "DataFrame",
) -> None:
    """Run a comprehensive validation suite on a DataFrame.

    Raises ``DataValidationError`` on the first failing check.
    """
    validate_not_empty(df, context=context)
    validate_row_count_bounds(df, min_rows=min_rows, max_rows=max_rows, context=context)

    if required_columns:
        validate_required_columns(df, required_columns, context=context)

    if not_null_columns:
        validate_no_all_null_columns(df, not_null_columns, context=context)

    if date_column:
        validate_date_range(
            df,
            date_column=date_column,
            min_date=min_date,
            max_date=max_date,
            context=context,
        )

    logger.debug(f"Validation passed for {context} ({len(df):,} rows)")
