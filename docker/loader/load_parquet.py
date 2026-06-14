#!/usr/bin/env python3
"""One-shot loader: Parquet files → TimescaleDB Hypertables."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import polars as pl
from sqlalchemy import Column, MetaData, Table, create_engine, text
from sqlalchemy.dialects.postgresql import insert as pg_insert

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://datadownloader:datadownloader123@postgres:5432/financial_data",
)


def get_engine():
    return create_engine(DATABASE_URL)


def ensure_hypertable(engine, table_name: str) -> None:
    """Ensure a table is registered as a hypertable."""
    with engine.connect() as conn:
        result = conn.execute(
            text("""
                SELECT 1 FROM timescaledb_information.hypertables
                WHERE hypertable_name = :table_name
            """),
            {"table_name": table_name},
        ).fetchone()

        if not result:
            # Try to convert to hypertable if it's a regular table
            conn.execute(
                text(f"""
                    SELECT create_hypertable(
                        '{table_name}',
                        'date',
                        if_not_exists => TRUE
                    );
                """)
            )
        conn.commit()


def get_db_columns(engine, table_name: str) -> list[str]:
    """Get column names from a database table."""
    with engine.connect() as conn:
        result = conn.execute(
            text("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = :table_name
                ORDER BY ordinal_position
            """),
            {"table_name": table_name},
        )
        return [row[0] for row in result]


def truncate_table(engine, table_name: str) -> None:
    """Truncate a table (fast delete all rows)."""
    with engine.connect() as conn:
        conn.execute(text(f"TRUNCATE TABLE {table_name} CASCADE"))
        conn.commit()


def load_table(df: pl.DataFrame, table_name: str, engine, clear_first: bool = True, require_date: bool = True) -> int:
    """Load a DataFrame into Postgres using Polars write_database.

    Only inserts columns that exist in both the DataFrame and the target table.
    By default, truncates the table first for a clean full load.
    """
    if df.is_empty():
        print(f"  [SKIP] {table_name}: empty DataFrame")
        return 0

    # Ensure date column exists for hypertables (can be disabled for regular tables)
    if require_date and "date" not in df.columns:
        print(f"  [SKIP] {table_name}: no 'date' column for hypertable")
        return 0

    # Get target table columns and intersect with DataFrame columns
    db_cols = get_db_columns(engine, table_name)
    if not db_cols:
        print(f"  [WARN] {table_name}: table not found in database")
        return 0

    available_cols = [c for c in db_cols if c in df.columns]
    if not available_cols:
        print(f"  [SKIP] {table_name}: no matching columns between parquet and table")
        return 0

    # Select only matching columns
    df = df.select(available_cols)
    print(f"  (matched {len(available_cols)}/{len(db_cols)} columns)")

    # Truncate before load for clean full refresh
    if clear_first:
        print(f"  [CLEAR] Truncating {table_name}...")
        truncate_table(engine, table_name)

    # For hypertables, we use 'append' mode (table is already empty after truncate)
    df.write_database(
        table_name,
        connection=engine,
        if_table_exists="append",
    )
    return len(df)


def count_rows(engine, table_name: str) -> int:
    """Count rows in a table."""
    with engine.connect() as conn:
        result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).fetchone()
        return result[0] if result else 0


# ---------------------------------------------------------------------------
# YFinance option-chain upsert (no truncate; preserve history)
# ---------------------------------------------------------------------------

# yfinance_options PK: (underlying, expiry, type, contract_symbol, snapshot_ts)
_YF_OPTIONS_PK = ["underlying", "expiry", "type", "contract_symbol", "snapshot_ts"]


def _df_to_rows(df: pl.DataFrame) -> list[dict]:
    """Convert a Polars DataFrame into a list of dicts ready for SQLAlchemy core.

    Naive datetimes are rejected — all timestamps must already carry UTC tzinfo.
    """
    rows: list[dict] = []
    for tup in df.iter_rows(named=True):
        row: dict = {}
        for k, v in tup.items():
            if v is None:
                row[k] = None
            else:
                row[k] = v
        rows.append(row)
    return rows


def upsert_yfinance_options(df: pl.DataFrame, engine) -> int:
    """Upsert option-chain rows into ``yfinance_options``.

    Uses ``INSERT ... ON CONFLICT (pk) DO UPDATE`` so historical snapshots
    are preserved and a re-run simply refreshes the latest snapshot per
    (underlying, expiry, type, contract_symbol, snapshot_ts).
    """
    if df.is_empty():
        print("  [SKIP] yfinance_options: empty DataFrame")
        return 0

    db_cols = get_db_columns(engine, "yfinance_options")
    if not db_cols:
        print("  [WARN] yfinance_options: table not found in database")
        return 0

    available_cols = [c for c in db_cols if c in df.columns]
    if not available_cols:
        print("  [SKIP] yfinance_options: no matching columns")
        return 0

    df = df.select(available_cols)
    print(f"  (matched {len(available_cols)}/{len(db_cols)} columns)")

    rows = _df_to_rows(df)
    if not rows:
        return 0

    update_cols = [c for c in available_cols if c not in _YF_OPTIONS_PK]

    yf_table = Table(
        "yfinance_options",
        MetaData(),
        *[Column(c) for c in available_cols],
    )
    stmt = pg_insert(yf_table)
    if update_cols:
        stmt = stmt.on_conflict_do_update(
            index_elements=_YF_OPTIONS_PK,
            set_={c: stmt.excluded[c] for c in update_cols},
        )
    else:
        stmt = stmt.on_conflict_do_nothing(index_elements=_YF_OPTIONS_PK)

    with engine.begin() as conn:
        conn.execute(stmt, rows)
    return len(rows)


def main() -> int:
    print("=" * 60)
    print("DataDownloader Parquet → TimescaleDB Loader")
    print("=" * 60)
    print(f"Database: {DATABASE_URL.replace('://', '://***:***@')}")
    print()

    engine = get_engine()
    base_dir = Path("/parquet")
    total_rows = 0
    total_tables = 0

    # Test connection
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        print("[OK] Database connection successful")
        print()
    except Exception as exc:
        print(f"[ERROR] Cannot connect to database: {exc}")
        return 1

    # --- FRED Yield Curves ---
    # All 4 components go into the same hypertable, so concatenate first
    fred_dir = base_dir / "yield_curve_usa"
    fred_files = [
        ("treasury_constant_maturity", "treasury_constant_maturity.parquet"),
        ("gsw_zero_coupon", "gsw_zero_coupon.parquet"),
        ("gsw_forward_rates", "gsw_forward_rates.parquet"),
        ("gsw_term_premiums", "gsw_term_premiums.parquet"),
    ]

    fred_dfs = []
    for component, file_name in fred_files:
        path = fred_dir / file_name
        if path.exists():
            print(f"[FRED] Reading {component}...")
            fred_dfs.append(pl.read_parquet(path))
        else:
            print(f"[FRED] {file_name} not found, skipping")

    if fred_dfs:
        print(f"[FRED] Concatenating {len(fred_dfs)} components into fred_yield_curves...")
        fred_combined = pl.concat(fred_dfs, how="diagonal_relaxed")
        rows = load_table(fred_combined, "fred_yield_curves", engine)
        total_rows += rows
        total_tables += 1
        print(f"  → {rows:,} rows")

    # --- ECB ---
    ecb_dir = base_dir / "ecb"
    ecb_files = [
        ("ecb_key_rates", "interest_rates.parquet"),
        ("ecb_hicp", "hicp.parquet"),
        ("ecb_monetary_aggregates", "monetary_aggregates.parquet"),
    ]

    for table, file_name in ecb_files:
        path = ecb_dir / file_name
        if path.exists():
            print(f"[ECB] Loading {table}...")
            df = pl.read_parquet(path)
            rows = load_table(df, table, engine)
            total_rows += rows
            total_tables += 1
            print(f"  → {rows:,} rows")
        else:
            print(f"[ECB] {file_name} not found, skipping")

    # --- Eurostat ---
    estat_dir = base_dir / "eurostat"
    estat_files = [
        ("eurostat_yield_curve", "euro_area_yield_curve.parquet"),
        ("eurostat_hicp", "hicp.parquet"),
        ("eurostat_gdp", "gdp.parquet"),
    ]

    for table, file_name in estat_files:
        path = estat_dir / file_name
        if path.exists():
            print(f"[EUROSTAT] Loading {table}...")
            df = pl.read_parquet(path)
            rows = load_table(df, table, engine)
            total_rows += rows
            total_tables += 1
            print(f"  → {rows:,} rows")
        else:
            print(f"[EUROSTAT] {file_name} not found, skipping")

    # --- Polygon Daily Bars ---
    bars_dir = base_dir / "daily_bars"
    if bars_dir.exists():
        files = sorted(bars_dir.glob("*.parquet"))
        if files:
            print(f"[POLYGON] Loading {len(files)} ticker files...")
            for f in files:
                df = pl.read_parquet(f)
                rows = load_table(df, "market_data_daily", engine)
                total_rows += rows
            total_tables += 1
            print(f"  → {sum(1 for _ in files)} files loaded")
    else:
        print("[POLYGON] daily_bars directory not found, skipping")

    # --- Alpha Vantage ---
    # Income Statement: regular PostgreSQL table with fiscal_date_ending
    av_dir = base_dir / "income_statement"
    if av_dir.exists():
        files = sorted(av_dir.glob("*.parquet"))
        if files:
            print(f"[ALPHA VANTAGE] Loading {len(files)} income_statement files...")
            for f in files:
                df = pl.read_parquet(f)
                # Select only known columns that exist in the DB schema
                known_cols = [
                    "ticker", "fiscal_date_ending", "report_type",
                    "reported_currency", "gross_profit", "total_revenue",
                    "net_income", "ebitda", "source", "last_fetched_at"
                ]
                available_cols = [c for c in known_cols if c in df.columns]
                if available_cols:
                    df = df.select(available_cols)
                # Load without requiring 'date' column (uses fiscal_date_ending)
                rows = load_table(df, "fundamentals_income_stmt", engine, require_date=False)
                total_rows += rows
            total_tables += 1
            print(f"  → {len(files)} files loaded")
    else:
        print("[ALPHA VANTAGE] income_statement directory not found, skipping")

    # Company Overview: regular PostgreSQL table (no date column)
    av_dir = base_dir / "overview"
    if av_dir.exists():
        files = sorted(av_dir.glob("*.parquet"))
        if files:
            print(f"[ALPHA VANTAGE] Loading {len(files)} overview files...")
            for f in files:
                df = pl.read_parquet(f)
                rows = load_table(df, "fundamentals_overview", engine, require_date=False)
                total_rows += rows
            total_tables += 1
            print(f"  → {len(files)} files loaded")
    else:
        print("[ALPHA VANTAGE] overview directory not found, skipping")

    # --- Yahoo Finance Option Chains ---
    # Per-ticker parquet files; UPSERT (no truncate) so historical
    # snapshots accumulate across runs.
    yf_dir = base_dir / "options"
    if yf_dir.exists():
        files = sorted(yf_dir.glob("*.parquet"))
        if files:
            print(f"[YFINANCE] Upserting option chains from {len(files)} ticker files...")
            total_upserted = 0
            for f in files:
                print(f"  [YFINANCE] {f.name}...")
                df = pl.read_parquet(f)
                rows = upsert_yfinance_options(df, engine)
                total_upserted += rows
                print(f"    → {rows:,} rows upserted")
            total_rows += total_upserted
            total_tables += 1
            print(f"  → {total_upserted:,} total rows across {len(files)} tickers")
        else:
            print("[YFINANCE] options/ directory is empty, skipping")
    else:
        print("[YFINANCE] options directory not found, skipping")

    # --- Summary ---
    print()
    print("=" * 60)
    print(f"Load complete!")
    print(f"  Tables loaded: {total_tables}")
    print(f"  Total rows: {total_rows:,}")
    print()

    # Show row counts per table
    print("--- Table Row Counts ---")
    all_tables = [
        "fred_yield_curves", "ecb_key_rates", "ecb_hicp",
        "ecb_monetary_aggregates", "eurostat_yield_curve",
        "eurostat_hicp", "eurostat_gdp", "market_data_daily",
        "fundamentals_income_stmt", "fundamentals_overview",
        "yfinance_options",
    ]
    for table in all_tables:
        try:
            count = count_rows(engine, table)
            print(f"  {table:<35} {count:>10,} rows")
        except Exception:
            print(f"  {table:<35} {'N/A':>10}")

    print("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
