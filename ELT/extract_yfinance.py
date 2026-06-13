from __future__ import annotations

import time
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any

import polars as pl
import yfinance as yf

from logger.logger import get_logger

logger = get_logger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _RateLimiter:
    """Simple rate limiter enforcing a minimum interval between calls."""

    def __init__(self, calls_per_minute: int) -> None:
        self.min_interval = 60.0 / calls_per_minute
        self.last_call: float | None = None

    def sleep_if_needed(self) -> None:
        if self.last_call is not None:
            elapsed = time.monotonic() - self.last_call
            if elapsed < self.min_interval:
                sleep_time = self.min_interval - elapsed
                logger.debug(f"Rate limit: sleeping {sleep_time:.2f}s")
                time.sleep(sleep_time)
        self.last_call = time.monotonic()


def _to_decimal(v: Any) -> Decimal | None:
    """Cast a primitive value to a ``Decimal(18, 6)`` or ``None``."""
    if v is None or (isinstance(v, float) and v != v):  # NaN guard
        return None
    try:
        d = Decimal(str(v))
        return d.quantize(Decimal("0.000001"))
    except (ValueError, TypeError):
        return None


def _to_int(v: Any) -> int | None:
    """Cast a primitive value to ``int`` or ``None``."""
    if v is None:
        return None
    try:
        return int(v)
    except (ValueError, TypeError):
        return None


def _to_bool(v: Any) -> bool | None:
    """Cast a primitive value to ``bool`` or ``None``."""
    if v is None:
        return None
    return bool(v)


def _to_polars_frame(df: Any) -> pl.DataFrame:
    """Convert a pandas or polars DataFrame to Polars, returning empty on None."""
    if df is None:
        return pl.DataFrame()
    if isinstance(df, pl.DataFrame):
        return df
    return pl.from_pandas(df)


def _parse_expiry(expiry: str) -> date:
    """Parse a yfinance expiry string ``YYYY-MM-DD`` to ``date``."""
    return datetime.strptime(expiry, "%Y-%m-%d").date()  # noqa: DTZ007


def _calls_puts_to_long(
    calls: pl.DataFrame,
    puts: pl.DataFrame,
    ticker: str,
    expiry: date,
    underlying_price: Decimal | None,
    snapshot_ts: datetime,
) -> pl.DataFrame:
    """
    Convert yfinance calls/puts frames into the canonical option-chain schema.

    Parameters
    ----------
    calls, puts
        Polars DataFrames from ``pl.from_pandas(t.option_chain(...).calls/puts)``.
    ticker
        Upper-case underlying ticker.
    expiry
        Expiration date.
    underlying_price
        Spot price captured at snapshot time.
    snapshot_ts
        UTC microsecond-precision timestamp for this snapshot.

    Returns
    -------
    polars.DataFrame
        One row per contract with the canonical schema.
    """
    spec_columns = [
        "underlying",
        "type",
        "expiry",
        "contract_symbol",
        "strike",
        "currency",
        "last_price",
        "bid",
        "ask",
        "change",
        "percent_change",
        "volume",
        "open_interest",
        "implied_volatility",
        "in_the_money",
        "contract_size",
        "last_trade_date",
        "underlying_price",
        "snapshot_ts",
        "snapshot_date",
        "source",
        "last_fetched_at",
    ]

    rename_map = {
        "contractSymbol": "contract_symbol",
        "lastTradeDate": "last_trade_date",
        "lastPrice": "last_price",
        "percentChange": "percent_change",
        "openInterest": "open_interest",
        "impliedVolatility": "implied_volatility",
        "inTheMoney": "in_the_money",
        "contractSize": "contract_size",
    }

    # yfinance sometimes returns empty calls or puts for an expiry; build a
    # frame with the expected source columns so the rest of the pipeline is
    # branch-free.
    expected_source_schema = {
        "contractSymbol": pl.Utf8,
        "lastTradeDate": pl.Datetime("us", "UTC"),
        "strike": pl.Float64,
        "lastPrice": pl.Float64,
        "bid": pl.Float64,
        "ask": pl.Float64,
        "change": pl.Float64,
        "percentChange": pl.Float64,
        "volume": pl.Int64,
        "openInterest": pl.Int64,
        "impliedVolatility": pl.Float64,
        "inTheMoney": pl.Boolean,
        "contractSize": pl.Utf8,
        "currency": pl.Utf8,
    }

    def _normalize(frame: pl.DataFrame, option_type: str) -> pl.DataFrame:
        if frame.is_empty():
            frame = pl.DataFrame(
                {c: [] for c in expected_source_schema},
                schema=expected_source_schema,
            )
        return frame.rename(rename_map).with_columns(
            pl.lit(option_type).alias("type")
        )

    calls = _normalize(calls, "call")
    puts = _normalize(puts, "put")

    combined = pl.concat([calls, puts], how="diagonal_relaxed")

    # Add snapshot-level audit columns
    combined = combined.with_columns(
        pl.lit(ticker.upper()).alias("underlying"),
        pl.lit(expiry).cast(pl.Date).alias("expiry"),
        pl.lit(underlying_price).cast(pl.Decimal(18, 6)).alias("underlying_price"),
        pl.lit(snapshot_ts).cast(pl.Datetime("us", "UTC")).alias("snapshot_ts"),
        pl.lit(snapshot_ts.date()).cast(pl.Date).alias("snapshot_date"),
        pl.lit("YFINANCE").alias("source"),
        pl.lit(snapshot_ts).cast(pl.Datetime("us", "UTC")).alias("last_fetched_at"),
    )

    # Ensure all spec columns exist, defaulting to null when missing
    for col in spec_columns:
        if col not in combined.columns:
            combined = combined.with_columns(pl.lit(None).alias(col))

    combined = combined.select(spec_columns)

    # Cast to final types
    combined = combined.with_columns(
        pl.col("underlying").cast(pl.Utf8),
        pl.col("type").cast(pl.Utf8),
        pl.col("expiry").cast(pl.Date),
        pl.col("contract_symbol").cast(pl.Utf8),
        pl.col("strike").map_elements(_to_decimal, return_dtype=pl.Decimal(18, 6)),
        pl.col("currency").cast(pl.Utf8),
        pl.col("last_price").map_elements(_to_decimal, return_dtype=pl.Decimal(18, 6)),
        pl.col("bid").map_elements(_to_decimal, return_dtype=pl.Decimal(18, 6)),
        pl.col("ask").map_elements(_to_decimal, return_dtype=pl.Decimal(18, 6)),
        pl.col("change").map_elements(_to_decimal, return_dtype=pl.Decimal(18, 6)),
        pl.col("percent_change").cast(pl.Float64),
        pl.col("volume").map_elements(_to_int, return_dtype=pl.Int64),
        pl.col("open_interest").map_elements(_to_int, return_dtype=pl.Int64),
        pl.col("implied_volatility").cast(pl.Float64),
        pl.col("in_the_money").map_elements(_to_bool, return_dtype=pl.Boolean),
        pl.col("contract_size").cast(pl.Utf8),
        pl.col("last_trade_date").cast(pl.Datetime("us", "UTC")),
        pl.col("underlying_price").map_elements(_to_decimal, return_dtype=pl.Decimal(18, 6)),
        pl.col("snapshot_ts").cast(pl.Datetime("us", "UTC")),
        pl.col("snapshot_date").cast(pl.Date),
        pl.col("source").cast(pl.Utf8),
        pl.col("last_fetched_at").cast(pl.Datetime("us", "UTC")),
    )

    return combined


# ---------------------------------------------------------------------------
# Extractor
# ---------------------------------------------------------------------------


class YFinanceExtractor:
    """
    Extract option-chain snapshots from Yahoo Finance via ``yfinance``.

    All returned DataFrames share the canonical option-chain schema and use
    UTC microsecond-precision timestamps for ``snapshot_ts`` / ``last_trade_date``.
    """

    def __init__(self, calls_per_minute: int = 30) -> None:
        self.rate_limiter = _RateLimiter(calls_per_minute)

    # -- low-level ---------------------------------------------------------

    def _fetch_ticker(self, ticker: str) -> yf.Ticker:
        """Return a rate-limited ``yf.Ticker``."""
        self.rate_limiter.sleep_if_needed()
        return yf.Ticker(ticker.upper())

    def _fetch_option_chain(
        self,
        ticker_obj: yf.Ticker,
        expiry: str,
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
        """Fetch one expiry's option chain and return calls/puts as Polars frames."""
        self.rate_limiter.sleep_if_needed()
        try:
            chain = ticker_obj.option_chain(expiry)
        except Exception as exc:
            logger.error(f"Failed to fetch option chain for {ticker_obj.ticker} {expiry}: {exc}")
            raise

        calls = _to_polars_frame(chain.calls)
        puts = _to_polars_frame(chain.puts)
        return calls, puts

    def _underlying_price(self, ticker_obj: yf.Ticker) -> Decimal | None:
        """Best-effort spot price; try fast_info, then info."""
        try:
            price = ticker_obj.fast_info.get("last_price")
            if price is not None:
                return _to_decimal(price)
        except Exception as exc:
            logger.debug(f"fast_info.last_price failed for {ticker_obj.ticker}: {exc}")

        try:
            price = ticker_obj.info.get("currentPrice")
            if price is not None:
                return _to_decimal(price)
        except Exception as exc:
            logger.debug(f"info.currentPrice failed for {ticker_obj.ticker}: {exc}")

        return None

    # -- option chain ------------------------------------------------------

    def get_option_chain(
        self,
        ticker: str,
        expiries: list[str] | None = None,
    ) -> pl.DataFrame:
        """
        Fetch the full option-chain snapshot for a single underlying.

        Parameters
        ----------
        ticker
            Stock symbol, e.g. ``"AAPL"``.
        expiries
            Optional list of ``"YYYY-MM-DD"`` expiry strings.  Defaults to all
            available expiries reported by yfinance.

        Returns
        -------
        polars.DataFrame
            One row per (contract, snapshot) in the canonical schema.
        """
        logger.info(f"Fetching option chain for {ticker.upper()}")
        ticker_obj = self._fetch_ticker(ticker)

        available_expiries = list(ticker_obj.options)
        if not available_expiries:
            logger.warning(f"No option expiries available for {ticker.upper()}")
            return pl.DataFrame()

        if expiries is None:
            expiries = available_expiries
        else:
            # Validate requested expiries against available ones
            missing = set(expiries) - set(available_expiries)
            if missing:
                logger.warning(
                    f"Requested expiries not available for {ticker.upper()}: {sorted(missing)}"
                )
            expiries = [e for e in expiries if e in available_expiries]

        if not expiries:
            return pl.DataFrame()

        snapshot_ts = datetime.now(timezone.utc)
        underlying_price = self._underlying_price(ticker_obj)

        frames: list[pl.DataFrame] = []
        for expiry in expiries:
            try:
                calls, puts = self._fetch_option_chain(ticker_obj, expiry)
            except Exception:
                logger.error(f"Skipping expiry {expiry} for {ticker.upper()}")
                continue

            if calls.is_empty() and puts.is_empty():
                continue

            expiry_date = _parse_expiry(expiry)
            frames.append(
                _calls_puts_to_long(
                    calls,
                    puts,
                    ticker,
                    expiry_date,
                    underlying_price,
                    snapshot_ts,
                )
            )

        if not frames:
            logger.warning(f"No option contracts returned for {ticker.upper()}")
            return pl.DataFrame()

        df = pl.concat(frames, how="diagonal_relaxed")
        # Deduplicate on natural grain, keeping latest fetch in case of overlap
        df = df.sort(["contract_symbol", "expiry", "type", "snapshot_ts"])
        df = df.unique(
            subset=["contract_symbol", "expiry", "type", "snapshot_ts"],
            keep="last",
        )

        logger.info(f"Fetched {len(df)} option contracts for {ticker.upper()}")
        return df

    def get_option_chains(
        self,
        tickers: list[str],
        expiries: list[str] | None = None,
    ) -> dict[str, pl.DataFrame | None]:
        """Fetch option-chain snapshots for multiple tickers."""
        results: dict[str, pl.DataFrame | None] = {}
        for ticker in tickers:
            try:
                results[ticker] = self.get_option_chain(ticker, expiries)
            except Exception as exc:
                logger.error(f"Failed to fetch option chain for {ticker}: {exc}")
                results[ticker] = None
        return results
