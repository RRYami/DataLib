from __future__ import annotations

import json
import time
from datetime import date, datetime, timezone
from typing import Any

import polars as pl
from massive import RESTClient

from get_api_keys import get_api_key
from logger.logger import get_logger
from utils.http import retry_call

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


def _ms_to_date(ts: int) -> date:
    """Convert Polygon millisecond timestamp to a ``datetime.date``."""
    return datetime.fromtimestamp(ts / 1000, tz=timezone.utc).date()


def _flatten_obj(obj: Any) -> dict[str, Any]:
    """
    Recursively flatten a Polygon SDK object into a plain dict.

    Nested objects are JSON-serialised; ``None`` and primitives are kept
    as-is so Polars can infer clean types.
    """
    if obj is None:
        return {}

    raw: dict[str, Any] = getattr(obj, "__dict__", {})
    if not raw:
        # Fallback for objects without __dict__
        return {"value": str(obj)}

    result: dict[str, Any] = {}
    for k, v in raw.items():
        if v is None or isinstance(v, (str, int, float, bool, list)):
            result[k] = v
        elif isinstance(v, dict):
            result[k] = json.dumps(v)
        else:
            # Nested SDK object – try to flatten, otherwise stringify
            try:
                result[k] = json.dumps(v.__dict__)
            except (AttributeError, TypeError):
                result[k] = str(v)
    return result


# ---------------------------------------------------------------------------
# Extractor
# ---------------------------------------------------------------------------


class PolygonExtractor:
    """
    Extract data from the Polygon.io API into tidy Polars DataFrames.

    Handles rate limiting (default 5 calls/min for the free tier) and
    converts all responses into DataFrames with consistent metadata columns.
    """

    def __init__(self, api_key: str | None = None, calls_per_minute: int = 5) -> None:
        self.api_key = api_key or get_api_key("POLYGON_KEY")
        self.rate_limiter = _RateLimiter(calls_per_minute)
        self.client = RESTClient(self.api_key)

    # -- low-level ---------------------------------------------------------

    def _request(self, fn, *args: Any, **kwargs: Any) -> Any:
        """Execute *fn* after enforcing the rate limit, with retries."""
        self.rate_limiter.sleep_if_needed()
        return retry_call(fn, *args, retries=3, backoff_factor=1.0, **kwargs)

    # -- ticker reference --------------------------------------------------

    def get_ticker_details(self, ticker: str) -> pl.DataFrame:
        """
        Fetch company details for a single ticker.

        Returns a single-row DataFrame.
        """
        batch_ts = datetime.now(timezone.utc)
        logger.info(f"Fetching ticker details for {ticker}")

        details = self._request(self.client.get_ticker_details, ticker)
        data = _flatten_obj(details)
        data["ticker"] = ticker.upper()
        data["source"] = "POLYGON"
        data["last_fetched_at"] = batch_ts

        df = pl.DataFrame([data])
        logger.info(f"Fetched details for {ticker}")
        return df

    def get_ticker_details_batch(
        self, tickers: list[str]
    ) -> dict[str, pl.DataFrame | None]:
        """Fetch details for multiple tickers."""
        results: dict[str, pl.DataFrame | None] = {}
        for ticker in tickers:
            try:
                results[ticker] = self.get_ticker_details(ticker)
            except Exception as exc:
                logger.error(f"Failed to fetch details for {ticker}: {exc}")
                results[ticker] = None
        return results

    def get_ticker_list(
        self,
        market: str = "stocks",
        limit: int = 2_500,
    ) -> pl.DataFrame:
        """
        Fetch the list of available tickers for a market.

        Parameters
        ----------
        market
            Market type (``'stocks'``, ``'indices'``, etc.).
        limit
            Maximum tickers to retrieve.  Polygon pages at 500 per request.
        """
        batch_ts = datetime.now(timezone.utc)
        logger.info(f"Fetching ticker list for market: {market}")

        tickers = self._request(
            self.client.list_tickers,
            market=market,
            order="asc",
            limit=500,
            sort="ticker",
        )

        data: list[dict[str, Any]] = []
        page_size = 500
        max_requests = (limit + page_size - 1) // page_size
        max_tickers = max_requests * page_size

        for ticker in tickers:
            data.append(_flatten_obj(ticker))
            if len(data) >= max_tickers:
                logger.info(f"Reached limit of {max_tickers} tickers")
                break
            if len(data) % page_size == 0 and len(data) < max_tickers:
                logger.info(
                    f"Fetched {len(data)} tickers, pausing 12 s for rate limit..."
                )
                time.sleep(12)

        if not data:
            return pl.DataFrame()

        df = pl.DataFrame(data)
        df = df.with_columns(
            pl.lit("POLYGON").alias("source"),
            pl.lit(batch_ts).alias("last_fetched_at"),
        )
        logger.info(f"Fetched {len(df)} tickers for market: {market}")
        return df

    # -- aggregates (OHLCV) ------------------------------------------------

    def get_daily_bars(
        self,
        ticker: str,
        start_date: str,
        end_date: str,
    ) -> pl.DataFrame:
        """
        Fetch daily OHLCV bars for *ticker* between two dates.

        Parameters
        ----------
        ticker
            Stock symbol.
        start_date, end_date
            Inclusive date bounds (``YYYY-MM-DD``).

        Returns
        -------
        polars.DataFrame
            Columns: ``date, open, high, low, close, volume, vwap,
            transactions, ticker, source, last_fetched_at``.
        """
        batch_ts = datetime.now(timezone.utc)
        logger.info(
            f"Fetching daily bars for {ticker} ({start_date} to {end_date})"
        )

        bars = self._request(
            self.client.get_aggs,
            ticker,
            1,
            "day",
            start_date,
            end_date,
        )

        if not bars:
            logger.warning(f"No bars returned for {ticker}")
            return pl.DataFrame()

        rows: list[dict[str, Any]] = []
        for bar in bars:
            rows.append(
                {
                    "ticker": ticker.upper(),
                    "date": _ms_to_date(bar.timestamp),
                    "open": getattr(bar, "open", None),
                    "high": getattr(bar, "high", None),
                    "low": getattr(bar, "low", None),
                    "close": getattr(bar, "close", None),
                    "volume": getattr(bar, "volume", None),
                    "vwap": getattr(bar, "vwap", None),
                    "transactions": getattr(bar, "transactions", None),
                    "source": "POLYGON",
                    "last_fetched_at": batch_ts,
                }
            )

        df = pl.DataFrame(rows)
        logger.info(f"Fetched {len(df)} bars for {ticker}")
        return df

    def get_daily_bars_batch(
        self,
        tickers: list[str],
        start_date: str,
        end_date: str,
    ) -> dict[str, pl.DataFrame | None]:
        """Fetch daily bars for multiple tickers."""
        results: dict[str, pl.DataFrame | None] = {}
        for ticker in tickers:
            try:
                results[ticker] = self.get_daily_bars(ticker, start_date, end_date)
            except Exception as exc:
                logger.error(
                    f"Failed to fetch daily bars for {ticker}: {exc}"
                )
                results[ticker] = None
        return results

    # -- daily open / close ------------------------------------------------

    def get_daily_open_close(
        self,
        ticker: str,
        date: str,
    ) -> pl.DataFrame:
        """
        Fetch open/close snapshot for *ticker* on a specific date.

        Returns a single-row DataFrame.
        """
        batch_ts = datetime.now(timezone.utc)
        logger.info(f"Fetching open/close for {ticker} on {date}")

        agg = self._request(
            self.client.get_daily_open_close_agg,
            ticker,
            date,
            adjusted=True,
        )

        df = pl.DataFrame(
            [
                {
                    "ticker": ticker.upper(),
                    "date": date,
                    "open": getattr(agg, "open", None),
                    "high": getattr(agg, "high", None),
                    "low": getattr(agg, "low", None),
                    "close": getattr(agg, "close", None),
                    "volume": getattr(agg, "volume", None),
                    "vwap": getattr(agg, "vwap", None),
                    "after_hours": getattr(agg, "afterHours", None),
                    "pre_market": getattr(agg, "preMarket", None),
                    "source": "POLYGON",
                    "last_fetched_at": batch_ts,
                }
            ]
        )
        logger.info(f"Fetched open/close for {ticker}")
        return df

    def get_daily_open_close_batch(
        self,
        tickers: list[str],
        date: str,
    ) -> dict[str, pl.DataFrame | None]:
        """Fetch open/close for multiple tickers on a specific date."""
        results: dict[str, pl.DataFrame | None] = {}
        for ticker in tickers:
            try:
                results[ticker] = self.get_daily_open_close(ticker, date)
            except Exception as exc:
                logger.error(
                    f"Failed to fetch open/close for {ticker}: {exc}"
                )
                results[ticker] = None
        return results
