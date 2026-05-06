from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any

import polars as pl
import requests

from get_api_keys import get_api_key
from logger.logger import get_logger, setup_logging

setup_logging()
logger = get_logger(__name__)

BASE_URL = "https://www.alphavantage.co/query"

# ---------------------------------------------------------------------------
# Endpoint configurations
# ---------------------------------------------------------------------------
_STATEMENT_ENDPOINTS = {
    "income_statement": "INCOME_STATEMENT",
    "balance_sheet": "BALANCE_SHEET",
    "cash_flow": "CASH_FLOW",
    "earnings": "EARNINGS",
}

_TIME_SERIES_ENDPOINTS = {
    "daily_adjusted": "TIME_SERIES_DAILY_ADJUSTED",
    "weekly_adjusted": "TIME_SERIES_WEEKLY_ADJUSTED",
    "monthly_adjusted": "TIME_SERIES_MONTHLY_ADJUSTED",
}

_OVERVIEW_ENDPOINT = "OVERVIEW"

# Keys that are already handled explicitly in the row builder
_SKIP_KEYS = {"fiscalDateEnding", "reportedCurrency"}


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


def _snake_case(s: str) -> str:
    """Convert PascalCase / camelCase to snake_case.

    Inserts an underscore only on a lowercase -> uppercase transition so
    consecutive capitals stay grouped (e.g. ``reportedEPS`` → ``reported_eps``).
    """
    result: list[str] = []
    for i, ch in enumerate(s):
        if ch.isupper() and i > 0 and s[i - 1].islower():
            result.append("_")
        result.append(ch.lower())
    return "".join(result)


def _try_float(v: Any) -> float | Any:
    """Attempt to cast *v* to ``float``; return *v* unchanged on failure."""
    if v in (None, "None", "", "null", "NULL"):
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return v


def _parse_fundamentals(response: dict, ticker: str, statement: str) -> pl.DataFrame:
    """Parse an Alpha Vantage fundamentals response into a tidy DataFrame."""
    batch_ts = datetime.now(timezone.utc)
    rows: list[dict[str, Any]] = []

    # Earnings uses ``annualEarnings`` / ``quarterlyEarnings``;
    # everything else uses ``annualReports`` / ``quarterlyReports``.
    if statement == "earnings":
        sections = [("annual", "annualEarnings"), ("quarterly", "quarterlyEarnings")]
    else:
        sections = [("annual", "annualReports"), ("quarterly", "quarterlyReports")]

    for report_type, key in sections:
        reports = response.get(key, [])
        if not reports:
            continue
        for report in reports:
            row: dict[str, Any] = {
                "ticker": ticker,
                "report_type": report_type,
                "fiscal_date_ending": report.get("fiscalDateEnding"),
                "reported_currency": report.get("reportedCurrency", "USD"),
                "source": "ALPHA_VANTAGE",
                "last_fetched_at": batch_ts,
            }
            for k, v in report.items():
                if k in _SKIP_KEYS:
                    continue
                row[_snake_case(k)] = _try_float(v)
            rows.append(row)

    if not rows:
        return pl.DataFrame()

    df = pl.DataFrame(rows)
    if "fiscal_date_ending" in df.columns:
        df = df.with_columns(
            pl.col("fiscal_date_ending").str.to_date("%Y-%m-%d", strict=False)
        )
    return df


def _parse_overview(response: dict, ticker: str) -> pl.DataFrame:
    """Parse an Overview response into a single-row DataFrame."""
    batch_ts = datetime.now(timezone.utc)

    if not response or "Symbol" not in response:
        return pl.DataFrame()

    row: dict[str, Any] = {
        "ticker": ticker,
        "source": "ALPHA_VANTAGE",
        "last_fetched_at": batch_ts,
    }
    for k, v in response.items():
        if k == "Symbol":
            continue
        row[_snake_case(k)] = _try_float(v)

    return pl.DataFrame([row])


def _parse_time_series(response: dict, ticker: str, interval: str) -> pl.DataFrame:
    """Parse a time-series response into a tidy DataFrame."""
    batch_ts = datetime.now(timezone.utc)

    # Find the time-series block key (e.g. "Time Series (Daily)")
    ts_key = next((k for k in response if k.startswith("Time Series")), None)
    if ts_key is None:
        logger.warning(f"No time-series data found for {ticker}")
        return pl.DataFrame()

    data = response[ts_key]
    rows: list[dict[str, Any]] = []

    for date_str, values in data.items():
        row: dict[str, Any] = {
            "ticker": ticker,
            "date": date_str,
            "interval": interval,
            "source": "ALPHA_VANTAGE",
            "last_fetched_at": batch_ts,
        }
        for k, v in values.items():
            # Keys look like "1. open", "5. adjusted close", etc.
            label = k.split(". ", 1)[1] if ". " in k else k
            row[_snake_case(label)] = _try_float(v)
        rows.append(row)

    if not rows:
        return pl.DataFrame()

    df = pl.DataFrame(rows)
    if "date" in df.columns:
        df = df.with_columns(pl.col("date").str.to_date("%Y-%m-%d"))
    return df


# ---------------------------------------------------------------------------
# Extractor
# ---------------------------------------------------------------------------


class AlphaVantageExtractor:
    """
    Extract data from the Alpha Vantage API.

    Handles rate limiting (default 5 calls/min for the free tier),
    automatic retries with exponential backoff, and converts all responses
    into tidy Polars DataFrames.
    """

    def __init__(self, api_key: str | None = None, calls_per_minute: int = 5) -> None:
        self.api_key = api_key or get_api_key("ALPHA_VANTAGE_KEY")
        self.rate_limiter = _RateLimiter(calls_per_minute)
        self.session = requests.Session()

    # -- low-level ---------------------------------------------------------

    def _request(self, params: dict) -> dict:
        """Make a rate-limited GET request with retries."""
        self.rate_limiter.sleep_if_needed()

        max_retries = 3
        for attempt in range(max_retries):
            try:
                response = self.session.get(BASE_URL, params=params, timeout=60)
                response.raise_for_status()
                data = response.json()

                # Alpha Vantage embeds some errors inside JSON
                if "Error Message" in data:
                    raise ValueError(data["Error Message"])
                if "Note" in data and "API call frequency" in data["Note"]:
                    raise RuntimeError(f"Rate-limit note from AV: {data['Note']}")
                if "Information" in data and "higher API call frequency" in str(
                    data["Information"]
                ):
                    raise RuntimeError(f"Rate-limit info from AV: {data['Information']}")

                return data
            except (requests.RequestException, ValueError, RuntimeError) as exc:
                if attempt < max_retries - 1:
                    sleep_time = 15 * (attempt + 1)
                    logger.warning(
                        f"Request failed (attempt {attempt + 1}/{max_retries}): {exc}. "
                        f"Retrying in {sleep_time}s..."
                    )
                    time.sleep(sleep_time)
                else:
                    logger.error(f"Request failed after {max_retries} attempts: {exc}")
                    raise

        # Unreachable — every path either returns or raises inside the loop.
        raise RuntimeError("Unexpected exit from retry loop")

    # -- statements --------------------------------------------------------

    def get_statement(self, ticker: str, statement: str) -> pl.DataFrame:
        """
        Fetch a single financial statement for *ticker*.

        Parameters
        ----------
        ticker
            Stock symbol, e.g. ``"AAPL"``.
        statement
            One of ``"income_statement"``, ``"balance_sheet"``,
            ``"cash_flow"``, ``"earnings"``.

        Returns
        -------
        polars.DataFrame
            One row per report (annual + quarterly), with all monetary
            fields cast to ``Float64``.
        """
        endpoint = _STATEMENT_ENDPOINTS.get(statement)
        if endpoint is None:
            raise ValueError(
                f"Unknown statement: {statement!r}. "
                f"Must be one of {list(_STATEMENT_ENDPOINTS)}"
            )

        params = {"function": endpoint, "symbol": ticker, "apikey": self.api_key}
        logger.info(f"Fetching {statement} for {ticker}")
        data = self._request(params)
        df = _parse_fundamentals(data, ticker, statement)
        logger.info(f"Fetched {len(df)} reports for {ticker} {statement}")
        return df

    def get_fundamentals(
        self,
        tickers: list[str],
        statements: list[str] | str = "ALL",
    ) -> dict[str, dict[str, pl.DataFrame | None]]:
        """
        Fetch fundamentals for multiple tickers and statements.

        Parameters
        ----------
        tickers
            List of ticker symbols.
        statements
            ``"ALL"`` fetches every statement type, or pass a list such as
            ``["income_statement", "balance_sheet"]``.

        Returns
        -------
        dict
            ``{ticker: {statement: DataFrame | None}}`` — ``None`` means the
            call failed for that ticker-statement pair.
        """
        if statements == "ALL":
            statements = list(_STATEMENT_ENDPOINTS.keys())
        elif isinstance(statements, str):
            statements = [statements]

        results: dict[str, dict[str, pl.DataFrame | None]] = {}
        for ticker in tickers:
            results[ticker] = {}
            for statement in statements:
                try:
                    df = self.get_statement(ticker, statement)
                    results[ticker][statement] = df
                except Exception as exc:
                    logger.error(
                        f"Failed to fetch {statement} for {ticker}: {exc}"
                    )
                    results[ticker][statement] = None
        return results

    # -- overview ----------------------------------------------------------

    def get_overview(self, ticker: str) -> pl.DataFrame:
        """
        Fetch company overview for *ticker*.

        Returns a single-row DataFrame.
        """
        params = {
            "function": _OVERVIEW_ENDPOINT,
            "symbol": ticker,
            "apikey": self.api_key,
        }
        logger.info(f"Fetching overview for {ticker}")
        data = self._request(params)
        df = _parse_overview(data, ticker)
        logger.info(f"Fetched overview for {ticker}")
        return df

    def get_overviews(self, tickers: list[str]) -> dict[str, pl.DataFrame | None]:
        """Fetch overviews for multiple tickers."""
        results: dict[str, pl.DataFrame | None] = {}
        for ticker in tickers:
            try:
                results[ticker] = self.get_overview(ticker)
            except Exception as exc:
                logger.error(f"Failed to fetch overview for {ticker}: {exc}")
                results[ticker] = None
        return results

    # -- time series -------------------------------------------------------

    def get_time_series(self, ticker: str, interval: str = "daily_adjusted") -> pl.DataFrame:
        """
        Fetch time-series data for *ticker*.

        Parameters
        ----------
        ticker
            Stock symbol.
        interval
            One of ``"daily_adjusted"``, ``"weekly_adjusted"``,
            ``"monthly_adjusted"``.
        """
        endpoint = _TIME_SERIES_ENDPOINTS.get(interval)
        if endpoint is None:
            raise ValueError(
                f"Unknown interval: {interval!r}. "
                f"Must be one of {list(_TIME_SERIES_ENDPOINTS)}"
            )

        params = {
            "function": endpoint,
            "symbol": ticker,
            "apikey": self.api_key,
        }
        logger.info(f"Fetching {interval} time series for {ticker}")
        data = self._request(params)
        df = _parse_time_series(data, ticker, interval)
        logger.info(f"Fetched {len(df)} observations for {ticker} {interval}")
        return df

    def get_time_series_batch(
        self,
        tickers: list[str],
        interval: str = "daily_adjusted",
    ) -> dict[str, pl.DataFrame | None]:
        """Fetch time series for multiple tickers."""
        results: dict[str, pl.DataFrame | None] = {}
        for ticker in tickers:
            try:
                results[ticker] = self.get_time_series(ticker, interval)
            except Exception as exc:
                logger.error(f"Failed to fetch {interval} for {ticker}: {exc}")
                results[ticker] = None
        return results
