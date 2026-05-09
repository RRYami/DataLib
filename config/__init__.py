"""Application-wide settings loaded from environment variables and .env files."""

from __future__ import annotations

from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Centralised application configuration.

    Values are read from environment variables first, then from a ``.env``
    file in the project root.  All fields have sensible defaults so the
    app works out-of-the-box in development.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",  # allow extra keys in .env without validation errors
    )

    # ------------------------------------------------------------------
    # Paths
    # ------------------------------------------------------------------
    data_dir: Path = Path("data/parquet")
    log_dir: Path = Path("logs")

    # ------------------------------------------------------------------
    # Defaults
    # ------------------------------------------------------------------
    default_lookback_days: int = 7
    log_level: str = "INFO"

    # ------------------------------------------------------------------
    # API rate limits (calls per minute)
    # ------------------------------------------------------------------
    fred_rate_limit: int = 60
    ecb_rate_limit: int = 60
    eurostat_rate_limit: int = 60
    polygon_rate_limit: int = 5
    alpha_vantage_rate_limit: int = 5

    # ------------------------------------------------------------------
    # API keys (loaded from .env)
    # ------------------------------------------------------------------
    fred_key: str | None = None
    polygon_key: str | None = None
    alpha_vantage_key: str | None = None


# Singleton instance — import this directly in modules that need config.
settings = Settings()
