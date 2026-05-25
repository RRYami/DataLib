from __future__ import annotations

from config import settings  # noqa: F401  triggers .env load
from ELT.save_alpha_vantage import AlphaVantageSaver
from ELT.save_ecb import EcbSaver
from ELT.save_eurostat import EurostatSaver
from ELT.save_fred import FredSaver
from ELT.save_polygon import PolygonSaver
from logger.logger import get_logger, setup_logging
from orchestrator import Pipeline

logger = get_logger(__name__)


def build_default_yield_pipeline() -> Pipeline:
    """Build the standard daily pipeline."""
    pipeline = Pipeline()
    pipeline.add_task("ecb", lambda: EcbSaver().save_all())
    pipeline.add_task("eurostat", lambda: EurostatSaver().save_all())
    pipeline.add_task("fred", lambda: FredSaver().save_all())
    return pipeline


def build_default_alpha_vantage_pipeline() -> Pipeline:
    """Build the standard daily pipeline."""
    pipeline = Pipeline()
    pipeline.add_task(
        "alpha_vantage", lambda: AlphaVantageSaver().save_daily_adjusted(["NVDA"])
    )
    return pipeline


def build_default_polygon_pipeline() -> Pipeline:
    """Build the standard daily pipeline."""
    pipeline = Pipeline()
    pipeline.add_task(
        "polygon_nvda",
        lambda: PolygonSaver().save_daily_bars(["META"], "2022-01-01", "2024-05-20"),
    )
    pipeline.add_task(
        "polygon_amd",
        lambda: PolygonSaver().save_daily_bars(["GOOGL"], "2022-01-01", "2024-05-20"),
    )
    return pipeline


def run_yield_pipeline() -> None:
    setup_logging()
    pipeline = build_default_yield_pipeline()
    result = pipeline.run()
    print(result.to_dict())
    if not result.ok:
        raise SystemExit(1)


def run_alpha_vantage_pipeline() -> None:
    setup_logging()
    pipeline = build_default_alpha_vantage_pipeline()
    result = pipeline.run()
    print(result.to_dict())
    if not result.ok:
        raise SystemExit(1)


def run_polygon_pipeline() -> None:
    setup_logging()
    pipeline = build_default_polygon_pipeline()
    result = pipeline.run()
    print(result.to_dict())
    if not result.ok:
        raise SystemExit(1)


if __name__ == "__main__":
    run_polygon_pipeline()
