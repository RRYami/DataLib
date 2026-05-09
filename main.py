from __future__ import annotations

from config import settings  # noqa: F401  triggers .env load
from logger.logger import get_logger, setup_logging

from ELT.save_ecb import EcbSaver
from ELT.save_eurostat import EurostatSaver
from ELT.save_fred import FredSaver
from orchestrator import Pipeline

logger = get_logger(__name__)


def build_default_pipeline() -> Pipeline:
    """Build the standard daily pipeline."""
    pipeline = Pipeline()
    pipeline.add_task("ecb", lambda: EcbSaver().save_all())
    pipeline.add_task("eurostat", lambda: EurostatSaver().save_all())
    pipeline.add_task("fred", lambda: FredSaver().save_all())
    return pipeline


def main() -> None:
    setup_logging()
    pipeline = build_default_pipeline()
    result = pipeline.run()
    print(result.to_dict())
    if not result.ok:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
