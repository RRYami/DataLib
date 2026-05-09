"""Pipeline orchestration for running ELT tasks with observability."""

from __future__ import annotations

import json
import traceback
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable

from logger.logger import get_logger

logger = get_logger(__name__)


@dataclass
class PipelineResult:
    """Result from running a pipeline."""

    ok: bool = True
    tasks_run: int = 0
    tasks_failed: int = 0
    start_time: datetime | None = None
    end_time: datetime | None = None
    task_results: dict[str, dict[str, Any]] = field(default_factory=dict)

    @property
    def duration_seconds(self) -> float | None:
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "tasks_run": self.tasks_run,
            "tasks_failed": self.tasks_failed,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration_seconds": self.duration_seconds,
            "task_results": self.task_results,
        }

    def __repr__(self) -> str:
        return (
            f"<PipelineResult ok={self.ok} "
            f"run={self.tasks_run} failed={self.tasks_failed} "
            f"duration={self.duration_seconds:.2f}s>"
            if self.duration_seconds
            else f"<PipelineResult ok={self.ok} run={self.tasks_run} failed={self.tasks_failed}>"
        )


class Pipeline:
    """Run a sequence of ELT tasks and produce a summary report.

    Example
    -------
    ::

        pipeline = Pipeline()
        pipeline.add_task("ecb", lambda: EcbSaver().save_all())
        pipeline.add_task("fred", lambda: FredSaver().save_all())
        result = pipeline.run()
        print(result.to_dict())
    """

    def __init__(self) -> None:
        self._tasks: dict[str, Callable[[], Any]] = {}

    def add_task(self, name: str, fn: Callable[[], Any]) -> None:
        """Register a task by name."""
        self._tasks[name] = fn

    def run(self) -> PipelineResult:
        """Execute all registered tasks in insertion order.

        Each task is wrapped in a try/except so that one failure does
        not stop the remaining tasks from running.

        Returns
        -------
        PipelineResult
            Structured summary of the run.
        """
        result = PipelineResult()
        result.start_time = datetime.now(timezone.utc)

        for name, fn in self._tasks.items():
            result.tasks_run += 1
            task_start = datetime.now(timezone.utc)
            try:
                fn()
                result.task_results[name] = {
                    "status": "success",
                    "started_at": task_start.isoformat(),
                    "finished_at": datetime.now(timezone.utc).isoformat(),
                }
                logger.info(f"Pipeline task '{name}' succeeded")
            except Exception as exc:
                result.tasks_failed += 1
                result.ok = False
                result.task_results[name] = {
                    "status": "failed",
                    "started_at": task_start.isoformat(),
                    "error": str(exc),
                    "traceback": traceback.format_exc(),
                }
                logger.error(f"Pipeline task '{name}' failed: {exc}")

        result.end_time = datetime.now(timezone.utc)
        self._emit_summary(result)
        return result

    def _emit_summary(self, result: PipelineResult) -> None:
        """Log a structured summary of the pipeline run."""
        summary = result.to_dict()
        if result.ok:
            logger.info(f"Pipeline completed successfully: {result}")
        else:
            logger.error(f"Pipeline completed with failures: {result}")
        logger.debug(f"Pipeline full summary: {json.dumps(summary, default=str)}")
