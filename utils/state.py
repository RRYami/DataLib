"""Persistent state tracking for pipeline runs.

Tracks ``last_successful_fetch`` per (source, dataset, symbol) so failed
items can be retried selectively without re-running the entire batch.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from logger.logger import get_logger

logger = get_logger(__name__)


class StateTracker:
    """Simple JSONL-backed state tracker.

    Each line is a JSON object with keys:
    ``source``, ``dataset``, ``symbol``, ``status``, ``timestamp``.
    """

    def __init__(self, state_dir: str | os.PathLike = "data/state") -> None:
        self.state_dir = Path(state_dir)
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self._state_file = self.state_dir / "fetch_state.jsonl"

    def record(
        self,
        source: str,
        dataset: str,
        symbol: str,
        status: str,
    ) -> None:
        """Append a state record."""
        record = {
            "source": source,
            "dataset": dataset,
            "symbol": symbol,
            "status": status,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        try:
            with open(self._state_file, "a", encoding="utf-8") as fh:
                fh.write(json.dumps(record, default=str) + "\n")
        except Exception as exc:
            logger.warning(f"Failed to write state record: {exc}")

    def get_last_status(
        self,
        source: str,
        dataset: str,
        symbol: str,
    ) -> dict[str, Any] | None:
        """Return the most recent state record for a key, or ``None``."""
        if not self._state_file.exists():
            return None
        latest: dict[str, Any] | None = None
        try:
            with open(self._state_file, "r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rec = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if (
                        rec.get("source") == source
                        and rec.get("dataset") == dataset
                        and rec.get("symbol") == symbol
                    ):
                        latest = rec
        except Exception as exc:
            logger.warning(f"Failed to read state file: {exc}")
        return latest

    def get_failed_symbols(
        self,
        source: str,
        dataset: str,
    ) -> list[str]:
        """Return symbols whose *most recent* status is ``failed``."""
        # Read all records, keep the latest per symbol
        per_symbol: dict[str, dict[str, Any]] = {}
        if not self._state_file.exists():
            return []
        try:
            with open(self._state_file, "r", encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rec = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if (
                        rec.get("source") == source
                        and rec.get("dataset") == dataset
                    ):
                        key = rec.get("symbol", "")
                        per_symbol[key] = rec
        except Exception as exc:
            logger.warning(f"Failed to read state file: {exc}")
        return [
            sym for sym, rec in per_symbol.items()
            if rec.get("status") == "failed"
        ]

    def reset(self) -> None:
        """Clear all state (useful for testing)."""
        if self._state_file.exists():
            self._state_file.unlink()
