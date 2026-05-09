"""Result types for batch operations."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass
class SaveResult:
    """Structured result from a batch save operation."""

    saved: list[str] = field(default_factory=list)
    failed: list[tuple[str, str]] = field(default_factory=list)
    skipped: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        """Return ``True`` if nothing failed."""
        return not self.failed

    @property
    def total(self) -> int:
        """Total number of items processed."""
        return len(self.saved) + len(self.failed) + len(self.skipped)

    def add_saved(self, item: str) -> None:
        self.saved.append(item)

    def add_failed(self, item: str, reason: str) -> None:
        self.failed.append((item, reason))

    def add_skipped(self, item: str) -> None:
        self.skipped.append(item)

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "total": self.total,
            "saved": self.saved,
            "failed": [{"item": i, "reason": r} for i, r in self.failed],
            "skipped": self.skipped,
        }

    def __repr__(self) -> str:
        return (
            f"<{self.__class__.__name__} "
            f"saved={len(self.saved)} failed={len(self.failed)} skipped={len(self.skipped)}>"
        )
