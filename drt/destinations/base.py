"""Destination Protocol — the interface all destinations must implement.

Designed with Rust-compatibility in mind: clear boundaries, no magic.
"""

from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable

from drt.config.models import SyncOptions


@dataclass
class SyncResult:
    """Result of a single sync batch."""

    success: int = 0
    failed: int = 0
    skipped: int = 0
    errors: list[str] = field(default_factory=list)

    @property
    def total(self) -> int:
        return self.success + self.failed + self.skipped


@runtime_checkable
class Destination(Protocol):
    """Load records into an external service."""

    def load(  # type: ignore[empty-body]
        self,
        records: list[dict],
        config: object,  # specific config type per destination
        sync_options: SyncOptions,
    ) -> SyncResult:
        """Send a batch of records to the destination."""
        ...
