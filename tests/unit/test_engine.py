"""Tests for the sync engine."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest

from drt.config.credentials import BigQueryProfile, ProfileConfig
from drt.config.models import DestinationConfig, SyncConfig, SyncOptions
from drt.destinations.base import SyncResult
from drt.engine.sync import batch, run_sync


# ---------------------------------------------------------------------------
# Fakes (prefer over MagicMock — they document the Protocol)
# ---------------------------------------------------------------------------

class FakeSource:
    def __init__(self, rows: list[dict]) -> None:
        self._rows = rows

    def extract(self, query: str, config: ProfileConfig) -> Iterator[dict]:
        yield from self._rows

    def test_connection(self, config: ProfileConfig) -> bool:
        return True


class FakeDestination:
    def __init__(self, fail_indices: set[int] | None = None) -> None:
        self.calls: list[list[dict]] = []
        self._fail_indices = fail_indices or set()

    def load(
        self,
        records: list[dict],
        config: DestinationConfig,
        sync_options: SyncOptions,
    ) -> SyncResult:
        self.calls.append(records)
        result = SyncResult()
        for i, _ in enumerate(records):
            global_idx = sum(len(c) for c in self.calls[:-1]) + i
            if global_idx in self._fail_indices:
                result.failed += 1
                result.errors.append(f"Forced failure at index {global_idx}")
            else:
                result.success += 1
        return result


def _make_profile() -> BigQueryProfile:
    return BigQueryProfile(type="bigquery", project="p", dataset="d")


def _make_sync(batch_size: int = 10, on_error: str = "fail") -> SyncConfig:
    return SyncConfig.model_validate({
        "name": "test_sync",
        "model": "ref('table')",
        "destination": {"type": "rest_api", "url": "https://example.com"},
        "sync": {"batch_size": batch_size, "on_error": on_error},
    })


# ---------------------------------------------------------------------------
# batch() helper
# ---------------------------------------------------------------------------

def test_batch_exact_multiple() -> None:
    result = list(batch(iter([1, 2, 3, 4]), 2))
    assert result == [[1, 2], [3, 4]]


def test_batch_remainder() -> None:
    result = list(batch(iter([1, 2, 3]), 2))
    assert result == [[1, 2], [3]]


def test_batch_empty() -> None:
    assert list(batch(iter([]), 10)) == []


def test_batch_single_item() -> None:
    assert list(batch(iter([42]), 5)) == [[42]]


def test_batch_larger_than_size() -> None:
    result = list(batch(iter(range(10)), 3))
    assert len(result) == 4
    assert result[-1] == [9]


# ---------------------------------------------------------------------------
# run_sync()
# ---------------------------------------------------------------------------

def test_run_sync_all_success(tmp_path: Path) -> None:
    rows = [{"id": i} for i in range(5)]
    source = FakeSource(rows)
    dest = FakeDestination()
    sync = _make_sync(batch_size=3)

    result = run_sync(sync, source, dest, _make_profile(), tmp_path)

    assert result.success == 5
    assert result.failed == 0
    assert len(dest.calls) == 2  # batches: [0,1,2] + [3,4]


def test_run_sync_dry_run(tmp_path: Path) -> None:
    rows = [{"id": i} for i in range(5)]
    source = FakeSource(rows)
    dest = FakeDestination()
    sync = _make_sync()

    result = run_sync(sync, source, dest, _make_profile(), tmp_path, dry_run=True)

    assert result.success == 5
    assert dest.calls == []  # destination never called


def test_run_sync_on_error_fail_stops(tmp_path: Path) -> None:
    rows = [{"id": i} for i in range(9)]
    source = FakeSource(rows)
    dest = FakeDestination(fail_indices={0})  # first record fails
    sync = _make_sync(batch_size=3, on_error="fail")

    result = run_sync(sync, source, dest, _make_profile(), tmp_path)

    assert result.failed > 0
    assert len(dest.calls) == 1  # stopped after first batch


def test_run_sync_on_error_skip_continues(tmp_path: Path) -> None:
    rows = [{"id": i} for i in range(6)]
    source = FakeSource(rows)
    dest = FakeDestination(fail_indices={0})
    sync = _make_sync(batch_size=3, on_error="skip")

    result = run_sync(sync, source, dest, _make_profile(), tmp_path)

    assert len(dest.calls) == 2  # both batches processed
    assert result.success == 5
    assert result.failed == 1


def test_run_sync_saves_state(tmp_path: Path) -> None:
    from drt.state.manager import StateManager

    rows = [{"id": 1}]
    source = FakeSource(rows)
    dest = FakeDestination()
    sync = _make_sync()
    state_mgr = StateManager(tmp_path)

    run_sync(sync, source, dest, _make_profile(), tmp_path, state_manager=state_mgr)

    state = state_mgr.get_last_sync("test_sync")
    assert state is not None
    assert state.status == "success"
    assert state.records_synced == 1
