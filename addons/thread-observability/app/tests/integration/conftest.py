"""Fixtures for integration tests."""

from __future__ import annotations

from pathlib import Path

import pytest

from thread_observability.storage.sqlite_store import (
    SQLiteStore,
    reset_store_for_tests,
)


@pytest.fixture()
def store(tmp_path: Path) -> SQLiteStore:
    db_path = tmp_path / "state.db"
    s = SQLiteStore(db_path=db_path)
    reset_store_for_tests(s)
    yield s
    reset_store_for_tests(None)
    s.close()
