"""Pytest fixtures for scenario tests.

We need a ``TestClient`` here just like in ``tests/contract`` — pytest
doesn't share fixtures across sibling test packages.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from thread_observability.api.http_api import create_core_app
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


@pytest.fixture()
def client(store: SQLiteStore) -> TestClient:
    return TestClient(create_core_app())
