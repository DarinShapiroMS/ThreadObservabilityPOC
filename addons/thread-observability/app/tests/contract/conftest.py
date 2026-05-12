"""Shared fixtures for contract tests.

Contract tests assert that every public ``/v1/...`` endpoint returns a
payload conforming to the Pydantic models in
:mod:`thread_observability.api.schemas`. They use FastAPI's ``TestClient``
without entering its lifespan context, so the background pipeline does
not start and ``reset_db_on_start`` does not wipe the seeded test store.
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
    """Per-test SQLite store registered as the module-level singleton.

    Duplicates the fixture from the parent ``conftest`` so contract tests
    can run in isolation (pytest does not inherit parent ``conftest``
    fixtures across nested packages when both define them — but redefining
    here is explicit and avoids surprises).
    """
    db_path = tmp_path / "state.db"
    s = SQLiteStore(db_path=db_path)
    reset_store_for_tests(s)
    yield s
    reset_store_for_tests(None)
    s.close()


@pytest.fixture()
def client(store: SQLiteStore) -> TestClient:
    """A ``TestClient`` bound to a fresh app instance.

    We deliberately do NOT use ``with TestClient(app)`` so the lifespan
    (which starts the background pipeline and wipes the DB) does not run.
    """
    app = create_core_app()
    return TestClient(app)
