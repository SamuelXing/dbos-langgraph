"""Worker controls for PostgreSQL integration tests."""

import asyncio
import json
import os
import time

import pytest

from .engine import execute
from .example import GRAPH, initial


def wait_for(predicate, workers, logs, message):
    deadline = time.monotonic() + 45
    while not predicate():
        for worker, log in zip(workers, logs, strict=True):
            assert worker.poll() is None, log.read_text()
        assert time.monotonic() < deadline, message
        time.sleep(0.01)


def postgres_url():
    url = os.environ.get("GRAPH_TEST_DATABASE_URL", "")
    if not url.startswith("postgresql"):
        pytest.skip("requires a dedicated PostgreSQL test database")
    return url


def worker_env(tmp_path, database):
    return {
        **os.environ,
        "DBOS_SYSTEM_DATABASE_URL": database,
        "GRAPH_AUDIT": str(tmp_path / "audit.jsonl"),
        "GRAPH_COMMITS": str(tmp_path / "commits.jsonl"),
        "GRAPH_READY": str(tmp_path / "ready"),
        "GRAPH_READY_KEY": "1:fast",
        "GRAPH_GATE_KEY": "1:slow",
        "GRAPH_GATE_RELEASE": str(tmp_path / "release"),
    }


def finish(worker, log):
    worker.wait(timeout=60)
    assert worker.returncode == 0, log.read_text()
    result = json.loads(log.read_text().strip().splitlines()[-1])["state"]
    expected = asyncio.run(execute(GRAPH, initial(4)))["state"]
    assert result == expected


def stop(workers):
    for worker in workers:
        if worker.poll() is None:
            worker.kill()
        worker.wait(timeout=10)
