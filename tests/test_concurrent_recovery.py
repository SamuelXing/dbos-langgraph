"""Competing recovery requests with distinct PostgreSQL workers."""

import asyncio
import json
import os
import subprocess
import sys
import time
import uuid
from contextlib import ExitStack

import pytest

from tests.support.engine import execute
from tests.support.example import GRAPH, initial
from tests.support.recovery import kill_at_boundary, read_counts


def wait_until(predicate, workers, logs, description):
    deadline = time.monotonic() + 60
    while not predicate():
        for worker, log in zip(workers, logs, strict=True):
            assert worker.poll() is None, log.read_text()
        assert time.monotonic() < deadline, description
        time.sleep(0.01)


def test_competing_workers_recover_one_history(tmp_path):
    database = os.environ.get("GRAPH_TEST_DATABASE_URL", "")
    if not database.startswith("postgresql"):
        pytest.skip("competing production workers require the PostgreSQL test database")
    identity = "compete-" + uuid.uuid4().hex
    owner = "plugin-" + identity
    audit = tmp_path / "audit.jsonl"
    commits = tmp_path / "commits.jsonl"
    ready = tmp_path / "ready"
    block_key = "1:slow"
    env = {
        **os.environ,
        "DBOS_SYSTEM_DATABASE_URL": database,
        "GRAPH_AUDIT": str(audit),
        "GRAPH_COMMITS": str(commits),
        "GRAPH_READY": str(ready),
        "GRAPH_READY_KEY": "1:fast",
        "GRAPH_BLOCK_KEY": block_key,
    }
    base = [sys.executable, "-m", "tests.support.plugin_worker"]
    kill_at_boundary(
        base + ["start", "--id", identity, "--mode", "cycle", "--rounds", "4"],
        env,
        tmp_path / "initial.log",
        ready,
        audit,
        block_key,
        0,
    )
    before = read_counts(audit)
    committed = set(read_counts(commits))
    for key in ("GRAPH_READY", "GRAPH_READY_KEY", "GRAPH_BLOCK_KEY"):
        env.pop(key)
    recovery_release = tmp_path / "recover-release"
    node_release = tmp_path / "node-release"
    env.update(
        GRAPH_RECOVER_OWNER=owner,
        GRAPH_RECOVER_RELEASE=str(recovery_release),
        GRAPH_GATE_KEY=block_key,
        GRAPH_GATE_RELEASE=str(node_release),
    )
    workers, logs, ready_paths, done_paths = [], [], [], []
    with ExitStack() as stack:
        try:
            for index in range(2):
                log_path = tmp_path / f"worker-{index}.log"
                ready_path = tmp_path / f"worker-{index}.ready"
                done_path = tmp_path / f"worker-{index}.done"
                worker_env = {
                    **env,
                    "GRAPH_EXECUTOR_ID": f"{identity}-worker-{index}",
                    "GRAPH_RECOVER_READY": str(ready_path),
                    "GRAPH_RECOVER_DONE": str(done_path),
                }
                log = stack.enter_context(log_path.open("w"))
                workers.append(
                    subprocess.Popen(
                        base + ["recover", "--id", identity],
                        env=worker_env,
                        stdout=log,
                        stderr=subprocess.STDOUT,
                    )
                )
                logs.append(log_path)
                ready_paths.append(ready_path)
                done_paths.append(done_path)
            wait_until(
                lambda: all(path.exists() for path in ready_paths),
                workers,
                logs,
                "workers did not reach the recovery barrier",
            )
            recovery_release.write_text("request recovery")
            wait_until(
                lambda: (
                    all(path.exists() for path in done_paths) and read_counts(audit)[block_key] >= 2
                ),
                workers,
                logs,
                "recovery requests did not reach the blocked body",
            )
            node_release.write_text("finish node")
            for worker, log_path in zip(workers, logs, strict=True):
                worker.wait(timeout=90)
                assert worker.returncode == 0, log_path.read_text()
                state = json.loads(log_path.read_text().strip().splitlines()[-1])["state"]
                assert state == asyncio.run(execute(GRAPH, initial(4)))["state"]
        finally:
            for worker in workers:
                if worker.poll() is None:
                    worker.kill()
                worker.wait(timeout=10)
    counts = read_counts(audit)
    assert counts[block_key] == 2, counts
    assert all(counts[key] == 1 for key in committed), counts
    assert all(count <= before[key] + 1 for key, count in counts.items()), counts
    assert len(counts) == 16
