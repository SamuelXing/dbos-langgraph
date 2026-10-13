"""Stored results survive a missing commit acknowledgment."""

import subprocess
import sys
import time
import uuid

import psycopg
import pytest

from tests.support.commit_ack_proxy import CommitAckProxy
from tests.support.postgres import finish, postgres_url, stop, wait_for, worker_env
from tests.support.recovery import read_counts


@pytest.mark.parametrize("crash_after_commit", [False, True])
def test_lost_commit_acknowledgment_reuses_stored_result(tmp_path, crash_after_commit):
    database = postgres_url()
    identity = "commit-ack-" + uuid.uuid4().hex
    step_name = "dbos_langgraph.acceptance-cycle.slow.step"
    base = [sys.executable, "-m", "tests.support.plugin_worker"]
    log_path = tmp_path / "worker.log"
    workers = []
    with CommitAckProxy(database, step_name) as proxy, log_path.open("w") as log:
        env = worker_env(tmp_path, proxy.database_url)
        worker = subprocess.Popen(
            base + ["start", "--id", identity, "--mode", "cycle", "--rounds", "4"],
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        workers.append(worker)
        try:
            wait_for(
                lambda: (
                    (tmp_path / "ready").exists()
                    and read_counts(tmp_path / "audit.jsonl")["1:slow"] == 1
                ),
                [worker],
                [log_path],
                "node did not reach the acknowledgment boundary",
            )
            proxy.armed.set()
            (tmp_path / "release").write_text("return result")
            wait_for(
                proxy.commit_intercepted.is_set,
                [worker],
                [log_path],
                "target commit was not intercepted",
            )
            deadline = time.monotonic() + 10
            with psycopg.connect(database, autocommit=True) as admin:
                while True:
                    outputs = admin.execute(
                        "SELECT count(*) FROM dbos.operation_outputs AS o "
                        "JOIN dbos.workflow_status AS w ON w.workflow_uuid = o.workflow_uuid "
                        "WHERE w.parent_workflow_id = %s AND o.function_name = %s "
                        "AND o.output IS NOT NULL",
                        (identity, step_name),
                    ).fetchone()[0]
                    if outputs == 1:
                        break
                    assert time.monotonic() < deadline, "commit was not visible to another session"
                    assert worker.poll() is None, log_path.read_text()
                    time.sleep(0.01)
                assert admin.execute(
                    "SELECT status FROM dbos.workflow_status WHERE workflow_uuid = %s",
                    (identity,),
                ).fetchone() == ("PENDING",)
            wait_for(
                proxy.response_held.is_set,
                [worker],
                [log_path],
                "server response was not held by the relay",
            )
            if crash_after_commit:
                stop([worker])
            proxy.drop_acknowledgment.set()
            if crash_after_commit:
                for key in (
                    "GRAPH_READY",
                    "GRAPH_READY_KEY",
                    "GRAPH_GATE_KEY",
                    "GRAPH_GATE_RELEASE",
                ):
                    env.pop(key)
                recovery_log = tmp_path / "recovery.log"
                with recovery_log.open("w") as stream:
                    recovered = subprocess.Popen(
                        base + ["recover", "--id", identity],
                        env=env,
                        stdout=stream,
                        stderr=subprocess.STDOUT,
                    )
                    workers.append(recovered)
                    finish(recovered, recovery_log)
            else:
                finish(worker, log_path)
        finally:
            proxy.drop_acknowledgment.set()
            stop(workers)
    counts = read_counts(tmp_path / "audit.jsonl")
    assert len(counts) == 16
    assert all(count == 1 for count in counts.values()), counts
