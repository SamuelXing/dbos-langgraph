"""A returned node result waits for database connectivity before commitment."""

import subprocess
import sys
import time
import uuid

import psycopg
import pytest

from tests.support.postgres import finish, postgres_url, stop, wait_for, worker_env
from tests.support.postgres_proxy import PostgresProxy
from tests.support.recovery import read_counts


@pytest.mark.parametrize("crash_during_outage", [False, True])
def test_result_commit_waits_through_outage(tmp_path, crash_during_outage):
    database = postgres_url()
    identity = "commit-outage-" + uuid.uuid4().hex
    log_path = tmp_path / "worker.log"
    body_finished = tmp_path / "body-finished"
    base = [sys.executable, "-m", "tests.support.plugin_worker"]
    workers = []
    with PostgresProxy(database) as proxy, log_path.open("w") as log:
        env = worker_env(tmp_path, proxy.database_url)
        env["GRAPH_BODY_FINISHED"] = str(body_finished)
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
                "node did not reach the commit boundary",
            )
            proxy.interrupt()
            (tmp_path / "release").write_text("return node output")
            wait_for(
                lambda: (
                    body_finished.exists() and "Database connection failed" in log_path.read_text()
                ),
                [worker],
                [log_path],
                "commit outage was not observed",
            )
            # Keep new connections unavailable after the body returns. The server
            # remains reachable by this separate inspection connection.
            deadline = time.monotonic() + 3
            with psycopg.connect(database, autocommit=True) as admin:
                while time.monotonic() < deadline:
                    assert worker.poll() is None, log_path.read_text()
                    status = admin.execute(
                        "SELECT status FROM dbos.workflow_status WHERE workflow_uuid = %s",
                        (identity,),
                    ).fetchone()
                    assert status == ("PENDING",)
                    outputs = admin.execute(
                        "SELECT count(*) FROM dbos.operation_outputs AS o "
                        "JOIN dbos.workflow_status AS w ON w.workflow_uuid = o.workflow_uuid "
                        "WHERE w.parent_workflow_id = %s AND o.function_name = %s "
                        "AND o.output IS NOT NULL",
                        (identity, "dbos_langgraph.acceptance-cycle.slow.step"),
                    ).fetchone()[0]
                    assert outputs == 0, "node output committed while proxy was disconnected"
                    assert read_counts(tmp_path / "audit.jsonl")["1:slow"] == 1
                    time.sleep(0.05)
            if crash_during_outage:
                stop([worker])
            proxy.restore()
            if crash_during_outage:
                for key in (
                    "GRAPH_READY",
                    "GRAPH_READY_KEY",
                    "GRAPH_GATE_KEY",
                    "GRAPH_GATE_RELEASE",
                    "GRAPH_BODY_FINISHED",
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
            proxy.restore()
            stop(workers)
    counts = read_counts(tmp_path / "audit.jsonl")
    assert counts["1:fast"] == 1
    assert counts["1:slow"] == (2 if crash_during_outage else 1)
    assert len(counts) == 16
    for key, count in counts.items():
        expected_count = 2 if crash_during_outage and key == "1:slow" else 1
        assert count == expected_count, counts
