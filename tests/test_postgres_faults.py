"""PostgreSQL fault probes scoped to this test's connections and histories."""

import subprocess
import sys
import uuid
from contextlib import ExitStack

import psycopg

from tests.support.postgres import finish, postgres_url, stop, wait_for, worker_env
from tests.support.recovery import read_counts


def test_terminated_connections_reconnect_without_reexecuting_body(tmp_path):
    database = postgres_url()
    identity = "disconnect-" + uuid.uuid4().hex
    env = worker_env(tmp_path, database)
    env["GRAPH_DB_APPLICATION_NAME"] = identity
    log_path = tmp_path / "worker.log"
    command = [
        sys.executable,
        "-m",
        "tests.support.plugin_worker",
        "start",
        "--id",
        identity,
        "--mode",
        "cycle",
        "--rounds",
        "4",
    ]
    with log_path.open("w") as log:
        worker = subprocess.Popen(command, env=env, stdout=log, stderr=subprocess.STDOUT)
        try:
            wait_for(
                lambda: (
                    (tmp_path / "ready").exists()
                    and read_counts(tmp_path / "audit.jsonl")["1:slow"] == 1
                ),
                [worker],
                [log_path],
                "node did not reach the connection-fault boundary",
            )
            with psycopg.connect(database, autocommit=True) as admin:
                # Match only connections tagged by this test. Never terminate the server.
                rows = admin.execute(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
                    "WHERE application_name = %s AND pid <> pg_backend_pid()",
                    (identity,),
                ).fetchall()
                assert rows and all(row[0] for row in rows), "no test connections terminated"
            (tmp_path / "release").write_text("finish node")
            finish(worker, log_path)
        finally:
            stop([worker])
    counts = read_counts(tmp_path / "audit.jsonl")
    assert len(counts) == 16
    assert all(count == 1 for count in counts.values()), counts


def test_stale_worker_cannot_overwrite_recovered_result(tmp_path):
    database = postgres_url()
    identity = "stale-" + uuid.uuid4().hex
    base = [sys.executable, "-m", "tests.support.plugin_worker"]
    original_env = worker_env(tmp_path, database)
    original_env.update(
        GRAPH_STALE_SCORE_OFFSET="1000",
        GRAPH_BODY_FINISHED=str(tmp_path / "stale-finished"),
        GRAPH_WORKER_EXIT_GATE=str(tmp_path / "exit"),
    )
    workers, logs = [], []
    with ExitStack() as stack:
        try:
            original_log = tmp_path / "original.log"
            stream = stack.enter_context(original_log.open("w"))
            original = subprocess.Popen(
                base + ["start", "--id", identity, "--mode", "cycle", "--rounds", "4"],
                env=original_env,
                stdout=stream,
                stderr=subprocess.STDOUT,
            )
            workers.append(original)
            logs.append(original_log)
            wait_for(
                lambda: (
                    (tmp_path / "ready").exists()
                    and read_counts(tmp_path / "audit.jsonl")["1:slow"] == 1
                ),
                workers,
                logs,
                "original worker did not reach its gate",
            )
            env = worker_env(tmp_path, database)
            for key in ("GRAPH_READY", "GRAPH_READY_KEY"):
                env.pop(key)
            env.update(
                GRAPH_EXECUTOR_ID=identity + "-replacement",
                GRAPH_RECOVER_OWNER="plugin-" + identity,
                GRAPH_RECOVER_READY=str(tmp_path / "recover-ready"),
                GRAPH_RECOVER_DONE=str(tmp_path / "recover-done"),
                GRAPH_RECOVER_RELEASE=str(tmp_path / "recover-release"),
                GRAPH_GATE_RELEASE=str(tmp_path / "replacement-release"),
            )
            (tmp_path / "recover-release").write_text("recover")
            replacement_log = tmp_path / "replacement.log"
            stream = stack.enter_context(replacement_log.open("w"))
            replacement = subprocess.Popen(
                base + ["recover", "--id", identity],
                env=env,
                stdout=stream,
                stderr=subprocess.STDOUT,
            )
            workers.append(replacement)
            logs.append(replacement_log)
            wait_for(
                lambda: (
                    (tmp_path / "recover-done").exists()
                    and read_counts(tmp_path / "audit.jsonl")["1:slow"] == 2
                ),
                workers,
                logs,
                "replacement did not take over",
            )
            # Return a different update from the old body after ownership changed.
            (tmp_path / "release").write_text("finish stale body")
            wait_for(
                lambda: (tmp_path / "stale-finished").exists(),
                [original],
                [original_log],
                "old body did not finish",
            )
            # The pinned SDK emits this after the stale execution tries to finish
            # its child history. Wait before allowing the replacement body to return.
            wait_for(
                lambda: (
                    "is no longer owned by this execution" in original_log.read_text()
                    and f"Workflow {identity}-" in original_log.read_text()
                ),
                [original],
                [original_log],
                "stale child did not reach its ownership check",
            )
            (tmp_path / "replacement-release").write_text("finish replacement")
            finish(replacement, replacement_log)
            (tmp_path / "exit").write_text("exit")
            finish(original, original_log)
        finally:
            stop(workers)
    assert read_counts(tmp_path / "audit.jsonl")["1:slow"] == 2
