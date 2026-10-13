"""Recursive cancellation can be retried after a process dies mid-request."""

import json
import os
import subprocess
import sys
import uuid

import pytest

from tests.support.postgres import stop, wait_for


@pytest.mark.parametrize("durable", [False, True], ids=["manual-retry", "durable-request"])
@pytest.mark.parametrize("boundary", ["cascade", "cleanup"])
def test_crash_during_recursive_cancellation(tmp_path, boundary, durable):
    identity = "cancel-crash-" + uuid.uuid4().hex
    env = {
        **os.environ,
        "DBOS_SYSTEM_DATABASE_URL": os.environ.get(
            "GRAPH_TEST_DATABASE_URL", f"sqlite:///{tmp_path}/history.sqlite"
        ),
    }
    command = [
        sys.executable,
        "-m",
        "tests.support.cancellation_crash_worker",
        "--id",
        identity,
        "--directory",
        str(tmp_path),
        "--boundary",
        boundary,
    ]
    if durable:
        command.append("--durable")
    log = tmp_path / "original.log"
    with log.open("w") as stream:
        worker = subprocess.Popen(command, env=env, stdout=stream, stderr=subprocess.STDOUT)
        try:

            def at_boundary():
                if boundary == "cascade":
                    return (tmp_path / "gap").exists()
                return (tmp_path / "cancel-returned").exists() and (tmp_path / "cleanup").exists()

            wait_for(at_boundary, [worker], [log], "cancellation boundary was not reached")
            worker.kill()
            worker.wait(timeout=10)
        finally:
            stop([worker])
    restarted = subprocess.run(
        command + ["--inspect", "--executor", "replacement"],
        env=env,
        capture_output=True,
        text=True,
        timeout=45,
    )
    (tmp_path / "restart.log").write_text(restarted.stdout + restarted.stderr)
    assert restarted.returncode == 0, restarted.stdout + restarted.stderr
    result = json.loads(restarted.stdout.strip().splitlines()[-1])
    expected = {
        "before_root": "CANCELLED",
        "before": ["PENDING" if boundary == "cascade" else "CANCELLED"],
        "root": "CANCELLED",
        "children": ["CANCELLED"],
        "errors": ["DBOSAwaitedWorkflowCancelledError"] * 2,
        "entered": False,
    }
    if durable:
        expected["request"] = "SUCCESS"
        expected["missing_target"] = "rejected"
    assert result == expected
    assert (tmp_path / "calls").read_text().splitlines() == ["entered"]
