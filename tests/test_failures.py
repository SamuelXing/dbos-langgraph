"""Failure policies run through the installed DBOS execution path."""

import json
import os
import subprocess
import sys
import uuid

import pytest


@pytest.mark.parametrize(
    "mode,attempts,status,error",
    [
        ("retry", 3, "SUCCESS", None),
        ("error", 1, "ERROR", "ValueError"),
        ("exhaust", 3, "ERROR", "DBOSMaxStepRetriesExceeded"),
        ("timeout", 1, "ERROR", "DBOSStepTimeoutError"),
        ("cancel", 1, "CANCELLED", "DBOSAwaitedWorkflowCancelledError"),
    ],
)
def test_node_failure_policy(tmp_path, mode, attempts, status, error):
    env = {
        **os.environ,
        "DBOS_SYSTEM_DATABASE_URL": os.environ.get(
            "GRAPH_TEST_DATABASE_URL", f"sqlite:///{tmp_path}/history.sqlite"
        ),
    }
    process = subprocess.run(
        [
            sys.executable,
            "-m",
            "tests.support.failure_worker",
            mode,
            "--id",
            "failure-" + uuid.uuid4().hex,
        ],
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )
    (tmp_path / "worker.log").write_text(process.stdout + process.stderr)
    assert process.returncode == 0, process.stdout + process.stderr
    result = json.loads(process.stdout.strip().splitlines()[-1])
    assert result["attempts"] == attempts
    assert result["status"] == status
    assert result["error"] == error
    assert result["second_error"] == error
    if mode == "retry":
        assert result["result"] == {"answer": "done"}
    if mode in ("cancel", "timeout"):
        assert result["stopped"]
