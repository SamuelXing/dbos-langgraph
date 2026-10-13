"""Discover control histories and recover them before deeper graph descendants."""

import json
import os
import subprocess
import sys
import uuid

import pytest

from tests.support.postgres import stop, wait_for


@pytest.mark.parametrize("multiple", [False, True], ids=["one-request", "two-requests"])
@pytest.mark.parametrize("repeat_crash", [False, True], ids=["one-crash", "reconciler-crash"])
def test_reconcile_cancellation_requests(tmp_path, multiple, repeat_crash):
    identity = "reconcile-" + uuid.uuid4().hex
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
        "cascade",
        "--durable",
        "--deep",
    ]
    if multiple:
        command.append("--multiple")

    def kill_at_marker(options, marker, log_name):
        log = tmp_path / log_name
        with log.open("w") as stream:
            worker = subprocess.Popen(
                command + options, env=env, stdout=stream, stderr=subprocess.STDOUT
            )
            try:
                wait_for(
                    lambda: (tmp_path / marker).exists(),
                    [worker],
                    [log],
                    "worker did not reach cancellation boundary",
                )
                # Multiple requests must both be stored before the original dies.
                if multiple and not options:
                    wait_for(
                        lambda: (tmp_path / "other-request-started").exists(),
                        [worker],
                        [log],
                        "second request did not start",
                    )
                worker.kill()
                worker.wait(timeout=10)
            finally:
                stop([worker])

    kill_at_marker([], "gap", "original.log")
    if repeat_crash:
        kill_at_marker(
            ["--inspect", "--reconcile", "--pause-reconcile", "--executor", "replacement"],
            "reconcile-gap",
            "replacement.log",
        )
    restarted = subprocess.run(
        command + ["--inspect", "--reconcile", "--executor", "final"],
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )
    (tmp_path / "final.log").write_text(restarted.stdout + restarted.stderr)
    assert restarted.returncode == 0, restarted.stdout + restarted.stderr
    result = json.loads(restarted.stdout.strip().splitlines()[-1])
    expected_requests = {identity + "-cancel"}
    if multiple:
        expected_requests.add(identity + "-other-cancel")
        assert result["other_root"] == "CANCELLED"
        assert result["other_children"] == ["CANCELLED"]
    assert set(result["completed"]) == expected_requests
    assert result["request"] == "SUCCESS"
    assert result["root"] == "CANCELLED"
    assert result["children"] == ["CANCELLED"]
    assert result["descendants"] == ["CANCELLED"]
    assert result["errors"] == ["DBOSAwaitedWorkflowCancelledError"] * 2
    assert result["entered"] is False
    assert (tmp_path / "calls").read_text().splitlines() == ["entered"] * (2 if multiple else 1)
