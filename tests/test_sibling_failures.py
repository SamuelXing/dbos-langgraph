"""A failed superstep must not leave interrupted children pending."""

import json
import os
import subprocess
import sys
import uuid

import pytest


@pytest.mark.parametrize("repeat_cancel", [False, True], ids=["ordinary", "repeated-cancel"])
def test_parallel_failure_cancels_interrupted_child(tmp_path, repeat_cancel):
    identity = "siblings-" + uuid.uuid4().hex
    env = {
        **os.environ,
        "DBOS_SYSTEM_DATABASE_URL": os.environ.get(
            "GRAPH_TEST_DATABASE_URL", f"sqlite:///{tmp_path}/history.sqlite"
        ),
    }
    command = [sys.executable, "-m", "tests.support.sibling_worker", "--id", identity]
    if repeat_cancel:
        command.append("--repeat-cancel")
    result = subprocess.run(command, env=env, capture_output=True, text=True, timeout=45)
    (tmp_path / "failure.log").write_text(result.stdout + result.stderr)
    assert result.returncode == 0, result.stdout + result.stderr
    observation = json.loads(result.stdout.strip().splitlines()[-1])
    assert observation == {
        "error": "ValueError",
        "stopped_at_failure": True,
        "finished": False,
        "child_status": "CANCELLED",
    }
    restarted = subprocess.run(
        command + ["--inspect"],
        env=env,
        capture_output=True,
        text=True,
        timeout=45,
    )
    (tmp_path / "restart.log").write_text(restarted.stdout + restarted.stderr)
    assert restarted.returncode == 0, restarted.stdout + restarted.stderr
    observation = json.loads(restarted.stdout.strip().splitlines()[-1])
    assert observation == {
        "error": "ValueError",
        "entered": False,
        "children": ["CANCELLED", "ERROR"],
    }
