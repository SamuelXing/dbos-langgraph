"""Committed context-aware node results survive a process crash."""

import json
import os
import subprocess
import sys
import uuid

import pytest

from tests.support.postgres import stop, wait_for


@pytest.mark.parametrize("drift", [False, True], ids=["compatible", "context-drift"])
def test_existing_graph_replays_context_without_rerunning_committed_node(tmp_path, drift):
    identity = "context-" + uuid.uuid4().hex
    env = {
        **os.environ,
        "DBOS_SYSTEM_DATABASE_URL": os.environ.get(
            "GRAPH_TEST_DATABASE_URL", f"sqlite:///{tmp_path}/history.sqlite"
        ),
    }
    command = [
        sys.executable,
        "-m",
        "tests.support.context_worker",
        "--id",
        identity,
        "--directory",
        str(tmp_path),
    ]
    log = tmp_path / "original.log"
    with log.open("w") as stream:
        worker = subprocess.Popen(command, env=env, stdout=stream, stderr=subprocess.STDOUT)
        try:
            wait_for(
                lambda: (tmp_path / "entered").exists(),
                [worker],
                [log],
                "context graph did not reach its unfinished node",
            )
            worker.kill()
            worker.wait(timeout=10)
        finally:
            stop([worker])
    (tmp_path / "release").touch()
    if drift:
        env["CONTEXT_DRIFT"] = "1"
    restarted = subprocess.run(
        command + ["--recover"], env=env, capture_output=True, text=True, timeout=45
    )
    (tmp_path / "restart.log").write_text(restarted.stdout + restarted.stderr)
    assert restarted.returncode == 0, restarted.stdout + restarted.stderr
    result = json.loads(restarted.stdout.strip().splitlines()[-1])
    calls = (tmp_path / "calls").read_text().splitlines()
    assert calls.count("agent") == 1
    if drift:
        assert result == {"error": "ReplayMismatchError"}
    else:
        assert result == {"answer": "Hello Alice", "done": True}
        assert calls == ["agent", "finish", "finish"]
