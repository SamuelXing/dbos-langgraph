"""Native interrupts rebuild memory checkpoints and reuse committed DBOS results."""

import json
import os
import subprocess
import sys
import uuid
from collections import Counter

import pytest

from tests.support.postgres import stop, wait_for


def worker_command(tmp_path, identity, mode, boundary="pause", response=True, scenario="normal"):
    return [
        sys.executable,
        "-m",
        "tests.support.interrupt_worker",
        mode,
        "--id",
        identity,
        "--directory",
        str(tmp_path),
        "--boundary",
        boundary,
        "--response",
        json.dumps(response),
        "--scenario",
        scenario,
    ]


def worker_env(tmp_path):
    return {
        **os.environ,
        "DBOS_SYSTEM_DATABASE_URL": os.environ.get(
            "GRAPH_TEST_DATABASE_URL", f"sqlite:///{tmp_path}/history.sqlite"
        ),
    }


def kill_at_marker(tmp_path, identity, mode, boundary, marker):
    log = tmp_path / f"{mode}.log"
    with log.open("w") as stream:
        worker = subprocess.Popen(
            worker_command(tmp_path, identity, mode, boundary),
            env=worker_env(tmp_path),
            stdout=stream,
            stderr=subprocess.STDOUT,
        )
        try:
            wait_for(
                lambda: (tmp_path / marker).exists(),
                [worker],
                [log],
                "interrupt boundary was not reached",
            )
            worker.kill()
            worker.wait(timeout=10)
        finally:
            stop([worker])


def finish(tmp_path, identity, mode, boundary="pause", response=True, scenario="normal"):
    result = subprocess.run(
        worker_command(tmp_path, identity, mode, boundary, response, scenario),
        env=worker_env(tmp_path),
        capture_output=True,
        text=True,
        timeout=40,
    )
    (tmp_path / f"{mode}.log").write_text(result.stdout + result.stderr)
    assert result.returncode == 0, result.stdout + result.stderr
    return json.loads(result.stdout.strip().splitlines()[-1])


@pytest.mark.parametrize("boundary", ["pause", "approval", "tool"])
def test_interrupt_survives_crashes_before_and_after_approval(tmp_path, boundary):
    identity = "interrupt-" + uuid.uuid4().hex
    kill_at_marker(tmp_path, identity, "start", boundary, "paused")
    original_pause = (tmp_path / "pause-id").read_text()
    if boundary != "pause":
        marker = "approval-ready" if boundary == "approval" else "tail-ready"
        kill_at_marker(tmp_path, identity, "resume", boundary, marker)
        assert (tmp_path / "pause-id").read_text() == original_pause
    (tmp_path / "release").touch()
    mode = "resume" if boundary == "pause" else "recover"
    assert finish(tmp_path, identity, mode, boundary) == {
        "prompt": "Run the tool?",
        "decision": True,
        "tool_result": 42,
        "done": True,
    }
    counts = Counter((tmp_path / "calls").read_text().splitlines())
    assert counts["model"] == counts["tool"] == 1
    assert counts["approval-entered"] == (3 if boundary == "approval" else 2)
    assert counts["approval-consumed"] == (2 if boundary == "approval" else 1)
    assert counts["tail"] == (2 if boundary == "tool" else 1)


@pytest.mark.parametrize("response", [False, None, {}])
def test_resume_keeps_json_values_and_conflicting_retries_are_idempotent(tmp_path, response):
    identity = "response-" + uuid.uuid4().hex
    result = finish(tmp_path, identity, "live", response=response)
    assert result == {"prompt": "Run the tool?", "decision": response, "done": True}
    assert "tool" not in (tmp_path / "calls").read_text().splitlines()


def test_cancelled_pause_cannot_run_the_tool(tmp_path):
    identity = "cancel-interrupt-" + uuid.uuid4().hex
    assert finish(tmp_path, identity, "cancel") == {"cancelled": True}
    assert (tmp_path / "calls").read_text().splitlines() == ["model", "approval-entered"]


@pytest.mark.asyncio
async def test_interrupt_api_requires_explicit_opt_in():
    from dbos_langgraph import DurableGraph

    async def workflow(*args):
        pytest.fail("disabled API must not execute a workflow")

    app = DurableGraph(workflow)
    with pytest.raises(ValueError, match="enable interrupts"):
        await app.get_interrupt("root")
    with pytest.raises(ValueError, match="enable interrupts"):
        await app.resume("root", "pause", True)


@pytest.mark.parametrize(
    "scenario, message",
    [
        ("second", "only one sequential interrupt"),
        ("workflow", "interrupts require opt-in step nodes"),
        ("parallel", "only one sequential interrupt"),
    ],
)
def test_unsupported_interrupt_shapes_fail(tmp_path, scenario, message):
    result = finish(tmp_path, "unsupported-" + uuid.uuid4().hex, "unsupported", scenario=scenario)
    assert message in result["unsupported"]


def test_sync_step_can_interrupt_and_resume(tmp_path):
    result = finish(tmp_path, "sync-interrupt-" + uuid.uuid4().hex, "live", scenario="sync")
    assert result["decision"] is True
    assert result["tool_result"] == 42
    assert (tmp_path / "calls").read_text().splitlines().count("approval-entered") == 2
