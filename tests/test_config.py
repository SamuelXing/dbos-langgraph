"""Replay preserves configuration without sharing it between concurrent invocations."""

import json
import os
import subprocess
import sys
import uuid
from collections import Counter

import pytest

from dbos_langgraph._config import normalize_config
from tests.support.postgres import stop, wait_for


@pytest.mark.parametrize(
    "config",
    [
        {"callbacks": []},
        {"configurable": {"value": object()}},
        {"configurable": {"__internal": 1}},
        {"tags": "tag"},
        {"metadata": []},
        {"recursion_limit": True},
        {"max_concurrency": 2},
    ],
)
def test_reject_config_that_cannot_be_replayed(config):
    with pytest.raises((ValueError, TypeError)):
        normalize_config(config, None)


def test_recursion_limit_conflicts_are_rejected():
    with pytest.raises(ValueError, match="conflicting"):
        normalize_config({"recursion_limit": 3}, 4)
    with pytest.raises(ValueError, match="positive"):
        normalize_config({"recursion_limit": 1}, True)
    assert normalize_config({"recursion_limit": 3}, None) == ({}, 3)


def test_sync_nodes_keep_config_and_context_through_recovery(tmp_path):
    identity = "config-" + uuid.uuid4().hex
    env = {
        **os.environ,
        "DBOS_SYSTEM_DATABASE_URL": os.environ.get(
            "GRAPH_TEST_DATABASE_URL", f"sqlite:///{tmp_path}/history.sqlite"
        ),
    }
    command = [
        sys.executable,
        "-m",
        "tests.support.config_worker",
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
                lambda: all((tmp_path / f"ready-{user}").exists() for user in ("Alice", "Bob")),
                [worker],
                [log],
                "configured graphs did not reach unfinished nodes",
            )
            worker.kill()
            worker.wait(timeout=10)
        finally:
            stop([worker])
    (tmp_path / "release").touch()
    result = subprocess.run(
        command + ["--recover"], env=env, capture_output=True, text=True, timeout=45
    )
    (tmp_path / "restart.log").write_text(result.stdout + result.stderr)
    assert result.returncode == 0, result.stdout + result.stderr
    assert json.loads(result.stdout.strip().splitlines()[-1]) == [
        {"value": 2, "user": "Alice", "done": True},
        {"value": 3, "user": "Bob", "done": True},
    ]
    assert Counter((tmp_path / "calls").read_text().splitlines()) == {
        "plan:Alice": 1,
        "plan:Bob": 1,
        "finish:Alice": 2,
        "finish:Bob": 2,
    }


@pytest.mark.parametrize("options", [{"preemptible": True}, {"timeout_seconds": 1}])
def test_sync_nodes_reject_thread_preemption(options):
    from langgraph.graph import StateGraph

    from dbos_langgraph import DBOSLangGraphPlugin, StepOptions

    def node(state):
        return {}

    with pytest.raises(ValueError, match="cannot be preempted"):
        DBOSLangGraphPlugin("sync-policy").add_node(
            StateGraph(dict), "node", node, step_options=StepOptions(**options)
        )
