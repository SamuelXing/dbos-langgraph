"""Public LangGraph/DBOS APIs, with real SIGKILL and positional-history guard."""

import asyncio
import json
import os
import subprocess
import sys
import time
import uuid
from collections import Counter

import pytest

from tests.support.engine import execute
from tests.support.example import GRAPH, initial


@pytest.mark.parametrize(
    "mode,rounds,ready_key,block_key,input_drift",
    [
        ("cycle", 3, "1:fast", "1:slow", False),
        ("cycle", 102, "301:fast", "301:slow", False),
        ("dynamic", 1, "map:1", "map:2", False),
        ("dynamic", 1, "map:1", "map:2", True),
        ("isolated", 1, "unused", "tail", False),
    ],
)
def test_native_pregel_crash(tmp_path, mode, rounds, ready_key, block_key, input_drift):
    database = os.environ.get("GRAPH_TEST_DATABASE_URL", f"sqlite:///{tmp_path}/history.sqlite")
    audit, ready = tmp_path / "audit.jsonl", tmp_path / "ready"
    commits = tmp_path / "commits.jsonl"
    identity = "pregel-" + uuid.uuid4().hex
    env = {
        **os.environ,
        "DBOS_SYSTEM_DATABASE_URL": database,
        "GRAPH_AUDIT": str(audit),
        "GRAPH_COMMITS": str(commits),
        "GRAPH_READY": str(ready),
        "GRAPH_READY_KEY": ready_key,
        "GRAPH_BLOCK_KEY": block_key,
    }
    command = [sys.executable, "-m", "tests.support.plugin_worker"]
    with (tmp_path / "crash.log").open("w") as log:
        worker = subprocess.Popen(
            command + ["start", "--id", identity, "--mode", mode, "--rounds", str(rounds)],
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        try:
            deadline = time.monotonic() + 60
            while not ready.exists() or block_key not in [
                r["key"] for r in map(json.loads, audit.read_text().splitlines())
            ]:
                assert worker.poll() is None, (tmp_path / "crash.log").read_text()
                assert time.monotonic() < deadline, "crash boundary not reached"
                time.sleep(0.01)
            worker.kill()
            worker.wait(timeout=10)
        finally:
            if worker.poll() is None:
                worker.kill()
                worker.wait(timeout=10)
    before = Counter(r["key"] for r in map(json.loads, audit.read_text().splitlines()))
    committed = (
        {r["key"] for r in map(json.loads, commits.read_text().splitlines())}
        if commits.exists()
        else set()
    )
    env.pop("GRAPH_BLOCK_KEY")
    env.pop("GRAPH_READY_KEY")
    env.pop("GRAPH_READY")
    if mode == "isolated":
        env["GRAPH_REPLAY_JITTER"] = "1"  # Force an allowed scheduling-order perturbation.
    if input_drift:
        env["GRAPH_INPUT_DRIFT"] = "1"
    recovered = subprocess.run(
        command + ["recover", "--id", identity], env=env, capture_output=True, text=True, timeout=90
    )
    (tmp_path / "recover.log").write_text(recovered.stdout + recovered.stderr)
    if input_drift:
        assert recovered.returncode != 0
        assert "replay input mismatch" in recovered.stderr + recovered.stdout
        return
    assert recovered.returncode == 0, recovered.stderr + recovered.stdout
    state = json.loads(recovered.stdout.strip().splitlines()[-1])["state"]
    if mode == "cycle":
        expected = asyncio.run(execute(GRAPH, initial(rounds)))["state"]
    elif mode == "isolated":
        expected = {"items": [1, 2, 3, 4], "score": 0, "events": ["b", "c"]}
    else:
        expected = {"items": [1, 2, 3, 4], "score": 10, "events": ["1", "2", "3", "4"]}
    assert state == expected
    counts = Counter(r["key"] for r in map(json.loads, audit.read_text().splitlines()))
    if mode == "isolated":
        assert counts == Counter({"b1": 1, "b2": 1, "c1": 1, "c2": 1, "tail": 2})
        return
    assert counts[ready_key] == 1
    assert counts[block_key] == 2
    # A child can commit before the parent writes its observation marker.
    # Such an unmarked child may be reused; an unfinished body may run once more.
    assert all(1 <= count <= before[key] + 1 for key, count in counts.items()), counts
    assert all(counts[k] == 1 for k in committed)
    assert len(counts) == (rounds * 4 if mode == "cycle" else 4)
