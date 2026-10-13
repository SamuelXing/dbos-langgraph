"""Recover the same history through several worker crashes."""

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


def read_counts(path):
    if not path.exists():
        return Counter()
    # The writer may be between write() and flush() while we poll.
    lines = path.read_text().splitlines(keepends=True)
    return Counter(json.loads(line)["key"] for line in lines if line.endswith("\n"))


def kill_at_boundary(command, env, log_path, ready, audit, block_key, previous_count):
    with log_path.open("w") as log:
        process = subprocess.Popen(command, env=env, stdout=log, stderr=subprocess.STDOUT)
        try:
            deadline = time.monotonic() + 60
            while not ready.exists() or read_counts(audit)[block_key] <= previous_count:
                assert process.poll() is None, log_path.read_text()
                assert time.monotonic() < deadline, f"boundary timeout: {log_path}"
                time.sleep(0.01)
        finally:
            if process.poll() is None:
                process.kill()
            process.wait(timeout=10)


@pytest.mark.parametrize("mode", ["cycle", "dynamic"])
@pytest.mark.parametrize("seed", [7, 19])
def test_three_crashes_reuse_committed_results(tmp_path, mode, seed):
    audit = tmp_path / "audit.jsonl"
    commits = tmp_path / "commits.jsonl"
    ready = tmp_path / "ready"
    identity = "repeat-" + uuid.uuid4().hex
    rounds = 4
    ready_key, block_key = ("1:fast", "1:slow") if mode == "cycle" else ("map:1", "map:2")
    env = {
        **os.environ,
        "DBOS_SYSTEM_DATABASE_URL": os.environ.get(
            "GRAPH_TEST_DATABASE_URL", f"sqlite:///{tmp_path}/history.sqlite"
        ),
        "GRAPH_AUDIT": str(audit),
        "GRAPH_COMMITS": str(commits),
        "GRAPH_READY": str(ready),
        "GRAPH_READY_KEY": ready_key,
        "GRAPH_BLOCK_KEY": block_key,
    }
    base_command = [sys.executable, "-m", "tests.support.plugin_worker"]
    observed_commits = set()
    counts_before_last_crash = Counter()
    for attempt in range(3):
        ready.unlink(missing_ok=True)
        env["GRAPH_SCHEDULE_SEED"] = str(seed + attempt)
        command = base_command + ["start" if attempt == 0 else "recover", "--id", identity]
        if attempt == 0:
            command += ["--mode", mode, "--rounds", str(rounds)]
        previous_count = read_counts(audit)[block_key]
        kill_at_boundary(
            command, env, tmp_path / f"crash-{attempt}.log", ready, audit, block_key, previous_count
        )
        counts_before_last_crash = read_counts(audit)
        assert counts_before_last_crash[block_key] == attempt + 1
        observed_commits.update(read_counts(commits))
        assert all(counts_before_last_crash[key] == 1 for key in observed_commits)

    for key in ("GRAPH_READY", "GRAPH_READY_KEY", "GRAPH_BLOCK_KEY"):
        env.pop(key)
    env["GRAPH_SCHEDULE_SEED"] = str(seed + 3)
    result = subprocess.run(
        base_command + ["recover", "--id", identity],
        env=env,
        capture_output=True,
        text=True,
        timeout=90,
    )
    (tmp_path / "final.log").write_text(result.stdout + result.stderr)
    assert result.returncode == 0, result.stdout + result.stderr
    actual = json.loads(result.stdout.strip().splitlines()[-1])["state"]
    expected = (
        asyncio.run(execute(GRAPH, initial(rounds)))["state"]
        if mode == "cycle"
        else {"items": [1, 2, 3, 4], "score": 10, "events": ["1", "2", "3", "4"]}
    )
    assert actual == expected
    final_counts = read_counts(audit)
    assert final_counts[block_key] == 4
    assert all(final_counts[key] == 1 for key in observed_commits)
    assert all(count <= counts_before_last_crash[key] + 1 for key, count in final_counts.items())
    assert len(final_counts) == (rounds * 4 if mode == "cycle" else 4)
