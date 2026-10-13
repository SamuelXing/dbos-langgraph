"""Crash a cyclic graph after progress to successive supersteps."""

import asyncio
import json
import os
import subprocess
import sys
import uuid

from tests.support.engine import execute
from tests.support.example import GRAPH, initial
from tests.support.recovery import kill_at_boundary, read_counts


def test_crashes_in_three_successive_rounds(tmp_path):
    identity = "progress-" + uuid.uuid4().hex
    audit = tmp_path / "audit.jsonl"
    commits = tmp_path / "commits.jsonl"
    ready = tmp_path / "ready"
    env = {
        **os.environ,
        "DBOS_SYSTEM_DATABASE_URL": os.environ.get(
            "GRAPH_TEST_DATABASE_URL", f"sqlite:///{tmp_path}/history.sqlite"
        ),
        "GRAPH_AUDIT": str(audit),
        "GRAPH_COMMITS": str(commits),
        "GRAPH_READY": str(ready),
    }
    base = [sys.executable, "-m", "tests.support.plugin_worker"]
    committed_keys = set()
    blocked_keys = []
    for attempt, index in enumerate((1, 4, 7)):
        ready.unlink(missing_ok=True)
        block_key = f"{index}:slow"
        blocked_keys.append(block_key)
        env.update(
            GRAPH_READY_KEY=f"{index}:fast",
            GRAPH_BLOCK_KEY=block_key,
            GRAPH_SCHEDULE_SEED=str(31 + attempt),
        )
        command = base + ["start" if attempt == 0 else "recover", "--id", identity]
        if attempt == 0:
            command += ["--mode", "cycle", "--rounds", "4"]
        kill_at_boundary(
            command, env, tmp_path / f"crash-{attempt}.log", ready, audit, block_key, 0
        )
        counts = read_counts(audit)
        assert all(counts[key] == 1 for key in committed_keys)
        # Exclude bodies that were unfinished at an earlier crash: their successful
        # recovery is the second execution, even once a commit marker appears.
        committed_keys.update(set(read_counts(commits)) - set(blocked_keys))

    for key in ("GRAPH_READY", "GRAPH_READY_KEY", "GRAPH_BLOCK_KEY"):
        env.pop(key)
    result = subprocess.run(
        base + ["recover", "--id", identity], env=env, capture_output=True, text=True, timeout=90
    )
    (tmp_path / "final.log").write_text(result.stdout + result.stderr)
    assert result.returncode == 0, result.stdout + result.stderr
    state = json.loads(result.stdout.strip().splitlines()[-1])["state"]
    assert state == asyncio.run(execute(GRAPH, initial(4)))["state"]
    counts = read_counts(audit)
    assert all(counts[key] == 2 for key in blocked_keys)
    assert all(counts[key] == 1 for key in committed_keys)
    assert len(counts) == 16
