"""An unfinished step's retry counter is not persisted across crashes."""

import json
import os
import subprocess
import sys
import time
import uuid

import pytest

from tests.support.recovery import read_counts


@pytest.mark.parametrize("outcome", ["success", "exhaust"])
def test_crash_during_backoff_restarts_attempt_budget(tmp_path, outcome):
    audit = tmp_path / "audit.jsonl"
    env = {
        **os.environ,
        "GRAPH_AUDIT": str(audit),
        "DBOS_SYSTEM_DATABASE_URL": os.environ.get(
            "GRAPH_TEST_DATABASE_URL", f"sqlite:///{tmp_path}/history.sqlite"
        ),
    }
    identity = "retry-crash-" + uuid.uuid4().hex
    base = [sys.executable, "-m", "tests.support.retry_crash_worker"]
    log_path = tmp_path / "crash.log"
    with log_path.open("w") as log:
        process = subprocess.Popen(
            base + ["start", outcome, "--id", identity],
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        try:
            deadline = time.monotonic() + 30
            # The SDK emits this after an exception, immediately before backoff.
            while "Step being automatically retried" not in log_path.read_text():
                assert process.poll() is None, log_path.read_text()
                assert time.monotonic() < deadline, "retry backoff not observed"
                time.sleep(0.01)
            time.sleep(0.02)
        finally:
            if process.poll() is None:
                process.kill()
            process.wait(timeout=10)
    assert read_counts(audit)["attempt"] == 1
    recovered = subprocess.run(
        base + ["recover", outcome, "--id", identity],
        env=env,
        capture_output=True,
        text=True,
        timeout=60,
    )
    (tmp_path / "recover.log").write_text(recovered.stdout + recovered.stderr)
    assert recovered.returncode == 0, recovered.stdout + recovered.stderr
    result = json.loads(recovered.stdout.strip().splitlines()[-1])
    assert result["attempts"] == 3
    assert read_counts(audit)["attempt"] == 4
    if outcome == "success":
        assert result["error"] is None
        assert result["result"] == {"answer": "done"}
    else:
        assert result["error"] == "DBOSMaxStepRetriesExceeded"
