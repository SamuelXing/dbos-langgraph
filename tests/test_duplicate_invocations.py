"""Characterize the SDK's first-input behavior for duplicate root IDs."""

import json
import os
import subprocess
import sys
import uuid

import pytest


@pytest.mark.parametrize("value", [1, 99], ids=["same-input", "different-input"])
def test_duplicate_root_id_uses_original_result(tmp_path, value):
    process = subprocess.run(
        [
            sys.executable,
            "-m",
            "tests.support.duplicate_worker",
            "--id",
            "duplicate-" + uuid.uuid4().hex,
            "--value",
            str(value),
        ],
        env={
            **os.environ,
            "DBOS_SYSTEM_DATABASE_URL": os.environ.get(
                "GRAPH_TEST_DATABASE_URL", f"sqlite:///{tmp_path}/history.sqlite"
            ),
        },
        capture_output=True,
        text=True,
        timeout=60,
    )
    (tmp_path / "worker.log").write_text(process.stdout + process.stderr)
    assert process.returncode == 0, process.stdout + process.stderr
    result = json.loads(process.stdout.strip().splitlines()[-1])
    assert result["calls"] == [1]
    assert result["results"] == [{"value": 2}] * 11
