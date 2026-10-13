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


@pytest.mark.parametrize("value", [1, 99], ids=["same-input", "different-input"])
def test_duplicate_root_id_across_processes(tmp_path, value):
    from tests.support.postgres import stop, wait_for

    database = os.environ.get("GRAPH_TEST_DATABASE_URL", f"sqlite:///{tmp_path}/history.sqlite")
    workflow_id = "duplicate-process-" + uuid.uuid4().hex
    workers, logs, streams = [], [], []

    def launch(name, input_value):
        log = tmp_path / f"{name}.log"
        stream = log.open("w")
        streams.append(stream)
        logs.append(log)
        workers.append(
            subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "tests.support.duplicate_process_worker",
                    "--id",
                    workflow_id,
                    "--worker",
                    name,
                    "--value",
                    str(input_value),
                    "--directory",
                    str(tmp_path),
                ],
                env={**os.environ, "DBOS_SYSTEM_DATABASE_URL": database},
                stdout=stream,
                stderr=subprocess.STDOUT,
            )
        )

    try:
        launch("original", 1)
        wait_for(
            lambda: (tmp_path / "entered").exists(), workers, logs, "original node did not enter"
        )
        for name in ("duplicate-a", "duplicate-b"):
            launch(name, value)
        wait_for(
            lambda: all(
                (tmp_path / f"started-{name}").exists() for name in ("duplicate-a", "duplicate-b")
            ),
            workers,
            logs,
            "duplicate starts did not return handles",
        )
        (tmp_path / "release").touch()
        for worker, log in zip(workers, logs, strict=True):
            worker.wait(timeout=45)
            assert worker.returncode == 0, log.read_text()
            assert json.loads(log.read_text().strip().splitlines()[-1]) == {"value": 2}
        calls = [
            json.loads(line)
            for path in tmp_path.glob("calls-*.jsonl")
            for line in path.read_text().splitlines()
        ]
        assert calls == [{"value": 1}]
    finally:
        stop(workers)
        for stream in streams:
            stream.close()
