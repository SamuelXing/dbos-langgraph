"""First creation and owner failure at duplicate-request boundaries."""

import json
import os
import subprocess
import sys
import uuid
from contextlib import ExitStack

import pytest

from tests.support.postgres import postgres_url, stop, wait_for


def read_calls(directory):
    return [
        json.loads(line)
        for path in directory.glob("calls-*.jsonl")
        for line in path.read_text().splitlines()
    ]


@pytest.mark.parametrize("conflicting", [False, True], ids=["same-input", "different-input"])
@pytest.mark.parametrize("crash", [False, True], ids=["creation-race", "owner-crash"])
def test_duplicate_creation_and_recovery(tmp_path, conflicting, crash):
    database = (
        postgres_url()
        if crash
        else os.environ.get("GRAPH_TEST_DATABASE_URL", f"sqlite:///{tmp_path}/history.sqlite")
    )
    identity = "duplicate-race-" + uuid.uuid4().hex
    workers, logs = [], []
    with ExitStack() as stack:

        def launch(name, value, *options):
            log = tmp_path / f"{name}.log"
            stream = stack.enter_context(log.open("w"))
            worker = subprocess.Popen(
                [
                    sys.executable,
                    "-m",
                    "tests.support.duplicate_process_worker",
                    "--id",
                    identity,
                    "--worker",
                    name,
                    "--value",
                    str(value),
                    "--directory",
                    str(tmp_path),
                    *options,
                ],
                env={**os.environ, "DBOS_SYSTEM_DATABASE_URL": database},
                stdout=stream,
                stderr=subprocess.STDOUT,
            )
            workers.append(worker)
            logs.append(log)
            return worker

        try:
            if crash:
                original = launch("original", 1)
                wait_for(
                    lambda: (tmp_path / "entered").exists(),
                    workers,
                    logs,
                    "original body did not enter",
                )
                launch("a", 99 if conflicting else 1, "--recover")
                launch("b", 99 if conflicting else 1)
            else:
                launch("a", 1, "--race")
                # Finish schema initialization before launching the second SDK.
                # Only workflow creation, not database migration, is raced here.
                wait_for(
                    lambda: (tmp_path / "ready-a").exists(),
                    workers,
                    logs,
                    "first worker did not finish initialization",
                )
                launch("b", 99 if conflicting else 1, "--race")
                wait_for(
                    lambda: all((tmp_path / f"ready-{name}").exists() for name in ("a", "b")),
                    workers,
                    logs,
                    "workers did not reach the creation barrier",
                )
                (tmp_path / "start").touch()
            wait_for(
                lambda: (
                    all((tmp_path / f"started-{name}").exists() for name in ("a", "b"))
                    and (tmp_path / "entered").exists()
                ),
                workers,
                logs,
                "starts did not reach the held node",
            )
            before = read_calls(tmp_path)
            assert len(before) == 1, before
            winner = before[0]["value"]
            assert winner in ({1, 99} if conflicting and not crash else {1})
            if crash:
                original.kill()
                original.wait(timeout=10)
                # The recovery caller declares only the confirmed-dead executor.
                (tmp_path / "recover-owner").write_text(f"{identity}-original")
                wait_for(
                    lambda: len(read_calls(tmp_path)) == 2,
                    workers[1:],
                    logs[1:],
                    "unfinished node was not recovered",
                )
            (tmp_path / "release").touch()
            live_workers = workers[1:] if crash else workers
            live_logs = logs[1:] if crash else logs
            for worker, log in zip(live_workers, live_logs, strict=True):
                worker.wait(timeout=45)
                assert worker.returncode == 0, log.read_text()
                assert json.loads(log.read_text().strip().splitlines()[-1]) == {"value": winner + 1}
            assert read_calls(tmp_path) == [{"value": winner}] * (2 if crash else 1)
        finally:
            stop(workers)
