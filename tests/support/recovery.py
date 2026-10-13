"""Process controls shared by crash/recovery tests."""

import json
import subprocess
import time
from collections import Counter


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
