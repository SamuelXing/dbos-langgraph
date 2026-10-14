"""Message types, reducer semantics, and stable identity across worker restarts."""

import json
import os
import subprocess
import sys
import uuid

import pytest
from langchain_core.messages import AIMessage, HumanMessage, RemoveMessage, ToolMessage
from langgraph.graph.message import add_messages

from dbos_langgraph._state import fingerprint, prepare_messages
from tests.support.postgres import stop, wait_for


def test_message_fingerprint_preserves_tool_data_and_distinguishes_json():
    message = AIMessage(
        content="",
        id="model",
        tool_calls=[
            {"name": "lookup", "args": {"key": "answer"}, "id": "call", "type": "tool_call"}
        ],
    )
    assert fingerprint(message) == fingerprint(message.model_copy(deep=True))
    assert fingerprint(message) != fingerprint(message.model_dump())
    changed = message.model_copy(deep=True)
    changed.tool_calls[0]["args"]["key"] = "other"
    assert fingerprint(message) != fingerprint(changed)
    with pytest.raises(TypeError, match="JSON"):
        fingerprint(ToolMessage(content="value", tool_call_id="call", artifact=object()))


def test_prepared_updates_keep_replacement_and_removal_semantics():
    original = HumanMessage(content="first", id="user")
    update = prepare_messages(
        {"messages": [HumanMessage(content="edited", id="user"), AIMessage(content="reply")]},
        seed="child",
    )
    again = prepare_messages(
        {"messages": [HumanMessage(content="edited", id="user"), AIMessage(content="reply")]},
        seed="child",
    )
    assert update == again
    assert update["messages"][1].id
    merged = add_messages([original], update["messages"])
    assert [message.content for message in merged] == ["edited", "reply"]
    removed = prepare_messages({"messages": [RemoveMessage(id="user")]}, seed="remove")
    assert add_messages(merged, removed["messages"]) == merged[1:]
    assert original.content == "first"


@pytest.mark.parametrize("missing_ids", [False, True], ids=["explicit-ids", "assigned-ids"])
def test_messages_state_recovers_tool_loop(tmp_path, missing_ids):
    identity = "messages-" + uuid.uuid4().hex
    env = {
        **os.environ,
        "DBOS_SYSTEM_DATABASE_URL": os.environ.get(
            "GRAPH_TEST_DATABASE_URL", f"sqlite:///{tmp_path}/history.sqlite"
        ),
    }
    command = [
        sys.executable,
        "-m",
        "tests.support.message_worker",
        "--id",
        identity,
        "--directory",
        str(tmp_path),
    ]
    if missing_ids:
        command.append("--missing-ids")
    log = tmp_path / "original.log"
    with log.open("w") as stream:
        worker = subprocess.Popen(command, env=env, stdout=stream, stderr=subprocess.STDOUT)
        try:
            wait_for(
                lambda: (tmp_path / "entered").exists(),
                [worker],
                [log],
                "graph did not reach unfinished tool",
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
    messages = json.loads(result.stdout.strip().splitlines()[-1])
    assert [message["type"] for message in messages] == ["human", "ai", "tool", "ai"]
    assert len({message["data"]["id"] for message in messages}) == 4
    assert all(message["data"]["id"] for message in messages)
    assert messages[1]["data"]["tool_calls"][0]["id"] == "lookup-1"
    assert messages[2]["data"]["tool_call_id"] == "lookup-1"
    assert messages[2]["data"]["artifact"] == {"source": "fixture"}
    assert messages[3]["data"]["content"] == [{"type": "text", "text": "The answer is 42"}]
    if not missing_ids:
        assert [message["data"]["id"] for message in messages] == ["user", "call", "tool", "answer"]
    assert (tmp_path / "calls").read_text().splitlines() == ["model", "tool", "tool", "answer"]
