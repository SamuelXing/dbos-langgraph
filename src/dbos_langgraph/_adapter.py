"""Explicit registration of durable nodes in native LangGraph graphs."""

import inspect
import re
from collections.abc import Awaitable, Callable, Coroutine
from copy import deepcopy
from typing import Any, Literal, TypedDict

from dbos import DBOS
from langgraph.graph import StateGraph

from ._state import fingerprint

Node = Callable[[Any], Awaitable[dict[str, Any]]]
ExecutionMode = Literal["step", "workflow"]


class _NodeResult(TypedDict):
    identity: str
    update: dict[str, Any]


class ReplayMismatchError(RuntimeError):
    """Recovered node history does not match the requested invocation."""


class DurableGraph:
    """An async DBOS workflow facade, not a drop-in CompiledStateGraph."""

    def __init__(self, workflow: Callable[..., Coroutine[Any, Any, Any]]) -> None:
        self.workflow = workflow

    async def ainvoke(self, state: Any, *, recursion_limit: int = 1000) -> Any:
        self._validate_limit(recursion_limit)
        return await self.workflow(state, recursion_limit)

    async def start(self, state: Any, *, recursion_limit: int = 1000) -> Any:
        self._validate_limit(recursion_limit)
        return await DBOS.start_workflow_async(self.workflow, state, recursion_limit)

    @staticmethod
    def _validate_limit(limit: int) -> None:
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
            raise ValueError("recursion_limit must be a positive integer")


class DBOSLangGraphPlugin:
    """Register one graph before launch. Each node gets an isolated child history."""

    def __init__(self, name: str) -> None:
        self._validate_name(name)
        self.name = name
        self._nodes: set[str] = set()
        self._compiled = False
        self._builder: StateGraph[Any, Any, Any, Any] | None = None

    @staticmethod
    def _validate_name(name: str) -> None:
        if not isinstance(name, str) or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]*", name) is None:
            raise ValueError("names must use ASCII letters, digits, underscores, or hyphens")

    def add_node(
        self,
        builder: StateGraph[Any, Any, Any, Any],
        name: str,
        function: Node,
        *,
        execute_in: ExecutionMode = "step",
    ) -> "DBOSLangGraphPlugin":
        if self._compiled:
            raise ValueError("register nodes before compiling")
        self._validate_name(name)
        if self._builder is not None and builder is not self._builder:
            raise ValueError("one builder per plugin instance")
        if name in self._nodes or name in builder.nodes:
            raise ValueError(f"duplicate node: {name}")
        if execute_in not in ("step", "workflow"):
            raise ValueError("execute_in must be step or workflow")
        if not inspect.iscoroutinefunction(function):
            raise TypeError("async nodes are required")
        parameters = list(inspect.signature(function).parameters.values())
        if len(parameters) != 1 or parameters[0].kind not in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        ):
            raise TypeError("nodes must accept only state; config/runtime injection is unsupported")

        async def leaf(state: Any) -> dict[str, Any]:
            result = await function(deepcopy(state))
            if not isinstance(result, dict):
                raise TypeError("nodes must return dict state updates")
            fingerprint(result)
            return result

        durable_leaf = (
            DBOS.step(name=f"dbos_langgraph.{self.name}.{name}.step")(leaf)
            if execute_in == "step"
            else leaf
        )

        async def isolated_node(state: Any) -> _NodeResult:
            identity = fingerprint([self.name, name, execute_in, state])
            return {"identity": identity, "update": await durable_leaf(state)}

        node_workflow = DBOS.workflow(name=f"dbos_langgraph.{self.name}.{name}.workflow")(
            isolated_node
        )

        async def node(state: Any) -> dict[str, Any]:
            expected = fingerprint([self.name, name, execute_in, state])
            envelope = await node_workflow(state)
            if "identity" not in envelope or "update" not in envelope:
                raise ReplayMismatchError(
                    f"unsupported history format for {self.name}/{name}; "
                    "recover with the adapter version that created this workflow"
                )
            if envelope["identity"] != expected:
                raise ReplayMismatchError(
                    f"replay input mismatch for {self.name}/{name}: "
                    f"expected {expected}, recovered {envelope['identity']}; "
                    "restore compatible routing and code before recovery"
                )
            result = envelope["update"]
            self._node_completed(name, state, result)
            return result

        builder.add_node(name, node)
        self._builder = builder
        self._nodes.add(name)
        return self

    def _node_completed(self, name: str, state: Any, result: dict[str, Any]) -> None:
        """Test-only observation hook. Executes again on replay; never a durable action."""

    def compile(self, builder: StateGraph[Any, Any, Any, Any]) -> DurableGraph:
        if self._compiled:
            raise ValueError("one graph per plugin instance")
        if set(builder.nodes) != self._nodes:
            raise ValueError("all nodes must be registered through plugin.add_node")
        if self._builder is not None and builder is not self._builder:
            raise ValueError("one builder per plugin instance")
        graph = builder.compile()

        async def invoke(state: Any, recursion_limit: int) -> Any:
            result = await graph.ainvoke(state, {"recursion_limit": recursion_limit})
            if isinstance(result, dict) and "__interrupt__" in result:
                raise NotImplementedError("interrupt/resume is not supported")
            return result

        durable = DBOS.workflow(name=f"dbos_langgraph.{self.name}.graph")(invoke)
        self._compiled = True
        return DurableGraph(durable)
