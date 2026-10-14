"""Explicit registration of durable nodes in native LangGraph graphs."""

import asyncio
import inspect
import re
from collections.abc import Awaitable, Callable, Coroutine
from copy import copy, deepcopy
from dataclasses import replace
from typing import Any, Literal, TypedDict, cast

from dbos import DBOS, WorkflowHandleAsync
from langgraph._internal._runnable import RunnableCallable
from langgraph.graph import StateGraph
from langgraph.runtime import Runtime

from ._policy import StepOptions
from ._state import fingerprint, prepare_messages

Node = Callable[..., Awaitable[dict[str, Any]]]
ExecutionMode = Literal["step", "workflow"]


class _NodeResult(TypedDict):
    identity: str
    update: dict[str, Any]


class ReplayMismatchError(RuntimeError):
    """Recovered node history does not match the requested invocation."""


class DurableGraph:
    """An async DBOS workflow facade, not a drop-in CompiledStateGraph."""

    def __init__(
        self,
        workflow: Callable[..., Coroutine[Any, Any, Any]],
        cancellation: Callable[[str], Coroutine[Any, Any, None]] | None = None,
        cancellation_name: str | None = None,
    ) -> None:
        self.workflow = workflow
        self._cancellation = cancellation
        self._cancellation_name = cancellation_name

    async def ainvoke(self, state: Any, *, recursion_limit: int = 1000, context: Any = None) -> Any:
        self._validate_limit(recursion_limit)
        state = prepare_messages(state)
        if context is None:
            return await self.workflow(state, recursion_limit)
        fingerprint(context, allow_messages=False)
        return await self.workflow(state, recursion_limit, context=deepcopy(context))

    async def start(self, state: Any, *, recursion_limit: int = 1000, context: Any = None) -> Any:
        self._validate_limit(recursion_limit)
        state = prepare_messages(state)
        if context is None:
            return await DBOS.start_workflow_async(self.workflow, state, recursion_limit)
        fingerprint(context, allow_messages=False)
        return await DBOS.start_workflow_async(
            self.workflow, state, recursion_limit, context=deepcopy(context)
        )

    async def request_cancel(self, workflow_id: str) -> WorkflowHandleAsync[None]:
        """Start an independent durable request to cancel this graph and its children.

        Call outside a DBOS workflow. SetWorkflowID may assign a distinct request
        ID; await the returned handle to observe completion of stored cancellation.
        """
        if not isinstance(workflow_id, str) or not workflow_id.strip():
            raise ValueError("workflow_id must be a nonempty string")
        if self._cancellation is None:
            raise ValueError("cancellation requires a graph compiled through the plugin")
        if DBOS.workflow_id is not None:
            raise RuntimeError("request cancellation outside a DBOS workflow")
        return await DBOS.start_workflow_async(self._cancellation, workflow_id)

    async def reconcile_cancellations(
        self, dead_executor_ids: list[str], *, batch_size: int = 100
    ) -> list[str]:
        """Finish unfinished cancellation requests owned by confirmed-dead executors.

        Run from one recovery coordinator, outside a workflow, before recovering
        target histories. Exceptions stop reconciliation and must block that recovery.
        """
        if (
            not isinstance(dead_executor_ids, list)
            or not dead_executor_ids
            or any(not isinstance(owner, str) or not owner.strip() for owner in dead_executor_ids)
        ):
            raise ValueError("provide nonempty confirmed-dead executor IDs")
        if isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size < 1:
            raise ValueError("batch_size must be a positive integer")
        if self._cancellation_name is None:
            raise ValueError("reconciliation requires a graph compiled through the plugin")
        if DBOS.workflow_id is not None:
            raise RuntimeError("reconcile cancellation outside a DBOS workflow")
        completed: list[str] = []
        while True:
            requests = await DBOS.list_workflows_async(
                name=self._cancellation_name,
                status=["PENDING", "ENQUEUED"],
                executor_id=dead_executor_ids,
                limit=batch_size,
                load_input=False,
                load_output=False,
            )
            if not requests:
                return completed
            for request in requests:
                handle = await DBOS.resume_workflow_async(request.workflow_id)
                await handle.get_result()
                completed.append(request.workflow_id)

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
        step_options: StepOptions | None = None,
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
        if step_options is not None and not isinstance(step_options, StepOptions):
            raise TypeError("step_options must be StepOptions")
        if execute_in == "workflow" and step_options is not None:
            raise ValueError("workflow-mode nodes configure their own DBOS steps")
        options = step_options or StepOptions()
        if not inspect.iscoroutinefunction(function):
            raise TypeError("async nodes are required")
        parameters = list(inspect.signature(function).parameters.values())
        positional = (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
        accepts_runtime = (
            len(parameters) == 2
            and parameters[1].name == "runtime"
            and parameters[1].kind
            in (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY)
        )
        if (
            not parameters
            or parameters[0].kind not in positional
            or (len(parameters) != 1 and not accepts_runtime)
        ):
            raise TypeError(
                "nodes must accept only state or state and runtime; config is unsupported"
            )

        def identity(state: Any, context: Any) -> str:
            inputs = [self.name, name, execute_in, state]
            # Retain the fingerprint format of histories created without context.
            if context is not None:
                inputs.append({"context": context})
            return fingerprint(inputs)

        async def leaf(state: Any, context: Any = None) -> dict[str, Any]:
            if accepts_runtime:
                result = await function(deepcopy(state), runtime=Runtime(context=deepcopy(context)))
            else:
                result = await function(deepcopy(state))
            if not isinstance(result, dict):
                raise TypeError("nodes must return dict state updates")
            return cast(dict[str, Any], prepare_messages(result, seed=DBOS.workflow_id))

        durable_leaf = (
            DBOS.step(
                name=f"dbos_langgraph.{self.name}.{name}.step",
                retries_allowed=options.max_attempts > 1,
                max_attempts=options.max_attempts,
                interval_seconds=options.interval_seconds,
                backoff_rate=options.backoff_rate,
                preemptible=options.preemptible,
                timeout_seconds=options.timeout_seconds,
            )(leaf)
            if execute_in == "step"
            else leaf
        )

        async def isolated_node(state: Any, context: Any = None) -> _NodeResult:
            invocation_identity = identity(state, context)
            try:
                update = (
                    await durable_leaf(state)
                    if context is None
                    else await durable_leaf(state, context)
                )
            except asyncio.CancelledError:
                # Pregel cancels sibling coroutines on failure. Close their durable
                # histories too, so startup recovery cannot revive abandoned nodes.
                workflow_id = DBOS.workflow_id
                if workflow_id is not None:
                    cancellation = asyncio.create_task(
                        DBOS.cancel_workflow_async(workflow_id, cancel_children=True)
                    )
                    # Pregel may cancel this task again while the SDK commits cleanup.
                    # Keep the cancellation operation alive and drain it before exit.
                    while not cancellation.done():
                        try:
                            await asyncio.shield(cancellation)
                        except asyncio.CancelledError:
                            continue
                    cancellation.result()
                raise
            return {"identity": invocation_identity, "update": update}

        node_workflow = DBOS.workflow(name=f"dbos_langgraph.{self.name}.{name}.workflow")(
            isolated_node
        )

        async def node(state: Any, runtime: Runtime[Any]) -> dict[str, Any]:
            context = runtime.context
            expected = identity(state, context)
            envelope = (
                await node_workflow(state)
                if context is None
                else await node_workflow(state, context)
            )
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

        builder.add_node(name, node, input_schema=builder.state_schema)
        self._builder = builder
        self._nodes.add(name)
        return self

    def register_graph(self, builder: StateGraph[Any, Any, Any, Any]) -> DurableGraph:
        """Wrap existing async callable nodes on a copy of an uncompiled builder.

        Each node must declare execute_in in metadata. Runnable compositions and
        LangGraph retry/cache/timeout policies require separate integrations.
        """
        if builder.compiled:
            raise ValueError("register an uncompiled builder")
        if self._compiled or self._nodes:
            raise ValueError("register an existing graph on a fresh plugin instance")
        registrations: list[tuple[str, Node, ExecutionMode, StepOptions | None]] = []
        for name, spec in builder.nodes.items():
            self._validate_name(name)
            # This extraction is tied to the pinned LangGraph version. Accept only
            # its plain callable wrapper, never a composed Runnable's inner function.
            if not isinstance(spec.runnable, RunnableCallable) or spec.runnable.kwargs:
                raise TypeError("existing nodes must be plain async functions")
            function = spec.runnable.afunc
            if function is None or not inspect.iscoroutinefunction(function):
                raise TypeError("existing nodes must be plain async functions")
            if (
                spec.retry_policy
                or spec.cache_policy
                or spec.timeout is not None
                or spec.is_error_handler
                or spec.error_handler_node is not None
            ):
                raise ValueError(
                    "configure DBOS policies; LangGraph retry/cache/timeout unsupported"
                )
            metadata = spec.metadata or {}
            mode = metadata.get("execute_in")
            if mode not in ("step", "workflow"):
                raise ValueError(f"node {name} must declare execute_in in metadata")
            options = metadata.get("step_options")
            if options is not None and not isinstance(options, StepOptions):
                raise TypeError("step_options metadata must be StepOptions")
            registrations.append((name, cast(Node, function), mode, options))
        wrapped = copy(builder)
        wrapped.nodes = dict(builder.nodes)
        wrapped.schemas = dict(builder.schemas)
        wrapped.channels = dict(builder.channels)
        wrapped.managed = dict(builder.managed)
        for name, function, mode, options in registrations:
            original = wrapped.nodes.pop(name)
            self.add_node(wrapped, name, function, execute_in=mode, step_options=options)
            # Keep the native node schema, destinations, and scheduling metadata.
            wrapped.nodes[name] = replace(original, runnable=wrapped.nodes[name].runnable)
        return self.compile(wrapped)

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

        async def invoke(state: Any, recursion_limit: int, context: Any = None) -> Any:
            result = await graph.ainvoke(
                state, {"recursion_limit": recursion_limit}, context=context
            )
            if isinstance(result, dict) and "__interrupt__" in result:
                raise NotImplementedError("interrupt/resume is not supported")
            return result

        durable = DBOS.workflow(name=f"dbos_langgraph.{self.name}.graph")(invoke)

        async def cancel(workflow_id: str) -> None:
            status = await DBOS.get_workflow_status_async(workflow_id)
            if status is None or status.name != f"dbos_langgraph.{self.name}.graph":
                raise ValueError("cancellation target must be an existing root of this graph")
            # The SDK records this operation only after the full cascade returns.
            # An interrupted request therefore retries the unfinished cascade.
            await DBOS.cancel_workflow_async(workflow_id, cancel_children=True)

        cancellation_name = f"dbos_langgraph.{self.name}.cancel"
        cancellation = DBOS.workflow(name=cancellation_name)(cancel)
        self._compiled = True
        return DurableGraph(durable, cancellation, cancellation_name)
