"""Explicit registration of durable nodes in native LangGraph graphs."""

import asyncio
import inspect
import math
import re
from collections.abc import Awaitable, Callable, Coroutine, Mapping
from contextlib import nullcontext
from copy import copy, deepcopy
from dataclasses import replace
from typing import Any, Literal, NotRequired, TypedDict, cast

from dbos import DBOS, SetWorkflowID, WorkflowHandleAsync
from langchain_core.runnables import RunnableConfig
from langgraph._internal._runnable import RunnableCallable
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.errors import GraphInterrupt
from langgraph.graph import StateGraph
from langgraph.runtime import Runtime
from langgraph.types import Command, Interrupt

from ._config import invocation_config, normalize_config
from ._interrupts import (
    EVENT_KEY,
    PausedNode,
    interrupt_scope,
    node_resume_values,
    resume_target,
    resume_topic,
    wait_for_resume,
)
from ._policy import StepOptions
from ._state import fingerprint, prepare_messages

Node = Callable[..., dict[str, Any] | Awaitable[dict[str, Any]]]
ExecutionMode = Literal["step", "workflow"]


class _NodeResult(TypedDict):
    identity: str
    update: dict[str, Any]
    interrupts: NotRequired[tuple[Interrupt[Any], ...]]


class ReplayMismatchError(RuntimeError):
    """Recovered node history does not match the requested invocation."""


class DurableGraph:
    """An async DBOS workflow facade, not a drop-in CompiledStateGraph."""

    def __init__(
        self,
        workflow: Callable[..., Coroutine[Any, Any, Any]],
        cancellation: Callable[[str], Coroutine[Any, Any, None]] | None = None,
        cancellation_name: str | None = None,
        resumption: Callable[[str, str, Any], Coroutine[Any, Any, None]] | None = None,
    ) -> None:
        self.workflow = workflow
        self._cancellation = cancellation
        self._cancellation_name = cancellation_name
        self._resumption = resumption

    async def ainvoke(
        self,
        state: Any,
        config: Mapping[str, Any] | None = None,
        *,
        recursion_limit: int | None = None,
        context: Any = None,
    ) -> Any:
        if self._resumption is not None:
            handle = await self.start(
                state, config, recursion_limit=recursion_limit, context=context
            )
            return await handle.get_result()
        state, limit, options = self._invocation(state, config, recursion_limit, context)
        return await self.workflow(state, limit, **options)

    async def start(
        self,
        state: Any,
        config: Mapping[str, Any] | None = None,
        *,
        recursion_limit: int | None = None,
        context: Any = None,
    ) -> Any:
        state, limit, options = self._invocation(state, config, recursion_limit, context)
        handle = await DBOS.start_workflow_async(self.workflow, state, limit, **options)
        if self._resumption is not None:
            # A polling handle observes terminal cancellation even while the
            # in-process workflow task is still waiting for its message timeout.
            return await DBOS.retrieve_workflow_async(handle.get_workflow_id())
        return handle

    @staticmethod
    def _invocation(
        state: Any,
        config: Mapping[str, Any] | None,
        limit: int | None,
        context: Any,
    ) -> tuple[Any, int, dict[str, Any]]:
        config, limit = normalize_config(config, limit)
        options: dict[str, Any] = {}
        if config:
            options["config"] = config
        if context is not None:
            fingerprint(context, allow_messages=False)
            options["context"] = deepcopy(context)
        return prepare_messages(state), limit, options

    async def get_interrupt(
        self, workflow_id: str, *, timeout_seconds: float = 60
    ) -> Interrupt[Any] | None:
        """Wait for a pause event; return None if the graph is running or complete."""
        if self._resumption is None or self._cancellation_name is None:
            raise ValueError("enable interrupts when registering this graph")
        if not isinstance(workflow_id, str) or not workflow_id.strip():
            raise ValueError("workflow ID is required")
        if (
            isinstance(timeout_seconds, bool)
            or not isinstance(timeout_seconds, (int, float))
            or not math.isfinite(timeout_seconds)
            or timeout_seconds < 0
        ):
            raise ValueError("timeout_seconds must be finite and nonnegative")
        status = await DBOS.get_workflow_status_async(workflow_id)
        expected_name = self._cancellation_name.removesuffix(".cancel") + ".graph"
        if status is None or status.name != expected_name:
            raise ValueError("interrupt target must be an existing root of this graph")
        if status.status not in ("PENDING", "ENQUEUED"):
            return None
        event = await DBOS.get_event_async(workflow_id, EVENT_KEY, timeout_seconds=timeout_seconds)
        return event["interrupt"] if event and event["status"] == "paused" else None

    async def resume(self, workflow_id: str, interrupt_id: str, value: Any) -> None:
        """Deliver one immutable approval; retries for the same pause reuse its first value."""
        if self._resumption is None or self._cancellation_name is None:
            raise ValueError("enable interrupts when registering this graph")
        if DBOS.workflow_id is not None:
            raise RuntimeError("resume from outside a DBOS workflow")
        if any(
            not isinstance(item, str) or not item.strip() for item in (workflow_id, interrupt_id)
        ):
            raise ValueError("workflow and interrupt IDs are required")
        fingerprint(value, allow_messages=False)
        request_id = "dbos_langgraph.resume." + fingerprint(
            [self._cancellation_name, workflow_id, interrupt_id]
        )
        existing = await DBOS.get_workflow_status_async(request_id)
        if existing is not None:
            # A delivered approval remains idempotent after the graph completes.
            # Failed validation never creates this immutable delivery history.
            handle: WorkflowHandleAsync[Any] = await DBOS.retrieve_workflow_async(request_id)
            await handle.get_result()
            return
        pause = await self.get_interrupt(workflow_id, timeout_seconds=0)
        if pause is None or pause.id != interrupt_id:
            raise ValueError("no matching pending interrupt")
        with SetWorkflowID(request_id):
            handle = await DBOS.start_workflow_async(
                self._resumption, workflow_id, interrupt_id, deepcopy(value)
            )
        await handle.get_result()

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


class DBOSLangGraphPlugin:
    """Register one graph before launch. Each node gets an isolated child history."""

    def __init__(self, name: str, *, interrupts: bool = False) -> None:
        self._validate_name(name)
        if type(interrupts) is not bool:
            raise TypeError("interrupts must be a bool")
        self.name = name
        self._interrupts = interrupts
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
        is_async = inspect.iscoroutinefunction(function)
        if not inspect.isfunction(function) and not inspect.ismethod(function):
            raise TypeError("nodes must be plain functions or bound methods")
        if not is_async and execute_in != "step":
            raise ValueError("sync nodes require step execution")
        if not is_async and (options.preemptible or options.timeout_seconds is not None):
            raise ValueError("sync node threads cannot be preempted or timed out")
        parameters = list(inspect.signature(function).parameters.values())
        positional = (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
        extras = parameters[1:]
        if (
            not parameters
            or parameters[0].kind not in positional
            or any(
                parameter.name not in ("runtime", "config")
                or parameter.kind
                not in (inspect.Parameter.POSITIONAL_OR_KEYWORD, inspect.Parameter.KEYWORD_ONLY)
                for parameter in extras
            )
        ):
            raise TypeError("nodes must accept only state, config and runtime")
        accepts_runtime = any(parameter.name == "runtime" for parameter in extras)
        accepts_config = any(parameter.name == "config" for parameter in extras)

        def identity(state: Any, context: Any, config: Any = None, resumes: Any = None) -> str:
            inputs = [self.name, name, execute_in, state]
            # Retain the fingerprint format of histories created without context.
            if context is not None:
                inputs.append({"context": context})
            if config is not None:
                inputs.append({"config": config})
            if resumes is not None:
                inputs.append({"resumes": resumes})
            return fingerprint(inputs)

        async def leaf(
            state: Any, context: Any = None, config: Any = None, resumes: Any = None
        ) -> dict[str, Any] | PausedNode:
            kwargs: dict[str, Any] = {}
            if accepts_runtime:
                kwargs["runtime"] = Runtime(context=deepcopy(context))
            if accepts_config:
                kwargs["config"] = deepcopy(config)
            scope = (
                interrupt_scope(str(DBOS.workflow_id), resumes or [])
                if self._interrupts
                else nullcontext()
            )
            with scope:
                try:
                    if is_async:
                        result = await cast(
                            Awaitable[dict[str, Any]], function(deepcopy(state), **kwargs)
                        )
                    else:
                        result = await asyncio.to_thread(function, deepcopy(state), **kwargs)
                except GraphInterrupt as exc:
                    if not self._interrupts or execute_in != "step":
                        raise NotImplementedError("interrupts require opt-in step nodes") from exc
                    pauses = cast(tuple[Interrupt[Any], ...], exc.args[0])
                    for pause in pauses:
                        fingerprint(pause.value, allow_messages=False)
                    return PausedNode(pauses)
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

        async def isolated_node(
            state: Any, context: Any = None, config: Any = None, resumes: Any = None
        ) -> _NodeResult:
            invocation_identity = identity(state, context, config, resumes)
            try:
                if resumes is not None:
                    update = await durable_leaf(state, context, config, resumes)
                elif config is not None:
                    update = await durable_leaf(state, context, config)
                elif context is not None:
                    update = await durable_leaf(state, context)
                else:
                    update = await durable_leaf(state)
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
            if isinstance(update, PausedNode):
                return {
                    "identity": invocation_identity,
                    "update": {},
                    "interrupts": update.interrupts,
                }
            return {"identity": invocation_identity, "update": update}

        node_workflow = DBOS.workflow(name=f"dbos_langgraph.{self.name}.{name}.workflow")(
            isolated_node
        )

        async def node(state: Any, runtime: Runtime[Any]) -> dict[str, Any]:
            context = runtime.context
            invocation = invocation_config.get()
            if invocation is None:
                raise RuntimeError("durable nodes require a plugin graph invocation")
            snapshot, configured = invocation
            config = snapshot if accepts_config or configured else None
            resumes = node_resume_values() if self._interrupts else None
            expected = identity(state, context, config, resumes)
            if resumes is not None:
                envelope = await node_workflow(state, context, config, resumes)
            elif config is not None:
                envelope = await node_workflow(state, context, config)
            elif context is not None:
                envelope = await node_workflow(state, context)
            else:
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
            if "interrupts" in envelope:
                raise GraphInterrupt(envelope["interrupts"])
            result = envelope["update"]
            self._node_completed(name, state, result)
            return result

        builder.add_node(name, node, input_schema=builder.state_schema)
        self._builder = builder
        self._nodes.add(name)
        return self

    def register_graph(self, builder: StateGraph[Any, Any, Any, Any]) -> DurableGraph:
        """Wrap existing callable nodes on a copy of an uncompiled builder.

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
                raise TypeError("existing nodes must be plain functions")
            function = spec.runnable.func or spec.runnable.afunc
            if function is None or not (inspect.isfunction(function) or inspect.ismethod(function)):
                raise TypeError("existing nodes must be plain functions")
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

        async def invoke(
            state: Any, recursion_limit: int, context: Any = None, config: Any = None
        ) -> Any:
            user_config = config or {}
            snapshot = {
                "configurable": {},
                "metadata": {},
                "tags": [],
                **user_config,
                "recursion_limit": recursion_limit,
            }
            token = invocation_config.set((snapshot, bool(user_config)))
            try:
                execution_graph = (
                    graph.copy(update={"checkpointer": InMemorySaver()})
                    if self._interrupts
                    else graph
                )
                native_config = cast(
                    RunnableConfig, {**user_config, "recursion_limit": recursion_limit}
                )
                if self._interrupts:
                    native_config["configurable"] = {
                        **user_config.get("configurable", {}),
                        "thread_id": DBOS.workflow_id,
                    }
                result = await execution_graph.ainvoke(state, native_config, context=context)
                rounds = 0
                while isinstance(result, dict) and result.get("__interrupt__"):
                    if not self._interrupts:
                        raise NotImplementedError("enable interrupts when registering this graph")
                    pauses = result["__interrupt__"]
                    if len(pauses) != 1 or rounds:
                        raise NotImplementedError(
                            "only one sequential interrupt per invocation is supported"
                        )
                    pause = pauses[0]
                    checkpoint = await execution_graph.aget_state(native_config)
                    tasks = [
                        task
                        for task in checkpoint.tasks
                        if any(item.id == pause.id for item in task.interrupts)
                    ]
                    if len(tasks) != 1:
                        raise RuntimeError("cannot identify interrupted graph task")
                    await DBOS.set_event_async(EVENT_KEY, {"status": "paused", "interrupt": pause})
                    response = await wait_for_resume(pause.id)
                    await DBOS.set_event_async(EVENT_KEY, {"status": "running"})
                    resume_token = resume_target.set((tasks[0].id, [response]))
                    try:
                        result = await execution_graph.ainvoke(
                            Command(resume={pause.id: response}),
                            native_config,
                            context=context,
                        )
                    finally:
                        resume_target.reset(resume_token)
                    rounds += 1
                if self._interrupts:
                    await DBOS.set_event_async(EVENT_KEY, {"status": "completed"})
                return result
            finally:
                invocation_config.reset(token)

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

        async def resume(workflow_id: str, interrupt_id: str, value: Any) -> None:
            status = await DBOS.get_workflow_status_async(workflow_id)
            if status is None or status.name != f"dbos_langgraph.{self.name}.graph":
                raise ValueError("resume target must be an existing root of this graph")
            if status.status not in ("PENDING", "ENQUEUED"):
                raise ValueError("resume target is not pending")
            await DBOS.send_async(
                workflow_id,
                {"interrupt_id": interrupt_id, "value": value},
                resume_topic(interrupt_id),
            )

        resumption = (
            DBOS.workflow(name=f"dbos_langgraph.{self.name}.resume")(resume)
            if self._interrupts
            else None
        )
        return DurableGraph(durable, cancellation, cancellation_name, resumption)
