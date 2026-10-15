"""Coordinate Host driver publication with the Bot graphs that bind its source."""

from __future__ import annotations

from contextlib import AsyncExitStack
from dataclasses import dataclass
from uuid import NAMESPACE_URL, uuid5

from forest.errors import ForestError


@dataclass
class _BotTransition:
    host: object
    previous: object
    assembled: object
    drain: tuple[str, ...] = ()
    capacity: object | None = None
    drained: bool = False
    finished: bool = False


def _fatal(
    controller, transitions: list[_BotTransition], code: str, message: str,
    cause: BaseException, *, ledger_errors: tuple[BaseException, ...] = (),
    cleanup_errors: tuple[BaseException, ...] = (),
) -> ForestError:
    failure = controller._fail(ForestError(
        code, message, 503,
        details={
            "bot_ids": [item.host.settings.bot_id for item in transitions],
            "cause": getattr(cause, "code", type(cause).__name__),
            "ledger_errors": [
                getattr(item, "code", type(item).__name__)
                for item in ledger_errors
            ],
            "cleanup_errors": [
                getattr(item, "code", type(item).__name__)
                for item in cleanup_errors
            ],
        },
    ))
    for item in transitions:
        item.host._fatal_replacement_error = failure
    return failure


async def _rollback(candidate, previous, transitions, host_drained):
    failures = []
    for item in reversed(transitions):
        try:
            failures.extend(await item.assembled.registry.close())
        except BaseException as exc:
            failures.append(exc)
    try:
        failures.extend(await candidate.close())
    except BaseException as exc:
        failures.append(exc)
    for item in reversed(transitions):
        if item.drained:
            try:
                await item.previous.resume_owned_work_after_failed_replacement()
            except BaseException as exc:
                failures.append(exc)
    if host_drained:
        try:
            await previous.resume_owned_work_after_failed_replacement()
        except BaseException as exc:
            failures.append(exc)
    return failures


def _handoff_bindings(item: _BotTransition, old_sources: frozenset[str]):
    source = tuple(sorted({
        binding for descriptor in item.previous.descriptors
        for binding in descriptor.effect_driver_bindings
        if binding[2] in old_sources
    }))
    keys = {(service, kind) for service, kind, _source in source}
    target = tuple(sorted({
        binding for descriptor in item.assembled.registry.descriptors
        for binding in descriptor.effect_driver_bindings
        if (binding[0], binding[1]) in keys
    }))
    if (
        not source or len(source) != len(keys) or len(target) != len(source)
        or tuple((service, kind) for service, kind, _source in source)
        != tuple((service, kind) for service, kind, _source in target)
        or any(old[2] == new[2] for old, new in zip(source, target, strict=True))
    ):
        raise ForestError(
            "CONTROL_DRIVER_CONSUMER_PLAN_INVALID",
            "The Bot handoff must replace each exact bound driver source", 409,
        )
    return [list(binding) for binding in source], [list(binding) for binding in target]


async def execute_joint_host_driver_transition(
    controller, previous, candidate, snapshots, affected, consumers,
):
    """Run the irreversible Host then Bot publication under the admission gate.

    The caller holds the Host controller lock and shared driver-binding gate.
    Only this supervisor-provided coordinator may pass a prepared Host graph to
    Bot candidate assembly. Normal Bot replacement still reads active exports.
    """

    gate = controller._driver_binding_gate
    ledger = controller.runtime.graph_change_authority
    if gate is None or not consumers or ledger is None:
        raise ForestError(
            "CONTROL_DRIVER_JOINT_AUTHORITY_UNAVAILABLE",
            "Joint driver transition needs the gate and Host result ledger", 503,
        )
    lease = controller.runtime.authority_lease()
    transitions: list[_BotTransition] = []
    host_drained = False
    journal_staged = False
    old_sources = frozenset(
        export["source_ref"] for export in controller.driver_exports()
        if export["component_id"] in set(affected)
    )
    async with AsyncExitStack() as stack:
        try:
            for host in consumers:
                lock = host._replacement_lock
                if lock is None or host.registry is None or host._closing:
                    raise ForestError(
                        "CONTROL_DRIVER_CONSUMER_UNAVAILABLE",
                        "A Bot consumer cannot enter the joint transition", 503,
                    )
                await stack.enter_async_context(lock)
                if host.registry is None or host._closing:
                    raise ForestError(
                        "CONTROL_DRIVER_CONSUMER_UNAVAILABLE",
                        "A Bot consumer stopped before candidate assembly", 503,
                    )

            await candidate.prewarm()
            prepared_host_graph = (candidate, snapshots)
            for host in consumers:
                previous_bot = host.registry
                bot_activation_id = str(uuid5(
                    NAMESPACE_URL,
                    f"forest.control-driver-joint/1:{candidate.activation_id}:"
                    f"{host.settings.bot_id}",
                ))
                assembled = await host._assemble_candidate(
                    activation_id=bot_activation_id,
                    prepared_host_graph=prepared_host_graph,
                )
                item = _BotTransition(host, previous_bot, assembled)
                transitions.append(item)
                bot_candidate = assembled.registry
                plan = bot_candidate.preview_graph_transition(previous_bot)
                item.drain = bot_candidate.plan_graph_transition(previous_bot)
                item.capacity = plan.worker_capacity
                if not item.drain:
                    raise ForestError(
                        "CONTROL_DRIVER_CONSUMER_PLAN_INVALID",
                        "Changing a bound Host source must replace its Bot consumer",
                        409,
                    )
                if any(
                    source in old_sources
                    for descriptor in bot_candidate.descriptors
                    for _service, _kind, source in descriptor.effect_driver_bindings
                ):
                    raise ForestError(
                        "CONTROL_DRIVER_SOURCE_STALE",
                        "Bot candidate still binds a replaced Host source", 409,
                    )
                await host._admit_worker_capacity(item.capacity)
                await bot_candidate.prepare()

            roster = []
            for item in transitions:
                source_bindings, target_bindings = _handoff_bindings(
                    item, old_sources
                )
                roster.append({
                    "bot_id": item.host.settings.bot_id,
                    "bot_activation_id": item.assembled.registry.activation_id,
                    "source_bot_graph_id": item.previous.graph_identity.graph_id,
                    "source_bot_graph_revision": (
                        item.previous.graph_identity.graph_revision
                    ),
                    "source_bindings": source_bindings,
                    "target_bindings": target_bindings,
                })
            await ledger.stage_consumers(
                lease,
                host_activation_id=candidate.activation_id,
                consumers=tuple(roster),
            )
            journal_staged = True
            for item in transitions:
                await stack.enter_async_context(
                    item.host.selection_reader.publication_fence(
                        expected_revision=item.assembled.selection.revision,
                        expected_digest=item.assembled.selection.digest,
                    )
                )
            for item in transitions:
                await item.previous.drain_owned_work_for_replacement(
                    item.assembled.registry.activation_id,
                    components=(
                        item.drain
                        if len(item.drain) < len(item.previous.order) else None
                    ),
                    candidate_components=(
                        item.assembled.registry.prepared_replacement_facts()
                    ),
                )
                item.drained = True
            if affected:
                await previous.drain_owned_work_for_replacement(
                    candidate.activation_id,
                    components=(
                        affected if len(affected) < len(previous.order) else None
                    ),
                    candidate_components=candidate.prepared_replacement_facts(),
                )
                host_drained = True

            await candidate.start()
            candidate.accept_retained_ownership()
            controller._registry = candidate
            controller._snapshots = snapshots

            for item in transitions:
                host = item.host
                bot_candidate = item.assembled.registry
                await host._admit_worker_capacity(item.capacity)
                await bot_candidate.prewarm()
                await bot_candidate.start()
                bot_candidate.accept_retained_ownership()
                host.registry = bot_candidate
                host._official_descriptors = item.assembled.official_descriptors
                host._official_defaults = item.assembled.official_defaults
                host._official_effect_drivers = (
                    item.assembled.official_effect_drivers
                )
                host.selection_reader = item.assembled.selection_reader
                gate.refresh(host)

            for item in transitions:
                errors = await item.previous.close()
                if errors:
                    raise errors[0]
                graph = await item.host._kernel.runtime.graph_for_activation(
                    item.assembled.registry.activation_id
                )
                if (
                    graph is None
                    or graph["state"] not in {"ACTIVE", "RETIRED"}
                    or graph["graph_id"]
                    != item.assembled.registry.graph_identity.graph_id
                ):
                    raise ForestError(
                        "CONTROL_DRIVER_BOT_PUBLICATION_UNPROVEN",
                        "Bot handoff has no matching durable graph evidence", 503,
                    )
                await ledger.finish_consumer(
                    lease, host_activation_id=candidate.activation_id,
                    bot_id=item.host.settings.bot_id,
                    state="SUCCEEDED", graph=graph,
                )
                item.finished = True
            errors = await previous.close()
            if errors:
                raise errors[0]
            return candidate
        except BaseException as error:
            committed = bool(candidate.publication_committed)
            if host_drained and not committed and candidate.graph_identity is not None:
                try:
                    head = await controller.runtime.generation_authority.current_graph()
                    committed = (
                        head is None or head.graph_id != previous.graph_identity.graph_id
                    )
                except BaseException:
                    committed = True
            if committed:
                ledger_errors = []
                if journal_staged:
                    for item in transitions:
                        if item.finished:
                            continue
                        try:
                            graph = await item.host._kernel.runtime.graph_for_activation(
                                item.assembled.registry.activation_id
                            )
                            if (
                                graph is None
                                or graph["state"] not in {"ACTIVE", "RETIRED"}
                                or graph["graph_id"]
                                != item.assembled.registry.graph_identity.graph_id
                            ):
                                graph = None
                            await ledger.finish_consumer(
                                lease, host_activation_id=candidate.activation_id,
                                bot_id=item.host.settings.bot_id,
                                state="PARTIAL" if graph is not None else "FAILED",
                                graph=graph,
                                error_code="CONTROL_DRIVER_JOINT_POST_PUBLICATION_FAILURE",
                            )
                        except BaseException as ledger_error:
                            # The PENDING row remains durable for recovery.
                            ledger_errors.append(ledger_error)
                cleanup_errors = []
                for item in reversed(transitions):
                    registry = (
                        item.previous
                        if item.host.registry is item.assembled.registry
                        else item.assembled.registry
                    )
                    try:
                        cleanup_errors.extend(await registry.close())
                    except BaseException as cleanup_error:
                        cleanup_errors.append(cleanup_error)
                registry = previous if controller._registry is candidate else candidate
                try:
                    cleanup_errors.extend(await registry.close())
                except BaseException as cleanup_error:
                    cleanup_errors.append(cleanup_error)
                failure = _fatal(
                    controller, transitions,
                    "CONTROL_DRIVER_JOINT_POST_PUBLICATION_FAILURE",
                    "Host driver published but a Bot consumer did not complete",
                    error, ledger_errors=tuple(ledger_errors),
                    cleanup_errors=tuple(cleanup_errors),
                )
                raise failure from error
            failures = await _rollback(
                candidate, previous, transitions, host_drained
            )
            if journal_staged:
                for item in transitions:
                    try:
                        await ledger.finish_consumer(
                            lease, host_activation_id=candidate.activation_id,
                            bot_id=item.host.settings.bot_id,
                            state="FAILED",
                            error_code="CONTROL_DRIVER_JOINT_ABORTED",
                        )
                    except BaseException as ledger_error:
                        failures.append(ledger_error)
            if failures or (
                isinstance(error, ForestError)
                and error.code == "RUNTIME_GRAPH_REPLACEMENT_ROLLBACK_FAILED"
            ):
                cause = failures[0] if failures else error
                failure = _fatal(
                    controller, transitions,
                    "CONTROL_DRIVER_JOINT_ROLLBACK_FAILED",
                    "Joint driver transition could not restore the old graphs",
                    cause,
                )
                raise failure from cause
            raise
