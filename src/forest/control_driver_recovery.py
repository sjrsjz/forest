"""Reconcile interrupted Host driver handoffs from live graph authorities."""

from __future__ import annotations

from forest.errors import ForestError


class HostDriverHandoffRecovery:
    def __init__(self, *, supervisor, controller, gate, ledger) -> None:
        self.supervisor = supervisor
        self.controller = controller
        self.gate = gate
        self.ledger = ledger

    async def __call__(self, request: dict, pending: tuple[dict, ...]) -> None:
        for row in pending:
            bot_id = row["bot_id"]
            try:
                async with self.supervisor.management_host(
                    self.supervisor.management_scope_id(bot_id)
                ) as bot:
                    async with self.gate:
                        await self._inspect_live(request, row, bot)
            except ForestError as exc:
                if exc.code not in {
                    "BOT_MANAGEMENT_SCOPE_NOT_FOUND", "BOT_MANAGEMENT_UNAVAILABLE"
                }:
                    raise
                bot = await self.supervisor.catalog.inspect(bot_id)
                if bot["desired_state"] == "OFFLINE":
                    await self.ledger.finish_consumer(
                        self.controller.runtime.authority_lease(),
                        host_activation_id=request["activation_id"],
                        bot_id=bot_id, state="FAILED",
                        error_code="CONTROL_DRIVER_CONSUMER_UNAVAILABLE",
                    )

    async def _inspect_live(self, request: dict, row: dict, bot) -> None:
        controller = self.controller
        registry = controller.registry
        selected = next(
            (item for item in registry.descriptors
             if item.component_id == request["target_component_id"]),
            None,
        )
        if selected is None or selected.artifact.package_digest != request["package_digest"]:
            raise ForestError(
                "HOST_DRIVER_RECOVERY_HOST_MISMATCH",
                "Current Host package differs from the interrupted handoff", 409,
            )
        host_graph = await controller.runtime.generation_authority.current_graph()
        if (
            host_graph is None or registry.graph_identity is None
            or host_graph.graph_id != registry.graph_identity.graph_id
            or host_graph.graph_revision != registry.graph_identity.graph_revision
            or host_graph.activation_id != registry.activation_id
        ):
            raise ForestError(
                "HOST_DRIVER_RECOVERY_HOST_MISMATCH",
                "Current Host graph does not match its runtime", 409,
            )
        sources = {}
        for export in controller.driver_exports():
            if export["kind"] in sources:
                raise ForestError(
                    "CONTROL_DRIVER_KIND_AMBIGUOUS",
                    "Host exports multiple active sources for one driver kind", 409,
                )
            sources[export["kind"]] = export["source_ref"]

        bot_registry = bot.registry
        if bot_registry is None or bot_registry.graph_identity is None:
            raise ForestError(
                "HOST_DRIVER_RECOVERY_BOT_MISMATCH",
                "Managed Bot has no active graph", 409,
            )
        bot_graph = await bot._kernel.runtime.graph_for_activation(
            bot_registry.activation_id
        )
        if (
            bot_graph is None or bot_graph["state"] != "ACTIVE"
            or bot_graph["graph_id"] != bot_registry.graph_identity.graph_id
            or bot_graph["graph_revision"] != bot_registry.graph_identity.graph_revision
        ):
            raise ForestError(
                "HOST_DRIVER_RECOVERY_BOT_MISMATCH",
                "Managed Bot graph differs from durable activation", 409,
            )
        bindings = tuple(sorted({
            binding for descriptor in bot_registry.descriptors
            for binding in descriptor.effect_driver_bindings
        }))
        source_refs = {source for _service, _kind, source in row["source_bindings"]}
        observed_by_key = {}
        for service, kind, source in bindings:
            observed_by_key.setdefault((service, kind), set()).add(source)
        if (
            any(source in source_refs for _service, _kind, source in bindings)
            or any(
                observed_by_key.get((service, kind)) != {sources.get(kind)}
                for service, kind, _source in row["target_bindings"]
            )
        ):
            raise controller._fail(ForestError(
                "HOST_DRIVER_RECOVERY_BINDING_MISMATCH",
                "Managed Bot graph does not bind the current Host driver", 409,
            ))
        lease = controller.runtime.authority_lease()
        if (
            host_graph.activation_id == request["activation_id"]
            and bot_graph["activation_id"] == row["bot_activation_id"]
        ):
            await self.ledger.finish_consumer(
                lease, host_activation_id=request["activation_id"],
                bot_id=row["bot_id"], state="SUCCEEDED", graph=bot_graph,
            )
            return
        await self.ledger.recover_consumer(
            lease, host_activation_id=request["activation_id"],
            bot_id=row["bot_id"],
            host_graph={
                "graph_id": host_graph.graph_id,
                "graph_revision": host_graph.graph_revision,
                "activation_id": host_graph.activation_id,
            },
            bot_graph=bot_graph,
            current_sources=sources,
            bot_bindings=bindings,
        )
