"""Process-local admission fence for cross-scope Host driver bindings."""

from __future__ import annotations

import asyncio

from forest.errors import ForestError


class ControlDriverBindingGate:
    """Serialize Bot binding publication with Host provider replacement.

    The registry remains the authority for each Bot's binding facts. This gate
    only keeps a process-local snapshot while that Bot graph is active. It
    gives the Host controller the exact active consumers before replacement.
    The joint coordinator drains and republishes those graphs while holding
    this fence; without that coordinator, replacement is rejected.
    """

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._active: dict[str, tuple[object, frozenset[str]]] = {}

    async def __aenter__(self) -> ControlDriverBindingGate:
        await self._lock.acquire()
        return self

    async def __aexit__(self, _type, _value, _traceback) -> None:
        self._lock.release()

    @staticmethod
    def _binding_sources(host: object) -> frozenset[str]:
        registry = getattr(host, "registry", None)
        if registry is None:
            raise ForestError(
                "CONTROL_DRIVER_CONSUMER_INDETERMINATE",
                "Bot Graph has no active binding snapshot", 503,
            )
        return frozenset(
            source
            for descriptor in registry.descriptors
            for _service, _kind, source in descriptor.effect_driver_bindings
            if source != "forest.host-native/1"
        )

    def attach(self, host: object) -> None:
        bot_id = host.settings.bot_id
        previous = self._active.get(bot_id)
        if previous is not None and previous[0] is not host:
            raise ForestError(
                "CONTROL_DRIVER_CONSUMER_CONFLICT",
                "Another Bot host owns the active driver binding", 409,
            )
        self._active[bot_id] = (host, self._binding_sources(host))

    def refresh(self, host: object) -> None:
        bot_id = host.settings.bot_id
        previous = self._active.get(bot_id)
        if previous is None or previous[0] is not host:
            raise ForestError(
                "CONTROL_DRIVER_CONSUMER_NOT_BOUND",
                "Bot host has no active driver binding", 409,
            )
        self._active[bot_id] = (host, self._binding_sources(host))

    def detach(self, host: object) -> None:
        bot_id = host.settings.bot_id
        previous = self._active.get(bot_id)
        if previous is not None and previous[0] is host:
            del self._active[bot_id]

    def require_empty(self) -> None:
        if self._active:
            raise ForestError(
                "CONTROL_DRIVER_CONSUMERS_ACTIVE",
                "Host provider cannot close while Bot Graphs remain active", 409,
                details={"bot_ids": sorted(self._active)},
            )

    def require_unbound(self, exports: tuple[dict, ...]) -> None:
        affected = self.consumers_for(exports)
        if affected:
            raise ForestError(
                "CONTROL_DRIVER_CONSUMERS_ACTIVE",
                "Host driver replacement requires a joint Bot transition", 409,
                details={"bot_ids": [host.settings.bot_id for host in affected]},
            )

    def consumers_for(self, exports: tuple[dict, ...]) -> tuple[object, ...]:
        """Return active consumers of exact source identities in Bot ID order."""

        sources = frozenset(item["source_ref"] for item in exports)
        return tuple(
            host for _bot_id, (host, bindings) in sorted(self._active.items())
            if sources & bindings
        )
