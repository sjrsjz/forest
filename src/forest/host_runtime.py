"""Host control scope supplied to host-owned package Generations."""

from __future__ import annotations

from forest.errors import ForestError
from forest.host_service_ledger import HostPackageServiceOperationLedger
from forest.storage.host_generation import HostGenerationAuthority
from forest.storage.host_identity import HostLease
from forest.storage.host_schema_bundles import HostSchemaBundleInstaller


class HostRuntimeScope:
    scope_kind = "host"

    def __init__(self, store, supervisor) -> None:
        self.store = store
        self.scope_id = store.host_id
        self.supervisor = supervisor
        self.generation_authority = HostGenerationAuthority(store)
        self.schema_bundle_installer = HostSchemaBundleInstaller(store)
        self.service_operation_ledger = HostPackageServiceOperationLedger(
            store, self.generation_authority,
        )

    def authority_lease(self, *, allow_closing: bool = False) -> HostLease:
        lease = self.supervisor.authority_lease(allow_closing=allow_closing)
        if type(lease) is not HostLease or lease.host_id != self.scope_id:
            raise ForestError(
                "HOST_RUNTIME_UNAVAILABLE", "Host Supervisor lease is unavailable", 503,
            )
        return lease
