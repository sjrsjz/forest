"""Generation-fenced, transaction-bound kernel Commands for official UI packages.

These ports never enter the package Worker. A handler may prepare an opaque value
outside the transaction, but its only durable effect must occur in execute_in.
"""

from __future__ import annotations

import asyncio
from copy import deepcopy
from dataclasses import asdict

from forest.control_kernel_management import (
    ControlKernelManagementAuthority,
    ControlKernelQueryAuthority,
)
from forest.errors import ForestError
from forest.management_operation_identity import control_management_operation_id
from forest.security import hash_password


class DeploymentKernelQueryAuthority(ControlKernelQueryAuthority):
    scope_kind = "deployment"


class BootstrapCatalogKernelQuery:
    def __init__(self, registry) -> None:
        self.registry = registry

    async def query(self, payload: dict) -> dict:
        if payload:
            raise ForestError(
                "BOOTSTRAP_CATALOG_QUERY_INVALID", "Bootstrap catalog query is invalid", 422,
            )
        return {
            "protocol": "forest.bootstrap-configuration-catalog/1",
            "items": self.registry.facts(),
        }


class BotCatalogKernelQuery:
    """Read the kernel-owned Bot directory through one bounded page port."""

    def __init__(self, catalog) -> None:
        self.catalog = catalog

    async def query(self, payload: dict) -> dict:
        return await self.catalog.catalog_page(**payload)


class RetirementJobsKernelQuery:
    """Project kernel Bot identities with their sole retirement Job."""

    def __init__(self, jobs) -> None:
        self.jobs = jobs

    async def query(self, payload: dict) -> dict:
        return await self.jobs.page(**payload)


class ProvisioningJobsKernelQuery:
    """Expose the kernel-owned provisioning ledger through a bounded page port."""

    def __init__(self, jobs) -> None:
        self.jobs = jobs

    async def query(self, payload: dict) -> dict:
        return await self.jobs.page(**payload)


class StorageRequestsKernelQuery:
    """Page frozen Host storage requests without exposing role credentials."""

    def __init__(self, dispatch) -> None:
        self.dispatch = dispatch

    async def query(self, payload: dict) -> dict:
        return await self.dispatch.page(**payload)


class SecretOperationKernelQuery:
    """Recover a committed Secret write for its authenticated Web subject."""

    def __init__(self, secrets) -> None:
        self.secrets = secrets

    async def query(self, payload: dict) -> dict:
        operation_id = control_management_operation_id(
            "deployment", self.secrets.store.deployment_id,
            "deployment.secrets", "secret.put", payload["idempotency_key"],
        )
        return await self.secrets.web_operation_result(
            operation_id=operation_id, web_subject_ref=payload["requested_by"],
        )


class DeploymentKernelManagementAuthority(ControlKernelManagementAuthority):
    scope_kind = "deployment"


class BotSetDesiredKernelCommand:
    """Commit one Bot intent change under the caller's Web receipt and Generation."""

    def __init__(self, catalog) -> None:
        self.catalog = catalog

    async def prepare(self, payload: dict, *, operation_id: str) -> dict:
        return dict(payload)

    async def execute_in(self, conn, prepared: dict, *, operation_id: str) -> dict:
        return await self.catalog.set_desired_in(
            conn, operation_id=operation_id, **prepared,
        )


class BotBeginRetirementKernelCommand:
    """Freeze Bot startup and admit its kernel Job under one Web transaction."""

    def __init__(self, catalog, *, notify=None) -> None:
        self.catalog = catalog
        self.notify = notify

    async def prepare(self, payload: dict, *, operation_id: str) -> dict:
        return dict(payload)

    async def execute_in(self, conn, prepared: dict, *, operation_id: str) -> dict:
        await self.catalog.require_host_pin_for_retirement_in(
            conn, prepared["bot_id"],
        )
        return await self.catalog.begin_retirement_in(
            conn, operation_id=operation_id, **prepared,
        )

    def after_commit(self, result: dict) -> None:
        if self.notify is not None:
            self.notify()


class RetirementRequeueKernelCommand:
    """Re-admit only the same blocked Job under Web receipt and revision CAS."""

    def __init__(self, jobs, *, notify=None) -> None:
        self.jobs = jobs
        self.notify = notify

    async def prepare(self, payload: dict, *, operation_id: str) -> dict:
        return dict(payload)

    async def execute_in(self, conn, prepared: dict, *, operation_id: str) -> dict:
        return await self.jobs.requeue(
            operation_id=operation_id, conn=conn, **prepared,
        )

    def after_commit(self, result: dict) -> None:
        if self.notify is not None:
            self.notify()


class StorageRequestKernelCommand:
    """Freeze one Host storage intent and its delivery row under Web admission."""

    def __init__(self, requests, *, host_id: str) -> None:
        self.requests = requests
        self.host_id = host_id

    async def prepare(self, payload: dict, *, operation_id: str) -> dict:
        return dict(payload)

    async def execute_in(self, conn, prepared: dict, *, operation_id: str) -> dict:
        request = await self.requests.request(
            operation_id=operation_id, conn=conn, host_id=self.host_id, **prepared,
        )
        return {
            "request_id": request.request_id,
            "bot_id": request.bot_id,
            "profile_ref": request.profile_ref,
        }


class _PreparedSecretPut:
    __slots__ = ("secret_ref", "purpose", "plaintext", "expected_revision")

    def __init__(self, payload: dict) -> None:
        self.secret_ref = payload["secret_ref"]
        self.purpose = payload["purpose"]
        try:
            self.plaintext = payload["plaintext"].encode("utf-8")
        except UnicodeError as exc:
            raise ForestError(
                "SECRET_INPUT_INVALID", "Secret text is not UTF-8", 422,
            ) from exc
        self.expected_revision = payload["expected_revision"]


class SecretPutKernelCommand:
    """Keep plaintext and master-key use inside the trusted deployment process."""

    def __init__(self, secrets) -> None:
        self.secrets = secrets

    async def prepare(self, payload: dict, *, operation_id: str) -> _PreparedSecretPut:
        return _PreparedSecretPut(payload)

    async def execute_in(
        self, conn, prepared: _PreparedSecretPut, *, operation_id: str,
    ) -> dict:
        saved = await self.secrets.put_in(
            conn, operation_id=operation_id, secret_ref=prepared.secret_ref,
            purpose=prepared.purpose, plaintext=prepared.plaintext,
            expected_revision=prepared.expected_revision,
        )
        return asdict(saved)


class WebAccountKernelCommand:
    """One declared account operation bound to the kernel identity authority."""

    _METHODS = {
        "deployment.account-create": "create_account",
        "deployment.account-update": "update_account",
        "deployment.account-password": "change_password",
        "deployment.account-revoke-sessions": "revoke_sessions",
    }

    def __init__(self, identity, kind: str) -> None:
        self.identity = identity
        self.method = self._METHODS[kind]

    async def prepare(self, payload: dict, *, operation_id: str) -> dict:
        prepared = dict(payload)
        if self.method in {"create_account", "change_password"}:
            prepared["_prepared_password_hash"] = await asyncio.to_thread(
                hash_password, prepared["password"],
            )
        return prepared

    async def execute_in(self, conn, prepared: dict, *, operation_id: str) -> dict:
        subject = await getattr(self.identity, self.method)(
            operation_id=operation_id, conn=conn, **prepared,
        )
        return subject.as_dict()


class BootstrapProfileKernelCommand:
    """Freeze module-owned Bootstrap seeds under the deployment transaction."""

    def __init__(self, profiles, registry) -> None:
        self.profiles = profiles
        self.registry = registry

    async def prepare(self, payload: dict, *, operation_id: str) -> dict:
        prepared = dict(payload)
        prepared["components"] = deepcopy(
            self.registry.validate_components(prepared["components"]),
        )
        return prepared

    async def execute_in(self, conn, prepared: dict, *, operation_id: str) -> dict:
        saved = await self.profiles.create(
            operation_id=operation_id, conn=conn,
            schema_digests=self.registry.schema_digests(),
            package_digests=self.registry.package_digests(),
            **prepared,
        )
        return {
            "profile_ref": saved.profile_ref,
            "bot_id": saved.bot_id,
            "manifest_digest": saved.manifest_digest,
        }


class ProvisioningJobKernelCommand:
    """One exact Job transition; enqueue wakes the supervisor after commit."""

    _METHODS = {
        "deployment.provisioning-enqueue": "enqueue",
        "deployment.provisioning-cancel": "cancel",
    }

    def __init__(self, jobs, kind: str, *, notify=None) -> None:
        self.jobs = jobs
        self.method = self._METHODS[kind]
        self.notify = notify if self.method == "enqueue" else None

    async def prepare(self, payload: dict, *, operation_id: str) -> dict:
        return dict(payload)

    async def execute_in(self, conn, prepared: dict, *, operation_id: str) -> dict:
        saved = await getattr(self.jobs, self.method)(
            operation_id=operation_id, conn=conn, **prepared,
        )
        return {
            key: saved[key] for key in (
                "job_id", "bot_id", "state", "revision", "attempt_no",
                "max_attempts", "last_error_code",
            )
        }

    def after_commit(self, result: dict) -> None:
        if self.notify is not None:
            self.notify()


class ProvisioningReconcileKernelCommand:
    """Resolve a blocked Job only after reading the dedicated Bot DB receipt."""

    def __init__(self, jobs, catalog, storage_profiles, bootstrap_profiles, provisioner):
        self.jobs = jobs
        self.catalog = catalog
        self.storage_profiles = storage_profiles
        self.bootstrap_profiles = bootstrap_profiles
        self.provisioner = provisioner

    async def prepare(self, payload: dict, *, operation_id: str) -> dict:
        row = await self.jobs.inspect(payload["job_id"])
        if row["state"] not in {"BLOCKED_UNKNOWN", "SUCCEEDED"}:
            raise ForestError(
                "PROVISIONING_NOT_RECONCILABLE",
                "Only blocked provisioning Jobs accept materialization proof", 409,
            )
        storage = await self.storage_profiles.resolve(
            profile_ref=row["storage_profile_ref"],
            expected_bot_id=row["bot_id"],
            expected_storage_generation=row["storage_generation"],
        )
        profile = await self.bootstrap_profiles.load(row["bootstrap_profile_ref"])
        if (
            profile.bot_id != row["bot_id"]
            or profile.storage_profile_ref != row["storage_profile_ref"]
            or profile.storage_generation != row["storage_generation"]
            or profile.manifest_digest != row["bootstrap_manifest_digest"]
        ):
            raise ForestError(
                "PROVISIONING_INPUT_TAMPERED",
                "Frozen Bootstrap profile differs from the Job", 409,
            )
        proof = await self.provisioner.read_materialization_receipt(storage=storage)
        if proof is None:
            raise ForestError(
                "PROVISIONING_PROOF_ABSENT",
                "Bot database has no completed materialization receipt", 409,
            )
        return {**payload, "proof": proof}

    async def execute_in(self, conn, prepared: dict, *, operation_id: str) -> dict:
        saved = await self.jobs.reconcile_ready(
            operation_id=operation_id,
            job_id=prepared["job_id"],
            expected_revision=prepared["expected_revision"],
            proof=prepared["proof"],
            catalog=self.catalog,
            conn=conn,
        )
        return {
            key: saved[key] for key in (
                "job_id", "bot_id", "state", "revision", "attempt_no",
                "max_attempts", "last_error_code",
            )
        }
