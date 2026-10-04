"""Generation-fenced, transaction-bound kernel Commands for official UI packages.

These ports never enter the package Worker. A handler may prepare an opaque value
outside the transaction, but its only durable effect must occur in execute_in.
"""

from __future__ import annotations

import asyncio
from copy import deepcopy
from dataclasses import asdict

import psycopg

from forest.errors import ForestError
from forest.security import hash_password


class DeploymentKernelQueryAuthority:
    """Exact official read ports, fenced to the active deployment Generation."""

    def __init__(
        self, store, generations, *, component_id: str, ports: tuple[dict, ...],
        handlers: dict,
    ) -> None:
        self.store = store
        self.generations = generations
        self.component_id = component_id
        self.ports = {item["name"]: item["kind"] for item in ports}
        self.handlers = dict(handlers)

    async def invoke_query(
        self, scope, declaration: dict, payload: dict, *, parent_call_id: str,
        timeout_seconds: float,
    ) -> dict:
        if (
            scope.scope_kind != "deployment"
            or scope.scope_id != self.store.deployment_id
            or scope.component_id != self.component_id
            or scope.bot_id is not None
            or self.ports.get(declaration["name"]) != declaration["kind"]
        ):
            raise ForestError(
                "KERNEL_MANAGEMENT_BINDING_INVALID",
                "Kernel Query differs from its official deployment binding", 409,
            )
        handler = self.handlers.get((self.component_id, declaration["kind"]))
        if handler is None:
            raise ForestError(
                "KERNEL_MANAGEMENT_PORT_UNAVAILABLE",
                "Kernel management Query has no bound handler", 503,
            )
        async with asyncio.timeout(timeout_seconds):
            await self.generations.verify_active_generation(
                component_id=self.component_id, generation_id=scope.generation_id,
                admission_lane="DATA",
            )
            result = await handler.query(payload)
            await self.generations.verify_active_generation(
                component_id=self.component_id, generation_id=scope.generation_id,
                admission_lane="DATA",
            )
        return result


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


class DeploymentKernelManagementAuthority:
    def __init__(
        self, store, generations, *, component_id: str, ports: tuple[dict, ...],
        handlers: dict,
    ) -> None:
        self.store = store
        self.generations = generations
        self.component_id = component_id
        self.ports = {item["name"]: item["kind"] for item in ports}
        self.handlers = dict(handlers)

    async def invoke_management_action(
        self, scope, declaration: dict, payload: dict, *, parent_call_id: str,
        timeout_seconds: float, before_write, management_authorization,
        result_validator,
    ) -> dict:
        if (
            scope.scope_kind != "deployment"
            or scope.scope_id != self.store.deployment_id
            or scope.component_id != self.component_id
            or scope.bot_id is not None
            or scope.call_operation_id is None
            or self.ports.get(declaration["name"]) != declaration["kind"]
            or management_authorization is None
            or not callable(before_write)
        ):
            raise ForestError(
                "KERNEL_MANAGEMENT_BINDING_INVALID",
                "Kernel Command differs from its official deployment binding", 409,
            )
        handler = self.handlers.get((self.component_id, declaration["kind"]))
        if handler is None:
            raise ForestError(
                "KERNEL_MANAGEMENT_PORT_UNAVAILABLE",
                "Kernel management Command has no bound handler", 503,
            )
        prepared = await handler.prepare(payload, operation_id=scope.call_operation_id)
        try:
            async with self.store.pool.connection() as conn, conn.transaction():
                await self.generations.verify_active_generation_in(
                    conn, component_id=self.component_id,
                    generation_id=scope.generation_id, admission_lane="CONTROL",
                )
                await before_write(conn)
                result = await handler.execute_in(
                    conn, prepared, operation_id=scope.call_operation_id,
                )
                if not result_validator(result):
                    raise ForestError(
                        "MODULE_MANAGEMENT_OUTPUT_INVALID",
                        "Kernel Command result violates its frozen package schema", 502,
                    )
            after_commit = getattr(handler, "after_commit", None)
            if after_commit is not None:
                after_commit(result)
            return result
        except ForestError:
            raise
        except psycopg.Error as exc:
            raise ForestError(
                "KERNEL_MANAGEMENT_TRANSACTION_FAILED",
                "Kernel management Command transaction failed", 503,
            ) from exc


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


class LocalStorageProfileKernelCommand:
    """Atomically bind one locally allocated DSN Secret to its profile."""

    def __init__(self, secrets, storage_profiles, allocator) -> None:
        self.secrets = secrets
        self.storage_profiles = storage_profiles
        self.allocator = allocator

    async def prepare(self, payload: dict, *, operation_id: str) -> dict:
        if self.allocator is None:
            raise ForestError(
                "STORAGE_ALLOCATION_UNAVAILABLE",
                "Local PostgreSQL allocation is unavailable", 409,
            )
        return dict(payload)

    async def execute_in(self, conn, prepared: dict, *, operation_id: str) -> dict:
        await conn.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
            (f"forest.deployment.local-storage/{operation_id}",),
        )
        profile_operation_id = operation_id + "/profile"
        replay = await (
            await conn.execute(
                "SELECT 1 FROM forest_deployment.storage_profile_operations "
                "WHERE operation_id=%s",
                (profile_operation_id,),
            )
        ).fetchone()
        if replay is None:
            secret_ref = prepared["dsn_secret_ref"]
            try:
                secret = await self.secrets.metadata_in(conn, secret_ref)
            except ForestError as exc:
                if exc.code != "SECRET_NOT_FOUND":
                    raise
                target_dsn = self.allocator.new_target_dsn(prepared["bot_id"])
                await self.secrets.put_in(
                    conn, operation_id=operation_id + "/dsn",
                    secret_ref=secret_ref, purpose="storage.postgresql.dsn",
                    plaintext=target_dsn.encode(), expected_revision=0,
                )
            else:
                if secret.purpose != "storage.postgresql.dsn":
                    raise ForestError(
                        "STORAGE_PROFILE_SECRET_PURPOSE_INVALID",
                        "Storage profile requires a PostgreSQL DSN secret", 409,
                    )
                target_dsn = await self.secrets.resolve_text_in(conn, secret_ref)
                self.allocator.validate_target(prepared["bot_id"], target_dsn)
        saved = await self.storage_profiles.create(
            operation_id=profile_operation_id, conn=conn, **prepared,
        )
        return asdict(saved)


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


class StorageProfileKernelCommand:
    def __init__(self, profiles) -> None:
        self.profiles = profiles

    async def prepare(self, payload: dict, *, operation_id: str) -> dict:
        return dict(payload)

    async def execute_in(self, conn, prepared: dict, *, operation_id: str) -> dict:
        saved = await self.profiles.create(
            operation_id=operation_id, conn=conn, **prepared,
        )
        return asdict(saved)


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
