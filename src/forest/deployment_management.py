"""Deployment-scoped management components contributed to the Supervisor WebUI."""

from __future__ import annotations

from dataclasses import asdict, dataclass

from forest.errors import ForestError
from forest.official_bot_provisioning import official_bootstrap_schema_digests
from forest.runtime_modules import RuntimeManagementContribution


def _fields(payload: object, required: set[str], optional: set[str] = frozenset()) -> dict:
    if not isinstance(payload, dict) or not required <= set(payload) or set(payload) - required - optional:
        raise ForestError("DEPLOYMENT_MANAGEMENT_INPUT_INVALID", "Action fields differ", 422)
    return payload


@dataclass(frozen=True)
class DeploymentManagementComponent:
    component_id: str
    contract: object
    contributions: tuple[RuntimeManagementContribution, ...]

    def describe(self) -> dict:
        return {
            "component_id": self.component_id,
            "state": "ACTIVE",
            "generation_id": "deployment/" + self.component_id + "/1",
            "management_contributions": [
                contribution.as_fact(public=True) for contribution in self.contributions
            ],
        }


class DeploymentCatalogManagement:
    def __init__(self, *, catalog, supervisor) -> None:
        self.catalog = catalog
        self.supervisor = supervisor

    async def query(self, query_id: str, payload: dict):
        if query_id != "bots.list" or payload:
            raise ForestError("MODULE_MANAGEMENT_OPERATION_NOT_FOUND", "Query does not exist", 404)
        return {"items": list(await self.catalog.list_bots())}

    async def action(self, action_id: str, *, operation_id: str, payload: dict):
        if action_id != "bot.set-desired":
            raise ForestError("MODULE_MANAGEMENT_OPERATION_NOT_FOUND", "Action does not exist", 404)
        values = _fields(payload, {"bot_id", "desired_state", "expected_revision"})
        saved = await self.catalog.set_desired(operation_id=operation_id, **values)
        self.supervisor.notify()
        return saved


class DeploymentSecretManagement:
    def __init__(self, *, secrets) -> None:
        self.secrets = secrets

    async def query(self, query_id: str, payload: dict):
        if query_id != "secrets.list":
            raise ForestError("MODULE_MANAGEMENT_OPERATION_NOT_FOUND", "Query does not exist", 404)
        values = _fields(payload, set(), {"limit"})
        rows = await self.secrets.list_metadata(limit=values.get("limit", 200))
        return {"items": [asdict(row) for row in rows]}

    async def action(self, action_id: str, *, operation_id: str, payload: dict):
        if action_id != "secret.put":
            raise ForestError("MODULE_MANAGEMENT_OPERATION_NOT_FOUND", "Action does not exist", 404)
        values = _fields(payload, {"secret_ref", "purpose", "plaintext", "expected_revision"})
        plaintext = values.pop("plaintext")
        if not isinstance(plaintext, str):
            raise ForestError("DEPLOYMENT_MANAGEMENT_INPUT_INVALID", "Secret must be text", 422)
        saved = await self.secrets.put(
            operation_id=operation_id,
            plaintext=plaintext.encode(),
            **values,
        )
        return asdict(saved)


class DeploymentProvisioningManagement:
    def __init__(
        self,
        *,
        secrets,
        storage_profiles,
        bootstrap_profiles,
        jobs,
        supervisor,
        database_allocator=None,
    ) -> None:
        self.secrets = secrets
        self.storage_profiles = storage_profiles
        self.bootstrap_profiles = bootstrap_profiles
        self.jobs = jobs
        self.supervisor = supervisor
        self.database_allocator = database_allocator

    async def query(self, query_id: str, payload: dict):
        if query_id == "provisioning.jobs.list":
            values = _fields(payload, set(), {"limit"})
            return {"items": list(await self.jobs.list(limit=values.get("limit", 100)))}
        if query_id == "provisioning.schemas":
            if payload:
                raise ForestError("DEPLOYMENT_MANAGEMENT_INPUT_INVALID", "Query takes no fields", 422)
            return {"schema_digests": official_bootstrap_schema_digests()}
        raise ForestError("MODULE_MANAGEMENT_OPERATION_NOT_FOUND", "Query does not exist", 404)

    async def action(self, action_id: str, *, operation_id: str, payload: dict):
        if action_id == "storage.create-local":
            if self.database_allocator is None:
                raise ForestError(
                    "STORAGE_ALLOCATION_UNAVAILABLE",
                    "Local PostgreSQL allocation is unavailable",
                    409,
                )
            values = _fields(
                payload,
                {"profile_ref", "bot_id", "storage_generation", "dsn_secret_ref"},
            )
            try:
                secret = await self.secrets.metadata(values["dsn_secret_ref"])
            except ForestError as exc:
                if exc.code != "SECRET_NOT_FOUND":
                    raise
                target_dsn = self.database_allocator.new_target_dsn(values["bot_id"])
                await self.secrets.put(
                    operation_id=operation_id + "/dsn",
                    secret_ref=values["dsn_secret_ref"],
                    purpose="storage.postgresql.dsn",
                    plaintext=target_dsn.encode(),
                    expected_revision=0,
                )
            else:
                if secret.purpose != "storage.postgresql.dsn":
                    raise ForestError(
                        "STORAGE_PROFILE_SECRET_PURPOSE_INVALID",
                        "Storage profile requires a PostgreSQL DSN secret",
                        409,
                    )
                target_dsn = await self.secrets.resolve_text(values["dsn_secret_ref"])
                self.database_allocator.validate_target(values["bot_id"], target_dsn)
            saved = await self.storage_profiles.create(
                operation_id=operation_id + "/profile",
                **values,
            )
            return asdict(saved)
        if action_id == "storage.create":
            values = _fields(
                payload,
                {"profile_ref", "bot_id", "storage_generation", "dsn_secret_ref"},
            )
            return asdict(await self.storage_profiles.create(operation_id=operation_id, **values))
        if action_id == "bootstrap.create":
            values = _fields(
                payload,
                {
                    "profile_ref",
                    "bot_id",
                    "storage_profile_ref",
                    "storage_generation",
                    "components",
                    "schema_digests",
                },
            )
            saved = await self.bootstrap_profiles.create(operation_id=operation_id, **values)
            return {
                "profile_ref": saved.profile_ref,
                "bot_id": saved.bot_id,
                "manifest_digest": saved.manifest_digest,
            }
        if action_id == "provisioning.enqueue":
            values = _fields(
                payload,
                {
                    "job_id",
                    "bot_id",
                    "display_name",
                    "storage_profile_ref",
                    "storage_generation",
                    "bootstrap_profile_ref",
                },
                {"max_attempts"},
            )
            saved = await self.jobs.enqueue(operation_id=operation_id, **values)
            self.supervisor.notify()
            return saved
        if action_id == "provisioning.cancel":
            values = _fields(payload, {"job_id", "expected_revision"})
            return await self.jobs.cancel(operation_id=operation_id, **values)
        raise ForestError("MODULE_MANAGEMENT_OPERATION_NOT_FOUND", "Action does not exist", 404)


def deployment_management_components(
    *,
    catalog,
    secrets,
    storage_profiles,
    bootstrap_profiles,
    jobs,
    supervisor,
    database_allocator=None,
):
    def contribution(identity: str, label: str, order: int) -> tuple[RuntimeManagementContribution, ...]:
        return (
            RuntimeManagementContribution(
                identity,
                label,
                ("deployment",),
                f"/static/modules/{identity}.html",
                f"/static/{identity}.js",
                style=f"/static/modules/{identity}.css",
                group="deployment",
                order=order,
            ),
        )

    return (
        DeploymentManagementComponent(
            "deployment.catalog",
            DeploymentCatalogManagement(catalog=catalog, supervisor=supervisor),
            contribution("deployment-catalog", "Bot 运行目录", 10),
        ),
        DeploymentManagementComponent(
            "deployment.secrets",
            DeploymentSecretManagement(secrets=secrets),
            contribution("deployment-secrets", "Secret 引用", 20),
        ),
        DeploymentManagementComponent(
            "deployment.provisioning",
            DeploymentProvisioningManagement(
                secrets=secrets,
                storage_profiles=storage_profiles,
                bootstrap_profiles=bootstrap_profiles,
                jobs=jobs,
                supervisor=supervisor,
                database_allocator=database_allocator,
            ),
            contribution("deployment-provisioning", "Bot 供应", 30),
        ),
    )
