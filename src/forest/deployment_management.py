"""Deployment-scoped management components contributed to the Supervisor WebUI."""

from __future__ import annotations

from dataclasses import asdict, dataclass

from forest.bootstrap_contributions import BootstrapConfigurationRegistry
from forest.deployment_action_authorization import authorize_deployment_action_in
from forest.deployment_kernel_management import LocalStorageProfileKernelCommand
from forest.errors import ForestError
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


class DeploymentProvisioningManagement:
    def __init__(
        self,
        *,
        secrets,
        storage_profiles,
        bootstrap_profiles,
        jobs,
        supervisor,
        bootstrap_contributions,
        database_allocator=None,
    ) -> None:
        self.storage_profiles = storage_profiles
        self.bootstrap_profiles = bootstrap_profiles
        self.jobs = jobs
        self.supervisor = supervisor
        self.bootstrap_registry = BootstrapConfigurationRegistry(
            tuple(bootstrap_contributions)
        )
        self.local_storage_command = LocalStorageProfileKernelCommand(
            secrets, storage_profiles, database_allocator,
        )

    async def query(self, query_id: str, payload: dict):
        if query_id == "provisioning.jobs.list":
            values = _fields(payload, set(), {"limit"})
            return {"items": list(await self.jobs.list(limit=values.get("limit", 100)))}
        if query_id == "provisioning.bootstrap-catalog":
            if payload:
                raise ForestError("DEPLOYMENT_MANAGEMENT_INPUT_INVALID", "Query takes no fields", 422)
            return {
                "protocol": "forest.bootstrap-configuration-catalog/1",
                "items": self.bootstrap_registry.facts(),
            }
        raise ForestError("MODULE_MANAGEMENT_OPERATION_NOT_FOUND", "Query does not exist", 404)

    async def action(
        self, action_id: str, *, operation_id: str, payload: dict, authorization=None
    ):
        if action_id == "storage.create-local":
            values = _fields(
                payload,
                {"profile_ref", "bot_id", "storage_generation", "dsn_secret_ref"},
            )
            prepared = await self.local_storage_command.prepare(
                values, operation_id=operation_id,
            )
            async with self.storage_profiles.store.transaction() as conn:
                await authorize_deployment_action_in(
                    conn, authorization, operation_id=operation_id,
                )
                return await self.local_storage_command.execute_in(
                    conn, prepared, operation_id=operation_id,
                )
        if action_id == "storage.create":
            values = _fields(
                payload,
                {"profile_ref", "bot_id", "storage_generation", "dsn_secret_ref"},
            )
            return asdict(
                await self.storage_profiles.create(
                    operation_id=operation_id, authorization=authorization, **values
                )
            )
        if action_id == "bootstrap.create":
            values = _fields(
                payload,
                {
                    "profile_ref",
                    "bot_id",
                    "storage_profile_ref",
                    "storage_generation",
                    "components",
                },
            )
            values["components"] = self.bootstrap_registry.validate_components(
                values["components"]
            )
            saved = await self.bootstrap_profiles.create(
                operation_id=operation_id,
                schema_digests=self.bootstrap_registry.schema_digests(),
                authorization=authorization,
                **values,
            )
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
            saved = await self.jobs.enqueue(
                operation_id=operation_id, authorization=authorization, **values
            )
            self.supervisor.notify()
            return saved
        if action_id == "provisioning.cancel":
            values = _fields(payload, {"job_id", "expected_revision"})
            return await self.jobs.cancel(
                operation_id=operation_id, authorization=authorization, **values
            )
        raise ForestError("MODULE_MANAGEMENT_OPERATION_NOT_FOUND", "Action does not exist", 404)


def deployment_management_components(
    *,
    secrets,
    storage_profiles,
    bootstrap_profiles,
    jobs,
    supervisor,
    bootstrap_contributions,
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

    components = [
        DeploymentManagementComponent(
            "deployment.provisioning",
            DeploymentProvisioningManagement(
                secrets=secrets,
                storage_profiles=storage_profiles,
                bootstrap_profiles=bootstrap_profiles,
                jobs=jobs,
                supervisor=supervisor,
                bootstrap_contributions=bootstrap_contributions,
                database_allocator=database_allocator,
            ),
            contribution("deployment-provisioning", "Bot 供应", 30),
        ),
    ]
    return tuple(components)
