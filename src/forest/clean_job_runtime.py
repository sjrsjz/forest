"""Lifecycle-owned durable scheduler for module-contributed job types."""

from __future__ import annotations

import asyncio
import hashlib
import json

from jsonschema import Draft202012Validator

from forest.builtin_job_shell import execute_job_shell
from forest.errors import ForestError
from forest.job_protocol import DurableJobExecutionRequest
from forest.runtime_generation import BoundGenerationIdentity, PreparedGenerationBinding
from forest.storage.job_scheduler_authority import JobRoute

_STATE_CONTRACT = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {},
    "additionalProperties": False,
}


class CleanJobRuntime:
    """Own job admission and leases while executor modules own task semantics."""

    state_schema = 1
    state_contract = _STATE_CONTRACT

    def __init__(
        self,
        *,
        repository,
        payloads,
        module_configuration,
        grants,
        generation_verifier,
        job_routes,
    ) -> None:
        self.repository = repository
        self.payloads = payloads
        self.module_configuration = module_configuration
        self.grants = grants
        self.generation_verifier = generation_verifier
        self.job_routes = {
            (contribution.job_type, contribution.job_version): (
                owner,
                contribution,
                service,
            )
            for owner, contribution, service in job_routes
        }
        self.runtime_contract = {
            "protocol": "forest.runtime-services/1",
            "services": [{"service_id": "shell.jobs.execute", "version": 1}],
            "requires": [
                {"service_id": contribution.service_id, "version": contribution.service_version}
                for _owner, contribution, _service in job_routes
            ],
        }
        self.generation_identity: BoundGenerationIdentity | None = None
        self.configuration_revision: int | None = None
        self.active = False
        self.failure: str | None = None
        self._closing = False
        self._task: asyncio.Task | None = None
        self._wake = asyncio.Event()
        self._admission = asyncio.Event()
        self._idle = asyncio.Event()
        self._idle.set()

    def runtime_service_handlers(self) -> dict:
        return {("shell.jobs.execute", 1): self.execute_shell_command}

    async def execute_shell_command(self, request):
        await self._admit("CONTROL")
        return await execute_job_shell(self, request)

    async def prepare_generation(self, context) -> PreparedGenerationBinding:
        desired = await self.module_configuration.desired(include_value=True)
        value = desired.value
        if value != {"protocol": "forest.job.scheduler/1"}:
            raise ForestError("JOB_CONFIGURATION_INVALID", "Job configuration is invalid", 500)
        self.configuration_revision = desired.revision
        return PreparedGenerationBinding(
            component_id=context.component_id,
            requires=context.requires,
            package_id=context.package_id,
            package_version=context.package_version,
            package_digest=context.package_digest,
            driver=context.driver,
            trust_level=context.trust_level,
            descriptor_digest=context.descriptor_digest,
            route_contract_digest=context.route_contract_digest,
            management_contract_digest=context.management_contract_digest,
            management_ui_digest=context.management_ui_digest,
            config_revision=desired.revision,
            config_payload_id=desired.payload_id,
            config_payload_digest=desired.payload_digest,
            instance_id=f"instance:job.scheduler:{context.activation_id}",
            worker_incarnation=f"worker:job.scheduler:{context.activation_id}",
            state_schema=context.state_schema,
            state_contract_digest=context.state_contract_digest,
            capability_grants_digest=self.grants.grant_set_digest,
            data_admission=True,
            control_admission=True,
            storage=context.storage,
        )

    async def bind_generation(self, identity: BoundGenerationIdentity) -> None:
        if identity.component_id != "job.scheduler":
            raise ForestError("RUNTIME_GENERATION_BINDING_MISMATCH", "Job binding differs", 409)
        if self.generation_identity is not None and self.generation_identity != identity:
            raise ForestError(
                "RUNTIME_GENERATION_BINDING_MISMATCH",
                "Job Generation identity is immutable",
                409,
            )
        self.generation_identity = identity

    async def start(self) -> None:
        if self.generation_identity is None or self.configuration_revision is None:
            raise ForestError(
                "RUNTIME_GENERATION_IDENTITY_UNAVAILABLE", "Job scheduler is not prepared", 503
            )
        self._task = asyncio.create_task(
            self._run(), name="job-scheduler/" + self.generation_identity.generation_id
        )

    def activate_generation(self) -> None:
        self.active = True
        self._admission.set()
        self._wake.set()

    def notify(self) -> None:
        self._wake.set()

    async def quiesce(self) -> None:
        self.active = False
        self._admission.clear()
        self._wake.set()
        await self._idle.wait()

    async def close(self) -> None:
        self.active = False
        self._closing = True
        self._admission.set()
        self._wake.set()
        if self._task is not None:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None

    def owned_tasks(self) -> tuple:
        return (self._task,) if self._task is not None else ()

    async def _admit(self, lane: str) -> None:
        identity = self.generation_identity
        if not self.active or identity is None:
            raise ForestError("RUNTIME_GENERATION_NOT_ACTIVE", "Job scheduler is not active", 409)
        await self.generation_verifier(
            component_id="job.scheduler",
            generation_id=identity.generation_id,
            admission_lane=lane,
        )

    def _route(self, job_type: str, job_version: int = 1):
        route = self.job_routes.get((job_type, job_version))
        if route is None:
            raise ForestError("JOB_TYPE_NOT_FOUND", "Job type is not available", 404)
        return route

    @staticmethod
    def _route_fact(owner, contribution, service) -> JobRoute:
        frozen = service.route
        if frozen.provider_component_id != owner:
            raise ForestError(
                "JOB_ROUTE_INVALID", "Job executor resolved outside its contributor Generation", 503
            )
        return JobRoute(
            contribution.job_type,
            contribution.job_version,
            contribution.input_schema_digest,
            contribution.max_attempts,
            contribution.timeout_seconds,
            contribution.max_concurrency,
            frozen.provider_component_id,
            frozen.provider_generation_id,
            frozen.service_id,
            frozen.version,
        )

    def _route_facts(self) -> dict[tuple[str, int], JobRoute]:
        return {
            key: self._route_fact(*route)
            for key, route in self.job_routes.items()
        }

    async def enqueue(
        self,
        *,
        job_type: str,
        operation_id: str,
        requested_by: str,
        input_value: dict,
        source_conversation_ref: str | None = None,
    ) -> dict:
        await self._admit("CONTROL")
        owner, contribution, service = self._route(job_type, 1)
        if not Draft202012Validator(contribution.input_schema).is_valid(input_value):
            raise ForestError("JOB_INPUT_INVALID", "Job input does not match its schema", 422)
        return await self.repository.enqueue(
            route=self._route_fact(owner, contribution, service),
            operation_id=operation_id,
            requested_by=requested_by,
            input_value=input_value,
            source_conversation_ref=source_conversation_ref,
            scheduler_generation_id=self.generation_identity.generation_id,
        )

    async def cancel(self, **values) -> dict:
        await self._admit("CONTROL")
        return await self.repository.cancel(
            **values,
            scheduler_generation_id=self.generation_identity.generation_id,
        )

    async def inspect(self, job_id: str) -> dict:
        await self._admit("CONTROL")
        return await self.repository.inspect(job_id)

    async def list_jobs(self, **filters) -> tuple[dict, ...]:
        await self._admit("CONTROL")
        return await self.repository.list_jobs(**filters)

    async def query_jobs(self, **filters) -> dict:
        await self._admit("CONTROL")
        return await self.repository.query_jobs(**filters)

    def job_types(self) -> tuple[dict, ...]:
        return tuple(
            {
                "job_type": contribution.job_type,
                "job_version": contribution.job_version,
                "input_schema": contribution.input_schema,
                "input_schema_digest": contribution.input_schema_digest,
                "max_attempts": contribution.max_attempts,
                "timeout_seconds": contribution.timeout_seconds,
                "max_concurrency": contribution.max_concurrency,
                "owner_component_id": owner,
                "target_generation_id": service.route.provider_generation_id,
            }
            for owner, contribution, service in sorted(
                self.job_routes.values(), key=lambda item: item[1].job_type
            )
        )

    async def _wait(self) -> None:
        try:
            await asyncio.wait_for(self._wake.wait(), timeout=0.25)
        except TimeoutError:
            pass
        self._wake.clear()

    async def _run(self) -> None:
        try:
            while not self._closing:
                await self._admission.wait()
                if self._closing:
                    return
                self._idle.clear()
                claim = None
                try:
                    identity = self.generation_identity
                    if identity is None:
                        raise ForestError(
                            "RUNTIME_GENERATION_IDENTITY_UNAVAILABLE",
                            "Job scheduler has no Generation",
                            503,
                        )
                    claim = await self.repository.claim(
                        worker_generation_id=identity.generation_id,
                        routes=self._route_facts(),
                    )
                    if claim is not None:
                        await self._execute(claim)
                finally:
                    self._idle.set()
                if claim is None or not self._admission.is_set():
                    await self._wait()
        except asyncio.CancelledError:
            raise
        except ForestError as exc:
            self.failure = exc.code
            raise
        except Exception:
            self.failure = "JOB_SCHEDULER_FAILED"
            raise

    async def _fail(self, claim, *, error_code: str, retryable: bool, unknown: bool = False) -> None:
        try:
            await self.repository.fail(
                claim,
                error_code=error_code,
                retryable=retryable,
                unknown=unknown,
            )
        except ForestError as exc:
            if exc.code != "STALE_JOB_CLAIM":
                raise

    async def _renew(self, claim, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=10)
                return
            except TimeoutError:
                await self.repository.renew(claim)

    async def _execute(self, claim) -> None:
        route = self.job_routes.get((claim.job_type, claim.job_version))
        if route is None:
            await self._fail(claim, error_code="JOB_ROUTE_UNAVAILABLE", retryable=False)
            return
        owner, contribution, service = route
        frozen = service.route
        if (
            frozen.provider_component_id != claim.target_component_id
            or frozen.provider_generation_id != claim.target_generation_id
            or frozen.service_id != claim.target_service_id
            or frozen.version != claim.target_service_version
            or owner != claim.target_component_id
        ):
            await self._fail(claim, error_code="JOB_ROUTE_STALE", retryable=False)
            return
        body = await self.payloads.read_bytes(claim.input_payload_id)
        if "sha256:" + hashlib.sha256(body).hexdigest() != claim.input_payload_digest:
            await self._fail(
                claim, error_code="JOB_INPUT_DIGEST_MISMATCH", retryable=False
            )
            return
        try:
            input_value = json.loads(body)
        except (UnicodeDecodeError, json.JSONDecodeError):
            await self._fail(claim, error_code="JOB_INPUT_INVALID", retryable=False)
            return
        if not Draft202012Validator(contribution.input_schema).is_valid(input_value):
            await self._fail(claim, error_code="JOB_INPUT_INVALID", retryable=False)
            return
        if await self.repository.cancellation_requested(claim):
            await self._fail(claim, error_code="JOB_CANCELLED", retryable=False)
            return
        request = DurableJobExecutionRequest(
            claim.job_id,
            claim.job_type,
            claim.job_version,
            claim.attempt_count,
            claim.requested_by,
            input_value,
            lambda: self.repository.cancellation_requested(claim),
        )
        renewal_stop = asyncio.Event()
        renewal = asyncio.create_task(
            self._renew(claim, renewal_stop), name="job-lease/" + claim.job_id
        )
        try:
            async with asyncio.timeout(claim.timeout_seconds):
                result = await service(request)
            if not isinstance(result, dict):
                raise ForestError("JOB_RESULT_INVALID", "Job executor returned no object", 502)
            await self.repository.succeed(claim, result=result)
        except asyncio.CancelledError:
            raise
        except TimeoutError:
            await self._fail(claim, error_code="JOB_EXECUTION_TIMEOUT", retryable=True)
        except ForestError as exc:
            if exc.code == "STALE_JOB_CLAIM":
                return
            await self._fail(
                claim,
                error_code=exc.code,
                retryable=exc.status >= 500,
                unknown=False,
            )
        except Exception:
            await self._fail(
                claim,
                error_code="JOB_EXECUTION_UNKNOWN",
                retryable=False,
                unknown=True,
            )
        finally:
            renewal_stop.set()
            renewal.cancel()
            await asyncio.gather(renewal, return_exceptions=True)
