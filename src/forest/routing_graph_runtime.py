"""Lifecycle owner for durable component Event and ComponentJob routing."""

from __future__ import annotations

import asyncio

from jsonschema import Draft202012Validator

from forest.errors import ForestError
from forest.routing_dispatcher import RoutingDispatcher
from forest.routing_protocol import (
    PublishComponentEvent,
    ScheduleComponentJob,
)
from forest.runtime_contracts import (
    FrozenServiceRoute,
    RoutedServiceEndpoint,
    RuntimeServiceInvocation,
)
from forest.runtime_generation import BoundGenerationIdentity, PreparedGenerationBinding
from forest.storage.routing_graph_types import (
    ComponentJobRoute,
    EventPublicationRoute,
    EventSubscriptionRoute,
)

_STATE_CONTRACT = {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "properties": {},
    "additionalProperties": False,
}


class RoutingGraphRuntime:
    """Own route publication, durable claims, and dispatch; no business semantics."""

    state_schema = 1
    state_contract = _STATE_CONTRACT

    def __init__(
        self,
        *,
        directory,
        event_commands,
        job_commands,
        payloads,
        module_configuration,
        grants,
        event_publications,
        event_subscriptions,
        component_jobs,
    ) -> None:
        self.directory = directory
        self.event_commands = event_commands
        self.job_commands = job_commands
        self.payloads = payloads
        self.module_configuration = module_configuration
        self.grants = grants
        self._event_publications = {
            (owner, contribution.topic, contribution.contract): (
                contribution,
                generation,
            )
            for owner, contribution, generation in event_publications
        }
        self._event_subscriptions = {
            (contribution.subscriber_id, contribution.contract_major): (
                owner,
                contribution,
                service,
            )
            for owner, contribution, service in event_subscriptions
        }
        self._component_jobs = {
            (owner, contribution.handler_id): (contribution, service)
            for owner, contribution, service in component_jobs
        }
        self.dispatcher = RoutingDispatcher(
            event_commands=event_commands,
            job_commands=job_commands,
            payloads=payloads,
            event_subscriptions=self._event_subscriptions,
            component_jobs=self._component_jobs,
            subscription_routes=self._subscription_routes,
            component_job_routes=self._component_job_routes,
        )
        required = sorted(
            {
                (contribution.service_id, contribution.service_version)
                for _owner, contribution, _service in event_subscriptions
            }
            | {
                (contribution.service_id, contribution.service_version)
                for _owner, contribution, _service in component_jobs
            }
        )
        self.runtime_contract = {
            "protocol": "forest.runtime-services/1",
            "services": [
                {"service_id": "routing.event.publish", "version": 1},
                {"service_id": "routing.component-job.schedule", "version": 1},
            ],
            "requires": [
                {"service_id": service_id, "version": version}
                for service_id, version in required
            ],
        }
        self.generation_identity: BoundGenerationIdentity | None = None
        self.configuration_revision: int | None = None
        self.active = False
        self._authority_inflight = 0
        self._authority_drained = asyncio.Event()
        self._authority_drained.set()

    def runtime_service_handlers(self) -> dict:
        return {
            ("routing.event.publish", 1): RoutedServiceEndpoint(
                self._publish_event
            ),
            ("routing.component-job.schedule", 1): RoutedServiceEndpoint(
                self._schedule_component_job
            ),
        }

    async def prepare_generation(self, context) -> PreparedGenerationBinding:
        desired = await self.module_configuration.desired(include_value=True)
        if desired.value != {"protocol": "forest.routing.graph/1"}:
            raise ForestError(
                "ROUTING_CONFIGURATION_INVALID",
                "Routing graph configuration is invalid",
                422,
            )
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
            instance_id=f"instance:routing.graph:{context.activation_id}",
            worker_incarnation=f"worker:routing.graph:{context.activation_id}",
            state_schema=context.state_schema,
            state_contract_digest=context.state_contract_digest,
            capability_grants_digest=self.grants.grant_set_digest,
            data_admission=True,
            control_admission=True,
            storage=context.storage,
        )

    async def bind_generation(self, identity: BoundGenerationIdentity) -> None:
        if identity.component_id != "routing.graph":
            raise ForestError(
                "RUNTIME_GENERATION_BINDING_MISMATCH",
                "Routing authority received another component identity",
                409,
            )
        if self.generation_identity is not None and self.generation_identity != identity:
            raise ForestError(
                "RUNTIME_GENERATION_BINDING_MISMATCH",
                "Routing Generation identity is immutable",
                409,
            )
        self.generation_identity = identity

    async def publish_generation_in(
        self, conn, *, generation_id: str, graph_id: str
    ) -> None:
        identity = self.generation_identity
        if (
            identity is None
            or identity.generation_id != generation_id
            or identity.graph_id != graph_id
        ):
            raise ForestError(
                "RUNTIME_GENERATION_BINDING_MISMATCH",
                "Routing directory belongs to another RuntimeGraph",
                409,
            )
        await self.directory.publish_directory_in(
            conn,
            graph_id=identity.graph_id,
            graph_digest=identity.graph_digest,
            routing_generation_id=identity.generation_id,
            publications=self._publication_routes(),
            subscriptions=self._subscription_routes(),
            component_jobs=self._component_job_routes(),
        )

    async def start(self) -> None:
        if self.generation_identity is None or self.configuration_revision is None:
            raise ForestError(
                "RUNTIME_GENERATION_IDENTITY_UNAVAILABLE",
                "Routing authority is not prepared",
                503,
            )
        await self.dispatcher.start(self.generation_identity)

    def activate_generation(self) -> None:
        self.active = True

    def notify(self) -> None:
        self.dispatcher.notify()

    def freeze_owned_admission(self) -> None:
        self.dispatcher.freeze_admission()

    async def drain_owned_work(self) -> None:
        await self.dispatcher.drain()

    def open_owned_admission(self) -> None:
        if not self.active:
            raise ForestError(
                "RUNTIME_MODULE_LIFECYCLE_INVALID",
                "Routing claims cannot resume outside an active Generation",
                409,
            )
        self.dispatcher.open_admission()

    async def quiesce(self) -> None:
        self.active = False
        self.dispatcher.freeze_admission()
        await self.dispatcher.drain()
        await self._authority_drained.wait()

    async def close(self) -> None:
        self.active = False
        await self.dispatcher.close()

    def owned_tasks(self) -> tuple:
        return self.dispatcher.owned_tasks()

    @property
    def failure(self) -> str | None:
        return self.dispatcher.failure

    def _publication_routes(self) -> tuple[EventPublicationRoute, ...]:
        return tuple(
            EventPublicationRoute(
                owner,
                generation.generation_id,
                contribution.topic,
                contribution.contract,
                contribution.event_schema_digest,
            )
            for (owner, _topic, _contract), (
                contribution,
                generation,
            ) in sorted(self._event_publications.items())
        )

    def _subscription_routes(self) -> tuple[EventSubscriptionRoute, ...]:
        routes = []
        for owner, contribution, service in self._event_subscriptions.values():
            frozen = service.route
            if (
                frozen.provider_component_id != owner
                or frozen.service_id != contribution.service_id
                or frozen.version != contribution.service_version
            ):
                raise ForestError(
                    "EVENT_ROUTE_INVALID",
                    "Event handler route differs from its contributor",
                    503,
                )
            routes.append(
                EventSubscriptionRoute(
                    contribution.subscriber_id,
                    contribution.contract_major,
                    contribution.topic,
                    contribution.contract,
                    contribution.event_schema_digest,
                    owner,
                    frozen.provider_generation_id,
                    frozen.service_id,
                    frozen.version,
                    contribution.max_attempts,
                )
            )
        return tuple(routes)

    def _component_job_routes(self) -> tuple[ComponentJobRoute, ...]:
        routes = []
        for (owner, _handler_id), (contribution, service) in self._component_jobs.items():
            frozen = service.route
            if (
                frozen.provider_component_id != owner
                or frozen.service_id != contribution.service_id
                or frozen.version != contribution.service_version
            ):
                raise ForestError(
                    "COMPONENT_JOB_ROUTE_INVALID",
                    "ComponentJob route differs from its contributor",
                    503,
                )
            routes.append(
                ComponentJobRoute(
                    owner,
                    frozen.provider_generation_id,
                    contribution.handler_id,
                    contribution.input_schema_digest,
                    frozen.service_id,
                    frozen.version,
                    contribution.max_attempts,
                    contribution.timeout_seconds,
                    contribution.max_concurrency,
                )
            )
        return tuple(routes)

    def _check_authority_route(
        self,
        route: FrozenServiceRoute,
        caller_component_id: str,
    ) -> None:
        identity = self.generation_identity
        if (
            identity is None
            or route.provider_component_id != "routing.graph"
            or route.provider_generation_id != identity.generation_id
            or route.caller_component_id != caller_component_id
        ):
            raise ForestError(
                "RUNTIME_SERVICE_ROUTE_STALE",
                "Routing command does not match its frozen caller and provider",
                409,
            )

    async def _publish_event(
        self,
        route: FrozenServiceRoute,
        invocation: RuntimeServiceInvocation,
        command: PublishComponentEvent,
    ) -> dict:
        if type(command) is not PublishComponentEvent:
            raise ForestError("EVENT_REQUEST_INVALID", "Event command is invalid", 422)
        self._check_authority_route(route, command.publisher_component_id)
        publication = self._event_publications.get(
            (command.publisher_component_id, command.topic, command.contract)
        )
        if publication is None:
            raise ForestError(
                "EVENT_ROUTE_UNAVAILABLE",
                "Publisher did not declare this event contract",
                409,
            )
        contribution, generation = publication
        if (
            generation.generation_id != command.publisher_generation_id
            or not Draft202012Validator(contribution.event_schema).is_valid(
                command.payload
            )
        ):
            raise ForestError(
                "EVENT_REQUEST_INVALID",
                "Event payload or publisher Generation differs from its declaration",
                422,
            )
        self._authority_inflight += 1
        self._authority_drained.clear()
        try:
            result = await self.event_commands.publish_event(
                publisher_component_id=command.publisher_component_id,
                publisher_generation_id=command.publisher_generation_id,
                topic=command.topic,
                contract=command.contract,
                operation_id=invocation.operation_id,
                causal_root_kind=invocation.causal_root_kind,
                causal_root_id=invocation.causal_root_id,
                payload=command.payload,
            )
            self.dispatcher.notify()
            return result
        finally:
            self._authority_inflight -= 1
            if self._authority_inflight == 0:
                self._authority_drained.set()

    async def _schedule_component_job(
        self,
        route: FrozenServiceRoute,
        invocation: RuntimeServiceInvocation,
        command: ScheduleComponentJob,
    ) -> dict:
        if type(command) is not ScheduleComponentJob:
            raise ForestError(
                "COMPONENT_JOB_REQUEST_INVALID", "ComponentJob command is invalid", 422
            )
        self._check_authority_route(route, command.owner_component_id)
        declared = self._component_jobs.get(
            (command.owner_component_id, command.handler_id)
        )
        if declared is None:
            raise ForestError(
                "COMPONENT_JOB_ROUTE_UNAVAILABLE",
                "Caller did not declare this ComponentJob handler",
                409,
            )
        contribution, service = declared
        if (
            service.route.provider_generation_id != command.owner_generation_id
            or not Draft202012Validator(contribution.input_schema).is_valid(
                command.payload
            )
        ):
            raise ForestError(
                "COMPONENT_JOB_REQUEST_INVALID",
                "ComponentJob payload or owner Generation differs from its declaration",
                422,
            )
        self._authority_inflight += 1
        self._authority_drained.clear()
        try:
            result = await self.job_commands.schedule_component_job(
                owner_component_id=command.owner_component_id,
                owner_generation_id=command.owner_generation_id,
                handler_id=command.handler_id,
                operation_id=invocation.operation_id,
                causal_root_kind=invocation.causal_root_kind,
                causal_root_id=invocation.causal_root_id,
                payload=command.payload,
                delay_seconds=command.delay_seconds,
            )
            self.dispatcher.notify()
            return result
        finally:
            self._authority_inflight -= 1
            if self._authority_inflight == 0:
                self._authority_drained.set()

