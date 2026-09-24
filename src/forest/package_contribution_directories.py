"""RuntimeGraph-frozen contribution directories exposed to isolated Workers."""

from __future__ import annotations

import re
from collections.abc import Callable, Iterable, Mapping
from typing import Any

from jsonschema import Draft202012Validator

from forest.errors import ForestError
from forest.job_protocol import DurableJobExecutionRequest
from forest.package_capabilities import (
    PackageAgentToolEffectAuthority,
    PackageCallScope,
    PackageDomainQueries,
)
from forest.packages import digest_json
from forest.routing_protocol import (
    DurableComponentJobRequest,
    DurableEventDeliveryRequest,
)
from forest.shell_command_protocol import (
    ShellCommandExecutionRequest,
    ShellCommandExecutionResult,
)


class FrozenContributionDirectories:
    """Own directory projection and invocation; never discover live routes by name."""

    def __init__(
        self,
        *,
        declarations: Iterable[dict],
        routes: Mapping[str, Any],
        domain_queries: PackageDomainQueries | None,
        domain_query_declarations: Iterable[dict],
        agent_tool_effects: PackageAgentToolEffectAuthority | None = None,
    ) -> None:
        self._declarations = {item["kind"]: item for item in declarations}
        self._routes = {
            kind: (
                {name: tuple(values) for name, values in items.items()}
                if kind == "routing-routes"
                else tuple(items)
            )
            for kind, items in routes.items()
        }
        self._domain_queries = domain_queries
        self._agent_tool_effects = agent_tool_effects
        self._query_declarations = {
            item["name"]: item for item in domain_query_declarations
        }
        if set(self._declarations) != set(self._routes):
            raise ForestError(
                "CONTRIBUTION_DIRECTORY_BINDING_INVALID",
                "Declared contribution directories require exact frozen route bindings",
                500,
            )
        if set(self._declarations) - {
            "job-types",
            "shell-commands",
            "routing-routes",
            "agent-tools",
        }:
            raise ForestError(
                "CONTRIBUTION_DIRECTORY_UNSUPPORTED",
                "Contribution directory kind is unsupported",
                500,
            )
        for kind, declaration in self._declarations.items():
            if kind == "routing-routes":
                routed_jobs = self._routes[kind]["component_jobs"]
                if any(
                    contribution.timeout_seconds
                    > declaration["max_execution_timeout_s"]
                    for _owner, contribution, _service in routed_jobs
                ):
                    raise ForestError(
                        "CONTRIBUTION_DIRECTORY_BINDING_INVALID",
                        "A routing contribution exceeds the consumer execution budget",
                        500,
                    )
                continue
            if kind == "agent-tools":
                continue
            if any(
                contribution.timeout_seconds
                > declaration["max_execution_timeout_s"]
                for _owner, contribution, _service in self._routes[kind]
            ):
                raise ForestError(
                    "CONTRIBUTION_DIRECTORY_BINDING_INVALID",
                    "A contribution exceeds the consumer execution budget",
                    500,
                )
        job_routes = self._routes.get("job-types", ())
        self._job_routes = {
            (contribution.job_type, contribution.job_version): (
                owner,
                contribution,
                service,
            )
            for owner, contribution, service in job_routes
        }
        if len(self._job_routes) != len(job_routes):
            raise ForestError(
                "CONTRIBUTION_DIRECTORY_BINDING_INVALID",
                "Frozen Job type routes must be unique",
                500,
            )
        shell_routes = self._routes.get("shell-commands", ())
        self._shell_routes = {
            (contribution.command_id, contribution.command_version): (
                owner,
                contribution,
                service,
            )
            for owner, contribution, service in shell_routes
        }
        if len(self._shell_routes) != len(shell_routes):
            raise ForestError(
                "CONTRIBUTION_DIRECTORY_BINDING_INVALID",
                "Frozen Shell command routes must be unique",
                500,
            )
        agent_tool_routes = self._routes.get("agent-tools", ())
        self._agent_tool_routes = {
            contribution.name: (owner, contribution, service)
            for owner, contribution, service in agent_tool_routes
        }
        if len(self._agent_tool_routes) != len(agent_tool_routes):
            raise ForestError(
                "CONTRIBUTION_DIRECTORY_BINDING_INVALID",
                "Frozen Agent tool routes must be unique",
                500,
            )
        routing = self._routes.get("routing-routes", {})
        publication_routes = routing.get("publications", ())
        self._event_publications = {
            (owner, contribution.topic, contribution.contract): (
                contribution,
                generation,
            )
            for owner, contribution, generation in publication_routes
        }
        subscription_routes = routing.get("subscriptions", ())
        self._event_subscriptions = {
            (contribution.subscriber_id, contribution.contract_major): (
                owner,
                contribution,
                service,
            )
            for owner, contribution, service in subscription_routes
        }
        component_job_routes = routing.get("component_jobs", ())
        self._component_jobs = {
            (owner, contribution.handler_id): (contribution, service)
            for owner, contribution, service in component_job_routes
        }
        if (
            len(self._event_publications) != len(publication_routes)
            or len(self._event_subscriptions) != len(subscription_routes)
            or len(self._component_jobs) != len(component_job_routes)
        ):
            raise ForestError(
                "CONTRIBUTION_DIRECTORY_BINDING_INVALID",
                "Frozen routing contribution identities must be unique",
                500,
            )

    @property
    def kinds(self) -> tuple[str, ...]:
        return tuple(sorted(self._declarations))

    def requirements(self) -> tuple[dict, ...]:
        required = []
        for kind, routes in self._routes.items():
            if kind == "routing-routes":
                routed = (
                    *routes["subscriptions"],
                    *routes["component_jobs"],
                )
            else:
                routed = routes
            required.extend(
                {
                    "service_id": contribution.service_id,
                    "version": (
                        getattr(contribution, "service_version", None)
                        or contribution.version
                    ),
                }
                for _owner, contribution, _service in routed
            )
        return tuple(required)

    def projection(self, graph_identity) -> dict:
        directories = {
            kind: self._directory_fact(kind, graph_identity)
            for kind in self.kinds
        }
        return {"contribution_directories": directories} if directories else {}

    def synchronization_requests(
        self, graph_identity, *, configuration: dict
    ) -> tuple[tuple[str, dict], ...]:
        return tuple(
            (
                self._declarations[kind]["synchronization_command"],
                {
                    **self._directory_fact(kind, graph_identity),
                    **(
                        {"consumer_configuration": configuration}
                        if self._declarations[kind].get(
                            "synchronize_configuration", False
                        )
                        else {}
                    ),
                },
            )
            for kind in self.kinds
            if "synchronization_command" in self._declarations[kind]
        )

    def cleanup_requests(self, graph_identity) -> tuple[tuple[str, dict], ...]:
        if graph_identity is None:
            raise ForestError(
                "RUNTIME_GENERATION_NOT_BOUND",
                "Contribution directory has no RuntimeGraph identity",
                409,
            )
        return tuple(
            (
                self._declarations[kind]["cleanup_command"],
                {
                    "protocol": "forest.contribution-directory-cleanup/1",
                    "kind": kind,
                    "graph_id": graph_identity.graph_id,
                    "graph_revision": graph_identity.graph_revision,
                },
            )
            for kind in self.kinds
            if "cleanup_command" in self._declarations[kind]
        )

    async def invoke(
        self,
        request: dict,
        *,
        graph_identity,
        scope: PackageCallScope,
        parent_call_id: str,
        remaining_timeout: Callable[[float], float],
    ) -> Any:
        kind = request.get("kind")
        if kind not in self._declarations:
            raise ForestError(
                "CAPABILITY_DENIED",
                "Package did not declare this contribution directory",
                403,
            )
        if request.get("action") == "list":
            if set(request) != {"kind", "action"}:
                raise ForestError(
                    "CONTRIBUTION_DIRECTORY_REQUEST_INVALID",
                    "Contribution directory list request contains unsupported fields",
                    422,
                )
            return self._directory_fact(kind, graph_identity)
        if kind == "job-types":
            return await self._execute_job(
                request,
                scope=scope,
                parent_call_id=parent_call_id,
                remaining_timeout=remaining_timeout,
            )
        if kind == "shell-commands":
            return await self._execute_shell_command(
                request,
                scope=scope,
                parent_call_id=parent_call_id,
                remaining_timeout=remaining_timeout,
            )
        if kind == "routing-routes":
            return await self._invoke_routing(
                request,
                scope=scope,
                parent_call_id=parent_call_id,
                remaining_timeout=remaining_timeout,
            )
        if kind == "agent-tools":
            if request.get("action") == "query-handed-off":
                return await self._query_handed_off_agent_tool(
                    request,
                    scope=scope,
                    parent_call_id=parent_call_id,
                    remaining_timeout=remaining_timeout,
                )
            return await self._dispatch_agent_tool(
                request,
                scope=scope,
                parent_call_id=parent_call_id,
                remaining_timeout=remaining_timeout,
            )
        raise ForestError(
            "CONTRIBUTION_DIRECTORY_UNSUPPORTED",
            "Contribution directory kind is unsupported",
            422,
        )

    async def _dispatch_agent_tool(
        self,
        request: dict,
        *,
        scope: PackageCallScope,
        parent_call_id: str,
        remaining_timeout: Callable[[float], float],
    ) -> dict:
        expected = {
            "kind",
            "action",
            "tool_name",
            "target_component_id",
            "target_generation_id",
            "target_service_id",
            "target_service_version",
            "contract_digest",
            "binding_digest",
            "effect_operation_id",
            "attempt_id",
            "permit",
            "command",
            "operation_id",
            "timeout",
        }
        if request.get("action") != "dispatch-prepared" or set(request) != expected:
            raise ForestError(
                "CONTRIBUTION_DIRECTORY_REQUEST_INVALID",
                "Agent tool dispatch request contains unsupported fields",
                422,
            )
        strings = tuple(
            request[field]
            for field in (
                "tool_name",
                "target_component_id",
                "target_generation_id",
                "target_service_id",
                "contract_digest",
                "binding_digest",
                "effect_operation_id",
                "attempt_id",
                "operation_id",
            )
        )
        timeout = request["timeout"]
        if (
            any(not isinstance(value, str) or not 1 <= len(value) <= 512 for value in strings)
            or type(request["target_service_version"]) is not int
            or request["target_service_version"] < 1
            or not isinstance(request["permit"], dict)
            or not isinstance(request["command"], dict)
            or isinstance(timeout, bool)
            or not isinstance(timeout, (int, float))
            or not 0 < timeout <= self._declarations["agent-tools"]["max_execution_timeout_s"]
        ):
            raise ForestError(
                "CONTRIBUTION_DIRECTORY_REQUEST_INVALID",
                "Agent tool dispatch request is invalid",
                422,
            )
        selected = self._agent_tool_routes.get(request["tool_name"])
        if selected is None:
            raise ForestError("AGENT_TOOL_NOT_FOUND", "Agent tool is not available", 404)
        owner, contribution, service = selected
        route = service.route
        binding = {
            "name": contribution.name,
            "component_id": owner,
            "generation_id": route.provider_generation_id,
            "service_id": contribution.service_id,
            "contract_digest": contribution.contract_digest,
            "approval_required": contribution.approval_required,
            "supports_query": contribution.supports_query,
        }
        if (
            (
                request["target_component_id"],
                request["target_generation_id"],
                request["target_service_id"],
                request["target_service_version"],
                request["contract_digest"],
                request["binding_digest"],
            )
            != (
                owner,
                route.provider_generation_id,
                route.service_id,
                route.version,
                contribution.contract_digest,
                digest_json(binding),
            )
        ):
            raise ForestError(
                "AGENT_TOOL_ROUTE_STALE",
                "Agent tool dispatch differs from the frozen contribution route",
                409,
            )
        command = request["command"]
        command_fields = {
            "protocol",
            "tool_call_id",
            "effect_operation_id",
            "effect_attempt_id",
            "component_id",
            "generation_id",
            "service_id",
            "contract_digest",
            "binding_digest",
            "authority_trigger_ref",
            "authority_principal_ref",
            "arguments",
        }
        if (
            set(command) != command_fields
            or command.get("protocol") != "forest.agent-tool-execute/1"
            or command.get("effect_operation_id") != request["effect_operation_id"]
            or command.get("effect_attempt_id") != request["attempt_id"]
            or command.get("component_id") != owner
            or command.get("generation_id") != route.provider_generation_id
            or command.get("service_id") != contribution.service_id
            or command.get("contract_digest") != contribution.contract_digest
            or command.get("binding_digest") != request["binding_digest"]
            or not isinstance(command.get("arguments"), dict)
            or any(
                not isinstance(command.get(field), str) or not command[field]
                for field in (
                    "tool_call_id",
                    "authority_trigger_ref",
                    "authority_principal_ref",
                )
            )
        ):
            raise ForestError(
                "AGENT_TOOL_COMMAND_INVALID",
                "Agent tool command differs from the frozen binding",
                422,
            )
        permit = request["permit"]
        if (
            permit.get("protocol") != "forest.effect-permit/1"
            or permit.get("effect_operation_id") != request["effect_operation_id"]
            or permit.get("attempt_id") != request["attempt_id"]
            or permit.get("executor_component_id") != owner
            or permit.get("executor_generation_id") != route.provider_generation_id
            or permit.get("wire_payload_digest") != digest_json(command)
            or request["operation_id"] != request["effect_operation_id"]
        ):
            raise ForestError(
                "AGENT_TOOL_EFFECT_FENCE_MISMATCH",
                "Agent tool permit differs from the prepared command",
                409,
            )
        if self._agent_tool_effects is None:
            raise ForestError(
                "CAPABILITY_UNAVAILABLE",
                "Agent tool Effect authority is unavailable",
                503,
            )
        loaded = await self._agent_tool_effects.load(request["effect_operation_id"])
        effect = loaded.get("effect") if isinstance(loaded, dict) else None
        attempt = loaded.get("attempt") if isinstance(loaded, dict) else None
        if (
            not isinstance(effect, dict)
            or not isinstance(attempt, dict)
            or effect.get("state") != "DISPATCHING"
            or attempt.get("state") != "DISPATCHING"
            or attempt.get("attempt_id") != request["attempt_id"]
            or attempt.get("executor_component_id") != owner
            or attempt.get("executor_generation_id") != route.provider_generation_id
            or attempt.get("wire_payload_digest") != permit["wire_payload_digest"]
        ):
            raise ForestError(
                "AGENT_TOOL_EFFECT_FENCE_MISMATCH",
                "Kernel Effect no longer authorizes this tool dispatch",
                409,
            )
        result = await service.invoke(
            command,
            operation_id=request["operation_id"],
            causal_root_kind=scope.causal_root_kind,
            causal_root_id=scope.causal_root_id,
            timeout_seconds=min(float(timeout), remaining_timeout(float(timeout))),
            parent_call_id=parent_call_id,
        )
        if not isinstance(result, dict):
            raise ForestError(
                "AGENT_TOOL_RESULT_INVALID",
                "Agent tool executor returned no object",
                502,
            )
        return result

    async def _query_handed_off_agent_tool(
        self,
        request: dict,
        *,
        scope: PackageCallScope,
        parent_call_id: str,
        remaining_timeout: Callable[[float], float],
    ) -> dict:
        expected = {
            "kind", "action", "tool_name", "target_component_id",
            "target_generation_id", "target_service_id", "target_service_version",
            "contract_digest", "binding_digest", "effect_operation_id", "attempt_id",
            "command", "operation_id", "timeout",
        }
        if set(request) != expected:
            raise ForestError(
                "CONTRIBUTION_DIRECTORY_REQUEST_INVALID",
                "Agent tool reconciliation request contains unsupported fields",
                422,
            )
        strings = tuple(
            request[field]
            for field in (
                "tool_name", "target_component_id", "target_generation_id",
                "target_service_id", "contract_digest", "binding_digest",
                "effect_operation_id", "attempt_id", "operation_id",
            )
        )
        timeout = request["timeout"]
        if (
            any(not isinstance(value, str) or not 1 <= len(value) <= 512 for value in strings)
            or type(request["target_service_version"]) is not int
            or request["target_service_version"] < 1
            or not isinstance(request["command"], dict)
            or isinstance(timeout, bool)
            or not isinstance(timeout, (int, float))
            or not 0 < timeout <= self._declarations["agent-tools"]["max_execution_timeout_s"]
        ):
            raise ForestError(
                "CONTRIBUTION_DIRECTORY_REQUEST_INVALID",
                "Agent tool reconciliation request is invalid",
                422,
            )
        selected = self._agent_tool_routes.get(request["tool_name"])
        if selected is None:
            raise ForestError("AGENT_TOOL_NOT_FOUND", "Agent tool is not available", 404)
        owner, contribution, service = selected
        route = service.route
        binding = {
            "name": contribution.name,
            "component_id": owner,
            "generation_id": route.provider_generation_id,
            "service_id": contribution.service_id,
            "contract_digest": contribution.contract_digest,
            "approval_required": contribution.approval_required,
            "supports_query": contribution.supports_query,
        }
        if not contribution.supports_query:
            raise ForestError(
                "AGENT_TOOL_QUERY_UNSUPPORTED",
                "Agent tool does not declare reconciliation query support",
                409,
            )
        if (
            request["target_component_id"], request["target_generation_id"],
            request["target_service_id"], request["target_service_version"],
            request["contract_digest"], request["binding_digest"],
        ) != (
            owner, route.provider_generation_id, route.service_id, route.version,
            contribution.contract_digest, digest_json(binding),
        ):
            raise ForestError(
                "AGENT_TOOL_ROUTE_STALE",
                "Agent tool reconciliation differs from the frozen contribution route",
                409,
            )
        command = request["command"]
        command_fields = {
            "protocol", "tool_call_id", "effect_operation_id", "effect_attempt_id",
            "component_id", "generation_id", "service_id", "contract_digest",
            "binding_digest", "authority_trigger_ref", "authority_principal_ref",
            "arguments",
        }
        if (
            set(command) != command_fields
            or command.get("protocol") != "forest.agent-tool-query/1"
            or command.get("effect_operation_id") != request["effect_operation_id"]
            or command.get("effect_attempt_id") != request["attempt_id"]
            or command.get("component_id") != owner
            or command.get("generation_id") != route.provider_generation_id
            or command.get("service_id") != contribution.service_id
            or command.get("contract_digest") != contribution.contract_digest
            or command.get("binding_digest") != request["binding_digest"]
            or not isinstance(command.get("arguments"), dict)
            or any(not isinstance(command.get(field), str) or not command[field]
                   for field in ("tool_call_id", "authority_trigger_ref", "authority_principal_ref"))
        ):
            raise ForestError(
                "AGENT_TOOL_QUERY_INVALID",
                "Agent tool reconciliation command differs from the frozen binding",
                422,
            )
        if self._agent_tool_effects is None:
            raise ForestError("CAPABILITY_UNAVAILABLE", "Agent tool Effect authority is unavailable", 503)
        loaded = await self._agent_tool_effects.load(request["effect_operation_id"])
        effect = loaded.get("effect") if isinstance(loaded, dict) else None
        attempt = loaded.get("attempt") if isinstance(loaded, dict) else None
        if (
            not isinstance(effect, dict) or not isinstance(attempt, dict)
            or effect.get("state") not in {"DISPATCHING", "UNKNOWN"}
            or attempt.get("state") not in {"DISPATCHING", "UNKNOWN"}
            or attempt.get("attempt_id") != request["attempt_id"]
            or attempt.get("executor_component_id") != owner
            or attempt.get("executor_generation_id") != route.provider_generation_id
        ):
            raise ForestError(
                "AGENT_TOOL_EFFECT_FENCE_MISMATCH",
                "Kernel Effect is not at a queryable handed-off boundary",
                409,
            )
        result = await service.invoke(
            command,
            operation_id=request["operation_id"],
            causal_root_kind=scope.causal_root_kind,
            causal_root_id=scope.causal_root_id,
            timeout_seconds=min(float(timeout), remaining_timeout(float(timeout))),
            parent_call_id=parent_call_id,
        )
        if not isinstance(result, dict):
            raise ForestError(
                "AGENT_TOOL_RESULT_INVALID",
                "Agent tool reconciliation returned no object",
                502,
            )
        return result

    def _directory_fact(self, kind: str, graph_identity) -> dict:
        if graph_identity is None:
            raise ForestError(
                "RUNTIME_GENERATION_NOT_BOUND",
                "Contribution directory has no RuntimeGraph identity",
                409,
            )
        if kind == "routing-routes":
            return self._routing_directory_fact(graph_identity)
        return {
            "protocol": "forest.contribution-directory/1",
            "kind": kind,
            "graph_id": graph_identity.graph_id,
            "graph_revision": graph_identity.graph_revision,
            "items": [
                {
                    **contribution.as_fact(),
                    "owner_component_id": owner,
                    "target_generation_id": service.route.provider_generation_id,
                }
                for owner, contribution, service in self._routes[kind]
            ],
        }

    def _routing_directory_fact(self, graph_identity) -> dict:
        return {
            "protocol": "forest.routing-directory/2",
            "kind": "routing-routes",
            "graph_id": graph_identity.graph_id,
            "graph_revision": graph_identity.graph_revision,
            "publications": [
                {
                    **contribution.as_fact(),
                    "owner_component_id": owner,
                    "owner_generation_id": generation.generation_id,
                }
                for (owner, _topic, _contract), (
                    contribution,
                    generation,
                ) in sorted(self._event_publications.items())
            ],
            "subscriptions": [
                {
                    **contribution.as_fact(),
                    "owner_component_id": owner,
                    "target_generation_id": service.route.provider_generation_id,
                }
                for (_subscriber, _major), (
                    owner,
                    contribution,
                    service,
                ) in sorted(self._event_subscriptions.items())
            ],
            "component_jobs": [
                {
                    **contribution.as_fact(),
                    "owner_component_id": owner,
                    "target_generation_id": service.route.provider_generation_id,
                }
                for (owner, _handler), (
                    contribution,
                    service,
                ) in sorted(self._component_jobs.items())
            ],
        }

    async def _execute_job(
        self,
        request: dict,
        *,
        scope: PackageCallScope,
        parent_call_id: str,
        remaining_timeout: Callable[[float], float],
    ) -> dict:
        expected = {
            "kind",
            "action",
            "job_id",
            "job_type",
            "job_version",
            "attempt",
            "requested_by",
            "input",
            "claim_token",
            "target_component_id",
            "target_generation_id",
            "target_service_id",
            "target_service_version",
            "operation_id",
            "timeout",
        }
        if request.get("action") != "execute" or set(request) != expected:
            raise ForestError(
                "CONTRIBUTION_DIRECTORY_REQUEST_INVALID",
                "Job directory execution request contains unsupported fields",
                422,
            )
        strings = (
            request["job_id"],
            request["job_type"],
            request["requested_by"],
            request["claim_token"],
            request["target_component_id"],
            request["target_generation_id"],
            request["target_service_id"],
            request["operation_id"],
        )
        timeout = request["timeout"]
        if (
            any(not isinstance(value, str) or not 1 <= len(value) <= 512 for value in strings)
            or type(request["job_version"]) is not int
            or request["job_version"] < 1
            or type(request["attempt"]) is not int
            or request["attempt"] < 1
            or type(request["target_service_version"]) is not int
            or request["target_service_version"] < 1
            or not isinstance(request["input"], dict)
            or isinstance(timeout, bool)
            or not isinstance(timeout, (int, float))
            or not 0 < timeout <= 86_400
        ):
            raise ForestError(
                "CONTRIBUTION_DIRECTORY_REQUEST_INVALID",
                "Job directory execution request is invalid",
                422,
            )
        selected = self._job_routes.get(
            (request["job_type"], request["job_version"])
        )
        if selected is None:
            raise ForestError("JOB_TYPE_NOT_FOUND", "Job type is not available", 404)
        owner, contribution, service = selected
        route = service.route
        if (
            request["target_component_id"],
            request["target_generation_id"],
            request["target_service_id"],
            request["target_service_version"],
        ) != (
            owner,
            route.provider_generation_id,
            route.service_id,
            route.version,
        ):
            raise ForestError(
                "JOB_ROUTE_STALE",
                "Job execution differs from the frozen contribution route",
                409,
            )
        if not Draft202012Validator(contribution.input_schema).is_valid(request["input"]):
            raise ForestError(
                "JOB_INPUT_INVALID",
                "Job input violates the frozen contribution schema",
                422,
            )
        if timeout > contribution.timeout_seconds:
            raise ForestError(
                "JOB_TIMEOUT_INVALID",
                "Job timeout exceeds the frozen contribution contract",
                422,
            )
        cancellation_name = self._declarations["job-types"]["cancellation_query"]
        cancellation_query = self._query_declarations.get(cancellation_name)
        if self._domain_queries is None or cancellation_query is None:
            raise ForestError(
                "CAPABILITY_UNAVAILABLE",
                "Job cancellation query is unavailable",
                503,
            )

        async def cancellation_requested() -> bool:
            result = await self._domain_queries.invoke_query(
                scope,
                cancellation_query,
                {
                    "protocol": "forest.job-cancellation-query/1",
                    "job_id": request["job_id"],
                    "claim_token": request["claim_token"],
                },
                parent_call_id=parent_call_id,
                timeout_seconds=min(5.0, remaining_timeout(5.0)),
            )
            if (
                not isinstance(result, dict)
                or set(result) != {"requested"}
                or type(result["requested"]) is not bool
            ):
                raise ForestError(
                    "JOB_CANCELLATION_RESULT_INVALID",
                    "Job cancellation query returned an invalid result",
                    502,
                )
            return result["requested"]

        result = await service.invoke(
            DurableJobExecutionRequest(
                request["job_id"],
                request["job_type"],
                request["job_version"],
                request["attempt"],
                request["requested_by"],
                request["input"],
                cancellation_requested,
            ),
            operation_id=request["operation_id"],
            causal_root_kind=scope.causal_root_kind,
            causal_root_id=scope.causal_root_id,
            timeout_seconds=min(float(timeout), remaining_timeout(float(timeout))),
            parent_call_id=parent_call_id,
        )
        if not isinstance(result, dict):
            raise ForestError("JOB_RESULT_INVALID", "Job executor returned no object", 502)
        return result

    async def _execute_shell_command(
        self,
        request: dict,
        *,
        scope: PackageCallScope,
        parent_call_id: str,
        remaining_timeout: Callable[[float], float],
    ) -> dict:
        expected = {
            "kind",
            "action",
            "command_id",
            "command_version",
            "arguments",
            "request_id",
            "principal_ref",
            "conversation_ref",
            "target_component_id",
            "target_generation_id",
            "target_service_id",
            "target_service_version",
            "operation_id",
            "timeout",
        }
        if request.get("action") != "execute" or set(request) != expected:
            raise ForestError(
                "CONTRIBUTION_DIRECTORY_REQUEST_INVALID",
                "Shell command execution request contains unsupported fields",
                422,
            )
        strings = (
            request["command_id"],
            request["request_id"],
            request["principal_ref"],
            request["conversation_ref"],
            request["target_component_id"],
            request["target_generation_id"],
            request["target_service_id"],
            request["operation_id"],
        )
        timeout = request["timeout"]
        if (
            any(not isinstance(value, str) or not 1 <= len(value) <= 512 for value in strings)
            or type(request["command_version"]) is not int
            or request["command_version"] < 1
            or type(request["target_service_version"]) is not int
            or request["target_service_version"] < 1
            or not isinstance(request["arguments"], dict)
            or isinstance(timeout, bool)
            or not isinstance(timeout, (int, float))
            or not 0 < timeout <= 900
        ):
            raise ForestError(
                "CONTRIBUTION_DIRECTORY_REQUEST_INVALID",
                "Shell command execution request is invalid",
                422,
            )
        selected = self._shell_routes.get(
            (request["command_id"], request["command_version"])
        )
        if selected is None:
            raise ForestError(
                "SHELL_ROUTE_UNSUPPORTED", "Shell command route is unavailable", 409
            )
        owner, contribution, service = selected
        route = service.route
        if (
            request["target_component_id"],
            request["target_generation_id"],
            request["target_service_id"],
            request["target_service_version"],
        ) != (
            owner,
            route.provider_generation_id,
            route.service_id,
            route.version,
        ):
            raise ForestError("SHELL_ROUTE_STALE", "Shell command route is stale", 409)
        argument_document = {
            "protocol": "forest.shell.arguments/1",
            "command_id": request["command_id"],
            "arguments": request["arguments"],
        }
        if not Draft202012Validator(contribution.argument_schema).is_valid(
            argument_document
        ):
            raise ForestError(
                "SHELL_ARGUMENTS_INVALID",
                "Shell command arguments violate the frozen contribution schema",
                422,
            )
        if timeout > contribution.timeout_seconds:
            raise ForestError(
                "SHELL_TIMEOUT_INVALID",
                "Shell command timeout exceeds the frozen contribution contract",
                422,
            )
        result = await service.invoke(
            ShellCommandExecutionRequest(
                request["command_id"],
                request["command_version"],
                request["arguments"],
                request["request_id"],
                request["principal_ref"],
                request["conversation_ref"],
            ),
            operation_id=request["operation_id"],
            causal_root_kind=scope.causal_root_kind,
            causal_root_id=scope.causal_root_id,
            timeout_seconds=min(float(timeout), remaining_timeout(float(timeout))),
            parent_call_id=parent_call_id,
        )
        if type(result) is not ShellCommandExecutionResult:
            raise ForestError(
                "SHELL_EXECUTION_RESULT_INVALID",
                "Shell command executor returned an invalid result",
                502,
            )
        return {"text": result.text}

    async def _invoke_routing(
        self,
        request: dict,
        *,
        scope: PackageCallScope,
        parent_call_id: str,
        remaining_timeout: Callable[[float], float],
    ) -> dict:
        action = request.get("action")
        if action == "deliver-event":
            return await self._deliver_event(
                request,
                scope=scope,
                parent_call_id=parent_call_id,
                remaining_timeout=remaining_timeout,
            )
        if action == "deliver-component-job":
            return await self._deliver_component_job(
                request,
                scope=scope,
                parent_call_id=parent_call_id,
                remaining_timeout=remaining_timeout,
            )
        raise ForestError(
            "CONTRIBUTION_DIRECTORY_REQUEST_INVALID",
            "Routing directory action is invalid",
            422,
        )

    async def _deliver_event(
        self,
        request: dict,
        *,
        scope: PackageCallScope,
        parent_call_id: str,
        remaining_timeout: Callable[[float], float],
    ) -> dict:
        expected = {
            "kind", "action", "event_id", "delivery_id", "subscriber_id",
            "topic", "contract", "publisher_component_id",
            "publisher_generation_id", "attempt", "payload",
            "target_component_id", "target_generation_id", "target_service_id",
            "target_service_version", "operation_id", "timeout",
        }
        if set(request) != expected:
            raise ForestError(
                "CONTRIBUTION_DIRECTORY_REQUEST_INVALID",
                "Event delivery request contains unsupported fields",
                422,
            )
        contract = request.get("contract")
        if not isinstance(contract, str) or re.fullmatch(
            r"[1-9][0-9]*\.[0-9]+", contract
        ) is None:
            raise ForestError(
                "EVENT_DELIVERY_INPUT_INVALID", "Event contract is invalid", 422
            )
        selected = self._event_subscriptions.get(
            (request["subscriber_id"], int(contract.split(".", 1)[0]))
        )
        if selected is None:
            raise ForestError("EVENT_ROUTE_UNAVAILABLE", "Event route is unavailable", 409)
        owner, contribution, service = selected
        route = service.route
        target = (
            request["target_component_id"], request["target_generation_id"],
            request["target_service_id"], request["target_service_version"],
        )
        if target != (
            owner, route.provider_generation_id, route.service_id, route.version
        ) or (
            request["topic"], request["contract"]
        ) != (contribution.topic, contribution.contract):
            raise ForestError("EVENT_ROUTE_STALE", "Event delivery route is stale", 409)
        if not Draft202012Validator(contribution.event_schema).is_valid(
            request["payload"]
        ):
            raise ForestError("EVENT_DELIVERY_INPUT_INVALID", "Event payload is invalid", 422)
        timeout = request["timeout"]
        if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not 0 < timeout <= 120:
            raise ForestError("EVENT_DELIVERY_INPUT_INVALID", "Event timeout is invalid", 422)
        result = await service.invoke(
            DurableEventDeliveryRequest(
                request["event_id"], request["delivery_id"],
                request["subscriber_id"], request["topic"], request["contract"],
                request["publisher_component_id"],
                request["publisher_generation_id"], request["attempt"],
                request["payload"],
            ),
            operation_id=request["operation_id"],
            causal_root_kind=scope.causal_root_kind,
            causal_root_id=scope.causal_root_id,
            timeout_seconds=min(float(timeout), remaining_timeout(float(timeout))),
            parent_call_id=parent_call_id,
        )
        if result != {"acknowledged": True}:
            raise ForestError("EVENT_DELIVERY_RESULT_INVALID", "Event handler returned an invalid acknowledgement", 502)
        return result

    async def _deliver_component_job(
        self,
        request: dict,
        *,
        scope: PackageCallScope,
        parent_call_id: str,
        remaining_timeout: Callable[[float], float],
    ) -> dict:
        expected = {
            "kind", "action", "job_id", "handler_id",
            "scheduled_by_component_id", "scheduled_by_generation_id", "attempt",
            "payload", "target_component_id", "target_generation_id",
            "target_service_id", "target_service_version", "operation_id", "timeout",
        }
        if set(request) != expected:
            raise ForestError(
                "CONTRIBUTION_DIRECTORY_REQUEST_INVALID",
                "ComponentJob delivery request contains unsupported fields",
                422,
            )
        selected = self._component_jobs.get(
            (request["target_component_id"], request["handler_id"])
        )
        if selected is None:
            raise ForestError(
                "COMPONENT_JOB_ROUTE_UNAVAILABLE",
                "ComponentJob route is unavailable",
                409,
            )
        contribution, service = selected
        route = service.route
        if (
            request["target_generation_id"], request["target_service_id"],
            request["target_service_version"],
        ) != (route.provider_generation_id, route.service_id, route.version):
            raise ForestError("COMPONENT_JOB_ROUTE_STALE", "ComponentJob route is stale", 409)
        if not Draft202012Validator(contribution.input_schema).is_valid(
            request["payload"]
        ):
            raise ForestError("COMPONENT_JOB_INPUT_INVALID", "ComponentJob input is invalid", 422)
        timeout = request["timeout"]
        if (
            isinstance(timeout, bool)
            or not isinstance(timeout, (int, float))
            or not 0 < timeout <= contribution.timeout_seconds
        ):
            raise ForestError("COMPONENT_JOB_INPUT_INVALID", "ComponentJob timeout is invalid", 422)
        result = await service.invoke(
            DurableComponentJobRequest(
                request["job_id"], request["handler_id"],
                request["scheduled_by_component_id"],
                request["scheduled_by_generation_id"], request["attempt"],
                request["payload"],
            ),
            operation_id=request["operation_id"],
            causal_root_kind=scope.causal_root_kind,
            causal_root_id=scope.causal_root_id,
            timeout_seconds=min(float(timeout), remaining_timeout(float(timeout))),
            parent_call_id=parent_call_id,
        )
        if not isinstance(result, dict):
            raise ForestError("COMPONENT_JOB_RESULT_INVALID", "ComponentJob handler returned no object", 502)
        return result
