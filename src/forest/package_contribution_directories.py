"""RuntimeGraph-frozen contribution directories exposed to isolated Workers."""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from typing import Any

from jsonschema import Draft202012Validator

from forest.errors import ForestError
from forest.job_protocol import DurableJobExecutionRequest
from forest.package_capabilities import PackageCallScope, PackageDomainQueries


class FrozenContributionDirectories:
    """Own directory projection and invocation; never discover live routes by name."""

    def __init__(
        self,
        *,
        declarations: Iterable[dict],
        routes: Mapping[str, tuple],
        domain_queries: PackageDomainQueries | None,
        domain_query_declarations: Iterable[dict],
    ) -> None:
        self._declarations = {item["kind"]: item for item in declarations}
        self._routes = {kind: tuple(items) for kind, items in routes.items()}
        self._domain_queries = domain_queries
        self._query_declarations = {
            item["name"]: item for item in domain_query_declarations
        }
        if set(self._declarations) != set(self._routes):
            raise ForestError(
                "CONTRIBUTION_DIRECTORY_BINDING_INVALID",
                "Declared contribution directories require exact frozen route bindings",
                500,
            )
        if set(self._declarations) - {"job-types"}:
            raise ForestError(
                "CONTRIBUTION_DIRECTORY_UNSUPPORTED",
                "Contribution directory kind is unsupported",
                500,
            )
        job_declaration = self._declarations.get("job-types")
        job_routes = self._routes.get("job-types", ())
        if job_declaration is not None and any(
            contribution.timeout_seconds
            > job_declaration["max_execution_timeout_s"]
            for _owner, contribution, _service in job_routes
        ):
            raise ForestError(
                "CONTRIBUTION_DIRECTORY_BINDING_INVALID",
                "A Job contribution exceeds the consumer execution budget",
                500,
            )
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

    @property
    def kinds(self) -> tuple[str, ...]:
        return tuple(sorted(self._declarations))

    def requirements(self) -> tuple[dict, ...]:
        return tuple(
            {
                "service_id": contribution.service_id,
                "version": contribution.service_version,
            }
            for routes in self._routes.values()
            for _owner, contribution, _service in routes
        )

    def projection(self, graph_identity) -> dict:
        directories = {
            kind: self._directory_fact(kind, graph_identity)
            for kind in self.kinds
        }
        return {"contribution_directories": directories} if directories else {}

    def synchronization_requests(self, graph_identity) -> tuple[tuple[str, dict], ...]:
        return tuple(
            (
                self._declarations[kind]["synchronization_command"],
                self._directory_fact(kind, graph_identity),
            )
            for kind in self.kinds
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
        raise ForestError(
            "CONTRIBUTION_DIRECTORY_UNSUPPORTED",
            "Contribution directory kind is unsupported",
            422,
        )

    def _directory_fact(self, kind: str, graph_identity) -> dict:
        if graph_identity is None:
            raise ForestError(
                "RUNTIME_GENERATION_NOT_BOUND",
                "Contribution directory has no RuntimeGraph identity",
                409,
            )
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
