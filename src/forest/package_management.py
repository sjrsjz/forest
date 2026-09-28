"""Generation-bound management ABI compiled from an immutable package manifest."""

from __future__ import annotations

from copy import deepcopy

from forest.clean_management_contract import (
    CleanManagementActionResult,
    CleanModuleConfigurationContract,
)
from forest.errors import ForestError
from forest.packages import digest_json


class PackageManagementContract(CleanModuleConfigurationContract):
    """Expose manifest-mapped read and command ports to the Web gateway."""

    protocol = "forest.package-management/1"

    def __init__(
        self,
        configuration,
        *,
        adapter,
        schema: dict,
        validator=None,
    ) -> None:
        super().__init__(configuration, schema=schema, validator=validator)
        self.adapter = adapter
        component = adapter.component
        query_ports = {
            item["name"]: item for item in component.get("domain_queries", ())
        }
        query_ports.update(
            (item["name"], item)
            for item in component.get("provides", ())
            if item["semantics"] == "read"
        )
        command_ports = {
            item["name"]: item for item in component.get("domain_commands", ())
        }
        self._queries = {
            item["id"]: (item, query_ports[item["port"]])
            for item in component.get("management_queries", ())
        }
        self._actions = {
            item["id"]: (item, command_ports[item["port"]])
            for item in component.get("management_actions", ())
        }

    def describe(self) -> dict:
        value = super().describe()
        value["protocol"] = self.protocol
        value["queries"] = [
            {
                "query_id": operation_id,
                "input_schema": self.adapter.snapshot.schema(port["input_schema"]),
                "output_schema": self.adapter.snapshot.schema(port["output_schema"]),
                **(
                    {"default_limit": declaration["default_limit"]}
                    if "default_limit" in declaration
                    else {}
                ),
                **(
                    {"max_limit": declaration["max_limit"]}
                    if "max_limit" in declaration
                    else {}
                ),
            }
            for operation_id, (declaration, port) in self._queries.items()
        ]
        value["actions"] = [
            {
                "action_id": operation_id,
                "priority": declaration["priority"],
                "input_schema": self._public_action_schema(declaration, port),
                "output_schema": self.adapter.snapshot.schema(port["output_schema"]),
            }
            for operation_id, (declaration, port) in self._actions.items()
        ]
        return value

    async def query(self, query_id: str, payload: dict) -> object:
        try:
            declaration, port = self._queries[query_id]
        except KeyError:
            raise ForestError(
                "MODULE_MANAGEMENT_OPERATION_NOT_FOUND",
                "Package management query does not exist",
                404,
            ) from None
        return await self.adapter.invoke_management_query(
            port,
            payload,
            timeout_seconds=float(declaration.get("timeout_s", 30)),
        )

    def authorization_plan_digest(self, action_id: str, payload: dict) -> str | None:
        try:
            declaration, _port = self._actions[action_id]
        except KeyError:
            raise ForestError(
                "MODULE_MANAGEMENT_OPERATION_NOT_FOUND",
                "Package management action does not exist",
                404,
            ) from None
        pointer = declaration.get("authorization_path")
        if pointer is None:
            return None
        return digest_json(self._pointer(payload, pointer))

    async def action(
        self,
        action_id: str,
        *,
        operation_id: str,
        payload: dict,
        authorization=None,
        authorization_guard=None,
    ) -> CleanManagementActionResult:
        try:
            declaration, port = self._actions[action_id]
        except KeyError:
            raise ForestError(
                "MODULE_MANAGEMENT_OPERATION_NOT_FOUND",
                "Package management action does not exist",
                404,
            ) from None
        if not isinstance(payload, dict):
            raise ForestError(
                "MODULE_MANAGEMENT_INPUT_INVALID",
                "Management action input must be an object",
                422,
            )
        plan_digest = self.authorization_plan_digest(action_id, payload)
        actor_field = declaration.get("actor_field")
        if actor_field is not None and actor_field in payload:
            raise ForestError(
                "MODULE_MANAGEMENT_INPUT_INVALID",
                "Authenticated actor fields are supplied by the management gateway",
                422,
            )
        command_payload = dict(payload)
        if actor_field is not None:
            command_payload[actor_field] = getattr(
                authorization, "web_subject_ref", "management:shared-token"
            )
        result = await self.adapter.invoke_management_action(
            port,
            command_payload,
            action_id=action_id,
            operation_id=operation_id,
            plan_digest=plan_digest,
            request_digest=digest_json(payload),
            timeout_seconds=float(declaration.get("timeout_s", 30)),
            authorization=authorization,
            authorization_guard=authorization_guard,
        )
        return CleanManagementActionResult(
            result,
            replace_runtime_graph=declaration.get("replace_runtime_graph", False),
        )

    def _public_action_schema(self, declaration: dict, port: dict) -> dict:
        schema = deepcopy(self.adapter.snapshot.schema(port["input_schema"]))
        actor_field = declaration.get("actor_field")
        if actor_field is None:
            return schema
        properties = schema.get("properties")
        if isinstance(properties, dict):
            properties.pop(actor_field, None)
        required = schema.get("required")
        if isinstance(required, list):
            schema["required"] = [item for item in required if item != actor_field]
        return schema

    @staticmethod
    def _pointer(value: object, pointer: str) -> object:
        current = value
        for encoded in pointer[1:].split("/"):
            token = encoded.replace("~1", "/").replace("~0", "~")
            if not isinstance(current, dict) or token not in current:
                raise ForestError(
                    "MODULE_MANAGEMENT_INPUT_INVALID",
                    "Authorization projection is absent from the action input",
                    422,
                )
            current = current[token]
        return current
