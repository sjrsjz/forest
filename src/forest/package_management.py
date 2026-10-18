"""Generation-bound management ABI compiled from an immutable package manifest."""

from __future__ import annotations

from contextlib import asynccontextmanager

from forest.configuration_documents import ConfigurationDocuments
from forest.errors import ForestError
from forest.module_management_contract import ModuleConfigurationContract
from forest.packages import digest_json


class PackageManagementContract(ModuleConfigurationContract):
    """Expose manifest-mapped read and command ports to the Web gateway."""

    protocol = "forest.package-management/1"

    def __init__(
        self,
        configuration,
        *,
        adapter,
        schema: dict,
        validator=None,
        apply_callback=None,
    ) -> None:
        super().__init__(
            configuration,
            schema=schema,
            validator=validator,
            apply_callback=apply_callback or adapter.apply_configuration,
        )
        self.adapter = adapter
        component = adapter.component
        declarations = component.get('configuration_documents', ())
        self._configuration_documents = ConfigurationDocuments(
            configuration, declarations, adapter.schema_snapshot, schema, validator,
        ) if declarations else None
        query_ports = {
            item["name"]: item for item in component.get("domain_queries", ())
        }
        query_ports.update(
            (item["name"], item) for item in component.get("kernel_queries", ())
        )
        query_ports.update(
            (item["name"], item)
            for item in component.get("provides", ())
            if item["semantics"] == "read"
        )
        command_ports = {
            item["name"]: item for item in component.get("domain_commands", ())
        }
        command_ports.update(
            (item["name"], item)
            for item in component.get("kernel_commands", ())
        )
        command_ports.update(
            (item["name"], item)
            for item in component.get("unit_of_work_plans", ())
        )
        self._queries = {
            item["id"]: (item, query_ports[item["port"]])
            for item in component.get("management_queries", ())
        }
        self._actions = {
            item["id"]: (item, command_ports[item["port"]])
            for item in component.get("management_actions", ())
        }
        self._assets = {
            item["id"]: item for item in component.get("management_assets", ())
        }
        effect_ports = {
            item["name"]: item for item in component.get("effect_services", ())
        }
        self._artifacts = {
            item["id"]: (item, effect_ports[item["port"]])
            for item in component.get("management_artifacts", ())
        }

    async def read(self) -> dict:
        value = await super().read()
        if self._configuration_documents is not None:
            value['documents'] = await self._configuration_documents.read(value)
        return value

    async def plan(self, *, operation_id, expected_desired_revision, value,
                   authorization=None, authorization_guard=None):
        if self._configuration_documents is None:
            return await super().plan(
                operation_id=operation_id, expected_desired_revision=expected_desired_revision,
                value=value, authorization=authorization, authorization_guard=authorization_guard,
            )
        plan = await self.configuration.plan(
            operation_id=operation_id, expected_desired_revision=expected_desired_revision,
            value=value, validation_digest=self.schema_digest,
            authorization=authorization, authorization_guard=authorization_guard,
            materialize_in=self._configuration_documents.materialize_in,
        )
        return {key: plan[key] for key in ('plan_id', 'expected_desired_revision', 'input_digest', 'state')} | {
            'reload_policy': self.reload_policy,
        }

    def describe(self) -> dict:
        value = super().describe()
        value["protocol"] = self.protocol
        bootstrap_catalog_query = self.adapter.component.get("management_ui", {}).get(
            "bootstrap_catalog_query"
        )
        if bootstrap_catalog_query is not None:
            value["bootstrap_catalog_query"] = bootstrap_catalog_query
        value["queries"] = [
            {
                "query_id": operation_id,
                "input_schema": self._public_action_schema(declaration, port),
                "output_schema": self.adapter.schema_snapshot(port["output_schema"]),
                "required_role": declaration.get("required_role", "AUDITOR"),
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
                **(
                    {"secret_field": declaration["secret_field"]}
                    if "secret_field" in declaration else {}
                ),
                "input_schema": self._public_action_schema(declaration, port),
                "output_schema": self.adapter.schema_snapshot(port["output_schema"]),
            }
            for operation_id, (declaration, port) in self._actions.items()
        ]
        value["assets"] = [
            {
                "asset_id": item["id"], "media_types": item["media_types"],
                "max_bytes": item["max_bytes"], "chunk_bytes": 262144,
            }
            for item in self._assets.values()
        ]
        value["artifacts"] = [
            {
                "artifact_id": item["id"],
                "direction": item["direction"],
                "media_type": item["media_type"],
                "max_bytes": item["max_bytes"],
                "required_role": item.get(
                    "required_role", "ADMIN" if item["direction"] == "upload" else "AUDITOR"
                ),
                "input_schema": self.adapter.schema_snapshot(port["input_schema"]),
                "idempotency_key_required": item["direction"] == "upload",
            }
            for item, port in self._artifacts.values()
        ]
        return value

    def artifact_required_role(self, artifact_id: str) -> str:
        try:
            item, _port = self._artifacts[artifact_id]
        except KeyError:
            raise ForestError(
                "MODULE_MANAGEMENT_ARTIFACT_NOT_FOUND",
                "Package artifact operation does not exist", 404,
            ) from None
        return item.get(
            "required_role", "ADMIN" if item["direction"] == "upload" else "AUDITOR"
        )

    async def download_artifact(self, artifact_id: str, payload: dict):
        try:
            declaration, port = self._artifacts[artifact_id]
        except KeyError:
            raise ForestError(
                "MODULE_MANAGEMENT_ARTIFACT_NOT_FOUND",
                "Package artifact operation does not exist", 404,
            ) from None
        if declaration["direction"] != "download":
            raise ForestError(
                "MODULE_MANAGEMENT_ARTIFACT_NOT_FOUND", "Artifact is not a download", 404,
            )
        return await self.adapter.invoke_management_artifact_download(
            port, payload, max_bytes=declaration["max_bytes"],
            media_type=declaration["media_type"],
        )

    @asynccontextmanager
    async def open_artifact_upload(self, artifact_id: str, payload: dict):
        try:
            declaration, port = self._artifacts[artifact_id]
        except KeyError:
            raise ForestError(
                "MODULE_MANAGEMENT_ARTIFACT_NOT_FOUND", "Artifact operation does not exist", 404,
            ) from None
        if declaration["direction"] != "upload":
            raise ForestError(
                "MODULE_MANAGEMENT_ARTIFACT_NOT_FOUND", "Artifact is not an upload", 404,
            )
        async with self.adapter.management_artifact_upload(
            port, payload, max_bytes=declaration["max_bytes"],
        ) as publish:
            yield declaration["max_bytes"], publish

    async def artifact_command_in(
        self, conn, artifact_id: str, phase: str, payload: dict, *, operation_id: str,
    ) -> dict | None:
        declaration, _port = self._artifacts[artifact_id]
        if phase not in {"reserve", "settle"}:
            raise ForestError("MODULE_MANAGEMENT_BINDING_INVALID", "Artifact phase is invalid", 409)
        name = declaration.get(f"{phase}_port")
        if name is None:
            return None
        port = next(
            item for item in self.adapter.component.get("domain_commands", ())
            if item["name"] == name
        )
        return await self.adapter.invoke_management_artifact_command_in(
            conn, port, payload, operation_id=operation_id,
        )

    async def artifact_close(self, artifact_id: str, payload: dict) -> dict | None:
        declaration, _port = self._artifacts[artifact_id]
        name = declaration.get("close_port")
        if name is None:
            return None
        port = next(
            item for item in self.adapter.component.get("effect_services", ())
            if item["name"] == name
        )
        return await self.adapter.invoke_management_artifact_close(port, payload)

    def artifact_terminal_rejection_codes(self, artifact_id: str) -> frozenset[str]:
        declaration, _port = self._artifacts[artifact_id]
        return frozenset(declaration.get("terminal_rejection_codes", ()))

    def asset(self, asset_id: str) -> dict:
        try:
            return self._assets[asset_id]
        except KeyError:
            raise ForestError(
                "MODULE_MANAGEMENT_ASSET_NOT_FOUND",
                "Package management asset does not exist", 404,
            ) from None

    def query_required_role(self, query_id: str) -> str:
        try:
            declaration, _port = self._queries[query_id]
        except KeyError:
            raise ForestError(
                "MODULE_MANAGEMENT_OPERATION_NOT_FOUND", "Package query does not exist", 404
            ) from None
        return declaration.get("required_role", "AUDITOR")

    def query_audit_severity(self, query_id: str) -> str | None:
        try:
            declaration, _port = self._queries[query_id]
        except KeyError:
            raise ForestError(
                "MODULE_MANAGEMENT_OPERATION_NOT_FOUND", "Package query does not exist", 404
            ) from None
        return declaration.get("audit")

    async def query(self, query_id: str, payload: dict) -> object:
        return await self.query_authorized(query_id, payload)

    async def query_authorized(
        self, query_id: str, payload: dict, *, authorization=None
    ) -> object:
        try:
            declaration, port = self._queries[query_id]
        except KeyError:
            raise ForestError(
                "MODULE_MANAGEMENT_OPERATION_NOT_FOUND",
                "Package management query does not exist",
                404,
            ) from None
        actor_field = declaration.get("actor_field")
        if actor_field is not None and actor_field in payload:
            raise ForestError(
                "MODULE_MANAGEMENT_INPUT_INVALID",
                "Authenticated actor fields are supplied by the management gateway", 422,
            )
        query_payload = dict(payload)
        if actor_field is not None:
            actor = getattr(authorization, "web_subject_ref", None)
            if actor is None:
                raise ForestError(
                    "WEB_AUTHORITY_TICKET_REQUIRED", "Actor-bound query requires Web authority", 403,
                )
            query_payload[actor_field] = actor
        return await self.adapter.invoke_management_query(
            port,
            query_payload,
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
    ) -> dict:
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
            actor = getattr(authorization, "actor_ref", None) or getattr(
                authorization, "web_subject_ref", None
            )
            if not isinstance(actor, str) or not actor:
                raise ForestError(
                    "WEB_AUTHORITY_TICKET_REQUIRED",
                    "Actor-bound action requires Web authority",
                    403,
                )
            command_payload[actor_field] = actor
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
        return result

    def _public_action_schema(self, declaration: dict, port: dict) -> dict:
        schema = self.adapter.schema_snapshot(port["input_schema"])
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
