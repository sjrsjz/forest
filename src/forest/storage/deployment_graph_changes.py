"""Durable, lease-fenced deployment RuntimeGraph change requests."""

from __future__ import annotations

import inspect
import re
from uuid import NAMESPACE_URL, uuid5

from forest.errors import ForestError
from forest.packages import digest_json
from forest.storage.deployment_generation import DeploymentGenerationAuthority
from forest.web_authority_ticket import WebAuthorityTicket

_COMPONENT = re.compile(r"[a-z0-9][a-z0-9._-]{0,127}\Z")
_DIGEST = re.compile(r"sha256:[a-f0-9]{64}\Z")


class DeploymentGraphChangeAuthority:
    def __init__(self, store) -> None:
        self.store = store
        self.generations = DeploymentGenerationAuthority(store)

    async def enqueue_web(
        self, *, lease, ticket: WebAuthorityTicket, body: dict, authorize_in
    ) -> dict:
        if not isinstance(ticket, WebAuthorityTicket):
            raise ForestError("WEB_AUTHORITY_TICKET_INVALID", "Web ticket is required", 403)
        if not isinstance(body, dict) or set(body) != {
            "component_id",
            "package_digest",
            "expected_graph_revision",
            "plan_digest",
        }:
            raise ForestError("DEPLOYMENT_GRAPH_CHANGE_INPUT_INVALID", "Graph change fields differ", 422)
        component_id = body["component_id"]
        package_digest = body["package_digest"]
        revision = body["expected_graph_revision"]
        plan_digest = body["plan_digest"]
        if (
            not isinstance(component_id, str)
            or _COMPONENT.fullmatch(component_id) is None
            or not isinstance(package_digest, str)
            or _DIGEST.fullmatch(package_digest) is None
            or not isinstance(plan_digest, str)
            or _DIGEST.fullmatch(plan_digest) is None
            or type(revision) is not int
            or revision < 1
            or ticket.target_component_id != "$runtime"
            or ticket.action_key not in {"module.add", "module.replace"}
            or ticket.input_digest != digest_json(body)
        ):
            raise ForestError(
                "DEPLOYMENT_GRAPH_CHANGE_INPUT_INVALID", "Graph change input is invalid", 422
            )
        operation_id = ticket.target_operation_ref
        operation_kind = "ADD" if ticket.action_key == "module.add" else "REPLACE"
        activation_id = str(
            uuid5(
                NAMESPACE_URL,
                f"forest:{self.store.deployment_id}:graph-change:{operation_id}",
            )
        )
        async with self.store.pool.connection() as conn, conn.transaction():
            await self.generations._lease_in(conn, lease)
            await conn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                (f"forest.deployment.graph-change/{self.store.deployment_id}/{operation_id}",),
            )
            existing = await (
                await conn.execute(
                    "SELECT * FROM forest_deployment.runtime_graph_change_requests "
                    "WHERE deployment_id=%s AND operation_id=%s",
                    (self.store.deployment_id, operation_id),
                )
            ).fetchone()
            if existing is not None:
                if (
                    existing["input_digest"] != ticket.input_digest
                    or existing["plan_digest"] != plan_digest
                    or existing["requested_by"] != ticket.web_subject_ref
                    or existing["operation_kind"] != operation_kind
                ):
                    raise ForestError(
                        "DEPLOYMENT_GRAPH_CHANGE_CONFLICT",
                        "Graph change operation belongs to another input or actor",
                        409,
                    )
                return dict(existing)
            if ticket.target_graph_revision != revision:
                raise ForestError(
                    "RUNTIME_GRAPH_REVISION_CONFLICT", "Deployment Graph changed", 409
                )
            clock = await (
                await conn.execute(
                    "SELECT active_graph_id,active_revision FROM "
                    "forest_deployment.runtime_graph_clock WHERE deployment_id=%s FOR UPDATE",
                    (self.store.deployment_id,),
                )
            ).fetchone()
            if clock is None or clock["active_revision"] != revision:
                raise ForestError(
                    "RUNTIME_GRAPH_REVISION_CONFLICT", "Deployment Graph changed", 409
                )
            member = await (
                await conn.execute(
                    "SELECT 1 FROM forest_deployment.runtime_graph_members "
                    "WHERE deployment_id=%s AND graph_id=%s AND component_id=%s",
                    (self.store.deployment_id, clock["active_graph_id"], component_id),
                )
            ).fetchone()
            if (member is None) != (operation_kind == "ADD"):
                raise ForestError(
                    "RUNTIME_MODULE_SELECTION_CONFLICT",
                    "Deployment module membership differs from the requested operation", 409,
                )
            receipt = authorize_in(conn)
            if inspect.isawaitable(receipt):
                receipt = await receipt
            if not isinstance(receipt, dict) or not isinstance(receipt.get("ticket_id"), str):
                raise ForestError(
                    "WEB_AUTHORITY_TRANSACTION_REQUIRED", "Web receipt is required", 503
                )
            saved = await (
                await conn.execute(
                    "INSERT INTO forest_deployment.runtime_graph_change_requests("
                    "deployment_id,operation_id,operation_kind,input_digest,plan_digest,requested_by,target_component_id,"
                    "package_digest,expected_graph_revision,activation_id,receipt_ticket_id) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *",
                    (
                        self.store.deployment_id,
                        operation_id,
                        operation_kind,
                        ticket.input_digest,
                        plan_digest,
                        ticket.web_subject_ref,
                        component_id,
                        package_digest,
                        revision,
                        activation_id,
                        receipt["ticket_id"],
                    ),
                )
            ).fetchone()
            return dict(saved)

    async def enqueue_configuration(
        self, *, lease, ticket: WebAuthorityTicket, configuration,
        plan_id: str, expected_desired_revision: int, validation_digest: str,
        authorize_in,
    ) -> dict:
        """Commit desired configuration and its exact Graph request together."""

        if not isinstance(ticket, WebAuthorityTicket):
            raise ForestError("WEB_AUTHORITY_TICKET_INVALID", "Web ticket is required", 403)
        component_id = configuration.component_id
        operation_id = ticket.target_operation_ref
        input_body = {
            "plan_id": plan_id,
            "expected_desired_revision": expected_desired_revision,
        }
        plan_digest = digest_json({"plan_id": plan_id})
        if (
            ticket.action_key != "configuration.apply"
            or ticket.target_component_id != component_id
            or ticket.input_digest != digest_json(input_body)
            or ticket.plan_digest != plan_digest
            or type(expected_desired_revision) is not int
            or expected_desired_revision < 1
        ):
            raise ForestError(
                "DEPLOYMENT_CONFIGURATION_CHANGE_INVALID",
                "Configuration change input differs from its Web authority", 422,
            )
        activation_id = str(uuid5(
            NAMESPACE_URL,
            f"forest:{self.store.deployment_id}:graph-change:{operation_id}",
        ))
        async with self.store.pool.connection() as conn, conn.transaction():
            await self.generations._lease_in(conn, lease)
            clock = await (
                await conn.execute(
                    "SELECT active_graph_id,active_revision FROM "
                    "forest_deployment.runtime_graph_clock WHERE deployment_id=%s FOR UPDATE",
                    (self.store.deployment_id,),
                )
            ).fetchone()
            await conn.execute(
                "SELECT pg_advisory_xact_lock(hashtextextended(%s,0))",
                (f"forest.deployment.graph-change/{self.store.deployment_id}/{operation_id}",),
            )
            existing = await (
                await conn.execute(
                    "SELECT * FROM forest_deployment.runtime_graph_change_requests "
                    "WHERE deployment_id=%s AND operation_id=%s",
                    (self.store.deployment_id, operation_id),
                )
            ).fetchone()
            if existing is not None:
                if (
                    existing["operation_kind"] != "CONFIGURE"
                    or existing["input_digest"] != ticket.input_digest
                    or existing["plan_digest"] != plan_digest
                    or existing["requested_by"] != ticket.web_subject_ref
                    or existing["target_component_id"] != component_id
                ):
                    raise ForestError(
                        "DEPLOYMENT_GRAPH_CHANGE_CONFLICT",
                        "Configuration operation belongs to another input or actor", 409,
                    )
                saved = await configuration.apply_in(
                    conn, operation_id=operation_id, plan_id=plan_id,
                    expected_desired_revision=expected_desired_revision,
                    expected_validation_digest=validation_digest,
                    target_generation_id=ticket.target_generation_id,
                    target_graph_revision=ticket.target_graph_revision,
                )
                if saved.revision != existing["configuration_revision"] or (
                    saved.payload_digest != existing["configuration_payload_digest"]
                ):
                    raise ForestError(
                        "DEPLOYMENT_GRAPH_CHANGE_RESULT_INVALID",
                        "Configuration request differs from its revision", 409,
                    )
                return {"configuration": saved, "request": dict(existing)}
            if clock is None or (
                clock["active_revision"] != ticket.target_graph_revision
                or clock["active_graph_id"] is None
            ):
                raise ForestError(
                    "RUNTIME_GRAPH_REVISION_CONFLICT", "Deployment Graph changed", 409,
                )
            await self.generations.verify_active_generation_in(
                conn, component_id=component_id,
                generation_id=ticket.target_generation_id, admission_lane="CONTROL",
            )
            member = await (
                await conn.execute(
                    "SELECT g.binding FROM forest_deployment.runtime_graph_members m "
                    "JOIN forest_deployment.runtime_generations g "
                    "ON g.deployment_id=m.deployment_id AND g.generation_id=m.generation_id "
                    "WHERE m.deployment_id=%s AND m.graph_id=%s AND m.component_id=%s",
                    (self.store.deployment_id, clock["active_graph_id"], component_id),
                )
            ).fetchone()
            if member is None or not isinstance(member["binding"], dict):
                raise ForestError("RUNTIME_GENERATION_BINDING_INVALID", "Generation binding is absent", 409)
            package_binding = member["binding"].get("package")
            package_digest = (
                package_binding.get("package_digest")
                if isinstance(package_binding, dict) else None
            )
            if not isinstance(package_digest, str) or _DIGEST.fullmatch(package_digest) is None:
                raise ForestError("RUNTIME_GENERATION_BINDING_INVALID", "Package binding is invalid", 409)
            pending = await (
                await conn.execute(
                    "SELECT 1 FROM forest_deployment.runtime_graph_change_requests "
                    "WHERE deployment_id=%s AND target_component_id=%s "
                    "AND state IN ('QUEUED','RUNNING') LIMIT 1",
                    (self.store.deployment_id, component_id),
                )
            ).fetchone()
            if pending is not None:
                raise ForestError(
                    "DEPLOYMENT_CONFIGURATION_CHANGE_PENDING",
                    "Component already has an unsettled Graph change", 409,
                )
            receipt = authorize_in(conn)
            if inspect.isawaitable(receipt):
                receipt = await receipt
            if not isinstance(receipt, dict) or not isinstance(receipt.get("ticket_id"), str):
                raise ForestError(
                    "WEB_AUTHORITY_TRANSACTION_REQUIRED", "Web receipt is required", 503,
                )
            saved = await configuration.apply_in(
                conn, operation_id=operation_id, plan_id=plan_id,
                expected_desired_revision=expected_desired_revision,
                expected_validation_digest=validation_digest,
                target_generation_id=ticket.target_generation_id,
                target_graph_revision=ticket.target_graph_revision,
            )
            request = await (
                await conn.execute(
                    "INSERT INTO forest_deployment.runtime_graph_change_requests("
                    "deployment_id,operation_id,operation_kind,input_digest,plan_digest,"
                    "requested_by,target_component_id,package_digest,configuration_revision,"
                    "configuration_payload_digest,expected_graph_revision,activation_id,"
                    "receipt_ticket_id) VALUES (%s,%s,'CONFIGURE',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                    "RETURNING *",
                    (
                        self.store.deployment_id, operation_id, ticket.input_digest,
                        plan_digest, ticket.web_subject_ref, component_id, package_digest,
                        saved.revision, saved.payload_digest, clock["active_revision"],
                        activation_id, receipt["ticket_id"],
                    ),
                )
            ).fetchone()
            return {"configuration": saved, "request": dict(request)}

    async def claim(self, lease) -> dict | None:
        async with self.store.pool.connection() as conn, conn.transaction():
            await self.generations._lease_in(conn, lease)
            row = await (
                await conn.execute(
                    "SELECT * FROM forest_deployment.runtime_graph_change_requests "
                    "WHERE deployment_id=%s AND (state='QUEUED' OR "
                    "(state='RUNNING' AND owner_epoch<%s)) "
                    "ORDER BY created_at,operation_id FOR UPDATE SKIP LOCKED LIMIT 1",
                    (self.store.deployment_id, lease.owner_epoch),
                )
            ).fetchone()
            if row is None:
                return None
            saved = await (
                await conn.execute(
                    "UPDATE forest_deployment.runtime_graph_change_requests "
                    "SET state='RUNNING',owner_epoch=%s,attempt_count=attempt_count+1,"
                    "updated_at=clock_timestamp() WHERE deployment_id=%s AND operation_id=%s "
                    "RETURNING *",
                    (lease.owner_epoch, self.store.deployment_id, row["operation_id"]),
                )
            ).fetchone()
            return dict(saved)

    async def published_graph(self, request: dict) -> dict | None:
        async with self.store.pool.connection() as conn:
            row = await (
                await conn.execute(
                    "SELECT g.graph_id,g.graph_revision,g.state,p.binding FROM "
                    "forest_deployment.runtime_graphs g JOIN "
                    "forest_deployment.runtime_graph_members m ON "
                    "m.deployment_id=g.deployment_id AND m.graph_id=g.graph_id JOIN "
                    "forest_deployment.runtime_generations p ON "
                    "p.deployment_id=m.deployment_id AND p.generation_id=m.generation_id "
                    "WHERE g.deployment_id=%s AND g.activation_id=%s "
                    "AND m.component_id=%s",
                    (
                        self.store.deployment_id,
                        request["activation_id"],
                        request["target_component_id"],
                    ),
                )
            ).fetchone()
            if row is None or row["state"] not in {"ACTIVE", "RETIRED"}:
                return None
            if row["binding"]["package"]["package_digest"] != request["package_digest"]:
                raise ForestError(
                    "DEPLOYMENT_GRAPH_CHANGE_RESULT_INVALID",
                    "Published package differs from the frozen request",
                    409,
                )
            if request["operation_kind"] == "CONFIGURE":
                configuration = row["binding"].get("configuration")
                if (
                    not isinstance(configuration, dict)
                    or configuration.get("revision") != request["configuration_revision"]
                    or configuration.get("payload_digest")
                    != request["configuration_payload_digest"]
                ):
                    raise ForestError(
                        "DEPLOYMENT_GRAPH_CHANGE_RESULT_INVALID",
                        "Published configuration differs from the frozen request", 409,
                    )
            return dict(row)

    async def settle(
        self,
        lease,
        *,
        operation_id: str,
        graph: dict | None = None,
        error_code: str | None = None,
    ) -> dict:
        if (graph is None) == (error_code is None) or (
            error_code is not None and re.fullmatch(r"[A-Z][A-Z0-9_]{0,127}", error_code) is None
        ):
            raise ForestError("DEPLOYMENT_GRAPH_CHANGE_RESULT_INVALID", "Invalid result", 422)
        async with self.store.pool.connection() as conn, conn.transaction():
            await self.generations._lease_in(conn, lease)
            row = await (
                await conn.execute(
                    "SELECT * FROM forest_deployment.runtime_graph_change_requests "
                    "WHERE deployment_id=%s AND operation_id=%s FOR UPDATE",
                    (self.store.deployment_id, operation_id),
                )
            ).fetchone()
            if row is None:
                raise ForestError("DEPLOYMENT_GRAPH_CHANGE_NOT_FOUND", "Request is absent", 404)
            if row["state"] in {"SUCCEEDED", "FAILED"}:
                if (
                    row["state"] != ("SUCCEEDED" if graph is not None else "FAILED")
                    or row["result_graph_id"] != (graph["graph_id"] if graph is not None else None)
                    or row["result_graph_revision"]
                    != (graph["graph_revision"] if graph is not None else None)
                    or row["error_code"] != error_code
                ):
                    raise ForestError(
                        "DEPLOYMENT_GRAPH_CHANGE_CONFLICT",
                        "Graph change terminal result differs",
                        409,
                    )
                return dict(row)
            if row["state"] != "RUNNING" or row["owner_epoch"] != lease.owner_epoch:
                raise ForestError("STALE_DEPLOYMENT_GRAPH_CHANGE", "Request owner changed", 409)
            if graph is not None:
                proof = await (
                    await conn.execute(
                        "SELECT graph_id,graph_revision,state FROM forest_deployment.runtime_graphs "
                        "WHERE deployment_id=%s AND activation_id=%s",
                        (self.store.deployment_id, row["activation_id"]),
                    )
                ).fetchone()
                if (
                    proof is None
                    or proof["graph_id"] != graph["graph_id"]
                    or proof["graph_revision"] != graph["graph_revision"]
                    or proof["state"] not in {"ACTIVE", "RETIRED"}
                ):
                    raise ForestError(
                        "DEPLOYMENT_GRAPH_CHANGE_RESULT_INVALID", "Graph proof differs", 409
                    )
            else:
                proof = await (
                    await conn.execute(
                        "SELECT state FROM forest_deployment.runtime_graphs "
                        "WHERE deployment_id=%s AND activation_id=%s",
                        (self.store.deployment_id, row["activation_id"]),
                    )
                ).fetchone()
                if proof is not None and proof["state"] in {"ACTIVE", "RETIRED"}:
                    raise ForestError(
                        "DEPLOYMENT_GRAPH_CHANGE_RESULT_INVALID", "Published Graph cannot fail", 409
                    )
            saved = await (
                await conn.execute(
                    "UPDATE forest_deployment.runtime_graph_change_requests "
                    "SET state=%s,result_graph_id=%s,result_graph_revision=%s,error_code=%s,"
                    "updated_at=clock_timestamp() WHERE deployment_id=%s AND operation_id=%s "
                    "RETURNING *",
                    (
                        "SUCCEEDED" if graph is not None else "FAILED",
                        graph["graph_id"] if graph is not None else None,
                        graph["graph_revision"] if graph is not None else None,
                        error_code,
                        self.store.deployment_id,
                        operation_id,
                    ),
                )
            ).fetchone()
            return dict(saved)

    async def find(self, operation_id: str) -> dict | None:
        async with self.store.pool.connection() as conn:
            row = await (
                await conn.execute(
                    "SELECT * FROM forest_deployment.runtime_graph_change_requests "
                    "WHERE deployment_id=%s AND operation_id=%s",
                    (self.store.deployment_id, operation_id),
                )
            ).fetchone()
            return dict(row) if row is not None else None

    async def get(self, operation_id: str) -> dict:
        row = await self.find(operation_id)
        if row is None:
            raise ForestError("DEPLOYMENT_GRAPH_CHANGE_NOT_FOUND", "Request is absent", 404)
        return row
