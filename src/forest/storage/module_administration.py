"""Bot component lifecycle metadata and a config-authority reload shortcut."""

from __future__ import annotations

from forest.errors import ForestError
from forest.storage.control import BotControl


class ModuleAdministration:
    """Keep Shell module actions on the same config and lifecycle facts as Web."""

    def __init__(self, store, *, control=None):
        self.store = store
        self.control = control or BotControl(store)

    @staticmethod
    def _read_payload(payload):
        if not isinstance(payload, dict) or not set(payload) <= {"component_id"}:
            raise ForestError("MODULE_CONTROL_INVALID", "Module query fields differ", 422)
        component_id = payload.get("component_id")
        if component_id is not None and (
            not isinstance(component_id, str) or not 1 <= len(component_id) <= 128
        ):
            raise ForestError("MODULE_CONTROL_INVALID", "Component identity is invalid", 422)
        return component_id

    @staticmethod
    def _reload_payload(payload):
        if (
            not isinstance(payload, dict)
            or set(payload) != {"component_id", "expected_revision"}
            or not isinstance(payload["component_id"], str)
            or not 1 <= len(payload["component_id"]) <= 128
            or type(payload["expected_revision"]) is not int
            or payload["expected_revision"] < 1
        ):
            raise ForestError(
                "MODULE_CONTROL_INVALID",
                "Reload requires a component and positive desired revision",
                422,
            )

    @staticmethod
    def _state(row):
        if row["desired_revision"] == 0:
            return "UNCONFIGURED"
        if row["desired_revision"] != row["effective_revision"]:
            return "PENDING"
        if (
            row["generation_id"] is not None
            and row["generation_status"] == "ACTIVE"
            and row["admission"]
        ):
            return "ACTIVE"
        return "INACTIVE"

    async def read(self, ticket, operation_id, payload):
        component_id = self._read_payload(payload)
        async with self.store.pool.connection() as conn, conn.transaction():
            claims = await self.control._guard(
                conn, ticket, "module.read", operation_id, payload
            )
            rows = await (
                await conn.execute(
                    "SELECT h.component_id,h.desired_revision,h.effective_revision,"
                    "d.values_metadata->>'package_digest' AS desired_package_digest,"
                    "e.values_metadata->>'package_digest' AS effective_package_digest,"
                    "r.generation_id,r.route_epoch,r.admission,r.control_admission,"
                    "g.status AS generation_status,l.operation_id AS lifecycle_operation_id,"
                    "l.phase AS lifecycle_phase,l.error_code AS lifecycle_error_code,"
                    "l.created_at AS lifecycle_created_at "
                    "FROM public.bot_config_heads h "
                    "LEFT JOIN public.bot_config_revisions d ON "
                    "(d.bot_id,d.component_id,d.revision)=(h.bot_id,h.component_id,h.desired_revision) "
                    "LEFT JOIN public.bot_config_revisions e ON "
                    "(e.bot_id,e.component_id,e.revision)=(h.bot_id,h.component_id,h.effective_revision) "
                    "LEFT JOIN public.bot_component_routes r ON "
                    "(r.bot_id,r.component_id)=(h.bot_id,h.component_id) "
                    "LEFT JOIN public.bot_generations g ON "
                    "(g.bot_id,g.generation_id)=(r.bot_id,r.generation_id) "
                    "LEFT JOIN LATERAL (SELECT operation_id,phase,error_code,created_at "
                    "FROM public.bot_lifecycle_operations x WHERE x.bot_id=h.bot_id "
                    "AND x.component_id=h.component_id ORDER BY created_at DESC,operation_id DESC "
                    "LIMIT 1) l ON true WHERE h.bot_id=%s "
                    "AND (%s::text IS NULL OR h.component_id=%s) "
                    "ORDER BY h.component_id LIMIT 64",
                    (self.store.bot_id, component_id, component_id),
                )
            ).fetchall()
            if component_id is not None and not rows:
                raise ForestError("MODULE_NOT_FOUND", "Component is not configured", 404)
            await self.control._time(conn, claims)
            return {
                "modules": [
                    {
                        **{key: row[key] for key in (
                            "component_id", "desired_revision", "effective_revision",
                            "desired_package_digest", "effective_package_digest",
                            "generation_id", "route_epoch", "admission", "control_admission",
                            "generation_status", "lifecycle_operation_id", "lifecycle_phase",
                            "lifecycle_error_code",
                        )},
                        "state": self._state(row),
                        "lifecycle_created_at": (
                            row["lifecycle_created_at"].isoformat()
                            if row["lifecycle_created_at"] else None
                        ),
                    }
                    for row in rows
                ]
            }

    async def reload(self, ticket, operation_id, payload):
        self._reload_payload(payload)
        component_id = payload["component_id"]
        expected = payload["expected_revision"]
        apply_operation = operation_id + ":module-apply"

        # Read the immutable source only after authorizing this exact command.
        # BotControl.plan repeats the head CAS, so a concurrent Web edit wins or
        # loses normally instead of being silently overwritten by this copy.
        async with self.store.pool.connection() as conn, conn.transaction():
            claims = await self.control._guard(
                conn, ticket, "module.reload", operation_id, payload
            )
            replay = await self.control._replay(conn, claims)
            if self.control.is_replay(replay):
                await self.control._time(conn, claims)
                return replay
            recovered = await self._recover_apply_in(
                conn, claims, operation_id, apply_operation, component_id, expected
            )
            if recovered is not None:
                return recovered
            head = await (
                await conn.execute(
                    "SELECT * FROM public.bot_config_heads WHERE bot_id=%s "
                    "AND component_id=%s FOR UPDATE",
                    (self.store.bot_id, component_id),
                )
            ).fetchone()
            if head is None or head["desired_revision"] == 0:
                raise ForestError("MODULE_NOT_FOUND", "Component is not configured", 404)
            if head["desired_revision"] != expected:
                raise ForestError(
                    "REVISION_CONFLICT", "Component configuration changed; inspect it again", 409
                )
            source = await self.control.config_revision_in(conn, component_id, expected)
            await self.control._time(conn, claims)
            values = source["values"]

        plan_operation = operation_id + ":module-plan"
        plan_payload = {
            "component_id": component_id,
            "expected_revision": expected,
            "source_revision": expected,
            "values": values,
        }
        plan_ticket = self.control.delegate_module_reload(
            "config.plan", plan_operation, plan_payload
        )
        plan = await self.control.plan(plan_ticket, plan_operation, plan_payload)
        if self.control.is_unavailable(plan) or not isinstance(plan, dict):
            raise ForestError(
                "CONTROL_CONTENT_REVOKED", "Reload plan content is unavailable", 409
            )

        apply_payload = {
            "component_id": component_id,
            "expected_revision": expected,
            "plan_id": plan["plan_id"],
            "plan_digest": plan["plan_digest"],
        }
        apply_ticket = self.control.delegate_module_reload(
            "config.apply",
            apply_operation,
            apply_payload,
            plan_digest=plan["plan_digest"],
        )
        applied = await self.control.apply(apply_ticket, apply_operation, apply_payload)
        if self.control.is_unavailable(applied) or not isinstance(applied, dict):
            raise ForestError(
                "CONTROL_CONTENT_REVOKED", "Reload acceptance content is unavailable", 409
            )

        result = {
            "operation_id": operation_id,
            "component_id": component_id,
            "reload_of_revision": expected,
            "desired_revision": applied["desired_revision"],
            "effective_revision": applied["effective_revision"],
            "status": applied["status"],
        }
        async with self.store.pool.connection() as conn, conn.transaction():
            claims = await self.control._guard(
                conn, ticket, "module.reload", operation_id, payload
            )
            replay = await self.control._replay(conn, claims)
            if self.control.is_replay(replay):
                await self.control._time(conn, claims)
                return replay
            operation = await (
                await conn.execute(
                    "SELECT action,input_digest,subject_id,issuer_id,result_payload_id "
                    "FROM public.bot_web_operations WHERE bot_id=%s AND operation_id=%s",
                    (self.store.bot_id, apply_operation),
                )
            ).fetchone()
            if (
                operation is None
                or operation["action"] != "config.apply"
                or operation["subject_id"] != claims["subject_id"]
                or operation["issuer_id"] != claims["issuer_id"]
                or operation["result_payload_id"] is None
            ):
                raise ForestError(
                    "CONTROL_SOURCE_CONFLICT", "Reload apply receipt differs", 409
                )
            await self.control._record(
                conn, claims, result, parents=(operation["result_payload_id"],)
            )
            await self.control._time(conn, claims)
            return result

    async def _recover_apply_in(
        self, conn, claims, operation_id, apply_operation, component_id, expected
    ):
        """Finish the wrapper if config.apply committed before the process stopped."""
        operation = await (
            await conn.execute(
                "SELECT * FROM public.bot_web_operations WHERE bot_id=%s AND operation_id=%s",
                (self.store.bot_id, apply_operation),
            )
        ).fetchone()
        if operation is None:
            return None
        basis = operation["ticket_basis"]
        if (
            operation["action"] != "config.apply"
            or operation["subject_id"] != claims["subject_id"]
            or operation["issuer_id"] != claims["issuer_id"]
            or not isinstance(basis, dict)
            or basis.get("origin_kind") != "shell"
            or basis.get("command_id") != self.control.command_id
            or basis.get("source_binding_id") != self.control.binding_id
            or basis.get("source_event_id") != claims["source_event_id"]
            or basis.get("source_conversation_id") != claims["source_conversation_id"]
        ):
            raise ForestError("CONTROL_SOURCE_CONFLICT", "Reload apply receipt differs", 409)
        epoch = (await self.control.content.payloads.lock_in(conn))["deletion_epoch"]
        admitted = await self.control.content.hydrate_input_in(conn, operation, epoch=epoch)
        recorded = await self.control.content.hydrate_result_in(conn, operation, epoch=epoch)
        apply_payload, applied = admitted["input"], recorded["result"]
        valid_input = (
            isinstance(apply_payload, dict)
            and set(apply_payload) == {
                "component_id", "expected_revision", "plan_id", "plan_digest"
            }
            and apply_payload["component_id"] == component_id
            and apply_payload["expected_revision"] == expected
            and isinstance(apply_payload["plan_id"], str)
            and isinstance(apply_payload["plan_digest"], str)
            and basis.get("plan_digest") == apply_payload["plan_digest"]
        )
        valid_result = (
            isinstance(applied, dict)
            and applied.get("operation_id") == apply_operation
            and applied.get("desired_revision") == expected + 1
            and type(applied.get("effective_revision")) is int
            and applied.get("status") == "ACCEPTED"
        )
        revision = await (
            await conn.execute(
                "SELECT 1 FROM public.bot_config_revisions WHERE bot_id=%s "
                "AND component_id=%s AND revision=%s AND previous_revision=%s "
                "AND plan_id=%s AND operation_id=%s",
                (
                    self.store.bot_id, component_id, expected + 1, expected,
                    apply_payload.get("plan_id") if isinstance(apply_payload, dict) else None,
                    apply_operation,
                ),
            )
        ).fetchone()
        if not valid_input or not valid_result or revision is None:
            raise ForestError("CONTROL_SOURCE_CONFLICT", "Reload apply receipt differs", 409)
        result = {
            "operation_id": operation_id,
            "component_id": component_id,
            "reload_of_revision": expected,
            "desired_revision": applied["desired_revision"],
            "effective_revision": applied["effective_revision"],
            "status": applied["status"],
        }
        await self.control._record(
            conn, claims, result, parents=(operation["result_payload_id"],)
        )
        await self.control._time(conn, claims)
        return result
