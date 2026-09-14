"""Bot component lifecycle facts, validation, and desired-state administration."""

from __future__ import annotations

from forest.errors import ForestError
from forest.module_validation import validate_desired_snapshot
from forest.storage.control import BotControl


class ModuleAdministration:
    """Keep Shell module actions on the same config and lifecycle facts as Web."""

    def __init__(self, store, *, control=None, packages=None):
        self.store = store
        self.control = control or BotControl(store)
        self.packages = {} if packages is None else packages

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
        if not row["desired_enabled"]:
            if row["desired_state_revision"] != row["effective_state_revision"]:
                return "PENDING_DISABLE"
            if row["generation_id"] is None and not row["admission"]:
                return "DISABLED"
            return "DEGRADED"
        if not row["effective_enabled"]:
            return "PENDING_ENABLE"
        if row["desired_revision"] != row["effective_revision"]:
            return "PENDING_ENABLE"
        if (
            row["generation_id"] is not None
            and row["generation_status"] == "ACTIVE"
            and row["admission"]
        ):
            return "ACTIVE"
        return "DEGRADED"

    async def _state_head_in(self, conn, component_id, *, create=False):
        if create:
            await conn.execute(
                "INSERT INTO public.bot_module_state_heads(bot_id,component_id) "
                "VALUES (%s,%s) ON CONFLICT DO NOTHING",
                (self.store.bot_id, component_id),
            )
        return await (
            await conn.execute(
                "SELECT * FROM public.bot_module_state_heads WHERE bot_id=%s "
                "AND component_id=%s " + ("FOR UPDATE" if create else "FOR SHARE"),
                (self.store.bot_id, component_id),
            )
        ).fetchone()

    @staticmethod
    async def _state_revision_in(conn, bot_id, component_id, revision):
        if revision == 0:
            return {"revision": 0, "enabled": True}
        return await (
            await conn.execute(
                "SELECT * FROM public.bot_module_state_revisions WHERE bot_id=%s "
                "AND component_id=%s AND revision=%s",
                (bot_id, component_id, revision),
            )
        ).fetchone()

    @staticmethod
    def _validate_payload(payload):
        if (
            not isinstance(payload, dict)
            or set(payload) != {
                "component_id", "expected_config_revision", "expected_state_revision"
            }
            or not isinstance(payload["component_id"], str)
            or not 1 <= len(payload["component_id"]) <= 128
            or type(payload["expected_config_revision"]) is not int
            or payload["expected_config_revision"] < 1
            or type(payload["expected_state_revision"]) is not int
            or payload["expected_state_revision"] < 0
        ):
            raise ForestError(
                "MODULE_CONTROL_INVALID", "Module validation snapshot is invalid", 422
            )
        return payload

    @staticmethod
    def _state_payload(payload):
        if (
            not isinstance(payload, dict)
            or set(payload) != {
                "component_id", "expected_state_revision", "enabled", "reason"
            }
            or not isinstance(payload["component_id"], str)
            or not 1 <= len(payload["component_id"]) <= 128
            or type(payload["expected_state_revision"]) is not int
            or payload["expected_state_revision"] < 0
            or type(payload["enabled"]) is not bool
            or not isinstance(payload["reason"], str)
            or not payload["reason"].strip()
            or len(payload["reason"]) > 256
            or any(ord(character) < 32 for character in payload["reason"])
        ):
            raise ForestError("MODULE_CONTROL_INVALID", "Module state input is invalid", 422)
        return payload

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
                    "COALESCE(m.desired_revision,0) AS desired_state_revision,"
                    "COALESCE(m.effective_revision,0) AS effective_state_revision,"
                    "COALESCE(md.enabled,true) AS desired_enabled,"
                    "COALESCE(me.enabled,true) AS effective_enabled,"
                    "r.generation_id,r.route_epoch,r.admission,r.control_admission,"
                    "g.status AS generation_status,l.operation_id AS lifecycle_operation_id,"
                    "l.phase AS lifecycle_phase,l.error_code AS lifecycle_error_code,"
                    "l.created_at AS lifecycle_created_at "
                    "FROM public.bot_config_heads h "
                    "LEFT JOIN public.bot_config_revisions d ON "
                    "(d.bot_id,d.component_id,d.revision)=(h.bot_id,h.component_id,h.desired_revision) "
                    "LEFT JOIN public.bot_config_revisions e ON "
                    "(e.bot_id,e.component_id,e.revision)=(h.bot_id,h.component_id,h.effective_revision) "
                    "LEFT JOIN public.bot_module_state_heads m ON "
                    "(m.bot_id,m.component_id)=(h.bot_id,h.component_id) "
                    "LEFT JOIN public.bot_module_state_revisions md ON "
                    "(md.bot_id,md.component_id,md.revision)=(m.bot_id,m.component_id,m.desired_revision) "
                    "LEFT JOIN public.bot_module_state_revisions me ON "
                    "(me.bot_id,me.component_id,me.revision)=(m.bot_id,m.component_id,m.effective_revision) "
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
                            "desired_state_revision", "effective_state_revision",
                            "desired_enabled", "effective_enabled",
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

    async def validate(self, ticket, operation_id, payload):
        payload = self._validate_payload(payload)
        async with self.store.pool.connection() as conn, conn.transaction():
            claims = await self.control._guard(
                conn, ticket, "module.validate", operation_id, payload
            )
            heads = await (
                await conn.execute(
                    "SELECT * FROM public.bot_config_heads WHERE bot_id=%s "
                    "AND desired_revision>0 ORDER BY component_id FOR SHARE",
                    (self.store.bot_id,),
                )
            ).fetchall()
            state_heads = await (
                await conn.execute(
                    "SELECT component_id,desired_revision FROM "
                    "public.bot_module_state_heads WHERE bot_id=%s "
                    "ORDER BY component_id FOR SHARE",
                    (self.store.bot_id,),
                )
            ).fetchall()
            state_revisions = {
                row["component_id"]: row["desired_revision"] for row in state_heads
            }
            target = next(
                (row for row in heads if row["component_id"] == payload["component_id"]), None
            )
            if target is None:
                raise ForestError("MODULE_NOT_FOUND", "Component is not configured", 404)
            if (
                target["desired_revision"] != payload["expected_config_revision"]
                or state_revisions.get(target["component_id"], 0)
                != payload["expected_state_revision"]
            ):
                raise ForestError(
                    "REVISION_CONFLICT", "Module desired snapshot changed; inspect it again", 409
                )
            revisions = []
            for head in heads:
                state_revision = state_revisions.get(head["component_id"], 0)
                saved = await self.control.config_revision_in(
                    conn, head["component_id"], head["desired_revision"]
                )
                state = await self._state_revision_in(
                    conn, self.store.bot_id, head["component_id"], state_revision
                )
                if state is None:
                    raise ForestError("MODULE_STATE_CONFLICT", "Module state revision is absent", 409)
                revisions.append({
                    **saved,
                    "desired_enabled": state["enabled"],
                })
            plan, snapshots = validate_desired_snapshot(
                self.packages,
                revisions,
                include_disabled=frozenset({payload["component_id"]}),
            )
            await self.control._time(conn, claims)
            snapshot = snapshots[payload["component_id"]]
            return {
                "component_id": payload["component_id"],
                "package_digest": snapshot.digest,
                "config_revision": target["desired_revision"],
                "state_revision": state_revisions.get(target["component_id"], 0),
                "required_components": list(plan.closures[payload["component_id"]]),
                "status": "VALID",
            }

    async def set_state(self, ticket, operation_id, payload):
        payload = self._state_payload(payload)
        action = "module.enable" if payload["enabled"] else "module.disable"
        async with self.store.pool.connection() as conn, conn.transaction():
            claims = await self.control._guard(conn, ticket, action, operation_id, payload)
            config = await (
                await conn.execute(
                    "SELECT * FROM public.bot_config_heads WHERE bot_id=%s "
                    "AND component_id=%s FOR UPDATE",
                    (self.store.bot_id, payload["component_id"]),
                )
            ).fetchone()
            if config is None or config["desired_revision"] == 0:
                raise ForestError("MODULE_NOT_FOUND", "Component is not configured", 404)
            head = await self._state_head_in(conn, payload["component_id"], create=True)
            replay = await self.control._replay(conn, claims)
            if self.control.is_replay(replay):
                return replay
            if head["desired_revision"] != payload["expected_state_revision"]:
                raise ForestError(
                    "REVISION_CONFLICT", "Module state changed; inspect it again", 409
                )
            current = await self._state_revision_in(
                conn, self.store.bot_id, payload["component_id"], head["desired_revision"]
            )
            if current["enabled"] == payload["enabled"]:
                raise ForestError(
                    "MODULE_STATE_UNCHANGED", "Module already has the requested desired state", 409
                )
            if (
                not payload["enabled"]
                and payload["component_id"] == "adapter.onebot"
                and claims.get("origin_kind") == "shell"
            ):
                raise ForestError(
                    "SHELL_CONTROL_CHANNEL_REQUIRED",
                    "The active group control Adapter cannot disable itself",
                    409,
                )
            revision = head["desired_revision"] + 1
            result = {
                "operation_id": operation_id,
                "component_id": payload["component_id"],
                "desired_config_revision": config["desired_revision"],
                "effective_config_revision": config["effective_revision"],
                "desired_state_revision": revision,
                "effective_state_revision": head["effective_revision"],
                "enabled": payload["enabled"],
                "status": "ACCEPTED",
            }
            await self.control._record(conn, claims, result)
            await conn.execute(
                "INSERT INTO public.bot_module_state_revisions("
                "bot_id,component_id,revision,previous_revision,enabled,reason,"
                "policy_revision,operation_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    self.store.bot_id, payload["component_id"], revision, revision - 1,
                    payload["enabled"], payload["reason"], claims["policy_revision"], operation_id,
                ),
            )
            await conn.execute(
                "UPDATE public.bot_module_state_heads SET desired_revision=%s "
                "WHERE bot_id=%s AND component_id=%s",
                (revision, self.store.bot_id, payload["component_id"]),
            )
            await self.control._time(conn, claims)
            return result

    async def bind_activation(
        self, lease, component_id, revision, operation_id, *, enabled
    ):
        if revision == 0:
            if not enabled:
                raise ForestError("MODULE_STATE_CONFLICT", "Default module state is enabled", 409)
            return
        async with self.store.pool.connection() as conn, conn.transaction():
            await self.store._host(conn, lease)
            head = await self._state_head_in(conn, component_id, create=False)
            saved = await self._state_revision_in(
                conn, self.store.bot_id, component_id, revision
            )
            if (
                head is None
                or head["desired_revision"] != revision
                or saved is None
                or saved["enabled"] != enabled
            ):
                raise ForestError(
                    "MODULE_STATE_SUPERSEDED", "A newer module state is pending", 409
                )
            existing = await (
                await conn.execute(
                    "SELECT * FROM public.bot_module_state_activations "
                    "WHERE bot_id=%s AND operation_id=%s",
                    (self.store.bot_id, operation_id),
                )
            ).fetchone()
            if existing is not None:
                if any(existing[key] != value for key, value in {
                    "component_id": component_id, "revision": revision, "enabled": enabled,
                    "host_incarnation": lease.incarnation,
                }.items()):
                    raise ForestError(
                        "IDEMPOTENCY_CONFLICT", "Module activation differs", 409
                    )
                return
            await conn.execute(
                "INSERT INTO public.bot_module_state_activations("
                "bot_id,operation_id,component_id,revision,enabled,host_incarnation) "
                "VALUES (%s,%s,%s,%s,%s,%s)",
                (
                    self.store.bot_id, operation_id, component_id, revision,
                    enabled, lease.incarnation,
                ),
            )
            await self.store._host(conn, lease)

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
