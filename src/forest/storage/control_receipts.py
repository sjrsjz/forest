"""Exact management receipt references and bounded plan/revision provenance.

This bridges existing immutable configuration rows to their recorded operations;
it does not migrate configuration values or unrelated event families. Callers
hold actual authority/business locks and pass only Host-selected identities.
"""

from uuid import uuid4

from forest.errors import ForestError
from forest.packages import digest_json
from forest.storage.control_payloads import ControlPayloadStore

PLAN_ACTIONS = frozenset({
    "config.plan", "agent.config.plan", "memory.config.plan", "binding.plan", "job.plan",
    "command.policy.plan", "command.limit.plan", "message.handler.policy.plan",
    "message.handler.limit.plan",
})


class ControlReceiptStore:
    def __init__(self, store):
        self.store = store
        self.content = ControlPayloadStore(store)

    async def _epoch(self, conn):
        return (await self.content.payloads.lock_in(conn))["deletion_epoch"]

    async def operation_in(self, conn, operation_id):
        row = await (await conn.execute(
            "SELECT * FROM public.bot_web_operations WHERE bot_id=%s AND operation_id=%s",
            (self.store.bot_id, operation_id),
        )).fetchone()
        if row is None:
            raise ForestError("CONTROL_SOURCE_MISSING", "Configuration has no recorded source operation", 409)
        return await self.content.hydrate_result_in(conn, row, epoch=await self._epoch(conn))

    async def register_plan_in(self, conn, claims, result, ref):
        if claims["action"] not in PLAN_ACTIONS:
            return
        if not isinstance(result, dict) or not isinstance(result.get("plan_id"), str):
            raise ForestError("CONTROL_PLAN_INVALID", "A plan result must identify its actual plan", 409)
        await conn.execute(
            "INSERT INTO public.bot_control_value_sources(bot_id,family,source_kind,source_id,operation_id,"
            "payload_id,payload_digest,legacy_untracked) VALUES (%s,%s,'PLAN',%s,%s,%s,%s,false)",
            (self.store.bot_id, claims["action"].removesuffix(".plan"), result["plan_id"],
             claims["operation_id"], ref.payload_id, ref.digest),
        )

    async def _bridge_in(self, conn, family, kind, identifier):
        row = await (await conn.execute(
            "SELECT * FROM public.bot_control_value_sources WHERE bot_id=%s AND family=%s "
            "AND source_kind=%s AND source_id=%s", (self.store.bot_id, family, kind, identifier),
        )).fetchone()
        if row is None:
            raise ForestError("CONTROL_SOURCE_MISSING", "Configuration source was not registered", 409)
        epoch = await self._epoch(conn)
        await self.content.require_in(conn, (row["payload_id"],), epoch=epoch)
        if row["legacy_untracked"]:
            value = await self.content.payloads.read_json_in(conn, row["payload_id"], expected_deletion_epoch=epoch)
            if digest_json(value) != row["payload_digest"]:
                raise self.content._conflict()
        else:
            operation = await self.operation_in(conn, row["operation_id"])
            if (operation["result_payload_id"], operation["result_payload_digest"]) != (row["payload_id"], row["payload_digest"]):
                raise self.content._conflict()
            value = operation["result"]
        return row["payload_id"], value

    async def plan_parent_in(self, conn, family, plan):
        if "values_payload_id" in plan:
            ref = await self.plan_values_proof_in(conn, family, plan)
            await self.content.require_in(conn, (ref,), epoch=await self._epoch(conn))
            return ref
        ref, value = await self._bridge_in(conn, family, "PLAN", plan["plan_id"])
        if not isinstance(value, dict) or any(value.get(key) != plan[key] for key in ("plan_id", "plan_digest", "values")):
            raise ForestError("CONTROL_PLAN_SOURCE_CONFLICT", "Plan differs from its immutable source receipt", 409)
        return ref

    async def revision_parent_in(self, conn, family, revision, *, resource=""):
        if revision is None:
            return None
        if "values_payload_id" in revision:
            ref = await self.revision_values_proof_in(conn, family, revision, resource=resource)
            await self.content.require_in(conn, (ref,), epoch=await self._epoch(conn))
            return ref
        row = await (await conn.execute(
            "SELECT operation_id,action FROM public.bot_web_operations WHERE bot_id=%s AND operation_id=%s",
            (self.store.bot_id, revision["operation_id"]),
        )).fetchone()
        if row is not None and row["action"] == family + ".apply":
            operation = await self.operation_in(conn, row["operation_id"])
            if operation["action"] != family + ".apply":
                raise ForestError("CONTROL_SOURCE_CONFLICT", "Revision belongs to another operation kind", 409)
            return operation["result_payload_id"]
        ref, value = await self._bridge_in(
            conn, family, "REVISION", digest_json([resource, revision["revision"]]),
        )
        if not isinstance(value, dict) or value.get("values") != revision["values"]:
            raise ForestError("CONTROL_SOURCE_CONFLICT", "Revision differs from its legacy source", 409)
        return ref

    async def plan_values_proof_in(self, conn, family, plan, *, resource=""):
        from forest.storage.control_value_proofs import plan_values_proof_in

        return await plan_values_proof_in(conn, self.store, family, plan, resource=resource)

    async def revision_values_proof_in(self, conn, family, revision, *, resource=""):
        from forest.storage.control_value_proofs import revision_values_proof_in

        return await revision_values_proof_in(conn, self.store, family, revision, resource=resource)

    async def activation_heads_in(self, conn, component, operation_id):
        """Only business heads, before any configuration/exposure body clock."""
        if operation_id is None:
            return
        await conn.execute(
            "SELECT h.component_id FROM public.bot_config_heads h JOIN public.bot_config_activations a "
            "ON a.bot_id=h.bot_id AND a.component_id=h.component_id "
            "WHERE a.bot_id=%s AND a.operation_id=%s AND a.component_id=%s FOR UPDATE OF h",
            (self.store.bot_id, operation_id, component),
        )
        await conn.execute(
            "SELECT h.watch_id FROM public.bot_dev_watch_heads h JOIN public.bot_dev_watch_activations a "
            "ON a.bot_id=h.bot_id AND a.watch_id=h.watch_id "
            "WHERE a.bot_id=%s AND a.operation_id=%s AND h.component_id=%s FOR UPDATE OF h",
            (self.store.bot_id, operation_id, component),
        )

    async def generation_parents_in(
        self,
        conn,
        component,
        operation_id,
        *,
        config,
        package_digest,
        grants,
        message_handler_takeover=False,
    ):
        if operation_id is None:
            return ()
        await self.activation_heads_in(conn, component, operation_id)
        row = await (await conn.execute(
            "SELECT r.* FROM public.bot_config_activations a JOIN public.bot_config_revisions r "
            "ON r.bot_id=a.bot_id AND r.component_id=a.component_id AND r.revision=a.revision "
            "WHERE a.bot_id=%s AND a.operation_id=%s AND a.component_id=%s",
            (self.store.bot_id, operation_id, component),
        )).fetchone()
        if row is None:
            from forest.storage.watch import WatchStore
            watch = await (await conn.execute(
                "SELECT h.*,b.package_digest AS activated_package FROM public.bot_dev_watch_activations a "
                "JOIN public.bot_dev_watch_heads h ON h.bot_id=a.bot_id AND h.watch_id=a.watch_id "
                "JOIN public.bot_dev_watch_builds b ON b.bot_id=a.bot_id AND b.request_id=a.request_id "
                "WHERE a.bot_id=%s AND a.operation_id=%s AND h.component_id=%s AND b.component_id=%s",
                (self.store.bot_id, operation_id, component, component),
            )).fetchone()
            if watch is None:
                return ()
            watch = await WatchStore(self.store).hydrate_in(conn, watch)
            if (watch["config"] != config or watch["activated_package"] != package_digest
                    or sorted(watch["grants"]) != sorted(grants)
                    or message_handler_takeover):
                raise ForestError("WATCH_SUPERSEDED", "Generation differs from its actual watch activation", 409)
            return (watch["spec_payload_id"],)
        from forest.storage.control import BotControl
        row = await BotControl(self.store).config_values_in(conn, row)
        values = row["values"]
        if (values["config"] != config or values["package_digest"] != package_digest
                or sorted(values["grants"]) != sorted(grants)
                or values.get("message_handler_takeover", False)
                is not message_handler_takeover):
            raise ForestError("CONFIGURATION_SUPERSEDED", "Generation differs from its actual activation", 409)
        ref = await self.revision_parent_in(conn, "config", row, resource=component)
        return (ref,)

    async def record_events_in(self, conn, topic, operation_id, subject_id, result):
        await self.content.require_in(conn, (result.payload_id,), epoch=await self._epoch(conn))
        for table, identity, label, body in (
            ("bot_audit_records", "audit_id", "action", "details"),
            ("bot_outbox_events", "event_id", "topic", "payload"),
        ):
            # Every interpolated SQL identifier is a fixed Host constant above.
            await conn.execute(
                f"INSERT INTO public.{table}(bot_id,{identity},{label},operation_id,{body},"
                "control_result_payload_id,control_result_payload_digest,control_subject_id) "
                "VALUES (%s,%s,%s,%s,NULL,%s,%s,%s)",
                (self.store.bot_id, str(uuid4()), topic, operation_id, result.payload_id, result.digest, subject_id),
            )

    async def hydrate_event_in(self, conn, row, *, kind, required=True):
        if kind not in {"audit", "outbox"}:
            raise ValueError("Invalid control event kind")
        body, label = ("details", "action") if kind == "audit" else ("payload", "topic")
        if row.get("control_result_payload_id") is None:
            return {**row, "control_payload_state": "OUTSIDE_FAMILY"}
        operation = await (await conn.execute(
            "SELECT * FROM public.bot_web_operations WHERE bot_id=%s AND operation_id=%s",
            (self.store.bot_id, row["operation_id"]),
        )).fetchone()
        if operation is None or (operation["action"], operation["subject_id"], operation["result_payload_id"], operation["result_payload_digest"]) != (
            row[label], row["control_subject_id"], row["control_result_payload_id"], row["control_result_payload_digest"],
        ):
            raise self.content._conflict()
        operation = await self.content.hydrate_result_in(conn, operation, epoch=await self._epoch(conn), required=required)
        value = operation["result"]
        if operation["result_payload_state"] == "ACTIVE":
            value = {"subject_id": row["control_subject_id"], **value} if isinstance(value, dict) else {"subject_id": row["control_subject_id"], "result": value}
        return {**row, body: value, "control_payload_state": operation["result_payload_state"]}
