"""Frozen 049: a missing historical template revision stays explicitly unknown.

Only an actual Kernel input with direct canonical Context parents can establish
the Binding scope. Neither a current revision nor matching template/group text
is provenance. An absent body proves no service method; it can still retain an
unknown prerequisite when its immutable input identity and Context scope agree.
"""

import hashlib
import json

from forest.storage.data_migrations.config_dependencies_v2 import import_dependency_in
from forest.storage.migration_v1 import (
    _digest,
    import_payload_in,
    legacy_source_ref,
    require_migration_scope,
)


async def _proof_in(conn, store, call):
    header = await (
        await conn.execute(
            "SELECT h.*,b.content FROM public.bot_payload_headers h "
            "LEFT JOIN public.bot_payload_bodies b USING(bot_id,payload_id) "
            "WHERE h.bot_id=%s AND h.payload_id=%s",
            (store.bot_id, call["input_payload_id"]),
        )
    ).fetchone()
    owner = "input:" + _digest([call["call_id"]])
    if (
        header is None
        or header["owner_kind"] != "kernel.input"
        or header["owner_id"] != owner
        or header["encoding"] != "json"
        or header["payload_id"]
        != "payload:"
        + _digest(
            [
                store.bot_id,
                "kernel.input",
                owner,
            ]
        )
    ):
        raise ValueError("Legacy template Call input has inconsistent payload identity")
    parents = [
        row["parent_payload_id"]
        for row in await (
            await conn.execute(
                "SELECT parent_payload_id FROM public.bot_payload_derivations "
                "WHERE bot_id=%s AND payload_id=%s ORDER BY parent_payload_id",
                (store.bot_id, header["payload_id"]),
            )
        ).fetchall()
    ]
    if parents != header["manifest"]["parents"]:
        raise ValueError("Legacy template Call input has inconsistent parent facts")
    if not parents:
        return None
    # New 047 admissions already use the actual effective V. A real direct
    # revision parent is evidence; merely finding that revision in a head is not.
    if await (
        await conn.execute(
            "SELECT 1 FROM public.bot_binding_revisions WHERE bot_id=%s "
            "AND values_payload_id=ANY(%s) LIMIT 1",
            (store.bot_id, parents),
        )
    ).fetchone():
        return None
    contexts = await (
        await conn.execute(
            "SELECT DISTINCT c.binding_id,c.conversation_id FROM public.bot_context_units u "
            "JOIN public.bot_conversations c ON c.bot_id=u.bot_id AND c.conversation_id=u.conversation_id "
            "JOIN public.bot_bindings b ON b.bot_id=c.bot_id AND b.binding_id=c.binding_id "
            "LEFT JOIN public.bot_message_inbox i ON i.bot_id=u.bot_id AND i.event_id=u.event_id "
            "WHERE u.bot_id=%s AND (CASE WHEN u.kind='INPUT' THEN i.payload_id ELSE u.payload_id END)=ANY(%s) "
            "ORDER BY c.binding_id,c.conversation_id LIMIT 257",
            (store.bot_id, parents),
        )
    ).fetchall()
    if not contexts or len(contexts) > 256 or len({r["binding_id"] for r in contexts}) != 1:
        return None
    proof = "CONTEXT_BOUND_INPUT_BODY_UNAVAILABLE"
    if header["content"] is not None:
        content = bytes(header["content"])
        if "sha256:" + hashlib.sha256(content).hexdigest() != header["digest"]:
            raise ValueError("Legacy template Call input body differs from its immutable digest")
        value = json.loads(content)
        if (
            not isinstance(value, dict)
            or set(value) != {"template", "text", "sender", "group"}
            or not all(isinstance(item, str) for item in value.values())
        ):
            return None
        identity = {
            "component": "forest.reply",
            "service": "reply.respond",
            "contract_major": 1,
            "payload": value,
            "input_parent_ids": parents,
        }
        if _digest(identity) != call["input_digest"]:
            return None
        proof = "EXACT_TEMPLATE_CALL_WITH_CONTEXT"
    return {
        "call_id": call["call_id"],
        "binding_id": contexts[0]["binding_id"],
        "conversation_ids": [r["conversation_id"] for r in contexts],
        "proof": proof,
    }


async def migrate_in(conn, store):
    require_migration_scope(conn, store)
    after = ""
    while True:
        calls = await (
            await conn.execute(
                "SELECT call_id,input_payload_id,input_digest FROM public.bot_calls WHERE bot_id=%s "
                "AND component_id='forest.reply' AND parent_call_id IS NULL AND admission_lane='DATA' "
                "AND agent_tool_id IS NULL AND NOT input_legacy_unknown AND input_payload_id IS NOT NULL "
                "AND call_id>%s ORDER BY call_id LIMIT 64",
                (store.bot_id, after),
            )
        ).fetchall()
        if not calls:
            return
        for call in calls:
            proof = await _proof_in(conn, store, call)
            if proof is None:
                continue
            anchor = await import_payload_in(
                conn,
                store,
                owner_kind="control.config",
                owner_id="legacy-binding-template:" + _digest([call["call_id"]]),
                value=proof,
                encoding="json",
                sources=(legacy_source_ref(store.bot_id),),
                parents=(),
            )
            await import_dependency_in(
                conn,
                store,
                call["input_payload_id"],
                anchor.payload_id,
                proof_kind="binding.unknown",
                proof_id=call["call_id"],
            )
        after = calls[-1]["call_id"]
