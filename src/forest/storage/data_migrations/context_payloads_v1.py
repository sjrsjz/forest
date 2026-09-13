"""Frozen 030 data conversion: preserve ordering and identity, move only bodies.

Old derived rows do not prove complete provenance. Their explicit conservative
source makes that limitation persistent and prevents later derivations escaping
the same deletion scope. This is not a content-based lineage guess.
"""

from forest.memory_types import SourceRef
from forest.storage.migration_v1 import _digest, legacy_source_ref
from forest.storage.payloads import PayloadStore


async def migrate_in(conn, store):
    payloads = PayloadStore(store)
    legacy = legacy_source_ref(store.bot_id)
    after = ""
    while True:
        rows = await (await conn.execute(
            "SELECT * FROM public.bot_conversation_replies WHERE bot_id=%s AND reply_id>%s "
            "ORDER BY reply_id LIMIT 64", (store.bot_id, after),
        )).fetchall()
        if not rows:
            break
        for row in rows:
            own = SourceRef("source-unit:" + _digest([
                store.bot_id, "source-reply:" + row["reply_id"],
            ]))
            ref = await payloads.import_legacy_json_in(
                conn, owner_kind="reply.parts", owner_id=row["reply_id"], value=row["parts"],
                sources=(own, legacy),
            )
            await conn.execute(
                "UPDATE public.bot_conversation_replies SET payload_id=%s,parts=NULL "
                "WHERE bot_id=%s AND reply_id=%s", (ref.payload_id, store.bot_id, row["reply_id"]),
            )
        after = rows[-1]["reply_id"]
    after = ""
    while True:
        rows = await (await conn.execute(
            "SELECT * FROM public.bot_context_units WHERE bot_id=%s AND unit_id>%s "
            "ORDER BY unit_id LIMIT 64", (store.bot_id, after),
        )).fetchall()
        if not rows:
            break
        for row in rows:
            if row["kind"] == "INPUT":
                continue
            if row["kind"] == "REPLY":
                reply = await (await conn.execute(
                    "SELECT payload_id FROM public.bot_conversation_replies WHERE bot_id=%s AND reply_id=%s",
                    (store.bot_id, row["reply_id"]),
                )).fetchone()
                payload_id = reply["payload_id"]
            else:
                value = row["control_parts"] if row["kind"] == "CONTROL" else row["step_parts"]
                ref = await payloads.import_legacy_json_in(
                    conn, owner_kind="context.parts", owner_id=row["unit_id"], value=value,
                    sources=(legacy,),
                )
                payload_id = ref.payload_id
            await conn.execute(
                "UPDATE public.bot_context_units SET payload_id=%s,control_parts=NULL,step_parts=NULL "
                "WHERE bot_id=%s AND unit_id=%s", (payload_id, store.bot_id, row["unit_id"]),
            )
        after = rows[-1]["unit_id"]
    await conn.execute("""
        ALTER TABLE public.bot_conversation_replies ALTER COLUMN payload_id SET NOT NULL;
        ALTER TABLE public.bot_conversation_replies ADD CONSTRAINT bot_reply_body_external
            CHECK(parts IS NULL);
        ALTER TABLE public.bot_context_units ADD CONSTRAINT bot_context_body_external
            CHECK(control_parts IS NULL AND step_parts IS NULL AND
                  ((kind='INPUT' AND payload_id IS NULL) OR (kind<>'INPUT' AND payload_id IS NOT NULL)));
        CREATE TRIGGER bot_conversation_replies_immutable BEFORE UPDATE OR DELETE ON public.bot_conversation_replies
            FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();
        CREATE TRIGGER bot_context_unit_identity BEFORE UPDATE OR DELETE ON public.bot_context_units
            FOR EACH ROW EXECUTE FUNCTION public.forest_agent_fixed_fields('{visible}');
    """
    )
