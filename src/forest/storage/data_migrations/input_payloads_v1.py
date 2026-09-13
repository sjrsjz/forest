"""Frozen 029 import: original input and image content, in the schema transaction."""

from psycopg.types.json import Jsonb

from forest.memory_types import SourceRef
from forest.storage.migration_v1 import _digest
from forest.storage.payloads import PayloadStore

IDENTITY_FIELDS = frozenset(
    {
        "external_user_id",
        "principal_ref",
        "identity_origin",
        "is_self",
        "job_id",
        "occurrence_id",
        "delegation_id",
    }
)


def identity(author):
    return {key: value for key, value in author.items() if key in IDENTITY_FIELDS}


def source_ref(bot_id, source_event_id, part_id="*"):
    return SourceRef("source-unit:" + _digest([bot_id, source_event_id]), 1, part_id)


async def register_source(conn, store, inbox, conversation, body):
    """Frozen 022 source admission, using the original body and immutable batches."""
    system = inbox["origin"] == "job"
    source_id = (
        "source-system:" + _digest([store.bot_id, conversation["conversation_id"]])
        if system
        else conversation["group_source_id"]
    )
    kind = "SYSTEM" if system else "GROUP" if conversation["channel_kind"] == "group" else "DIRECT"
    await conn.execute(
        "INSERT INTO public.bot_memory_sources(bot_id,source_id,source_kind,visibility,platform,realm_id,channel_kind,channel_id,target_group_source_id,owner_conversation_id) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
        (
            store.bot_id,
            source_id,
            kind,
            "bot_shared" if kind == "GROUP" else "source_restricted",
            "forest" if system else conversation["platform"],
            "forest-internal" if system else "qq-global",
            "system" if system else conversation["channel_kind"],
            conversation["conversation_id"] if system else conversation["channel_id"],
            conversation["group_source_id"],
            conversation["conversation_id"] if system else None,
        ),
    )
    source = await (
        await conn.execute(
            "SELECT * FROM public.bot_memory_sources WHERE bot_id=%s AND source_id=%s FOR UPDATE",
            (store.bot_id, source_id),
        )
    ).fetchone()
    unit_id = source_ref(store.bot_id, inbox["source_event_id"]).source_unit_id
    parts, media = [], []
    for index, part in enumerate(body["segments"]):
        part_id = str(index)
        if part["type"] == "image":
            parts.append(
                {
                    "type": "image",
                    "part_id": part_id,
                    "media_ref": "source-media:" + _digest([unit_id, part_id]),
                }
            )
            media.append((part_id, part["media_ref"]))
        else:
            parts.append({**part, "part_id": part_id})
    value = {"schema": "forest-source-v1", "author": body["author"], "parts": parts}
    unit = await (
        await conn.execute(
            "SELECT * FROM public.bot_memory_source_units WHERE bot_id=%s AND source_unit_id=%s",
            (store.bot_id, unit_id),
        )
    ).fetchone()
    changed = False
    if unit is None:
        payload = await PayloadStore(store).import_legacy_json_in(
            conn,
            owner_kind="memory.source",
            owner_id=unit_id,
            value=value,
            sources=(SourceRef(unit_id),),
        )
        await conn.execute(
            "INSERT INTO public.bot_memory_source_units(bot_id,source_unit_id,source_id,source_event_id,source_order_seq,order_origin,kind,principal_ref,identity_origin,payload_id,content_digest,occurred_at,lineage_complete) VALUES (%s,%s,%s,%s,%s,'RECONSTRUCTED','INPUT',%s,%s,%s,%s,%s,true)",
            (
                store.bot_id,
                unit_id,
                source_id,
                inbox["source_event_id"],
                source["next_order_seq"],
                body["author"]["principal_ref"],
                body["author"]["identity_origin"],
                payload.payload_id,
                _digest(value),
                inbox["sent_at"],
            ),
        )
        await conn.execute(
            "UPDATE public.bot_memory_sources SET next_order_seq=next_order_seq+1,has_reconstructed_order=true WHERE bot_id=%s AND source_id=%s",
            (store.bot_id, source_id),
        )
        changed = True
    elif (
        unit["source_id"] != source_id
        or unit["content_digest"] != _digest(value)
        or unit["kind"] != "INPUT"
    ):
        raise ValueError("Legacy source identity conflicts with original Inbox")
    occurrence_id = "source-occurrence:" + _digest([store.bot_id, "INPUT", inbox["event_id"]])
    added = await conn.execute(
        "INSERT INTO public.bot_memory_source_occurrences(bot_id,occurrence_id,source_unit_id,conversation_id,binding_id,thread_id,origin,event_id,ingress_seq) VALUES (%s,%s,%s,%s,%s,%s,'INPUT',%s,%s) ON CONFLICT DO NOTHING",
        (
            store.bot_id,
            occurrence_id,
            unit_id,
            conversation["conversation_id"],
            conversation["binding_id"],
            conversation["thread_id"],
            inbox["event_id"],
            inbox["ingress_seq"],
        ),
    )
    changed |= bool(added.rowcount)
    for part_id, media_ref in media:
        await conn.execute(
            "INSERT INTO public.bot_memory_source_media(bot_id,source_unit_id,part_id,occurrence_id,media_ref) VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
            (store.bot_id, unit_id, part_id, occurrence_id, media_ref),
        )
    eligible = await conn.execute(
        "INSERT INTO public.bot_memory_source_eligibility(bot_id,occurrence_id,input_batch_id,context_revision) SELECT %s,%s,u.input_batch_id,u.context_revision FROM public.bot_context_units u JOIN public.bot_context_input_batches b USING(bot_id,input_batch_id) WHERE u.bot_id=%s AND u.event_id=%s AND b.conversation_id=%s AND b.selected_input_end_seq>=%s ON CONFLICT DO NOTHING",
        (
            store.bot_id,
            occurrence_id,
            store.bot_id,
            inbox["event_id"],
            conversation["conversation_id"],
            inbox["ingress_seq"],
        ),
    )
    if changed or eligible.rowcount:
        await conn.execute(
            "UPDATE public.bot_memory_clock SET fact_revision=fact_revision+1 WHERE bot_id=%s",
            (store.bot_id,),
        )


FINAL_SQL = """
ALTER TABLE public.bot_message_inbox ALTER COLUMN payload_id SET NOT NULL,
 ADD CHECK(segments IS NULL),
 ADD CHECK(author - ARRAY['external_user_id','principal_ref','identity_origin','is_self','job_id','occurrence_id','delegation_id'] = '{}'::jsonb);
ALTER TABLE public.bot_inbound_media_refs ALTER COLUMN source_payload_id SET NOT NULL,
 ADD CHECK(source IS NULL);
ALTER TABLE public.bot_media_captures ALTER COLUMN payload_id SET NOT NULL,
 ALTER COLUMN manifest_payload_id SET NOT NULL,
 ADD CHECK(content IS NULL AND source_manifest IS NULL);
CREATE OR REPLACE FUNCTION public.forest_inbox_source_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='DELETE' OR ROW(NEW.bot_id,NEW.conversation_id,NEW.event_id,NEW.ingress_seq,
  NEW.platform_message_id,NEW.source_event_id,NEW.input_digest,NEW.author,NEW.payload_id,NEW.sent_at,NEW.received_at)
  IS DISTINCT FROM ROW(OLD.bot_id,OLD.conversation_id,OLD.event_id,OLD.ingress_seq,
  OLD.platform_message_id,OLD.source_event_id,OLD.input_digest,OLD.author,OLD.payload_id,OLD.sent_at,OLD.received_at) THEN
  RAISE EXCEPTION 'Input identity and payload reference are immutable' USING ERRCODE='55000';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER bot_message_source_immutable BEFORE UPDATE OR DELETE ON public.bot_message_inbox
 FOR EACH ROW EXECUTE FUNCTION public.forest_inbox_source_immutable();
CREATE OR REPLACE FUNCTION public.forest_media_source_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='DELETE' OR ROW(NEW.bot_id,NEW.media_ref,NEW.event_id,NEW.part_index,NEW.source_payload_id)
  IS DISTINCT FROM ROW(OLD.bot_id,OLD.media_ref,OLD.event_id,OLD.part_index,OLD.source_payload_id) THEN
  RAISE EXCEPTION 'Image source identity and payload reference are immutable' USING ERRCODE='55000';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER bot_media_source_immutable BEFORE UPDATE OR DELETE ON public.bot_inbound_media_refs
 FOR EACH ROW EXECUTE FUNCTION public.forest_media_source_immutable();
CREATE TRIGGER bot_media_captures_immutable BEFORE UPDATE OR DELETE ON public.bot_media_captures
 FOR EACH ROW EXECUTE FUNCTION public.forest_bot_reject_mutation();
"""


async def migrate_in(conn, store):
    payloads = PayloadStore(store)
    cursor = ""
    while True:
        rows = await (
            await conn.execute(
                "SELECT * FROM public.bot_message_inbox WHERE bot_id=%s AND event_id>%s ORDER BY event_id LIMIT 64",
                (store.bot_id, cursor),
            )
        ).fetchall()
        if not rows:
            break
        for row in rows:
            body = {"author": row["author"], "segments": row["segments"]}
            ref = await payloads.import_legacy_json_in(
                conn,
                owner_kind="context.parts",
                owner_id="input:" + row["event_id"],
                value=body,
                sources=(source_ref(store.bot_id, row["source_event_id"]),),
            )
            conversation = await (
                await conn.execute(
                    "SELECT * FROM public.bot_conversations WHERE bot_id=%s AND conversation_id=%s",
                    (store.bot_id, row["conversation_id"]),
                )
            ).fetchone()
            # Source refs exist independently of their payload, including erased
            # imports. Actual immutable input batches remain the eligibility proof.
            await register_source(conn, store, row, conversation, body)
            await conn.execute(
                "UPDATE public.bot_message_inbox SET author=%s,segments=NULL,payload_id=%s WHERE bot_id=%s AND event_id=%s",
                (Jsonb(identity(row["author"])), ref.payload_id, store.bot_id, row["event_id"]),
            )
        cursor = rows[-1]["event_id"]
    cursor = ""
    while True:
        rows = await (
            await conn.execute(
                "SELECT m.*,i.source_event_id FROM public.bot_inbound_media_refs m JOIN public.bot_message_inbox i USING(bot_id,event_id) WHERE m.bot_id=%s AND m.media_ref>%s ORDER BY m.media_ref LIMIT 64",
                (store.bot_id, cursor),
            )
        ).fetchall()
        if not rows:
            break
        for row in rows:
            ref = await payloads.import_legacy_json_in(
                conn,
                owner_kind="media.source",
                owner_id=row["media_ref"],
                value=row["source"],
                sources=(source_ref(store.bot_id, row["source_event_id"], str(row["part_index"])),),
            )
            await conn.execute(
                "UPDATE public.bot_inbound_media_refs SET source=NULL,source_payload_id=%s WHERE bot_id=%s AND media_ref=%s",
                (ref.payload_id, store.bot_id, row["media_ref"]),
            )
        cursor = rows[-1]["media_ref"]
    cursor = ""
    while True:
        rows = await (
            await conn.execute(
                "SELECT m.*,i.source_event_id FROM public.bot_media_captures m JOIN public.bot_message_inbox i USING(bot_id,event_id) WHERE m.bot_id=%s AND m.media_ref>%s ORDER BY m.media_ref LIMIT 16",
                (store.bot_id, cursor),
            )
        ).fetchall()
        if not rows:
            break
        for row in rows:
            refs = (source_ref(store.bot_id, row["source_event_id"], str(row["part_index"])),)
            body = await payloads.import_legacy_bytes_in(
                conn,
                owner_kind="media.capture",
                owner_id="input-capture:" + row["media_ref"],
                value=bytes(row["content"]),
                sources=refs,
            )
            manifest = await payloads.import_legacy_json_in(
                conn,
                owner_kind="media.source",
                owner_id="capture-manifest:" + row["media_ref"],
                value=row["source_manifest"],
                sources=refs,
            )
            await conn.execute(
                "UPDATE public.bot_media_captures SET content=NULL,source_manifest=NULL,payload_id=%s,manifest_payload_id=%s WHERE bot_id=%s AND media_ref=%s",
                (body.payload_id, manifest.payload_id, store.bot_id, row["media_ref"]),
            )
        cursor = rows[-1]["media_ref"]
    await conn.execute(FINAL_SQL)
