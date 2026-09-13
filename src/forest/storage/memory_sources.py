"""Bot-wide native source registration and conversation-cut eligibility.

This ledger does not run embedding or an LLM. PayloadStore owns new content;
legacy Inbox/Agent copies remain until the separate payload migration lands.
"""

from __future__ import annotations

from forest.errors import ForestError
from forest.memory_types import SourceRef
from forest.packages import digest_json
from forest.storage.payloads import PayloadStore


class MemorySourceStore:
    def __init__(self, messages):
        self.messages, self.store = messages, messages.store
        self.payloads = PayloadStore(self.store)

    def unit_id(self, source_event_id):
        return "source-unit:" + digest_json([self.store.bot_id, source_event_id])

    @staticmethod
    def reply_event_id(reply_id):
        return "source-reply:" + reply_id

    def _source(self, conversation, *, system=False):
        kind = (
            "SYSTEM" if system else "GROUP" if conversation["channel_kind"] == "group" else "DIRECT"
        )
        return {
            "source_id": (
                "source-system:" + digest_json([self.store.bot_id, conversation["conversation_id"]])
                if system
                else conversation["group_source_id"]
            ),
            "source_kind": kind,
            "visibility": "bot_shared" if kind == "GROUP" else "source_restricted",
            "platform": "forest" if system else conversation["platform"],
            "realm_id": "forest-internal" if system else "qq-global",
            "channel_kind": "system" if system else conversation["channel_kind"],
            "channel_id": conversation["conversation_id"] if system else conversation["channel_id"],
            "target_group_source_id": conversation["group_source_id"],
            "owner_conversation_id": conversation["conversation_id"] if system else None,
        }

    async def _source_in(self, conn, source):
        await conn.execute(
            "INSERT INTO public.bot_memory_sources(bot_id,source_id,source_kind,visibility,platform,realm_id,channel_kind,channel_id,target_group_source_id,owner_conversation_id) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
            (
                self.store.bot_id,
                *(
                    source[key]
                    for key in (
                        "source_id",
                        "source_kind",
                        "visibility",
                        "platform",
                        "realm_id",
                        "channel_kind",
                        "channel_id",
                        "target_group_source_id",
                        "owner_conversation_id",
                    )
                ),
            ),
        )
        row = await (
            await conn.execute(
                "SELECT * FROM public.bot_memory_sources WHERE bot_id=%s AND source_id=%s FOR UPDATE",
                (self.store.bot_id, source["source_id"]),
            )
        ).fetchone()
        if any(row[key] != value for key, value in source.items()):
            raise ForestError("MEMORY_SOURCE_CONFLICT", "Source identity was reused", 409)
        return row

    async def _advance_in(self, conn):
        return await (
            await conn.execute(
                "UPDATE public.bot_memory_clock SET fact_revision=fact_revision+1 WHERE bot_id=%s RETURNING *",
                (self.store.bot_id,),
            )
        ).fetchone()

    @staticmethod
    def _parts(source_unit_id, parts, *, reply=False):
        canonical, media, private = [], [], {}
        for index, part in enumerate(parts):
            part_id = str(index)
            if part["type"] == "image":
                canonical.append(
                    {
                        "type": "image",
                        "part_id": part_id,
                        "media_ref": "source-media:" + digest_json([source_unit_id, part_id]),
                    }
                )
                if not reply:
                    media.append((part_id, part["media_ref"]))
                else:
                    # Explicit outbound image sources stay typed/private. The
                    # retrieval DTO never exposes transport URIs as text.
                    private[part_id] = part
            else:
                canonical.append({**part, "part_id": part_id})
        return canonical, media, private

    async def _register_in(
        self,
        conn,
        conversation,
        *,
        source_event_id,
        author,
        parts,
        occurred_at,
        kind,
        event_id=None,
        reply_id=None,
        ingress_seq,
        system=False,
        reconstructed=False,
        dependencies=(),
        lineage_complete=True,
    ):
        clock = await self.payloads.lock_in(conn)
        source = await self._source_in(conn, self._source(conversation, system=system))
        unit_id = self.unit_id(source_event_id)
        canonical, media, private = self._parts(unit_id, parts, reply=kind == "REPLY")
        value = {"schema": "forest-source-v1", "author": author, "parts": canonical}
        if private:
            value["private_media_sources"] = private
        digest = digest_json(value)
        existing = await (
            await conn.execute(
                "SELECT * FROM public.bot_memory_source_units WHERE bot_id=%s AND source_unit_id=%s",
                (self.store.bot_id, unit_id),
            )
        ).fetchone()
        if existing:
            if (
                existing["source_id"] != source["source_id"]
                or existing["content_digest"] != digest
                or existing["kind"] != kind
            ):
                raise ForestError("MEMORY_SOURCE_CONFLICT", "Canonical source content differs", 409)
            # A repeated source identity survives suppression; retrieval still
            # requires a live body. This path never recreates scrubbed content.
            unit = existing
        else:
            inherited = await (
                await conn.execute(
                    "SELECT DISTINCT p.source_unit_id,p.source_revision,p.part_id "
                    "FROM public.bot_memory_source_units d JOIN public.bot_payload_sources p USING(bot_id,payload_id) "
                    "WHERE d.bot_id=%s AND d.source_unit_id=ANY(%s) LIMIT 4097",
                    (self.store.bot_id, [ref.source_unit_id for ref in dependencies]),
                )
            ).fetchall()
            source_refs = {SourceRef(unit_id), *dependencies}
            source_refs.update(SourceRef(**row) for row in inherited)
            payload = await self.payloads.put_json_in(
                conn,
                owner_kind="memory.source",
                owner_id=unit_id,
                value=value,
                sources=tuple(sorted(source_refs)),
                expected_deletion_epoch=clock["deletion_epoch"],
            )
            unit = await (
                await conn.execute(
                    "INSERT INTO public.bot_memory_source_units(bot_id,source_unit_id,source_id,source_event_id,source_order_seq,order_origin,kind,principal_ref,identity_origin,payload_id,content_digest,occurred_at,lineage_complete) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *",
                    (
                        self.store.bot_id,
                        unit_id,
                        source["source_id"],
                        source_event_id,
                        source["next_order_seq"],
                        "RECONSTRUCTED" if reconstructed else "LIVE",
                        kind,
                        author["principal_ref"],
                        author["identity_origin"],
                        payload.payload_id,
                        digest,
                        occurred_at,
                        lineage_complete,
                    ),
                )
            ).fetchone()
            await conn.execute(
                "UPDATE public.bot_memory_sources SET next_order_seq=next_order_seq+1,"
                "has_reconstructed_order=has_reconstructed_order OR %s WHERE bot_id=%s AND source_id=%s",
                (reconstructed, self.store.bot_id, source["source_id"]),
            )
        occurrence_id = "source-occurrence:" + digest_json(
            [self.store.bot_id, kind, event_id or reply_id]
        )
        occurrence = await (
            await conn.execute(
                "INSERT INTO public.bot_memory_source_occurrences(bot_id,occurrence_id,source_unit_id,conversation_id,binding_id,thread_id,origin,event_id,reply_id,ingress_seq) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING RETURNING occurrence_id",
                (
                    self.store.bot_id,
                    occurrence_id,
                    unit_id,
                    conversation["conversation_id"],
                    conversation["binding_id"],
                    conversation["thread_id"],
                    kind,
                    event_id,
                    reply_id,
                    ingress_seq,
                ),
            )
        ).fetchone()
        for part_id, media_ref in media:
            await conn.execute(
                "INSERT INTO public.bot_memory_source_media(bot_id,source_unit_id,part_id,occurrence_id,media_ref) "
                "VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                (self.store.bot_id, unit_id, part_id, occurrence_id, media_ref),
            )
        if existing is None or occurrence is not None:
            clock = await self._advance_in(conn)
        return {
            **unit,
            "occurrence_id": occurrence_id,
            "deletion_epoch": clock["deletion_epoch"],
            "fact_revision": clock["fact_revision"],
        }

    async def register_input_in(self, conn, conversation, inbox, *, reconstructed=False):
        if (
            inbox["conversation_id"] != conversation["conversation_id"]
            or inbox["bot_id"] != self.store.bot_id
        ):
            raise ForestError("MEMORY_SOURCE_SCOPE", "Inbox belongs to another source", 403)
        result = await self._register_in(
            conn,
            conversation,
            source_event_id=inbox["source_event_id"],
            author=inbox["author"],
            parts=inbox["segments"],
            occurred_at=inbox["sent_at"],
            kind="INPUT",
            event_id=inbox["event_id"],
            ingress_seq=inbox["ingress_seq"],
            system=inbox["origin"] == "job",
            reconstructed=reconstructed,
        )
        # Existing/legacy input may already have been drained before its source
        # was registered. Only the actual immutable batch proves eligibility.
        eligible = await (
            await conn.execute(
                "INSERT INTO public.bot_memory_source_eligibility(bot_id,occurrence_id,input_batch_id,context_revision) "
                "SELECT %s,%s,u.input_batch_id,u.context_revision FROM public.bot_context_units u "
                "JOIN public.bot_context_input_batches b USING(bot_id,input_batch_id) "
                "WHERE u.bot_id=%s AND u.event_id=%s AND b.conversation_id=%s AND b.selected_input_end_seq>=%s "
                "ON CONFLICT DO NOTHING RETURNING occurrence_id",
                (
                    self.store.bot_id,
                    result["occurrence_id"],
                    self.store.bot_id,
                    inbox["event_id"],
                    conversation["conversation_id"],
                    inbox["ingress_seq"],
                ),
            )
        ).fetchone()
        if eligible:
            clock = await self._advance_in(conn)
            result["fact_revision"] = clock["fact_revision"]
        return result

    async def accept_batch_in(self, conn, conversation, batch):
        """Grant only registered observations actually inside this committed batch."""
        await self.payloads.lock_in(conn)
        if batch["conversation_id"] != conversation["conversation_id"]:
            raise ForestError(
                "MEMORY_SOURCE_SCOPE", "Input batch belongs to another conversation", 403
            )
        rows = await (
            await conn.execute(
                "INSERT INTO public.bot_memory_source_eligibility(bot_id,occurrence_id,input_batch_id,context_revision) "
                "SELECT o.bot_id,o.occurrence_id,%s,%s FROM public.bot_memory_source_occurrences o "
                "JOIN public.bot_context_units u ON u.bot_id=o.bot_id AND u.event_id=o.event_id "
                "WHERE o.bot_id=%s AND o.conversation_id=%s AND u.input_batch_id=%s "
                "AND o.ingress_seq<=%s ON CONFLICT DO NOTHING RETURNING occurrence_id",
                (
                    batch["input_batch_id"],
                    batch["context_revision"],
                    self.store.bot_id,
                    conversation["conversation_id"],
                    batch["input_batch_id"],
                    batch["selected_input_end_seq"],
                ),
            )
        ).fetchall()
        if rows:
            await self._advance_in(conn)
        return len(rows)

    async def register_reply_in(self, conn, conversation, reply, *, reconstructed=False):
        if (
            reply["conversation_id"] != conversation["conversation_id"]
            or reply["bot_id"] != self.store.bot_id
        ):
            raise ForestError("MEMORY_SOURCE_SCOPE", "Reply belongs to another conversation", 403)
        existing = await (
            await conn.execute(
                "SELECT u.*,o.occurrence_id FROM public.bot_memory_source_units u "
                "JOIN public.bot_memory_source_occurrences o USING(bot_id,source_unit_id) WHERE o.bot_id=%s AND o.reply_id=%s",
                (self.store.bot_id, reply["reply_id"]),
            )
        ).fetchone()
        if existing:
            return existing
        binding = await (
            await conn.execute(
                "SELECT self_id,display_name FROM public.bot_bindings WHERE bot_id=%s AND binding_id=%s",
                (self.store.bot_id, conversation["binding_id"]),
            )
        ).fetchone()
        run = await (
            await conn.execute(
                "SELECT selected_input_end_seq FROM public.bot_conversation_runs WHERE bot_id=%s AND run_id=%s",
                (self.store.bot_id, reply["run_id"]),
            )
        ).fetchone()
        context = await (
            await conn.execute(
                "SELECT u.context_revision FROM public.bot_context_units u WHERE u.bot_id=%s AND u.reply_id=%s",
                (self.store.bot_id, reply["reply_id"]),
            )
        ).fetchone()
        # Context bodies are never copied into metadata. For legacy executions,
        # this is a conservative dependency set for their visible input view.
        inputs = await (
            await conn.execute(
                "SELECT i.source_event_id,u.reply_id FROM public.bot_context_units u "
                "LEFT JOIN public.bot_message_inbox i ON i.bot_id=u.bot_id AND i.event_id=u.event_id "
                "WHERE u.bot_id=%s AND u.conversation_id=%s AND u.kind IN ('INPUT','REPLY') "
                "AND u.reply_id IS DISTINCT FROM %s AND u.context_revision<%s "
                "AND (u.visible OR %s) ORDER BY u.context_revision,u.unit_index LIMIT 2049",
                (
                    self.store.bot_id,
                    conversation["conversation_id"],
                    reply["reply_id"],
                    context["context_revision"],
                    reconstructed,
                ),
            )
        ).fetchall()
        # A reconstructed reply can cover an arbitrarily long legacy history.
        # Its original wire/source manifests require the separate full migration;
        # never claim complete deletion lineage from a truncated source scan.
        if len(inputs) > 2048:
            inputs = inputs[:2048]
        dependencies = tuple(
            SourceRef(self.unit_id(row["source_event_id"] or self.reply_event_id(row["reply_id"])))
            for row in inputs
        )
        author = {
            "external_user_id": binding["self_id"],
            "display_name": binding["display_name"],
            "principal_ref": "principal:"
            + digest_json([self.store.bot_id, conversation["platform"], binding["self_id"]]),
            "identity_origin": "host_output",
            "is_self": True,
        }
        result = await self._register_in(
            conn,
            conversation,
            source_event_id=self.reply_event_id(reply["reply_id"]),
            author=author,
            parts=[part for group in reply["parts"] for part in group],
            occurred_at=reply["created_at"],
            kind="REPLY",
            reply_id=reply["reply_id"],
            ingress_seq=run["selected_input_end_seq"],
            reconstructed=reconstructed,
            dependencies=dependencies,
            lineage_complete=False,
        )
        eligible = await (
            await conn.execute(
                "INSERT INTO public.bot_memory_source_eligibility(bot_id,occurrence_id,context_revision) "
                "VALUES (%s,%s,%s) ON CONFLICT DO NOTHING RETURNING occurrence_id",
                (self.store.bot_id, result["occurrence_id"], context["context_revision"]),
            )
        ).fetchone()
        if eligible:
            clock = await self._advance_in(conn)
            result["fact_revision"] = clock["fact_revision"]
        return result

    async def authorize_sources_in(
        self,
        conn,
        conversation=None,
        *,
        input_cut=None,
        source_unit_ids=None,
        limit=256,
        include_payload=True,
        after_source_unit_id=None,
    ):
        """Trusted host transaction: global index or frozen conversation candidates.

        Caller holds its current runtime/host and, for a conversation, its
        authority/row locks. Global mode removes only local cut and visibility;
        it never waives eligibility, complete lineage or suppression checks.
        Metadata mode verifies headers and body existence without loading bytes.
        """
        if type(limit) is not int or not 1 <= limit <= 256 or type(include_payload) is not bool:
            raise ValueError("Source read limits")
        if conversation is not None:
            if conversation["bot_id"] != self.store.bot_id:
                raise ForestError("MEMORY_SOURCE_SCOPE", "Another Bot conversation", 403)
            if type(input_cut) is not int or input_cut < 0:
                raise ValueError("Source input cut")
            if input_cut > conversation["context_applied_seq"]:
                raise ForestError(
                    "MEMORY_INPUT_CUT",
                    "Source retrieval cannot advance the committed input cut",
                    409,
                )
        elif input_cut is not None:
            raise ValueError("Global source authorization has no conversation cut")
        if source_unit_ids is not None:
            source_unit_ids = list(source_unit_ids)
            if len(source_unit_ids) > 256 or any(
                not isinstance(v, str) or not 1 <= len(v) <= 256 for v in source_unit_ids
            ):
                raise ValueError("Source identifiers")
        paged = conversation is None and source_unit_ids is None
        if after_source_unit_id is not None and (
            not paged
            or not isinstance(after_source_unit_id, str)
            or len(after_source_unit_id) > 256
        ):
            raise ValueError("Source cursor")
        clock = await self.payloads.lock_in(conn)
        rows = await (
            await conn.execute(
                "SELECT u.*,s.source_kind,s.visibility,s.platform,s.realm_id,s.identity_schema,s.channel_kind,s.channel_id,s.target_group_source_id,s.owner_conversation_id,s.has_reconstructed_order "
                "FROM public.bot_memory_source_units u JOIN public.bot_memory_sources s USING(bot_id,source_id) "
                "JOIN public.bot_payload_headers h USING(bot_id,payload_id) "
                "WHERE u.bot_id=%s AND (%s::text[] IS NULL OR u.source_unit_id=ANY(%s)) "
                "AND (%s::text IS NULL OR u.source_unit_id>%s) "
                "AND h.state='ACTIVE' AND u.lineage_complete "
                "AND EXISTS(SELECT 1 FROM public.bot_payload_bodies b WHERE b.bot_id=h.bot_id AND b.payload_id=h.payload_id) "
                "AND NOT EXISTS(SELECT 1 FROM public.bot_payload_sources p "
                "LEFT JOIN public.bot_memory_source_units d ON d.bot_id=p.bot_id AND d.source_unit_id=p.source_unit_id "
                "LEFT JOIN public.bot_memory_sources ds ON ds.bot_id=d.bot_id AND ds.source_id=d.source_id "
                "LEFT JOIN public.bot_payload_headers dh ON dh.bot_id=d.bot_id AND dh.payload_id=d.payload_id "
                "WHERE p.bot_id=u.bot_id AND p.payload_id=u.payload_id AND ("
                "d.source_unit_id IS NULL OR NOT d.lineage_complete OR d.source_revision<>p.source_revision OR dh.state<>'ACTIVE' "
                "OR NOT EXISTS(SELECT 1 FROM public.bot_payload_bodies db WHERE db.bot_id=dh.bot_id AND db.payload_id=dh.payload_id) "
                "OR (%s AND NOT (ds.visibility='bot_shared' OR (ds.source_kind='DIRECT' AND ds.source_id=%s) OR COALESCE(ds.owner_conversation_id=%s,false))) "
                "OR NOT EXISTS(SELECT 1 FROM public.bot_memory_source_occurrences o JOIN public.bot_memory_source_eligibility e USING(bot_id,occurrence_id) WHERE o.bot_id=d.bot_id AND o.source_unit_id=d.source_unit_id) "
                "OR EXISTS(SELECT 1 FROM public.bot_memory_source_occurrences o LEFT JOIN public.bot_memory_source_eligibility e USING(bot_id,occurrence_id) "
                "WHERE o.bot_id=d.bot_id AND o.source_unit_id=d.source_unit_id AND o.conversation_id=%s AND (o.ingress_seq>%s OR e.occurrence_id IS NULL)) "
                "OR EXISTS(SELECT 1 FROM public.bot_memory_source_occurrences o JOIN public.bot_message_inbox i USING(bot_id,event_id) "
                "LEFT JOIN public.bot_conversation_triggers t ON t.bot_id=i.bot_id AND t.event_id=i.event_id "
                "WHERE o.bot_id=d.bot_id AND o.source_unit_id=d.source_unit_id AND (i.context_disposition<>'READABLE' OR (i.origin='job' AND t.status='CANCELLED'))) "
                "OR EXISTS(SELECT 1 FROM public.bot_memory_source_occurrences o JOIN public.bot_message_echo_aliases a USING(bot_id,event_id) WHERE o.bot_id=d.bot_id AND o.source_unit_id=d.source_unit_id))) "
                + (
                    "ORDER BY u.source_unit_id LIMIT %s"
                    if paged
                    else "ORDER BY u.accepted_at DESC,u.source_unit_id LIMIT %s"
                ),
                (
                    self.store.bot_id,
                    source_unit_ids,
                    source_unit_ids,
                    (after_source_unit_id or "") if paged else None,
                    (after_source_unit_id or "") if paged else None,
                    conversation is not None,
                    conversation["group_source_id"] if conversation else None,
                    conversation["conversation_id"] if conversation else None,
                    conversation["conversation_id"] if conversation else None,
                    input_cut,
                    limit,
                ),
            )
        ).fetchall()
        result = []
        for row in rows:
            try:
                if include_payload:
                    value = await self.payloads.read_json_in(
                        conn, row["payload_id"], expected_deletion_epoch=clock["deletion_epoch"]
                    )
                else:
                    await self.payloads.require_available_in(
                        conn, (row["payload_id"],), expected_deletion_epoch=clock["deletion_epoch"]
                    )
            except ForestError as exc:
                if exc.code not in {
                    "PAYLOAD_ERASED",
                    "PAYLOAD_SUPPRESSED",
                    "SOURCE_SUPPRESSED",
                    "PAYLOAD_UNAVAILABLE",
                    "PAYLOAD_INCOMPLETE",
                }:
                    raise
                continue
            source = {
                key: row[key]
                for key in (
                    "source_id",
                    "source_kind",
                    "platform",
                    "realm_id",
                    "identity_schema",
                    "channel_kind",
                    "channel_id",
                    "target_group_source_id",
                )
            }
            source["group_source_id"] = row["source_id"]
            result.append(
                {
                    **row,
                    "source": source,
                    **(
                        {"author": value["author"], "parts": value["parts"]}
                        if include_payload
                        else {}
                    ),
                }
            )
        return {
            "deletion_epoch": clock["deletion_epoch"],
            "fact_revision": clock["fact_revision"],
            "input_cut": input_cut,
            "sources": result,
        }

    async def read(
        self,
        lease,
        conversation_id,
        *,
        input_cut,
        source_unit_ids=None,
        limit=64,
        include_payload=True,
    ):
        """Return only candidates with complete lineage and current source rights."""
        async with self.store.pool.connection() as conn, conn.transaction():
            location = await self.messages._locate(conn, conversation_id)
            binding = await self.messages.bindings._guard(
                conn, lease, location["binding_id"], data=False
            )
            conversation = await self.messages._conversation(conn, conversation_id)
            result = await self.authorize_sources_in(
                conn,
                conversation,
                input_cut=input_cut,
                source_unit_ids=source_unit_ids,
                limit=limit,
                include_payload=include_payload,
            )
            await self.messages.bindings._final_guard(conn, lease, binding, data=False)
            return result

    async def backfill(self, lease, *, limit=64):
        """Bounded primary-key scan; persistent cursors never sort all history.

        New code registers every new row inline. The one-time legacy scan may
        therefore finish permanently; old writers cannot run across this schema.
        """
        if type(limit) is not int or not 1 <= limit <= 256:
            raise ValueError("Source backfill batch")
        rows, finished = [], []
        async with self.store.pool.connection() as conn, conn.transaction():
            await self.store._host(conn, lease)
            for kind in ("INPUT", "REPLY"):
                await conn.execute(
                    "INSERT INTO public.bot_memory_backfill_heads(bot_id,kind) VALUES (%s,%s) ON CONFLICT DO NOTHING",
                    (self.store.bot_id, kind),
                )
                head = await (
                    await conn.execute(
                        "SELECT * FROM public.bot_memory_backfill_heads WHERE bot_id=%s AND kind=%s",
                        (self.store.bot_id, kind),
                    )
                ).fetchone()
                if head["completed"]:
                    continue
                allowance = min(limit - len(rows), max(1, limit // 2))
                if not allowance:
                    break
                query = (
                    "SELECT event_id AS identity,conversation_id FROM public.bot_message_inbox "
                    "WHERE bot_id=%s AND event_id>%s ORDER BY event_id LIMIT %s"
                    if kind == "INPUT"
                    else "SELECT reply_id AS identity,conversation_id FROM public.bot_conversation_replies "
                    "WHERE bot_id=%s AND reply_id>%s ORDER BY reply_id LIMIT %s"
                )
                chunk = await (
                    await conn.execute(query, (self.store.bot_id, head["cursor_id"], allowance))
                ).fetchall()
                rows.extend({**row, "kind": kind} for row in chunk)
                if len(chunk) < allowance:
                    finished.append(kind)
            await self.store._host(conn, lease)
        completed = 0
        for candidate in rows:
            async with self.store.pool.connection() as conn, conn.transaction():
                location = await self.messages._locate(conn, candidate["conversation_id"])
                # Backfill needs no online platform connection; it still uses
                # Binding -> runtime/host -> conversation -> memory lock order.
                await conn.execute(
                    "SELECT binding_id FROM public.bot_bindings WHERE bot_id=%s AND binding_id=%s FOR UPDATE",
                    (self.store.bot_id, location["binding_id"]),
                )
                await self.store._host(conn, lease)
                conversation = await self.messages._conversation(conn, candidate["conversation_id"])
                old = await (
                    await conn.execute(
                        "SELECT occurrence_id FROM public.bot_memory_source_occurrences WHERE bot_id=%s AND "
                        + ("event_id=%s" if candidate["kind"] == "INPUT" else "reply_id=%s"),
                        (self.store.bot_id, candidate["identity"]),
                    )
                ).fetchone()
                if candidate["kind"] == "INPUT":
                    inbox = await (
                        await conn.execute(
                            "SELECT * FROM public.bot_message_inbox WHERE bot_id=%s AND event_id=%s",
                            (self.store.bot_id, candidate["identity"]),
                        )
                    ).fetchone()
                    await self.register_input_in(conn, conversation, inbox, reconstructed=True)
                else:
                    reply = await (
                        await conn.execute(
                            "SELECT * FROM public.bot_conversation_replies WHERE bot_id=%s AND reply_id=%s",
                            (self.store.bot_id, candidate["identity"]),
                        )
                    ).fetchone()
                    await self.register_reply_in(conn, conversation, reply, reconstructed=True)
                await conn.execute(
                    "UPDATE public.bot_memory_backfill_heads SET cursor_id=GREATEST(cursor_id,%s) WHERE bot_id=%s AND kind=%s",
                    (candidate["identity"], self.store.bot_id, candidate["kind"]),
                )
                await self.store._host(conn, lease)
                completed += old is None
        async with self.store.pool.connection() as conn, conn.transaction():
            await self.store._host(conn, lease)
            if finished:
                await conn.execute(
                    "UPDATE public.bot_memory_backfill_heads SET completed=true WHERE bot_id=%s AND kind=ANY(%s)",
                    (self.store.bot_id, finished),
                )
            pending = await (
                await conn.execute(
                    "SELECT 1 FROM public.bot_memory_backfill_heads WHERE bot_id=%s AND NOT completed LIMIT 1",
                    (self.store.bot_id,),
                )
            ).fetchone()
            await self.store._host(conn, lease)
        return {"processed": completed, "scanned": len(rows), "more_possible": pending is not None}
