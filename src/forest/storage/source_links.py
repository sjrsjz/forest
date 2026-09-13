"""Host-only exact-output proof registration; no arbitrary source-link API."""

from forest.errors import ForestError
from forest.packages import digest_json
from forest.storage.payloads import PayloadStore


class SourceLinkStore:
    def __init__(self, store):
        self.store, self.payloads = store, PayloadStore(store)

    async def link_exact_echo_in(self, conn, event_id, *, expected_deletion_epoch):
        clock = await self.payloads._epoch_in(conn, expected_deletion_epoch)
        proof = await (
            await conn.execute(
                "SELECT e.attempt_id,o.source_unit_id,a.wire_payload_id FROM public.bot_message_echo_aliases e "
                "JOIN public.bot_memory_source_occurrences o ON o.bot_id=e.bot_id AND o.event_id=e.event_id "
                "JOIN public.bot_send_attempts a ON a.bot_id=e.bot_id AND a.attempt_id=e.attempt_id "
                "WHERE e.bot_id=%s AND e.event_id=%s",
                (self.store.bot_id, event_id),
            )
        ).fetchone()
        if not proof:
            raise ForestError("SOURCE_LINK_PROOF_INVALID", "Exact echo proof is incomplete", 409)
        link_id = digest_json(
            [self.store.bot_id, event_id, proof["attempt_id"], proof["wire_payload_id"]]
        )
        old = await (
            await conn.execute(
                "SELECT * FROM public.bot_payload_source_links WHERE bot_id=%s AND event_id=%s",
                (self.store.bot_id, event_id),
            )
        ).fetchone()
        if old and old["link_id"] != link_id:
            raise ForestError("SOURCE_LINK_CONFLICT", "Exact source link identity changed", 409)
        if not old:
            await conn.execute(
                "INSERT INTO public.bot_payload_source_links(bot_id,link_id,event_id,attempt_id,dependent_source_unit_id,"
                "dependent_source_revision,parent_payload_id,accepted_deletion_epoch) VALUES (%s,%s,%s,%s,%s,1,%s,%s)",
                (
                    self.store.bot_id,
                    link_id,
                    event_id,
                    proof["attempt_id"],
                    proof["source_unit_id"],
                    proof["wire_payload_id"],
                    clock["deletion_epoch"],
                ),
            )
        after = await self.payloads.lock_in(conn)
        return {
            "link_id": link_id,
            "created": old is None,
            "revoked": after["deletion_epoch"] != clock["deletion_epoch"],
            "deletion_epoch": after["deletion_epoch"],
        }
