"""Content-only input/media access; callers retain their real Host/Actor guards.

Legacy columns are never a read fallback. Identity metadata remains available
for audit and authorization after display names and message bodies are erased.
"""

from forest.errors import ForestError
from forest.memory_types import SourceRef
from forest.packages import digest_json
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
UNAVAILABLE = frozenset(
    {
        "PAYLOAD_ERASED",
        "PAYLOAD_SUPPRESSED",
        "SOURCE_SUPPRESSED",
        "SOURCE_LINK_SCOPE_LIMIT",
        "PAYLOAD_UNAVAILABLE",
        "PAYLOAD_INCOMPLETE",
    }
)


def identity(author):
    return {key: value for key, value in author.items() if key in IDENTITY_FIELDS}


class InputContentStore:
    def __init__(self, store):
        self.store, self.payloads = store, PayloadStore(store)

    def source_ref(self, source_event_id, part_id="*"):
        return SourceRef(
            "source-unit:" + digest_json([self.store.bot_id, source_event_id]), 1, part_id
        )

    async def put_input_in(
        self, conn, *, event_id, source_event_id, author, segments, expected_deletion_epoch
    ):
        return await self.payloads.put_json_in(
            conn,
            owner_kind="context.parts",
            owner_id="input:" + event_id,
            value={"author": author, "segments": segments},
            sources=(self.source_ref(source_event_id),),
            expected_deletion_epoch=expected_deletion_epoch,
        )

    async def put_media_source_in(
        self, conn, *, media_ref, source_event_id, part_index, source, expected_deletion_epoch
    ):
        return await self.payloads.put_json_in(
            conn,
            owner_kind="media.source",
            owner_id=media_ref,
            value=source,
            sources=(self.source_ref(source_event_id, str(part_index)),),
            expected_deletion_epoch=expected_deletion_epoch,
        )

    async def read_input_in(self, conn, event_id, *, expected_deletion_epoch, required=False):
        row = await (
            await conn.execute(
                "SELECT * FROM public.bot_message_inbox WHERE bot_id=%s AND event_id=%s",
                (self.store.bot_id, event_id),
            )
        ).fetchone()
        if row is None:
            raise ForestError("EVENT_NOT_FOUND", "Input does not exist in this Bot", 404)
        if not row.get("payload_id"):
            raise ForestError(
                "INPUT_PAYLOAD_MIGRATION_REQUIRED", "Input content has not migrated", 409
            )
        try:
            value = await self.payloads.read_json_in(
                conn, row["payload_id"], expected_deletion_epoch=expected_deletion_epoch
            )
        except ForestError as exc:
            if required or exc.code not in UNAVAILABLE:
                raise
            return {
                **row,
                "payload_state": "UNAVAILABLE",
                "segments": [{"type": "unavailable", "reason": exc.code}],
                "content_error": exc.code,
            }
        if identity(value["author"]) != row["author"]:
            raise ForestError(
                "INPUT_PAYLOAD_CONFLICT", "Input identity differs from its payload", 409
            )
        return {
            **row,
            "payload_state": "ACTIVE",
            "author": value["author"],
            "segments": value["segments"],
        }

    async def read_media_source_in(self, conn, media_ref, *, expected_deletion_epoch):
        row = await (
            await conn.execute(
                "SELECT m.*,i.source_event_id FROM public.bot_inbound_media_refs m JOIN public.bot_message_inbox i USING(bot_id,event_id) WHERE m.bot_id=%s AND m.media_ref=%s",
                (self.store.bot_id, media_ref),
            )
        ).fetchone()
        if not row or not row.get("source_payload_id"):
            raise ForestError("MEDIA_SOURCE_UNAVAILABLE", "Image source is unavailable", 409)
        value = await self.payloads.read_json_in(
            conn, row["source_payload_id"], expected_deletion_epoch=expected_deletion_epoch
        )
        return {
            **row,
            "source": value,
            "source_unit_id": self.source_ref(row["source_event_id"]).source_unit_id,
        }

    async def read_capture_in(
        self, conn, media_ref, *, expected_deletion_epoch, media_revision=1, required=False
    ):
        row = await (
            await conn.execute(
                "SELECT * FROM public.bot_media_captures WHERE bot_id=%s AND media_ref=%s AND media_revision=%s",
                (self.store.bot_id, media_ref, media_revision),
            )
        ).fetchone()
        if row is None:
            return None
        if not row.get("payload_id") or not row.get("manifest_payload_id"):
            raise ForestError(
                "INPUT_PAYLOAD_MIGRATION_REQUIRED", "Image content has not migrated", 409
            )
        try:
            await self.payloads.require_available_in(
                conn,
                (row["payload_id"], row["manifest_payload_id"]),
                expected_deletion_epoch=expected_deletion_epoch,
            )
            content = await self.payloads.read_bytes_in(
                conn, row["payload_id"], expected_deletion_epoch=expected_deletion_epoch
            )
            manifest = await self.payloads.read_json_in(
                conn, row["manifest_payload_id"], expected_deletion_epoch=expected_deletion_epoch
            )
        except ForestError as exc:
            if required or exc.code not in UNAVAILABLE:
                raise
            return None
        return {**row, "content": content, "source_manifest": manifest}
