"""Independent Host input domains: deletable dependencies, never recall items.

Callers already hold their authority and memory clock. The owner is a stable
Host identifier, not copied text or a Worker-supplied provenance declaration.
Registration does not bypass PayloadStore suppression checks or grant access to
any memory source. Re-registering a forgotten domain retains the same identity.
"""

from forest.errors import ForestError
from forest.memory_types import SourceRef
from forest.packages import digest_json

KINDS = frozenset({"SERVICE_INPUT", "GENERATION_CONFIG", "STATE_CONTRACT", "CONTROL_INPUT"})


class SourceDomainStore:
    def __init__(self, store):
        self.store = store

    async def source_in(self, conn, kind, owner_id):
        if (
            kind not in KINDS
            or not isinstance(owner_id, str)
            or not 1 <= len(owner_id.encode()) <= 256
            or any(ord(c) < 32 or ord(c) == 127 for c in owner_id)
        ):
            raise ForestError(
                "INVALID_SOURCE_DOMAIN",
                "Source domain requires a supported kind and stable owner",
                422,
            )
        source = SourceRef("domain:" + digest_json([self.store.bot_id, kind, owner_id]))
        await conn.execute(
            "INSERT INTO public.bot_payload_domains(bot_id,kind,owner_id,source_unit_id) "
            "VALUES (%s,%s,%s,%s) ON CONFLICT(bot_id,kind,owner_id) DO NOTHING",
            (self.store.bot_id, kind, owner_id, source.source_unit_id),
        )
        row = await (
            await conn.execute(
                "SELECT source_unit_id FROM public.bot_payload_domains WHERE bot_id=%s AND kind=%s AND owner_id=%s",
                (self.store.bot_id, kind, owner_id),
            )
        ).fetchone()
        if row is None or row["source_unit_id"] != source.source_unit_id:
            raise ForestError("SOURCE_DOMAIN_CONFLICT", "Source domain identity differs", 409)
        return source
