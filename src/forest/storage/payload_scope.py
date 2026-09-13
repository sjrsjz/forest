"""Host-only visibility checks for business results entering a conversation.

The caller already owns runtime/Host and its conversation authority. This
helper acquires only the memory clock; it neither reads content into Python nor
grants permission to a Worker, an index, or an unrelated conversation.
"""

from dataclasses import asdict
from types import SimpleNamespace

from psycopg.types.json import Jsonb

from forest.errors import ForestError
from forest.memory_types import SourceRef
from forest.storage.memory_sources import MemorySourceStore
from forest.storage.payloads import PayloadStore

MAX_SOURCES = 4096
MAX_PAYLOAD_IDS = 8192  # 128 model steps and their bounded 16-call tool groups.


def _denied():
    return ForestError(
        "PAYLOAD_SCOPE_DENIED", "Content sources are unavailable in this conversation", 409
    )


async def guard_payload_scope_in(conn, store, conversation, input_cut, payload_ids, *, legacy_run_id=None):
    if (
        not isinstance(conversation, dict) or conversation.get("bot_id") != store.bot_id
        or type(input_cut) is not int or input_cut < 0
        or input_cut > conversation["context_applied_seq"]
        or not isinstance(payload_ids, (tuple, list, set, frozenset))
        or len(payload_ids) > MAX_PAYLOAD_IDS
        or any(not isinstance(value, str) or not 1 <= len(value) <= 256 for value in payload_ids)
    ):
        raise _denied()
    ids = sorted(set(payload_ids))
    if not ids:
        return
    payloads = PayloadStore(store)
    await payloads.lock_in(conn)
    headers = await (await conn.execute(
        "SELECT h.payload_id,h.state,EXISTS(SELECT 1 FROM public.bot_payload_bodies b "
        "WHERE b.bot_id=h.bot_id AND b.payload_id=h.payload_id) AS has_body "
        "FROM public.bot_payload_headers h WHERE h.bot_id=%s AND h.payload_id=ANY(%s)",
        (store.bot_id, ids),
    )).fetchall()
    if len(headers) != len(ids) or any(r["state"] != "ACTIVE" or not r["has_body"] for r in headers):
        raise _denied()
    initial = await (await conn.execute(
        "SELECT DISTINCT source_unit_id,source_revision,part_id FROM public.bot_payload_sources "
        "WHERE bot_id=%s AND payload_id=ANY(%s) LIMIT %s",
        (store.bot_id, ids, MAX_SOURCES + 1),
    )).fetchall()
    if len(initial) > MAX_SOURCES:
        raise _denied()
    from forest.storage.payload_history import authorize_legacy_history_in, legacy_marker

    marker, local = legacy_marker(store.bot_id), set()
    historical = any(row["source_unit_id"] == marker for row in initial)
    if historical:
        extra, local = await authorize_legacy_history_in(
            conn, store, conversation, input_cut, ids, run_id=legacy_run_id,
        )
        extra = sorted(extra)
        clock = await payloads.lock_in(conn)
        for offset in range(0, len(extra), 256):
            try:
                await payloads.require_available_in(
                    conn, extra[offset:offset + 256], expected_deletion_epoch=clock["deletion_epoch"],
                )
            except ForestError as exc:
                if exc.code not in {"PAYLOAD_ERASED", "SOURCE_SUPPRESSED", "PAYLOAD_INCOMPLETE", "SOURCE_LINK_SCOPE_LIMIT"}:
                    raise
                raise _denied() from None
        initial = await (await conn.execute(
            "SELECT DISTINCT source_unit_id,source_revision,part_id FROM public.bot_payload_sources "
            "WHERE bot_id=%s AND payload_id=ANY(%s) LIMIT %s",
            (store.bot_id, sorted(set(ids) | set(extra)), MAX_SOURCES + 1),
        )).fetchall()
        if len(initial) > MAX_SOURCES:
            raise _denied()
    linked = await (await conn.execute(
        "SELECT * FROM public.forest_source_dependency_closure(%s,%s,%s)",
        (store.bot_id, Jsonb(initial), MAX_SOURCES),
    )).fetchall()
    if len(linked) > MAX_SOURCES:
        raise _denied()
    refs = tuple(SourceRef(**row) for row in linked)
    if not refs:
        return  # Explicit Host-only source-free notifications remain supported.
    encoded = Jsonb([asdict(ref) for ref in refs])
    suppressed = await (await conn.execute(
        "SELECT 1 FROM public.bot_payload_suppression s JOIN jsonb_to_recordset(%s) "
        "r(source_unit_id text,source_revision bigint,part_id text) ON r.source_unit_id=s.source_unit_id "
        "AND (s.part_id='*' OR r.part_id='*' OR s.part_id=r.part_id) WHERE s.bot_id=%s LIMIT 1",
        (encoded, store.bot_id),
    )).fetchone()
    if suppressed:
        raise _denied()
    historical_refs = local | ({marker} if historical else set())
    if any(ref.source_revision != 1 or ref.part_id != "*" for ref in refs if ref.source_unit_id in historical_refs):
        raise _denied()
    domains = {row["source_unit_id"] for row in await (await conn.execute(
        "SELECT source_unit_id FROM public.bot_payload_domains WHERE bot_id=%s AND source_unit_id=ANY(%s)",
        (store.bot_id, sorted({ref.source_unit_id for ref in refs})),
    )).fetchall()}
    if any(ref.source_revision != 1 or ref.part_id != "*" for ref in refs if ref.source_unit_id in domains):
        raise _denied()
    exempt = domains | historical_refs
    required = sorted({ref.source_unit_id for ref in refs if ref.source_unit_id not in exempt})
    sources = MemorySourceStore(SimpleNamespace(store=store))
    authorized = {}
    for offset in range(0, len(required), 256):
        selected = required[offset:offset + 256]
        current = await sources.authorize_sources_in(
            conn, conversation, input_cut=input_cut, source_unit_ids=selected,
            limit=256, include_payload=False,
        )
        if {row["source_unit_id"] for row in current["sources"]} != set(selected):
            raise _denied()
        authorized.update({row["source_unit_id"]: row for row in current["sources"]})
    part_sources = set()
    for ref in refs:
        if ref.source_unit_id in exempt:
            continue
        if ref.source_revision != authorized[ref.source_unit_id]["source_revision"]:
            raise _denied()
        if ref.part_id != "*":
            part_sources.add(ref.source_unit_id)
    # Most inputs carry '*'. Only explicit parts require parsing the canonical
    # source's registered part IDs. Return identifiers alone, never its body.
    found_parts = set()
    for offset in range(0, len(part_sources), 256):
        selected = sorted(part_sources)[offset:offset + 256]
        parts = await (await conn.execute(
            "SELECT u.source_unit_id,p->>'part_id' AS part_id FROM public.bot_memory_source_units u "
            "JOIN public.bot_payload_bodies b USING(bot_id,payload_id) "
            "CROSS JOIN LATERAL jsonb_array_elements(convert_from(b.content,'UTF8')::jsonb->'parts') p "
            "JOIN jsonb_to_recordset(%s) r(source_unit_id text,source_revision bigint,part_id text) "
            "ON r.source_unit_id=u.source_unit_id AND r.source_revision=u.source_revision "
            "AND r.part_id=p->>'part_id' WHERE u.bot_id=%s AND u.source_unit_id=ANY(%s)",
            (encoded, store.bot_id, selected),
        )).fetchall()
        found_parts.update((row["source_unit_id"], row["part_id"]) for row in parts)
    if any((ref.source_unit_id, ref.part_id) not in found_parts for ref in refs
           if ref.source_unit_id not in exempt and ref.part_id != "*"):
        raise _denied()
