"""Frozen helpers for the 029--034 content migration chain.

This source is included in those migration checksums. Future changes to the
algorithm require a new versioned module, not editing a released helper.
All work uses the dispatcher's existing transaction; there is no external I/O.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import asdict

from psycopg.pq import TransactionStatus
from psycopg.types.json import Jsonb

from forest.errors import ForestError
from forest.memory_types import SourceRef

WRITER_GENERATION = 1
_SCOPE = ContextVar("forest_migration_v1_scope", default=None)
_OWNERS = frozenset(
    {
        "memory.source",
        "memory.assertion",
        "memory.artifact",
        "memory.evidence",
        "memory.query",
        "memory.embedding",
        "memory.vector",
        "memory.index",
        "media.source",
        "media.capture",
        "media.variant",
        "model.snapshot",
        "model.manifest",
        "model.request",
        "model.result",
        "agent.tool",
        "agent.observation",
        "agent.child",
        "context.parts",
        "reply.parts",
        "delivery.payload",
        "delivery.request",
        "delivery.receipt",
        "kernel.result",
        "state.value",
        "state.migration",
        "control.result",
        "control.config",
        "control.audit",
        "control.outbox",
        "job.definition",
        "shell.arguments",
    }
)


def _json(value):
    try:
        body = json.dumps(
            value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
        ).encode()
    except (ValueError, TypeError, OverflowError, RecursionError, UnicodeError) as exc:
        raise ForestError("INVALID_PAYLOAD", "Payload must be bounded JSON", 422) from exc
    if len(body) > 33554432:
        raise ForestError("PAYLOAD_TOO_LARGE", "Payload exceeds storage limit", 413)
    return body


def _digest(value):
    return "sha256:" + hashlib.sha256(_json(value)).hexdigest()


def legacy_source_ref(bot_id: str) -> SourceRef:
    """Unknown pre-cutover dependencies, never an ordinary source identity."""
    return SourceRef("legacy-source:" + _digest([bot_id, "untracked-pre-029"]))


@contextmanager
def _migration_context(conn, store, version):
    """Only the dispatcher enters this scope; copied contexts cannot use it."""
    token = _SCOPE.set((conn, store, version, asyncio.current_task()))
    try:
        yield
    finally:
        _SCOPE.reset(token)


def require_migration_scope(conn, store):
    scope = _SCOPE.get()
    if (
        scope is None
        or scope[0] is not conn
        or scope[1] is not store
        or scope[3] is not asyncio.current_task()
        or conn.info.transaction_status != TransactionStatus.INTRANS
    ):
        raise ForestError(
            "MIGRATION_REQUIRED", "Legacy imports require the migration transaction", 403
        )


async def install_writer_gate_in(conn):
    """Called after old runtime/host ownership is quiescent, before new DDL."""
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS public.bot_schema_writer_gate (
            singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
            minimum_generation integer NOT NULL CHECK(minimum_generation>0)
        );
        INSERT INTO public.bot_schema_writer_gate(singleton,minimum_generation)
            VALUES (true,1) ON CONFLICT(singleton) DO UPDATE
            SET minimum_generation=GREATEST(bot_schema_writer_gate.minimum_generation,1);
        CREATE OR REPLACE FUNCTION public.forest_bot_schema_writer_guard()
        RETURNS trigger LANGUAGE plpgsql AS $$
        DECLARE required_generation integer; writer text;
        BEGIN
            SELECT minimum_generation INTO required_generation
                FROM public.bot_schema_writer_gate WHERE singleton;
            writer := current_setting('forest.writer_generation',true);
            IF writer IS NULL OR writer !~ '^[0-9]{1,9}$'
               OR writer::integer < required_generation THEN
                RAISE EXCEPTION 'WRITER_SCHEMA_TOO_OLD' USING ERRCODE='55000';
            END IF;
            IF TG_OP='DELETE' THEN RETURN OLD; END IF;
            RETURN NEW;
        END $$;
        DROP TRIGGER IF EXISTS bot_schema_writer_guard ON public.bot_host_lease;
        CREATE TRIGGER bot_schema_writer_guard BEFORE INSERT OR UPDATE OR DELETE
            ON public.bot_host_lease FOR EACH ROW
            EXECUTE FUNCTION public.forest_bot_schema_writer_guard();
    """)


def _identifier(value):
    if (
        not isinstance(value, str)
        or not value
        or len(value.encode()) > 256
        or any(ord(c) < 32 for c in value)
    ):
        raise ForestError("INVALID_PAYLOAD_REFERENCE", "Invalid payload reference", 422)
    return value


async def import_payload_in(
    conn, store, *, owner_kind, owner_id, value, encoding, sources=(), parents=()
):
    """Import an immutable legacy body, or its already-deleted identity only.

    Suppressed content is never inserted even transiently. The temporary ACTIVE
    header exists only inside this transaction so existing immutable lineage
    triggers can validate source/parent rows before the header becomes REVOKED.
    """
    from forest.storage.payloads import PayloadRef

    require_migration_scope(conn, store)
    if owner_kind not in _OWNERS:
        raise ForestError("INVALID_PAYLOAD_OWNER", "Unsupported payload owner kind", 422)
    _identifier(owner_id)
    content = _json(value) if encoding == "json" else value
    if encoding not in ("json", "bytes") or type(content) is not bytes or len(content) > 33554432:
        raise ForestError("PAYLOAD_TOO_LARGE", "Payload exceeds storage limit", 413)
    source_set = tuple(sources)
    if len(source_set) > 4096 or any(type(s) is not SourceRef for s in source_set):
        raise ForestError("INVALID_SOURCE_MANIFEST", "Invalid source dependency set", 422)
    parent_set = tuple(sorted(set(_identifier(p) for p in parents)))
    if len(parent_set) > 256:
        raise ForestError("PAYLOAD_DEPENDENCY_LIMIT", "Too many payload dependencies", 422)
    await conn.execute(
        "INSERT INTO public.bot_memory_clock(bot_id) VALUES (%s) ON CONFLICT DO NOTHING",
        (store.bot_id,),
    )
    clock = await (
        await conn.execute(
            "SELECT * FROM public.bot_memory_clock WHERE bot_id=%s FOR UPDATE",
            (store.bot_id,),
        )
    ).fetchone()
    parent_rows = []
    for parent in parent_set:
        row = await (
            await conn.execute(
                "SELECT * FROM public.bot_payload_headers WHERE bot_id=%s AND payload_id=%s",
                (store.bot_id, parent),
            )
        ).fetchone()
        if row is None:
            raise ForestError("PAYLOAD_NOT_FOUND", "Legacy parent payload is missing", 409)
        parent_rows.append(row)
        source_set += tuple(SourceRef(**s) for s in row["manifest"]["sources"])
    source_set = tuple(sorted(set(source_set)))
    if len(source_set) > 4096:
        raise ForestError("INVALID_SOURCE_MANIFEST", "Invalid source dependency set", 422)
    manifest = {"sources": [asdict(s) for s in source_set], "parents": list(parent_set)}
    digest = "sha256:" + hashlib.sha256(content).hexdigest()
    input_digest = _digest([owner_kind, owner_id, encoding, digest, manifest])
    payload_id = "payload:" + _digest([store.bot_id, owner_kind, owner_id])
    if payload_id in parent_set:
        raise ForestError("PAYLOAD_DEPENDENCY_CYCLE", "Payload cannot depend on itself", 422)
    previous = await (
        await conn.execute(
            "SELECT * FROM public.bot_payload_headers WHERE bot_id=%s AND payload_id=%s",
            (store.bot_id, payload_id),
        )
    ).fetchone()
    if (previous is None or previous["input_digest"] != input_digest) and any(
        s.source_unit_id == legacy_source_ref(store.bot_id).source_unit_id for s in source_set
    ):
        # A delete may predate the very first legacy marker. Its immutable
        # decision cannot be widened afterwards. Unknown legacy derivations
        # conservatively inherit one real suppressed source instead, so the
        # original deletion receipt authorizes REVOKED without ever admitting
        # these unproved bytes. Earliest suppression is permanent and stable.
        anchor = await (
            await conn.execute(
                "SELECT source_unit_id,part_id FROM public.bot_payload_suppression WHERE bot_id=%s "
                "ORDER BY deletion_epoch,operation_id,source_unit_id,part_id LIMIT 1",
                (store.bot_id,),
            )
        ).fetchone()
        if anchor:
            source_set = tuple(
                sorted(
                    set(
                        (
                            *source_set,
                            SourceRef(anchor["source_unit_id"], part_id=anchor["part_id"]),
                        )
                    )
                )
            )
            if len(source_set) > 4096:
                raise ForestError(
                    "INVALID_SOURCE_MANIFEST", "Legacy deletion closure exceeds source limit", 422
                )
            manifest = {"sources": [asdict(s) for s in source_set], "parents": list(parent_set)}
            input_digest = _digest([owner_kind, owner_id, encoding, digest, manifest])
    if previous:
        if previous["input_digest"] != input_digest:
            raise ForestError(
                "PAYLOAD_CONFLICT", "Payload owner already has different content", 409
            )
        return PayloadRef(
            payload_id,
            previous["digest"],
            previous["encoding"],
            previous["byte_length"],
            previous["state"],
        )
    forgotten = await (
        await conn.execute(
            "SELECT s.operation_id FROM public.bot_payload_suppression s, "
            "jsonb_array_elements(%s::jsonb) r WHERE s.bot_id=%s "
            "AND s.source_unit_id=r->>'source_unit_id' "
            "AND (s.part_id='*' OR r->>'part_id'='*' OR s.part_id=r->>'part_id') "
            "ORDER BY s.deletion_epoch,s.operation_id LIMIT 1",
            (Jsonb(manifest["sources"]), store.bot_id),
        )
    ).fetchone()
    if not forgotten:
        for row in parent_rows:
            body = await (
                await conn.execute(
                    "SELECT 1 FROM public.bot_payload_bodies WHERE bot_id=%s AND payload_id=%s",
                    (store.bot_id, row["payload_id"]),
                )
            ).fetchone()
            if row["state"] != "ACTIVE" or body is None:
                raise ForestError("PAYLOAD_INCOMPLETE", "Legacy parent has no available body", 409)
    await conn.execute(
        "INSERT INTO public.bot_payload_headers(bot_id,payload_id,owner_kind,owner_id,encoding,digest,"
        "byte_length,input_digest,manifest,accepted_deletion_epoch) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        (
            store.bot_id,
            payload_id,
            owner_kind,
            owner_id,
            encoding,
            digest,
            len(content),
            input_digest,
            Jsonb(manifest),
            clock["deletion_epoch"],
        ),
    )
    for source in source_set:
        await conn.execute(
            "INSERT INTO public.bot_payload_sources(bot_id,payload_id,source_unit_id,source_revision,part_id) "
            "VALUES (%s,%s,%s,%s,%s)",
            (
                store.bot_id,
                payload_id,
                source.source_unit_id,
                source.source_revision,
                source.part_id,
            ),
        )
    for parent in parent_set:
        await conn.execute(
            "INSERT INTO public.bot_payload_derivations(bot_id,payload_id,parent_payload_id) VALUES (%s,%s,%s)",
            (store.bot_id, payload_id, parent),
        )
    if forgotten:
        await conn.execute(
            "UPDATE public.bot_payload_headers SET state='REVOKED',forget_operation_id=%s "
            "WHERE bot_id=%s AND payload_id=%s",
            (forgotten["operation_id"], store.bot_id, payload_id),
        )
        state = "REVOKED"
    else:
        await conn.execute(
            "INSERT INTO public.bot_payload_bodies(bot_id,payload_id,content) VALUES (%s,%s,%s)",
            (store.bot_id, payload_id, content),
        )
        state = "ACTIVE"
    return PayloadRef(payload_id, digest, encoding, len(content), state)
