"""Frozen 034: reconcile unproved old SourcePayloads after an earlier deletion."""

from psycopg.types.json import Jsonb

from forest.storage.migration_v1 import _digest, legacy_source_ref, require_migration_scope


async def migrate_in(conn, store):
    require_migration_scope(conn, store)
    original = await (
        await conn.execute(
            "SELECT operation_id,deletion_epoch FROM public.bot_payload_suppression WHERE bot_id=%s "
            "ORDER BY deletion_epoch,operation_id,source_unit_id,part_id LIMIT 1",
            (store.bot_id,),
        )
    ).fetchone()
    if original is None:
        return
    clock = await (
        await conn.execute(
            "SELECT * FROM public.bot_memory_clock WHERE bot_id=%s FOR UPDATE",
            (store.bot_id,),
        )
    ).fetchone()
    marker = legacy_source_ref(store.bot_id).source_unit_id
    batch = 0
    while True:
        rows = await (
            await conn.execute(
                "SELECT u.source_unit_id FROM public.bot_memory_source_units u "
                "JOIN public.bot_payload_headers h USING(bot_id,payload_id) "
                "WHERE u.bot_id=%s AND NOT u.lineage_complete AND h.state='ACTIVE' "
                "ORDER BY u.source_unit_id LIMIT 256",
                (store.bot_id,),
            )
        ).fetchall()
        marker_active = await (
            await conn.execute(
                "SELECT 1 FROM public.bot_payload_sources s JOIN public.bot_payload_headers h USING(bot_id,payload_id) "
                "WHERE s.bot_id=%s AND s.source_unit_id=%s AND h.state='ACTIVE' LIMIT 1",
                (store.bot_id, marker),
            )
        ).fetchone()
        if not rows and not marker_active:
            return
        batch += 1
        targets = [
            {"source_unit_id": unit, "part_id": "*"}
            for unit in sorted({marker} | {row["source_unit_id"] for row in rows})
        ]
        epoch = clock["deletion_epoch"] + 1
        operation_id = "migration:034:legacy-reconcile:" + str(batch)
        await conn.execute(
            "INSERT INTO public.bot_payload_forget_requests(bot_id,operation_id,input_digest,targets,deletion_epoch) "
            "VALUES (%s,%s,%s,%s,%s)",
            (
                store.bot_id,
                operation_id,
                _digest([clock["deletion_epoch"], targets]),
                Jsonb(targets),
                epoch,
            ),
        )
        for target in targets:
            await conn.execute(
                "INSERT INTO public.bot_payload_suppression(bot_id,source_unit_id,part_id,operation_id,deletion_epoch) "
                "VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                (store.bot_id, target["source_unit_id"], target["part_id"], operation_id, epoch),
            )
        await conn.execute(
            "UPDATE public.bot_memory_clock SET deletion_epoch=%s,fact_revision=fact_revision+1 WHERE bot_id=%s",
            (epoch, store.bot_id),
        )
        changed = await (
            await conn.execute(
                "UPDATE public.bot_payload_headers h SET state='REVOKED',forget_operation_id=%s "
                "WHERE h.bot_id=%s AND h.state='ACTIVE' AND EXISTS(SELECT 1 FROM public.bot_payload_sources s,"
                "jsonb_array_elements(%s::jsonb) t WHERE s.bot_id=h.bot_id AND s.payload_id=h.payload_id "
                "AND s.source_unit_id=t->>'source_unit_id' "
                "AND (s.part_id='*' OR t->>'part_id'='*' OR s.part_id=t->>'part_id')) RETURNING payload_id",
                (operation_id, store.bot_id, Jsonb(targets)),
            )
        ).fetchall()
        if not changed:
            # An incomplete source without its own SourceRef is corrupted data,
            # not evidence that it is safe to erase or to silently skip forever.
            raise RuntimeError("LEGACY_SOURCE_MANIFEST_INCOMPLETE")
        details = {
            "reason_code": "LEGACY_INCOMPLETE_LINEAGE_AFTER_FORGET",
            "preexisting_operation_id": original["operation_id"],
            "deletion_epoch": epoch,
            "legacy_conservative": True,
            "source_count": len(rows),
            "payload_count": len(changed),
        }
        await conn.execute(
            "INSERT INTO public.bot_audit_records(bot_id,audit_id,action,operation_id,details) VALUES (%s,%s,%s,%s,%s)",
            (
                store.bot_id,
                operation_id,
                "memory.legacy_suppression_reconciled",
                operation_id,
                Jsonb(details),
            ),
        )
        await conn.execute(
            "INSERT INTO public.bot_outbox_events(bot_id,event_id,topic,operation_id,payload) VALUES (%s,%s,%s,%s,%s)",
            (
                store.bot_id,
                operation_id,
                "memory.legacy_suppression_reconciled",
                operation_id,
                Jsonb(details),
            ),
        )
        clock["deletion_epoch"] = epoch
