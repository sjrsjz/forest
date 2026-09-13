"""Frozen 044 supplemental dependency importer, covered by its migration hash.

Each family handler supplies references proved from its actual old business
rows. This module never chooses a revision from matching text or current heads.
It preserves all original payload identities, manifests and source ledgers.
"""

from forest.errors import ForestError
from forest.storage.migration_v1 import require_migration_scope


async def migrate_in(conn, store):
    require_migration_scope(conn, store)


async def import_dependency_in(conn, store, payload_id, parent_payload_id, *, proof_kind, proof_id):
    require_migration_scope(conn, store)
    await conn.execute(
        "SELECT 1 FROM public.bot_memory_clock WHERE bot_id=%s FOR UPDATE", (store.bot_id,)
    )
    # Original parents plus frozen supplemental roots establish acyclicity.
    # A budget violation aborts the migration; no edge or body is truncated.
    ancestors = await (await conn.execute(
        "WITH RECURSIVE edges AS (SELECT payload_id,parent_payload_id FROM public.bot_payload_derivations WHERE bot_id=%s "
        "UNION SELECT payload_id,parent_payload_id FROM public.bot_payload_legacy_dependencies WHERE bot_id=%s), "
        "ancestors(id) AS (SELECT %s::text UNION SELECT e.parent_payload_id FROM ancestors a JOIN edges e ON e.payload_id=a.id) "
        "SELECT id FROM ancestors LIMIT 8193", (store.bot_id, store.bot_id, parent_payload_id),
    )).fetchall()
    if len(ancestors) > 8192:
        raise ForestError("PAYLOAD_DEPENDENCY_LIMIT", "Legacy dependency proof exceeds its bound", 409)
    if payload_id in {row["id"] for row in ancestors}:
        raise ForestError("PAYLOAD_DEPENDENCY_CYCLE", "Legacy dependency would create a cycle", 409)
    previous = await (await conn.execute(
        "SELECT proof_kind,proof_id FROM public.bot_payload_legacy_dependencies "
        "WHERE bot_id=%s AND payload_id=%s AND parent_payload_id=%s",
        (store.bot_id, payload_id, parent_payload_id),
    )).fetchone()
    if previous:
        if (previous["proof_kind"], previous["proof_id"]) != (proof_kind, proof_id):
            raise ForestError("LEGACY_DEPENDENCY_CONFLICT", "Legacy prerequisite proof differs", 409)
        return
    await conn.execute("ALTER TABLE public.bot_payload_legacy_dependencies DISABLE TRIGGER bot_legacy_dependencies_frozen")
    await conn.execute(
        "INSERT INTO public.bot_payload_legacy_dependencies(bot_id,payload_id,parent_payload_id,proof_kind,proof_id) "
        "VALUES (%s,%s,%s,%s,%s)", (store.bot_id, payload_id, parent_payload_id, proof_kind, proof_id),
    )
    await conn.execute("ALTER TABLE public.bot_payload_legacy_dependencies ENABLE TRIGGER bot_legacy_dependencies_frozen")
    await conn.execute("ALTER TABLE public.bot_payload_legacy_closure DISABLE TRIGGER bot_legacy_closure_guard")
    # Recompute the monotone closure over all roots. A later family can add a
    # prerequisite to an ancestor of an earlier root, including old descendants.
    # SQL UNION deduplicates paths and preserves every proven copy without a
    # graph-size LIMIT. Consumer source budgets remain independently enforced.
    await conn.execute(
        "WITH RECURSIVE edges AS (SELECT payload_id,parent_payload_id FROM public.bot_payload_derivations WHERE bot_id=%s "
        "UNION SELECT payload_id,parent_payload_id FROM public.bot_payload_legacy_dependencies WHERE bot_id=%s), "
        "copies(id,root,parent) AS (SELECT payload_id,payload_id,parent_payload_id "
        "FROM public.bot_payload_legacy_dependencies WHERE bot_id=%s "
        "UNION SELECT e.payload_id,c.root,c.parent FROM copies c JOIN edges e ON e.parent_payload_id=c.id) "
        "INSERT INTO public.bot_payload_legacy_closure(bot_id,payload_id,root_payload_id,parent_payload_id) "
        "SELECT %s,id,root,parent FROM copies ON CONFLICT DO NOTHING",
        (store.bot_id,) * 4,
    )
    await conn.execute("ALTER TABLE public.bot_payload_legacy_closure ENABLE TRIGGER bot_legacy_closure_guard")
    # A deletion can predate this proof. Revoke every newly proved copy inside
    # this migration transaction, using the original immutable forget decision.
    # No original chat source is added to that decision, and no body is revived.
    await conn.execute(
        "UPDATE public.bot_payload_headers h SET state='REVOKED',forget_operation_id=("
        "SELECT x.operation_id FROM public.forest_payload_effective_sources s JOIN public.bot_payload_suppression x "
        "ON x.bot_id=s.bot_id AND x.source_unit_id=s.source_unit_id "
        "AND (x.part_id='*' OR s.part_id='*' OR x.part_id=s.part_id) "
        "WHERE s.bot_id=h.bot_id AND s.payload_id=h.payload_id ORDER BY x.deletion_epoch,x.operation_id LIMIT 1) "
        "WHERE h.bot_id=%s AND h.state='ACTIVE' AND EXISTS("
        "SELECT 1 FROM public.forest_payload_effective_sources s JOIN public.bot_payload_suppression x "
        "ON x.bot_id=s.bot_id AND x.source_unit_id=s.source_unit_id "
        "AND (x.part_id='*' OR s.part_id='*' OR x.part_id=s.part_id) WHERE s.bot_id=h.bot_id AND s.payload_id=h.payload_id)",
        (store.bot_id,),
    )
