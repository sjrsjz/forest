"""Frozen 035 existing exact echo links; SQL owns proof and deletion closure."""

from forest.storage.migration_v1 import _digest


async def migrate_in(conn, store):
    cursor = ""
    while True:
        rows = await (
            await conn.execute(
                "SELECT e.event_id,e.attempt_id,o.source_unit_id,a.wire_payload_id FROM public.bot_message_echo_aliases e "
                "JOIN public.bot_memory_source_occurrences o ON o.bot_id=e.bot_id AND o.event_id=e.event_id "
                "JOIN public.bot_send_attempts a ON a.bot_id=e.bot_id AND a.attempt_id=e.attempt_id "
                "WHERE e.bot_id=%s AND e.event_id>%s ORDER BY e.event_id LIMIT 64",
                (store.bot_id, cursor),
            )
        ).fetchall()
        if not rows:
            break
        for row in rows:
            link_id = _digest(
                [store.bot_id, row["event_id"], row["attempt_id"], row["wire_payload_id"]]
            )
            await conn.execute(
                "INSERT INTO public.bot_payload_source_links(bot_id,link_id,event_id,attempt_id,dependent_source_unit_id,"
                "dependent_source_revision,parent_payload_id,accepted_deletion_epoch) "
                "SELECT %s,%s,%s,%s,%s,1,%s,deletion_epoch FROM public.bot_memory_clock WHERE bot_id=%s",
                (
                    store.bot_id,
                    link_id,
                    row["event_id"],
                    row["attempt_id"],
                    row["source_unit_id"],
                    row["wire_payload_id"],
                    store.bot_id,
                ),
            )
        cursor = rows[-1]["event_id"]
    # A dependent may sort before the link that later revokes its parent. Work
    # to a fixed point in bounded batches, preserving every prior fact/target.
    while True:
        result = await (
            await conn.execute(
                "SELECT public.forest_reconcile_source_links(%s,256) AS count",
                (store.bot_id,),
            )
        ).fetchone()
        if result["count"] == 0:
            break
