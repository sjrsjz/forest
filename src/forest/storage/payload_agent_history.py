"""Current-Run proofs for imported specifications and old Agent config leaves.

Modern specs never terminate provenance traversal. Imported Run specs retain
their exact local owner; old configuration leaves must belong to that Run's
actual frozen revision or its actual plan, with a frozen 040/041 source proof.
"""

from forest.packages import digest_json


async def imported_specs_in(conn, bot_id, ids):
    """Return actual imported specs whose own unknown body needs local proof.

    A configuration parent only explains inherited configuration content. It
    cannot explain an imported spec's additional, explicitly unknown fields.
    This is a bounded metadata lookup on the traversal's current batch.
    """
    return {row["spec_payload_id"] for row in await (await conn.execute(
        "SELECT DISTINCT spec_payload_id FROM public.bot_agent_runs WHERE bot_id=%s "
        "AND spec_imported_legacy AND spec_payload_id=ANY(%s)", (bot_id, list(ids)),
    )).fetchall()}


async def old_agent_leaves_in(conn, store, conversation, cut, ids, run_id):
    if run_id is None or not ids:
        return set()
    row = await (await conn.execute(
        "SELECT a.run_id,a.conversation_id,a.spec_payload_id,a.spec_payload_digest,a.spec_digest,a.spec_metadata,"
        "a.spec_imported_legacy,h.owner_kind,h.owner_id,h.encoding,h.digest "
        "FROM public.bot_agent_runs a JOIN public.bot_payload_headers h ON h.bot_id=a.bot_id AND h.payload_id=a.spec_payload_id "
        "JOIN public.bot_payload_bodies b ON b.bot_id=h.bot_id AND b.payload_id=h.payload_id "
        "WHERE a.bot_id=%s AND a.run_id=%s AND a.conversation_id=%s "
        "AND a.selected_input_end_seq<=%s AND h.state='ACTIVE'",
        (store.bot_id, run_id, conversation["conversation_id"], cut),
    )).fetchone()
    if row is None:
        return set()
    owner = "agent-run-spec-v1:" + digest_json([run_id])
    if (row["owner_kind"], row["owner_id"], row["encoding"], row["digest"]) != (
            "control.config", owner, "json", row["spec_payload_digest"]):
        return set()
    # Only immutable identity/digest facts enter this authorizer. Avoid fetching
    # the actual prompt or any tool schema into the scope traversal.
    if row["spec_digest"] != row["digest"] or row["spec_payload_id"] != "payload:" + digest_json([store.bot_id, "control.config", owner]):
        return set()
    accepted = {row["spec_payload_id"]} & set(ids) if row["spec_imported_legacy"] else set()
    revision_number = row["spec_metadata"]["config_revision"]
    if revision_number == 0:
        return accepted
    revision = await (await conn.execute(
        "SELECT r.* FROM public.bot_agent_config_revisions r JOIN public.bot_payload_derivations d "
        "ON d.bot_id=r.bot_id AND d.parent_payload_id=r.values_payload_id "
        "WHERE r.bot_id=%s AND r.revision=%s AND d.payload_id=%s",
        (store.bot_id, revision_number, row["spec_payload_id"]),
    )).fetchone()
    if revision is None:
        return set()
    plan = await (await conn.execute(
        "SELECT * FROM public.bot_agent_config_plans WHERE bot_id=%s AND plan_id=%s",
        (store.bot_id, revision["plan_id"]),
    )).fetchone()
    source_ids = [digest_json(["", revision_number])]
    if plan is not None:
        source_ids.append(plan["plan_id"])
    leaves = await (await conn.execute(
        "SELECT v.*,h.owner_kind,h.owner_id,h.digest,w.action,w.input_legacy_unknown,w.input_payload_id,"
        "w.input_payload_digest,w.result_payload_id,w.result_payload_digest,w.subject_id,w.issuer_id "
        "FROM public.bot_control_value_sources v JOIN public.bot_payload_headers h USING(bot_id,payload_id) "
        "LEFT JOIN public.bot_web_operations w ON w.bot_id=v.bot_id AND w.operation_id=v.operation_id "
        "WHERE v.bot_id=%s AND v.family='agent.config' AND v.source_id=ANY(%s) AND v.payload_id=ANY(%s) "
        "AND h.state='ACTIVE' AND h.encoding='json' AND h.manifest->'parents'='[]'::jsonb "
        "AND NOT EXISTS(SELECT 1 FROM public.bot_payload_derivations d WHERE d.bot_id=h.bot_id AND d.payload_id=h.payload_id) "
        "AND EXISTS(SELECT 1 FROM public.bot_payload_bodies b WHERE b.bot_id=h.bot_id AND b.payload_id=h.payload_id)",
        (store.bot_id, source_ids, list(ids)),
    )).fetchall()
    for leaf in leaves:
        if leaf["digest"] != leaf["payload_digest"]:
            continue
        if leaf["source_kind"] == "PLAN" and plan is not None and leaf["source_id"] == plan["plan_id"]:
            action, actual_values = "agent.config.plan", plan["values_payload_id"]
        elif leaf["source_kind"] == "REVISION" and leaf["source_id"] == digest_json(["", revision_number]):
            action, actual_values = "agent.config.apply", revision["values_payload_id"]
        else:
            continue
        # The frozen values importer must have selected this exact old leaf.
        if not await (await conn.execute(
            "SELECT 1 FROM public.bot_payload_derivations WHERE bot_id=%s AND payload_id=%s AND parent_payload_id=%s",
            (store.bot_id, actual_values, leaf["payload_id"]),
        )).fetchone():
            continue
        if leaf["legacy_untracked"]:
            if (leaf["operation_id"] is not None or leaf["owner_kind"] != "control.config"
                    or leaf["owner_id"] != "legacy-control:" + digest_json(["agent.config", leaf["source_kind"], leaf["source_id"]])):
                continue
        else:
            if (leaf["input_legacy_unknown"] is not True or leaf["input_payload_id"] is not None
                    or leaf["input_payload_digest"] is not None or leaf["action"] != action
                    or leaf["owner_kind"] != "control.result"
                    or leaf["owner_id"] != "result:" + digest_json([leaf["operation_id"]])
                    or leaf["result_payload_id"] != leaf["payload_id"] or leaf["result_payload_digest"] != leaf["digest"]):
                continue
            if leaf["source_kind"] == "PLAN":
                if (leaf["subject_id"], leaf["issuer_id"]) != (plan["subject_id"], plan["issuer_id"]):
                    continue
            elif leaf["operation_id"] != revision["operation_id"]:
                continue
        accepted.add(leaf["payload_id"])
    return accepted
