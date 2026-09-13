"""Narrow visibility proof for frozen 040/041 component-configuration leaves.

Only real pre-cutover component configuration can terminate a legacy branch.
Neither the control owner prefix nor a modern configuration receipt grants that
exception. The caller owns the memory clock and still authorizes every ordinary
source, exact-echo link, and suppression in the enclosing bounded payload DAG.
"""

from forest.errors import ForestError
from forest.packages import digest_json

MAX_BATCH = 256


async def old_config_leaves_in(conn, bot_id, payload_ids):
    ids = sorted(set(payload_ids))
    if not ids:
        return set()
    if len(ids) > MAX_BATCH:
        raise ForestError("PAYLOAD_SCOPE_DENIED", "Content sources are unavailable in this conversation", 409)
    rows = await (await conn.execute(
        "SELECT h.payload_id,h.owner_kind,h.owner_id,h.digest,h.encoding,"
        "v.source_kind,v.source_id,v.operation_id,v.payload_digest,v.legacy_untracked,"
        "w.action,w.input_legacy_unknown,w.input_payload_id,w.input_payload_digest,"
        "w.result_payload_id,w.result_payload_digest,w.subject_id,w.issuer_id "
        "FROM public.bot_payload_headers h JOIN public.bot_control_value_sources v "
        "ON v.bot_id=h.bot_id AND v.payload_id=h.payload_id AND v.family='config' "
        "LEFT JOIN public.bot_web_operations w ON w.bot_id=v.bot_id AND w.operation_id=v.operation_id "
        "WHERE h.bot_id=%s AND h.payload_id=ANY(%s) AND h.state='ACTIVE' AND h.encoding='json' "
        "AND EXISTS(SELECT 1 FROM public.bot_payload_bodies b WHERE b.bot_id=h.bot_id AND b.payload_id=h.payload_id) "
        "AND h.manifest->'parents'='[]'::jsonb "
        "AND NOT EXISTS(SELECT 1 FROM public.bot_payload_derivations d WHERE d.bot_id=h.bot_id AND d.payload_id=h.payload_id) "
        "ORDER BY h.payload_id,v.source_kind,v.source_id LIMIT %s",
        (bot_id, ids, MAX_BATCH + 1),
    )).fetchall()
    if len(rows) > MAX_BATCH:
        raise ForestError("PAYLOAD_SCOPE_DENIED", "Content sources are unavailable in this conversation", 409)
    # PLAN uses its real primary key; do not scan revisions for this common
    # case. A hash identity is compared only after bounded identity selection.
    plan_ids = [r["source_id"] for r in rows if r["source_kind"] == "PLAN"]
    plans = {}
    if plan_ids:
        plans = {r["plan_id"]: r for r in await (await conn.execute(
            "SELECT plan_id,subject_id,issuer_id FROM public.bot_config_plans "
            "WHERE bot_id=%s AND plan_id=ANY(%s) LIMIT %s", (bot_id, plan_ids, MAX_BATCH + 1),
        )).fetchall()}
    revisions = {}
    operations = sorted({r["operation_id"] for r in rows
        if r["source_kind"] == "REVISION" and not r["legacy_untracked"] and r["operation_id"]})
    if operations:
        selected = await (await conn.execute(
            "SELECT component_id,revision,operation_id FROM public.bot_config_revisions "
            "WHERE bot_id=%s AND operation_id=ANY(%s) LIMIT %s", (bot_id, operations, MAX_BATCH + 1),
        )).fetchall()
        if len(selected) > MAX_BATCH:
            raise ForestError("PAYLOAD_SCOPE_DENIED", "Content sources are unavailable in this conversation", 409)
        revisions.update({digest_json([r["component_id"], r["revision"]]): r for r in selected})
    untracked = [r["payload_id"] for r in rows if r["source_kind"] == "REVISION" and r["legacy_untracked"]]
    if untracked:
        # A missing old operation cannot be reverse-engineered from a hash.
        # The actual configuration activation of a published generation gives
        # a bounded, exact revision identity. Without it we decline the proof.
        selected = await (await conn.execute(
            "SELECT DISTINCT d.parent_payload_id,r.component_id,r.revision,r.operation_id "
            "FROM public.bot_payload_derivations d JOIN public.bot_generations g "
            "ON g.bot_id=d.bot_id AND g.config_payload_id=d.payload_id "
            "JOIN public.bot_lifecycle_operations l ON l.bot_id=g.bot_id "
            "AND l.candidate_generation_id=g.generation_id AND l.component_id=g.component_id AND l.phase='COMMITTED' "
            "JOIN public.bot_config_activations a ON a.bot_id=l.bot_id "
            "AND a.operation_id=l.operation_id AND a.component_id=g.component_id "
            "JOIN public.bot_config_revisions r ON r.bot_id=a.bot_id "
            "AND r.component_id=a.component_id AND r.revision=a.revision "
            "WHERE d.bot_id=%s AND d.parent_payload_id=ANY(%s) LIMIT %s",
            (bot_id, untracked, MAX_BATCH + 1),
        )).fetchall()
        if len(selected) > MAX_BATCH:
            raise ForestError("PAYLOAD_SCOPE_DENIED", "Content sources are unavailable in this conversation", 409)
        revisions.update({(r["parent_payload_id"], digest_json([r["component_id"], r["revision"]])): r for r in selected})
    accepted, seen, ambiguous = set(), set(), set()
    for row in rows:
        identity = row["payload_id"]
        if identity in seen:
            ambiguous.add(identity)
        seen.add(identity)
        if row["digest"] != row["payload_digest"]:
            continue
        if row["source_kind"] == "PLAN":
            plan = plans.get(row["source_id"])
            if plan is None:
                continue
            action = "config.plan"
        elif row["source_kind"] == "REVISION":
            revision = revisions.get((identity, row["source_id"]) if row["legacy_untracked"] else row["source_id"])
            if revision is None:
                continue
            action = "config.apply"
        else:
            continue
        if row["legacy_untracked"]:
            # Frozen 041 imported a missing-source configuration body. Its
            # immutable bridge and exact importer owner are both required.
            if (row["operation_id"] is not None or row["owner_kind"] != "control.config"
                    or row["owner_id"] != "legacy-control:" + digest_json([
                        "config", row["source_kind"], row["source_id"]])):
                continue
        else:
            # Frozen 040 imported an existing operation result without making
            # up its absent request. New results can never take this branch.
            if (row["input_legacy_unknown"] is not True or row["input_payload_id"] is not None
                    or row["input_payload_digest"] is not None or row["action"] != action
                    or row["owner_kind"] != "control.result"
                    or row["owner_id"] != "result:" + digest_json([row["operation_id"]])
                    or row["result_payload_id"] != identity
                    or row["result_payload_digest"] != row["digest"]):
                continue
            if row["source_kind"] == "PLAN":
                if (row["subject_id"], row["issuer_id"]) != (plan["subject_id"], plan["issuer_id"]):
                    continue
            elif revision["operation_id"] != row["operation_id"]:
                continue
        accepted.add(identity)
    return accepted - ambiguous
