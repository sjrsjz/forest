"""Bounded, conversation-local proofs for pre-cutover content.

A legacy deletion marker is never a visibility grant. Each branch carrying it
must reach an exact migrated Run owner or an already published local body.
Frozen, proven component-configuration leaves retain their prior Host scope.
The caller still checks every ordinary source and the current deletion clock.
"""

import hashlib
import json
from collections import defaultdict, deque

from forest.errors import ForestError
from forest.packages import digest_json

MAX_NODES = 8192
MAX_EDGES = 16384
MAX_METADATA_BYTES = 16 * 1024 * 1024
BATCH = 256


def legacy_marker(bot_id):
    return "legacy-source:" + digest_json([bot_id, "untracked-pre-029"])


def _deny():
    raise ForestError("PAYLOAD_SCOPE_DENIED", "Content sources are unavailable in this conversation", 409)


def _owner(table, key, field):
    encoded = json.dumps([table, key, field], ensure_ascii=False, separators=(",", ":")).encode()
    return "legacy-agent:" + hashlib.sha256(encoded).hexdigest()


async def _published(conn, bot_id, conversation, cut, ids):
    rows = await (await conn.execute(
        "SELECT DISTINCT u.payload_id,u.kind,r.reply_id FROM public.bot_context_units u "
        "LEFT JOIN public.bot_conversation_replies r ON r.bot_id=u.bot_id AND r.reply_id=u.reply_id "
        "AND r.conversation_id=u.conversation_id AND r.payload_id=u.payload_id "
        "LEFT JOIN public.bot_conversation_runs rr ON rr.bot_id=r.bot_id AND rr.run_id=r.run_id "
        "LEFT JOIN public.bot_agent_steps s ON s.bot_id=u.bot_id AND s.logical_step_id=u.logical_step_id "
        "LEFT JOIN public.bot_agent_runs ar ON ar.bot_id=s.bot_id AND ar.run_id=s.run_id "
        "LEFT JOIN public.bot_shell_requests q ON q.bot_id=u.bot_id AND q.command_id=u.control_command_id "
        "LEFT JOIN public.bot_message_inbox i ON i.bot_id=q.bot_id AND i.event_id=q.event_id "
        "WHERE u.bot_id=%s AND u.conversation_id=%s AND u.context_revision<=%s AND u.payload_id=ANY(%s) "
        "AND ((u.kind='REPLY' AND rr.conversation_id=u.conversation_id AND rr.selected_input_end_seq<=%s) "
        "OR (u.kind='RUN_STEP' AND ar.conversation_id=u.conversation_id AND s.selected_input_end_seq<=%s "
        "AND s.published_context_revision=u.context_revision) "
        "OR (u.kind='CONTROL' AND q.conversation_id=u.conversation_id AND q.status='COMPLETED' "
        "AND i.conversation_id=u.conversation_id AND i.ingress_seq<=%s)) LIMIT %s",
        (bot_id, conversation["conversation_id"], conversation["context_revision"], ids, cut, cut, cut, BATCH + 1),
    )).fetchall()
    if len(rows) > BATCH:
        _deny()
    return rows


async def _old_run_roots(conn, bot_id, conversation, cut, ids, run_id, *, scoped=True):
    if scoped and run_id is None:
        return []
    # Identity-only mode finds actual imported unknown roots in this batch;
    # it grants nothing. A root still needs the scoped current-Run/cut proof.
    scope_sql = "AND s.run_id=%s AND a.conversation_id=%s AND s.selected_input_end_seq<=%s " if scoped else ""
    scope_args = (run_id, conversation["conversation_id"], cut) if scoped else ()
    # All table/field names below are fixed Host code, never request arguments.
    families = (
        ("bot_agent_steps", "s", "logical_step_id", "", (("snapshot", "model.snapshot"),
            ("output", "model.result"), ("closed_parts", "context.parts"))),
        ("bot_agent_tools", "t", "operation_id", "JOIN public.bot_agent_tools t USING(bot_id,logical_step_id)",
            (("arguments", "agent.tool"), ("result", "agent.tool"))),
        ("bot_model_transport_attempts", "t", "transport_attempt_id",
            "JOIN public.bot_model_transport_attempts t USING(bot_id,logical_step_id)",
            (("request", "model.request"), ("result", "model.result"))),
        ("bot_model_request_manifests", "t", "model_request_id",
            "JOIN public.bot_model_request_manifests t USING(bot_id,logical_step_id)",
            (("manifest", "model.manifest"),)),
    )
    found = []
    for table, alias, key, join, fields in families:
        values = ",".join(f"('{field}', '{kind}', {alias}.{field}_payload_id)" for field, kind in fields)
        rows = await (await conn.execute(
            f"SELECT {alias}.{key} AS owner_key,v.field,v.kind,v.payload_id,h.owner_id,h.owner_kind,"
            "s.logical_step_id,s.snapshot_payload_id,s.input_revision,s.selected_input_end_seq,s.step_seq "
            f"FROM public.bot_agent_steps s {join} "
            f"CROSS JOIN LATERAL (VALUES {values}) v(field,kind,payload_id) "
            "JOIN public.bot_agent_runs a ON a.bot_id=s.bot_id AND a.run_id=s.run_id "
            "JOIN public.bot_payload_headers h ON h.bot_id=s.bot_id AND h.payload_id=v.payload_id "
            "WHERE s.bot_id=%s " + scope_sql + "AND v.payload_id=ANY(%s) LIMIT %s",
            (bot_id, *scope_args, ids, BATCH + 1),
        )).fetchall()
        if len(rows) > BATCH:
            _deny()
        for row in rows:
            old_field = "request_body" if row["field"] == "request" else row["field"]
            if row["owner_kind"] == row["kind"] and row["owner_id"] == _owner(table, row["owner_key"], old_field):
                found.append(row)
    return found


async def _private_units(conn, bot_id, run_id, budget):
    if budget["private"] is not None:
        return budget["private"]
    rows = await (await conn.execute(
        "SELECT s.closed_parts_payload_id AS payload_id,h.byte_length FROM public.bot_agent_steps s "
        "JOIN public.bot_payload_headers h ON h.bot_id=s.bot_id AND h.payload_id=s.closed_parts_payload_id "
        "WHERE s.bot_id=%s AND s.run_id=%s AND s.status IN ('CLOSED','FINAL_COMMITTED') LIMIT 129",
        (bot_id, run_id),
    )).fetchall()
    budget["bytes"] += sum(row["byte_length"] for row in rows)
    if len(rows) > 128 or budget["bytes"] > MAX_METADATA_BYTES:
        _deny()
    units = await (await conn.execute(
        "SELECT v->>'unit_id' AS unit_id,s.closed_parts_payload_id AS payload_id,"
        "s.step_seq,s.selected_input_end_seq FROM public.bot_agent_steps s "
        "JOIN public.bot_payload_bodies b ON b.bot_id=s.bot_id AND b.payload_id=s.closed_parts_payload_id "
        "CROSS JOIN LATERAL jsonb_array_elements(convert_from(b.content,'UTF8')::jsonb) v "
        "WHERE s.bot_id=%s AND s.run_id=%s AND s.closed_parts_payload_id=ANY(%s) LIMIT %s",
        (bot_id, run_id, [row["payload_id"] for row in rows], MAX_NODES + 1),
    )).fetchall()
    if len(units) > MAX_NODES or len({row["unit_id"] for row in units}) != len(units):
        _deny()
    budget["private"] = {row["unit_id"]: row for row in units}
    return budget["private"]


async def _snapshot_inputs(conn, bot_id, conversation, cut, root, run_id, budget):
    # Decode only bounded identity projections; never fetch original message,
    # tool or exact HTTP request text into the scope authorizer.
    header = await (await conn.execute(
        "SELECT byte_length FROM public.bot_payload_headers WHERE bot_id=%s AND payload_id=%s",
        (bot_id, root["snapshot_payload_id"]),
    )).fetchone()
    if header is None:
        _deny()
    budget["bytes"] += 2 * header["byte_length"]
    if budget["bytes"] > MAX_METADATA_BYTES:
        _deny()
    value = await (await conn.execute(
        "SELECT j->>'bot_id' AS bot,j->>'run_id' AS run,j->>'logical_step_id' AS step,"
        "j->>'input_revision' AS revision,j->>'selected_input_end_seq' AS cut,"
        "j->>'context_revision' AS context_revision,"
        "j->'conversation'->>'conversation_id' AS conversation,jsonb_typeof(j->'units') AS units_type "
        "FROM public.bot_payload_bodies b CROSS JOIN LATERAL "
        "(SELECT convert_from(b.content,'UTF8')::jsonb AS j) p WHERE b.bot_id=%s AND b.payload_id=%s",
        (bot_id, root["snapshot_payload_id"]),
    )).fetchone()
    if (value is None or value["bot"] != bot_id or value["run"] != run_id
            or value["step"] != root["logical_step_id"]
            or value["revision"] != str(root["input_revision"])
            or value["cut"] != str(root["selected_input_end_seq"])
            or value["conversation"] != conversation["conversation_id"] or value["units_type"] != "array"):
        _deny()
    revision = value["context_revision"]
    if not isinstance(revision, str) or not revision.isdecimal() or len(revision) > 19:
        _deny()
    revision = int(revision)
    if revision > conversation["context_revision"]:
        _deny()
    units = await (await conn.execute(
        "SELECT p->>'unit_id' AS unit_id,p->'source'->>'conversation_id' AS source_conversation,"
        "u.kind,u.conversation_id,u.context_revision,u.payload_id,i.payload_id AS input_payload_id,"
        "i.ingress_seq,i.context_disposition,i.origin,t.status AS trigger_status,e.event_id AS echo_id "
        "FROM public.bot_payload_bodies b CROSS JOIN LATERAL "
        "jsonb_array_elements(convert_from(b.content,'UTF8')::jsonb->'units') p "
        "LEFT JOIN public.bot_context_units u ON u.bot_id=b.bot_id AND u.unit_id=p->>'unit_id' "
        "LEFT JOIN public.bot_message_inbox i ON i.bot_id=u.bot_id AND i.event_id=u.event_id "
        "LEFT JOIN public.bot_conversation_triggers t ON t.bot_id=i.bot_id AND t.event_id=i.event_id "
        "LEFT JOIN public.bot_message_echo_aliases e ON e.bot_id=i.bot_id AND e.event_id=i.event_id "
        "WHERE b.bot_id=%s AND b.payload_id=%s LIMIT %s",
        (bot_id, root["snapshot_payload_id"], MAX_NODES + 1),
    )).fetchall()
    if len(units) > MAX_NODES:
        _deny()
    parents = set()
    for unit in units:
        if unit["kind"] is None:
            private = (await _private_units(conn, bot_id, run_id, budget)).get(unit["unit_id"])
            if (private is None or private["step_seq"] >= root["step_seq"]
                    or private["selected_input_end_seq"] > root["selected_input_end_seq"]
                    or unit["source_conversation"] not in {None, conversation["conversation_id"]}):
                _deny()
            parents.add(private["payload_id"])
            continue
        if (unit["kind"] is None or unit["conversation_id"] != conversation["conversation_id"]
                or unit["source_conversation"] not in {None, conversation["conversation_id"]}
                or unit["context_revision"] > revision):
            _deny()
        if unit["kind"] == "INPUT":
            if (unit["ingress_seq"] > min(cut, root["selected_input_end_seq"])
                    or unit["context_disposition"] != "READABLE" or unit["echo_id"]
                    or (unit["origin"] == "job" and unit["trigger_status"] == "CANCELLED")):
                _deny()
            parents.add(unit["input_payload_id"])
        else:
            parents.add(unit["payload_id"])
    if None in parents:
        _deny()
    return parents


async def _local_reply_sources(conn, store, conversation, cut, ids, extra, budget):
    """Follow known historical Reply facts, including clipped local ancestors."""
    pending, seen, local = set(ids) | extra, set(), set()
    while pending:
        batch = sorted(pending)[:BATCH]
        pending.difference_update(batch)
        seen.update(batch)
        budget["nodes"].update(batch)
        if len(budget["nodes"] | pending) > MAX_NODES:
            _deny()
        rows = await (await conn.execute(
            "SELECT DISTINCT p.payload_id AS from_payload_id,u.source_unit_id,u.source_revision,u.payload_id,o.reply_id,"
            "r.payload_id AS reply_payload_id FROM public.bot_payload_sources p "
            "JOIN public.bot_memory_source_units u USING(bot_id,source_unit_id) "
            "JOIN public.bot_memory_source_occurrences o USING(bot_id,source_unit_id) "
            "JOIN public.bot_memory_source_eligibility e USING(bot_id,occurrence_id) "
            "JOIN public.bot_conversation_replies r ON r.bot_id=o.bot_id AND r.reply_id=o.reply_id "
            "AND r.conversation_id=o.conversation_id "
            "WHERE p.bot_id=%s AND p.payload_id=ANY(%s) AND u.kind='REPLY' AND o.conversation_id=%s "
            "AND NOT EXISTS(SELECT 1 FROM public.bot_memory_source_occurrences x "
            "LEFT JOIN public.bot_memory_source_eligibility q USING(bot_id,occurrence_id) "
            "WHERE x.bot_id=u.bot_id AND x.source_unit_id=u.source_unit_id AND x.conversation_id=%s "
            "AND (x.ingress_seq>%s OR q.occurrence_id IS NULL)) LIMIT %s",
            (store.bot_id, batch, conversation["conversation_id"], conversation["conversation_id"], cut, MAX_EDGES + 1),
        )).fetchall()
        if len(rows) > MAX_EDGES:
            _deny()
        candidate_ids = sorted({row["reply_payload_id"] for row in rows if row["source_unit_id"] not in local})
        accepted = set()
        for offset in range(0, len(candidate_ids), BATCH):
            accepted.update(row["payload_id"] for row in await _published(
                conn, store.bot_id, conversation, cut, candidate_ids[offset:offset + BATCH],
            ))
        for row in rows:
            if row["source_unit_id"] not in local and row["reply_payload_id"] not in accepted:
                continue
            if row["source_revision"] != 1 or row["source_unit_id"] != "source-unit:" + digest_json([
                    store.bot_id, "source-reply:" + row["reply_id"]]):
                _deny()
            local.add(row["source_unit_id"])
            # A local publication cannot hide the actual partial provenance
            # already known to its SourceUnit or canonical body.
            added = {row["payload_id"], row["reply_payload_id"]}
            budget["edges"].update({
                (row["from_payload_id"], row["payload_id"]),
                (row["payload_id"], row["reply_payload_id"]),
            })
            extra.update(added)
            pending.update(added - seen)
        if (len(extra) > MAX_NODES or len(budget["nodes"] | pending) > MAX_NODES
                or len(budget["edges"]) > MAX_EDGES):
            _deny()
    return local


async def authorize_legacy_history_in(conn, store, conversation, cut, payload_ids, *, run_id=None):
    """Return additional frozen-input parents and exact local Reply source IDs.

    Every legacy-carrying edge must terminate at a real local publication, an
    exact 031 row, or a proven frozen 040/041 component-configuration leaf.
    A cycle, unknown leaf or mixed unpublished cache denies.
    """
    marker = legacy_marker(store.bot_id)
    pending, checked, anchors, extra = set(payload_ids), {}, set(), set()
    seen = set()
    snapshot_cache = {}
    budget = {"bytes": 0, "private": None, "nodes": set(), "edges": set()}
    while pending:
        batch = sorted(pending)[:BATCH]
        pending.difference_update(batch)
        seen.update(batch)
        budget["nodes"].update(batch)
        if len(seen) > MAX_NODES:
            _deny()
        nodes = await (await conn.execute(
            "SELECT h.payload_id,h.state,EXISTS(SELECT 1 FROM public.bot_payload_bodies b "
            "WHERE b.bot_id=h.bot_id AND b.payload_id=h.payload_id) AS has_body "
            "FROM public.bot_payload_headers h WHERE h.bot_id=%s AND h.payload_id=ANY(%s) "
            "AND EXISTS(SELECT 1 FROM public.forest_payload_effective_sources s WHERE s.bot_id=h.bot_id "
            "AND s.payload_id=h.payload_id AND s.source_unit_id=%s)",
            (store.bot_id, batch, marker),
        )).fetchall()
        active = {row["payload_id"] for row in nodes}
        if any(row["state"] != "ACTIVE" or not row["has_body"] for row in nodes):
            _deny()
        if len(checked) + len(active) > MAX_NODES:
            _deny()
        for identity in active:
            checked[identity] = set()
        published = await _published(conn, store.bot_id, conversation, cut, list(active))
        for row in published:
            anchors.add(row["payload_id"])
        remaining = active - anchors
        from forest.storage.payload_control_history import old_config_leaves_in

        anchors.update(await old_config_leaves_in(conn, store.bot_id, remaining))
        from forest.storage.payload_agent_history import imported_specs_in, old_agent_leaves_in

        anchors.update(await old_agent_leaves_in(conn, store, conversation, cut, remaining, run_id))
        # Imported specs introduce their own unknown body, independent of any
        # shared configuration ancestor. Only their exact current-Run proof
        # can authorize that body; all-safe parents must not launder it.
        requires_own_proof = await imported_specs_in(conn, store.bot_id, remaining)
        old = await _old_run_roots(conn, store.bot_id, conversation, cut, list(remaining), run_id)
        for row in old:
            key = row["snapshot_payload_id"]
            if key not in snapshot_cache:
                snapshot_cache[key] = await _snapshot_inputs(conn, store.bot_id, conversation, cut, row, run_id, budget)
            parents = snapshot_cache[key]
            extra.update(parents)
            if len(extra) > MAX_NODES:
                _deny()
            checked[row["payload_id"]].update(parents)
            anchors.add(row["payload_id"])
        # 044 can add a spec prerequisite to a 031 root without proving the
        # root's own earlier snapshot/output/request. Failed cut or Run checks
        # must not be bypassed by the newly attached, independently safe parent.
        requires_own_proof.update(row["payload_id"] for row in await _old_run_roots(
            conn, store.bot_id, conversation, cut, list(remaining), run_id, scoped=False,
        ))
        if requires_own_proof - anchors:
            _deny()
        edges = await (await conn.execute(
            "SELECT DISTINCT d.payload_id,d.parent_payload_id FROM ("
            "SELECT bot_id,payload_id,parent_payload_id FROM public.bot_payload_derivations UNION "
            "SELECT bot_id,payload_id,parent_payload_id FROM public.bot_payload_legacy_closure) d "
            "JOIN public.forest_payload_effective_sources s ON s.bot_id=d.bot_id AND s.payload_id=d.parent_payload_id "
            "AND s.source_unit_id=%s WHERE d.bot_id=%s AND d.payload_id=ANY(%s) LIMIT %s",
            (marker, store.bot_id, list(remaining), MAX_EDGES - len(budget["edges"]) + 1),
        )).fetchall()
        for edge in edges:
            checked[edge["payload_id"]].add(edge["parent_payload_id"])
        budget["edges"].update((key, parent) for key in active for parent in checked[key])
        if len(budget["edges"]) > MAX_EDGES:
            _deny()
        for identity in active:
            pending.update(checked[identity] - seen)
        if len(pending) + len(seen) > MAX_NODES:
            _deny()
    if not checked:
        _deny()  # A dynamically linked marker is not a free visibility exception.
    # Non-legacy virtual input parents are checked by the ordinary source guard.
    unresolved = {key: (parents & checked.keys()) for key, parents in checked.items()}
    safe = {key for key, parents in unresolved.items() if not parents and key in anchors}
    reverse, counts, ready = defaultdict(set), {}, deque(safe)
    for key, parents in unresolved.items():
        counts[key] = len(parents)
        for parent in parents:
            reverse[parent].add(key)
    while ready:
        for child in reverse[ready.popleft()]:
            counts[child] -= 1
            if counts[child] == 0 and child not in safe:
                safe.add(child)
                ready.append(child)
    if safe != checked.keys():
        _deny()
    local = await _local_reply_sources(conn, store, conversation, cut, payload_ids, extra, budget)
    return extra, local
