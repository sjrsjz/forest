"""Immutable value identity proofs independent of mixed preview receipt bodies."""

from forest.errors import ForestError
from forest.packages import digest_json
from forest.storage.config_payloads import ConfigPayloadStore

FAMILIES = {
    "config": ("bot_config", "component_id"),
    "agent.config": ("bot_agent_config", None),
    "memory.config": ("bot_memory_config", None),
    "binding": ("bot_binding", "binding_id"),
    "job": ("bot_job", "job_id"),
    "command.policy": ("bot_command_policy", "resource_id"),
    "command.limit": ("bot_command_limit", None),
}


def _conflict():
    return ForestError("CONTROL_SOURCE_CONFLICT", "Configuration differs from its immutable source identity", 409)


async def _actual(conn, store, family, kind, row, resource):
    if family not in FAMILIES or kind not in {"PLAN", "REVISION"}:
        raise _conflict()
    table, field = FAMILIES[family]
    if field and resource and resource != row.get(field):
        raise _conflict()
    resource = row.get(field, resource) if field else ""
    key, suffix = ("plan_id", "plans") if kind == "PLAN" else ("revision", "revisions")
    predicate, params = "", [store.bot_id, row.get(key)]
    if field:
        predicate, params = " AND " + field + "=%s", [*params, resource]
    actual = await (await conn.execute(
        f"SELECT * FROM public.{table}_{suffix} WHERE bot_id=%s AND {key}=%s" + predicate, params,
    )).fetchone()
    required = (key, "values_payload_id", "values_payload_digest", "plan_id")
    if actual is None or any(actual.get(k) != row.get(k) for k in required):
        raise _conflict()
    if field and actual[field] != row.get(field):
        raise _conflict()
    owner = ConfigPayloadStore.owner_id(family=family, kind=kind, resource=resource, identity=str(actual[key]))
    header = await (await conn.execute(
        "SELECT * FROM public.bot_payload_headers WHERE bot_id=%s AND payload_id=%s",
        (store.bot_id, actual["values_payload_id"]),
    )).fetchone()
    if (header is None or (header["owner_kind"], header["owner_id"], header["encoding"], header["digest"]) !=
            ("control.config", owner, "json", actual["values_payload_digest"])
            or header["payload_id"] != "payload:" + digest_json([store.bot_id, "control.config", owner])):
        raise _conflict()
    return actual, resource


async def _edge(conn, store, child, parent):
    return bool(await (await conn.execute(
        "SELECT 1 FROM public.bot_payload_derivations WHERE bot_id=%s AND payload_id=%s AND parent_payload_id=%s",
        (store.bot_id, child, parent),
    )).fetchone())


async def _operation(conn, store, operation_id, family, kind):
    operation = await (await conn.execute(
        "SELECT * FROM public.bot_web_operations WHERE bot_id=%s AND operation_id=%s",
        (store.bot_id, operation_id),
    )).fetchone()
    if operation is None or operation["action"] != family + "." + kind:
        raise _conflict()
    return operation


async def plan_values_proof_in(conn, store, family, plan, *, resource=""):
    actual, resource = await _actual(conn, store, family, "PLAN", plan, resource)
    bridge = await (await conn.execute(
        "SELECT * FROM public.bot_control_value_sources WHERE bot_id=%s AND family=%s AND source_kind='PLAN' AND source_id=%s",
        (store.bot_id, family, actual["plan_id"]),
    )).fetchone()
    if bridge is None:
        raise _conflict()
    value_id = actual["values_payload_id"]
    if bridge["legacy_untracked"]:
        if bridge["operation_id"] is not None or not await _edge(conn, store, value_id, bridge["payload_id"]):
            raise _conflict()
    else:
        operation = await _operation(conn, store, bridge["operation_id"], family, "plan")
        if (operation["result_payload_id"], operation["result_payload_digest"], operation["subject_id"], operation["issuer_id"]) != (
                bridge["payload_id"], bridge["payload_digest"], actual["subject_id"], actual["issuer_id"]):
            raise _conflict()
        # Frozen old values inherited their already recorded receipt. New full
        # values precede the preview and inherit only their admitted input.
        imported = await _edge(conn, store, value_id, bridge["payload_id"])
        modern = (await _edge(conn, store, value_id, operation["input_payload_id"])
                  and await _edge(conn, store, bridge["payload_id"], value_id))
        if not (imported or modern):
            raise _conflict()
    return value_id


async def revision_values_proof_in(conn, store, family, revision, *, resource=""):
    actual, resource = await _actual(conn, store, family, "REVISION", revision, resource)
    value_id = actual["values_payload_id"]
    operation = await (await conn.execute(
        "SELECT * FROM public.bot_web_operations WHERE bot_id=%s AND operation_id=%s",
        (store.bot_id, actual["operation_id"]),
    )).fetchone()
    if operation is not None and operation["action"] == family + ".apply":
        if await _edge(conn, store, value_id, operation["result_payload_id"]):
            return value_id  # Immutable imported row; no receipt body replay.
        table, _ = FAMILIES[family]
        plan = await (await conn.execute(
            f"SELECT * FROM public.{table}_plans WHERE bot_id=%s AND plan_id=%s",
            (store.bot_id, actual["plan_id"]),
        )).fetchone()
        if plan is None or (operation["subject_id"], operation["issuer_id"]) != (plan["subject_id"], plan["issuer_id"]):
            raise _conflict()
        plan_id = await plan_values_proof_in(conn, store, family, plan, resource=resource)
        if (not await _edge(conn, store, value_id, plan_id)
                or not await _edge(conn, store, value_id, operation["input_payload_id"])
                or not await _edge(conn, store, operation["result_payload_id"], value_id)):
            raise _conflict()
    else:
        bridge = await (await conn.execute(
            "SELECT * FROM public.bot_control_value_sources WHERE bot_id=%s AND family=%s AND source_kind='REVISION' AND source_id=%s",
            (store.bot_id, family, digest_json([resource, actual["revision"]])),
        )).fetchone()
        if bridge is None or not await _edge(conn, store, value_id, bridge["payload_id"]):
            raise _conflict()
    return value_id
