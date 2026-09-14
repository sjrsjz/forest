"""Independent SQL audit of original facts and exact bytes after body migration."""

import hashlib
import json

from psycopg import sql

_FIELDS = {
    "bot_calls": {"result": "result_payload_id"},
    "bot_agent_runs": {"spec": "spec_payload_id"},
    "bot_agent_steps": {"snapshot": "snapshot_payload_id", "output": "output_payload_id", "closed_parts": "closed_parts_payload_id"},
    "bot_model_request_manifests": {"manifest": "manifest_payload_id"},
    "bot_model_transport_attempts": {"request_body": "request_payload_id", "result": "result_payload_id"},
    "bot_agent_tools": {"arguments": "arguments_payload_id", "result": "result_payload_id"},
    "bot_agent_tool_observations": {"observation": "observation_payload_id"},
    "bot_inbound_media_refs": {"source": "source_payload_id"},
    "bot_media_captures": {"content": "payload_id", "source_manifest": "manifest_payload_id"},
    "bot_conversation_replies": {"parts": "payload_id"},
    "bot_agent_children": {"snapshot": "snapshot_payload_id", "spec": "spec_payload_id", "result": "result_payload_id"},
    "bot_child_model_attempts": {"request_body": "request_payload_id", "manifest": "manifest_payload_id", "result": "result_payload_id"},
    "bot_send_parts": {"payload": "payload_id"},
    "bot_send_attempts": {"wire_body": "wire_payload_id"},
}


_CONFIG_FAMILIES = {
    "bot_config_plans": ("config", "PLAN", "component_id"),
    "bot_config_revisions": ("config", "REVISION", "component_id"),
    "bot_agent_config_plans": ("agent.config", "PLAN", None),
    "bot_agent_config_revisions": ("agent.config", "REVISION", None),
    "bot_memory_config_plans": ("memory.config", "PLAN", None),
    "bot_memory_config_revisions": ("memory.config", "REVISION", None),
    "bot_binding_plans": ("binding", "PLAN", "binding_id"),
    "bot_binding_revisions": ("binding", "REVISION", "binding_id"),
    "bot_job_plans": ("job", "PLAN", "job_id"),
    "bot_job_revisions": ("job", "REVISION", "job_id"),
}


def digest(value):
    body = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()
    return "sha256:" + hashlib.sha256(body).hexdigest()


async def configuration_identity(conn, row, reference, expected_digest, owner):
    """Independent SQL identity check, without the production values reader."""
    header = await (await conn.execute(
        "SELECT owner_kind,owner_id,encoding,digest FROM public.bot_payload_headers "
        "WHERE bot_id=%s AND payload_id=%s", (row["bot_id"], reference),
    )).fetchone()
    assert header == {"owner_kind": "control.config", "owner_id": owner,
                      "encoding": "json", "digest": expected_digest}
    assert reference == "payload:" + digest([row["bot_id"], "control.config", owner])


async def content(conn, bot_id, payload_id):
    row = await (await conn.execute(
        "SELECT h.*,b.content FROM public.bot_payload_headers h JOIN public.bot_payload_bodies b "
        "USING(bot_id,payload_id) WHERE h.bot_id=%s AND h.payload_id=%s", (bot_id, payload_id),
    )).fetchone()
    assert row and row["state"] == "ACTIVE"
    body = bytes(row["content"])
    assert row["byte_length"] == len(body)
    assert row["digest"] == "sha256:" + hashlib.sha256(body).hexdigest()
    return json.loads(body) if row["encoding"] == "json" else body


def encoded(value):
    def default(item):
        return bytes(item).hex() if isinstance(item, (bytes, memoryview)) else str(item)

    return json.dumps(value, sort_keys=True, ensure_ascii=False, default=default)


async def assert_preserved(conn, before, *, append_only=()):
    """Check every old field, with bodies independently loaded from the new ID.

Only explicitly listed tables may gain rows. Existing rows remain exact except
the monotonic memory clock, whose independently allocated fact counter may grow.
"""
    for table, originals in before.items():
        actual = await (await conn.execute(sql.SQL("SELECT * FROM public.{}").format(
            sql.Identifier(table),
        ))).fetchall()
        if not originals:
            if table not in append_only:
                assert not actual, table
            continue
        keys = set(originals[0])
        restored = []
        for row in actual:
            value = dict(row)
            for field, reference in _FIELDS.get(table, {}).items():
                if reference in row:
                    assert row[field] is None, (table, field)
                    value[field] = await content(conn, row["bot_id"], row[reference]) if row[reference] else None
            if table == "bot_agent_runs" and "spec_payload_id" in row:
                assert row["spec_payload_id"] and row["spec_payload_digest"] == row["spec_digest"] == digest(value["spec"])
                await configuration_identity(conn, row, row["spec_payload_id"], row["spec_payload_digest"],
                    "agent-run-spec-v1:" + digest([row["run_id"]]))
            if table in _CONFIG_FAMILIES and "values_payload_id" in row:
                assert row["values"] is None and row["values_payload_id"]
                family, kind, resource_key = _CONFIG_FAMILIES[table]
                resource = row[resource_key] if resource_key else ""
                identity = str(row["plan_id"] if kind == "PLAN" else row["revision"])
                body = await content(conn, row["bot_id"], row["values_payload_id"])
                assert set(body) == {"family", "kind", "resource", "identity", "values"}
                assert {key: body[key] for key in ("family", "kind", "resource", "identity")} == {
                    "family": family, "kind": kind, "resource": resource, "identity": identity}
                assert row["values_payload_digest"] == digest(body)
                await configuration_identity(conn, row, row["values_payload_id"], row["values_payload_digest"],
                    "config-values-v1:" + digest([family, kind, resource, identity]))
                value["values"] = body["values"]
            if table == "bot_job_revisions" and "delegation_payload_id" in row:
                assert row["delegation"] is None and row["delegation_payload_id"]
                body = await content(conn, row["bot_id"], row["delegation_payload_id"])
                assert set(body) == {"family", "kind", "resource", "identity", "values"}
                assert {key: body[key] for key in ("family", "kind", "resource", "identity")} == {
                    "family": "job.delegation", "kind": "REVISION",
                    "resource": row["job_id"], "identity": str(row["revision"]),
                }
                assert row["delegation_payload_digest"] == digest(body)
                await configuration_identity(
                    conn,
                    row,
                    row["delegation_payload_id"],
                    row["delegation_payload_digest"],
                    "config-values-v1:" + digest(
                        ["job.delegation", "REVISION", row["job_id"], str(row["revision"])]
                    ),
                )
                value["delegation"] = body["values"]
            if table == "bot_message_inbox" and "payload_id" in row:
                assert row["segments"] is None
                body = await content(conn, row["bot_id"], row["payload_id"])
                value.update({"author": body["author"], "segments": body["segments"]})
            if table == "bot_context_units" and "payload_id" in row:
                assert row["control_parts"] is None and row["step_parts"] is None
                if row["kind"] in {"CONTROL", "RUN_STEP"}:
                    value["control_parts" if row["kind"] == "CONTROL" else "step_parts"] = await content(
                        conn, row["bot_id"], row["payload_id"],
                    )
            if table in {"bot_send_parts", "bot_send_attempts"} and "receipt_payload_id" in row:
                receipt = await content(conn, row["bot_id"], row["receipt_payload_id"]) if row["receipt_payload_id"] else None
                assert row["receipt"] is None or set(row["receipt"]) <= {"message_id", "status", "retcode", "error_code"}
                value["receipt"] = receipt
            if table == "bot_delivery_resolutions" and "payload_id" in row:
                assert row["reason"] is None and row["result"] is None
                body = await content(conn, row["bot_id"], row["payload_id"])
                value.update({"reason": body["reason"], "result": body["result"]})
            if table == "bot_web_operations" and "result_payload_id" in row:
                assert row["result"] is None and row["result_payload_id"]
                body = await content(conn, row["bot_id"], row["result_payload_id"])
                assert set(body) == {"action", "operation_id", "value"}
                assert body["action"] == row["action"]
                assert body["operation_id"] == row["operation_id"]
                header = await (await conn.execute(
                    "SELECT digest FROM public.bot_payload_headers WHERE bot_id=%s AND payload_id=%s",
                    (row["bot_id"], row["result_payload_id"]),
                )).fetchone()
                assert header["digest"] == row["result_payload_digest"]
                value["result"] = body["value"]
            restored.append({key: value[key] for key in keys})
        if table == "bot_memory_clock":
            assert len(restored) == len(originals) == 1
            assert restored[0]["fact_revision"] >= originals[0]["fact_revision"]
            restored[0]["fact_revision"] = originals[0]["fact_revision"]
        expected, values = sorted(map(encoded, originals)), sorted(map(encoded, restored))
        if table in append_only:
            assert set(expected).issubset(values), table
        else:
            assert values == expected, table
