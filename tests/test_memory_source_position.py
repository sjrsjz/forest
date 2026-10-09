from pathlib import Path

import pytest
from psycopg import sql
from psycopg.types.json import Jsonb
from test_memory_graph_package import _configuration
from test_schema_bundles import clean_authority as clean_authority

from forest.builtin_packages import bootstrap_builtin_package
from forest.core_runtime import bootstrap_kernel
from forest.packages import PackageCatalog, digest_json


@pytest.mark.postgres
async def test_onebot_source_position_is_fixed_at_admission(clean_authority, tmp_path):
    store, installer = clean_authority
    kernel = await bootstrap_kernel(store, installer)
    package = await bootstrap_builtin_package(
        Path(__file__).parents[1] / "src/forest/builtin/memory_graph",
        PackageCatalog(tmp_path / "position-packages"), store, installer,
        kernel=kernel, initial_configuration=_configuration(), resolved_contracts={},
    )
    namespace = sql.Identifier(package.descriptor.storage.spec.namespace)
    identity = {
        "source_id": "onebot-source:group-42", "source_kind": "GROUP",
        "identity_incarnation": 1, "platform": "onebot-v11",
        "realm_id": "qq:bot-1", "external_group_id": "42",
        "trunk_id": "onebot-trunk:group-42",
    }
    digest = "sha256:" + "a" * 64

    async def call(conn, function, operation, body, caller, prior):
        row = await (await conn.execute(
            sql.SQL("SELECT {}.{}(%s::jsonb,%s::jsonb) AS result").format(
                namespace, sql.Identifier(function)),
            (Jsonb({"bot_id": store.bot_id, "operation_id": operation,
                    "participant_input_digest": digest_json(body)}),
             Jsonb({"protocol": "forest.uow-step/1",
                    "caller": {"component_id": caller}, "input": body, "prior": prior})),
        )).fetchone()
        return row["result"]

    async def reserve(conn, index, event=None):
        event = event or f"event/{index}"
        body = {
            "identity": identity, "source_unit_id": f"unit/{index}",
            "conversation_ref": f"conversation/{index}", "event_ref": event,
            "conversation_step_id": "conversation",
        }
        prior = {"conversation": {
            "conversation_id": body["conversation_ref"],
            "event_id": event, "ingress_seq": 1,
        }}
        return await call(conn, "memory_reserve_source_position",
                          f"reserve/{index}", body, "adapter.onebot", prior)

    async def capture(conn, index):
        event = f"event/{index}"
        body = {
            "conversation_step_id": None, "deletion_epoch_step_id": "epoch",
            "capture": {
                "identity": identity, "source_unit_id": f"unit/{index}",
                "source_revision": 1, "unit_kind": "GROUP_MESSAGE",
                "author_subject_ref": "qq:user-1",
                "author": {"payload_id": f"author/{index}", "digest": digest,
                           "owner_component_id": "adapter.onebot",
                           "owner_kind": "onebot.author-snapshot", "owner_id": event},
                "parts": [{"part_id": "part:text:0", "part_index": 0,
                           "part_kind": "TEXT", "media_type": "text/plain",
                           "payload": {"payload_id": f"text/{index}", "digest": digest,
                                       "owner_component_id": "adapter.onebot",
                                       "owner_kind": "onebot.memory-text", "owner_id": event},
                           "width": None, "height": None, "byte_length": 1}],
                "occurrence": {"occurrence_id": f"occurrence/{index}",
                               "binding_ref": "binding/one", "conversation_ref": f"conversation/{index}",
                               "event_ref": event, "ingress_seq": 1,
                               "observed_at": "2026-10-09T00:00:00+00:00"},
                "occurred_at": "2026-10-09T00:00:00+00:00",
            },
        }
        return await call(conn, "memory_capture_source", f"capture/{index}",
                          body, "memory.graph", {"epoch": {
                              "protocol": "forest.payload-epoch/1", "deletion_epoch": 0}})

    async with store.pool.connection() as conn, conn.transaction():
        first = await reserve(conn, 1)
        second = await reserve(conn, 2)
        assert first["value"]["source_order_seq"] == 1
        assert second["value"]["source_order_seq"] == 2
        missing = await capture(conn, 3)
        assert missing["error"]["code"] == "MEMORY_POSITION_MISSING"
        assert (await capture(conn, 2))["ok"] is True
        assert (await capture(conn, 1))["ok"] is True
        replay = await reserve(conn, 1)
        assert replay == first
        conflict = await reserve(conn, 1, event="event/changed")
        assert conflict["error"]["code"] == "MEMORY_POSITION_CONFLICT"
        rows = await (await conn.execute(sql.SQL(
            "SELECT source_unit_id,source_order_seq FROM {}.source_units "
            "WHERE bot_id=%s ORDER BY source_order_seq"
        ).format(namespace), (store.bot_id,))).fetchall()
        assert [(r["source_unit_id"], r["source_order_seq"]) for r in rows] == [
            ("unit/1", 1), ("unit/2", 2),
        ]
