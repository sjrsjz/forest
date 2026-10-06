"""PostgreSQL must publish one immutable model-view plan per Run step."""

from __future__ import annotations

import hashlib
import json
from importlib.resources import files
from pathlib import Path

import pytest
from psycopg import sql
from psycopg.types.json import Jsonb
from test_schema_bundles import clean_authority as clean_authority

from forest.builtin_packages import bootstrap_builtin_package
from forest.core_runtime import bootstrap_kernel
from forest.packages import PackageCatalog


def _digest(value: bytes) -> str:
    return "sha256:" + hashlib.sha256(value).hexdigest()


@pytest.mark.postgres
async def test_model_view_plan_publication_fences_and_replays(clean_authority, tmp_path):
    store, installer = clean_authority
    kernel = await bootstrap_kernel(store, installer)
    source = Path(str(files("forest").joinpath("builtin/conversation_scheduler")))
    bootstrap = json.loads((source / "bootstrap.json").read_text(encoding="utf-8"))
    package = await bootstrap_builtin_package(
        source,
        PackageCatalog(tmp_path / "conversation-packages"),
        store,
        installer,
        kernel=kernel,
        initial_configuration=bootstrap,
        resolved_contracts={},
    )
    namespace = package.descriptor.storage.spec.namespace
    profile = bootstrap["context_view"]
    profile_digest = _digest(
        json.dumps(profile, sort_keys=True, separators=(",", ":")).encode()
    )
    source_digest = _digest(b"")
    actor = {"conversation_id": "conversation/view-plan", "owner_id": "actor/one", "epoch": 1}
    async with store.pool.connection() as conn, conn.transaction():
        await conn.execute(
            sql.SQL(
                "INSERT INTO {}.conversation_lanes("
                "bot_id,conversation_id,conversation_key_digest,source_component_id,"
                "binding_ref,platform,channel_kind,channel_ref,actor_owner_id,actor_epoch,"
                "actor_expires_at,active_session_ref,active_run_ref) VALUES "
                "(%s,%s,%s,'adapter.onebot','binding/one','onebot-v11','GROUP','933179609',"
                "%s,1,clock_timestamp()+interval '5 minutes','session/one','run/one')"
            ).format(sql.Identifier(namespace)),
            (store.bot_id, actor["conversation_id"], _digest(b"view-plan"), actor["owner_id"]),
        )
        await conn.execute(
            sql.SQL(
                "INSERT INTO {}.context_run_view_profiles("
                "bot_id,conversation_id,session_ref,run_ref,profile,profile_digest) "
                "VALUES (%s,%s,'session/one','run/one',%s::jsonb,%s)"
            ).format(sql.Identifier(namespace)),
            (store.bot_id, actor["conversation_id"], Jsonb(profile), profile_digest),
        )

    def request(
        step: int, operation: str, prior: str | None = None,
        *, revision: int = 0, source: str = source_digest, bitmap: str = "",
    ):
        candidate_key = hashlib.sha256(operation.encode()).hexdigest()
        owner_id = f"model-view:run/one:step:{step}:{candidate_key}"
        value = {
            "actor": actor,
            "session_ref": "session/one",
            "run_ref": "run/one",
            "step_seq": step,
            "context_revision": revision,
            "context_view_revision": 0,
            "media_cost_revision": 0,
            "context_applied_seq": 0,
            "profile_digest": profile_digest,
            "source_set_digest": source,
            "selected_bitmap": bitmap,
            "prior_plan_digest": prior,
            "quote_owner_id": owner_id,
            "plan_owner_id": owner_id,
            "quote": {
                "payload_id": f"quote/{step}", "digest": _digest(f"quote/{step}".encode()),
                "owner_component_id": "agent.runtime", "owner_kind": "model.view.quote",
                "owner_id": owner_id,
            },
            "plan": {
                "payload_id": f"plan/{step}", "digest": _digest(f"plan/{step}".encode()),
                "owner_component_id": "conversation.scheduler", "owner_kind": "model.view.plan",
                "owner_id": owner_id,
            },
        }
        context = {
            "bot_id": store.bot_id,
            "component_id": "conversation.scheduler",
            "port_name": "conversation.model-view.publish",
            "port_kind": "command",
            "operation_id": operation,
            "input_digest": _digest(repr(value).encode()),
        }
        return context, value

    async def publish(context, value):
        async with store.pool.connection() as conn, conn.transaction():
            row = await (
                await conn.execute(
                    sql.SQL("SELECT {}.conversation_publish_model_view_plan("
                            "%s::jsonb,%s::jsonb) AS result").format(sql.Identifier(namespace)),
                    (Jsonb(context), Jsonb(value)),
                )
            ).fetchone()
        return row["result"]

    first_context, first = request(1, "view-plan/one")
    published = await publish(first_context, first)
    assert published["ok"] is True
    assert published["value"]["plan"] == first["plan"]
    assert await publish(first_context, first) == published

    second_context, second = request(2, "view-plan/two", first["plan"]["digest"])
    assert (await publish(second_context, second))["ok"] is True
    bad_parent_context, bad_parent = request(3, "view-plan/bad-parent", _digest(b"other"))
    assert (await publish(bad_parent_context, bad_parent))["error"]["code"] == (
        "CONTEXT_MODEL_VIEW_PLAN_PARENT_CHANGED"
    )
    wrong_step_context, wrong_step = request(2, "view-plan/wrong-step", first["plan"]["digest"])
    assert (await publish(wrong_step_context, wrong_step))["error"]["code"] == (
        "CONTEXT_MODEL_VIEW_PLAN_STEP_CONFLICT"
    )
    stale_context, stale = request(3, "view-plan/stale", second["plan"]["digest"])
    stale["actor"] = {**actor, "epoch": 2}
    assert (await publish(stale_context, stale))["error"]["code"] == (
        "CONTEXT_MODEL_VIEW_PLAN_FENCE_CHANGED"
    )

    async with store.pool.connection() as conn, conn.transaction():
        for index, unit_id in enumerate(("unit/one", "unit/two")):
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}.context_units("
                    "bot_id,conversation_id,unit_id,context_revision,unit_index,kind,"
                    "source_component_id,source_kind,source_id,payload_id,payload_digest,"
                    "payload_byte_length,payload_owner_component_id,payload_owner_kind,"
                    "payload_owner_id) VALUES (%s,%s,%s,1,%s,'CONTROL',"
                    "'conversation.scheduler','test.control',%s,%s,%s,1,"
                    "'conversation.scheduler','test.control',%s)"
                ).format(sql.Identifier(namespace)),
                (store.bot_id, actor["conversation_id"], unit_id, index, unit_id,
                 f"payload/{unit_id}", _digest(unit_id.encode()), unit_id),
            )
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}.active_context_units("
                    "bot_id,conversation_id,unit_id,context_revision,unit_index) "
                    "VALUES (%s,%s,%s,1,%s)"
                ).format(sql.Identifier(namespace)),
                (store.bot_id, actor["conversation_id"], unit_id, index),
            )
        await conn.execute(
            sql.SQL(
                "UPDATE {}.conversation_lanes SET context_revision=1,"
                "context_view_unit_count=2,context_view_payload_bytes=2 "
                "WHERE bot_id=%s AND conversation_id=%s"
            ).format(sql.Identifier(namespace)),
            (store.bot_id, actor["conversation_id"]),
        )

    two_sources = _digest(b"unit/one\x1funit/two")
    third_context, third = request(
        3, "view-plan/three", second["plan"]["digest"],
        revision=1, source=two_sources, bitmap="10",
    )
    assert (await publish(third_context, third))["ok"] is True
    reselect_context, reselect = request(
        4, "view-plan/reselect", third["plan"]["digest"],
        revision=1, source=two_sources, bitmap="11",
    )
    assert (await publish(reselect_context, reselect))["error"]["code"] == (
        "CONTEXT_MODEL_VIEW_PLAN_RESELECTED"
    )
    fourth_context, fourth = request(
        4, "view-plan/four", third["plan"]["digest"],
        revision=1, source=two_sources, bitmap="10",
    )
    assert (await publish(fourth_context, fourth))["ok"] is True
