"""Conversation must not spin on repeatedly failing owned Agent work."""

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
async def test_agent_work_failure_backoff_is_durable_and_replay_safe(clean_authority, tmp_path):
    store, installer = clean_authority
    kernel = await bootstrap_kernel(store, installer)
    source = Path(str(files("forest").joinpath("builtin/conversation_scheduler")))
    bootstrap = json.loads((source / "bootstrap.json").read_text(encoding="utf-8"))
    package = await bootstrap_builtin_package(
        source, PackageCatalog(tmp_path / "conversation-packages"), store, installer,
        kernel=kernel, initial_configuration=bootstrap, resolved_contracts={},
    )
    namespace = package.descriptor.storage.spec.namespace
    conversation_id = "conversation/backoff"

    async def call(function: str, body: dict):
        async with store.pool.connection() as conn, conn.transaction():
            row = await (
                await conn.execute(
                    sql.SQL("SELECT {}.{}(%s::jsonb,%s::jsonb) AS result").format(
                        sql.Identifier(namespace), sql.Identifier(function)
                    ),
                    (Jsonb({"bot_id": store.bot_id}), Jsonb(body)),
                )
            ).fetchone()
        return row["result"]

    async def lane_state():
        async with store.pool.connection() as conn:
            row = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT agent_work_failure_streak,resume_reason_code,"
                        "EXTRACT(EPOCH FROM (resume_not_before-clock_timestamp())) AS delay "
                        "FROM {}.conversation_lanes WHERE bot_id=%s AND conversation_id=%s"
                    ).format(sql.Identifier(namespace)),
                    (store.bot_id, conversation_id),
                )
            ).fetchone()
        return row

    async def set_due():
        async with store.pool.connection() as conn, conn.transaction():
            await conn.execute(
                sql.SQL(
                    "UPDATE {}.conversation_lanes SET resume_not_before="
                    "clock_timestamp()-interval '1 second' "
                    "WHERE bot_id=%s AND conversation_id=%s"
                ).format(sql.Identifier(namespace)),
                (store.bot_id, conversation_id),
            )

    async def claim(index: int):
        result = await call("conversation_claim_agent_run", {
            "protocol": "forest.owned-work-claim-request/1",
            "claim_token": f"backoff-owner/{index}", "lease_seconds": 60,
        })
        assert result["ok"] is True
        return result["value"]

    async def settle(claimed: dict, status: str, *, disposition: str = "DEFER"):
        outcome = (
            {"status": "FAILED", "error": {"code": "TEST_WORK_FAILURE"}}
            if status == "FAILED" else {
                "status": "SUCCEEDED",
                "result": {
                    "protocol": "forest.conversation-agent-work/1",
                    "disposition": disposition,
                    "delay_seconds": 1,
                    "reason_code": "TEST_DEFER",
                },
            }
        )
        request = {
            "protocol": "forest.owned-work-settle-request/1",
            "work_id": claimed["work_id"],
            "claim_token": claimed["claim_token"],
            "attempt": claimed["attempt"],
            "outcome": outcome,
        }
        result = await call("conversation_settle_agent_run", request)
        assert result["ok"] is True
        assert await call("conversation_settle_agent_run", request) == result
        return result

    async with store.pool.connection() as conn, conn.transaction():
        await conn.execute(
            sql.SQL(
                "INSERT INTO {}.conversation_lanes("
                "bot_id,conversation_id,conversation_key_digest,source_component_id,"
                "binding_ref,platform,channel_kind,channel_ref,active_session_ref,active_run_ref) "
                "VALUES (%s,%s,%s,'adapter.onebot','binding/one','onebot-v11',"
                "'GROUP','933179609','session/one','run/one')"
            ).format(sql.Identifier(namespace)),
            (store.bot_id, conversation_id, _digest(b"backoff")),
        )

    first = await claim(1)
    await settle(first, "FAILED")
    state = await lane_state()
    assert state["agent_work_failure_streak"] == 1
    assert state["resume_reason_code"] == "AGENT_WORK_FAILED"
    assert 4 <= state["delay"] <= 5
    assert await claim(2) is None

    await set_due()
    second = await claim(2)
    await settle(second, "FAILED")
    state = await lane_state()
    assert state["agent_work_failure_streak"] == 2
    assert 9 <= state["delay"] <= 10
    assert await claim(3) is None

    await set_due()
    third = await claim(3)
    await settle(third, "SUCCEEDED")
    assert (await lane_state())["agent_work_failure_streak"] == 0
    await set_due()
    fourth = await claim(4)
    await settle(fourth, "FAILED")
    assert (await lane_state())["agent_work_failure_streak"] == 1

    # A failure before Session creation must also delay this lane. The next
    # claim must keep the streak even though its selected Session is new.
    async with store.pool.connection() as conn, conn.transaction():
        await conn.execute(
            sql.SQL(
                "UPDATE {}.conversation_lanes SET active_session_ref=NULL,active_run_ref=NULL,"
                "resume_not_before=NULL,resume_reason_code=NULL,resume_reason_ref=NULL,"
                "agent_work_failure_streak=0,next_ingress_seq=2,classified_through_seq=1 "
                "WHERE bot_id=%s AND conversation_id=%s"
            ).format(sql.Identifier(namespace)),
            (store.bot_id, conversation_id),
        )
        await conn.execute(
            sql.SQL(
                "INSERT INTO {}.inbox_messages("
                "bot_id,conversation_id,event_id,ingress_seq,admission_operation_id,"
                "input_digest,source_component_id,source_event_ref,event_kind,"
                "author_subject_ref,content_payload_id,content_payload_digest,"
                "content_byte_length,content_owner_component_id,content_owner_kind,"
                "content_owner_id,author_payload_id,author_payload_digest,"
                "author_owner_component_id,author_owner_kind,author_owner_id,"
                "context_disposition,received_at,classification_deadline) VALUES ("
                "%s,%s,'event/one',1,'admit/one',%s,'adapter.onebot','event/one',"
                "'message','user/one','payload/content',%s,1,'adapter.onebot',"
                "'event.content','event/one','payload/author',%s,'adapter.onebot',"
                "'event.author','event/one','MATERIALIZE',clock_timestamp(),"
                "clock_timestamp()+interval '1 hour')"
            ).format(sql.Identifier(namespace)),
            (store.bot_id, conversation_id, _digest(b"input"),
             _digest(b"content"), _digest(b"author")),
        )
        await conn.execute(
            sql.SQL(
                "INSERT INTO {}.conversation_triggers("
                "bot_id,conversation_id,trigger_id,event_id,ingress_seq,principal_ref,"
                "input_digest) VALUES (%s,%s,'trigger/one','event/one',1,'user/one',%s)"
            ).format(sql.Identifier(namespace)),
            (store.bot_id, conversation_id, _digest(b"trigger")),
        )

    fifth = await claim(5)
    assert fifth["payload"]["resume"] is False
    await settle(fifth, "FAILED")
    assert (await lane_state())["agent_work_failure_streak"] == 1
    assert await claim(6) is None
    await set_due()
    sixth = await claim(6)
    assert sixth["payload"]["resume"] is False
    await settle(sixth, "FAILED")
    state = await lane_state()
    assert state["agent_work_failure_streak"] == 2
    assert 9 <= state["delay"] <= 10
