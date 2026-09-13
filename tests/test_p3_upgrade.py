"""P2 authorities with actual 001-012 data upgraded through the current P3 schema."""

from __future__ import annotations

import base64
import hashlib
from datetime import UTC, datetime, timedelta
from importlib.resources import files
from uuid import uuid4

import psycopg
import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from test_upgrade import upgrade_database as upgrade_database

from forest.errors import ForestError
from forest.models import ModelCapabilities, prepare_model_request
from forest.packages import digest_json
from forest.storage.agent_config import AgentConfig
from forest.storage.bot import BotStore
from forest.storage.control import BotControl
from forest.storage.messages import MessageStore

pytestmark = pytest.mark.postgres
P2_ACTIONS = ["config.read", "config.history", "config.plan", "config.apply", "service.invoke",
              "binding.create", "binding.read", "binding.plan", "binding.apply", "message.read",
              "message.cancel", "delivery.reconcile"]


async def p2_authority(dsn):
    private = Ed25519PrivateKey.generate()
    public = base64.b64encode(private.public_key().public_bytes_raw()).decode()
    async with await psycopg.AsyncConnection.connect(dsn, autocommit=True, row_factory=dict_row) as conn:
        async with conn.transaction():
            await conn.execute("CREATE TABLE public.forest_bot_schema_migrations(version text PRIMARY KEY,checksum text NOT NULL,applied_at timestamptz NOT NULL DEFAULT clock_timestamp())")
            migrations = sorted(files("forest.storage").joinpath("bot_migrations").iterdir(), key=lambda p: p.name)
            selected = [path for path in migrations if path.name.endswith(".sql") and int(path.name[:3]) <= 12]
            assert len(selected) == 12
            for path in selected:
                source = path.read_text(encoding="utf-8")
                await conn.execute(source)
                await conn.execute("INSERT INTO public.forest_bot_schema_migrations(version,checksum) VALUES (%s,%s)",
                                   (path.name, hashlib.sha256(source.encode()).hexdigest()))
            await conn.execute("INSERT INTO public.forest_bot_identity(bot_id) VALUES ('bot-a')")
            await conn.execute("INSERT INTO public.bot_runtime_state(bot_id) VALUES ('bot-a')")
            await conn.execute("INSERT INTO public.bot_host_lease(bot_id) VALUES ('bot-a')")
            await conn.execute("INSERT INTO public.bot_web_issuers(bot_id,issuer_id,key_revision,public_key,actions) VALUES ('bot-a','old-issuer',1,%s,%s)",
                               (public, Jsonb(P2_ACTIONS)))
            await conn.execute("INSERT INTO public.bot_bindings(bot_id,binding_id,platform,self_id,display_name,registry_operation_id,enabled,receive_enabled,state) VALUES ('bot-a','binding','onebot-v11','1000','Bot','register',true,true,'ONLINE')")
            await conn.execute(
                "INSERT INTO public.bot_conversations(bot_id,conversation_id,binding_id,platform,channel_kind,channel_id,"
                "group_source_id,next_ingress_seq,classified_through_seq,context_applied_seq,context_revision,context_view_revision,"
                "mailbox_revision,next_response_seq,active_session_id) "
                "VALUES ('bot-a','conversation','binding','onebot-v11','group','group-a','trunk-a',4,3,3,3,1,3,2,'open-session')"
            )
            for number in (1, 2, 3):
                parts = ([{"type": "image", "media_ref": "old-media", "fetch_status": "PENDING_FETCH"}] if number == 3 else
                         [{"type": "text", "text": "old message " + str(number)}])
                await conn.execute(
                    "INSERT INTO public.bot_message_inbox(bot_id,conversation_id,event_id,ingress_seq,platform_message_id,source_event_id,"
                    "input_digest,author,segments,sent_at,trigger_decision,estimated_tokens) "
                    "VALUES ('bot-a','conversation',%s,%s,%s,%s,%s,%s,%s,clock_timestamp(),%s,20)",
                    (f"event-{number}", number, str(number), f"source-event-{number}", f"input-{number}",
                     Jsonb({"principal_ref": f"principal-{number}", "external_user_id": str(number), "display_name": "同名用户",
                            "is_self": False, "identity_origin": "adapter_verified"}), Jsonb(parts),
                     "NO_TRIGGER" if number == 1 else "TRIGGER"),
                )
            await conn.execute(
                "INSERT INTO public.bot_inbound_media_refs(bot_id,media_ref,event_id,part_index,source) "
                "VALUES ('bot-a','old-media','event-3',0,%s)",
                (Jsonb({"kind": "onebot-source", "uri": "https://private.invalid/image?token=old-signed-token", "platform_file": "old-file"}),),
            )
            for state in ("old", "open"):
                await conn.execute(
                    "INSERT INTO public.bot_conversation_sessions(bot_id,session_id,conversation_id,status,active_run_id) "
                    "VALUES ('bot-a',%s,'conversation',%s,%s)",
                    (state + "-session", "CLOSED" if state == "old" else "RUNNING", state + "-run"),
                )
                await conn.execute(
                    "INSERT INTO public.bot_conversation_runs(bot_id,run_id,session_id,conversation_id,status,input_revision,selected_input_end_seq) "
                    "VALUES ('bot-a',%s,%s,'conversation',%s,1,%s)",
                    (state + "-run", state + "-session", "COMPLETED" if state == "old" else "RUNNING", 2 if state == "old" else 3),
                )
                index = 2 if state == "old" else 3
                await conn.execute(
                    "INSERT INTO public.bot_context_input_batches(bot_id,input_batch_id,operation_id,conversation_id,session_id,run_id,kind,"
                    "start_seq,end_seq,trigger_cut_seq,selected_input_end_seq,trigger_ids,context_revision,input_revision) "
                    "VALUES ('bot-a',%s,%s,'conversation',%s,%s,'TRIGGER_INPUT',%s,%s,%s,%s,%s,%s,1)",
                    (state + "-batch", state + "-selection", state + "-session", state + "-run", 1 if state == "old" else 3,
                     index, index, index, Jsonb(["trigger-" + str(index)]), 1 if state == "old" else 3),
                )
                await conn.execute(
                    "INSERT INTO public.bot_conversation_triggers(bot_id,trigger_id,conversation_id,event_id,ingress_seq,principal_ref,status,"
                    "claimed_session_id,claimed_run_id,input_batch_id,answered_reply_id) "
                    "VALUES ('bot-a',%s,'conversation',%s,%s,%s,%s,%s,%s,%s,%s)",
                    (f"trigger-{index}", f"event-{index}", index, f"principal-{index}", "ANSWERED" if state == "old" else "CLAIMED",
                     state + "-session", state + "-run", state + "-batch", "old-reply" if state == "old" else None),
                )
            await conn.execute(
                "INSERT INTO public.bot_conversation_replies(bot_id,reply_id,operation_id,conversation_id,run_id,input_revision,response_seq,"
                "parts,input_digest,delivery) VALUES ('bot-a','old-reply','old-reply-op','conversation','old-run',1,1,%s,'reply-input',%s)",
                (Jsonb([[{"type": "text", "text": "old answer"}]]), Jsonb({"status": "DELIVERED", "intent_id": "old-intent"})),
            )
            for number in (1, 2, 3):
                await conn.execute(
                    "INSERT INTO public.bot_context_units(bot_id,conversation_id,unit_id,context_revision,unit_index,kind,event_id,input_batch_id,visible,estimated_tokens) "
                    "VALUES ('bot-a','conversation',%s,%s,%s,'INPUT',%s,%s,%s,20)",
                    (f"event-{number}", 3 if number == 3 else 1, number, f"event-{number}",
                     "open-batch" if number == 3 else "old-batch", number != 1),
                )
            await conn.execute("INSERT INTO public.bot_context_units(bot_id,conversation_id,unit_id,context_revision,unit_index,kind,reply_id,estimated_tokens) VALUES ('bot-a','conversation','old-reply',2,0,'REPLY','old-reply',20)")
            await conn.execute(
                "INSERT INTO public.bot_context_compactions(bot_id,conversation_id,view_revision,context_revision,policy_revision,token_budget,"
                "archive_through_seq,source_manifest_digest,removed_unit_ids,retained_unit_ids) "
                "VALUES ('bot-a','conversation',1,3,'old-policy',512,1,'old-source-cut','[\"event-1\"]','[\"event-2\",\"old-reply\",\"event-3\"]')"
            )
            for component in ("counter", "adapter.onebot"):
                await conn.execute(
                    "INSERT INTO public.bot_generations(bot_id,generation_id,component_id,package_digest,config_digest,config,instance_id,"
                    "worker_incarnation,host_incarnation,host_owner_epoch,status) "
                    "VALUES ('bot-a',%s,%s,'old-package','old-config','{}',%s,%s,'old-host',1,'RETIRED')",
                    (component + "-generation", component, component + "-instance", component + "-worker"),
                )
            for call_id, component, parent, root, depth, lane, service in (
                ("old-root", "counter", None, "old-root", 0, "DATA", None),
                ("old-child", "counter", "old-root", "old-root", 1, "DATA", None),
                ("old-control", "adapter.onebot", None, "old-control", 0, "CONTROL", "onebot.normalize"),
            ):
                await conn.execute(
                    "INSERT INTO public.bot_calls(bot_id,call_id,operation_id,component_id,generation_id,route_epoch,worker_incarnation,"
                    "host_id,host_incarnation,owner_epoch,storage_generation,maintenance_epoch,policy_revision,deadline_at,input_digest,"
                    "status,result,parent_call_id,root_call_id,call_depth,admission_lane,control_service) "
                    "VALUES ('bot-a',%s,%s,%s,%s,1,%s,'old-host','old-host',1,1,1,1,clock_timestamp(),'old-input','COMPLETED',%s,%s,%s,%s,%s,%s)",
                    (call_id, call_id + "-operation", component, component + "-generation", component + "-worker",
                     Jsonb({"value": call_id}), parent, root, depth, lane, service),
                )
    return private, public


async def attach(store):
    await store.open()
    await store.migrate()
    lease = await store.acquire_host("new-host", "new-incarnation", ttl_seconds=120)
    async with store.pool.connection() as conn:
        await conn.execute("UPDATE public.bot_bindings SET host_incarnation=%s,host_owner_epoch=%s WHERE bot_id='bot-a' AND binding_id='binding'",
                           (lease.incarnation, lease.owner_epoch))
    messages = MessageStore(store)
    actor = await messages.acquire_actor(lease, "conversation", "new-actor", ttl_seconds=120)
    return lease, messages, actor


async def test_upgrade_keeps_old_input_reply_archive_and_legacy_run_readable(upgrade_database):
    await p2_authority(upgrade_database)
    store = BotStore(upgrade_database, "bot-a")
    try:
        _, messages, actor = await attach(store)
        context = await messages.context(actor, run_id="open-run")
        assert [unit["unit_id"] for unit in context["units"]] == ["event-2", "old-reply", "event-3"]
        assert context["units"][1]["reply_parts"] == [[{"type": "text", "text": "old answer"}]]
        assert context["archive"]["count"] == 1 and [u["unit_id"] for u in context["archive"]["units"]] == ["event-1"]
        assert context["archive"]["source"]["group_source_id"] == "trunk-a"
        assert [trigger["trigger_id"] for trigger in context["triggers"]] == ["trigger-3"]
        units = [{"unit_id": u["unit_id"], "role": u["context_role"], "source": u["source"], "author": u["author"],
                  "parts": u["segments"] if u["kind"] == "INPUT" else [p for chunk in u["reply_parts"] for p in chunk]}
                 for u in context["units"]]
        snapshot = {"bot_id": "bot-a", "run_id": "open-run", "logical_step_id": "upgrade-test", "input_revision": 1,
                    "context_revision": 3, "context_view_revision": 1, "selected_input_end_seq": 3,
                    "trigger_ids": ["trigger-3"], "input_batch_ids": ["open-batch"], "units": units}
        prepared = prepare_model_request(snapshot, ModelCapabilities("deployment", "model", "1"),
                                         request_id="request", transport_attempt_id="attempt")
        assert b"old answer" in prepared.body and b"old-signed-token" not in prepared.body
        assert prepared.manifest["omitted_part_refs"][0]["reason"] == "MODEL_IMAGE_UNSUPPORTED"
        async with store.pool.connection() as conn:
            with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState):
                await conn.execute("UPDATE public.bot_context_units SET event_id='changed' WHERE unit_id='event-2'")
            await conn.execute("UPDATE public.bot_context_units SET visible=false WHERE unit_id='event-2'")
        assert (await messages.context(actor, run_id="open-run"))["archive"]["count"] == 2
        ended = await messages.end_run(actor, run_id="open-run", reason="upgrade recovery", status="CANCELLED")
        assert ended["status"] == "CANCELLED"
        await store.migrate()  # Repeated upgrade is a checksum-verified no-op.
    finally:
        await store.close()


async def test_upgrade_preserves_old_call_trees_control_lane_and_pending_media_sources(upgrade_database):
    await p2_authority(upgrade_database)
    store = BotStore(upgrade_database, "bot-a")
    try:
        await attach(store)
        async with store.pool.connection() as conn:
            calls = await (await conn.execute("SELECT * FROM public.bot_calls ORDER BY call_id")).fetchall()
            assert len(calls) == 3 and all(row["agent_tool_id"] is None for row in calls)
            by_id = {row["call_id"]: row for row in calls}
            assert (by_id["old-child"]["parent_call_id"], by_id["old-child"]["root_call_id"], by_id["old-child"]["call_depth"]) == ("old-root", "old-root", 1)
            assert by_id["old-control"]["admission_lane"] == "CONTROL"
            assert by_id["old-control"]["control_service"] == "onebot.normalize"
            assert [BotStore._grant_value(row).agent_tool_id for row in calls] == [None, None, None]
            assert all(row["result"] == {"value": row["call_id"]} for row in calls)
            row = await (await conn.execute("SELECT * FROM public.bot_inbound_media_refs WHERE media_ref='old-media'")).fetchone()
            assert row["status"] == "PENDING_FETCH" and row["capture_epoch"] == 0
            assert all(row[key] is None for key in ("capture_id", "capture_expires_at", "retry_after", "error_code"))
            assert row["source"]["uri"].endswith("token=old-signed-token")
            with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState):
                await conn.execute("UPDATE public.bot_inbound_media_refs SET source='{}' WHERE media_ref='old-media'")
            await conn.execute("UPDATE public.bot_inbound_media_refs SET status='FETCH_FAILED',capture_epoch=1,error_code='TRANSPORT' WHERE media_ref='old-media'")
            assert (await (await conn.execute("SELECT count(*) AS n FROM public.bot_media_captures")).fetchone())["n"] == 0
            assert (await (await conn.execute("SELECT count(*) AS n FROM public.bot_agent_runs")).fetchone())["n"] == 0
    finally:
        await store.close()


@pytest.mark.parametrize("scope", ["full", "readonly", "empty", "disabled"])
async def test_upgrade_keeps_preexisting_web_issuer_usable_for_agent_configuration(upgrade_database, scope):
    private, public = await p2_authority(upgrade_database)
    previous_actions = ["config.read"] if scope == "readonly" else [] if scope == "empty" else P2_ACTIONS
    async with await psycopg.AsyncConnection.connect(upgrade_database, autocommit=True) as conn:
        await conn.execute("UPDATE public.bot_web_issuers SET actions=%s,enabled=%s WHERE issuer_id='old-issuer'",
                           (Jsonb(previous_actions), scope != "disabled"))
    store = BotStore(upgrade_database, "bot-a")
    try:
        lease, _, _ = await attach(store)
        await BotControl(store).trust(lease, issuer_id="old-issuer", key_revision=1, public_key=public)
        async with store.pool.connection() as conn:
            issuer = await (await conn.execute("SELECT * FROM public.bot_web_issuers WHERE issuer_id='old-issuer'")).fetchone()
        assert issuer["public_key"] == public and issuer["key_revision"] == 1
        assert issuer["enabled"] is (scope != "disabled")
        if scope == "readonly":
            assert set(issuer["actions"]) == {"config.read", "config.history", "agent.config.read", "memory.config.read"}
        elif scope == "empty":
            assert issuer["actions"] == []
        else:
            assert set(previous_actions).issubset(issuer["actions"])
            assert {"agent.config.read", "agent.config.plan", "agent.config.apply", "agent.read", "agent.cancel",
                    "agent.approval", "agent.resolve"}.issubset(issuer["actions"])
        # The current target also applies 023's independent permission mapping;
        # an old config reader gains neither memory contents nor write actions.
        for permission, required in (("memory.config.read", "config.read"), ("memory.config.plan", "config.plan"),
                                     ("memory.config.apply", "config.apply"), ("memory.read", "message.read")):
            assert (permission in issuer["actions"]) == (required in previous_actions)
        now = datetime.now(UTC)
        claims = {"ticket_id": str(uuid4()), "issuer_id": "old-issuer", "subject_id": "old-admin", "session_ref": "session",
                  "bot_id": "bot-a", "operation_id": "upgrade-agent-read", "action": "agent.config.read", "input_digest": digest_json({}),
                  "plan_digest": "", "issued_at": now.isoformat(), "expires_at": (now + timedelta(seconds=20)).isoformat(),
                  "key_revision": 1, "account_revision": 1, "session_revision": 1, "policy_revision": 1}
        ticket = {"claims": claims, "signature": base64.b64encode(private.sign(digest_json(claims).encode("ascii"))).decode()}
        if scope in {"empty", "disabled"}:
            with pytest.raises(ForestError) as caught:
                await AgentConfig(store).read(ticket, "upgrade-agent-read")
            assert caught.value.code == "WEB_AUTHORITY_REJECTED"
        else:
            result = await AgentConfig(store).read(ticket, "upgrade-agent-read")
            assert result["revision"] == 0 and not result["values"]["enabled"] and not result["has_api_key"]
    finally:
        await store.close()
