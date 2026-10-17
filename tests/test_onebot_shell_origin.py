"""The package proves QQ origin after the kernel binds the raw Payload."""

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from psycopg import sql
from psycopg.types.json import Jsonb
from test_schema_bundles import clean_authority as clean_authority


@pytest.mark.postgres
async def test_shell_origin_checks_raw_qq_identity_in_same_transaction(clean_authority):
    store, _installer = clean_authority
    schema_name = "onebot_shell_origin_test"
    source = Path(__file__).resolve().parents[1] / (
        "src/forest/builtin/adapter_onebot/schema/adapter_onebot.sql"
    )
    baseline = source.read_text(encoding="utf-8").replace(
        "__forest_module_schema__", '"' + schema_name + '"'
    )
    event_ref = "onebot-event:" + "a" * 64
    conversation_ref = "onebot:group:933179609"
    now = datetime.now(timezone.utc)
    raw = {
        "post_type": "message", "message_type": "group",
        "self_id": 10000, "user_id": 42, "group_id": 933179609,
        "message_id": 7, "time": int(now.timestamp()),
        "sender": {"user_id": 42}, "message": "#forest core status",
    }
    gate = {
        "protocol": "forest.shell-ingress-proof/1",
        "event_ref": event_ref, "binding_ref": "binding/1", "binding_epoch": 1,
        "conversation_ref": conversation_ref, "principal_ref": "qq:42",
        "adapter_generation_id": "generation/adapter", "self_id": "10000",
    }
    envelope = {
        "protocol": "forest.uow-step/1",
        "caller": {"component_id": "adapter.onebot",
                   "generation_id": "generation/adapter"},
        "input": {"raw_frame": raw},
        "prior": {
            "adapter": {"event_id": event_ref, "binding_ref": "binding/1",
                        "binding_epoch": 1},
            "conversation": {"conversation_id": conversation_ref},
            "gate": gate,
        },
    }
    async with store.pool.connection() as conn:
        await conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema_name)))
        await conn.execute(sql.SQL("SET search_path TO {}").format(sql.Identifier(schema_name)))
        await conn.execute(baseline)
        publication = {
            "protocol": "forest.configuration-publication/1",
            "generation_id": "generation/adapter",
            "graph_id": "graph/adapter",
            "revision": 1,
            "payload_digest": "sha256:" + "b" * 64,
            "value": {
                "protocol": "forest.adapter.onebot/2",
                "bindings": [{
                    "binding_ref": "binding/1", "binding_epoch": 1,
                    "self_id": "10000", "enabled": True,
                }],
            },
        }
        published = await (await conn.execute(
            sql.SQL("SELECT {}.adapter_publish_configuration(%s,%s) AS result").format(
                sql.Identifier(schema_name)
            ),
            (Jsonb({"port_kind": "publication", "component_id": "adapter.onebot",
                    "generation_id": "generation/adapter"}), Jsonb(publication)),
        )).fetchone()
        assert published["result"]["ok"] is True
        await conn.execute(
            sql.SQL(
                "INSERT INTO {}.platform_events (event_id,generation_id,operation_id,binding_ref,"
                "binding_epoch,client_incarnation,platform_message_ref,event_kind,"
                "channel_kind,channel_ref,conversation_ref,author_subject_ref,"
                "raw_payload,normalized_payload,author_payload,received_at,"
                "classification_deadline,sent_at,input_payload) VALUES "
                "(%s,'generation/adapter','operation/1','binding/1',1,'client/1','7','group.message',"
                "'GROUP','933179609',%s,'qq:42',%s,%s,%s,%s,%s,%s,%s)"
            ).format(sql.Identifier(schema_name)),
            (event_ref, conversation_ref, Jsonb({"payload_id": "raw/1"}),
             Jsonb({"payload_id": "normalized/1"}), Jsonb({"payload_id": "author/1"}),
             now, now + timedelta(minutes=2), now, Jsonb({"operation_id": "operation/1"})),
        )

        async def prove(candidate):
            row = await (await conn.execute(
                sql.SQL("SELECT {}.adapter_prove_shell_origin(%s,%s) AS result").format(
                    sql.Identifier(schema_name)
                ),
                (Jsonb({"generation_id": "generation/adapter"}), Jsonb(candidate)),
            )).fetchone()
            return row["result"]

        accepted = await prove(envelope)
        assert accepted["ok"] is True
        assert accepted["value"]["principal_ref"] == "qq:42"
        stale_event = {
            "event_id": "onebot-event:" + "b" * 64,
            "platform_message_ref": "8", "event_kind": "group.message",
            "channel_kind": "GROUP", "channel_ref": "933179609",
            "conversation_ref": conversation_ref, "author_subject_ref": "qq:42",
            "raw": {}, "normalized": {}, "author": {},
            "received_at": now.isoformat(),
            "classification_deadline": (now + timedelta(minutes=2)).isoformat(),
            "sent_at": now.isoformat(),
        }
        stale_ingress = {
            "protocol": "forest.uow-step/1",
            "caller": {"component_id": "adapter.onebot",
                       "generation_id": "generation/adapter"},
            "input": {
                "operation_id": "operation/2", "binding_ref": "binding/1",
                "binding_epoch": 2, "client_incarnation": "client/1",
                "event": stale_event,
            },
        }
        stale_result = await (await conn.execute(
            sql.SQL("SELECT {}.adapter_record_ingress(%s,%s) AS result").format(
                sql.Identifier(schema_name)
            ),
            (Jsonb({"generation_id": "generation/adapter"}), Jsonb(stale_ingress)),
        )).fetchone()
        assert stale_result["result"]["error"]["code"] == "ONEBOT_BINDING_NOT_CURRENT"
        forged_binding = deepcopy(envelope)
        forged_binding["input"]["raw_frame"]["self_id"] = 99999
        forged_binding["prior"]["gate"]["self_id"] = "99999"
        assert (await prove(forged_binding))["error"]["code"] == "ONEBOT_SHELL_ORIGIN_INVALID"
        for path, forged_value in (
            (("input", "raw_frame", "user_id"), 43),
            (("input", "raw_frame", "self_id"), 99999),
            (("input", "raw_frame", "group_id"), 12345),
            (("input", "raw_frame", "message_id"), 8),
            (("input", "raw_frame", "sender"), {"user_id": 43}),
        ):
            forged = deepcopy(envelope)
            cursor = forged
            for key in path[:-1]:
                cursor = cursor[key]
            cursor[path[-1]] = forged_value
            rejected = await prove(forged)
            assert rejected["ok"] is False
            assert rejected["error"]["code"] == "ONEBOT_SHELL_ORIGIN_INVALID"

        # Execution can occur after a hot replacement. The event stays owned by
        # its admission generation; only the current public Binding is re-proved.
        replacement = deepcopy(publication)
        replacement["generation_id"] = "generation/adapter/new"
        replacement["graph_id"] = "graph/adapter/new"
        replacement["revision"] = 2
        published = await (await conn.execute(
            sql.SQL("SELECT {}.adapter_publish_configuration(%s,%s) AS result").format(
                sql.Identifier(schema_name)
            ),
            (Jsonb({"port_kind": "publication", "component_id": "adapter.onebot",
                    "generation_id": replacement["generation_id"]}), Jsonb(replacement)),
        )).fetchone()
        assert published["result"]["ok"] is True

        binding_check = {
            "protocol": "forest.uow-step/1",
            "caller": {"component_id": "kernel.runtime", "generation_id": "kernel/1"},
            "input": {
                "protocol": "forest.core-rescue-binding-check/1",
                "event_ref": event_ref, "binding_ref": "binding/1",
                "binding_epoch": 1, "self_id": "10000",
                "adapter_generation_id": "generation/adapter",
                "conversation_ref": conversation_ref,
                "group_ref": "933179609", "principal_ref": "qq:42",
                "client_incarnation": "client/1",
            },
            "prior": {},
        }

        async def prove_current(candidate, generation="generation/adapter/new"):
            row = await (await conn.execute(
                sql.SQL("SELECT {}.adapter_prove_rescue_binding(%s,%s) AS result").format(
                    sql.Identifier(schema_name)
                ),
                (Jsonb({"port_kind": "participant", "component_id": "adapter.onebot",
                        "generation_id": generation}), Jsonb(candidate)),
            )).fetchone()
            return row["result"]

        current = await prove_current(binding_check)
        assert current["ok"] is True
        assert current["value"]["adapter_generation_id"] == "generation/adapter/new"
        assert current["value"]["source_adapter_generation_id"] == "generation/adapter"
        assert current["value"]["client_incarnation"] == "client/1"
        assert current["value"]["adapter_config_revision"] == 2
        for field, forged_value in (
            ("event_ref", "missing"), ("binding_ref", "binding/other"),
            ("binding_epoch", 2), ("self_id", "99999"),
            ("adapter_generation_id", "generation/forged"),
            ("conversation_ref", "onebot:group:123"),
            ("group_ref", "123"), ("principal_ref", "qq:43"),
            ("client_incarnation", "client/other"),
        ):
            forged = deepcopy(binding_check)
            forged["input"][field] = forged_value
            assert (await prove_current(forged))["error"]["code"] == (
                "ONEBOT_RESCUE_BINDING_STALE"
            )
        forged_caller = deepcopy(binding_check)
        forged_caller["caller"]["component_id"] = "shell.command"
        assert (await prove_current(forged_caller))["error"]["code"] == (
            "ONEBOT_RESCUE_PROOF_INVALID"
        )

        disabled = deepcopy(replacement)
        disabled["generation_id"] = "generation/adapter/disabled"
        disabled["graph_id"] = "graph/adapter/disabled"
        disabled["revision"] = 3
        disabled["value"]["bindings"][0]["enabled"] = False
        await conn.execute(
            sql.SQL("SELECT {}.adapter_publish_configuration(%s,%s)").format(
                sql.Identifier(schema_name)
            ),
            (Jsonb({"port_kind": "publication", "component_id": "adapter.onebot",
                    "generation_id": disabled["generation_id"]}), Jsonb(disabled)),
        )
        assert (await prove_current(binding_check, disabled["generation_id"]))["error"][
            "code"
        ] == "ONEBOT_RESCUE_BINDING_STALE"
