"""Scale check for the Agent package's Session and Run management read model."""

from __future__ import annotations

import tomllib
from pathlib import Path
from time import perf_counter

import pytest
from psycopg import sql
from psycopg.types.json import Jsonb
from test_schema_bundles import clean_authority as clean_authority

from forest.core_runtime import bootstrap_kernel
from forest.schema_bundles import StorageBundleSpec


@pytest.mark.postgres
async def test_agent_management_pages_at_100k_sessions_and_runs(clean_authority):
    store, installer = clean_authority
    await bootstrap_kernel(store, installer)
    source = Path(__file__).parents[1] / "src/forest/builtin/agent_runtime"
    manifest = tomllib.loads((source / "module.toml").read_text(encoding="utf-8"))
    storage = manifest["components"][0]["storage"]
    spec = StorageBundleSpec(
        component_id="agent.runtime", schema_id=storage["schema_id"],
        schema_version=storage["schema_version"],
        baseline_sha256=storage["baseline_sha256"],
        repository_contract=storage["repository_contract"],
    )
    await installer.install(
        spec, (source / storage["baseline"]).read_bytes(),
        "sha256:" + "b" * 64, execution_profile="official",
    )
    namespace = sql.Identifier(spec.namespace)
    digest = "sha256:" + "a" * 64
    async with store.pool.connection() as conn:
        await conn.execute(sql.SQL("""
            INSERT INTO {}.agent_sessions (
                bot_id,session_id,conversation_ref,start_operation_id,input_digest,
                state,closed_at,created_at,updated_at
            )
            SELECT %s,'session/'||lpad(n::text,6,'0'),'group/'||(n%%4),
                   'session-start/'||n,%s,'CLOSED',clock_timestamp(),
                   statement_timestamp()-make_interval(secs=>100001-n),
                   statement_timestamp()-make_interval(secs=>100001-n)
            FROM generate_series(1,100000) n
        """).format(namespace), (store.bot_id, digest))
        await conn.execute(sql.SQL("""
            INSERT INTO {}.agent_runs (
                bot_id,run_id,session_id,conversation_ref,start_operation_id,
                input_digest,state,agent_spec_revision,config_payload_id,
                config_payload_digest,config_owner_component_id,config_owner_kind,
                config_owner_id,preset_revision,preset_payload_id,preset_payload_digest,
                preset_owner_component_id,preset_owner_kind,preset_owner_id,
                core_protocol_id,core_protocol_version,core_prompt_digest,
                parser_revision,absolute_deadline,input_revision,selected_input_end_seq,
                context_revision,context_view_revision,created_at,updated_at,terminal_at
            )
            SELECT %s,'run/'||lpad(n::text,6,'0'),
                   'session/'||lpad(n::text,6,'0'),'group/'||(n%%4),
                   'run-start/'||n,%s,'COMPLETED','spec/1','config/1',%s,
                   'agent.runtime','module.configuration','config/1',
                   'preset/1','preset/1',%s,'agent.runtime','preset.context',
                   'preset/1','xml',1,%s,'parser/1',statement_timestamp(),
                   1,1,1,0,
                   statement_timestamp()-make_interval(secs=>100001-n),
                   statement_timestamp()-make_interval(secs=>100001-n),
                   clock_timestamp()
            FROM generate_series(1,100000) n
        """).format(namespace), (store.bot_id, digest, digest, digest, digest))
        await conn.commit()
        for relation in ("agent_sessions", "agent_runs"):
            await conn.execute(sql.SQL("ANALYZE {}.{}").format(
                namespace, sql.Identifier(relation)
            ))

        async def page(function_name, values):
            started = perf_counter()
            result = await (await conn.execute(sql.SQL(
                "SELECT {}.{}(%s::jsonb,%s::jsonb) AS result"
            ).format(namespace, sql.Identifier(function_name)), (
                Jsonb({"bot_id": store.bot_id}), Jsonb(values),
            ))).fetchone()
            assert result["result"]["ok"]
            print(f"{function_name} {values}: {(perf_counter()-started)*1000:.3f} ms")
            return result["result"]["value"]

        sessions = await page("agent_management_sessions_page", {
            "conversation_ref": "group/0", "state": "CLOSED", "limit": 50,
        })
        assert len(sessions["items"]) == 50 and sessions["has_more"]
        assert sessions["items"][0]["session_id"] == "session/100000"
        assert sessions["items"][-1]["session_id"] == "session/099804"
        next_sessions = await page("agent_management_sessions_page", {
            "conversation_ref": "group/0", "state": "CLOSED", "limit": 50,
            "snapshot_at": sessions["snapshot_at"],
            "cursor_filter_digest": sessions["filter_digest"],
            "after_created_at": sessions["next_cursor"]["created_at"],
            "after_session_id": sessions["next_cursor"]["session_id"],
        })
        assert next_sessions["items"][0]["session_id"] == "session/099800"
        deep_session = await (await conn.execute(sql.SQL(
            "SELECT created_at FROM {}.agent_sessions WHERE bot_id=%s AND session_id=%s"
        ).format(namespace), (store.bot_id, "session/001000"))).fetchone()
        deep_sessions = await page("agent_management_sessions_page", {
            "conversation_ref": "group/0", "state": "CLOSED", "limit": 50,
            "snapshot_at": sessions["snapshot_at"],
            "cursor_filter_digest": sessions["filter_digest"],
            "after_created_at": deep_session["created_at"].isoformat(),
            "after_session_id": "session/001000",
        })
        assert deep_sessions["items"][0]["session_id"] == "session/000996"

        runs = await page("agent_management_runs_page", {
            "conversation_ref": "group/0", "state": "COMPLETED", "limit": 50,
        })
        assert len(runs["items"]) == 50 and runs["has_more"]
        assert runs["items"][0]["run_id"] == "run/100000"
        deep_run = await (await conn.execute(sql.SQL(
            "SELECT created_at FROM {}.agent_runs WHERE bot_id=%s AND run_id=%s"
        ).format(namespace), (store.bot_id, "run/001000"))).fetchone()
        deep_runs = await page("agent_management_runs_page", {
            "conversation_ref": "group/0", "state": "COMPLETED", "limit": 50,
            "snapshot_at": runs["snapshot_at"],
            "cursor_filter_digest": runs["filter_digest"],
            "after_created_at": deep_run["created_at"].isoformat(),
            "after_run_id": "run/001000",
        })
        assert deep_runs["items"][0]["run_id"] == "run/000996"
        exact = await page("agent_management_runs_page", {
            "session_id": "session/001000", "limit": 50,
        })
        assert [item["run_id"] for item in exact["items"]] == ["run/001000"]

        for relation, index_name, where_clause in (
            ("agent_sessions", "agent_session_management_",
             "bot_id=%s AND conversation_ref='group/0' AND state='CLOSED'"),
            ("agent_runs", "agent_run_management_",
             "bot_id=%s AND conversation_ref='group/0' AND state='COMPLETED'"),
            ("agent_runs", "agent_run_management_session_idx",
             "bot_id=%s AND session_id='session/001000'"),
        ):
            plan = await (await conn.execute(sql.SQL(
                "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) "
                "SELECT 1 FROM {}.{} WHERE " + where_clause +
                " ORDER BY created_at DESC,{} DESC LIMIT 50"
            ).format(namespace, sql.Identifier(relation),
                     sql.Identifier("session_id" if relation == "agent_sessions" else "run_id")),
                (store.bot_id,),
            )).fetchone()
            fact = plan["QUERY PLAN"][0]
            index_scan = fact["Plan"]["Plans"][0]
            assert index_scan["Node Type"] in ("Index Scan", "Index Only Scan"), fact
            assert index_name in index_scan["Index Name"], fact
            assert fact["Plan"]["Shared Hit Blocks"] < 500, fact
            assert fact["Plan"]["Temp Read Blocks"] == 0, fact
            assert fact["Plan"]["Temp Written Blocks"] == 0, fact
            print(
                f"{relation} {where_clause}: {index_scan['Index Name']}, "
                f"{fact['Execution Time']:.3f} ms, "
                f"{fact['Plan']['Shared Hit Blocks']} shared hits"
            )
