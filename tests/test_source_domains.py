"""Independent Host input domains do not become searchable chat sources."""

import psycopg
import pytest
from psycopg.types.json import Jsonb
from test_agent_store import prepare, respond, start
from test_memory_pipeline_upgrade import old_schema
from test_memory_sources import drain, rows, source_read
from test_messages import messages as messages
from test_messages import receive
from test_upgrade import upgrade_database as upgrade_database

from forest.errors import ForestError
from forest.memory_types import SourceRef
from forest.packages import digest_json
from forest.storage.bot import BotStore
from forest.storage.delivery import DeliveryStore
from forest.storage.memory_sources import MemorySourceStore
from forest.storage.payloads import PayloadStore
from forest.storage.source_domains import SourceDomainStore

pytestmark = pytest.mark.postgres


async def domain_payload(repository, lease, *, owner="service-one", kind="SERVICE_INPUT"):
    payloads = PayloadStore(repository.store)
    async with repository.store.pool.connection() as conn, conn.transaction():
        await repository.store._host(conn, lease)
        clock = await payloads.lock_in(conn)
        source = await SourceDomainStore(repository.store).source_in(conn, kind, owner)
        ref = await payloads.put_json_in(
            conn,
            owner_kind="control.config",
            owner_id=owner,
            value={"template": "CONFIG-CANARY"},
            sources=(source,),
            expected_deletion_epoch=clock["deletion_epoch"],
        )
        return source, ref


async def test_domain_registration_is_stable_immutable_and_never_a_recall_candidate(messages):
    repository, lease, _ = messages
    sources = MemorySourceStore(repository)
    source, payload = await domain_payload(repository, lease)
    assert await domain_payload(repository, lease) == (source, payload)
    chat = await receive(repository, lease, 1)
    await drain(repository, lease, chat)
    assert await source_read(sources, lease, chat, 1, source_unit_ids=[source.source_unit_id]) == []
    assert len(await source_read(sources, lease, chat, 1)) == 1
    assert len(await rows(repository, "SELECT * FROM public.bot_payload_domains")) == 1
    for sql in (
        "UPDATE public.bot_payload_domains SET owner_id='other'",
        "DELETE FROM public.bot_payload_domains",
    ):
        with pytest.raises(psycopg.Error):
            async with repository.store.pool.connection() as conn:
                await conn.execute(sql)
    await sources.payloads.forget_sources(
        lease, "forget-domain", (source,), expected_deletion_epoch=0
    )
    async with repository.store.pool.connection() as conn, conn.transaction():
        await repository.store._host(conn, lease)
        await sources.payloads.lock_in(conn)
        assert (
            await SourceDomainStore(repository.store).source_in(
                conn, "SERVICE_INPUT", "service-one"
            )
            == source
        )
        with pytest.raises(ForestError) as exc:
            await sources.payloads.put_json_in(
                conn,
                owner_kind="kernel.result",
                owner_id="cannot-revive",
                value="copied",
                sources=(source,),
                expected_deletion_epoch=1,
            )
        assert exc.value.code == "SOURCE_SUPPRESSED"


@pytest.mark.parametrize("dependency", ["registered", "unknown", "wrong_revision", "wrong_part"])
async def test_formal_reply_recognizes_only_registered_exact_domain_dependencies(
    messages, monkeypatch, dependency
):
    repository, lease, _ = messages
    source, config = await domain_payload(repository, lease)
    if dependency != "registered":
        claimed = {
            "unknown": SourceRef("domain:sha256:" + "f" * 64),
            "wrong_revision": SourceRef(source.source_unit_id, 2),
            "wrong_part": SourceRef(source.source_unit_id, part_id="invented"),
        }[dependency]
        config = await PayloadStore(repository.store).put_json(
            lease,
            owner_kind="control.config",
            owner_id="unproven",
            value="unproven template",
            sources=(claimed,),
            expected_deletion_epoch=0,
        )
    state, actor, head = await start(messages)
    original = state.payloads.step_parents_in

    async def with_host_domain(*args, **kwargs):
        # Trusted Host provenance for an independently registered template.
        # The actual Agent request/output/Reply repositories still construct
        # every payload and its complete ancestry under their real guards.
        return (*await original(*args, **kwargs), config.payload_id)

    monkeypatch.setattr(state.payloads, "step_parents_in", with_host_domain)
    _, request, permit = await prepare(state, actor, head)
    await respond(state, actor, permit, request, text="template-derived formal reply")
    if dependency != "registered":
        # The final Agent scope fence now rejects unregistered domain claims
        # before publishing any Reply, instead of creating an incomplete source.
        with pytest.raises(ForestError) as exc:
            await state.commit_final(
                actor, head["run_id"], "final", DeliveryStore(repository.store)
            )
        assert exc.value.code == "AGENT_MEMORY_REVOKED"
        assert not await rows(repository, "SELECT 1 FROM public.bot_conversation_replies")
        assert not await rows(repository, "SELECT 1 FROM public.bot_send_intents")
        assert not await rows(
            repository, "SELECT 1 FROM public.bot_memory_source_units WHERE kind='REPLY'"
        )
        return
    reply = await state.commit_final(
        actor, head["run_id"], "final", DeliveryStore(repository.store)
    )
    unit = (
        await rows(
            repository,
            "SELECT * FROM public.bot_memory_source_units WHERE source_event_id=%s",
            (MemorySourceStore.reply_event_id(reply["reply_id"]),),
        )
    )[0]
    assert unit["lineage_complete"] is (dependency == "registered")
    sources = MemorySourceStore(repository)
    receipt = {"conversation_id": actor.conversation_id}
    selected = await source_read(
        sources, lease, receipt, 1, source_unit_ids=[unit["source_unit_id"]]
    )
    assert bool(selected) is (dependency == "registered")
    if selected:
        assert selected[0]["kind"] == "REPLY" and selected[0]["source_kind"] == "GROUP"
        await sources.payloads.forget_sources(
            lease, "erase-template", (source,), expected_deletion_epoch=0
        )
        assert (
            await source_read(sources, lease, receipt, 1, source_unit_ids=[unit["source_unit_id"]])
            == []
        )


@pytest.mark.parametrize("preempt_configuration", [False, True])
async def test_real_old_generation_configuration_moves_once_and_keeps_original_identity(
    upgrade_database, preempt_configuration
):
    from legacy_022 import FrozenPayloadStore

    await old_schema(upgrade_database)
    store = BotStore(upgrade_database, "bot-a")
    await store.open()
    try:
        lease = await store.acquire_host("old-host", "old-incarnation", ttl_seconds=120)
        config = {"template": "OLD-CONFIG-CANARY", "image": {"uri": "https://private.invalid/a"}}
        schema = {"type": "object", "description": "OLD-SCHEMA-CANARY"}
        config_digest = digest_json(config)
        expected_domain = SourceRef(
            "domain:" + digest_json([store.bot_id, "GENERATION_CONFIG", "old-generation"])
        )
        async with store.pool.connection() as conn, conn.transaction():
            await conn.execute(
                "INSERT INTO public.bot_component_resources(bot_id,component_id) VALUES ('bot-a','counter')"
            )
            await conn.execute(
                "INSERT INTO public.bot_component_routes(bot_id,component_id) VALUES ('bot-a','counter')"
            )
            await conn.execute(
                "INSERT INTO public.bot_component_state_heads(bot_id,component_id) VALUES ('bot-a','counter')"
            )
            await conn.execute(
                "INSERT INTO public.bot_generations(bot_id,generation_id,component_id,package_digest,config_digest,config,"
                "state_value_schema,instance_id,worker_incarnation,host_incarnation,host_owner_epoch,status) "
                "VALUES ('bot-a','old-generation','counter','old-package',%s,%s,%s,'old-instance','old-worker',%s,%s,'ACTIVE')",
                (config_digest, Jsonb(config), Jsonb(schema), lease.incarnation, lease.owner_epoch),
            )
            original = await (await conn.execute("SELECT * FROM public.bot_generations")).fetchone()
        forgotten = expected_domain if preempt_configuration else SourceRef("unrelated-chat")
        await FrozenPayloadStore(store).forget_sources(
            lease, "old-forget", (forgotten,), expected_deletion_epoch=0
        )
        async with store.pool.connection() as conn:
            old_decision = await (
                await conn.execute("SELECT * FROM public.bot_payload_forget_requests")
            ).fetchone()
        await store.release_host(lease)
        await store.migrate()
        lease = await store.acquire_host("new-host", "new-incarnation", ttl_seconds=120)
        async with store.pool.connection() as conn:
            row = await (await conn.execute("SELECT * FROM public.bot_generations")).fetchone()
            header = await (
                await conn.execute(
                    "SELECT * FROM public.bot_payload_headers WHERE payload_id=%s",
                    (row["config_payload_id"],),
                )
            ).fetchone()
            assert (
                await (
                    await conn.execute(
                        "SELECT * FROM public.bot_payload_forget_requests WHERE operation_id='old-forget'"
                    )
                ).fetchone()
                == old_decision
            )
            assert {k: row[k] for k in original if k not in {"config", "state_value_schema"}} == {
                k: v for k, v in original.items() if k not in {"config", "state_value_schema"}
            }
            assert row["config"] is None and row["state_value_schema"] is None
            assert (
                row["config_digest"] == config_digest
                and row["config_payload_digest"] == header["digest"]
            )
            assert header["manifest"]["sources"] == [
                {
                    "source_unit_id": expected_domain.source_unit_id,
                    "source_revision": 1,
                    "part_id": "*",
                }
            ]
            if preempt_configuration:
                assert header["state"] == "REVOKED"
                assert not await (
                    await conn.execute(
                        "SELECT 1 FROM public.bot_payload_bodies WHERE payload_id=%s",
                        (row["config_payload_id"],),
                    )
                ).fetchall()
            else:
                assert await PayloadStore(store).read_json(
                    lease, row["config_payload_id"], expected_deletion_epoch=1
                ) == {"config": config, "state_value_schema": schema}
        for statement, params in (
            ("UPDATE public.bot_generations SET config=%s", (Jsonb(config),)),
            ("UPDATE public.bot_generations SET state_value_schema=%s", (Jsonb(schema),)),
            ("UPDATE public.bot_generations SET config_payload_digest=%s", ("sha256:" + "f" * 64,)),
            ("UPDATE public.bot_generations SET config_payload_id='other'", ()),
        ):
            with pytest.raises(psycopg.Error):
                async with store.pool.connection() as conn:
                    await conn.execute(statement, params)
        await store.migrate()
        async with store.pool.connection() as conn:
            assert (
                await (await conn.execute("SELECT * FROM public.bot_generations")).fetchone() == row
            )
    finally:
        await store.close()
