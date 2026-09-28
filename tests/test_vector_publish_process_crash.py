"""Exercise the Memory/VectorIndex handoff across an actual host process death."""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.rows import dict_row
from pymilvus import AsyncMilvusClient
from test_clean_bot_host import (
    FakeEmbeddingClient,
    FakeMediaClient,
    FakeModelClient,
    FakeWebSocketClient,
)

from forest.clean_bot_host import CleanBotHost, CleanBotSettings
from forest.embeddings import EmbeddingProfile
from forest.packages import PackageCatalog, digest_json
from forest.schema_bundles import module_schema_namespace
from forest.storage.vector_index_authority import VectorIndexIdentityRepository


class CrashAfterMilvusUpsert(AsyncMilvusClient):
    async def upsert(self, **kwargs):
        await super().upsert(**kwargs)
        # Milvus has acknowledged the write; neither the ref transaction nor
        # Memory's batch publication can commit after this process exit.
        os._exit(137)


def settings_for(dsn: str, bot_id: str, milvus_uri: str) -> CleanBotSettings:
    return CleanBotSettings(
        bot_id=bot_id,
        dsn=dsn,
        model_base_url="http://127.0.0.1:8000/v1",
        model_api_key="test-model-key",
        model="test-model",
        model_deployment_id="provider.chat-completions/test",
        model_max_output_tokens=1024,
        model_input_modalities=("text",),
        embedding_endpoint="http://127.0.0.1:8001/v1/embeddings",
        embedding_api_key="test-embedding-key",
        embedding_deployment_id="embedding.multimodal/vector-crash-test",
        embedding_model_revision="test-revision/1",
        embedding_model="Qwen/Qwen3-VL-Embedding-8B",
        embedding_dimensions=4096,
        onebot_self_id="123456789",
        onebot_websocket_url="ws://127.0.0.1:3001/",
        onebot_access_token="test-onebot-token",
        allowed_group_ids=("933179609",),
        group_trigger_mode="MENTION",
        private_enabled=False,
        shell_administrator_ids=(),
        agent_system_prompt="Vector publish crash fixture.",
        vector_index_endpoint=milvus_uri,
    )


async def _crash_child(spec_path: Path) -> None:
    spec = json.loads(spec_path.read_text(encoding="utf-8"))
    settings = settings_for(spec["dsn"], spec["bot_id"], spec["milvus_uri"])
    sockets = []

    async def create_websocket(_url, _token, **_limits):
        socket = FakeWebSocketClient(settings.onebot_self_id)
        sockets.append(socket)
        return socket

    host = CleanBotHost(
        settings,
        model_client_factory=FakeModelClient,
        embedding_client_factory=FakeEmbeddingClient,
        media_client_factory=FakeMediaClient,
        vector_index_client_factory=CrashAfterMilvusUpsert,
        websocket_client_factory=create_websocket,
        package_catalog=PackageCatalog(Path(spec["catalog"])),
    )
    try:
        await host.start()
        conversation = host.registry.get("conversation.scheduler")
        configuration = conversation.module_configuration
        plan = await configuration.plan(
            operation_id="vector-crash-compact-plan",
            expected_desired_revision=1,
            value={
                "protocol": "forest.conversation.scheduler/2",
                "context_view": {
                    "high_watermark_units": 2,
                    "retain_units": 1,
                    "policy_revision": "context-view-unit-window-v1",
                },
            },
            validation_digest=digest_json(conversation.snapshot.schema("schemas/config.json")),
        )
        await configuration.apply(
            operation_id="vector-crash-compact-apply",
            plan_id=plan["plan_id"],
            expected_desired_revision=1,
        )
        await host.replace_runtime_graph()
        memory_schema = module_schema_namespace("forest.memory.graph")
        async with asyncio.timeout(90):
            while True:
                async with host.store.pool.connection() as conn:
                    domain = await (
                        await conn.execute(
                            sql.SQL(
                                "SELECT state FROM {}.memory_domain_bind_work WHERE bot_id=%s"
                            ).format(sql.Identifier(memory_schema)),
                            (settings.bot_id,),
                        )
                    ).fetchone()
                if domain is not None and domain["state"] == "READY":
                    break
                await asyncio.sleep(0.1)
        for message_id in range(98001, 98005):
            await sockets[-1].handler(
                json.dumps(
                    {
                        "self_id": int(settings.onebot_self_id),
                        "post_type": "message",
                        "message_type": "group",
                        "sub_type": "normal",
                        "time": int(datetime.now(timezone.utc).timestamp()),
                        "message_id": message_id,
                        "user_id": 20001,
                        "group_id": 933179609,
                        "sender": {
                            "user_id": 20001,
                            "nickname": "Crash tester",
                            "card": "",
                            "role": "member",
                        },
                        "message": [
                            {"type": "text", "data": {"text": f"Background {message_id}."}}
                        ],
                    }
                ).encode()
            )
        # The upsert hook must terminate this process. Otherwise the fixture
        # failed to reach the handoff window.
        await asyncio.sleep(120)
        raise AssertionError("Document vector was never published")
    finally:
        await host.close()


@pytest.mark.postgres
async def test_memory_vector_publish_recovers_after_process_crash(tmp_path):
    dsn = os.environ.get("FOREST_TEST_DSN")
    milvus_uri = os.environ.get("FOREST_TEST_MILVUS_URI")
    if not dsn or not milvus_uri:
        pytest.skip("Private PostgreSQL and Milvus are required")

    database = "forest_vector_crash_" + uuid4().hex
    bot_id = "bot-vector-crash-" + uuid4().hex[:16]
    admin = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
    await admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
    parsed = urlsplit(dsn)
    uri = urlunsplit((parsed.scheme, parsed.netloc, "/" + database, "", ""))
    settings = settings_for(uri, bot_id, milvus_uri)
    catalog = tmp_path / "vector-crash-packages"
    spec_path = tmp_path / "vector-crash.json"
    spec_path.write_text(
        json.dumps({"dsn": uri, "bot_id": bot_id, "milvus_uri": milvus_uri,
                    "catalog": str(catalog)}),
        encoding="utf-8",
    )
    profile = EmbeddingProfile(
        deployment_id=settings.embedding_deployment_id,
        model_revision=settings.embedding_model_revision,
        model=settings.embedding_model,
        dimensions=settings.embedding_dimensions,
    )
    collection = VectorIndexIdentityRepository.collection_name(bot_id, profile.space_id)
    memory_schema = module_schema_namespace("forest.memory.graph")
    vector_schema = module_schema_namespace("forest.vector.index")
    kernel_schema = module_schema_namespace("forest.kernel.runtime")
    milvus = AsyncMilvusClient(uri=milvus_uri)
    host = None
    try:
        child = await asyncio.to_thread(
            subprocess.run,
            [sys.executable, str(Path(__file__).resolve()), "--crash-child", str(spec_path)],
            capture_output=True,
            text=True,
            timeout=220,
            check=False,
        )
        assert child.returncode == 137, (child.returncode, child.stdout, child.stderr)
        async with await psycopg.AsyncConnection.connect(uri, row_factory=dict_row) as conn:
            batches = await (
                await conn.execute(
                    sql.SQL("SELECT attempt_id,state,attempt_count,lease_deadline "
                            "FROM {}.vector_publish_batches WHERE bot_id=%s")
                    .format(sql.Identifier(memory_schema)),
                    (bot_id,),
                )
            ).fetchall()
            claimed = [row for row in batches if row["state"] == "CLAIMED"]
            assert len(claimed) == 1 and claimed[0]["attempt_count"] == 1
            attempt_id = claimed[0]["attempt_id"]
            facets = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT f.facet_id,f.state FROM {m}.embedding_batch_attempts a "
                        "JOIN {m}.embedding_claim_items ci ON ci.bot_id=a.bot_id "
                        "AND ci.claim_id=a.claim_id "
                        "JOIN {m}.embedding_facets f ON f.bot_id=ci.bot_id "
                        "AND f.facet_id=ci.facet_id "
                        "WHERE a.bot_id=%s AND a.attempt_id=%s"
                    ).format(m=sql.Identifier(memory_schema)),
                    (bot_id, attempt_id),
                )
            ).fetchall()
            assert facets and all(row["state"] == "EMBEDDED" for row in facets)
            facet_ids = [row["facet_id"] for row in facets]
            refs = await (
                await conn.execute(
                    sql.SQL("SELECT ref_id FROM {}.vector_index_refs "
                            "WHERE bot_id=%s AND ref_id=ANY(%s)")
                    .format(sql.Identifier(vector_schema)),
                    (bot_id, facet_ids),
                )
            ).fetchall()
            assert refs == []
            # Wait for the real host lease to expire; never forge a new owner.
            host_lease = await (
                await conn.execute(
                    sql.SQL("SELECT expires_at FROM {}.host_lease WHERE bot_id=%s")
                    .format(sql.Identifier(kernel_schema)),
                    (bot_id,),
                )
            ).fetchone()
        written = await milvus.get(
            collection_name=collection,
            ids=facet_ids,
            output_fields=["purpose"],
            consistency_level="Strong",
        )
        assert written and all(row["purpose"] == "DOCUMENT" for row in written)
        delay = (host_lease["expires_at"] - datetime.now(timezone.utc)).total_seconds()
        if delay > 0:
            await asyncio.sleep(delay + 0.5)

        async def create_websocket(_url, _token, **_limits):
            return FakeWebSocketClient(settings.onebot_self_id)

        host = CleanBotHost(
            settings,
            model_client_factory=FakeModelClient,
            embedding_client_factory=FakeEmbeddingClient,
            media_client_factory=FakeMediaClient,
            websocket_client_factory=create_websocket,
            package_catalog=PackageCatalog(catalog),
        )
        await host.start()
        async with asyncio.timeout(160):
            while True:
                async with host.store.pool.connection() as conn:
                    recovered = await (
                        await conn.execute(
                            sql.SQL("SELECT facet_id,state,index_ref_id FROM {}.embedding_facets "
                                    "WHERE bot_id=%s AND facet_id=ANY(%s)")
                            .format(sql.Identifier(memory_schema)),
                            (bot_id, facet_ids),
                        )
                    ).fetchall()
                    batch = await (
                        await conn.execute(
                            sql.SQL("SELECT state,attempt_count FROM {}.vector_publish_batches "
                                    "WHERE bot_id=%s AND attempt_id=%s")
                            .format(sql.Identifier(memory_schema)),
                            (bot_id, attempt_id),
                        )
                    ).fetchone()
                if (len(recovered) == len(facets)
                        and all(row["state"] == "READY" for row in recovered)
                        and batch["state"] == "SETTLED"):
                    break
                await asyncio.sleep(0.2)
        assert batch["attempt_count"] >= 2
        assert all(row["index_ref_id"] == row["facet_id"] for row in recovered)
        async with host.store.pool.connection() as conn:
            refs = await (
                await conn.execute(
                    sql.SQL("SELECT ref_id,state FROM {}.vector_index_refs "
                            "WHERE bot_id=%s AND ref_id=ANY(%s)")
                    .format(sql.Identifier(vector_schema)),
                    (bot_id, facet_ids),
                )
            ).fetchall()
        assert {row["ref_id"] for row in refs} == {row["facet_id"] for row in facets}
        assert all(row["state"] == "ACTIVE" for row in refs)
    finally:
        if host is not None:
            await host.close()
        if await milvus.has_collection(collection_name=collection):
            await milvus.drop_collection(collection_name=collection)
        await milvus.close()
        await admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(database)))
        await admin.close()


if __name__ == "__main__" and sys.argv[1:2] == ["--crash-child"]:
    asyncio.run(_crash_child(Path(sys.argv[2])))
