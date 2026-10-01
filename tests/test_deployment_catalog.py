from __future__ import annotations

import asyncio
import os
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import psycopg
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from psycopg import sql

from forest.deployment_management import deployment_management_components
from forest.deployment_management_app import create_deployment_management_app
from forest.deployment_provisioning import DeploymentProvisioningExecutor
from forest.embeddings import EmbeddingProfile
from forest.errors import ForestError
from forest.model_contract import ModelCapabilities
from forest.multi_bot_supervisor import MultiBotSupervisor
from forest.official_bot_provisioning import (
    OfficialBotCatalogSettingsResolver,
    OfficialBotProvisioner,
    OfficialBotSettingsResolver,
    default_vector_index_configuration,
    official_bootstrap_contributions,
    official_bootstrap_schema_digests,
)
from forest.postgres_database_allocator import LocalPostgresDatabaseAllocator
from forest.schema_bundles import module_schema_namespace
from forest.storage.bot_store import BotStore
from forest.storage.deployment_audit import DeploymentAuditRepository
from forest.storage.deployment_bootstrap_profiles import DeploymentBootstrapProfileAuthority
from forest.storage.deployment_catalog import DeploymentCatalogRepository
from forest.storage.deployment_provisioning_jobs import DeploymentProvisioningJobAuthority
from forest.storage.deployment_secret_authority import DeploymentSecretAuthority
from forest.storage.deployment_storage_profiles import DeploymentStorageProfileAuthority
from forest.storage.deployment_store import DeploymentStore
from forest.storage.deployment_web_authority import DeploymentWebAuthorityRepository
from forest.storage.deployment_web_identity import DeploymentWebIdentityAuthority


@pytest_asyncio.fixture
async def deployment_catalog():
    dsn = os.environ.get("FOREST_TEST_DSN")
    if not dsn:
        pytest.skip("FOREST_TEST_DSN must name a private PostgreSQL test instance")
    name = "forest_deployment_" + uuid4().hex
    admin = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
    await admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    parsed = urlsplit(dsn)
    uri = urlunsplit((parsed.scheme, parsed.netloc, "/" + name, "", ""))
    store = DeploymentStore(uri, "deployment/test")
    await store.open()
    repository = DeploymentCatalogRepository(store)
    try:
        yield repository
    finally:
        await store.close()
        assert name.startswith("forest_deployment_") and len(name) == 50
        await admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))
        await admin.close()


async def _storage_profile(repository, bot_id: str, name: str):
    key = bytes(range(32))
    secrets = DeploymentSecretAuthority(
        repository.store,
        active_key_id="master/test/1",
        key_resolver=lambda key_id: key if key_id == "master/test/1" else None,
    )
    secret_ref = f"secret://{name}/postgresql-dsn"
    await secrets.put(
        operation_id=f"secret/{name}/dsn/1",
        secret_ref=secret_ref,
        purpose="storage.postgresql.dsn",
        plaintext=f"postgresql://forest:secret@db.example/forest_{name}".encode(),
        expected_revision=0,
    )
    profiles = DeploymentStorageProfileAuthority(repository.store, secrets=secrets)
    return await profiles.create(
        operation_id=f"storage-profile/{name}/create",
        profile_ref=f"storage-profile/{name}/1",
        bot_id=bot_id,
        storage_generation=1,
        dsn_secret_ref=secret_ref,
    )


@pytest.mark.postgres
async def test_deployment_catalog_keeps_bot_identity_intent_and_observation_separate(
    deployment_catalog,
):
    repository = deployment_catalog
    first_identity = await repository.initialize()
    second_identity = await repository.initialize()
    assert first_identity == second_identity

    alpha_profile = await _storage_profile(repository, "bot/alpha", "alpha")
    beta_profile = await _storage_profile(repository, "bot/beta", "beta")
    alpha_bootstrap = await _bootstrap_profile(repository, alpha_profile, "alpha")
    beta_bootstrap = await _bootstrap_profile(repository, beta_profile, "beta")
    secrets = DeploymentSecretAuthority(
        repository.store,
        active_key_id="master/test/1",
        key_resolver=lambda _key_id: bytes(range(32)),
    )
    profiles = DeploymentStorageProfileAuthority(repository.store, secrets=secrets)
    resolved = await profiles.resolve(
        profile_ref=alpha_profile.profile_ref,
        expected_bot_id="bot/alpha",
        expected_storage_generation=1,
    )
    assert resolved.dsn == "postgresql://forest:secret@db.example/forest_alpha"
    assert "secret" not in repr(resolved)
    with pytest.raises(ForestError) as caught:
        await profiles.resolve(
            profile_ref=alpha_profile.profile_ref,
            expected_bot_id="bot/beta",
            expected_storage_generation=1,
        )
    assert caught.value.code == "STORAGE_PROFILE_SCOPE_MISMATCH"
    with pytest.raises(ForestError) as caught:
        await repository.register_ready(
            operation_id="provision/profile-mismatch",
            bot_id="bot/other",
            display_name="Other",
            storage_profile_ref=alpha_profile.profile_ref,
            bootstrap_profile_ref=alpha_bootstrap.profile_ref,
            bootstrap_manifest_digest=alpha_bootstrap.manifest_digest,
        )
    assert caught.value.code == "STORAGE_PROFILE_SCOPE_MISMATCH"

    alpha = await repository.register_ready(
        operation_id="provision/alpha",
        bot_id="bot/alpha",
        display_name="Alpha",
        storage_profile_ref=alpha_profile.profile_ref,
        bootstrap_profile_ref=alpha_bootstrap.profile_ref,
        bootstrap_manifest_digest=alpha_bootstrap.manifest_digest,
    )
    replay = await repository.register_ready(
        operation_id="provision/alpha",
        bot_id="bot/alpha",
        display_name="Alpha",
        storage_profile_ref=alpha_profile.profile_ref,
        bootstrap_profile_ref=alpha_bootstrap.profile_ref,
        bootstrap_manifest_digest=alpha_bootstrap.manifest_digest,
    )
    beta = await repository.register_ready(
        operation_id="provision/beta",
        bot_id="bot/beta",
        display_name="Beta",
        storage_profile_ref=beta_profile.profile_ref,
        bootstrap_profile_ref=beta_bootstrap.profile_ref,
        bootstrap_manifest_digest=beta_bootstrap.manifest_digest,
    )
    assert replay == alpha
    assert alpha["desired_state"] == beta["desired_state"] == "OFFLINE"
    assert alpha["observed_state"] is None

    alpha = await repository.set_desired(
        operation_id="runtime/alpha/online",
        bot_id="bot/alpha",
        desired_state="ONLINE",
        expected_revision=alpha["intent_revision"],
    )
    assert alpha["desired_state"] == "ONLINE"
    with pytest.raises(ForestError) as caught:
        await repository.set_desired(
            operation_id="runtime/alpha/stale",
            bot_id="bot/alpha",
            desired_state="OFFLINE",
            expected_revision=1,
        )
    assert caught.value.code == "BOT_INTENT_REVISION_CONFLICT"

    lease = await repository.claim_supervisor("host/one", "incarnation/one")
    starting = await repository.observe(
        lease,
        bot_id="bot/alpha",
        observed_state="STARTING",
    )
    online = await repository.observe(
        lease,
        bot_id="bot/alpha",
        observed_state="ONLINE",
        graph_id="graph/alpha/1",
    )
    assert online["revision"] == starting["revision"] + 1
    assert (await repository.inspect("bot/alpha"))["graph_id"] == "graph/alpha/1"
    assert (await repository.inspect("bot/beta"))["observed_state"] is None

    with pytest.raises(ForestError) as caught:
        await repository.claim_supervisor("host/two", "incarnation/two")
    assert caught.value.code == "DEPLOYMENT_SUPERVISOR_BUSY"
    await repository.release_supervisor(lease)
    successor = await repository.claim_supervisor("host/two", "incarnation/two")
    assert successor.owner_epoch == lease.owner_epoch + 1
    with pytest.raises(ForestError) as caught:
        await repository.observe(
            lease,
            bot_id="bot/alpha",
            observed_state="OFFLINE",
        )
    assert caught.value.code == "STALE_DEPLOYMENT_SUPERVISOR"


@pytest.mark.postgres
async def test_deployment_failure_backoff_is_fenced_and_visible(deployment_catalog):
    repository = deployment_catalog
    await repository.initialize()
    profile = await _storage_profile(repository, "bot/failing", "failing")
    bootstrap = await _bootstrap_profile(repository, profile, "failing")
    bot = await repository.register_ready(
        operation_id="provision/failing",
        bot_id="bot/failing",
        display_name="Failing",
        storage_profile_ref=profile.profile_ref,
        bootstrap_profile_ref=bootstrap.profile_ref,
        bootstrap_manifest_digest=bootstrap.manifest_digest,
    )
    await repository.set_desired(
        operation_id="runtime/failing/online",
        bot_id=bot["bot_id"],
        desired_state="ONLINE",
        expected_revision=bot["intent_revision"],
    )
    lease = await repository.claim_supervisor("host/one", "incarnation/one")
    first = await repository.observe(
        lease,
        bot_id=bot["bot_id"],
        observed_state="FAILED",
        error_code="BOT_START_FAILED",
        retry_seconds=30,
    )
    second = await repository.observe(
        lease,
        bot_id=bot["bot_id"],
        observed_state="FAILED",
        error_code="BOT_START_FAILED",
        retry_seconds=60,
    )
    assert second["consecutive_failures"] == first["consecutive_failures"] + 1 == 2
    projected = await repository.inspect(bot["bot_id"])
    assert projected["desired_state"] == "ONLINE"
    assert projected["observed_state"] == "FAILED"
    assert projected["retry_not_before"] is not None


@pytest.mark.postgres
async def test_deployment_catalog_rejects_unknown_schema_objects(deployment_catalog):
    repository = deployment_catalog
    await repository.initialize()
    async with repository.store.pool.connection() as conn:
        await conn.execute("CREATE TABLE forest_deployment.unknown_object(id integer)")
    with pytest.raises(ForestError) as caught:
        await repository.initialize()
    assert caught.value.code == "DEPLOYMENT_SCHEMA_TAMPERED"


@pytest.mark.postgres
async def test_deployment_secret_rotation_is_encrypted_cas_and_write_only(
    deployment_catalog,
):
    repository = deployment_catalog
    await repository.initialize()
    key = bytes(range(32))
    secrets = DeploymentSecretAuthority(
        repository.store,
        active_key_id="master/test/1",
        key_resolver=lambda key_id: key if key_id == "master/test/1" else None,
    )
    first = await secrets.put(
        operation_id="secret/model/1",
        secret_ref="secret://bot/alpha/model-api/1",
        purpose="provider.chat-completions.credential",
        plaintext=b"model-secret-value",
        expected_revision=0,
    )
    replay = await secrets.put(
        operation_id="secret/model/1",
        secret_ref="secret://bot/alpha/model-api/1",
        purpose="provider.chat-completions.credential",
        plaintext=b"model-secret-value",
        expected_revision=0,
    )
    assert replay == first
    assert first.version == first.revision == 1
    assert not hasattr(first, "plaintext")
    assert await secrets.resolve_text(first.secret_ref) == "model-secret-value"

    second = await secrets.put(
        operation_id="secret/model/2",
        secret_ref=first.secret_ref,
        purpose=first.purpose,
        plaintext=b"rotated-model-secret",
        expected_revision=first.revision,
    )
    assert second.version == second.revision == 2
    assert await secrets.resolve(second.secret_ref) == b"rotated-model-secret"
    with pytest.raises(ForestError) as caught:
        await secrets.put(
            operation_id="secret/model/stale",
            secret_ref=first.secret_ref,
            purpose=first.purpose,
            plaintext=b"stale-write",
            expected_revision=first.revision,
        )
    assert caught.value.code == "SECRET_REVISION_CONFLICT"

    async with repository.store.pool.connection() as conn:
        rows = await (
            await conn.execute(
                "SELECT ciphertext FROM forest_deployment.secret_versions "
                "WHERE secret_ref=%s ORDER BY version",
                (first.secret_ref,),
            )
        ).fetchall()
    ciphertext = b"".join(bytes(row["ciphertext"]) for row in rows)
    assert b"model-secret-value" not in ciphertext
    assert b"rotated-model-secret" not in ciphertext

    wrong = DeploymentSecretAuthority(
        repository.store,
        active_key_id="master/test/1",
        key_resolver=lambda _key_id: b"x" * 32,
    )
    with pytest.raises(ForestError) as caught:
        await wrong.resolve(first.secret_ref)
    assert caught.value.code == "SECRET_DECRYPTION_FAILED"


@pytest.mark.postgres
async def test_deployment_secret_keyring_rotates_writes_and_retains_old_decryption(
    deployment_catalog,
):
    repository = deployment_catalog
    await repository.initialize()
    old_key = bytes(range(32))
    new_key = bytes(reversed(range(32)))
    old = DeploymentSecretAuthority(
        repository.store,
        active_key_id="master/test/1",
        key_resolver={"master/test/1": old_key}.get,
    )
    first = await old.put(
        operation_id="secret/keyring/old",
        secret_ref="secret://keyring/old",
        purpose="test.credential",
        plaintext=b"old-key-secret",
        expected_revision=0,
    )
    rotated_keys = {"master/test/1": old_key, "master/test/2": new_key}
    rotated = DeploymentSecretAuthority(
        repository.store,
        active_key_id="master/test/2",
        key_resolver=rotated_keys.get,
    )
    assert await rotated.resolve(first.secret_ref) == b"old-key-secret"
    second = await rotated.put(
        operation_id="secret/keyring/new",
        secret_ref="secret://keyring/new",
        purpose="test.credential",
        plaintext=b"new-key-secret",
        expected_revision=0,
    )
    assert second.key_id == "master/test/2"
    assert await rotated.resolve(second.secret_ref) == b"new-key-secret"

    missing_old = DeploymentSecretAuthority(
        repository.store,
        active_key_id="master/test/2",
        key_resolver={"master/test/2": new_key}.get,
    )
    with pytest.raises(ForestError) as unavailable:
        await missing_old.resolve(first.secret_ref)
    assert unavailable.value.code == "SECRET_MASTER_KEY_UNAVAILABLE"


@pytest.mark.postgres
async def test_bootstrap_profile_is_component_scoped_immutable_and_secret_ref_only(
    deployment_catalog,
):
    repository = deployment_catalog
    await repository.initialize()
    storage = await _storage_profile(repository, "bot/profile", "profile")
    key = bytes(range(32))
    secrets = DeploymentSecretAuthority(
        repository.store,
        active_key_id="master/test/1",
        key_resolver=lambda _key_id: key,
    )
    await secrets.put(
        operation_id="secret/profile/model/1",
        secret_ref="secret://profile/model-api",
        purpose="provider.chat-completions.credential",
        plaintext=b"provider-key",
        expected_revision=0,
    )
    profiles = DeploymentBootstrapProfileAuthority(repository.store, secrets=secrets)
    components = {
        "provider.chat-completions": {
            "protocol": "forest.provider.chat-completions/1",
            "base_url": "https://model.example/v1",
            "credential_ref": "secret://profile/model-api",
        },
        "conversation.trigger-policy": {
            "protocol": "forest.conversation.trigger-policy/1",
            "allowed_group_ids": ["933179609"],
        },
    }
    schemas = {component_id: "sha256:" + str(index) * 64 for index, component_id in enumerate(components, 1)}
    created = await profiles.create(
        operation_id="bootstrap-profile/profile/1",
        profile_ref="bootstrap-profile/profile/1",
        bot_id="bot/profile",
        storage_profile_ref=storage.profile_ref,
        storage_generation=1,
        components=components,
        schema_digests=schemas,
    )
    replay = await profiles.create(
        operation_id="bootstrap-profile/profile/1",
        profile_ref="bootstrap-profile/profile/1",
        bot_id="bot/profile",
        storage_profile_ref=storage.profile_ref,
        storage_generation=1,
        components=components,
        schema_digests=schemas,
    )
    assert replay == created
    assert [item.component_id for item in created.components] == sorted(components)
    assert all(item.config_digest.startswith("sha256:") for item in created.components)

    with pytest.raises(ForestError) as caught:
        await profiles.create(
            operation_id="bootstrap-profile/plaintext/1",
            profile_ref="bootstrap-profile/plaintext/1",
            bot_id="bot/profile",
            storage_profile_ref=storage.profile_ref,
            storage_generation=1,
            components={"provider.chat-completions": {"model_api_key": "forbidden"}},
            schema_digests={"provider.chat-completions": "sha256:" + "f" * 64},
        )
    assert caught.value.code == "BOOTSTRAP_SECRET_PLAINTEXT_FORBIDDEN"


async def _bootstrap_profile(repository, storage, name: str):
    secrets = DeploymentSecretAuthority(
        repository.store,
        active_key_id="master/test/1",
        key_resolver=lambda _key_id: bytes(range(32)),
    )
    profiles = DeploymentBootstrapProfileAuthority(repository.store, secrets=secrets)
    return await profiles.create(
        operation_id=f"bootstrap-profile/{name}/create",
        profile_ref=f"bootstrap-profile/{name}/1",
        bot_id=storage.bot_id,
        storage_profile_ref=storage.profile_ref,
        storage_generation=storage.storage_generation,
        components={"conversation.trigger-policy": {"allowed_group_ids": ["933179609"]}},
        schema_digests={"conversation.trigger-policy": "sha256:" + "a" * 64},
    )


@pytest.mark.postgres
async def test_provisioning_job_claim_renew_retry_and_success_are_fenced(deployment_catalog):
    repository = deployment_catalog
    await repository.initialize()
    storage = await _storage_profile(repository, "bot/jobs", "jobs")
    profile = await _bootstrap_profile(repository, storage, "jobs")
    jobs = DeploymentProvisioningJobAuthority(repository.store)
    queued = await jobs.enqueue(
        operation_id="provisioning/jobs/enqueue",
        job_id="provisioning/jobs/1",
        bot_id="bot/jobs",
        display_name="Jobs",
        storage_profile_ref=storage.profile_ref,
        storage_generation=storage.storage_generation,
        bootstrap_profile_ref=profile.profile_ref,
        max_attempts=2,
    )
    replay = await jobs.enqueue(
        operation_id="provisioning/jobs/enqueue",
        job_id="provisioning/jobs/1",
        bot_id="bot/jobs",
        display_name="Jobs",
        storage_profile_ref=storage.profile_ref,
        storage_generation=storage.storage_generation,
        bootstrap_profile_ref=profile.profile_ref,
        max_attempts=2,
    )
    assert queued["job_id"] == replay["job_id"]
    supervisor = await repository.claim_supervisor("host/jobs", "incarnation/jobs")
    first, duplicate = await asyncio.gather(
        jobs.claim(supervisor, worker_id="worker/one"),
        jobs.claim(supervisor, worker_id="worker/two"),
    )
    claim = first or duplicate
    assert claim is not None
    assert (first is None) != (duplicate is None)
    revision = (await jobs.inspect(claim.job_id))["revision"]
    renewed = await jobs.renew(supervisor, claim)
    assert renewed.lease_epoch == claim.lease_epoch
    assert (await jobs.inspect(claim.job_id))["revision"] == revision
    waiting = await jobs.fail(
        supervisor,
        renewed,
        error_code="EXPECTED_PROVISIONING_FAILURE",
        retry_seconds=1,
    )
    assert waiting["state"] == "RETRY_WAIT"
    await asyncio.sleep(1.05)
    second = await jobs.claim(supervisor, worker_id="worker/two")
    assert second is not None
    assert second.attempt_no == 2
    assert second.lease_epoch == claim.lease_epoch + 1
    await repository.register_ready(
        operation_id="provision-ready/provisioning/jobs/1",
        bot_id="bot/jobs",
        display_name="Jobs",
        storage_profile_ref=storage.profile_ref,
        bootstrap_profile_ref=profile.profile_ref,
        bootstrap_manifest_digest=profile.manifest_digest,
        storage_generation=storage.storage_generation,
    )
    succeeded = await jobs.succeed(supervisor, second)
    assert succeeded["state"] == "SUCCEEDED"
    with pytest.raises(ForestError) as caught:
        await jobs.fail(supervisor, claim, error_code="STALE_TEST")
    assert caught.value.code == "STALE_PROVISIONING_CLAIM"


@pytest.mark.postgres
async def test_provisioning_executor_resolves_frozen_inputs_before_ready_publication(
    deployment_catalog,
):
    repository = deployment_catalog
    await repository.initialize()
    storage = await _storage_profile(repository, "bot/executor", "executor")
    profile = await _bootstrap_profile(repository, storage, "executor")
    secrets = DeploymentSecretAuthority(
        repository.store,
        active_key_id="master/test/1",
        key_resolver=lambda _key_id: bytes(range(32)),
    )
    storage_profiles = DeploymentStorageProfileAuthority(repository.store, secrets=secrets)
    bootstrap_profiles = DeploymentBootstrapProfileAuthority(repository.store, secrets=secrets)
    jobs = DeploymentProvisioningJobAuthority(repository.store)
    await jobs.enqueue(
        operation_id="provisioning/executor/enqueue",
        job_id="provisioning/executor/1",
        bot_id="bot/executor",
        display_name="Executor",
        storage_profile_ref=storage.profile_ref,
        storage_generation=storage.storage_generation,
        bootstrap_profile_ref=profile.profile_ref,
    )
    calls = []

    async def provisioner(*, claim, storage, profile):
        calls.append((claim, storage, profile))
        assert storage.dsn.endswith("/forest_executor")
        assert profile.manifest_digest == claim.bootstrap_manifest_digest

    executor = DeploymentProvisioningExecutor(
        jobs=jobs,
        catalog=repository,
        storage_profiles=storage_profiles,
        bootstrap_profiles=bootstrap_profiles,
        provisioner=provisioner,
        worker_id="worker/executor",
    )
    supervisor = await repository.claim_supervisor("host/executor", "incarnation/executor")
    result = await executor.run_once(supervisor)
    assert result["state"] == "SUCCEEDED"
    assert len(calls) == 1
    bot = await repository.inspect("bot/executor")
    assert bot["supply_state"] == "READY"
    assert bot["desired_state"] == "OFFLINE"
    assert await executor.run_once(supervisor) is None


@pytest.mark.postgres
async def test_official_provisioner_materializes_module_authorities_without_starting_graph(
    deployment_catalog,
):
    repository = deployment_catalog
    await repository.initialize()
    root_dsn = os.environ["FOREST_TEST_DSN"]
    allocator = LocalPostgresDatabaseAllocator(root_dsn)
    target_name = allocator.database_name("bot/official")
    target_role = allocator.role_name("bot/official")
    target_dsn = allocator.new_target_dsn("bot/official")
    admin = await psycopg.AsyncConnection.connect(root_dsn, autocommit=True)
    await admin.execute(
        sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(target_name))
    )
    key = bytes(range(32))
    secrets = DeploymentSecretAuthority(
        repository.store,
        active_key_id="master/test/1",
        key_resolver=lambda _key_id: key,
    )
    try:
        values = {
            "secret://official/storage": ("storage.postgresql.dsn", target_dsn),
            "secret://official/model": ("provider.chat-completions.credential", "model-key"),
            "secret://official/embedding": ("embedding.multimodal.credential", "embed-key"),
            "secret://official/onebot": ("adapter.onebot.access-token", "onebot-token"),
        }
        for index, (secret_ref, (purpose, plaintext)) in enumerate(values.items(), 1):
            await secrets.put(
                operation_id=f"secret/official/{index}",
                secret_ref=secret_ref,
                purpose=purpose,
                plaintext=plaintext.encode(),
                expected_revision=0,
            )
        storage_profiles = DeploymentStorageProfileAuthority(repository.store, secrets=secrets)
        storage = await storage_profiles.create(
            operation_id="storage-profile/official/create",
            profile_ref="storage-profile/official/1",
            bot_id="bot/official",
            storage_generation=1,
            dsn_secret_ref="secret://official/storage",
        )
        capabilities = ModelCapabilities(
            deployment_id="provider.chat-completions/test",
            model="test-model",
            capability_revision="test/1",
            input_modalities=("text", "image"),
            max_output_tokens=2048,
        )
        components = {
            "provider.chat-completions": {
                "protocol": "forest.provider.chat-completions/1",
                "base_url": "https://model.example/v1",
                "credential_ref": "secret://official/model",
                "timeout_seconds": 30,
                "max_connections": 3,
                "capabilities": capabilities.as_dict(),
            },
            "embedding.multimodal": {
                "protocol": "forest.embedding.multimodal/1",
                "endpoint": "https://embedding.example/v1/embeddings",
                "credential_ref": "secret://official/embedding",
                "timeout_seconds": 20,
                "max_connections": 2,
                "profile": {
                    "deployment_id": "embedding.multimodal/test",
                    "model_revision": "test/1",
                    "model": "Qwen/Qwen3-VL-Embedding-8B",
                    "dimensions": 4096,
                },
            },
            "vector.index": {
                **default_vector_index_configuration(
                    space_id=EmbeddingProfile(
                        deployment_id="embedding.multimodal/test",
                        model_revision="test/1",
                        model="Qwen/Qwen3-VL-Embedding-8B",
                        dimensions=4096,
                    ).space_id,
                    dimensions=4096,
                ),
                "endpoint": "http://milvus:19530",
            },
            "conversation.trigger-policy": {
                "protocol": "forest.conversation.trigger-policy/1",
                "allowed_group_ids": ["933179609"],
                "group_trigger_mode": "MENTION",
                "private_enabled": True,
            },
                "shell.command": {
                    "protocol": "forest.shell.command/1",
                "binding_ref": "binding/main",
                "administrator_principals": ["qq:10001"],
            },
            "agent.runtime": {
                "protocol": "forest.agent.runtime.bootstrap/1",
                "preset_system": ["你是测试 Bot。"],
            },
            "adapter.onebot": {
                "protocol": "forest.adapter.onebot.bootstrap/1",
                "binding_id": "binding/main",
                "self_id": "123456789",
                "websocket_url": "ws://127.0.0.1:3001/",
                "access_token_ref": "secret://official/onebot",
            },
        }
        bootstrap_profiles = DeploymentBootstrapProfileAuthority(
            repository.store, secrets=secrets
        )
        bootstrap = await bootstrap_profiles.create(
            operation_id="bootstrap-profile/official/create",
            profile_ref="bootstrap-profile/official/1",
            bot_id="bot/official",
            storage_profile_ref=storage.profile_ref,
            storage_generation=storage.storage_generation,
            components=components,
            schema_digests=official_bootstrap_schema_digests(),
        )
        jobs = DeploymentProvisioningJobAuthority(repository.store)
        await jobs.enqueue(
            operation_id="provisioning/official/enqueue",
            job_id="provisioning/official/1",
            bot_id="bot/official",
            display_name="Official",
            storage_profile_ref=storage.profile_ref,
            storage_generation=storage.storage_generation,
            bootstrap_profile_ref=bootstrap.profile_ref,
        )
        resolver = OfficialBotSettingsResolver(secrets=secrets)
        executor = DeploymentProvisioningExecutor(
            jobs=jobs,
            catalog=repository,
            storage_profiles=storage_profiles,
            bootstrap_profiles=bootstrap_profiles,
            provisioner=OfficialBotProvisioner(
                settings_resolver=resolver,
                database_allocator=allocator,
            ),
            worker_id="worker/official",
        )
        supervisor = MultiBotSupervisor(
            repository,
            storage_profiles=storage_profiles,
            settings_resolver=OfficialBotCatalogSettingsResolver(
                bootstrap_profiles=bootstrap_profiles,
                settings_resolver=resolver,
            ),
            host_id="host/official",
            incarnation="incarnation/official",
            poll_seconds=0.05,
            provisioning_executor=executor,
        )
        await supervisor.start()
        try:
            async def provisioned():
                job = await jobs.inspect("provisioning/official/1")
                assert job["state"] != "FAILED", job.get("last_error_code")
                return job["state"] == "SUCCEEDED"

            await _eventually(provisioned, timeout=60)
        finally:
            await supervisor.close()
        bot = await repository.inspect("bot/official")
        assert bot["bootstrap_profile_ref"] == bootstrap.profile_ref
        assert bot["bootstrap_manifest_digest"] == bootstrap.manifest_digest

        target = BotStore(target_dsn, "bot/official")
        await target.open()
        try:
            async with target.pool.connection() as conn:
                bundles = await (
                    await conn.execute("SELECT count(*) AS count FROM forest_kernel.schema_bundles")
                ).fetchone()
                assert bundles["count"] == 22
                runtime_namespace = module_schema_namespace("forest.kernel.runtime")
                graph = await (
                    await conn.execute(
                        sql.SQL("SELECT count(*) AS count FROM {}.runtime_graph_head").format(
                            sql.Identifier(runtime_namespace)
                        )
                    )
                ).fetchone()
                assert graph["count"] == 0
                provider_namespace = module_schema_namespace(
                    "forest.provider.chat-completions"
                )
                head = await (
                    await conn.execute(
                        sql.SQL("SELECT * FROM {}.module_configuration_heads").format(
                            sql.Identifier(provider_namespace)
                        )
                    )
                ).fetchone()
                assert head["desired_revision"] == head["effective_revision"] == 1
        finally:
            await target.close()
    finally:
        assert target_name.startswith("forest_bot_") and len(target_name) == 35
        await admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(target_name)))
        await admin.execute(sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(target_role)))
        await admin.close()


@pytest.mark.postgres
async def test_deployment_webui_discovers_components_and_keeps_secrets_write_only(
    deployment_catalog,
):
    repository = deployment_catalog
    await repository.initialize()
    secrets = DeploymentSecretAuthority(
        repository.store,
        active_key_id="master/test/1",
        key_resolver=lambda _key_id: bytes(range(32)),
    )
    storage_profiles = DeploymentStorageProfileAuthority(repository.store, secrets=secrets)
    bootstrap_profiles = DeploymentBootstrapProfileAuthority(repository.store, secrets=secrets)
    jobs = DeploymentProvisioningJobAuthority(repository.store)
    web_identity = DeploymentWebIdentityAuthority(
        repository.store,
        active_key_id="master/test/1",
        key_resolver=lambda _key_id: bytes(range(32)),
    )
    pairing_code = "p" * 48
    setup_issued_at = datetime.now(timezone.utc)
    await web_identity.initialize_setup(
        SimpleNamespace(
            setup_id="setup/catalogwebui0001",
            pairing_code=pairing_code,
            issued_at=setup_issued_at,
            expires_at=setup_issued_at + timedelta(hours=1),
        )
    )
    await web_identity.complete_setup(
        pairing_code=pairing_code,
        username="admin",
        display_name="Administrator",
        password="deployment-web-password",
    )
    supervisor = SimpleNamespace(notify=lambda: None)
    allocated_dsn = "postgresql://forest:hidden-password@postgres/forest_bot_web"
    allocator = SimpleNamespace(
        new_target_dsn=lambda _bot_id: allocated_dsn,
        validate_target=lambda _bot_id, _target_dsn: None,
    )
    deployment_audit = DeploymentAuditRepository(repository.store)
    app = create_deployment_management_app(
        deployment_id="deployment/test",
        components=deployment_management_components(
            catalog=repository,
            secrets=secrets,
            storage_profiles=storage_profiles,
            bootstrap_profiles=bootstrap_profiles,
            jobs=jobs,
            supervisor=supervisor,
            audit=deployment_audit,
            bootstrap_contributions=official_bootstrap_contributions(),
            database_allocator=allocator,
            web_identity=web_identity,
        ),
        identity_authority=web_identity,
        deployment_web_receipts=DeploymentWebAuthorityRepository(repository.store),
        deployment_audit=deployment_audit,
        allowed_origin="http://forest.test",
    )
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://forest.test"
    ) as client:
        assert (await client.get("/management/v1/runtime")).status_code == 401
        login = await client.post(
            "/management/v1/auth/login",
            headers={"origin": "http://forest.test"},
            json={"username": "admin", "password": "deployment-web-password"},
        )
        assert login.status_code == 200
        csrf = client.cookies["forest_management_csrf"]
        headers = {"origin": "http://forest.test", "x-forest-csrf": csrf}
        runtime = await client.get("/management/v1/runtime", headers=headers)
        assert runtime.status_code == 200
        body = runtime.json()
        assert body["runtime"]["graph_state"] == "CONTROL_PLANE"
        assert {item["component_id"] for item in body["modules"]} == {
            "deployment.catalog",
            "deployment.secrets",
            "deployment.provisioning",
            "deployment.accounts",
            "deployment.audit",
        }
        assert (await client.get("/static/deployment-provisioning.js")).status_code == 200
        shell_script = (await client.get("/static/app.js")).text
        assert "/management/v1/scopes/" in shell_script
        assert "bot-scopes" not in shell_script
        assert "scope_kind}:${scope.scope_id" in shell_script
        assert runtime.json()["scopes"][0]["scope_kind"] == "deployment"
        saved = await client.post(
            "/management/v1/scopes/deployment/deployment/modules/"
            "deployment.secrets/actions/secret.put",
            headers={**headers, "idempotency-key": "secret-web-1"},
            json={
                "secret_ref": "secret://web/test",
                "purpose": "test.credential",
                "plaintext": "never-return-this",
                "expected_revision": 0,
            },
        )
        assert saved.status_code == 200
        assert "never-return-this" not in saved.text
        rejected_secret = await client.post(
            "/management/v1/scopes/deployment/deployment/modules/"
            "deployment.secrets/actions/secret.put",
            headers={**headers, "idempotency-key": "secret-web-rejected"},
            json={
                "secret_ref": "secret://web/rejected",
                "purpose": "test.credential",
                "plaintext": "must-rollback",
                "expected_revision": 1,
            },
        )
        assert rejected_secret.status_code == 409
        assert rejected_secret.json()["code"] == "SECRET_REVISION_CONFLICT"
        listed = await client.post(
            "/management/v1/scopes/deployment/deployment/modules/"
            "deployment.secrets/queries/secrets.list",
            headers=headers,
            json={},
        )
        assert listed.status_code == 200
        assert listed.json()["items"][0]["secret_ref"] == "secret://web/test"
        assert "never-return-this" not in listed.text
        storage = await client.post(
            "/management/v1/scopes/deployment/deployment/modules/"
            "deployment.provisioning/actions/storage.create-local",
            headers={**headers, "idempotency-key": "storage-web-1"},
            json={
                "profile_ref": "storage-profile/web/1",
                "bot_id": "bot/web",
                "storage_generation": 1,
                "dsn_secret_ref": "secret://web/storage",
            },
        )
        assert storage.status_code == 200
        assert storage.json() == {
            "profile_ref": "storage-profile/web/1",
            "bot_id": "bot/web",
            "storage_generation": 1,
            "dsn_secret_ref": "secret://web/storage",
        }
        assert "hidden-password" not in storage.text
        assert await secrets.resolve_text("secret://web/storage") == allocated_dsn
        replayed_storage = await client.post(
            "/management/v1/scopes/deployment/deployment/modules/"
            "deployment.provisioning/actions/storage.create-local",
            headers={**headers, "idempotency-key": "storage-web-1"},
            json={
                "profile_ref": "storage-profile/web/1",
                "bot_id": "bot/web",
                "storage_generation": 1,
                "dsn_secret_ref": "secret://web/storage",
            },
        )
        assert replayed_storage.json() == storage.json()
        bootstrap_catalog = await client.post(
            "/management/v1/scopes/deployment/deployment/modules/"
            "deployment.provisioning/queries/provisioning.bootstrap-catalog",
            headers=headers,
            json={},
        )
        assert bootstrap_catalog.status_code == 200
        catalog_body = bootstrap_catalog.json()
        assert catalog_body["protocol"] == "forest.bootstrap-configuration-catalog/1"
        assert len(catalog_body["items"]) == 7
        assert all(item["surface"]["kind"] == "trusted-module" for item in catalog_body["items"])
        assert all(item["schema_digest"].startswith("sha256:") for item in catalog_body["items"])
        assert (await client.get("/static/bootstrap-component.js")).status_code == 200
        assert "textarea name=\"components\"" not in (
            await client.get("/static/modules/deployment-provisioning.html")
        ).text

        components = {
            item["component_id"]: item["initial_value"] for item in catalog_body["items"]
        }
        for index, (secret_ref, purpose) in enumerate(
            (
                (
                    components["provider.chat-completions"]["credential_ref"],
                    "provider.chat-completions.credential",
                ),
                (
                    components["embedding.multimodal"]["credential_ref"],
                    "embedding.multimodal.credential",
                ),
                (
                    components["adapter.onebot"]["access_token_ref"],
                    "adapter.onebot.access-token",
                ),
            ),
            1,
        ):
            await secrets.put(
                operation_id=f"secret/bootstrap-ui/{index}",
                secret_ref=secret_ref,
                purpose=purpose,
                plaintext=b"test-only",
                expected_revision=0,
            )
        bootstrap = await client.post(
            "/management/v1/scopes/deployment/deployment/modules/"
            "deployment.provisioning/actions/bootstrap.create",
            headers={**headers, "idempotency-key": "bootstrap-web-1"},
            json={
                "profile_ref": "bootstrap-profile/web/1",
                "bot_id": "bot/web",
                "storage_profile_ref": "storage-profile/web/1",
                "storage_generation": 1,
                "components": components,
            },
        )
        assert bootstrap.status_code == 200
        assert bootstrap.json()["manifest_digest"].startswith("sha256:")
        client_digest_injection = await client.post(
            "/management/v1/scopes/deployment/deployment/modules/"
            "deployment.provisioning/actions/bootstrap.create",
            headers={**headers, "idempotency-key": "bootstrap-web-injected"},
            json={
                "profile_ref": "bootstrap-profile/web/2",
                "bot_id": "bot/web",
                "storage_profile_ref": "storage-profile/web/1",
                "storage_generation": 1,
                "components": components,
                "schema_digests": {},
            },
        )
        assert client_digest_injection.status_code == 422
        async with repository.store.pool.connection() as conn:
            receipts = await (
                await conn.execute(
                    "SELECT target_component_id,target_generation_id,action_key "
                    "FROM forest_deployment.web_authority_receipts ORDER BY action_key"
                )
            ).fetchall()
        assert [
            (row["target_component_id"], row["target_generation_id"], row["action_key"])
            for row in receipts
        ] == [
            (
                "deployment.provisioning",
                "deployment/deployment.provisioning/1",
                "bootstrap.create",
            ),
            (
                "deployment.secrets",
                "deployment/deployment.secrets/1",
                "secret.put",
            ),
            (
                "deployment.provisioning",
                "deployment/deployment.provisioning/1",
                "storage.create-local",
            ),
        ]
        audit_page = await client.post(
            "/management/v1/scopes/deployment/deployment/modules/"
            "deployment.audit/queries/audit.records.query",
            headers=headers,
            json={"limit": 10},
        )
        assert audit_page.status_code == 200
        audit_records = audit_page.json()["records"]
        assert {item["action"] for item in audit_records} == {
            "secret.put",
            "storage.create-local",
            "bootstrap.create",
        }
        assert {item["producer_component_id"] for item in audit_records} == {
            "deployment.secrets",
            "deployment.provisioning",
        }
        assert {item["actor_id"] for item in audit_records} == {
            "web-subject/8c6976e5b5410415bde908bd4dee15df"
        }
        assert all(item["origin_kind"] == "web.management" for item in audit_records)


async def _eventually(assertion, *, timeout: float = 3) -> None:
    async def wait() -> None:
        while True:
            if await assertion():
                return
            await asyncio.sleep(0.01)

    async with asyncio.timeout(timeout):
        await wait()


@pytest.mark.postgres
async def test_multi_bot_supervisor_reconciles_independent_hosts(deployment_catalog):
    repository = deployment_catalog
    await repository.initialize()
    bots = []
    for name in ("alpha", "beta"):
        profile = await _storage_profile(repository, f"bot/{name}", name)
        bootstrap = await _bootstrap_profile(repository, profile, name)
        bot = await repository.register_ready(
            operation_id=f"provision/{name}",
            bot_id=f"bot/{name}",
            display_name=name.title(),
            storage_profile_ref=profile.profile_ref,
            bootstrap_profile_ref=bootstrap.profile_ref,
            bootstrap_manifest_digest=bootstrap.manifest_digest,
        )
        bots.append(
            await repository.set_desired(
                operation_id=f"runtime/{name}/online",
                bot_id=bot["bot_id"],
                desired_state="ONLINE",
                expected_revision=bot["intent_revision"],
            )
        )

    instances = {}
    secrets = DeploymentSecretAuthority(
        repository.store,
        active_key_id="master/test/1",
        key_resolver=lambda _key_id: bytes(range(32)),
    )
    storage_profiles = DeploymentStorageProfileAuthority(repository.store, secrets=secrets)

    class FakeHost:
        def __init__(self, settings):
            self.settings = settings
            self.started = False
            self.closed = False
            self.registry = SimpleNamespace(
                graph_identity=SimpleNamespace(graph_id="graph/" + settings.bot_id)
            )
            instances[settings.bot_id] = self

        async def start(self):
            self.started = True

        async def close(self):
            self.closed = True

    supervisor = MultiBotSupervisor(
        repository,
        storage_profiles=storage_profiles,
        settings_resolver=lambda bot, storage: SimpleNamespace(
            bot_id=bot["bot_id"], dsn=storage.dsn
        ),
        host_factory=FakeHost,
        host_id="host/test",
        incarnation="incarnation/test",
        poll_seconds=0.05,
    )
    await supervisor.start()
    try:
        async def both_online():
            rows = await repository.list_bots()
            return len(rows) == 2 and all(row["observed_state"] == "ONLINE" for row in rows)

        await _eventually(both_online)
        assert set(instances) == {"bot/alpha", "bot/beta"}
        scopes = {item["bot_id"]: item for item in await supervisor.management_scopes()}
        assert scopes["bot/alpha"]["available"] is True
        assert scopes["bot/beta"]["available"] is True
        beta_scope = supervisor.management_scope_id("bot/beta")
        async with supervisor.management_host(beta_scope) as managed_host:
            assert managed_host is instances["bot/beta"]

        alpha = await repository.inspect("bot/alpha")
        alpha_scope = supervisor.management_scope_id("bot/alpha")
        async with supervisor.management_host(alpha_scope):
            await repository.set_desired(
                operation_id="runtime/alpha/offline",
                bot_id="bot/alpha",
                desired_state="OFFLINE",
                expected_revision=alpha["intent_revision"],
            )
            supervisor.notify()

            async def alpha_withdrawn():
                rows = {
                    item["bot_id"]: item for item in await supervisor.management_scopes()
                }
                return rows["bot/alpha"]["available"] is False

            await _eventually(alpha_withdrawn)
            assert instances["bot/alpha"].closed is False

        async def alpha_offline():
            return (await repository.inspect("bot/alpha"))["observed_state"] == "OFFLINE"

        await _eventually(alpha_offline)
        assert instances["bot/alpha"].closed is True
        assert instances["bot/beta"].closed is False
        assert (await repository.inspect("bot/beta"))["observed_state"] == "ONLINE"
        scopes = {item["bot_id"]: item for item in await supervisor.management_scopes()}
        assert scopes["bot/alpha"]["available"] is False
        with pytest.raises(ForestError) as unavailable:
            async with supervisor.management_host(
                supervisor.management_scope_id("bot/alpha")
            ):
                pass
        assert unavailable.value.code == "BOT_MANAGEMENT_UNAVAILABLE"
    finally:
        await supervisor.close()
    assert instances["bot/beta"].closed is True


@pytest.mark.postgres
async def test_multi_bot_supervisor_contains_one_bot_start_failure(deployment_catalog):
    repository = deployment_catalog
    await repository.initialize()
    for name in ("healthy", "broken"):
        profile = await _storage_profile(repository, f"bot/{name}", name)
        bootstrap = await _bootstrap_profile(repository, profile, name)
        bot = await repository.register_ready(
            operation_id=f"provision/{name}",
            bot_id=f"bot/{name}",
            display_name=name.title(),
            storage_profile_ref=profile.profile_ref,
            bootstrap_profile_ref=bootstrap.profile_ref,
            bootstrap_manifest_digest=bootstrap.manifest_digest,
        )
        await repository.set_desired(
            operation_id=f"runtime/{name}/online",
            bot_id=bot["bot_id"],
            desired_state="ONLINE",
            expected_revision=bot["intent_revision"],
        )

    class FakeHost:
        def __init__(self, settings):
            self.settings = settings
            self.registry = SimpleNamespace(
                graph_identity=SimpleNamespace(graph_id="graph/" + settings.bot_id)
            )

        async def start(self):
            if self.settings.bot_id == "bot/broken":
                raise ForestError("BOT_TEST_START_FAILED", "Expected failure", 503)

        async def close(self):
            return None

    secrets = DeploymentSecretAuthority(
        repository.store,
        active_key_id="master/test/1",
        key_resolver=lambda _key_id: bytes(range(32)),
    )
    storage_profiles = DeploymentStorageProfileAuthority(repository.store, secrets=secrets)

    supervisor = MultiBotSupervisor(
        repository,
        storage_profiles=storage_profiles,
        settings_resolver=lambda bot, storage: SimpleNamespace(
            bot_id=bot["bot_id"], dsn=storage.dsn
        ),
        host_factory=FakeHost,
        host_id="host/fault-test",
        incarnation="incarnation/fault-test",
        poll_seconds=0.05,
    )
    await supervisor.start()
    try:
        async def converged():
            rows = {row["bot_id"]: row for row in await repository.list_bots()}
            return (
                rows["bot/healthy"]["observed_state"] == "ONLINE"
                and rows["bot/broken"]["observed_state"] == "FAILED"
            )

        await _eventually(converged)
        assert supervisor.failure is None
        broken = await repository.inspect("bot/broken")
        assert broken["error_code"] == "BOT_TEST_START_FAILED"
        assert broken["retry_not_before"] is not None
    finally:
        await supervisor.close()


@pytest.mark.postgres
async def test_multi_bot_supervisor_marks_runtime_owned_task_failure(deployment_catalog):
    repository = deployment_catalog
    await repository.initialize()
    profile = await _storage_profile(repository, "bot/runtime-failure", "runtime-failure")
    bootstrap = await _bootstrap_profile(repository, profile, "runtime-failure")
    bot = await repository.register_ready(
        operation_id="provision/runtime-failure",
        bot_id="bot/runtime-failure",
        display_name="Runtime Failure",
        storage_profile_ref=profile.profile_ref,
        bootstrap_profile_ref=bootstrap.profile_ref,
        bootstrap_manifest_digest=bootstrap.manifest_digest,
    )
    await repository.set_desired(
        operation_id="runtime/runtime-failure/online",
        bot_id=bot["bot_id"],
        desired_state="ONLINE",
        expected_revision=bot["intent_revision"],
    )

    class FakeHost:
        def __init__(self, settings):
            self.settings = settings
            self.registry = SimpleNamespace(
                graph_identity=SimpleNamespace(graph_id="graph/" + settings.bot_id)
            )
            self._supervision_task = None

        async def start(self):
            async def fail():
                await asyncio.sleep(0)
                raise ForestError(
                    "MEMORY_OWNED_TASK_FAILED",
                    "Expected runtime-owned task failure",
                    503,
                )

            self._supervision_task = asyncio.create_task(fail())

        async def close(self):
            if self._supervision_task is not None:
                await asyncio.gather(self._supervision_task, return_exceptions=True)

    secrets = DeploymentSecretAuthority(
        repository.store,
        active_key_id="master/test/1",
        key_resolver=lambda _key_id: bytes(range(32)),
    )
    storage_profiles = DeploymentStorageProfileAuthority(repository.store, secrets=secrets)
    supervisor = MultiBotSupervisor(
        repository,
        storage_profiles=storage_profiles,
        settings_resolver=lambda bot, storage: SimpleNamespace(
            bot_id=bot["bot_id"], dsn=storage.dsn
        ),
        host_factory=FakeHost,
        host_id="host/runtime-failure-test",
        incarnation="incarnation/runtime-failure-test",
        poll_seconds=0.05,
    )
    await supervisor.start()
    try:
        async def failed():
            current = await repository.inspect("bot/runtime-failure")
            return current["observed_state"] == "FAILED"

        await _eventually(failed)
        current = await repository.inspect("bot/runtime-failure")
        assert current["error_code"] == "MEMORY_OWNED_TASK_FAILED"
        assert supervisor.failure is None
    finally:
        await supervisor.close()
