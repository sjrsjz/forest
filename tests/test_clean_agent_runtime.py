from __future__ import annotations

import asyncio
import hashlib
import json
import os
from datetime import datetime, timedelta, timezone
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import psycopg
import pytest
import pytest_asyncio
from psycopg import sql

from forest.agent_execution import DurableSingleAgentExecution
from forest.agent_model_executor import AgentModelStep, DurableAgentModelExecutor
from forest.builtin.agent_runtime.python.agent_protocol import (
    CORE_PROMPT,
    CORE_PROMPT_SHA256,
    PARSER_REVISION,
    PROTOCOL_ID,
    PROTOCOL_VERSION,
    parse_model_turn,
)
from forest.conversation_worker import AgentExecutionRequest
from forest.errors import ForestError
from forest.packages import digest_json
from forest.runtime_modules import official_storage_bundle
from forest.storage.agent_runtime_authority import (
    AgentInputBatch,
    AgentPayloadRef,
    AgentRuntimeRepository,
    AgentTriggerInput,
    ModelRequestPlan,
    ToolProposal,
)
from forest.storage.authorization_authority import KernelAuthorizationRepository
from forest.storage.clean_bot_store import CleanBotStore
from forest.storage.payload_authority import KernelPayloadRepository, PayloadSource
from forest.storage.schema_bundles import SchemaBundleInstaller
from forest.web_authority_ticket import WebAuthorityTicketAuthority


@pytest_asyncio.fixture
async def clean_agent_runtime():
    dsn = os.environ.get("FOREST_TEST_DSN")
    if not dsn:
        pytest.skip("FOREST_TEST_DSN must name a private PostgreSQL test instance")
    name = "forest_agent_runtime_" + uuid4().hex
    admin = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
    await admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    parsed = urlsplit(dsn)
    uri = urlunsplit((parsed.scheme, parsed.netloc, "/" + name, "", ""))
    store = CleanBotStore(uri, "bot-agent-runtime")
    await store.open()
    installer = SchemaBundleInstaller(store)
    await installer.initialize_kernel()
    payload_storage = official_storage_bundle(
        "kernel.payload",
        "forest.kernel.payload",
        "forest.kernel-payload.repository/1",
        "storage/official/kernel_payload.sql",
    )
    agent_storage = official_storage_bundle(
        "agent.runtime",
        "forest.agent.runtime",
        "forest.agent-runtime.repository/1",
        "builtin/agent_runtime/schema/agent_runtime.sql",
    )
    for storage in (payload_storage, agent_storage):
        await installer.install(
            storage.spec,
            storage.baseline,
            storage.package_digest,
            execution_profile=storage.execution_profile,
        )
    payloads = KernelPayloadRepository(store, namespace=payload_storage.spec.namespace)
    agent = AgentRuntimeRepository(
        store,
        namespace=agent_storage.spec.namespace,
        payload_verifier=payloads.verify_in,
    )
    try:
        yield store, agent_storage, payloads, agent
    finally:
        await store.close()
        assert name.startswith("forest_agent_runtime_") and len(name) == 53
        await admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))
        await admin.close()


async def _payload(
    payloads,
    *,
    owner_kind: str,
    owner_id: str,
    body: bytes,
    source_id: str = "config/source",
    owner_component_id: str = "agent.runtime",
):
    epoch = await payloads.epoch()
    owner = (owner_component_id, owner_kind, owner_id)
    result = await payloads.put_bytes(
        owner_component_id=owner[0],
        owner_kind=owner[1],
        owner_id=owner[2],
        media_type="application/json",
        content=body,
        sources=(
            PayloadSource(
                source_component_id=owner_component_id,
                source_kind="runtime.input",
                source_id=source_id,
                source_revision=1,
            ),
        ),
        expected_deletion_epoch=epoch,
    )
    return AgentPayloadRef(result.payload_id, result.content_digest, *owner)


def _json_bytes(value) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode()


def _batch(number: int, *triggers: tuple[str, str, int]):
    return AgentInputBatch(
        input_batch_ref=f"batch/{number}",
        input_batch_digest="sha256:" + f"{number:064x}",
        selected_input_end_seq=max(item[2] for item in triggers),
        context_revision=number,
        context_view_revision=number - 1,
        source_manifest_digest="sha256:" + f"{number + 100:064x}",
        triggers=tuple(AgentTriggerInput(*item) for item in triggers),
    )


async def _start(
    agent, payloads, *, suffix="one", conversation="group/one", batch_number=1
):
    config = await _payload(
        payloads,
        owner_kind="agent.config",
        owner_id=f"config/{suffix}",
        body=b'{"model":"test"}',
    )
    preset = await _payload(
        payloads,
        owner_kind="preset.context",
        owner_id=f"preset/{suffix}",
        body=b'{"system":["You are Alice"]}',
    )
    values = dict(
        operation_id=f"start/{suffix}",
        conversation_ref=conversation,
        session_id=f"session/{suffix}",
        run_id=f"run/{suffix}",
        input_batch=_batch(
            batch_number,
            (f"trigger/{suffix}/1", "principal/a", 1),
            (f"trigger/{suffix}/2", "principal/b", 2),
        ),
        agent_spec_revision="agent-spec/1",
        config=config,
        preset_revision="preset/1",
        preset=preset,
        absolute_deadline=datetime.now(timezone.utc) + timedelta(minutes=20),
    )
    return values, config, preset


@pytest.mark.postgres
async def test_clean_agent_runtime_freezes_core_prompt_and_converges_start(
    clean_agent_runtime,
):
    store, storage, payloads, agent = clean_agent_runtime
    lowered = storage.baseline.lower()
    for forbidden in (
        b"child_agent",
        b"sub_agent",
        b"conversation_triggers",
        b"input_batches",
        b"send_intent",
        b"onebot",
        b"memory_graph",
    ):
        assert forbidden not in lowered
    assert b"run_cancel_requests" in lowered
    assert CORE_PROMPT
    assert CORE_PROMPT_SHA256.startswith("sha256:")

    values, _config, _preset = await _start(agent, payloads)
    left, right = await asyncio.gather(agent.start_run(**values), agent.start_run(**values))
    assert left["run_id"] == right["run_id"] == "run/one"
    assert left["state"] == "PREPARING_MODEL"
    assert left["core_protocol_id"] == PROTOCOL_ID
    assert left["core_protocol_version"] == PROTOCOL_VERSION
    assert left["core_prompt_digest"] == CORE_PROMPT_SHA256
    assert left["parser_revision"] == PARSER_REVISION
    assert [item["principal_ref"] for item in left["triggers"]] == [
        "principal/a",
        "principal/b",
    ]
    recovery = await agent.recoverable_runs()
    assert [(item["session_id"], item["active_run_id"], item["run_state"]) for item in recovery] == [
        ("session/one", "run/one", "PREPARING_MODEL")
    ]

    conflict, _config, _preset = await _start(
        agent, payloads, suffix="other", conversation="group/one"
    )
    with pytest.raises(ForestError) as caught:
        await agent.start_run(**conflict)
    assert caught.value.code == "AGENT_SESSION_CONFLICT"

    async with store.pool.connection() as conn:
        columns = await (
            await conn.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_schema=%s "
                "AND table_name='agent_runs' ORDER BY column_name",
                (storage.spec.namespace,),
            )
        ).fetchall()
        assert "core_prompt" not in {row["column_name"] for row in columns}


@pytest.mark.postgres
async def test_run_management_query_is_filtered_and_keyset_paginated(
    clean_agent_runtime,
):
    _store, _storage, payloads, agent = clean_agent_runtime
    for batch_number, suffix, conversation in (
        (1, "alpha", "group/alpha"),
        (2, "beta", "group/beta"),
        (3, "gamma", "group/gamma"),
    ):
        values, _config, _preset = await _start(
            agent,
            payloads,
            suffix=suffix,
            conversation=conversation,
            batch_number=batch_number,
        )
        await agent.start_run(**values)

    first = await agent.query_runs(limit=2)
    assert len(first["items"]) == 2
    assert first["has_more"] is True
    second = await agent.query_runs(
        snapshot_created_at=first["snapshot_created_at"],
        before_created_at=first["next_before_created_at"],
        before_run_id=first["next_before_run_id"],
        limit=2,
    )
    assert len(second["items"]) == 1
    assert second["has_more"] is False
    assert {item["run_id"] for item in first["items"] + second["items"]} == {
        "run/alpha",
        "run/beta",
        "run/gamma",
    }

    conversation = await agent.query_runs(conversation_ref="group/alpha", limit=10)
    assert [item["run_id"] for item in conversation["items"]] == ["run/alpha"]

    states = await agent.query_runs(states=("PREPARING_MODEL",), limit=10)
    assert {item["run_id"] for item in states["items"]} == {
        "run/alpha",
        "run/beta",
        "run/gamma",
    }
    with pytest.raises(ForestError) as caught:
        await agent.query_runs(before_run_id="run/alpha")
    assert caught.value.code == "AGENT_RUNTIME_INPUT_INVALID"


@pytest.mark.postgres
async def test_cancel_request_is_checkpoint_cas_idempotent_and_unique_per_run(
    clean_agent_runtime,
):
    _store, _storage, payloads, agent = clean_agent_runtime
    values, _config, _preset = await _start(agent, payloads)
    await agent.start_run(**values)

    request = dict(
        operation_id="cancel/one",
        run_id="run/one",
        expected_checkpoint_seq=0,
        requested_by="qq:3244658766",
        reason_code="ADMIN_REQUESTED",
    )
    saved = await agent.request_cancel(**request)
    assert await agent.request_cancel(**request) == saved
    assert saved["conversation_ref"] == "group/one"
    assert (await agent.load("run/one"))["cancel_request"]["operation_id"] == "cancel/one"

    with pytest.raises(ForestError) as caught:
        await agent.request_cancel(**{**request, "reason_code": "DIFFERENT"})
    assert caught.value.code == "AGENT_CANCEL_CONFLICT"

    with pytest.raises(ForestError) as caught:
        await agent.request_cancel(**{**request, "operation_id": "cancel/two"})
    assert caught.value.code == "AGENT_CANCEL_ALREADY_REQUESTED"


@pytest.mark.postgres
async def test_web_agent_cancel_commits_receipt_and_request_in_one_transaction(
    clean_agent_runtime,
):
    store, _storage, payloads, agent = clean_agent_runtime
    authorization_storage = official_storage_bundle(
        "kernel.authorization",
        "forest.kernel.authorization",
        "forest.kernel-authorization.repository/1",
        "storage/official/kernel_authorization.sql",
    )
    installer = SchemaBundleInstaller(store)
    await installer.install(
        authorization_storage.spec,
        authorization_storage.baseline,
        authorization_storage.package_digest,
        execution_profile=authorization_storage.execution_profile,
    )
    receipts = KernelAuthorizationRepository(
        store, namespace=authorization_storage.spec.namespace
    )
    signer = WebAuthorityTicketAuthority(
        issuer_id="deployment/test",
        active_key_id="master/test/1",
        key_resolver=lambda _key_id: b"w" * 32,
    )
    session = type(
        "Session",
        (),
        {
            "session_ref": "web-session/agent-cancel",
            "session_revision": 1,
            "subject": type(
                "Subject",
                (),
                {"subject_id": "web-subject/admin", "role": "ADMIN"},
            )(),
        },
    )()

    async def issue(operation_id: str, body: dict):
        return await signer.issue(
            session=session,
            target_scope_kind="bot",
            target_scope_id=store.bot_id,
            target_component_id="agent.runtime",
            target_generation_id="generation/agent/one",
            target_graph_revision=1,
            target_operation_ref=operation_id,
            action_key="agent.run.cancel",
            input_digest=digest_json(body),
        )

    async def authorize(conn, *, ticket, **expected):
        expected = {
            "target_component_id": expected["component_id"],
            "target_operation_ref": expected["operation_id"],
            "action_key": expected["action_key"],
            "input_digest": expected["input_digest"],
            "plan_digest": expected["plan_digest"],
        }
        await signer.verify(
            ticket,
            target_scope_kind="bot",
            target_scope_id=store.bot_id,
            target_generation_id="generation/agent/one",
            target_graph_revision=1,
            required_role="ADMIN",
            **expected,
        )
        return await receipts.accept_web_authority_ticket_in(conn, ticket=ticket)

    values, _config, _preset = await _start(agent, payloads)
    await agent.start_run(**values)
    rejected_body = {"run_id": "run/one", "expected_checkpoint_seq": 1}
    with pytest.raises(ForestError) as rejected:
        await agent.request_cancel(
            operation_id="web:agent-cancel/rejected",
            run_id="run/one",
            expected_checkpoint_seq=1,
            requested_by="management.web",
            reason_code="ADMIN_REQUESTED",
            authorization=await issue("web:agent-cancel/rejected", rejected_body),
            authorization_guard=authorize,
            authorization_input_digest=digest_json(rejected_body),
        )
    assert rejected.value.code == "AGENT_CHECKPOINT_CONFLICT"

    accepted_body = {"run_id": "run/one", "expected_checkpoint_seq": 0}
    saved = await agent.request_cancel(
        operation_id="web:agent-cancel/accepted",
        run_id="run/one",
        expected_checkpoint_seq=0,
        requested_by="management.web",
        reason_code="ADMIN_REQUESTED",
        authorization=await issue("web:agent-cancel/accepted", accepted_body),
        authorization_guard=authorize,
        authorization_input_digest=digest_json(accepted_body),
    )
    assert saved["operation_id"] == "web:agent-cancel/accepted"
    async with store.pool.connection() as conn:
        rows = await (
            await conn.execute(
                sql.SQL(
                    "SELECT target_operation_ref FROM {}.web_authority_receipts "
                    "ORDER BY target_operation_ref"
                ).format(sql.Identifier(authorization_storage.spec.namespace))
            )
        ).fetchall()
    assert [row["target_operation_ref"] for row in rows] == [
        "web:agent-cancel/accepted"
    ]


@pytest.mark.postgres
async def test_cancel_request_fences_a_prepared_model_attempt_before_handoff(
    clean_agent_runtime,
):
    _store, _storage, payloads, agent = clean_agent_runtime
    values, _config, _preset = await _start(agent, payloads)
    await agent.start_run(**values)
    owner = await agent.acquire_run("run/one", "worker/cancel-fence")
    projection = await _payload(
        payloads,
        owner_kind="model.projection",
        owner_id="request/cancel-fence",
        body=b'{"messages":[]}',
        source_id="run/one",
    )
    plan = ModelRequestPlan(
        request_id="request/cancel-fence",
        request_kind="PRIMARY",
        provider_component_id="provider.test",
        provider_generation_id="generation/provider/1",
        provider_profile_ref="profile/test",
        model_ref="model/test",
        instruction_channel="SYSTEM",
        capability_digest="sha256:" + "a" * 64,
        projection=projection,
        projected_content_digest=projection.digest,
        source_manifest_digest="sha256:" + "b" * 64,
        output_token_budget=1024,
    )
    await agent.prepare_model_request(owner, plan)
    wire = await _payload(
        payloads,
        owner_kind="effect.wire",
        owner_id="attempt/cancel-fence",
        body=b'{"wire":"exact"}',
        source_id="run/one",
        owner_component_id="kernel.effect",
    )
    await agent.prepare_model_attempt(
        owner,
        request_id=plan.request_id,
        attempt_id="attempt/cancel-fence",
        effect_operation_ref="effect/model/cancel-fence",
        wire=wire,
    )
    await agent.request_cancel(
        operation_id="cancel/fence",
        run_id="run/one",
        expected_checkpoint_seq=0,
        requested_by="qq:3244658766",
        reason_code="ADMIN_REQUESTED",
    )

    with pytest.raises(ForestError) as caught:
        await agent.mark_model_handoff(
            owner,
            request_id=plan.request_id,
            attempt_id="attempt/cancel-fence",
        )
    assert caught.value.code == "AGENT_CANCEL_REQUESTED"
    loaded = await agent.load("run/one")
    assert loaded["state"] == "PREPARING_MODEL"


@pytest.mark.postgres
async def test_execution_converges_a_safe_cancel_request_to_terminal_state(
    clean_agent_runtime,
):
    _store, _storage, payloads, agent = clean_agent_runtime
    values, _config, _preset = await _start(agent, payloads)
    await agent.start_run(**values)
    owner = await agent.acquire_run("run/one", "worker/cancel-apply")
    await agent.request_cancel(
        operation_id="cancel/apply",
        run_id="run/one",
        expected_checkpoint_seq=0,
        requested_by="qq:3244658766",
        reason_code="ADMIN_REQUESTED",
    )
    execution = DurableSingleAgentExecution(
        agent=agent,
        provider=object(),
        model_effects=object(),
        memory=object(),
        media=object(),
        payloads=payloads,
        owner_id="worker/cancel-apply",
    )

    assert await execution._apply_requested_cancel(owner) is True
    loaded = await agent.load("run/one")
    assert loaded["state"] == "CANCELLED"
    assert loaded["error_code"] == "ADMIN_REQUESTED"
    assert loaded["session"]["state"] == "CANCELLED"


@pytest.mark.postgres
async def test_recovered_model_retry_limit_fails_without_another_dispatch(
    clean_agent_runtime,
):
    _store, _storage, payloads, repository = clean_agent_runtime
    values, _config, _preset = await _start(
        repository,
        payloads,
        suffix="retry-limit",
    )
    await repository.start_run(**values)
    owner = await repository.acquire_run("run/retry-limit", "worker/retry-setup")
    request_id = "model-request:" + hashlib.sha256(
        "run/retry-limit\x001".encode()
    ).hexdigest()
    projection = await _payload(
        payloads,
        owner_kind="model.projection",
        owner_id=request_id,
        body=b'{"messages":[]}',
        source_id="run/retry-limit",
    )
    plan = ModelRequestPlan(
        request_id=request_id,
        request_kind="PRIMARY",
        provider_component_id="provider.test",
        provider_generation_id="generation/provider/1",
        provider_profile_ref="profile/test",
        model_ref="model/test",
        instruction_channel="SYSTEM",
        capability_digest="sha256:" + "a" * 64,
        projection=projection,
        projected_content_digest=projection.digest,
        source_manifest_digest="sha256:" + "b" * 64,
        output_token_budget=1024,
    )
    await repository.prepare_model_request(owner, plan)
    for attempt_number in range(1, 4):
        attempt_id = f"attempt/retry-limit/{attempt_number}"
        wire = await _payload(
            payloads,
            owner_kind="effect.wire",
            owner_id=attempt_id,
            body=b'{"wire":"exact"}',
            source_id="run/retry-limit",
            owner_component_id="kernel.effect",
        )
        await repository.prepare_model_attempt(
            owner,
            request_id=request_id,
            attempt_id=attempt_id,
            effect_operation_ref=f"effect/retry-limit/{attempt_number}",
            wire=wire,
        )
        await repository.mark_model_handoff(
            owner,
            request_id=request_id,
            attempt_id=attempt_id,
        )
        observed = await repository.observe_model(
            owner,
            observation_operation_id=f"observe/retry-limit/{attempt_number}",
            request_id=request_id,
            attempt_id=attempt_id,
            observed_state="NOT_APPLIED",
            evidence_kind="EXECUTOR",
            effect_observation_ref=f"provider/retry-limit/{attempt_number}",
        )
        assert observed["accepted"] is True
    await repository.release_run(owner)

    class ActiveAgentFacade:
        bot_id = repository.bot_id

        def profile(self):
            class Profile:
                max_steps = 32

            return Profile()

        def __getattr__(self, name):
            return getattr(repository, name)

    class ForbiddenProvider:
        async def prepare_request(self, *_args, **_values):
            raise AssertionError("retry exhaustion dispatched another provider request")

    async def forbidden_callback():
        raise AssertionError("retry exhaustion crossed into conversation projection")

    execution = DurableSingleAgentExecution(
        agent=ActiveAgentFacade(),
        provider=ForbiddenProvider(),
        model_effects=object(),
        memory=object(),
        media=object(),
        payloads=payloads,
        owner_id="worker/retry-recovery",
    )
    with pytest.raises(ForestError) as caught:
        await execution.execute(
            AgentExecutionRequest(
                conversation_id="group/one",
                session_ref="session/retry-limit",
                run_ref="run/retry-limit",
                initial_batch=None,
                recovery=True,
                context_snapshot=forbidden_callback,
                next_model_input=forbidden_callback,
                before_external_step=forbidden_callback,
            )
        )
    assert caught.value.code == "AGENT_MODEL_TRANSPORT_FAILED"
    assert caught.value.details == {
        "request_id": request_id,
        "attempt_count": 3,
        "last_error_code": None,
    }
    failed = await repository.load_recovery("run/retry-limit")
    assert failed["state"] == "FAILED"
    assert failed["error_code"] == "AGENT_MODEL_TRANSPORT_FAILED"
    assert failed["session"]["state"] == "FAILED"


@pytest.mark.postgres
async def test_cancel_and_model_handoff_race_has_one_durable_order(
    clean_agent_runtime,
):
    _store, _storage, payloads, agent = clean_agent_runtime
    values, _config, _preset = await _start(agent, payloads)
    await agent.start_run(**values)
    owner = await agent.acquire_run("run/one", "worker/cancel-race")
    projection = await _payload(
        payloads,
        owner_kind="model.projection",
        owner_id="request/cancel-race",
        body=b'{"messages":[]}',
        source_id="run/one",
    )
    plan = ModelRequestPlan(
        request_id="request/cancel-race",
        request_kind="PRIMARY",
        provider_component_id="provider.test",
        provider_generation_id="generation/provider/1",
        provider_profile_ref="profile/test",
        model_ref="model/test",
        instruction_channel="SYSTEM",
        capability_digest="sha256:" + "a" * 64,
        projection=projection,
        projected_content_digest=projection.digest,
        source_manifest_digest="sha256:" + "b" * 64,
        output_token_budget=1024,
    )
    await agent.prepare_model_request(owner, plan)
    wire = await _payload(
        payloads,
        owner_kind="effect.wire",
        owner_id="attempt/cancel-race",
        body=b'{"wire":"exact"}',
        source_id="run/one",
        owner_component_id="kernel.effect",
    )
    await agent.prepare_model_attempt(
        owner,
        request_id=plan.request_id,
        attempt_id="attempt/cancel-race",
        effect_operation_ref="effect/model/cancel-race",
        wire=wire,
    )

    cancellation, handoff = await asyncio.gather(
        agent.request_cancel(
            operation_id="cancel/race",
            run_id="run/one",
            expected_checkpoint_seq=0,
            requested_by="qq:3244658766",
            reason_code="ADMIN_REQUESTED",
        ),
        agent.mark_model_handoff(
            owner,
            request_id=plan.request_id,
            attempt_id="attempt/cancel-race",
        ),
        return_exceptions=True,
    )

    assert not isinstance(cancellation, BaseException)
    loaded = await agent.load("run/one")
    assert loaded["cancel_request"]["operation_id"] == "cancel/race"
    if isinstance(handoff, BaseException):
        assert isinstance(handoff, ForestError)
        assert handoff.code == "AGENT_CANCEL_REQUESTED"
        assert loaded["state"] == "PREPARING_MODEL"
    else:
        assert handoff["state"] == "DISPATCHING"
        assert loaded["state"] == "MODEL_PENDING"


@pytest.mark.postgres
async def test_run_owner_checkpoint_safe_boundary_and_new_input_are_monotonic(
    clean_agent_runtime,
):
    store, storage, payloads, agent = clean_agent_runtime
    values, _config, _preset = await _start(agent, payloads)
    await agent.start_run(**values)
    owner = await agent.acquire_run("run/one", "worker/one")
    with pytest.raises(ForestError) as caught:
        await agent.acquire_run("run/one", "worker/two")
    assert caught.value.code == "AGENT_RUN_BUSY"

    same_phase = await _payload(
        payloads,
        owner_kind="checkpoint.state",
        owner_id="checkpoint/same",
        body=b'{"phase":"PREPARING_MODEL"}',
        source_id="run/one",
    )
    first_checkpoint = await agent.checkpoint(
        owner, checkpoint_id="checkpoint/same", state=same_phase
    )
    assert first_checkpoint["checkpoint_seq"] == 1
    assert await agent.checkpoint(
        owner, checkpoint_id="checkpoint/same", state=same_phase
    ) == first_checkpoint

    phase_chain = (
        ("PREPARING_MODEL", "MODEL_PENDING"),
        ("MODEL_PENDING", "PARSING"),
        ("PARSING", "SAFE_BOUNDARY"),
    )
    for index, (expected, target) in enumerate(phase_chain, 2):
        state = await _payload(
            payloads,
            owner_kind="checkpoint.state",
            owner_id=f"checkpoint/{index}",
            body=(f'{{"phase":"{target}"}}').encode(),
            source_id="run/one",
        )
        checkpoint = await agent.transition(
            owner,
            checkpoint_id=f"checkpoint/{index}",
            expected_phase=expected,
            target_phase=target,
            state=state,
        )
        assert checkpoint["phase"] == target
        assert checkpoint["checkpoint_seq"] == index

    second = _batch(2, ("trigger/one/3", "principal/c", 3))
    advanced = await agent.append_input(owner, second)
    assert advanced["state"] == "PREPARING_MODEL"
    assert advanced["input_revision"] == 2
    assert advanced["selected_input_end_seq"] == 3
    assert len(advanced["inputs"]) == 2
    assert [item["principal_ref"] for item in advanced["triggers"]] == [
        "principal/a",
        "principal/b",
        "principal/c",
    ]
    assert await agent.append_input(owner, second) == advanced

    async with store.pool.connection() as conn:
        with pytest.raises(psycopg.Error):
            await conn.execute(
                sql.SQL("UPDATE {}.run_inputs SET selected_input_end_seq=99 "
                        "WHERE bot_id=%s").format(sql.Identifier(storage.spec.namespace)),
                (store.bot_id,),
            )
        await conn.rollback()
        await conn.execute(
            sql.SQL("UPDATE {}.agent_runs SET owner_expires_at=clock_timestamp()-"
                    "interval '1 second' WHERE bot_id=%s AND run_id=%s").format(
                sql.Identifier(storage.spec.namespace)
            ),
            (store.bot_id, "run/one"),
        )
    replacement = await agent.acquire_run("run/one", "worker/two")
    assert replacement.epoch == owner.epoch + 1
    with pytest.raises(ForestError) as caught:
        await agent.checkpoint(owner, checkpoint_id="checkpoint/stale", state=same_phase)
    assert caught.value.code == "STALE_AGENT_RUN_OWNER"


@pytest.mark.postgres
async def test_reload_wait_is_bound_to_one_lifecycle_operation_and_resume_phase(
    clean_agent_runtime,
):
    _store, _storage, payloads, agent = clean_agent_runtime
    values, _config, _preset = await _start(agent, payloads)
    await agent.start_run(**values)
    owner = await agent.acquire_run("run/one", "worker/reload")
    state = await _payload(
        payloads,
        owner_kind="checkpoint.state",
        owner_id="checkpoint/reload",
        body=b'{"phase":"RELOAD_WAIT","operation_id":"replacement/one"}',
        source_id="run/one",
    )
    checkpoint = await agent.transition(
        owner,
        checkpoint_id="checkpoint/reload",
        expected_phase="PREPARING_MODEL",
        target_phase="RELOAD_WAIT",
        state=state,
        reload_operation_id="replacement/one",
        reload_resume_phase="PREPARING_MODEL",
        reload_generation_id="generation/agent-runtime/old",
    )
    assert checkpoint["phase"] == "RELOAD_WAIT"
    parked = await agent.load("run/one")
    assert parked["reload_operation_id"] == "replacement/one"
    assert parked["reload_resume_phase"] == "PREPARING_MODEL"
    await agent.release_run(owner)

    assert await agent.resume_reload_wait(
        "replacement/one",
        resuming_generation_id="generation/agent-runtime/old",
    ) == ()
    assert await agent.resume_reload_wait(
        "replacement/wrong",
        resuming_generation_id="generation/agent-runtime/new",
    ) == ()
    assert (await agent.load("run/one"))["state"] == "RELOAD_WAIT"
    exact = await agent.resume_reload_wait(
        "replacement/one",
        resuming_generation_id="generation/agent-runtime/new",
    )
    assert [(item["run_id"], item["conversation_ref"]) for item in exact] == [
        ("run/one", "group/one")
    ]
    resumed = await agent.load("run/one")
    assert resumed["state"] == "PREPARING_MODEL"
    assert resumed["reload_operation_id"] is None
    assert resumed["reload_resume_phase"] is None
    assert resumed["reload_generation_id"] is None
    assert await agent.resume_reload_wait(
        "replacement/one",
        resuming_generation_id="generation/agent-runtime/new",
    ) == ()

    recovery_owner = await agent.acquire_run("run/one", "worker/recovery")
    recovery_state = await _payload(
        payloads,
        owner_kind="checkpoint.state",
        owner_id="checkpoint/reload-abandoned",
        body=b'{"phase":"RELOAD_WAIT","operation_id":"replacement/abandoned"}',
        source_id="run/one",
    )
    await agent.transition(
        recovery_owner,
        checkpoint_id="checkpoint/reload-abandoned",
        expected_phase="PREPARING_MODEL",
        target_phase="RELOAD_WAIT",
        state=recovery_state,
        reload_operation_id="replacement/abandoned",
        reload_resume_phase="PREPARING_MODEL",
        reload_generation_id="generation/agent-runtime/abandoned",
    )
    await agent.release_run(recovery_owner)
    recovered = await agent.recover_reload_waits(
        resuming_generation_id="generation/agent-runtime/recovered"
    )
    assert [item["reload_operation_id"] for item in recovered] == [
        "replacement/abandoned"
    ]
    assert (await agent.load("run/one"))["state"] == "PREPARING_MODEL"


@pytest.mark.postgres
async def test_final_candidate_is_fenced_and_session_closes_only_after_drain_decision(
    clean_agent_runtime,
):
    store, storage, payloads, agent = clean_agent_runtime
    values, _config, _preset = await _start(agent, payloads)
    await agent.start_run(**values)
    owner = await agent.acquire_run("run/one", "worker/final")
    for index, (expected, target) in enumerate(
        (
            ("PREPARING_MODEL", "MODEL_PENDING"),
            ("MODEL_PENDING", "PARSING"),
            ("PARSING", "SAFE_BOUNDARY"),
        ),
        1,
    ):
        state = await _payload(
            payloads,
            owner_kind="checkpoint.state",
            owner_id=f"final/checkpoint/{index}",
            body=(f'{{"phase":"{target}"}}').encode(),
            source_id="run/one",
        )
        await agent.transition(
            owner,
            checkpoint_id=f"final/checkpoint/{index}",
            expected_phase=expected,
            target_phase=target,
            state=state,
        )
    final_state = await _payload(
        payloads,
        owner_kind="checkpoint.state",
        owner_id="final/checkpoint/ready",
        body=b'{"phase":"FINAL_READY"}',
        source_id="run/one",
    )
    candidate = await _payload(
        payloads,
        owner_kind="final.candidate",
        owner_id="reply/one",
        body=b'{"assistant":"done"}',
        source_id="run/one",
    )
    await agent.transition(
        owner,
        checkpoint_id="final/checkpoint/ready",
        expected_phase="SAFE_BOUNDARY",
        target_phase="FINAL_READY",
        state=final_state,
        final_candidate=candidate,
    )
    completed_state = await _payload(
        payloads,
        owner_kind="checkpoint.state",
        owner_id="final/checkpoint/completed",
        body=b'{"phase":"COMPLETED"}',
        source_id="run/one",
    )
    await agent.transition(
        owner,
        checkpoint_id="final/checkpoint/completed",
        expected_phase="FINAL_READY",
        target_phase="COMPLETED",
        state=completed_state,
    )
    completed = await agent.load("run/one")
    assert completed["state"] == "COMPLETED"
    assert completed["final_candidate_payload_id"] == candidate.payload_id
    assert completed["session"]["state"] == "ACTIVE"
    assert completed["session"]["active_run_id"] == "run/one"

    closed = await agent.close_session(
        operation_id="close/session/one",
        session_id="session/one",
        completed_run_id="run/one",
    )
    assert closed["completed_run_id"] == "run/one"
    assert await agent.close_session(
        operation_id="close/session/one",
        session_id="session/one",
        completed_run_id="run/one",
    ) == closed
    completed = await agent.load("run/one")
    assert completed["session"]["state"] == "CLOSED"
    assert completed["session"]["active_run_id"] is None

    with pytest.raises(ForestError) as caught:
        await agent.acquire_run("run/one", "worker/late")
    assert caught.value.code == "AGENT_RUN_TERMINAL"
    async with store.pool.connection() as conn:
        with pytest.raises(psycopg.Error):
            await conn.execute(
                sql.SQL("UPDATE {}.agent_runs SET core_prompt_digest=%s WHERE bot_id=%s").format(
                    sql.Identifier(storage.spec.namespace)
                ),
                ("sha256:" + "0" * 64, store.bot_id),
            )


@pytest.mark.postgres
async def test_completed_run_can_start_one_successor_in_the_same_session(clean_agent_runtime):
    _store, _storage, payloads, agent = clean_agent_runtime
    values, _config, _preset = await _start(agent, payloads)
    await agent.start_run(**values)
    owner = await agent.acquire_run("run/one", "worker/successor")
    state = await _payload(
        payloads,
        owner_kind="checkpoint.state",
        owner_id="successor/final-ready",
        body=b'{"phase":"FINAL_READY"}',
        source_id="run/one",
    )
    candidate = await _payload(
        payloads,
        owner_kind="final.candidate",
        owner_id="successor/reply",
        body=b'{"assistant":"done"}',
        source_id="run/one",
    )
    phase = "PREPARING_MODEL"
    for index, target in enumerate(("MODEL_PENDING", "PARSING", "SAFE_BOUNDARY"), 1):
        checkpoint = await _payload(
            payloads,
            owner_kind="checkpoint.state",
            owner_id=f"successor/phase/{index}",
            body=(f'{{"phase":"{target}"}}').encode(),
            source_id="run/one",
        )
        await agent.transition(
            owner,
            checkpoint_id=f"successor/phase/{index}",
            expected_phase=phase,
            target_phase=target,
            state=checkpoint,
        )
        phase = target
    await agent.transition(
        owner,
        checkpoint_id="successor/final-ready",
        expected_phase="SAFE_BOUNDARY",
        target_phase="FINAL_READY",
        state=state,
        final_candidate=candidate,
    )
    completed_state = await _payload(
        payloads,
        owner_kind="checkpoint.state",
        owner_id="successor/completed",
        body=b'{"phase":"COMPLETED"}',
        source_id="run/one",
    )
    await agent.transition(
        owner,
        checkpoint_id="successor/completed",
        expected_phase="FINAL_READY",
        target_phase="COMPLETED",
        state=completed_state,
    )

    second = _batch(2, ("trigger/one/3", "principal/c", 3))
    successor = await agent.start_successor_run(
        operation_id="start/successor",
        session_id="session/one",
        completed_run_id="run/one",
        run_id="run/two",
        input_batch=second,
        absolute_deadline=datetime.now(timezone.utc) + timedelta(minutes=20),
    )
    assert successor["run_id"] == "run/two"
    assert successor["session_id"] == "session/one"
    assert successor["session"]["state"] == "ACTIVE"
    assert successor["session"]["active_run_id"] == "run/two"
    assert successor["session"]["session_epoch"] == 2
    assert successor["config_payload_id"] == values["config"].payload_id
    assert await agent.start_successor_run(
        operation_id="start/successor",
        session_id="session/one",
        completed_run_id="run/one",
        run_id="run/two",
        input_batch=second,
        absolute_deadline=successor["absolute_deadline"],
    ) == successor


async def _prepare_dispatched(agent, payloads, owner, suffix: str):
    projection = await _payload(
        payloads,
        owner_kind="model.projection",
        owner_id=f"request/{suffix}",
        body=b'{"messages":[]}',
        source_id="run/one",
    )
    plan = ModelRequestPlan(
        request_id=f"request/{suffix}",
        request_kind="PRIMARY",
        provider_component_id="provider.test",
        provider_generation_id="generation/provider/1",
        provider_profile_ref="profile/test",
        model_ref="model/test",
        instruction_channel="SYSTEM",
        capability_digest="sha256:" + "a" * 64,
        projection=projection,
        projected_content_digest=projection.digest,
        source_manifest_digest="sha256:" + "b" * 64,
        output_token_budget=1024,
    )
    request = await agent.prepare_model_request(owner, plan)
    assert await agent.prepare_model_request(owner, plan) == request
    with pytest.raises(ForestError) as caught:
        await agent.append_input(
            owner,
            _batch(2, ("trigger/late/3", "principal/late", 3)),
        )
    assert caught.value.code == "AGENT_INPUT_BOUNDARY_CLOSED"
    wire = await _payload(
        payloads,
        owner_kind="effect.wire",
        owner_id=f"attempt/{suffix}",
        body=b'{"wire":"exact"}',
        source_id="run/one",
        owner_component_id="kernel.effect",
    )
    attempt = await agent.prepare_model_attempt(
        owner,
        request_id=plan.request_id,
        attempt_id=f"attempt/{suffix}",
        effect_operation_ref=f"effect/model/{suffix}",
        wire=wire,
    )
    dispatched = await agent.mark_model_handoff(
        owner, request_id=plan.request_id, attempt_id=attempt["attempt_id"]
    )
    assert dispatched["state"] == "DISPATCHING"
    return plan, attempt


async def _prepare_repair_dispatched(agent, payloads, owner, suffix: str):
    projection = await _payload(
        payloads,
        owner_kind="model.projection",
        owner_id=f"request/{suffix}",
        body=b'{"messages":["repair"]}',
        source_id="run/one",
    )
    plan = ModelRequestPlan(
        request_id=f"request/{suffix}",
        request_kind="REPAIR",
        provider_component_id="provider.test",
        provider_generation_id="generation/provider/1",
        provider_profile_ref="profile/test",
        model_ref="model/test",
        instruction_channel="SYSTEM",
        capability_digest="sha256:" + "a" * 64,
        projection=projection,
        projected_content_digest=projection.digest,
        source_manifest_digest="sha256:" + "b" * 64,
        output_token_budget=1024,
    )
    request = await agent.prepare_model_request(owner, plan)
    wire = await _payload(
        payloads,
        owner_kind="effect.wire",
        owner_id=f"attempt/{suffix}",
        body=b'{"wire":"repair"}',
        source_id="run/one",
        owner_component_id="kernel.effect",
    )
    attempt = await agent.prepare_model_attempt(
        owner,
        request_id=plan.request_id,
        attempt_id=f"attempt/{suffix}",
        effect_operation_ref=f"effect/model/{suffix}",
        wire=wire,
    )
    await agent.mark_model_handoff(
        owner,
        request_id=request["request_id"],
        attempt_id=attempt["attempt_id"],
    )
    return plan, attempt


async def _observe_candidate(agent, payloads, owner, plan, attempt, candidate: str, suffix: str):
    result = await _payload(
        payloads,
        owner_kind="model.result",
        owner_id=attempt["attempt_id"],
        body=_json_bytes(
            {
                "status": "RESPONDED",
                "request_id": plan.request_id,
                "transport_attempt_id": attempt["attempt_id"],
                "payload_digest": "sha256:" + "9" * 64,
                "text": candidate,
                "tool_calls": [],
                "usage": None,
                "finish_reason": "stop",
                "provider_response_id": f"response/{suffix}",
                "provider_model": "test",
                "error_code": None,
            }
        ),
        source_id="run/one",
        owner_component_id="provider.test",
    )
    await agent.observe_model(
        owner,
        observation_operation_id=f"observe/{suffix}/success",
        request_id=plan.request_id,
        attempt_id=attempt["attempt_id"],
        observed_state="SUCCEEDED",
        evidence_kind="EXECUTOR",
        effect_observation_ref=f"effect-observation/{suffix}/success",
        result=result,
    )


@pytest.mark.postgres
async def test_model_request_attempt_success_and_terminal_turn_are_one_agent_chain(
    clean_agent_runtime,
):
    _store, _storage, payloads, agent = clean_agent_runtime
    values, _config, _preset = await _start(agent, payloads)
    await agent.start_run(**values)
    owner = await agent.acquire_run("run/one", "worker/model")
    plan, attempt = await _prepare_dispatched(agent, payloads, owner, "terminal")
    result = await _payload(
        payloads,
        owner_kind="model.result",
        owner_id=attempt["attempt_id"],
        body=b'{"candidate":"terminal"}',
        source_id="run/one",
        owner_component_id="provider.test",
    )
    usage = await _payload(
        payloads,
        owner_kind="model.usage",
        owner_id=attempt["attempt_id"],
        body=b'{"input_tokens":10,"output_tokens":5}',
        source_id="run/one",
        owner_component_id="provider.test",
    )
    observation = await agent.observe_model(
        owner,
        observation_operation_id="observe/terminal/success",
        request_id=plan.request_id,
        attempt_id=attempt["attempt_id"],
        observed_state="SUCCEEDED",
        evidence_kind="EXECUTOR",
        effect_observation_ref="effect-observation/terminal/success",
        result=result,
        usage=usage,
    )
    assert observation["accepted"] is True
    assert (await agent.load("run/one"))["state"] == "PARSING"

    candidate = (
        '<valid_agent_protocol_content_block block_type="ModelTurn"><payload><blocks>'
        '<block block_type="Assistant"><content><text>done</text></content></block>'
        "</blocks></payload></valid_agent_protocol_content_block>"
    )
    turn = parse_model_turn(candidate)
    turn_payload = await _payload(
        payloads,
        owner_kind="model.turn",
        owner_id="turn/terminal",
        body=_json_bytes(turn.as_dict()),
        source_id="run/one",
    )
    assistant = await _payload(
        payloads,
        owner_kind="assistant.candidate",
        owner_id="turn/terminal",
        body=_json_bytes({"text": "done"}),
        source_id="run/one",
    )
    stored = await agent.record_model_turn(
        owner,
        turn_id="turn/terminal",
        request_id=plan.request_id,
        turn=turn,
        turn_payload=turn_payload,
        assistant_payload=assistant,
    )
    assert stored["terminal"] is True
    assert await agent.record_model_turn(
        owner,
        turn_id="turn/terminal",
        request_id=plan.request_id,
        turn=turn,
        turn_payload=turn_payload,
        assistant_payload=assistant,
    ) == stored
    run = await agent.load("run/one")
    assert run["state"] == "SAFE_BOUNDARY"
    assert run["next_step_seq"] == 2
    history = await agent.execution_history("run/one")
    assert len(history) == 1
    assert history[0]["turn_id"] == "turn/terminal"
    assert history[0]["input_revision"] == 1
    assert history[0]["tool_call_id"] is None
    assert (await agent.load_recovery("run/one"))["latest_turn"]["turn_id"] == (
        "turn/terminal"
    )
    resumed = await DurableAgentModelExecutor(
        agent=agent,
        provider=None,
        payloads=payloads,
    ).resume_safe_boundary(
        owner,
        AgentModelStep(
            request_id=plan.request_id,
            attempt_id=attempt["attempt_id"],
            effect_operation_ref=attempt["effect_operation_ref"],
            turn_id="turn/terminal",
            final_reply_ref="reply/recovered-terminal",
        ),
    )
    assert resumed.status == "FINAL_READY"
    assert resumed.text == "done"
    assert (await agent.load("run/one"))["state"] == "FINAL_READY"


@pytest.mark.postgres
async def test_durable_model_response_resumes_parsing_without_provider_recall(
    clean_agent_runtime,
):
    _store, _storage, payloads, agent = clean_agent_runtime
    values, _config, _preset = await _start(agent, payloads)
    await agent.start_run(**values)
    owner = await agent.acquire_run("run/one", "worker/parse-recovery")
    plan, attempt = await _prepare_dispatched(agent, payloads, owner, "parse-recovery")
    candidate = (
        '<valid_agent_protocol_content_block block_type="ModelTurn"><payload><blocks>'
        '<block block_type="Assistant"><content><text>recovered</text></content></block>'
        "</blocks></payload></valid_agent_protocol_content_block>"
    )
    result = await _payload(
        payloads,
        owner_kind="model.result",
        owner_id=attempt["attempt_id"],
        body=_json_bytes(
            {
                "status": "RESPONDED",
                "request_id": plan.request_id,
                "transport_attempt_id": attempt["attempt_id"],
                "payload_digest": "sha256:" + "9" * 64,
                "text": candidate,
                "tool_calls": [],
                "usage": None,
                "finish_reason": "stop",
                "provider_response_id": "response/recovered",
                "provider_model": "test",
                "error_code": None,
            }
        ),
        source_id="run/one",
        owner_component_id="provider.test",
    )
    await agent.observe_model(
        owner,
        observation_operation_id="observe/parse-recovery/success",
        request_id=plan.request_id,
        attempt_id=attempt["attempt_id"],
        observed_state="SUCCEEDED",
        evidence_kind="EXECUTOR",
        effect_observation_ref="effect-observation/parse-recovery/success",
        result=result,
    )
    executor = DurableAgentModelExecutor(agent=agent, provider=None, payloads=payloads)
    resumed = await executor.resume_parsing(
        owner,
        AgentModelStep(
            request_id=plan.request_id,
            attempt_id=attempt["attempt_id"],
            effect_operation_ref=attempt["effect_operation_ref"],
            turn_id="turn/parse-recovery",
            final_reply_ref="reply/parse-recovery",
        ),
    )
    assert resumed.status == "FINAL_READY"
    assert resumed.text == "recovered"
    run = await agent.load_recovery("run/one")
    assert run["state"] == "FINAL_READY"
    assert run["latest_turn"]["turn_id"] == "turn/parse-recovery"


@pytest.mark.postgres
async def test_invalid_model_turn_recovers_one_durable_repair_after_owner_restart(
    clean_agent_runtime,
):
    _store, _storage, payloads, agent = clean_agent_runtime
    values, _config, _preset = await _start(agent, payloads)
    await agent.start_run(**values)
    owner = await agent.acquire_run("run/one", "worker/invalid-primary")
    plan, attempt = await _prepare_dispatched(agent, payloads, owner, "invalid-primary")
    await _observe_candidate(agent, payloads, owner, plan, attempt, "not xml", "invalid-primary")
    executor = DurableAgentModelExecutor(agent=agent, provider=None, payloads=payloads)
    failed = await executor.resume_parsing(
        owner,
        AgentModelStep(
            request_id=plan.request_id,
            attempt_id=attempt["attempt_id"],
            effect_operation_ref=attempt["effect_operation_ref"],
            turn_id="turn/invalid-primary",
            final_reply_ref="reply/invalid-primary",
        ),
    )
    assert failed.status == "REPAIR_READY"
    recovery = await agent.load_recovery("run/one")
    assert recovery["state"] == "REPAIR_READY"
    assert recovery["repair_count"] == 1
    assert recovery["next_step_seq"] == 2
    failure = recovery["current_protocol_failure"]
    assert failure["failed_request_id"] == plan.request_id
    assert failure["failed_attempt_id"] == attempt["attempt_id"]
    assert failure["error_code"] == "AGENT_PROTOCOL_XML"
    assert failure["resulting_state"] == "REPAIR_READY"
    assert failure["repair_step_seq"] == 2

    await agent.release_run(owner)
    owner = await agent.acquire_run("run/one", "worker/repaired-after-restart")
    recovery = await agent.load_recovery("run/one")
    execution = object.__new__(DurableSingleAgentExecution)
    execution.payloads = payloads
    repair_view, repair_of = await execution._protocol_repair_view(recovery)
    assert repair_of == plan.request_id
    assert repair_view.repair_count == 1
    assert repair_view.error_code == "AGENT_PROTOCOL_XML"
    assert repair_view.validation_reason == "Model candidate is not one complete XML document"
    assert repair_view.candidate_digest == failure["candidate_digest"]
    assert repair_view.invalid_candidate == "not xml"

    repair_plan, repair_attempt = await _prepare_repair_dispatched(
        agent,
        payloads,
        owner,
        "repair",
    )
    candidate = (
        '<valid_agent_protocol_content_block block_type="ModelTurn"><payload><blocks>'
        '<block block_type="Assistant"><content><text>fixed</text></content></block>'
        "</blocks></payload></valid_agent_protocol_content_block>"
    )
    await _observe_candidate(
        agent,
        payloads,
        owner,
        repair_plan,
        repair_attempt,
        candidate,
        "repair",
    )
    repaired = await executor.resume_parsing(
        owner,
        AgentModelStep(
            request_id=repair_plan.request_id,
            attempt_id=repair_attempt["attempt_id"],
            effect_operation_ref=repair_attempt["effect_operation_ref"],
            turn_id="turn/repair",
            final_reply_ref="reply/repair",
        ),
    )
    assert repaired.status == "FINAL_READY"
    assert repaired.text == "fixed"
    final = await agent.load_recovery("run/one")
    assert final["state"] == "FINAL_READY"
    assert final["latest_turn"]["repair_of_request_id"] == plan.request_id
    assert final["latest_turn"]["request_id"] == repair_plan.request_id


@pytest.mark.postgres
async def test_second_invalid_model_turn_fails_protocol_without_a_third_dispatch(
    clean_agent_runtime,
):
    _store, _storage, payloads, agent = clean_agent_runtime
    values, _config, _preset = await _start(agent, payloads, suffix="twice")
    await agent.start_run(**values)
    owner = await agent.acquire_run("run/twice", "worker/invalid-twice")
    first_plan, first_attempt = await _prepare_dispatched(
        agent,
        payloads,
        owner,
        "first-invalid",
    )
    await _observe_candidate(
        agent,
        payloads,
        owner,
        first_plan,
        first_attempt,
        "bad one",
        "first-invalid",
    )
    executor = DurableAgentModelExecutor(agent=agent, provider=None, payloads=payloads)
    assert (
        await executor.resume_parsing(
            owner,
            AgentModelStep(
                request_id=first_plan.request_id,
                attempt_id=first_attempt["attempt_id"],
                effect_operation_ref=first_attempt["effect_operation_ref"],
                turn_id="turn/first-invalid",
                final_reply_ref="reply/first-invalid",
            ),
        )
    ).status == "REPAIR_READY"
    second_plan, second_attempt = await _prepare_repair_dispatched(
        agent,
        payloads,
        owner,
        "second-invalid",
    )
    await _observe_candidate(
        agent,
        payloads,
        owner,
        second_plan,
        second_attempt,
        "bad two",
        "second-invalid",
    )
    terminal = await executor.resume_parsing(
        owner,
        AgentModelStep(
            request_id=second_plan.request_id,
            attempt_id=second_attempt["attempt_id"],
            effect_operation_ref=second_attempt["effect_operation_ref"],
            turn_id="turn/second-invalid",
            final_reply_ref="reply/second-invalid",
        ),
    )
    assert terminal.status == "FAILED"
    assert terminal.error_code == "AGENT_PROTOCOL_FAILED"
    failed = await agent.load_recovery("run/twice")
    assert failed["state"] == "FAILED"
    assert failed["error_code"] == "AGENT_PROTOCOL_FAILED"
    assert failed["session"]["state"] == "FAILED"
    assert failed["repair_count"] == 1
    assert failed["next_step_seq"] == 3
    assert failed["current_protocol_failure"] is None


@pytest.mark.postgres
async def test_model_unknown_requires_reconciliation_and_conflicting_late_evidence_is_retained(
    clean_agent_runtime,
):
    store, storage, payloads, agent = clean_agent_runtime
    values, _config, _preset = await _start(agent, payloads)
    await agent.start_run(**values)
    owner = await agent.acquire_run("run/one", "worker/unknown")
    plan, attempt = await _prepare_dispatched(agent, payloads, owner, "unknown")
    recovery = await agent.load_recovery("run/one")
    assert recovery["current_request"]["request_id"] == plan.request_id
    assert recovery["current_attempt"]["attempt_id"] == attempt["attempt_id"]
    await agent.release_run(owner)
    owner = await agent.acquire_run("run/one", "worker/restarted")
    unknown = await DurableAgentModelExecutor(
        agent=agent,
        provider=None,
        payloads=None,
    ).reconcile_after_restart(owner)
    assert unknown.status == "UNKNOWN"
    assert unknown.error_code == "MODEL_RESULT_UNQUERYABLE_AFTER_RESTART"
    replay = await DurableAgentModelExecutor(
        agent=agent,
        provider=None,
        payloads=None,
    ).reconcile_after_restart(
        owner,
    )
    assert replay.status == "UNKNOWN"
    assert (await agent.load_recovery("run/one"))["state"] == "BLOCKED_UNKNOWN"

    result = await _payload(
        payloads,
        owner_kind="model.result",
        owner_id=attempt["attempt_id"],
        body=b'{"candidate":"late"}',
        source_id="run/one",
        owner_component_id="provider.test",
    )
    premature = await agent.observe_model(
        owner,
        observation_operation_id="observe/late-executor",
        request_id=plan.request_id,
        attempt_id=attempt["attempt_id"],
        observed_state="SUCCEEDED",
        evidence_kind="EXECUTOR",
        effect_observation_ref="effect-observation/late-executor",
        result=result,
    )
    assert premature["accepted"] is False
    success, not_applied = await asyncio.gather(
        agent.observe_model(
            owner,
            observation_operation_id="reconcile/success",
            request_id=plan.request_id,
            attempt_id=attempt["attempt_id"],
            observed_state="SUCCEEDED",
            evidence_kind="RECONCILIATION",
            effect_observation_ref="effect-observation/reconcile-success",
            result=result,
        ),
        agent.observe_model(
            owner,
            observation_operation_id="reconcile/not-applied",
            request_id=plan.request_id,
            attempt_id=attempt["attempt_id"],
            observed_state="NOT_APPLIED",
            evidence_kind="RECONCILIATION",
            effect_observation_ref="effect-observation/reconcile-not-applied",
        ),
    )
    assert [success["accepted"], not_applied["accepted"]].count(True) == 1
    winner = "SUCCEEDED" if success["accepted"] else "NOT_APPLIED"
    assert (await agent.load("run/one"))["state"] == {
        "SUCCEEDED": "PARSING",
        "NOT_APPLIED": "PREPARING_MODEL",
    }[winner]
    async with store.pool.connection() as conn:
        rows = await (
            await conn.execute(
                sql.SQL("SELECT accepted,resulting_state FROM {}.model_observations "
                        "WHERE bot_id=%s ORDER BY observed_at").format(
                    sql.Identifier(storage.spec.namespace)
                ),
                (store.bot_id,),
            )
        ).fetchall()
        assert [row["accepted"] for row in rows].count(True) == 2
        assert [row["accepted"] for row in rows].count(False) == 2
        assert rows[-1]["resulting_state"] == winner


@pytest.mark.postgres
async def test_function_call_binds_to_a_claimed_trigger_principal_without_child_agent(
    clean_agent_runtime,
):
    store, storage, payloads, agent = clean_agent_runtime
    values, _config, _preset = await _start(agent, payloads)
    await agent.start_run(**values)
    owner = await agent.acquire_run("run/one", "worker/tool")
    plan, attempt = await _prepare_dispatched(agent, payloads, owner, "tool")
    result = await _payload(
        payloads,
        owner_kind="model.result",
        owner_id=attempt["attempt_id"],
        body=b'{"candidate":"tool"}',
        source_id="run/one",
        owner_component_id="provider.test",
    )
    await agent.observe_model(
        owner,
        observation_operation_id="observe/tool/success",
        request_id=plan.request_id,
        attempt_id=attempt["attempt_id"],
        observed_state="SUCCEEDED",
        evidence_kind="EXECUTOR",
        effect_observation_ref="effect-observation/tool/success",
        result=result,
    )
    candidate = (
        '<valid_agent_protocol_content_block block_type="ModelTurn"><payload><blocks>'
        '<block block_type="Assistant"><content><text>checking</text></content></block>'
        '<block block_type="FunctionCall"><function name="lookup">'
        '<arguments encoding="json">'
        '{"authority_trigger_id":"trigger/one/1","arguments":{"key":"x"}}'
        "</arguments></function></block>"
        "</blocks></payload></valid_agent_protocol_content_block>"
    )
    turn = parse_model_turn(candidate, allowed_tools={"lookup"})
    turn_payload = await _payload(
        payloads,
        owner_kind="model.turn",
        owner_id="turn/tool",
        body=_json_bytes(turn.as_dict()),
        source_id="run/one",
    )
    assistant = await _payload(
        payloads,
        owner_kind="assistant.candidate",
        owner_id="turn/tool",
        body=_json_bytes({"text": "checking"}),
        source_id="run/one",
    )
    arguments = await _payload(
        payloads,
        owner_kind="tool.arguments",
        owner_id="tool-call/one",
        body=_json_bytes(turn.function_call.arguments),
        source_id="run/one",
    )
    proposal = ToolProposal(
        tool_call_id="tool-call/one",
        arguments=arguments,
        component_id="tool.echo",
        generation_id="generation/tool/one",
        service_id="tool.echo.execute",
        contract_digest="sha256:" + "b" * 64,
        binding_digest="sha256:" + "c" * 64,
        approval_required=True,
        supports_query=True,
        authority_trigger_ref="trigger/one/1",
        effect_operation_ref="effect/tool/one",
    )
    await agent.record_model_turn(
        owner,
        turn_id="turn/tool",
        request_id=plan.request_id,
        turn=turn,
        turn_payload=turn_payload,
        assistant_payload=assistant,
        tool=proposal,
    )
    run = await agent.load("run/one")
    assert run["state"] == "WAIT_TOOL"
    assert run["current_tool_call_id"] == "tool-call/one"
    async with store.pool.connection() as conn:
        tool = await (
            await conn.execute(
                sql.SQL("SELECT tool_name,tool_component_id,tool_generation_id,tool_service_id,"
                        "tool_contract_digest,tool_binding_digest,approval_required,supports_query,"
                        "authority_trigger_ref,authority_principal_ref,state "
                        "FROM {}.tool_calls WHERE bot_id=%s AND tool_call_id=%s").format(
                    sql.Identifier(storage.spec.namespace)
                ),
                (store.bot_id, "tool-call/one"),
            )
        ).fetchone()
        assert tool == {
            "tool_name": "lookup",
            "tool_component_id": "tool.echo",
            "tool_generation_id": "generation/tool/one",
            "tool_service_id": "tool.echo.execute",
            "tool_contract_digest": "sha256:" + "b" * 64,
            "tool_binding_digest": "sha256:" + "c" * 64,
            "approval_required": True,
            "supports_query": True,
            "authority_trigger_ref": "trigger/one/1",
            "authority_principal_ref": "principal/a",
            "state": "PROPOSED",
        }

    waiting = await agent.observe_tool(
        owner,
        observation_operation_id="tool-observe/wait-approval",
        tool_call_id="tool-call/one",
        observed_state="WAIT_APPROVAL",
        evidence_kind="AUTHORIZATION",
        approval_ref="approval/tool-call/one",
    )
    assert waiting["accepted"] is True
    assert (await agent.load("run/one"))["state"] == "WAIT_APPROVAL"
    approval_context = await agent.approval_context("approval/tool-call/one")
    assert approval_context["conversation_ref"] == "group/one"
    assert approval_context["run_id"] == "run/one"
    assert approval_context["tool_call_id"] == "tool-call/one"
    dispatching = await agent.observe_tool(
        owner,
        observation_operation_id="tool-observe/dispatching",
        tool_call_id="tool-call/one",
        observed_state="DISPATCHING",
        evidence_kind="AUTHORIZATION",
        effect_observation_ref="effect-observation/tool/dispatching",
        approval_ref="approval/tool-call/one",
    )
    assert dispatching["accepted"] is True
    assert (await agent.load("run/one"))["state"] == "WAIT_TOOL"
    unknown = await agent.observe_tool(
        owner,
        observation_operation_id="tool-observe/unknown",
        tool_call_id="tool-call/one",
        observed_state="UNKNOWN",
        evidence_kind="EXECUTOR",
        effect_observation_ref="effect-observation/tool/unknown",
    )
    assert unknown["accepted"] is True
    assert (await agent.load("run/one"))["state"] == "BLOCKED_UNKNOWN"
    tool_result = await _payload(
        payloads,
        owner_kind="tool.result",
        owner_id="tool-call/one",
        body=b'{"value":"found"}',
        source_id="run/one",
        owner_component_id="tool.test",
    )
    premature = await agent.observe_tool(
        owner,
        observation_operation_id="tool-observe/late-executor",
        tool_call_id="tool-call/one",
        observed_state="SUCCEEDED",
        evidence_kind="EXECUTOR",
        effect_observation_ref="effect-observation/tool/late-executor",
        result=tool_result,
    )
    assert premature["accepted"] is False
    assert premature["evidence_kind"] == "EXECUTOR"
    assert premature["effect_observation_ref"] == "effect-observation/tool/late-executor"
    assert premature["approval_ref"] is None
    assert premature["result_payload_id"] == tool_result.payload_id
    assert premature["result_payload_digest"] == tool_result.digest
    reconciled = await agent.observe_tool(
        owner,
        observation_operation_id="tool-reconcile/success",
        tool_call_id="tool-call/one",
        observed_state="SUCCEEDED",
        evidence_kind="RECONCILIATION",
        effect_observation_ref="effect-observation/tool/reconcile-success",
        result=tool_result,
    )
    assert reconciled["accepted"] is True
    resumed = await agent.load("run/one")
    assert resumed["state"] == "PREPARING_MODEL"
    assert resumed["current_tool_call_id"] is None

    async with store.pool.connection() as conn:
        with pytest.raises(psycopg.Error):
            await conn.execute(
                sql.SQL("UPDATE {}.tool_calls SET state='UNKNOWN' "
                        "WHERE bot_id=%s AND tool_call_id=%s").format(
                    sql.Identifier(storage.spec.namespace)
                ),
                (store.bot_id, "tool-call/one"),
            )
