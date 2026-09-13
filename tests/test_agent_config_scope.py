"""Agent legacy configuration proves no unrelated Run's unknown content."""

import hashlib

import pytest
import test_memory_pipeline_upgrade as legacy
from binding_seed import VALUES
from test_config_writer_fence import resources
from test_memory_pipeline_upgrade import old_bot
from test_messages import owner, receive
from test_payload_history import check, row
from test_upgrade import upgrade_database as upgrade_database

from forest.errors import ForestError
from forest.memory_types import SourceRef
from forest.storage import bot as bot_module
from forest.storage.agent import AgentStore
from forest.storage.bindings import BindingStore
from forest.storage.messages import MessageStore
from forest.storage.payload_history import legacy_marker
from forest.storage.payloads import PayloadStore

pytestmark = pytest.mark.postgres


@pytest.mark.parametrize("current", ["old_cross_conversation", "modern_same_conversation", "modern_cross_conversation"])
async def test_legacy_config_shared_by_two_runs_cannot_authorize_the_other_spec(
    upgrade_database, tmp_path, monkeypatch, current,
):
    # Pin this authorization regression to the complete Agent/Binding cutover;
    # the independent component/Watch family is not part of its source proof.
    prefix = resources(tmp_path / "schema-050", cutoff="050")
    monkeypatch.setattr(bot_module, "files", lambda namespace: prefix)
    monkeypatch.setattr(legacy, "OLD_ACTIONS", [*legacy.OLD_ACTIONS, "binding.plan", "binding.apply"])
    async with old_bot(upgrade_database, tmp_path) as env:
        await env.store.release_host(env.lease)
        await env.store.migrate()
        env.lease = await env.store.acquire_host("scope-upgraded", "scope-upgraded-incarnation", ttl_seconds=300)
        # The released fixture had a revision-zero Binding. Admit a real current
        # connection; rewriting its ONLINE metadata would bypass the 047 guard.
        ticket = env.ticket
        bindings = BindingStore(env.store)
        payload = {"binding_id": "binding-a", "expected_revision": 0, "values": VALUES,
                   "credentials_changed": True, "secret_digest": hashlib.sha256(b"test").hexdigest()}
        proposed = await bindings.plan(await ticket("binding.plan", "scope-binding-plan", payload),
                                       "scope-binding-plan", payload, "test")
        payload = {"binding_id": "binding-a", **{key: proposed[key] for key in
                   ("expected_revision", "plan_id", "plan_digest")}}
        await bindings.apply(await ticket("binding.apply", "scope-binding-apply", payload,
                                           plan_digest=payload["plan_digest"]), "scope-binding-apply", payload)
        binding = await row(env, "SELECT binding_epoch FROM public.bot_bindings WHERE binding_id='binding-a'")
        await bindings.activate(env.lease, "binding-a", 1, enabled=True,
                                client_incarnation="scope-current-client", expected_binding_epoch=binding["binding_epoch"])
        env.messages = MessageStore(env.store)
        local = await row(env, "SELECT * FROM public.bot_agent_runs WHERE run_id=%s", (env.retry.run_id,))
        other = await row(env, "SELECT * FROM public.bot_agent_runs WHERE run_id=%s", (env.completed.run_id,))
        assert local["spec_imported_legacy"] and other["spec_imported_legacy"]
        if current.startswith("modern"):
            group = "completed-group" if current == "modern_same_conversation" else "modern-current"
            incoming = await receive(env.messages, env.lease, "modern-trigger", group=group, trigger=True)
            actor = await owner(env.messages, env.lease, incoming["conversation_id"], "modern-actor")
            selected = await env.messages.select_input(actor, "modern-current-input")
            local = await AgentStore(env.messages).ensure_run(actor, selected["run_id"], {
                "config_revision": 1, "secret_revision": 1, "values": env.values, "tools": [],
            })
            assert local["spec_imported_legacy"] is False
        else:
            actor = await owner(env.messages, env.lease, local["conversation_id"], "scope-current")
        assert local["spec_metadata"]["config_revision"] == other["spec_metadata"]["config_revision"] == 1
        assert (local["conversation_id"] == other["conversation_id"]) == (current == "modern_same_conversation")
        await check(env, actor, [local["spec_payload_id"]], run_id=local["run_id"])
        with pytest.raises(ForestError) as cut_rejected:
            await check(env, actor, [local["spec_payload_id"]], run_id=local["run_id"], cut=0)
        assert cut_rejected.value.code == "PAYLOAD_SCOPE_DENIED"
        if current == "modern_same_conversation":
            # A published answer is already local context; that publication
            # does not publish the former Run's entire private specification.
            reply = await row(env, "SELECT payload_id FROM public.bot_conversation_replies WHERE reply_id=%s",
                              (env.completed.reply["reply_id"],))
            await check(env, actor, [reply["payload_id"]], run_id=local["run_id"])
        # Source/body identity is still present. The failure must be scope, not
        # missing migration data or an unavailable payload accidentally helping.
        assert await row(env, "SELECT 1 FROM public.bot_payload_bodies WHERE payload_id=%s", (other["spec_payload_id"],))
        with pytest.raises(ForestError) as caught:
            await check(env, actor, [other["spec_payload_id"]], run_id=local["run_id"])
        assert caught.value.code == "PAYLOAD_SCOPE_DENIED"
        if current.startswith("modern"):
            payloads = PayloadStore(env.store)
            unknown = await payloads.put_json(
                env.lease, owner_kind="kernel.result", owner_id="unpublished-unknown-cache",
                value={"private": "Unknown cached branch"},
                sources=(SourceRef(legacy_marker(env.store.bot_id)),), expected_deletion_epoch=0,
            )
            mixed = await payloads.put_json(
                env.lease, owner_kind="kernel.result", owner_id="modern-spec-plus-cache", value={"result": "mixed"},
                parents=(local["spec_payload_id"], unknown.payload_id), expected_deletion_epoch=0,
            )
            with pytest.raises(ForestError) as caught:
                await check(env, actor, [mixed.payload_id], run_id=local["run_id"])
            assert caught.value.code == "PAYLOAD_SCOPE_DENIED"
        else:
            with pytest.raises(ForestError) as caught:
                await check(env, actor, [local["spec_payload_id"]], run_id=other["run_id"])
            assert caught.value.code == "PAYLOAD_SCOPE_DENIED"
        await PayloadStore(env.store).forget_sources(
            env.lease, "forget-legacy-spec", (SourceRef(legacy_marker(env.store.bot_id)),), expected_deletion_epoch=0,
        )
        with pytest.raises(ForestError) as caught:
            await check(env, actor, [local["spec_payload_id"]], run_id=local["run_id"])
        assert caught.value.code == "PAYLOAD_SCOPE_DENIED"
