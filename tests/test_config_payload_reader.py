"""Real PostgreSQL values reads, Python interoperability and deletion fences."""

import json
from dataclasses import asdict
from types import SimpleNamespace

import psycopg
import pytest
import pytest_asyncio
from psycopg.types.json import Jsonb
from test_agent_store import prepare as model_prepare
from test_agent_store import respond as model_respond
from test_agent_store import start
from test_bot_store import database as database
from test_bot_store import store as store
from test_delivery import permit as delivery_permit
from test_messages import messages as messages
from test_messages import receive

from forest.errors import ForestError
from forest.memory_types import SourceRef
from forest.packages import digest_json
from forest.storage.config_payloads import ConfigPayloadStore
from forest.storage.control_payloads import ControlPayloadStore
from forest.storage.delivery import DeliveryStore

pytestmark = pytest.mark.postgres
IDENTITY = dict(family="config", kind="PLAN", resource="component", identity="plan-1")
READER = "public.forest_config_payload_values(%s,%s,%s,%s,%s,%s,%s)"


@pytest_asyncio.fixture
async def content(store):
    return ConfigPayloadStore(store), await store.acquire_host("host", "reader", ttl_seconds=300)


async def input_body(content, operation="input", value=None):
    config, lease = content
    async with config.store.pool.connection() as conn, conn.transaction():
        await config.store._host(conn, lease)
        original = await ControlPayloadStore(config.store).put_input_in(
            conn,
            operation,
            value,
            action="config.plan",
            epoch=0,
        )
        await config.store._host(conn, lease)
        return original


async def put(content, value, *, identity=None, parents=None):
    config, lease = content
    if parents is None:
        parents = ((await input_body(content)).payload_id,)
    async with config.store.pool.connection() as conn, conn.transaction():
        await config.store._host(conn, lease)
        ref = await config.put_values_in(
            conn,
            **(identity or IDENTITY),
            values=value,
            parents=parents,
            epoch=0,
        )
        await config.store._host(conn, lease)
        return ref


async def read(content, ref, *, identity=None, bot_id=None, digest=None, payload_id=None):
    config, lease = content
    selected = identity or IDENTITY
    args = (
        bot_id or config.store.bot_id,
        payload_id or ref.payload_id,
        digest if digest is not None else ref.digest,
        *[selected[k] for k in ("family", "kind", "resource", "identity")],
    )
    async with config.store.pool.connection() as conn, conn.transaction():
        await config.store._host(conn, lease)
        row = await (
            await conn.execute(
                "SELECT value, value IS NULL AS sql_null FROM (SELECT " + READER + " AS value) v",
                args,
            )
        ).fetchone()
        await config.store._host(conn, lease)
        return row


async def rejected(operation, code="CONFIG_PAYLOAD_IDENTITY_CONFLICT"):
    with pytest.raises(psycopg.Error) as exc:
        await operation
    assert exc.value.diag.message_primary == code
    assert exc.value.sqlstate == ("P0001" if code == "CONTROL_CONTENT_REVOKED" else "23514")


async def rows(content, query, params=()):
    async with content[0].store.pool.connection() as conn:
        return await (await conn.execute(query, params)).fetchall()


async def test_python_values_are_readable_before_any_owner_business_row(content):
    config, lease = content
    original = await input_body(content, value={"independent": "管理员输入"})
    value = {"text": "群 甲/乙", "nested": [True, None, {"key": 'a,  b\\c"d'}], "scale": 1e-8}
    async with config.store.pool.connection() as conn, conn.transaction():
        await config.store._host(conn, lease)
        for family in ("config", "agent.config", "memory.config", "binding", "watch"):
            selected = dict(family=family, kind="PLAN", resource="", identity="独立 plan")
            ref = await config.put_values_in(
                conn,
                **selected,
                values=value,
                parents=(original.payload_id,),
                epoch=0,
            )
            actual = await (
                await conn.execute(
                    "SELECT " + READER + " AS value",
                    (config.store.bot_id, ref.payload_id, ref.digest, *selected.values()),
                )
            ).fetchone()
            assert actual["value"] == value
            hydrated = await config.hydrate_values_in(
                conn,
                {"values_payload_id": ref.payload_id, "values_payload_digest": ref.digest},
                **selected,
                epoch=0,
            )
            assert hydrated["values"] == actual["value"]
        for table in (
            "bot_config_plans",
            "bot_agent_config_plans",
            "bot_memory_config_plans",
            "bot_binding_plans",
            "bot_dev_watch_heads",
        ):
            assert not await (await conn.execute("SELECT 1 FROM public." + table)).fetchall()
        await config.store._host(conn, lease)


async def test_compact_unicode_identity_limits_and_json_null_preserve_exact_values(content):
    parent = (await input_body(content)).payload_id
    for selected in (
        dict(family="config", kind="INITIAL", resource='房,  "间\\🙂', identity="界" * 1365 + "a"),
        dict(family="memory.config", kind="REVISION", resource="", identity="0"),
        dict(
            family="agent.config", kind="REVISION", resource="界" * 1365 + "b", identity="9" * 4096
        ),
    ):
        ref = await put(content, None, identity=selected, parents=(parent,))
        assert await read(content, ref, identity=selected) == {"value": None, "sql_null": False}
        assert (
            await rows(
                content,
                "SELECT owner_id FROM public.bot_payload_headers WHERE payload_id=%s",
                (ref.payload_id,),
            )
        )[0]["owner_id"] == ConfigPayloadStore.owner_id(**selected)


async def test_invalid_identity_and_receipt_metadata_never_return_body(content):
    ref = await put(content, "PRIVATE-CANARY")
    for field, values in (
        ("family", [None, "job", "", "CONFIG"]),
        ("kind", [None, "plan", "CURRENT"]),
        ("resource", [None, "x" * 4097, "界" * 1366, "line\nbreak", "delete\x7f"]),
        ("identity", [None, "", "界" * 1366, "tab\t", "delete\x7f"]),
    ):
        for value in values:
            await rejected(read(content, ref, identity={**IDENTITY, field: value}))
    for value in ("00", "01", "-1", "+1", "1.0", "1e2", "１", "1\n"):
        await rejected(
            read(content, ref, identity={**IDENTITY, "kind": "REVISION", "identity": value})
        )
    for value in ("", "sha256:" + "0" * 64, "sha256:" + "a" * 64 + "\n", "sha256:" + "A" * 64):
        await rejected(read(content, ref, digest=value))
    await rejected(read(content, ref, bot_id="another-bot"), "CONTROL_CONTENT_REVOKED")
    await rejected(read(content, ref, payload_id="payload:unknown"))


async def test_owner_encoding_and_envelope_are_checked_against_actual_bytes(content):
    config, lease = content
    parent = (await input_body(content)).payload_id
    for index, changed in enumerate(
        (
            {"family": "watch"},
            {"kind": "INITIAL"},
            {"resource": "other"},
            {"identity": "another"},
            {"unexpected": "PRIVATE-CANARY"},
        )
    ):
        selected = {**IDENTITY, "identity": "bad-envelope-" + str(index)}
        ref = await config.payloads.put_json(
            lease,
            owner_kind="control.config",
            owner_id=config.owner_id(**selected),
            value={**selected, "values": "PRIVATE-CANARY", **changed},
            parents=(parent,),
            expected_deletion_epoch=0,
        )
        await rejected(read(content, ref, identity=selected))
    for number, body in enumerate(([], None, {**IDENTITY})):
        selected = {**IDENTITY, "identity": "shape-" + str(number)}
        ref = await config.payloads.put_json(
            lease,
            owner_kind="control.config",
            owner_id=config.owner_id(**selected),
            value=body,
            parents=(parent,),
            expected_deletion_epoch=0,
        )
        await rejected(read(content, ref, identity=selected))
    wrong_owner = await config.payloads.put_json(
        lease,
        owner_kind="control.config",
        owner_id="not-the-config-owner",
        value={**IDENTITY, "values": "PRIVATE-CANARY"},
        parents=(parent,),
        expected_deletion_epoch=0,
    )
    await rejected(read(content, wrong_owner))
    selected = {**IDENTITY, "identity": "bytes"}
    ref = await config.payloads.put_bytes(
        lease,
        owner_kind="control.config",
        owner_id=config.owner_id(**selected),
        value=json.dumps({**selected, "values": "PRIVATE-CANARY"}).encode(),
        parents=(parent,),
        expected_deletion_epoch=0,
    )
    await rejected(read(content, ref, identity=selected))
    selected = {**IDENTITY, "identity": "malformed-json"}
    async with config.store.pool.connection() as conn, conn.transaction():
        await config.store._host(conn, lease)
        ref = await config.payloads._put_in(
            conn,
            owner_kind="control.config",
            owner_id=config.owner_id(**selected),
            content=b'{"values":',
            encoding="json",
            sources=(),
            parents=(parent,),
            expected_deletion_epoch=0,
        )
    await rejected(read(content, ref, identity=selected))


async def test_values_require_actual_nonempty_parents_and_accept_the_exact_limit(content):
    config, lease = content
    orphan = await config.payloads.put_json(
        lease,
        owner_kind="control.config",
        owner_id=config.owner_id(**IDENTITY),
        value={**IDENTITY, "values": "ORPHAN"},
        expected_deletion_epoch=0,
    )
    await rejected(read(content, orphan))
    async with config.store.pool.connection() as conn, conn.transaction():
        await config.store._host(conn, lease)
        parent_ids = []
        for number in range(256):
            ref = await ControlPayloadStore(config.store).put_input_in(
                conn,
                "parent:" + str(number),
                None,
                action="config.plan",
                epoch=0,
            )
            parent_ids.append(ref.payload_id)
        selected = {**IDENTITY, "identity": "full-parent-limit"}
        ref = await config.put_values_in(
            conn,
            **selected,
            values={"complete": 256},
            parents=parent_ids,
            epoch=0,
        )
    assert (await read(content, ref, identity=selected))["value"] == {"complete": 256}
    assert (
        len(
            await rows(
                content,
                "SELECT 1 FROM public.bot_payload_derivations WHERE payload_id=%s",
                (ref.payload_id,),
            )
        )
        == 256
    )


async def test_pending_header_cannot_fake_owner_or_unregistered_parent_metadata(content):
    config, lease = content
    parent = (await input_body(content)).payload_id
    # Header insertion legitimately precedes lineage/body insertion. A reader
    # must reject unfinished identities without requiring an owner business row.
    for mode in ("wrong-owner", "missing-derivation"):
        selected = {**IDENTITY, "identity": mode}
        owner = config.owner_id(**selected)
        ref = SimpleNamespace(
            payload_id="payload:" + digest_json([config.store.bot_id, "control.config", owner]),
            digest=digest_json({**selected, "values": None}),
        )
        async with config.store.pool.connection() as conn, conn.transaction():
            await config.store._host(conn, lease)
            await config.payloads.lock_in(conn)
            await conn.execute(
                "INSERT INTO public.bot_payload_headers(bot_id,payload_id,owner_kind,owner_id,"
                "encoding,digest,byte_length,input_digest,manifest,accepted_deletion_epoch) "
                "VALUES (%s,%s,'control.config',%s,'json',%s,0,%s,%s,0)",
                (
                    config.store.bot_id,
                    ref.payload_id,
                    "other:" + owner if mode == "wrong-owner" else owner,
                    ref.digest,
                    digest_json([mode]),
                    Jsonb({"parents": [parent], "sources": []}),
                ),
            )
        await rejected(read(content, ref, identity=selected))


async def test_forgetting_real_input_revokes_values_before_and_after_scrub(content):
    config, lease = content
    ref = await put(content, {"secret-free": "CONTENT-CANARY"})
    source = (
        await rows(
            content,
            "SELECT source_unit_id FROM public.bot_payload_sources WHERE payload_id=%s",
            (ref.payload_id,),
        )
    )[0]["source_unit_id"]
    assert (await read(content, ref))["value"] == {"secret-free": "CONTENT-CANARY"}
    await config.payloads.forget_sources(
        lease,
        "forget-config-source",
        (SourceRef(source),),
        expected_deletion_epoch=0,
    )
    await rejected(read(content, ref), "CONTROL_CONTENT_REVOKED")
    assert (await config.payloads.scrub(lease))["removed"] == 2
    await rejected(read(content, ref), "CONTROL_CONTENT_REVOKED")
    assert not await rows(content, "SELECT 1 FROM public.bot_payload_bodies")


async def test_scrubbed_parent_is_not_replaced_by_still_active_child_bytes(content):
    config, lease = content
    parent = await input_body(content)
    ref = await put(content, "still-present-child", parents=(parent.payload_id,))
    source = (
        await rows(
            content,
            "SELECT source_unit_id FROM public.bot_payload_sources WHERE payload_id=%s",
            (parent.payload_id,),
        )
    )[0]["source_unit_id"]
    # Exercise the independent parent-header fence with legal SQL lifecycle
    # transitions. No trigger is disabled or immutable source fact overwritten;
    # deliberately keep the child's bytes and omit global source suppression.
    async with config.store.pool.connection() as conn, conn.transaction():
        await config.store._host(conn, lease)
        await config.payloads.lock_in(conn)
        await conn.execute(
            "INSERT INTO public.bot_payload_forget_requests(bot_id,operation_id,input_digest,targets,deletion_epoch) "
            "VALUES (%s,'parent-only',%s,%s,1)",
            (config.store.bot_id, digest_json([source]), Jsonb([asdict(SourceRef(source))])),
        )
        await conn.execute(
            "UPDATE public.bot_memory_clock SET deletion_epoch=1 WHERE bot_id=%s",
            (config.store.bot_id,),
        )
        await conn.execute(
            "UPDATE public.bot_payload_headers SET state='REVOKED',forget_operation_id='parent-only' "
            "WHERE bot_id=%s AND payload_id=%s",
            (config.store.bot_id, parent.payload_id),
        )
    assert (await config.payloads.scrub(lease))["removed"] == 1
    assert (await config.payloads.read_json(lease, ref.payload_id, expected_deletion_epoch=1))[
        "values"
    ] == "still-present-child"
    await rejected(read(content, ref), "CONTROL_CONTENT_REVOKED")
    async with config.store.pool.connection() as conn, conn.transaction():
        with pytest.raises(ForestError) as exc:
            await config.hydrate_values_in(
                conn,
                {"values_payload_id": ref.payload_id, "values_payload_digest": ref.digest},
                **IDENTITY,
                epoch=1,
            )
        assert exc.value.code == "CONTROL_CONTENT_REVOKED"


async def test_real_late_echo_closure_overflow_cannot_be_truncated_by_sql_reader(messages):
    repository, lease, _ = messages
    state, actor, head = await start(messages, group="42")

    class BroadHostDelivery(DeliveryStore):
        async def enqueue_in(self, conn, lease, **kwargs):
            # Fill the actual current ancestry to its limit, including the
            # separately tracked configuration/spec/name sources.
            required = set(kwargs.get("sources", ()))
            for parent in kwargs.get("parents", ()):
                required.update(await self.contents.payloads.effective_sources_in(conn, parent))
            binding = await (await conn.execute(
                "SELECT config_payload_id FROM public.bot_bindings WHERE bot_id=%s AND binding_id=%s",
                (self.store.bot_id, kwargs["route"]["binding_id"]),
            )).fetchone()
            required.update(await self.contents.payloads.effective_sources_in(
                conn, binding["config_payload_id"]
            ))
            assert len(required) < 4096
            kwargs["sources"] = tuple(required) + tuple(
                # The exact wire also parents the effective Binding V already
                # included above. A late echo then adds the 4097th source.
                SourceRef("broad-source:" + str(i)) for i in range(4096 - len(required)))
            return await super().enqueue_in(conn, lease, **kwargs)

    delivery = BroadHostDelivery(repository.store)
    _, request, permit = await model_prepare(state, actor, head)
    await model_respond(state, actor, permit, request, text="wide output")
    await state.commit_final(actor, head["run_id"], "wide-output", delivery)
    dispatch = await delivery_permit(delivery, lease, await delivery.claim(lease, "sender"))
    await delivery.mark_started(dispatch)
    echo = await receive(repository, lease, 999, group="42", user="1000", text="wide output")
    content = ConfigPayloadStore(repository.store), lease
    parent = (
        await rows(
            content,
            "SELECT payload_id FROM public.bot_message_inbox WHERE event_id=%s",
            (echo["event_id"],),
        )
    )[0]["payload_id"]
    ref = await put(content, "copy made before echo proof", parents=(parent,))
    assert (await read(content, ref))["value"] == "copy made before echo proof"
    await delivery.complete(dispatch, "SENT", {"message_id": "999"})
    assert await repository.reconcile_exact_echo(lease, echo["event_id"])
    closure = await rows(
        content,
        "SELECT s.* FROM public.bot_payload_headers h CROSS JOIN LATERAL "
        "public.forest_source_dependency_closure(h.bot_id,h.manifest->'sources',5000) s WHERE h.payload_id=%s",
        (ref.payload_id,),
    )
    assert len(closure) > 4096
    await rejected(read(content, ref), "CONTROL_CONTENT_REVOKED")
    assert (
        await rows(
            content,
            "SELECT state FROM public.bot_payload_headers WHERE payload_id=%s",
            (ref.payload_id,),
        )
    )[0]["state"] == "ACTIVE"
