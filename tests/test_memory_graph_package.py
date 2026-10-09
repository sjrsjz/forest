import asyncio
import base64
import hashlib
import json
import os
import socket
import sys
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
from jsonschema import Draft202012Validator
from psycopg import errors, sql
from psycopg.types.json import Jsonb
from test_deployment_management_app import _IdentityAuthority
from test_schema_bundles import clean_authority as clean_authority

from forest.builtin.memory_graph.python.facet_preparation import document_facets, query_part_facets
from forest.builtin_packages import bootstrap_builtin_package
from forest.core_runtime import bootstrap_kernel
from forest.deployment_management_app import create_deployment_management_app
from forest.errors import ForestError
from forest.kernel_payload_participant import KernelPayloadUnitOfWorkParticipant
from forest.management_gateway import ManagementGateway
from forest.packages import PackageCatalog, digest_json
from forest.runtime_contracts import FrozenServiceRoute, RuntimeServiceInvocation
from forest.runtime_modules import RuntimeModuleRegistry
from forest.storage.deployment_web_identity import AuthenticatedWebSession, WebSubject
from forest.storage.management_asset_authority import ManagementAssetAuthority
from forest.storage.module_configuration import ModuleConfigurationRepository
from forest.storage.payload_authority import PayloadSource
from forest.web_authority_ticket import WebAuthorityTicketAuthority


def _isolated_memory_descriptors(kernel, package):
    # These focused fixtures omit Worker Host; the full Bot Host fixture
    # exercises its management and configuration retention edges.
    return tuple(
        replace(descriptor, retention_requires=tuple(
            component_id for component_id in descriptor.retention_requires
            if component_id != "component.worker-host"
        ))
        for descriptor in (*kernel.descriptors, package.descriptor)
    )


def _trial_owner_id(query_id: str, part_id: str) -> str:
    return "trial:" + hashlib.sha256(
        (query_id + "\0" + part_id).encode("utf-8")
    ).hexdigest()


@pytest.mark.postgres
async def test_trunk_directory_pages_beyond_old_limit(clean_authority, tmp_path):
    store, installer = clean_authority
    kernel = await bootstrap_kernel(store, installer)
    package = await bootstrap_builtin_package(
        Path(__file__).parents[1] / "src/forest/builtin/memory_graph",
        PackageCatalog(tmp_path / "memory-packages"), store, installer, kernel=kernel,
        initial_configuration=_configuration(), resolved_contracts={},
    )
    namespace = sql.Identifier(package.descriptor.storage.spec.namespace)
    async with store.pool.connection() as conn, conn.transaction():
        await conn.execute(sql.SQL(
            "INSERT INTO {}.memory_sources "
            "(bot_id,source_id,registration_operation_id,input_digest,source_kind,"
            "platform,realm_id,external_group_id,identity_incarnation,identity_digest,"
            "trunk_id) "
            "SELECT %s,'source/test/'||g,'register/test/'||g,%s,'GROUP',"
            "'onebot','qq','group-'||g,1,"
            "'sha256:'||repeat(lpad(to_hex(g),4,'0'),16),"
            "'trunk/test/'||lpad(g::text,4,'0') "
            "FROM generate_series(1,4105) AS g"
        ).format(namespace), (store.bot_id, "sha256:" + "a" * 64))
        await conn.execute(sql.SQL(
            "INSERT INTO {}.memory_trunks(bot_id,trunk_id,source_id) "
            "SELECT bot_id,trunk_id,source_id FROM {}.memory_sources "
            "WHERE bot_id=%s AND source_id LIKE 'source/test/%%'"
        ).format(namespace, namespace), (store.bot_id,))

    async def page(input_value):
        async with store.pool.connection() as conn:
            row = await (await conn.execute(sql.SQL(
                "SELECT {}.memory_list_trunks(%s::jsonb,%s::jsonb)"
            ).format(namespace), (
                Jsonb({"bot_id": store.bot_id}), Jsonb(input_value),
            ))).fetchone()
        result = row["memory_list_trunks"]
        assert result["ok"] is True
        return result["value"]

    first = await page({"limit": 100})
    assert len(first["items"]) == 100
    assert first["has_more"] is True
    assert first["next_after"] == first["items"][-1]["trunk_id"]
    second = await page({"limit": 100, "after_trunk_id": first["next_after"]})
    assert len(second["items"]) == 100
    assert second["has_more"] is True
    assert {item["trunk_id"] for item in first["items"]}.isdisjoint(
        item["trunk_id"] for item in second["items"]
    )
    tail = await page({"limit": 100, "after_trunk_id": "trunk/test/4095"})
    assert len(tail["items"]) == 10
    assert tail["has_more"] is False and tail["next_after"] is None
    exact = await page({"limit": 1, "external_group_id": "group-120"})
    assert [item["external_group_id"] for item in exact["items"]] == ["group-120"]
    assert exact["has_more"] is False


@pytest.mark.postgres
async def test_embedding_claim_budgets_bounded_image_renditions(clean_authority, tmp_path):
    store, installer = clean_authority
    kernel = await bootstrap_kernel(store, installer)
    package = await bootstrap_builtin_package(
        Path(__file__).parents[1] / "src/forest/builtin/memory_graph",
        PackageCatalog(tmp_path / "memory-packages"), store, installer, kernel=kernel,
        initial_configuration=_configuration(), resolved_contracts={},
    )
    namespace = sql.Identifier(package.descriptor.storage.spec.namespace)
    digest = "sha256:" + "a" * 64
    space = "sha256:" + "b" * 64
    async with store.pool.connection() as conn, conn.transaction():
        await conn.execute(sql.SQL(
            "INSERT INTO {}.memory_domains "
            "(bot_id,initialization_operation_id,input_digest,space_id,"
            "embedding_model_kind,model_revision_digest,dimensions,max_batch_items,"
            "normalization,fusion_policy_revision) VALUES "
            "(%s,'domain/large-images',%s,%s,'TEXT_IMAGE',%s,4096,8,'L2','facets/1')"
        ).format(namespace), (store.bot_id, digest, space, digest))
        await conn.execute(sql.SQL(
            "INSERT INTO {}.memory_sources "
            "(bot_id,source_id,registration_operation_id,input_digest,source_kind,"
            "platform,realm_id,external_group_id,identity_incarnation,identity_digest,"
            "trunk_id) VALUES "
            "(%s,'source/images','register/images',%s,'GROUP','onebot','qq',"
            "'933179609',1,%s,'trunk/images')"
        ).format(namespace), (store.bot_id, digest, digest))
        await conn.execute(sql.SQL(
            "INSERT INTO {}.memory_trunks(bot_id,trunk_id,source_id) "
            "VALUES (%s,'trunk/images','source/images')"
        ).format(namespace), (store.bot_id,))
        for index, byte_length in enumerate((12, 6, 1), start=1):
            unit = f"unit/image/{index}"
            part = f"part/image/{index}"
            await conn.execute(sql.SQL(
                "INSERT INTO {}.source_units "
                "(bot_id,source_unit_id,capture_operation_id,input_digest,source_id,"
                "trunk_id,source_order_seq,source_revision,unit_kind,modality,part_count,"
                "content_manifest_digest,author_subject_ref,author_payload_id,"
                "author_payload_digest,author_owner_component_id,author_owner_kind,"
                "author_owner_id,accepted_deletion_epoch,eligibility_state,eligible_at,"
                "occurred_at) VALUES "
                "(%s,%s,%s,%s,'source/images','trunk/images',%s,1,'message','IMAGE',1,"
                "%s,'onebot:user:42',%s,%s,'adapter.onebot','message.author',%s,0,"
                "'ELIGIBLE',clock_timestamp(),clock_timestamp())"
            ).format(namespace), (
                store.bot_id, unit, f"capture/image/{index}", digest, index, digest,
                f"author/image/{index}", digest, f"event/image/{index}",
            ))
            await conn.execute(sql.SQL(
                "INSERT INTO {}.source_parts "
                "(bot_id,source_unit_id,part_id,part_index,part_kind,media_type,"
                "payload_id,payload_digest,payload_owner_component_id,payload_owner_kind,"
                "payload_owner_id,width,height,byte_length) VALUES "
                "(%s,%s,%s,0,'IMAGE','image/png',%s,%s,'adapter.onebot',"
                "'context.image',%s,2048,2048,%s)"
            ).format(namespace), (
                store.bot_id, unit, part, f"payload/image/{index}", digest,
                f"payload-owner/image/{index}", byte_length * 1024 * 1024,
            ))
            await conn.execute(sql.SQL(
                "INSERT INTO {}.source_occurrences "
                "(bot_id,occurrence_id,source_unit_id,binding_ref,conversation_ref,"
                "event_ref,eligibility_state,input_batch_ref,context_revision,observed_at) "
                "VALUES (%s,%s,%s,'onebot:group:933179609',"
                "'onebot:group:933179609',%s,'ELIGIBLE',%s,1,clock_timestamp())"
            ).format(namespace), (
                store.bot_id, f"occurrence/image/{index}", unit,
                f"event/image/{index}", f"batch/image/{index}",
            ))
            await conn.execute(sql.SQL(
                "INSERT INTO {}.embedding_facets "
                "(bot_id,facet_id,purpose,input_kind,input_ref,input_revision,"
                "source_unit_id,source_revision,part_id,modality,input_payload_id,"
                "input_payload_digest,facet_content_digest,input_media_type,"
                "input_byte_length,space_id,accepted_deletion_epoch) VALUES "
                "(%s,%s,'DOCUMENT','SOURCE_UNIT',%s,1,%s,1,%s,'IMAGE',%s,%s,%s,"
                "'image/png',%s,%s,0)"
            ).format(namespace), (
                store.bot_id, f"facet/image/{index}", unit, unit, part,
                f"payload/image/{index}", digest, digest,
                byte_length * 1024 * 1024, space,
            ))

    async def claim(token):
        async with store.pool.connection() as conn:
            return (await (await conn.execute(sql.SQL(
                "SELECT {}.memory_claim_embedding_batch(%s,%s) AS result"
            ).format(namespace), (
                Jsonb({"bot_id": store.bot_id, "generation_id": "generation/images"}),
                Jsonb({"protocol": "forest.owned-work-claim-request/1",
                       "claim_token": token, "lease_seconds": 360}),
            ))).fetchone())["result"]

    first = await claim("claim/images/first")
    assert first["ok"], first
    assert [item["facet_id"] for item in first["value"]["payload"]["facets"]] == [
        "facet/image/1", "facet/image/2",
    ]
    rejection = {
        "protocol": "forest.memory-embedding-facet-reject/1",
        "claim_id": "claim/images/first", "facet_id": "facet/image/1",
        "facet_content_digest": digest, "space_id": space,
        "reason_code": "EMBEDDING_FACET_INVALID",
    }
    async with store.pool.connection() as conn, conn.transaction():
        rejected = (await (await conn.execute(sql.SQL(
            "SELECT {}.memory_reject_embedding_facet(%s,%s) AS result"
        ).format(namespace), (
            Jsonb({"bot_id": store.bot_id, "generation_id": "generation/images",
                   "operation_id": "reject/images/first",
                   "participant_input_digest": digest_json(rejection)}),
            Jsonb({"protocol": "forest.uow-step/1",
                   "caller": {"component_id": "memory.graph",
                              "generation_id": "generation/images"},
                   "input": rejection, "prior": {}}),
        ))).fetchone())["result"]
        assert rejected["ok"], rejected
        assert rejected["value"]["state"] == "FAILED"
        state = (await (await conn.execute(sql.SQL(
            "SELECT state,failure_code FROM {}.embedding_facets "
            "WHERE bot_id=%s AND facet_id='facet/image/1'"
        ).format(namespace), (store.bot_id,))).fetchone())
        assert state == {"state": "FAILED", "failure_code": "EMBEDDING_FACET_INVALID"}
        source_page = (await (await conn.execute(sql.SQL(
            "SELECT {}.memory_query_sources(%s,%s) AS result"
        ).format(namespace), (
            Jsonb({"bot_id": store.bot_id}),
            Jsonb({"limit": 50, "source_id": "source/images"}),
        ))).fetchone())["result"]
        assert source_page["ok"], source_page
        assert sum(item["failed_document_facet_count"]
                   for item in source_page["value"]["items"]) == 1
    second = await claim("claim/images/second")
    assert second["ok"], second
    assert [item["facet_id"] for item in second["value"]["payload"]["facets"]] == [
        "facet/image/2", "facet/image/3",
    ]

    # Stale claims without a prepared attempt are retired in bounded batches.
    # The live second claim must remain untouched while this backlog drains.
    async with store.pool.connection() as conn, conn.transaction():
        await conn.execute(sql.SQL(
            "INSERT INTO {}.embedding_claims("
            "bot_id,claim_id,purpose,space_id,source_epoch_high_watermark,"
            "lease_seconds,lease_deadline,item_limit,item_count,claim_payload) "
            "SELECT %s,'stale/image/'||lpad(n::text,3,'0'),'DOCUMENT',%s,0,"
            "3,clock_timestamp()-interval '1 second',1,0,'{{}}'::jsonb "
            "FROM generate_series(1,70) AS n"
        ).format(namespace), (store.bot_id, space))
    for round_no, expired_count in enumerate((32, 64, 70), 1):
        empty = await claim(f"claim/images/drain/{round_no}")
        assert empty["ok"] and empty["value"] is None
        async with store.pool.connection() as conn:
            counts = await (await conn.execute(sql.SQL(
                "SELECT count(*) FILTER (WHERE state='EXPIRED') AS expired,"
                "count(*) FILTER (WHERE state='CLAIMED') AS active "
                "FROM {}.embedding_claims WHERE bot_id=%s "
                "AND left(claim_id,12)='stale/image/'"
            ).format(namespace), (store.bot_id,))).fetchone()
            live = await (await conn.execute(sql.SQL(
                "SELECT state FROM {}.embedding_claims "
                "WHERE bot_id=%s AND claim_id='claim/images/second'"
            ).format(namespace), (store.bot_id,))).fetchone()
        assert counts == {"expired": expired_count, "active": 70 - expired_count}
        assert live["state"] == "CLAIMED"


async def _browser_apply_real_forget_batch(host, selection_id: str) -> str:
    import uvicorn
    from playwright.sync_api import expect, sync_playwright

    bot_id = host.settings.bot_id

    class Supervisor:
        async def management_scopes(self):
            return ({
                "scope_id": bot_id, "scope_kind": "bot", "bot_id": bot_id,
                "label": "Memory batch integration", "available": True,
                "desired_state": "ONLINE", "observed_state": "ONLINE",
            },)

        @asynccontextmanager
        async def management_host(self, scope_id):
            assert scope_id == bot_id
            yield host

    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.bind(("127.0.0.1", 0))
    listener.listen(128)
    origin = f"http://127.0.0.1:{listener.getsockname()[1]}"
    identity = _IdentityAuthority()
    identity.subject = WebSubject("admin/one", "admin", "Administrator", "ADMIN", True, 1)
    app = create_deployment_management_app(
        deployment_id="deployment/real-memory-batch",         identity_authority=identity, allowed_origin=origin,
        bot_supervisor=Supervisor(),
    )
    server = uvicorn.Server(uvicorn.Config(
        app, host="127.0.0.1", port=listener.getsockname()[1], log_level="error",
    ))
    serving = asyncio.create_task(server.serve(sockets=[listener]))
    try:
        async with asyncio.timeout(10):
            while not server.started:
                if serving.done():
                    await serving
                await asyncio.sleep(0.05)
        def drive_browser():
            executable = os.environ.get("FOREST_BROWSER_EXECUTABLE")
            if executable is None and sys.platform == "win32":
                edge = Path("C:/Program Files (x86)/Microsoft/Edge/Application/msedge.exe")
                if edge.is_file():
                    executable = str(edge)
            with sync_playwright() as playwright:
                browser = playwright.chromium.launch(
                    headless=True,
                    **({"executable_path": executable} if executable else {}),
                )
                try:
                    page = browser.new_page()
                    page.set_default_timeout(60_000)
                    errors = []
                    page.on("pageerror", lambda error: errors.append(str(error)))
                    page.goto(origin + "/")
                    expect(page.locator("#login")).to_be_visible()
                    page.locator('#login-form [name="username"]').fill("admin")
                    page.locator('#login-form [name="password"]').fill("correct-password")
                    page.locator("#login-form button").click()
                    expect(page.locator("#console")).to_be_visible()
                    page.locator("#scope-select").select_option(f"bot:{bot_id}")
                    page.locator("#module-nav button", has_text="memory.graph").click()
                    frame = page.frame_locator("iframe.package-ui-frame")
                    expect(frame.locator('[data-tab="forget"]')).to_be_visible()
                    frame.locator('[data-tab="forget"]').click()
                    frame.locator("#forget details summary").click()
                    frame.locator("#batch-selection-id").fill(selection_id)
                    frame.locator("#batch-load").click()
                    expect(frame.locator("#batch-selection-meta")).to_contain_text("冻结成员 1 个")
                    expect(frame.locator("#batch-members")).to_contain_text("unit/browser")
                    frame.locator("#batch-preview").click()
                    expect(frame.locator("#batch-impact")).to_contain_text("来源 1")
                    frame.locator("#batch-confirm").check()
                    frame.locator("#batch-enqueue").click()
                    expect(frame.locator("#batch-state")).to_contain_text("SUCCEEDED")
                    expect(frame.locator("#batch-selection-meta")).to_contain_text("全部完成")
                    assert not errors, errors
                    return frame.locator("#batch-request").input_value()
                finally:
                    browser.close()

        previous_policy = asyncio.get_event_loop_policy()
        try:
            if sys.platform == "win32":
                asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
            return await asyncio.to_thread(drive_browser)
        finally:
            asyncio.set_event_loop_policy(previous_policy)
    finally:
        server.should_exit = True
        await asyncio.wait_for(serving, timeout=15)


@pytest.mark.postgres
async def test_embedding_budget_serializes_claims_and_keeps_unknown_spend(
    clean_authority, tmp_path
):
    store, installer = clean_authority
    kernel = await bootstrap_kernel(store, installer)
    package = await bootstrap_builtin_package(
        Path(__file__).parents[1] / "src/forest/builtin/memory_graph",
        PackageCatalog(tmp_path / "budget-packages"),
        store, installer, kernel=kernel, initial_configuration=_configuration(),
        resolved_contracts={},
    )
    namespace = package.descriptor.storage.spec.namespace

    async def invoke(function, operation, body):
        context = {
            "bot_id": store.bot_id,
            "operation_id": operation,
            "participant_input_digest": digest_json(body),
        }
        envelope = {
            "protocol": "forest.uow-step/1",
            "caller": {"component_id": "memory.graph", "generation_id": "generation/1"},
            "input": body,
            "prior": {},
        }
        async with store.pool.connection() as conn, conn.transaction():
            row = await (
                await conn.execute(
                    sql.SQL("SELECT {}.{}(%s::jsonb,%s::jsonb) AS result").format(
                        sql.Identifier(namespace), sql.Identifier(function)
                    ),
                    (Jsonb(context), Jsonb(envelope)),
                )
            ).fetchone()
        return row["result"]

    bound = await invoke("memory_bind_domain", "domain/budget", {
        "protocol": "forest.memory-domain-bind/1",
        "space_id": "sha256:" + "a" * 64,
        "embedding_model_kind": "TEXT_IMAGE",
        "model_revision_digest": "sha256:" + "b" * 64,
        "dimensions": 4096,
        "max_batch_items": 8,
        "normalization": "L2",
        "fusion_policy_revision": "independent-facets/1",
    })
    assert bound["ok"] is True

    def reserve_body(name, *, request_limit=1):
        return {
            "protocol": "forest.memory-embedding-budget-reserve/1",
            "reservation_id": f"budget/{name}",
            "batch_ref": f"batch/{name}",
            "policy_revision": "daily-cost-cap/1",
            "request_limit": request_limit,
            "token_limit": 1000,
            "reserved_tokens": 100,
        }

    first, second = await asyncio.gather(
        invoke("memory_reserve_embedding_budget", "reserve/one", reserve_body("one")),
        invoke("memory_reserve_embedding_budget", "reserve/two", reserve_body("two")),
    )
    assert sorted(item["ok"] for item in (first, second)) == [False, True]
    winner = first if first["ok"] else second
    loser = second if first["ok"] else first
    assert loser["error"]["code"] == "MEMORY_EMBEDDING_BUDGET_EXHAUSTED"
    name = "one" if first["ok"] else "two"
    operation = f"reserve/{name}"
    assert await invoke(
        "memory_reserve_embedding_budget", operation, reserve_body(name)
    ) == winner
    same_batch = reserve_body("different-id")
    same_batch["batch_ref"] = f"batch/{name}"
    conflict = await invoke(
        "memory_reserve_embedding_budget", "reserve/different-id", same_batch
    )
    assert conflict["error"]["code"] == "MEMORY_EMBEDDING_BUDGET_CONFLICT"

    unknown = {
        "protocol": "forest.memory-embedding-budget-settle/1",
        "reservation_id": f"budget/{name}",
        "state": "UNKNOWN", "charged_tokens": None,
        "error_code": "EMBEDDING_TRANSPORT_UNKNOWN",
    }
    settled = await invoke("memory_settle_embedding_budget", "settle/unknown", unknown)
    assert settled["value"]["state"] == "UNKNOWN"
    assert await invoke(
        "memory_settle_embedding_budget", "settle/unknown", unknown
    ) == settled
    conflicting = await invoke(
        "memory_settle_embedding_budget", "settle/different", unknown
    )
    assert conflicting["error"]["code"] == "MEMORY_EMBEDDING_BUDGET_CONFLICT"
    exhausted = await invoke(
        "memory_reserve_embedding_budget", "reserve/after-unknown",
        reserve_body("after-unknown"),
    )
    assert exhausted["error"]["code"] == "MEMORY_EMBEDDING_BUDGET_EXHAUSTED"

    charged = await invoke(
        "memory_reserve_embedding_budget", "reserve/charged",
        reserve_body("charged", request_limit=2),
    )
    assert charged["value"]["state"] == "RESERVED"
    result = await invoke("memory_settle_embedding_budget", "settle/charged", {
        "protocol": "forest.memory-embedding-budget-settle/1",
        "reservation_id": "budget/charged",
        "state": "CHARGED", "charged_tokens": 150, "error_code": None,
    })
    assert result["value"]["charged_tokens"] == 150
    released = await invoke(
        "memory_reserve_embedding_budget", "reserve/released",
        reserve_body("released", request_limit=3),
    )
    assert released["value"]["state"] == "RESERVED"
    for name in ("released", "after-release"):
        if name == "after-release":
            admitted = await invoke(
                "memory_reserve_embedding_budget", f"reserve/{name}",
                reserve_body(name, request_limit=3),
            )
            assert admitted["value"]["state"] == "RESERVED"
        settled_release = await invoke("memory_settle_embedding_budget", f"settle/{name}", {
            "protocol": "forest.memory-embedding-budget-settle/1",
            "reservation_id": f"budget/{name}",
            "state": "RELEASED", "charged_tokens": None, "error_code": None,
        })
        assert settled_release["value"]["state"] == "RELEASED"
    async with store.pool.connection() as conn:
        row = await (
            await conn.execute(
                sql.SQL("SELECT reserved_requests,reserved_tokens,settled_requests,"
                        "charged_tokens,unknown_requests,unknown_tokens "
                        "FROM {}.embedding_budget_days WHERE bot_id=%s").format(
                    sql.Identifier(namespace)
                ),
                (store.bot_id,),
            )
        ).fetchone()
    assert tuple(row.values()) == (0, 0, 1, 150, 1, 100)


@pytest.mark.postgres
async def test_direct_source_identity_collision_is_rejected_before_capture_writes(
    clean_authority, tmp_path
):
    store, installer = clean_authority
    kernel = await bootstrap_kernel(store, installer)
    package = await bootstrap_builtin_package(
        Path(__file__).parents[1] / "src/forest/builtin/memory_graph",
        PackageCatalog(tmp_path / "source-identity-packages"),
        store, installer, kernel=kernel, initial_configuration=_configuration(),
        resolved_contracts={},
    )
    namespace = package.descriptor.storage.spec.namespace
    identity = {
        "source_id": "source/direct/one",
        "source_kind": "DIRECT",
        "platform": "test-platform",
        "realm_id": "qq",
        "external_group_id": None,
        "identity_incarnation": 1,
        "trunk_id": None,
    }
    async with store.pool.connection() as conn, conn.transaction():
        digest_row = await (
            await conn.execute(
                "SELECT 'sha256:'||encode(sha256(convert_to(%s::jsonb::text,'UTF8')),'hex') "
                "AS digest",
                (Jsonb(identity),),
            )
        ).fetchone()
        for source_id, identity_digest in (
            (identity["source_id"], "sha256:" + "a" * 64),
            ("source/direct/two", digest_row["digest"]),
        ):
            await conn.execute(
                sql.SQL("INSERT INTO {}.memory_sources("
                        "bot_id,source_id,registration_operation_id,input_digest,"
                        "source_kind,platform,realm_id,identity_incarnation,identity_digest) "
                        "VALUES(%s,%s,%s,%s,'DIRECT','test-platform','qq',1,%s)")
                .format(sql.Identifier(namespace)),
                (store.bot_id, source_id, "register:" + source_id,
                 "sha256:" + "b" * 64, identity_digest),
            )
        body = {
            "deletion_epoch_step_id": "epoch",
            "capture": {
                "identity": identity,
                "source_unit_id": "unit/direct/one",
                "occurrence": {
                    "conversation_ref": "conversation/direct/one",
                    "event_ref": "event/direct/one",
                    "ingress_seq": 1,
                },
                "author": {},
                "parts": [{}],
            },
        }
        result = await (
            await conn.execute(
                sql.SQL("SELECT {}.memory_capture_source(%s::jsonb,%s::jsonb) AS result")
                .format(sql.Identifier(namespace)),
                (
                    Jsonb({
                        "bot_id": store.bot_id,
                        "operation_id": "capture/direct/collision",
                        "participant_input_digest": digest_json(body),
                    }),
                    Jsonb({
                        "protocol": "forest.uow-step/1",
                        "caller": {"component_id": "memory.graph"},
                        "input": body,
                        "prior": {
                            "epoch": {
                                "protocol": "forest.payload-epoch/1",
                                "deletion_epoch": 0,
                            },
                        },
                    }),
                ),
            )
        ).fetchone()
        sources = await (
            await conn.execute(
                sql.SQL("SELECT count(*) AS count FROM {}.memory_sources WHERE bot_id=%s")
                .format(sql.Identifier(namespace)),
                (store.bot_id,),
            )
        ).fetchone()
    assert result["result"]["error"]["code"] == "MEMORY_SOURCE_IDENTITY_CONFLICT"
    assert sources["count"] == 2


@pytest.mark.postgres
async def test_pending_archive_and_late_capture_preserve_batch_identity(
    clean_authority, tmp_path
):
    store, installer = clean_authority
    kernel = await bootstrap_kernel(store, installer)
    package = await bootstrap_builtin_package(
        Path(__file__).parents[1] / "src/forest/builtin/memory_graph",
        PackageCatalog(tmp_path / "late-capture-packages"),
        store, installer, kernel=kernel, initial_configuration=_configuration(),
        resolved_contracts={},
    )
    namespace = package.descriptor.storage.spec.namespace
    conversation = "conversation/late-capture"
    digest = "sha256:" + "a" * 64

    async def archive(conn, event, batch):
        envelope = {
            "protocol": "forest.uow-step/1",
            "caller": {"component_id": "conversation.scheduler"},
            "input": {},
            "prior": {
                "prepare": {
                    "status": "WRITE",
                    "commit": {
                        "conversation_id": conversation,
                        "compaction_id": "compaction/" + event,
                        "archived_unit_count": 1,
                        "units": [{
                            "event_id": event,
                            "input_batch_id": batch,
                            "context_revision": 1,
                        }],
                    },
                },
                "commit": {
                    "conversation_id": conversation,
                    "memory_archive": {
                        "compaction_ref": "compaction/" + event,
                        "event_refs": [event],
                    },
                },
                "payload_epoch": {
                    "protocol": "forest.payload-epoch/1",
                    "deletion_epoch": 0,
                },
            },
        }
        return (await (await conn.execute(
            sql.SQL("SELECT {}.memory_qualify_archive(%s::jsonb,%s::jsonb) AS result")
            .format(sql.Identifier(namespace)),
            (Jsonb({
                "bot_id": store.bot_id,
                "operation_id": "archive/" + event,
                "participant_input_digest": digest,
            }), Jsonb(envelope)),
        )).fetchone())["result"]

    async with store.pool.connection() as conn, conn.transaction():
        await conn.execute(
            sql.SQL("INSERT INTO {}.pending_input_events("
                    "bot_id,conversation_ref,event_ref,operation_id,input_digest,"
                    "input_batch_ref,context_revision,accepted_deletion_epoch) "
                    "VALUES(%s,%s,%s,%s,%s,%s,1,0)")
            .format(sql.Identifier(namespace)),
            (store.bot_id, conversation, "event/conflict", "input/conflict",
             digest, "batch/selected"),
        )
        rejected = await archive(conn, "event/conflict", "batch/different")
        assert rejected["error"]["code"] == "MEMORY_ARCHIVE_CONFLICT"
        conflict_pending = await (await conn.execute(
            sql.SQL("SELECT count(*) AS count FROM {}.pending_archive_events "
                    "WHERE bot_id=%s AND event_ref=%s")
            .format(sql.Identifier(namespace)),
            (store.bot_id, "event/conflict"),
        )).fetchone()
        assert conflict_pending["count"] == 0

        pending = await archive(conn, "event/late", "batch/late")
        assert pending["ok"] is True
        before = await (await conn.execute(
            sql.SQL("SELECT state FROM {}.pending_archive_events "
                    "WHERE bot_id=%s AND event_ref=%s")
            .format(sql.Identifier(namespace)),
            (store.bot_id, "event/late"),
        )).fetchone()
        assert before["state"] == "PENDING_CAPTURE"

        capture_body = {
            "deletion_epoch_step_id": "epoch",
            "capture": {
                "identity": {
                    "source_id": "source/direct/late",
                    "source_kind": "DIRECT",
                    "platform": "test-platform",
                    "realm_id": "qq",
                    "external_group_id": None,
                    "identity_incarnation": 1,
                    "trunk_id": None,
                },
                "source_unit_id": "unit/direct/late",
                "source_revision": 1,
                "unit_kind": "MESSAGE",
                "author_subject_ref": "qq:42",
                "author": {
                    "payload_id": "payload/author/late",
                    "digest": digest,
                    "owner_component_id": "adapter.onebot",
                    "owner_kind": "message.author",
                    "owner_id": "event/late",
                },
                "parts": [{
                    "part_id": "part/late/text",
                    "part_index": 0,
                    "part_kind": "TEXT",
                    "media_type": "text/plain",
                    "payload": {
                        "payload_id": "payload/text/late",
                        "digest": digest,
                        "owner_component_id": "adapter.onebot",
                        "owner_kind": "context.input",
                        "owner_id": "unit/direct/late:text",
                    },
                    "byte_length": 4,
                }],
                "occurrence": {
                    "occurrence_id": "occurrence/late",
                    "binding_ref": "binding/main",
                    "conversation_ref": conversation,
                    "event_ref": "event/late",
                    "ingress_seq": 1,
                    "observed_at": "2026-10-01T00:00:00+00:00",
                },
                "occurred_at": "2026-10-01T00:00:00+00:00",
            },
        }
        captured = (await (await conn.execute(
            sql.SQL("SELECT {}.memory_capture_source(%s::jsonb,%s::jsonb) AS result")
            .format(sql.Identifier(namespace)),
            (Jsonb({
                "bot_id": store.bot_id,
                "operation_id": "capture/late",
                "participant_input_digest": digest_json(capture_body),
            }), Jsonb({
                "protocol": "forest.uow-step/1",
                "caller": {"component_id": "memory.graph"},
                "input": capture_body,
                "prior": {"epoch": {
                    "protocol": "forest.payload-epoch/1",
                    "deletion_epoch": 0,
                }},
            })),
        )).fetchone())["result"]
        assert captured["value"]["eligibility_state"] == "ELIGIBLE"
        after = await (await conn.execute(
            sql.SQL("SELECT state,input_batch_ref,source_unit_id FROM "
                    "{}.pending_archive_events WHERE bot_id=%s AND event_ref=%s")
            .format(sql.Identifier(namespace)),
            (store.bot_id, "event/late"),
        )).fetchone()
        assert after == {
            "state": "RESOLVED",
            "input_batch_ref": "batch/late",
            "source_unit_id": "unit/direct/late",
        }


@pytest.mark.postgres
async def test_graph_snapshot_excludes_facts_committed_after_registration(
    clean_authority, tmp_path
):
    store, installer = clean_authority
    kernel = await bootstrap_kernel(store, installer)
    package = await bootstrap_builtin_package(
        Path(__file__).parents[1] / "src/forest/builtin/memory_graph",
        PackageCatalog(tmp_path / "snapshot-packages"),
        store, installer, kernel=kernel, initial_configuration=_configuration(),
        resolved_contracts={},
    )
    namespace = sql.Identifier(package.descriptor.storage.spec.namespace)
    async with store.pool.connection() as writer, store.pool.connection() as reader:
        async with writer.transaction():
            await writer.execute(
                sql.SQL(
                    "INSERT INTO {}.relation_contributions "
                    "(bot_id,contribution_id,operation_id,input_digest,relation_id,"
                    "relation_kind,left_kind,left_id,right_kind,right_id,origin_kind,"
                    "weight_micros,admitted_deletion_epoch,cross_trunk) VALUES "
                    "(%s,'late-contribution','late-operation',%s,'late-relation',"
                    "'ASSOCIATES_WITH','ENTITY','left','ENTITY','right','RULE',500000,0,false)"
                ).format(namespace),
                (store.bot_id, "sha256:" + "a" * 64),
            )
            boundary = await (
                await reader.execute(
                    "SELECT pg_current_snapshot()::text AS snapshot, clock_timestamp() AS observed_at"
                )
            ).fetchone()
        result = await (
            await reader.execute(
                sql.SQL(
                    "SELECT pg_visible_in_snapshot(created_xid,%s::pg_snapshot) AS old_visible,"
                    "pg_visible_in_snapshot(created_xid,pg_current_snapshot()) AS new_visible,"
                    "created_at <= %s AS timestamp_precedes_boundary "
                    "FROM {}.relation_contributions WHERE bot_id=%s AND contribution_id=%s"
                ).format(namespace),
                (boundary["snapshot"], boundary["observed_at"], store.bot_id, "late-contribution"),
            )
        ).fetchone()
        assert result == {
            "old_visible": False,
            "new_visible": True,
            "timestamp_precedes_boundary": True,
        }
        context = Jsonb({"bot_id": store.bot_id})
        async def browse(values):
            row = await (
                await reader.execute(
                    sql.SQL("SELECT {}.memory_query_relations(%s,%s) AS result").format(namespace),
                    (context, Jsonb({"endpoint_kind": "ENTITY", "endpoint_id": "left", **values})),
                )
            ).fetchone()
            assert row["result"]["ok"] is True
            Draft202012Validator(package.snapshot.schema(
                "schemas/query-relations-output.json"
            )).validate(row["result"]["value"])
            return row["result"]["value"]

        assert (await browse({"snapshot_xid": boundary["snapshot"]}))["items"] == []
        current = await browse({"origin_kinds": ["RULE"], "cross_trunk": False})
        assert len(current["items"]) == 1
        assert current["items"][0]["direction"] == "UNDIRECTED"
        assert current["items"][0]["origin_kind"] == "RULE"
        assert current["items"][0]["cross_trunk"] is False
        assert (await browse({"snapshot_xid": current["snapshot_xid"],
                             "relation_kinds": ["CONTRADICTS"]}))["items"] == []
        async def global_browse(values):
            row = await (
                await reader.execute(
                    sql.SQL("SELECT {}.memory_search_relations(%s,%s) AS result").format(namespace),
                    (context, Jsonb(values)),
                )
            ).fetchone()
            assert row["result"]["ok"] is True
            Draft202012Validator(package.snapshot.schema(
                "schemas/query-relations-output.json"
            )).validate(row["result"]["value"])
            return row["result"]["value"]

        assert (await global_browse({"snapshot_xid": boundary["snapshot"]}))["items"] == []
        async with writer.transaction():
            await writer.execute(
                sql.SQL(
                    "INSERT INTO {}.relation_contributions "
                    "(bot_id,contribution_id,operation_id,input_digest,relation_id,"
                    "relation_kind,left_kind,left_id,right_kind,right_id,origin_kind,"
                    "weight_micros,admitted_deletion_epoch,cross_trunk) VALUES "
                    "(%s,'directed-contribution','directed-operation',%s,'directed-relation',"
                    "'CONTRADICTS','ENTITY','left','ENTITY','right','MANUAL',500000,0,false)"
                ).format(namespace),
                (store.bot_id, "sha256:" + "b" * 64),
            )
        assert (await global_browse({
            "snapshot_xid": current["snapshot_xid"],
            "relation_kinds": ["CONTRADICTS"],
        }))["items"] == []
        directed = await global_browse({"relation_kinds": ["CONTRADICTS"]})
        assert [item["direction"] for item in directed["items"]] == ["CANONICAL"]


@pytest.mark.postgres
async def test_memory_neighbourhood_browses_two_hops_with_shared_snapshot_and_bounds(
    clean_authority, tmp_path
):
    store, installer = clean_authority
    kernel = await bootstrap_kernel(store, installer)
    package = await bootstrap_builtin_package(
        Path(__file__).parents[1] / "src/forest/builtin/memory_graph",
        PackageCatalog(tmp_path / "neighbourhood-packages"),
        store, installer, kernel=kernel, initial_configuration=_configuration(),
        resolved_contracts={},
    )
    namespace = sql.Identifier(package.descriptor.storage.spec.namespace)
    async with store.pool.connection() as conn, conn.transaction():
        for left, right in (("a", "b"), ("b", "c")):
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}.relation_contributions "
                    "(bot_id,contribution_id,operation_id,input_digest,relation_id,"
                    "relation_kind,left_kind,left_id,right_kind,right_id,origin_kind,"
                    "weight_micros,admitted_deletion_epoch,cross_trunk) VALUES "
                    "(%s,%s,%s,%s,%s,'ASSOCIATES_WITH','ENTITY',%s,'ENTITY',%s,"
                    "'RULE',500000,0,false)"
                ).format(namespace),
                (
                    store.bot_id, f"edge/{left}-{right}", f"operation/{left}-{right}",
                    "sha256:" + "a" * 64, f"relation/{left}-{right}", left, right,
                ),
            )

    async def browse(**budget):
        async with store.pool.connection() as conn:
            row = await (
                await conn.execute(
                    sql.SQL("SELECT {}.memory_query_neighbourhood(%s,%s) AS result").format(
                        namespace
                    ),
                    (Jsonb({"bot_id": store.bot_id}), Jsonb({
                        "endpoint_kind": "ENTITY", "endpoint_id": "a", **budget,
                    })),
                )
            ).fetchone()
        assert row["result"]["ok"] is True
        value = row["result"]["value"]
        Draft202012Validator(package.snapshot.schema(
            "schemas/query-neighbourhood-output.json"
        )).validate(value)
        return value

    full = await browse(max_hops=2, max_nodes=3, max_edges=2)
    assert [(node["id"], node["depth"]) for node in full["nodes"]] == [
        ("a", 0), ("b", 1), ("c", 2)
    ]
    assert [(edge["from_id"], edge["traversal_depth"]) for edge in full["edges"]] == [
        ("a", 1), ("b", 2)
    ]
    assert full["truncated"] is False
    limited = await browse(max_hops=2, max_nodes=2, max_edges=2)
    assert [edge["relation_id"] for edge in limited["edges"]] == ["relation/a-b"]
    assert limited["truncation_reason"] == "NODE_LIMIT"
    edge_limited = await browse(max_hops=2, max_nodes=3, max_edges=1)
    assert [edge["relation_id"] for edge in edge_limited["edges"]] == [
        "relation/a-b"
    ]
    assert edge_limited["truncation_reason"] == "EDGE_LIMIT"
    one_hop = await browse(max_hops=1, max_nodes=3, max_edges=2)
    assert [edge["relation_id"] for edge in one_hop["edges"]] == ["relation/a-b"]
    assert one_hop["truncated"] is False


@pytest.mark.postgres
async def test_source_cursor_excludes_source_committed_after_first_page(
    clean_authority, tmp_path
):
    store, installer = clean_authority
    kernel = await bootstrap_kernel(store, installer)
    package = await bootstrap_builtin_package(
        Path(__file__).parents[1] / "src/forest/builtin/memory_graph",
        PackageCatalog(tmp_path / "source-cursor-packages"),
        store, installer, kernel=kernel, initial_configuration=_configuration(),
        resolved_contracts={},
    )
    namespace = sql.Identifier(package.descriptor.storage.spec.namespace)
    digest = "sha256:" + "a" * 64
    async with store.pool.connection() as conn, conn.transaction():
        await conn.execute(
            sql.SQL(
                "INSERT INTO {}.memory_sources "
                "(bot_id,source_id,registration_operation_id,input_digest,source_kind,"
                "identity_incarnation,identity_digest) "
                "VALUES (%s,'source/cursor','register/cursor',%s,'DOCUMENT',1,%s)"
            ).format(namespace),
            (store.bot_id, digest, digest),
        )

    async def insert_unit(conn, name, order, occurred):
        await conn.execute(
            sql.SQL(
                "INSERT INTO {}.source_units "
                "(bot_id,source_unit_id,capture_operation_id,input_digest,source_id,"
                "source_order_seq,source_revision,unit_kind,modality,part_count,"
                "content_manifest_digest,author_subject_ref,author_payload_id,"
                "author_payload_digest,author_owner_component_id,author_owner_kind,"
                "author_owner_id,accepted_deletion_epoch,eligibility_state,"
                "eligible_at,eligible_xid,occurred_at) VALUES "
                "(%s,%s,%s,%s,'source/cursor',%s,1,'MESSAGE','TEXT',1,"
                "%s,'author/test',%s,%s,'memory.graph','test',%s,0,'ELIGIBLE',"
                "clock_timestamp(),pg_current_xact_id(),%s::timestamptz)"
            ).format(namespace),
            (
                store.bot_id, f"unit/{name}", f"capture/{name}", digest,
                order, digest, f"payload/{name}", digest, f"unit/{name}", occurred,
            ),
        )

    async with store.pool.connection() as conn, conn.transaction():
        await insert_unit(conn, "newest", 1, "2026-01-03T00:00:00Z")
        await insert_unit(conn, "oldest", 3, "2026-01-01T00:00:00Z")
        for name in ("newest", "oldest"):
            await conn.execute(sql.SQL(
                "INSERT INTO {}.source_parts "
                "(bot_id,source_unit_id,part_id,part_index,part_kind,media_type,"
                "payload_id,payload_digest,payload_owner_component_id,"
                "payload_owner_kind,payload_owner_id) VALUES "
                "(%s,%s,%s,0,'TEXT','text/plain',%s,%s,'memory.graph','test',%s)"
            ).format(namespace), (
                store.bot_id, f"unit/{name}", f"part/{name}",
                f"payload/{name}", digest, f"unit/{name}",
            ))
        for name, index, state in (
            ("newest", 0, "READY"), ("newest", 1, "UNKNOWN"),
            ("oldest", 0, "READY"),
        ):
            facet_id = f"facet/{name}/{index}"
            await conn.execute(sql.SQL(
                "INSERT INTO {}.embedding_facets "
                "(bot_id,facet_id,purpose,input_kind,input_ref,input_revision,"
                "source_unit_id,source_revision,part_id,modality,selector_start_char,"
                "selector_end_char,input_payload_id,input_payload_digest,"
                "facet_content_digest,input_media_type,input_byte_length,space_id,"
                "accepted_deletion_epoch,state,index_ref_id,vector_digest,"
                "result_payload_id,result_payload_digest,published_at) VALUES "
                "(%s,%s,'DOCUMENT','SOURCE_UNIT',%s,1,%s,1,%s,'TEXT',%s,%s,"
                "%s,%s,%s,'text/plain',8,'space/cursor',0,%s,%s,%s,%s,%s,%s)"
            ).format(namespace), (
                store.bot_id, facet_id, f"unit/{name}", f"unit/{name}",
                f"part/{name}", index, index + 1, f"payload/{name}",
                digest, digest, state,
                facet_id if state == "READY" else None,
                digest if state == "READY" else None,
                f"result/{facet_id}" if state == "READY" else None,
                digest if state == "READY" else None,
                "2026-01-01T00:00:00Z" if state == "READY" else None,
            ))

    async with store.pool.connection() as writer, store.pool.connection() as reader:
        async with writer.transaction():
            await insert_unit(writer, "late", 2, "2026-01-02T00:00:00Z")
            first = await (
                await reader.execute(
                    sql.SQL("SELECT {}.memory_query_sources(%s,%s) AS result").format(namespace),
                    (Jsonb({"bot_id": store.bot_id}), Jsonb({"limit": 1})),
                )
            ).fetchone()
            first = first["result"]["value"]
            assert [item["source_unit_id"] for item in first["items"]] == ["unit/newest"]
            assert (first["items"][0]["index_coverage"],
                    first["items"][0]["ready_document_facet_count"],
                    first["items"][0]["document_facet_count"],
                    first["items"][0]["unknown_document_facet_count"]) == (
                        "PARTIAL", 1, 2, 1
                    )
            assert first["has_more"] is True
        cursor = {
            "limit": 1,
            "snapshot_accepted_at": first["snapshot_accepted_at"],
            "snapshot_xid": first["snapshot_xid"],
            "after_occurred_at": first["next_cursor"]["occurred_at"],
            "after_source_unit_id": first["next_cursor"]["source_unit_id"],
            "cursor_filter_digest": first["filter_digest"],
        }
        second = await (
            await reader.execute(
                sql.SQL("SELECT {}.memory_query_sources(%s,%s) AS result").format(namespace),
                (Jsonb({"bot_id": store.bot_id}), Jsonb(cursor)),
            )
        ).fetchone()
        assert second["result"]["ok"] is True
        assert [item["source_unit_id"] for item in second["result"]["value"]["items"]] == [
            "unit/oldest"
        ]
        assert second["result"]["value"]["items"][0]["index_coverage"] == "COMPLETE"
        changed_boundary = await (
            await reader.execute(
                sql.SQL("SELECT {}.memory_query_sources(%s,%s) AS result").format(namespace),
                (Jsonb({"bot_id": store.bot_id}), Jsonb({
                    **cursor, "snapshot_accepted_at": "2030-01-01T00:00:00Z"
                })),
            )
        ).fetchone()
        assert changed_boundary["result"]["error"]["code"] == "MEMORY_QUERY_INVALID"
        fresh = await (
            await reader.execute(
                sql.SQL("SELECT {}.memory_query_sources(%s,%s) AS result").format(namespace),
                (Jsonb({"bot_id": store.bot_id}), Jsonb({"limit": 3})),
            )
        ).fetchone()
        Draft202012Validator(package.snapshot.schema(
            "schemas/query-sources-output.json"
        )).validate(fresh["result"]["value"])
        assert [item["source_unit_id"] for item in fresh["result"]["value"]["items"]] == [
            "unit/newest", "unit/late", "unit/oldest"
        ]
        exact = await (
            await reader.execute(
                sql.SQL("SELECT {}.memory_query_sources(%s,%s) AS result").format(namespace),
                (Jsonb({"bot_id": store.bot_id}),
                 Jsonb({"source_unit_id": "unit/oldest"})),
            )
        ).fetchone()
        assert [item["source_unit_id"] for item in exact["result"]["value"]["items"]] == [
            "unit/oldest"
        ]
        missing = await (
            await reader.execute(
                sql.SQL("SELECT {}.memory_query_sources(%s,%s) AS result").format(namespace),
                (Jsonb({"bot_id": store.bot_id}),
                 Jsonb({"source_unit_id": "unit/absent"})),
            )
        ).fetchone()
        assert missing["result"]["value"]["items"] == []
        exact_unknown = await (
            await reader.execute(
                sql.SQL("SELECT {}.memory_query_sources(%s,%s) AS result").format(namespace),
                (Jsonb({"bot_id": store.bot_id}), Jsonb({
                    "source_unit_id": "unit/oldest", "only_unknown_facets": True,
                })),
            )
        ).fetchone()
        assert exact_unknown["result"]["value"]["items"] == []
        selected_unknown = await (await reader.execute(
            sql.SQL("SELECT {}.memory_query_sources(%s,%s) AS result").format(namespace),
            (Jsonb({"bot_id": store.bot_id}), Jsonb({"only_unknown_facets": True})),
        )).fetchone()
        assert [item["source_unit_id"] for item in
                selected_unknown["result"]["value"]["items"]] == ["unit/newest"]
        assert [item["unknown_document_facet_count"] for item in
                fresh["result"]["value"]["items"]] == [1, 0, 0]
        first_facet = await (await reader.execute(
            sql.SQL("SELECT {}.memory_query_source_embedding_facets(%s,%s) AS result").format(
                namespace
            ),
            (Jsonb({"bot_id": store.bot_id}),
             Jsonb({"source_unit_id": "unit/newest", "limit": 1})),
        )).fetchone()
        facet_page = first_facet["result"]["value"]
        Draft202012Validator(package.snapshot.schema(
            "schemas/query-source-facets-output.json"
        )).validate(facet_page)
        assert [item["state"] for item in facet_page["items"]] == ["READY"]
        assert facet_page["has_more"] is True
        second_facet = await (await reader.execute(
            sql.SQL("SELECT {}.memory_query_source_embedding_facets(%s,%s) AS result").format(
                namespace
            ),
            (Jsonb({"bot_id": store.bot_id}), Jsonb({
                "source_unit_id": "unit/newest", "state": "UNKNOWN",
                "after_facet_id": facet_page["next_after"],
            })),
        )).fetchone()
        assert [item["facet_id"] for item in
                second_facet["result"]["value"]["items"]] == ["facet/newest/1"]
        assert second_facet["result"]["value"]["items"][0]["attempt_id"] is None
        for coverage, expected in (
            ("PARTIAL", "unit/newest"),
            ("NO_FACETS", "unit/late"),
            ("COMPLETE", "unit/oldest"),
        ):
            selected = await (await reader.execute(
                sql.SQL("SELECT {}.memory_query_sources(%s,%s) AS result").format(namespace),
                (Jsonb({"bot_id": store.bot_id}),
                 Jsonb({"index_coverage": coverage})),
            )).fetchone()
            assert [item["source_unit_id"] for item in selected["result"]["value"]["items"]] == [
                expected
            ]
        frozen = await (await reader.execute(sql.SQL(
            "SELECT {}.memory_freeze_forget_selection(%s,%s) AS result"
        ).format(namespace), (
            Jsonb({"bot_id": store.bot_id, "operation_id": "freeze/complete"}),
            Jsonb({"selection_id": "selection/complete",
                   "requested_by": "admin/coverage",
                   "filters": {"index_coverage": "COMPLETE"}}),
        ))).fetchone()
        assert frozen["result"]["value"]["member_count"] == 1
        member = await (await reader.execute(sql.SQL(
            "SELECT {}.memory_query_forget_selection(%s,%s) AS result"
        ).format(namespace), (
            Jsonb({"bot_id": store.bot_id}),
            Jsonb({"selection_id": "selection/complete",
                   "requested_by": "admin/coverage"}),
        ))).fetchone()
        assert member["result"]["value"]["items"][0]["source_unit_id"] == "unit/oldest"


@pytest.mark.postgres
async def test_forget_selection_freezes_filtered_members_and_actor_bound_pages(
    clean_authority, tmp_path
):
    store, installer = clean_authority
    kernel = await bootstrap_kernel(store, installer)
    package = await bootstrap_builtin_package(
        Path(__file__).parents[1] / "src/forest/builtin/memory_graph",
        PackageCatalog(tmp_path / "forget-selection-packages"),
        store, installer, kernel=kernel, initial_configuration=_configuration(),
        resolved_contracts={},
    )
    namespace = sql.Identifier(package.descriptor.storage.spec.namespace)
    digest = "sha256:" + "a" * 64
    async with store.pool.connection() as conn, conn.transaction():
        await conn.execute(sql.SQL(
            "INSERT INTO {}.memory_sources "
            "(bot_id,source_id,registration_operation_id,input_digest,source_kind,"
            "identity_incarnation,identity_digest) "
            "VALUES (%s,'source/selection','register/selection',%s,'DOCUMENT',1,%s)"
        ).format(namespace), (store.bot_id, digest, digest))

    async def insert_unit(name, order, author):
        epoch = await kernel.payload.epoch()
        root = await kernel.payload.put_bytes(
            owner_component_id="conversation.scheduler", owner_kind="context.input",
            owner_id=f"input/{name}", media_type="text/plain",
            content=f"message {name}".encode(),
            sources=(PayloadSource(
                "conversation.scheduler", "group.message", f"message/{name}", 1,
            ),), expected_deletion_epoch=epoch,
        )
        async with store.pool.connection() as conn, conn.transaction():
            await conn.execute(sql.SQL(
                "INSERT INTO {}.source_units "
                "(bot_id,source_unit_id,capture_operation_id,input_digest,source_id,"
                "source_order_seq,source_revision,unit_kind,modality,part_count,"
                "content_manifest_digest,author_subject_ref,author_payload_id,"
                "author_payload_digest,author_owner_component_id,author_owner_kind,"
                "author_owner_id,accepted_deletion_epoch,eligibility_state,"
                "eligible_at,eligible_xid,occurred_at) VALUES "
                "(%s,%s,%s,%s,'source/selection',%s,1,'MESSAGE','TEXT',1,"
                "%s,%s,%s,%s,'conversation.scheduler','context.input',%s,%s,'ELIGIBLE',"
                "clock_timestamp(),pg_current_xact_id(),%s::timestamptz)"
            ).format(namespace), (
                store.bot_id, f"unit/{name}", f"capture/{name}", digest,
                order, digest, author, root.payload_id, root.content_digest,
                f"input/{name}", epoch, f"2026-01-{order:02d}T00:00:00Z",
            ))
        return root

    first_root = await insert_unit("one", 1, "author/target")
    await insert_unit("other", 2, "author/other")
    third_root = await insert_unit("three", 3, "author/target")
    shared = await kernel.payload.put_bytes(
        owner_component_id="memory.graph", owner_kind="retrieval.evidence",
        owner_id="evidence/selection", media_type="application/json",
        content=b'{"shared":true}',
        parents=(first_root.payload_id, third_root.payload_id),
        expected_deletion_epoch=0,
    )

    async def invoke(function, context, body):
        async with store.pool.connection() as conn, conn.transaction():
            row = await (await conn.execute(
                sql.SQL("SELECT {}.{}(%s,%s) AS result").format(
                    namespace, sql.Identifier(function)),
                (Jsonb(context), Jsonb(body)),
            )).fetchone()
            return row["result"]

    body = {"selection_id": "selection/target", "requested_by": "admin/one",
            "filters": {"author_subject_ref": "author/target"}}
    context = {"bot_id": store.bot_id, "operation_id": "selection/create"}
    created = await invoke("memory_freeze_forget_selection", context, body)
    assert created["ok"] is True, created
    assert created["value"]["member_count"] == 2
    assert await invoke("memory_freeze_forget_selection", context, body) == created
    await insert_unit("late", 4, "author/target")
    page = await invoke("memory_query_forget_selection", {"bot_id": store.bot_id}, {
        "selection_id": "selection/target", "requested_by": "admin/one", "limit": 1,
    })
    assert page["ok"] is True, page
    assert [item["source_unit_id"] for item in page["value"]["items"]] == [
        "unit/three"
    ]
    assert page["value"]["items"][0]["forget_state"] == "ACTIVE"
    assert page["value"]["items"][0]["forget_operation_id"] is None
    assert page["value"]["progress_state"] == "FROZEN"
    assert page["value"]["applied_batch_count"] == 0
    assert page["value"]["applied_member_count"] == 0
    assert page["value"]["next_unapplied_ordinal"] == 1
    assert page["value"]["has_more"] is True
    second = await invoke("memory_query_forget_selection", {"bot_id": store.bot_id}, {
        "selection_id": "selection/target", "requested_by": "admin/one",
        "after_ordinal": 1, "limit": 1,
    })
    assert [item["source_unit_id"] for item in second["value"]["items"]] == [
        "unit/one"
    ]
    assert second["value"]["has_more"] is False
    batch_request = {"selection_id": "selection/target", "requested_by": "admin/one",
                     "after_ordinal": 0, "batch_size": 2}
    step = {"protocol": "forest.uow-step/1", "caller": {
        "component_id": "memory.graph", "generation_id": "memory/test",
    }}
    participant = KernelPayloadUnitOfWorkParticipant(
        SimpleNamespace(generation_identity=SimpleNamespace(generation_id="payload/test")),
        kernel.payload,
    )
    async with store.pool.connection() as conn, conn.transaction():
        prepared = (await (await conn.execute(sql.SQL(
            "SELECT {}.memory_prepare_forget_batch(%s,%s) AS result"
        ).format(namespace), (
            Jsonb({"bot_id": store.bot_id}),
            Jsonb({**step, "input": batch_request, "prior": {}}),
        ))).fetchone())["result"]
        assert prepared["ok"] is True, prepared
        anchors = prepared["value"]
        assert set(anchors["anchor_payload_ids"]) == {
            first_root.payload_id, third_root.payload_id,
        }
        preview = await participant.invoke_in(
            conn, "payload.preview-forget-anchors-from-prior", {
                **step, "input": {
                    "protocol": "forest.payload-forget-anchors-preview/1",
                    "prior_step_id": "prepare",
                }, "prior": {"prepare": anchors},
            },
        )
        planned = (await (await conn.execute(sql.SQL(
            "SELECT {}.memory_preview_forget_batch(%s,%s) AS result"
        ).format(namespace), (
            Jsonb({"bot_id": store.bot_id}),
            Jsonb({**step, "input": {**batch_request,
                "prepare_step_id": "prepare", "preview_step_id": "payload"},
                "prior": {"prepare": anchors, "payload": preview}}),
        ))).fetchone())["result"]
    assert planned["ok"] is True, planned
    assert planned["value"]["batch_member_count"] == 2
    assert planned["value"]["source_unit_count"] == 2
    assert planned["value"]["selected_payload_count"] == 3
    assert shared.payload_id in preview["selected_payload_ids"]
    assert planned["value"]["batch_source_unit_sample"] == [
        "unit/three", "unit/one",
    ]
    lease = await kernel.runtime.claim_host(
        "forget-preview-host", "forget-preview-incarnation",
        ttl=timedelta(minutes=5),
    )
    host = SimpleNamespace(
        scope_kind="bot", bot=SimpleNamespace(store=store),
        kernel=SimpleNamespace(_authority=lambda **_kwargs: lease),
        schema_bundle_installer=installer, generation_authority=kernel.runtime,
        authority_lease=lambda **_kwargs: lease,
    )
    registry = RuntimeModuleRegistry(host, _isolated_memory_descriptors(kernel, package))
    await registry.start()
    try:
        actual = await registry.get("memory.graph").management_contract.query_authorized(
            "memory.forget.batch.preview",
            {"selection_id": "selection/target", "after_ordinal": 0,
             "batch_size": 2},
            authorization=SimpleNamespace(actor_ref="admin/one", web_subject_ref="admin/one"),
        )
        assert actual["plan_digest"] == planned["value"]["plan_digest"]
        with pytest.raises(ForestError) as forbidden:
            await registry.get("memory.graph").management_contract.query_authorized(
                "memory.forget.batch.preview",
                {"selection_id": "selection/target", "after_ordinal": 0,
                 "batch_size": 2},
                authorization=SimpleNamespace(actor_ref="admin/two", web_subject_ref="admin/two"),
            )
        assert forbidden.value.code == "MEMORY_FORGET_SELECTION_NOT_FOUND"
        with pytest.raises(ForestError) as forbidden_action:
            await registry.get("memory.graph").management_contract.action(
                "memory.forget.batch.enqueue",
                operation_id="batch/foreign-actor",
                payload={"selection_id": "selection/target", "after_ordinal": 0,
                         "batch_size": 2, "plan": planned["value"]},
                authorization=SimpleNamespace(actor_ref="admin/two", web_subject_ref="admin/two"),
                authorization_guard=lambda _conn, **_kwargs: asyncio.sleep(0),
            )
        assert forbidden_action.value.code == "MEMORY_FORGET_SELECTION_NOT_FOUND"
    finally:
        assert await registry.close() == []
    foreign = await invoke("memory_query_forget_selection", {"bot_id": store.bot_id}, {
        "selection_id": "selection/target", "requested_by": "admin/two",
    })
    assert foreign["error"]["code"] == "MEMORY_FORGET_SELECTION_NOT_FOUND"
    conflict = await invoke("memory_freeze_forget_selection", context, {
        **body, "filters": {"author_subject_ref": "author/other"},
    })
    assert conflict["error"]["code"] == "MEMORY_FORGET_SELECTION_CONFLICT"
    with pytest.raises(errors.ObjectNotInPrerequisiteState):
        async with store.pool.connection() as conn, conn.transaction():
            await conn.execute(sql.SQL(
                "DELETE FROM {}.forget_selection_members "
                "WHERE bot_id=%s AND selection_id='selection/target' AND ordinal=1"
            ).format(namespace), (store.bot_id,))

    operation_id = "batch/selection-apply"
    input_digest = "sha256:" + "b" * 64
    before_epoch = await kernel.payload.epoch()

    async def execute_batch(plan):
        async with store.pool.connection() as conn, conn.transaction():
            request = {**batch_request, "operation_id": operation_id}
            prepared = (await (await conn.execute(sql.SQL(
                "SELECT {}.memory_prepare_forget_batch(%s,%s) AS result"
            ).format(namespace), (
                Jsonb({"bot_id": store.bot_id}),
                Jsonb({**step, "input": request, "prior": {}}),
            ))).fetchone())["result"]
            assert prepared["ok"] is True, prepared
            forgotten = await participant.invoke_in(
                conn, "payload.forget-anchors-from-prior", {
                    **step, "input": {
                        "protocol": "forest.payload-forget-anchors/1",
                        "prior_step_id": "prepare", "operation_id": operation_id,
                        "expected_deletion_epoch": plan["deletion_epoch"],
                        "expected_selection_digest": plan["selection_digest"],
                    }, "prior": {"prepare": prepared["value"]},
                },
            )
            applied = (await (await conn.execute(sql.SQL(
                "SELECT {}.memory_apply_forget_batch(%s,%s) AS result"
            ).format(namespace), (
                Jsonb({"bot_id": store.bot_id,
                       "participant_input_digest": input_digest}),
                Jsonb({**step, "input": {**request,
                    "prepare_step_id": "prepare", "forget_step_id": "payload",
                    "plan": plan},
                    "prior": {"prepare": prepared["value"], "payload": forgotten}}),
            ))).fetchone())["result"]
            if not applied["ok"]:
                raise ForestError(**applied["error"])
            return applied["value"]

    stale_plan = {**planned["value"], "source_unit_count": 3}
    with pytest.raises(ForestError) as changed:
        await execute_batch(stale_plan)
    assert changed.value.code == "MEMORY_FORGET_BATCH_PLAN_CHANGED"
    assert await kernel.payload.epoch() == before_epoch
    queue_context = {"bot_id": store.bot_id, "operation_id": operation_id}
    queue_input = {**batch_request, "plan": planned["value"]}
    queued = await invoke("memory_enqueue_management_forget_batch",
                          queue_context, queue_input)
    assert queued["ok"] and queued["value"]["request_id"] == operation_id
    assert await invoke("memory_enqueue_management_forget_batch",
                        queue_context, queue_input) == queued
    hidden = await invoke("memory_query_management_forget_batch",
                          {"bot_id": store.bot_id}, {
                              "request_id": operation_id,
                              "requested_by": "admin/two",
                          })
    assert hidden["error"]["code"] == "MEMORY_FORGET_BATCH_REQUEST_NOT_FOUND"
    source_status = await invoke("memory_query_management_forget",
                                 {"bot_id": store.bot_id}, {
                                     "request_id": operation_id,
                                 })
    assert source_status["error"]["code"] == "MEMORY_FORGET_REQUEST_NOT_FOUND"
    claimed = await invoke("memory_claim_management_forget",
                           {"bot_id": store.bot_id}, {
                               "protocol": "forest.owned-work-claim-request/1",
                               "claim_token": "batch/selection-claim",
                               "lease_seconds": 180,
                           })
    assert claimed["ok"] and claimed["value"]["payload"] == {
        "request_id": operation_id, "work_kind": "BATCH",
        **batch_request, "plan": planned["value"],
    }
    applied = await execute_batch(planned["value"])
    assert applied["protocol"] == "forest.memory-forget-batch-applied/1"
    assert applied["suppressed_source_units"] == 2
    assert applied["deletion_epoch"] == before_epoch + 1
    assert await execute_batch(planned["value"]) == applied
    assert await kernel.payload.epoch() == before_epoch + 1
    # The UOW committed, but the worker lost its settlement before its lease expired.
    async with store.pool.connection() as conn, conn.transaction():
        await conn.execute(sql.SQL(
            "UPDATE {}.management_forget_work SET lease_deadline=clock_timestamp()"
            " - interval '1 second' WHERE bot_id=%s AND request_id=%s"
        ).format(namespace), (store.bot_id, operation_id))
    recovered = await invoke("memory_claim_management_forget",
                             {"bot_id": store.bot_id}, {
                                 "protocol": "forest.owned-work-claim-request/1",
                                 "claim_token": "batch/recovery-probe",
                                 "lease_seconds": 90,
                             })
    assert recovered["ok"] and recovered["value"] is None
    status = await invoke("memory_query_management_forget_batch",
                          {"bot_id": store.bot_id}, {
                              "request_id": operation_id,
                              "requested_by": "admin/one",
                          })
    assert status["value"]["state"] == "SUCCEEDED"
    assert status["value"]["attempt_count"] == 1
    assert status["value"]["result"] == applied
    stale_settlement = await invoke("memory_settle_management_forget",
                                    {"bot_id": store.bot_id}, {
                                        "protocol": "forest.owned-work-settle-request/1",
                                        "work_id": operation_id,
                                        "claim_token": "batch/selection-claim",
                                        "attempt": 1,
                                        "outcome": {"status": "SUCCEEDED", "result": applied},
                                    })
    assert stale_settlement["error"]["code"] == "MEMORY_FORGET_CLAIM_LOST"
    assert await kernel.payload.epoch() == before_epoch + 1
    forgotten_page = await invoke(
        "memory_query_forget_selection", {"bot_id": store.bot_id}, {
            "selection_id": "selection/target", "requested_by": "admin/one",
            "limit": 2,
        })
    assert [(item["source_unit_id"], item["forget_state"],
             item["forget_operation_id"])
            for item in forgotten_page["value"]["items"]] == [
        ("unit/three", "FORGOTTEN_BY_SELECTION", operation_id),
        ("unit/one", "FORGOTTEN_BY_SELECTION", operation_id),
    ]
    assert forgotten_page["value"]["progress_state"] == "COMPLETE"
    assert forgotten_page["value"]["applied_batch_count"] == 1
    assert forgotten_page["value"]["applied_member_count"] == 2
    assert forgotten_page["value"]["next_unapplied_ordinal"] is None
    async with store.pool.connection() as conn:
        rows = await (await conn.execute(sql.SQL(
            "SELECT source_unit_id,eligibility_state FROM {}.source_units "
            "WHERE bot_id=%s ORDER BY source_unit_id"
        ).format(namespace), (store.bot_id,))).fetchall()
    assert {row["source_unit_id"]: row["eligibility_state"] for row in rows} == {
        "unit/late": "ELIGIBLE", "unit/one": "SUPPRESSED",
        "unit/other": "ELIGIBLE", "unit/three": "SUPPRESSED",
    }

    # A fresh Generation accepts a real P0 Web ticket for the remaining root;
    # its own durable worker executes the batch without a host-side forget path.
    late_selection = await invoke("memory_freeze_forget_selection", {
        "bot_id": store.bot_id, "operation_id": "selection/create-late",
    }, {
        "selection_id": "selection/late", "requested_by": "admin/one",
        "filters": {"author_subject_ref": "author/target"},
    })
    assert late_selection["value"]["member_count"] == 1
    host.settings = SimpleNamespace(bot_id=store.bot_id)
    host._kernel = kernel
    registry = RuntimeModuleRegistry(host, _isolated_memory_descriptors(kernel, package))
    await registry.start()
    host.registry = registry
    try:
        memory = registry.get("memory.graph")
        plan = await memory.management_contract.query_authorized(
            "memory.forget.batch.preview",
            {"selection_id": "selection/late", "after_ordinal": 0,
             "batch_size": 1},
            authorization=SimpleNamespace(actor_ref="admin/one", web_subject_ref="admin/one"),
        )
        signer = WebAuthorityTicketAuthority(
            issuer_id="deployment/batch-test", active_key_id="batch-key/1",
            key_resolver=lambda _key_id: b"k" * 32,
        )
        gateway = ManagementGateway(host, ticket_authority=signer)
        action_id = "memory.forget.batch.enqueue"
        action_body = {"selection_id": "selection/late", "after_ordinal": 0,
                       "batch_size": 1, "plan": plan}
        operation_id = gateway.operation_id("memory.graph", action_id, "batch-late")
        ticket = await signer.issue(
            session=AuthenticatedWebSession(
                "token-digest", "session/batch-test", 1, "csrf-digest",
                WebSubject("admin/one", "admin", "Administrator", "ADMIN", True, 1),
                datetime.now(timezone.utc) + timedelta(hours=1),
            ),
            target_scope_kind="bot", target_scope_id=store.bot_id,
            target_component_id="memory.graph",
            target_generation_id=registry.graph_identity.generations["memory.graph"],
            target_graph_revision=registry.graph_identity.graph_revision,
            target_operation_ref=operation_id, action_key=action_id,
            input_digest=digest_json(action_body), plan_digest=digest_json(plan),
        )
        submitted = await gateway.module_action(
            "memory.graph", action_id, body=action_body,
            idempotency_key="batch-late", authorization=ticket,
            operation_id=operation_id,
        )
        assert submitted["request_id"] == operation_id
        async with asyncio.timeout(30):
            while True:
                work = await memory.management_contract.query_authorized(
                    "memory.forget.batch.status", {"request_id": operation_id},
                    authorization=SimpleNamespace(actor_ref="admin/one", web_subject_ref="admin/one"),
                )
                if work["state"] in {"SUCCEEDED", "FAILED", "UNKNOWN"}:
                    break
                await asyncio.sleep(0.2)
        assert work["state"] == "SUCCEEDED", work
        assert work["result"]["suppressed_source_units"] == 1
        progress = await memory.management_contract.query_authorized(
            "memory.forget.selection", {"selection_id": "selection/late"},
            authorization=SimpleNamespace(actor_ref="admin/one", web_subject_ref="admin/one"),
        )
        assert progress["progress_state"] == "COMPLETE"
        if os.environ.get("FOREST_BROWSER_SMOKE") == "1":
            await insert_unit("browser", 5, "author/browser")
            browser_selection = await invoke("memory_freeze_forget_selection", {
                "bot_id": store.bot_id, "operation_id": "selection/create-browser",
            }, {
                "selection_id": "selection/browser-real",
                "requested_by": "admin/one",
                "filters": {"author_subject_ref": "author/browser"},
            })
            assert browser_selection["value"]["member_count"] == 1
            browser_request = await _browser_apply_real_forget_batch(
                host, "selection/browser-real",
            )
            browser_work = await memory.management_contract.query_authorized(
                "memory.forget.batch.status", {"request_id": browser_request},
                authorization=SimpleNamespace(actor_ref="admin/one", web_subject_ref="admin/one"),
            )
            assert browser_work["state"] == "SUCCEEDED"
            assert browser_work["result"]["suppressed_source_units"] == 1
            browser_progress = await memory.management_contract.query_authorized(
                "memory.forget.selection", {"selection_id": "selection/browser-real"},
                authorization=SimpleNamespace(actor_ref="admin/one", web_subject_ref="admin/one"),
            )
            assert browser_progress["progress_state"] == "COMPLETE"
    finally:
        assert await registry.close() == []

    # Expired work is reconciled in bounded transactions, then fully converges.
    async with store.pool.connection() as conn, conn.transaction():
        await conn.execute(sql.SQL(
            "INSERT INTO {}.management_forget_work("
            "bot_id,request_id,input_digest,source_unit_id,requested_by,plan,"
            "state,claim_token,lease_deadline,attempt_count) "
            "SELECT %s,'expired/'||lpad(n::text,3,'0'),%s,'unit/late',"
            "'admin/one','{{}}'::jsonb,'RUNNING','expired-claim/'||n,"
            "clock_timestamp()-interval '1 second',3 "
            "FROM generate_series(1,70) AS n"
        ).format(namespace), (store.bot_id, digest))
    for round_no, expected in enumerate((32, 64, 70), 1):
        claim = await invoke("memory_claim_management_forget", {
            "bot_id": store.bot_id,
        }, {
            "protocol": "forest.owned-work-claim-request/1",
            "claim_token": f"expired/reconcile/{round_no}",
            "lease_seconds": 90,
        })
        assert claim["ok"] and claim["value"] is None
        async with store.pool.connection() as conn:
            counts = await (await conn.execute(sql.SQL(
                "SELECT count(*) FILTER (WHERE state='UNKNOWN') AS unknown_count,"
                "count(*) FILTER (WHERE state='RUNNING') AS running_count "
                "FROM {}.management_forget_work "
                "WHERE bot_id=%s AND left(request_id,8)='expired/'"
            ).format(namespace), (store.bot_id,))).fetchone()
        assert counts["unknown_count"] == expected
        assert counts["running_count"] == 70 - expected

    # A result committed after an UNKNOWN observation is recovered from the
    # immutable batch operation rather than remaining uncertain forever.
    async with store.pool.connection() as conn, conn.transaction():
        await conn.execute(sql.SQL(
            "UPDATE {}.management_forget_work SET state='UNKNOWN',"
            "result_payload=NULL,error_code='MEMORY_FORGET_OUTCOME_UNKNOWN',"
            "reconcile_checked_at=clock_timestamp()-interval '2 hours' "
            "WHERE bot_id=%s AND request_id=%s"
        ).format(namespace), (store.bot_id, "batch/selection-apply"))
    claim = await invoke("memory_claim_management_forget", {
        "bot_id": store.bot_id,
    }, {
        "protocol": "forest.owned-work-claim-request/1",
        "claim_token": "unknown/positive-evidence",
        "lease_seconds": 90,
    })
    assert claim["ok"] and claim["value"] is None
    recovered_batch = await invoke("memory_query_management_forget_batch", {
        "bot_id": store.bot_id,
    }, {
        "request_id": "batch/selection-apply", "requested_by": "admin/one",
    })
    assert recovered_batch["value"]["state"] == "SUCCEEDED"
    assert recovered_batch["value"]["result"] == applied


@pytest.mark.postgres
async def test_expired_facet_preparation_claims_recover_in_bounded_batches(
    clean_authority, tmp_path
):
    store, installer = clean_authority
    kernel = await bootstrap_kernel(store, installer)
    package = await bootstrap_builtin_package(
        Path(__file__).parents[1] / "src/forest/builtin/memory_graph",
        PackageCatalog(tmp_path / "facet-recovery-packages"),
        store, installer, kernel=kernel, initial_configuration=_configuration(),
        resolved_contracts={},
    )
    namespace = sql.Identifier(package.descriptor.storage.spec.namespace)
    digest = "sha256:" + "a" * 64
    async with store.pool.connection() as conn, conn.transaction():
        await conn.execute(sql.SQL(
            "INSERT INTO {}.memory_domains("
            "bot_id,initialization_operation_id,input_digest,space_id,"
            "embedding_model_kind,model_revision_digest,dimensions,max_batch_items,"
            "normalization,fusion_policy_revision) VALUES "
            "(%s,'domain/facet-recovery',%s,'space/facet-recovery',"
            "'TEXT_IMAGE',%s,4096,8,'L2','facets/1')"
        ).format(namespace), (store.bot_id, digest, digest))
        await conn.execute(sql.SQL(
            "INSERT INTO {}.memory_sources("
            "bot_id,source_id,registration_operation_id,input_digest,source_kind,"
            "identity_incarnation,identity_digest) VALUES "
            "(%s,'source/facet-recovery','register/facet-recovery',%s,"
            "'DOCUMENT',1,%s)"
        ).format(namespace), (store.bot_id, digest, digest))
        await conn.execute(sql.SQL(
            "INSERT INTO {}.source_units("
            "bot_id,source_unit_id,capture_operation_id,input_digest,source_id,"
            "source_order_seq,source_revision,unit_kind,modality,part_count,"
            "content_manifest_digest,author_subject_ref,author_payload_id,"
            "author_payload_digest,author_owner_component_id,author_owner_kind,"
            "author_owner_id,accepted_deletion_epoch,eligibility_state,eligible_at,"
            "occurred_at) SELECT %s,'unit/facet/'||lpad(n::text,3,'0'),"
            "'capture/facet/'||n,%s,'source/facet-recovery',n,1,'MESSAGE','TEXT',1,"
            "%s,'author/test','payload/author',%s,'conversation.scheduler',"
            "'context.input','input/facet/'||n,0,'ELIGIBLE',clock_timestamp(),"
            "clock_timestamp() FROM generate_series(1,70) AS n"
        ).format(namespace), (store.bot_id, digest, digest, digest))
        await conn.execute(sql.SQL(
            "INSERT INTO {}.facet_preparation_work("
            "bot_id,source_unit_id,state,claim_token,lease_deadline,attempt_count) "
            "SELECT %s,'unit/facet/'||lpad(n::text,3,'0'),"
            "'CLAIMED','stale/'||n,clock_timestamp()-interval '1 second',1 "
            "FROM generate_series(1,70) AS n"
        ).format(namespace), (store.bot_id,))

    for round_no, remaining in enumerate((38, 6, 0), 1):
        async with store.pool.connection() as conn, conn.transaction():
            claim = (await (await conn.execute(sql.SQL(
                "SELECT {}.memory_claim_facet_preparation(%s,%s) AS result"
            ).format(namespace), (
                Jsonb({"bot_id": store.bot_id}),
                Jsonb({"protocol": "forest.owned-work-claim-request/1",
                       "claim_token": f"new/{round_no}", "lease_seconds": 90}),
            ))).fetchone())["result"]
        assert claim["ok"] and claim["value"] is not None
        async with store.pool.connection() as conn:
            counts = await (await conn.execute(sql.SQL(
                "SELECT count(*) FILTER (WHERE left(claim_token,6)='stale/') "
                "AS still_expired, count(*) FILTER (WHERE "
                "last_error_code='MEMORY_FACET_LEASE_EXPIRED') AS recovered "
                "FROM {}.facet_preparation_work WHERE bot_id=%s"
            ).format(namespace), (store.bot_id,))).fetchone()
        assert counts["still_expired"] == remaining
        assert counts["recovered"] == 70 - remaining


def _configuration():
    return {
        "protocol": "forest.memory.graph/2",
        "embedding": {
            "model_kind": "TEXT_IMAGE",
            "deployment_id": "embedding.multimodal/qiyu-vl/default",
            "model_revision": "operator-declared/1",
            "model": "Qwen/Qwen3-VL-Embedding-8B",
            "dimensions": 4096,
            "normalization": "L2",
        },
        "fusion_policy_revision": "independent-facets/1",
        "retrieval": {
            "policy_revision": "qiyu-graph-region/2",
            "seed_limit": 4,
            "candidate_limit_per_facet": 64,
            "min_similarity": 0.35,
            "min_graph_relevance": 0.05,
            "max_graph_nodes": 32,
            "max_association_edges_per_node": 32,
            "max_retrieval_chars": 24000,
            "max_retrieval_media_bytes": 12582912,
            "sequence_weight": 0.55,
            "association_weight": 0.9,
        },
        "embedding_budget": {
            "policy_revision": "daily-cost-cap/1",
            "daily_request_limit": 1000,
            "daily_token_limit": 10000000,
            "reservation_tokens": 32768,
        },
    }


def test_document_facets_preserve_text_spans_and_image_identity():
    text = "甲" * 4200
    text_ref = {
        "payload_id": "payload/text", "digest": "sha256:" + "1" * 64,
        "owner_component_id": "adapter.onebot", "owner_kind": "context.input",
        "owner_id": "source/text",
    }
    image_ref = {
        **text_ref, "payload_id": "payload/image", "digest": "sha256:" + "2" * 64,
        "owner_id": "source/image",
    }
    parts = [
        {"part_id": "part/text", "part_kind": "TEXT", "payload": text_ref,
         "byte_length": len(text.encode())},
        {"part_id": "part/image", "part_kind": "IMAGE", "payload": image_ref,
         "byte_length": 128},
    ]
    first = document_facets("source/1", 1, "sha256:" + "a" * 64, parts, {
        "part/text": text,
    })
    assert first == document_facets("source/1", 1, "sha256:" + "a" * 64, parts, {
        "part/text": text,
    })
    assert [(f["selector_start_char"], f["selector_end_char"])
            for f in first[:3]] == [(0, 2048), (1920, 3968), (3840, 4200)]
    assert first[3]["modality"] == "IMAGE"
    assert first[3]["facet_content_digest"] == image_ref["digest"]
    assert len({item["facet_id"] for item in first}) == 4


def test_query_facets_keep_trial_input_identity_separate_from_source_identity():
    text = "same words and image intent"
    part = {
        "part_id": "part/text", "part_kind": "TEXT", "byte_length": len(text.encode()),
        "payload": {
            "payload_id": "payload/input", "digest": "sha256:" + "1" * 64,
            "owner_component_id": "memory.graph", "owner_kind": "trial.input",
            "owner_id": "input/1",
        },
    }
    source = query_part_facets(
        "query/1", "space/1", {
            **part, "source_unit_id": "unit/1", "source_revision": 1,
            "input_identity": {
                "kind": "SOURCE_UNIT", "ref": "unit/1", "revision": 1,
            },
        }, {"part/text": text},
    )
    trial = query_part_facets(
        "query/1", "space/1", {
            **part, "input_identity": {
                "kind": "ADMIN_TRIAL", "ref": "unit/1", "revision": 1,
            },
        }, {"part/text": text},
    )
    assert source[0]["facet_id"] != trial[0]["facet_id"]
    with pytest.raises(ValueError, match="must not carry admitted source identity"):
        query_part_facets(
            "query/1", "space/1", {
                **part, "source_unit_id": "unit/1",
                "input_identity": {
                    "kind": "ADMIN_TRIAL", "ref": "unit/1", "revision": 1,
                },
            }, {"part/text": text},
        )
    with pytest.raises(ValueError, match="differs from its admitted part"):
        query_part_facets(
            "query/1", "space/1", {
                **part, "source_unit_id": "unit/1", "source_revision": 1,
                "input_identity": {
                    "kind": "SOURCE_UNIT", "ref": "another/source", "revision": 1,
                },
            }, {"part/text": text},
        )
    large_image = {
        **part, "part_id": "part/large-image", "part_kind": "IMAGE",
        "byte_length": 12 * 1024 * 1024,
        "source_unit_id": "unit/1", "source_revision": 1,
        "input_identity": {"kind": "SOURCE_UNIT", "ref": "unit/1", "revision": 1},
    }
    assert query_part_facets("query/1", "space/1", large_image, {})[0][
        "input_byte_length"
    ] == 12 * 1024 * 1024


def test_query_preparation_contract_discriminates_source_and_trial_identity():
    schema_dir = Path(__file__).parents[1] / "src/forest/builtin/memory_graph/schemas"
    schema = json.loads((schema_dir / "prepare-query-part-facets-input.json").read_text(
        encoding="utf-8"
    ))
    validator = Draft202012Validator(schema)
    request = {
        "protocol": "forest.memory-query-part-facets/1",
        "query_id": "query/1", "space_id": "sha256:" + "a" * 64,
        "batch_index": 0, "part_id": "part/1", "facets": [],
        "input_identity": {"kind": "SOURCE_UNIT", "ref": "unit/1", "revision": 1},
        "source_unit_id": "unit/1", "source_revision": 1,
    }
    validator.validate(request)
    trial = {
        **request,
        "input_identity": {"kind": "ADMIN_TRIAL", "ref": "trial/1", "revision": 1},
    }
    trial.pop("source_unit_id")
    trial.pop("source_revision")
    validator.validate(trial)
    assert list(validator.iter_errors({**trial, "source_unit_id": "unit/1"}))
    assert list(validator.iter_errors({**request, "input_identity": trial["input_identity"]}))
    read_schema = json.loads((schema_dir / "read-query-parts-output.json").read_text(
        encoding="utf-8"
    ))
    read_validator = Draft202012Validator(read_schema)
    part = {
        "part_id": "part/1", "part_index": 0, "part_kind": "TEXT",
        "media_type": "text/plain", "byte_length": 4, "query_part_index": 0,
        "payload": {
            "payload_id": "payload/1", "digest": "sha256:" + "b" * 64,
            "owner_component_id": "memory.graph", "owner_kind": "trial.input",
            "owner_id": "trial/1",
        },
        "input_identity": trial["input_identity"],
    }
    page = {"parts": [part], "next_part_index": None, "has_more": False,
            "space_id": request["space_id"]}
    read_validator.validate(page)
    assert list(read_validator.iter_errors({**page, "parts": [{
        **part, "source_revision": 1,
    }]}))


@pytest.mark.postgres
async def test_query_facet_persistence_rejects_unbound_input_identity(
    clean_authority, tmp_path
):
    store, installer = clean_authority
    kernel = await bootstrap_kernel(store, installer)
    package = await bootstrap_builtin_package(
        Path(__file__).parents[1] / "src/forest/builtin/memory_graph",
        PackageCatalog(tmp_path / "facet-identity-packages"),
        store, installer, kernel=kernel, initial_configuration=_configuration(),
        resolved_contracts={},
    )
    namespace = sql.Identifier(package.descriptor.storage.spec.namespace)
    body = {
        "protocol": "forest.memory-query-part-facets/1", "query_id": "query/1",
        "source_unit_id": "unit/1", "source_revision": 1,
        "input_identity": {"kind": "SOURCE_UNIT", "ref": "unit/other", "revision": 1},
        "space_id": "sha256:" + "a" * 64,
        "batch_index": 0, "part_id": "part/1", "facets": [],
    }
    envelope = {
        "protocol": "forest.uow-step/1",
        "caller": {"component_id": "memory.graph"},
        "input": body,
    }
    context = {
        "bot_id": store.bot_id, "operation_id": "operation/facet-identity",
        "participant_input_digest": digest_json(body),
    }
    async with store.pool.connection() as conn:
        result = (await (await conn.execute(
            sql.SQL("SELECT {}.memory_prepare_facets(%s,%s) AS result").format(namespace),
            (Jsonb(context), Jsonb(envelope)),
        )).fetchone())["result"]
    assert result["ok"] is False
    assert result["error"]["code"] == "MEMORY_FACET_INPUT_INVALID"


@pytest.mark.postgres
async def test_management_trial_action_requires_live_exact_payload_reference(
    clean_authority, tmp_path
):
    store, installer = clean_authority
    kernel = await bootstrap_kernel(store, installer)
    package = await bootstrap_builtin_package(
        Path(__file__).parents[1] / "src/forest/builtin/memory_graph",
        PackageCatalog(tmp_path / "trial-authority-packages"),
        store, installer, kernel=kernel, initial_configuration=_configuration(),
        resolved_contracts={},
    )
    lease = await kernel.runtime.claim_host(
        "trial-authority-host", "trial-authority-incarnation", ttl=timedelta(minutes=5),
    )
    host = SimpleNamespace(
        scope_kind="bot", bot=SimpleNamespace(store=store),
        kernel=SimpleNamespace(_authority=lambda **_kwargs: lease),
        schema_bundle_installer=installer, generation_authority=kernel.runtime,
        authority_lease=lambda **_kwargs: lease,
    )
    registry = RuntimeModuleRegistry(host, _isolated_memory_descriptors(kernel, package))
    await registry.start()
    try:
        instance = registry.get("memory.graph")
        relation_search = await instance.management_contract.query(
            "memory.relations.search", {"limit": 5}
        )
        assert relation_search["items"] == []
        digest = "sha256:" + "a" * 64
        namespace = sql.Identifier(package.descriptor.storage.spec.namespace)
        async with store.pool.connection() as conn, conn.transaction():
            await conn.execute(sql.SQL(
                "INSERT INTO {}.memory_domains "
                "(bot_id,initialization_operation_id,input_digest,space_id,"
                "embedding_model_kind,model_revision_digest,dimensions,max_batch_items,normalization,"
                "fusion_policy_revision) VALUES (%s,'trial-domain',%s,%s,'TEXT_IMAGE',%s,"
                "4096,8,'L2','fusion/1') ON CONFLICT (bot_id) DO NOTHING"
            ).format(namespace), (store.bot_id, digest, digest, digest))
        generation_id = registry.graph_identity.generations["memory.graph"]
        web_actor = SimpleNamespace(
            actor_ref="admin-1", web_subject_ref="admin-1",
            target_generation_id=generation_id,
        )
        async def accept_fixture_ticket(_conn, *, ticket, **_kwargs):
            assert ticket is not None
        assets = ManagementAssetAuthority(kernel.payload)

        async def uploaded(part_id, upload_id, content):
            part_digest = "sha256:" + hashlib.sha256(content).hexdigest()
            async with store.pool.connection() as conn, conn.transaction():
                await assets.begin_in(
                    conn, upload_id=upload_id, component_id="memory.graph",
                    asset_id="trial.input", actor_subject_ref="admin-1",
                    generation_id=generation_id, target_ref="query/authority",
                    part_id=part_id, owner_kind="trial.input",
                    owner_id=_trial_owner_id("query/authority", part_id),
                    media_type="text/plain", byte_length=len(content),
                    content_digest=part_digest, chunk_count=1,
                    allowed_media_types=("text/plain",), max_bytes=8388608,
                )
                await assets.put_chunk_in(
                    conn, upload_id=upload_id, component_id="memory.graph",
                    asset_id="trial.input", actor_subject_ref="admin-1",
                    generation_id=generation_id, chunk_index=0,
                    content=content, content_digest=part_digest,
                )
                return (await assets.finalize_in(
                    conn, upload_id=upload_id, component_id="memory.graph",
                    asset_id="trial.input", actor_subject_ref="admin-1",
                    generation_id=generation_id,
                ))["payload"]

        reference = await uploaded("text/0", "upload-authority-0", b"hello trial")
        second_reference = await uploaded("text/1", "upload-authority-1", b"second trial")
        assert second_reference["payload_id"] != reference["payload_id"]
        body = {"query_id": "query/authority", "parts": [{
            "part_id": "text/0", "modality": "TEXT", "media_type": "text/plain",
            "byte_length": 11, "width": None, "height": None, "payload": reference,
        }, {
            "part_id": "text/1", "modality": "TEXT", "media_type": "text/plain",
            "byte_length": 12, "width": None, "height": None,
            "payload": second_reference,
        }]}
        invalid = {**body, "parts": [{**body["parts"][0], "payload": {
            **reference, "digest": "sha256:" + "b" * 64,
        }}]}
        with pytest.raises(ForestError) as rejected:
            await instance.management_contract.action(
                "memory.trial.start", operation_id="web:trial-invalid", payload=invalid,
                authorization=web_actor, authorization_guard=accept_fixture_ticket,
            )
        assert rejected.value.code == "PAYLOAD_IDENTITY_CONFLICT"
        wrong_length = {**body, "parts": [{**body["parts"][0], "byte_length": 10}]}
        with pytest.raises(ForestError) as rejected:
            await instance.management_contract.action(
                "memory.trial.start", operation_id="web:trial-wrong-length",
                payload=wrong_length,
                authorization=web_actor, authorization_guard=accept_fixture_ticket,
            )
        assert rejected.value.code == "DOMAIN_PAYLOAD_METADATA_CONFLICT"
        with pytest.raises(ForestError) as rejected:
            await instance.management_contract.action(
                "memory.trial.start", operation_id="web:trial-no-asset-authority",
                payload=body,
            )
        assert rejected.value.code == "WEB_AUTHORITY_TICKET_REQUIRED"
        with pytest.raises(ForestError) as rejected:
            await instance.management_contract.action(
                "memory.trial.start", operation_id="web:trial-other-actor",
                payload=body,
                authorization=SimpleNamespace(
                    actor_ref="admin-2", web_subject_ref="admin-2",
                    target_generation_id=generation_id,
                ),
                authorization_guard=accept_fixture_ticket,
            )
        assert rejected.value.code == "MANAGEMENT_ASSET_IDENTITY_CONFLICT"
        started = await instance.management_contract.action(
            "memory.trial.start", operation_id="web:trial-valid", payload=body,
            authorization=web_actor, authorization_guard=accept_fixture_ticket,
        )
        assert started["query_id"] == "query/authority"
        assert await instance.management_contract.action(
            "memory.trial.start", operation_id="web:trial-valid", payload=body,
            authorization=web_actor, authorization_guard=accept_fixture_ticket,
        ) == started
        async with store.pool.connection() as conn:
            actor = (await (await conn.execute(sql.SQL(
                "SELECT actor_subject_ref FROM {}.memory_trial_inputs "
                "WHERE bot_id=%s AND query_id='query/authority'"
            ).format(namespace), (store.bot_id,))).fetchone())["actor_subject_ref"]
        assert actor == "admin-1"
        viewed = await instance.management_contract.query_authorized(
            "memory.trial.get", {"query_id": "query/authority"},
            authorization=web_actor,
        )
        assert viewed["query_id"] == "query/authority"
        cancelled = await instance.management_contract.action(
            "memory.trial.cancel", operation_id="web:trial-cancel",
            payload={"query_id": "query/authority"},
            authorization=web_actor, authorization_guard=accept_fixture_ticket,
        )
        assert cancelled["query_id"] == "query/authority"
    finally:
        assert await registry.close() == []


@pytest.mark.postgres
async def test_management_trial_registers_mixed_input_and_separate_cancellation(
    clean_authority, tmp_path
):
    store, installer = clean_authority
    kernel = await bootstrap_kernel(store, installer)
    package = await bootstrap_builtin_package(
        Path(__file__).parents[1] / "src/forest/builtin/memory_graph",
        PackageCatalog(tmp_path / "trial-action-packages"),
        store, installer, kernel=kernel, initial_configuration=_configuration(),
        resolved_contracts={},
    )
    namespace = sql.Identifier(package.descriptor.storage.spec.namespace)
    digest = "sha256:" + "a" * 64
    async with store.pool.connection() as conn, conn.transaction():
        await conn.execute(sql.SQL(
            "INSERT INTO {}.memory_domains "
            "(bot_id,initialization_operation_id,input_digest,space_id,"
                "embedding_model_kind,model_revision_digest,dimensions,max_batch_items,normalization,"
            "fusion_policy_revision) VALUES (%s,'trial-domain',%s,%s,'TEXT_IMAGE',%s,"
                "4096,8,'L2','fusion/1')"
        ).format(namespace), (store.bot_id, digest, digest, digest))

    def part(index, modality, media_type):
        return {
            "part_id": f"part/{index}", "modality": modality,
            "media_type": media_type, "byte_length": 4,
            "width": 1 if modality == "IMAGE" else None,
            "height": 1 if modality == "IMAGE" else None,
            "payload": {"payload_id": f"payload/{index}", "digest": digest,
                        "owner_component_id": "memory.graph", "owner_kind": "trial.input",
                        "owner_id": _trial_owner_id("query/management", f"part/{index}")},
        }

    body = {"query_id": "query/management", "requested_by": "web:admin",
            "parts": [part(0, "TEXT", "text/plain"), part(1, "IMAGE", "image/png")]}

    async def invoke(function, context, payload):
        if function == "memory_read_management_trial":
            payload = {"requested_by": "web:admin", **payload}
        async with store.pool.connection() as conn:
            return (await (await conn.execute(sql.SQL(
                "SELECT {}.{}(%s,%s) AS result"
            ).format(namespace, sql.Identifier(function)), (
                Jsonb({"bot_id": store.bot_id, "generation_id": "generation/trial", **context}),
                Jsonb(payload),
            ))).fetchone())["result"]

    start_context = {"operation_id": "trial/start/1", "input_digest": digest_json(body)}
    started = await invoke("memory_start_management_trial", start_context, body)
    assert started["ok"], started
    Draft202012Validator(package.snapshot.schema(
        "schemas/trial-start-output.json"
    )).validate(started["value"])
    assert await invoke("memory_start_management_trial", start_context, body) == started
    conflict = await invoke("memory_start_management_trial", {
        "operation_id": "trial/start/other", "input_digest": digest_json(body),
    }, body)
    assert conflict["error"]["code"] == "MEMORY_TRIAL_IDENTITY_CONFLICT"
    current = await invoke("memory_read_management_trial", {}, {"query_id": "query/management"})
    assert current["ok"] and current["value"]["query_mode"] == "MIXED"
    other_actor = await invoke("memory_read_management_trial", {}, {
        "query_id": "query/management", "requested_by": "web:other",
    })
    assert other_actor["error"]["code"] == "MEMORY_TRIAL_NOT_FOUND"
    assert current["value"]["state"] == "PENDING"
    Draft202012Validator(package.snapshot.schema(
        "schemas/trial-get-output.json"
    )).validate(current["value"])
    public_lookup = await invoke("memory_assert_public_query", {}, {
        "query_id": "query/management",
    })
    assert public_lookup["error"]["code"] == "MEMORY_QUERY_NOT_FOUND"
    absent_lookup = await invoke("memory_assert_public_query", {}, {
        "query_id": "query/not-yet-registered",
    })
    assert absent_lookup["ok"]
    public_cancel = await invoke("memory_cancel_recall", {}, {
        "protocol": "forest.uow-step/1",
        "caller": {"component_id": "memory.graph"},
        "input": {"protocol": "forest.memory-recall-cancel/1",
                  "query_id": "query/management", "reason_code": "SUPERSEDED"},
        "prior": {},
    })
    assert public_cancel["error"]["code"] == "MEMORY_QUERY_NOT_FOUND"
    public_request = await invoke("memory_request_recall", {}, {
        "protocol": "forest.uow-step/1",
        "caller": {"component_id": "memory.graph"},
        "input": {"protocol": "forest.memory-recall-request/1",
                  "query_id": "query/management",
                  "deadline_at": started["value"]["deadline_at"]},
        "prior": {},
    })
    assert public_request["error"]["code"] == "MEMORY_QUERY_NOT_FOUND"
    async with store.pool.connection() as conn:
        source_count = (await (await conn.execute(sql.SQL(
            "SELECT count(*) AS n FROM {}.source_units WHERE bot_id=%s"
        ).format(namespace), (store.bot_id,))).fetchone())["n"]
    assert source_count == 0
    other_cancel = await invoke("memory_cancel_management_trial", {}, {
        "query_id": "query/management", "requested_by": "web:other",
    })
    assert other_cancel["error"]["code"] == "MEMORY_TRIAL_NOT_FOUND"
    cancelled = await invoke("memory_cancel_management_trial", {}, {
        "query_id": "query/management", "requested_by": "web:admin",
    })
    assert cancelled["ok"] and cancelled["value"]["status"] == "CANCELLED"
    Draft202012Validator(package.snapshot.schema(
        "schemas/trial-cancel-output.json"
    )).validate(cancelled["value"])
    assert await invoke("memory_cancel_management_trial", {}, {
        "query_id": "query/management", "requested_by": "web:admin",
    }) == cancelled
    after = await invoke("memory_read_management_trial", {}, {"query_id": "query/management"})
    assert after["value"]["state"] == "CANCELLED"
    foreign_generation = await invoke("memory_read_management_trial", {
        "generation_id": "generation/replaced",
    }, {"query_id": "query/management"})
    assert foreign_generation["error"]["code"] == "MEMORY_TRIAL_INPUT_UNAVAILABLE"

    in_flight_body = json.loads(json.dumps(body))
    in_flight_body["query_id"] = "query/management-in-flight"
    for item in in_flight_body["parts"]:
        item["payload"]["owner_id"] = _trial_owner_id(
            in_flight_body["query_id"], item["part_id"]
        )
    in_flight_start = await invoke("memory_start_management_trial", {
        "operation_id": "trial/start/2", "input_digest": digest_json(in_flight_body),
    }, in_flight_body)
    assert in_flight_start["ok"]
    async with store.pool.connection() as conn, conn.transaction():
        await conn.execute(sql.SQL(
            "INSERT INTO {}.embedding_facets "
            "(bot_id,facet_id,purpose,query_id,input_kind,input_ref,input_revision,"
            "trial_query_id,part_id,modality,selector_start_char,selector_end_char,"
            "input_payload_id,input_payload_digest,facet_content_digest,input_media_type,"
            "input_byte_length,space_id,accepted_deletion_epoch) VALUES "
            "(%s,'trial/cancel/facet','QUERY','query/management-in-flight',"
            "'ADMIN_TRIAL','query/management-in-flight',1,"
            "'query/management-in-flight','part/0','TEXT',0,4,'payload/0',"
            "%s,%s,'text/plain',4,%s,0)"
        ).format(namespace), (store.bot_id, digest, digest, digest))
        await conn.execute(sql.SQL(
            "INSERT INTO {}.embedding_facets "
            "(bot_id,facet_id,purpose,query_id,input_kind,input_ref,input_revision,"
            "trial_query_id,part_id,modality,input_payload_id,input_payload_digest,"
            "facet_content_digest,input_media_type,input_byte_length,space_id,"
            "accepted_deletion_epoch) VALUES "
            "(%s,'trial/cancel/prepared','QUERY','query/management-in-flight',"
            "'ADMIN_TRIAL','query/management-in-flight',1,"
            "'query/management-in-flight','part/1','IMAGE','payload/1',"
            "%s,%s,'image/png',4,%s,0)"
        ).format(namespace), (store.bot_id, digest, digest, digest))
        await conn.execute(sql.SQL(
            "UPDATE {}.embedding_facets SET state='PREPARED' WHERE bot_id=%s "
            "AND facet_id='trial/cancel/prepared'"
        ).format(namespace), (store.bot_id,))
    claim = await invoke("memory_claim_recall", {}, {
        "protocol": "forest.owned-work-claim-request/1",
        "claim_token": "trial/claim/in-flight", "lease_seconds": 60,
    })
    assert claim["value"]["work_id"] == in_flight_body["query_id"]
    cancelling = await invoke("memory_cancel_management_trial", {}, {
        "query_id": in_flight_body["query_id"], "requested_by": "web:admin",
    })
    assert cancelling["value"]["status"] == "CANCELLING"
    rejected_parts = await invoke("memory_read_query_parts", {}, {
        "query_id": in_flight_body["query_id"], "after_part_index": -1, "limit": 1,
    })
    assert rejected_parts["error"]["code"] == "MEMORY_TRIAL_INPUT_UNAVAILABLE"
    skipped_embedding = await invoke("memory_claim_embedding_batch", {}, {
        "protocol": "forest.owned-work-claim-request/1",
        "claim_token": "trial/embedding/cancelled", "lease_seconds": 60,
    })
    assert skipped_embedding["ok"] and skipped_embedding["value"] is None
    retired = await invoke("memory_claim_vector_deletion", {}, {
        "protocol": "forest.owned-work-claim-request/1",
        "claim_token": "trial/delete/cancelled", "lease_seconds": 60,
    })
    assert retired["ok"] and retired["value"] is None
    async with store.pool.connection() as conn:
        facet_states = (await (await conn.execute(sql.SQL(
            "SELECT facet_id,state FROM {}.embedding_facets WHERE bot_id=%s "
            "AND facet_id IN ('trial/cancel/facet','trial/cancel/prepared') "
            "ORDER BY facet_id"
        ).format(namespace), (store.bot_id,))).fetchall())
    assert [row["state"] for row in facet_states] == ["SUPPRESSED", "SUPPRESSED"]
    settled = await invoke("memory_settle_recall", {}, {
        "protocol": "forest.owned-work-settle-request/1",
        "work_id": in_flight_body["query_id"],
        "claim_token": "trial/claim/in-flight", "attempt": 1,
        "outcome": {"status": "SUCCEEDED", "result": {
            "query_id": in_flight_body["query_id"],
        }},
    })
    assert settled["ok"]
    cancelled_in_flight = await invoke("memory_read_management_trial", {}, {
        "query_id": in_flight_body["query_id"],
    })
    assert cancelled_in_flight["value"]["state"] == "CANCELLED"


@pytest.mark.postgres
async def test_trial_parts_are_isolated_from_source_membership_and_generation(
    clean_authority, tmp_path
):
    store, installer = clean_authority
    kernel = await bootstrap_kernel(store, installer)
    package = await bootstrap_builtin_package(
        Path(__file__).parents[1] / "src/forest/builtin/memory_graph",
        PackageCatalog(tmp_path / "trial-input-packages"),
        store, installer, kernel=kernel, initial_configuration=_configuration(),
        resolved_contracts={},
    )
    namespace = sql.Identifier(package.descriptor.storage.spec.namespace)
    digest = "sha256:" + "a" * 64
    async with store.pool.connection() as conn, conn.transaction():
        await conn.execute(sql.SQL(
            "INSERT INTO {}.memory_domains "
            "(bot_id,initialization_operation_id,input_digest,space_id,"
                "embedding_model_kind,model_revision_digest,dimensions,max_batch_items,normalization,"
            "fusion_policy_revision) VALUES (%s,'trial-domain',%s,%s,'TEXT_IMAGE',%s,"
                "4096,8,'L2','fusion/1')"
        ).format(namespace), (store.bot_id, digest, digest, digest))
        await conn.execute(sql.SQL(
            "INSERT INTO {}.memory_queries "
            "(bot_id,query_id,operation_id,input_digest,input_kind,query_mode,"
            "fusion_policy_revision,accepted_deletion_epoch) VALUES "
            "(%s,'query/trial','trial-register',%s,'ADMIN_TRIAL','MIXED','fusion/1',0)"
        ).format(namespace), (store.bot_id, digest))
        await conn.execute(sql.SQL(
            "INSERT INTO {}.memory_trial_inputs "
            "(bot_id,query_id,actor_subject_ref,generation_id,expires_at) "
            "VALUES (%s,'query/trial','web:admin','generation/trial',"
            "clock_timestamp()+interval '10 minutes')"
        ).format(namespace), (store.bot_id,))
        for index, modality, media_type in (
            (0, "TEXT", "text/plain"), (1, "IMAGE", "image/png")
        ):
            await conn.execute(sql.SQL(
                "INSERT INTO {}.memory_trial_parts "
                "(bot_id,query_id,query_part_index,part_id,modality,media_type,"
                "byte_length,width,height,payload_id,payload_digest,payload_owner_component_id,"
                "payload_owner_kind,payload_owner_id) VALUES "
                "(%s,'query/trial',%s,%s,%s,%s,4,%s,%s,%s,%s,'memory.graph',"
                "'trial.input',%s)"
            ).format(namespace), (
                store.bot_id, index, f"part/{index}", modality, media_type,
                1 if modality == "IMAGE" else None,
                1 if modality == "IMAGE" else None,
                f"payload/{index}", digest,
                _trial_owner_id("query/trial", f"part/{index}"),
            ))

    async with store.pool.connection() as conn:
        dimensions = (await (await conn.execute(sql.SQL(
            "SELECT modality,width,height FROM {}.memory_trial_parts "
            "WHERE bot_id=%s AND query_id='query/trial' ORDER BY query_part_index"
        ).format(namespace), (store.bot_id,))).fetchall())
    assert [(row["modality"], row["width"], row["height"])
            for row in dimensions] == [("TEXT", None, None), ("IMAGE", 1, 1)]
    async with store.pool.connection() as conn:
        with pytest.raises(errors.CheckViolation):
            async with conn.transaction():
                await conn.execute(sql.SQL(
                    "INSERT INTO {}.memory_trial_parts "
                    "(bot_id,query_id,query_part_index,part_id,modality,media_type,"
                    "byte_length,payload_id,payload_digest,payload_owner_component_id,"
                    "payload_owner_kind,payload_owner_id) VALUES "
                    "(%s,'query/trial',2,'part/no-size','IMAGE','image/png',4,"
                    "'payload/no-size',%s,'memory.graph','trial.input',%s)"
                ).format(namespace), (store.bot_id, digest,
                    _trial_owner_id("query/trial", "part/no-size")))

    async def browse(generation, after=-1):
        async with store.pool.connection() as conn:
            return (await (await conn.execute(sql.SQL(
                "SELECT {}.memory_read_query_parts(%s,%s) AS result"
            ).format(namespace), (
                Jsonb({"bot_id": store.bot_id, "generation_id": generation}),
                Jsonb({"query_id": "query/trial", "after_part_index": after, "limit": 1}),
            ))).fetchone())["result"]

    first = await browse("generation/trial")
    assert first["ok"] is True
    Draft202012Validator(package.snapshot.schema(
        "schemas/read-query-parts-output.json"
    )).validate(first["value"])
    assert first["value"]["has_more"] is True
    assert first["value"]["parts"][0]["input_identity"] == {
        "kind": "ADMIN_TRIAL", "ref": "query/trial", "revision": 1,
    }
    assert "source_unit_id" not in first["value"]["parts"][0]
    second = await browse("generation/trial", first["value"]["next_part_index"])
    assert [part["part_kind"] for part in second["value"]["parts"]] == ["IMAGE"]
    assert second["value"]["has_more"] is False
    for index, part in enumerate((first["value"]["parts"][0], second["value"]["parts"][0])):
        facet = {
            "facet_id": f"trial-facet/{index}", "part_id": part["part_id"],
            "modality": part["part_kind"],
            "selector_start_char": 0 if index == 0 else None,
            "selector_end_char": 4 if index == 0 else None,
            "input_payload": part["payload"], "facet_content_digest": digest,
            "input_byte_length": 4,
        }
        body = {
            "protocol": "forest.memory-query-part-facets/1",
            "query_id": "query/trial", "input_identity": part["input_identity"],
            "space_id": digest, "batch_index": index,
            "part_id": part["part_id"], "facets": [facet],
        }
        context = {
            "bot_id": store.bot_id, "generation_id": "generation/trial",
            "operation_id": f"trial-facets/{index}",
            "participant_input_digest": digest_json(body),
        }
        envelope = {"protocol": "forest.uow-step/1",
                    "caller": {"component_id": "memory.graph"}, "input": body}
        async with store.pool.connection() as conn:
            result = (await (await conn.execute(sql.SQL(
                "SELECT {}.memory_prepare_facets(%s,%s) AS result"
            ).format(namespace), (Jsonb(context), Jsonb(envelope)))).fetchone())["result"]
        assert result["ok"], result
        Draft202012Validator(package.snapshot.schema(
            "schemas/prepare-query-part-facets-output.json"
        )).validate(result["value"])
        assert "source_unit_id" not in result["value"]
    async with store.pool.connection() as conn:
        stale_receipt = (await (await conn.execute(sql.SQL(
            "SELECT {}.memory_prepare_facets(%s,%s) AS result"
        ).format(namespace), (
            Jsonb({**context, "generation_id": "generation/replaced"}),
            Jsonb(envelope),
        ))).fetchone())["result"]
    assert stale_receipt["error"]["code"] == "MEMORY_TRIAL_INPUT_UNAVAILABLE"
    async with store.pool.connection() as conn:
        rows = (await (await conn.execute(sql.SQL(
            "SELECT input_kind,input_ref,input_revision,source_unit_id,trial_query_id "
            "FROM {}.embedding_facets WHERE bot_id=%s AND query_id='query/trial' "
            "ORDER BY facet_id"
        ).format(namespace), (store.bot_id,))).fetchall())
    assert len(rows) == 2
    assert all(row["input_kind"] == "ADMIN_TRIAL" and row["input_ref"] == "query/trial"
               and row["input_revision"] == 1 and row["source_unit_id"] is None
               and row["trial_query_id"] == "query/trial" for row in rows)
    complete = {
        "protocol": "forest.uow-step/1", "caller": {"component_id": "memory.graph"},
        "input": {"protocol": "forest.memory-query-complete/1",
                  "query_id": "query/trial", "facet_count": 2,
                  "deletion_epoch_step_id": "payload_epoch"},
        "prior": {"payload_epoch": {"protocol": "forest.payload-epoch/1",
                                    "deletion_epoch": 0}},
    }
    async with store.pool.connection() as conn:
        result = (await (await conn.execute(sql.SQL(
            "SELECT {}.memory_complete_query_facets(%s,%s) AS result"
        ).format(namespace), (
            Jsonb({"bot_id": store.bot_id, "generation_id": "generation/trial"}),
            Jsonb(complete),
        ))).fetchone())["result"]
    assert result["ok"] and result["value"]["facet_count"] == 2, result
    async def claim_trial(generation, token):
        async with store.pool.connection() as conn:
            return (await (await conn.execute(sql.SQL(
                "SELECT {}.memory_claim_embedding_batch(%s,%s) AS result"
            ).format(namespace), (
                Jsonb({"bot_id": store.bot_id, "generation_id": generation}),
                Jsonb({"protocol": "forest.owned-work-claim-request/1",
                       "claim_token": token, "lease_seconds": 360}),
            ))).fetchone())["result"]

    stale_claim = await claim_trial("generation/replaced", "claim/stale-trial")
    assert stale_claim["ok"] and stale_claim["value"] is None
    trial_claim = await claim_trial("generation/trial", "claim/current-trial")
    assert trial_claim["ok"] and trial_claim["value"] is not None, trial_claim
    trial_work = trial_claim["value"]["payload"]
    Draft202012Validator(package.snapshot.schema(
        "schemas/embedding-work-input.json"
    )).validate(trial_work)
    assert [(facet["input_kind"], facet["input_ref"], facet["input_revision"])
            for facet in trial_work["facets"]] == [
                ("ADMIN_TRIAL", "query/trial", 1),
                ("ADMIN_TRIAL", "query/trial", 1),
            ]
    assert all("source_unit_id" not in facet for facet in trial_work["facets"])
    assert trial_work["facets"][1]["width"] == 1
    assert trial_work["facets"][1]["height"] == 1
    assert await claim_trial("generation/trial", "claim/current-trial") == trial_claim
    async with store.pool.connection() as conn:
        eligibility = (await (await conn.execute(sql.SQL(
            "SELECT {}.memory_count_eligible_embedding_facets("
            "%s,%s,%s,0,'PENDING',NULL,%s) AS valid"
        ).format(namespace), (
            store.bot_id, "claim/current-trial", digest, "generation/trial",
        ))).fetchone())["valid"]
        stale_eligibility = (await (await conn.execute(sql.SQL(
            "SELECT {}.memory_count_eligible_embedding_facets("
            "%s,%s,%s,0,'PENDING',NULL,%s) AS valid"
        ).format(namespace), (
            store.bot_id, "claim/current-trial", digest, "generation/replaced",
        ))).fetchone())["valid"]
    assert eligibility == 2 and stale_eligibility == 0
    async with store.pool.connection() as conn, conn.transaction():
        await conn.execute(sql.SQL(
            "INSERT INTO {}.memory_domain_bind_work(bot_id,state) VALUES (%s,'READY')"
        ).format(namespace), (store.bot_id,))
        await conn.execute(sql.SQL(
            "INSERT INTO {}.embedding_budget_days "
            "(bot_id,budget_day,settled_requests,charged_tokens) "
            "VALUES (%s,current_date,1,1)"
        ).format(namespace), (store.bot_id,))
        await conn.execute(sql.SQL(
            "INSERT INTO {}.embedding_budget_reservations "
            "(bot_id,reservation_id,operation_id,input_digest,batch_ref,"
            "policy_revision,budget_day,request_limit,token_limit,reserved_tokens,"
            "deadline_at,state,settlement_operation_id,settlement_input_digest,"
            "charged_tokens,settled_at) VALUES "
            "(%s,'reservation/trial','reserve/trial',%s,'claim/current-trial',"
            "'policy/trial',current_date,10,1000,1,"
            "clock_timestamp()+interval '1 minute','CHARGED',"
            "'settle/trial',%s,1,clock_timestamp())"
        ).format(namespace), (store.bot_id, digest, digest))
        await conn.execute(sql.SQL(
            "INSERT INTO {}.embedding_batch_attempts "
            "(bot_id,attempt_id,claim_id,effect_operation_id,executor_component_id,"
            "executor_generation_id,fence_graph_id,fence_graph_revision,"
            "accepted_deletion_epoch,effect_wire_payload_id,effect_wire_payload_digest,"
            "reservation_id,deadline_at,state,handoff_operation_id,"
            "settlement_operation_id,settlement_input_digest,result_payload_id,"
            "result_payload_digest,dispatched_at,settled_at) VALUES "
            "(%s,'attempt/trial','claim/current-trial','effect/trial',"
            "'embedding.multimodal','generation/trial','graph/trial',1,0,"
            "'effect-wire/trial',%s,'reservation/trial',"
            "clock_timestamp()+interval '1 minute','SUCCEEDED','handoff/trial',"
            "'settlement/trial',%s,'embedding-result/trial',%s,"
            "clock_timestamp(),clock_timestamp())"
        ).format(namespace), (store.bot_id, digest, digest, digest))
        await conn.execute(sql.SQL(
            "UPDATE {}.embedding_claims SET state='ATTEMPT_BOUND' "
            "WHERE bot_id=%s AND claim_id='claim/current-trial'"
        ).format(namespace), (store.bot_id,))
        await conn.execute(sql.SQL(
            "UPDATE {}.embedding_claims SET state='SETTLED',"
            "settled_at=clock_timestamp() "
            "WHERE bot_id=%s AND claim_id='claim/current-trial'"
        ).format(namespace), (store.bot_id,))
        for state in ("PREPARED", "DISPATCHED", "EMBEDDED"):
            await conn.execute(sql.SQL(
                "UPDATE {}.embedding_facets SET state=%s,current_attempt_id='attempt/trial',"
                "vector_digest=CASE WHEN %s='EMBEDDED' THEN %s ELSE NULL END,"
                "result_payload_id=CASE WHEN %s='EMBEDDED' "
                "THEN 'embedding-result/trial' ELSE NULL END,"
                "result_payload_digest=CASE WHEN %s='EMBEDDED' THEN %s ELSE NULL END "
                "WHERE bot_id=%s AND query_id='query/trial'"
            ).format(namespace), (state, state, digest, state, state, digest,
                                  store.bot_id))
    async with store.pool.connection() as conn:
        publish_claim = (await (await conn.execute(sql.SQL(
            "SELECT {}.memory_claim_vector_publish(%s,%s) AS result"
        ).format(namespace), (
            Jsonb({"bot_id": store.bot_id, "generation_id": "generation/trial"}),
            Jsonb({"protocol": "forest.owned-work-claim-request/1",
                   "claim_token": "publish/trial", "lease_seconds": 3}),
        ))).fetchone())["result"]
    assert publish_claim["ok"], publish_claim
    publish_work = publish_claim["value"]["payload"]
    Draft202012Validator(package.snapshot.schema(
        "schemas/vector-publish-work-input.json"
    )).validate(publish_work)
    assert all("source_unit_id" not in facet for facet in publish_work["facets"])
    publish_body = {
        "protocol": "forest.memory-vector-publish/1",
        "attempt_id": "attempt/trial", "claim_token": "publish/trial",
        "space_id": digest,
        "results": [{"facet_id": facet["facet_id"], "status": "PUBLISHED"}
                    for facet in publish_work["facets"]],
    }
    publish_context = {
        "bot_id": store.bot_id, "generation_id": "generation/trial",
        "operation_id": "publish/trial-operation",
        "participant_input_digest": digest_json(publish_body),
    }
    publish_envelope = {
        "protocol": "forest.uow-step/1", "caller": {"component_id": "memory.graph"},
        "input": publish_body,
        "prior": {"payload_epoch": {"protocol": "forest.payload-epoch/1",
                                    "deletion_epoch": 0}},
    }
    with pytest.raises(RuntimeError, match="rollback stale publication"):
        async with store.pool.connection() as conn, conn.transaction():
            stale_publish = (await (await conn.execute(sql.SQL(
                "SELECT {}.memory_publish_vector_batch(%s,%s) AS result"
            ).format(namespace), (
                Jsonb({**publish_context, "generation_id": "generation/replaced"}),
                Jsonb(publish_envelope),
            ))).fetchone())["result"]
            assert stale_publish["ok"] and stale_publish["value"]["deletion_count"] == 2
            deleted = (await (await conn.execute(sql.SQL(
                "SELECT count(*) AS n FROM {}.vector_index_deletions WHERE bot_id=%s"
            ).format(namespace), (store.bot_id,))).fetchone())["n"]
            assert deleted == 2
            deletion_claim = (await (await conn.execute(sql.SQL(
                "SELECT {}.memory_claim_vector_deletion(%s,%s) AS result"
            ).format(namespace), (
                Jsonb({"bot_id": store.bot_id}),
                Jsonb({"protocol": "forest.owned-work-claim-request/1",
                       "claim_token": "delete/trial", "lease_seconds": 360}),
            ))).fetchone())["result"]
            assert deletion_claim["ok"] and deletion_claim["value"] is not None
            deletion_ref = deletion_claim["value"]["payload"]["index_ref_id"]
            deletion_settlement = (await (await conn.execute(sql.SQL(
                "SELECT {}.memory_settle_vector_deletion(%s,%s) AS result"
            ).format(namespace), (
                Jsonb({"bot_id": store.bot_id}),
                Jsonb({"protocol": "forest.owned-work-settle-request/1",
                       "work_id": deletion_ref, "claim_token": "delete/trial",
                       "attempt": 1,
                       "outcome": {"status": "SUCCEEDED",
                                   "result": {"index_ref_id": deletion_ref,
                                              "deleted": True}}}),
            ))).fetchone())["result"]
            assert deletion_settlement["ok"], deletion_settlement
            raise RuntimeError("rollback stale publication")
    async with store.pool.connection() as conn:
        published = (await (await conn.execute(sql.SQL(
            "SELECT {}.memory_publish_vector_batch(%s,%s) AS result"
        ).format(namespace), (
            Jsonb(publish_context), Jsonb(publish_envelope),
        ))).fetchone())["result"]
    assert published["ok"] and published["value"]["ready_count"] == 2, published
    async with store.pool.connection() as conn:
        ready = (await (await conn.execute(sql.SQL(
            "SELECT count(*) AS n FROM {}.embedding_facets "
            "WHERE bot_id=%s AND query_id='query/trial' AND state='READY'"
        ).format(namespace), (store.bot_id,))).fetchone())["n"]
    assert ready == 2
    # Publication committed, then the worker disappeared before settling its
    # claim. A new worker must replay the same batch after the lease expires.
    await asyncio.sleep(3.1)
    async with store.pool.connection() as conn:
        reclaimed = (await (await conn.execute(sql.SQL(
            "SELECT {}.memory_claim_vector_publish(%s,%s) AS result"
        ).format(namespace), (
            Jsonb({"bot_id": store.bot_id, "generation_id": "generation/trial"}),
            Jsonb({"protocol": "forest.owned-work-claim-request/1",
                   "claim_token": "publish/trial/recovered", "lease_seconds": 3}),
        ))).fetchone())["result"]
    assert reclaimed["ok"] and reclaimed["value"]["attempt"] == 2, reclaimed
    assert {facet["facet_id"] for facet in reclaimed["value"]["payload"]["facets"]} == {
        facet["facet_id"] for facet in publish_work["facets"]
    }
    assert all(facet["state"] == "READY"
               for facet in reclaimed["value"]["payload"]["facets"])
    recovered_body = {**publish_body, "claim_token": "publish/trial/recovered"}
    recovered_context = {
        **publish_context,
        "operation_id": "publish/trial-operation/recovered",
        "participant_input_digest": digest_json(recovered_body),
    }
    recovered_envelope = {**publish_envelope, "input": recovered_body}
    async with store.pool.connection() as conn:
        recovered = (await (await conn.execute(sql.SQL(
            "SELECT {}.memory_publish_vector_batch(%s,%s) AS result"
        ).format(namespace), (
            Jsonb(recovered_context), Jsonb(recovered_envelope),
        ))).fetchone())["result"]
    assert recovered["ok"] and recovered["value"]["ready_count"] == 2, recovered
    async with store.pool.connection() as conn:
        stale_settlement = (await (await conn.execute(sql.SQL(
            "SELECT {}.memory_settle_vector_publish(%s,%s) AS result"
        ).format(namespace), (
            Jsonb({"bot_id": store.bot_id}),
            Jsonb({"protocol": "forest.owned-work-settle-request/1",
                   "work_id": "attempt/trial", "claim_token": "publish/trial",
                   "attempt": 1,
                   "outcome": {"status": "SUCCEEDED", "result": {
                       "attempt_id": "attempt/trial", "ready_count": 2,
                       "deletion_count": 0}}}),
        ))).fetchone())["result"]
    assert stale_settlement["error"]["code"] == "MEMORY_VECTOR_PUBLISH_CLAIM_LOST"
    async with store.pool.connection() as conn:
        settled = (await (await conn.execute(sql.SQL(
            "SELECT {}.memory_settle_vector_publish(%s,%s) AS result"
        ).format(namespace), (
            Jsonb({"bot_id": store.bot_id}),
            Jsonb({"protocol": "forest.owned-work-settle-request/1",
                   "work_id": "attempt/trial",
                   "claim_token": "publish/trial/recovered", "attempt": 2,
                   "outcome": {"status": "SUCCEEDED", "result": recovered["value"]}}),
        ))).fetchone())["result"]
    assert settled["ok"], settled
    stale = await browse("generation/replaced")
    assert stale["error"]["code"] == "MEMORY_TRIAL_INPUT_UNAVAILABLE"
    async with store.pool.connection() as conn, conn.transaction():
        await conn.execute(sql.SQL(
            "INSERT INTO {}.query_recall_work(bot_id,query_id,deadline_at) "
            "VALUES (%s,'query/trial',clock_timestamp()+interval '5 minutes')"
        ).format(namespace), (store.bot_id,))
    async def read_recall(query_id, generation):
        async with store.pool.connection() as conn:
            return (await (await conn.execute(sql.SQL(
                "SELECT {}.memory_read_recall_work(%s,%s) AS result"
            ).format(namespace), (
                Jsonb({"bot_id": store.bot_id, "generation_id": generation}),
                Jsonb({"query_id": query_id}),
            ))).fetchone())["result"]

    current_recall = await read_recall("query/trial", "generation/trial")
    assert current_recall["ok"] and current_recall["value"]["state"] == "PENDING"
    stale_recall = await read_recall("query/trial", "generation/replaced")
    assert stale_recall["error"]["code"] == "MEMORY_TRIAL_INPUT_UNAVAILABLE"
    async def claim_recall(generation, token):
        async with store.pool.connection() as conn:
            return (await (await conn.execute(sql.SQL(
                "SELECT {}.memory_claim_recall(%s,%s) AS result"
            ).format(namespace), (
                Jsonb({"bot_id": store.bot_id, "generation_id": generation}),
                Jsonb({"protocol": "forest.owned-work-claim-request/1",
                       "claim_token": token, "lease_seconds": 60}),
            ))).fetchone())["result"]

    current_claim = await claim_recall("generation/trial", "recall/current-trial")
    assert current_claim["ok"] and current_claim["value"]["work_id"] == "query/trial"
    assert await claim_recall("generation/trial", "recall/current-trial") == current_claim
    stale_claim = await claim_recall("generation/replaced", "recall/current-trial")
    assert stale_claim["ok"] and stale_claim["value"] is None
    async with store.pool.connection() as conn:
        failed_recall = (await (await conn.execute(sql.SQL(
            "SELECT state,last_error_code FROM {}.query_recall_work "
            "WHERE bot_id=%s AND query_id='query/trial'"
        ).format(namespace), (store.bot_id,))).fetchone())
    assert failed_recall["state"] == "FAILED"
    assert failed_recall["last_error_code"] == "MEMORY_TRIAL_INPUT_UNAVAILABLE"
    async with store.pool.connection() as conn:
        with pytest.raises(errors.ObjectNotInPrerequisiteState):
            async with conn.transaction():
                await conn.execute(sql.SQL(
                    "DELETE FROM {}.memory_trial_parts WHERE bot_id=%s AND query_id='query/trial'"
                ).format(namespace), (store.bot_id,))
    async with store.pool.connection() as conn, conn.transaction():
        await conn.execute(sql.SQL(
            "INSERT INTO {}.memory_queries "
            "(bot_id,query_id,operation_id,input_digest,input_kind,query_mode,"
            "fusion_policy_revision,accepted_deletion_epoch) VALUES "
            "(%s,'query/expired','trial-expired',%s,'ADMIN_TRIAL','TEXT','fusion/1',0)"
        ).format(namespace), (store.bot_id, digest))
        await conn.execute(sql.SQL(
            "INSERT INTO {}.memory_trial_inputs "
            "(bot_id,query_id,actor_subject_ref,generation_id,created_at,expires_at) "
            "VALUES (%s,'query/expired','web:admin','generation/trial',"
            "clock_timestamp()-interval '2 minutes',clock_timestamp()-interval '1 minute')"
        ).format(namespace), (store.bot_id,))
        await conn.execute(sql.SQL(
            "INSERT INTO {}.memory_trial_parts "
            "(bot_id,query_id,query_part_index,part_id,modality,media_type,byte_length,"
            "payload_id,payload_digest,payload_owner_component_id,payload_owner_kind,"
            "payload_owner_id) VALUES (%s,'query/expired',0,'part/expired','TEXT',"
            "'text/plain',4,'payload/expired',%s,'memory.graph','trial.input',"
            "%s)"
        ).format(namespace), (store.bot_id, digest,
            _trial_owner_id("query/expired", "part/expired")))
        await conn.execute(sql.SQL(
            "INSERT INTO {}.query_recall_work(bot_id,query_id,deadline_at) "
            "VALUES (%s,'query/expired',clock_timestamp()+interval '5 minutes')"
        ).format(namespace), (store.bot_id,))
    expired_recall = await read_recall("query/expired", "generation/trial")
    assert expired_recall["error"]["code"] == "MEMORY_TRIAL_INPUT_UNAVAILABLE"
    expired_claim = await claim_recall("generation/trial", "recall/expired-trial")
    assert expired_claim["ok"] and expired_claim["value"] is None
    async with store.pool.connection() as conn, conn.transaction():
        await conn.execute(sql.SQL(
            "DELETE FROM {}.memory_trial_parts WHERE bot_id=%s AND query_id='query/expired'"
        ).format(namespace), (store.bot_id,))
        await conn.execute(sql.SQL(
            "DELETE FROM {}.memory_trial_inputs WHERE bot_id=%s AND query_id='query/expired'"
        ).format(namespace), (store.bot_id,))


@pytest.mark.postgres
async def test_expired_trial_vectors_are_suppressed_and_deleted(clean_authority, tmp_path):
    store, installer = clean_authority
    kernel = await bootstrap_kernel(store, installer)
    package = await bootstrap_builtin_package(
        Path(__file__).parents[1] / "src/forest/builtin/memory_graph",
        PackageCatalog(tmp_path / "expired-trial-packages"),
        store, installer, kernel=kernel, initial_configuration=_configuration(),
        resolved_contracts={},
    )
    namespace = sql.Identifier(package.descriptor.storage.spec.namespace)
    digest = "sha256:" + "a" * 64
    async with store.pool.connection() as conn, conn.transaction():
        await conn.execute(sql.SQL(
            "INSERT INTO {}.memory_domains "
            "(bot_id,initialization_operation_id,input_digest,space_id,"
                "embedding_model_kind,model_revision_digest,dimensions,max_batch_items,normalization,"
            "fusion_policy_revision) VALUES "
                "(%s,'expired-domain',%s,%s,'TEXT_IMAGE',%s,4096,8,'L2','fusion/1')"
        ).format(namespace), (store.bot_id, digest, digest, digest))
        await conn.execute(sql.SQL(
            "INSERT INTO {}.memory_queries "
            "(bot_id,query_id,operation_id,input_digest,input_kind,query_mode,"
            "fusion_policy_revision,accepted_deletion_epoch) VALUES "
            "(%s,'query/expired-vector','expired-register',%s,'ADMIN_TRIAL',"
            "'TEXT','fusion/1',0)"
        ).format(namespace), (store.bot_id, digest))
        await conn.execute(sql.SQL(
            "INSERT INTO {}.memory_trial_inputs "
            "(bot_id,query_id,actor_subject_ref,generation_id,created_at,expires_at) "
            "VALUES (%s,'query/expired-vector','web:admin','generation/expired',"
            "clock_timestamp()-interval '2 minutes',"
            "clock_timestamp()-interval '1 minute')"
        ).format(namespace), (store.bot_id,))
        await conn.execute(sql.SQL(
            "INSERT INTO {}.memory_trial_parts "
            "(bot_id,query_id,query_part_index,part_id,modality,media_type,"
            "byte_length,payload_id,payload_digest,payload_owner_component_id,"
            "payload_owner_kind,payload_owner_id) VALUES "
            "(%s,'query/expired-vector',0,'part/expired-vector','TEXT',"
            "'text/plain',4,'payload/expired-vector',%s,'memory.graph',"
            "'trial.input',%s)"
        ).format(namespace), (store.bot_id, digest,
            _trial_owner_id("query/expired-vector", "part/expired-vector")))
        await conn.execute(sql.SQL(
            "INSERT INTO {}.embedding_facets "
            "(bot_id,facet_id,purpose,query_id,input_kind,input_ref,input_revision,"
            "trial_query_id,part_id,modality,selector_start_char,selector_end_char,"
            "input_payload_id,input_payload_digest,facet_content_digest,"
            "input_media_type,input_byte_length,space_id,accepted_deletion_epoch,"
            "state,index_ref_id,vector_digest,result_payload_id,"
            "result_payload_digest,published_at) VALUES "
            "(%s,'facet/expired-vector','QUERY','query/expired-vector',"
            "'ADMIN_TRIAL','query/expired-vector',1,'query/expired-vector',"
            "'part/expired-vector','TEXT',0,4,'payload/expired-vector',%s,%s,"
            "'text/plain',4,%s,0,'READY','facet/expired-vector',%s,"
            "'embedding-result/expired',%s,clock_timestamp())"
        ).format(namespace), (store.bot_id, digest, digest, digest, digest, digest))
        await conn.execute(sql.SQL(
            "INSERT INTO {}.query_facet_preparations "
            "(bot_id,query_id,facet_count,accepted_deletion_epoch) "
            "VALUES (%s,'query/expired-vector',1,0)"
        ).format(namespace), (store.bot_id,))
        await conn.execute(sql.SQL(
            "INSERT INTO {}.query_match_preparations "
            "(bot_id,query_id,operation_id,input_digest,space_id,policy,"
            "facet_count,accepted_deletion_epoch) VALUES "
            "(%s,'query/expired-vector','match/expired',%s,%s,%s,1,0)"
        ).format(namespace), (store.bot_id, digest, digest, Jsonb({})))
        await conn.execute(sql.SQL(
            "INSERT INTO {}.query_match_receipts "
            "(bot_id,query_id,query_facet_id,operation_id,input_digest,"
            "accepted_deletion_epoch,match_count) VALUES "
            "(%s,'query/expired-vector','facet/expired-vector',"
            "'match-receipt/expired',%s,0,0)"
        ).format(namespace), (store.bot_id, digest))
        await conn.execute(sql.SQL(
            "INSERT INTO {}.retrieval_bundles "
            "(bot_id,bundle_id,query_id,operation_id,input_digest,"
            "accepted_deletion_epoch,read_count,truncated,association_sampled,"
            "seed_count,result_count) VALUES "
            "(%s,'bundle/expired-vector','query/expired-vector',"
            "'freeze/expired',%s,0,0,false,false,0,0)"
        ).format(namespace), (store.bot_id, digest))
        await conn.execute(sql.SQL(
            "UPDATE {}.memory_queries SET state='FROZEN',"
            "bundle_id='bundle/expired-vector',frozen_at=clock_timestamp() "
            "WHERE bot_id=%s AND query_id='query/expired-vector'"
        ).format(namespace), (store.bot_id,))
        await conn.execute(sql.SQL(
            "INSERT INTO {}.retrieval_materializations "
            "(bot_id,bundle_id,evidence_ref,item_indexes,accepted_deletion_epoch,"
            "content_truncated,missing_count) VALUES "
            "(%s,'bundle/expired-vector',%s,%s,0,false,0)"
        ).format(namespace), (store.bot_id, Jsonb({"payload_id": "evidence/expired"}), []))
        await conn.execute(sql.SQL(
            "INSERT INTO {}.embedding_claims "
            "(bot_id,claim_id,purpose,space_id,source_epoch_high_watermark,"
            "lease_seconds,lease_deadline,item_limit,item_count,claim_payload,state) "
            "VALUES (%s,'claim/expired','QUERY',%s,0,60,"
            "clock_timestamp()-interval '1 minute',1,1,%s,'EXPIRED')"
        ).format(namespace), (store.bot_id, digest, Jsonb({"claim": "expired"})))
        await conn.execute(sql.SQL(
            "INSERT INTO {}.embedding_claim_items "
            "(bot_id,claim_id,item_index,facet_id) "
            "VALUES (%s,'claim/expired',0,'facet/expired-vector')"
        ).format(namespace), (store.bot_id,))
        await conn.execute(sql.SQL(
            "INSERT INTO {}.embedding_claims "
            "(bot_id,claim_id,purpose,space_id,source_epoch_high_watermark,"
            "lease_seconds,lease_deadline,item_limit,item_count,claim_payload) "
            "VALUES (%s,'claim/stale','QUERY',%s,0,60,"
            "clock_timestamp()-interval '1 minute',1,1,%s)"
        ).format(namespace), (store.bot_id, digest, Jsonb({"claim": "stale"})))
        await conn.execute(sql.SQL(
            "INSERT INTO {}.embedding_claim_items "
            "(bot_id,claim_id,item_index,facet_id) "
            "VALUES (%s,'claim/stale',0,'facet/expired-vector')"
        ).format(namespace), (store.bot_id,))
        await conn.execute(sql.SQL(
            "INSERT INTO {}.embedding_claims "
            "(bot_id,claim_id,purpose,space_id,source_epoch_high_watermark,"
            "lease_seconds,lease_deadline,item_limit,item_count,claim_payload) "
            "VALUES (%s,'claim/live','QUERY',%s,0,60,"
            "clock_timestamp()+interval '10 minutes',1,1,%s)"
        ).format(namespace), (store.bot_id, digest, Jsonb({"claim": "live"})))
        await conn.execute(sql.SQL(
            "INSERT INTO {}.embedding_claim_items "
            "(bot_id,claim_id,item_index,facet_id) "
            "VALUES (%s,'claim/live',0,'facet/expired-vector')"
        ).format(namespace), (store.bot_id,))
        await conn.execute(sql.SQL(
            "INSERT INTO {}.embedding_recovery_work (bot_id,claim_id,state) "
            "VALUES (%s,'claim/expired','SETTLED')"
        ).format(namespace), (store.bot_id,))
        await conn.execute(sql.SQL(
            "INSERT INTO {}.embedding_work_settlements "
            "(bot_id,claim_token,input_payload,result_payload) "
            "VALUES (%s,'claim/expired',%s,%s)"
        ).format(namespace), (store.bot_id, Jsonb({}), Jsonb({})))
        await conn.execute(sql.SQL(
            "INSERT INTO {}.query_recall_work (bot_id,query_id,deadline_at,state) "
            "VALUES (%s,'query/expired-vector',clock_timestamp(),'READY')"
        ).format(namespace), (store.bot_id,))
        await conn.execute(sql.SQL(
            "INSERT INTO {}.query_recall_settlements "
            "(bot_id,claim_token,input_payload,result_payload) "
            "VALUES (%s,'recall/expired',%s,%s)"
        ).format(namespace), (store.bot_id, Jsonb({"payload": {
            "query_id": "query/expired-vector"}}), Jsonb({})))
    async with store.pool.connection() as conn:
        expired_read = (await (await conn.execute(sql.SQL(
            "SELECT {}.memory_read_query_parts(%s,%s) AS result"
        ).format(namespace), (
            Jsonb({"bot_id": store.bot_id, "generation_id": "generation/expired"}),
            Jsonb({"query_id": "query/expired-vector", "after_part_index": -1,
                   "limit": 1}),
        ))).fetchone())["result"]
    assert expired_read["error"]["code"] == "MEMORY_TRIAL_INPUT_UNAVAILABLE"
    async def claim_deletion(token):
        async with store.pool.connection() as conn, conn.transaction():
            return (await (await conn.execute(sql.SQL(
                "SELECT {}.memory_claim_vector_deletion(%s,%s) AS result"
            ).format(namespace), (
                Jsonb({"bot_id": store.bot_id}),
                Jsonb({"protocol": "forest.owned-work-claim-request/1",
                       "claim_token": token, "lease_seconds": 360}),
            ))).fetchone())["result"]

    claims = await asyncio.gather(
        claim_deletion("delete/expired-vector/a"),
        claim_deletion("delete/expired-vector/b"),
    )
    assert all(claim["ok"] for claim in claims)
    assert sum(claim["value"] is not None for claim in claims) == 1
    deletion_claim = next(claim for claim in claims if claim["value"] is not None)
    deletion_token = deletion_claim["value"]["claim_token"]
    assert deletion_claim["ok"] and deletion_claim["value"] is not None
    assert deletion_claim["value"]["payload"] == {
        "index_ref_id": "facet/expired-vector", "space_id": digest,
    }
    async with store.pool.connection() as conn:
        row = (await (await conn.execute(sql.SQL(
            "SELECT f.state,d.state AS deletion_state "
            "FROM {}.embedding_facets f JOIN {}.vector_index_deletions d "
            "ON d.bot_id=f.bot_id AND d.index_ref_id=f.facet_id "
            "WHERE f.bot_id=%s AND f.facet_id='facet/expired-vector'"
        ).format(namespace, namespace), (store.bot_id,))).fetchone())
    assert row == {"state": "SUPPRESSED", "deletion_state": "PENDING"}
    async with store.pool.connection() as conn:
        retained = (await (await conn.execute(sql.SQL(
            "SELECT count(*) AS count FROM {}.memory_trial_inputs "
            "WHERE bot_id=%s AND query_id='query/expired-vector'"
        ).format(namespace), (store.bot_id,))).fetchone())["count"]
    assert retained == 1
    async with store.pool.connection() as conn:
        settled = (await (await conn.execute(sql.SQL(
            "SELECT {}.memory_settle_vector_deletion(%s,%s) AS result"
        ).format(namespace), (
            Jsonb({"bot_id": store.bot_id}),
            Jsonb({"protocol": "forest.owned-work-settle-request/1",
                   "work_id": "facet/expired-vector",
                   "claim_token": deletion_token, "attempt": 1,
                   "outcome": {"status": "SUCCEEDED",
                               "result": {"index_ref_id": "facet/expired-vector",
                                          "deleted": True}}}),
        ))).fetchone())["result"]
    assert settled["ok"], settled
    async with store.pool.connection() as conn:
        next_claim = (await (await conn.execute(sql.SQL(
            "SELECT {}.memory_claim_vector_deletion(%s,%s) AS result"
        ).format(namespace), (
            Jsonb({"bot_id": store.bot_id}),
            Jsonb({"protocol": "forest.owned-work-claim-request/1",
                   "claim_token": "delete/expired-vector/next", "lease_seconds": 360}),
        ))).fetchone())["result"]
    assert next_claim["ok"] and next_claim["value"] is None
    async with store.pool.connection() as conn, conn.transaction():
        retained_with_live_claim = (await (await conn.execute(sql.SQL(
            "SELECT count(*) AS count FROM {}.memory_trial_inputs "
            "WHERE bot_id=%s AND query_id='query/expired-vector'"
        ).format(namespace), (store.bot_id,))).fetchone())["count"]
        assert retained_with_live_claim == 1
        await conn.execute(sql.SQL(
            "UPDATE {}.embedding_claims SET state='EXPIRED' "
            "WHERE bot_id=%s AND claim_id='claim/live'"
        ).format(namespace), (store.bot_id,))
    final_claim = await claim_deletion("delete/expired-vector/final")
    assert final_claim["ok"] and final_claim["value"] is None
    async with store.pool.connection() as conn:
        counts = {}
        for table in ("memory_queries", "memory_trial_inputs", "memory_trial_parts",
                      "embedding_facets", "query_facet_preparations",
                      "query_match_preparations", "query_match_receipts",
                      "retrieval_bundles", "retrieval_materializations",
                      "query_recall_work"):
            column = ("trial_query_id" if table == "embedding_facets"
                      else "bundle_id" if table == "retrieval_materializations"
                      else "query_id")
            identifier = ("bundle/expired-vector" if column == "bundle_id"
                          else "query/expired-vector")
            counts[table] = (await (await conn.execute(sql.SQL(
                "SELECT count(*) AS count FROM {}.{} WHERE bot_id=%s AND {}=%s"
            ).format(namespace, sql.Identifier(table), sql.Identifier(column)),
                (store.bot_id, identifier))).fetchone())["count"]
        fence = (await (await conn.execute(sql.SQL(
            "SELECT state FROM {}.vector_index_deletions "
            "WHERE bot_id=%s AND index_ref_id='facet/expired-vector'"
        ).format(namespace), (store.bot_id,))).fetchone())
        claim = (await (await conn.execute(sql.SQL(
            "SELECT count(*) AS count FROM {}.embedding_claims "
            "WHERE bot_id=%s AND claim_id IN "
            "('claim/expired','claim/stale','claim/live')"
        ).format(namespace), (store.bot_id,))).fetchone())["count"]
        recall_settlement = (await (await conn.execute(sql.SQL(
            "SELECT count(*) AS count FROM {}.query_recall_settlements "
            "WHERE bot_id=%s AND claim_token='recall/expired'"
        ).format(namespace), (store.bot_id,))).fetchone())["count"]
    assert counts == {table: 0 for table in counts}
    assert fence == {"state": "DELETED"}
    assert claim == 0 and recall_settlement == 0


@pytest.mark.postgres
async def test_trial_gc_cursor_reaches_later_trial_behind_live_claims(clean_authority, tmp_path):
    store, installer = clean_authority
    kernel = await bootstrap_kernel(store, installer)
    package = await bootstrap_builtin_package(
        Path(__file__).parents[1] / "src/forest/builtin/memory_graph",
        PackageCatalog(tmp_path / "trial-gc-cursor-packages"),
        store, installer, kernel=kernel, initial_configuration=_configuration(),
        resolved_contracts={},
    )
    namespace = sql.Identifier(package.descriptor.storage.spec.namespace)
    digest = "sha256:" + "b" * 64
    async with store.pool.connection() as conn, conn.transaction():
        for index in range(9):
            query_id = f"trial/gc/{index:02}"
            part_id = f"part/{index:02}"
            await conn.execute(sql.SQL(
                "INSERT INTO {}.memory_queries "
                "(bot_id,query_id,operation_id,input_digest,input_kind,query_mode,"
                "fusion_policy_revision,accepted_deletion_epoch) "
                "VALUES (%s,%s,%s,%s,'ADMIN_TRIAL','TEXT','fusion/1',0)"
            ).format(namespace), (store.bot_id, query_id, f"register/{index:02}", digest))
            await conn.execute(sql.SQL(
                "INSERT INTO {}.memory_trial_inputs "
                "(bot_id,query_id,actor_subject_ref,generation_id,created_at,expires_at) "
                "VALUES (%s,%s,'web:admin','generation/gc',"
                "clock_timestamp()-interval '3 minutes',"
                "clock_timestamp()-interval '2 minutes'+%s*interval '1 second')"
            ).format(namespace), (store.bot_id, query_id, index))
            await conn.execute(sql.SQL(
                "INSERT INTO {}.memory_trial_parts "
                "(bot_id,query_id,query_part_index,part_id,modality,media_type,"
                "byte_length,payload_id,payload_digest,payload_owner_component_id,"
                "payload_owner_kind,payload_owner_id) VALUES "
                "(%s,%s,0,%s,'TEXT','text/plain',4,%s,%s,'memory.graph',"
                "'trial.input',%s)"
            ).format(namespace), (store.bot_id, query_id, part_id,
                f"payload/{index:02}", digest, _trial_owner_id(query_id, part_id)))
            if index < 8:
                await conn.execute(sql.SQL(
                    "INSERT INTO {}.query_recall_work "
                    "(bot_id,query_id,deadline_at,state,claim_token,claim_payload,"
                    "lease_deadline) VALUES "
                    "(%s,%s,clock_timestamp()-interval '1 minute','CLAIMED',%s,%s,"
                    "clock_timestamp()+interval '10 minutes')"
                ).format(namespace), (store.bot_id, query_id,
                    f"recall/claimed/{index:02}", Jsonb({"query_id": query_id})))

    async def poll(token):
        async with store.pool.connection() as conn:
            return (await (await conn.execute(sql.SQL(
                "SELECT {}.memory_claim_vector_deletion(%s,%s) AS result"
            ).format(namespace), (
                Jsonb({"bot_id": store.bot_id}),
                Jsonb({"protocol": "forest.owned-work-claim-request/1",
                       "claim_token": token, "lease_seconds": 75}),
            ))).fetchone())["result"]

    first = await poll("trial/gc/poll/1")
    assert first["ok"] and first["value"] is None
    async with store.pool.connection() as conn:
        cursor = (await (await conn.execute(sql.SQL(
            "SELECT last_query_id FROM {}.memory_trial_gc_cursors WHERE bot_id=%s"
        ).format(namespace), (store.bot_id,))).fetchone())
    assert cursor == {"last_query_id": "trial/gc/07"}
    second = await poll("trial/gc/poll/2")
    assert second["ok"] and second["value"] is None
    async with store.pool.connection() as conn:
        rows = (await (await conn.execute(sql.SQL(
            "SELECT query_id FROM {}.memory_trial_inputs "
            "WHERE bot_id=%s ORDER BY query_id"
        ).format(namespace), (store.bot_id,))).fetchall())
    assert [row["query_id"] for row in rows] == [f"trial/gc/{index:02}" for index in range(8)]


def _envelope(
    generation_id,
    selection,
    deletion_epoch,
    *,
    operation="agent/acquire-model-input/1",
):
    return {
        "protocol": "forest.uow-step/1",
        "caller": {
            "component_id": "agent.runtime",
            "generation_id": generation_id,
        },
        "input": {
            "selection_step_id": "conversation",
            "deletion_epoch_step_id": "payload_epoch",
        },
        "prior": {
            "conversation": selection,
            "payload_epoch": deletion_epoch,
        },
        "_operation": operation,
    }


@pytest.mark.postgres
async def test_memory_embedding_identity_is_immutable_across_configuration_plans(
    clean_authority, tmp_path
):
    store, installer = clean_authority
    kernel = await bootstrap_kernel(store, installer)
    source = Path(__file__).parents[1] / "src/forest/builtin/memory_graph"
    catalog = PackageCatalog(tmp_path / "memory-packages")
    package = await bootstrap_builtin_package(
        source, catalog, store, installer, kernel=kernel,
        initial_configuration=_configuration(),
    )
    schema_digest = digest_json(package.snapshot.schema("schemas/config.json"))
    changed_model = _configuration()
    changed_model["embedding"]["model_revision"] = "different-model/1"
    with pytest.raises(ForestError) as caught:
        await package.configuration.plan(
            operation_id="change-memory-model",
            expected_desired_revision=1,
            value=changed_model,
            validation_digest=schema_digest,
        )
    assert caught.value.code == "MODULE_CONFIGURATION_IMMUTABLE"

    # An older writer's outstanding plan cannot bypass the new package policy.
    old_writer = ModuleConfigurationRepository(
        store,
        component_id="memory.graph",
        namespace=package.configuration.namespace,
        payloads=kernel.payload,
    )
    old_plan = await old_writer.plan(
        operation_id="old-writer-model-plan",
        expected_desired_revision=1,
        value=changed_model,
        validation_digest=schema_digest,
    )
    with pytest.raises(ForestError) as caught:
        await package.configuration.apply(
            operation_id="apply-old-model-plan",
            plan_id=old_plan["plan_id"],
            expected_desired_revision=1,
        )
    assert caught.value.code == "MODULE_CONFIGURATION_IMMUTABLE"

    changed_budget = _configuration()
    changed_budget["embedding_budget"]["daily_request_limit"] = 250
    plan = await package.configuration.plan(
        operation_id="change-memory-budget",
        expected_desired_revision=1,
        value=changed_budget,
        validation_digest=schema_digest,
    )
    await package.configuration.apply(
        operation_id="apply-memory-budget",
        plan_id=plan["plan_id"],
        expected_desired_revision=1,
    )
    with pytest.raises(ForestError) as caught:
        await bootstrap_builtin_package(
            source, catalog, store, installer, kernel=kernel,
            initial_configuration=changed_model,
        )
    assert caught.value.code == "MODULE_CONFIGURATION_IMMUTABLE"


@pytest.mark.postgres
@pytest.mark.parametrize(
    "resolution", ["HANDOFF", "ABANDON", "RESPONDED", "NOT_SENT", "UNKNOWN"]
)
async def test_memory_package_qualifies_exact_conversation_batch(
    clean_authority, tmp_path, resolution
):
    store, installer = clean_authority
    kernel = await bootstrap_kernel(store, installer)
    package = await bootstrap_builtin_package(
        Path(__file__).parents[1] / "src/forest/builtin/memory_graph",
        PackageCatalog(tmp_path / "memory-packages"),
        store,
        installer,
        kernel=kernel,
        initial_configuration=_configuration(),
        resolved_contracts={},
    )
    lease = await kernel.runtime.claim_host(
        "memory-package-host",
        "memory-package-incarnation",
        ttl=timedelta(minutes=5),
    )
    host = SimpleNamespace(
        scope_kind="bot",
        bot=SimpleNamespace(store=store),
        kernel=SimpleNamespace(_authority=lambda **_kwargs: lease),
        authority_lease=lambda **_kwargs: lease,
        schema_bundle_installer=installer,
        generation_authority=kernel.runtime,
    )
    registry = RuntimeModuleRegistry(host, _isolated_memory_descriptors(kernel, package))
    await registry.start()
    try:
        instance = registry.get("memory.graph")
        generation_id = instance.generation_identity.generation_id
        participant = instance.unit_of_work_participant()
        payload_participant = registry.get("kernel.payload").unit_of_work_participant()
        domain_input = {
            "protocol": "forest.memory-domain-bind/1",
            "space_id": "sha256:" + "a" * 64,
            "embedding_model_kind": "TEXT_IMAGE",
            "model_revision_digest": "sha256:" + "b" * 64,
            "dimensions": 4096,
            "max_batch_items": 8,
            "normalization": "L2",
            "fusion_policy_revision": "independent-facets/1",
        }
        async with store.pool.connection() as conn, conn.transaction():
            bound = await participant.invoke_in(
                conn,
                "memory.bind-domain-record",
                {
                    "protocol": "forest.uow-step/1",
                    "caller": {
                        "component_id": "memory.graph",
                        "generation_id": generation_id,
                    },
                    "input": domain_input,
                    "prior": {},
                },
                operation_id="memory/domain/test",
                causal_root_kind="memory.domain",
                causal_root_id="bot-bundle",
                parent_call_id="call/memory/domain",
                timeout_seconds=10,
            )
            assert bound["space_id"] == domain_input["space_id"]
        async with store.pool.connection() as conn, conn.transaction():
            with pytest.raises(ForestError) as conflict:
                await participant.invoke_in(
                    conn,
                    "memory.bind-domain-record",
                    {
                        "protocol": "forest.uow-step/1",
                        "caller": {
                            "component_id": "memory.graph",
                            "generation_id": generation_id,
                        },
                        "input": {**domain_input, "space_id": "sha256:" + "c" * 64},
                        "prior": {},
                    },
                    operation_id="memory/domain/conflict",
                    causal_root_kind="memory.domain",
                    causal_root_id="bot-bundle",
                    parent_call_id="call/memory/domain-conflict",
                    timeout_seconds=10,
                )
            assert conflict.value.code == "MEMORY_DOMAIN_IDENTITY_CONFLICT"
        text_payload = await kernel.payload.put_bytes(
            owner_component_id="adapter.onebot",
            owner_kind="context.input",
            owner_id="unit/event/captured:text",
            media_type="text/plain; charset=utf-8",
            content="来自 933179609 群的原始消息".encode(),
            expected_deletion_epoch=await kernel.payload.epoch(),
        )
        image_payload = await kernel.payload.put_bytes(
            owner_component_id="adapter.onebot",
            owner_kind="context.image",
            owner_id="unit/event/captured:image",
            media_type="image/png",
            content=base64.b64decode(
                "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+/lXcAAAAASUVORK5CYII="
            ),
            expected_deletion_epoch=await kernel.payload.epoch(),
        )
        author_payload = await kernel.payload.put_bytes(
            owner_component_id="adapter.onebot",
            owner_kind="message.author",
            owner_id="event/captured",
            media_type="application/json",
            content=b'{"platform":"onebot-v11","user_id":"42"}',
            expected_deletion_epoch=await kernel.payload.epoch(),
        )

        pending_selection = {
            "status": "BATCH",
            "input_batch_id": "batch/memory/pending",
            "conversation_id": "onebot:group:933179609",
            "context_revision": 1,
            "event_ids": ["event/pending"],
        }
        async with store.pool.connection() as conn, conn.transaction():
            deletion_epoch = await payload_participant.invoke_in(
                conn,
                "payload.capture-deletion-epoch",
                {
                    "protocol": "forest.uow-step/1",
                    "caller": {
                        "component_id": "agent.runtime",
                        "generation_id": generation_id,
                    },
                    "input": {"protocol": "forest.payload-epoch-capture/1"},
                    "prior": {},
                },
            )
            pending_envelope = _envelope(
                generation_id,
                pending_selection,
                deletion_epoch,
            )
            pending = await participant.invoke_in(
                conn,
                "memory.qualify-input-batch",
                {key: value for key, value in pending_envelope.items() if key != "_operation"},
                operation_id=pending_envelope["_operation"],
                causal_root_kind="agent.execute",
                causal_root_id="run/memory/package",
                parent_call_id="call/memory/pending",
                timeout_seconds=10,
            )
        assert pending == {
            "protocol": "forest.memory-input-qualified/1",
            "input_batch_ref": "batch/memory/pending",
            "qualified_count": 0,
            "pending_count": 1,
            "deletion_epoch": 0,
        }

        namespace = package.descriptor.storage.spec.namespace
        async with store.pool.connection() as conn, conn.transaction():
            archived_pending = await participant.invoke_in(
                conn,
                "memory.qualify-archive",
                {
                    "protocol": "forest.uow-step/1",
                    "caller": {
                        "component_id": "conversation.scheduler",
                        "generation_id": generation_id,
                    },
                    "input": {},
                    "prior": {
                        "prepare": {
                            "status": "WRITE",
                            "commit": {
                                "conversation_id": "onebot:group:933179609",
                                "compaction_id": "compaction/memory/pending",
                                "archived_unit_count": 1,
                                "units": [{
                                    "event_id": "event/pending",
                                    "input_batch_id": "batch/memory/pending",
                                    "context_revision": 1,
                                }],
                            },
                        },
                        "commit": {
                            "status": "BATCH",
                            "operation_id": "conversation/silent-archive/pending",
                            "conversation_id": "onebot:group:933179609",
                            "classified_through_seq": 1,
                            "selected_input_end_seq": 1,
                            "mailbox_revision": 1,
                            "input_batch_id": "batch/memory/pending",
                            "boundary": "IDLE",
                            "kind": "SILENT_DRAIN",
                            "start_seq": 1,
                            "end_seq": 1,
                            "trigger_cut_seq": None,
                            "session_ref": None,
                            "run_ref": None,
                            "context_revision": 1,
                            "context_view_revision": 1,
                            "event_ids": ["event/pending"],
                            "trigger_claim": None,
                            "memory_archive": {
                                "compaction_ref": "compaction/memory/pending",
                                "event_refs": ["event/pending"],
                            },
                        },
                        "payload_epoch": deletion_epoch,
                    },
                },
                operation_id="memory/archive/pending",
                causal_root_kind="conversation.silent-drain",
                causal_root_id="onebot:group:933179609",
                parent_call_id="call/memory/archive/pending",
                timeout_seconds=10,
            )
            assert archived_pending["memory_archive"]["event_refs"] == ["event/pending"]
            await participant.invoke_in(
                conn,
                "memory.mark-source-unavailable-record",
                {
                    "protocol": "forest.uow-step/1",
                    "caller": {
                        "component_id": "memory.graph",
                        "generation_id": generation_id,
                    },
                    "input": {
                        "conversation_ref": "onebot:group:933179609",
                        "event_ref": "event/pending",
                        "error_code": "SOURCE_NOT_AVAILABLE",
                    },
                    "prior": {},
                },
                operation_id="memory/unavailable/pending",
                causal_root_kind="memory.capture",
                causal_root_id="event/pending",
                parent_call_id="call/memory/unavailable/pending",
                timeout_seconds=10,
            )
        async with store.pool.connection() as conn:
            pending_states = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT i.state AS input_state,a.state AS archive_state "
                        "FROM {}.pending_input_events i JOIN {}.pending_archive_events a "
                        "USING (bot_id,conversation_ref,event_ref) "
                        "WHERE i.bot_id=%s AND i.event_ref=%s"
                    ).format(sql.Identifier(namespace), sql.Identifier(namespace)),
                    (store.bot_id, "event/pending"),
                )
            ).fetchone()
        assert pending_states == {
            "input_state": "UNAVAILABLE",
            "archive_state": "UNAVAILABLE",
        }

        async with store.pool.connection() as conn, conn.transaction():
            deletion_epoch = await payload_participant.invoke_in(
                conn,
                "payload.capture-deletion-epoch",
                {
                    "protocol": "forest.uow-step/1",
                    "caller": {
                        "component_id": "adapter.onebot",
                        "generation_id": generation_id,
                    },
                    "input": {"protocol": "forest.payload-epoch-capture/1"},
                    "prior": {},
                },
            )
            reserved_position = await participant.invoke_in(
                conn,
                "memory.reserve-source-position-record",
                {
                    "protocol": "forest.uow-step/1",
                    "caller": {
                        "component_id": "adapter.onebot",
                        "generation_id": generation_id,
                    },
                    "input": {
                        "identity": {
                            "source_id": "source/group/933179609",
                            "source_kind": "GROUP",
                            "identity_incarnation": 1,
                            "platform": "onebot-v11",
                            "realm_id": "qq",
                            "external_group_id": "933179609",
                            "trunk_id": "trunk/group/933179609",
                        },
                        "source_unit_id": "unit/event/captured",
                        "conversation_ref": "onebot:group:933179609",
                        "event_ref": "event/captured",
                        "conversation_step_id": "conversation",
                    },
                    "prior": {"conversation": {
                        "conversation_id": "onebot:group:933179609",
                        "event_id": "event/captured",
                        "ingress_seq": 1,
                    }},
                },
                operation_id="position/event/captured",
                causal_root_kind="adapter.ingress",
                causal_root_id="event/captured",
                parent_call_id="call/memory/position",
                timeout_seconds=10,
            )
            assert reserved_position["source_order_seq"] == 1
            captured_source = await participant.invoke_in(
                conn,
                "memory.capture-source-record",
                {
                    "protocol": "forest.uow-step/1",
                    "caller": {
                        "component_id": "adapter.onebot",
                        "generation_id": generation_id,
                    },
                    "input": {
                        "capture": {
                            "identity": {
                                "source_id": "source/group/933179609",
                                "source_kind": "GROUP",
                                "identity_incarnation": 1,
                                "platform": "onebot-v11",
                                "realm_id": "qq",
                                "external_group_id": "933179609",
                                "trunk_id": "trunk/group/933179609",
                            },
                            "source_unit_id": "unit/event/captured",
                            "source_revision": 1,
                            "unit_kind": "MESSAGE",
                            "author_subject_ref": "qq:42",
                            "author": {
                                "payload_id": author_payload.payload_id,
                                "digest": author_payload.content_digest,
                                "owner_component_id": "adapter.onebot",
                                "owner_kind": "message.author",
                                "owner_id": "event/captured",
                            },
                            "parts": [
                                {
                                    "part_id": "part/event/captured/text",
                                    "part_index": 0,
                                    "part_kind": "TEXT",
                                    "media_type": text_payload.media_type,
                                    "payload": {
                                        "payload_id": text_payload.payload_id,
                                        "digest": text_payload.content_digest,
                                        "owner_component_id": "adapter.onebot",
                                        "owner_kind": "context.input",
                                        "owner_id": "unit/event/captured:text",
                                    },
                                    "width": None,
                                    "height": None,
                                    "byte_length": text_payload.byte_length,
                                },
                                {
                                    "part_id": "part/event/captured/image",
                                    "part_index": 1,
                                    "part_kind": "IMAGE",
                                    "media_type": image_payload.media_type,
                                    "payload": {
                                        "payload_id": image_payload.payload_id,
                                        "digest": image_payload.content_digest,
                                        "owner_component_id": "adapter.onebot",
                                        "owner_kind": "context.image",
                                        "owner_id": "unit/event/captured:image",
                                    },
                                    "width": 1,
                                    "height": 1,
                                    "byte_length": image_payload.byte_length,
                                },
                            ],
                            "occurrence": {
                                "occurrence_id": "occurrence/event/captured",
                                "binding_ref": "binding/main",
                                "conversation_ref": "onebot:group:933179609",
                                "event_ref": "event/captured",
                                "ingress_seq": None,
                                "observed_at": "2026-09-25T00:00:00+00:00",
                            },
                            "occurred_at": "2026-09-25T00:00:00+00:00",
                        },
                        "conversation_step_id": "conversation",
                        "deletion_epoch_step_id": "payload_epoch",
                    },
                    "prior": {
                        "conversation": {
                            "conversation_id": "onebot:group:933179609",
                            "event_id": "event/captured",
                            "ingress_seq": 1,
                        },
                        "payload_epoch": deletion_epoch,
                    },
                },
                operation_id="capture/event/captured",
                causal_root_kind="adapter.ingress",
                causal_root_id="event/captured",
                parent_call_id="call/memory/capture",
                timeout_seconds=10,
            )
        assert captured_source["eligibility_state"] == "REGISTERED"

        captured_selection = {
            "status": "BATCH",
            "input_batch_id": "batch/memory/captured",
            "conversation_id": "onebot:group:933179609",
            "context_revision": 2,
            "event_ids": ["event/captured"],
        }
        for replay in range(2):
            async with store.pool.connection() as conn, conn.transaction():
                deletion_epoch = await payload_participant.invoke_in(
                    conn,
                    "payload.capture-deletion-epoch",
                    {
                        "protocol": "forest.uow-step/1",
                        "caller": {
                            "component_id": "agent.runtime",
                            "generation_id": generation_id,
                        },
                        "input": {"protocol": "forest.payload-epoch-capture/1"},
                        "prior": {},
                    },
                )
                captured_envelope = _envelope(
                    generation_id,
                    captured_selection,
                    deletion_epoch,
                    operation="agent/acquire-model-input/2",
                )
                captured = await participant.invoke_in(
                    conn,
                    "memory.qualify-input-batch",
                    {
                        key: value
                        for key, value in captured_envelope.items()
                        if key != "_operation"
                    },
                    operation_id=captured_envelope["_operation"],
                    causal_root_kind="agent.execute",
                    causal_root_id="run/memory/package",
                    parent_call_id=f"call/memory/captured/{replay}",
                    timeout_seconds=10,
                )
            assert captured["qualified_count"] == 1
            assert captured["pending_count"] == 0

        async with store.pool.connection() as conn:
            active_unit = await (
                await conn.execute(
                    sql.SQL("SELECT eligibility_state FROM {}.source_units "
                            "WHERE bot_id=%s AND source_unit_id=%s").format(
                        sql.Identifier(namespace)
                    ),
                    (store.bot_id, "unit/event/captured"),
                )
            ).fetchone()
        assert active_unit["eligibility_state"] == "REGISTERED"
        async with store.pool.connection() as conn:
            pending_facets = await (
                await conn.execute(
                    sql.SQL("SELECT count(*) AS n FROM {}.facet_preparation_work "
                            "WHERE bot_id=%s AND source_unit_id=%s").format(
                        sql.Identifier(namespace)
                    ),
                    (store.bot_id, "unit/event/captured"),
                )
            ).fetchone()
        assert pending_facets["n"] == 0

        archived_selection = {
            "status": "BATCH",
            "operation_id": "conversation/silent-archive/test",
            "conversation_id": "onebot:group:933179609",
            "classified_through_seq": 1,
            "selected_input_end_seq": 1,
            "mailbox_revision": 1,
            "input_batch_id": "batch/memory/captured",
            "boundary": "IDLE",
            "kind": "SILENT_DRAIN",
            "start_seq": 1,
            "end_seq": 1,
            "trigger_cut_seq": None,
            "session_ref": None,
            "run_ref": None,
            "context_revision": 2,
            "context_view_revision": 2,
            "event_ids": ["event/captured"],
            "trigger_claim": None,
            "memory_archive": {
                "compaction_ref": "compaction/memory/test",
                "event_refs": ["event/captured"],
            },
        }
        async with store.pool.connection() as conn, conn.transaction():
            epoch = await payload_participant.invoke_in(
                conn,
                "payload.capture-deletion-epoch",
                {
                    "protocol": "forest.uow-step/1",
                    "caller": {
                        "component_id": "conversation.scheduler",
                        "generation_id": generation_id,
                    },
                    "input": {"protocol": "forest.payload-epoch-capture/1"},
                    "prior": {},
                },
            )
            archive = await participant.invoke_in(
                conn,
                "memory.qualify-archive",
                {
                    "protocol": "forest.uow-step/1",
                    "caller": {
                        "component_id": "conversation.scheduler",
                        "generation_id": generation_id,
                    },
                    "input": {},
                    "prior": {
                        "prepare": {
                            "status": "WRITE",
                            "commit": {
                                "conversation_id": "onebot:group:933179609",
                                "compaction_id": "compaction/memory/test",
                                "archived_unit_count": 1,
                                "units": [{
                                    "event_id": "event/captured",
                                    "input_batch_id": "batch/memory/captured",
                                    "context_revision": 2,
                                }],
                            },
                        },
                        "commit": archived_selection,
                        "payload_epoch": epoch,
                    },
                },
                operation_id="memory/archive/test",
                causal_root_kind="conversation.silent-drain",
                causal_root_id="onebot:group:933179609",
                parent_call_id="call/memory/archive",
                timeout_seconds=10,
            )
        assert archive == archived_selection

        async with asyncio.timeout(20):
            while True:
                async with store.pool.connection() as conn:
                    prepared = await (
                        await conn.execute(
                            sql.SQL(
                                "SELECT state,prepared_facet_count,last_error_code "
                                "FROM {}.facet_preparation_work WHERE bot_id=%s "
                                "AND source_unit_id=%s"
                            ).format(sql.Identifier(namespace)),
                            (store.bot_id, "unit/event/captured"),
                        )
                    ).fetchone()
                if prepared is not None and prepared["state"] == "READY":
                    break
                await asyncio.sleep(0.1)
        assert prepared["prepared_facet_count"] == 2
        async with store.pool.connection() as conn:
            saved_facets = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT modality,purpose,state,source_unit_id,space_id "
                        "FROM {}.embedding_facets WHERE bot_id=%s ORDER BY modality DESC"
                    ).format(sql.Identifier(namespace)),
                    (store.bot_id,),
                )
            ).fetchall()
        assert [(row["modality"], row["purpose"], row["state"])
                for row in saved_facets] == [
                    ("TEXT", "DOCUMENT", "PENDING"),
                    ("IMAGE", "DOCUMENT", "PENDING"),
                ]
        assert {row["space_id"] for row in saved_facets} == {domain_input["space_id"]}
        async with store.pool.connection() as conn:
            candidate_row = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT {}.memory_read_embedding_candidates(%s::jsonb,%s::jsonb) "
                        "AS result"
                    ).format(sql.Identifier(namespace)),
                    (Jsonb({"bot_id": store.bot_id}), Jsonb({"limit": 1})),
                )
            ).fetchone()
        candidates = candidate_row["result"]
        assert candidates["ok"] is True
        assert candidates["value"]["space_id"] == domain_input["space_id"]
        assert candidates["value"]["has_more"] is True
        assert len(candidates["value"]["facets"]) == 1
        candidate = candidates["value"]["facets"][0]
        assert candidate["trunk_id"] == "trunk/group/933179609"
        assert candidate["conversation_ref"] == "onebot:group:933179609"
        assert candidate["author_ref"] == "qq:42"
        assert candidate["input_payload"]["digest"] in {
            text_payload.content_digest, image_payload.content_digest,
        }

        async def claim_batch(token, lease_seconds=3):
            async with store.pool.connection() as conn, conn.transaction():
                row = await (
                    await conn.execute(
                        sql.SQL(
                            "SELECT {}.memory_claim_embedding_batch(%s::jsonb,%s::jsonb) "
                            "AS result"
                        ).format(sql.Identifier(namespace)),
                        (
                            Jsonb({"bot_id": store.bot_id}),
                            Jsonb({
                                "protocol": "forest.owned-work-claim-request/1",
                                "claim_token": token,
                                "lease_seconds": lease_seconds,
                            }),
                        ),
                    )
                ).fetchone()
            return row["result"]

        # Freeze the background owner before arbitrating the same SQL queue by hand.
        instance.freeze_owned_admission()
        first_claim, second_claim = await asyncio.gather(
            claim_batch("embedding/claim/one"), claim_batch("embedding/claim/two")
        )
        assert first_claim["ok"] and second_claim["ok"]
        async with store.pool.connection() as conn:
            claim_limits = (await (await conn.execute(sql.SQL(
                "SELECT item_limit FROM {}.embedding_claims WHERE bot_id=%s"
            ).format(sql.Identifier(namespace)), (store.bot_id,))).fetchall())
        assert claim_limits and {row["item_limit"] for row in claim_limits} == {8}
        claimed = [
            claim["value"] for claim in (first_claim, second_claim)
            if claim["value"] is not None
        ]
        assert sum(len(claim["payload"]["facets"]) for claim in claimed) == 2
        assert len({
            facet["facet_id"] for claim in claimed for facet in claim["payload"]["facets"]
        }) == 2
        assert await claim_batch("embedding/claim/one") == first_claim
        assert (await claim_batch("embedding/claim/three"))["value"] is None
        assert all(
            claim["payload"]["space_id"] == domain_input["space_id"]
            and claim["payload"]["source_epoch_high_watermark"] >= 0
            for claim in claimed
        )
        await instance.drain_owned_work()
        await asyncio.sleep(3.1)
        reclaimed = await claim_batch("embedding/claim/after-expiry", lease_seconds=360)
        assert reclaimed["ok"] is True
        assert {
            facet["facet_id"] for facet in reclaimed["value"]["payload"]["facets"]
        } == {
            facet["facet_id"] for claim in claimed for facet in claim["payload"]["facets"]
        }
        claim_id = reclaimed["value"]["claim_token"]
        async with store.pool.connection() as conn, conn.transaction():
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}.embedding_budget_days("
                    "bot_id,budget_day,reserved_requests,reserved_tokens) "
                    "VALUES(%s,current_date,1,100)"
                ).format(sql.Identifier(namespace)),
                (store.bot_id,),
            )
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}.embedding_budget_reservations("
                    "bot_id,reservation_id,operation_id,input_digest,batch_ref,"
                    "policy_revision,budget_day,request_limit,token_limit,"
                    "reserved_tokens,deadline_at) "
                    "VALUES(%s,%s,%s,%s,%s,%s,current_date,1000,100000,100,"
                    "clock_timestamp()+interval '1 hour')"
                ).format(sql.Identifier(namespace)),
                (
                    store.bot_id, "budget/embedding/claim", "reserve/embedding/claim",
                    "sha256:" + "b" * 64, claim_id, "daily-cost-cap/1",
                ),
            )
            prepared_attempt = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT {}.memory_prepare_embedding_attempt(%s::jsonb,%s::jsonb) "
                        "AS result"
                    ).format(sql.Identifier(namespace)),
                    (
                        Jsonb({"bot_id": store.bot_id}),
                        Jsonb({
                            "protocol": "forest.uow-step/1",
                            "caller": {
                                "component_id": "memory.graph",
                                "generation_id": generation_id,
                            },
                            "input": {
                                "protocol": "forest.memory-embedding-attempt-prepare/1",
                                "claim_id": claim_id,
                                "attempt_id": "attempt/embedding/claim",
                                "effect_operation_id": "effect/embedding/claim",
                                "reservation_id": "budget/embedding/claim",
                                "space_id": domain_input["space_id"],
                            },
                            "prior": {
                                "payload_epoch": {
                                    "protocol": "forest.payload-epoch/1",
                                    "deletion_epoch": 0,
                                },
                                "budget": {
                                    "protocol": "forest.memory-embedding-budget/1",
                                    "reservation_id": "budget/embedding/claim",
                                    "state": "RESERVED",
                                },
                                "authorization": {},
                                "effect": {
                                    "protocol": "forest.effect-command-permit/1",
                                    "authorization_ticket_id": "ticket/embedding/claim",
                                    "wire_payload": {
                                        "payload_id": "payload/effect-wire",
                                        "digest": "sha256:" + "d" * 64,
                                        "owner_component_id": "kernel.effect",
                                        "owner_kind": "effect.wire",
                                        "owner_id": "attempt/embedding/claim",
                                    },
                                    "permit": {
                                        "protocol": "forest.effect-permit/1",
                                        "effect_operation_id": "effect/embedding/claim",
                                        "attempt_id": "attempt/embedding/claim",
                                        "attempt_no": 1,
                                        "executor_component_id": "embedding.multimodal",
                                        "executor_generation_id": "generation/embedding",
                                        "fence_kind": "runtime.graph",
                                        "fence_id": "graph/1",
                                        "fence_epoch": 1,
                                        "wire_payload_id": "payload/effect-wire",
                                        "wire_payload_digest": "sha256:" + "d" * 64,
                                        "deadline_at": "2099-01-01T00:00:00+00:00",
                                    },
                                },
                            },
                        }),
                    ),
                )
            ).fetchone()
            claim_state = await (
                await conn.execute(
                    sql.SQL("SELECT state FROM {}.embedding_claims "
                            "WHERE bot_id=%s AND claim_id=%s").format(
                        sql.Identifier(namespace)
                    ),
                    (store.bot_id, claim_id),
                )
            ).fetchone()
            attempt_members = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT count(*) AS n FROM {}.embedding_facets "
                        "WHERE bot_id=%s AND current_attempt_id=%s"
                    ).format(sql.Identifier(namespace)),
                    (store.bot_id, "attempt/embedding/claim"),
                )
            ).fetchone()
        assert attempt_members["n"] == 2
        assert prepared_attempt["result"]["value"]["facet_count"] == 2
        assert claim_state["state"] == "ATTEMPT_BOUND"

        if resolution == "ABANDON":
            budget_input = {
                "protocol": "forest.memory-embedding-budget-settle/1",
                "reservation_id": "budget/embedding/claim",
                "state": "RELEASED",
                "charged_tokens": None,
                "error_code": "MEMORY_EMBEDDING_HANDOFF_NOT_SENT",
            }
            abandon_input = {
                "protocol": "forest.memory-embedding-abandon/1",
                "claim_id": claim_id,
                "attempt_id": "attempt/embedding/claim",
                "effect_operation_id": "effect/embedding/claim",
                "abandonment_operation_id": "effect/abandon/embedding/claim",
                "reservation_id": "budget/embedding/claim",
                "reason_code": "MEMORY_EMBEDDING_HANDOFF_NOT_SENT",
            }
            async with store.pool.connection() as conn, conn.transaction():
                released = await (
                    await conn.execute(
                        sql.SQL(
                            "SELECT {}.memory_settle_embedding_budget(%s::jsonb,%s::jsonb) "
                            "AS result"
                        ).format(sql.Identifier(namespace)),
                        (
                            Jsonb({
                                "bot_id": store.bot_id,
                                "operation_id": "budget/release/embedding/claim",
                                "participant_input_digest": digest_json(budget_input),
                            }),
                            Jsonb({
                                "protocol": "forest.uow-step/1",
                                "caller": {
                                    "component_id": "memory.graph",
                                    "generation_id": generation_id,
                                },
                                "input": budget_input,
                                "prior": {},
                            }),
                        ),
                    )
                ).fetchone()
                assert released["result"]["ok"] is True
                abandoned = await (
                    await conn.execute(
                        sql.SQL(
                            "SELECT {}.memory_abandon_embedding_attempt(%s::jsonb,%s::jsonb) "
                            "AS result"
                        ).format(sql.Identifier(namespace)),
                        (
                            Jsonb({
                                "bot_id": store.bot_id,
                                "operation_id": "memory/abandon/embedding/claim",
                                "participant_input_digest": digest_json(abandon_input),
                            }),
                            Jsonb({
                                "protocol": "forest.uow-step/1",
                                "caller": {
                                    "component_id": "memory.graph",
                                    "generation_id": generation_id,
                                },
                                "input": abandon_input,
                                "prior": {
                                    "effect": {
                                        "attempt_id": "attempt/embedding/claim",
                                        "abandonment_operation_id": "effect/abandon/embedding/claim",
                                        "reason_code": "MEMORY_EMBEDDING_HANDOFF_NOT_SENT",
                                        "cancel_effect": False,
                                    },
                                    "budget": released["result"]["value"],
                                },
                            }),
                        ),
                    )
                ).fetchone()
                states = await (
                    await conn.execute(
                        sql.SQL(
                            "SELECT state,current_attempt_id FROM {}.embedding_facets "
                            "WHERE bot_id=%s AND facet_id IN "
                            "(SELECT facet_id FROM {}.embedding_claim_items "
                            "WHERE bot_id=%s AND claim_id=%s)"
                        ).format(sql.Identifier(namespace), sql.Identifier(namespace)),
                        (store.bot_id, store.bot_id, claim_id),
                    )
                ).fetchall()
            assert abandoned["result"]["value"]["state"] == "NOT_SENT"
            assert len(states) == 2
            assert all(row["state"] == "PENDING" and row["current_attempt_id"] is None
                       for row in states)
            retry_claim = await claim_batch("embedding/claim/after-abandon", lease_seconds=360)
            assert retry_claim["ok"] is True
            assert retry_claim["value"] is None
            async with store.pool.connection() as conn:
                cooldown = await (
                    await conn.execute(
                        sql.SQL(
                            "SELECT count(*) AS n FROM {}.embedding_facets "
                            "WHERE bot_id=%s AND state='PENDING' "
                            "AND next_attempt_at>clock_timestamp()"
                        ).format(sql.Identifier(namespace)),
                        (store.bot_id,),
                    )
                ).fetchone()
            assert cooldown["n"] == 2
            return

        async def handoff(epoch):
            async with store.pool.connection() as conn, conn.transaction():
                row = await (
                    await conn.execute(
                        sql.SQL(
                            "SELECT {}.memory_mark_embedding_handoff(%s::jsonb,%s::jsonb) "
                            "AS result"
                        ).format(sql.Identifier(namespace)),
                        (
                            Jsonb({"bot_id": store.bot_id}),
                            Jsonb({
                                "protocol": "forest.uow-step/1",
                                "caller": {
                                    "component_id": "memory.graph",
                                    "generation_id": generation_id,
                                },
                                "input": {
                                    "protocol": "forest.memory-embedding-handoff/1",
                                    "claim_id": claim_id,
                                    "attempt_id": "attempt/embedding/claim",
                                    "effect_operation_id": "effect/embedding/claim",
                                    "handoff_operation_id": "handoff/embedding/claim",
                                },
                                "prior": {
                                    "payload_epoch": {
                                        "protocol": "forest.payload-epoch/1",
                                        "deletion_epoch": epoch,
                                    },
                                    "effect": {
                                        "effect_operation_id": "effect/embedding/claim",
                                        "attempt_id": "attempt/embedding/claim",
                                        "handoff_operation_id": "handoff/embedding/claim",
                                        "state": "DISPATCHING",
                                        "wire_payload_id": "payload/effect-wire",
                                        "wire_payload_digest": "sha256:" + "d" * 64,
                                        "executor_component_id": "embedding.multimodal",
                                        "executor_generation_id": "generation/embedding",
                                        "fence_id": "graph/1",
                                        "fence_epoch": 1,
                                    },
                                },
                            }),
                        ),
                    )
                ).fetchone()
                return row["result"]

        stale_handoff = await handoff(1)
        assert stale_handoff["ok"] is False
        assert stale_handoff["error"]["code"] == "MEMORY_EMBEDDING_HANDOFF_STALE"
        dispatched_handoff = await handoff(0)
        assert dispatched_handoff["ok"] is True
        assert dispatched_handoff["value"]["facet_count"] == 2
        async with store.pool.connection() as conn:
            dispatched_facets = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT count(*) AS n FROM {}.embedding_facets "
                        "WHERE bot_id=%s AND current_attempt_id=%s AND state='DISPATCHED'"
                    ).format(sql.Identifier(namespace)),
                    (store.bot_id, "attempt/embedding/claim"),
                )
            ).fetchone()
        assert dispatched_facets["n"] == 2

        if resolution == "HANDOFF":
            failed_work = {
                "protocol": "forest.owned-work-settle-request/1",
                "work_id": claim_id,
                "claim_token": claim_id,
                "attempt": 1,
                "outcome": {
                    "status": "FAILED",
                    "error": {
                        "code": "WORKER_RESTARTED",
                        "status": 503,
                        "retryable": False,
                        "unknown": True,
                    },
                },
            }
            async with store.pool.connection() as conn, conn.transaction():
                settled = await (
                    await conn.execute(
                        sql.SQL(
                            "SELECT {}.memory_settle_embedding_batch("
                            "%s::jsonb,%s::jsonb) AS result"
                        ).format(sql.Identifier(namespace)),
                        (Jsonb({"bot_id": store.bot_id}), Jsonb(failed_work)),
                    )
                ).fetchone()
                persisted = await (
                    await conn.execute(
                        sql.SQL(
                            "SELECT c.state AS claim_state,a.state AS attempt_state "
                            "FROM {}.embedding_claims c JOIN {}.embedding_batch_attempts a "
                            "USING(bot_id,claim_id) WHERE c.bot_id=%s AND c.claim_id=%s"
                        ).format(sql.Identifier(namespace), sql.Identifier(namespace)),
                        (store.bot_id, claim_id),
                    )
                ).fetchone()
            assert settled["result"]["value"]["work_id"] == claim_id
            assert persisted == {"claim_state": "ATTEMPT_BOUND",
                                 "attempt_state": "DISPATCHED"}

        if resolution != "HANDOFF":
            observed_state = {
                "RESPONDED": "SUCCEEDED",
                "NOT_SENT": "NOT_APPLIED",
                "UNKNOWN": "UNKNOWN",
            }[resolution]
            budget_state = {
                "RESPONDED": "CHARGED",
                "NOT_SENT": "RELEASED",
                "UNKNOWN": "UNKNOWN",
            }[resolution]
            charged_tokens = 12 if resolution == "RESPONDED" else None
            result_ref = ({
                "payload_id": "payload/embedding-result",
                "digest": "sha256:" + "c" * 64,
                "owner_component_id": "embedding.multimodal",
                "owner_kind": "embedding.result",
                "owner_id": "attempt/embedding/claim",
            } if resolution == "RESPONDED" else None)
            vectors = [
                {
                    "facet_id": facet["facet_id"],
                    "index": index,
                    "vector_digest": "sha256:" + str(index + 1) * 64,
                }
                for index, facet in enumerate(reclaimed["value"]["payload"]["facets"])
            ] if result_ref is not None else []
            budget_input = {
                "protocol": "forest.memory-embedding-budget-settle/1",
                "reservation_id": "budget/embedding/claim",
                "state": budget_state,
                "charged_tokens": charged_tokens,
                "error_code": None if result_ref is not None else "EMBEDDING_RESULT_UNKNOWN",
            }
            memory_input = {
                "protocol": "forest.memory-embedding-observe/1",
                "claim_id": claim_id,
                "attempt_id": "attempt/embedding/claim",
                "effect_operation_id": "effect/embedding/claim",
                "observation_operation_id": "effect/observation/embedding/claim",
                "reservation_id": "budget/embedding/claim",
                "observed_state": observed_state,
                "evidence_digest": "sha256:" + "e" * 64,
                "result": result_ref,
                "vectors": vectors,
                "charged_tokens": charged_tokens,
                "error_code": budget_input["error_code"],
            }
            async with store.pool.connection() as conn, conn.transaction():
                released = await (
                    await conn.execute(
                        sql.SQL(
                            "SELECT {}.memory_settle_embedding_budget(%s::jsonb,%s::jsonb) "
                            "AS result"
                        ).format(sql.Identifier(namespace)),
                        (
                            Jsonb({
                                "bot_id": store.bot_id,
                                "operation_id": "budget/observe/embedding/claim",
                                "participant_input_digest": digest_json(budget_input),
                            }),
                            Jsonb({
                                "protocol": "forest.uow-step/1",
                                "caller": {
                                    "component_id": "memory.graph",
                                    "generation_id": generation_id,
                                },
                                "input": budget_input,
                                "prior": {},
                            }),
                        ),
                    )
                ).fetchone()
                assert released["result"]["ok"] is True
                observed = await (
                    await conn.execute(
                        sql.SQL(
                            "SELECT {}.memory_observe_embedding_attempt(%s::jsonb,%s::jsonb) "
                            "AS result"
                        ).format(sql.Identifier(namespace)),
                        (
                            Jsonb({
                                "bot_id": store.bot_id,
                                "operation_id": "memory/observe/embedding/claim",
                                "participant_input_digest": digest_json(memory_input),
                            }),
                            Jsonb({
                                "protocol": "forest.uow-step/1",
                                "caller": {
                                    "component_id": "memory.graph",
                                    "generation_id": generation_id,
                                },
                                "input": memory_input,
                                "prior": {
                                    "effect": {
                                        "protocol": "forest.effect-command-observation/1",
                                        "observation": {
                                            "attempt_id": "attempt/embedding/claim",
                                            "observation_operation_id":
                                                "effect/observation/embedding/claim",
                                            "observed_status": observed_state,
                                            "resulting_state": observed_state,
                                            "evidence_kind": "executor",
                                            "accepted": True,
                                        },
                                        "evidence_payload": {
                                            "owner_component_id": "kernel.effect",
                                            "owner_kind": "effect.evidence",
                                            "owner_id": "effect/observation/embedding/claim",
                                            "digest": "sha256:" + "e" * 64,
                                        },
                                        "evidence_parents": [result_ref] if result_ref else [],
                                    },
                                    "budget": released["result"]["value"],
                                },
                            }),
                        ),
                    )
                ).fetchone()
                facet_states = await (
                    await conn.execute(
                        sql.SQL(
                            "SELECT state,vector_digest,result_payload_id "
                            "FROM {}.embedding_facets WHERE bot_id=%s "
                            "AND current_attempt_id=%s"
                        ).format(sql.Identifier(namespace)),
                        (store.bot_id, "attempt/embedding/claim"),
                    )
                ).fetchall()
            assert observed["result"]["ok"] is True
            assert observed["result"]["value"]["state"] == {
                "RESPONDED": "SUCCEEDED", "NOT_SENT": "NOT_SENT", "UNKNOWN": "UNKNOWN"
            }[resolution]
            if resolution == "NOT_SENT":
                retry_claim = await claim_batch(
                    "embedding/claim/after-not-sent", lease_seconds=360
                )
                assert retry_claim["ok"] is True
                assert retry_claim["value"] is None
                async with store.pool.connection() as conn:
                    cooldown = await (
                        await conn.execute(
                            sql.SQL(
                                "SELECT count(*) AS n FROM {}.embedding_facets "
                                "WHERE bot_id=%s AND state='PENDING' "
                                "AND next_attempt_at>clock_timestamp()"
                            ).format(sql.Identifier(namespace)),
                            (store.bot_id,),
                        )
                    ).fetchone()
                assert cooldown["n"] == 2
            else:
                assert len(facet_states) == 2
                assert all(row["state"] == (
                    "EMBEDDED" if result_ref else "UNKNOWN"
                ) for row in facet_states)
                assert all((row["result_payload_id"] == "payload/embedding-result")
                           == (result_ref is not None) for row in facet_states)
            if resolution == "UNKNOWN":
                async with store.pool.connection() as conn:
                    details = await (await conn.execute(sql.SQL(
                        "SELECT {}.memory_query_source_embedding_facets(%s,%s) AS result"
                    ).format(sql.Identifier(namespace)), (
                        Jsonb({"bot_id": store.bot_id}),
                        Jsonb({"source_unit_id": "unit/event/captured", "state": "UNKNOWN"}),
                    ))).fetchone()
                detail_page = details["result"]["value"]
                Draft202012Validator(package.snapshot.schema(
                    "schemas/query-source-facets-output.json"
                )).validate(detail_page)
                assert len(detail_page["items"]) == 2
                assert {item["modality"] for item in detail_page["items"]} == {
                    "TEXT", "IMAGE"
                }
                assert all(item["attempt_id"] == "attempt/embedding/claim"
                           and item["claim_id"] == claim_id
                           and item["attempt_state"] == "UNKNOWN"
                           and item["effect_operation_id"] == "effect/embedding/claim"
                           and item["reservation_state"] == "UNKNOWN"
                           and item["reserved_tokens"] > 0
                           and item["budget_day"] is not None
                           for item in detail_page["items"])
                managed = await instance.management_contract.query_authorized(
                    "memory.source-facets.query",
                    {"source_unit_id": "unit/event/captured", "state": "UNKNOWN"},
                    authorization=SimpleNamespace(
                        actor_ref="admin/one", web_subject_ref="admin/one"
                    ),
                )
                assert managed == detail_page
                inspected = await instance.management_contract.query_authorized(
                    "memory.embedding-attempt.inspect",
                    {"attempt_id": "attempt/embedding/claim"},
                    authorization=SimpleNamespace(
                        actor_ref="admin/one", web_subject_ref="admin/one"
                    ),
                )
                Draft202012Validator(package.snapshot.schema(
                    "schemas/inspect-embedding-attempt-output.json"
                )).validate(inspected)
                assert inspected["attempt_state"] == "UNKNOWN"
                assert inspected["reservation_state"] == "UNKNOWN"
                assert inspected["retry_operation_id"] is None
                assert inspected["item_count"] == 2
                assert [item["item_index"] for item in inspected["items"]] == [0, 1]
                assert {item["modality"] for item in inspected["items"]} == {
                    "TEXT", "IMAGE"
                }
                assert all(item["current_attempt_id"] == inspected["attempt_id"]
                           and item["external_group_id"] == "933179609"
                           and item["retry_eligible"]
                           for item in inspected["items"])
                web_actor = SimpleNamespace(
                    actor_ref="admin/one", web_subject_ref="admin/one",
                    target_generation_id=generation_id,
                )

                async def accept_retry_ticket(_conn, *, ticket, **_kwargs):
                    assert ticket is not None

                retry_payload = {
                    "attempt_id": inspected["attempt_id"],
                    "expected_facet_ids": [
                        item["facet_id"] for item in inspected["items"]
                    ],
                    "acknowledge_possible_duplicate_charge": True,
                }
                with pytest.raises(ForestError) as stale:
                    await instance.management_contract.action(
                        "memory.embedding-unknown.retry",
                        operation_id="web:embedding-retry-stale",
                        payload={**retry_payload, "expected_facet_ids": ["wrong"]},
                        authorization=web_actor,
                        authorization_guard=accept_retry_ticket,
                    )
                assert stale.value.code == "MEMORY_EMBEDDING_RETRY_MEMBERS_CHANGED"
                first_retry, concurrent_replay = await asyncio.gather(
                    *(
                        instance.management_contract.action(
                            "memory.embedding-unknown.retry",
                            operation_id="web:embedding-retry-valid",
                            payload=retry_payload,
                            authorization=web_actor,
                            authorization_guard=accept_retry_ticket,
                        )
                        for _ in range(2)
                    )
                )
                assert first_retry == concurrent_replay
                retried = first_retry
                assert retried["state"] == "REQUEUED"
                assert retried["requeued_count"] == 2
                assert retried["previous_budget_state"] == "UNKNOWN"
                assert await instance.management_contract.action(
                    "memory.embedding-unknown.retry",
                    operation_id="web:embedding-retry-valid",
                    payload=retry_payload,
                    authorization=web_actor,
                    authorization_guard=accept_retry_ticket,
                ) == retried
                with pytest.raises(ForestError) as duplicate:
                    await instance.management_contract.action(
                        "memory.embedding-unknown.retry",
                        operation_id="web:embedding-retry-other",
                        payload=retry_payload,
                        authorization=web_actor,
                        authorization_guard=accept_retry_ticket,
                    )
                assert duplicate.value.code == "MEMORY_EMBEDDING_RETRY_BOUNDARY_CLOSED"
                async with store.pool.connection() as conn:
                    state = await (await conn.execute(sql.SQL(
                        "SELECT a.state AS attempt_state,r.state AS budget_state,"
                        "r.reserved_tokens,(SELECT count(*) FROM {}.embedding_facets f "
                        "WHERE f.bot_id=%s AND f.state='PENDING' "
                        "AND f.current_attempt_id IS NULL) AS pending_count "
                        "FROM {}.embedding_batch_attempts a "
                        "JOIN {}.embedding_budget_reservations r "
                        "ON r.bot_id=a.bot_id AND r.reservation_id=a.reservation_id "
                        "WHERE a.bot_id=%s AND a.attempt_id=%s"
                    ).format(
                        sql.Identifier(namespace), sql.Identifier(namespace),
                        sql.Identifier(namespace),
                    ), (
                        store.bot_id, store.bot_id, inspected["attempt_id"]
                    ))).fetchone()
                assert state["attempt_state"] == "UNKNOWN"
                assert state["budget_state"] == "UNKNOWN"
                assert state["reserved_tokens"] == retried["previous_reserved_tokens"]
                assert state["pending_count"] == 2
                historical = await instance.management_contract.query_authorized(
                    "memory.embedding-attempt.inspect",
                    {"attempt_id": inspected["attempt_id"]},
                    authorization=web_actor,
                )
                assert historical["retry_operation_id"] == "web:embedding-retry-valid"
                assert historical["retry_requested_by"] == "admin/one"
                assert historical["retry_created_at"] is not None
                assert all(not item["retry_eligible"] for item in historical["items"])
                recovered = await instance.management_contract.query_authorized(
                    "memory.source-facets.query",
                    {"source_unit_id": "unit/event/captured", "state": "PENDING"},
                    authorization=web_actor,
                )
                assert len(recovered["items"]) == 2
                assert all(item["attempt_id"] is None
                           and item["last_unknown_attempt_id"] == inspected["attempt_id"]
                           for item in recovered["items"])
            return

        identity = instance.generation_identity
        endpoint = instance.runtime_service_handlers()[
            ("agent.tool.memory-exact-recall", 1)
        ]
        receipt = await endpoint.invoke_routed(
            FrozenServiceRoute(
                service_id="agent.tool.memory-exact-recall",
                version=1,
                caller_component_id="agent.runtime",
                provider_component_id="memory.graph",
                provider_generation_id=identity.generation_id,
                graph_id=identity.graph_id,
                graph_revision=identity.graph_revision,
                admission_lane="DATA",
                owned_work=False,
            ),
            RuntimeServiceInvocation(
                "memory-exact-recall/package-test",
                20,
                "agent.tool",
                "tool-call/memory/package-test",
            ),
            (),
            {
                "protocol": "forest.agent-tool-execute/1",
                "tool_call_id": "tool-call/memory/package-test",
                "effect_operation_id": "effect/memory/package-test",
                "effect_attempt_id": "attempt/memory/package-test/1",
                "component_id": "memory.graph",
                "generation_id": identity.generation_id,
                "service_id": "agent.tool.memory-exact-recall",
                "contract_digest": "sha256:" + "5" * 64,
                "binding_digest": "sha256:" + "6" * 64,
                "authority_trigger_ref": "event/captured",
                "authority_principal_ref": "qq:42",
                "arguments": {
                    "source_unit_ids": [
                        "unit/event/captured",
                        "unit/event/missing",
                    ]
                },
            },
        )
        assert receipt["status"] == "SUCCEEDED"
        assert receipt["effect_attempt_id"] == "attempt/memory/package-test/1"
        result = json.loads(
            (await kernel.payload.read_bytes(receipt["result"]["payload_id"])).decode()
        )
        assert result["missing_source_unit_ids"] == ["unit/event/missing"]
        assert result["evidence"] == [
            {
                "source_unit_id": "unit/event/captured",
                "source_revision": 1,
                "source": {
                    "source_id": "source/group/933179609",
                    "source_kind": "GROUP",
                    "platform": "onebot-v11",
                    "realm_id": "qq",
                    "group_id": "933179609",
                    "trunk_id": "trunk/group/933179609",
                },
                "author": {
                    "principal_ref": "qq:42",
                    "identity_origin": "memory_verified",
                },
                "occurred_at": result["evidence"][0]["occurred_at"],
                "parts": [
                    {
                        "part_id": "part/event/captured/text",
                        "type": "text",
                        "text": "来自 933179609 群的原始消息",
                    },
                    {
                        "part_id": "part/event/captured/image",
                        "type": "image",
                        "media_ref": {
                            "payload_id": image_payload.payload_id,
                            "digest": image_payload.content_digest,
                            "owner_component_id": "adapter.onebot",
                            "owner_kind": "context.image",
                            "owner_id": "unit/event/captured:image",
                        },
                        "media_type": "image/png",
                        "width": 1,
                        "height": 1,
                    },
                ],
            }
        ]

        queries = {
            declaration["name"]: declaration
            for declaration in instance.component["domain_queries"]
        }
        trunks = await instance.invoke_management_query(
            queries["memory.list-trunks"], {}, timeout_seconds=10
        )
        assert trunks["items"] == [
            {
                "trunk_id": "trunk/group/933179609",
                "source_id": "source/group/933179609",
                "platform": "onebot-v11",
                "realm_id": "qq",
                "external_group_id": "933179609",
                "registered_sources": 1,
                "eligible_sources": 1,
            }
        ]
        assert trunks["has_more"] is False and trunks["next_after"] is None
        assert (await instance.invoke_management_query(
            queries["memory.list-trunks"],
            {"external_group_id": "933179609", "limit": 1},
            timeout_seconds=10,
        ))["items"] == trunks["items"]
        assert (await instance.invoke_management_query(
            queries["memory.list-trunks"],
            {"after_trunk_id": "trunk/group/933179609", "limit": 1},
            timeout_seconds=10,
        )) == {"items": [], "has_more": False, "next_after": None}
        assert (await instance.invoke_management_query(
            queries["memory.list-trunks"],
            {"external_group_id": "missing-group", "limit": 1},
            timeout_seconds=10,
        ))["items"] == []
        sources = await instance.invoke_management_query(
            queries["memory.query-sources"],
            {"author_subject_ref": "qq:42", "trunk_id": "trunk/group/933179609"},
            timeout_seconds=10,
        )
        assert [item["source_unit_id"] for item in sources["items"]] == [
            "unit/event/captured"
        ]
        assert sources["items"][0]["external_group_id"] == "933179609"
        assert sources["items"][0]["author_subject_ref"] == "qq:42"
        assert sources["items"][0]["eligibility_state"] == "ELIGIBLE"
        assert sources["items"][0]["source_status"] == "ACTIVE"
        assert sources["has_more"] is False
        selected = await instance.invoke_management_query(
            queries["memory.query-sources"],
            {
                "group_source_ids": ["source/group/933179609"],
                "source_id": "source/group/933179609",
                "source_kind": "GROUP",
                "platform": "onebot-v11",
                "realm_id": "qq",
                "external_group_id": "933179609",
                "binding_ref": "binding/main",
                "source_order_from": 1,
                "source_order_to": 2,
                "eligibility_state": "ELIGIBLE",
                "source_status": "ACTIVE",
            },
            timeout_seconds=10,
        )
        assert [item["source_unit_id"] for item in selected["items"]] == [
            "unit/event/captured"
        ]
        assert (await instance.invoke_management_query(
            queries["memory.query-sources"],
            {"group_source_ids": ["source/group/other"]},
            timeout_seconds=10,
        ))["items"] == []
        cursor = {
            "snapshot_accepted_at": sources["snapshot_accepted_at"],
            "snapshot_xid": sources["snapshot_xid"],
            "after_occurred_at": sources["items"][0]["occurred_at"],
            "after_source_unit_id": sources["items"][0]["source_unit_id"],
            "cursor_filter_digest": sources["filter_digest"],
        }
        assert (await instance.invoke_management_query(
            queries["memory.query-sources"],
            {"author_subject_ref": "qq:42", "trunk_id": "trunk/group/933179609", **cursor},
            timeout_seconds=10,
        ))["items"] == []
        with pytest.raises(ForestError) as changed_filter:
            await instance.invoke_management_query(
                queries["memory.query-sources"],
                {"author_subject_ref": "qq:other", "trunk_id": "trunk/group/933179609", **cursor},
                timeout_seconds=10,
            )
        assert changed_filter.value.code == "MEMORY_QUERY_INVALID"
        with pytest.raises(ForestError) as changed_state:
            await instance.invoke_management_query(
                queries["memory.query-sources"],
                {
                    "author_subject_ref": "qq:42",
                    "trunk_id": "trunk/group/933179609",
                    "eligibility_state": "SUPPRESSED",
                    **cursor,
                },
                timeout_seconds=10,
            )
        assert changed_state.value.code == "MEMORY_QUERY_INVALID"
        assert (await instance.invoke_management_query(
            queries["memory.query-sources"],
            {"author_subject_ref": "qq:other"}, timeout_seconds=10
        ))["items"] == []
        async with store.pool.connection() as conn, conn.transaction():
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}.relation_contributions "
                    "(bot_id,contribution_id,operation_id,input_digest,relation_id,"
                    "relation_kind,left_kind,left_id,right_kind,right_id,origin_kind,"
                    "weight_micros,admitted_deletion_epoch,cross_trunk) "
                    "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,false)"
                ).format(sql.Identifier(namespace)),
                (
                    store.bot_id, "contribution/test", "relation/test",
                    "sha256:" + "a" * 64, "relation/topic/test", "ASSOCIATES_WITH",
                    "SOURCE_UNIT", "unit/event/captured", "ENTITY", "topic/test",
                    "MANUAL", 900000, 0,
                ),
            )
            await conn.execute(
                sql.SQL(
                    "INSERT INTO {}.relation_evidence "
                    "(bot_id,contribution_id,evidence_index,source_unit_id,"
                    "source_revision,part_id) VALUES (%s,%s,%s,%s,%s,%s)"
                ).format(sql.Identifier(namespace)),
                (
                    store.bot_id, "contribution/test", 0, "unit/event/captured",
                    1, "part/event/captured/text",
                ),
            )
        relations = await instance.invoke_management_query(
            queries["memory.query-relations"],
            {"endpoint_kind": "SOURCE_UNIT", "endpoint_id": "unit/event/captured"},
            timeout_seconds=10,
        )
        assert len(relations["items"]) == 1
        assert relations["items"][0]["right_id"] == "topic/test"
        assert relations["items"][0]["direction"] == "UNDIRECTED"
        assert relations["items"][0]["origin_kind"] == "MANUAL"
        assert relations["items"][0]["cross_trunk"] is False
        assert relations["items"][0]["evidence_count"] == 1
        assert relations["items"][0]["evidence"] == [{
            "source_unit_id": "unit/event/captured",
            "source_revision": 1,
            "part_id": "part/event/captured/text",
            "trunk_id": "trunk/group/933179609",
            "source_id": "source/group/933179609",
            "source_kind": "GROUP",
            "platform": "onebot-v11",
            "realm_id": "qq",
            "external_group_id": "933179609",
            "author_subject_ref": "qq:42",
        }]
        evidence_page = await instance.management_contract.query(
            "memory.relations.evidence",
            {"relation_id": "relation/topic/test", "contribution_id": "contribution/test",
             "snapshot_xid": relations["snapshot_xid"]},
        )
        assert evidence_page["total_count"] == 1
        assert evidence_page["items"][0]["source_unit_id"] == "unit/event/captured"
        assert evidence_page["items"][0]["evidence_index"] == 0
        assert not evidence_page["has_more"]

        async with store.pool.connection() as conn:
            occurrence = await (
                await conn.execute(
                    sql.SQL(
                        "SELECT eligibility_state,input_batch_ref,context_revision FROM {}."
                        "source_occurrences WHERE bot_id=%s AND occurrence_id=%s"
                    ).format(sql.Identifier(namespace)),
                    (store.bot_id, "occurrence/event/captured"),
                )
            ).fetchone()
            operation_count = (
                await (
                    await conn.execute(
                        sql.SQL(
                            "SELECT count(*) AS n FROM {}.eligibility_operations "
                            "WHERE bot_id=%s"
                        ).format(sql.Identifier(namespace)),
                        (store.bot_id,),
                    )
                ).fetchone()
            )["n"]
        assert occurrence == {
            "eligibility_state": "ELIGIBLE",
            "input_batch_ref": "batch/memory/captured",
            "context_revision": 2,
        }
        assert operation_count == 1
    finally:
        assert await registry.close() == []
