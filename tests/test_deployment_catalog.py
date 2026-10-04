from __future__ import annotations

import asyncio
import hashlib
import os
import shutil
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from importlib.resources import files
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import psycopg
import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from psycopg import sql

from forest.bootstrap_contributions import BootstrapConfigurationRegistry
from forest.builtin_packages import official_builtin_configuration
from forest.deployment_kernel_management import (
    BootstrapCatalogKernelQuery,
    BootstrapProfileKernelCommand,
    DeploymentKernelManagementAuthority,
    LocalStorageProfileKernelCommand,
    ProvisioningJobKernelCommand,
    ProvisioningReconcileKernelCommand,
    SecretPutKernelCommand,
    StorageProfileKernelCommand,
    WebAccountKernelCommand,
)
from forest.deployment_management_app import create_deployment_management_app
from forest.deployment_package_controller import DeploymentPackageController
from forest.deployment_packages import bootstrap_builtin_deployment_package
from forest.deployment_provisioning_driver import DeploymentProvisioningEffectDriver
from forest.deployment_provisioning_owned_work import DeploymentProvisioningOwnedWorkAuthority
from forest.deployment_runtime import DeploymentRuntimeScope
from forest.embeddings import EmbeddingProfile
from forest.errors import ForestError
from forest.kernel_package_catalog_snapshot import (
    package_catalog_artifact_service,
    package_catalog_page_service,
)
from forest.model_contract import ModelCapabilities
from forest.multi_bot_supervisor import MultiBotSupervisor, _BotRunner
from forest.official_bot_provisioning import (
    OfficialBotProvisioner,
    OfficialBotSettingsResolver,
    official_bootstrap_contributions,
    official_bootstrap_schema_digests,
)
from forest.package_capabilities import (
    PackageCallScope,
    PackageServiceError,
    PackageServiceRoute,
)
from forest.package_domain_authority import PostgresPackageDomainAuthority
from forest.packages import PackageCatalog, digest_json, storage_bundle_digest
from forest.postgres_database_allocator import LocalPostgresDatabaseAllocator
from forest.runtime_generation import PreparedGenerationBinding, RuntimeStorageBinding
from forest.runtime_modules import RuntimeModuleRegistry
from forest.schema_bundles import StorageBundleSpec, module_schema_namespace
from forest.storage.bot_store import BotStore
from forest.storage.deployment_audit import DeploymentAuditRepository
from forest.storage.deployment_bootstrap_profiles import DeploymentBootstrapProfileAuthority
from forest.storage.deployment_catalog import DeploymentCatalogRepository
from forest.storage.deployment_generation import DeploymentGenerationAuthority
from forest.storage.deployment_graph_changes import DeploymentGraphChangeAuthority
from forest.storage.deployment_module_configuration import DeploymentModuleConfigurationRepository
from forest.storage.deployment_provisioning_jobs import DeploymentProvisioningJobAuthority
from forest.storage.deployment_schema_bundles import DeploymentSchemaBundleInstaller
from forest.storage.deployment_secret_authority import DeploymentSecretAuthority, SecretMetadata
from forest.storage.deployment_storage_profiles import DeploymentStorageProfileAuthority
from forest.storage.deployment_store import DeploymentStore
from forest.storage.deployment_web_authority import DeploymentWebAuthorityRepository
from forest.storage.deployment_web_identity import DeploymentWebIdentityAuthority
from forest.web_authority_ticket import WebAuthorityTicket


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


@pytest.mark.postgres
async def test_deployment_package_storage_has_its_own_scope_fenced_ledger(deployment_catalog):
    repository = deployment_catalog
    await repository.initialize()
    baseline = b"CREATE TABLE deployment_state (value text NOT NULL);"
    spec = StorageBundleSpec(
        component_id="deployment.sample",
        schema_id="deployment.sample.state",
        schema_version="1.0.0",
        baseline_sha256=hashlib.sha256(baseline).hexdigest(),
        repository_contract="deployment.sample/1",
        scope="deployment",
    )
    digest = storage_bundle_digest(spec, "declarative")
    installer = DeploymentSchemaBundleInstaller(repository.store)
    installed = await installer.install(spec, baseline, digest)
    assert installed["scope"] == "deployment"
    assert installed["deployment_id"] == repository.store.deployment_id
    assert await installer.install(spec, baseline, digest) == installed
    assert (await repository.initialize())["deployment_id"] == repository.store.deployment_id

    authority = PostgresPackageDomainAuthority(
        repository.store,
        object(),
        component_id=spec.component_id,
        runtime_package_digest="sha256:" + "1" * 64,
        storage_spec=spec,
        storage_package_digest=digest,
        storage_execution_profile="declarative",
    )

    def call_scope(scope_id: str) -> PackageCallScope:
        return PackageCallScope(
            bot_id=None,
            component_id=spec.component_id,
            generation_id="generation/1",
            instance_id="instance/1",
            worker_incarnation="worker/1",
            call_id="call/1",
            call_operation_id="operation/1",
            causal_root_kind="management",
            causal_root_id="root/1",
            scope_kind="deployment",
            scope_id=scope_id,
        )

    async with repository.store.pool.connection() as conn:
        await authority._verify_storage(conn, call_scope(repository.store.deployment_id))
        with pytest.raises(ForestError) as caught:
            await authority._verify_storage(conn, call_scope("deployment/other"))
        assert caught.value.code == "DOMAIN_STORAGE_FENCE_MISMATCH"

    with pytest.raises(ForestError) as caught:
        await installer.install(
            StorageBundleSpec(
                spec.component_id,
                spec.schema_id,
                spec.schema_version,
                spec.baseline_sha256,
                spec.repository_contract,
            ),
            baseline,
            digest,
        )
    assert caught.value.code == "INVALID_STORAGE_BUNDLE"


@pytest.mark.postgres
async def test_deployment_configuration_revision_activates_with_real_generation(deployment_catalog):
    catalog = deployment_catalog
    await catalog.initialize()
    source = (
        files("forest.storage.official")
        .joinpath("deployment_module_configuration.sql")
        .read_bytes()
    )
    spec = StorageBundleSpec(
        component_id="deployment.settings",
        schema_id="deployment.settings",
        schema_version="1.0.0",
        baseline_sha256=hashlib.sha256(source).hexdigest(),
        repository_contract="forest.deployment-module-configuration.repository/1",
        scope="deployment",
    )
    storage_digest = storage_bundle_digest(spec, "official")
    await DeploymentSchemaBundleInstaller(catalog.store).install(
        spec, source, storage_digest, execution_profile="official"
    )
    configuration = DeploymentModuleConfigurationRepository(
        catalog.store, component_id=spec.component_id, namespace=spec.namespace
    )
    schema_digest = digest_json({"protocol": "forest.deployment-settings/1"})
    first = await configuration.ensure_initialized(
        operation_id="bootstrap/settings",
        value={"name": "forest"},
        validation_digest=schema_digest,
    )
    assert (
        await configuration.ensure_initialized(
            operation_id="bootstrap/settings",
            value={"name": "forest"},
            validation_digest=schema_digest,
        )
        == first
    )
    assert (await configuration.desired_checked()).value == {"name": "forest"}

    lease = await catalog.claim_supervisor("host/config", "incarnation/config")
    authority = DeploymentGenerationAuthority(catalog.store)
    digest = "sha256:" + "1" * 64

    def binding(current):
        return PreparedGenerationBinding(
            component_id=spec.component_id,
            requires=(),
            package_id="deployment.settings",
            package_version="1.0.0",
            package_digest=digest,
            driver="python-worker",
            trust_level="official",
            descriptor_digest=digest,
            route_contract_digest=digest,
            management_contract_digest=digest,
            management_ui_digest=digest,
            config_revision=current.revision,
            config_payload_id=current.payload_id,
            config_payload_digest=current.payload_digest,
            instance_id="instance/settings",
            worker_incarnation="worker/settings",
            state_schema=1,
            state_contract_digest=digest,
            capability_grants_digest=digest,
            data_admission=False,
            control_admission=True,
            storage=RuntimeStorageBinding(
                schema_id=spec.schema_id,
                schema_version=spec.schema_version,
                namespace=spec.namespace,
                baseline_sha256=spec.baseline_sha256,
                package_digest=storage_digest,
                repository_contract=spec.repository_contract,
                execution_profile="official",
            ),
        )

    graph = await authority.prepare_graph(lease, [binding(first)], str(uuid4()))
    assert (
        await authority.publish_graph(
            lease, graph, configuration_activators={spec.component_id: configuration}
        )
        == graph.graph_revision
    )
    assert await configuration.effective() == first

    plan = await configuration.plan(
        operation_id="plan/settings/2",
        expected_desired_revision=1,
        value={"name": "forest two"},
        validation_digest=schema_digest,
    )
    second = await configuration.apply(
        operation_id="apply/settings/2",
        plan_id=plan["plan_id"],
        expected_desired_revision=1,
    )
    assert (await configuration.desired()).revision == 2
    assert (await configuration.effective()).revision == 1
    assert (
        await configuration.ensure_initialized(
            operation_id="bootstrap/settings",
            value={"name": "forest"},
            validation_digest=schema_digest,
        )
    ).revision == 2
    recovered = await authority.prepare_graph(lease, [binding(first)], str(uuid4()))
    await authority.publish_graph(
        lease, recovered, configuration_activators={spec.component_id: configuration}
    )
    assert (await configuration.desired()).revision == 2
    assert (await configuration.effective()).revision == 1
    wrong = await authority.prepare_graph(
        lease,
        [
            replace(
                binding(second),
                config_payload_id=first.payload_id,
                config_payload_digest=first.payload_digest,
            )
        ],
        str(uuid4()),
    )
    replacement = await authority.prepare_graph(lease, [binding(second)], str(uuid4()))
    with pytest.raises(ForestError) as caught:
        await authority.publish_graph(
            lease, wrong, configuration_activators={spec.component_id: configuration}
        )
    assert caught.value.code == "MODULE_CONFIGURATION_IDENTITY_CONFLICT"
    assert (await configuration.effective()).revision == 1
    assert await authority.current_graph() == recovered
    await authority.publish_graph(
        lease, replacement, configuration_activators={spec.component_id: configuration}
    )
    assert await authority.current_graph() == replacement
    assert (await configuration.effective(include_value=True)).value == {"name": "forest two"}

    async def plan_third(operation_id: str, name: str):
        return await configuration.plan(
            operation_id=operation_id,
            expected_desired_revision=2,
            value={"name": name},
            validation_digest=schema_digest,
        )

    replay_a, replay_b = await asyncio.gather(
        plan_third("plan/settings/replay", "replayed"),
        plan_third("plan/settings/replay", "replayed"),
    )
    assert replay_a == replay_b
    competing = await plan_third("plan/settings/competing", "competing")

    async def apply_third(plan_id: str, operation_id: str):
        return await configuration.apply(
            operation_id=operation_id,
            plan_id=plan_id,
            expected_desired_revision=2,
        )

    outcomes = await asyncio.gather(
        apply_third(replay_a["plan_id"], "apply/settings/replay"),
        apply_third(competing["plan_id"], "apply/settings/competing"),
        return_exceptions=True,
    )
    assert (
        sum(
            isinstance(value, ForestError) and value.code == "MODULE_CONFIGURATION_CONFLICT"
            for value in outcomes
        )
        == 1
    )
    winner = next(value for value in outcomes if not isinstance(value, BaseException))
    assert winner.revision == 3
    assert (await configuration.desired(include_value=True)).value in (
        {"name": "replayed"},
        {"name": "competing"},
    )
    assert (await configuration.effective()).revision == 2
    stale = await authority.prepare_graph(lease, [binding(first)], str(uuid4()))
    with pytest.raises(ForestError) as stale_error:
        await authority.publish_graph(
            lease, stale, configuration_activators={spec.component_id: configuration}
        )
    assert stale_error.value.code == "MODULE_CONFIGURATION_CONFLICT"
    assert await authority.current_graph() == replacement
    assert (await configuration.effective()).revision == 2


@pytest.mark.postgres
async def test_deployment_package_registry_runs_as_reloadable_generation(
    deployment_catalog, tmp_path
):
    repository = deployment_catalog
    await repository.initialize()
    lease = await repository.claim_supervisor("host/package-registry", "incarnation/package-registry")
    package_catalog = PackageCatalog(tmp_path / "packages")
    runtime = DeploymentRuntimeScope(
        repository.store, SimpleNamespace(authority_lease=lambda **_values: lease),
        package_catalog=package_catalog,
    )
    audit_source = Path(__file__).parents[1] / "src/forest/builtin/deployment_audit"
    registry_source = tmp_path / "registry-with-action"
    shutil.copytree(
        Path(__file__).parents[1] / "src/forest/builtin/deployment_package_registry",
        registry_source,
    )
    owned_sql = registry_source / "schema/imports.sql"
    original_hash = hashlib.sha256(owned_sql.read_bytes()).hexdigest()
    owned_sql.write_text(
        owned_sql.read_text(encoding="utf-8") + """
CREATE TABLE management_probe_operations (
    operation_id text PRIMARY KEY,
    input_value text NOT NULL
);
CREATE FUNCTION package_management_probe(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path = pg_catalog AS $$
DECLARE
    saved text;
BEGIN
    IF context->>'scope_kind' IS DISTINCT FROM 'deployment'
       OR context->>'component_id' IS DISTINCT FROM 'deployment.package-registry'
       OR context->>'port_kind' IS DISTINCT FROM 'management_action'
       OR context->>'port_name' IS DISTINCT FROM 'deployment.package-registry.management.probe'
       OR input->>'value' IS NULL THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MANAGEMENT_PROBE_INVALID',
                'message','Probe input is invalid','status',422));
    END IF;
    IF input->>'value' = 'reject' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MANAGEMENT_PROBE_REJECTED',
                'message','Probe deliberately rejected','status',422));
    END IF;
    INSERT INTO __forest_module_schema__.management_probe_operations
        (operation_id,input_value)
    VALUES (context->>'operation_id',input->>'value')
    ON CONFLICT (operation_id) DO NOTHING;
    SELECT input_value INTO saved
    FROM __forest_module_schema__.management_probe_operations
    WHERE operation_id=context->>'operation_id';
    IF saved IS DISTINCT FROM input->>'value' THEN
        RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,
            'error',jsonb_build_object('code','MANAGEMENT_PROBE_CONFLICT',
                'message','Probe operation differs','status',409));
    END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,
        'value',jsonb_build_object('value',saved));
END;
$$;
""", encoding="utf-8",
    )
    (registry_source / "schemas/management-probe-input.json").write_text(
        '{"type":"object","additionalProperties":false,"required":["value"],'
        '"properties":{"value":{"type":"string","minLength":1}}}',
        encoding="utf-8",
    )
    (registry_source / "schemas/management-probe-output.json").write_text(
        '{"type":"object","additionalProperties":false,"required":["value"],'
        '"properties":{"value":{"type":"string"}}}',
        encoding="utf-8",
    )
    manifest_path = registry_source / "module.toml"
    manifest_path.write_text(
        manifest_path.read_text(encoding="utf-8").replace(
            original_hash, hashlib.sha256(owned_sql.read_bytes()).hexdigest(), 1,
        ) + """
[[components.domain_commands]]
name = "deployment.package-registry.management.probe"
contract = "1.0"
function = "package_management_probe"
input_schema = "schemas/management-probe-input.json"
output_schema = "schemas/management-probe-output.json"

[[components.management_actions]]
id = "probe"
port = "deployment.package-registry.management.probe"
priority = "P1"
""", encoding="utf-8",
    )
    controller = DeploymentPackageController(
        runtime, package_catalog, official_sources=(audit_source, registry_source),
    )
    restarted = None
    try:
        initial = await controller.start()
        module = initial.get("deployment.package-registry")
        page = await module.management_contract.query(
            "packages", {"limit": 16, "scope": "deployment"},
        )
        assert {item["component_id"] for item in page["items"]} == {
            "deployment.audit", "deployment.package-registry",
        }
        assert all(item["scope"] == "deployment" for item in page["items"])
        artifact = await module.management_contract.query(
            "artifact", {"package_digest": module.snapshot.digest},
        )
        assert artifact["component_id"] == "deployment.package-registry"
        bot_package = package_catalog.build(
            Path(__file__).parents[1] / "src/forest/builtin/component_worker_host"
        )
        upload_source = PackageCatalog(tmp_path / "upload-source")
        upload_package = upload_source.build(Path(__file__).parents[1] / "examples/counter")
        upload_bytes = upload_source.export_archive(upload_package.digest)
        assert module.management_contract.describe()["artifacts"] == [
            {
                "artifact_id": "archive", "direction": "download",
                "media_type": "application/zip", "max_bytes": 33554432,
                "required_role": "ADMIN", "idempotency_key_required": False,
                "input_schema": module.snapshot.schema("schemas/artifact-input.json"),
            },
            {
                "artifact_id": "import", "direction": "upload",
                "media_type": "application/zip", "max_bytes": 33554432,
                "required_role": "ADMIN", "idempotency_key_required": True,
                "input_schema": module.snapshot.schema("schemas/import-input.json"),
            },
        ]
        archive = await module.management_contract.download_artifact(
            "archive", {"package_digest": bot_package.digest},
        )
        assert archive.media_type == "application/zip"
        assert PackageCatalog(tmp_path / "export-check").import_archive(
            archive.content
        ).digest == bot_package.digest
        identity = DeploymentWebIdentityAuthority(
            repository.store, active_key_id="key/package-registry",
            key_resolver=lambda _key_id: bytes(range(32)),
        )
        now = datetime.now(timezone.utc)
        await identity.initialize_setup(SimpleNamespace(
            setup_id="setup/package-registry0001", pairing_code="r" * 48,
            issued_at=now, expires_at=now + timedelta(hours=1),
        ))
        await identity.complete_setup(
            pairing_code="r" * 48, username="admin",
            display_name="Administrator", password="deployment-web-password",
        )
        app = create_deployment_management_app(
            deployment_id=repository.store.deployment_id,             deployment_registry_provider=lambda: controller.registry,
            identity_authority=identity,
            deployment_web_receipts=DeploymentWebAuthorityRepository(repository.store),
            deployment_audit=DeploymentAuditRepository(repository.store),
            allowed_origin="http://forest.test",
        )
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://forest.test"
        ) as client:
            login = await client.post(
                "/management/v1/auth/login",
                headers={"origin": "http://forest.test"},
                json={"username": "admin", "password": "deployment-web-password"},
            )
            assert login.status_code == 200, login.text
            action_path = (
                "/management/v1/scopes/deployment/deployment/modules/"
                "deployment.package-registry/actions/probe"
            )
            action_headers = {
                "origin": "http://forest.test",
                "x-forest-csrf": client.cookies["forest_management_csrf"],
                "idempotency-key": "deployment-package-action-probe",
            }
            action = await client.post(
                action_path, headers=action_headers, json={"value": "action-works"},
            )
            assert action.status_code == 200, action.text
            assert action.json() == {"value": "action-works"}
            replayed_action = await client.post(
                action_path, headers=action_headers, json={"value": "action-works"},
            )
            assert replayed_action.json() == action.json()
            denied_action = await client.post(
                action_path,
                headers={**action_headers, "idempotency-key": "deployment-probe-reject"},
                json={"value": "reject"},
            )
            assert denied_action.status_code == 422, denied_action.text
            assert denied_action.json()["code"] == "MANAGEMENT_PROBE_REJECTED"
            probe_schema = module_schema_namespace("forest.deployment.package-imports")
            async with repository.store.pool.connection() as conn:
                probe_rows = await (await conn.execute(
                    sql.SQL("SELECT input_value FROM {}.management_probe_operations")
                    .format(sql.Identifier(probe_schema))
                )).fetchall()
                accepted_probe = await (await conn.execute(
                    "SELECT action_key FROM forest_deployment.web_authority_receipts "
                    "WHERE action_key='probe'"
                )).fetchall()
            assert [row["input_value"] for row in probe_rows] == ["action-works"]
            assert len(accepted_probe) == 1
            download = await client.post(
                "/management/v1/scopes/deployment/deployment/modules/"
                "deployment.package-registry/artifacts/archive/download",
                headers={
                    "origin": "http://forest.test",
                    "x-forest-csrf": client.cookies["forest_management_csrf"],
                },
                json={"package_digest": bot_package.digest},
            )
            assert download.status_code == 200, download.text
            assert download.headers["content-type"] == "application/zip"
            assert download.headers["x-content-type-options"] == "nosniff"
            assert len(download.content) == int(download.headers["content-length"])
            assert PackageCatalog(tmp_path / "http-export-check").import_archive(
                download.content
            ).digest == bot_package.digest
            oversized = await client.post(
                "/management/v1/scopes/deployment/deployment/modules/"
                "deployment.package-registry/artifacts/archive/download",
                headers={
                    "origin": "http://forest.test",
                    "x-forest-csrf": client.cookies["forest_management_csrf"],
                    "content-type": "application/json",
                },
                content=b"x" * 16385,
            )
            assert oversized.status_code == 413
            upload_path = (
                "/management/v1/scopes/deployment/deployment/modules/"
                "deployment.package-registry/artifacts/import/upload"
            )
            upload_headers = {
                "origin": "http://forest.test",
                "x-forest-csrf": client.cookies["forest_management_csrf"],
                "content-type": "application/zip",
                "x-forest-artifact-input": "{}",
                "x-forest-artifact-length": str(len(upload_bytes)),
                "x-forest-artifact-sha256": "sha256:" + hashlib.sha256(upload_bytes).hexdigest(),
                "idempotency-key": "test-package-upload-1",
            }
            uploaded = await client.post(
                upload_path, headers=upload_headers, content=upload_bytes,
            )
            assert uploaded.status_code == 200, uploaded.text
            assert uploaded.json()["package_digest"] == upload_package.digest
            assert package_catalog.get(upload_package.digest).digest == upload_package.digest
            replay = await client.post(
                upload_path, headers=upload_headers, content=upload_bytes,
            )
            assert replay.status_code == 200, replay.text
            assert replay.json() == uploaded.json()
            async with repository.store.pool.connection() as conn:
                receipt_row = await (await conn.execute(
                    "SELECT target_operation_ref,web_subject_ref FROM "
                    "forest_deployment.web_authority_receipts "
                    "WHERE action_key='artifact.import.upload' AND input_digest=%s",
                    (digest_json({
                        "payload": {}, "content_length": len(upload_bytes),
                        "content_sha256": upload_headers["x-forest-artifact-sha256"],
                    }),),
                )).fetchone()
            receipt_input = {
                "operation_id": receipt_row["target_operation_ref"],
                "actor_ref": receipt_row["web_subject_ref"],
                "archive_sha256": upload_headers["x-forest-artifact-sha256"],
            }
            recovered = await client.post(
                "/management/v1/scopes/deployment/deployment/modules/"
                "deployment.package-registry/queries/import.receipt",
                headers={
                    "origin": "http://forest.test",
                    "x-forest-csrf": client.cookies["forest_management_csrf"],
                },
                json=receipt_input,
            )
            assert recovered.status_code == 200, recovered.text
            assert recovered.json() == {
                "protocol": "forest.package-import-receipt/1", "found": True,
                "package_digest": upload_package.digest,
            }
            imports = await client.post(
                "/management/v1/scopes/deployment/deployment/modules/"
                "deployment.package-registry/queries/import.page",
                headers={
                    "origin": "http://forest.test",
                    "x-forest-csrf": client.cookies["forest_management_csrf"],
                },
                json={"operation_id": receipt_input["operation_id"]},
            )
            assert imports.status_code == 200, imports.text
            assert len(imports.json()["records"]) == 1
            assert imports.json()["records"][0]["status"] == "SUCCEEDED"
            assert imports.json()["records"][0]["package_digest"] == upload_package.digest
            bad_bytes = b"x" * len(upload_bytes)
            bad_replay = await client.post(
                upload_path, headers=upload_headers, content=bad_bytes,
            )
            assert bad_replay.status_code == 422, bad_replay.text
            still_succeeded = await client.post(
                "/management/v1/scopes/deployment/deployment/modules/"
                "deployment.package-registry/queries/import.page",
                headers={
                    "origin": "http://forest.test",
                    "x-forest-csrf": client.cookies["forest_management_csrf"],
                },
                json={"operation_id": receipt_input["operation_id"]},
            )
            assert still_succeeded.json()["records"][0]["status"] == "SUCCEEDED"
            mismatched_headers = {
                **upload_headers,
                "idempotency-key": "test-package-upload-bad-digest",
                "x-forest-artifact-sha256": "sha256:" + "0" * 64,
            }
            mismatched = await client.post(
                upload_path, headers=mismatched_headers, content=upload_bytes,
            )
            assert mismatched.status_code == 422, mismatched.text
            assert mismatched.json()["code"] == "PACKAGE_ARCHIVE_DIGEST_MISMATCH"
            rejected = await client.post(
                "/management/v1/scopes/deployment/deployment/modules/"
                "deployment.package-registry/queries/import.page",
                headers={
                    "origin": "http://forest.test",
                    "x-forest-csrf": client.cookies["forest_management_csrf"],
                },
                json={"status": "REJECTED"},
            )
            assert rejected.status_code == 200, rejected.text
            assert len(rejected.json()["records"]) == 1
            assert rejected.json()["records"][0]["error_code"] == (
                "PACKAGE_ARCHIVE_DIGEST_MISMATCH"
            )
            rejected_import = rejected.json()["records"][0]
            assert package_catalog.close_import_if_absent(
                operation_id=rejected_import["operation_id"],
                actor_ref=rejected_import["actor_ref"],
                archive_sha256=rejected_import["archive_sha256"],
            ) == {"found": False, "package_digest": None, "closed": True}
            with pytest.raises(ForestError) as closed_import:
                package_catalog.import_archive_idempotent(
                    upload_bytes,
                    operation_id=rejected_import["operation_id"],
                    actor_ref=rejected_import["actor_ref"],
                    archive_sha256=rejected_import["archive_sha256"],
                )
            assert closed_import.value.code == "PACKAGE_IMPORT_CLOSED"
            terminal_replay = await client.post(
                upload_path, headers=mismatched_headers, content=upload_bytes,
            )
            assert terminal_replay.status_code == 409, terminal_replay.text
            assert terminal_replay.json()["code"] == "PACKAGE_IMPORT_ALREADY_TERMINAL"
            changed_bytes = upload_bytes + b"changed"
            changed_headers = {
                **upload_headers,
                "x-forest-artifact-length": str(len(changed_bytes)),
                "x-forest-artifact-sha256":
                    "sha256:" + hashlib.sha256(changed_bytes).hexdigest(),
            }
            conflict = await client.post(
                upload_path, headers=changed_headers, content=changed_bytes,
            )
            assert conflict.status_code == 409, conflict.text
            reserved_headers = {
                **upload_headers,
                "idempotency-key": "test-package-upload-reserved",
                "x-forest-artifact-length": str(len(archive.content)),
                "x-forest-artifact-sha256":
                    "sha256:" + hashlib.sha256(archive.content).hexdigest(),
            }
            reserved = await client.post(
                upload_path, headers=reserved_headers, content=archive.content,
            )
            assert reserved.status_code == 403, reserved.text
            assert reserved.json()["code"] == "PACKAGE_RESERVED_NAMESPACE"
            reserved_page = await client.post(
                "/management/v1/scopes/deployment/deployment/modules/"
                "deployment.package-registry/queries/import.page",
                headers={
                    "origin": "http://forest.test",
                    "x-forest-csrf": client.cookies["forest_management_csrf"],
                },
                json={"status": "RESERVED", "limit": 1},
            )
            assert reserved_page.status_code == 200, reserved_page.text
            assert len(reserved_page.json()["records"]) == 1
            assert reserved_page.json()["records"][0]["package_digest"] is None
            assert reserved_page.json()["records"][0]["error_code"] is None
            reserved_import = reserved_page.json()["records"][0]
            namespace = module_schema_namespace("forest.deployment.package-imports")
            async with repository.store.pool.connection() as conn, conn.transaction():
                await conn.execute(
                    sql.SQL("UPDATE {}.package_import_requests SET created_at="
                            "clock_timestamp()-interval '2 minutes' "
                            "WHERE operation_id=%s").format(sql.Identifier(namespace)),
                    (reserved_import["operation_id"],),
                )
            for _ in range(100):
                recovered_reserved = await module.management_contract.query(
                    "import.page", {"operation_id": reserved_import["operation_id"]},
                )
                if recovered_reserved["records"][0]["status"] == "INTERRUPTED":
                    break
                await asyncio.sleep(0.1)
            assert recovered_reserved["records"][0]["status"] == "INTERRUPTED"
            assert package_catalog.close_import_if_absent(
                operation_id=reserved_import["operation_id"],
                actor_ref=reserved_import["actor_ref"],
                archive_sha256=reserved_import["archive_sha256"],
            ) == {"found": False, "package_digest": None, "closed": True}

            lost_settlement = {
                "operation_id": "test-recovery-published",
                "actor_ref": "admin:recovery",
                "archive_sha256": "sha256:" + hashlib.sha256(upload_bytes).hexdigest(),
            }
            async with repository.store.pool.connection() as conn, conn.transaction():
                await conn.execute(
                    sql.SQL("INSERT INTO {}.package_import_requests "
                            "(operation_id,actor_ref,generation_id,content_length,"
                            "archive_sha256,status,created_at) VALUES "
                            "(%s,%s,%s,%s,%s,'RESERVED',"
                            "clock_timestamp()-interval '2 minutes')")
                    .format(sql.Identifier(namespace)),
                    (
                        lost_settlement["operation_id"], lost_settlement["actor_ref"],
                        initial.graph_identity.generations["deployment.package-registry"],
                        len(upload_bytes), lost_settlement["archive_sha256"],
                    ),
                )
                assert package_catalog.import_archive_idempotent(
                    upload_bytes, **lost_settlement,
                ).digest == upload_package.digest
            for _ in range(100):
                recovered_published = await module.management_contract.query(
                    "import.page", {"operation_id": lost_settlement["operation_id"]},
                )
                if recovered_published["records"][0]["status"] == "SUCCEEDED":
                    break
                await asyncio.sleep(0.1)
            assert recovered_published["records"][0]["status"] == "SUCCEEDED"
            assert recovered_published["records"][0]["package_digest"] == (
                upload_package.digest
            )

            retry_operation = "test-recovery-effect-failure"
            close_driver = module.management_contract.adapter._effect_drivers[
                "deployment.catalog.import-close"
            ]
            original_invoke = close_driver.invoke
            failed_once = False

            async def fail_first_close(payload, *, scope, invocation):
                nonlocal failed_once
                if payload["operation_id"] == retry_operation and not failed_once:
                    failed_once = True
                    raise ForestError(
                        "PACKAGE_CATALOG_TEMPORARY", "Catalog is temporarily unavailable", 503,
                    )
                return await original_invoke(payload, scope=scope, invocation=invocation)

            close_driver.invoke = fail_first_close
            async with repository.store.pool.connection() as conn, conn.transaction():
                await conn.execute(
                    sql.SQL("INSERT INTO {}.package_import_requests "
                            "(operation_id,actor_ref,generation_id,content_length,"
                            "archive_sha256,status,created_at) VALUES "
                            "(%s,%s,%s,%s,%s,'RESERVED',"
                            "clock_timestamp()-interval '2 minutes')")
                    .format(sql.Identifier(namespace)),
                    (
                        retry_operation, "admin:recovery",
                        initial.graph_identity.generations["deployment.package-registry"],
                        len(upload_bytes), lost_settlement["archive_sha256"],
                    ),
                )
            for _ in range(100):
                retry_page = await module.management_contract.query(
                    "import.page", {"operation_id": retry_operation},
                )
                if retry_page["records"][0]["recovery_error_code"] is not None:
                    break
                await asyncio.sleep(0.1)
            assert failed_once
            assert retry_page["records"][0]["status"] == "RESERVED"
            assert retry_page["records"][0]["recovery_error_code"] == (
                "PACKAGE_CATALOG_TEMPORARY"
            )
            async with repository.store.pool.connection() as conn, conn.transaction():
                await conn.execute(
                    sql.SQL("UPDATE {}.package_import_requests SET "
                            "recovery_next_attempt_at=clock_timestamp() "
                            "WHERE operation_id=%s").format(sql.Identifier(namespace)),
                    (retry_operation,),
                )
            for _ in range(100):
                retry_page = await module.management_contract.query(
                    "import.page", {"operation_id": retry_operation},
                )
                if retry_page["records"][0]["status"] == "INTERRUPTED":
                    break
                await asyncio.sleep(0.1)
            assert retry_page["records"][0]["status"] == "INTERRUPTED"
            assert retry_page["records"][0]["recovery_error_code"] is None
        bridge = SimpleNamespace(package_registry_reader=controller.read_package_registry)
        bot_page = await package_catalog_page_service(bridge)({
            "protocol": "forest.package-catalog-page-request/1", "limit": 16,
        })
        assert {item["package_digest"] for item in bot_page["items"]} == {
            bot_package.digest, upload_package.digest,
        }
        assert all(item["scope"] == "bot" for item in bot_page["items"])
        bot_bridge_artifact = await package_catalog_artifact_service(bridge)({
            "protocol": "forest.package-catalog-artifact-request/1",
            "package_digest": bot_package.digest,
        })
        assert bot_bridge_artifact["package_digest"] == bot_package.digest
        with pytest.raises(ForestError) as bridge_scope:
            await package_catalog_artifact_service(bridge)({
                "protocol": "forest.package-catalog-artifact-request/1",
                "package_digest": module.snapshot.digest,
            })
        assert bridge_scope.value.code == "PACKAGE_SCOPE_UNSUPPORTED"
        bot_artifact = await module.management_contract.query(
            "artifact", {"package_digest": bot_package.digest},
        )
        assert bot_artifact["scope"] == "bot"
        assert any(
            item["package_digest"] == bot_package.digest
            for item in (await module.management_contract.query(
                "packages", {"limit": 16},
            ))["items"]
        )
        untrusted_source = tmp_path / "untrusted-package-registry"
        shutil.copytree(registry_source, untrusted_source)
        untrusted_manifest = untrusted_source / "module.toml"
        untrusted_manifest.write_text(
            untrusted_manifest.read_text(encoding="utf-8").replace(
                'version = "1.6.0"', 'version = "1.6.2"', 1
            ), encoding="utf-8",
        )
        untrusted = package_catalog.build(untrusted_source)
        with pytest.raises(ForestError) as untrusted_port:
            await controller.preview_change(
                operation_kind="REPLACE", component_id="deployment.package-registry",
                package_digest=untrusted.digest,
                expected_graph_revision=initial.graph_identity.graph_revision,
            )
        assert untrusted_port.value.code == "OFFICIAL_PACKAGE_NOT_REGISTERED"
        next_source = tmp_path / "next-package-registry"
        shutil.copytree(registry_source, next_source)
        manifest = next_source / "module.toml"
        manifest.write_text(
            manifest.read_text(encoding="utf-8").replace(
                'version = "1.6.0"', 'version = "1.6.1"', 1
            ), encoding="utf-8",
        )
        replacement = package_catalog.build_official(next_source)
        audit_generation = initial.graph_identity.generations["deployment.audit"]
        async with module.management_contract.open_artifact_upload("import", {}):
            async with module.management_contract.open_artifact_upload("import", {}):
                with pytest.raises(ForestError) as capacity:
                    async with module.management_contract.open_artifact_upload("import", {}):
                        pass
                assert capacity.value.code == "PACKAGE_ARTIFACT_BUSY"
        async with module.management_contract.open_artifact_upload("import", {}) as _permit:
            replacing = asyncio.create_task(controller.replace(
                "deployment.package-registry", replacement.digest,
                expected_graph_revision=initial.graph_identity.graph_revision,
            ))
            for _ in range(100):
                if module.state.name == "QUIESCING":
                    break
                await asyncio.sleep(0.05)
            assert module.state.name == "QUIESCING"
            assert not replacing.done()
            with pytest.raises(ForestError) as old_generation:
                async with module.management_contract.open_artifact_upload("import", {}):
                    pass
            assert old_generation.value.code == "RUNTIME_GENERATION_NOT_ACTIVE"
        updated = await replacing
        assert updated.graph_identity.generations["deployment.audit"] == audit_generation
        assert (await package_catalog_page_service(bridge)({
            "protocol": "forest.package-catalog-page-request/1", "limit": 16,
        }))["items"]
        page = await updated.get("deployment.package-registry").management_contract.query(
            "packages", {"limit": 16},
        )
        assert any(item["package_digest"] == replacement.digest for item in page["items"])
        restarted = DeploymentPackageController(
            runtime, package_catalog, official_sources=(audit_source, registry_source),
        )
        recovered = await restarted.start()
        assert recovered.get("deployment.package-registry").snapshot.digest == replacement.digest
        assert (await recovered.get("deployment.package-registry").management_contract.query(
            "packages", {"limit": 16},
        ))["items"]
    finally:
        if restarted is not None:
            await restarted.close()
        await controller.close()


@pytest.mark.postgres
async def test_deployment_configuration_apply_publishes_frozen_generation(
    deployment_catalog, tmp_path
):
    repository = deployment_catalog
    await repository.initialize()
    lease = await repository.claim_supervisor("host/config-web", "incarnation/config-web")
    changes = DeploymentGraphChangeAuthority(repository.store)
    runtime = DeploymentRuntimeScope(
        repository.store, SimpleNamespace(authority_lease=lambda **_values: lease),
        graph_change_authority=changes,
    )
    package_catalog = PackageCatalog(tmp_path / "config-packages")
    controller = DeploymentPackageController(
        runtime, package_catalog,
        official_sources=(Path(__file__).parents[1] / "src/forest/builtin/deployment_audit",),
    )
    await controller.start()
    contributions = controller.registry._descriptor_fact(
        next(descriptor for descriptor in controller.registry.descriptors
             if descriptor.component_id == "deployment.audit")
    )["management_contributions"]
    assert any(item["contribution_id"] == "configuration" and
               item["scope_kinds"] == ["deployment"] for item in contributions)
    identity = DeploymentWebIdentityAuthority(
        repository.store,
        active_key_id="key/config-web",
        key_resolver=lambda _key_id: bytes(range(32)),
    )
    now = datetime.now(timezone.utc)
    await identity.initialize_setup(SimpleNamespace(
        setup_id="setup/config-web-00001", pairing_code="c" * 48,
        issued_at=now, expires_at=now + timedelta(hours=1),
    ))
    await identity.complete_setup(
        pairing_code="c" * 48, username="admin",
        display_name="Administrator", password="deployment-web-password",
    )
    app = create_deployment_management_app(
        deployment_id=repository.store.deployment_id,
        deployment_registry_provider=lambda: controller.registry,
        deployment_graph_changes=changes,
        deployment_change_previewer=controller.preview_change,
        identity_authority=identity,
        deployment_web_receipts=DeploymentWebAuthorityRepository(repository.store),
        deployment_audit=DeploymentAuditRepository(repository.store),
        allowed_origin="http://forest.test",
    )
    worker = None
    try:
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://forest.test"
        ) as client:
            login = await client.post(
                "/management/v1/auth/login",
                headers={"origin": "http://forest.test"},
                json={"username": "admin", "password": "deployment-web-password"},
            )
            assert login.status_code == 200, login.text
            headers = {
                "origin": "http://forest.test",
                "x-forest-csrf": client.cookies["forest_management_csrf"],
            }
            base = (
                "/management/v1/scopes/deployment/deployment/modules/"
                "deployment.audit/configuration"
            )
            before = (await client.get(base, headers=headers)).json()
            assert before["desired"]["revision"] == before["effective"]["revision"] == 1
            invalid = await client.post(
                base + "/plans",
                headers={**headers, "idempotency-key": "config-invalid"},
                json={"expected_desired_revision": 1, "value": {"protocol": "invalid"}},
            )
            assert invalid.status_code == 422, invalid.text
            source_graph = controller.registry.graph_identity
            plan = await client.post(
                base + "/plans",
                headers={**headers, "idempotency-key": "config-plan"},
                json={
                    "expected_desired_revision": 1,
                    "value": before["desired"]["value"],
                },
            )
            assert plan.status_code == 200, plan.text
            applied = await client.post(
                base + "/apply",
                headers={**headers, "idempotency-key": "config-apply"},
                json={"plan_id": plan.json()["plan_id"], "expected_desired_revision": 1},
            )
            assert applied.status_code == 200, applied.text
            result = applied.json()
            assert result["configuration"]["revision"] == 2
            assert result["rebuild"]["state"] == "QUEUED"
            request_id = result["rebuild"]["request_id"]
            second_plan = await client.post(
                base + "/plans",
                headers={**headers, "idempotency-key": "config-plan-next"},
                json={
                    "expected_desired_revision": 2,
                    "value": before["desired"]["value"],
                },
            )
            assert second_plan.status_code == 200, second_plan.text
            pending = await client.post(
                base + "/apply",
                headers={**headers, "idempotency-key": "config-apply-next"},
                json={
                    "plan_id": second_plan.json()["plan_id"],
                    "expected_desired_revision": 2,
                },
            )
            assert pending.status_code == 409, pending.text
            assert pending.json()["code"] == "DEPLOYMENT_CONFIGURATION_CHANGE_PENDING"
            assert controller.registry.graph_identity == source_graph
            assert (await controller.registry.get("deployment.audit").module_configuration.desired()).revision == 2
            worker = asyncio.create_task(controller.serve_graph_changes(changes))
            status_path = (
                "/management/v1/scopes/deployment/deployment/modules/"
                "$runtime/queries/module.change.status"
            )
            async with asyncio.timeout(15):
                while True:
                    status = await client.post(
                        status_path, headers=headers, json={"operation_id": request_id},
                    )
                    assert status.status_code == 200, status.text
                    if status.json()["state"] in {"SUCCEEDED", "FAILED"}:
                        break
                    await asyncio.sleep(0.05)
            assert status.json()["state"] == "SUCCEEDED", status.text
            assert status.json()["operation_kind"] == "CONFIGURE"
            assert controller.registry.graph_identity.graph_revision > source_graph.graph_revision
            after = (await client.get(base, headers=headers)).json()
            assert after["desired"]["revision"] == after["effective"]["revision"] == 2
            assert controller.registry.get("deployment.audit")._prepared_binding.config_revision == 2
            stale = await client.post(
                base + "/apply",
                headers={**headers, "idempotency-key": "config-apply-stale-plan"},
                json={
                    "plan_id": second_plan.json()["plan_id"],
                    "expected_desired_revision": 2,
                },
            )
            assert stale.status_code == 409, stale.text
            assert stale.json()["code"] == "MODULE_CONFIGURATION_CONFLICT"
            assert (await controller.registry.get("deployment.audit").module_configuration.desired()).revision == 2
            replay = await client.post(
                base + "/apply",
                headers={**headers, "idempotency-key": "config-apply"},
                json={"plan_id": plan.json()["plan_id"], "expected_desired_revision": 1},
            )
            assert replay.status_code == 200, replay.text
            assert replay.json()["rebuild"]["request_id"] == request_id
    finally:
        if worker is not None:
            worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)
        await controller.close()


@pytest.mark.postgres
async def test_deployment_web_queues_and_reconciles_package_replacement(
    deployment_catalog, tmp_path
):
    repository = deployment_catalog
    await repository.initialize()
    lease = await repository.claim_supervisor("host/web-replace", "incarnation/web-replace")
    runtime = DeploymentRuntimeScope(
        repository.store, SimpleNamespace(authority_lease=lambda **_values: lease)
    )
    package_catalog = PackageCatalog(tmp_path / "replacement-packages")
    controller = DeploymentPackageController(
        runtime,
        package_catalog,
        official_sources=(Path(__file__).parents[1] / "src/forest/builtin/deployment_audit",),
    )
    await controller.start()
    graph_changes = DeploymentGraphChangeAuthority(repository.store)
    identity = DeploymentWebIdentityAuthority(
        repository.store,
        active_key_id="key/replacement",
        key_resolver=lambda _key_id: bytes(range(32)),
    )
    now = datetime.now(timezone.utc)
    await identity.initialize_setup(
        SimpleNamespace(
            setup_id="setup/webreplacement0001",
            pairing_code="q" * 48,
            issued_at=now,
            expires_at=now + timedelta(hours=1),
        )
    )
    await identity.complete_setup(
        pairing_code="q" * 48,
        username="admin",
        display_name="Administrator",
        password="deployment-web-password",
    )
    app = create_deployment_management_app(
        deployment_id=repository.store.deployment_id,
                deployment_registry_provider=lambda: controller.registry,
        deployment_graph_changes=graph_changes,
        deployment_change_previewer=controller.preview_change,
        identity_authority=identity,
        deployment_web_receipts=DeploymentWebAuthorityRepository(repository.store),
        deployment_audit=DeploymentAuditRepository(repository.store),
        allowed_origin="http://forest.test",
    )
    worker = asyncio.create_task(controller.serve_graph_changes(graph_changes))
    restarted = None
    try:
        async with AsyncClient(
            transport=ASGITransport(app=app), base_url="http://forest.test"
        ) as client:
            login = await client.post(
                "/management/v1/auth/login",
                headers={"origin": "http://forest.test"},
                json={"username": "admin", "password": "deployment-web-password"},
            )
            assert login.status_code == 200
            headers = {
                "origin": "http://forest.test",
                "x-forest-csrf": client.cookies["forest_management_csrf"],
            }
            old = controller.registry.graph_identity
            body = {
                "component_id": "deployment.audit",
                "package_digest": controller.registry.get("deployment.audit").snapshot.digest,
                "expected_graph_revision": old.graph_revision,
            }
            action_path = (
                "/management/v1/scopes/deployment/deployment/modules/"
                "$runtime/actions/module.replace"
            )
            preview_path = action_path.replace(
                "/actions/module.replace", "/queries/module.change.preview"
            )
            async def preview_for(kind, candidate):
                response = await client.post(
                    preview_path, headers=headers,
                    json={"operation_kind": kind, **candidate},
                )
                assert response.status_code == 200, response.text
                return response.json()

            initial_preview = await preview_for("REPLACE", body)
            assert initial_preview["impact"]["replace"] == ["deployment.audit"]
            body["plan_digest"] = initial_preview["plan_digest"]
            wrong_plan = await client.post(
                action_path,
                headers={**headers, "idempotency-key": "replace-audit-wrong-plan"},
                json={**body, "plan_digest": "sha256:" + "0" * 64},
            )
            assert wrong_plan.status_code == 409
            assert wrong_plan.json()["code"] == "DEPLOYMENT_GRAPH_CHANGE_PLAN_CONFLICT"
            saved = await client.post(
                action_path,
                headers={**headers, "idempotency-key": "replace-audit-1"},
                json=body,
            )
            assert saved.status_code == 200, saved.text
            request = saved.json()
            assert request["state"] == "QUEUED"
            assert request["receipt_ticket_id"]
            async with repository.store.pool.connection() as conn:
                audit = await (
                    await conn.execute(
                        "SELECT operation_id,receipt_ticket_id FROM "
                        "forest_deployment.audit_records WHERE deployment_id=%s "
                        "AND operation_id=%s",
                        (repository.store.deployment_id, request["operation_id"]),
                    )
                ).fetchall()
            assert len(audit) == 1
            assert audit[0]["receipt_ticket_id"] == request["receipt_ticket_id"]
            status_path = (
                "/management/v1/scopes/deployment/deployment/modules/"
                "$runtime/queries/module.change.status"
            )
            async with asyncio.timeout(15):
                while True:
                    status = await client.post(
                        status_path,
                        headers=headers,
                        json={"operation_id": request["operation_id"]},
                    )
                    assert status.status_code == 200, status.text
                    if status.json()["state"] in {"SUCCEEDED", "FAILED"}:
                        break
                    await asyncio.sleep(0.05)
            assert status.json()["state"] == "SUCCEEDED", status.text
            assert controller.registry.graph_identity.graph_revision > old.graph_revision
            replay = await client.post(
                action_path,
                headers={**headers, "idempotency-key": "replace-audit-1"},
                json=body,
            )
            assert replay.status_code == 200, replay.text
            assert replay.json()["operation_id"] == request["operation_id"]
            assert replay.json()["state"] == "SUCCEEDED"
            conflict = await client.post(
                action_path, headers={**headers, "idempotency-key": "replace-audit-1"},
                json={**body, "package_digest": "sha256:" + "1" * 64},
            )
            assert conflict.status_code == 409
            assert conflict.json()["code"] == "DEPLOYMENT_GRAPH_CHANGE_CONFLICT"
            async with repository.store.pool.connection() as conn:
                count = await (
                    await conn.execute(
                        "SELECT count(*) AS value FROM forest_deployment.audit_records "
                        "WHERE deployment_id=%s AND operation_id=%s",
                        (repository.store.deployment_id, request["operation_id"]),
                    )
                ).fetchone()
            assert count["value"] == 1
            assert (
                controller.registry.graph_identity.graph_revision
                == status.json()["result_graph_revision"]
            )
            probe_source = Path(__file__).parents[1] / "examples/deployment_probe"
            staged_catalog = PackageCatalog(tmp_path / "staged-deployment-probe")
            staged = staged_catalog.build(probe_source)
            probe = package_catalog.import_archive(staged_catalog.export_archive(staged.digest))
            add_body = {
                "component_id": probe.component["id"],
                "package_digest": probe.digest,
                "expected_graph_revision": controller.registry.graph_identity.graph_revision,
            }
            probe_preview = await preview_for("ADD", add_body)
            assert probe_preview["impact"]["add"] == [probe.component["id"]]
            assert probe_preview["impact"]["retain"] == ["deployment.audit"]
            assert (await preview_for("ADD", add_body))["plan_digest"] == probe_preview["plan_digest"]
            async with repository.store.pool.connection() as conn:
                schema = await (
                    await conn.execute(
                        "SELECT 1 FROM forest_deployment.schema_bundles "
                        "WHERE deployment_id=%s AND component_id=%s",
                        (repository.store.deployment_id, probe.component["id"]),
                    )
                ).fetchone()
                queued = await (
                    await conn.execute(
                        "SELECT count(*) AS value FROM "
                        "forest_deployment.runtime_graph_change_requests "
                        "WHERE deployment_id=%s",
                        (repository.store.deployment_id,),
                    )
                ).fetchone()
            assert schema is None
            assert queued["value"] == 1
            add_body["plan_digest"] = probe_preview["plan_digest"]
            added = await client.post(
                action_path.replace("module.replace", "module.add"),
                headers={**headers, "idempotency-key": "add-probe-1"},
                json=add_body,
            )
            assert added.status_code == 200, added.text
            assert added.json()["operation_kind"] == "ADD"
            async with asyncio.timeout(15):
                while True:
                    add_status = await client.post(
                        status_path, headers=headers,
                        json={"operation_id": added.json()["operation_id"]},
                    )
                    assert add_status.status_code == 200, add_status.text
                    if add_status.json()["state"] in {"SUCCEEDED", "FAILED"}:
                        break
                    await asyncio.sleep(0.05)
            assert add_status.json()["state"] == "SUCCEEDED", add_status.text
            result = await client.post(
                "/management/v1/scopes/deployment/deployment/modules/"
                "example.deployment-probe/queries/probe.echo",
                headers=headers, json={"value": "live"},
            )
            assert result.status_code == 200, result.text
            assert result.json() == {"echo": "live"}
            ui_metadata, _ui_content = probe.management_ui()
            ui_response = await client.get(
                "/management/v1/scopes/deployment/deployment/modules/"
                f"example.deployment-probe/management-ui/{ui_metadata['sha256']}"
            )
            assert ui_response.status_code == 200, ui_response.text
            assert "查询插件" in ui_response.text
            next_source = tmp_path / "next-deployment-probe"
            shutil.copytree(probe_source, next_source)
            manifest = next_source / "module.toml"
            manifest.write_text(
                manifest.read_text(encoding="utf-8").replace(
                    'version = "0.1.0"', 'version = "0.1.1"', 1
                ), encoding="utf-8",
            )
            worker_source = next_source / "python/deployment_probe.py"
            worker_source.write_text(
                worker_source.read_text(encoding="utf-8").replace(
                    'payload["value"]', '"next:" + payload["value"]'
                ), encoding="utf-8",
            )
            next_staged = staged_catalog.build(next_source)
            next_probe = package_catalog.import_archive(
                staged_catalog.export_archive(next_staged.digest)
            )
            retained_audit = controller.registry.graph_identity.generations["deployment.audit"]
            upgrade_body = {
                "component_id": probe.component["id"],
                "package_digest": next_probe.digest,
                "expected_graph_revision": controller.registry.graph_identity.graph_revision,
            }
            upgrade_preview = await preview_for("REPLACE", upgrade_body)
            assert upgrade_preview["impact"]["replace"] == [probe.component["id"]]
            upgraded = await client.post(
                action_path,
                headers={**headers, "idempotency-key": "replace-probe-1"},
                json={**upgrade_body, "plan_digest": upgrade_preview["plan_digest"]},
            )
            assert upgraded.status_code == 200, upgraded.text
            async with asyncio.timeout(15):
                while True:
                    upgrade_status = await client.post(
                        status_path, headers=headers,
                        json={"operation_id": upgraded.json()["operation_id"]},
                    )
                    assert upgrade_status.status_code == 200, upgrade_status.text
                    if upgrade_status.json()["state"] in {"SUCCEEDED", "FAILED"}:
                        break
                    await asyncio.sleep(0.05)
            assert upgrade_status.json()["state"] == "SUCCEEDED", upgrade_status.text
            assert controller.registry.graph_identity.generations["deployment.audit"] == retained_audit
            result = await client.post(
                "/management/v1/scopes/deployment/deployment/modules/"
                "example.deployment-probe/queries/probe.echo",
                headers=headers, json={"value": "live"},
            )
            assert result.status_code == 200, result.text
            assert result.json() == {"echo": "next:live"}
            restarted = DeploymentPackageController(
                runtime, package_catalog,
                official_sources=(Path(__file__).parents[1] / "src/forest/builtin/deployment_audit",),
            )
            recovered = await restarted.start()
            assert recovered.get(probe.component["id"]).snapshot.digest == next_probe.digest
            assert await recovered.get(probe.component["id"]).management_contract.query(
                "probe.echo", {"value": "recovered"}
            ) == {"echo": "next:recovered"}
    finally:
        worker.cancel()
        await asyncio.gather(worker, return_exceptions=True)
        if restarted is not None:
            await restarted.close()
        await controller.close()


@pytest.mark.postgres
async def test_deployment_package_controller_restores_selected_artifact_after_restart(
    deployment_catalog, tmp_path
):
    repository = deployment_catalog
    await repository.initialize()
    lease = await repository.claim_supervisor("host/packages", "incarnation/packages")
    runtime = DeploymentRuntimeScope(
        repository.store, SimpleNamespace(authority_lease=lambda **_values: lease)
    )
    package_catalog = PackageCatalog(tmp_path / "catalog")
    source = Path(__file__).parents[1] / "src/forest/builtin/deployment_audit"
    controller = DeploymentPackageController(runtime, package_catalog, official_sources=(source,))
    restarted = None
    try:
        original = await controller.start()
        original_digest = original.get("deployment.audit").snapshot.digest
        next_source = tmp_path / "next-audit"
        shutil.copytree(source, next_source)
        manifest = next_source / "module.toml"
        manifest.write_text(
            manifest.read_text(encoding="utf-8").replace(
                'version = "1.0.0"', 'version = "1.0.1"', 1
            ),
            encoding="utf-8",
        )
        replacement = package_catalog.build(next_source)
        assert replacement.digest != original_digest
        old_revision = original.graph_identity.graph_revision
        with pytest.raises(ForestError) as conflict:
            await controller.replace(
                "deployment.audit",
                replacement.digest,
                expected_graph_revision=old_revision + 1,
            )
        assert conflict.value.code == "RUNTIME_GRAPH_REVISION_CONFLICT"
        assert controller.registry is original

        with pytest.raises(ForestError) as untrusted:
            await controller.replace(
                "deployment.audit", replacement.digest,
                expected_graph_revision=old_revision,
            )
        assert untrusted.value.code == "OFFICIAL_PACKAGE_NOT_REGISTERED"
        assert controller.registry is original

        assert package_catalog.build_official(next_source).digest == replacement.digest

        active = await controller.replace(
            "deployment.audit",
            replacement.digest,
            expected_graph_revision=old_revision,
        )
        assert active.get("deployment.audit").snapshot.digest == replacement.digest
        assert active.graph_identity.graph_revision > old_revision
        _graph, selection = await runtime.generation_authority.active_package_selection()
        assert selection["deployment.audit"]["package_digest"] == replacement.digest
        assert selection["deployment.audit"]["configuration"]["revision"] == 1

        receipts = DeploymentWebAuthorityRepository(repository.store)
        now = datetime.now(timezone.utc)

        def ticket(component_id: str, generation_id: str, revision: int, operation: str):
            return WebAuthorityTicket(
                "forest.web-authority-ticket/1",
                f"ticket/{operation}",
                "issuer/packages",
                "key/packages",
                "subject/packages",
                "ADMIN",
                "session/packages",
                1,
                "deployment",
                "deployment",
                component_id,
                generation_id,
                revision,
                f"operation/{operation}",
                "module.replace",
                digest_json({"operation": operation}),
                None,
                now,
                now + timedelta(seconds=20),
                "hmac-sha256:" + "0" * 64,
            )

        with pytest.raises(ForestError) as stale:
            await receipts.accept(
                ticket=ticket("$runtime", original.graph_identity.graph_id, old_revision, "stale")
            )
        assert stale.value.code == "WEB_AUTHORITY_TICKET_TARGET_MISMATCH"
        accepted = await receipts.accept(
            ticket=ticket(
                "$runtime",
                active.graph_identity.graph_id,
                active.graph_identity.graph_revision,
                "current",
            )
        )
        assert accepted["target_graph_revision"] == active.graph_identity.graph_revision
        generation = active.graph_identity.generations["deployment.audit"]
        assert (
            await receipts.accept(
                ticket=ticket(
                    "deployment.audit",
                    generation,
                    active.graph_identity.graph_revision,
                    "member",
                )
            )
        )["target_generation_id"] == generation

        configuration = active.get("deployment.audit").module_configuration
        frozen = await configuration.effective(include_value=True)
        plan = await configuration.plan(
            operation_id="pending-deployment-config-plan",
            expected_desired_revision=frozen.revision,
            value=frozen.value,
            validation_digest=frozen.validation_digest,
        )
        pending = await configuration.apply(
            operation_id="pending-deployment-config-apply",
            plan_id=plan["plan_id"], expected_desired_revision=frozen.revision,
        )
        assert pending.revision == frozen.revision + 1
        assert (await configuration.effective()).revision == frozen.revision

        restarted = DeploymentPackageController(
            runtime, package_catalog, official_sources=(source,)
        )
        recovered = await restarted.start()
        assert recovered.get("deployment.audit").snapshot.digest == replacement.digest
        assert recovered.get("deployment.audit")._prepared_binding.config_revision == frozen.revision
        assert (
            await recovered.get("deployment.audit").management_contract.query(
                "audit.records.query", {"limit": 1}
            )
        ) == {"records": [], "next_before": None}
        manifest.write_text(
            manifest.read_text(encoding="utf-8").replace(
                'version = "1.0.1"', 'version = "1.0.2"', 1
            ),
            encoding="utf-8",
        )
        newer = package_catalog.build_official(next_source)
        upgraded = await restarted.replace(
            "deployment.audit", newer.digest,
            expected_graph_revision=recovered.graph_identity.graph_revision,
        )
        assert upgraded.get("deployment.audit")._prepared_binding.config_revision == frozen.revision
        assert (await configuration.desired()).revision == pending.revision
        assert (await configuration.effective()).revision == frozen.revision
    finally:
        if restarted is not None:
            await restarted.close()
        await controller.close()


@pytest.mark.postgres
async def test_deployment_owned_work_operation_ledger_fences_replay_and_unknown(
    deployment_catalog, tmp_path
):
    repository = deployment_catalog
    await repository.initialize()
    lease = await repository.claim_supervisor("host/owned-ledger", "incarnation/owned-ledger")
    runtime = DeploymentRuntimeScope(
        repository.store, SimpleNamespace(authority_lease=lambda **_values: lease)
    )
    source = Path(__file__).parents[1] / "src/forest/builtin/deployment_audit"
    snapshot = PackageCatalog(tmp_path / "packages").build(source)
    descriptor = await bootstrap_builtin_deployment_package(snapshot, runtime)
    registry = RuntimeModuleRegistry(runtime, (descriptor,))
    try:
        await registry.start()
        graph = registry.graph_identity
        generation = graph.generations["deployment.audit"]
        route = PackageServiceRoute(
            service_id="owned-work.deployment.audit.recovery",
            graph_id=graph.graph_id, graph_revision=graph.graph_revision,
            admission_lane="DATA", owned_work=True, parent_call_id=None,
            timeout_seconds=10,
        )
        scope = PackageCallScope(
            bot_id=None, scope_kind="deployment", scope_id=repository.store.deployment_id,
            component_id="deployment.audit", generation_id=generation,
            instance_id="instance/ledger", worker_incarnation="worker/ledger",
            call_id="call:ledger-a", call_operation_id="owned:ledger-success",
            causal_root_kind="package.owned-work", causal_root_id="owned:ledger-success",
        )
        ledger = runtime.service_operation_ledger
        assert (await ledger.begin(scope, route, {"value": 1})).execute is True
        competing = replace(scope, call_id="call:ledger-b")
        in_progress = await ledger.begin(competing, route, {"value": 1})
        assert in_progress.error.code == "SERVICE_OPERATION_IN_PROGRESS"
        await ledger.succeed(scope, route, {"value": "done"})
        assert (await ledger.begin(competing, route, {"value": 1})).result == {
            "value": "done",
        }
        parallel = replace(
            scope, call_id="call:parallel-a", call_operation_id="owned:ledger-parallel",
            causal_root_id="owned:ledger-parallel",
        )
        parallel_other = replace(parallel, call_id="call:parallel-b")
        admissions = await asyncio.gather(
            ledger.begin(parallel, route, {"value": "parallel"}),
            ledger.begin(parallel_other, route, {"value": "parallel"}),
        )
        assert sum(item.execute for item in admissions) == 1
        assert sorted(
            item.error.code for item in admissions if item.error is not None
        ) == ["SERVICE_OPERATION_IN_PROGRESS"]
        await ledger.succeed(
            parallel if admissions[0].execute else parallel_other,
            route, {"value": "parallel"},
        )
        with pytest.raises(ForestError) as conflict:
            await ledger.begin(competing, route, {"value": 2})
        assert conflict.value.code == "SERVICE_OPERATION_CONFLICT"

        failure = replace(
            scope, call_id="call:ledger-fail", call_operation_id="owned:ledger-fail",
            causal_root_id="owned:ledger-fail",
        )
        assert (await ledger.begin(failure, route, {})).execute is True
        await ledger.fail(
            failure, route, PackageServiceError("RECOVERY_PROBE_REJECTED", "Rejected", 422),
        )
        replay_failure = await ledger.begin(
            replace(failure, call_id="call:ledger-fail-replay"), route, {},
        )
        assert replay_failure.error.code == "RECOVERY_PROBE_REJECTED"

        unknown = replace(
            scope, call_id="call:ledger-unknown", call_operation_id="owned:ledger-unknown",
            causal_root_id="owned:ledger-unknown",
        )
        assert (await ledger.begin(unknown, route, {})).execute is True
        async with repository.store.pool.connection() as conn:
            await conn.execute(
                "UPDATE forest_deployment.package_service_operations "
                "SET deadline_at=clock_timestamp()-interval '1 second' "
                "WHERE deployment_id=%s AND component_id=%s AND operation_id=%s",
                (
                    repository.store.deployment_id, unknown.component_id,
                    unknown.call_operation_id,
                ),
            )
        replay_unknown = await ledger.begin(
            replace(unknown, call_id="call:ledger-unknown-replay"), route, {},
        )
        assert replay_unknown.error.code == "SERVICE_OPERATION_UNKNOWN"
        with pytest.raises(ForestError) as stale:
            await ledger.succeed(unknown, route, {"value": "late"})
        assert stale.value.code == "SERVICE_OPERATION_STALE_ATTEMPT"
    finally:
        await registry.close()


@pytest.mark.postgres
async def test_deployment_audit_builtin_runs_in_real_package_generation(
    deployment_catalog, tmp_path
):
    catalog = deployment_catalog
    await catalog.initialize()
    lease = await catalog.claim_supervisor("host/audit", "incarnation/audit")
    runtime = DeploymentRuntimeScope(
        catalog.store, SimpleNamespace(authority_lease=lambda **_values: lease)
    )
    snapshot = PackageCatalog(tmp_path / "deployment-packages").build(
        Path(__file__).parents[1] / "src/forest/builtin/deployment_audit"
    )
    descriptor = await bootstrap_builtin_deployment_package(snapshot, runtime)
    registry = RuntimeModuleRegistry(runtime, (descriptor,))
    try:
        await registry.start()
        assert await runtime.generation_authority.current_graph() == registry.graph_identity
        module = registry.get("deployment.audit")
        assert (
            module.generation_identity.generation_id
            == (registry.graph_identity.generations["deployment.audit"])
        )
        query = module.management_contract.query
        assert await query("audit.records.query", {"limit": 10}) == {
            "records": [],
            "next_before": None,
        }

        async with catalog.store.pool.connection() as conn, conn.transaction():
            await conn.execute(
                "INSERT INTO forest_deployment.web_authority_receipts("
                "deployment_id,ticket_id,ticket_digest,issuer_id,key_id,web_subject_ref,"
                "web_role,session_ref,session_revision,target_scope_kind,target_scope_id,"
                "target_component_id,target_generation_id,target_graph_revision,"
                "target_operation_ref,action_key,input_digest,issued_at,expires_at) "
                "VALUES (%s,'ticket/audit','sha256:' || repeat('1',64),'issuer/audit',"
                "'key/audit','subject/audit','ADMIN','session/audit',1,'deployment',"
                "'deployment','deployment.audit',%s,1,'operation/audit','audit.query',"
                "'sha256:' || repeat('2',64),clock_timestamp(),"
                "clock_timestamp() + interval '20 seconds')",
                (catalog.store.deployment_id, module.generation_identity.generation_id),
            )
            await conn.execute(
                "INSERT INTO forest_deployment.audit_records("
                "deployment_id,audit_id,action,severity,producer_component_id,"
                "producer_generation_id,origin_kind,actor_kind,actor_id,resource_kind,"
                "resource_id,operation_kind,operation_id,receipt_ticket_id) "
                "VALUES (%s,'audit:one','audit.query','SECURITY','deployment.audit',"
                "%s,'web.management','web.subject','subject/audit',"
                "'deployment.component','deployment.audit','web.action',"
                "'operation/audit','ticket/audit')",
                (catalog.store.deployment_id, module.generation_identity.generation_id),
            )
        page = await query("audit.records.query", {"limit": 10, "action": "audit.query"})
        assert len(page["records"]) == 1
        assert page["records"][0]["audit_id"] == "audit:one"
        assert page["records"][0]["actor_id"] == "subject/audit"
        assert page["records"][0]["details_payload_id"] is None
        assert (await query("audit.records.query", {"action": "missing"}))["records"] == []

        previous = registry
        candidate = RuntimeModuleRegistry(runtime, (descriptor,))
        assert candidate.plan_graph_transition(
            previous, target_component_id="deployment.audit"
        ) == ("deployment.audit",)
        await candidate.prewarm()
        await previous.drain_owned_work_for_replacement(candidate.activation_id)
        await candidate.start()
        assert candidate.graph_identity.graph_revision == 2
        assert (
            candidate.graph_identity.generations["deployment.audit"]
            != (previous.graph_identity.generations["deployment.audit"])
        )
        assert (
            await candidate.get("deployment.audit").management_contract.query(
                "audit.records.query", {"limit": 10}
            )
        )["records"][0]["audit_id"] == "audit:one"
        assert await previous.close() == []
        registry = candidate
    finally:
        await registry.close()


@pytest.mark.postgres
async def test_deployment_generations_publish_under_the_real_supervisor_lease(deployment_catalog):
    repository = deployment_catalog
    await repository.initialize()
    lease = await repository.claim_supervisor("host/one", "incarnation/one")
    authority = DeploymentGenerationAuthority(repository.store)
    runtime_scope = DeploymentRuntimeScope(
        repository.store, SimpleNamespace(authority_lease=lambda **_kwargs: lease)
    )
    assert runtime_scope.scope_kind == "deployment"
    assert runtime_scope.scope_id == repository.store.deployment_id
    assert runtime_scope.authority_lease() == lease
    digest = "sha256:" + "1" * 64

    def binding(component_id: str, *, revision: int = 1) -> PreparedGenerationBinding:
        return PreparedGenerationBinding(
            component_id=component_id,
            requires=(),
            package_id="deployment.test",
            package_version="1.0.0",
            package_digest=digest,
            driver="python-worker",
            trust_level="installed",
            descriptor_digest=digest,
            route_contract_digest=digest,
            management_contract_digest=digest,
            management_ui_digest=digest,
            config_revision=revision,
            config_payload_id="config/" + component_id,
            config_payload_digest=digest,
            instance_id="instance/" + component_id,
            worker_incarnation="worker/" + component_id,
            state_schema=1,
            state_contract_digest=digest,
            capability_grants_digest=digest,
            data_admission=False,
            control_admission=True,
        )

    class Configuration:
        async def activate_in(
            self,
            conn,
            *,
            operation_id,
            revision,
            generation_id,
            expected_payload_id,
            expected_payload_digest,
        ):
            assert operation_id.startswith("runtime-publish:")
            assert generation_id.startswith("generation:")
            assert revision > 0 and expected_payload_id.startswith("config/")
            assert expected_payload_digest == digest
            await conn.execute("SELECT 1")

    with pytest.raises(ForestError) as caught:
        await authority.prepare_graph(
            lease,
            [
                replace(
                    binding("deployment.unregistered"),
                    storage=RuntimeStorageBinding(
                        schema_id="deployment.unregistered.state",
                        schema_version="1.0.0",
                        namespace=module_schema_namespace("deployment.unregistered.state"),
                        baseline_sha256="1" * 64,
                        package_digest=digest,
                        repository_contract="deployment.unregistered/1",
                        execution_profile="declarative",
                    ),
                )
            ],
            str(uuid4()),
        )
    assert caught.value.code == "RUNTIME_STORAGE_BINDING_MISMATCH"

    first = await authority.prepare_graph(
        lease, [binding("deployment.alpha"), binding("deployment.beta")], str(uuid4())
    )
    assert first.graph_revision == 1
    assert (
        await authority.prepare_graph(
            lease, [binding("deployment.alpha"), binding("deployment.beta")], first.activation_id
        )
        == first
    )
    assert await authority.current_graph() is None
    assert (
        await authority.publish_graph(
            lease,
            first,
            configuration_activators={item: Configuration() for item in first.generations},
        )
        == 1
    )
    assert await authority.current_graph() == first

    replacement = await authority.prepare_graph(
        lease,
        [binding("deployment.beta", revision=2)],
        str(uuid4()),
        retained_generations={"deployment.alpha": first.generations["deployment.alpha"]},
        source_graph=first,
    )
    stale = await authority.prepare_graph(
        lease, [binding("deployment.beta", revision=3)], str(uuid4())
    )
    assert replacement.graph_revision == 2
    assert replacement.generations["deployment.alpha"] == first.generations["deployment.alpha"]
    async with repository.store.pool.connection() as conn:
        epochs = await (
            await conn.execute(
                "SELECT component_id,route_epoch FROM forest_deployment.runtime_graph_members "
                "WHERE deployment_id=%s AND graph_id=%s",
                (repository.store.deployment_id, replacement.graph_id),
            )
        ).fetchall()
    assert {row["component_id"]: row["route_epoch"] for row in epochs} == {
        "deployment.alpha": 1,
        "deployment.beta": 2,
    }
    assert (
        await authority.publish_graph(
            lease,
            replacement,
            configuration_activators={"deployment.beta": Configuration()},
        )
        == 2
    )
    assert await authority.current_graph() == replacement
    async with repository.store.pool.connection() as conn, conn.transaction():
        await conn.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        await authority.verify_generation_member_snapshot_in(
            conn,
            component_id="deployment.alpha",
            generation_id=replacement.generations["deployment.alpha"],
        )
    with pytest.raises(ForestError) as caught:
        await authority.publish_graph(
            lease,
            stale,
            configuration_activators={"deployment.beta": Configuration()},
        )
    assert caught.value.code == "RUNTIME_GRAPH_STALE"
    async with repository.store.pool.connection() as conn:
        await authority.verify_active_generation_in(
            conn,
            component_id="deployment.alpha",
            generation_id=first.generations["deployment.alpha"],
            admission_lane="CONTROL",
        )
        await authority.verify_active_generation_in(
            conn,
            component_id="deployment.beta",
            generation_id=replacement.generations["deployment.beta"],
            admission_lane="CONTROL",
        )
        with pytest.raises(ForestError) as caught:
            await authority.verify_active_generation_in(
                conn,
                component_id="deployment.beta",
                generation_id=first.generations["deployment.beta"],
                admission_lane="CONTROL",
            )
        assert caught.value.code == "RUNTIME_GENERATION_NOT_ACTIVE"
    with pytest.raises(ForestError) as caught:
        await authority.record_states(lease, first, {"deployment.alpha": ("STOPPED", None)})
    assert caught.value.code == "RUNTIME_GENERATION_STILL_ACTIVE"
    await authority.record_states(lease, first, {"deployment.beta": ("STOPPED", None)})
    async with repository.store.pool.connection() as conn, conn.transaction():
        await authority.verify_active_generation_in(
            conn,
            component_id="deployment.beta",
            generation_id=replacement.generations["deployment.beta"],
            admission_lane="CONTROL",
        )
        freezing = asyncio.create_task(
            authority.freeze_graph_admission(lease, replacement, components=("deployment.beta",))
        )
        done, pending = await asyncio.wait({freezing}, timeout=0.1)
        assert not done and pending == {freezing}
    assert await freezing == 1
    async with repository.store.pool.connection() as conn:
        with pytest.raises(ForestError) as caught:
            await authority.verify_active_generation_in(
                conn,
                component_id="deployment.beta",
                generation_id=replacement.generations["deployment.beta"],
                admission_lane="CONTROL",
            )
        assert caught.value.code == "RUNTIME_GENERATION_NOT_ACTIVE"
        await authority.verify_active_generation_in(
            conn,
            component_id="deployment.alpha",
            generation_id=replacement.generations["deployment.alpha"],
            admission_lane="CONTROL",
        )
    assert (
        await authority.resume_graph_admission(lease, replacement, components=("deployment.beta",))
        == 1
    )
    async with repository.store.pool.connection() as conn:
        epoch = await (
            await conn.execute(
                "SELECT route_epoch FROM forest_deployment.runtime_graph_members "
                "WHERE deployment_id=%s AND graph_id=%s AND component_id=%s",
                (repository.store.deployment_id, replacement.graph_id, "deployment.beta"),
            )
        ).fetchone()
    assert epoch["route_epoch"] == 4
    assert await authority.retire_graph(lease, replacement) == replacement.graph_revision
    assert await authority.current_graph() is None
    await authority.record_states(
        lease,
        replacement,
        {"deployment.alpha": ("STOPPED", None), "deployment.beta": ("STOPPED", None)},
    )
    async with repository.store.pool.connection() as conn:
        await authority.verify_generation_cleanup_in(
            conn,
            component_id="deployment.beta",
            generation_id=replacement.generations["deployment.beta"],
        )
    await repository.release_supervisor(lease)
    async with repository.store.pool.connection() as conn:
        with pytest.raises(ForestError) as caught:
            await authority.verify_generation_cleanup_in(
                conn,
                component_id="deployment.beta",
                generation_id=replacement.generations["deployment.beta"],
            )
        assert caught.value.code == "RUNTIME_GENERATION_CLEANUP_DENIED"


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
        expected_intent_revision=alpha["intent_revision"],
    )
    online = await repository.observe(
        lease,
        bot_id="bot/alpha",
        observed_state="ONLINE",
        expected_intent_revision=alpha["intent_revision"],
        graph_id="graph/alpha/1",
    )
    assert online["revision"] == starting["revision"] + 1
    assert (await repository.inspect("bot/alpha"))["graph_id"] == "graph/alpha/1"
    assert (await repository.inspect("bot/beta"))["observed_state"] is None

    offline = await repository.set_desired(
        operation_id="runtime/alpha/offline-after-start",
        bot_id="bot/alpha", desired_state="OFFLINE",
        expected_revision=alpha["intent_revision"],
    )
    with pytest.raises(ForestError) as changed:
        await repository.observe(
            lease, bot_id="bot/alpha", observed_state="ONLINE",
            expected_intent_revision=alpha["intent_revision"],
            graph_id="graph/alpha/stale",
        )
    assert changed.value.code == "BOT_INTENT_CHANGED"
    assert (await repository.inspect("bot/alpha"))["graph_id"] == "graph/alpha/1"
    await repository.set_desired(
        operation_id="runtime/alpha/online-again", bot_id="bot/alpha",
        desired_state="ONLINE", expected_revision=offline["intent_revision"],
    )
    with pytest.raises(ForestError) as changed_again:
        await repository.observe(
            lease, bot_id="bot/alpha", observed_state="ONLINE",
            expected_intent_revision=alpha["intent_revision"],
            graph_id="graph/alpha/old-run",
        )
    assert changed_again.value.code == "BOT_INTENT_CHANGED"

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
    assert await secrets.resolve_text_for_purpose(
        first.secret_ref, "provider.chat-completions.credential"
    ) == "model-secret-value"
    metadata_only = DeploymentSecretAuthority(
        repository.store,
        active_key_id="master/test/1",
        key_resolver=lambda _key_id: (_ for _ in ()).throw(
            AssertionError("Secret verification must not request a decryption key")
        ),
    )
    await metadata_only.verify_reference_for_purpose(
        first.secret_ref, "provider.chat-completions.credential"
    )
    with pytest.raises(ForestError) as wrong_reference_purpose:
        await metadata_only.verify_reference_for_purpose(
            first.secret_ref, "embedding.multimodal.credential"
        )
    assert wrong_reference_purpose.value.code == "SECRET_PURPOSE_CONFLICT"
    with pytest.raises(ForestError) as wrong_purpose:
        await secrets.resolve_text_for_purpose(
            first.secret_ref, "embedding.multimodal.credential"
        )
    assert wrong_purpose.value.code == "SECRET_PURPOSE_CONFLICT"

    second = await secrets.put(
        operation_id="secret/model/2",
        secret_ref=first.secret_ref,
        purpose=first.purpose,
        plaintext=b"rotated-model-secret",
        expected_revision=first.revision,
    )
    assert second.version == second.revision == 2
    assert await secrets.resolve(second.secret_ref) == b"rotated-model-secret"
    assert await secrets.resolve_text_for_purpose(
        second.secret_ref, "provider.chat-completions.credential"
    ) == "rotated-model-secret"
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

    racing = await asyncio.gather(
        secrets.put(
            operation_id="secret/race/shared-operation",
            secret_ref="secret://race/alpha",
            purpose="test.credential",
            plaintext=b"alpha",
            expected_revision=0,
        ),
        secrets.put(
            operation_id="secret/race/shared-operation",
            secret_ref="secret://race/beta",
            purpose="test.credential",
            plaintext=b"beta",
            expected_revision=0,
        ),
        return_exceptions=True,
    )
    assert sum(isinstance(item, SecretMetadata) for item in racing) == 1
    assert [item.code for item in racing if isinstance(item, ForestError)] == [
        "SECRET_OPERATION_CONFLICT"
    ]


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
    schemas = {
        component_id: "sha256:" + str(index) * 64
        for index, component_id in enumerate(components, 1)
    }
    packages = {component_id: "sha256:" + "c" * 64 for component_id in components}
    created = await profiles.create(
        operation_id="bootstrap-profile/profile/1",
        profile_ref="bootstrap-profile/profile/1",
        bot_id="bot/profile",
        storage_profile_ref=storage.profile_ref,
        storage_generation=1,
        components=components,
        schema_digests=schemas,
        package_digests=packages,
    )
    replay = await profiles.create(
        operation_id="bootstrap-profile/profile/1",
        profile_ref="bootstrap-profile/profile/1",
        bot_id="bot/profile",
        storage_profile_ref=storage.profile_ref,
        storage_generation=1,
        components=components,
        schema_digests=schemas,
        package_digests=packages,
    )
    assert replay == created
    assert [item.component_id for item in created.components] == sorted(components)
    assert all(item.config_digest.startswith("sha256:") for item in created.components)
    assert {item.component_id: item.package_digest for item in created.components} == packages

    with pytest.raises(ForestError) as caught:
        await profiles.create(
            operation_id="bootstrap-profile/plaintext/1",
            profile_ref="bootstrap-profile/plaintext/1",
            bot_id="bot/profile",
            storage_profile_ref=storage.profile_ref,
            storage_generation=1,
            components={"provider.chat-completions": {"model_api_key": "forbidden"}},
            schema_digests={"provider.chat-completions": "sha256:" + "f" * 64},
            package_digests={"provider.chat-completions": "sha256:" + "c" * 64},
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
        package_digests={"conversation.trigger-policy": "sha256:" + "c" * 64},
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
    with pytest.raises(ForestError) as stale_publication:
        await jobs.complete_ready(supervisor, claim, catalog=repository)
    assert stale_publication.value.code == "STALE_PROVISIONING_CLAIM"
    with pytest.raises(ForestError) as missing_bot:
        await repository.inspect("bot/jobs")
    assert missing_bot.value.code == "BOT_NOT_FOUND"
    succeeded = await jobs.complete_ready(supervisor, second, catalog=repository)
    assert succeeded["state"] == "SUCCEEDED"
    with pytest.raises(ForestError) as caught:
        await jobs.fail(supervisor, claim, error_code="STALE_TEST")
    assert caught.value.code == "STALE_PROVISIONING_CLAIM"


@pytest.mark.postgres
async def test_expired_provisioning_claim_blocks_unknown_result(deployment_catalog):
    repository = deployment_catalog
    await repository.initialize()
    storage = await _storage_profile(repository, "bot/expired-job", "expired-job")
    profile = await _bootstrap_profile(repository, storage, "expired-job")
    jobs = DeploymentProvisioningJobAuthority(repository.store)
    await jobs.enqueue(
        operation_id="provisioning/expired-job/enqueue",
        job_id="provisioning/expired-job/1",
        bot_id="bot/expired-job",
        display_name="Expired Job",
        storage_profile_ref=storage.profile_ref,
        storage_generation=storage.storage_generation,
        bootstrap_profile_ref=profile.profile_ref,
    )
    supervisor = await repository.claim_supervisor("host/expired-job", "incarnation/expired-job")
    claim = await jobs.claim(supervisor, worker_id="worker/expired-job")
    assert claim is not None
    async with repository.store.transaction() as conn:
        await conn.execute(
            "UPDATE forest_deployment.provisioning_jobs "
            "SET lease_expires_at=clock_timestamp()-interval '1 second' "
            "WHERE job_id=%s", (claim.job_id,),
        )
    assert await jobs.claim(supervisor, worker_id="worker/retry") is None
    blocked = await jobs.inspect(claim.job_id)
    assert blocked["state"] == "BLOCKED_UNKNOWN"
    assert blocked["attempt_no"] == 1
    assert blocked["last_error_code"] == "PROVISIONING_CLAIM_EXPIRED"
    assert await jobs.claim(supervisor, worker_id="worker/retry") is None
    with pytest.raises(ForestError) as mismatch:
        await jobs.reconcile_ready(
            operation_id="provisioning/expired-job/reconcile",
            job_id=claim.job_id,
            expected_revision=blocked["revision"],
            proof={
                "deployment_id": repository.store.deployment_id,
                "job_id": claim.job_id,
                "bot_id": claim.bot_id,
                "storage_profile_ref": claim.storage_profile_ref,
                "storage_generation": claim.storage_generation,
                "bootstrap_profile_ref": claim.bootstrap_profile_ref,
                "bootstrap_manifest_digest": "sha256:" + "0" * 64,
            },
            catalog=repository,
        )
    assert mismatch.value.code == "PROVISIONING_PROOF_MISMATCH"
    assert (await jobs.inspect(claim.job_id))["state"] == "BLOCKED_UNKNOWN"
    with pytest.raises(ForestError) as absent:
        await repository.inspect(claim.bot_id)
    assert absent.value.code == "BOT_NOT_FOUND"


@pytest.mark.postgres
async def test_deterministic_provisioning_failure_does_not_retry(deployment_catalog):
    repository = deployment_catalog
    await repository.initialize()
    storage = await _storage_profile(repository, "bot/nonretryable", "nonretryable")
    profile = await _bootstrap_profile(repository, storage, "nonretryable")
    jobs = DeploymentProvisioningJobAuthority(repository.store)
    await jobs.enqueue(
        operation_id="provisioning/nonretryable/enqueue",
        job_id="provisioning/nonretryable/1",
        bot_id=storage.bot_id,
        display_name="Nonretryable",
        storage_profile_ref=storage.profile_ref,
        storage_generation=storage.storage_generation,
        bootstrap_profile_ref=profile.profile_ref,
    )
    supervisor = await repository.claim_supervisor("host/nonretryable", "incarnation/nonretryable")
    claim = await jobs.claim(supervisor, worker_id="worker/nonretryable")
    assert claim is not None
    failed = await jobs.fail(
        supervisor, claim, error_code="PROVISIONING_INPUT_TAMPERED", retryable=False,
    )
    assert failed["state"] == "FAILED"
    assert failed["attempt_no"] == 1
    assert await jobs.claim(supervisor, worker_id="worker/again") is None


@pytest.mark.postgres
async def test_provisioning_kernel_command_notifies_only_after_commit(deployment_catalog):
    repository = deployment_catalog
    await repository.initialize()
    storage = await _storage_profile(repository, "bot/kernel-job", "kernel-job")
    profile = await _bootstrap_profile(repository, storage, "kernel-job")
    jobs = DeploymentProvisioningJobAuthority(repository.store)
    observed = []

    def notify():
        with psycopg.connect(repository.store.pool.conninfo, autocommit=True) as conn:
            row = conn.execute(
                "SELECT state FROM forest_deployment.provisioning_jobs WHERE job_id=%s",
                ("provisioning/kernel-job/1",),
            ).fetchone()
            observed.append(None if row is None else row[0])

    async def verify_generation(_conn, **_values):
        return None

    async def before_write(_conn):
        return None

    declaration = {
        "name": "deployment.provisioning.enqueue",
        "kind": "deployment.provisioning-enqueue",
    }
    authority = DeploymentKernelManagementAuthority(
        repository.store,
        SimpleNamespace(verify_active_generation_in=verify_generation),
        component_id="deployment.provisioning", ports=(declaration,),
        handlers={
            ("deployment.provisioning", "deployment.provisioning-enqueue"):
                ProvisioningJobKernelCommand(
                    jobs, "deployment.provisioning-enqueue", notify=notify,
                ),
        },
    )
    scope = SimpleNamespace(
        scope_kind="deployment", scope_id=repository.store.deployment_id,
        component_id="deployment.provisioning", bot_id=None,
        call_operation_id="provisioning/kernel-job/enqueue",
        generation_id="generation/kernel-job/1",
    )
    payload = {
        "job_id": "provisioning/kernel-job/1", "bot_id": storage.bot_id,
        "display_name": "Kernel Job", "storage_profile_ref": storage.profile_ref,
        "storage_generation": storage.storage_generation,
        "bootstrap_profile_ref": profile.profile_ref,
    }
    arguments = {
        "parent_call_id": "call/kernel-job/1", "timeout_seconds": 10,
        "before_write": before_write, "management_authorization": object(),
    }
    with pytest.raises(ForestError) as rejected:
        await authority.invoke_management_action(
            scope, declaration, payload,
            result_validator=lambda _value: False, **arguments,
        )
    assert rejected.value.code == "MODULE_MANAGEMENT_OUTPUT_INVALID"
    assert observed == []
    with pytest.raises(ForestError) as absent:
        await jobs.inspect(payload["job_id"])
    assert absent.value.code == "PROVISIONING_JOB_NOT_FOUND"
    result = await authority.invoke_management_action(
        scope, declaration, payload,
        result_validator=lambda value: value["state"] == "QUEUED", **arguments,
    )
    assert result["job_id"] == payload["job_id"]
    assert observed == ["QUEUED"]


@pytest.mark.postgres
async def test_provisioning_ready_publication_and_job_settlement_are_atomic(
    deployment_catalog, monkeypatch,
):
    repository = deployment_catalog
    await repository.initialize()
    storage = await _storage_profile(repository, "bot/atomic", "atomic")
    profile = await _bootstrap_profile(repository, storage, "atomic")
    jobs = DeploymentProvisioningJobAuthority(repository.store)
    await jobs.enqueue(
        operation_id="provisioning/atomic/enqueue",
        job_id="provisioning/atomic/1",
        bot_id="bot/atomic",
        display_name="Atomic",
        storage_profile_ref=storage.profile_ref,
        storage_generation=storage.storage_generation,
        bootstrap_profile_ref=profile.profile_ref,
    )
    supervisor = await repository.claim_supervisor("host/atomic", "incarnation/atomic")
    claim = await jobs.claim(supervisor, worker_id="worker/atomic")
    assert claim is not None

    original_finish = jobs._finish_in

    async def reject_settlement(*_args, **_kwargs):
        raise ForestError("SETTLEMENT_REJECTED", "Settlement failed", 409)

    monkeypatch.setattr(jobs, "_finish_in", reject_settlement)
    with pytest.raises(ForestError) as rejected:
        await jobs.complete_ready(supervisor, claim, catalog=repository)
    assert rejected.value.code == "SETTLEMENT_REJECTED"
    with pytest.raises(ForestError) as missing:
        await repository.inspect(claim.bot_id)
    assert missing.value.code == "BOT_NOT_FOUND"
    assert (await jobs.inspect(claim.job_id))["state"] == "RUNNING"
    async with repository.store.pool.connection() as conn:
        operation = await (
            await conn.execute(
                "SELECT 1 FROM forest_deployment.operations WHERE operation_id=%s",
                (f"provision-ready/{claim.job_id}",),
            )
        ).fetchone()
    assert operation is None

    monkeypatch.setattr(jobs, "_finish_in", original_finish)
    settled = await jobs.complete_ready(supervisor, claim, catalog=repository)
    assert settled["state"] == "SUCCEEDED"
    assert (await repository.inspect(claim.bot_id))["supply_state"] == "READY"


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
                **official_builtin_configuration(
                    "vector_index_milvus",
                    overrides={
                        "space_id": EmbeddingProfile(
                            deployment_id="embedding.multimodal/test",
                            model_revision="test/1",
                            model="Qwen/Qwen3-VL-Embedding-8B",
                            dimensions=4096,
                        ).space_id,
                        "dimensions": 4096,
                    },
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
                "protocol": "forest.shell.command.bootstrap/1",
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
        bootstrap_profiles = DeploymentBootstrapProfileAuthority(repository.store, secrets=secrets)
        bootstrap = await bootstrap_profiles.create(
            operation_id="bootstrap-profile/official/create",
            profile_ref="bootstrap-profile/official/1",
            bot_id="bot/official",
            storage_profile_ref=storage.profile_ref,
            storage_generation=storage.storage_generation,
            components=components,
            schema_digests=official_bootstrap_schema_digests(),
            package_digests={
                item.component_id: item.package_digest
                for item in official_bootstrap_contributions()
            },
        )
        jobs = DeploymentProvisioningJobAuthority(repository.store)
        await jobs.enqueue(
            operation_id="provisioning/official/enqueue",
            job_id="provisioning/official/1", bot_id="bot/official",
            display_name="Official", storage_profile_ref=storage.profile_ref,
            storage_generation=storage.storage_generation,
            bootstrap_profile_ref=bootstrap.profile_ref,
        )
        resolver = OfficialBotSettingsResolver(secrets=secrets)
        resolved_storage = await storage_profiles.resolve(
            profile_ref=storage.profile_ref,
            expected_bot_id=storage.bot_id,
            expected_storage_generation=storage.storage_generation,
        )
        resolved_profile = await bootstrap_profiles.load(bootstrap.profile_ref)
        resolved_settings = await resolver.resolve(
            profile=resolved_profile, storage=resolved_storage
        )
        assert set(dict(resolved_settings.components)) == set(components)
        assert not hasattr(resolved_settings, "model_api_key")
        provisioner = OfficialBotProvisioner(
            settings_resolver=resolver,
            database_allocator=allocator,
        )
        await provisioner(
            claim=SimpleNamespace(
                bot_id="bot/official", deployment_id=repository.store.deployment_id,
                job_id="provisioning/official/1",
                storage_profile_ref=storage.profile_ref,
                storage_generation=storage.storage_generation,
                bootstrap_profile_ref=bootstrap.profile_ref,
                bootstrap_manifest_digest=bootstrap.manifest_digest,
            ),
            storage=resolved_storage,
            profile=resolved_profile,
        )
        proof = await provisioner.read_materialization_receipt(storage=resolved_storage)
        assert proof["job_id"] == "provisioning/official/1"
        assert proof["bootstrap_manifest_digest"] == bootstrap.manifest_digest
        supervisor = await repository.claim_supervisor("host/official", "incarnation/official")
        claim = await jobs.claim(supervisor, worker_id="worker/official")
        assert claim is not None
        blocked = await jobs.block_unknown(
            supervisor, claim, error_code="PROVISIONING_RESULT_UNKNOWN"
        )
        command = ProvisioningReconcileKernelCommand(
            jobs, repository, storage_profiles, bootstrap_profiles, provisioner,
        )
        prepared = await command.prepare(
            {"job_id": claim.job_id, "expected_revision": blocked["revision"]},
            operation_id="provisioning/official/reconcile",
        )
        async with repository.store.transaction() as conn:
            settled = await command.execute_in(
                conn, prepared, operation_id="provisioning/official/reconcile"
            )
        assert settled["state"] == "SUCCEEDED"
        async with repository.store.transaction() as conn:
            replay = await command.execute_in(
                conn, prepared, operation_id="provisioning/official/reconcile"
            )
        assert replay == settled
        bot = await repository.inspect("bot/official")
        assert bot["supply_state"] == "READY"
        assert bot["desired_state"] == "OFFLINE"

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
                provider_namespace = module_schema_namespace("forest.provider.chat-completions")
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
        await admin.execute(
            sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(target_name))
        )
        await admin.execute(sql.SQL("DROP ROLE IF EXISTS {}").format(sql.Identifier(target_role)))
        await admin.close()


@pytest.mark.postgres
async def test_deployment_webui_discovers_components_and_keeps_secrets_write_only(
    deployment_catalog,
    tmp_path,
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
    bot_profile = await _storage_profile(repository, "bot/webui", "webui")
    bot_bootstrap = await _bootstrap_profile(repository, bot_profile, "webui")
    await repository.register_ready(
        operation_id="provision/webui",
        bot_id="bot/webui",
        display_name="WebUI Bot",
        storage_profile_ref=bot_profile.profile_ref,
        bootstrap_profile_ref=bot_bootstrap.profile_ref,
        bootstrap_manifest_digest=bot_bootstrap.manifest_digest,
    )
    supervisor = SimpleNamespace(notify=lambda: None)
    allocated_dsn = "postgresql://forest:hidden-password@postgres/forest_bot_web"
    allocator = SimpleNamespace(
        new_target_dsn=lambda _bot_id: allocated_dsn,
        validate_target=lambda _bot_id, _target_dsn: None,
    )
    bootstrap_registry = BootstrapConfigurationRegistry(official_bootstrap_contributions())
    deployment_audit = DeploymentAuditRepository(repository.store)
    lease = await repository.claim_supervisor("host/webui", "incarnation/webui")
    deployment_runtime = DeploymentRuntimeScope(
        repository.store, SimpleNamespace(authority_lease=lambda **_values: lease),
        kernel_query_handlers={
            ("deployment.provisioning", "deployment.bootstrap-catalog"):
                BootstrapCatalogKernelQuery(bootstrap_registry),
        },
        kernel_management_handlers={
            ("deployment.secrets", "deployment.secret-put"):
                SecretPutKernelCommand(secrets),
            **{
                ("deployment.accounts", kind): WebAccountKernelCommand(web_identity, kind)
                for kind in WebAccountKernelCommand._METHODS
            },
            ("deployment.provisioning", "deployment.storage-create-local"):
                LocalStorageProfileKernelCommand(secrets, storage_profiles, allocator),
            ("deployment.provisioning", "deployment.storage-create"):
                StorageProfileKernelCommand(storage_profiles),
            ("deployment.provisioning", "deployment.bootstrap-create"):
                BootstrapProfileKernelCommand(bootstrap_profiles, bootstrap_registry),
            **{
                ("deployment.provisioning", kind): ProvisioningJobKernelCommand(
                    jobs, kind, notify=supervisor.notify,
                ) for kind in ProvisioningJobKernelCommand._METHODS
            },
            ("deployment.provisioning", "deployment.provisioning-reconcile"):
                ProvisioningReconcileKernelCommand(
                    jobs, repository, storage_profiles, bootstrap_profiles,
                    SimpleNamespace(read_materialization_receipt=None),
                ),
        },
    )
    deployment_runtime.kernel_owned_work_authorities["deployment.provisioning"] = (
        DeploymentProvisioningOwnedWorkAuthority(deployment_runtime, jobs, repository)
    )
    materialized = []
    drain_materialize_started = asyncio.Event()
    release_drain_materialize = asyncio.Event()

    async def materialize(*, claim, storage, profile):
        materialized.append((claim.job_id, storage.profile_ref, profile.profile_ref))
        if claim.job_id == "provisioning/package-drain/1":
            drain_materialize_started.set()
            await release_drain_materialize.wait()

    deployment_runtime.effect_driver_factories[
        "deployment.provisioning-materializer"
    ] = lambda configuration, revision, identity, declaration: (
        DeploymentProvisioningEffectDriver(
            configuration, revision, identity, declaration,
            runtime=deployment_runtime, jobs=jobs,
            storage_profiles=storage_profiles,
            bootstrap_profiles=bootstrap_profiles,
            provisioner=materialize,
        )
    )
    package_catalog = PackageCatalog(tmp_path / "webui-deployment-packages")
    agent_setup_snapshot = package_catalog.build(
        Path(__file__).parents[1] / "src/forest/builtin/agent_runtime"
    )
    audit_snapshot = package_catalog.build(
        Path(__file__).parents[1] / "src/forest/builtin/deployment_audit"
    )
    bot_snapshot = package_catalog.build(
        Path(__file__).parents[1] / "src/forest/builtin/deployment_bot_catalog"
    )
    secret_snapshot = package_catalog.build(
        Path(__file__).parents[1] / "src/forest/builtin/deployment_secrets"
    )
    account_snapshot = package_catalog.build(
        Path(__file__).parents[1] / "src/forest/builtin/deployment_accounts"
    )
    provisioning_snapshot = package_catalog.build(
        Path(__file__).parents[1] / "src/forest/builtin/deployment_provisioning"
    )
    wrong_binding = DeploymentRuntimeScope(
        repository.store, SimpleNamespace(authority_lease=lambda **_values: lease),
        kernel_management_handlers={
            ("deployment.other", "deployment.secret-put"):
                SecretPutKernelCommand(secrets),
        },
    )
    with pytest.raises(ForestError) as denied:
        await bootstrap_builtin_deployment_package(
            secret_snapshot, wrong_binding, plan_only=True,
        )
    assert denied.value.code == "DEPLOYMENT_PACKAGE_PORT_UNAVAILABLE"
    audit_descriptor = await bootstrap_builtin_deployment_package(
        audit_snapshot, deployment_runtime
    )
    bot_descriptor = await bootstrap_builtin_deployment_package(
        bot_snapshot, deployment_runtime
    )
    secret_descriptor = await bootstrap_builtin_deployment_package(
        secret_snapshot, deployment_runtime
    )
    account_descriptor = await bootstrap_builtin_deployment_package(
        account_snapshot, deployment_runtime
    )
    provisioning_descriptor = await bootstrap_builtin_deployment_package(
        provisioning_snapshot, deployment_runtime
    )
    deployment_registry = RuntimeModuleRegistry(
        deployment_runtime, (audit_descriptor, bot_descriptor, secret_descriptor,
                             account_descriptor, provisioning_descriptor)
    )
    await deployment_registry.start()
    app = create_deployment_management_app(
        deployment_id="deployment/test",
                deployment_registry_provider=lambda: deployment_registry,
        identity_authority=web_identity,
        deployment_web_receipts=DeploymentWebAuthorityRepository(repository.store),
        deployment_audit=deployment_audit,
        allowed_origin="http://forest.test",
        bootstrap_registry=bootstrap_registry,
        bootstrap_package_catalog=package_catalog,
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
        assert body["runtime"]["graph_state"] == "ACTIVE"
        assert body["runtime"]["graph_id"] == deployment_registry.graph_identity.graph_id
        assert {item["component_id"] for item in body["modules"]} == {
            "deployment.bot-catalog",
            "deployment.secrets",
            "deployment.provisioning",
            "deployment.accounts",
            "deployment.audit",
        }
        secret_module = next(
            item for item in body["modules"]
            if item["component_id"] == "deployment.secrets"
        )
        assert secret_module["management_contract"]["actions"][0]["secret_field"] == "plaintext"
        account_module = next(
            item for item in body["modules"] if item["component_id"] == "deployment.accounts"
        )
        assert account_module["generation_id"] == (
            deployment_registry.graph_identity.generations["deployment.accounts"]
        )
        assert {
            item["action_id"]: item.get("secret_field")
            for item in account_module["management_contract"]["actions"]
        } == {
            "account.create": "password", "account.update": None,
            "account.password": "password", "account.sessions.revoke": None,
        }
        audit_module = next(
            item for item in body["modules"] if item["component_id"] == "deployment.audit"
        )
        assert (
            audit_module["generation_id"]
            == (deployment_registry.graph_identity.generations["deployment.audit"])
        )
        audit_ui = await client.get(
            "/management/v1/scopes/deployment/deployment/modules/"
            f"deployment.audit/management-ui/{audit_snapshot.management_ui()[0]['sha256']}",
            headers=headers,
        )
        assert audit_ui.status_code == 200
        assert "forest.management-ui-channel/1" in audit_ui.text
        bot_ui = await client.get(
            "/management/v1/scopes/deployment/deployment/modules/"
            f"deployment.bot-catalog/management-ui/{bot_snapshot.management_ui()[0]['sha256']}",
            headers=headers,
        )
        assert bot_ui.status_code == 200
        secret_ui = await client.get(
            "/management/v1/scopes/deployment/deployment/modules/"
            f"deployment.secrets/management-ui/{secret_snapshot.management_ui()[0]['sha256']}",
            headers=headers,
        )
        assert secret_ui.status_code == 200
        assert 'name="plaintext"' not in secret_ui.text
        account_ui = await client.get(
            "/management/v1/scopes/deployment/deployment/modules/"
            f"deployment.accounts/management-ui/{account_snapshot.management_ui()[0]['sha256']}",
            headers=headers,
        )
        assert account_ui.status_code == 200
        assert 'name="password"' not in account_ui.text
        account_page = await client.post(
            "/management/v1/scopes/deployment/deployment/modules/"
            "deployment.accounts/queries/accounts.list",
            headers=headers, json={"limit": 1},
        )
        assert account_page.status_code == 200, account_page.text
        assert [item["username"] for item in account_page.json()["items"]] == ["admin"]
        created_account = await client.post(
            "/management/v1/scopes/deployment/deployment/modules/"
            "deployment.accounts/actions/account.create",
            headers={**headers, "idempotency-key": "webui-account-create"},
            json={"username": "reader", "display_name": "Reader",
                  "password": "reader-password-123", "role": "AUDITOR"},
        )
        assert created_account.status_code == 200, created_account.text
        assert created_account.json()["username"] == "reader"
        assert "reader-password-123" not in created_account.text
        assert (await client.post(
            "/management/v1/scopes/deployment/deployment/modules/"
            "deployment.accounts/queries/accounts.list",
            headers=headers, json={"limit": 1, "after": "admin"},
        )).json()["items"][0]["username"] == "reader"
        page_path = (
            "/management/v1/scopes/deployment/deployment/modules/"
            "deployment.bot-catalog/queries/bots.list"
        )
        page = await client.post(page_path, headers=headers, json={"limit": 20})
        assert page.status_code == 200, page.text
        assert [item["bot_id"] for item in page.json()["items"]] == ["bot/webui"]
        action_path = (
            "/management/v1/scopes/deployment/deployment/modules/"
            "deployment.bot-catalog/actions/bot.set-desired"
        )
        action_headers = {**headers, "idempotency-key": "webui-bot-online"}
        action = await client.post(action_path, headers=action_headers, json={
            "bot_id": "bot/webui", "desired_state": "ONLINE", "expected_revision": 1,
        })
        assert action.status_code == 200, action.text
        assert action.json() == {"bot_id": "bot/webui", "intent_revision": 2}
        assert (await repository.inspect("bot/webui"))["desired_state"] == "ONLINE"
        replay = await client.post(action_path, headers=action_headers, json={
            "bot_id": "bot/webui", "desired_state": "ONLINE", "expected_revision": 1,
        })
        assert replay.json() == action.json()
        stale = await client.post(
            action_path,
            headers={**headers, "idempotency-key": "webui-bot-stale"},
            json={
                "bot_id": "bot/webui", "desired_state": "OFFLINE",
                "expected_revision": 1,
            },
        )
        assert stale.status_code == 409, stale.text
        assert stale.json()["code"] == "BOT_INTENT_REVISION_CONFLICT"
        assert (await repository.inspect("bot/webui"))["desired_state"] == "ONLINE"
        filtered = await client.post(
            page_path, headers=headers, json={"limit": 1, "search": "WebUI"}
        )
        assert filtered.status_code == 200, filtered.text
        assert [item["bot_id"] for item in filtered.json()["items"]] == ["bot/webui"]
        provisioning_ui = await client.get(
            "/management/v1/scopes/deployment/deployment/modules/"
            f"deployment.provisioning/management-ui/{provisioning_snapshot.management_ui()[0]['sha256']}",
            headers=headers,
        )
        assert provisioning_ui.status_code == 200
        assert "bootstrap-editors-open" in provisioning_ui.text
        assert "sandbox allow-scripts allow-forms" in (
            provisioning_ui.headers["content-security-policy"]
        )
        assert "form-action 'none'" in provisioning_ui.headers["content-security-policy"]
        provisioning_module = next(
            item for item in body["modules"]
            if item["component_id"] == "deployment.provisioning"
        )
        assert provisioning_module["generation_id"] == (
            deployment_registry.graph_identity.generations["deployment.provisioning"]
        )
        assert provisioning_module["management_contract"]["bootstrap_catalog_query"] == (
            "provisioning.bootstrap-catalog"
        )
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
        assert any(
            item["secret_ref"] == "secret://web/test" for item in listed.json()["items"]
        )
        assert "never-return-this" not in listed.text
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
        rejected_storage = await client.post(
            "/management/v1/scopes/deployment/deployment/modules/"
            "deployment.provisioning/actions/storage.create-local",
            headers={**headers, "idempotency-key": "storage-web-conflict"},
            json={
                "profile_ref": "storage-profile/web/1",
                "bot_id": "bot/web",
                "storage_generation": 1,
                "dsn_secret_ref": "secret://web/orphan",
            },
        )
        assert rejected_storage.status_code == 409, rejected_storage.text
        assert rejected_storage.json()["code"] == "STORAGE_PROFILE_EXISTS"
        with pytest.raises(ForestError) as absent_secret:
            await secrets.metadata("secret://web/orphan")
        assert absent_secret.value.code == "SECRET_NOT_FOUND"
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
        assert sum(item["surface"]["kind"] == "trusted-module" for item in catalog_body["items"]) == 6
        agent_setup = next(
            item for item in catalog_body["items"] if item["component_id"] == "agent.runtime"
        )
        assert agent_setup["package_digest"] == agent_setup_snapshot.digest
        assert agent_setup["surface"]["kind"] == "sandboxed-bootstrap"
        asset = await client.get(agent_setup["surface"]["artifact"])
        assert asset.status_code == 200
        assert "sandbox allow-scripts allow-forms" in asset.headers["content-security-policy"]
        assert "Agent 核心提示词" in asset.text
        artifact_path = agent_setup["surface"]["artifact"]
        missing_asset = await client.get(
            artifact_path[:-1] + ("0" if artifact_path[-1] != "0" else "1")
        )
        assert missing_asset.status_code == 404
        assert all(item["schema_digest"].startswith("sha256:") for item in catalog_body["items"])
        assert (await client.get("/static/bootstrap-component.js")).status_code == 200
        assert 'textarea name="components"' not in provisioning_ui.text

        components = {item["component_id"]: item["initial_value"] for item in catalog_body["items"]}
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
                "deployment.accounts",
                deployment_registry.graph_identity.generations["deployment.accounts"],
                "account.create",
            ),
            (
                "deployment.provisioning",
                deployment_registry.graph_identity.generations["deployment.provisioning"],
                "bootstrap.create",
            ),
            (
                "deployment.bot-catalog",
                deployment_registry.graph_identity.generations["deployment.bot-catalog"],
                "bot.set-desired",
            ),
            (
                "deployment.secrets",
                deployment_registry.graph_identity.generations["deployment.secrets"],
                "secret.put",
            ),
            (
                "deployment.provisioning",
                deployment_registry.graph_identity.generations["deployment.provisioning"],
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
            "account.create",
            "bot.set-desired",
            "secret.put",
            "storage.create-local",
            "bootstrap.create",
        }
        assert {item["producer_component_id"] for item in audit_records} == {
            "deployment.accounts",
            "deployment.bot-catalog",
            "deployment.secrets",
            "deployment.provisioning",
        }
        assert {item["actor_id"] for item in audit_records} == {
            "web-subject/8c6976e5b5410415bde908bd4dee15df"
        }
        assert all(item["origin_kind"] == "web.management" for item in audit_records)
        provisioning_base = (
            "/management/v1/scopes/deployment/deployment/modules/deployment.provisioning/"
        )
        queued = await client.post(
            provisioning_base + "actions/provisioning.enqueue",
            headers={**headers, "idempotency-key": "provisioning-web-enqueue"},
            json={
                "job_id": "provisioning/web/1", "bot_id": "bot/web",
                "display_name": "Web Bot", "storage_profile_ref": "storage-profile/web/1",
                "storage_generation": 1,
                "bootstrap_profile_ref": "bootstrap-profile/web/1",
            },
        )
        assert queued.status_code == 200, queued.text
        assert queued.json()["state"] == "QUEUED"
        job_page = await client.post(
            provisioning_base + "queries/provisioning.jobs.list",
            headers=headers, json={"limit": 1, "bot_id": "bot/web", "state": "QUEUED"},
        )
        assert job_page.status_code == 200, job_page.text
        assert [item["job_id"] for item in job_page.json()["items"]] == [
            "provisioning/web/1"
        ]
        cancelled = await client.post(
            provisioning_base + "actions/provisioning.cancel",
            headers={**headers, "idempotency-key": "provisioning-web-cancel"},
            json={"job_id": "provisioning/web/1", "expected_revision": 1},
        )
        assert cancelled.status_code == 200, cancelled.text
        assert cancelled.json()["state"] == "CANCELLED"
        not_reconcilable = await client.post(
            provisioning_base + "actions/provisioning.reconcile",
            headers={**headers, "idempotency-key": "provisioning-web-reconcile-cancelled"},
            json={"job_id": "provisioning/web/1", "expected_revision": 2},
        )
        assert not_reconcilable.status_code == 409
        assert not_reconcilable.json()["code"] == "PROVISIONING_NOT_RECONCILABLE"
        package_storage = await _storage_profile(
            repository, "bot/package-worker", "package-worker"
        )
        package_bootstrap = await _bootstrap_profile(
            repository, package_storage, "package-worker"
        )
        await jobs.enqueue(
            operation_id="provisioning/package-worker/enqueue",
            job_id="provisioning/package-worker/1",
            bot_id="bot/package-worker",
            display_name="Package Worker",
            storage_profile_ref=package_storage.profile_ref,
            storage_generation=package_storage.storage_generation,
            bootstrap_profile_ref=package_bootstrap.profile_ref,
        )

        async def package_job_completed():
            state = (await jobs.inspect("provisioning/package-worker/1"))["state"]
            assert state not in {"FAILED", "BLOCKED_UNKNOWN"}
            return state == "SUCCEEDED"

        await _eventually(package_job_completed, timeout=20)
        assert materialized == [
            ("provisioning/package-worker/1", package_storage.profile_ref,
             package_bootstrap.profile_ref)
        ]
        package_bot = await repository.inspect("bot/package-worker")
        assert package_bot["supply_state"] == "READY"
        assert package_bot["desired_state"] == "OFFLINE"
        previous = deployment_registry
        candidate = RuntimeModuleRegistry(
            deployment_runtime, (audit_descriptor, bot_descriptor, secret_descriptor,
                                 account_descriptor, provisioning_descriptor)
        )
        drained = candidate.plan_graph_transition(
            previous, target_component_id="deployment.secrets"
        )
        assert drained == ("deployment.secrets",)
        await candidate.prewarm()
        await previous.drain_owned_work_for_replacement(
            candidate.activation_id, components=drained,
        )
        await candidate.start()
        candidate.accept_retained_ownership()
        assert (
            candidate.graph_identity.generations["deployment.secrets"]
            != previous.graph_identity.generations["deployment.secrets"]
        )
        await previous.close()
        deployment_registry = candidate
        rotated = await client.post(
            "/management/v1/scopes/deployment/deployment/modules/"
            "deployment.secrets/actions/secret.put",
            headers={**headers, "idempotency-key": "secret-web-after-reload"},
            json={
                "secret_ref": "secret://web/test",
                "purpose": "test.credential",
                "plaintext": "replacement-value",
                "expected_revision": 1,
            },
        )
        assert rotated.status_code == 200, rotated.text
        assert rotated.json()["revision"] == 2
        assert "replacement-value" not in rotated.text
        account_path = (
            "/management/v1/scopes/deployment/deployment/modules/"
            "deployment.accounts/actions/"
        )
        updated = await client.post(
            account_path + "account.update",
            headers={**headers, "idempotency-key": "webui-account-update"},
            json={"subject_id": created_account.json()["subject_id"],
                  "display_name": "Reader Two", "role": "AUDITOR",
                  "enabled": True, "expected_revision": 1},
        )
        assert updated.status_code == 200, updated.text
        assert updated.json()["revision"] == 2
        password = await client.post(
            account_path + "account.password",
            headers={**headers, "idempotency-key": "webui-account-password"},
            json={"subject_id": created_account.json()["subject_id"],
                  "password": "reader-new-password-123", "expected_revision": 2},
        )
        assert password.status_code == 200, password.text
        assert password.json()["revision"] == 3
        assert "reader-new-password-123" not in password.text
        revoked = await client.post(
            account_path + "account.sessions.revoke",
            headers={**headers, "idempotency-key": "webui-account-revoke"},
            json={"subject_id": created_account.json()["subject_id"],
                  "expected_revision": 3},
        )
        assert revoked.status_code == 200, revoked.text
        account_previous = deployment_registry
        account_candidate = RuntimeModuleRegistry(
            deployment_runtime, (audit_descriptor, bot_descriptor, secret_descriptor,
                                 account_descriptor, provisioning_descriptor)
        )
        account_drained = account_candidate.plan_graph_transition(
            account_previous, target_component_id="deployment.accounts"
        )
        assert account_drained == ("deployment.accounts",)
        await account_candidate.prewarm()
        await account_previous.drain_owned_work_for_replacement(
            account_candidate.activation_id, components=account_drained,
        )
        await account_candidate.start()
        account_candidate.accept_retained_ownership()
        assert account_candidate.graph_identity.generations["deployment.accounts"] != (
            account_previous.graph_identity.generations["deployment.accounts"]
        )
        await account_previous.close()
        deployment_registry = account_candidate
        updated_after_reload = await client.post(
            account_path + "account.update",
            headers={**headers, "idempotency-key": "webui-account-after-reload"},
            json={"subject_id": created_account.json()["subject_id"],
                  "display_name": "Reader Three", "role": "AUDITOR",
                  "enabled": True, "expected_revision": 3},
        )
        assert updated_after_reload.status_code == 200, updated_after_reload.text
        assert updated_after_reload.json()["revision"] == 4
        concurrent_storage_path = (
            "/management/v1/scopes/deployment/deployment/modules/"
            "deployment.provisioning/actions/storage.create-local"
        )
        concurrent_storage_payload = {
            "profile_ref": "storage-profile/web/concurrent",
            "bot_id": "bot/web", "storage_generation": 2,
            "dsn_secret_ref": "secret://web/concurrent",
        }
        concurrent_storage = await asyncio.gather(*(
            client.post(
                concurrent_storage_path,
                headers={**headers, "idempotency-key": "storage-web-concurrent"},
                json=concurrent_storage_payload,
            ) for _ in range(2)
        ))
        assert [response.status_code for response in concurrent_storage] == [200, 200]
        assert concurrent_storage[0].json() == concurrent_storage[1].json()
        async with repository.store.pool.connection() as conn:
            versions = await (
                await conn.execute(
                    "SELECT count(*) AS count FROM forest_deployment.secret_versions "
                    "WHERE secret_ref=%s", ("secret://web/concurrent",),
                )
            ).fetchone()
        assert versions["count"] == 1
        drain_storage = await _storage_profile(
            repository, "bot/package-drain", "package-drain"
        )
        drain_bootstrap = await _bootstrap_profile(
            repository, drain_storage, "package-drain"
        )
        await jobs.enqueue(
            operation_id="provisioning/package-drain/enqueue",
            job_id="provisioning/package-drain/1",
            bot_id="bot/package-drain",
            display_name="Package Drain",
            storage_profile_ref=drain_storage.profile_ref,
            storage_generation=drain_storage.storage_generation,
            bootstrap_profile_ref=drain_bootstrap.profile_ref,
        )
        await asyncio.wait_for(drain_materialize_started.wait(), timeout=10)
        provision_previous = deployment_registry
        provision_candidate = RuntimeModuleRegistry(
            deployment_runtime, (audit_descriptor, bot_descriptor, secret_descriptor,
                                 account_descriptor, provisioning_descriptor)
        )
        provision_drained = provision_candidate.plan_graph_transition(
            provision_previous, target_component_id="deployment.provisioning"
        )
        assert provision_drained == ("deployment.provisioning",)
        await provision_candidate.prewarm()
        draining = asyncio.create_task(
            provision_previous.drain_owned_work_for_replacement(
                provision_candidate.activation_id, components=provision_drained,
            )
        )
        await asyncio.sleep(0.05)
        assert not draining.done()
        release_drain_materialize.set()
        await asyncio.wait_for(draining, timeout=20)
        assert (await jobs.inspect("provisioning/package-drain/1"))["state"] == "SUCCEEDED"
        await provision_candidate.start()
        provision_candidate.accept_retained_ownership()
        assert provision_candidate.graph_identity.generations["deployment.provisioning"] != (
            provision_previous.graph_identity.generations["deployment.provisioning"]
        )
        await provision_previous.close()
        deployment_registry = provision_candidate
        after_reload = await client.post(
            provisioning_base + "queries/provisioning.jobs.list",
            headers=headers, json={"limit": 1, "state": "CANCELLED"},
        )
        assert after_reload.status_code == 200, after_reload.text
        assert after_reload.json()["items"][0]["job_id"] == "provisioning/web/1"
    await deployment_registry.close()


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
                rows = {item["bot_id"]: item for item in await supervisor.management_scopes()}
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
            async with supervisor.management_host(supervisor.management_scope_id("bot/alpha")):
                pass
        assert unavailable.value.code == "BOT_MANAGEMENT_UNAVAILABLE"
    finally:
        await supervisor.close()
    assert instances["bot/beta"].closed is True


@pytest.mark.postgres
async def test_multi_bot_supervisor_discards_start_when_intent_changes(deployment_catalog):
    repository = deployment_catalog
    await repository.initialize()
    profile = await _storage_profile(repository, "bot/retracted", "retracted")
    bootstrap = await _bootstrap_profile(repository, profile, "retracted")
    bot = await repository.register_ready(
        operation_id="provision/retracted", bot_id="bot/retracted",
        display_name="Retracted", storage_profile_ref=profile.profile_ref,
        bootstrap_profile_ref=bootstrap.profile_ref,
        bootstrap_manifest_digest=bootstrap.manifest_digest,
    )
    bot = await repository.set_desired(
        operation_id="runtime/retracted/online", bot_id=bot["bot_id"],
        desired_state="ONLINE", expected_revision=bot["intent_revision"],
    )
    secrets = DeploymentSecretAuthority(
        repository.store, active_key_id="master/test/1",
        key_resolver=lambda _key_id: bytes(range(32)),
    )
    storage_profiles = DeploymentStorageProfileAuthority(repository.store, secrets=secrets)
    starting = asyncio.Event()
    finish_start = asyncio.Event()
    closed = asyncio.Event()

    class FakeHost:
        def __init__(self, settings):
            self.registry = SimpleNamespace(
                graph_identity=SimpleNamespace(graph_id="graph/" + settings.bot_id)
            )

        async def start(self):
            starting.set()
            await finish_start.wait()

        async def close(self):
            closed.set()

    supervisor = MultiBotSupervisor(
        repository, storage_profiles=storage_profiles,
        settings_resolver=lambda current, storage: SimpleNamespace(
            bot_id=current["bot_id"], dsn=storage.dsn,
        ), host_factory=FakeHost,
    )
    lease = await repository.claim_supervisor("host/retracted", "incarnation/retracted")
    supervisor.lease = lease
    runner = _BotRunner(asyncio.Event())
    task = asyncio.create_task(supervisor._run_bot(bot, runner))
    try:
        await asyncio.wait_for(starting.wait(), timeout=5)
        await repository.set_desired(
            operation_id="runtime/retracted/offline", bot_id=bot["bot_id"],
            desired_state="OFFLINE", expected_revision=bot["intent_revision"],
        )
        finish_start.set()
        await asyncio.wait_for(task, timeout=5)
        current = await repository.inspect(bot["bot_id"])
        assert current["desired_state"] == "OFFLINE"
        assert current["observed_state"] == "OFFLINE"
        assert current["error_code"] is None
        assert closed.is_set()
        assert runner.host is None
        assert not runner.management_accepting
    finally:
        finish_start.set()
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        await repository.release_supervisor(lease)


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
