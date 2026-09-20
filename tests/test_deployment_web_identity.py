from __future__ import annotations

import asyncio
import os
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

import psycopg
import pytest
import pytest_asyncio
from psycopg import sql

from forest.errors import ForestError
from forest.storage.clean_deployment_store import CleanDeploymentStore
from forest.storage.deployment_catalog import DeploymentCatalogRepository
from forest.storage.deployment_web_identity import DeploymentWebIdentityAuthority

_KEYS = {"master/test/1": bytes(range(32))}


@pytest_asyncio.fixture
async def deployment_catalog():
    dsn = os.environ.get("FOREST_TEST_DSN")
    if not dsn:
        pytest.skip("FOREST_TEST_DSN must name a private PostgreSQL test instance")
    name = "forest_web_identity_" + uuid4().hex
    admin = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
    await admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    parsed = urlsplit(dsn)
    uri = urlunsplit((parsed.scheme, parsed.netloc, "/" + name, "", ""))
    store = CleanDeploymentStore(uri, "deployment/test")
    await store.open()
    repository = DeploymentCatalogRepository(store)
    try:
        yield repository
    finally:
        await store.close()
        await admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name)))
        await admin.close()


def _authority(deployment_catalog):
    return DeploymentWebIdentityAuthority(
        deployment_catalog.store,
        active_key_id="master/test/1",
        key_resolver=_KEYS.get,
    )


@pytest.mark.postgres
async def test_web_identity_bootstrap_login_csrf_and_revocation(deployment_catalog):
    await deployment_catalog.initialize()
    authority = _authority(deployment_catalog)
    admin = await authority.ensure_bootstrap_admin(
        operation_id="web/bootstrap/admin",
        username="admin",
        display_name="Deployment Administrator",
        password="bootstrap-password-1",
    )
    replay = await authority.ensure_bootstrap_admin(
        operation_id="web/bootstrap/admin",
        username="admin",
        display_name="Deployment Administrator",
        password="bootstrap-password-1",
    )
    assert replay == admin
    assert admin.role == "ADMIN"
    assert "password_hash" not in (await authority.list_accounts())[0]

    with pytest.raises(ForestError) as invalid:
        await authority.login(username="admin", password="incorrect-password")
    assert invalid.value.code == "MANAGEMENT_AUTHENTICATION_REQUIRED"

    session = await authority.login(
        username="admin", password="bootstrap-password-1"
    )
    authenticated = await authority.authenticate(session.session_token)
    assert authenticated.subject == admin
    authenticated.verify_csrf(session.csrf_token)
    with pytest.raises(ForestError) as csrf:
        authenticated.verify_csrf("x" * 43)
    assert csrf.value.code == "MANAGEMENT_CSRF_REQUIRED"

    await authority.logout(session.session_token)
    await authority.logout(session.session_token)
    with pytest.raises(ForestError) as revoked:
        await authority.authenticate(session.session_token)
    assert revoked.value.code == "MANAGEMENT_AUTHENTICATION_REQUIRED"


@pytest.mark.postgres
async def test_web_identity_roles_last_admin_and_password_change_revoke_sessions(
    deployment_catalog,
):
    await deployment_catalog.initialize()
    authority = _authority(deployment_catalog)
    admin = await authority.ensure_bootstrap_admin(
        operation_id="web/bootstrap/admin",
        username="admin",
        display_name="Administrator",
        password="bootstrap-password-1",
    )
    auditor = await authority.create_account(
        operation_id="web/account/auditor",
        username="auditor",
        display_name="Audit Reader",
        password="auditor-password-1",
        role="AUDITOR",
    )
    auditor_session = await authority.login(
        username="auditor", password="auditor-password-1"
    )

    with pytest.raises(ForestError) as final_admin:
        await authority.update_account(
            operation_id="web/account/admin/demote",
            subject_id=admin.subject_id,
            display_name=admin.display_name,
            role="AUDITOR",
            enabled=True,
            expected_revision=admin.revision,
        )
    assert final_admin.value.code == "WEB_LAST_ADMINISTRATOR_REQUIRED"

    promoted = await authority.update_account(
        operation_id="web/account/auditor/promote",
        subject_id=auditor.subject_id,
        display_name=auditor.display_name,
        role="ADMIN",
        enabled=True,
        expected_revision=auditor.revision,
    )
    demoted = await authority.update_account(
        operation_id="web/account/admin/demote/after-promotion",
        subject_id=admin.subject_id,
        display_name=admin.display_name,
        role="AUDITOR",
        enabled=True,
        expected_revision=admin.revision,
    )
    assert promoted.role == "ADMIN"
    assert demoted.role == "AUDITOR"

    changed = await authority.change_password(
        operation_id="web/account/auditor/password",
        subject_id=promoted.subject_id,
        password="auditor-password-2",
        expected_revision=promoted.revision,
    )
    assert changed.revision == promoted.revision + 1
    original_create_receipt = await authority.create_account(
        operation_id="web/account/auditor",
        username="auditor",
        display_name="Audit Reader",
        password="auditor-password-1",
        role="AUDITOR",
    )
    assert original_create_receipt == auditor
    with pytest.raises(ForestError):
        await authority.authenticate(auditor_session.session_token)
    with pytest.raises(ForestError):
        await authority.login(username="auditor", password="auditor-password-1")
    assert (
        await authority.login(username="auditor", password="auditor-password-2")
    ).subject.revision == changed.revision
    active = await authority.login(username="auditor", password="auditor-password-2")
    await authority.revoke_sessions(
        operation_id="web/account/auditor/revoke",
        subject_id=changed.subject_id,
        expected_revision=changed.revision,
    )
    with pytest.raises(ForestError):
        await authority.authenticate(active.session_token)


@pytest.mark.postgres
async def test_concurrent_first_administrator_has_one_authoritative_winner(
    deployment_catalog,
):
    await deployment_catalog.initialize()
    authority = _authority(deployment_catalog)

    async def create(index: int):
        try:
            return await authority.ensure_bootstrap_admin(
                operation_id=f"web/bootstrap/{index}",
                username=f"admin{index}",
                display_name=f"Administrator {index}",
                password=f"bootstrap-password-{index}",
            )
        except ForestError as exc:
            return exc.code

    results = await asyncio.gather(create(1), create(2))
    winners = [result for result in results if not isinstance(result, str)]
    failures = [result for result in results if isinstance(result, str)]
    assert len(winners) == 1
    assert failures == ["WEB_BOOTSTRAP_ALREADY_COMPLETE"]
    assert len(await authority.list_accounts()) == 1


@pytest.mark.postgres
async def test_credential_operation_replay_uses_recorded_key_after_rotation(
    deployment_catalog,
):
    await deployment_catalog.initialize()
    keys = {"master/test/1": bytes(range(32)), "master/test/2": b"z" * 32}
    first = DeploymentWebIdentityAuthority(
        deployment_catalog.store,
        active_key_id="master/test/1",
        key_resolver=keys.get,
    )
    saved = await first.ensure_bootstrap_admin(
        operation_id="web/bootstrap/admin",
        username="admin",
        display_name="Administrator",
        password="bootstrap-password-1",
    )
    rotated = DeploymentWebIdentityAuthority(
        deployment_catalog.store,
        active_key_id="master/test/2",
        key_resolver=keys.get,
    )
    assert (
        await rotated.ensure_bootstrap_admin(
            operation_id="web/bootstrap/admin",
            username="admin",
            display_name="Administrator",
            password="bootstrap-password-1",
        )
    ) == saved
    with pytest.raises(ForestError) as conflict:
        await rotated.ensure_bootstrap_admin(
            operation_id="web/bootstrap/admin",
            username="admin",
            display_name="Administrator",
            password="different-password-1",
        )
    assert conflict.value.code == "WEB_ACCOUNT_OPERATION_CONFLICT"
