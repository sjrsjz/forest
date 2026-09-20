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


def _credential(*, code: str = "p" * 48):
    issued_at = datetime.now(timezone.utc)
    return SimpleNamespace(
        setup_id="setup/0123456789abcdef",
        pairing_code=code,
        issued_at=issued_at,
        expires_at=issued_at + timedelta(hours=1),
    )


async def _initial_admin(authority, *, password="bootstrap-password-1"):
    credential = _credential()
    await authority.initialize_setup(credential)
    return await authority.complete_setup(
        pairing_code=credential.pairing_code,
        username="admin",
        display_name="Administrator",
        password=password,
    )


@pytest.mark.postgres
async def test_web_identity_bootstrap_login_csrf_and_revocation(deployment_catalog):
    await deployment_catalog.initialize()
    authority = _authority(deployment_catalog)
    credential = _credential()
    pending = await authority.initialize_setup(credential)
    assert pending["setup_required"] is True
    assert set(pending) == {
        "state",
        "setup_required",
        "expires_at",
        "credential_expired",
    }
    admin = await authority.complete_setup(
        pairing_code=credential.pairing_code,
        username="admin",
        display_name="Deployment Administrator",
        password="bootstrap-password-1",
    )
    restarted = await authority.initialize_setup(credential)
    assert restarted == {
        "state": "COMPLETED",
        "setup_required": False,
        "expires_at": None,
        "credential_expired": False,
    }
    replacement = _credential(code="z" * 48)
    replacement.setup_id = "setup/replacement000000"
    assert await authority.initialize_setup(replacement) == restarted
    async with deployment_catalog.store.pool.connection() as conn:
        state = await (
            await conn.execute(
                "SELECT state,pairing_code_digest,initial_admin_ref,revision "
                "FROM forest_deployment.web_setup_state"
            )
        ).fetchone()
    assert state == {
        "state": "COMPLETED",
        "pairing_code_digest": None,
        "initial_admin_ref": admin.subject_id,
        "revision": 2,
    }
    with pytest.raises(ForestError) as consumed:
        await authority.complete_setup(
            pairing_code=credential.pairing_code,
            username="other",
            display_name="Other",
            password="other-password-1",
        )
    assert consumed.value.code == "WEB_SETUP_ALREADY_COMPLETE"
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
    with pytest.raises(ForestError) as ticket_revalidation:
        await authority.revalidate_session(authenticated)
    assert ticket_revalidation.value.code == "MANAGEMENT_AUTHENTICATION_REQUIRED"
    with pytest.raises(ForestError) as revoked:
        await authority.authenticate(session.session_token)
    assert revoked.value.code == "MANAGEMENT_AUTHENTICATION_REQUIRED"


@pytest.mark.postgres
async def test_web_setup_rejects_wrong_pairing_credential(deployment_catalog):
    await deployment_catalog.initialize()
    authority = _authority(deployment_catalog)
    credential = _credential()
    await authority.initialize_setup(credential)
    assert (await authority.setup_status())["credential_expired"] is False
    active_replacement = _credential(code="q" * 48)
    with pytest.raises(ForestError) as conflict:
        await authority.initialize_setup(active_replacement)
    assert conflict.value.code == "WEB_SETUP_CREDENTIAL_CONFLICT"
    with pytest.raises(ForestError) as wrong:
        await authority.complete_setup(
            pairing_code="x" * 48,
            username="admin",
            display_name="Administrator",
            password="bootstrap-password-1",
        )
    assert wrong.value.code == "WEB_SETUP_CREDENTIAL_INVALID"
    assert await authority.list_accounts() == ()


@pytest.mark.postgres
async def test_web_setup_rejects_expired_pairing_credential(deployment_catalog):
    await deployment_catalog.initialize()
    authority = _authority(deployment_catalog)
    credential = SimpleNamespace(
        setup_id="setup/expired0123456789",
        pairing_code="e" * 48,
        issued_at=datetime.now(timezone.utc) - timedelta(hours=1),
        expires_at=datetime.now(timezone.utc) - timedelta(seconds=1),
    )
    await authority.initialize_setup(credential)
    assert (await authority.setup_status())["credential_expired"] is True
    with pytest.raises(ForestError) as expired:
        await authority.complete_setup(
            pairing_code=credential.pairing_code,
            username="admin",
            display_name="Administrator",
            password="bootstrap-password-1",
        )
    assert expired.value.code == "WEB_SETUP_CREDENTIAL_EXPIRED"
    assert await authority.list_accounts() == ()

    renewed_at = datetime.now(timezone.utc)
    renewed = SimpleNamespace(
        setup_id=credential.setup_id,
        pairing_code="r" * 48,
        issued_at=renewed_at,
        expires_at=renewed_at + timedelta(hours=1),
    )
    status = await authority.initialize_setup(renewed)
    assert status["credential_expired"] is False
    with pytest.raises(ForestError) as stale:
        await authority.complete_setup(
            pairing_code=credential.pairing_code,
            username="admin",
            display_name="Administrator",
            password="bootstrap-password-1",
        )
    assert stale.value.code == "WEB_SETUP_CREDENTIAL_INVALID"
    assert (
        await authority.complete_setup(
            pairing_code=renewed.pairing_code,
            username="admin",
            display_name="Administrator",
            password="bootstrap-password-1",
        )
    ).role == "ADMIN"


@pytest.mark.postgres
async def test_web_identity_roles_last_admin_and_password_change_revoke_sessions(
    deployment_catalog,
):
    await deployment_catalog.initialize()
    authority = _authority(deployment_catalog)
    admin = await _initial_admin(authority)
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
    credential = _credential()
    await authority.initialize_setup(credential)

    async def create(index: int):
        try:
            return await authority.complete_setup(
                pairing_code=credential.pairing_code,
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
    assert failures == ["WEB_SETUP_ALREADY_COMPLETE"]
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
    await _initial_admin(first)
    saved = await first.create_account(
        operation_id="web/account/operator",
        username="operator",
        display_name="Operator",
        password="operator-password-1",
        role="AUDITOR",
    )
    rotated = DeploymentWebIdentityAuthority(
        deployment_catalog.store,
        active_key_id="master/test/2",
        key_resolver=keys.get,
    )
    assert (
        await rotated.create_account(
            operation_id="web/account/operator",
            username="operator",
            display_name="Operator",
            password="operator-password-1",
            role="AUDITOR",
        )
    ) == saved
    with pytest.raises(ForestError) as conflict:
        await rotated.create_account(
            operation_id="web/account/operator",
            username="operator",
            display_name="Operator",
            password="different-password-1",
            role="AUDITOR",
        )
    assert conflict.value.code == "WEB_ACCOUNT_OPERATION_CONFLICT"


@pytest.mark.postgres
async def test_login_failures_converge_into_persistent_identity_throttle(
    deployment_catalog,
):
    await deployment_catalog.initialize()
    authority = _authority(deployment_catalog)
    await _initial_admin(authority)

    async def reject_once(index: int) -> str:
        try:
            await authority.login(
                username="admin",
                password=f"incorrect-password-{index}",
                peer_id="192.0.2.10",
            )
        except ForestError as exc:
            return exc.code
        raise AssertionError("Invalid credentials created a session")

    assert await asyncio.gather(*(reject_once(index) for index in range(8))) == [
        "MANAGEMENT_AUTHENTICATION_REQUIRED"
    ] * 8

    restarted = _authority(deployment_catalog)
    with pytest.raises(ForestError) as limited:
        await restarted.login(
            username="admin",
            password="bootstrap-password-1",
            peer_id="192.0.2.11",
        )
    assert limited.value.code == "MANAGEMENT_LOGIN_RATE_LIMITED"
    assert 1 <= limited.value.details["retry_after_seconds"] <= 900

    async with deployment_catalog.store.pool.connection() as conn:
        rows = await (
            await conn.execute(
                "SELECT bucket_kind,failure_count,blocked_until "
                "FROM forest_deployment.web_login_throttles ORDER BY bucket_kind"
            )
        ).fetchall()
    assert sorted((row["bucket_kind"], row["failure_count"]) for row in rows) == [
        ("IDENTITY", 8),
        ("PEER", 0),
        ("PEER", 8),
    ]
    assert rows[0]["blocked_until"] is not None

    async with deployment_catalog.store.pool.connection() as conn:
        await conn.execute(
            "UPDATE forest_deployment.web_login_throttles SET "
            "window_started_at=clock_timestamp()-interval '16 minutes',"
            "blocked_until=clock_timestamp()-interval '1 second',revision=revision+1"
        )
    session = await restarted.login(
        username="admin",
        password="bootstrap-password-1",
        peer_id="192.0.2.11",
    )
    assert session.subject.username == "admin"
