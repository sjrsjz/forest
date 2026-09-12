from __future__ import annotations

import json
import multiprocessing
import sqlite3
from pathlib import Path

import pytest

from forest.bootstrap import BootstrapState, normalize_dsn
from forest.errors import ForestError
from forest.security import hash_password

DSN = "postgresql://forest:synthetic-db-password@localhost/forest"


@pytest.fixture(scope="module")
def password_hash() -> str:
    return hash_password("synthetic-owner-password")


def _freeze_worker(
    directory: str, token: str, dsn: str, password_hash: str, barrier, queue
) -> None:
    state = BootstrapState(directory)
    barrier.wait(timeout=15)
    try:
        plan = state.freeze(token, dsn, "admin", password_hash)
    except ForestError as error:
        queue.put(("error", error.code))
    else:
        queue.put(("success", plan["target_identity"]["dbname"]))


def _initialize_worker(directory: str, barrier, queue) -> None:
    state = BootstrapState(directory)
    barrier.wait(timeout=15)
    queue.put(state.initialize())


def test_setup_credential_is_returned_once_and_expires(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr("forest.bootstrap.time.time", lambda: 1000)
    state = BootstrapState(tmp_path, token_ttl_seconds=10)
    token = state.initialize()
    assert token
    assert state.verify_token(token)
    assert state.initialize() is None
    restarted = BootstrapState(tmp_path)
    assert restarted.initialize() is None
    assert restarted.verify_token(token)
    monkeypatch.setattr("forest.bootstrap.time.time", lambda: 1010)
    assert not state.verify_token(token)
    assert state.status()["stage"] == "UNINITIALIZED"
    replacement = state.rotate_setup_token()
    assert replacement != token and state.verify_token(replacement)
    assert not state.verify_token(token)


def test_freeze_recovery_and_completion_are_immutable(tmp_path: Path, password_hash: str) -> None:
    state = BootstrapState(tmp_path)
    token = state.initialize()
    plan = state.freeze(token, DSN, "admin", password_hash)
    assert state.freeze(token, DSN, "admin", password_hash) == plan
    assert state.status()["stage"] == "FROZEN"
    for changed_dsn, changed_name in [
        (DSN.replace("/forest", "/other"), "admin"),
        (DSN, "someone"),
    ]:
        with pytest.raises(ForestError) as error:
            state.freeze(token, changed_dsn, changed_name, password_hash)
        assert error.value.code == "SETUP_PLAN_FROZEN"
    recovered = BootstrapState(tmp_path)
    assert recovered.load_plan() == plan
    assert recovered.load_dsn() == plan["dsn"]
    with pytest.raises(ForestError):
        recovered.complete(plan["setup_id"], "stale-plan-digest")
    with pytest.raises(ForestError):
        recovered.complete("wrong-setup", plan["plan_digest"])
    with pytest.raises(ForestError):
        recovered.rotate_setup_token()
    assert recovered.complete(plan["setup_id"], plan["plan_digest"])["setup_required"] is False
    assert recovered.complete(plan["setup_id"], plan["plan_digest"])["stage"] == "COMPLETE"
    assert not recovered.verify_token(token)
    assert recovered.initialize() is None
    assert BootstrapState(tmp_path).status()["stage"] == "COMPLETE"
    assert recovered.load_plan() == plan
    with pytest.raises(ForestError):
        recovered.freeze(token, DSN, "admin", password_hash)
    with pytest.raises(ForestError):
        recovered.rotate_setup_token()


def test_safe_status_and_state_file_never_contain_plain_credentials(
    tmp_path: Path, password_hash: str
) -> None:
    state = BootstrapState(tmp_path)
    token = state.initialize()
    state.freeze(token, DSN, "admin", password_hash)
    status = json.dumps(state.status())
    data = (tmp_path / "bootstrap.sqlite3").read_bytes()
    for secret in (token, password_hash, "synthetic-db-password", DSN):
        assert secret not in status
        assert secret.encode() not in data
    assert set(state.status()) == {"stage", "setup_required", "deployment_id", "setup_id"}


def test_plan_integrity_rejects_corrupt_local_digest(tmp_path: Path, password_hash: str) -> None:
    state = BootstrapState(tmp_path)
    token = state.initialize()
    state.freeze(token, DSN, "admin", password_hash)
    with sqlite3.connect(state.path) as connection:
        connection.execute("UPDATE bootstrap_state SET plan_digest=?", ("0" * 64,))
    with pytest.raises(ForestError) as error:
        state.load_plan()
    assert error.value.code == "SETUP_STATE_INVALID"


def test_two_processes_cannot_choose_two_target_databases(
    tmp_path: Path, password_hash: str
) -> None:
    state = BootstrapState(tmp_path)
    token = state.initialize()
    context = multiprocessing.get_context("spawn")
    barrier, queue = context.Barrier(2), context.Queue()
    workers = [
        context.Process(
            target=_freeze_worker,
            args=(
                str(tmp_path),
                token,
                DSN.replace("/forest", f"/target{index}"),
                password_hash,
                barrier,
                queue,
            ),
        )
        for index in range(2)
    ]
    for worker in workers:
        worker.start()
    try:
        outcomes = [queue.get(timeout=25) for _ in workers]
        for worker in workers:
            worker.join(timeout=15)
            assert worker.exitcode == 0
        assert sum(result[0] == "success" for result in outcomes) == 1
        assert ("error", "SETUP_PLAN_FROZEN") in outcomes
        winner = next(value for kind, value in outcomes if kind == "success")
        assert BootstrapState(tmp_path).load_plan()["target_identity"]["dbname"] == winner
    finally:
        for worker in workers:
            if worker.is_alive():
                worker.terminate()
                worker.join(timeout=5)
        queue.close()


def test_two_installers_receive_only_one_setup_credential(tmp_path: Path) -> None:
    context = multiprocessing.get_context("spawn")
    barrier, queue = context.Barrier(2), context.Queue()
    workers = [
        context.Process(target=_initialize_worker, args=(str(tmp_path), barrier, queue))
        for _ in range(2)
    ]
    for worker in workers:
        worker.start()
    try:
        tokens = [queue.get(timeout=25) for _ in workers]
        for worker in workers:
            worker.join(timeout=15)
            assert worker.exitcode == 0
        assert tokens.count(None) == 1
        assert BootstrapState(tmp_path).verify_token(next(token for token in tokens if token))
    finally:
        for worker in workers:
            if worker.is_alive():
                worker.terminate()
                worker.join(timeout=5)
        queue.close()


@pytest.mark.parametrize(
    "dsn",
    [
        "sqlite:///tmp/example",
        "host=localhost dbname=forest user=forest",
        "postgresql:///forest",
        "postgresql://forest@localhost/forest?options=-csearch_path%3Dother",
        "postgresql://forest@host1,host2/forest",
        "postgresql://forest@localhost:999999/forest",
        "postgresql://forest@localhost/forest?connect_timeout=0",
    ],
)
def test_ambiguous_or_unsupported_storage_targets_are_rejected(dsn: str) -> None:
    with pytest.raises(ForestError) as error:
        normalize_dsn(dsn)
    assert error.value.code == "VALIDATION_FAILED"
    assert dsn not in str(error.value)


def test_equivalent_explicit_locators_share_plan_identity(
    tmp_path: Path, password_hash: str
) -> None:
    state = BootstrapState(tmp_path)
    token = state.initialize()
    first = state.freeze(token, DSN, "admin", password_hash)
    alternate = "postgres://forest:synthetic-db-password@LOCALHOST.:5432/forest?connect_timeout=5&sslmode=prefer"
    assert state.freeze(token, alternate, "admin", password_hash) == first


@pytest.mark.parametrize(
    "token",
    [None, "", "invalid", "x" * 100000, "\ud800"],
    ids=["wrong-type", "empty", "invalid", "oversized", "invalid-unicode"],
)
def test_invalid_setup_tokens_are_bounded_and_cannot_freeze(
    tmp_path: Path, password_hash: str, token
) -> None:
    state = BootstrapState(tmp_path)
    state.initialize()
    assert not state.verify_token(token)
    with pytest.raises(ForestError) as error:
        state.freeze(token, DSN, "admin", password_hash)
    assert error.value.code == "SETUP_TOKEN_INVALID"
    assert state.load_plan() is None
