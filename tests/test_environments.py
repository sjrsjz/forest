from __future__ import annotations

import asyncio
import base64
import csv
import hashlib
import io
import json
import shutil
import threading
import zipfile
from pathlib import Path
from uuid import uuid4

import pytest

from forest.drivers import PythonWorker, WorkerIdentity
from forest.environments import (
    EnvironmentCatalog,
    create_dependency_lock,
    current_target,
    validate_lock,
)
from forest.errors import ForestError
from forest.packages import PackageCatalog

EXAMPLE = Path(__file__).resolve().parents[1] / "examples" / "counter"


def wheel(name, version="1.0", *, requires=(), extra_files=None, code=None):
    info = f"{name}-{version}.dist-info"
    files = {
        f"{name}/__init__.py": (code or f"VERSION = {version!r}\n").encode(),
        f"{info}/METADATA": (
            f"Metadata-Version: 2.1\nName: {name}\nVersion: {version}\n"
            + "".join(f"Requires-Dist: {value}\n" for value in requires) + "\n"
        ).encode(),
        f"{info}/WHEEL": b"Wheel-Version: 1.0\nRoot-Is-Purelib: true\nTag: py3-none-any\n",
        **(extra_files or {}),
    }
    record = io.StringIO(newline="")
    writer = csv.writer(record)
    for path, value in files.items():
        checksum = base64.urlsafe_b64encode(hashlib.sha256(value).digest()).rstrip(b"=").decode()
        writer.writerow([path, "sha256=" + checksum, len(value)])
    writer.writerow([f"{info}/RECORD", "", ""])
    files[f"{info}/RECORD"] = record.getvalue().encode()
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for path, value in files.items():
            archive.writestr(path, value)
    content = output.getvalue()
    return {
        "file": f"wheelhouse/{name}-{version}-py3-none-any.whl",
        "name": name, "version": version, "sha256": hashlib.sha256(content).hexdigest(),
    }, content


def locked_files(*wheels, roots=None):
    lock = {
        "lock_version": 1, "target": current_target(),
        "requirements": roots or [f"{wheels[0][0]['name']}=={wheels[0][0]['version']}"],
        "wheels": [entry for entry, _ in wheels],
    }
    return {"dependencies.lock.json": json.dumps(lock).encode(), **{
        entry["file"]: value for entry, value in wheels
    }}


def source(tmp_path, *, version="1.0"):
    root = tmp_path / ("source-" + uuid4().hex)
    shutil.copytree(EXAMPLE, root)
    path = root / "module.toml"
    path.write_text('dependency_lock = "dependencies.lock.json"\n' + path.read_text())
    for name, value in locked_files(wheel("forest_demo_dependency", version)).items():
        target = root / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(value)
    (root / "python/counter.py").write_text('''
import importlib.util
import sys
from forest.sdk import Plugin, service
import forest_demo_dependency

class Counter(Plugin):
    @service("counter.increment", semantics="read")
    async def increment(self, payload, ctx):
        return {"version": forest_demo_dependency.VERSION,
                "dependency_file": forest_demo_dependency.__file__,
                "host_httpx": importlib.util.find_spec("httpx") is not None,
                "paths": sys.path}
''')
    return root


def test_lock_validates_complete_transitive_closure_and_extra_markers():
    files = locked_files(
        wheel("parent", requires=["child>=2", 'optional==3; extra == "images"']),
        wheel("child", "2"), wheel("optional", "3"), roots=["parent[images]==1.0"],
    )
    result = validate_lock(files, "dependencies.lock.json")
    assert "child/__init__.py" in result.files
    assert "optional/__init__.py" in result.files
    bad = locked_files(wheel("parent", requires=["child>=2"]), wheel("child", "1"))
    with pytest.raises(ForestError, match="transitive"):
        validate_lock(bad, "dependencies.lock.json")
    extra = locked_files(wheel("parent"), wheel("unused"))
    with pytest.raises(ForestError, match="closure"):
        validate_lock(extra, "dependencies.lock.json")


def test_multitarget_lock_selects_only_current_wheel_and_checks_foreign_hash():
    current = current_target()
    foreign = {**current, "platform": "another-platform"}
    native_wheel = wheel("native_demo")
    foreign_wheel = wheel("foreign_demo")
    lock = {
        "lock_version": 2,
        "targets": [
            {"target": current, "requirements": ["native_demo==1.0"],
             "wheels": [native_wheel[0]]},
            {"target": foreign, "requirements": ["foreign_demo==1.0"],
             "wheels": [foreign_wheel[0]]},
        ],
    }
    files = {
        "dependencies.lock.json": json.dumps(lock).encode(),
        native_wheel[0]["file"]: native_wheel[1],
        foreign_wheel[0]["file"]: foreign_wheel[1],
    }
    selected = validate_lock(files, "dependencies.lock.json")
    assert "native_demo/__init__.py" in selected.files
    assert "foreign_demo/__init__.py" not in selected.files
    damaged = {**files, foreign_wheel[0]["file"]: b"damaged"}
    with pytest.raises(ForestError, match="hash"):
        validate_lock(damaged, "dependencies.lock.json")


@pytest.mark.parametrize("change,code", [
    ("target", "DEPENDENCY_TARGET_MISMATCH"),
    ("sdk", "DEPENDENCY_TARGET_MISMATCH"),
    ("hash", "DEPENDENCY_TAMPERED"),
    ("unlocked", "INVALID_DEPENDENCY_LOCK"),
    ("url", "INVALID_DEPENDENCY_LOCK"),
    ("duplicate", "INVALID_DEPENDENCY_LOCK"),
])
def test_dependency_identity_lock_and_target_fail_closed(change, code):
    files = locked_files(wheel("demo"))
    lock = json.loads(files["dependencies.lock.json"])
    if change == "target":
        lock["target"]["python_version"] = "0.0.0"
    elif change == "sdk":
        lock["target"]["sdk_api"] = "9.0"
    elif change == "hash":
        lock["wheels"][0]["sha256"] = "0" * 64
    elif change == "unlocked":
        lock["requirements"] = ["demo>=1"]
    elif change == "url":
        lock["requirements"] = ["demo @ https://example.invalid/demo.whl"]
    else:
        lock["wheels"] *= 2
    files["dependencies.lock.json"] = json.dumps(lock).encode()
    with pytest.raises(ForestError) as caught:
        validate_lock(files, "dependencies.lock.json")
    assert caught.value.code == code


@pytest.mark.parametrize("name", ["unsafe.pth", "forest/sdk.py", "json/__init__.py",
                                      "demo-1.0.data/scripts/run-demo"])
def test_wheel_startup_sdk_shadow_and_external_install_schemes_are_rejected(name):
    files = locked_files(wheel("demo", extra_files={name: b"raise RuntimeError('never execute')"}))
    with pytest.raises(ForestError):
        validate_lock(files, "dependencies.lock.json")


def test_whole_wheel_hash_does_not_hide_a_broken_record_or_wrong_tag():
    entry, content = wheel("demo")
    changed = io.BytesIO()
    with zipfile.ZipFile(io.BytesIO(content)) as original, zipfile.ZipFile(changed, "w") as archive:
        for info in original.infolist():
            value = original.read(info)
            if info.filename == "demo/__init__.py":
                value = b"VERSION = 'changed without updating RECORD'\n"
            archive.writestr(info, value)
    damaged = changed.getvalue()
    entry["sha256"] = hashlib.sha256(damaged).hexdigest()
    with pytest.raises(ForestError) as caught:
        validate_lock(locked_files((entry, damaged)), "dependencies.lock.json")
    assert caught.value.code == "DEPENDENCY_TAMPERED"
    entry, content = wheel("demo")
    entry["file"] = entry["file"].replace("py3-none-any", "cp27-none-any")
    with pytest.raises(ForestError) as caught:
        validate_lock(locked_files((entry, content)), "dependencies.lock.json")
    assert caught.value.code == "DEPENDENCY_TARGET_MISMATCH"


def test_lock_validation_and_materialization_never_import_dependency_code(tmp_path):
    files = locked_files(wheel("demo", code="raise RuntimeError('must not be imported')\n"))
    assert "demo/__init__.py" in validate_lock(files, "dependencies.lock.json").files
    root = source(tmp_path)
    snapshot = PackageCatalog(tmp_path / "catalog").build(root)
    environment = EnvironmentCatalog(tmp_path / "environments").materialize(snapshot)
    assert environment.descriptor["target"] == current_target()
    same = EnvironmentCatalog(tmp_path / "environments").materialize(snapshot)
    assert same.digest == environment.digest
    dependency = environment.root / "lib/forest_demo_dependency/__init__.py"
    dependency.chmod(0o600)
    dependency.write_text("VERSION = 'tampered'\n")
    with pytest.raises(ForestError) as caught:
        EnvironmentCatalog(tmp_path / "environments").materialize(snapshot)
    assert caught.value.code == "ENVIRONMENT_TAMPERED"


def test_author_lock_builder_hashes_complete_wheelhouse_without_changing_source(tmp_path):
    root = source(tmp_path)
    lock_path = root / "dependencies.lock.json"
    before = lock_path.read_bytes()
    generated = create_dependency_lock(root, ["forest_demo_dependency==1.0"])
    expected = json.loads(before)
    expected["wheels"][0]["name"] = "forest-demo-dependency"
    assert generated == expected
    assert lock_path.read_bytes() == before
    with pytest.raises(ForestError, match="transitive"):
        create_dependency_lock(root, ["forest_demo_dependency==2.0"])
    assert lock_path.read_bytes() == before


async def test_real_workers_isolate_locked_dependency_generations_and_host_packages(tmp_path):
    catalog = PackageCatalog(tmp_path / "catalog")
    snapshots = [catalog.build(source(tmp_path, version=version), development=True) for version in (
        "1.0", "2.0",
    )]
    workers = [PythonWorker(snapshot, WorkerIdentity(str(uuid4()), str(uuid4()), str(uuid4()), "bot"))
               for snapshot in snapshots]
    try:
        for worker in workers:
            await worker.launch()
            await worker.prepare({})
            await worker.start()
            await worker.activate()
        responses = await asyncio.gather(*(
            worker.invoke("counter.increment", {}, call_id=str(uuid4()), operation_id=str(uuid4()))
            for worker in workers
        ))
        assert [result["version"] for result in responses] == ["1.0", "2.0"]
        assert workers[0].environment.digest != workers[1].environment.digest
        for worker, result in zip(workers, responses):
            assert result["host_httpx"] is False
            assert Path(result["dependency_file"]).is_relative_to(worker.environment.root)
            assert not any("site-packages" in path for path in result["paths"])
        # Loading the new generation never changes the old interpreter's module objects.
        assert (await workers[0].invoke("counter.increment", {}, call_id="again", operation_id="again"))[
            "version"] == "1.0"
    finally:
        await asyncio.gather(*(worker.terminate() for worker in workers))


async def test_builtin_onebot_codec_is_a_complete_isolated_package(tmp_path):

    root = Path(__file__).resolve().parents[1] / "src/forest/builtin/onebot_codec"
    snapshot = PackageCatalog(tmp_path / "catalog").build(root)
    worker = PythonWorker(snapshot, WorkerIdentity("codec", "generation", "incarnation", "bot"))
    try:
        await worker.launch()
        await worker.prepare({})
        await worker.start()
        await worker.activate()
        segments = [{"type": "text", "text": "literal [CQ:at,qq=42]"}]
        result = await worker.invoke("onebot.codec.prepare-send", {
            "channel_kind": "group", "channel_id": "123", "segments": segments,
        }, call_id="prepare", operation_id="prepare")
        assert result["body"]["message"] == [
            {"type": "text", "data": {"text": "literal [CQ:at,qq=42]"}}
        ]
        normalized = await worker.invoke("onebot.codec.normalize", {
            "self_id": "42", "body_b64": base64.b64encode(json.dumps({
                "post_type": "message", "message_type": "group", "self_id": 42,
                "message_id": 1, "user_id": 2, "group_id": 123, "time": 1,
                "message": [{"type": "text", "data": {"text": "hello"}}],
                "sender": {"user_id": 2, "nickname": "Alice"},
            }).encode()).decode(),
        }, call_id="normalize", operation_id="normalize")
        assert normalized["message"]["author"]["display_name"] == "Alice"
        assert "lib/httpx/__init__.py" not in worker.environment.descriptor["files"]
    finally:
        await worker.terminate()


@pytest.mark.parametrize("cancel_launch", [True, False])
async def test_materialization_is_joined_on_cancel_and_cannot_launch_after_stop(
        tmp_path, monkeypatch, cancel_launch):
    snapshot = PackageCatalog(tmp_path / "catalog").build(source(tmp_path))
    worker = PythonWorker(snapshot, WorkerIdentity("instance", "generation", "incarnation", "bot"))
    entered = threading.Event()
    release = threading.Event()
    finished = threading.Event()
    original = EnvironmentCatalog.materialize

    def delayed(catalog, package):
        entered.set()
        assert release.wait(timeout=10)
        try:
            return original(catalog, package)
        finally:
            finished.set()

    monkeypatch.setattr(EnvironmentCatalog, "materialize", delayed)
    launch = asyncio.create_task(worker.launch())
    assert await asyncio.to_thread(entered.wait, 5)
    closing = None
    try:
        if cancel_launch:
            launch.cancel()
            await asyncio.sleep(0)
            launch.cancel()  # Repeated shutdown cannot detach the live thread either.
        else:
            closing = asyncio.create_task(worker.terminate())
        await asyncio.sleep(0.03)
        assert not launch.done()
        if closing:
            assert not closing.done()
        assert worker.process is None
    finally:
        release.set()
    results = await asyncio.gather(launch, return_exceptions=True)
    if closing:
        await closing
    assert isinstance(results[0], asyncio.CancelledError if cancel_launch else ForestError)
    assert finished.is_set()
    assert worker._environment_task is None
    assert worker.state == "STOPPED" and worker.process is None
