import shutil
from pathlib import Path
from uuid import uuid4

import pytest

from forest.drivers import PythonWorker, WorkerIdentity
from forest.errors import ForestError
from forest.packages import PackageCatalog


def source(tmp_path: Path) -> Path:
    root = tmp_path / "message-runtime-source"
    module = root / "src" / "message_runtime"
    module.mkdir(parents=True)
    (root / "pyproject.toml").write_text(
        '[project]\nname="message-runtime"\nversion="0.1.0"\n'
        '[tool.forest]\nmodule-root="src"\n',
        encoding="utf-8",
    )
    (module / "__init__.py").write_text(
        '''from forest.sdk import Consumed, Pass, Reply, message_handler, plugin

@plugin(id="message.runtime", version="0.1.0")
class MessageRuntime:
    @message_handler("ordinary", priority=10)
    async def ordinary(self, event, ctx):
        if event["mode"] == "reply":
            return Reply([[{"type": "text", "text": event["text"]}]])
        if event["mode"] == "consumed":
            return Consumed()
        if event["mode"] == "pass":
            return Pass()
        return None
''',
        encoding="utf-8",
    )
    return root


def identity() -> WorkerIdentity:
    return WorkerIdentity(str(uuid4()), str(uuid4()), str(uuid4()), "bot-test")


async def active(tmp_path: Path) -> PythonWorker:
    snapshot = PackageCatalog(tmp_path / "catalog").build(source(tmp_path))
    worker = PythonWorker(snapshot, identity())
    try:
        await worker.launch()
        prepared = await worker.prepare({})
        assert prepared["message_handlers"] == ["ordinary"]
        await worker.start()
        await worker.activate()
        return worker
    except BaseException:
        await worker.terminate()
        raise


async def test_worker_invokes_only_typed_message_handler_results(tmp_path: Path):
    worker = await active(tmp_path)
    try:
        reply = await worker.invoke_message_handler(
            "ordinary",
            {"mode": "reply", "text": "你好"},
            call_id="handler-reply",
            operation_id="handler-operation-reply",
        )
        consumed = await worker.invoke_message_handler(
            "ordinary",
            {"mode": "consumed", "text": ""},
            call_id="handler-consumed",
            operation_id="handler-operation-consumed",
        )
        passed = await worker.invoke_message_handler(
            "ordinary",
            {"mode": "pass", "text": ""},
            call_id="handler-pass",
            operation_id="handler-operation-pass",
        )
        assert reply == {"kind": "REPLY", "parts": [[{"type": "text", "text": "你好"}]]}
        assert consumed == {"kind": "CONSUMED"}
        assert passed == {"kind": "PASS"}

        with pytest.raises(ForestError) as invalid:
            await worker.invoke_message_handler(
                "ordinary",
                {"mode": "invalid", "text": ""},
                call_id="handler-invalid",
                operation_id="handler-operation-invalid",
            )
        assert invalid.value.code == "INVALID_MESSAGE_HANDLER_RESULT"

        with pytest.raises(ForestError) as missing:
            await worker.invoke_message_handler(
                "missing",
                {"mode": "pass", "text": ""},
                call_id="handler-missing",
                operation_id="handler-operation-missing",
            )
        assert missing.value.code == "MESSAGE_HANDLER_NOT_FOUND"
    finally:
        await worker.stop()


async def test_worker_reaps_candidate_when_handler_manifest_differs(tmp_path: Path):
    first = PackageCatalog(tmp_path / "first-catalog").build(source(tmp_path))
    explicit = tmp_path / "explicit"
    shutil.copytree(first.root, explicit)
    (explicit / "checksums.json").unlink()
    manifest = explicit / "module.toml"
    text = manifest.read_text(encoding="utf-8")
    assert "priority = 10" in text
    manifest.write_text(text.replace("priority = 10", "priority = 11"), encoding="utf-8")
    snapshot = PackageCatalog(tmp_path / "second-catalog").build(explicit)
    worker = PythonWorker(snapshot, identity())
    await worker.launch()
    with pytest.raises(ForestError) as caught:
        await worker.prepare({})
    assert caught.value.code == "MESSAGE_HANDLER_MANIFEST_MISMATCH"
    assert not worker.alive
