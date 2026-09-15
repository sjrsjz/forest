"""Native Worker execution for declared business-command handlers."""

from pathlib import Path
from uuid import uuid4

import pytest

from forest.drivers import PythonWorker, WorkerIdentity
from forest.errors import ForestError
from forest.packages import PackageCatalog
from forest.sdk import Reply


def source(tmp_path: Path) -> Path:
    root = tmp_path / "command-runtime-source"
    module = root / "src" / "command_runtime"
    module.mkdir(parents=True)
    (root / "pyproject.toml").write_text(
        '[project]\nname="command-runtime"\nversion="0.1.0"\n'
        '[tool.forest]\nmodule-root="src"\n',
        encoding="utf-8",
    )
    (module / "__init__.py").write_text(
        '''from forest.sdk import Consumed, Pass, Reply, command, plugin

@plugin(id="command.runtime", version="0.1.0")
class CommandRuntime:
    @command("/runtime", "command.runtime", id="runtime", args="tail_text")
    async def runtime(self, event, ctx):
        mode = event["mode"]
        if mode == "reply":
            return Reply([[{"type": "text", "text": event["argument_text"]}]])
        if mode == "consumed":
            return Consumed()
        if mode == "pass":
            return Pass()
        return None
''',
        encoding="utf-8",
    )
    return root


def identity() -> WorkerIdentity:
    return WorkerIdentity(str(uuid4()), str(uuid4()), str(uuid4()), "bot-test")


async def active(tmp_path):
    snapshot = PackageCatalog(tmp_path / "catalog").build(source(tmp_path))
    worker = PythonWorker(snapshot, identity())
    try:
        await worker.launch()
        prepared = await worker.prepare({})
        assert prepared["commands"] == ["runtime"]
        await worker.start()
        await worker.activate()
        return worker
    except BaseException:
        await worker.terminate()
        raise


async def test_worker_invokes_only_typed_command_results(tmp_path):
    worker = await active(tmp_path)
    try:
        base = {"command_id": "runtime", "argument_text": "你好"}
        reply = await worker.invoke_command(
            "runtime", {**base, "mode": "reply"},
            call_id="command-call-reply", operation_id="command-operation-reply",
        )
        consumed = await worker.invoke_command(
            "runtime", {**base, "mode": "consumed"},
            call_id="command-call-consumed", operation_id="command-operation-consumed",
        )
        passed = await worker.invoke_command(
            "runtime", {**base, "mode": "pass"},
            call_id="command-call-pass", operation_id="command-operation-pass",
        )
        assert reply == {
            "kind": "REPLY",
            "parts": [[{"type": "text", "text": "你好"}]],
        }
        assert consumed == {"kind": "CONSUMED"}
        assert passed == {"kind": "PASS"}

        with pytest.raises(ForestError) as invalid:
            await worker.invoke_command(
                "runtime", {**base, "mode": "invalid"},
                call_id="command-call-invalid", operation_id="command-operation-invalid",
            )
        assert invalid.value.code == "INVALID_COMMAND_RESULT"
        with pytest.raises(ForestError) as unknown:
            await worker.invoke_command(
                "missing", base,
                call_id="command-call-missing", operation_id="command-operation-missing",
            )
        assert unknown.value.code == "BUSINESS_COMMAND_NOT_FOUND"
    finally:
        await worker.stop()


def test_reply_is_deeply_immutable_and_bounded():
    source_parts = [[{"type": "text", "text": "immutable"}]]
    reply = Reply(source_parts)
    source_parts[0][0]["text"] = "changed"
    assert reply.parts[0][0]["text"] == "immutable"
    with pytest.raises(TypeError):
        reply.parts[0][0]["text"] = "forbidden"
    with pytest.raises(TypeError):
        Reply([])
