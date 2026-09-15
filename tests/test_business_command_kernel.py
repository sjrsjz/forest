"""Kernel admission and route fencing for business-command calls."""

from pathlib import Path

import pytest
from test_kernel import bot_database as bot_database

from forest.errors import ForestError
from forest.kernel import Kernel
from forest.packages import PackageCatalog
from forest.storage.command_registry import CommandRegistryStore

pytestmark = pytest.mark.postgres


def command_source(tmp_path: Path, version: str, marker: str) -> Path:
    root = tmp_path / ("command-kernel-" + version)
    module = root / "src" / "command_kernel"
    module.mkdir(parents=True)
    (root / "pyproject.toml").write_text(
        f'[project]\nname="command-kernel"\nversion="{version}"\n'
        '[tool.forest]\nmodule-root="src"\n',
        encoding="utf-8",
    )
    (module / "__init__.py").write_text(
        f'''from forest.sdk import Reply, command, plugin

@plugin(id="command.kernel", version="{version}")
class CommandKernel:
    @command("/kernel", "command.kernel", id="kernel", args="tail_text")
    async def kernel(self, event, ctx):
        return Reply([[{{"type": "text", "text": "{marker}:" + event["argument_text"]}}]])
''',
        encoding="utf-8",
    )
    return root


def claimed(head, request_id):
    return {
        **head,
        "request_id": request_id,
        "status": "CLAIMED",
        "command_name": head["name"],
        "command_spec_digest": head["spec_digest"],
    }


async def test_kernel_invokes_frozen_command_and_rejects_old_route(
    bot_database, tmp_path
):
    store, _ = bot_database
    catalog = PackageCatalog(tmp_path / "catalog")
    kernel = Kernel(store)
    try:
        await kernel.start()
        await kernel.install(catalog.build(command_source(tmp_path, "0.1.0", "v1")))
        first = (await CommandRegistryStore(store).snapshot())["commands"][0]
        request = claimed(first, "business-command:first")
        event = {"command_id": "kernel", "argument_text": "payload"}
        receipt = await kernel.invoke_business_command_with_receipt(
            request,
            event,
            operation_id="business-command-call:first",
        )
        assert receipt.value == {
            "kind": "REPLY",
            "parts": [[{"type": "text", "text": "v1:payload"}]],
        }
        replay = await kernel.invoke_business_command_with_receipt(
            request,
            event,
            operation_id="business-command-call:first",
        )
        assert replay == receipt

        await kernel.install(catalog.build(command_source(tmp_path, "0.2.0", "v2")))
        with pytest.raises(ForestError) as stale:
            await kernel.invoke_business_command_with_receipt(
                request,
                event,
                operation_id="business-command-call:stale-route",
            )
        assert stale.value.code == "BUSINESS_COMMAND_FENCED"
    finally:
        await kernel.close()
