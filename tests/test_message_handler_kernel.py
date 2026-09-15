"""Kernel admission and generation fences for ordinary-message handlers."""

from pathlib import Path

import pytest
from test_kernel import bot_database as bot_database

from forest.errors import ForestError
from forest.kernel import Kernel
from forest.packages import PackageCatalog

pytestmark = pytest.mark.postgres


def handler_source(tmp_path: Path, version: str, marker: str) -> Path:
    root = tmp_path / ("message-kernel-" + version)
    module = root / "src" / "message_kernel"
    module.mkdir(parents=True)
    (root / "pyproject.toml").write_text(
        f'[project]\nname="message-kernel"\nversion="{version}"\n'
        '[tool.forest]\nmodule-root="src"\n',
        encoding="utf-8",
    )
    (module / "__init__.py").write_text(
        f'''from forest.sdk import Reply, message_handler, plugin

@plugin(id="message.kernel", version="{version}")
class MessageKernel:
    @message_handler("ordinary", priority=15)
    async def ordinary(self, event, ctx):
        return Reply([[{{"type": "text", "text": "{marker}:" + event["text"]}}]])
''',
        encoding="utf-8",
    )
    return root


def claimed(kernel: Kernel, item_id: str) -> dict:
    component = kernel.components["message.kernel"]
    definition = component.message_handlers["ordinary"]
    return {
        "chain_id": "message-chain:first",
        "item_id": item_id,
        "status": "CLAIMED",
        "component_id": "message.kernel",
        "handler_id": "ordinary",
        "generation_id": component.worker.identity.generation_id,
        "route_epoch": component.route_epoch,
        "handler_spec_digest": definition.spec_digest,
    }


async def test_kernel_invokes_frozen_handler_and_rejects_old_route(bot_database, tmp_path):
    store, _ = bot_database
    catalog = PackageCatalog(tmp_path / "catalog")
    kernel = Kernel(store)
    try:
        await kernel.start()
        await kernel.install(catalog.build(handler_source(tmp_path, "0.1.0", "v1")))
        request = claimed(kernel, "message-item:first")
        receipt = await kernel.invoke_message_handler_with_receipt(
            request,
            {"text": "payload"},
            operation_id="message-handler-call:first",
        )
        assert receipt.value == {
            "kind": "REPLY",
            "parts": [[{"type": "text", "text": "v1:payload"}]],
        }
        replay = await kernel.invoke_message_handler_with_receipt(
            request,
            {"text": "payload"},
            operation_id="message-handler-call:first",
        )
        assert replay == receipt

        await kernel.install(catalog.build(handler_source(tmp_path, "0.2.0", "v2")))
        with pytest.raises(ForestError) as stale:
            await kernel.invoke_message_handler_with_receipt(
                request,
                {"text": "payload"},
                operation_id="message-handler-call:stale",
            )
        assert stale.value.code == "MESSAGE_HANDLER_FENCED"
    finally:
        await kernel.close()
