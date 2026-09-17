from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from forest.component_workers import ComponentWorkerHost
from forest.contract_router import ContractExecutionRouter
from forest.errors import ForestError
from forest.group_scheduler import MessageHandlerExecution


class Kernel:
    def __init__(self):
        self.components = {}
        self.installs = []
        self.receipt = SimpleNamespace(
            value={"handled": True},
            result_payload_id="payload-result",
            result_digest="sha256:" + "1" * 64,
        )

    def _authority(self):
        return object()

    async def install(self, package, **kwargs):
        self.installs.append((package, kwargs))
        self.components[package] = SimpleNamespace(
            worker=SimpleNamespace(alive=True, state="ACTIVE")
        )

    async def invoke_message_handler_with_receipt(self, *args, **kwargs):
        self.invocation = (args, kwargs)
        return self.receipt


@pytest.mark.asyncio
async def test_component_worker_host_serializes_duplicate_install_and_owns_errors():
    kernel = Kernel()
    host = ComponentWorkerHost(kernel, {"handler": "handler"})
    await host.ensure(("handler", "handler"))
    assert len(kernel.installs) == 1
    assert host.errors == {}
    with pytest.raises(ForestError) as caught:
        await host.ensure(("missing",))
    assert caught.value.code == "COMPONENT_PACKAGE_NOT_FOUND"
    await host.close()
    with pytest.raises(ForestError) as caught:
        await host.ensure(("handler",))
    assert caught.value.code == "COMPONENT_WORKER_HOST_CLOSED"


@pytest.mark.asyncio
async def test_contract_router_executes_frozen_message_handler_through_worker_host():
    kernel = Kernel()
    host = SimpleNamespace(ensure=AsyncMock())
    adapter = SimpleNamespace(
        connection=lambda _binding_id: SimpleNamespace(row={"binding_epoch": 7})
    )
    router = ContractExecutionRouter(
        SimpleNamespace(kernel=kernel),
        host,
        adapter,
        SimpleNamespace(runner=SimpleNamespace(advance=AsyncMock())),
    )
    actor = SimpleNamespace(binding_id="binding", binding_epoch=7)
    selected = {
        "status": "MESSAGE_HANDLER",
        "request": {"component_id": "handler"},
        "attempt": {"call_operation_id": "call-1"},
        "event": {"event_id": "event-1"},
        "input_parent_ids": ["payload-input"],
    }
    result = await router.execute(actor, selected, {})
    assert result == MessageHandlerExecution(
        "call-1", {"handled": True}, "payload-result", "sha256:" + "1" * 64
    )
    host.ensure.assert_awaited_once_with(("handler",))
    assert kernel.invocation[1] == {
        "operation_id": "call-1",
        "input_parent_ids": ("payload-input",),
    }


@pytest.mark.asyncio
async def test_contract_router_delegates_agent_mode_without_starting_component_worker():
    kernel = Kernel()
    host = SimpleNamespace(ensure=AsyncMock())
    bindings = SimpleNamespace(
        execution_config=AsyncMock(
            return_value={
                "config": {"response_mode": "agent"},
                "config_payload_id": "payload-config",
            }
        )
    )
    adapter = SimpleNamespace(
        connection=lambda _binding_id: SimpleNamespace(row={"binding_epoch": 7}),
        bindings=bindings,
    )
    advance = AsyncMock(return_value="agent-disposition")
    router = ContractExecutionRouter(
        SimpleNamespace(kernel=kernel),
        host,
        adapter,
        SimpleNamespace(runner=SimpleNamespace(advance=advance)),
    )
    actor = SimpleNamespace(binding_id="binding", binding_epoch=7)
    selected = {"status": "AGENT", "agent_phase": "MODEL"}
    context = {"units": []}
    assert await router.execute(actor, selected, context) == "agent-disposition"
    advance.assert_awaited_once_with(actor, selected, context)
    host.ensure.assert_not_awaited()
