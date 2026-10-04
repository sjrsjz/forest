from types import SimpleNamespace

import pytest

from forest.builtin.conversation_scheduler.python.conversation_scheduler import (
    ConversationScheduler,
)


@pytest.mark.asyncio
async def test_pre_run_compaction_reclaims_in_segments_without_replaying_old_claim():
    scheduler = ConversationScheduler()
    scheduler._context_view = {"policy_revision": "test-policy"}
    operation_ids = []
    responses = ["COMPACTED"] * 8 + ["COMPACTED", "SKIPPED"]

    async def call(port, payload, *, operation_id):
        assert port == "conversation.compact-before-run"
        assert payload["anchor"]["actor"]["epoch"] in (7, 8)
        operation_ids.append(operation_id)
        return {
            "protocol": "forest.context-pre-run-compaction/1",
            "conversation_id": "conversation/test",
            "status": responses.pop(0),
        }

    ctx = SimpleNamespace(
        uow=SimpleNamespace(call=call),
        operations=SimpleNamespace(stable=lambda value: value),
    )
    work = {"conversation_id": "conversation/test", "run_ref": "run/test",
            "actor": {"epoch": 7}}
    assert await scheduler._compact_before_run(work, ctx) is False
    assert len(set(operation_ids)) == 8
    work["actor"] = {"epoch": 8}
    assert await scheduler._compact_before_run(work, ctx) is True
    assert len(set(operation_ids)) == 10
    assert operation_ids[-2:] == [
        "compact-before-run:run/test:8:0", "compact-before-run:run/test:8:1",
    ]
