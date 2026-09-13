"""Control grammar keeps unknown arguments out of retained and model data."""

import pytest

from forest.shell_commands import parse_command


def command(text, *, extras=(), self_message=False):
    return parse_command({
        "self_id": "1000", "author": {"is_self": self_message},
        "segments": [{"type": "text", "text": text}, *extras],
    })


@pytest.mark.parametrize(("text", "name", "payload", "priority"), [
    (" #forest", "help", {}, 1),
    ("#forest jobs list", "job.read", {}, 1),
    ("#forest job status job:1", "job.read", {"job_id": "job:1"}, 1),
    ("#forest job run job:1 --expect-revision 2", "job.run",
     {"job_id": "job:1", "expected_revision": 2}, 2),
    ('#forest run cancel run:1 --reason "停止这次回答" --expect-checkpoint 7', "run.cancel",
     {"run_id": "run:1", "expected_checkpoint_seq": 7, "reason": "停止这次回答"}, 0),
    ('#forest job cancel job:1 --expect-revision 2 --scope future --reason "停止计划"', "job.cancel",
     {"job_id": "job:1", "expected_revision": 2, "scope": "future", "reason": "停止计划"}, 0),
    ("#forest run reject approval:1 --digest sha256:abc", "run.approval",
     {"approval_id": "approval:1", "arguments_digest": "sha256:abc", "decision": "REJECT"}, 0),
    ("#forest memory list", "memory.read", {}, 1),
    ("#forest memory forget-plan source-unit:abc --expect-deletion-epoch 4",
     "memory.forget.plan",
     {"source_unit_ids": ["source-unit:abc"], "expected_deletion_epoch": 4}, 1),
    ("#forest memory forget-apply plan:1 --digest sha256:abc --expect-deletion-epoch 4",
     "memory.forget.apply",
     {"plan_id": "plan:1", "plan_digest": "sha256:abc", "expected_deletion_epoch": 4}, 0),
])
def test_typed_commands(text, name, payload, priority):
    parsed = command(text)
    assert (parsed.name, parsed.payload, parsed.priority) == (name, payload, priority)


@pytest.mark.parametrize("text", [
    '#forest config set key SECRET', '#forest job run a --expect-revision -1',
    '#forest job run a --expect-revision 01', '#forest job run a --expect-revision true',
    '#forest job run a --expect-revision 1 --expect-revision 2',
    '#forest run cancel a --expect-checkpoint 1 --reason "unterminated',
    '#forest run cancel a --expect-checkpoint 1 --reason ""',
    '#forest job cancel a --expect-revision 1 --scope unknown --reason test',
    '#forest job run a --expect-revision 1; rm anything', '#forest confirm SECRET',
    '#forest job status ' + 'a' * 129,
    '#forest ' + 'a' * 4096,
])
def test_invalid_reserved_command_discards_arguments(text):
    parsed = command(text)
    assert parsed.name == "unknown" and parsed.payload == {}


def test_quotes_media_echo_and_nonleading_namespace_are_not_executable():
    assert command("quoted #forest status") is None
    assert command("#foresting status") is None
    assert command("#forest status", self_message=True) is None
    assert command("#forest status", extras=[{"type": "image", "media_ref": "x"}]).name == "unknown"
    value = {"self_id": "1000", "author": {"is_self": False}, "segments": [
        {"type": "mention", "external_user_id": "1000"},
        {"type": "text", "text": " #forest status"},
    ]}
    assert parse_command(value).name == "status"
    value["segments"].insert(0, {"type": "quote", "text": "#forest status"})
    assert parse_command(value) is None


def test_new_admissions_are_explicitly_shell_gate_v3():
    assert command("#forest status").gate_version == "shell-gate-v3"
    assert command("#forest memory list").gate_version == "shell-gate-v3"
