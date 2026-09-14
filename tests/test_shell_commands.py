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
    ('#forest bot pause --reason "上游维护" --expect-maintenance 7', "bot.pause",
     {"expected_maintenance_epoch": 7, "data_admission": False, "reason": "上游维护"}, 0),
    ('#forest bot resume --expect-maintenance 8 --reason "上游恢复"', "bot.resume",
     {"expected_maintenance_epoch": 8, "data_admission": True, "reason": "上游恢复"}, 1),
    ("#forest events list", "event.read", {}, 1),
    ("#forest event replay delivery:1 --expect-revision 3", "event.replay",
     {"delivery_id": "delivery:1", "expected_revision": 3}, 2),
    ("#forest event retire-plan subscriber:1 --contract-major 2 --expect-revision 4",
     "event.retire.plan",
     {"subscriber_id": "subscriber:1", "contract_major": 2, "expected_revision": 4}, 1),
    ("#forest event retire-apply plan:1 --digest sha256:abc --expect-revision 4",
     "event.retire.apply",
     {"plan_id": "plan:1", "plan_digest": "sha256:abc", "expected_revision": 4}, 1),
    ("#forest component-jobs list", "component_job.read", {}, 1),
    ("#forest component-job replay job:private --expect-revision 5",
     "component_job.replay", {"job_id": "job:private", "expected_revision": 5}, 2),
    ("#forest component-job retire-plan component.a work.persist --expect-revision 6",
     "component_job.retire.plan",
     {"component_id": "component.a", "handler_id": "work.persist", "expected_revision": 6},
     1),
    ("#forest component-job retire-apply plan:2 --digest sha256:def --expect-revision 6",
     "component_job.retire.apply",
     {"plan_id": "plan:2", "plan_digest": "sha256:def", "expected_revision": 6}, 1),
    ("#forest audit list", "audit.read", {"limit": 12}, 1),
    ("#forest logs list --limit 7 --action agent.cancel --before audit:9", "audit.read",
     {"limit": 7, "action": "agent.cancel", "before": "audit:9"}, 1),
    ("#forest context inspect", "context.read", {}, 1),
    ("#forest context compact --expect-view 9", "context.compact",
     {"expected_view_revision": 9}, 1),
    ("#forest modules list", "module.read", {}, 1),
    ("#forest module status forest.weather", "module.read",
     {"component_id": "forest.weather"}, 1),
    ("#forest module reload forest.weather --expect-revision 18", "module.reload",
     {"component_id": "forest.weather", "expected_revision": 18}, 1),
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
    '#forest bot pause --expect-maintenance 1 --reason ""',
    '#forest bot resume --expect-maintenance true --reason restore',
    '#forest event retire-plan subscriber --contract-major 0 --expect-revision 1',
    '#forest event replay delivery --expect-revision 1 --digest secret',
    '#forest component-job retire-plan component --expect-revision 1',
    '#forest component-job replay job --expect-revision -1',
    '#forest audit list --limit 0', '#forest audit list --limit 21',
    '#forest audit list --before "bad cursor"', '#forest audit list --unknown value',
    '#forest context inspect extra', '#forest context compact',
    '#forest context compact --expect-view -1',
    '#forest context compact --expect-view 1 --expect-view 2',
    '#forest module status', '#forest module status bad/name',
    '#forest module reload component',
    '#forest module reload component --expect-revision 0',
    '#forest module reload component --expect-revision 1 --expect-revision 2',
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


def test_new_admissions_are_explicitly_shell_gate_v8():
    assert command("#forest status").gate_version == "shell-gate-v8"
    assert command("#forest memory list").gate_version == "shell-gate-v8"
    assert command("#forest bot pause --expect-maintenance 1 --reason test").gate_version == "shell-gate-v8"
    assert command("#forest component-job list").gate_version == "shell-gate-v8"
    assert command("#forest audit list").gate_version == "shell-gate-v8"
    assert command("#forest context inspect").gate_version == "shell-gate-v8"
    assert command("#forest module status forest.weather").gate_version == "shell-gate-v8"
