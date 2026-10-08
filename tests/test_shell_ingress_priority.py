from types import SimpleNamespace

import pytest

from forest.builtin.shell_command.python.shell_command import ShellCommand


@pytest.mark.parametrize("trailing", [
    {"type": "mention", "external_user_id": "10000"},
    {"type": "image", "media_ref": "media/1"},
])
async def test_reserved_shell_prefix_with_nontext_tail_is_rejected_by_shell(trailing):
    async def no_directory_lookup():
        pytest.fail("Malformed Shell input must not resolve or execute a command")

    context = SimpleNamespace(
        identity={"generation_id": "shell/generation"},
        shell_commands=SimpleNamespace(list=no_directory_lookup),
    )
    ingress = await ShellCommand().inspect({
        "normalized": {
            "self_id": "10000",
            "segments": [
                {"type": "text", "text": "#forest core status"},
                trailing,
            ],
        },
        "event_ref": "event/1",
    }, context)

    assert ingress["command_id"] == "shell.invalid"
    assert ingress["arguments"] == {"error_code": "SHELL_SYNTAX_INVALID"}
    assert ingress["protocol"] == "forest.shell-ingress/2"


async def test_incomplete_shell_prefix_is_not_claimed():
    context = SimpleNamespace(identity={"generation_id": "shell/generation"})
    ingress = await ShellCommand().inspect({
        "normalized": {
            "self_id": "10000",
            "segments": [
                {"type": "text", "text": "#forestX core status"},
                {"type": "mention", "external_user_id": "10000"},
            ],
        },
        "event_ref": "event/2",
    }, context)
    assert ingress is None
