import pytest

from forest.command_contract import command_identifier
from forest.errors import ForestError
from forest.sdk import Plugin, command, declared_commands


def test_command_declaration_normalizes_name_and_derives_stable_localized_id():
    class Commands(Plugin):
        @command(
            "/re\N{COMBINING ACUTE ACCENT}sum\N{LATIN SMALL LETTER E WITH ACUTE}",
            "command.resume",
        )
        async def resume(self, event, ctx):
            return event, ctx

    declared = declared_commands(Commands())
    normalized = "/r\N{LATIN SMALL LETTER E WITH ACUTE}sum\N{LATIN SMALL LETTER E WITH ACUTE}"
    command_id = command_identifier(normalized)
    assert list(declared) == [command_id]
    assert declared[command_id][0] == {
        "command_id": command_id,
        "name": normalized,
        "handler": "resume",
        "permission": "command.resume",
        "argument_mode": "none",
        "schema": None,
        "priority": 0,
    }


@pytest.mark.parametrize(
    "kwargs,error",
    [
        ({"name": "hello", "permission": "command.hello"}, ValueError),
        ({"name": "/hello world", "permission": "command.hello"}, ValueError),
        ({"name": "/hello\u202e", "permission": "command.hello"}, ValueError),
        ({"name": "/hello", "permission": "message.read"}, ValueError),
        ({"name": "/hello", "permission": "command.hello", "args": "json"}, ValueError),
        ({"name": "/hello", "permission": "command.hello", "priority": True}, ValueError),
        ({"name": "/hello", "permission": "command.hello", "priority": 101}, ValueError),
    ],
)
def test_command_decorator_rejects_ambiguous_or_unbounded_contract(kwargs, error):
    with pytest.raises(error):
        command(**kwargs)


def test_declared_commands_require_exact_event_context_signature():
    class Invalid(Plugin):
        @command("/hello", "command.hello")
        async def hello(self, payload, ctx):
            return payload

    with pytest.raises(ForestError) as caught:
        declared_commands(Invalid())
    assert caught.value.code == "COMMAND_DECLARATION_INVALID"


def test_declared_commands_reject_duplicate_normalized_names():
    class Invalid(Plugin):
        @command("/hello", "command.one", id="one")
        async def one(self, event, ctx):
            return event

        @command("/hello", "command.two", id="two")
        async def two(self, event, ctx):
            return event

    with pytest.raises(ForestError) as caught:
        declared_commands(Invalid())
    assert caught.value.code == "COMMAND_DECLARATION_INVALID"
