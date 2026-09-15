from pathlib import Path

import pytest

from forest.errors import ForestError
from forest.packages import PackageCatalog
from forest.sdk import Plugin, declared_message_handlers, message_handler


def test_message_handler_declaration_is_stable_and_bounded():
    class Handlers(Plugin):
        @message_handler("greeting", priority=20)
        async def greeting(self, event, ctx):
            return event, ctx

    assert declared_message_handlers(Handlers())["greeting"][0] == {
        "handler_id": "greeting",
        "handler": "greeting",
        "priority": 20,
    }
    with pytest.raises(ValueError):
        message_handler("bad handler")
    with pytest.raises(ValueError):
        message_handler("valid", priority=True)
    with pytest.raises(ValueError):
        message_handler("valid", priority=101)


def test_message_handler_requires_exact_event_context_signature():
    class Invalid(Plugin):
        @message_handler("bad")
        async def bad(self, payload, ctx):
            return payload, ctx

    with pytest.raises(ForestError) as caught:
        declared_message_handlers(Invalid())
    assert caught.value.code == "MESSAGE_HANDLER_DECLARATION_INVALID"


def test_message_handler_only_package_is_statically_generated(tmp_path: Path):
    root = tmp_path / "source"
    module = root / "src" / "message_plugin"
    module.mkdir(parents=True)
    (root / "pyproject.toml").write_text(
        '[project]\nname="message-plugin"\nversion="0.1.0"\n'
        '[tool.forest]\nmodule-root="src"\n',
        encoding="utf-8",
    )
    (module / "__init__.py").write_text(
        '''from forest.sdk import message_handler, plugin

@plugin(id="message.plugin", version="0.1.0")
class MessagePlugin:
    @message_handler("late", priority=-5)
    async def late(self, event, ctx):
        return None

    @message_handler("early", priority=25)
    async def early(self, event, ctx):
        return None
''',
        encoding="utf-8",
    )
    snapshot = PackageCatalog(tmp_path / "catalog").build(root)
    assert snapshot.component["provides"] == []
    assert snapshot.component["message_handlers"] == [
        {"handler_id": "early", "handler": "early", "priority": 25},
        {"handler_id": "late", "handler": "late", "priority": -5},
    ]
    snapshot.verify()
