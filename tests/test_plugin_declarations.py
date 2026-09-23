import io
import json
import shutil
import zipfile
from argparse import Namespace
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest

from forest.cli import run_plugin_command
from forest.errors import ForestError
from forest.module_validation import validate_component_values
from forest.package_runtime import package_runtime_descriptor
from forest.packages import PackageCatalog, source_fingerprint
from forest.sdk import Plugin, command, declared_commands, declared_services, plugin, service


def auto_source(tmp_path: Path, *, module: str = "demo") -> Path:
    root = tmp_path / "source"
    package = root / "src" / module
    package.mkdir(parents=True)
    (root / "pyproject.toml").write_text(
        '[project]\nname = "demo"\nversion = "0.1.0"\n'
        '[tool.forest]\nmodule-root = "src"\n',
        encoding="utf-8",
    )
    (package / "__init__.py").write_text(
        '''from forest.sdk import plugin, service

@plugin(id="example.auto", version="0.1.0", requests=[])
class Echo:
    @service(
        "echo.call",
        semantics="read",
        contract="1.2",
        input_schema={"type": "object", "required": ["value"]},
        output_schema={"type": "object", "required": ["value"]},
    )
    async def echo(self, payload, ctx):
        return {"value": payload["value"]}
''',
        encoding="utf-8",
    )
    return root


def test_auto_declaration_build_is_static_complete_and_deterministic(tmp_path):
    source = auto_source(tmp_path)
    marker = tmp_path / "imported"
    module = source / "src" / "demo" / "__init__.py"
    module.write_text(
        f"from pathlib import Path\nPath({str(marker)!r}).write_text('bad')\n"
        + module.read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    catalog = PackageCatalog(tmp_path / "catalog")
    first = catalog.build(source)
    second = catalog.build(source)

    assert first.digest == second.digest
    assert first.entrypoint == "demo:Echo"
    assert first.module_root == first.root / "src"
    assert first.component["provides"] == [
        {
            "name": "echo.call",
            "contract": "1.2",
            "semantics": "read",
            "input_schema": "forest_generated/service-0-input.json",
            "output_schema": "forest_generated/service-0-output.json",
        }
    ]
    assert not marker.exists()
    checksums = json.loads((first.root / "checksums.json").read_text(encoding="utf-8"))
    assert checksums["algorithm"] == "sha256"
    assert set(checksums["files"]) == {
        "forest_generated/config.json",
        "forest_generated/service-0-input.json",
        "forest_generated/service-0-output.json",
        "module.toml",
        "pyproject.toml",
        "src/demo/__init__.py",
    }
    first.verify()


async def test_auto_declared_package_passes_real_worker_check(tmp_path, capsys):
    source = auto_source(tmp_path)
    await run_plugin_command(
        Namespace(
            command="plugin",
            plugin_command="check",
            source=source,
            cache_dir=tmp_path / "catalog",
            config_json="{}",
        )
    )
    result = json.loads(capsys.readouterr().out)
    assert result["status"] == "checked"
    PackageCatalog(tmp_path / "catalog").get(result["package_digest"]).verify()


async def test_command_only_auto_package_passes_real_worker_full_contract_check(
    tmp_path, capsys
):
    source = auto_source(tmp_path)
    module = source / "src" / "demo" / "__init__.py"
    module.write_text(
        '''from forest.sdk import command, plugin

@plugin(id="example.commands", version="0.1.0")
class Commands:
    @command(
        "/记住", "command.remember", id="remember",
        args="tail_text", schema={"type": "object", "required": ["argument_text"]},
        priority=25,
    )
    async def remember(self, event, ctx):
        return None
''',
        encoding="utf-8",
    )
    await run_plugin_command(
        Namespace(
            command="plugin",
            plugin_command="check",
            source=source,
            cache_dir=tmp_path / "catalog",
            config_json="{}",
        )
    )
    result = json.loads(capsys.readouterr().out)
    snapshot = PackageCatalog(tmp_path / "catalog").get(result["package_digest"])
    assert snapshot.component["provides"] == []
    assert snapshot.component["commands"] == [
        {
            "command_id": "remember",
            "name": "/记住",
            "handler": "remember",
            "permission": "command.remember",
            "argument_mode": "tail_text",
            "input_schema": "forest_generated/command-0-input.json",
            "priority": 25,
        }
    ]


def test_auto_declaration_emits_compatible_message_handler_takeover(tmp_path):
    source = auto_source(tmp_path)
    module = source / "src" / "demo" / "__init__.py"
    module.write_text(
        '''from forest.sdk import Pass, message_handler, plugin

@plugin(
    id="example.handlers", version="0.1.0",
    message_handler_takeover="compatible",
)
class Handlers:
    @message_handler("ordinary", priority=5)
    async def ordinary(self, event, ctx):
        return Pass()
''',
        encoding="utf-8",
    )
    snapshot = PackageCatalog(tmp_path / "catalog").build(source)
    assert snapshot.component["message_handler_takeover"] == "compatible"
    assert snapshot.component["message_handlers"][0]["handler_id"] == "ordinary"


@pytest.mark.parametrize("mode", ["compatible", "unsafe"])
def test_takeover_declaration_requires_handlers_and_known_mode(tmp_path, mode):
    source = auto_source(tmp_path)
    module = source / "src" / "demo" / "__init__.py"
    module.write_text(
        f'''from forest.sdk import plugin, service

@plugin(
    id="example.handlers", version="0.1.0",
    message_handler_takeover="{mode}",
)
class Handlers:
    @service("echo.call", semantics="read")
    async def echo(self, payload, ctx):
        return payload
''',
        encoding="utf-8",
    )
    with pytest.raises(ForestError) as caught:
        PackageCatalog(tmp_path / "catalog").build(source)
    assert caught.value.code in {"INVALID_MANIFEST", "INVALID_MESSAGE_HANDLERS"}


def test_component_configuration_cannot_enable_undeclared_takeover(tmp_path):
    snapshot = PackageCatalog(tmp_path / "catalog").build(auto_source(tmp_path))
    with pytest.raises(ForestError) as caught:
        validate_component_values(
            {snapshot.digest: snapshot},
            "example.auto",
            {
                "package_digest": snapshot.digest,
                "config": {},
                "grants": [],
                "message_handler_takeover": True,
            },
        )
    assert caught.value.code == "CONFIG_INVALID"


@pytest.mark.parametrize(
    "field,replacement",
    [
        ("command_id", ('command_id = "hello"', 'command_id = "changed"')),
        ("name", ('name = "/hello"', 'name = "/changed"')),
        ("handler", ('handler = "hello"', 'handler = "changed"')),
        ("permission", ('permission = "command.hello"', 'permission = "command.changed"')),
        ("argument_mode", ('argument_mode = "none"', 'argument_mode = "tail_text"')),
        ("priority", ("priority = 0", "priority = 1")),
        ("schema", None),
    ],
)
async def test_command_manifest_runtime_field_mismatch_reaps_candidate(
    tmp_path, field, replacement
):
    source = auto_source(tmp_path)
    module = source / "src" / "demo" / "__init__.py"
    module.write_text(
        '''from forest.sdk import command, plugin
@plugin(id="example.commands", version="0.1.0")
class Commands:
    @command("/hello", "command.hello", schema={"type": "object"})
    async def hello(self, event, ctx):
        return None
''',
        encoding="utf-8",
    )
    first = PackageCatalog(tmp_path / "first-catalog").build(source)
    explicit = tmp_path / "explicit"
    shutil.copytree(first.root, explicit)
    (explicit / "checksums.json").unlink()
    if field == "schema":
        schema = explicit / "forest_generated" / "command-0-input.json"
        schema.chmod(0o600)
        schema.write_text('{"type":"string"}\n', encoding="utf-8")
    else:
        manifest = explicit / "module.toml"
        manifest.chmod(0o600)
        old, new = replacement
        text = manifest.read_text(encoding="utf-8")
        assert old in text
        manifest.write_text(text.replace(old, new), encoding="utf-8")
    snapshot = PackageCatalog(tmp_path / "second-catalog").build(explicit)
    from forest.drivers import PythonWorker, WorkerIdentity

    worker = PythonWorker(
        snapshot,
        WorkerIdentity(uuid4().hex, uuid4().hex, uuid4().hex, "bot-test"),
    )
    await worker.launch()
    with pytest.raises(ForestError) as caught:
        await worker.prepare({})
    assert caught.value.code == "COMMAND_MANIFEST_MISMATCH"
    assert not worker.alive


async def test_event_and_job_declarations_generate_the_existing_worker_contract(tmp_path, capsys):
    source = auto_source(tmp_path)
    module = source / "src" / "demo" / "__init__.py"
    module.write_text(
        '''from forest.sdk import job, plugin, service, subscribe

@plugin(
    id="example.auto", version="0.1.0",
    requests=["event.publish:event.ready", "job.schedule:work.refresh"],
    publishes=[{
        "topic": "event.ready", "contract": "1.0",
        "schema": {"type": "object", "required": ["value"]},
    }],
)
class Worker:
    @service("echo.call", semantics="read")
    async def echo(self, payload, ctx):
        return payload

    @subscribe(
        "projection.ready", "event.ready", "1.0",
        schema={"type": "object"}, max_attempts=4,
    )
    async def project(self, event, ctx):
        return None

    @job(
        "work.refresh", input_schema={"type": "object"},
        max_attempts=3, timeout_s=2.5, max_concurrency=2,
    )
    async def refresh(self, task, ctx):
        return None
''',
        encoding="utf-8",
    )
    await run_plugin_command(
        Namespace(
            command="plugin",
            plugin_command="check",
            source=source,
            cache_dir=tmp_path / "catalog",
            config_json="{}",
        )
    )
    result = json.loads(capsys.readouterr().out)
    snapshot = PackageCatalog(tmp_path / "catalog").get(result["package_digest"])
    assert snapshot.component["publishes"][0]["schema"] == (
        "forest_generated/publication-0.json"
    )
    assert snapshot.component["subscribes"][0]["max_attempts"] == 4
    assert snapshot.component["jobs"][0] == {
        "handler_id": "work.refresh",
        "input_schema": "forest_generated/job-0-input.json",
        "max_attempts": 3,
        "timeout_s": 2.5,
        "max_concurrency": 2,
    }
    descriptor = package_runtime_descriptor(
        snapshot,
        bot_id="bot-routing-contract",
        module_configuration=object(),
        grants=SimpleNamespace(entries=()),
        resolved_contracts={},
        startup_components=(),
        generation_verifier=object(),
        generation_member_verifier=object(),
        state_queries=None,
        state_commands=None,
        service_operation_ledger=object(),
    )
    subscription = descriptor.event_subscriptions[0]
    publication = descriptor.event_publications[0]
    assert (
        publication.topic,
        publication.contract,
    ) == (
        "event.ready",
        "1.0",
    )
    assert publication.event_schema_digest.startswith("sha256:")
    assert (
        subscription.subscriber_id,
        subscription.topic,
        subscription.contract,
        subscription.max_attempts,
    ) == ("projection.ready", "event.ready", "1.0", 4)
    component_job = descriptor.component_jobs[0]
    assert (
        component_job.handler_id,
        component_job.max_attempts,
        component_job.timeout_seconds,
        component_job.max_concurrency,
    ) == ("work.refresh", 3, 2.5, 2)
    assert subscription.service_id != component_job.service_id


def test_dynamic_declaration_is_rejected_with_source_location(tmp_path):
    source = auto_source(tmp_path)
    module = source / "src" / "demo" / "__init__.py"
    module.write_text(
        '''from forest.sdk import plugin, service
VERSION = "0.1.0"
@plugin(id="example.auto", version=VERSION)
class Echo:
    @service("echo.call", semantics="read")
    async def echo(self, payload, ctx):
        return payload
''',
        encoding="utf-8",
    )
    with pytest.raises(ForestError) as caught:
        PackageCatalog(tmp_path / "catalog").build(source)
    assert caught.value.code == "PLUGIN_DECLARATION_INVALID"
    assert caught.value.details == {
        "phase": "declaration",
        "file": "src/demo/__init__.py",
        "line": 3,
        "column": 36,
    }


def test_dynamic_command_argument_is_rejected_with_source_location(tmp_path):
    source = auto_source(tmp_path)
    module = source / "src" / "demo" / "__init__.py"
    module.write_text(
        '''from forest.sdk import command, plugin
PERMISSION = "command.hello"
@plugin(id="example.auto", version="0.1.0")
class Commands:
    @command("/hello", PERMISSION)
    async def hello(self, event, ctx):
        return None
''',
        encoding="utf-8",
    )
    with pytest.raises(ForestError) as caught:
        PackageCatalog(tmp_path / "catalog").build(source)
    assert caught.value.code == "PLUGIN_DECLARATION_INVALID"
    assert caught.value.details == {
        "phase": "declaration",
        "file": "src/demo/__init__.py",
        "line": 5,
        "column": 24,
    }


def test_static_command_declaration_follows_sdk_module_alias(tmp_path):
    source = auto_source(tmp_path)
    module = source / "src" / "demo" / "__init__.py"
    module.write_text(
        '''import forest.sdk as sdk
@sdk.plugin(id="example.alias-command", version="0.1.0")
class Commands:
    @sdk.command("/hello", "command.hello")
    async def hello(self, event, ctx):
        return None
''',
        encoding="utf-8",
    )
    snapshot = PackageCatalog(tmp_path / "catalog").build(source)
    assert snapshot.component["commands"][0]["command_id"] == "hello"


@pytest.mark.parametrize(
    "imports,plugin_name,service_name",
    [
        ("from forest.sdk import plugin as declare, service as endpoint", "declare", "endpoint"),
        ("import forest.sdk as sdk", "sdk.plugin", "sdk.service"),
        ("import forest.sdk", "forest.sdk.plugin", "forest.sdk.service"),
        ("from forest import sdk", "sdk.plugin", "sdk.service"),
    ],
)
def test_static_declarations_follow_forest_sdk_import_aliases(
    tmp_path, imports, plugin_name, service_name
):
    source = auto_source(tmp_path)
    module = source / "src" / "demo" / "__init__.py"
    module.write_text(
        f'''{imports}
@{plugin_name}(id="example.auto", version="0.1.0")
class Echo:
    @{service_name}("echo.call", semantics="read")
    async def echo(self, payload, ctx):
        return payload
''',
        encoding="utf-8",
    )
    snapshot = PackageCatalog(tmp_path / "catalog").build(source)
    assert snapshot.entrypoint == "demo:Echo"


def test_unrelated_same_named_decorator_is_not_treated_as_sdk_declaration(tmp_path):
    source = auto_source(tmp_path)
    module = source / "src" / "demo" / "__init__.py"
    module.write_text(
        '''def plugin(**metadata):
    return lambda candidate: candidate
@plugin(id="not.forest", version="0.1.0")
class Unrelated:
    pass
''',
        encoding="utf-8",
    )
    with pytest.raises(ForestError) as caught:
        PackageCatalog(tmp_path / "catalog").build(source)
    assert caught.value.code == "PLUGIN_DECLARATION_INVALID"


def test_declared_checksums_reject_modified_archive_before_publication(tmp_path):
    source = auto_source(tmp_path)
    catalog = PackageCatalog(tmp_path / "source-catalog")
    archive = catalog.export_archive(catalog.build(source).digest)
    source_zip = zipfile.ZipFile(io.BytesIO(archive))
    output = io.BytesIO()
    with source_zip, zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as target:
        for info in source_zip.infolist():
            value = source_zip.read(info.filename)
            if info.filename == "src/demo/__init__.py":
                value += b"\n# modified in transit\n"
            target.writestr(info, value)
    with pytest.raises(ForestError) as caught:
        PackageCatalog(tmp_path / "destination").import_archive(output.getvalue())
    assert caught.value.code == "PACKAGE_CHECKSUM_MISMATCH"


def test_generated_artifact_namespace_cannot_shadow_source_files(tmp_path):
    source = auto_source(tmp_path)
    reserved = source / "forest_generated"
    reserved.mkdir()
    (reserved / "config.json").write_text("{}", encoding="utf-8")
    with pytest.raises(ForestError) as caught:
        PackageCatalog(tmp_path / "catalog").build(source)
    assert caught.value.code == "PLUGIN_DECLARATION_INVALID"


def test_development_version_refreshes_generated_checksums(tmp_path):
    source = auto_source(tmp_path)
    snapshot = PackageCatalog(tmp_path / "catalog").build(source, development=True)
    assert snapshot.manifest["version"] == (
        "0.1.0+dev." + source_fingerprint(source).removeprefix("sha256:")
    )
    snapshot.verify()


def test_runtime_plugin_decorator_adapts_plain_class_without_changing_handlers():
    @plugin(id="example.runtime", version="0.1.0")
    class Plain:
        @service(
            "echo.call", semantics="read", contract="1.2",
            input_schema={}, output_schema={}
        )
        async def echo(self, payload, ctx):
            return payload, ctx

    instance = Plain()
    assert isinstance(instance, Plugin)
    assert set(declared_services(instance)) == {"echo.call"}
    assert Plain.__forest_plugin__["id"] == "example.runtime"


def test_runtime_plugin_decorator_preserves_command_declaration():
    @plugin(id="example.command-runtime", version="0.1.0")
    class Plain:
        @command("/hello", "command.hello")
        async def hello(self, event, ctx):
            return event, ctx

    instance = Plain()
    assert set(declared_commands(instance)) == {"hello"}
