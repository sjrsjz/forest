import json
import sys
from argparse import Namespace

import pytest

from forest.cli import main, run_plugin_command
from forest.errors import ForestError
from forest.packages import PackageCatalog
from forest.plugin_scaffold import initialize_plugin


async def test_cli_init_creates_a_statically_valid_worker_package(tmp_path, capsys):
    target = tmp_path / "plugins" / "greeter"
    await run_plugin_command(
        Namespace(
            command="plugin",
            plugin_command="init",
            target=target,
            package_id="example.greeter",
            version="1.2.3",
            module=None,
        )
    )
    result = json.loads(capsys.readouterr().out)
    assert result["status"] == "initialized"
    assert result["module"] == "greeter"
    assert result["service"] == "example.greeter.echo"
    assert (target / "pyproject.toml").is_file()
    snapshot = PackageCatalog(tmp_path / "catalog").build(target)
    assert snapshot.digest == result["validated_digest"]

    await run_plugin_command(
        Namespace(
            command="plugin",
            plugin_command="check",
            source=target,
            cache_dir=tmp_path / "check-catalog",
            config_json="{}",
        )
    )
    assert json.loads(capsys.readouterr().out)["status"] == "checked"


def test_init_never_overwrites_an_existing_destination(tmp_path):
    target = tmp_path / "plugin"
    target.mkdir()
    marker = target / "keep.txt"
    marker.write_text("keep", encoding="utf-8")
    with pytest.raises(ForestError) as caught:
        initialize_plugin(target, "example.safe", "0.1.0", None)
    assert caught.value.code == "PLUGIN_PATH_EXISTS"
    assert marker.read_text(encoding="utf-8") == "keep"


def test_main_parser_exposes_plugin_init(tmp_path, monkeypatch, capsys):
    target = tmp_path / "custom"
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "forest",
            "plugin",
            "init",
            str(target),
            "--id",
            "example.custom",
            "--module",
            "custom_plugin",
        ],
    )
    main()
    result = json.loads(capsys.readouterr().out)
    assert result["module"] == "custom_plugin"
    assert (target / "src" / "custom_plugin" / "__init__.py").is_file()


@pytest.mark.parametrize(
    "package_id,version,module,code",
    [
        ("bad id", "0.1.0", None, "PLUGIN_ID_INVALID"),
        ("example.valid", "latest", None, "PLUGIN_VERSION_INVALID"),
        ("example.valid", "0.1.0", "class", "PLUGIN_MODULE_INVALID"),
        ("example.valid", "0.1.0", "nested.name", "PLUGIN_MODULE_INVALID"),
    ],
)
def test_init_rejects_invalid_source_identities(tmp_path, package_id, version, module, code):
    with pytest.raises(ForestError) as caught:
        initialize_plugin(tmp_path / "plugin", package_id, version, module)
    assert caught.value.code == code
    assert not (tmp_path / "plugin").exists()
