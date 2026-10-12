"""Production CLI for the clean Bot host and offline plugin artifacts."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import sys
import tempfile
import tomllib
from pathlib import Path
from uuid import uuid4

from forest import __version__
from forest.errors import ForestError


def _reject_constant(value: str):
    raise ValueError("JSON numbers must be finite")


def _write_atomic(path: Path, content: bytes) -> None:
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(
            dir=path.parent, prefix=".forest-output-", delete=False
        ) as stream:
            temporary = Path(stream.name)
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    except OSError:
        raise ForestError(
            "PACKAGE_IO_FAILED", "Unable to write the requested package output", 422
        ) from None
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="forest")
    parser.add_argument("--version", action="version", version=__version__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("bot", help="启动一个干净的微内核 Bot RuntimeGraph")
    sub.add_parser("supervisor", help="启动部署级多 Bot 供应与运行协调器")

    plugin = sub.add_parser("plugin", help="构建或验证 Python 模块制品")
    actions = plugin.add_subparsers(dest="plugin_command", required=True)
    initialize = actions.add_parser("init", help="创建自声明 Python 模块")
    initialize.add_argument("target", type=Path)
    initialize.add_argument("--id", required=True, dest="package_id")
    initialize.add_argument("--version", default="0.1.0")
    initialize.add_argument("--module")
    for name in ("build", "check"):
        command = actions.add_parser(name)
        command.add_argument("source", type=Path)
        command.add_argument("--cache-dir", type=Path, default=Path(".forest/packages"))
        if name == "check":
            command.add_argument("--config-json", default="{}")
    lock = actions.add_parser("lock", help="为包内 wheelhouse 生成依赖锁")
    lock.add_argument("source", type=Path)
    lock.add_argument("--require", action="append", default=[], dest="requirements")
    export = actions.add_parser("export", help="导出按摘要寻址的模块 ZIP")
    export.add_argument("digest")
    export.add_argument("--output", type=Path, required=True)
    export.add_argument("--cache-dir", type=Path, default=Path(".forest/packages"))
    archive = actions.add_parser("import", help="验证并导入模块 ZIP")
    archive.add_argument("archive", type=Path)
    archive.add_argument("--cache-dir", type=Path, default=Path(".forest/packages"))
    for scope in ("bot", "deployment", "host"):
        release = actions.add_parser(
            f"publish-official-{scope}",
            help=f"由宿主操作员发布 {scope} 官方包",
        )
        release.add_argument("archive", type=Path)
        release.add_argument("--catalog-dir", type=Path, required=True)
        release.add_argument("--archive-sha256", required=True)
        release.add_argument("--package-digest", required=True)
        release.add_argument("--manifest-digest", required=True)
        release.add_argument("--package-id", required=True)
        release.add_argument("--component-id", required=True)
    return parser


def main() -> None:
    parser = _parser()
    args = parser.parse_args()
    try:
        if sys.platform == "win32":
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        if args.command == "bot":
            from forest.bot_host import BotSettings, run_bot

            asyncio.run(run_bot(BotSettings.from_environment()))
            return
        if args.command == "supervisor":
            from forest.supervisor_host import (
                SupervisorSettings,
                run_supervisor,
            )

            asyncio.run(run_supervisor(SupervisorSettings.from_environment()))
            return
        asyncio.run(run_plugin_command(args))
    except ForestError as exc:
        parser.exit(1, f"{exc.code}: {exc.message}\n")
    except KeyboardInterrupt:
        parser.exit(130, "Forest stopped.\n")


async def run_plugin_command(args: argparse.Namespace) -> None:
    from jsonschema import Draft202012Validator

    from forest.drivers.python_worker import PythonWorker, WorkerIdentity
    from forest.packages import PackageCatalog, digest_json
    from forest.worker_limits import WorkerLimits

    if args.plugin_command == "init":
        from forest.plugin_scaffold import initialize_plugin

        result = await asyncio.to_thread(
            initialize_plugin, args.target, args.package_id, args.version, args.module
        )
        print(json.dumps(result, ensure_ascii=False))
        return
    if args.plugin_command in {
        "publish-official-bot", "publish-official-deployment", "publish-official-host",
    }:
        await run_official_release_command(args)
        return
    if args.plugin_command in {"lock", "import", "export"}:
        await run_artifact_command(args)
        return
    try:
        raw_config = getattr(args, "config_json", "{}")
        if len(raw_config.encode("utf-8")) > 262144:
            raise ValueError("Too large")
        config = json.loads(raw_config, parse_constant=_reject_constant)
        if not isinstance(config, dict):
            raise ValueError("Expected object")
    except (ValueError, UnicodeError, RecursionError):
        raise ForestError("CONFIG_INVALID", "--config-json must contain JSON", 422) from None
    snapshot = await asyncio.to_thread(PackageCatalog(args.cache_dir).build, args.source)
    building = args.plugin_command == "build"
    if not building and not Draft202012Validator(
        snapshot.schema(snapshot.component["config_schema"])
    ).is_valid(config):
        raise ForestError("CONFIG_INVALID", "Configuration does not match the package schema", 422)
    if args.plugin_command == "check":
        scope_kind = snapshot.component["scope"]
        identity = WorkerIdentity(
            str(uuid4()), str(uuid4()), str(uuid4()),
            "package-check" if scope_kind == "bot" else None,
            scope_kind=scope_kind,
            scope_id="package-check",
        )
        worker = PythonWorker(
            snapshot, identity, limits=WorkerLimits.from_component(snapshot.component)
        )
        timeouts = snapshot.component.get("lifecycle", {})
        try:
            await worker.launch(timeout=timeouts.get("prepare_timeout_s", 10))
            await worker.prepare(config, timeout=timeouts.get("prepare_timeout_s", 10))
            await worker.start(timeout=timeouts.get("start_timeout_s", 10))
            if (await worker.health()).get("healthy") is not True:
                raise ForestError("WORKER_UNHEALTHY", "Candidate failed readiness")
        finally:
            await worker.stop(timeout=timeouts.get("stop_timeout_s", 5))
    print(
        json.dumps(
            {
                "status": "checked" if args.plugin_command == "check" else "built",
                "package_digest": snapshot.digest,
                "manifest_digest": digest_json(snapshot.manifest),
                "package_id": snapshot.manifest["package_id"],
                "component_id": snapshot.component["id"],
                "artifact": str(snapshot.root),
            },
            ensure_ascii=False,
        )
    )


async def run_artifact_command(args: argparse.Namespace) -> None:
    from forest.environments import create_dependency_lock
    from forest.packages import MAX_ARCHIVE_BYTES, PackageCatalog, digest_json

    if args.plugin_command == "lock":
        lock = await asyncio.to_thread(create_dependency_lock, args.source, args.requirements)
        output = args.source / "dependencies.lock.json"
        await asyncio.to_thread(
            _write_atomic,
            output,
            (json.dumps(lock, ensure_ascii=False, indent=2) + "\n").encode("utf-8"),
        )
        print(
            json.dumps(
                {
                    "status": "locked",
                    "lock_file": str(output),
                    "wheels": len(lock["wheels"]),
                    "manifest_declaration": 'dependency_lock = "dependencies.lock.json"',
                },
                ensure_ascii=False,
            )
        )
        return
    catalog = PackageCatalog(args.cache_dir)
    if args.plugin_command == "export":
        content = await asyncio.to_thread(catalog.export_archive, args.digest)
        await asyncio.to_thread(_write_atomic, args.output, content)
        print(
            json.dumps(
                {
                    "status": "exported",
                    "package_digest": args.digest,
                    "archive_sha256": "sha256:" + hashlib.sha256(content).hexdigest(),
                    "archive": str(args.output),
                },
                ensure_ascii=False,
            )
        )
        return

    def read_archive():
        try:
            with args.archive.open("rb") as stream:
                return stream.read(MAX_ARCHIVE_BYTES + 1)
        except OSError:
            raise ForestError(
                "PACKAGE_IO_FAILED", "Unable to read the requested package archive", 422
            ) from None

    content = await asyncio.to_thread(read_archive)
    snapshot = await asyncio.to_thread(catalog.import_archive, content)
    print(
        json.dumps(
            {
                "status": "imported",
                "package_digest": snapshot.digest,
                "manifest_digest": digest_json(snapshot.manifest),
                "archive_sha256": "sha256:" + hashlib.sha256(content).hexdigest(),
                "package_id": snapshot.manifest["package_id"],
                "component_id": snapshot.component["id"],
                "artifact": str(snapshot.root),
            },
            ensure_ascii=False,
        )
    )


async def run_official_release_command(args: argparse.Namespace) -> None:
    """Publish under the local catalog owner's OS authority, never a Bot grant."""
    from forest.packages import (
        MAX_ARCHIVE_BYTES,
        PackageCatalog,
        auto_install_official_sources,
        validate_source_package,
    )

    scope = args.plugin_command.removeprefix("publish-official-")
    if scope not in {"bot", "deployment", "host"}:
        raise ForestError(
            "OFFICIAL_RELEASE_SCOPE_INVALID", "Official release scope is invalid", 422,
        )
    catalog_root = args.catalog_dir
    if not catalog_root.is_absolute() or not catalog_root.is_dir():
        raise ForestError(
            "OFFICIAL_RELEASE_CATALOG_INVALID",
            "Official release requires an existing absolute catalog directory", 422,
        )
    if scope == "bot" and args.component_id == "component.worker-host":
        raise ForestError(
            "OFFICIAL_RELEASE_COMPONENT_UNSUPPORTED",
            "Worker Host cannot be selected by an official Bot package digest", 422,
        )
    source = None
    bundled_runtime = None
    for candidate in auto_install_official_sources(scope):
        try:
            manifest = tomllib.loads(candidate.joinpath("module.toml").read_text(encoding="utf-8"))
        except (OSError, UnicodeError, ValueError):
            raise ForestError(
                "OFFICIAL_RELEASE_SOURCE_INVALID",
                "Bundled official source is invalid", 500,
            ) from None
        if manifest["components"][0]["id"] == args.component_id:
            source = candidate
            bundled_runtime = manifest["runtime"]
            if manifest["package_id"] != args.package_id:
                raise ForestError(
                    "OFFICIAL_RELEASE_IDENTITY_MISMATCH",
                    "Official release differs from the bundled component identity", 422,
                )
            break
    if source is None:
        raise ForestError(
            "OFFICIAL_RELEASE_COMPONENT_UNKNOWN",
            "Official release target is not a bundled component in its scope", 422,
        )
    await asyncio.to_thread(validate_source_package, source)
    try:
        with args.archive.open("rb") as stream:
            content = stream.read(MAX_ARCHIVE_BYTES + 1)
    except OSError:
        raise ForestError(
            "PACKAGE_IO_FAILED", "Unable to read the official package archive", 422,
        ) from None
    catalog = PackageCatalog(catalog_root)
    snapshot = await asyncio.to_thread(
        catalog.publish_official_archive,
        content,
        scope=scope,
        expected_archive_sha256=args.archive_sha256,
        expected_package_digest=args.package_digest,
        expected_manifest_digest=args.manifest_digest,
        expected_package_id=args.package_id,
        expected_component_id=args.component_id,
        expected_runtime=bundled_runtime,
    )
    print(json.dumps({
        "status": f"published-official-{scope}",
        "package_digest": snapshot.digest,
        "manifest_digest": args.manifest_digest,
        "package_id": snapshot.manifest["package_id"],
        "component_id": snapshot.component["id"],
        "catalog": str(catalog.root),
    }, ensure_ascii=False))


if __name__ == "__main__":
    main()
