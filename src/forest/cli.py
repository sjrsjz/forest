"""Production CLI for the clean Bot host and offline plugin artifacts."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import tempfile
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
    return parser


def main() -> None:
    parser = _parser()
    args = parser.parse_args()
    try:
        if sys.platform == "win32":
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        if args.command == "bot":
            from forest.clean_bot_host import CleanBotSettings, run_clean_bot

            asyncio.run(run_clean_bot(CleanBotSettings.from_environment()))
            return
        if args.command == "supervisor":
            from forest.clean_supervisor_host import (
                CleanSupervisorSettings,
                run_clean_supervisor,
            )

            asyncio.run(run_clean_supervisor(CleanSupervisorSettings.from_environment()))
            return
        asyncio.run(run_plugin_command(args))
    except ForestError as exc:
        parser.exit(1, f"{exc.code}: {exc.message}\n")
    except KeyboardInterrupt:
        parser.exit(130, "Forest stopped.\n")


async def run_plugin_command(args: argparse.Namespace) -> None:
    from jsonschema import Draft202012Validator

    from forest.drivers.python_worker import PythonWorker, WorkerIdentity, WorkerLimits
    from forest.packages import PackageCatalog

    if args.plugin_command == "init":
        from forest.plugin_scaffold import initialize_plugin

        result = await asyncio.to_thread(
            initialize_plugin, args.target, args.package_id, args.version, args.module
        )
        print(json.dumps(result, ensure_ascii=False))
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
        identity = WorkerIdentity(str(uuid4()), str(uuid4()), str(uuid4()), "package-check")
        worker = PythonWorker(
            snapshot, identity, limits=WorkerLimits(**snapshot.component.get("resources", {}))
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
                "artifact": str(snapshot.root),
            },
            ensure_ascii=False,
        )
    )


async def run_artifact_command(args: argparse.Namespace) -> None:
    from forest.environments import create_dependency_lock
    from forest.packages import MAX_ARCHIVE_BYTES, PackageCatalog

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
                "artifact": str(snapshot.root),
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    main()
