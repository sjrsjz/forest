from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import tempfile
from pathlib import Path
from urllib.parse import quote, urlsplit
from uuid import uuid4

from forest import __version__
from forest.bootstrap import BootstrapState
from forest.errors import ForestError


def managed_database() -> str | None:
    filename = os.environ.get("FOREST_DATABASE_PASSWORD_FILE")
    if not filename:
        return None
    password = Path(filename).read_text(encoding="utf-8").strip()
    host = os.environ.get("FOREST_DATABASE_HOST", "postgres")
    return f"postgresql://forest:{quote(password, safe='')}@{host}:5432/forest"


def _reject_constant(value: str):
    raise ValueError("JSON numbers must be finite")


def _write_atomic(path: Path, content: bytes) -> None:
    """Publish complete local author-tool output without partial watched files."""
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


def main() -> None:
    parser = argparse.ArgumentParser(prog="forest")
    parser.add_argument("--version", action="version", version=__version__)
    sub = parser.add_subparsers(dest="command", required=True)
    serve = sub.add_parser("serve", help="启动独立管理服务")
    serve.add_argument(
        "--data-dir", type=Path, default=Path(os.environ.get("FOREST_DATA_DIR", ".forest"))
    )
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8080)
    serve.add_argument(
        "--public-origin", default=os.environ.get("FOREST_PUBLIC_ORIGIN", "http://127.0.0.1:8080")
    )
    pair = sub.add_parser("setup-token", help="通过本机运维通道获取/轮换一次性初始化凭据")
    pair.add_argument(
        "--data-dir", type=Path, default=Path(os.environ.get("FOREST_DATA_DIR", ".forest"))
    )
    plugin = sub.add_parser("plugin", help="构建或验证普通 Python 插件")
    actions = plugin.add_subparsers(dest="plugin_command", required=True)
    for name in ("build", "check"):
        command = actions.add_parser(name)
        command.add_argument("source", type=Path)
        command.add_argument("--cache-dir", type=Path, default=Path(".forest/packages"))
        if name == "check":
            command.add_argument("--config-json", default="{}")
    lock = actions.add_parser("lock", help="对包内 wheelhouse 的完整依赖闭包计算锁文件")
    lock.add_argument("source", type=Path)
    lock.add_argument("--require", action="append", default=[], dest="requirements")
    export = actions.add_parser("export", help="按摘要导出已校验的不可变插件 ZIP")
    export.add_argument("digest")
    export.add_argument("--output", type=Path, required=True)
    export.add_argument("--cache-dir", type=Path, default=Path(".forest/packages"))
    archive = actions.add_parser("import", help="校验完整插件 ZIP 并导入工件目录")
    archive.add_argument("archive", type=Path)
    archive.add_argument("--cache-dir", type=Path, default=Path(".forest/packages"))
    dev = sub.add_parser("dev", help="在专用开发 Bot 数据库运行插件或持续监视整个开发包")
    dev.add_argument("--bot", required=True)
    source = dev.add_mutually_exclusive_group(required=True)
    source.add_argument("--package", type=Path)
    source.add_argument("--watch", type=Path)
    dev.add_argument("--service")
    dev.add_argument("--input-json", default="{}", help="JSON 值；使用 - 从标准输入读取")
    dev.add_argument("--config-json", default="{}")
    dev.add_argument("--operation-id", default=None)
    dev.add_argument("--grant", action="append", default=[])
    dev.add_argument("--cache-dir", type=Path, default=Path(".forest/packages"))
    args = parser.parse_args()
    if args.command == "dev" and args.package is not None and not args.service:
        parser.error("--package requires --service for a single invocation")
    if (
        args.command == "dev"
        and args.watch is not None
        and (args.service or args.operation_id or args.input_json != "{}")
    ):
        parser.error(
            "--watch is a continuous lifecycle; service/input/operation options belong to --package"
        )
    try:
        if sys.platform == "win32":
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        if args.command in {"plugin", "dev"}:
            asyncio.run(run_plugin_command(args))
            return
        state = BootstrapState(args.data_dir)
        token = state.initialize()
        if args.command == "setup-token":
            print(token or state.rotate_setup_token())
            return
        origin = urlsplit(args.public_origin)
        if (
            origin.scheme not in {"http", "https"}
            or not origin.hostname
            or origin.username
            or origin.password
            or origin.query
            or origin.fragment
            or origin.path
        ):
            parser.error("--public-origin 必须是准确的 http(s)://host:port 来源，不包含路径或凭据")
        if origin.scheme == "http" and origin.hostname not in {"127.0.0.1", "localhost", "::1"}:
            parser.error("远程管理入口必须使用 HTTPS；本机开发可使用 loopback HTTP")
        import uvicorn

        from forest.management.app import WebSettings, create_app

        print(f"Forest 管理入口：{args.public_origin}")
        if state.status()["setup_required"]:
            print("通过 forest setup-token（使用相同 --data-dir）取得一次性配对码。")
        app = create_app(WebSettings(args.data_dir, args.public_origin, managed_database()))
        uvicorn.run(
            app,
            host=args.host,
            port=args.port,
            access_log=False,
            proxy_headers=False,
            limit_concurrency=128,
        )
    except ForestError as exc:
        parser.exit(1, f"{exc.code}: {exc.message}\n")
    except KeyboardInterrupt:
        parser.exit(130, "Development host stopped.\n")


async def run_plugin_command(args: argparse.Namespace) -> None:
    from jsonschema import Draft202012Validator

    from forest.drivers.python_worker import PythonWorker, WorkerIdentity, WorkerLimits
    from forest.packages import PackageCatalog

    if args.command == "plugin" and args.plugin_command in {"lock", "import", "export"}:
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
    if args.command == "dev" and getattr(args, "watch", None) is not None:
        await run_watch_command(args, config)
        return
    source = args.source if args.command == "plugin" else args.package
    snapshot = await asyncio.to_thread(PackageCatalog(args.cache_dir).build, source)
    building = args.command == "plugin" and args.plugin_command == "build"
    if not building and not Draft202012Validator(
        snapshot.schema(snapshot.component["config_schema"])
    ).is_valid(config):
        raise ForestError("CONFIG_INVALID", "Configuration does not match the package schema", 422)
    if args.command == "plugin":
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
                }
            )
        )
        return
    from forest.kernel import Kernel
    from forest.storage.bot import BotStore

    dsn = os.environ.get("FOREST_BOT_DSN")
    if not dsn:
        raise ForestError(
            "BOT_DATABASE_REQUIRED",
            "FOREST_BOT_DSN must name a dedicated development Bot database",
            422,
        )
    try:
        raw = sys.stdin.read(262145) if args.input_json == "-" else args.input_json
        if len(raw.encode("utf-8")) > 262144:
            raise ValueError("Input too large")
        payload = json.loads(raw, parse_constant=_reject_constant)
    except (ValueError, UnicodeError, RecursionError):
        raise ForestError("INPUT_INVALID", "--input-json must contain a JSON value", 422) from None
    store = BotStore(dsn, args.bot)
    kernel = Kernel(store)
    try:
        await store.open()
        await store.migrate()
        await kernel.start()
        await kernel.install(snapshot, config=config, grants=frozenset(args.grant))
        result = await kernel.invoke(
            snapshot.component["id"],
            args.service,
            payload,
            operation_id=args.operation_id or str(uuid4()),
        )
        print(json.dumps({"status": "completed", "result": result}, ensure_ascii=False))
    finally:
        await kernel.close()
        await store.close()


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
                }
            )
        )
        return
    catalog = PackageCatalog(args.cache_dir)
    if args.plugin_command == "export":
        content = await asyncio.to_thread(catalog.export_archive, args.digest)
        await asyncio.to_thread(_write_atomic, args.output, content)
        print(
            json.dumps(
                {"status": "exported", "package_digest": args.digest, "archive": str(args.output)}
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
            }
        )
    )


async def run_watch_command(args: argparse.Namespace, config: dict) -> None:
    from forest.kernel import Kernel
    from forest.packages import PackageCatalog
    from forest.storage.bot import BotStore
    from forest.watch import DevWatcher

    dsn = os.environ.get("FOREST_BOT_DSN")
    if not dsn:
        raise ForestError(
            "BOT_DATABASE_REQUIRED",
            "FOREST_BOT_DSN must name a dedicated development Bot database",
            422,
        )
    store = BotStore(dsn, args.bot)
    kernel = Kernel(store)
    watcher = DevWatcher(
        kernel,
        PackageCatalog(args.cache_dir),
        args.watch,
        config=config,
        grants=frozenset(args.grant),
        on_event=lambda event: print(json.dumps(event, ensure_ascii=False), flush=True),
    )
    try:
        await store.open()
        await store.migrate()
        await kernel.start()
        await watcher.start()
        await watcher.wait()
    finally:
        try:
            await watcher.close()
        finally:
            try:
                await kernel.close()
            finally:
                await store.close()


if __name__ == "__main__":
    main()
