from __future__ import annotations

import argparse
import asyncio
import os
import sys
from pathlib import Path
from urllib.parse import quote, urlsplit

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
    args = parser.parse_args()
    try:
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
        if sys.platform == "win32":
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
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


if __name__ == "__main__":
    main()
