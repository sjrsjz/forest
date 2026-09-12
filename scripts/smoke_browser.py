"""Exercise a disposable Compose deployment in a real browser; never log secrets.

Run after `docker compose build`. Requires the dev dependencies and a Playwright
Chromium installation, or --browser pointing to a compatible installed browser.
All created containers/volumes have a unique forest-e2e- prefix and are removed.
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import io
import json
import os
import secrets
import socket
import subprocess
import zipfile
from pathlib import Path
from urllib.request import ProxyHandler, Request, build_opener
from uuid import uuid4

from playwright.sync_api import expect, sync_playwright


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--browser", type=Path)
    args = parser.parse_args()
    if args.port == 0:
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            args.port = reservation.getsockname()[1]
    root = Path(__file__).resolve().parents[1]
    project = "forest-e2e-" + uuid4().hex[:12]
    env = dict(os.environ)
    origin = f"http://127.0.0.1:{args.port}"
    env.update(FOREST_HTTP_PORT=str(args.port), FOREST_PUBLIC_ORIGIN=origin)
    command = ["docker", "compose", "-p", project]
    artifacts = root / ".local" / "browser-artifacts"
    artifacts.mkdir(parents=True, exist_ok=True)

    def compose(*arguments: str) -> str:
        result = subprocess.run(
            command + list(arguments),
            cwd=root,
            env=env,
            capture_output=True,
            text=True,
            timeout=180,
        )
        if result.returncode:
            raise RuntimeError(f"Compose operation failed: {arguments[0]}\n{result.stderr[-2000:]}")
        return result.stdout

    try:
        compose("up", "-d", "--no-build", "--wait", "--wait-timeout", "90")
        # Loopback traffic must not inherit a developer machine's outbound proxy.
        direct_http = build_opener(ProxyHandler({}))
        with direct_http.open(origin + "/health/live", timeout=5) as health:
            if json.load(health).get("service") != "forest-management":
                raise RuntimeError("The selected host port does not serve this Forest deployment")
        token = compose("exec", "-T", "forest", "forest", "setup-token").strip()
        password = secrets.token_urlsafe(32)
        api_token, webhook_secret = secrets.token_urlsafe(24), secrets.token_urlsafe(24)

        def start_onebot():
            # Disposable test transport inside the disposable Forest container;
            # localhost is never exposed as an unauthenticated host API.
            source = '''import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
TOKEN = __TOKEN__
class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args): pass
    def do_POST(self):
        if self.headers.get('Authorization') != 'Bearer ' + TOKEN:
            self.send_error(403); return
        body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        if self.path == '/get_login_info': data = {'user_id':10001,'nickname':'Browser OneBot'}
        elif self.path == '/send_group_msg':
            log = Path('/tmp/onebot-sends.json')
            rows = json.loads(log.read_text()) if log.exists() else []
            rows.append(body); log.write_text(json.dumps(rows))
            data = {'message_id':8000+len(rows)}
            uncertain = Path('/tmp/onebot-unknown')
            if uncertain.exists(): uncertain.unlink(); self.close_connection=True; return
        else: self.send_error(404); return
        raw = json.dumps({'status':'ok','retcode':0,'data':data}).encode()
        self.send_response(200); self.send_header('Content-Length',str(len(raw)))
        self.send_header('Content-Type','application/json'); self.end_headers(); self.wfile.write(raw)
HTTPServer(('127.0.0.1',5900),Handler).serve_forever()
'''.replace("__TOKEN__", repr(api_token))
            compose("exec", "-T", "-d", "forest", "python", "-c", source)

        start_onebot()
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(
                headless=True,
                args=["--no-proxy-server", "--disable-gpu"],
                **({"executable_path": str(args.browser)} if args.browser else {}),
            )
            context = browser.new_context(viewport={"width": 1360, "height": 900})
            page = context.new_page()
            page.goto(origin)
            expect(page.locator("#setup")).to_be_visible()
            page.locator('#setup-form [name="token"]').fill(token)
            page.locator('#setup-form [name="username"]').fill("browser-admin")
            page.locator('#setup-form [name="password"]').fill(password)
            page.locator("#setup-form button").click()
            expect(page.locator("#login")).to_be_visible(timeout=20000)
            page.locator('#login-form [name="username"]').fill("browser-admin")
            page.locator('#login-form [name="password"]').fill(password)
            page.locator("#login-form button").click()
            expect(page.locator("#configuration")).to_be_visible()
            rival = context.new_page()
            rival.goto(origin)
            expect(rival.locator("#configuration")).to_be_visible()
            page.locator('#config-form [name="display_name"]').fill("Forest 浏览器验证")
            page.locator('#config-form [name="session_ttl_minutes"]').fill("90")
            page.locator('#config-form button[type="submit"]').click()
            expect(page.locator("#preview")).to_be_visible()
            page.locator("#apply").click()
            expect(page.locator("#revision")).to_have_text("1")
            rival.locator('#config-form [name="display_name"]').fill("另一个窗口的草稿")
            rival.locator('#config-form button[type="submit"]').click()
            expect(rival.locator("#notice")).to_contain_text("REVISION_CONFLICT")
            expect(rival.locator('#config-form [name="display_name"]')).to_have_value(
                "另一个窗口的草稿"
            )
            page.bring_to_front()
            page.screenshot(path=str(artifacts / "configuration-desktop.png"), full_page=True)
            page.set_viewport_size({"width": 390, "height": 844})
            page.screenshot(path=str(artifacts / "configuration-mobile.png"), full_page=True)
            page.set_viewport_size({"width": 1360, "height": 1000})
            page.locator("#nav-bots").click()

            def create_bot(name: str, multiplier: int) -> str:
                page.locator('#create-bot-form [name="display_name"]').fill(name)
                page.locator('#create-bot-form button').click()
                expect(page.locator("#bot-name")).to_have_text(name)
                expect(page.locator("#bot-components")).to_be_visible(timeout=45000)
                page.locator('#bot-config-fields [name="label"]').fill(name)
                page.locator('#bot-config-fields [name="multiplier"]').fill(str(multiplier))
                page.locator("#bot-grants").check()
                page.locator('#bot-config-form button[type="submit"]').click()
                expect(page.locator("#bot-config-preview")).to_be_visible()
                page.locator("#bot-config-apply").click()
                expect(page.locator("#bot-effective")).to_have_text("1", timeout=30000)
                expect(page.locator("#bot-runtime-status")).to_have_text("组件已生效并运行中", timeout=30000)
                return page.locator("#bot-list button").filter(has_text=name).get_attribute("data-bot-id")

            def select_bot(page, bot_id: str) -> None:
                page.locator(f'#bot-list button[data-bot-id="{bot_id}"]').click()
                expect(page.locator("#bot-runtime-status")).to_have_text("组件已生效并运行中", timeout=30000)

            def invoke(value: int, expected: str) -> None:
                page.locator('#bot-invoke-form [name="increment"]').fill(str(value))
                page.locator('#bot-invoke-form button').click()
                expect(page.locator("#bot-invoke-result")).to_contain_text(expected, timeout=15000)

            alpha = create_bot("Alpha 浏览器 Bot", 2)
            invoke(3, "：6（状态修订 1）")
            beta = create_bot("Beta 浏览器 Bot", 1)
            invoke(3, "：3（状态修订 1）")
            select_bot(page, alpha)
            rival.locator("#nav-bots").click()
            select_bot(rival, alpha)
            rival.locator('#bot-config-fields [name="multiplier"]').fill("77")
            page.locator('#bot-config-fields [name="multiplier"]').fill("4")
            page.locator('#bot-config-form button[type="submit"]').click()
            page.locator("#bot-config-apply").click()
            expect(page.locator("#bot-effective")).to_have_text("2", timeout=30000)
            expect(page.locator("#bot-runtime-status")).to_have_text("组件已生效并运行中", timeout=30000)
            rival.locator('#bot-config-form button[type="submit"]').click()
            expect(rival.locator("#notice")).to_contain_text("REVISION_CONFLICT")
            expect(rival.locator('#bot-config-fields [name="multiplier"]')).to_have_value("77")
            invoke(1, "：10（状态修订 2）")
            expect(page.locator("#bot-bindings")).to_be_visible()
            page.locator('#binding-create [name="self_id"]').fill("10001")
            page.locator('#binding-create [name="display_name"]').fill("浏览器 QQ 接入")
            page.locator('#binding-create button').click()
            expect(page.locator("#binding-detail")).to_be_visible()
            page.locator('#binding-config [name="api_base_url"]').fill("http://127.0.0.1:5900")
            page.locator('#binding-config [name="access_token"]').fill(api_token)
            page.locator('#binding-config [name="webhook_secret"]').fill(webhook_secret)
            page.locator('#binding-config [name="admin_user_ids"]').fill("20001")
            page.locator('#binding-config [name="enabled"]').check()
            page.locator('#binding-config [name="response_mode"]').select_option("component")
            page.locator('#binding-config button[type="submit"]').click()
            expect(page.locator("#binding-preview")).to_be_visible()
            page.locator('#apply-binding').click()
            expect(page.locator("#binding-state")).to_contain_text("在线", timeout=30000)
            expect(page.locator('#binding-config [name="access_token"]')).to_have_value("")
            expect(page.locator('#binding-config [name="webhook_secret"]')).to_have_value("")
            webhook = page.locator('#binding-webhook').input_value()

            def group_message(number, message):
                raw = json.dumps({'time':1700000000+number,'self_id':10001,'post_type':'message',
                    'message_type':'group','sub_type':'normal','group_id':30001,'user_id':20001,
                    'message_id':number,'message':[{'type':'text','data':{'text':message}}],
                    'sender':{'user_id':20001,'nickname':'浏览器用户','role':'member'}}, ensure_ascii=False).encode()
                signature = 'sha1=' + hmac.new(webhook_secret.encode(), raw, hashlib.sha1).hexdigest()
                request = Request(webhook, data=raw, headers={'Content-Type':'application/json','X-Signature':signature})
                with direct_http.open(request, timeout=15) as response:
                    assert response.status == 200

            group_message(1, '不触发的群聊消息')
            expect(page.locator('#conversation-list')).to_contain_text('已推入上下文 1', timeout=15000)
            expect(page.locator('#delivery-list')).to_contain_text('暂无发送记录')
            group_message(2, '/chat 浏览器接入验证')
            expect(page.locator('#delivery-list')).to_contain_text('SENT', timeout=20000)
            compose('exec','-T','forest','python','-c',"from pathlib import Path; Path('/tmp/onebot-unknown').touch()")
            group_message(3, '/chat 等待核对的消息')
            expect(page.locator('#delivery-list')).to_contain_text('UNKNOWN', timeout=20000)
            resolution = page.locator('#delivery-list form').first
            resolution.locator('[name="decision"]').select_option('SKIP')
            resolution.locator('[name="reason"]').fill('已核对平台，跳过这次不确定结果')
            resolution.locator('button').click()
            expect(page.locator('#delivery-list')).to_contain_text('SKIPPED', timeout=15000)
            expect(page.locator('#delivery-list form')).to_have_count(0)
            page.bring_to_front()
            page.screenshot(path=str(artifacts / "bots-desktop.png"), full_page=True)
            page.set_viewport_size({"width": 390, "height": 844})
            page.screenshot(path=str(artifacts / "bots-mobile.png"), full_page=True)
            compose("restart", "forest")
            compose("up", "-d", "--no-build", "--wait", "--wait-timeout", "90")
            start_onebot()
            page.goto(origin)
            expect(page.locator("#configuration")).to_be_visible(timeout=20000)
            expect(page.locator("#revision")).to_have_text("1")
            expect(page.locator('#config-form [name="display_name"]')).to_have_value(
                "Forest 浏览器验证"
            )
            page.locator("#nav-bots").click()
            select_bot(page, alpha)
            invoke(1, "：14（状态修订 3）")
            page.locator('#binding-list button').first.click()
            expect(page.locator('#binding-state')).to_contain_text('在线', timeout=30000)
            group_message(4, '/chat 重启后继续')
            expect(page.locator('#conversation-list')).to_contain_text('已推入上下文 4', timeout=20000)
            select_bot(page, beta)
            invoke(1, "：4（状态修订 2）")

            # Import complete native packages through the authenticated WebUI.
            # Two versions share a component, and history restore preserves its
            # current durable count instead of restoring an old state snapshot.
            def plugin_archive(version):
                output = io.BytesIO()
                source = root / 'src/forest/builtin/counter'
                with zipfile.ZipFile(output, 'w', compression=zipfile.ZIP_DEFLATED) as archive:
                    for path in source.rglob('*'):
                        if not path.is_file() or '__pycache__' in path.parts:
                            continue
                        content = path.read_bytes()
                        content = content.replace(b'counter.increment', b'browser.increment')
                        if path.name == 'module.toml':
                            content = content.replace(b'forest.counter', b'demo.browser').replace(
                                b'version = "0.1.0"', f'version = "{version}"'.encode())
                        archive.writestr(path.relative_to(source).as_posix(), content)
                return output.getvalue()

            page.locator('summary').filter(has_text='插件包管理').click()
            for version in ('1.0.0', '2.0.0'):
                page.locator('#package-import-form [name="archive"]').set_input_files({
                    'name': f'plugin-{version}.zip', 'mimeType': 'application/zip',
                    'buffer': plugin_archive(version),
                })
                page.locator('#package-import-form button').click()
                expect(page.locator('#package-list')).to_contain_text(f'demo.browser · {version}')
            page.locator('#bot-package').select_option(label='demo.browser · 1.0.0')
            expect(page.locator('#bot-desired')).to_have_text('0')
            page.locator('#bot-config-fields [name="label"]').fill('Web 插件')
            page.locator('#bot-config-fields [name="multiplier"]').fill('2')
            page.locator('#bot-grants').check()
            page.locator('#bot-config-form button[type="submit"]').click()
            expect(page.locator('#bot-config-preview')).to_be_visible()
            page.locator('#bot-config-apply').click()
            expect(page.locator('#bot-effective')).to_have_text('1', timeout=30000)
            expect(page.locator('#bot-runtime-status')).to_have_text('组件已生效并运行中', timeout=30000)
            invoke(2, '：4（状态修订 1）')
            page.locator('#bot-package').select_option(label='demo.browser · 2.0.0')
            expect(page.locator('#bot-config-fields [name="label"]')).to_have_value('Web 插件')
            page.locator('#bot-config-fields [name="multiplier"]').fill('5')
            page.locator('#bot-config-form button[type="submit"]').click()
            expect(page.locator('#bot-config-preview')).to_be_visible()
            page.locator('#bot-config-apply').click()
            expect(page.locator('#bot-effective')).to_have_text('2', timeout=30000)
            expect(page.locator('#bot-runtime-status')).to_have_text('组件已生效并运行中', timeout=30000)
            invoke(1, '：9（状态修订 2）')
            page.locator('#read-bot-history').click()
            expect(page.locator('#bot-history button')).to_have_count(2)
            page.locator('#bot-history button').last.click()
            expect(page.locator('#bot-config-fields [name="multiplier"]')).to_have_value('2')
            page.locator('#bot-config-form button[type="submit"]').click()
            expect(page.locator('#bot-config-preview')).to_be_visible()
            page.locator('#bot-config-apply').click()
            expect(page.locator('#bot-effective')).to_have_text('3', timeout=30000)
            expect(page.locator('#bot-runtime-status')).to_have_text('组件已生效并运行中', timeout=30000)
            invoke(1, '：11（状态修订 3）')
            page.screenshot(path=str(artifacts / 'packages-mobile.png'), full_page=True)
            page.set_viewport_size({"width": 1360, "height": 1000})
            page.screenshot(path=str(artifacts / 'packages-desktop.png'), full_page=True)
            page.locator("#logout").click()
            expect(page.locator("#login")).to_be_visible()
            rival.reload()
            expect(rival.locator("#login")).to_be_visible()
            browser.close()
        print(
            json.dumps(
                {
                    "status": "passed",
                    "checks": [
                        "setup",
                        "login",
                        "preview",
                        "apply",
                        "two-tab conflict",
                        "restart persistence",
                        "two isolated Bot databases and Workers",
                        "component preview/apply and hot reload",
                        "component two-tab conflict preserves draft",
                        "Bot configuration and state recover after restart",
                        "OneBot Web configuration and credentials stay private",
                        "real HTTP ingress, silent context, template reply and delivery",
                        "UNKNOWN operator reconciliation and restart connection recovery",
                        "Web ZIP import, generic forms, version upgrade and history restore preserve live state",
                        "logout revocation",
                    ],
                    "artifacts": str(artifacts),
                },
                ensure_ascii=False,
            )
        )
    finally:
        # This project name was generated here, never supplied by another caller.
        assert project.startswith("forest-e2e-") and len(project) == 23
        compose("down", "--volumes", "--remove-orphans")


if __name__ == "__main__":
    main()
