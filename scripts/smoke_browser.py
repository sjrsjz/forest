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
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.request import ProxyHandler, Request, build_opener
from uuid import uuid4

from playwright.sync_api import expect, sync_playwright


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=0)
    parser.add_argument("--browser", type=Path)
    parser.add_argument("--image", help="Runtime image for this disposable Compose project")
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
    override = root / ".local" / (project + "-compose.json")
    if args.image:
        override.write_text(json.dumps({"services": {"forest": {"image": args.image}}}), encoding="utf-8")
        command += ["-f", str(root / "compose.yaml"), "-f", str(override)]

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
        model_key = secrets.token_urlsafe(24)
        memory_key = secrets.token_urlsafe(24)

        def start_model():
            source = '''import json
from http.server import BaseHTTPRequestHandler, HTTPServer
KEY = __KEY__
class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args): pass
    def do_POST(self):
        if self.path != '/v1/chat/completions' or self.headers.get('Authorization') != 'Bearer ' + KEY:
            self.send_error(403); return
        body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
        if body.get('model') != 'browser-model' or body.get('stream') is not False:
            self.send_error(422); return
        raw = json.dumps({'id':'browser-completion','model':'browser-model',
            'choices':[{'index':0,'finish_reason':'stop','message':{
                'role':'assistant','content':'<valid_agent_protocol_content_block block_type="ModelTurn"><payload><blocks><block block_type="Assistant"><content><text>浏览器 Agent 已回复</text></content></block></blocks></payload></valid_agent_protocol_content_block>'}}],
            'usage':{'prompt_tokens':10,'completion_tokens':10,'total_tokens':20}}).encode()
        self.send_response(200); self.send_header('Content-Type','application/json')
        self.send_header('Content-Length',str(len(raw))); self.end_headers(); self.wfile.write(raw)
HTTPServer(('127.0.0.1',5901),Handler).serve_forever()
'''.replace("__KEY__", repr(model_key))
            compose("exec", "-T", "-d", "forest", "python", "-c", source)

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
        start_model()
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

            memory_budgets = {
                "alpha": {"daily_request_budget":17, "daily_token_budget":54000,
                    "request_token_reservation":1024, "capture_daily_bytes":1048576,
                    "capture_storage_bytes":8388608, "query_max_parts":5, "query_max_images":2},
                "beta": {"daily_request_budget":29, "daily_token_budget":87000,
                    "request_token_reservation":2048, "capture_daily_bytes":2097152,
                    "capture_storage_bytes":16777216, "query_max_parts":3, "query_max_images":1},
            }

            def check_memory(bot_id: str, label: str, *, has_key: bool) -> None:
                expect(page.locator('#memory-config-status')).to_contain_text('配置版本 1')
                expect(page.locator('#memory-config-status')).to_contain_text('未启用')
                expect(page.locator('#memory-config [name="enabled"]')).not_to_be_checked()
                expect(page.locator('#memory-config [name="api_key"]')).to_have_value('')
                for field, expected_value in memory_budgets[label].items():
                    expect(page.locator(f'#memory-config [name="{field}"]')).to_have_value(str(expected_value))
                result = page.evaluate("async prefix => {const config=await fetch(prefix+'/configuration');"
                    "const status=await fetch(prefix+'/status');if(!config.ok||!status.ok)throw Error('Memory read failed');"
                    "return {config:await config.json(),status:await status.json()};}",
                    '/management/v1/bots/' + bot_id + '/memory')
                serialized = json.dumps(result, ensure_ascii=False)
                assert memory_key not in serialized and 'secret_ciphertext' not in serialized
                assert result['config']['revision'] == 1 and result['config']['has_api_key'] is has_key
                assert result['config']['values']['enabled'] is False
                assert result['config']['values']['deployment_id'] == 'browser-disabled-' + label
                assert all(result['config']['values'][name] == value for name, value in memory_budgets[label].items())
                # Disabled configuration must not start embedding or image downloads.
                assert result['status']['enabled'] is False
                assert result['status']['counts']['attempts'] == {}
                assert result['status']['counts']['captures'] == {}
                assert result['status']['budget'] is None

            def save_memory(bot_id: str, label: str, *, with_key: bool) -> None:
                expect(page.locator('#bot-memory')).to_be_visible()
                expect(page.locator('#memory-config-status')).to_contain_text('配置版本 0')
                form = page.locator('#memory-config')
                form.locator('[name="enabled"]').uncheck()
                # A disabled loopback endpoint makes this test independent of any
                # external embedding service; saving is not inference acceptance.
                form.locator('[name="endpoint"]').fill('http://127.0.0.1:5902/v1/embeddings')
                form.locator('[name="deployment_id"]').fill('browser-disabled-' + label)
                for field, value in memory_budgets[label].items():
                    form.locator(f'[name="{field}"]').fill(str(value))
                if with_key:
                    form.locator('[name="api_key"]').fill(memory_key)
                prefix = '/management/v1/bots/' + bot_id + '/memory'
                with page.expect_response(lambda response: response.url.endswith(prefix + '/change-plans')) as planned:
                    form.locator('button[type="submit"]').click()
                assert planned.value.ok
                assert memory_key not in planned.value.text()
                expect(page.locator('#memory-preview')).to_be_visible()
                assert memory_key not in page.locator('#memory-diff').text_content()
                expect(page.locator('#memory-diff')).to_contain_text(str(memory_budgets[label]['daily_token_budget']))
                with page.expect_response(lambda response: response.url.endswith(prefix + '/management-requests')) as applied:
                    page.locator('#memory-apply').click()
                assert applied.value.ok and memory_key not in applied.value.text()
                check_memory(bot_id, label, has_key=with_key)

            alpha = create_bot("Alpha 浏览器 Bot", 2)
            invoke(3, "：6（状态修订 1）")
            save_memory(alpha, 'alpha', with_key=True)
            beta = create_bot("Beta 浏览器 Bot", 1)
            invoke(3, "：3（状态修订 1）")
            save_memory(beta, 'beta', with_key=False)
            select_bot(page, alpha)
            check_memory(alpha, 'alpha', has_key=True)
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
            expect(page.locator('#bot-agent')).to_be_visible()
            page.locator('#agent-config [name="endpoint"]').fill('http://127.0.0.1:5901/v1')
            page.locator('#agent-config [name="model"]').fill('browser-model')
            page.locator('#agent-config [name="api_key"]').fill(model_key)
            page.locator('#agent-config [name="enabled"]').check()
            page.locator('#agent-config [name="stream"]').uncheck()
            page.locator('#agent-config details').filter(has_text='协议与单次运行预算').locator('summary').click()
            page.locator('#agent-config [name="run_token_budget"]').fill('24000')
            page.locator('#agent-config [name="preset_context"]').fill('浏览器 Agent 人格')
            page.locator('#agent-config button[type="submit"]').click()
            expect(page.locator('#agent-preview')).to_be_visible()
            assert model_key not in page.locator('#agent-config-diff').text_content()
            page.locator('#agent-apply').click()
            expect(page.locator('#agent-config-status')).to_contain_text('配置修订 1')
            expect(page.locator('#agent-config [name="api_key"]')).to_have_value('')
            page.locator('#binding-config [name="response_mode"]').select_option('agent')
            page.locator('#binding-config button[type="submit"]').click()
            page.locator('#apply-binding').click()
            expect(page.locator('#binding-state')).to_contain_text('已生效 2', timeout=30000)
            group_message(4, '/chat 使用真正 Agent 链路')
            expect(page.locator('#delivery-list')).to_contain_text('浏览器 Agent 已回复', timeout=20000)
            page.locator('#agent-refresh').click()
            expect(page.locator('#agent-runs')).to_contain_text('COMPLETED', timeout=15000)
            # A newly discovered group becomes available without switching Bots.
            # Every write below uses the actual shipped form and authenticated API.
            page.locator('#job-refresh').click()
            job_form = page.locator('#job-config')
            expect(job_form.locator('[name="name"]')).to_be_enabled()
            expect(job_form.locator('[name="conversation_id"]')).to_contain_text('30001')
            job_form.locator('[name="name"]').fill('浏览器定时报告')
            job_form.locator('[name="prompt"]').fill('整理本群的定时报告，并明确来源群。')
            start = datetime.now(UTC) + timedelta(days=1)
            job_form.locator('[name="start_at"]').fill(start.isoformat())
            job_form.locator('[name="delegation_expires_at"]').fill((start + timedelta(days=7)).isoformat())
            job_form.locator('button[type="submit"]').click()
            expect(page.locator('#job-preview')).to_be_visible()
            expect(page.locator('#job-diff')).to_contain_text('群 30001')
            expect(page.locator('#job-diff')).to_contain_text('Asia/Shanghai')
            page.locator('#job-apply').click()
            expect(page.locator('#job-current')).to_contain_text('修订 1')
            sent_before = int(compose('exec', '-T', 'forest', 'python', '-c',
                "import json;from pathlib import Path;print(len(json.loads(Path('/tmp/onebot-sends.json').read_text())))").strip())
            page.locator('#job-run').click()
            jobs_path = '/management/v1/bots/' + alpha + '/jobs'
            delivered = page.evaluate("async path => {for(let i=0;i<120;i++){const r=await fetch(path);"
                                      "if(r.ok){const data=await r.json();if(data.occurrences.some(o=>o.reply_id&&o.delivery_state==='SENT'))return true;}"
                                      "await new Promise(resolve=>setTimeout(resolve,250));}return false;}", jobs_path)
            assert delivered, 'The scheduled Agent response did not reach the OneBot transport'
            page.locator('#job-runs-refresh').click()
            expect(page.locator('#job-runs')).to_contain_text('回复已提交到发送队列')
            expect(page.locator('#job-runs')).to_contain_text('平台已确认送达')
            sent_after = int(compose('exec', '-T', 'forest', 'python', '-c',
                "import json;from pathlib import Path;r=json.loads(Path('/tmp/onebot-sends.json').read_text());"
                "assert '浏览器 Agent 已回复' in json.dumps(r[-1],ensure_ascii=False);print(len(r))").strip())
            assert sent_after == sent_before + 1
            # The same button retries its fixed operation key; it creates no
            # second occurrence or group input. A separate button starts another.
            page.locator('#job-run').click()
            expect(page.locator('#job-runs .job-occurrence')).to_have_count(1)
            page.locator('#job-toggle').click()
            expect(page.locator('#job-diff')).to_contain_text('启用 → 停用')
            page.locator('#job-apply').click()
            expect(page.locator('#job-current')).to_contain_text('修订 2')
            expect(page.locator('#job-current')).to_contain_text('已停用')
            page.bring_to_front()
            page.screenshot(path=str(artifacts / "bots-desktop.png"), full_page=True)
            page.set_viewport_size({"width": 390, "height": 844})
            page.screenshot(path=str(artifacts / "bots-mobile.png"), full_page=True)
            compose("restart", "forest")
            compose("up", "-d", "--no-build", "--wait", "--wait-timeout", "90")
            start_onebot()
            start_model()
            page.goto(origin)
            expect(page.locator("#configuration")).to_be_visible(timeout=20000)
            expect(page.locator("#revision")).to_have_text("1")
            expect(page.locator('#config-form [name="display_name"]')).to_have_value(
                "Forest 浏览器验证"
            )
            page.locator("#nav-bots").click()
            select_bot(page, alpha)
            check_memory(alpha, 'alpha', has_key=True)
            invoke(1, "：14（状态修订 3）")
            page.locator('#binding-list button').first.click()
            expect(page.locator('#binding-state')).to_contain_text('在线', timeout=30000)
            expect(page.locator('#agent-config [name="model"]')).to_have_value('browser-model')
            expect(page.locator('#agent-config [name="api_key"]')).to_have_value('')
            expect(page.locator('#agent-config [name="preset_context"]')).to_have_value('浏览器 Agent 人格')
            expect(page.locator('#agent-config [name="run_token_budget"]')).to_have_value('24000')
            page.locator('#job-list button').filter(has_text='浏览器定时报告').click()
            expect(page.locator('#job-current')).to_contain_text('修订 2')
            expect(page.locator('#job-current')).to_contain_text('已停用')
            expect(page.locator('#job-runs')).to_contain_text('平台已确认送达')
            group_message(5, '/chat 重启后继续')
            expect(page.locator('#conversation-list')).to_contain_text('已推入上下文 6', timeout=20000)
            select_bot(page, beta)
            check_memory(beta, 'beta', has_key=False)
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
                        "Web model configuration, real Agent HTTP reply and credential recovery after restart",
                        "Web Jobs source refresh, preview/apply, idempotent manual Agent reply, delivery, disable and restart persistence",
                        "Private child controls saved and recovered after restart",
                        "Disabled multimodal Memory form preview/apply, independent Bot budgets and keys, restart persistence and no inference requests",
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
        try:
            compose("down", "--volumes", "--remove-orphans")
        finally:
            override.unlink(missing_ok=True)


if __name__ == "__main__":
    main()
