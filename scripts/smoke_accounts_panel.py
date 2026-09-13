"""Real-browser account creation, preview/apply and stale-read fencing checks."""

from __future__ import annotations

import argparse
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from playwright.sync_api import expect, sync_playwright

BOOTSTRAP = r"""
import {accountsPanel} from '/static/accounts-panel.js';
window.fixture={accounts:[
  {id:'admin-id',username:'admin',role:'admin',disabled:false,revision:1,active_sessions:1,created_at:'2030-01-01T00:00:00Z'},
  {id:'viewer-id',username:'observer',role:'viewer',disabled:false,revision:1,active_sessions:1,created_at:'2030-01-01T00:00:01Z'}
],calls:[],receipts:{},holdOnce:false,pending:null,errors:[],sessionEnded:null};
async function api(path,body,headers={}) {
  fixture.calls.push({path,body:structuredClone(body),headers});
  if(path==='/accounts'&&body===undefined) {
    const result={accounts:structuredClone(fixture.accounts),roles:['admin','viewer']};
    if(fixture.holdOnce){fixture.holdOnce=false;return await new Promise(resolve=>fixture.pending=()=>resolve(result));}
    return result;
  }
  if(path==='/accounts'&&body!==undefined) {
    if(!headers['Idempotency-Key'])throw Error('missing key');
    const key=headers['Idempotency-Key'];if(fixture.receipts[key])return structuredClone(fixture.receipts[key]);
    const row={id:'created-id',username:body.username,role:body.role,disabled:false,revision:1,active_sessions:0,created_at:'2030-01-01T00:00:02Z'};
    fixture.accounts.push(row);fixture.receipts[key]=row;return structuredClone(row);
  }
  if(path.endsWith('/change-plans')) return {plan_id:'plan-1',plan_digest:'digest-1',target_account_id:'viewer-id',expected_revision:1,expires_at:'2030-01-01T00:10:00Z',before:{role:'viewer',disabled:false},after:{role:body.role,disabled:body.disabled,password_change:body.new_password!==undefined}};
  if(path.endsWith('/management-requests')) {
    if(!headers['Idempotency-Key'])throw Error('missing key');
    const row=fixture.accounts.find(item=>item.id==='viewer-id');row.role='admin';row.revision=2;row.active_sessions=0;
    return {...structuredClone(row),request_id:'request-1',revoked_sessions:1,reauthentication_required:false};
  }
  throw Error('unknown request '+path);
}
async function busy(button,work){button.disabled=true;try{return await work();}catch(error){fixture.errors.push(error.message);}finally{button.disabled=false;}}
const notice=text=>document.getElementById('notice').textContent=text;
window.panel=accountsPanel(api,busy,notice,message=>fixture.sessionEnded=message);
document.getElementById('accounts').classList.remove('hidden');window.ready=panel.load('admin-id');
"""


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--browser", type=Path)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    static = root / "src/forest/management/static"
    html = (static / "index.html").read_text(encoding="utf-8").replace(
        '<script type="module" src="/static/app.js"></script>',
        '<script type="module" src="/fixture.js"></script>',
    )

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            resources = {
                "/": (html.encode(), "text/html; charset=utf-8"),
                "/fixture.js": (BOOTSTRAP.encode(), "text/javascript"),
                "/static/accounts-panel.js": (
                    (static / "accounts-panel.js").read_bytes(), "text/javascript"
                ),
                "/static/style.css": ((static / "style.css").read_bytes(), "text/css"),
            }
            if self.path not in resources:
                self.send_error(404)
                return
            body, kind = resources[self.path]
            self.send_response(200)
            self.send_header("Content-Type", kind)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(
                headless=True,
                executable_path=str(args.browser) if args.browser else None,
            )
            page = browser.new_page(viewport={"width": 1360, "height": 1000})
            errors = []
            page.on("pageerror", lambda error: errors.append(str(error)))
            page.goto(f"http://127.0.0.1:{server.server_port}")
            page.evaluate("window.ready")
            expect(page.locator("#account-list")).to_contain_text("admin（当前）")
            expect(page.locator("#account-list")).to_contain_text("observer")

            form = page.locator("#account-create-form")
            form.locator('[name="username"]').fill("auditor")
            form.locator('[name="role"]').select_option("viewer")
            form.locator('[name="password"]').fill("browser-new-user-password")
            form.locator('[name="current_password"]').fill("browser-admin-password")
            form.get_by_role("button", name="创建账号").click()
            expect(page.locator("#account-list")).to_contain_text("auditor")
            create = page.evaluate("fixture.calls.find(call=>call.path==='/accounts'&&call.body)")
            assert create["headers"]["Idempotency-Key"]

            page.get_by_role("button", name="observer · 查看者 · 活跃会话 1").click()
            edit = page.locator("#account-plan-form")
            edit.locator('[name="role"]').select_option("admin")
            edit.locator('[name="new_password"]').fill("browser-replacement-password")
            edit.get_by_role("button", name="预览账号变更").click()
            expect(page.locator("#account-preview")).to_be_visible()
            expect(page.locator("#account-preview-diff")).not_to_contain_text(
                "browser-replacement-password"
            )
            page.locator('#account-apply-form [name="current_password"]').fill(
                "browser-admin-password"
            )
            page.get_by_role("button", name="应用账号变更").click()
            expect(page.locator("#account-list")).to_contain_text("observer · 管理员")

            page.evaluate("fixture.holdOnce=true")
            page.locator("#account-refresh").click()
            page.wait_for_function("fixture.pending!==null")
            page.evaluate("fixture.accounts[1].username='observer-new';window.newer=panel.load('admin-id')")
            page.evaluate("window.newer")
            expect(page.locator("#account-list")).to_contain_text("observer-new")
            page.evaluate("fixture.pending();fixture.pending=null")
            expect(page.locator("#account-list")).to_contain_text("observer-new")
            expect(page.locator("#account-list")).not_to_contain_text("observer · 管理员")

            assert errors == [], errors
            assert page.evaluate("fixture.errors") == []
            artifacts = root / ".local" / "browser-artifacts"
            artifacts.mkdir(parents=True, exist_ok=True)
            page.screenshot(path=str(artifacts / "accounts-desktop.png"), full_page=True)
            page.set_viewport_size({"width": 390, "height": 844})
            assert page.evaluate("document.documentElement.scrollWidth <= innerWidth")
            page.screenshot(path=str(artifacts / "accounts-mobile.png"), full_page=True)
            browser.close()
            print("PASS: account creation, redacted preview/apply, stale-read fencing and mobile layout")
    finally:
        server.shutdown()
        thread.join()
        server.server_close()


if __name__ == "__main__":
    main()
