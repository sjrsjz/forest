"""Real Edge/Chromium checks of the shipped business-command limit form."""

from __future__ import annotations

import argparse
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from playwright.sync_api import expect, sync_playwright

BOOTSTRAP = r"""
import {businessCommandDiagnosticsPanel} from '/static/business-command-diagnostics.js';
window.fixture={
  values:{a:{max_argument_bytes:16384,max_pending_bot:2048,max_pending_binding:512,max_pending_principal:8,principal_burst:8,principal_refill_seconds:2},b:{max_argument_bytes:8192,max_pending_bot:100,max_pending_binding:50,max_pending_principal:5,principal_burst:4,principal_refill_seconds:10}},
  revisions:{a:0,b:3}, calls:[], plans:{}, receipts:{}, pending:[], hold:false, errors:[]
};
function botOf(path){return new URL(path,'http://fixture').pathname.split('/')[2];}
function response(path,body,headers){
  const bot=botOf(path);
  if(path.endsWith('/bindings')) return {bindings:[]};
  if(path.includes('/business-commands/diagnostics')) return {summary:{pending_requests:0,claimed_requests:0,failed_requests:0,pending_classifications:0,pending_notices:0,claimed_notices:0},requests:[],classifications:[],notices:[]};
  if(path.endsWith('/business-command-limits')) return {revision:fixture.revisions[bot],values:structuredClone(fixture.values[bot]),defaults:structuredClone(fixture.values.a),values_payload_state:fixture.revisions[bot]?'ACTIVE':'DEFAULT'};
  const key=bot+path+headers['Idempotency-Key'];
  if(fixture.receipts[key]) return structuredClone(fixture.receipts[key]);
  let result;
  if(path.endsWith('/business-command-limit-change-plans')) {
    result={expected_revision:body.expected_revision,plan_id:'plan-'+bot,plan_digest:'digest-'+bot,before_values:structuredClone(fixture.values[bot]),before_values_payload_state:fixture.revisions[bot]?'ACTIVE':'DEFAULT',values:structuredClone(body.values)};
    fixture.plans[result.plan_id]=structuredClone(result);
  } else if(path.endsWith('/business-command-limit-management-requests')) {
    const plan=fixture.plans[body.plan_id]; fixture.revisions[bot]++;
    fixture.values[bot]=structuredClone(plan.values);
    result={revision:fixture.revisions[bot],status:'APPLIED'};
  } else throw Error('Unknown fixture request '+path);
  fixture.receipts[key]=structuredClone(result); return result;
}
async function api(path,body,headers={}){
  fixture.calls.push({path,body:structuredClone(body),headers:structuredClone(headers)});
  const result=response(path,body,headers);
  if(fixture.hold&&path.endsWith('/business-command-limits')) return await new Promise(resolve=>fixture.pending.push(()=>resolve(structuredClone(result))));
  return structuredClone(result);
}
async function busy(button,work){button.disabled=true;try{await work();}catch(error){fixture.errors.push(error.message);}finally{button.disabled=false;}}
const notice=text=>{document.getElementById('notice').textContent=text;document.getElementById('notice').classList.remove('hidden');};
window.panel=businessCommandDiagnosticsPanel(api,busy,notice);
document.getElementById('bots').classList.remove('hidden');
document.getElementById('bot-detail').classList.remove('hidden');
window.ready=panel.select({bot_id:'a'});
"""


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--browser", type=Path)
    parser.add_argument("--artifact", type=Path)
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
                "/static/business-command-diagnostics.js": (
                    (static / "business-command-diagnostics.js").read_bytes(),
                    "text/javascript",
                ),
                "/static/style.css": ((static / "style.css").read_bytes(), "text/css"),
            }
            if self.path not in resources:
                self.send_error(404)
                return
            body, content_type = resources[self.path]
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    results = []
    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(
                headless=True,
                executable_path=str(args.browser) if args.browser else None,
            )
            page = browser.new_page(viewport={"width": 1380, "height": 1000})
            page_errors = []
            page.on("pageerror", lambda error: page_errors.append(str(error)))
            page.goto(f"http://127.0.0.1:{server.server_port}")
            page.evaluate("window.ready")
            form = page.locator("#command-limit-form")
            expect(form.locator('[name="max_pending_bot"]')).to_have_value("2048")
            form.locator('[name="max_pending_bot"]').fill("40")
            form.locator('[name="max_pending_binding"]').fill("20")
            form.locator('[name="max_pending_principal"]').fill("4")
            form.locator('[name="principal_burst"]').fill("3")
            form.locator('[name="principal_refill_seconds"]').fill("15")
            page.locator("#command-limit-password").fill("browser-test-password")
            form.locator('button[type="submit"]').click()
            expect(page.locator("#command-limit-preview")).to_be_visible()
            expect(page.locator("#command-limit-diff")).to_contain_text('"max_pending_bot": 40')
            planned = page.evaluate(
                "fixture.calls.find(c=>c.path.endsWith('/business-command-limit-change-plans'))"
            )
            assert planned["body"]["current_password"] == "browser-test-password"
            assert planned["headers"]["Idempotency-Key"]
            page.locator("#command-limit-apply").click()
            expect(page.locator("#command-limit-status")).to_contain_text("当前修订 1")
            expect(page.locator("#command-limit-password")).to_have_value("")
            results.append("preview/apply uses normalized values and stable operation keys")

            page.evaluate("fixture.hold=true")
            page.locator("#command-limit-reload").click()
            page.wait_for_function("fixture.pending.length===1")
            form.locator('[name="max_pending_bot"]').fill("41")
            page.evaluate("fixture.hold=false;fixture.pending.shift()()")
            expect(form.locator('[name="max_pending_bot"]')).to_have_value("41")
            results.append("late same-Bot reads preserve a newer form draft")

            page.evaluate("fixture.hold=true;void panel.select({bot_id:'a'})")
            page.wait_for_function("fixture.pending.length===1")
            page.evaluate("void panel.select({bot_id:'b'})")
            page.wait_for_function("fixture.pending.length===2")
            page.evaluate("fixture.hold=false;fixture.pending[1]();fixture.pending[0]();fixture.pending=[]")
            expect(form.locator('[name="max_pending_bot"]')).to_have_value("100")
            results.append("late Bot A reads cannot overwrite Bot B")

            page.set_viewport_size({"width": 390, "height": 844})
            assert page.evaluate("document.documentElement.scrollWidth <= document.documentElement.clientWidth")
            assert not page_errors and not page.evaluate("fixture.errors")
            results.append("mobile viewport has no horizontal overflow or page errors")
            if args.artifact:
                args.artifact.parent.mkdir(parents=True, exist_ok=True)
                page.screenshot(path=str(args.artifact), full_page=True)
            browser.close()
    finally:
        server.shutdown()
        server.server_close()
        thread.join()
    print(json.dumps({"kind": "real Edge / shipped command-limit UI", "cases": results}, ensure_ascii=False))


if __name__ == "__main__":
    main()
