"""Real browser form/approval races with shipped UI and controlled API replies."""

from __future__ import annotations

import argparse
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from playwright.sync_api import expect, sync_playwright

from forest.agent_protocol import protocol_descriptor
from forest.agent_settings import DEFAULT_AGENT_SETTINGS

BOOTSTRAP = r"""
import {runtimeModuleViews} from '/static/runtime-module-views.js';
window.fixture={rows:{},calls:[],hold:[],pending:[],errors:[]};
const defaults=__DEFAULTS__;
for (const bot of ['a','b']) fixture.rows[bot]={revision:1,values:{...defaults,model:'model-'+bot},protocol:__PROTOCOL__,has_api_key:true};
function respond(path, options={}) {
  const bot=path.split('/')[2], row=fixture.rows[bot];
  if(path.endsWith('/configuration')) return structuredClone(row);
  if(path.endsWith('/runs')) return {runs:[{run_id:'run-a',conversation_id:'group-a',phase:'WAIT_APPROVAL',checkpoint_seq:3}],
    approvals:[{approval_id:'approval-a',arguments_digest:'digest-a',tool_name:'counter',arguments:{amount:2}}],
    run_budgets:[{run_id:'run-a',reserved_tokens:100,charged_tokens:30}]};
  if(path.endsWith('/actions/configuration.plan')) return {plan_id:'plan-a',plan_digest:'plan-digest',
    expected_revision:row.revision,values:options.body.values,before_values:row.values,
    credentials_changed:options.body.api_key!==null||options.body.clear_api_key};
  if(path.endsWith('/actions/configuration.apply')) return {revision:++row.revision,secret_revision:2};
  if(path.endsWith('/actions/approval.decide')||path.endsWith('/actions/run.cancel')||path.endsWith('/actions/tool.resolve')) return {status:'OK'};
  throw Error('Unexpected fixture path '+path);
}
async function api(path, body, headers={}) {
  if (body !== undefined && (!headers['Idempotency-Key'] || body.method || body.body)) throw Error('Invalid shared API invocation');
  const options = body === undefined ? {} : {body, headers};
  fixture.calls.push({path,...structuredClone(options)});
  const response=respond(path,options);
  if(fixture.hold.includes(path)) return await new Promise(resolve=>fixture.pending.push({path,resolve:()=>resolve(response)}));
  return response;
}
async function busy(button,work) {
  if(button) button.disabled=true;
  try { await work(); } catch(e) { fixture.errors.push(e.message); } finally { if(button)button.disabled=false; }
}
document.getElementById('bots').classList.remove('hidden');
document.getElementById('bot-detail').classList.remove('hidden');
const nav=document.createElement('nav'),registry=document.createElement('div'),root=document.createElement('div');
document.getElementById('bot-detail').append(nav,registry,root);
window.panel=runtimeModuleViews({api,busy,
  notice:message=>{document.getElementById('notice').textContent=message;},
  nav,registry,root,packageElements:[]});
panel.update([{component_id:'agent.runtime',requires:['memory.graph'],management_view:{
  kind:'trusted-module',protocol:'forest.trusted-management-view/1',
  label:'Agent 与模型',template:'/static/modules/agent.runtime.html',
  module:'/static/agent-panel.js',style:'/static/modules/agent.runtime.css'}}]);
window.ready=panel.select({bot_id:'a'}).then(()=>{panel.show('agent.runtime');return true;});
"""


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--browser", type=Path)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    static = root / "src/forest/management/static"
    html = (static / "index.html").read_text(encoding="utf-8").replace(
        '<script type="module" src="/static/app.js"></script>',
        '<script type="module" src="/fixture.js"></script>',
    )
    bootstrap = BOOTSTRAP.replace("__DEFAULTS__", json.dumps(DEFAULT_AGENT_SETTINGS)).replace(
        "__PROTOCOL__", json.dumps(protocol_descriptor())
    )

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            resources = {"/": (html.encode(), "text/html; charset=utf-8"),
                         "/fixture.js": (bootstrap.encode(), "text/javascript")}
            for name in (
                "agent-panel.js", "configuration-forms.js", "runtime-module-views.js", "style.css"
            ):
                resources["/static/" + name] = ((static / name).read_bytes(),
                    "text/css" if name.endswith(".css") else "text/javascript")
            resources["/static/modules/agent.runtime.html"] = (
                (static / "modules/agent.runtime.html").read_bytes(), "text/html; charset=utf-8"
            )
            resources["/static/modules/agent.runtime.css"] = (
                (static / "modules/agent.runtime.css").read_bytes(), "text/css"
            )
            if self.path not in resources:
                self.send_error(404)
                return
            body, kind = resources[self.path]
            self.send_response(200)
            self.send_header("Content-Type", kind)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True,
                executable_path=str(args.browser) if args.browser else None)
            page = browser.new_page(viewport={"width": 1360, "height": 1100})
            errors = []
            page.on("pageerror", lambda error: errors.append(str(error)))
            page.goto(f"http://127.0.0.1:{server.server_port}")
            page.evaluate("window.ready")
            model = page.locator('#agent-config [name="model"]')
            expect(model).to_have_value("model-a")
            model.fill("edited-model")
            page.get_by_text('协议与单次运行预算', exact=True).click()
            page.locator('.agent-advanced > summary').click()
            page.locator('[name="run_token_budget"]').fill('24000')
            page.locator('[name="max_tool_iterations"]').fill('6')
            page.locator('[name="preset_context"]').fill('浏览器测试人格')
            page.get_by_role("button", name="预览 Agent 变更").click()
            expect(page.locator('#agent-preview')).to_be_visible()
            planned = page.evaluate("fixture.calls.find(c=>c.path.endsWith('/actions/configuration.plan')).body.values")
            assert planned['preset_context'] == ['浏览器测试人格']
            assert planned['max_tool_iterations'] == 6 and planned['run_token_budget'] == 24000
            page.locator('#agent-apply').click()
            expect(page.locator('#agent-config-status')).to_contain_text("配置修订 2")
            expect(model).to_have_value("edited-model")
            expect(page.locator('#agent-runs')).to_contain_text('每次运行预算')
            expect(page.locator('#agent-protocol-version')).to_contain_text('forest-agent-protocol v1')
            expect(page.locator('#agent-core-prompt')).to_contain_text('valid_agent_protocol_content_block')
            page.get_by_role("button", name="批准本次参数").click()
            page.wait_for_function("fixture.calls.some(c=>c.path.endsWith('/actions/approval.decide'))")
            assert page.evaluate("fixture.calls.find(c=>c.path.endsWith('/actions/approval.decide')).body") == {
                "approval_id": "approval-a", "arguments_digest": "digest-a", "decision": "APPROVE"}
            # A read started before typing must preserve the newer draft.
            page.evaluate("fixture.hold.push('/bots/a/runtime-modules/agent.runtime/queries/configuration')")
            page.locator('#agent-reload').click()
            page.wait_for_function("fixture.pending.length===1")
            model.fill("draft-during-read")
            page.evaluate("fixture.pending.shift().resolve();fixture.hold=[]")
            expect(model).to_have_value("draft-during-read")
            # Applying a reviewed plan does not overwrite a later local edit.
            page.get_by_role("button", name="预览 Agent 变更").click()
            page.evaluate("fixture.hold.push('/bots/a/runtime-modules/agent.runtime/actions/configuration.apply')")
            page.locator('#agent-apply').click()
            page.wait_for_function("fixture.pending.length===1")
            model.fill("draft-during-apply")
            page.evaluate("fixture.pending.shift().resolve();fixture.hold=[]")
            expect(model).to_have_value("draft-during-apply")
            # A late response for Bot A cannot fill Bot B's form.
            page.evaluate("fixture.hold.push('/bots/a/runtime-modules/agent.runtime/queries/configuration')")
            page.locator('#agent-reload').click()
            page.wait_for_function("fixture.pending.length===1")
            page.evaluate("panel.select({bot_id:'b'})")
            page.evaluate("fixture.pending.shift().resolve();fixture.hold=[]")
            expect(model).to_have_value("model-b")
            # Invalid tool JSON is reported as a form error, not an uncaught event.
            page.get_by_text('工具授权', exact=True).click()
            page.locator('[name="allowed_tools"]').fill('{broken')
            page.get_by_role("button", name="预览 Agent 变更").click()
            expect(page.locator('#notice')).to_contain_text("JSON")
            page.locator('[name="allowed_tools"]').fill('[]')
            assert errors == [], errors
            assert page.evaluate("fixture.errors") == []
            artifacts = root / ".local/browser-artifacts"
            artifacts.mkdir(parents=True, exist_ok=True)
            page.screenshot(path=str(artifacts / "agent-desktop.png"), full_page=True)
            page.set_viewport_size({"width": 390, "height": 844})
            assert page.evaluate("document.documentElement.scrollWidth <= innerWidth"), "mobile overflow"
            page.screenshot(path=str(artifacts / "agent-mobile.png"), full_page=True)
            browser.close()
            print("PASS: single-Agent protocol/preset config, budgets, approval, read/apply drafts, Bot switching and mobile layout")
    finally:
        server.shutdown()
        thread.join()
        server.server_close()


if __name__ == "__main__":
    main()
