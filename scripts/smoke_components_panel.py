"""Real Edge/Chromium checks of shipped component forms and API response races.

The local server serves the actual UI files. Only management API responses and
the periodic poll clock are controlled; this is not external service acceptance.
"""

from __future__ import annotations

import argparse
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from playwright.sync_api import expect, sync_playwright

from forest.memory_settings import DEFAULT_MEMORY_SETTINGS

BOOTSTRAP = r"""
const memoryDefaults = __MEMORY_DEFAULTS__;
const fields = {type:'object',additionalProperties:false,required:['label','enabled','mode','tags'],properties:{
  label:{type:'string',minLength:1},enabled:{type:'boolean'},mode:{type:'string',enum:['fast','precise']},
  note:{type:'string'},tags:{type:'array',items:{type:'string'}}}};
const input = {type:'object',additionalProperties:false,required:['query'],properties:{query:{type:'string',default:'initial request'}}};
const pkg = (id, component, version) => ({package_digest:id,package_id:component,component_id:component,
  version,state_schema:1,state_migrations:[],config_schema:fields,grants:[],
  services:[{name:component+'.first',input_schema:input},{name:component+'.second',input_schema:input}]});
const packages = [pkg('pkg-a','fixture.alpha','1.0.0'),pkg('pkg-b','fixture.beta','1.0.0'),pkg('pkg-a-old','fixture.alpha','0.9.0')];
const values = label => ({label,enabled:true,mode:'fast',tags:['original']});
const state = (component, label, digest) => ({desired_revision:1,effective_revision:1,
  desired_values:{package_digest:digest,config:values(label),grants:[]},effective_values:{package_digest:digest,config:values(label),grants:[]},
  desired_values_payload_state:'ACTIVE',effective_values_payload_state:'ACTIVE',
  defaults:{package_digest:'',config:{},grants:[]},
  operations:[],runtime:{components:[{component_id:component,worker_state:'ACTIVE',loaded_revision:1}],error_code:null}});
window.fixture = {packages,states:{'fixture.alpha':state('fixture.alpha','Alpha','pkg-a'),
  'fixture.beta':state('fixture.beta','Beta','pkg-b')},hold:[],pending:[],calls:[],plans:{},unavailable:[],poll:null};
fixture.bot = {bot_id:'bot-a',display_name:'Browser fixture',stage:'READY',revision:1,job_id:'job-a'};
function reply(path, method, body) {
  if (fixture.unavailable.includes(path)) return {operation_id:'recorded-operation',action:'config.apply',
    operation_status:'RECORDED',result_payload_state:'UNAVAILABLE'};
  if (path==='/setup') return {setup_required:false,database_ready:true};
  if (path==='/session') return {username:'fixture-admin'};
  if (path==='/configuration') return {head_revision:1,defaults:{display_name:'Forest fixture',session_ttl_minutes:60}};
  if (path==='/bots') return {bots:[fixture.bot],managed_available:true};
  if (path==='/bots/bot-a') return fixture.bot;
  if (path==='/provision-jobs/job-a') return {events:[]};
  if (path==='/packages') return method==='POST' ? packages[0] : {packages:fixture.packages,errors:{}};
  if (path.endsWith('/agent/configuration')) return {revision:0,values:{enabled:false,allowed_tools:[]},has_api_key:false};
  if (path.endsWith('/agent/runs')) return {runs:[],approvals:[]};
  if (path.endsWith('/memory/configuration')) return {revision:0,values:memoryDefaults,has_api_key:false,secret_revision:0};
  if (path.endsWith('/memory/status')) return {configured:false,enabled:false,revision:0,counts:{facets:{},captures:{},attempts:{}},budget:null,errors:{}};
  if (path.endsWith('/jobs')) return {jobs:[],occurrences:[]};
  if (path.endsWith('/bindings')) return {bindings:[]};
  if (path.endsWith('/conversations')) return {conversations:[]};
  if (path.endsWith('/delivery')) return {parts:[]};
  const component = path.match(/\/components\/([^/]+)/)?.[1], current=fixture.states[component];
  if (path.endsWith('/configuration')) return current;
  if (path.includes('/configuration/history')) return {next_before:null,revisions:[{revision:1,
    created_at:'2026-09-13T00:00:00Z',values_payload_state:'ACTIVE',
    values:{package_digest:'pkg-a-old',config:values('Historical draft'),grants:[]}}]};
  if (path.endsWith('/change-plans')) {
    const plan={plan_id:'plan-'+fixture.calls.length,plan_digest:'digest-'+fixture.calls.length,
      expected_revision:body.expected_revision,values:body.values};
    fixture.plans[plan.plan_id]=structuredClone(plan); return plan;
  }
  if (path.endsWith('/management-requests')) {
    current.desired_revision++; current.desired_values=structuredClone(fixture.plans[body.plan_id].values);
    return {revision:current.desired_revision};
  }
  if (path.endsWith('/invoke')) return {operation_id:'fixture-invoke',result:{service:body.service,query:body.payload.query}};
  throw Error('Unexpected fixture API '+method+' '+path);
}
const originalFetch=window.fetch.bind(window);
window.fetch=async(url,options={}) => {
  const path=String(url).replace('/management/v1',''), method=options.method||'GET';
  if (!String(url).startsWith('/management/v1')) return originalFetch(url,options);
  const body=options.body instanceof Blob ? {archive_size:options.body.size} : options.body ? JSON.parse(options.body) : undefined;
  fixture.calls.push({path,method,body:structuredClone(body),headers:structuredClone(options.headers)});
  const result=structuredClone(reply(path,method,body)), key=method+' '+path;
  const held=fixture.hold.indexOf(key);
  if (held>=0) {
    fixture.hold.splice(held,1);
    await new Promise(resolve=>fixture.pending.push({key,resolve,result}));
  }
  return new Response(JSON.stringify(result),{status:200,headers:{'Content-Type':'application/json'}});
};
// Tests advance explicit operations without a periodic request stealing a hold.
window.setInterval=(callback,ms)=>{fixture.poll=callback; return 1;};
await import('/static/app.js');
window.fixtureReady=true;
""".replace("__MEMORY_DEFAULTS__", json.dumps(DEFAULT_MEMORY_SETTINGS))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--browser", type=Path)
    args = parser.parse_args()
    static = Path(__file__).resolve().parents[1] / "src" / "forest" / "management" / "static"
    html = (static / "index.html").read_text(encoding="utf-8").replace(
        '<script type="module" src="/static/app.js"></script>',
        '<script type="module" src="/fixture.js"></script>',
    )

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            resources = {"/": (html.encode(), "text/html; charset=utf-8"),
                         "/fixture.js": (BOOTSTRAP.encode(), "text/javascript")}
            for name in ("app.js", "schema-form.js", "configuration-forms.js", "control-receipts.js", "bindings.js", "agent-panel.js", "jobs-panel.js", "memory.js", "style.css"):
                resources["/static/" + name] = ((static / name).read_bytes(),
                    "text/css" if name.endswith(".css") else "text/javascript")
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
                args=["--no-proxy-server", "--disable-gpu"],
                **({"executable_path": str(args.browser)} if args.browser else {}))
            page = browser.new_page(viewport={"width": 1440, "height": 1080})
            errors = []
            page.on("pageerror", lambda error: errors.append(str(error)))
            page.goto(f"http://127.0.0.1:{server.server_port}")
            page.wait_for_function("window.fixtureReady")
            expect(page.locator("#configuration")).to_be_visible()
            page.locator("#nav-bots").click()
            page.locator('[data-bot-id="bot-a"]').click()
            label = page.locator('#bot-config-fields [name="label"]')
            expect(label).to_have_value("Alpha")
            # Bot selection must finish all installed panels, not silently stop
            # at a missing fixture endpoint before reaching component race checks.
            expect(page.locator('#memory-config-status')).to_contain_text('配置版本 0')
            expect(page.locator('#memory-state')).to_contain_text('"configured": false')

            def hold(key):
                page.evaluate("key => fixture.hold.push(key)", key)

            def release(key):
                page.wait_for_function("key => fixture.pending.some(item=>item.key===key)", arg=key)
                page.evaluate("key => {const index=fixture.pending.findIndex(item=>item.key===key); fixture.pending.splice(index,1)[0].resolve();}", key)

            alpha = "/bots/bot-a/components/fixture.alpha"
            # Import's package refresh waits while the user chooses another form.
            hold("GET /packages")
            page.locator("#bots > details summary").click()
            page.locator('#package-import-form [name="archive"]').set_input_files(
                {"name": "fixture.zip", "mimeType": "application/zip", "buffer": b"fixture-bytes"})
            page.locator('#package-import-form button[type="submit"]').click()
            page.wait_for_function("fixture.pending.some(item=>item.key==='GET /packages')")
            page.locator("#bot-package").select_option("pkg-b")
            expect(label).to_have_value("Beta")
            label.fill("Beta edited while package list waits")
            release("GET /packages")
            expect(page.locator('#package-import-form button[type="submit"]')).to_be_enabled()
            expect(page.locator("#bot-package")).to_have_value("pkg-b")
            expect(label).to_have_value("Beta edited while package list waits")
            page.locator("#bot-package").select_option("pkg-a")
            expect(label).to_have_value("Alpha")

            # Typed values survive an old explicit read and an accepted apply.
            hold("GET " + alpha + "/configuration")
            page.locator("#reload-bot-config").click()
            page.wait_for_function("fixture.pending.length===1")
            label.fill("Edited during read")
            release("GET " + alpha + "/configuration")
            expect(page.locator("#reload-bot-config")).to_be_enabled()
            expect(label).to_have_value("Edited during read")
            page.locator('#bot-config-form button[type="submit"]').click()
            expect(page.locator("#bot-config-preview")).to_be_visible()
            assert page.evaluate("fixture.calls.filter(call=>call.path.endsWith('/change-plans')).at(-1).body.source_revision") == 1
            hold("POST " + alpha + "/management-requests")
            page.locator("#bot-config-apply").click()
            page.wait_for_function("fixture.pending.length===1")
            label.fill("New draft while apply waits")
            release("POST " + alpha + "/management-requests")
            expect(page.locator("#bot-config-apply")).to_be_enabled()
            expect(label).to_have_value("New draft while apply waits")

            # The actual generated form preserves JSON types and optional omission.
            page.locator('#bot-config-fields [name="enabled"]').uncheck()
            page.locator('#bot-config-fields [name="mode"]').select_option("1")
            page.get_by_text("设置 note（可选）", exact=True).locator("input").check()
            page.locator('#bot-config-fields [name="note"]').fill("optional note")
            page.locator('#bot-config-fields [name="tags"]').fill('["text", "image"]')
            page.locator('#bot-config-form button[type="submit"]').click()
            expect(page.locator("#bot-config-preview")).to_be_visible()
            submitted = page.evaluate("fixture.calls.filter(call=>call.path.endsWith('/change-plans')).at(-1).body.values.config")
            assert submitted == {"label": "New draft while apply waits", "enabled": False,
                                 "mode": "precise", "note": "optional note", "tags": ["text", "image"]}
            page.get_by_text("设置 note（可选）", exact=True).locator("input").uncheck()
            expect(page.locator('#bot-config-fields [name="note"]')).to_be_disabled()
            before = page.evaluate("fixture.calls.filter(call=>call.path.endsWith('/change-plans')).length")
            page.locator('#bot-config-fields [name="tags"]').fill('[broken JSON')
            page.locator('#bot-config-form button[type="submit"]').click()
            expect(page.locator('#bot-config-form button[type="submit"]')).to_be_enabled()
            assert page.evaluate("fixture.calls.filter(call=>call.path.endsWith('/change-plans')).length") == before
            page.locator('#bot-config-fields [name="tags"]').fill('[]')
            page.locator('#bot-config-form button[type="submit"]').click()
            expect(page.locator("#bot-config-preview")).to_be_visible()
            assert "note" not in page.evaluate("fixture.calls.filter(call=>call.path.endsWith('/change-plans')).at(-1).body.values.config")

            query = page.locator('#bot-invoke-fields [name="query"]')
            result = page.locator("#bot-invoke-result")
            invoke = page.locator('#bot-invoke-form button[type="submit"]')
            # Service change and input edits independently invalidate old results.
            for change in ("service", "input"):
                query.fill("old " + change)
                hold("POST " + alpha + "/invoke")
                invoke.click()
                page.wait_for_function("fixture.pending.length===1")
                if change == "service":
                    page.locator("#bot-service").select_option("fixture.alpha.second")
                else:
                    query.fill("new input during old request")
                release("POST " + alpha + "/invoke")
                expect(invoke).to_be_enabled()
                expect(result).to_have_text("")
            query.fill("current result")
            invoke.click()
            expect(result).to_contain_text("current result")
            expect(result).to_contain_text("fixture.alpha.second")

            # History creates a draft and invalidates the pending old invocation.
            query.fill("old invocation before history")
            hold("POST " + alpha + "/invoke")
            invoke.click()
            page.wait_for_function("fixture.pending.length===1")
            page.locator("#read-bot-history").click()
            page.locator("#bot-history button").click()
            expect(label).to_have_value("Historical draft")
            expect(page.locator("#bot-package")).to_have_value("pkg-a-old")
            release("POST " + alpha + "/invoke")
            expect(invoke).to_be_enabled()
            expect(result).to_have_text("")
            assert page.evaluate("fixture.pending.length") == 0

            # An erased replay is still the recorded operation. Clear the old
            # preview/body, discard both earlier reads, and never auto-reapply.
            page.locator('#bot-config-form button[type="submit"]').click()
            expect(page.locator('#bot-config-preview')).to_be_visible()
            hold('GET ' + alpha + '/configuration')
            page.locator('#reload-bot-config').click()
            hold('GET ' + alpha + '/configuration/history')
            page.locator('#read-bot-history').click()
            page.wait_for_function('fixture.pending.length===2')
            page.evaluate('path => fixture.unavailable.push(path)', alpha + '/management-requests')
            posts = page.evaluate("fixture.calls.filter(c=>c.method==='POST').length")
            page.locator('#bot-config-apply').click()
            expect(page.locator('#notice')).to_contain_text('原操作已记录')
            expect(page.locator('#notice')).to_contain_text('recorded-operation')
            expect(page.locator('#bot-config-preview')).to_be_hidden()
            expect(page.locator('#bot-config-diff')).to_have_text('')
            expect(page.locator('#bot-config-fields')).to_be_empty()
            release('GET ' + alpha + '/configuration')
            release('GET ' + alpha + '/configuration/history')
            expect(page.locator('#reload-bot-config')).to_be_enabled()
            expect(page.locator('#read-bot-history')).to_be_enabled()
            expect(page.locator('#bot-config-fields')).to_be_empty()
            expect(page.locator('#bot-history')).to_be_empty()
            assert page.evaluate("fixture.calls.filter(c=>c.method==='POST').length") == posts + 1
            # A fresh explicit read is permitted and another component remains
            # independent of the old operation's unavailable receipt.
            page.evaluate('fixture.unavailable=[]')
            page.locator('#reload-bot-config').click()
            expect(label).to_have_value('Edited during read')
            page.locator('#bot-package').select_option('pkg-b')
            expect(label).to_have_value('Beta')
            assert errors == [], errors
            browser.close()
        print("Component panel browser checks passed: package refresh races, preserved drafts, stale invocation/history fencing, typed forms, and unavailable recorded receipts without re-execution.")
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


if __name__ == "__main__":
    main()
