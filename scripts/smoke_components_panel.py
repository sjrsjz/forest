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
const packages = [{...pkg('pkg-a','fixture.alpha','1.0.0'),message_handler_takeover:'compatible'},
  pkg('pkg-b','fixture.beta','1.0.0'),pkg('pkg-a-old','fixture.alpha','0.9.0')];
const values = label => ({label,enabled:true,mode:'fast',tags:['original']});
const commandLimits = {max_argument_bytes:16384,max_pending_bot:2048,max_pending_binding:512,
  max_pending_principal:8,principal_burst:8,principal_refill_seconds:2};
const handlerLimits = {max_event_bytes:262144,max_pending_bot:2048,max_pending_binding:512,
  max_pending_principal:8,max_handlers_per_chain:64,principal_burst:8,
  principal_refill_seconds:2,deadline_seconds:30};
const state = (component, label, digest) => ({desired_revision:1,effective_revision:1,
  desired_values:{package_digest:digest,config:values(label),grants:[]},effective_values:{package_digest:digest,config:values(label),grants:[]},
  desired_values_payload_state:'ACTIVE',effective_values_payload_state:'ACTIVE',
  defaults:{package_digest:'',config:{},grants:[]},
  operations:[],runtime:{components:[{component_id:component,worker_state:'ACTIVE',loaded_revision:1}],error_code:null}});
window.fixture = {packages,states:{'fixture.alpha':state('fixture.alpha','Alpha','pkg-a'),
  'fixture.beta':state('fixture.beta','Beta','pkg-b')},hold:[],pending:[],calls:[],plans:{},unavailable:[],poll:null};
fixture.bot = {bot_id:'bot-a',display_name:'Browser fixture',stage:'READY',revision:1,job_id:'job-a'};
fixture.runtime = {status:'ACTIVE',storage_generation:1,maintenance_epoch:1,policy_revision:1,
  data_admission:true,control_admission:true,supervisor:{attached:true,error_code:null,components:[]}};
function reply(path, method, body) {
  if (fixture.unavailable.includes(path)) return {operation_id:'recorded-operation',action:'config.apply',
    operation_status:'RECORDED',result_payload_state:'UNAVAILABLE'};
  if (path==='/setup') return {setup_required:false,database_ready:true};
  if (path==='/session') return {username:'fixture-admin'};
  if (path==='/configuration') return {head_revision:1,defaults:{display_name:'Forest fixture',session_ttl_minutes:60}};
  if (path==='/bots') return {bots:[fixture.bot],managed_available:true};
  if (path==='/bots/bot-a') return fixture.bot;
  if (path==='/bots/bot-a/runtime') return fixture.runtime;
  if (path==='/bots/bot-a/maintenance' && method==='POST') {
    if (body.expected_maintenance_epoch!==fixture.runtime.maintenance_epoch) throw Error('stale maintenance fixture');
    fixture.runtime={...fixture.runtime,maintenance_epoch:fixture.runtime.maintenance_epoch+1,
      data_admission:body.data_admission};
    return fixture.runtime;
  }
  if (path==='/provision-jobs/job-a') return {events:[]};
  if (path==='/packages') return method==='POST' ? packages[0] : {packages:fixture.packages,errors:{}};
  if (path.endsWith('/agent/configuration')) return {revision:0,values:{enabled:false,allowed_tools:[]},has_api_key:false};
  if (path.endsWith('/agent/runs')) return {runs:[],approvals:[]};
  if (path.endsWith('/memory/configuration')) return {revision:0,values:memoryDefaults,has_api_key:false,secret_revision:0};
  if (path.endsWith('/memory/status')) return {configured:false,enabled:false,revision:0,counts:{facets:{},captures:{},attempts:{}},budget:null,errors:{}};
  if (path.includes('/memory/sources?')) return {deletion_epoch:0,sources:[],next_after:null,scrub:{revoked_payloads:0,pending_links:0}};
  if (path.includes('/memory/associations?')) return {deletion_epoch:0,graph_revision:0,associations:[]};
  if (path.endsWith('/jobs')) return {jobs:[],occurrences:[]};
  if (path.endsWith('/bindings')) return {bindings:[]};
  if (path.endsWith('/conversations')) return {conversations:[]};
  if (path.endsWith('/delivery')) return {parts:[]};
  if (path.includes('/events?')) return {subscriptions:[],deliveries:[]};
  if (path.includes('/component-jobs?')) return {handlers:[],jobs:[]};
  if (path.endsWith('/message-handler-limits')) return {revision:0,values:handlerLimits,values_payload_state:'ACTIVE'};
  if (path.includes('/message-handlers/diagnostics?')) return {summary:{pending:0,claimed:0,
    claimed_call_admitted:0,claimed_call_completed:0,replied:0,failed:0,cancelled:0},chains:[],attempts:[]};
  if (path.endsWith('/business-command-limits')) return {revision:0,values:commandLimits,values_payload_state:'ACTIVE'};
  if (path.includes('/business-commands/diagnostics?')) return {summary:{pending_requests:0,
    claimed_requests:0,failed_requests:0,pending_classifications:0,pending_notices:0,claimed_notices:0},
    requests:[],classifications:[],notices:[]};
  if (path.includes('/audit?')) return {records:[],next_before:null};
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
            for asset in static.iterdir():
                if asset.is_file() and asset.suffix in {".js", ".css"}:
                    resources["/static/" + asset.name] = (
                        asset.read_bytes(),
                        "text/css" if asset.suffix == ".css" else "text/javascript",
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
                args=["--no-proxy-server", "--disable-gpu"],
                **({"executable_path": str(args.browser)} if args.browser else {}))
            page = browser.new_page(viewport={"width": 1440, "height": 1080})
            errors = []
            page.on("pageerror", lambda error: errors.append(str(error)))
            page.goto(f"http://127.0.0.1:{server.server_port}")
            try:
                page.wait_for_function("window.fixtureReady")
            except Exception as exc:
                raise AssertionError(f"Component fixture failed to initialize: {errors}") from exc
            expect(page.locator("#configuration")).to_be_visible()
            page.locator("#nav-bots").click()
            page.locator('[data-bot-id="bot-a"]').click()
            label = page.locator('#bot-config-fields [name="label"]')
            expect(label).to_have_value("Alpha")
            takeover = page.locator('#bot-handler-takeover')
            expect(takeover).to_be_enabled()
            expect(takeover).not_to_be_checked()
            expect(page.locator('#bot-handler-takeover-label')).to_contain_text('允许兼容版本接管')
            # Bot selection must finish all installed panels, not silently stop
            # at a missing fixture endpoint before reaching component race checks.
            expect(page.locator('#memory-config-status')).to_contain_text('配置版本 0')
            expect(page.locator('#memory-state')).to_contain_text('"configured": false')

            def hold(key):
                page.evaluate("key => fixture.hold.push(key)", key)

            def release(key):
                page.wait_for_function("key => fixture.pending.some(item=>item.key===key)", arg=key)
                page.evaluate("key => {const index=fixture.pending.findIndex(item=>item.key===key); fixture.pending.splice(index,1)[0].resolve();}", key)

            # A delayed old runtime read cannot overwrite a maintenance write.
            runtime_key = "GET /bots/bot-a/runtime"
            hold(runtime_key)
            page.locator("#bot-maintenance-refresh").click()
            page.wait_for_function("fixture.pending.some(item=>item.key==='GET /bots/bot-a/runtime')")
            page.locator('#bot-maintenance-form [name="reason"]').fill('provider maintenance')
            page.locator("#bot-maintenance-toggle").click()
            expect(page.locator("#bot-maintenance-status")).to_contain_text("业务已暂停 · 维护版本 2")
            release(runtime_key)
            expect(page.locator("#bot-maintenance-refresh")).to_be_enabled()
            expect(page.locator("#bot-maintenance-status")).to_contain_text("业务已暂停 · 维护版本 2")
            pause = page.evaluate("fixture.calls.filter(call=>call.path==='/bots/bot-a/maintenance').at(-1)")
            assert pause["body"] == {"expected_maintenance_epoch": 1, "data_admission": False,
                                     "reason": "provider maintenance"}
            page.locator('#bot-maintenance-form [name="reason"]').fill('provider recovered')
            page.locator("#bot-maintenance-toggle").click()
            expect(page.locator("#bot-maintenance-status")).to_contain_text("正在处理业务 · 维护版本 3")

            # Re-selecting this Bot while its write response is delayed must
            # refresh after the write instead of leaving runtime state hidden.
            maintenance_key = "POST /bots/bot-a/maintenance"
            hold(maintenance_key)
            page.locator('#bot-maintenance-form [name="reason"]').fill('second incident')
            page.locator("#bot-maintenance-toggle").click()
            page.wait_for_function(
                "fixture.pending.some(item=>item.key==='POST /bots/bot-a/maintenance')"
            )
            page.locator('[data-bot-id="bot-a"]').click()
            release(maintenance_key)
            expect(page.locator("#bot-maintenance-status")).to_contain_text(
                "业务已暂停 · 维护版本 4"
            )

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
            expect(takeover).to_be_disabled()
            expect(takeover).not_to_be_checked()
            label.fill("Beta edited while package list waits")
            release("GET /packages")
            expect(page.locator('#package-import-form button[type="submit"]')).to_be_enabled()
            expect(page.locator("#bot-package")).to_have_value("pkg-b")
            expect(label).to_have_value("Beta edited while package list waits")
            page.locator("#bot-package").select_option("pkg-a")
            expect(label).to_have_value("Alpha")
            expect(takeover).to_be_enabled()

            # Typed values survive an old explicit read and an accepted apply.
            hold("GET " + alpha + "/configuration")
            page.locator("#reload-bot-config").click()
            page.wait_for_function("fixture.pending.length===1")
            label.fill("Edited during read")
            release("GET " + alpha + "/configuration")
            expect(page.locator("#reload-bot-config")).to_be_enabled()
            expect(label).to_have_value("Edited during read")
            takeover.check()
            page.locator('#bot-config-form button[type="submit"]').click()
            expect(page.locator("#bot-config-preview")).to_be_visible()
            assert page.evaluate("fixture.calls.filter(call=>call.path.endsWith('/change-plans')).at(-1).body.source_revision") == 1
            assert page.evaluate("fixture.calls.filter(call=>call.path.endsWith('/change-plans')).at(-1).body.values.message_handler_takeover") is True
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
        print(
            "Component panel browser checks passed: maintenance read/write and reselection races, package refresh "
            "races, Handler takeover opt-in, preserved drafts, stale invocation/history fencing, typed forms, and "
            "unavailable recorded receipts without re-execution."
        )
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


if __name__ == "__main__":
    main()
