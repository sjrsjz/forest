"""Real Edge/Chromium checks of the shipped memory form and delayed API replies."""

import argparse
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from playwright.sync_api import expect, sync_playwright

from forest.memory_settings import DEFAULT_MEMORY_SETTINGS

BOOTSTRAP = r"""
import {runtimeModuleViews} from '/static/runtime-module-views.js';
window.fixture = {calls:[],hold:[],pending:[],plans:{},receipts:{},associations:[],
  row:{revision:1,secret_revision:1,has_api_key:true,values:DEFAULT_VALUES},
  sources:[
    {source_unit_id:'source-one',source_revision:1,kind:'INPUT',source:{channel_id:'group-one'},
      author:{display_name:'Alice'},parts:[{type:'text',text:'first memory'}]},
    {source_unit_id:'source-two',source_revision:1,kind:'INPUT',source:{channel_id:'group-two'},
      author:{display_name:'Bob'},parts:[{type:'text',text:'second memory'}]},
  ]};
const api = async (path,body,headers) => {
  fixture.calls.push({path,body:structuredClone(body),headers:structuredClone(headers)});
  let result;
  if(path.endsWith('/configuration')) result=structuredClone(fixture.row);
  else if(path.endsWith('/status')) result={state:'DISABLED',last_error:null};
  else if(path.includes('/sources?')) result={deletion_epoch:0,fact_revision:2,
    sources:structuredClone(fixture.sources),next_after:null,scrub:{revoked_payloads:0,pending_links:0}};
  else if(path.includes('/trunks?')) result={trunks:[
    {group_source_id:'trunk-one',channel_id:'group-one',stored_sources:1,registered_sources:1,authors:1,image_sources:0},
    {group_source_id:'trunk-two',channel_id:'group-two',stored_sources:1,registered_sources:1,authors:1,image_sources:0},
  ],next_after:null};
  else if(path.includes('/associations?')) result={deletion_epoch:0,fact_revision:2,graph_revision:1,
    associations:structuredClone(fixture.associations),next_after:null};
  else if(path.includes('/graph.scopes?')) result={graph_revision:1,scopes:[],next_after:null};
  else if(path.endsWith('/actions/association.plan')) {
    const endpoints=body.active ? body.source_unit_ids.map(id=>fixture.sources.find(row=>row.source_unit_id===id))
      : [fixture.associations.find(row=>row.association_id===body.association_id).left,
         fixture.associations.find(row=>row.association_id===body.association_id).right];
    result={plan_id:'association-plan-'+fixture.calls.length,plan_digest:'association-digest',
      association_id:body.association_id||'association-one',expected_revision:body.expected_revision,
      expected_deletion_epoch:body.expected_deletion_epoch,active:body.active,
      left:structuredClone(endpoints[0]),right:structuredClone(endpoints[1]),expires_at:'2030-01-01T00:00:00Z'};
    fixture.plans[result.plan_id]=structuredClone(result);
  } else if(path.endsWith('/actions/association.apply')) {
    const plan=fixture.plans[body.plan_id],key=headers['Idempotency-Key'];
    if(!fixture.receipts[key]) {
      const row={association_id:plan.association_id,revision:plan.expected_revision+1,active:plan.active,
        projection_state:'PENDING',left:plan.left,right:plan.right};
      fixture.associations=[row];
      fixture.receipts[key]={association_id:plan.association_id,revision:row.revision,
        active:plan.active,projection_state:'PENDING'};
    }
    result=fixture.receipts[key];
  }
  else if(path.endsWith('/actions/configuration.plan')) {
    result={plan_id:'plan-'+fixture.calls.length,plan_digest:'digest',expected_revision:body.expected_revision,
      values:body.values,before_values:fixture.row.values,credentials_changed:body.api_key!==null||body.clear_api_key};
    fixture.plans[result.plan_id]=structuredClone(result);
  } else if(path.endsWith('/actions/configuration.apply')) {
    const key=headers['Idempotency-Key'];
    if(!fixture.receipts[key]) {
      fixture.row.values=structuredClone(fixture.plans[body.plan_id].values);fixture.row.revision++;
      fixture.receipts[key]={revision:fixture.row.revision,secret_revision:1};
    }
    result=fixture.receipts[key];
  } else throw Error('Unexpected fixture API '+path);
  const index=fixture.hold.indexOf(path);
  if(index>=0) {fixture.hold.splice(index,1);return await new Promise((resolve,reject)=>fixture.pending.push({result,resolve,reject}));}
  return structuredClone(result);
};
const notice=text=>{document.getElementById('notice').textContent=text;};
const busy=async(button,work)=>{button.disabled=true;try{await work();}catch(error){notice(error.message);}finally{button.disabled=false;}};
document.getElementById('bots').classList.remove('hidden');
document.getElementById('bot-detail').classList.remove('hidden');
const nav=document.createElement('nav'),registry=document.createElement('div'),root=document.createElement('div');
document.getElementById('bot-detail').append(nav,registry,root);
window.panel=runtimeModuleViews({api,busy,notice,nav,registry,root,packageElements:[]});
panel.update([{component_id:'memory.graph',requires:[],management_view:{kind:'trusted-module',
  protocol:'forest.trusted-management-view/1',label:'图文记忆',
  template:'/static/modules/memory.graph.html',module:'/static/memory.js',
  style:'/static/modules/memory.graph.css'}}]);
window.ready=panel.select({bot_id:'bot-a',stage:'READY'}).then(()=>{panel.show('memory.graph');return true;});
"""


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--browser", type=Path)
    args = parser.parse_args()
    static = Path(__file__).resolve().parents[1] / "src/forest/management/static"
    html = (
        (static / "index.html")
        .read_text(encoding="utf-8")
        .replace(
            '<script type="module" src="/static/app.js"></script>',
            '<script type="module" src="/fixture.js"></script>',
        )
    )
    bootstrap = BOOTSTRAP.replace("DEFAULT_VALUES", json.dumps(DEFAULT_MEMORY_SETTINGS))

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            resources = {
                "/": (html.encode(), "text/html; charset=utf-8"),
                "/fixture.js": (bootstrap.encode(), "text/javascript"),
                "/static/memory.js": ((static / "memory.js").read_bytes(), "text/javascript"),
                "/static/runtime-module-views.js": (
                    (static / "runtime-module-views.js").read_bytes(), "text/javascript"
                ),
                "/static/modules/memory.graph.html": (
                    (static / "modules/memory.graph.html").read_bytes(),
                    "text/html; charset=utf-8",
                ),
                "/static/modules/memory.graph.css": (
                    (static / "modules/memory.graph.css").read_bytes(), "text/css"
                ),
                "/static/configuration-forms.js": ((static / "configuration-forms.js").read_bytes(), "text/javascript"),
                "/static/style.css": ((static / "style.css").read_bytes(), "text/css"),
            }
            if self.path not in resources:
                self.send_error(404)
                return
            content, kind = resources[self.path]
            self.send_response(200)
            self.send_header("Content-Type", kind)
            self.send_header("Content-Length", str(len(content)))
            self.end_headers()
            self.wfile.write(content)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(
                headless=True,
                args=["--no-proxy-server", "--disable-gpu"],
                **({"executable_path": str(args.browser)} if args.browser else {}),
            )
            page = browser.new_page(viewport={"width": 1440, "height": 1100})
            errors = []
            page.on("pageerror", lambda error: errors.append(str(error)))
            page.goto(f"http://127.0.0.1:{server.server_port}")
            page.wait_for_function("window.ready")
            page.evaluate("() => window.ready")
            for name in DEFAULT_MEMORY_SETTINGS:
                assert page.locator(f'#memory-config [name="{name}"]').count() == 1
            budget = page.locator('#memory-config [name="daily_request_budget"]')
            batch = page.locator('#memory-config [name="batch_size"]')
            key = page.locator('#memory-config [name="api_key"]')
            endpoint = page.locator('#memory-config [name="endpoint"]')
            expect(budget).to_have_value("200")
            for name in ("deployment_id", "model", "model_revision", "dimensions"):
                expect(page.locator(f'#memory-config [name="{name}"]')).to_be_disabled()
            expect(endpoint).to_be_enabled()
            assert page.locator(
                '#memory-config [name="model"] option[value="Qwen/Qwen3-VL-Embedding-8B"]'
            ).count() == 1
            expect(page.locator("#memory-state")).to_contain_text("DISABLED")
            assert page.locator(".memory-section[open]").count() == 1
            collapsed_height = page.evaluate("document.documentElement.scrollHeight")
            assert collapsed_height < 2500, collapsed_height
            sources = page.locator("#memory-sources input[type=checkbox]")
            expect(sources).to_have_count(2)
            expect(page.locator("#memory-association-plan")).to_be_disabled()
            sources.nth(0).check()
            expect(page.locator("#memory-association-plan")).to_be_disabled()
            sources.nth(1).check()
            expect(page.locator("#memory-association-plan")).to_be_enabled()
            page.locator("#memory-association-plan").click()
            expect(page.locator("#memory-association-preview")).to_be_visible()
            expect(page.locator("#memory-association-diff")).to_contain_text("group-one")
            page.locator("#memory-association-apply").click()
            page.get_by_text("跨主干联想与审核", exact=True).click()
            expect(page.locator("#memory-associations .memory-association")).to_have_count(1)
            expect(page.locator("#memory-associations-status")).to_contain_text("有效 1")
            page.locator("#memory-associations .memory-association button").click()
            expect(page.locator("#memory-association-preview")).to_be_visible()
            expect(page.locator("#memory-association-diff")).to_contain_text("解除联想")
            page.locator("#memory-association-apply").click()
            expect(page.locator("#memory-associations .memory-association")).to_have_count(1)
            expect(page.locator("#memory-associations-status")).to_contain_text("有效 0")
            expect(page.locator("#memory-associations .memory-association button")).to_contain_text("恢复")
            page.locator(".memory-section summary").first.click()
            endpoint.fill("https://embedding.example.com/v1/embeddings")
            page.locator('#memory-config [name="enabled"]').check()
            page.locator('#memory-config [name="semantic_associations_enabled"]').uncheck()
            page.locator('#memory-config [name="media_allowed_origins"]').fill(
                "https://images.example.com"
            )
            key.fill("private-browser-key")
            page.locator('#memory-config button[type="submit"]').click()
            expect(page.locator("#memory-preview")).to_be_visible()
            assert "private-browser-key" not in page.locator("#memory-diff").inner_text()
            sent = page.evaluate(
                "fixture.calls.filter(c=>c.path.endsWith('/actions/configuration.plan')).at(-1).body"
            )
            assert sent["values"]["dimensions"] == 512 and sent["values"]["enabled"]
            assert sent["values"]["endpoint"] == "https://embedding.example.com/v1/embeddings"
            assert not sent["values"]["semantic_associations_enabled"]
            assert sent["values"]["media_allowed_origins"] == ["https://images.example.com"]

            # An old read cannot replace edits made while it was in flight.
            page.evaluate("fixture.hold.push('/bots/bot-a/runtime-modules/memory.graph/queries/configuration')")
            page.locator("#memory-reload").click()
            page.wait_for_function("fixture.pending.length===1")
            budget.fill("301")
            page.evaluate("() => {const item=fixture.pending.shift();item.resolve(item.result);}")
            expect(page.locator("#memory-reload")).to_be_enabled()
            expect(budget).to_have_value("301")
            expect(key).to_have_value("private-browser-key")
            expect(page.locator("#memory-preview")).not_to_be_visible()

            # Preserve new draft and secret input while a previous plan applies.
            page.locator('#memory-config button[type="submit"]').click()
            expect(page.locator("#memory-preview")).to_be_visible()
            page.evaluate("fixture.hold.push('/bots/bot-a/runtime-modules/memory.graph/actions/configuration.apply')")
            page.locator("#memory-apply").click()
            page.wait_for_function("fixture.pending.length===1")
            batch.fill("7")
            key.fill("new-draft-secret")
            page.evaluate("() => {const item=fixture.pending.shift();item.resolve(item.result);}")
            expect(page.locator("#memory-apply")).to_be_enabled()
            expect(batch).to_have_value("7")
            expect(key).to_have_value("new-draft-secret")

            # Changing Bot invalidates the old Bot's delayed read.
            page.evaluate("fixture.hold.push('/bots/bot-a/runtime-modules/memory.graph/queries/configuration')")
            page.locator("#memory-reload").click()
            page.wait_for_function("fixture.pending.length===1")
            page.evaluate(
                "async()=>{fixture.row.values.batch_size=24;await panel.select({bot_id:'bot-b',stage:'READY'});}"
            )
            page.evaluate("() => {const item=fixture.pending.shift();item.resolve(item.result);}")
            expect(batch).to_have_value("24")
            expect(key).to_have_value("")
            assert not errors, errors
            browser.close()
        print(
            "Memory panel: real browser field, secret, delayed read/apply and Bot-switch checks passed"
        )
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


if __name__ == "__main__":
    main()
