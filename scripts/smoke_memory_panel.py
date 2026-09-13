"""Real Edge/Chromium checks of the shipped memory form and delayed API replies."""

import argparse
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from playwright.sync_api import expect, sync_playwright

from forest.memory_settings import DEFAULT_MEMORY_SETTINGS

BOOTSTRAP = r"""
import {memoryPanel} from '/static/memory.js';
window.fixture = {calls:[],hold:[],pending:[],plans:{},receipts:{},
  row:{revision:1,secret_revision:1,has_api_key:true,values:DEFAULT_VALUES}};
const api = async (path,body,headers) => {
  fixture.calls.push({path,body:structuredClone(body),headers:structuredClone(headers)});
  let result;
  if(path.endsWith('/configuration')) result=structuredClone(fixture.row);
  else if(path.endsWith('/status')) result={state:'DISABLED',last_error:null};
  else if(path.endsWith('/change-plans')) {
    result={plan_id:'plan-'+fixture.calls.length,plan_digest:'digest',expected_revision:body.expected_revision,
      values:body.values,before_values:fixture.row.values,credentials_changed:body.api_key!==null||body.clear_api_key};
    fixture.plans[result.plan_id]=structuredClone(result);
  } else if(path.endsWith('/management-requests')) {
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
window.panel=memoryPanel(api,busy,notice);
document.getElementById('bots').classList.remove('hidden');
document.getElementById('bot-detail').classList.remove('hidden');
window.ready=panel.select({bot_id:'bot-a',stage:'READY'}).then(()=>true);
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
            expect(budget).to_have_value("200")
            expect(page.locator("#memory-state")).to_contain_text("DISABLED")
            page.locator('#memory-config [name="dimensions"]').select_option("1024")
            page.locator('#memory-config [name="enabled"]').check()
            page.locator('#memory-config [name="media_allowed_origins"]').fill(
                "https://images.example.com"
            )
            key.fill("private-browser-key")
            page.locator('#memory-config button[type="submit"]').click()
            expect(page.locator("#memory-preview")).to_be_visible()
            assert "private-browser-key" not in page.locator("#memory-diff").inner_text()
            sent = page.evaluate(
                "fixture.calls.filter(c=>c.path.endsWith('/change-plans')).at(-1).body"
            )
            assert sent["values"]["dimensions"] == 1024 and sent["values"]["enabled"]
            assert sent["values"]["media_allowed_origins"] == ["https://images.example.com"]

            # An old read cannot replace edits made while it was in flight.
            page.evaluate("fixture.hold.push('/bots/bot-a/memory/configuration')")
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
            page.evaluate("fixture.hold.push('/bots/bot-a/memory/management-requests')")
            page.locator("#memory-apply").click()
            page.wait_for_function("fixture.pending.length===1")
            batch.fill("7")
            key.fill("new-draft-secret")
            page.evaluate("() => {const item=fixture.pending.shift();item.resolve(item.result);}")
            expect(page.locator("#memory-apply")).to_be_enabled()
            expect(batch).to_have_value("7")
            expect(key).to_have_value("new-draft-secret")

            # Changing Bot invalidates the old Bot's delayed read.
            page.evaluate("fixture.hold.push('/bots/bot-a/memory/configuration')")
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
