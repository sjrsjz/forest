"""Real-browser Binding panel race tests with controlled API response ordering.

This exercises the actual shipped DOM/module, without touching a deployment or
claiming real QQ integration. Run with --browser for a local Chromium/Edge path.
"""

from __future__ import annotations

import argparse
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from playwright.sync_api import expect, sync_playwright

BOOTSTRAP = r"""
import {bindingsPanel} from '/static/bindings.js';
const values = {enabled:true,api_base_url:'http://127.0.0.1:5700',response_mode:'component',
  response_template:'original',trigger_mentions:true,trigger_prefix:'/chat',private_trigger:true,admin_user_ids:['123']};
const row = {binding_id:'binding-a',self_id:'123',display_name:'Fixture',state:'ONLINE',
  revision:1,effective_revision:1,registration_stage:'REGISTERED',desired_values:values,
  webhook_url:'http://127.0.0.1/adapters/onebot/v11/binding-a'};
const part = {intent_id:'intent-a',binding_id:'binding-a',part_index:0,part_operation_id:'part-op',
  operation_id:'reply-op',root_operation_id:'run-a',send_lane:'CHAT',state:'UNKNOWN',revision:4,
  attempt_count:1,attempt_id:'attempt-a',payload:[{type:'text',text:'Precise original <img src=bad> body'}],
  reply_route:{self_id:'123',channel_kind:'group',channel_id:'456'},
  created_at:'2026-09-13T01:00:00Z',permitted_at:'2026-09-13T01:00:01Z',started_at:'2026-09-13T01:00:02Z'};
window.fixture = {row,parts:[part],calls:[],hold:[],pending:[],original:structuredClone(part)};
function reply(path, body) {
  if (path.endsWith('/bindings')) return {bindings:[fixture.row]};
  if (path.endsWith('/conversations')) return {conversations:[]};
  if (path.endsWith('/delivery')) return {parts:fixture.parts};
  if (path.endsWith('/configuration')) return fixture.row;
  if (path.endsWith('/change-plans')) return {plan_id:'plan-a',plan_digest:'digest',expected_revision:body.expected_revision,values:body.values,credentials_changed:true};
  if (path.endsWith('/management-requests')) {
    fixture.row.revision++; return {revision:fixture.row.revision};
  }
  if (path.endsWith('/resolution')) return {state:'SENT',revision:body.expected_revision+1};
  throw Error('Unexpected fixture path');
}
const api = async (path,body,headers) => {
  fixture.calls.push({path,body:structuredClone(body),headers:structuredClone(headers)});
  const result = structuredClone(reply(path,body));
  const held = fixture.hold.indexOf(path);
  if (held >= 0) {
    fixture.hold.splice(held,1);
    return await new Promise((resolve,reject) => fixture.pending.push({path,result,resolve,reject}));
  }
  return result;
};
const notice = text => document.getElementById('notice').textContent = text;
const busy = async(button, work) => {button.disabled=true; try {await work();} catch(error) {notice(error.message);} finally {button.disabled=false;}};
window.panel = bindingsPanel(api,busy,notice);
document.getElementById('bots').classList.remove('hidden');
document.getElementById('bot-detail').classList.remove('hidden');
window.ready = panel.select({bot_id:'bot-a',stage:'READY'}).then(() => true);
"""


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--browser", type=Path)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    static = root / "src" / "forest" / "management" / "static"
    html = (
        (static / "index.html")
        .read_text(encoding="utf-8")
        .replace(
            '<script type="module" src="/static/app.js"></script>',
            '<script type="module" src="/fixture.js"></script>',
        )
    )

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            resources = {
                "/": (html.encode(), "text/html; charset=utf-8"),
                "/fixture.js": (BOOTSTRAP.encode(), "text/javascript"),
                "/static/bindings.js": ((static / "bindings.js").read_bytes(), "text/javascript"),
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
            page = browser.new_page(viewport={"width": 1440, "height": 1080})
            errors = []
            page.on("pageerror", lambda error: errors.append(str(error)))
            page.goto(f"http://127.0.0.1:{server.server_port}")
            page.wait_for_function("window.ready")
            page.evaluate("() => window.ready")
            expect(page.locator("#delivery-list pre")).to_contain_text(
                "Precise original <img src=bad> body"
            )
            expect(page.locator("#delivery-list pre")).to_contain_text("2026-09-13T01:00:01Z")
            assert page.locator("#delivery-list img").count() == 0

            # A late old refresh must not restore an UNKNOWN result over SENT.
            page.evaluate(
                "() => {fixture.hold.push('/bots/bot-a/delivery'); window.oldRefresh=panel.refresh();}"
            )
            page.wait_for_function("fixture.pending.length===1")
            page.evaluate("async () => {fixture.parts[0].state='SENT'; await panel.refresh();}")
            page.evaluate(
                "async () => {const old=fixture.pending.shift(); old.resolve(old.result); await window.oldRefresh;}"
            )
            expect(page.locator("#delivery-list")).to_contain_text("SENT")
            assert page.locator("#delivery-list form").count() == 0

            page.locator('[data-binding-id="binding-a"]').click()
            template = page.locator('#binding-config [name="response_template"]')
            expect(template).to_have_value("original")
            template.fill("submitted")
            page.locator('#binding-config button[type="submit"]').click()
            expect(page.locator("#binding-preview")).to_be_visible()
            page.evaluate("fixture.hold.push('/bots/bot-a/bindings/binding-a/management-requests')")
            page.locator("#apply-binding").click()
            page.wait_for_function("fixture.pending.length===1")
            template.fill("new draft during apply")
            token = page.locator('#binding-config [name="access_token"]')
            token.fill("ephemeral-draft-token")
            page.evaluate("() => {const held=fixture.pending.shift(); held.resolve(held.result);}")
            expect(page.locator("#notice")).to_contain_text("新输入已保留")
            expect(template).to_have_value("new draft during apply")
            expect(token).to_have_value("ephemeral-draft-token")

            # A delayed explicit reload also preserves input entered after it.
            page.evaluate("fixture.hold.push('/bots/bot-a/bindings/binding-a/configuration')")
            page.locator("#reload-binding").click()
            page.wait_for_function("fixture.pending.length===1")
            template.fill("typed during reload")
            page.evaluate("() => {const held=fixture.pending.shift(); held.resolve(held.result);}")
            expect(page.locator("#notice")).to_contain_text("读取期间")
            expect(template).to_have_value("typed during reload")

            # Polling preserves the active form and its exact request snapshot.
            page.evaluate(
                "async () => {fixture.parts=[structuredClone(fixture.original)]; await panel.refresh();}"
            )
            reason = page.locator('#delivery-list [name="reason"]')
            decision = page.locator('#delivery-list [name="decision"]')
            reason.fill("Checked exact platform history")
            reason.focus()
            page.evaluate(
                "() => {window.originalForm=document.querySelector('#delivery-list form'); return panel.refresh();}"
            )
            assert page.evaluate(
                "window.originalForm===document.querySelector('#delivery-list form')"
            )
            expect(reason).to_be_focused()
            decision.select_option("NOT_SENT")
            resolution = "/bots/bot-a/delivery/intent-a/0/resolution"
            page.evaluate("path => fixture.hold.push(path)", resolution)
            page.locator('#delivery-list button[type="submit"]').click()
            page.wait_for_function("fixture.pending.length===1")
            await_refresh = "async () => {await panel.refresh(); document.querySelector('#delivery-list form').dispatchEvent(new Event('submit',{cancelable:true}));}"
            page.evaluate(await_refresh)
            expect(reason).to_be_disabled()
            expect(decision).to_be_disabled()
            assert (
                page.evaluate(
                    "fixture.calls.filter(call=>call.path.endsWith('/resolution')).length"
                )
                == 1
            )
            page.evaluate(
                "() => {fixture.parts[0].state='SENT'; const held=fixture.pending.shift(); held.resolve(held.result);}"
            )
            expect(page.locator("#delivery-list form")).to_have_count(0)
            page.evaluate(
                "async () => {fixture.parts=[{...structuredClone(fixture.original),revision:8,attempt_count:2,attempt_id:'attempt-b'}]; await panel.refresh();}"
            )
            expect(page.locator('#delivery-list [name="decision"]')).to_have_value("")
            expect(page.locator('#delivery-list [name="reason"]')).to_have_value("")
            page.locator('#delivery-list [name="decision"]').select_option("SKIP")
            page.locator('#delivery-list [name="reason"]').fill("New attempt reviewed")
            page.locator('#delivery-list button[type="submit"]').click()
            page.wait_for_function(
                "fixture.calls.filter(call=>call.path.endsWith('/resolution')).length===2"
            )
            submitted = page.evaluate(
                "fixture.calls.filter(call=>call.path.endsWith('/resolution')).map(call=>({revision:call.body.expected_revision,key:call.headers['Idempotency-Key']}))"
            )
            assert [item["revision"] for item in submitted] == [4, 8]
            assert submitted[0]["key"] != submitted[1]["key"]
            assert errors == [], errors
            browser.close()
        print(
            "Binding panel browser checks passed: out-of-order responses, preserved drafts, frozen resolution requests and reviewable content."
        )
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


if __name__ == "__main__":
    main()
