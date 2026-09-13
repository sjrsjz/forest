"""Real-browser component-job filtering, replay, retirement and stale-read checks."""

from __future__ import annotations

import argparse
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from playwright.sync_api import expect, sync_playwright

BOOTSTRAP = r"""
import {componentJobsPanel} from '/static/component-jobs-panel.js';
const handler={component_id:'component.jobs',handler_id:'work.persist',generation_id:'generation-a',schema_digest:'sha256:schema',max_attempts:3,timeout_seconds:30,max_concurrency:1,active:true,management_revision:0,updated_at:'2030-01-01T00:00:00Z',retired_at:null};
const dead={job_id:'job-dead',job_seq:2,component_id:'component.jobs',handler_id:'work.persist',operation_id:'private-operation',status:'DEAD',attempt_count:3,dispatch_count:3,max_attempts:3,replay_epoch:0,management_revision:0,last_error_code:'PLUGIN_FAILED',payload_state:'ACTIVE',first_available_at:'2030-01-01T00:00:00Z',available_at:'2030-01-01T00:00:00Z',created_at:'2030-01-01T00:00:00Z',acked_at:null,retired_at:null,replay_allowed:true};
const acked={...dead,job_id:'job-acked',job_seq:1,status:'ACKED',attempt_count:1,dispatch_count:1,last_error_code:null,replay_allowed:false};
window.fixture={handlers:{a:[structuredClone(handler)],b:[]},jobs:{a:[structuredClone(dead),structuredClone(acked)],b:[]},calls:[],receipts:{},hold:[],pending:[],errors:[]};
function respond(path,body,headers) {
  const url=new URL(path,'http://fixture'), bot=url.pathname.split('/')[2];
  if(body===undefined) {
    const status=url.searchParams.get('status');
    return {handlers:structuredClone(fixture.handlers[bot]),jobs:structuredClone(fixture.jobs[bot].filter(row=>!status||row.status===status))};
  }
  const op=bot+url.pathname+headers['Idempotency-Key']; if(fixture.receipts[op]) return structuredClone(fixture.receipts[op]);
  let result;
  if(path.endsWith('/replays')) {
    const row=fixture.jobs[bot].find(item=>item.job_id===body.job_id);
    row.status='RETRY_WAIT';row.attempt_count=0;row.dispatch_count=0;row.replay_epoch++;row.management_revision++;row.replay_allowed=false;
    result={job_id:row.job_id,status:row.status,replay_epoch:row.replay_epoch,management_revision:row.management_revision};
  } else if(path.endsWith('/retirement-plans')) {
    result={plan_id:'plan-retire',plan_digest:'digest-retire',component_id:body.component_id,handler_id:body.handler_id,expected_revision:body.expected_revision,impact:{retire_pending:1,inflight:0,acked:1,dead:0,retired:0,total:2},expires_at:'2030-01-01T00:10:00Z'};
  } else if(path.endsWith('/retirement-requests')) {
    const row=fixture.handlers[bot][0];row.active=false;row.management_revision++;row.retired_at='2030-01-01T00:01:00Z';
    for(const job of fixture.jobs[bot]) if(['PENDING','RETRY_WAIT'].includes(job.status)){job.status='RETIRED';job.last_error_code='JOB_HANDLER_RETIRED';}
    result={component_id:row.component_id,handler_id:row.handler_id,active:false,management_revision:row.management_revision,retired_job_count:1};
  } else throw Error('Unknown fixture request '+path);
  fixture.receipts[op]=structuredClone(result);return result;
}
async function api(path,body,headers={}) {
  if(body!==undefined&&!headers['Idempotency-Key']) throw Error('Missing idempotency key');
  fixture.calls.push({path,body:structuredClone(body),headers});const result=respond(path,body,headers);
  if(fixture.hold.includes(path)) return await new Promise(resolve=>fixture.pending.push({path,resolve:()=>resolve(structuredClone(result))}));
  return structuredClone(result);
}
async function busy(button,work){button.disabled=true;try{return await work();}catch(error){fixture.errors.push(error.message);document.getElementById('notice').textContent=error.message;}finally{button.disabled=false;}}
const notice=text=>{document.getElementById('notice').textContent=text;};
window.panel=componentJobsPanel(api,busy,notice);document.getElementById('bots').classList.remove('hidden');document.getElementById('bot-detail').classList.remove('hidden');
window.ready=panel.select({bot_id:'a'});
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
                "/static/component-jobs-panel.js": (
                    (static / "component-jobs-panel.js").read_bytes(),
                    "text/javascript",
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
            page = browser.new_page(viewport={"width": 1380, "height": 1100})
            errors = []
            page.on("pageerror", lambda error: errors.append(str(error)))
            page.goto(f"http://127.0.0.1:{server.server_port}")
            page.evaluate("window.ready")
            expect(page.locator("#component-job-handlers")).to_contain_text("work.persist")
            expect(page.locator("#component-job-items")).to_contain_text("死信")
            expect(page.locator("#component-job-items")).not_to_contain_text(
                "private-job-body"
            )

            page.locator("#component-job-status-filter").select_option("DEAD")
            expect(page.locator("#component-job-items")).to_contain_text("job-dead")
            expect(page.locator("#component-job-items")).not_to_contain_text("job-acked")
            page.get_by_role("button", name="重放死信").click()
            replay = page.evaluate(
                "fixture.calls.find(call=>call.path.endsWith('/replays'))"
            )
            assert replay["body"] == {"job_id": "job-dead", "expected_revision": 0}
            assert replay["headers"]["Idempotency-Key"]
            expect(page.locator("#component-job-items")).to_contain_text("没有组件持久任务")

            page.get_by_role("button", name="预览处理器退役").click()
            expect(page.locator("#component-job-retirement-preview")).to_be_visible()
            expect(page.locator("#component-job-retirement-diff")).to_contain_text(
                "retire_pending"
            )
            page.locator("#component-job-retirement-apply").click()
            expect(page.locator("#component-job-handlers")).to_contain_text("已退役")
            retirement = page.evaluate(
                "fixture.calls.find(call=>call.path.endsWith('/retirement-requests'))"
            )
            assert retirement["body"]["plan_id"] == "plan-retire"
            assert retirement["headers"]["Idempotency-Key"]

            page.evaluate("panel.select({bot_id:'a'})")
            page.wait_for_function(
                "fixture.calls.at(-1).path==='/bots/a/component-jobs?limit=200'"
            )
            page.evaluate("fixture.hold.push('/bots/a/component-jobs?limit=200')")
            page.locator("#component-job-refresh").click()
            page.wait_for_function("fixture.pending.length===1")
            page.evaluate("panel.select({bot_id:'b'})")
            page.wait_for_function(
                "fixture.calls.at(-1).path==='/bots/b/component-jobs?limit=200'"
            )
            page.evaluate("fixture.pending.shift().resolve();fixture.hold=[]")
            expect(page.locator("#component-job-handlers")).to_contain_text(
                "当前 Bot 没有组件持久任务处理器"
            )
            assert errors == [], errors
            assert page.evaluate("fixture.errors") == []
            artifacts = root / ".local" / "browser-artifacts"
            artifacts.mkdir(parents=True, exist_ok=True)
            page.screenshot(path=str(artifacts / "component-jobs-desktop.png"), full_page=True)
            page.set_viewport_size({"width": 390, "height": 844})
            assert page.evaluate("document.documentElement.scrollWidth <= innerWidth")
            page.screenshot(path=str(artifacts / "component-jobs-mobile.png"), full_page=True)
            browser.close()
            print(
                "PASS: component-job filtering, replay, retirement, Bot-switch fencing "
                "and mobile layout"
            )
    finally:
        server.shutdown()
        thread.join()
        server.server_close()


if __name__ == "__main__":
    main()
