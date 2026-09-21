"""Render the shipped job workspace and verify terminal-state tracking."""

from __future__ import annotations

import argparse
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from playwright.sync_api import expect, sync_playwright

HTML = """<!doctype html><html lang="zh-CN"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<link rel="stylesheet" href="/static/style.css">
<link rel="stylesheet" href="/static/modules/clean-jobs.css">
<style>#root{height:100dvh;padding:12px}#notice{position:fixed;right:12px;bottom:12px;z-index:9}</style>
</head><body><div id="root"></div><output id="notice"></output>
<script type="module" src="/fixture.js"></script></body></html>"""

BOOTSTRAP = """
import {mount} from '/static/clean-jobs.js';
window.fixture={calls:[],errors:[],phase:'NONE',reads:0};
const root=document.getElementById('root');
root.innerHTML=await fetch('/static/modules/clean-jobs.html').then(response=>response.text());
const jobId='job-fixture-1';
function row(){return {job_id:jobId,job_type:'native.execute',state:fixture.phase,revision:fixture.phase==='FAILED'?3:2,attempt_count:1,max_attempts:1,updated_at:'2026-09-21T13:32:30Z',cancel_requested_at:null};}
async function api(path,options={}){
  fixture.calls.push({path,options:structuredClone(options)});
  if(path.endsWith('/jobs.types'))return {items:[{job_type:'native.execute',owner_component_id:'process.executor',target_generation_id:'generation/process',timeout_seconds:5,input_schema:{type:'object'}}]};
  if(path.endsWith('/jobs.query'))return {items:fixture.phase==='NONE'?[]:[row()],has_more:false,snapshot_created_at:'2026-09-21T13:32:30Z'};
  if(path.endsWith('/jobs.run')){fixture.phase='RUNNING';return {job_id:jobId};}
  if(path.endsWith('/jobs.read')){fixture.reads++;if(fixture.reads>=2)fixture.phase='FAILED';return {...row(),created_at:'2026-09-21T13:32:30Z',started_at:'2026-09-21T13:32:30Z',completed_at:fixture.phase==='FAILED'?'2026-09-21T13:32:31Z':null,error_code:fixture.phase==='FAILED'?'PROCESS_PROFILE_NOT_FOUND':null,input:{profile_id:'diagnostic.echo',arguments:{value:'forest-live-profile'}},result:null};}
  throw Error('Unexpected fixture request '+path);
}
function notice(message){document.getElementById('notice').textContent=message;}
window.ready=mount({root,component:{component_id:'job.scheduler'},api,notice}).catch(error=>{fixture.errors.push(error.message);throw error;});
"""


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--browser", type=Path)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    static = root / "src" / "forest" / "clean_management_static"

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            resources = {
                "/": (HTML.encode(), "text/html; charset=utf-8"),
                "/fixture.js": (BOOTSTRAP.encode(), "text/javascript; charset=utf-8"),
                "/static/style.css": ((static / "style.css").read_bytes(), "text/css"),
                "/static/clean-jobs.js": (
                    (static / "clean-jobs.js").read_bytes(),
                    "text/javascript; charset=utf-8",
                ),
                "/static/modules/clean-jobs.html": (
                    (static / "modules" / "clean-jobs.html").read_bytes(),
                    "text/html; charset=utf-8",
                ),
                "/static/modules/clean-jobs.css": (
                    (static / "modules" / "clean-jobs.css").read_bytes(),
                    "text/css",
                ),
            }
            if self.path not in resources:
                self.send_error(404)
                return
            body, media_type = resources[self.path]
            self.send_response(200)
            self.send_header("Content-Type", media_type)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_args):
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
            page = browser.new_page(viewport={"width": 1440, "height": 900})
            errors: list[str] = []
            page.on("pageerror", lambda error: errors.append(str(error)))
            page.goto(f"http://127.0.0.1:{server.server_port}")
            page.evaluate("window.ready")
            expect(page.locator("#job-run-help")).to_contain_text("process.executor")
            page.locator("#job-input").fill(
                '{"profile_id":"diagnostic.echo","arguments":{"value":"forest-live-profile"}}'
            )
            page.locator("#job-run").click()
            expect(page.locator(".job-state")).to_have_text("执行失败", timeout=5_000)
            expect(page.locator("#job-detail")).to_contain_text("PROCESS_PROFILE_NOT_FOUND")
            expect(page.locator("#job-detail")).to_contain_text("先打开 process.executor 工作区")
            expect(page.locator("#notice")).to_contain_text("任务执行失败")
            assert page.evaluate("fixture.reads") >= 2
            assert errors == [], errors
            assert page.evaluate("fixture.errors") == []
            assert page.evaluate("document.documentElement.scrollHeight===innerHeight")
            browser.close()
            print("PASS: job prerequisite hint, automatic terminal refresh and friendly failure detail")
    finally:
        server.shutdown()
        thread.join()
        server.server_close()


if __name__ == "__main__":
    main()
