"""Render the shipped process executor workspace against a controlled API."""

from __future__ import annotations

import argparse
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from playwright.sync_api import expect, sync_playwright

HTML = """<!doctype html><html lang="zh-CN"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<link rel="stylesheet" href="/static/style.css">
<link rel="stylesheet" href="/static/modules/clean-process-executor.css">
<style>#root{height:100dvh;padding:12px}#notice{position:fixed;right:12px;bottom:12px;z-index:9}</style>
</head><body><div id="root"></div><output id="notice"></output>
<script type="module" src="/fixture.js"></script></body></html>"""

BOOTSTRAP = """
import {mount} from '/static/clean-process-executor.js';
window.fixture={calls:[],refreshes:0,errors:[]};
const root=document.getElementById('root');
root.innerHTML=await fetch('/static/modules/clean-process-executor.html').then(response=>response.text());
const configuration={desired:{revision:1,value:{protocol:'forest.process.executor/1',profiles:[]}},effective:{revision:1},reload_policy:'replace-runtime-graph',schema_digest:'sha256:fixture',schema:{type:'object'}};
async function api(path,options={}){
  fixture.calls.push({path,options:structuredClone(options)});
  if(path.endsWith('/configuration'))return structuredClone(configuration);
  if(path.endsWith('/configuration/plans'))return {plan_id:'plan-1'};
  if(path.endsWith('/configuration/apply'))return {configuration:{revision:2},graph_revision:2};
  throw Error('Unexpected fixture request '+path);
}
function notice(message){document.getElementById('notice').textContent=message;}
window.ready=mount({root,component:{component_id:'process.executor'},api,notice,refresh:async()=>{fixture.refreshes++;}}).catch(error=>{fixture.errors.push(error.message);throw error;});
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
                "/static/clean-process-executor.js": (
                    (static / "clean-process-executor.js").read_bytes(),
                    "text/javascript; charset=utf-8",
                ),
                "/static/modules/clean-process-executor.html": (
                    (static / "modules" / "clean-process-executor.html").read_bytes(),
                    "text/html; charset=utf-8",
                ),
                "/static/modules/clean-process-executor.css": (
                    (static / "modules" / "clean-process-executor.css").read_bytes(),
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
            expect(page.locator("#executor-count")).to_have_text("0")
            page.get_by_role("button", name="新增诊断模板").click()
            expect(page.locator("#executor-count")).to_have_text("1")
            expect(page.locator("#executor-id")).to_have_value("diagnostic.echo")
            expect(page.locator("#executor-network")).to_have_value("deny")
            expect(page.locator("#executor-posix")).to_be_checked()

            page.locator("#executor-sha").fill("sha256:bad")
            page.get_by_role("button", name="预览并应用").click()
            expect(page.locator("#notice")).to_contain_text("SHA-256")
            assert not page.evaluate(
                "fixture.calls.some(call=>call.path.endsWith('/configuration/plans'))"
            )
            page.locator("#executor-sha").fill(
                "sha256:92d7e40ec50be176cb1b790c7568b7e08cd862137b5aa69f1413ba1967886b79"
            )
            page.get_by_role("button", name="预览并应用").click()
            page.wait_for_function("fixture.refreshes===1")
            planned = page.evaluate(
                "JSON.parse(fixture.calls.find(call=>call.path.endsWith('/configuration/plans')).options.body).value"
            )
            assert planned["protocol"] == "forest.process.executor/1"
            assert planned["profiles"][0]["profile_id"] == "diagnostic.echo"
            assert planned["profiles"][0]["limits"]["memory_bytes"] == 268435456
            assert planned["profiles"][0]["argv"][2] == {"argument": "value"}

            page.get_by_role("button", name="新增 Profile").click()
            expect(page.locator("#executor-count")).to_have_text("2")
            page.locator("#executor-id").fill("diagnostic.echo")
            page.locator(".executor-profile").first.click()
            page.get_by_role("button", name="预览并应用").click()
            expect(page.locator("#notice")).to_contain_text("已存在")
            expect(page.locator("#executor-id")).to_have_value("diagnostic.echo")
            page.locator(".executor-profile").nth(1).click()
            page.locator("#executor-id").fill("new.profile")
            page.get_by_role("button", name="删除当前 Profile").click()
            expect(page.locator("#executor-count")).to_have_text("1")

            assert errors == [], errors
            assert page.evaluate("fixture.errors") == []
            assert page.evaluate("document.documentElement.scrollHeight===innerHeight")
            page.locator(".executor-editor").evaluate("element=>{element.scrollTop=0}")
            artifacts = root / ".local" / "browser-artifacts"
            artifacts.mkdir(parents=True, exist_ok=True)
            page.screenshot(path=str(artifacts / "process-executor-desktop.png"))
            page.set_viewport_size({"width": 480, "height": 900})
            assert page.evaluate("document.documentElement.scrollHeight===innerHeight")
            assert page.evaluate("document.documentElement.scrollWidth<=innerWidth")
            page.screenshot(path=str(artifacts / "process-executor-narrow.png"))
            browser.close()
            print(
                "PASS: process executor profile form, validation, plan/apply, duplicate guard, "
                "delete and bounded desktop/narrow layout"
            )
    finally:
        server.shutdown()
        thread.join()
        server.server_close()


if __name__ == "__main__":
    main()
