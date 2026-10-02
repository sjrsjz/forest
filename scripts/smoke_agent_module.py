"""Exercise the Agent package's isolated management UI in a real browser."""

from __future__ import annotations

import argparse
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from jsonschema import Draft202012Validator
from playwright.sync_api import expect, sync_playwright


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--browser", type=Path, help="Chromium or Edge executable")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    ui = (root / "src/forest/builtin/agent_runtime/ui/index.html").read_bytes()
    page_schema = json.loads((
        root / "src/forest/builtin/agent_runtime/schemas/management-runs-page-output.json"
    ).read_text(encoding="utf-8"))
    row = {
        "run_id": "run:one", "session_id": "session:one",
        "conversation_ref": "onebot:group:933179609", "state": "PREPARING_MODEL",
        "input_revision": 1, "checkpoint_seq": 0, "next_step_seq": 1,
        "current_model_request_id": None, "current_tool_call_id": None,
        "error_code": None, "created_at": "2026-10-02T00:00:00Z",
        "updated_at": "2026-10-02T00:00:01Z", "terminal_at": None,
    }
    first = {
        "snapshot_at": "2026-10-02T01:00:00Z",
        "filter_digest": "sha256:" + "a" * 64,
        "items": [row], "has_more": True,
        "next_cursor": {"created_at": row["created_at"], "run_id": row["run_id"]},
    }
    second = {**first, "items": [], "has_more": False, "next_cursor": None}
    for value in (first, second):
        Draft202012Validator(page_schema).validate(value)
    step_schema = json.loads((
        root / "src/forest/builtin/agent_runtime/schemas/management-run-steps-page-output.json"
    ).read_text(encoding="utf-8"))
    step_base = {
        "run_id": "run:one", "snapshot_at": first["snapshot_at"],
        "filter_digest": first["filter_digest"], "has_more": False,
        "next_cursor": None,
    }
    turn_page = {**step_base, "kind": "MODEL_TURN", "items": [{
        "kind": "MODEL_TURN", "turn_id": "turn:one", "request_id": "request:one",
        "step_seq": 1, "terminal": False, "thinking_only": False,
        "truncated_after_function_call": False, "repair_of_request_id": None,
        "raw_candidate_digest": "sha256:" + "b" * 64,
        "created_at": row["created_at"],
    }]}
    tool_page = {**step_base, "kind": "TOOL_CALL", "items": [{
        "kind": "TOOL_CALL", "tool_call_id": "tool:one", "turn_id": "turn:one",
        "step_seq": 1, "tool_name": "diagnostic_echo",
        "tool_component_id": "process.executor", "tool_generation_id": "generation:one",
        "tool_service_id": "tool.execute", "state": "SUCCEEDED",
        "approval_required": False, "supports_query": False,
        "error_code": None, "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }]}
    for value in (turn_page, tool_page):
        Draft202012Validator(step_schema).validate(value)
    parent = """<!doctype html><html><meta charset="utf-8"><body>
<iframe id="agent" src="/agent" width="1100" height="650"
 sandbox="allow-scripts allow-same-origin"></iframe>
<script>
const frame=document.getElementById('agent'),channel=new MessageChannel(),nonce='agent-smoke';
window.calls=[];window.ready=false;
frame.addEventListener('load',()=>{
  channel.port1.onmessage=event=>{
    const data=event.data;
    if(data.protocol!=='forest.management-ui-channel/1'||data.nonce!==nonce)return;
    if(data.type==='ready'){
      channel.port1.postMessage({protocol:data.protocol,type:'initialize',nonce});
      window.ready=true;return;
    }
    if(data.type!=='query')return;
    calls.push({operation_id:data.operation_id,value:data.value});
    let value;
    if(data.operation_id==='agent.runs.page'){
      value=data.value.after_run_id?__SECOND__:__FIRST__;
    }else if(data.operation_id==='agent.run'){
      value={run_id:'run:one',session_id:'session:one',conversation_ref:'onebot:group:933179609',
        state:'PREPARING_MODEL',error_code:null,progress:{input_revision:1,checkpoint_seq:0},
        current:{model_request_id:null,tool_call_id:null},created_at:'2026-10-02T00:00:00Z',
        updated_at:'2026-10-02T00:00:01Z',terminal_at:null};
    }else if(data.operation_id==='agent.steps.page'){
      value=data.value.kind==='MODEL_TURN'?__TURN__:__TOOL__;
    }else throw new Error('Unexpected operation '+data.operation_id);
    channel.port1.postMessage({protocol:data.protocol,nonce,type:'operation-result',
      request_id:data.request_id,value});
  };
  frame.contentWindow.postMessage({protocol:'forest.management-ui-connect/1',type:'connect',
    nonce},location.origin,[channel.port2]);
});
</script></body></html>""".replace("__FIRST__", json.dumps(first)).replace(
        "__SECOND__", json.dumps(second)
    ).replace("__TURN__", json.dumps(turn_page)).replace(
        "__TOOL__", json.dumps(tool_page)
    )

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/":
                body = parent.encode()
            elif self.path == "/agent":
                body = ui
            else:
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
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
                **({"executable_path": str(args.browser)} if args.browser else {}),
            )
            try:
                page = browser.new_page(viewport={"width": 1200, "height": 800})
                errors = []
                page.on("pageerror", lambda error: errors.append(str(error)))
                page.goto(f"http://127.0.0.1:{server.server_port}/")
                page.wait_for_function("window.ready")
                frame = page.frame_locator("#agent")
                expect(frame.locator("#items .item")).to_have_count(1)
                frame.locator("#items .item").click()
                expect(frame.locator("#detail")).to_contain_text("run:one")
                expect(frame.locator("#model-turns")).to_contain_text("turn:one")
                expect(frame.locator("#tool-calls")).to_contain_text("diagnostic_echo")
                frame.locator("#next").click()
                expect(frame.locator("#next")).to_be_hidden()
                frame.locator("#conversation").fill("onebot:group:933179609")
                frame.locator("#search").click()
                expect(frame.locator("#items .item")).to_have_count(1)
                calls = page.evaluate("window.calls")
                page_calls = [call for call in calls
                              if call["operation_id"] == "agent.runs.page"]
                assert page_calls[1]["value"]["after_run_id"] == "run:one"
                assert page_calls[-1]["value"]["conversation_ref"] == (
                    "onebot:group:933179609"
                )
                assert not errors, errors
            finally:
                browser.close()
    finally:
        server.shutdown()
        server.server_close()


if __name__ == "__main__":
    main()
