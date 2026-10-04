"""Exercise the shipped Memory package UI in a real browser.

The fixture speaks the isolated management MessageChannel protocol. It checks
image rendering and attribution without contacting a Bot or sending QQ traffic.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import re
import struct
import threading
import zlib
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from jsonschema import Draft202012Validator
from playwright.sync_api import expect, sync_playwright


def fixture_png() -> bytes:
    """Produce an incompressible image that requires several 96 KiB chunks."""

    width = height = 256
    raw = b"".join(
        b"\0" + b"".join(
            hashlib.sha256(struct.pack(">II", row, column)).digest()[:3]
            for column in range(width)
        )
        for row in range(height)
    )

    def chunk(kind: bytes, content: bytes) -> bytes:
        body = kind + content
        return struct.pack(">I", len(content)) + body + struct.pack(">I", zlib.crc32(body))

    return (
        b"\x89PNG\r\n\x1a\n"
        + chunk(b"IHDR", struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0))
        + chunk(b"IDAT", zlib.compress(raw))
        + chunk(b"IEND", b"")
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--browser", type=Path, help="Chromium or Edge executable")
    args = parser.parse_args()
    ui = (
        Path(__file__).resolve().parents[1]
        / "src/forest/builtin/memory_graph/ui/index.html"
    ).read_bytes()
    picture = fixture_png()
    assert len(picture) > 2 * 98304
    digest = "sha256:" + hashlib.sha256(picture).hexdigest()
    fixture = {
        "image_base64": base64.b64encode(picture).decode("ascii"),
        "digest": digest,
        "byte_length": len(picture),
    }
    parent = """<!doctype html><html><meta charset="utf-8"><body>
<iframe id="memory" src="/memory" width="1000" height="650"
  sandbox="allow-scripts allow-same-origin"></iframe>
<script>
const fixture=__FIXTURE__;
const bytes=Uint8Array.from(atob(fixture.image_base64),char=>char.charCodeAt(0));
const frame=document.getElementById('memory'),channel=new MessageChannel(),nonce='memory-smoke';
    window.calls=[];window.responses=[];window.ready=false;window.corrupt=false;window.batchApplied=false;
frame.addEventListener('load',()=>{
  channel.port1.onmessage=event=>{
    const data=event.data;
    if(data.protocol!=='forest.management-ui-channel/1'||data.nonce!==nonce)return;
    if(data.type==='ready'){
      channel.port1.postMessage({protocol:data.protocol,type:'initialize',nonce,
        configuration:{desired:{revision:1,value:{
          embedding:{deployment_id:'fixture',model:'fixture',model_kind:'VL',dimensions:3},
          retrieval:{}}},effective:{revision:1}}});
      window.ready=true;return;
    }
    if(data.type!=='query'&&data.type!=='action')return;
    calls.push({type:data.type,operation_id:data.operation_id,
      idempotency_key:data.idempotency_key,value:data.value});
    let value;
    if(data.type==='action'&&data.operation_id==='memory.forget.batch.enqueue'){
      window.batchApplied=true;
      value={protocol:'forest.memory-forget-batch-enqueued/1',request_id:'batch:smoke'};
    }
    else if(data.operation_id==='memory.trunks.list')value={items:[]};
    else if(data.operation_id==='memory.sources.query')value={items:[{
      source_unit_id:'unit/unknown',source_id:'group-source-1',trunk_id:'trunk-1',
      source_kind:'GROUP',platform:'onebot',realm_id:'qq',
      external_group_id:'933179609',author_subject_ref:'qq:author-1',
      unit_kind:'MESSAGE',modality:'IMAGE',occurred_at:'2026-09-28T00:00:00Z',
      source_order_seq:1,part_count:1,eligibility_state:'ELIGIBLE',source_status:'ACTIVE',
      document_facet_count:1,ready_document_facet_count:0,
      failed_document_facet_count:0,unknown_document_facet_count:1,
      index_coverage:'NOT_READY'}],has_more:false,
      snapshot_accepted_at:'2026-09-28T00:00:00Z'};
    else if(data.operation_id==='memory.source-facets.query')value={
      source_unit_id:'unit/unknown',source_id:'group-source-1',source_revision:1,
      trunk_id:'trunk-1',external_group_id:'933179609',
      author_subject_ref:'qq:author-1',has_more:false,next_after:null,items:[{
        facet_id:'facet/unknown',part_id:'part/image',modality:'IMAGE',state:'UNKNOWN',
        failure_code:null,created_at:'2026-09-28T00:00:00Z',
        attempt_id:'attempt/unknown',claim_id:'claim/unknown',attempt_state:'UNKNOWN',
        effect_operation_id:'effect/unknown',executor_generation_id:'generation/embedding',
        dispatched_at:'2026-09-28T00:00:01Z',settled_at:'2026-09-28T00:00:02Z',
        budget_day:'2026-09-28',reservation_state:'UNKNOWN',
        reserved_tokens:32768,charged_tokens:null,
        reservation_error_code:'EMBEDDING_RESULT_UNKNOWN'}]};
    else if(data.operation_id==='memory.forget.selection')value={
      selection_id:'selection:smoke',member_count:2,
      member_digest:'sha256:'+'a'.repeat(64),
      snapshot_accepted_at:'2026-09-28T00:00:00Z',
      progress_state:window.batchApplied?'COMPLETE':'FROZEN',
      applied_member_count:window.batchApplied?2:0,
      applied_batch_count:window.batchApplied?1:0,
      next_unapplied_ordinal:window.batchApplied?null:1,
      filters:{author_subject_ref:'qq:author-1'},items:[],has_more:false};
    else if(data.operation_id==='memory.forget.batch.preview')value={
      protocol:'forest.memory-forget-batch-impact-plan/1',
      selection_id:'selection:smoke',first_ordinal:1,last_ordinal:2,
      batch_member_count:2,source_unit_count:2,selected_payload_count:2,
      vector_deletion_count:0,relation_retraction_count:0,
      association_journal_count:0,deletion_epoch:0,
      plan_digest:'sha256:'+'b'.repeat(64),
      batch_source_unit_sample:['unit/one','unit/two']};
    else if(data.operation_id==='memory.forget.batch.status')value={
      state:'SUCCEEDED',selection_id:'selection:smoke',after_ordinal:0,
      attempt_count:1,error_code:null,result:{first_ordinal:1,last_ordinal:2,
        suppressed_source_units:2,vector_deletions:0,retracted_relations:0}};
    else if(data.operation_id==='memory.trial.get')value={
      protocol:'forest.memory-trial/1',query_id:'trial:smoke',state:'READY',query_mode:'MIXED',
      deadline_at:'2026-09-28T23:00:00Z',expires_at:'2026-09-29T00:00:00Z',
      error_code:null,evidence_ref:{payload_id:'evidence-1',
        digest:fixture.digest,owner_component_id:'memory.graph',
        owner_kind:'retrieval.evidence',owner_id:'trial:smoke'}};
    else if(data.operation_id==='memory.trial.result')value={
      protocol:'forest.memory-trial-result-page/2',query_id:'trial:smoke',
      bundle_id:'bundle-1',graph_snapshot_ref:'snapshot-1',total_count:1,
      content_truncated:false,truncated:false,
      association_sampled:false,has_more:false,next_index:null,items:[{
        item_index:0,source_unit_id:'source-1',source_id:'group-source-1',source_kind:'GROUP',
        platform:'onebot',external_group_id:'933179609',trunk_id:'trunk-1',
        author_subject_ref:'qq:author-1',occurred_at:'2026-09-28T00:00:00Z',
        is_seed:true,rank_score:0.9,excerpt:'跨群图像证据',excerpt_truncated:false,
        image_count:1,images:[{part_id:'image-1',media_type:'image/png',
          width:256,height:256,media_ref:{payload_id:'image-1',digest:fixture.digest,
            owner_component_id:'memory.graph',owner_kind:'retrieval.image',
            owner_id:'trial:smoke'}}]
      }]};
    else if(data.operation_id==='memory.trial.image'){
      const offset=data.value.offset,size=Math.min(98304,bytes.length-offset);
      const chunkBytes=bytes.slice(offset,offset+size);
      if(window.corrupt&&offset===0)chunkBytes[100]^=1;
      value={protocol:'forest.memory-trial-image-chunk/1',query_id:'trial:smoke',
        source_unit_id:'source-1',part_id:'image-1',media_type:'image/png',
        width:256,height:256,digest:fixture.digest,offset,
        byte_length:bytes.length,eof:offset+size===bytes.length,
        data_base64:btoa(String.fromCharCode(...chunkBytes))};
    }else throw Error('Unexpected operation '+data.operation_id);
    if(data.operation_id.startsWith('memory.trial.'))
      window.responses.push({operation_id:data.operation_id,value});
    channel.port1.postMessage({protocol:data.protocol,
      type:data.type==='action'?'action-result':'query-result',nonce,
      request_id:data.request_id,value});
  };
  channel.port1.start();
  frame.contentWindow.postMessage({protocol:'forest.management-ui-connect/1',
    type:'connect',nonce},location.origin,[channel.port2]);
});
</script></body></html>""".replace("__FIXTURE__", json.dumps(fixture))

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/":
                content, media_type = parent.encode(), "text/html; charset=utf-8"
            elif self.path == "/memory":
                content, media_type = ui, "text/html; charset=utf-8"
            else:
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header("Content-Type", media_type)
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
                **({"executable_path": str(args.browser)} if args.browser else {}),
            )
            try:
                page = browser.new_page(viewport={"width": 1100, "height": 800})
                errors = []
                page.on("pageerror", lambda error: errors.append(str(error)))
                page.goto(f"http://127.0.0.1:{server.server_port}/")
                page.wait_for_function("window.ready")
                frame = page.frame_locator("#memory")
                expect(frame.locator('#source-rows')).to_contain_text('UNKNOWN 1')
                frame.locator('#source-rows button', has_text='嵌入').click()
                expect(frame.locator('#facet-rows')).to_contain_text('attempt/unknown')
                expect(frame.locator('#facet-rows')).to_contain_text('估计预留 32768 token')
                assert any(call['operation_id'] == 'memory.source-facets.query'
                           and call['value']['state'] == 'UNKNOWN'
                           for call in page.evaluate('window.calls'))
                frame.locator('[data-tab="trial"]').click()
                frame.locator("#trial-query").fill("trial:smoke")
                frame.locator("#trial-get").click()
                expect(frame.locator("#trial-results")).to_contain_text("933179609")
                expect(frame.locator("#trial-results")).to_contain_text("qq:author-1")
                frame.locator("#trial-results button").click()
                image = frame.locator("#trial-results img")
                expect(image).to_have_attribute("src", re.compile(r"^blob:"))
                expect(image).to_have_js_property("naturalWidth", 256)
                calls = page.evaluate("window.calls")
                assert len([call for call in calls if call["operation_id"] == "memory.trial.image"]) >= 3
                page.evaluate("window.corrupt=true")
                frame.locator("#trial-get").click()
                expect(frame.locator("#trial-results img")).to_have_count(0)
                frame.locator("#trial-results button").click()
                expect(frame.locator("#status")).to_have_text("图片摘要不匹配")
                expect(frame.locator("#trial-results img")).to_have_count(0)
                frame.locator('[data-tab="forget"]').click()
                frame.locator("#forget details summary").click()
                frame.locator("#batch-selection-id").fill("selection:smoke")
                frame.locator("#batch-load").click()
                expect(frame.locator("#batch-selection-meta")).to_contain_text("冻结成员 2 个")
                frame.locator("#batch-size").fill("2")
                frame.locator("#batch-preview").click()
                expect(frame.locator("#batch-impact")).to_contain_text("成员 1–2")
                frame.locator("#batch-confirm").check()
                frame.locator("#batch-enqueue").click()
                expect(frame.locator("#batch-state")).to_contain_text("SUCCEEDED")
                expect(frame.locator("#batch-selection-meta")).to_contain_text("全部完成")
                expect(frame.locator("#batch-after")).to_have_value("2")
                assert not errors, errors
                action_calls = [call for call in page.evaluate("window.calls")
                                if call["operation_id"] == "memory.forget.batch.enqueue"]
                assert len(action_calls) == 1, action_calls
                assert action_calls[0]["idempotency_key"].startswith("forget-batch:")
                assert set(action_calls[0]["value"]) == {
                    "selection_id", "after_ordinal", "batch_size", "plan",
                }
                schema_root = Path(__file__).resolve().parents[1] / "src/forest/builtin/memory_graph/schemas"
                trial_schemas = {
                    "memory.trial.get": "trial-get-output.json",
                    "memory.trial.result": "trial-result-output.json",
                    "memory.trial.image": "trial-image-output.json",
                }
                validators = {
                    operation: Draft202012Validator(json.loads((schema_root / name).read_text(encoding="utf-8")))
                    for operation, name in trial_schemas.items()
                }
                for response in page.evaluate("window.responses"):
                    validators[response["operation_id"]].validate(response["value"])
            finally:
                browser.close()
    finally:
        server.shutdown()
        server.server_close()


if __name__ == "__main__":
    main()
