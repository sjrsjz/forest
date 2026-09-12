"""Real browser schedule forms, idempotent commands and stale response races."""

from __future__ import annotations

import argparse
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from playwright.sync_api import expect, sync_playwright

BOOTSTRAP = r"""
import {jobsPanel} from '/static/jobs-panel.js';
window.fixture={rows:{a:[],b:[]},plans:{},occurrences:{a:[],b:[]},calls:[],hold:[],pending:[],errors:[],receipts:{},sourcesEmpty:true};
function respond(path, body, headers) {
  const url=new URL(path,'http://fixture'), bot=url.pathname.split('/')[2];
  if(path.endsWith('/conversations')) return {conversations:fixture.sourcesEmpty?[]:[{conversation_id:'conversation-'+bot,channel_kind:'group',channel_id:bot==='a'?'10001':'20002',binding_id:'qq-'+bot,platform:'onebot-v11',group_source_id:'trunk-'+bot}]};
  if(path.endsWith('/bindings')) return {bindings:[{binding_id:'qq-'+bot,self_id:bot==='a'?'90001':'90002'}]};
  if(body===undefined) return {jobs:structuredClone(fixture.rows[bot].filter(r=>!url.searchParams.get('job_id')||r.job_id===url.searchParams.get('job_id'))),occurrences:structuredClone(fixture.occurrences[bot]),next_occurrences:{}};
  const op=bot+url.pathname+headers['Idempotency-Key'];
  if(fixture.receipts[op]) return structuredClone(fixture.receipts[op]);
  let result;
  if(path.endsWith('/change-plans')) {
    const old=fixture.rows[bot].find(r=>r.job_id===body.job_id);
    result={job_id:body.job_id,expected_revision:body.expected_revision,plan_id:'plan-'+fixture.calls.length,plan_digest:'digest-'+fixture.calls.length,values:body.values,before_values:old?.values||null,next_occurrences:['2030-01-01T01:00:00Z','2030-01-02T01:00:00Z']};
    fixture.plans[result.plan_id]=structuredClone(result);
  } else if(path.endsWith('/management-requests')) {
    const plan=fixture.plans[body.plan_id],old=fixture.rows[bot].find(r=>r.job_id===body.job_id);
    if((old?.revision||0)!==body.expected_revision) throw Error('REVISION_CONFLICT');
    const row={job_id:body.job_id,revision:body.expected_revision+1,values:plan.values,enabled:plan.values.enabled,emitted_count:0,delegation:{subject_id:'admin-'+bot,principal_ref:'web:admin-'+bot},next_due_at:'2030-01-01T01:00:00Z'};
    fixture.rows[bot]=[...fixture.rows[bot].filter(r=>r.job_id!==body.job_id),row]; result={revision:row.revision,job_id:row.job_id};
  } else if(path.endsWith('/run-requests')) {
    const occurrence={job_id:body.job_id,occurrence_id:'manual-'+fixture.occurrences[bot].length,revision:1,status:'ENQUEUED',scheduled_for:'2030-01-01T00:00:00Z',reply_id:'reply-committed',delivery_state:'UNKNOWN'};
    fixture.occurrences[bot].push(occurrence); result=occurrence;
  } else if(path.endsWith('/cancellations')) {
    if(fixture.activeRun&&body.scope==='all') {const error=Error('Active Run');error.code='JOB_RUN_ALREADY_ACTIVE';error.details={runs:[{run_id:'active-run-1',expected_checkpoint_seq:9}]};throw error;}
    const row=fixture.rows[bot].find(r=>r.job_id===body.job_id); row.revision++;row.enabled=row.enabled&&body.scope==='pending';row.values.enabled=row.enabled;
    result={job_id:body.job_id,revision:row.revision,enabled:row.enabled,active_runs:[]};
  } else if(path.endsWith('/replays')) result={occurrence_id:body.occurrence_id,status:'QUEUED'};
  else throw Error('Unknown fixture request '+path);
  fixture.receipts[op]=structuredClone(result); return result;
}
async function api(path,body,headers={}) {
  if(body!==undefined&&(!headers['Idempotency-Key']||body.method||body.body)) throw Error('Shared API signature violated');
  fixture.calls.push({path,body:structuredClone(body),headers}); const result=respond(path,body,headers);
  if(fixture.hold.includes(path)) return await new Promise(resolve=>fixture.pending.push({path,resolve:()=>resolve(structuredClone(result))}));
  return structuredClone(result);
}
async function busy(button,work) {
  button.disabled=true; try {await work();} catch(e) {fixture.errors.push(e.message);document.getElementById('notice').textContent=e.message;} finally {button.disabled=false;}
}
const notice=(text)=>{document.getElementById('notice').textContent=text;document.getElementById('notice').classList.remove('hidden');};
window.panel=jobsPanel(api,busy,notice);
document.getElementById('bots').classList.remove('hidden');document.getElementById('bot-detail').classList.remove('hidden');
window.ready=panel.select({bot_id:'a'});
"""


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--browser", type=Path)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    static = root / "src/forest/management/static"
    html = (static / "index.html").read_text(encoding="utf-8").replace(
        '<script type="module" src="/static/app.js"></script>', '<script type="module" src="/fixture.js"></script>')

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            resources = {"/": (html.encode(), "text/html; charset=utf-8"), "/fixture.js": (BOOTSTRAP.encode(), "text/javascript")}
            for name in ("jobs-panel.js", "style.css"):
                resources["/static/" + name] = ((static / name).read_bytes(), "text/css" if name.endswith("css") else "text/javascript")
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
            browser = playwright.chromium.launch(headless=True, executable_path=str(args.browser) if args.browser else None)
            page = browser.new_page(viewport={"width": 1380, "height": 1100})
            errors = []
            page.on("pageerror", lambda error: errors.append(str(error)))
            page.goto(f"http://127.0.0.1:{server.server_port}")
            page.evaluate("window.ready")
            form = page.locator('#job-config')
            name, prompt = form.locator('[name="name"]'), form.locator('[name="prompt"]')
            expect(name).to_be_disabled()
            page.evaluate('fixture.sourcesEmpty=false')
            page.locator('#job-refresh').click()
            expect(name).to_be_enabled()
            name.fill("每日检查")
            page.evaluate("fixture.hold.push('/bots/a/jobs')")
            page.locator('#job-refresh').click()
            page.wait_for_function('fixture.pending.length===1')
            name.fill('新群刷新期间的草稿')
            page.evaluate('fixture.pending.shift().resolve();fixture.hold=[]')
            expect(name).to_have_value('新群刷新期间的草稿')
            name.fill("每日检查")
            prompt.fill("整理今天需要关注的事情。")
            form.locator('[name="start_at"]').fill('2030-01-01T08:00:00+08:00')
            form.locator('[name="delegation_expires_at"]').fill('2031-01-01T00:00:00+08:00')
            page.get_by_role('button', name='预览计划变更', exact=True).click()
            expect(page.locator('#job-preview')).to_be_visible()
            expect(page.locator('#job-diff')).to_contain_text('Asia/Shanghai')
            expect(page.locator('#job-diff')).to_contain_text('账号 90001')
            first = page.evaluate("fixture.calls.find(c=>c.path.endsWith('/change-plans')).body")
            assert first['values']['conversation_id'] == 'conversation-a'
            assert first['values']['schedule']['start_at'] == '2030-01-01T00:00:00.000Z'
            assert 'delegation' not in first['values']
            page.locator('#job-apply').click()
            expect(page.locator('#job-current')).to_contain_text('修订 1')
            form.locator('[name="schedule_kind"]').select_option('cron')
            form.locator('[name="cron"]').fill('0 9 * * *')
            form.locator('[name="misfire"]').select_option('catch_up')
            form.locator('[name="catch_up_limit"]').fill('2')
            form.locator('[name="dst_fold"]').select_option('both')
            page.get_by_role('button', name='预览计划变更', exact=True).click()
            expect(page.locator('#job-diff')).to_contain_text('Cron 0 9 * * *')
            expect(page.locator('#job-diff')).to_contain_text('每 3600 秒 → Cron 0 9 * * *')
            page.locator('#job-apply').click()
            expect(page.locator('#job-current')).to_contain_text('修订 2')
            page.locator('#job-run').click()
            expect(page.locator('#job-runs')).to_contain_text('回复已提交到发送队列')
            expect(page.locator('#job-runs')).to_contain_text('结果待核实')
            page.locator('#job-run').click()
            assert page.evaluate('fixture.occurrences.a.length') == 1
            page.locator('#job-run-another').click()
            page.wait_for_function('fixture.occurrences.a.length===2')
            page.evaluate("fixture.occurrences.a.push({occurrence_id:'safe-failure',job_id:fixture.rows.a[0].job_id,revision:7,status:'FAILED',replay_allowed:true,scheduled_for:'2030-01-01T00:00:00Z'})")
            page.locator('#job-runs-refresh').click()
            page.get_by_role('button', name='按当前授权重新执行').click()
            assert page.evaluate("fixture.calls.find(c=>c.path.endsWith('/replays')).body.expected_revision") == 7
            # A late read cannot replace a plan draft typed while it was pending.
            page.evaluate("fixture.hold.push('/bots/a/jobs?job_id='+fixture.rows.a[0].job_id)")
            page.locator('#job-reload').click()
            page.wait_for_function('fixture.pending.length===1')
            name.fill('读取期间的新草稿')
            page.evaluate('fixture.pending.shift().resolve();fixture.hold=[]')
            expect(name).to_have_value('读取期间的新草稿')
            # Editing while a preview is in flight invalidates the pending plan.
            page.evaluate("fixture.hold.push('/bots/a/jobs/change-plans')")
            page.get_by_role('button', name='预览计划变更', exact=True).click()
            page.wait_for_function('fixture.pending.length===1')
            name.fill('预览期间的新草稿')
            page.evaluate('fixture.pending.shift().resolve();fixture.hold=[]')
            expect(page.locator('#job-preview')).to_be_hidden()
            # A late apply for A cannot overwrite Bot B's form or post to B.
            page.get_by_role('button', name='预览计划变更', exact=True).click()
            page.evaluate("fixture.hold.push('/bots/a/jobs/management-requests')")
            page.locator('#job-apply').click()
            page.wait_for_function('fixture.pending.length===1')
            page.evaluate("panel.select({bot_id:'b'})")
            name.fill('Bot B 草稿')
            page.evaluate('fixture.pending.shift().resolve();fixture.hold=[]')
            expect(name).to_have_value('Bot B 草稿')
            assert not page.evaluate("fixture.calls.some(c=>c.path==='/bots/b/jobs/management-requests')")
            # Cancellation states its scope and reason, and uses a fixed operation key.
            page.evaluate("panel.select({bot_id:'a'})")
            page.locator('#job-list button').first.click()
            page.locator('#job-cancel-scope').select_option('pending')
            page.locator('#job-cancel-reason').fill('取消尚未被 Agent 领取的任务')
            page.locator('#job-cancel').click()
            page.wait_for_function("fixture.calls.some(c=>c.path.endsWith('/cancellations'))")
            cancellation = page.evaluate("fixture.calls.find(c=>c.path.endsWith('/cancellations'))")
            assert cancellation['body']['scope'] == 'pending' and cancellation['headers']['Idempotency-Key']
            expect(page.locator('#job-current')).to_contain_text('已启用')
            page.evaluate('fixture.activeRun=true')
            page.locator('#job-cancel-scope').select_option('all')
            page.locator('#job-cancel').click()
            expect(page.locator('#job-cancel-result')).to_contain_text('取消请求未应用')
            expect(page.locator('#job-cancel-result')).to_contain_text('active-run-1')
            expect(page.locator('#job-cancel-result')).to_contain_text('9')
            expect(page.locator('#job-current')).to_contain_text('已启用')
            page.evaluate('fixture.activeRun=false')
            page.locator('#job-cancel').click()
            expect(page.locator('#job-current')).to_contain_text('已停用')
            page.locator('#job-toggle').click()
            expect(page.locator('#job-preview')).to_be_visible()
            expect(page.locator('#job-diff')).to_contain_text('停用 → 启用')
            page.locator('#job-apply').click()
            expect(page.locator('#job-current')).to_contain_text('已启用')
            assert errors == [], errors
            assert page.evaluate('fixture.errors') == []
            artifacts = root / '.local/browser-artifacts'
            artifacts.mkdir(parents=True, exist_ok=True)
            page.screenshot(path=str(artifacts / 'jobs-desktop.png'), full_page=True)
            page.set_viewport_size({'width':390,'height':844})
            assert page.evaluate('document.documentElement.scrollWidth <= innerWidth'), 'mobile overflow'
            page.screenshot(path=str(artifacts / 'jobs-mobile.png'), full_page=True)
            browser.close()
            print('PASS: Job interval/cron forms, previews, idempotent manual runs, safe replay, cancellation, stale responses and mobile layout')
    finally:
        server.shutdown()
        thread.join()
        server.server_close()


if __name__ == '__main__':
    main()
