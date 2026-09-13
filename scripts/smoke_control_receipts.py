"""Real Edge acceptance of shipped control-receipt barriers and panel cleanup.

The local server serves unmodified shipped JS/HTML. A controlled browser fetch
fixture captures responses before releasing them in fault order; this is browser
protocol acceptance, not PostgreSQL, credential-storage, or live-service testing.
Isolation is Bot + component ID or panel family (Agent/Memory/Binding collection/
Job collection/Delivery collection), not individual Binding or Job IDs.
"""

from __future__ import annotations

import argparse
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from playwright.sync_api import expect, sync_playwright

from forest.agent_settings import DEFAULT_AGENT_SETTINGS
from forest.memory_settings import DEFAULT_MEMORY_SETTINGS

BOOTSTRAP = r"""
const agentDefaults={...__AGENT__,model:'fixture-model'}, memoryDefaults=__MEMORY__;
const fields={type:'object',additionalProperties:false,required:['label'],properties:{label:{type:'string',minLength:1}}};
const packages=['alpha','beta'].map(name=>({package_digest:'pkg-'+name,package_id:name,component_id:name,
  version:'1.0.0',state_schema:1,grants:[],config_schema:fields,
  services:[{name:name+'.run',input_schema:{type:'object',additionalProperties:false,properties:{query:{type:'string',default:'request'}}}}]}));
const bindValues={enabled:true,api_base_url:'http://127.0.0.1:5700',response_mode:'component',
  response_template:'Stored binding body',trigger_mentions:true,trigger_prefix:'/chat',private_trigger:true,admin_user_ids:['123']};
const jobValues={name:'Stored job',enabled:true,kind:'conversation.trigger',conversation_id:'conversation-a',prompt:'Stored job body',
  schedule:{kind:'interval',timezone:'Asia/Shanghai',start_at:'2030-01-01T00:00:00Z',end_at:null,interval_seconds:3600,cron:null,dst_fold:'first'},
  misfire:'coalesce',misfire_grace_seconds:60,catch_up_limit:3,max_overlap:1,
  delegation_expires_at:'2031-01-01T00:00:00Z',max_runs:1000};
const binding={binding_id:'binding-a',self_id:'123',display_name:'Browser account',state:'ONLINE',revision:1,effective_revision:1,
  registration_stage:'REGISTERED',desired_values:bindValues,webhook_url:'http://127.0.0.1/onebot/binding-a'};
const job={job_id:'job-a',revision:1,enabled:true,values:jobValues,emitted_count:0,
  delegation:{subject_id:'admin',principal_ref:'web:admin'},next_due_at:'2030-01-01T00:00:00Z'};
window.fixture={calls:[],hold:[],pending:[],overrides:{},unexpected:[],completed:[],plans:{},packages,
  bots:['bot-a','bot-b'].map(id=>({bot_id:id,display_name:id,stage:'READY',revision:1,job_id:'provision-'+id}))};
fixture.receipt={operation_id:'recorded-operation',action:'config.apply',operation_status:'RECORDED',result_payload_state:'UNAVAILABLE'};
function reply(path,method,body) {
  const clean=path.split('?')[0];
  if (path==='/setup') return {setup_required:false,database_ready:true};
  if (path==='/session') return {username:'fixture-admin'};
  if (path==='/configuration') return {head_revision:1,defaults:{display_name:'Forest fixture',session_ttl_minutes:60}};
  if (path==='/bots') return {bots:fixture.bots,managed_available:true};
  if (/^\/bots\/[^/]+$/.test(path)) return fixture.bots.find(bot=>bot.bot_id===path.split('/')[2]);
  if (path.startsWith('/provision-jobs/')) return {events:[]};
  if (path==='/packages') return {packages,errors:{}};
  if (clean.endsWith('/conversations')) return {conversations:[{conversation_id:'conversation-a',channel_kind:'group',channel_id:'456',
    binding_id:'binding-a',platform:'onebot-v11',next_ingress_seq:2,context_applied_seq:1,pending_triggers:0}]};
  if (clean.endsWith('/bindings')) return {bindings:[binding]};
  if (clean.endsWith('/delivery')) return {parts:[]};
  if (clean.endsWith('/jobs')) return {jobs:[job],occurrences:[],next_occurrences:{}};
  if (clean.endsWith('/agent/runs')) return {runs:[{run_id:'old-run',conversation_id:'conversation-a',phase:'COMPLETED',text:'Stored run body'}],approvals:[]};
  if (clean.endsWith('/memory/status')) return {configured:true,enabled:false,revision:1,counts:{},errors:{},note:'Stored memory status'};
  if (clean.endsWith('/configuration')) {
    if (path.includes('/bindings/')) return binding;
    if (path.includes('/agent/')) return {revision:1,secret_revision:1,has_api_key:true,values:agentDefaults};
    if (path.includes('/memory/')) return {revision:1,secret_revision:1,has_api_key:true,values:memoryDefaults};
    const component=path.match(/\/components\/([^/]+)/)[1], value={package_digest:'pkg-'+component,config:{label:'Stored '+component},grants:[]};
    return {desired_revision:1,effective_revision:1,desired_values:value,effective_values:value,
      desired_values_payload_state:'ACTIVE',effective_values_payload_state:'ACTIVE',
      defaults:{package_digest:'',config:{},grants:[]},operations:[],runtime:{components:[]}};
  }
  if (clean.endsWith('/configuration/history')) return {next_before:null,revisions:[{revision:1,created_at:'2030-01-01T00:00:00Z',
    values:{package_digest:'pkg-alpha',config:{label:'Stored history body'},grants:[]}}]};
  if (clean.endsWith('/change-plans')) {
    const plan={plan_id:'plan-'+fixture.calls.length,plan_digest:'digest-'+fixture.calls.length,expected_revision:body.expected_revision,
      values:body.values,before_values:body.values,credentials_changed:false,next_occurrences:[],job_id:body.job_id};
    fixture.plans[plan.plan_id]=structuredClone(plan); return plan;
  }
  if (clean.endsWith('/management-requests')) return {revision:2,secret_revision:1};
  if (clean.endsWith('/invoke')) return {operation_id:'invoke-null',result:null};
  fixture.unexpected.push({path,method}); throw Error('Unexpected fixture API '+method+' '+path);
}
const originalFetch=window.fetch.bind(window);
window.fetch=async(url,options={})=>{
  if (!String(url).startsWith('/management/v1')) return originalFetch(url,options);
  const path=String(url).replace('/management/v1',''),method=options.method||'GET',key=method+' '+path;
  const body=options.body?JSON.parse(options.body):undefined;
  fixture.calls.push({path,method,body:structuredClone(body)});
  const response=structuredClone(fixture.overrides[key]||{status:200,body:reply(path,method,body)});
  const index=fixture.hold.indexOf(key);
  if (index>=0) {fixture.hold.splice(index,1);await new Promise(resolve=>fixture.pending.push({key,response,resolve}));}
  fixture.completed.push(key);
  return new Response(JSON.stringify(response.body),{status:response.status,headers:{'Content-Type':'application/json'}});
};
// Periodic refresh is driven only by explicit buttons in this response-order fixture.
window.setInterval=()=>1;
fixture.release=key=>{const i=fixture.pending.findIndex(item=>item.key===key);if(i<0)throw Error('Missing held request');fixture.pending.splice(i,1)[0].resolve();};
fixture.module=await import('/static/control-receipts.js');
await import('/static/app.js');
window.fixtureReady=true;
""".replace("__AGENT__", json.dumps(DEFAULT_AGENT_SETTINGS)).replace(
    "__MEMORY__", json.dumps(DEFAULT_MEMORY_SETTINGS)
)


def protocol_checks(page):
    result = page.evaluate("""async()=>{
      const events=[], read=fixture.module.controlReceipts(scope=>events.push(scope));
      const scope='/bots/bot-a/agent', receipt={...fixture.receipt,unexpected_body:'DO-NOT-RETAIN'};
      const nullValue=await read(scope+'/configuration',async()=>null);
      const caught=promise=>promise.then(value=>({value}),error=>({code:error.code,receipt:error.receipt}));
      let release;
      const late=caught(read(scope+'/configuration',()=>new Promise(resolve=>{release=resolve;})));
      const releases=[];
      const independent=[['/bots/bot-b/agent/configuration','bot'],
        ['/bots/bot-a/memory/configuration','panel'],['/bots/bot-a/components/alpha/configuration','component']];
      const separatePending=independent.map(([path,kind])=>read(path,()=>new Promise(resolve=>{
        releases.push(()=>resolve({independent:kind}));})));
      const unavailable=await caught(read(scope+'/management-requests',async()=>receipt));
      releases.forEach(release=>release());const separate=await Promise.all(separatePending);
      release({body:'OLD-AGENT-BODY'});const stale=await late;
      const fresh=await read(scope+'/configuration',async()=>({fresh:true}));
      let releaseHttp;
      const lateHttp=caught(read(scope+'/runs',()=>new Promise(resolve=>{releaseHttp=resolve;})));
      const http=await caught(read(scope+'/change-plans',async()=>{const e=Error('HTTP rejected');e.code='CONTROL_CONTENT_REVOKED';throw e;}));
      releaseHttp({body:'OLD-HTTP-BODY'});const staleHttp=await lateHttp;
      // Different component IDs are different resources, even inside the same Bot.
      let releaseAlpha;
      const alpha=caught(read('/bots/bot-a/components/alpha/configuration',()=>new Promise(resolve=>{releaseAlpha=resolve;})));
      await caught(read('/bots/bot-a/components/beta/management-requests',async()=>receipt));
      releaseAlpha({independent:'component-id'});
      return {nullValue,separate,unavailable,stale,fresh,http,staleHttp,alpha:await alpha,events};
    }""")
    assert result["nullValue"] is None
    assert result["separate"] == [
        {"independent": "bot"}, {"independent": "panel"}, {"independent": "component"}
    ]
    for key in ("unavailable", "stale", "http", "staleHttp"):
        assert result[key]["code"] == "CONTROL_CONTENT_REVOKED", (key, result)
        assert "value" not in result[key]
    assert result["unavailable"]["receipt"] == {
        "operation_id": "recorded-operation", "action": "config.apply"
    }
    assert result["fresh"] == {"fresh": True}
    assert result["alpha"] == {"value": {"independent": "component-id"}}
    assert result["events"] == [
        "/bots/bot-a/agent", "/bots/bot-a/agent", "/bots/bot-a/components/beta"
    ]


PANELS = {
    "component": ("components/alpha", "bot-config-form", "bot-config-preview", "bot-config-diff", "bot-config-apply", "reload-bot-config"),
    "agent": ("agent", "agent-config", "agent-preview", "agent-config-diff", "agent-apply", "agent-reload"),
    "memory": ("memory", "memory-config", "memory-preview", "memory-diff", "memory-apply", "memory-reload"),
    "binding": ("bindings/binding-a", "binding-config", "binding-preview", "binding-diff", "apply-binding", "reload-binding"),
    "job": ("jobs", "job-config", "job-preview", "job-diff", "job-apply", "job-reload"),
}


def select_bot(page, bot="bot-a"):
    page.locator('[data-bot-id="' + bot + '"]').click()
    expect(page.locator("#memory-config-status")).to_contain_text("配置版本 1")
    expect(page.locator('#bot-config-fields [name="label"]')).to_have_value("Stored alpha")
    expect(page.locator("#job-list button")).to_have_count(1)


def panel_check(page, name, mode):
    prefix, form_id, preview_id, diff_id, apply_id, reload_id = PANELS[name]
    base = "/bots/bot-a/" + prefix
    if name == "binding":
        page.locator('[data-binding-id="binding-a"]').click()
        expect(page.locator("#binding-detail")).to_be_visible()
    if name == "job":
        page.locator("#job-list button").click()
        expect(page.locator("#job-form-title")).to_contain_text("Stored job")
    form = page.locator("#" + form_id)
    field = {"component": "label", "agent": "preset_context", "memory": "deployment_id",
             "binding": "response_template", "job": "prompt"}[name]
    canary = "ERASE-" + name + "-" + mode
    form.locator('[name="' + field + '"]').fill(canary)
    if name in {"agent", "memory"}:
        form.locator('[name="api_key"]').fill("fixture-secret-cleared")
    elif name == "binding":
        form.locator('[name="access_token"]').fill("fixture-secret-cleared")
    form.locator('button[type="submit"]').click()
    expect(page.locator("#" + preview_id)).to_be_visible()
    expect(page.locator("#" + diff_id)).to_contain_text(canary)
    get_path = base + ("?job_id=job-a" if name == "job" else "/configuration")
    get_key = "GET " + get_path
    page.evaluate("key=>fixture.hold.push(key)", get_key)
    page.locator("#" + reload_id).click()
    page.wait_for_function("key=>fixture.pending.some(item=>item.key===key)", arg=get_key)
    post_key = "POST " + base + "/management-requests"
    page.evaluate("({key,mode})=>{fixture.overrides[key]=mode==='receipt'?{status:200,body:fixture.receipt}:"
                  "{status:409,body:{code:'CONTROL_CONTENT_REVOKED',message:'相关内容已不可用'}};}",
                  {"key": post_key, "mode": mode})
    page.locator("#" + apply_id).click()
    expect(page.locator("#notice")).to_contain_text("不可用")
    expect(page.locator("#notice")).not_to_have_class("notice success")
    if mode == "receipt":
        expect(page.locator("#notice")).to_contain_text("recorded-operation")
    expect(page.locator("#" + preview_id)).to_be_hidden()
    expect(page.locator("#" + diff_id)).to_have_text("")
    page.evaluate("key=>fixture.release(key)", get_key)
    page.wait_for_function("key=>fixture.completed.includes(key)&&fixture.pending.length===0", arg=get_key)
    # Let the held response traverse the actual app wrapper and panel callback.
    page.evaluate("()=>new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve)))")
    expect(page.locator("#" + preview_id)).to_be_hidden()
    expect(page.locator("#" + diff_id)).to_have_text("")
    if name == "component":
        expect(page.locator("#bot-config-fields")).to_be_empty()
        expect(page.locator("#bot-history")).to_be_empty()
        expect(page.locator("#bot-invoke-result")).to_be_empty()
    else:
        assert canary not in form.locator('[name="' + field + '"]').input_value()
        if name == "binding":
            expect(page.locator("#binding-detail")).to_be_hidden()
            expect(page.locator("#binding-list")).to_be_empty()
        else:
            expect(form.locator(":scope > fieldset")).to_have_js_property("disabled", True)
            expect(form.locator('[name="' + field + '"]')).to_be_disabled()
    if name in {"agent", "memory", "binding"}:
        secret_field = "access_token" if name == "binding" else "api_key"
        expect(form.locator('[name="' + secret_field + '"]')).to_have_value("")
    for container in {"agent": ["agent-runs"], "memory": ["memory-state"],
                      "job": ["job-list", "job-runs", "job-cancel-result"]}.get(name, []):
        expect(page.locator("#" + container)).to_be_empty()
    before = page.evaluate("fixture.calls.filter(call=>call.method==='POST').length")
    # Even a stale hidden button reference cannot submit the revoked cached plan.
    page.locator("#" + apply_id).dispatch_event("click")
    page.evaluate("()=>new Promise(resolve=>requestAnimationFrame(resolve))")
    assert page.evaluate("fixture.calls.filter(call=>call.method==='POST').length") == before
    if name in {"agent", "memory"}:
        # Recovery uses the same panel's read-only button, outside its disabled
        # editor. The next read returns a distinct, currently available revision.
        values = dict(DEFAULT_AGENT_SETTINGS if name == "agent" else DEFAULT_MEMORY_SETTINGS)
        recovered = "Independent-" + name + "-current"
        values[field] = [recovered] if name == "agent" else recovered
        if name == "agent":
            values["model"] = "fixture-model"
        page.evaluate("({get,post,row})=>{delete fixture.overrides[post];fixture.overrides[get]={status:200,body:row};}",
                      {"get": get_key, "post": post_key,
                       "row": {"revision": 2, "secret_revision": 2, "has_api_key": True, "values": values}})
        expect(page.locator("#" + reload_id)).to_be_enabled()
        page.locator("#" + reload_id).click()
        expect(form.locator('[name="' + field + '"]')).to_be_enabled()
        expect(form.locator('[name="' + field + '"]')).to_have_value(recovered)
        expect(form.locator('[name="api_key"]')).to_have_value("")
        expect(page.locator("#" + preview_id)).to_be_hidden()
        expect(page.locator("#" + diff_id)).to_have_text("")
        for width in (1440, 390):
            page.set_viewport_size({"width": width, "height": 1080})
            dimensions = page.evaluate("""()=>({viewport:innerWidth,
              document:document.documentElement.scrollWidth,body:document.body.scrollWidth})""")
            assert max(dimensions["document"], dimensions["body"]) <= dimensions["viewport"], dimensions
        page.set_viewport_size({"width": 1440, "height": 1080})
    # Selecting another Bot does not inherit the former resource's invalidation.
    select_bot(page, "bot-b")
    expect(page.locator('#agent-config [name="preset_context"]')).to_be_enabled()
    expect(page.locator('#memory-config [name="deployment_id"]')).to_be_enabled()
    assert not page.evaluate("fixture.unexpected")


def cancel_check(page, mode):
    page.locator("#job-list button").click()
    page.evaluate("fixture.overrides['POST /bots/bot-a/jobs/run-requests']={status:200,body:{status:'ENQUEUED'}}")
    page.locator("#job-run").click()
    expect(page.locator("#notice")).to_have_class("notice success")
    key = "POST /bots/bot-a/jobs/cancellations"
    page.evaluate("({key,mode})=>fixture.overrides[key]=mode==='receipt'?{status:200,body:fixture.receipt}:"
                  "{status:409,body:{code:'CONTROL_CONTENT_REVOKED',message:'相关内容已不可用'}}", {"key": key, "mode": mode})
    page.locator("#job-cancel-reason").fill("Current Bot cancellation receipt")
    page.locator("#job-cancel").click()
    expect(page.locator("#notice")).to_contain_text("不可用")
    expect(page.locator("#notice")).not_to_have_class("notice success")
    if mode == "receipt":
        expect(page.locator("#notice")).to_contain_text("recorded-operation")
    expect(page.locator("#job-existing-actions")).to_be_hidden()
    expect(page.locator("#job-list")).to_be_empty()
    before = page.evaluate("fixture.calls.filter(call=>call.method==='POST').length")
    page.locator("#job-cancel").dispatch_event("click")
    page.evaluate("()=>new Promise(resolve=>requestAnimationFrame(resolve))")
    assert page.evaluate("fixture.calls.filter(call=>call.method==='POST').length") == before

    # A response for the previous Bot may invalidate that resource's requests,
    # but must not replace the new Bot's notice, draft, or selected Job.
    select_bot(page)
    page.locator("#job-list button").click()
    page.locator("#job-cancel-reason").fill("Old Bot cancellation receipt")
    page.evaluate("key=>fixture.hold.push(key)", key)
    page.locator("#job-cancel").click()
    page.wait_for_function("key=>fixture.pending.some(item=>item.key===key)", arg=key)
    select_bot(page, "bot-b")
    page.locator("#job-list button").click()
    page.locator('#job-config [name="prompt"]').fill("Keep independent Bot B draft")
    page.evaluate("fixture.overrides['POST /bots/bot-b/jobs/run-requests']={status:200,body:{status:'ENQUEUED'}}")
    page.locator("#job-run").click()
    expect(page.locator("#notice")).to_have_class("notice success")
    current_notice = page.locator("#notice").inner_text()
    page.evaluate("key=>fixture.release(key)", key)
    page.evaluate("()=>new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve)))")
    expect(page.locator("#notice")).to_have_text(current_notice)
    expect(page.locator("#notice")).to_have_class("notice success")
    expect(page.locator('#job-config [name="prompt"]')).to_have_value("Keep independent Bot B draft")
    expect(page.locator("#job-existing-actions")).to_be_visible()
    assert not page.evaluate("fixture.pending")


def logout_check(page):
    values = {**DEFAULT_MEMORY_SETTINGS, "deployment_id": "PRIVATE-OLD-MEMORY"}
    key = "GET /bots/bot-a/memory/configuration"
    page.evaluate("({key,row})=>fixture.overrides[key]={status:200,body:row}", {
        "key": key, "row": {"revision": 1, "values": values, "has_api_key": True, "secret_revision": 1}
    })
    page.locator('#memory-config [name="api_key"]').fill("PRIVATE-KEY-DRAFT")
    page.evaluate("key=>fixture.hold.push(key)", key)
    page.locator("#memory-reload").click()
    page.wait_for_function("key=>fixture.pending.some(item=>item.key===key)", arg=key)
    page.evaluate("fixture.overrides['POST /logout']={status:200,body:{}}")
    page.locator("#logout").click()
    expect(page.locator("#login")).to_be_visible()
    expect(page.locator('#memory-config [name="api_key"]')).to_have_value("")
    expect(page.locator("#memory-config-status")).to_have_text("")
    page.evaluate("key=>fixture.release(key)", key)
    page.evaluate("()=>new Promise(resolve=>requestAnimationFrame(()=>requestAnimationFrame(resolve)))")
    expect(page.locator('#memory-config [name="deployment_id"]')).to_have_value("")
    expect(page.locator('#memory-config [name="deployment_id"]')).to_be_disabled()
    expect(page.locator("#memory-state")).to_be_empty()
    expect(page.locator("#bot-memory")).to_be_hidden()
    expect(page.locator("#login")).to_be_visible()
    assert not page.evaluate("fixture.pending")


def configuration_repair_check(page, name, late_read):
    prefix, form_id, preview_id, _, apply_id, reload_id = PANELS[name]
    key = "GET /bots/bot-a/" + prefix + "/configuration"
    post = "/bots/bot-a/" + prefix + "/change-plans"
    if name == "binding":
        page.locator('[data-binding-id="binding-a"]').click()
        defaults = {"enabled": True, "api_base_url": "http://127.0.0.1:5700", "response_mode": "component",
            "response_template": "New template", "trigger_mentions": True, "trigger_prefix": "",
            "private_trigger": True, "admin_user_ids": []}
        row = {"binding_id": "binding-a", "self_id": "123", "display_name": None, "state": "ONLINE",
            "revision": 7, "effective_revision": 6, "desired_values": None, "defaults": defaults,
            "desired_values_payload_state": "UNAVAILABLE", "config_payload_state": "ACTIVE",
            "display_name_payload_state": "UNAVAILABLE", "webhook_url": "http://127.0.0.1/onebot/binding-a"}
        field, secret, status = "response_template", "access_token", "binding-state"
    else:
        defaults = ({**DEFAULT_AGENT_SETTINGS, "model": "fixture-model"}
                    if name == "agent" else DEFAULT_MEMORY_SETTINGS)
        row = {"revision": 7, "secret_revision": 4, "has_api_key": True, "values": None,
            "values_payload_state": "UNAVAILABLE", "defaults": defaults}
        field, secret, status = (("preset_context", "api_key", "agent-config-status") if name == "agent"
                                else ("deployment_id", "api_key", "memory-config-status"))
    form = page.locator("#" + form_id)
    form.locator('[name="' + field + '"]').fill("OLD-CONFIGURATION-CANARY")
    form.locator('[name="' + secret + '"]').fill("OLD-SECRET-DRAFT")
    page.evaluate("({key,row})=>fixture.overrides[key]={status:200,body:row}", {"key": key, "row": row})
    page.locator("#" + reload_id).click()
    expect(page.locator("#" + status)).to_contain_text("正文已清理")
    expect(form.locator('[name="' + field + '"]')).to_have_value("")
    expect(form.locator('[name="' + field + '"]')).to_be_disabled()
    expect(form.locator('[name="' + secret + '"]')).to_have_value("")
    if name == "binding":
        expect(page.locator("#binding-name")).to_have_text("名称已清理 · 123")
        expect(page.locator("#binding-state")).to_contain_text("已生效 6")
    before = page.evaluate("path=>fixture.calls.filter(x=>x.method==='POST'&&x.path===path).length", post)
    form.evaluate("form=>form.dispatchEvent(new Event('submit',{bubbles:true,cancelable:true}))")
    assert page.evaluate("path=>fixture.calls.filter(x=>x.method==='POST'&&x.path===path).length", post) == before
    if late_read:
        page.evaluate("key=>fixture.hold.push(key)", key)
        page.locator("#" + reload_id).click()
        page.wait_for_function("key=>fixture.pending.some(x=>x.key===key)", arg=key)
    page.locator("#" + name + "-replace-configuration").click()
    expect(form.locator('[name="' + field + '"]')).to_be_enabled()
    expect(page.locator("#" + status)).to_contain_text("草稿")
    canary = "EXPLICIT-NEW-" + name
    form.locator('[name="' + field + '"]').fill(canary)
    if late_read:
        page.evaluate("key=>fixture.release(key)", key)
        expect(page.locator("#notice")).to_contain_text("保留")
        expect(form.locator('[name="' + field + '"]')).to_have_value(canary)
        expect(form.locator('[name="' + field + '"]')).to_be_enabled()
    form.locator('button[type="submit"]').click()
    expect(page.locator("#" + preview_id)).to_be_visible()
    submitted = page.evaluate("path=>fixture.calls.filter(x=>x.method==='POST'&&x.path===path).at(-1).body", post)
    assert submitted["expected_revision"] == 7 and submitted["values"][field] == canary
    assert set(submitted["values"]) == set(defaults)
    assert "OLD-CONFIGURATION-CANARY" not in json.dumps(submitted) and "OLD-SECRET-DRAFT" not in json.dumps(submitted)
    saved = {**row, "revision": 8, "values_payload_state": "ACTIVE", "values": submitted["values"]}
    if name == "binding":
        saved.update(desired_values=submitted["values"], desired_values_payload_state="ACTIVE")
    page.evaluate("({key,row,post})=>{fixture.overrides[key]={status:200,body:row};"
        "fixture.overrides[post]={status:200,body:{revision:8,secret_revision:4}};}",
        {"key": key, "row": saved, "post": "POST /bots/bot-a/" + prefix + "/management-requests"})
    page.locator("#" + apply_id).click()
    expect(page.locator("#" + preview_id)).to_be_hidden()
    expect(form.locator('[name="' + field + '"]')).to_have_value(canary)
    expect(page.locator("#" + status)).to_contain_text("8")
    assert not page.evaluate("fixture.pending")


def component_configuration_repair_check(page, late_read):
    prefix = "/bots/bot-a/components/alpha"
    key = "GET " + prefix + "/configuration"
    row = {"desired_revision": 7, "effective_revision": 6, "desired_values": None,
        "effective_values": {"package_digest": "pkg-alpha", "config": {"label": "EFFECTIVE-ONLY"}, "grants": []},
        "desired_values_payload_state": "UNAVAILABLE", "effective_values_payload_state": "ACTIVE",
        "defaults": {"package_digest": "", "config": {}, "grants": []},
        "operations": [], "runtime": {"components": []}}
    form = page.locator("#bot-config-form")
    form.locator('[name="label"]').fill("OLD-COMPONENT-CANARY")
    page.evaluate("({key,row})=>fixture.overrides[key]={status:200,body:row}", {"key": key, "row": row})
    page.locator("#reload-bot-config").click()
    expect(page.locator("#bot-runtime-status")).to_contain_text("已保存配置正文已清理")
    expect(page.locator("#bot-runtime-status")).not_to_contain_text("已生效配置正文已清理")
    expect(page.locator("#bot-config-fields")).to_be_empty()
    expect(form.locator('button[type="submit"]')).to_be_disabled()
    expect(page.locator("#bot-grants")).not_to_be_checked()
    form.evaluate("form=>form.dispatchEvent(new Event('submit',{bubbles:true,cancelable:true}))")
    expect(form.locator('button[type="submit"]')).to_be_disabled()
    assert not page.evaluate("path=>fixture.calls.some(x=>x.method==='POST'&&x.path===path)", prefix + "/change-plans")
    page.evaluate("({key})=>fixture.overrides[key]={status:200,body:{next_before:null,revisions:["
        "{revision:7,created_at:'2030-01-01T00:00:00Z',values:null,values_payload_state:'UNAVAILABLE'},"
        "{revision:6,created_at:'2029-01-01T00:00:00Z',values:{package_digest:'pkg-alpha',config:{label:'HISTORY-ONLY'},grants:[]},values_payload_state:'ACTIVE'}]}}",
        {"key": "GET " + prefix + "/configuration/history"})
    page.locator("#read-bot-history").click()
    expect(page.locator("#bot-history button")).to_have_count(2)
    expect(page.locator("#bot-history button").nth(0)).to_be_disabled()
    expect(page.locator("#bot-history button").nth(1)).to_be_enabled()
    expect(page.locator("#bot-history")).to_contain_text("修订 7")
    expect(page.locator("#bot-history")).to_contain_text("配置正文已清理")
    page.locator("#bot-history button").nth(1).click()
    expect(form.locator('[name="label"]')).to_have_value("HISTORY-ONLY")
    form.locator('[name="label"]').fill("EDITED-HISTORY")
    form.locator('button[type="submit"]').click()
    expect(page.locator("#bot-config-preview")).to_be_visible()
    copied = page.evaluate("path=>fixture.calls.filter(x=>x.method==='POST'&&x.path===path).at(-1).body", prefix + "/change-plans")
    assert copied == {"expected_revision": 7, "source_revision": 6, "values": {"package_digest": "pkg-alpha",
        "config": {"label": "EDITED-HISTORY"}, "grants": []}}
    if late_read:
        page.evaluate("key=>fixture.hold.push(key)", key)
        page.locator("#reload-bot-config").click()
        page.wait_for_function("key=>fixture.pending.some(x=>x.key===key)", arg=key)
    page.locator("#component-replace-configuration").click()
    expect(form.locator('[name="label"]')).to_have_value("")
    expect(page.locator("#bot-runtime-status")).to_contain_text("草稿")
    form.locator('[name="label"]').fill("EXPLICIT-NEW-COMPONENT")
    if late_read:
        page.evaluate("key=>fixture.release(key)", key)
        expect(page.locator("#notice")).to_contain_text("保留")
        expect(form.locator('[name="label"]')).to_have_value("EXPLICIT-NEW-COMPONENT")
    form.locator('button[type="submit"]').click()
    expect(page.locator("#bot-config-preview")).to_be_visible()
    submitted = page.evaluate("path=>fixture.calls.filter(x=>x.method==='POST'&&x.path===path).at(-1).body", prefix + "/change-plans")
    assert submitted == {"expected_revision": 7, "values": {"package_digest": "pkg-alpha",
        "config": {"label": "EXPLICIT-NEW-COMPONENT"}, "grants": []}}
    saved = {**row, "desired_revision": 8, "desired_values": submitted["values"], "desired_values_payload_state": "ACTIVE"}
    page.evaluate("({key,row,post})=>{fixture.overrides[key]={status:200,body:row};"
        "fixture.overrides[post]={status:200,body:{revision:8}};}",
        {"key": key, "row": saved, "post": "POST " + prefix + "/management-requests"})
    page.locator("#bot-config-apply").click()
    expect(page.locator("#bot-config-preview")).to_be_hidden()
    expect(page.locator("#bot-desired")).to_have_text("8")
    expect(page.locator("#bot-effective")).to_have_text("6")
    expect(form.locator('[name="label"]')).to_have_value("EXPLICIT-NEW-COMPONENT")
    assert not page.evaluate("fixture.pending")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--browser", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    static = Path(__file__).resolve().parents[1] / "src/forest/management/static"
    html = (static / "index.html").read_text(encoding="utf-8").replace(
        '<script type="module" src="/static/app.js"></script>',
        '<script type="module" src="/fixture.js"></script>',
    )

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            resources = {"/": (html.encode(), "text/html; charset=utf-8"),
                         "/fixture.js": (BOOTSTRAP.encode(), "text/javascript")}
            for file in static.iterdir():
                if file.suffix in {".js", ".css"}:
                    resources["/static/" + file.name] = (
                        file.read_bytes(), "text/css" if file.suffix == ".css" else "text/javascript"
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

        def log_message(self, *_):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    results = []
    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(
                executable_path=str(args.browser), headless=True,
                args=["--no-proxy-server", "--disable-gpu"],
            )
            try:
                for name, mode in [("protocol", "barriers"), *[
                    (name, mode) for name in PANELS for mode in ("receipt", "http")
                ], ("job-cancel", "receipt"), ("job-cancel", "http"), ("logout", "late-memory"),
                    *[(name + "-repair", mode) for name in ("agent", "memory", "binding", "component") for mode in ("replace", "late-read")]]:
                    page = browser.new_page(viewport={"width": 1440, "height": 1080})
                    errors = []
                    page.on("pageerror", lambda error: errors.append(str(error)))
                    try:
                        page.goto(f"http://127.0.0.1:{server.server_port}")
                        page.wait_for_function("window.fixtureReady")
                        expect(page.locator("#configuration")).to_be_visible()
                        if name == "protocol":
                            protocol_checks(page)
                        else:
                            page.locator("#nav-bots").click()
                            select_bot(page)
                            if name == "job-cancel":
                                cancel_check(page, mode)
                            elif name == "component-repair":
                                component_configuration_repair_check(page, mode == "late-read")
                            elif name.endswith("-repair"):
                                configuration_repair_check(page, name.removesuffix("-repair"), mode == "late-read")
                            elif name == "logout":
                                logout_check(page)
                            else:
                                panel_check(page, name, mode)
                        assert not errors, errors
                        results.append({"panel": name, "mode": mode, "status": "PASS"})
                        print(f"PASS {name}: {mode}", flush=True)
                    finally:
                        page.close()
            finally:
                browser.close()
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
        assert not thread.is_alive()
    report = {"kind": "real Edge / shipped JS / controlled API fixture", "cases": results}
    if args.output:
        args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    main()
