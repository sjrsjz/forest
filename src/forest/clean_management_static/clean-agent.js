export async function mount({root, component, api, notice, refresh}) {
  const get=id=>root.querySelector(`#${id}`);
  const path=`/management/v1/modules/${encodeURIComponent(component.component_id)}/configuration`;
  const queryPath=`/management/v1/modules/${encodeURIComponent(component.component_id)}/queries`;
  const actionPath=`/management/v1/modules/${encodeURIComponent(component.component_id)}/actions`;
  const current=await api(path), profile=current.desired.value;
  let messages=[...profile.preset_context];
  let runCursor=null, runSnapshot=null, runCount=0;
  get('agent-state').textContent=`期望 r${current.desired.revision} · 生效 r${current.effective.revision} · ${current.reload_policy}`;
  get('agent-spec').value=profile.agent_spec_revision;
  get('agent-timeout').value=profile.run_timeout_seconds;
  get('agent-steps').value=profile.max_steps;
  get('agent-tokens').value=profile.output_tokens;
  get('agent-stream').checked=profile.stream;
  get('core-identity').textContent=`${current.core_prompt.protocol_id}/v${current.core_prompt.protocol_version} · ${current.core_prompt.digest} · ${current.core_prompt.parser_revision}`;
  get('core-text').value=current.core_prompt.text;

  const terminal=new Set(['COMPLETED','FAILED','CANCELLED']);
  const stamp=value=>value?new Date(value).toLocaleString():'—';
  async function inspectRun(runId){
    const detail=await api(`${queryPath}/agent.run.read`,{method:'POST',body:JSON.stringify({run_id:runId})});
    const visible={run_id:detail.run_id,session_id:detail.session_id,conversation_ref:detail.conversation_ref,state:detail.state,checkpoint_seq:detail.checkpoint_seq,input_revision:detail.input_revision,next_step_seq:detail.next_step_seq,error_code:detail.error_code,cancel_request:detail.cancel_request,triggers:detail.triggers,current_request:detail.current_request,current_attempt:detail.current_attempt,current_tool:detail.current_tool,latest_checkpoint:detail.latest_checkpoint,history:detail.history};
    get('run-detail').textContent=JSON.stringify(visible,null,2);get('run-detail').classList.remove('hidden');
  }
  async function cancelRun(row,button){
    button.disabled=true;notice('');try{
      const result=await api(`${actionPath}/agent.run.cancel`,{method:'POST',headers:{'idempotency-key':crypto.randomUUID()},body:JSON.stringify({run_id:row.run_id,expected_checkpoint_seq:row.checkpoint_seq,reason_code:'ADMIN_REQUESTED'})});
      notice(`已登记取消：${result.run_id}，检查点 ${result.expected_checkpoint_seq}`);await loadRuns(true);
    }catch(error){notice(error.message);}finally{button.disabled=false;}
  }
  function appendRuns(items){
    const list=get('run-list');
    for(const row of items){const item=document.createElement('div');item.className='run-grid run-row';
      const identity=document.createElement('span');const run=document.createElement('strong');run.textContent=row.run_id;const conversation=document.createElement('small');conversation.textContent=row.conversation_ref;identity.append(run,conversation);
      const state=document.createElement('span');state.textContent=row.state;state.className=`run-state state-${row.state.toLowerCase()}`;
      const checkpoint=document.createElement('code');checkpoint.textContent=`${row.checkpoint_seq} / input ${row.input_revision}`;
      const updated=document.createElement('span');updated.textContent=stamp(row.updated_at);
      const actions=document.createElement('span');actions.className='run-actions';const inspect=document.createElement('button');inspect.type='button';inspect.className='quiet';inspect.textContent='详情';inspect.onclick=()=>inspectRun(row.run_id).catch(error=>notice(error.message));
      const cancel=document.createElement('button');cancel.type='button';cancel.textContent=row.cancel_operation_id?'已请求取消':'取消';cancel.disabled=terminal.has(row.state)||Boolean(row.cancel_operation_id);cancel.onclick=()=>cancelRun(row,cancel);actions.append(inspect,cancel);item.append(identity,state,checkpoint,updated,actions);list.append(item);}
  }
  async function loadRuns(reset){
    const button=get('run-search');button.disabled=true;notice('');try{
      if(reset){runCursor=null;runSnapshot=null;runCount=0;get('run-list').replaceChildren();get('run-detail').classList.add('hidden');}
      const states=get('run-state').value?get('run-state').value.split(','):[];
      const payload={states,conversation_ref:get('run-conversation').value.trim()||null,limit:50};
      if(runSnapshot)payload.snapshot_created_at=runSnapshot;
      if(runCursor){payload.before_created_at=runCursor.created_at;payload.before_run_id=runCursor.run_id;}
      const page=await api(`${queryPath}/agent.runs.query`,{method:'POST',body:JSON.stringify(payload)});runSnapshot=page.snapshot_created_at;appendRuns(page.items);runCount+=page.items.length;
      runCursor=page.has_more?{created_at:page.next_before_created_at,run_id:page.next_before_run_id}:null;get('run-more').classList.toggle('hidden',!runCursor);get('run-count').textContent=`已显示 ${runCount} 条`;
    }catch(error){notice(error.message);}finally{button.disabled=false;}
  }
  get('run-search').onclick=()=>loadRuns(true);get('run-refresh').onclick=()=>loadRuns(true);get('run-more').onclick=()=>loadRuns(false);
  await loadRuns(true);

  function renderPreset() {
    const list=get('preset-list');list.replaceChildren();
    if(!messages.length){const empty=document.createElement('p');empty.textContent='当前没有 Bot 预设段落。';empty.className='muted';list.append(empty);return;}
    messages.forEach((message,index)=>{const item=document.createElement('div');item.className='preset-item';
      const editor=document.createElement('textarea');editor.value=message;editor.maxLength=32768;editor.setAttribute('aria-label',`PresetContext ${index+1}`);editor.oninput=()=>{messages[index]=editor.value;};
      const remove=document.createElement('button');remove.type='button';remove.className='quiet';remove.textContent='删除';remove.onclick=()=>{messages.splice(index,1);renderPreset();};item.append(editor,remove);list.append(item);});
  }
  renderPreset();
  get('preset-add').onclick=()=>{if(messages.length>=32){notice('PresetContext 最多 32 段。');return;}messages.push('');renderPreset();};
  get('agent-save').onclick=async()=>{const button=get('agent-save');button.disabled=true;notice('');try{
    const value={protocol:'forest.agent.management-profile/1',agent_spec_revision:get('agent-spec').value,preset_context:messages,run_timeout_seconds:Number(get('agent-timeout').value),max_steps:Number(get('agent-steps').value),output_tokens:Number(get('agent-tokens').value),stream:get('agent-stream').checked};
    const key=crypto.randomUUID();const plan=await api(`${path}/plans`,{method:'POST',headers:{'idempotency-key':`${key}:plan`},body:JSON.stringify({expected_desired_revision:current.desired.revision,value})});
    const applied=await api(`${path}/apply`,{method:'POST',headers:{'idempotency-key':`${key}:apply`},body:JSON.stringify({plan_id:plan.plan_id,expected_desired_revision:current.desired.revision})});
    notice(`Agent 配置 r${applied.configuration.revision} 已随 RuntimeGraph ${applied.graph_revision} 生效。`);await refresh();
  }catch(error){notice(error.message);}finally{button.disabled=false;}};
}
