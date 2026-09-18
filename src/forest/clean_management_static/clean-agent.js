export async function mount({root, component, api, notice, refresh}) {
  const get=id=>root.querySelector(`#${id}`);
  const path=`/management/v1/modules/${encodeURIComponent(component.component_id)}/configuration`;
  const current=await api(path), profile=current.desired.value;
  let messages=[...profile.preset_context];
  get('agent-state').textContent=`期望 r${current.desired.revision} · 生效 r${current.effective.revision} · ${current.reload_policy}`;
  get('agent-spec').value=profile.agent_spec_revision;
  get('agent-timeout').value=profile.run_timeout_seconds;
  get('agent-steps').value=profile.max_steps;
  get('agent-tokens').value=profile.output_tokens;
  get('agent-stream').checked=profile.stream;
  get('core-identity').textContent=`${current.core_prompt.protocol_id}/v${current.core_prompt.protocol_version} · ${current.core_prompt.digest} · ${current.core_prompt.parser_revision}`;
  get('core-text').value=current.core_prompt.text;

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
