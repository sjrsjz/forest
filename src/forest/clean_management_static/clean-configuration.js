export async function mount({root, component, api, notice, refresh}) {
  const get=id=>root.querySelector(`#${id}`), current=await api(`/management/v1/modules/${encodeURIComponent(component.component_id)}/configuration`);
  get('config-editor').value=JSON.stringify(current.desired.value,null,2);
  get('config-state').textContent=`期望 r${current.desired.revision} · 生效 r${current.effective.revision} · ${current.reload_policy}`;
  get('config-contract').textContent=JSON.stringify({schema_digest:current.schema_digest,schema:current.schema},null,2);
  get('config-save').onclick=async()=>{const button=get('config-save');button.disabled=true;notice('');try{let value;try{value=JSON.parse(get('config-editor').value);}catch{throw new Error('JSON 格式无效。');}
    const key=crypto.randomUUID();const plan=await api(`/management/v1/modules/${encodeURIComponent(component.component_id)}/configuration/plans`,{method:'POST',headers:{'idempotency-key':`${key}:plan`},body:JSON.stringify({expected_desired_revision:current.desired.revision,value})});
    const applied=await api(`/management/v1/modules/${encodeURIComponent(component.component_id)}/configuration/apply`,{method:'POST',headers:{'idempotency-key':`${key}:apply`},body:JSON.stringify({plan_id:plan.plan_id,expected_desired_revision:current.desired.revision})});
    notice(`配置 r${applied.configuration.revision} 已随 RuntimeGraph ${applied.graph_revision} 生效。`);await refresh();
  }catch(error){notice(error.message);}finally{button.disabled=false;}};
}
