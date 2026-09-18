export async function mount({root, component, api, notice, refresh}) {
  const get=id=>root.querySelector(`#${id}`), base=`/management/v1/modules/${encodeURIComponent(component.component_id)}`;
  const response=await api(`${base}/queries/onebot.bindings.list`,{method:'POST',body:'{}'}), bindings=response.items;
  let selected=null;
  function value(id,fallback){const raw=get(id).value;return raw===''?fallback:Number(raw);}
  function open(binding,button){selected=binding;root.querySelectorAll('.binding-item').forEach(x=>x.classList.toggle('selected',x===button));
    get('binding-title').textContent=binding.binding_id;get('binding-state').textContent=`${binding.state} · 期望 r${binding.desired_revision} · 生效 r${binding.effective_revision}`;
    get('binding-self').value=binding.self_id;get('binding-transport').value=binding.transport;get('binding-secret').value=binding.secret_ref;get('binding-error').value=binding.error_code||'—';
    get('binding-enabled').checked=binding.desired_enabled;get('binding-receive').checked=binding.receive_enabled;
    const endpoint=binding.transport==='FORWARD_WEBSOCKET'?'websocket_url':'api_base_url';get('binding-endpoint').value=binding.config[endpoint]||'';get('binding-timeout').value=binding.config.timeout_seconds||15;get('binding-event-capacity').value=binding.config.event_capacity||256;get('binding-pending-capacity').value=binding.config.pending_capacity||64;
    get('binding-fence').textContent=JSON.stringify({binding_epoch:binding.binding_epoch,connection_epoch:binding.connection_epoch,current_client_incarnation:binding.current_client_incarnation,secret_digest:binding.secret_digest},null,2);
  }
  const list=get('binding-items');for(const binding of bindings){const button=document.createElement('button');button.className='binding-item';const title=document.createElement('strong');title.textContent=binding.binding_id;const detail=document.createElement('span');detail.textContent=`QQ ${binding.self_id}`;const state=document.createElement('small');state.textContent=binding.state;button.append(title,detail,state);button.onclick=()=>open(binding,button);list.append(button);if(!selected)open(binding,button);}
  if(!bindings.length){list.textContent='没有已配置的 Binding';get('binding-save').disabled=true;}
  get('binding-enabled').onchange=()=>{if(!get('binding-enabled').checked)get('binding-receive').checked=false;};
  get('binding-receive').onchange=()=>{if(get('binding-receive').checked)get('binding-enabled').checked=true;};
  get('binding-save').onclick=async()=>{if(!selected)return;const button=get('binding-save');button.disabled=true;notice('');try{const endpoint=selected.transport==='FORWARD_WEBSOCKET'?'websocket_url':'api_base_url';
    const config={...selected.config,transport:selected.transport,[endpoint]:get('binding-endpoint').value,timeout_seconds:value('binding-timeout',15)};if(selected.transport==='FORWARD_WEBSOCKET'){config.event_capacity=value('binding-event-capacity',256);config.pending_capacity=value('binding-pending-capacity',64);}
    const input={binding_id:selected.binding_id,expected_revision:selected.desired_revision,enabled:get('binding-enabled').checked,receive_enabled:get('binding-receive').checked,transport:selected.transport,config,secret_ref:selected.secret_ref,secret_digest:selected.secret_digest};
    const key=crypto.randomUUID();const plan=await api(`${base}/actions/onebot.binding.plan`,{method:'POST',headers:{'idempotency-key':`${key}:plan`},body:JSON.stringify(input)});
    const applied=await api(`${base}/actions/onebot.binding.apply`,{method:'POST',headers:{'idempotency-key':`${key}:apply`},body:JSON.stringify({plan_id:plan.plan_id})});notice(`Binding ${applied.binding_id} r${applied.revision} 已随 RuntimeGraph ${applied.graph_revision} 生效。`);await refresh();
  }catch(error){notice(error.message);}finally{button.disabled=false;}};
}
