let token = sessionStorage.getItem('forest.management.token') || '';
let snapshot = null;
const $ = id => document.getElementById(id);

async function api(path, options = {}) {
  const response = await fetch(path, {credentials:'omit', ...options, headers:{
    'content-type':'application/json', 'x-forest-management-token':token, ...(options.headers || {}),
  }});
  const body = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(body.message || body.code || `HTTP ${response.status}`);
  return body;
}
function notice(message) { $('notice').textContent=message; $('notice').classList.toggle('hidden',!message); }
function showOverview(){ $('overview').classList.remove('hidden'); $('module-view').classList.add('hidden'); document.querySelectorAll('#module-nav button').forEach(x=>x.classList.remove('selected')); }

async function openModule(row, button) {
  document.querySelectorAll('#module-nav button').forEach(x=>x.classList.toggle('selected',x===button));
  $('overview').classList.add('hidden'); $('module-view').classList.remove('hidden');
  $('component-title').textContent=row.component_id; $('component-kind').textContent=`${row.state} · GENERATION`;
  const nav=$('contribution-nav'), root=$('contribution-root'); nav.replaceChildren(); root.replaceChildren();
  const contributions=row.management_contributions || [];
  if (!contributions.length) { const p=document.createElement('p');p.textContent='此 Generation 没有发布管理工作区。';p.className='muted';root.append(p);return; }
  for (const [index, contribution] of contributions.entries()) {
    const tab=document.createElement('button');tab.textContent=contribution.label;nav.append(tab);
    tab.onclick=async()=>{ nav.querySelectorAll('button').forEach(x=>x.classList.toggle('selected',x===tab));root.replaceChildren();
      try { const template=await fetch(contribution.surface.template).then(r=>{if(!r.ok)throw new Error('管理模板读取失败');return r.text()});root.innerHTML=template;
        if(contribution.surface.style&&!document.querySelector(`link[data-module-style="${CSS.escape(contribution.surface.style)}"]`)){const link=document.createElement('link');link.rel='stylesheet';link.href=contribution.surface.style;link.dataset.moduleStyle=contribution.surface.style;document.head.append(link);}
        const module=await import(contribution.surface.module);await module.mount({root,component:row,api,notice,refresh});
      } catch(error){ notice(error.message); }
    }; if(index===0)tab.click();
  }
}

async function refresh(){ snapshot=await api('/management/v1/runtime'); const runtime=snapshot.runtime;
  $('bot-id').textContent=runtime.bot_id;$('runtime-state').textContent=runtime.graph_state;$('graph-id').textContent=runtime.graph_id;$('module-count').textContent=snapshot.modules.length;
  const nav=$('module-nav'), table=$('module-table');nav.replaceChildren();table.replaceChildren();
  for(const row of snapshot.modules){const button=document.createElement('button');button.textContent=row.component_id;button.onclick=()=>openModule(row,button);nav.append(button);
    const item=document.createElement('div');item.className='module-row';
    const name=document.createElement('strong');name.textContent=row.component_id;const state=document.createElement('span');state.className='state';state.textContent=row.state;const generation=document.createElement('code');generation.textContent=row.generation_id||'—';item.append(name,state,generation);item.onclick=()=>openModule(row,button);table.append(item);}
}
$('login-form').onsubmit=async event=>{event.preventDefault();token=new FormData(event.currentTarget).get('token');try{await refresh();sessionStorage.setItem('forest.management.token',token);$('login').classList.add('hidden');$('console').classList.remove('hidden');$('login-error').textContent='';}catch(error){$('login-error').textContent=error.message;}};
$('logout').onclick=()=>{sessionStorage.removeItem('forest.management.token');location.reload();};$('back').onclick=showOverview;
if(token){refresh().then(()=>{$('login').classList.add('hidden');$('console').classList.remove('hidden');}).catch(()=>sessionStorage.removeItem('forest.management.token'));}
