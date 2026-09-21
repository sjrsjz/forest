const diagnosticProfile=()=>({
  profile_id:'diagnostic.echo',
  executable:'/usr/local/bin/python3.11',
  executable_sha256:'sha256:92d7e40ec50be176cb1b790c7568b7e08cd862137b5aa69f1413ba1967886b79',
  arguments_schema:{$schema:'https://json-schema.org/draft/2020-12/schema',type:'object',required:['value'],properties:{value:{type:'string',maxLength:256}},additionalProperties:false},
  argv:[{literal:'-c'},{literal:'import sys;print(sys.argv[1])'},{argument:'value'}],
  working_directory:'/app',environment:{PYTHONUNBUFFERED:'1'},run_as:{uid:10001,gid:10001},network_policy:'deny',require_posix_limits:true,
  limits:{cpu_seconds:5,memory_bytes:268435456,wall_seconds:5,output_bytes:4096,open_files:64,processes:16,terminate_grace_seconds:1},
});

const emptyProfile=()=>({
  profile_id:'new.profile',executable:'',executable_sha256:'sha256:',
  arguments_schema:{$schema:'https://json-schema.org/draft/2020-12/schema',type:'object',required:[],properties:{},additionalProperties:false},
  argv:[{literal:'--version'}],working_directory:'/app',environment:{},run_as:{uid:10001,gid:10001},network_policy:'deny',require_posix_limits:true,
  limits:{cpu_seconds:5,memory_bytes:268435456,wall_seconds:30,output_bytes:65536,open_files:64,processes:16,terminate_grace_seconds:2},
});

export async function mount({root,component,api,notice,refresh}){
  const get=id=>root.querySelector(`#${id}`);
  const path=`/management/v1/modules/${encodeURIComponent(component.component_id)}/configuration`;
  const current=await api(path);
  let profiles=structuredClone(current.desired.value.profiles||[]),selected=-1;
  get('executor-state').textContent=`期望 r${current.desired.revision} · 生效 r${current.effective.revision} · ${current.reload_policy}`;
  get('executor-contract').textContent=JSON.stringify({schema_digest:current.schema_digest,schema:current.schema},null,2);

  const parseJson=(id,label,accept)=>{let value;try{value=JSON.parse(get(id).value);}catch{throw new Error(`${label} 不是有效 JSON。`);}if(!accept(value))throw new Error(`${label} 的 JSON 类型无效。`);return value;};
  const integer=(id,label,min,max)=>{const value=Number(get(id).value);if(!Number.isInteger(value)||value<min||value>max)throw new Error(`${label} 超出允许范围。`);return value;};
  function readForm(validate=false){
    const profileId=get('executor-id').value.trim(),executable=get('executor-path').value.trim(),digest=get('executor-sha').value.trim(),workingDirectory=get('executor-workdir').value.trim();
    if(validate&&!/^[a-z0-9][a-z0-9._-]{0,63}$/.test(profileId))throw new Error('Profile ID 格式无效。');
    if(validate&&!executable.startsWith('/'))throw new Error('可执行文件必须使用绝对路径。');
    if(validate&&!/^sha256:[0-9a-f]{64}$/.test(digest))throw new Error('SHA-256 必须是 sha256: 加 64 位小写十六进制。');
    if(validate&&!workingDirectory.startsWith('/'))throw new Error('工作目录必须使用绝对路径。');
    return {profile_id:profileId,executable,executable_sha256:digest,arguments_schema:parseJson('executor-arguments-schema','arguments_schema',value=>value&&typeof value==='object'&&!Array.isArray(value)),argv:parseJson('executor-argv','argv',value=>Array.isArray(value)&&value.length>0),working_directory:workingDirectory,environment:parseJson('executor-environment','环境变量',value=>value&&typeof value==='object'&&!Array.isArray(value)),run_as:{uid:integer('executor-uid','UID',0,2147483647),gid:integer('executor-gid','GID',0,2147483647)},network_policy:get('executor-network').value,require_posix_limits:get('executor-posix').checked,limits:{cpu_seconds:integer('executor-cpu','CPU 秒',1,3600),memory_bytes:integer('executor-memory','内存字节',16777216,68719476736),wall_seconds:integer('executor-wall','墙钟秒',1,3600),output_bytes:integer('executor-output','输出字节',1024,33554432),open_files:integer('executor-files','打开文件数',8,4096),processes:integer('executor-processes','进程数',1,1024),terminate_grace_seconds:integer('executor-grace','终止宽限秒',1,60)}};
  }
  function saveSelected(validate=false){if(selected<0)return;const value=readForm(validate);if(validate&&profiles.some((item,index)=>index!==selected&&item.profile_id===value.profile_id))throw new Error(`Profile ID ${value.profile_id} 已存在。`);profiles[selected]=value;}
  function writeForm(value){get('executor-id').value=value.profile_id;get('executor-path').value=value.executable;get('executor-sha').value=value.executable_sha256;get('executor-workdir').value=value.working_directory;get('executor-network').value=value.network_policy;get('executor-uid').value=value.run_as.uid;get('executor-gid').value=value.run_as.gid;get('executor-posix').checked=value.require_posix_limits;get('executor-arguments-schema').value=JSON.stringify(value.arguments_schema,null,2);get('executor-argv').value=JSON.stringify(value.argv,null,2);get('executor-environment').value=JSON.stringify(value.environment,null,2);get('executor-cpu').value=value.limits.cpu_seconds;get('executor-memory').value=value.limits.memory_bytes;get('executor-wall').value=value.limits.wall_seconds;get('executor-output').value=value.limits.output_bytes;get('executor-files').value=value.limits.open_files;get('executor-processes').value=value.limits.processes;get('executor-grace').value=value.limits.terminate_grace_seconds;}
  function renderList(){const list=get('executor-list');list.replaceChildren();get('executor-count').textContent=String(profiles.length);for(const [index,profile] of profiles.entries()){const button=document.createElement('button');button.type='button';button.className='executor-profile';button.classList.toggle('selected',index===selected);const name=document.createElement('span');name.textContent=profile.profile_id;const pathLabel=document.createElement('small');pathLabel.textContent=profile.executable||'尚未设置可执行文件';button.append(name,pathLabel);button.onclick=()=>{notice('');try{saveSelected();selected=index;writeForm(profiles[index]);get('executor-empty').classList.add('hidden');get('executor-form').classList.remove('hidden');renderList();}catch(error){notice(error.message);}};list.append(button);}}
  function append(profile){notice('');try{saveSelected();if(profiles.some(item=>item.profile_id===profile.profile_id))throw new Error(`Profile ID ${profile.profile_id} 已存在。`);profiles.push(profile);selected=profiles.length-1;writeForm(profile);get('executor-empty').classList.add('hidden');get('executor-form').classList.remove('hidden');renderList();}catch(error){notice(error.message);}}
  get('executor-add').onclick=()=>append(emptyProfile());
  get('executor-template').onclick=()=>append(diagnosticProfile());
  get('executor-remove').onclick=()=>{if(selected<0)return;profiles.splice(selected,1);selected=Math.min(selected,profiles.length-1);if(selected<0){get('executor-form').classList.add('hidden');get('executor-empty').classList.remove('hidden');}else writeForm(profiles[selected]);renderList();};
  get('executor-apply').onclick=async()=>{const button=get('executor-apply');button.disabled=true;notice('');try{saveSelected(true);const key=crypto.randomUUID(),value={protocol:'forest.process.executor/1',profiles};const plan=await api(`${path}/plans`,{method:'POST',headers:{'idempotency-key':`${key}:plan`},body:JSON.stringify({expected_desired_revision:current.desired.revision,value})});const applied=await api(`${path}/apply`,{method:'POST',headers:{'idempotency-key':`${key}:apply`},body:JSON.stringify({plan_id:plan.plan_id,expected_desired_revision:current.desired.revision})});notice(`配置 r${applied.configuration.revision} 已随 RuntimeGraph ${applied.graph_revision} 生效。`);await refresh();}catch(error){notice(error.message);}finally{button.disabled=false;}};
  renderList();if(profiles.length){selected=0;writeForm(profiles[0]);get('executor-empty').classList.add('hidden');get('executor-form').classList.remove('hidden');renderList();}
}
