import {clearConfiguration, replacementButton} from './configuration-forms.js';

const $ = id => document.getElementById(id);

// All fields are ordinary controls; users never edit configuration JSON.
const groups = [
  ['嵌入服务', [
    ['enabled', '启用此 Bot 的图文记忆', 'checkbox'],
    ['semantic_associations_enabled', '启用跨群自动语义联想（复用已生成向量）', 'checkbox'],
    ['endpoint', '完整嵌入 API 地址', 'url'],
    ['deployment_id', '部署身份（首次配置后随 Bot 生命周期冻结）', 'text'],
    ['model', '模型协议', ['jina-clip-v2', 'Qwen/Qwen3-VL-Embedding-8B']],
    ['model_revision', '模型与预处理版本声明', 'text'],
    ['dimensions', '向量维度', [64, 128, 256, 512, 768, 1024, 2048, 4096]],
    ['api_key', 'API 密钥（留空保留）', 'password'],
    ['clear_api_key', '清除已有 API 密钥', 'checkbox'],
  ]],
  ['索引与费用上限', [
    ['batch_size', '每批片段数', 'number', 1, 32],
    ['index_concurrency', '索引并发数', 'number', 1, 4],
    ['request_timeout', '嵌入请求超时（秒）', 'number', 1, 120],
    ['daily_request_budget', '每日请求上限', 'number', 1, 100000],
    ['daily_token_budget', '每日 token 上限', 'number', 1, 1000000000],
    ['request_token_reservation', '单次请求预留 token', 'number', 1, 1000000],
  ]],
  ['图片抓取与存储', [
    ['media_allowed_origins', '允许抓取的图片来源（每行一个完整 HTTP origin）', 'textarea'],
    ['capture_concurrency', '图片抓取并发数', 'number', 1, 4],
    ['capture_timeout', '单次抓取超时（秒）', 'number', 1, 30],
    ['capture_daily_bytes', '每日图片抓取预算（bytes）', 'number', 1, Number.MAX_SAFE_INTEGER],
    ['capture_storage_bytes', '图片存储上限（bytes）', 'number', 1, Number.MAX_SAFE_INTEGER],
    ['media_max_bytes', '单张图片上限（bytes，最多 4 MiB）', 'number', 1, 4194304],
    ['capture_max_attempts', '图片最多抓取次数', 'number', 1, 10],
  ]],
  ['检索与召回输出', [
    ['query_max_parts', '单次查询最多片段数', 'number', 1, 32],
    ['query_max_images', '单次查询最多图片数', 'number', 0, 8],
    ['query_limit', '最多召回条数', 'number', 1, 32],
    ['query_timeout', '查询超时（秒）', 'number', 1, 60],
    ['max_result_tokens', '召回文本 token 上限', 'number', 256, 16000],
    ['max_result_images', '召回图片上限', 'number', 0, 8],
    ['max_result_bytes', '召回总大小上限（bytes）', 'number', 1, 33554432],
  ]],
];

export function memoryPanel(api, busy, notice) {
  const form = $('memory-config');
  let bot = null, epoch = 0, edit = 0, readSeq = 0, statusSeq = 0, sourceSeq = 0;
  let associationSeq = 0;
  let configuration = null, pending = null, planKey = null, applyKey = null;
  let sourceView = null, forgetPlan = null, forgetPlanKey = null, forgetApplyKey = null;
  let associationView = null, associationPlan = null;
  let associationPlanKey = null, associationApplyKey = null;
  const replace = replacementButton($('memory-config-status'), 'memory-replace-configuration', () => {
    if (!configuration?.defaults) return;
    show({...configuration, values:structuredClone(configuration.defaults), values_payload_state:'DRAFT'});
  });
  for (const [title, fields] of groups) {
    const section = document.createElement('section'), heading = document.createElement('h3');
    heading.textContent = title; section.append(heading);
    for (const [name, title, type, min, max] of fields) {
      const label = document.createElement('label'); label.textContent = title;
      const field = document.createElement(Array.isArray(type) ? 'select' : type === 'textarea' ? 'textarea' : 'input');
      field.name = name;
      if (Array.isArray(type)) {
        for (const value of type) {
          const option = document.createElement('option'); option.value = value; option.textContent = value;
          field.append(option);
        }
      } else if (type === 'textarea') { field.rows = 3; field.maxLength = 65536; }
      else {
        field.type = type;
        if (type === 'number') { field.min = min; field.max = max; field.step = 1; }
        if (type === 'checkbox') label.className = 'check';
        if (type === 'text') field.maxLength = 128;
        if (type === 'url') field.maxLength = 2048;
        if (type === 'password') { field.maxLength = 4096; field.autocomplete = 'new-password'; }
      }
      field.required = !['checkbox','password','textarea'].includes(type);
      label.append(field); section.append(label);
    }
    $('memory-fields').append(section);
  }
  const path = () => `/bots/${encodeURIComponent(bot.bot_id)}/runtime-modules/memory.graph`;
  const valid = own => bot && own === epoch;
  function clearPlan() {
    edit++; pending = null; planKey = null; applyKey = null;
    $('memory-preview').classList.add('hidden');
  }
  function reset() {
    epoch++; readSeq++; statusSeq++; sourceSeq++; associationSeq++;
    bot = null; configuration = null;
    clearPlan(); form.reset(); $('memory-edit').disabled = true;
    $('bot-memory').classList.add('hidden'); $('memory-state').textContent = '';
    $('memory-config-status').textContent = '';
    clearForget(); $('memory-sources').replaceChildren(); $('memory-sources-status').textContent = '';
    $('memory-sources-more').disabled = true;
    clearAssociations();
    replace.classList.add('hidden');
  }
  function clearForget() {
    forgetPlan = null; forgetPlanKey = null; forgetApplyKey = null;
    $('memory-forget-plan').disabled = true;
    $('memory-forget-preview').classList.add('hidden');
    $('memory-forget-diff').textContent = '';
  }
  function clearAssociationPlan() {
    associationPlan = null; associationPlanKey = null; associationApplyKey = null;
    $('memory-association-preview').classList.add('hidden');
    $('memory-association-diff').textContent = '';
  }
  function clearAssociations() {
    associationView = null; clearAssociationPlan();
    $('memory-association-plan').disabled = true;
    $('memory-associations').replaceChildren();
    $('memory-associations-status').textContent = '';
  }
  function selectedSourceIds() {
    return [...$('memory-sources').querySelectorAll('input:checked')].map(item => item.value);
  }
  function updateSourceActions() {
    const count = selectedSourceIds().length;
    forgetPlan = null; forgetPlanKey = null; forgetApplyKey = null;
    $('memory-forget-preview').classList.add('hidden');
    clearAssociationPlan();
    $('memory-forget-plan').disabled = count === 0;
    $('memory-association-plan').disabled = count !== 2;
  }
  function sourceLabel(row) {
    const origin = row.source || {}, author = row.author || {};
    const group = origin.channel_id || origin.group_source_id || origin.source_id || '未知来源';
    const who = author.display_name || author.external_user_id || author.principal_ref || '未知作者';
    const text = (row.parts || []).filter(part => part.type === 'text').map(part => part.text).join(' ').trim();
    const images = (row.parts || []).filter(part => part.type === 'image').length;
    return `${group} · ${who} · ${row.kind} · ${text.slice(0, 180) || (images ? `${images} 张图片` : '无文本预览')}`;
  }
  function renderSources(result, append = false) {
    const root = $('memory-sources');
    if (append && sourceView && sourceView.deletion_epoch === result.deletion_epoch) {
      sourceView = {...result, sources:[...sourceView.sources, ...result.sources]};
      forgetPlan = null; forgetPlanKey = null; forgetApplyKey = null;
      $('memory-forget-preview').classList.add('hidden');
      clearAssociationPlan();
    } else {
      sourceView = result; clearForget(); clearAssociationPlan(); root.replaceChildren();
    }
    for (const row of result.sources) {
      const label = document.createElement('label'); label.className = 'check';
      const input = document.createElement('input'); input.type = 'checkbox'; input.value = row.source_unit_id;
      const body = document.createElement('span'), title = document.createElement('span'), id = document.createElement('code');
      title.textContent = sourceLabel(row); id.textContent = row.source_unit_id;
      body.append(title, document.createElement('br'), id); label.append(input, body); root.append(label);
      input.addEventListener('change', updateSourceActions);
    }
    updateSourceActions();
    const scrub = result.scrub || {};
    $('memory-sources-more').disabled = !sourceView.next_after;
    $('memory-sources-status').textContent = `删除版本 ${sourceView.deletion_epoch} · 已加载来源 ${sourceView.sources.length} · 待物理清理 ${scrub.revoked_payloads || 0} · 待传播 ${scrub.pending_links || 0}`;
  }
  async function refreshSources(after = null, append = false) {
    if (!bot) return;
    const own = epoch, seq = ++sourceSeq, prefix = path();
    const cursor = after ? `&after=${encodeURIComponent(after)}` : '';
    const result = await api(`${prefix}/queries/sources?limit=50${cursor}`);
    if (!valid(own) || seq !== sourceSeq) return;
    renderSources(result, append);
  }
  function renderAssociations(result) {
    associationView = result;
    const root = $('memory-associations'); root.replaceChildren();
    for (const row of result.associations) {
      const item = document.createElement('div'); item.className = 'memory-association';
      const description = document.createElement('p');
      description.textContent = `${sourceLabel(row.left)} ↔ ${sourceLabel(row.right)}`;
      const identity = document.createElement('code'); identity.textContent = row.association_id;
      const state = document.createElement('span');
      state.textContent = ` · 版本 ${row.revision} · ${row.active ? '有效' : '已解除'} · ${row.projection_state === 'CURRENT' ? '图已同步' : '等待后台发布'}`;
      const remove = document.createElement('button'); remove.type = 'button';
      remove.className = 'subtle';
      remove.textContent = row.active ? '预览解除联想' : '预览恢复联想';
      remove.addEventListener('click', () => previewAssociation(row.active ? {
          expected_deletion_epoch:result.deletion_epoch,
          expected_revision:row.revision,
          active:false,
          association_id:row.association_id,
        } : {
          expected_deletion_epoch:result.deletion_epoch,
          expected_revision:row.revision,
          active:true,
          source_unit_ids:[row.left.source_unit_id, row.right.source_unit_id],
        }, remove));
      item.append(description, identity, state, document.createElement('br'), remove); root.append(item);
    }
    $('memory-associations-status').textContent =
      `图版本 ${result.graph_revision} · 关系记录 ${result.associations.length} · 有效 ${result.associations.filter(row => row.active).length}`;
  }
  async function refreshAssociations() {
    if (!bot) return;
    const own = epoch, seq = ++associationSeq, prefix = path();
    const result = await api(`${prefix}/queries/associations?limit=50`);
    if (!valid(own) || seq !== associationSeq) return;
    renderAssociations(result);
  }
  async function previewAssociation(payload, button) {
    if (!bot) return;
    clearAssociationPlan();
    const own = epoch, seq = sourceSeq, prefix = path();
    associationPlanKey = crypto.randomUUID(); const key = associationPlanKey;
    await busy(button, async () => {
      const result = await api(`${prefix}/actions/association.plan`, payload, {
        'Idempotency-Key':key,
      });
      if (!valid(own) || seq !== sourceSeq || key !== associationPlanKey) return;
      associationPlan = result; associationApplyKey = crypto.randomUUID();
      $('memory-association-diff').textContent = JSON.stringify({
        action:result.active ? '创建联想' : '解除联想',
        left:sourceLabel(result.left), right:sourceLabel(result.right),
        association_id:result.association_id,
        expected_revision:result.expected_revision,
        deletion_epoch:result.expected_deletion_epoch,
        expires_at:result.expires_at,
      }, null, 2);
      $('memory-association-preview').classList.remove('hidden');
    });
  }
  function show(row) {
    configuration = row; clearPlan(); form.reset();
    replace.classList.add('hidden');
    if (row.values_payload_state === 'UNAVAILABLE' || row.values === null) {
      clearConfiguration(form); $('memory-edit').disabled = true;
      $('memory-config-status').textContent = `配置版本 ${row.revision} 的正文已清理。原修订和密钥记录仍保留；填写完整新配置后可保存替换。`;
      replace.disabled = !row.defaults; replace.classList.remove('hidden'); return;
    }
    for (const [name, value] of Object.entries(row.values)) {
      const field = form.elements[name]; if (!field) continue;
      if (field.type === 'checkbox') field.checked = value;
      else field.value = Array.isArray(value) ? value.join('\n') : value;
    }
    for (const name of ['deployment_id', 'model', 'model_revision', 'dimensions']) {
      if (form.elements[name]) form.elements[name].disabled = row.revision > 0;
    }
    $('memory-edit').disabled = false;
    $('memory-config-status').textContent = row.values_payload_state === 'DRAFT'
      ? `新配置草稿 · 保存后替换版本 ${row.revision}。当前显示的是新模板。`
      : `配置版本 ${row.revision} · ${row.has_api_key ? '已保存密钥' : '无密钥'} · ${row.values.enabled ? '已启用（服务状态见下方）' : '未启用'}`;
  }
  async function read() {
    if (!bot) return;
    const own = epoch, seq = ++readSeq, edited = edit;
    const result = await api(`${path()}/queries/configuration`);
    if (!valid(own) || seq !== readSeq) return;
    if (edited !== edit) { notice('读取期间已编辑配置，保留当前草稿。'); return; }
    show(result);
  }
  async function refresh() {
    if (!bot) return;
    const own = epoch, seq = ++statusSeq;
    const result = await api(`${path()}/queries/status`);
    if (!valid(own) || seq !== statusSeq) return;
    $('memory-state').textContent = JSON.stringify(result, null, 2);
  }
  form.addEventListener('input', clearPlan);
  form.addEventListener('change', clearPlan);
  form.addEventListener('submit', event => {
    event.preventDefault(); if (!configuration?.values || configuration.values_payload_state === 'UNAVAILABLE' || !form.reportValidity()) return;
    const own = epoch, edited = edit, values = structuredClone(configuration.values), prefix = path();
    for (const [name, old] of Object.entries(values)) {
      const field = form.elements[name];
      if (typeof old === 'boolean') values[name] = field.checked;
      else if (typeof old === 'number') values[name] = Number(field.value);
      else if (Array.isArray(old)) values[name] = field.value.split(/\r?\n/).map(x => x.trim()).filter(Boolean);
      else values[name] = field.value;
    }
    const payload = {expected_revision:configuration.revision, values,
      api_key:form.elements.api_key.value || null, clear_api_key:form.elements.clear_api_key.checked};
    if (payload.api_key && payload.clear_api_key) { notice('替换密钥和清除密钥只能选择一项。'); return; }
    planKey ||= crypto.randomUUID(); const key = planKey;
    busy(event.submitter, async () => {
      const result = await api(`${prefix}/actions/configuration.plan`, payload, {'Idempotency-Key':key});
      if (!valid(own) || edited !== edit || planKey !== key) return;
      pending = {...result, has_api_key:result.credentials_changed ? !payload.clear_api_key : configuration.has_api_key};
      applyKey = crypto.randomUUID();
      $('memory-diff').textContent = JSON.stringify({before:result.before_values, after:result.values,
        api_key:result.credentials_changed ? (payload.clear_api_key ? '清除密钥' : '替换密钥') : '保留密钥'}, null, 2);
      $('memory-preview').classList.remove('hidden');
    });
  });
  $('memory-apply').addEventListener('click', event => {
    if (!pending) return;
    const own = epoch, edited = edit, plan = pending, key = applyKey, prefix = path();
    busy(event.currentTarget, async () => {
      const result = await api(`${prefix}/actions/configuration.apply`, {
        expected_revision:plan.expected_revision, plan_id:plan.plan_id, plan_digest:plan.plan_digest,
      }, {'Idempotency-Key':key});
      if (!valid(own)) return;
      if (edited === edit && pending === plan) {
        show({revision:result.revision, secret_revision:result.secret_revision,
          values:plan.values, has_api_key:plan.has_api_key});
      }
      notice('记忆配置已保存。旧版本的后续请求与索引发布已失效；当前编辑的草稿会保留。', true);
    });
  });
  $('memory-reload').addEventListener('click', event => busy(event.currentTarget, read));
  $('memory-refresh').addEventListener('click', event => busy(event.currentTarget, refresh));
  $('memory-sources-refresh').addEventListener('click', event => busy(event.currentTarget, refreshSources));
  $('memory-sources-more').addEventListener('click', event => {
    const after = sourceView?.next_after;
    if (after) busy(event.currentTarget, () => refreshSources(after, true));
  });
  $('memory-associations-refresh').addEventListener('click', event =>
    busy(event.currentTarget, refreshAssociations));
  $('memory-association-plan').addEventListener('click', event => {
    if (!sourceView) return;
    const ids = selectedSourceIds();
    if (ids.length !== 2) return;
    previewAssociation({
      expected_deletion_epoch:sourceView.deletion_epoch,
      expected_revision:0,
      active:true,
      source_unit_ids:ids,
    }, event.currentTarget);
  });
  $('memory-association-apply').addEventListener('click', event => {
    if (!associationPlan) return;
    const own = epoch, plan = associationPlan, key = associationApplyKey, prefix = path();
    busy(event.currentTarget, async () => {
      const result = await api(`${prefix}/actions/association.apply`, {
        expected_deletion_epoch:plan.expected_deletion_epoch,
        plan_id:plan.plan_id,
        plan_digest:plan.plan_digest,
      }, {'Idempotency-Key':key});
      if (!valid(own) || plan !== associationPlan) return;
      clearAssociationPlan();
      notice(`联想变更已提交：关系版本 ${result.revision}，后台正在发布召回图。`, true);
      await refreshAssociations(); await refresh();
    });
  });
  $('memory-forget-plan').addEventListener('click', event => {
    if (!sourceView) return;
    const ids = [...$('memory-sources').querySelectorAll('input:checked')].map(item => item.value);
    if (!ids.length) return;
    const own = epoch, seq = sourceSeq, deletionEpoch = sourceView.deletion_epoch, prefix = path();
    forgetPlanKey ||= crypto.randomUUID(); const key = forgetPlanKey;
    busy(event.currentTarget, async () => {
      const result = await api(`${prefix}/actions/forget.plan`, {
        expected_deletion_epoch:deletionEpoch, source_unit_ids:ids,
      }, {'Idempotency-Key':key});
      if (!valid(own) || seq !== sourceSeq || key !== forgetPlanKey) return;
      forgetPlan = result; forgetApplyKey = crypto.randomUUID();
      $('memory-forget-diff').textContent = JSON.stringify({
        sources:result.sources.map(sourceLabel), source_count:result.source_count,
        resolved_target_count:result.resolved_target_count,
        affected_payloads:result.affected_payloads,
        legacy_conservative:result.legacy_conservative,
        deletion_epoch:result.expected_deletion_epoch, expires_at:result.expires_at,
      }, null, 2);
      $('memory-forget-preview').classList.remove('hidden');
    });
  });
  $('memory-forget-apply').addEventListener('click', event => {
    if (!forgetPlan) return;
    const own = epoch, plan = forgetPlan, key = forgetApplyKey, prefix = path();
    busy(event.currentTarget, async () => {
      const result = await api(`${prefix}/actions/forget.apply`, {
        expected_deletion_epoch:plan.expected_deletion_epoch,
        plan_id:plan.plan_id, plan_digest:plan.plan_digest,
      }, {'Idempotency-Key':key});
      if (!valid(own) || plan !== forgetPlan) return;
      notice(`遗忘已提交：删除版本 ${result.deletion_epoch}，后台正在清理正文。`, true);
      await refreshSources(); await refreshAssociations(); await refresh();
    });
  });
  async function select(row) {
    reset(); bot = row; $('bot-memory').classList.remove('hidden');
    const own = epoch;
    await read();
    if (valid(own)) await refresh();
    if (valid(own)) await refreshSources();
    if (valid(own)) await refreshAssociations();
  }
  function invalidateContent(scope) {
    if (!bot || scope !== path()) return;
    epoch++; readSeq++; statusSeq++; sourceSeq++; associationSeq++;
    configuration = null; clearPlan();
    clearConfiguration(form); $('memory-edit').disabled = true;
    $('memory-diff').replaceChildren(); $('memory-state').replaceChildren();
    clearForget(); $('memory-sources').replaceChildren(); $('memory-sources-status').textContent = '';
    $('memory-sources-more').disabled = true;
    clearAssociations();
    $('memory-config-status').textContent = '相关内容已不可用，请重新加载当前配置。';
    replace.classList.add('hidden');
  }
  return {reset, select, invalidateContent};
}

export const createModulePanel = memoryPanel;
