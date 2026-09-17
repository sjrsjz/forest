import { schemaForm } from './schema-form.js';
import { replacementButton } from './configuration-forms.js';
import { mountModuleUi } from './module-ui-host.js';

const $ = id => document.getElementById(id);

export function createModulePanel(api, busy, notice) {
  let bot = null, packages = [], botConfig = null, botPlan = null, botApplyKey = null;
  let componentReload = null, moduleStateRequest = null, invokeKey = null;
  let epoch = 0, formEpoch = 0, configReadSeq = 0, packageReadSeq = 0;
  let readConfigFields = null, readInvokeFields = null, historyBefore = null;
  let mountedModuleUi = null, pollTimer = null;
  let replacementRequired = false, replacementDraft = false, sourceRevision = null;
  const base = () => `/scopes/bot/${encodeURIComponent(bot.bot_id)}/modules/component.worker-host`;
  const selectedPackage = () => packages.find(row => row.package_digest === $('bot-package').value);
  const params = values => new URLSearchParams(values).toString();

  const replaceComponent = replacementButton(
    $('bot-runtime-status'), 'component-replace-configuration', () => {
      if (!botConfig?.defaults || !selectedPackage()) return;
      clearPlan(); replacementRequired = false; replacementDraft = true;
      sourceRevision = null; replaceComponent.classList.add('hidden');
      renderComponent(undefined, [], false); updateState(botConfig);
    },
  );

  function clearPlan() {
    formEpoch++; botPlan = null; botApplyKey = null;
    $('bot-config-preview').classList.add('hidden');
  }

  function resetComponent() {
    mountedModuleUi?.destroy(); mountedModuleUi = null;
    botConfig = null; readConfigFields = null; readInvokeFields = null; invokeKey = null;
    componentReload = null; moduleStateRequest = null;
    $('restart-bot-component').disabled = true;
    replacementRequired = false; replacementDraft = false; sourceRevision = null;
    replaceComponent.classList.add('hidden'); $('bot-grants').checked = false;
    $('bot-handler-takeover').checked = false;
    $('bot-config-fields').replaceChildren(); $('bot-invoke-fields').replaceChildren();
    $('bot-service').replaceChildren(); $('bot-history').replaceChildren();
    $('bot-grants').disabled = true; $('bot-handler-takeover').disabled = true;
    $('bot-config-form').querySelector('button[type="submit"]').disabled = true;
    $('bot-invoke-form').querySelector('button[type="submit"]').disabled = true;
  }

  async function loadPackages() {
    const own = epoch, seq = ++packageReadSeq;
    const response = await api(`${base()}/queries/packages`);
    if (own !== epoch || seq !== packageReadSeq || !bot) return;
    const previous = $('bot-package').value;
    packages = (response.packages || []).map(pkg => ({...pkg,
      management_ui_url: `/management/v1${base()}/downloads/package.management-ui?${
        params({package_digest: pkg.package_digest})}`}));
    $('bot-package').replaceChildren();
    for (const item of packages) {
      const option = document.createElement('option'); option.value = item.package_digest;
      option.textContent = `${item.package_id} · ${item.version}`; $('bot-package').append(option);
    }
    if (packages.some(item => item.package_digest === previous)) $('bot-package').value = previous;
  }

  function updateState(state) {
    if (botConfig && (state.desired_revision < botConfig.desired_revision ||
        state.effective_revision < botConfig.effective_revision)) return;
    $('bot-desired').textContent = state.desired_revision;
    $('bot-effective').textContent = state.effective_revision;
    const component = state.component || {};
    const error = component.error_code;
    $('bot-runtime-status').textContent = error ? `组件暂未就绪：${error}` :
      state.desired_enabled === false && state.effective_enabled === false ? '组件已停用，路由已排空' :
      state.desired_enabled === false ? '停用已接纳，正在冻结并排空调用…' :
      state.effective_enabled === false ? '启用已接纳，正在验证并启动组件…' :
      component.worker_state === 'ACTIVE' && component.loaded_revision === state.desired_revision ? '组件已生效并运行中' :
      state.desired_revision ? '已保存，等待组件启动或切换…' : '保存组件配置后即可启动';
    const unavailable = [];
    if (state.desired_values_payload_state === 'UNAVAILABLE') unavailable.push('已保存配置正文已清理');
    if (state.effective_values_payload_state === 'UNAVAILABLE') unavailable.push('已生效配置正文已清理');
    if (replacementDraft) unavailable.push('正在填写独立新配置草稿');
    if (unavailable.length) $('bot-runtime-status').textContent += ` · ${unavailable.join(' · ')}`;
    $('bot-operations').replaceChildren();
    for (const operation of state.operations || []) {
      const row = document.createElement('li');
      row.textContent = `${operation.created_at} · ${operation.operation_kind || 'INSTALL'} · ${operation.phase}${operation.error_code ? ` · ${operation.error_code}` : ''}`;
      $('bot-operations').append(row);
    }
  }

  function renderComponent(config, grants = [], handlerTakeover = false) {
    const pkg = selectedPackage();
    if (!pkg) return;
    mountedModuleUi?.destroy(); mountedModuleUi = null;
    $('bot-config-fields').classList.remove('hidden');
    readConfigFields = schemaForm($('bot-config-fields'), pkg.config_schema, config);
    if (pkg.management_ui) {
      const packageDigest = pkg.package_digest;
      let customDraft = structuredClone(config || {});
      mountModuleUi($('bot-module-ui'), pkg, customDraft, values => {
        if (selectedPackage()?.package_digest !== packageDigest) return;
        customDraft = values; readConfigFields = () => structuredClone(customDraft); clearPlan();
      }).then(host => {
        if (selectedPackage()?.package_digest !== packageDigest) { host.destroy(); return; }
        mountedModuleUi = host; $('bot-config-fields').classList.add('hidden');
        readConfigFields = () => structuredClone(customDraft);
      }).catch(error => notice(`${error.message} 已回退到通用 schema 表单。`));
    } else {
      $('bot-module-ui').classList.add('hidden');
    }
    $('bot-grants').required = pkg.grants.length > 0; $('bot-grants').disabled = false;
    $('bot-config-form').querySelector('button[type="submit"]').disabled = false;
    $('bot-grants').checked = pkg.grants.every(grant => grants.includes(grant));
    $('bot-grants-label').textContent = pkg.grants.length ? `允许组件使用：${pkg.grants.join('、')}` : '此组件未申请额外能力';
    const declaresTakeover = pkg.message_handler_takeover === 'compatible';
    $('bot-handler-takeover').disabled = !declaresTakeover;
    $('bot-handler-takeover').checked = declaresTakeover && handlerTakeover === true;
    $('bot-handler-takeover-label').textContent = declaresTakeover
      ? '允许兼容版本接管尚未开始执行的群消息 Handler' : '此组件包未声明兼容 Handler 接管';
    $('bot-service').replaceChildren();
    for (const service of pkg.services) {
      const option = document.createElement('option'); option.value = service.name;
      option.textContent = service.name; $('bot-service').append(option);
    }
    renderService();
  }

  function renderService() {
    const pkg = selectedPackage();
    const service = pkg?.services.find(item => item.name === $('bot-service').value);
    $('bot-invoke-form').classList.toggle('hidden', !service);
    readInvokeFields = service ? schemaForm($('bot-invoke-fields'), service.input_schema) : null;
    $('bot-invoke-form').querySelector('button[type="submit"]').disabled = !service;
    invokeKey = null; $('bot-invoke-result').textContent = '';
  }

  async function readConfiguration() {
    if (!bot || !selectedPackage()) return;
    const own = epoch, edit = formEpoch, seq = ++configReadSeq;
    const componentId = selectedPackage().component_id;
    const state = await api(`${base()}/queries/configuration?${params({component_id: componentId})}`);
    if (own !== epoch || seq !== configReadSeq) return;
    if (edit !== formEpoch) {
      updateState(state); notice('读取期间有新的配置输入，已保留。需要丢弃草稿时可再次重新加载。'); return;
    }
    botConfig = state; clearPlan(); replacementDraft = false;
    sourceRevision = state.desired_values_payload_state === 'ACTIVE' && state.desired_values !== null
      ? state.desired_revision : null;
    replacementRequired = state.desired_values_payload_state === 'UNAVAILABLE' ||
      (state.desired_revision > 0 && state.desired_values === null);
    replaceComponent.classList.toggle('hidden', !replacementRequired);
    replaceComponent.disabled = !state.defaults; updateState(state);
    if (replacementRequired) {
      mountedModuleUi?.destroy(); mountedModuleUi = null; readConfigFields = null;
      $('bot-config-fields').replaceChildren(); $('bot-grants').checked = false;
      $('bot-handler-takeover').checked = false; $('bot-grants').disabled = true;
      $('bot-handler-takeover').disabled = true;
      $('bot-config-form').querySelector('button[type="submit"]').disabled = true;
    } else {
      renderComponent(state.desired_values?.config, state.desired_values?.grants,
        state.desired_values?.message_handler_takeover === true);
    }
    $('restart-bot-component').disabled = replacementRequired || state.desired_revision < 1 ||
      state.desired_values_payload_state !== 'ACTIVE';
    $('validate-bot-component').disabled = replacementRequired || state.desired_revision < 1 ||
      state.desired_values_payload_state !== 'ACTIVE';
    $('enable-bot-component').disabled = state.desired_revision < 1 || state.desired_enabled !== false;
    $('disable-bot-component').disabled = state.desired_revision < 1 || state.desired_enabled !== true;
    $('bot-history').replaceChildren(); historyBefore = null;
  }

  $('bot-package').addEventListener('change', () => {
    epoch++; clearPlan(); resetComponent(); busy($('reload-bot-config'), readConfiguration);
  });
  $('reload-bot-config').addEventListener('click', event => busy(event.currentTarget, readConfiguration));
  $('restart-bot-component').addEventListener('click', event => busy(event.currentTarget, async () => {
    if (!botConfig || botConfig.desired_revision < 1 ||
        botConfig.desired_values_payload_state !== 'ACTIVE' || !botConfig.desired_values) return;
    const own = epoch;
    componentReload ||= {revision:botConfig.desired_revision, values:structuredClone(botConfig.desired_values),
      planKey:crypto.randomUUID(), applyKey:crypto.randomUUID(), plan:null};
    const request = componentReload;
    try {
      request.plan ||= await api(`${base()}/actions/configuration.plan`, {
        component_id:selectedPackage().component_id, expected_revision:request.revision,
        source_revision:request.revision, values:request.values,
      }, {'Idempotency-Key':request.planKey});
      if (own !== epoch || componentReload !== request) return;
      await api(`${base()}/actions/configuration.apply`, {
        component_id:selectedPackage().component_id, plan_id:request.plan.plan_id,
        plan_digest:request.plan.plan_digest, expected_revision:request.plan.expected_revision,
      }, {'Idempotency-Key':request.applyKey});
      if (own !== epoch || componentReload !== request) return;
      componentReload = null; await readConfiguration();
      notice('组件重载已接纳，正在等待新修订完成生命周期切换。', true);
    } catch (error) {
      if (error.code === 'REVISION_CONFLICT' && componentReload === request) {
        componentReload = null; if (own === epoch) await readConfiguration();
      }
      throw error;
    }
  }));
  $('validate-bot-component').addEventListener('click', event => busy(event.currentTarget, async () => {
    if (!botConfig || botConfig.desired_revision < 1) return;
    const result = await api(`${base()}/queries/module.validate?${params({
      component_id:selectedPackage().component_id,
      expected_config_revision:String(botConfig.desired_revision),
      expected_state_revision:String(botConfig.desired_state_revision),
    })}`);
    notice(`组件验证通过。必需依赖：${result.required_components.join('、') || '无'}。`, true);
  }));

  async function changeModuleState(enabled) {
    if (!botConfig || botConfig.desired_revision < 1) return;
    const reason = $('bot-module-reason').value.trim();
    if (!reason) { $('bot-module-reason').focus(); notice('启用或停用组件必须填写原因。'); return; }
    const expected = botConfig.desired_state_revision;
    if (!moduleStateRequest || moduleStateRequest.enabled !== enabled ||
        moduleStateRequest.expected !== expected || moduleStateRequest.reason !== reason) {
      moduleStateRequest = {enabled, expected, reason, key:crypto.randomUUID()};
    }
    const request = moduleStateRequest;
    try {
      await api(`${base()}/actions/module.${enabled ? 'enable' : 'disable'}`, {
        component_id:selectedPackage().component_id,
        expected_state_revision:request.expected, reason:request.reason,
      }, {'Idempotency-Key':request.key});
      if (moduleStateRequest !== request) return;
      moduleStateRequest = null; $('bot-module-reason').value = ''; await readConfiguration();
      notice(enabled ? '组件启用已接纳，正在等待 Worker 发布。' : '组件停用已接纳，正在排空调用。', true);
    } catch (error) {
      if (error.code === 'REVISION_CONFLICT' && moduleStateRequest === request) {
        moduleStateRequest = null; await readConfiguration();
      }
      throw error;
    }
  }
  $('enable-bot-component').addEventListener('click', event =>
    busy(event.currentTarget, () => changeModuleState(true)));
  $('disable-bot-component').addEventListener('click', event =>
    busy(event.currentTarget, () => changeModuleState(false)));
  $('bot-config-form').addEventListener('input', clearPlan);
  $('bot-config-form').addEventListener('submit', event => {
    event.preventDefault(); const form = event.currentTarget;
    if (!botConfig || replacementRequired || !readConfigFields || !form.reportValidity()) return;
    busy(form.querySelector('button'), async () => {
      const own = epoch, edit = formEpoch, pkg = selectedPackage(), config = readConfigFields();
      const plan = await api(`${base()}/actions/configuration.plan`, {
        component_id:pkg.component_id, expected_revision:botConfig.desired_revision,
        ...(sourceRevision === null ? {} : {source_revision:sourceRevision}),
        values:{package_digest:pkg.package_digest, config, grants:[...pkg.grants].sort(),
          message_handler_takeover:$('bot-handler-takeover').checked},
      }, {'Idempotency-Key':crypto.randomUUID()});
      if (own !== epoch || edit !== formEpoch) return;
      botPlan = plan; botApplyKey = crypto.randomUUID();
      $('bot-config-diff').textContent = JSON.stringify({当前:botConfig.desired_values,
        待应用:{插件:`${pkg.package_id} ${pkg.version}`, config, grants:pkg.grants,
          message_handler_takeover:$('bot-handler-takeover').checked}, 状态格式:pkg.state_schema}, null, 2);
      $('bot-config-preview').classList.remove('hidden');
    });
  });
  $('bot-config-apply').addEventListener('click', event => busy(event.currentTarget, async () => {
    if (!botPlan) return;
    const own = epoch, edit = formEpoch;
    await api(`${base()}/actions/configuration.apply`, {
      component_id:selectedPackage().component_id, plan_id:botPlan.plan_id,
      plan_digest:botPlan.plan_digest, expected_revision:botPlan.expected_revision,
    }, {'Idempotency-Key':botApplyKey});
    if (own !== epoch || edit !== formEpoch) return;
    await readConfiguration(); notice('配置已保存，正在启动或切换组件。', true);
  }));
  $('bot-invoke-form').addEventListener('input', () => { invokeKey = null; });
  $('bot-service').addEventListener('change', renderService);
  $('bot-invoke-form').addEventListener('submit', event => {
    event.preventDefault(); const form = event.currentTarget;
    busy(form.querySelector('button'), async () => {
      invokeKey ||= crypto.randomUUID(); const own = epoch, sentKey = invokeKey;
      const data = await api(`${base()}/actions/service.invoke`, {
        component_id:selectedPackage().component_id, service:$('bot-service').value,
        payload:readInvokeFields(),
      }, {'Idempotency-Key':sentKey});
      if (own !== epoch || invokeKey !== sentKey) return;
      invokeKey = null;
      $('bot-invoke-result').textContent = data.result?.label !== undefined && data.result?.revision !== undefined
        ? `${data.result.label}：${data.result.value}（状态修订 ${data.result.revision}）`
        : JSON.stringify(data.result, null, 2);
    });
  });

  async function readHistory(append = false) {
    if (!bot || !botConfig || !selectedPackage()) return;
    const own = epoch;
    const query = {component_id:selectedPackage().component_id};
    if (append && historyBefore) query.before = String(historyBefore);
    const data = await api(`${base()}/queries/configuration-history?${params(query)}`);
    if (own !== epoch) return;
    if (!append) $('bot-history').replaceChildren();
    historyBefore = data.next_before;
    $('bot-history-more').classList.toggle('hidden', !historyBefore);
    for (const revision of data.revisions) {
      const row = document.createElement('div');
      const unavailable = revision.values_payload_state === 'UNAVAILABLE' || !revision.values;
      const pkg = !unavailable && packages.find(item => item.package_digest === revision.values.package_digest);
      const text = document.createElement('p');
      text.textContent = `修订 ${revision.revision} · ${revision.created_at} · ${unavailable ? '配置正文已清理' : pkg?.version || '插件包不可用'}`;
      const button = document.createElement('button'); button.className = 'subtle';
      button.textContent = '载入此配置'; button.disabled = !pkg;
      button.addEventListener('click', () => {
        if (own !== epoch) return;
        $('bot-package').value = pkg.package_digest; clearPlan(); replacementRequired = false;
        replacementDraft = true; sourceRevision = revision.revision;
        replaceComponent.classList.remove('hidden'); replaceComponent.disabled = !botConfig.defaults;
        renderComponent(revision.values.config, revision.values.grants,
          revision.values.message_handler_takeover === true); updateState(botConfig);
        notice(`已载入修订 ${revision.revision}。预览并应用后会创建新的配置修订；持久状态按兼容性迁移。`, true);
      });
      row.append(text, button); $('bot-history').append(row);
    }
  }
  $('read-bot-history').addEventListener('click', event => busy(event.currentTarget, () => readHistory()));
  $('bot-history-more').addEventListener('click', event => busy(event.currentTarget, () => readHistory(true)));

  function reset() {
    epoch++; configReadSeq++; packageReadSeq++; bot = null; packages = [];
    if (pollTimer !== null) { clearInterval(pollTimer); pollTimer = null; }
    clearPlan(); resetComponent(); $('bot-package').replaceChildren();
    $('bot-config-diff').replaceChildren(); $('bot-invoke-result').replaceChildren();
  }
  async function select(row) {
    reset(); bot = row;
    await loadPackages();
    if (!bot || !selectedPackage()) {
      $('bot-runtime-status').textContent = '没有可供当前 Bot 选择的组件包。'; return;
    }
    await readConfiguration();
    pollTimer = setInterval(() => {
      if (!bot || !selectedPackage()) return;
      readConfiguration().catch(error => notice(error.message));
    }, 2500);
  }
  function invalidateContent(scope) {
    if (!bot || scope !== base()) return;
    epoch++; configReadSeq++; clearPlan(); resetComponent();
    $('bot-runtime-status').textContent = '相关内容已不可用，请重新加载当前配置。';
  }
  return {select, reset, refresh:readConfiguration, invalidateContent};
}
