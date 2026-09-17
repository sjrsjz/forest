const $ = id => document.getElementById(id);
const states = {PENDING:'排队中', QUEUED:'排队中', CLAIMED:'已被调度器接纳', DISPATCHING:'正在提交触发',
  ADMITTED:'触发已接纳', ENQUEUED:'触发已入群队列', TRIGGERED:'触发已接纳', RUNNING:'Agent 运行中', COMPLETED:'执行已完成',
  SUCCEEDED:'执行已完成', FAILED:'执行失败', CANCELLED:'已取消', EXPIRED:'已过期', MISSED:'已跳过',
  SKIPPED:'已跳过', UNKNOWN:'结果待核实', REPLY_COMMITTED:'回复已提交', SENT:'平台已确认送达'};
const misfires = {skip:'跳过漏执行', coalesce:'合并为一次', catch_up:'限量补执行'};

export function createModulePanel(api, busy, notice) {
  let bot = null, epoch = 0, edit = 0, readSeq = 0, detailSeq = 0;
  let rows = [], conversations = [], bindings = [], selected = null, draftId = null, plan = null, dirty = false;
  let planKey = null, applyKey = null, runKey = null, cancelKey = null;
  const form = $('job-config');
  const scope = () => `/scopes/bot/${encodeURIComponent(bot.bot_id)}/modules`;
  const base = () => `${scope()}/scheduler.jobs`;
  const adapter = () => `${scope()}/adapter.onebot`;
  const post = (path, body, key) => api(path, body, {'Idempotency-Key':key});
  const valid = own => bot !== null && own === epoch;
  const option = (value, label) => { const node = document.createElement('option'); node.value = value; node.textContent = label; return node; };
  function invalidate() {
    edit++; dirty = true; plan = null; planKey = null; applyKey = null;
    $('job-preview').classList.add('hidden');
  }
  function mode() {
    const kind = form.elements.schedule_kind.value;
    $('job-cron-fields').classList.toggle('hidden', kind !== 'cron');
    $('job-interval-fields').classList.toggle('hidden', kind !== 'interval');
    form.elements.cron.required = kind === 'cron';
    form.elements.interval_seconds.required = kind === 'interval';
    $('job-catch-up').classList.toggle('hidden', form.elements.misfire.value !== 'catch_up');
  }
  function defaults() {
    const now = Date.now();
    return {name:'', enabled:true, kind:'conversation.trigger', conversation_id:conversations[0]?.conversation_id || '', prompt:'',
      schedule:{kind:'interval', timezone:'Asia/Shanghai', start_at:new Date(now + 60000).toISOString(), end_at:null,
        interval_seconds:3600, cron:null, dst_fold:'first'}, misfire:'coalesce', misfire_grace_seconds:60,
      catch_up_limit:3, max_overlap:1, delegation_expires_at:new Date(now + 7 * 86400000).toISOString(), max_runs:1000};
  }
  function sourceLabel(id) {
    const row = conversations.find(item => item.conversation_id === id);
    const binding = bindings.find(item => item.binding_id === row?.binding_id);
    return row ? `${row.channel_kind === 'group' ? '群' : '私聊'} ${row.channel_id} · ${row.platform} · 账号 ${row.self_id || binding?.self_id || row.binding_id}` : id;
  }
  function sourceOptions(selectedId) {
    const field = form.elements.conversation_id; field.replaceChildren();
    for (const row of conversations) field.append(option(row.conversation_id, sourceLabel(row.conversation_id)));
    if (selectedId && !conversations.some(row => row.conversation_id === selectedId)) {
      field.append(option(selectedId, `当前列表未包含的原来源 · ${selectedId}`));
    }
    if (selectedId) field.value = selectedId;
    field.disabled = Boolean(selected);
  }
  function fill(row = null) {
    selected = row; draftId = row?.job_id || crypto.randomUUID();
    invalidate(); runKey = null; cancelKey = null; detailSeq++;
    const value = row?.values || defaults(); form.reset(); sourceOptions(value.conversation_id);
    for (const name of ['name','prompt','misfire','misfire_grace_seconds','catch_up_limit','max_overlap','delegation_expires_at','max_runs']) {
      form.elements[name].value = value[name] ?? '';
    }
    form.elements.enabled.checked = Boolean(value.enabled);
    form.elements.schedule_kind.value = value.schedule.kind;
    for (const name of ['timezone','start_at','end_at','interval_seconds','cron','dst_fold']) form.elements[name].value = value.schedule[name] ?? '';
    $('job-form-title').textContent = row ? `修改计划：${value.name}` : '创建定时任务';
    $('job-current').textContent = row ? `修订 ${row.revision} · ${value.enabled ? '已启用' : '已停用'} · ${sourceLabel(value.conversation_id)}` : '新计划将由当前管理账号委托执行。';
    form.querySelector('fieldset').disabled = conversations.length === 0 && !row;
    $('job-existing-actions').classList.toggle('hidden', !row);
    $('job-runs').replaceChildren(); $('job-cancel-result').replaceChildren(); mode(); dirty = false;
  }
  function readValues() {
    const fields = form.elements, kind = fields.schedule_kind.value;
    const instant = (value, required) => {
      if (!value.trim() && !required) return null;
      if (!/(?:Z|[+-]\d{2}:\d{2})$/i.test(value) || !Number.isFinite(Date.parse(value))) throw Error('时间需要包含明确时区，例如 2026-09-13T08:00:00+08:00。');
      return new Date(value).toISOString();
    };
    return {name:fields.name.value, enabled:fields.enabled.checked, kind:'conversation.trigger',
      conversation_id:fields.conversation_id.value, prompt:fields.prompt.value,
      schedule:{kind, timezone:fields.timezone.value.trim(), start_at:instant(fields.start_at.value, true),
        end_at:instant(fields.end_at.value, false), interval_seconds:kind === 'interval' ? Number(fields.interval_seconds.value) : null,
        cron:kind === 'cron' ? fields.cron.value.trim() : null, dst_fold:fields.dst_fold.value},
      misfire:fields.misfire.value, misfire_grace_seconds:Number(fields.misfire_grace_seconds.value),
      catch_up_limit:Number(fields.catch_up_limit.value), max_overlap:Number(fields.max_overlap.value),
      delegation_expires_at:instant(fields.delegation_expires_at.value, true), max_runs:Number(fields.max_runs.value)};
  }
  function facts(root, entries) {
    const list = document.createElement('dl');
    for (const [name, value] of entries) {
      const key = document.createElement('dt'), detail = document.createElement('dd');
      key.textContent = name; detail.textContent = String(value ?? '—'); list.append(key, detail);
    }
    root.append(list);
  }
  function time(value, timezone) {
    if (!value) return '—';
    try { return `${new Intl.DateTimeFormat('zh-CN', {timeZone:timezone, dateStyle:'medium', timeStyle:'long'}).format(new Date(value))} (${timezone}) · ${new Date(value).toISOString()}`; }
    catch { return String(value); }
  }
  function definitionFacts(value) {
    return [['名称',value.name], ['启用状态',value.enabled ? '启用' : '停用'], ['对话来源',sourceLabel(value.conversation_id)],
      ['周期',value.schedule.kind === 'cron' ? `Cron ${value.schedule.cron}` : `每 ${value.schedule.interval_seconds} 秒`],
      ['日历时区',value.schedule.timezone], ['开始',time(value.schedule.start_at, value.schedule.timezone)],
      ['结束',value.schedule.end_at ? time(value.schedule.end_at, value.schedule.timezone) : '由委托期限或最大次数限制'],
      ['夏令时重复时刻',({first:'第一次',second:'第二次',both:'两次均执行'})[value.schedule.dst_fold]],
      ['错过执行',`${misfires[value.misfire]} · 宽限 ${value.misfire_grace_seconds} 秒 · 最多补 ${value.catch_up_limit} 次`],
      ['最大未完成任务数',value.max_overlap], ['委托过期',time(value.delegation_expires_at, value.schedule.timezone)],
      ['最多执行次数',value.max_runs]];
  }
  function preview(result) {
    const root = $('job-diff'); root.replaceChildren();
    const value = result.values, before = result.before_values;
    const old = new Map(before ? definitionFacts(before) : []);
    facts(root, [['变更',before ? `修改「${before.name}」· 预期修订 ${result.expected_revision}；变更项按原值 → 新值显示` : '创建新计划'],
      ...definitionFacts(value).map(([name, next]) => [name, before && old.get(name) !== next ? `${old.get(name)} → ${next}` : next])]);
    if (before && before.prompt !== value.prompt) {
      const prior = document.createElement('p'); prior.className = 'job-prompt'; prior.textContent = `原任务内容：${before.prompt}`; root.append(prior);
    }
    const prompt = document.createElement('p'); prompt.className = 'job-prompt'; prompt.textContent = `任务内容：${value.prompt}`; root.append(prompt);
    const title = document.createElement('h4'); title.textContent = '接下来预计执行的时间'; root.append(title);
    const list = document.createElement('ol');
    for (const occurrence of result.next_occurrences || []) {
      const item = document.createElement('li'); item.textContent = time(typeof occurrence === 'string' ? occurrence : occurrence.scheduled_for || occurrence.at, value.schedule.timezone); list.append(item);
    }
    if (!list.children.length) { const text = document.createElement('p'); text.textContent = '当前条件下没有后续执行时间。'; root.append(text); }
    else root.append(list);
    $('job-preview').classList.remove('hidden');
  }
  function listing(data) {
    rows = data.jobs || []; const root = $('job-list'); root.replaceChildren();
    for (const row of rows) {
      if (row.conversation && !conversations.some(item => item.conversation_id === row.conversation.conversation_id)) conversations.push(row.conversation);
      const button = document.createElement('button'); button.type = 'button'; button.className = 'bot-row subtle';
      button.textContent = `${row.values.name} · ${row.values.enabled ? '已启用' : '已停用'} · ${sourceLabel(row.values.conversation_id)}`;
      button.addEventListener('click', () => busy(button, async () => { fill(row); await detail(); })); root.append(button);
    }
    if (!rows.length) root.textContent = '还没有定时任务。填写右侧表单并预览后创建。';
  }
  function occurrences(data) {
    const root = $('job-runs'); root.replaceChildren();
    const own = epoch, prefix = base();
    const current = (data.jobs || []).find(item => item.job_id === selected?.job_id);
    if (current) {
      const authority = document.createElement('section');
      facts(authority, [['委托管理账号',current.delegation?.subject_id], ['委托主体',current.delegation?.principal_ref],
        ['计划状态',current.enabled ? '启用' : '停用'], ['累计触发',current.emitted_count],
        ['下次执行',time(current.next_due_at, current.values.schedule.timezone)],
        ['委托截止',time(current.values.delegation_expires_at, current.values.schedule.timezone)]]);
      root.append(authority);
    }
    for (const row of data.occurrences || []) {
      const item = document.createElement('article'); item.className = 'job-occurrence';
      const title = document.createElement('h4'); title.textContent = `${states[row.status] || row.status} · ${row.scheduled_for || row.created_at}`; item.append(title);
      const committed = row.reply_id || row.reply_committed_at || row.reply_status === 'COMMITTED';
      const delivered = row.delivery_status || row.delivery_state || row.delivery?.state;
      facts(item, [['执行记录',row.occurrence_id], ['群来源',sourceLabel(row.conversation_id || selected?.values.conversation_id)],
        ['触发接纳',row.trigger_id || row.event_id || '尚未接纳'], ['触发状态',states[row.trigger_status] || row.trigger_status],
        ['Agent 运行',row.run_id || '尚未建立'], ['运行状态',states[row.run_status] || row.run_status],
        ['回复',committed ? '回复已提交到发送队列' : '尚无已提交回复'],
        ['平台送达',delivered ? String(delivered).split(',').map(state => states[state] || state).join('、') : '尚未确认送达'],
        ['过期时间',time(row.expires_at, selected?.values.schedule.timezone || 'UTC')], ['尝试次数',row.attempt_count],
        ['错误/跳过原因',row.error_code || row.reason || '—']]);
      if (row.status === 'UNKNOWN') {
        const warning = document.createElement('p'); warning.className = 'hint'; warning.textContent = '结果未知可能已产生外部效果。此处不提供重放；请先核实执行与送达记录。'; item.append(warning);
      }
      if (row.replay_allowed === true) {
        const button = document.createElement('button'); button.textContent = '按当前授权重新执行'; const key = crypto.randomUUID();
        button.addEventListener('click', () => busy(button, async () => {
          await post(prefix + '/actions/occurrence.replay', {occurrence_id:row.occurrence_id, expected_revision:row.revision}, key);
          if (valid(own)) { notice('重放请求已提交，执行结果与送达仍以记录为准。', true); await detail(); }
        })); item.append(button);
      }
      root.append(item);
    }
    if (!(data.occurrences || []).length) { const note = document.createElement('p'); note.textContent = '此计划尚无执行记录。'; root.append(note); }
  }
  async function refresh() {
    if (!bot || draftId === null) return;
    const own = epoch, sequence = ++readSeq, prefix = base();
    const [data, sources, accounts] = await Promise.all([
      api(prefix + '/queries/definitions'),
      api(adapter() + '/queries/conversations'),
      api(adapter() + '/queries/bindings'),
    ]);
    if (!valid(own) || sequence !== readSeq) return;
    const sourceId = form.elements.conversation_id.value;
    conversations = sources.conversations || []; bindings = accounts.bindings || []; listing(data);
    sourceOptions(sourceId);
    if (form.elements.conversation_id.value !== sourceId) invalidate();
    form.querySelector('fieldset').disabled = conversations.length === 0 && !selected;
    $('job-source-empty').classList.toggle('hidden', conversations.length !== 0);
  }
  function activeRuns(runs, rejected = false) {
    const root = $('job-cancel-result'); root.replaceChildren();
    if (!runs?.length) return;
    const note = document.createElement('p'); note.className = 'hint';
    note.textContent = rejected ? '取消请求未应用：任务已进入活跃 Agent 运行，可能与其他触发共用同一次运行。请先在运行管理中核对并显式取消，再重试计划取消。' : '已领取的 Agent 运行仍在继续；如需停止，请先核对其中包含的其他触发，再显式取消。';
    root.append(note);
    for (const row of runs) facts(root, [['活跃运行',row.run_id], ['核对时检查点',row.expected_checkpoint_seq]]);
    const hint = document.createElement('p'); hint.className = 'hint';
    hint.textContent = '请从当前作用域的 Agent 管理贡献打开运行记录并核对取消。'; root.append(hint);
  }
  async function detail() {
    if (!bot || !selected) return;
    const own = epoch, sequence = ++detailSeq, id = selected.job_id, prefix = base();
    const data = await api(prefix + '/queries/definitions?job_id=' + encodeURIComponent(id));
    if (!valid(own) || sequence !== detailSeq || selected?.job_id !== id) return;
    occurrences(data);
  }
  async function reload() {
    if (!bot || !selected) return;
    const own = epoch, edited = edit, id = selected.job_id, sequence = ++detailSeq;
    const data = await api(base() + '/queries/definitions?job_id=' + encodeURIComponent(id));
    if (!valid(own) || sequence !== detailSeq || selected?.job_id !== id) return;
    if (edited !== edit) { notice('读取期间计划已编辑，已保留草稿。'); return; }
    const row = (data.jobs || []).find(item => item.job_id === id);
    if (row) { fill(row); occurrences(data); }
  }
  function reset() {
    epoch++; readSeq++; detailSeq++; bot = null; selected = null; rows = []; conversations = []; bindings = []; draftId = null;
    invalidate(); form.reset(); form.querySelector('fieldset').disabled = true;
    $('bot-jobs').classList.add('hidden'); $('job-list').replaceChildren(); $('job-runs').replaceChildren(); $('job-cancel-result').replaceChildren();
    $('job-existing-actions').classList.add('hidden');
  }
  async function select(row) {
    reset(); bot = row; const own = epoch;
    $('bot-jobs').classList.remove('hidden');
    const [data, sources, accounts] = await Promise.all([
      api(base() + '/queries/definitions'),
      api(adapter() + '/queries/conversations'),
      api(adapter() + '/queries/bindings'),
    ]);
    if (!valid(own)) return;
    conversations = sources.conversations || []; bindings = accounts.bindings || []; listing(data); fill();
    $('job-source-empty').classList.toggle('hidden', conversations.length !== 0);
  }
  form.addEventListener('input', invalidate);
  form.addEventListener('change', () => { invalidate(); mode(); });
  form.addEventListener('submit', event => {
    event.preventDefault(); if (!bot) return;
    let values; try { values = readValues(); } catch (error) { notice(error.message); return; }
    const own = epoch, edited = edit, id = draftId, revision = selected?.revision || 0, prefix = base();
    planKey ||= crypto.randomUUID(); const key = planKey;
    busy(event.submitter || form.querySelector('button[type="submit"]'), async () => {
      const result = await post(prefix + '/actions/definition.plan', {job_id:id, expected_revision:revision, values}, key);
      if (!valid(own) || edited !== edit || id !== draftId || key !== planKey) return;
      plan = result; applyKey = crypto.randomUUID(); preview(result);
    });
  });
  $('job-apply').addEventListener('click', event => {
    if (!plan || !bot) return;
    const own = epoch, edited = edit, proposed = plan, id = draftId, key = applyKey, prefix = base();
    busy(event.currentTarget, async () => {
      const result = await post(prefix + '/actions/definition.apply', {job_id:id, expected_revision:proposed.expected_revision,
        plan_id:proposed.plan_id, plan_digest:proposed.plan_digest}, key);
      if (!valid(own)) return;
      if (edited === edit && plan === proposed && draftId === id) fill({job_id:id, revision:result.revision, values:proposed.values});
      notice('计划已保存。任务执行和平台送达是独立状态，请查看执行记录。', true); await refresh();
      if (selected?.job_id === id) await detail();
    });
  });
  $('job-new').addEventListener('click', () => { if (bot) fill(); });
  $('job-refresh').addEventListener('click', event => busy(event.currentTarget, refresh));
  $('job-reload').addEventListener('click', event => busy(event.currentTarget, reload));
  $('job-runs-refresh').addEventListener('click', event => busy(event.currentTarget, detail));
  $('job-toggle').addEventListener('click', () => { if (!selected) return; form.elements.enabled.checked = !form.elements.enabled.checked; invalidate(); form.requestSubmit(); });
  $('job-run').addEventListener('click', event => {
    if (!selected || !bot) return;
    const own = epoch, prefix = base(), row = selected; runKey ||= crypto.randomUUID(); const key = runKey;
    busy(event.currentTarget, async () => {
      await post(prefix + '/actions/occurrence.run', {job_id:row.job_id, expected_revision:row.revision}, key);
      if (!valid(own)) return;
      notice('手动执行请求已接纳；重试同一次请求不会重复触发。', true);
      if (selected?.job_id === row.job_id) await detail();
    });
  });
  $('job-run-another').addEventListener('click', () => { runKey = null; $('job-run').click(); });
  for (const id of ['job-cancel-scope','job-cancel-reason']) $(id).addEventListener('input', () => { cancelKey = null; });
  $('job-cancel').addEventListener('click', event => {
    if (!selected || !bot) return;
    const reason = $('job-cancel-reason').value.trim();
    if (!reason) { notice('请填写停止计划的原因。'); return; }
    const own = epoch, prefix = base(), row = selected, scope = $('job-cancel-scope').value, edited = edit, wasDirty = dirty;
    cancelKey ||= crypto.randomUUID(); const key = cancelKey;
    busy(event.currentTarget, async () => {
      let result;
      try { result = await post(prefix + '/actions/definition.cancel', {job_id:row.job_id, expected_revision:row.revision, scope, reason}, key); }
      catch (error) {
        // Invalidation has already advanced this panel's epoch and cleared its
        // selection. Surface the receipt for this Bot, but never on another Bot.
        if (error.code === 'CONTROL_CONTENT_REVOKED' && bot && base() === prefix) throw error;
        if (!valid(own) || selected?.job_id !== row.job_id) return;
        if (error.code === 'JOB_RUN_ALREADY_ACTIVE') { activeRuns(error.details?.runs, true); notice('存在活跃 Agent 运行，本次取消未应用。请核对下方运行记录。'); return; }
        throw error;
      }
      if (!valid(own)) return;
      notice(scope === 'pending' ? '待领取任务已取消，未来计划保持原状态。' : '未来计划已停止。已经送达的消息和已经发生的外部效果不会撤回。', true); await refresh();
      if (selected?.job_id === row.job_id) {
        if (!wasDirty && edited === edit) await reload();
        else { await detail(); notice('停止请求已提交，当前草稿已保留；提交新修改前请核对最新计划修订。', true); }
        if (valid(own) && selected?.job_id === row.job_id) activeRuns(result.active_runs);
      }
    });
  });
  function invalidateContent(scope) {
    if (!bot || scope !== base()) return;
    epoch++; readSeq++; detailSeq++; selected = null; rows = []; invalidate();
    form.reset(); form.querySelector('fieldset').disabled = true;
    for (const id of ['job-list','job-runs','job-cancel-result','job-diff']) $(id).replaceChildren();
    $('job-form-title').textContent = '相关内容已不可用'; $('job-current').replaceChildren();
    $('job-cancel-reason').value = '';
    $('job-existing-actions').classList.add('hidden');
  }
  return {select, reset, refresh, invalidateContent};
}
