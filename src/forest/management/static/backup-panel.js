const $ = (id) => document.getElementById(id);

const states = {
  QUEUED: '等待执行', CAPTURING: '正在抓取数据库', SEALING: '正在加密封口',
  READY: '可下载', INCONSISTENT: '目录在抓取期间发生变化', FAILED: '失败', EXPIRED: '已过期',
  VERIFIED: '只读校验通过',
  RESERVED: '等待上传', UPLOADING: '正在流式上传', UPLOADED: '等待口令确认',
  IMPORTING: '正在解密校验并发布', IMPORTED: '导入完成',
  VERIFYING: '正在校验工件', RESTORING: '正在恢复隔离数据库',
  VALIDATING: '正在验证恢复结果', STAGED: '隔离恢复完成',
  FENCING: '正在封存旧库', SWITCHING: '正在切换存储目录', ACTIVATING: '正在启用恢复库',
  COMPLETE: '激活完成', BLOCKED: '需要处理后重试',
  CURRENT: '当前存储', SUPERSEDED: '已被替代', RETIRED: '已退休', UNPROVEN: '缺少替代凭据',
};

function size(value) {
  if (value === null || value === undefined) return '—';
  const units = ['B', 'KiB', 'MiB', 'GiB'];
  let amount = Number(value), unit = 0;
  while (amount >= 1024 && unit < units.length - 1) { amount /= 1024; unit++; }
  return `${amount.toFixed(unit ? 1 : 0)} ${units[unit]}`;
}

export function backupPanel(api, busy, notice, uploadArtifact) {
  let rows = [], verificationRows = [], restoreRows = [], importRows = [], activationRows = [];
  let generationRows = [];
  let createKey = null, restoreKey = null, cleanupKey = null;
  let importKey = null, activeImport = null;
  let cleanupPlan = null, cleanupRetry = null;
  let activationPlan = null, activationRetry = null, activationKey = null;
  let selectedBackup = null, selectedVerification = null, formMode = null, readSeq = 0;

  function closeRestoreForm() {
    selectedBackup = null; selectedVerification = null; formMode = null; restoreKey = null;
    $('backup-restore-form').reset(); $('backup-restore-form').classList.add('hidden');
    $('backup-restore-source').textContent = '';
  }

  function openVerificationForm(backupId) {
    selectedBackup = backupId; selectedVerification = null; formMode = 'VERIFY'; restoreKey = null;
    $('backup-restore-form').reset();
    $('backup-restore-source').textContent = `来源备份：${backupId}`;
    $('backup-restore-title').textContent = '只读校验备份';
    $('backup-recovery-label').classList.remove('hidden');
    $('backup-restore-form').elements.recovery_passphrase.required = true;
    $('backup-restore-submit').textContent = '开始只读校验';
    $('backup-restore-hint').textContent = '校验会完整解密和检查工件，但不会创建或连接恢复数据库。凭据有效期为 24 小时。';
    $('backup-restore-form').classList.remove('hidden');
    $('backup-restore-form').scrollIntoView({behavior: 'smooth', block: 'nearest'});
  }

  function openRestoreForm(backupId, verificationId) {
    selectedBackup = backupId; selectedVerification = verificationId;
    formMode = 'RESTORE'; restoreKey = null;
    $('backup-restore-form').reset();
    $('backup-restore-source').textContent = `来源备份：${backupId} · 校验凭据：${verificationId}`;
    $('backup-restore-title').textContent = '建立隔离恢复副本';
    $('backup-recovery-label').classList.add('hidden');
    $('backup-restore-form').elements.recovery_passphrase.required = false;
    $('backup-restore-submit').textContent = '恢复到隔离数据库';
    $('backup-restore-hint').textContent = '目标使用随机数据库名，不登记到 Fleet；Bot 副本保持 SEALED。';
    $('backup-restore-form').classList.remove('hidden');
    $('backup-restore-form').scrollIntoView({behavior: 'smooth', block: 'nearest'});
  }

  function closeCleanupForm() {
    cleanupPlan = null; cleanupRetry = null; cleanupKey = null;
    $('storage-cleanup-form').reset(); $('storage-cleanup-form').classList.add('hidden');
    $('storage-cleanup-resource').textContent = '';
  }

  function closeActivationForm() {
    activationPlan = null; activationRetry = null; activationKey = null;
    $('restore-activation-form').reset();
    $('restore-activation-form').classList.add('hidden');
    $('restore-activation-target').textContent = '';
  }

  async function openActivation(restoreId, target) {
    closeActivationForm();
    activationPlan = await api(
      `/restore-stages/${encodeURIComponent(restoreId)}/activation-plans`,
      {target_ordinal: target.ordinal},
    );
    $('restore-activation-target').textContent =
      `Bot ${target.bot_id} · ${activationPlan.before.database_name} → ${activationPlan.after.database_name}`;
    $('restore-activation-warning').textContent =
      `存储代际 ${activationPlan.before.storage_generation} → ${activationPlan.after.storage_generation}。旧库会先停止接纳；切换期间 Bot 可能短暂离线。`;
    $('restore-activation-form').querySelector('button[type="submit"]').textContent = '封存旧库并切换';
    $('restore-activation-form').classList.remove('hidden');
    $('restore-activation-form').scrollIntoView({behavior: 'smooth', block: 'nearest'});
  }

  function openActivationRetry(row) {
    closeActivationForm(); activationRetry = row.activation_id;
    $('restore-activation-target').textContent = `激活任务 ${row.activation_id} · Bot ${row.bot_id}`;
    $('restore-activation-warning').textContent =
      `任务在 ${states[row.state] || row.state} 阶段阻断：${row.error_code || '未知错误'}。排除外部故障后可从持久边界继续。`;
    $('restore-activation-form').querySelector('button[type="submit"]').textContent = '重试激活';
    $('restore-activation-form').classList.remove('hidden');
  }

  async function openCleanup(kind, resourceId, generation = null) {
    closeCleanupForm();
    const base = kind === 'BACKUP_ARTIFACT' ? `/backups/${encodeURIComponent(resourceId)}` :
      kind === 'RESTORE_STAGE' ? `/restore-stages/${encodeURIComponent(resourceId)}` :
      `/bots/${encodeURIComponent(generation.bot_id)}/storage-generations/${generation.storage_generation}`;
    cleanupPlan = await api(`${base}/cleanup-plan`, {});
    const label = kind === 'BACKUP_ARTIFACT' ? '备份' :
      kind === 'RESTORE_STAGE' ? '隔离恢复' : 'Bot 旧存储代际';
    $('storage-cleanup-resource').textContent = `${label}：${resourceId}`;
    $('storage-cleanup-warning').textContent = kind === 'BACKUP_ARTIFACT' ?
      '应用后下载立即失效，后台随后删除加密工件。' : kind === 'RESTORE_STAGE' ?
      '应用后隔离副本立即进入 EXPIRED；后台删除名称和 owner marker 都匹配的未采用数据库，并保留已经成为当前 storage head 的 Bot 数据库。' :
      '应用后写入不可变退休记录；后台复核当前 head、受管集群、数据库 owner 和 Forest marker 后永久删除旧数据库。当前代际不会被接受。';
    $('storage-cleanup-form').querySelector('button[type="submit"]').textContent = '撤销并清理';
    $('storage-cleanup-form').classList.remove('hidden');
    $('storage-cleanup-form').scrollIntoView({behavior: 'smooth', block: 'nearest'});
  }

  function openCleanupRetry(row) {
    closeCleanupForm(); cleanupRetry = row.cleanup_id;
    $('storage-cleanup-resource').textContent = `清理任务：${row.cleanup_id}`;
    $('storage-cleanup-warning').textContent = `上次清理已阻塞：${row.cleanup_error_code || row.error_code || '未知错误'}。确认外部冲突已排除后再重试。`;
    $('storage-cleanup-form').querySelector('button[type="submit"]').textContent = '重试清理';
    $('storage-cleanup-form').classList.remove('hidden');
  }

  function renderCut(cut) {
    const item = document.createElement('li');
    const source = cut.database_scope === 'deployment' ? '部署数据库' : `Bot ${cut.bot_id}`;
    item.textContent = `${source} · ${cut.member_count} 张表 · ${size(cut.byte_size)} · ` +
      `WAL ${cut.wal_lsn} · ${cut.captured_at}`;
    return item;
  }

  function render() {
    const target = $('backup-list'); target.replaceChildren();
    $('backup-empty').classList.toggle('hidden', rows.length !== 0);
    for (const row of rows) {
      const card = document.createElement('article'); card.className = 'audit-row';
      const title = document.createElement('h3');
      title.textContent = `${states[row.state] || row.state} · ${row.backup_id}`;
      const progress = document.createElement('p'); progress.className = 'muted';
      progress.textContent = `数据库 ${row.database_completed}/${row.database_total} · ` +
        `${row.include_packages ? '包含插件工件' : '不含插件工件'} · ${size(row.byte_size)}`;
      const meta = document.createElement('p'); meta.className = 'hint';
      meta.textContent = `创建 ${row.created_at} · 更新 ${row.updated_at}` +
        `${row.error_code ? ` · ${row.error_code}` : ''}` +
        `${row.cleanup_state ? ` · 清理 ${row.cleanup_state}${row.cleanup_error_code ? ` (${row.cleanup_error_code})` : ''}` : ''}`;
      card.append(title, progress, meta);
      if (row.roster_start_digest && row.roster_end_digest &&
          row.roster_start_digest !== row.roster_end_digest) {
        const warning = document.createElement('p'); warning.className = 'notice';
        warning.textContent = '抓取期间 Bot 目录已变化。工件仅用于取证，不能直接进入恢复计划。';
        card.append(warning);
      }
      if (row.databases?.length) {
        const cuts = document.createElement('ul');
        for (const cut of row.databases) cuts.append(renderCut(cut));
        card.append(cuts);
      }
      if (row.download_available) {
        const link = document.createElement('a'); link.className = 'button-link';
        link.href = `/management/v1/backups/${encodeURIComponent(row.backup_id)}/download`;
        link.textContent = '下载加密备份';
        card.append(link);
      }
      if (row.state === 'READY') {
        const latest = verificationRows.find((item) => item.backup_id === row.backup_id);
        if (latest) {
          const receipt = document.createElement('p'); receipt.className = 'hint';
          receipt.textContent = `最近校验：${states[latest.state] || latest.state}` +
            `${latest.expires_at ? ` · 有效期至 ${latest.expires_at}` : ''}` +
            `${latest.error_code ? ` · ${latest.error_code}` : ''}`;
          card.append(receipt);
        }
        const verify = document.createElement('button'); verify.className = 'subtle';
        verify.type = 'button'; verify.textContent = '只读校验备份';
        verify.addEventListener('click', () => openVerificationForm(row.backup_id));
        card.append(verify);
        const valid = verificationRows.find(
          (item) => item.backup_id === row.backup_id && item.receipt_valid
        );
        if (valid) {
          const restore = document.createElement('button'); restore.className = 'subtle';
          restore.type = 'button'; restore.textContent = '建立隔离恢复副本';
          restore.addEventListener('click', () => openRestoreForm(
            row.backup_id, valid.verification_id
          ));
          card.append(restore);
        }
      }
      if (['READY', 'INCONSISTENT'].includes(row.state)) {
        const remove = document.createElement('button'); remove.className = 'danger';
        remove.type = 'button'; remove.textContent = '预览并删除备份';
        remove.addEventListener('click', () => busy(remove, () => openCleanup('BACKUP_ARTIFACT', row.backup_id)));
        card.append(remove);
      } else if (row.cleanup_state === 'BLOCKED') {
        const retry = document.createElement('button'); retry.className = 'danger';
        retry.type = 'button'; retry.textContent = '处理冲突后重试清理';
        retry.addEventListener('click', () => openCleanupRetry(row)); card.append(retry);
      }
      target.append(card);
    }
    const importTarget = $('backup-import-list'); importTarget.replaceChildren();
    $('backup-import-empty').classList.toggle('hidden', importRows.length !== 0);
    for (const row of importRows) {
      const card = document.createElement('article'); card.className = 'audit-row';
      const title = document.createElement('h3');
      title.textContent = `${states[row.state] || row.state} · ${row.import_id}`;
      const progress = document.createElement('p'); progress.className = 'muted';
      progress.textContent = `${size(row.byte_size ?? 0)} / ${size(row.expected_size)}` +
        `${row.backup_id ? ` · 备份 ${row.backup_id}` : ''}` +
        `${row.database_total ? ` · ${row.database_total} 个数据库` : ''}`;
      const meta = document.createElement('p'); meta.className = 'hint';
      meta.textContent = `创建 ${row.created_at} · 更新 ${row.updated_at}` +
        `${row.error_code ? ` · ${row.error_code}` : ''}`;
      card.append(title, progress, meta); importTarget.append(card);
    }
    const restoreTarget = $('restore-list'); restoreTarget.replaceChildren();
    $('restore-empty').classList.toggle('hidden', restoreRows.length !== 0);
    for (const row of restoreRows) {
      const card = document.createElement('article'); card.className = 'audit-row';
      const title = document.createElement('h3');
      title.textContent = `${states[row.state] || row.state} · ${row.restore_id}`;
      const progress = document.createElement('p'); progress.className = 'muted';
      progress.textContent = `数据库 ${row.database_completed}/${row.database_total} · 来源 ${row.backup_id}`;
      const meta = document.createElement('p'); meta.className = 'hint';
      meta.textContent = `创建 ${row.created_at} · 更新 ${row.updated_at}` +
        `${row.error_code ? ` · ${row.error_code}` : ''}` +
        `${row.cleanup_state ? ` · 清理 ${row.cleanup_state}${row.cleanup_error_code ? ` (${row.cleanup_error_code})` : ''}` : ''}`;
      card.append(title, progress, meta);
      if (row.targets?.length) {
        const list = document.createElement('ul');
        for (const target of row.targets) {
          const item = document.createElement('li');
          const source = target.database_scope === 'deployment' ? '部署副本' : `Bot ${target.bot_id}`;
          item.textContent = `${source} · ${target.database_name} · ${states[target.state] || target.state}`;
          if (target.database_scope === 'bot' && target.state === 'STAGED') {
            const activation = activationRows.find((candidate) =>
              candidate.restore_id === row.restore_id && candidate.target_ordinal === target.ordinal
            );
            if (!activation) {
              const activate = document.createElement('button'); activate.className = 'danger';
              activate.type = 'button'; activate.textContent = '预览并激活此 Bot 副本';
              activate.addEventListener('click', () => busy(
                activate, () => openActivation(row.restore_id, target)
              ));
              item.append(document.createTextNode(' '), activate);
            } else if (activation.state === 'BLOCKED') {
              const retry = document.createElement('button'); retry.className = 'danger';
              retry.type = 'button'; retry.textContent = '处理问题后重试激活';
              retry.addEventListener('click', () => openActivationRetry(activation));
              item.append(document.createTextNode(' '), retry);
            } else {
              item.append(document.createTextNode(
                ` · 激活 ${states[activation.state] || activation.state}`
              ));
            }
          }
          list.append(item);
        }
        card.append(list);
      }
      if (['STAGED', 'FAILED'].includes(row.state)) {
        const remove = document.createElement('button'); remove.className = 'danger';
        remove.type = 'button'; remove.textContent = '预览并删除隔离副本';
        remove.addEventListener('click', () => busy(remove, () => openCleanup('RESTORE_STAGE', row.restore_id)));
        card.append(remove);
      } else if (row.cleanup_state === 'BLOCKED') {
        const retry = document.createElement('button'); retry.className = 'danger';
        retry.type = 'button'; retry.textContent = '处理冲突后重试清理';
        retry.addEventListener('click', () => openCleanupRetry(row)); card.append(retry);
      }
      restoreTarget.append(card);
    }
    const generationTarget = $('storage-generation-list'); generationTarget.replaceChildren();
    $('storage-generation-empty').classList.toggle('hidden', generationRows.length !== 0);
    for (const row of generationRows) {
      const card = document.createElement('article'); card.className = 'audit-row';
      const title = document.createElement('h3');
      title.textContent = `${row.display_name} · 第 ${row.storage_generation} 代 · ` +
        `${states[row.state] || row.state}`;
      const progress = document.createElement('p'); progress.className = 'muted';
      progress.textContent = row.source_kind === 'PROVISIONED' ? '初始供应数据库' :
        `恢复副本 ${row.source_restore_id} / ${row.source_ordinal}`;
      const meta = document.createElement('p'); meta.className = 'hint';
      meta.textContent = `创建 ${row.created_at}` +
        `${row.superseded_at ? ` · 被替代 ${row.superseded_at}` : ''}` +
        `${row.retire_after && !row.current ? ` · 自动回收不早于 ${row.retire_after}` : ''}` +
        `${row.cleanup_state ? ` · 清理 ${row.cleanup_state}${row.error_code ? ` (${row.error_code})` : ''}` : ''}`;
      card.append(title, progress, meta);
      if (row.state === 'SUPERSEDED') {
        const remove = document.createElement('button'); remove.className = 'danger';
        remove.type = 'button'; remove.textContent = '预览并回收旧数据库';
        const id = `${row.bot_id}:${row.storage_generation}`;
        remove.addEventListener('click', () => busy(
          remove, () => openCleanup('BOT_STORAGE_GENERATION', id, row)
        ));
        card.append(remove);
      } else if (row.cleanup_state === 'BLOCKED') {
        const retry = document.createElement('button'); retry.className = 'danger';
        retry.type = 'button'; retry.textContent = '处理冲突后重试回收';
        retry.addEventListener('click', () => openCleanupRetry(row)); card.append(retry);
      }
      generationTarget.append(card);
    }
  }

  async function load() {
    const seq = ++readSeq;
    const [
      data, verificationData, restoreData, importData, activationData, generationData,
    ] = await Promise.all([
      api('/backups'), api('/backup-verifications'), api('/restore-stages'),
      api('/backup-imports'), api('/restore-activations'), api('/storage-generations'),
    ]);
    const detailed = await Promise.all(data.backups.map(async (row) => {
      if (!['READY', 'INCONSISTENT'].includes(row.state)) return row;
      return api(`/backups/${encodeURIComponent(row.backup_id)}`);
    }));
    const restoreDetails = await Promise.all(restoreData.restore_stages.map(async (row, index) => {
      if (index >= 20 || !['STAGED', 'FAILED'].includes(row.state)) return row;
      return api(`/restore-stages/${encodeURIComponent(row.restore_id)}`);
    }));
    if (seq !== readSeq) return data;
    rows = detailed; verificationRows = verificationData.verifications;
    restoreRows = restoreDetails; importRows = importData.imports;
    activationRows = activationData.restore_activations;
    generationRows = generationData.storage_generations; render(); return data;
  }

  $('backup-refresh').addEventListener('click', () => busy($('backup-refresh'), load));
  $('backup-create-form').addEventListener('input', () => { createKey = null; });
  $('backup-create-form').addEventListener('submit', (event) => {
    event.preventDefault(); const form = event.currentTarget;
    if (!form.reportValidity()) return;
    busy(form.querySelector('button'), async () => {
      const recovery = form.elements.recovery_passphrase.value;
      if (recovery !== form.elements.recovery_confirmation.value) {
        throw new Error('两次输入的恢复口令不一致。');
      }
      createKey ||= crypto.randomUUID();
      try {
        await api('/backups', {
          recovery_passphrase: recovery,
          current_password: form.elements.current_password.value,
          include_packages: form.elements.include_packages.checked,
        }, {'Idempotency-Key': createKey});
      } finally {
        form.elements.current_password.value = '';
        form.elements.recovery_passphrase.value = '';
        form.elements.recovery_confirmation.value = '';
      }
      createKey = null; await load();
      notice('备份任务已接纳。请妥善保存恢复口令；服务端不会保留口令明文。', true);
    });
  });
  $('backup-import-form').addEventListener('change', (event) => {
    if (event.target.name === 'artifact') { importKey = null; activeImport = null; }
  });
  $('backup-import-form').addEventListener('submit', (event) => {
    event.preventDefault(); const form = event.currentTarget;
    if (!form.reportValidity()) return;
    busy(form.querySelector('button'), async () => {
      const artifact = form.elements.artifact.files[0];
      if (!artifact || artifact.size < 1) throw new Error('请选择非空的加密备份文件。');
      importKey ||= crypto.randomUUID();
      if (!activeImport) {
        activeImport = await api('/backup-imports', {
          expected_size: artifact.size,
          current_password: form.elements.current_password.value,
        }, {'Idempotency-Key': importKey});
      }
      if (activeImport.upload_available) {
        activeImport = await uploadArtifact(
          `/backup-imports/${encodeURIComponent(activeImport.import_id)}/artifact`, artifact
        );
      }
      activeImport = await api(
        `/backup-imports/${encodeURIComponent(activeImport.import_id)}/finalize`, {
          recovery_passphrase: form.elements.recovery_passphrase.value,
          current_password: form.elements.current_password.value,
        }
      );
      form.reset(); importKey = null; activeImport = null; await load();
      notice('外部备份已接纳；后台正在完整解密校验，成功后会出现在标准备份列表。', true);
    });
  });
  $('backup-restore-cancel').addEventListener('click', closeRestoreForm);
  $('backup-restore-form').addEventListener('input', () => { restoreKey = null; });
  $('backup-restore-form').addEventListener('submit', (event) => {
    event.preventDefault(); const form = event.currentTarget;
    if (!selectedBackup || !formMode || !form.reportValidity()) return;
    busy(form.querySelector('button[type="submit"]'), async () => {
      restoreKey ||= crypto.randomUUID();
      const backupId = selectedBackup;
      try {
        if (formMode === 'VERIFY') {
          await api(`/backups/${encodeURIComponent(backupId)}/verifications`, {
            recovery_passphrase: form.elements.recovery_passphrase.value,
            current_password: form.elements.current_password.value,
          }, {'Idempotency-Key': restoreKey});
        } else {
          await api(`/backups/${encodeURIComponent(backupId)}/restore-stages`, {
            verification_id: selectedVerification,
            current_password: form.elements.current_password.value,
          }, {'Idempotency-Key': restoreKey});
        }
      } finally {
        form.elements.recovery_passphrase.value = '';
        form.elements.current_password.value = '';
      }
      const completedMode = formMode;
      closeRestoreForm(); await load();
      notice(completedMode === 'VERIFY' ?
        '只读校验任务已接纳；通过后会显示建立隔离恢复副本按钮。' :
        '隔离恢复任务已接纳。目标数据库不会登记为在线 Bot。', true);
    });
  });
  $('storage-cleanup-cancel').addEventListener('click', closeCleanupForm);
  $('storage-cleanup-form').addEventListener('input', () => { cleanupKey = null; });
  $('storage-cleanup-form').addEventListener('submit', (event) => {
    event.preventDefault(); const form = event.currentTarget;
    if ((!cleanupPlan && !cleanupRetry) || !form.reportValidity()) return;
    busy(form.querySelector('button[type="submit"]'), async () => {
      cleanupKey ||= crypto.randomUUID();
      try {
        if (cleanupRetry) {
          await api(`/storage-cleanups/${encodeURIComponent(cleanupRetry)}/retry`, {
            current_password: form.elements.current_password.value,
          }, {'Idempotency-Key': cleanupKey});
        } else {
          await api('/storage-cleanups', {
            plan_id: cleanupPlan.plan_id, plan_digest: cleanupPlan.plan_digest,
            current_password: form.elements.current_password.value,
          }, {'Idempotency-Key': cleanupKey});
        }
      } finally {
        form.elements.current_password.value = '';
      }
      closeCleanupForm(); await load();
      notice('资源可见性已撤销；物理清理状态会在列表中继续更新。', true);
    });
  });
  $('restore-activation-cancel').addEventListener('click', closeActivationForm);
  $('restore-activation-form').addEventListener('input', () => { activationKey = null; });
  $('restore-activation-form').addEventListener('submit', (event) => {
    event.preventDefault(); const form = event.currentTarget;
    if ((!activationPlan && !activationRetry) || !form.reportValidity()) return;
    busy(form.querySelector('button[type="submit"]'), async () => {
      activationKey ||= crypto.randomUUID();
      try {
        if (activationRetry) {
          await api(`/restore-activations/${encodeURIComponent(activationRetry)}/retry`, {
            current_password: form.elements.current_password.value,
          }, {'Idempotency-Key': activationKey});
        } else {
          await api('/restore-activations', {
            plan_id: activationPlan.plan_id, plan_digest: activationPlan.plan_digest,
            current_password: form.elements.current_password.value,
          }, {'Idempotency-Key': activationKey});
        }
      } finally {
        form.elements.current_password.value = '';
      }
      closeActivationForm(); await load();
      notice('恢复激活任务已接纳；页面会持续显示封存、目录切换与重新接入状态。', true);
    });
  });

  return {
    load,
    hasRunning: () => rows.some((row) => ['QUEUED', 'CAPTURING', 'SEALING'].includes(row.state) ||
      ['QUEUED', 'CLEANING', 'RETRY'].includes(row.cleanup_state)) ||
      restoreRows.some((row) => ['QUEUED', 'VERIFYING', 'RESTORING', 'VALIDATING'].includes(row.state) ||
        ['QUEUED', 'CLEANING', 'RETRY'].includes(row.cleanup_state)) ||
      verificationRows.some((row) => ['QUEUED', 'VERIFYING'].includes(row.state)) ||
      importRows.some((row) => ['UPLOADING', 'QUEUED', 'IMPORTING'].includes(row.state)) ||
      activationRows.some((row) =>
        ['QUEUED', 'FENCING', 'SWITCHING', 'ACTIVATING'].includes(row.state)
      ) || generationRows.some((row) =>
        ['QUEUED', 'CLEANING', 'RETRY'].includes(row.cleanup_state)
      ),
    reset() {
      readSeq++; rows = []; verificationRows = []; restoreRows = []; importRows = [];
      activationRows = []; generationRows = [];
      createKey = null; importKey = null; activeImport = null;
      closeRestoreForm(); closeCleanupForm(); closeActivationForm();
      $('backup-create-form').reset(); $('backup-list').replaceChildren();
      $('backup-import-form').reset(); $('backup-import-list').replaceChildren();
      $('restore-list').replaceChildren();
      $('storage-generation-list').replaceChildren();
      $('backup-empty').classList.remove('hidden'); $('restore-empty').classList.remove('hidden');
      $('backup-import-empty').classList.remove('hidden');
      $('storage-generation-empty').classList.remove('hidden');
    },
  };
}
