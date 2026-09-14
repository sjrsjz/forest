const $ = (id) => document.getElementById(id);

const states = {
  QUEUED: '等待执行', CAPTURING: '正在抓取数据库', SEALING: '正在加密封口',
  READY: '可下载', INCONSISTENT: '目录在抓取期间发生变化', FAILED: '失败', EXPIRED: '已过期',
  VERIFYING: '正在校验工件', RESTORING: '正在恢复隔离数据库',
  VALIDATING: '正在验证恢复结果', STAGED: '隔离恢复完成',
};

function size(value) {
  if (value === null || value === undefined) return '—';
  const units = ['B', 'KiB', 'MiB', 'GiB'];
  let amount = Number(value), unit = 0;
  while (amount >= 1024 && unit < units.length - 1) { amount /= 1024; unit++; }
  return `${amount.toFixed(unit ? 1 : 0)} ${units[unit]}`;
}

export function backupPanel(api, busy, notice) {
  let rows = [], restoreRows = [], createKey = null, restoreKey = null;
  let selectedBackup = null, readSeq = 0;

  function closeRestoreForm() {
    selectedBackup = null; restoreKey = null;
    $('backup-restore-form').reset(); $('backup-restore-form').classList.add('hidden');
    $('backup-restore-source').textContent = '';
  }

  function openRestoreForm(backupId) {
    selectedBackup = backupId; restoreKey = null;
    $('backup-restore-form').reset();
    $('backup-restore-source').textContent = `来源备份：${backupId}`;
    $('backup-restore-form').classList.remove('hidden');
    $('backup-restore-form').scrollIntoView({behavior: 'smooth', block: 'nearest'});
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
        `${row.error_code ? ` · ${row.error_code}` : ''}`;
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
        const restore = document.createElement('button'); restore.className = 'subtle';
        restore.type = 'button'; restore.textContent = '建立隔离恢复副本';
        restore.addEventListener('click', () => openRestoreForm(row.backup_id));
        card.append(restore);
      }
      target.append(card);
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
        `${row.error_code ? ` · ${row.error_code}` : ''}`;
      card.append(title, progress, meta);
      if (row.targets?.length) {
        const list = document.createElement('ul');
        for (const target of row.targets) {
          const item = document.createElement('li');
          const source = target.database_scope === 'deployment' ? '部署副本' : `Bot ${target.bot_id}`;
          item.textContent = `${source} · ${target.database_name} · ${states[target.state] || target.state}`;
          list.append(item);
        }
        card.append(list);
      }
      restoreTarget.append(card);
    }
  }

  async function load() {
    const seq = ++readSeq;
    const [data, restoreData] = await Promise.all([api('/backups'), api('/restore-stages')]);
    const detailed = await Promise.all(data.backups.map(async (row) => {
      if (!['READY', 'INCONSISTENT'].includes(row.state)) return row;
      return api(`/backups/${encodeURIComponent(row.backup_id)}`);
    }));
    const restoreDetails = await Promise.all(restoreData.restore_stages.map(async (row, index) => {
      if (index >= 20 || !['STAGED', 'FAILED'].includes(row.state)) return row;
      return api(`/restore-stages/${encodeURIComponent(row.restore_id)}`);
    }));
    if (seq !== readSeq) return data;
    rows = detailed; restoreRows = restoreDetails; render(); return data;
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
  $('backup-restore-cancel').addEventListener('click', closeRestoreForm);
  $('backup-restore-form').addEventListener('input', () => { restoreKey = null; });
  $('backup-restore-form').addEventListener('submit', (event) => {
    event.preventDefault(); const form = event.currentTarget;
    if (!selectedBackup || !form.reportValidity()) return;
    busy(form.querySelector('button[type="submit"]'), async () => {
      restoreKey ||= crypto.randomUUID();
      const backupId = selectedBackup;
      try {
        await api(`/backups/${encodeURIComponent(backupId)}/restore-stages`, {
          recovery_passphrase: form.elements.recovery_passphrase.value,
          current_password: form.elements.current_password.value,
        }, {'Idempotency-Key': restoreKey});
      } finally {
        form.elements.recovery_passphrase.value = '';
        form.elements.current_password.value = '';
      }
      closeRestoreForm(); await load();
      notice('隔离恢复任务已接纳。目标数据库不会登记为在线 Bot。', true);
    });
  });

  return {
    load,
    hasRunning: () => rows.some((row) => ['QUEUED', 'CAPTURING', 'SEALING'].includes(row.state)) ||
      restoreRows.some((row) => ['QUEUED', 'VERIFYING', 'RESTORING', 'VALIDATING'].includes(row.state)),
    reset() {
      readSeq++; rows = []; restoreRows = []; createKey = null;
      closeRestoreForm();
      $('backup-create-form').reset(); $('backup-list').replaceChildren();
      $('restore-list').replaceChildren();
      $('backup-empty').classList.remove('hidden'); $('restore-empty').classList.remove('hidden');
    },
  };
}
