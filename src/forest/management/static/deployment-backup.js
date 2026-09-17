const $ = id => document.getElementById(id);
const size = value => value < 1024 ? `${value} B` : value < 1048576
  ? `${(value / 1024).toFixed(1)} KiB` : `${(value / 1048576).toFixed(1)} MiB`;
const params = values => new URLSearchParams(values).toString();

export function createModulePanel(api, busy, notice) {
  let scope = null, epoch = 0, poll = null, selectedBackup = null;
  let createKey = null, reserveKey = null, uploadKey = null, finalizeKey = null;
  let verificationKey = null, activeImport = null;
  const base = () => `/scopes/deployment/${encodeURIComponent(scope.scope_id)}/modules/deployment.backup`;

  function closeVerification() {
    selectedBackup = null; verificationKey = null;
    $('deployment-backup-verification-form').reset();
    $('deployment-backup-verification').classList.add('hidden');
  }

  function openVerification(backupId) {
    selectedBackup = backupId; verificationKey = null;
    $('deployment-backup-verification-target').textContent = `备份 ${backupId}`;
    $('deployment-backup-verification').classList.remove('hidden');
  }

  function render(backups, imports, verifications) {
    const target = $('deployment-backup-list'); target.replaceChildren();
    $('deployment-backup-empty').classList.toggle('hidden', backups.length !== 0);
    for (const row of backups) {
      const card = document.createElement('article'); card.className = 'audit-row';
      const title = document.createElement('h3');
      title.textContent = `${row.state} · ${row.backup_id}`;
      const meta = document.createElement('p'); meta.className = 'muted';
      meta.textContent = `${row.database_completed}/${row.database_total} 个数据库 · ${size(row.byte_size || 0)}${row.error_code ? ` · ${row.error_code}` : ''}`;
      card.append(title, meta);
      if (['READY', 'INCONSISTENT'].includes(row.state)) {
        const download = document.createElement('a'); download.className = 'button-link';
        download.textContent = '下载';
        download.href = `/management/v1${base()}/downloads/backup.archive?${params({backup_id:row.backup_id})}`;
        const verify = document.createElement('button'); verify.type = 'button'; verify.className = 'subtle';
        verify.textContent = '只读验证'; verify.addEventListener('click', () => openVerification(row.backup_id));
        card.append(download, verify);
      }
      const receipt = verifications.find(item => item.backup_id === row.backup_id && item.receipt_valid);
      if (receipt) {
        const verified = document.createElement('p'); verified.className = 'hint';
        verified.textContent = `验证凭据 ${receipt.verification_id} · 有效至 ${receipt.expires_at}`;
        card.append(verified);
      }
      target.append(card);
    }
    const importTarget = $('deployment-backup-import-list'); importTarget.replaceChildren();
    $('deployment-backup-import-empty').classList.toggle('hidden', imports.length !== 0);
    for (const row of imports) {
      const card = document.createElement('article'); card.className = 'audit-row';
      const title = document.createElement('h3'); title.textContent = `${row.state} · ${row.import_id}`;
      const meta = document.createElement('p'); meta.className = 'muted';
      meta.textContent = `${size(row.byte_size || 0)} / ${size(row.expected_size)}${row.backup_id ? ` · 备份 ${row.backup_id}` : ''}${row.error_code ? ` · ${row.error_code}` : ''}`;
      card.append(title, meta); importTarget.append(card);
    }
  }

  async function load() {
    if (!scope) return;
    const own = epoch;
    const [backupData, importData, verificationData] = await Promise.all([
      api(`${base()}/queries/backups`), api(`${base()}/queries/imports`),
      api(`${base()}/queries/verifications`),
    ]);
    const detailed = await Promise.all(backupData.backups.map(row =>
      ['READY', 'INCONSISTENT'].includes(row.state)
        ? api(`${base()}/queries/backup?${params({backup_id:row.backup_id})}`) : row));
    if (own !== epoch) return;
    render(detailed, importData.imports, verificationData.verifications);
  }

  $('deployment-backup-refresh').addEventListener('click', event =>
    busy(event.currentTarget, load));
  $('deployment-backup-create-form').addEventListener('input', () => { createKey = null; });
  $('deployment-backup-create-form').addEventListener('submit', event => {
    event.preventDefault(); const form = event.currentTarget;
    if (!form.reportValidity()) return;
    busy(form.querySelector('button'), async () => {
      const recovery = form.elements.recovery_passphrase.value;
      if (recovery !== form.elements.recovery_confirmation.value) {
        throw new Error('两次输入的恢复口令不一致。');
      }
      createKey ||= crypto.randomUUID();
      try {
        await api(`${base()}/actions/backup.create`, {
          recovery_passphrase:recovery,
          current_password:form.elements.current_password.value,
          include_packages:form.elements.include_packages.checked,
        }, {'Idempotency-Key':createKey});
      } finally {
        form.elements.current_password.value = '';
        form.elements.recovery_passphrase.value = '';
        form.elements.recovery_confirmation.value = '';
      }
      createKey = null; await load(); notice('备份任务已接纳。', true);
    });
  });
  $('deployment-backup-import-form').addEventListener('change', event => {
    if (event.target.name === 'artifact') {
      reserveKey = uploadKey = finalizeKey = null; activeImport = null;
    }
  });
  $('deployment-backup-import-form').addEventListener('submit', event => {
    event.preventDefault(); const form = event.currentTarget;
    if (!form.reportValidity()) return;
    busy(form.querySelector('button'), async () => {
      const artifact = form.elements.artifact.files[0];
      if (!artifact || artifact.size < 1) throw new Error('请选择非空备份文件。');
      reserveKey ||= crypto.randomUUID(); uploadKey ||= crypto.randomUUID();
      finalizeKey ||= crypto.randomUUID();
      if (!activeImport) activeImport = await api(`${base()}/actions/import.reserve`, {
        expected_size:artifact.size, current_password:form.elements.current_password.value,
      }, {'Idempotency-Key':reserveKey});
      if (activeImport.upload_available) activeImport = await api(
        `${base()}/uploads/import.artifact?${params({import_id:activeImport.import_id})}`,
        artifact, {'Content-Type':'application/vnd.forest.backup', 'Idempotency-Key':uploadKey});
      activeImport = await api(`${base()}/actions/import.finalize`, {
        import_id:activeImport.import_id,
        recovery_passphrase:form.elements.recovery_passphrase.value,
        current_password:form.elements.current_password.value,
      }, {'Idempotency-Key':finalizeKey});
      form.reset(); reserveKey = uploadKey = finalizeKey = null; activeImport = null;
      await load(); notice('外部备份已进入后台校验。', true);
    });
  });
  $('deployment-backup-verification-cancel').addEventListener('click', closeVerification);
  $('deployment-backup-verification-form').addEventListener('input', () => { verificationKey = null; });
  $('deployment-backup-verification-form').addEventListener('submit', event => {
    event.preventDefault(); const form = event.currentTarget;
    if (!selectedBackup || !form.reportValidity()) return;
    busy(form.querySelector('button[type="submit"]'), async () => {
      verificationKey ||= crypto.randomUUID();
      await api(`${base()}/actions/verification.create`, {
        backup_id:selectedBackup,
        recovery_passphrase:form.elements.recovery_passphrase.value,
        current_password:form.elements.current_password.value,
      }, {'Idempotency-Key':verificationKey});
      closeVerification(); await load(); notice('只读验证任务已接纳。', true);
    });
  });

  function reset() {
    epoch++; scope = null; if (poll !== null) clearInterval(poll); poll = null;
    createKey = reserveKey = uploadKey = finalizeKey = verificationKey = null;
    activeImport = null; closeVerification();
    $('deployment-backup-list').replaceChildren();
    $('deployment-backup-import-list').replaceChildren();
  }
  async function select(value) {
    reset(); scope = value; await load();
    poll = setInterval(() => load().catch(error => notice(error.message)), 1500);
  }
  function invalidateContent(value) {
    if (scope && value === base()) reset();
  }
  return {select, reset, refresh:load, invalidateContent};
}
