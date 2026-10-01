export async function mount({root, component, api, notice}) {
  const list = root.querySelector('[data-list]');
  const endpoint = `/management/v1/modules/${component.component_id}`;
  const passwordDialog = root.querySelector('[data-password-dialog]');
  const passwordForm = root.querySelector('[data-password-form]');
  let passwordTarget = null;

  async function action(actionId, payload) {
    return api(`${endpoint}/actions/${actionId}`, {
      method: 'POST',
      headers: {'idempotency-key': crypto.randomUUID()},
      body: JSON.stringify(payload),
    });
  }

  function label(text, control) {
    const node = document.createElement('label');
    node.append(text, control);
    return node;
  }

  function render(account) {
    const row = document.createElement('form');
    row.className = 'account-row';
    const identity = document.createElement('div');
    identity.className = 'account-id';
    const username = document.createElement('strong');
    username.textContent = account.username;
    const subject = document.createElement('code');
    subject.textContent = `${account.subject_id} · rev ${account.revision}`;
    identity.append(username, subject);

    const displayName = document.createElement('input');
    displayName.value = account.display_name;
    displayName.required = true;
    displayName.maxLength = 128;
    const role = document.createElement('select');
    for (const value of ['ADMIN', 'AUDITOR']) {
      const option = document.createElement('option');
      option.value = option.textContent = value;
      option.selected = value === account.role;
      role.append(option);
    }
    const enabled = document.createElement('input');
    enabled.type = 'checkbox';
    enabled.checked = account.enabled;
    const actions = document.createElement('div');
    actions.className = 'account-actions';
    const save = document.createElement('button');
    save.textContent = '保存';
    const password = document.createElement('button');
    password.type = 'button';
    password.textContent = '改密';
    const revoke = document.createElement('button');
    revoke.type = 'button';
    revoke.className = 'danger';
    revoke.textContent = '撤销会话';
    actions.append(save, password, revoke);
    row.append(
      identity,
      label('显示名', displayName),
      label('角色', role),
      label('启用', enabled),
      actions,
    );
    row.onsubmit = async event => {
      event.preventDefault();
      try {
        await action('account.update', {
          subject_id: account.subject_id,
          display_name: displayName.value,
          role: role.value,
          enabled: enabled.checked,
          expected_revision: account.revision,
        });
        await load();
      } catch (error) { notice(error.message); }
    };
    password.onclick = async () => {
      passwordTarget = account;
      root.querySelector('[data-password-user]').textContent = account.username;
      passwordForm.reset();
      passwordDialog.showModal();
    };
    revoke.onclick = async () => {
      try {
        await action('account.sessions.revoke', {
          subject_id: account.subject_id,
          expected_revision: account.revision,
        });
        notice('该账号的活动会话已撤销。');
      } catch (error) { notice(error.message); }
    };
    return row;
  }

  async function load() {
    try {
      const data = await api(`${endpoint}/queries/accounts.list`, {
        method: 'POST', body: '{}',
      });
      list.replaceChildren(...data.items.map(render));
    } catch (error) { notice(error.message); }
  }

  root.querySelector('[data-refresh]').onclick = load;
  root.querySelector('[data-password-cancel]').onclick = () => passwordDialog.close();
  passwordForm.onsubmit = async event => {
    event.preventDefault();
    if (!passwordTarget) return;
    try {
      await action('account.password', {
        subject_id: passwordTarget.subject_id,
        password: new FormData(passwordForm).get('password'),
        expected_revision: passwordTarget.revision,
      });
      passwordDialog.close();
      notice('密码已更新；该账号的旧会话已撤销。');
      await load();
    } catch (error) { notice(error.message); }
  };
  root.querySelector('[data-create]').onsubmit = async event => {
    event.preventDefault();
    const values = Object.fromEntries(new FormData(event.currentTarget));
    try {
      await action('account.create', values);
      event.currentTarget.reset();
      await load();
    } catch (error) { notice(error.message); }
  };
  await load();
}
