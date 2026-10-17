import {randomId} from './random-id.js';

const ACTIONS = ['status', 'cancel', 'disable', 'reload'];

export async function mount({root, component, api, notice}) {
  const base = `/management/v1/modules/${encodeURIComponent(component.component_id)}`;
  const form = root.querySelector('#core-rescue-form');
  const revision = root.querySelector('#core-rescue-revision');
  const grants = root.querySelector('#core-rescue-grants');
  const revoke = root.querySelector('#core-rescue-revoke');
  let current = null;

  async function load() {
    current = await api(`${base}/queries/rescue.policy.read`, {
      method: 'POST', body: JSON.stringify({}),
    });
    revision.textContent = `策略 r${current.revision} · ${current.grants.length}/32 条`;
    grants.replaceChildren();
    for (const grant of current.grants) {
      const row = document.createElement('tr');
      for (const value of [grant.binding_ref, grant.group_ref, grant.principal_ref,
        grant.actions.join('、'), `r${grant.changed_revision}`]) {
        const cell = document.createElement('td');
        cell.textContent = value;
        row.append(cell);
      }
      row.onclick = () => {
        for (const field of ['binding_ref', 'group_ref', 'principal_ref']) {
          form.elements.namedItem(field).value = grant[field];
        }
        for (const action of ACTIONS) {
          form.elements.namedItem(action).checked = grant.actions.includes(action);
        }
      };
      grants.append(row);
    }
  }

  async function put(actions) {
    if (!form.reportValidity()) return;
    const body = {
      expected_revision: current.revision,
      binding_ref: form.elements.namedItem('binding_ref').value.trim(),
      group_ref: form.elements.namedItem('group_ref').value.trim(),
      principal_ref: form.elements.namedItem('principal_ref').value.trim(),
      actions,
    };
    form.querySelectorAll('button').forEach(button => { button.disabled = true; });
    notice('');
    try {
      const saved = await api(`${base}/actions/rescue.grant.put`, {
        method: 'POST', headers: {'idempotency-key': randomId()},
        body: JSON.stringify(body),
      });
      await load();
      notice(`救援授权策略已更新到 r${saved.revision}。`);
    } catch (error) {
      notice(error.message);
      await load();
    } finally {
      form.querySelectorAll('button').forEach(button => { button.disabled = false; });
    }
  }

  form.onsubmit = async event => {
    event.preventDefault();
    await put(ACTIONS.filter(action => form.elements.namedItem(action).checked));
  };
  revoke.onclick = async () => { await put([]); };
  await load();
}
