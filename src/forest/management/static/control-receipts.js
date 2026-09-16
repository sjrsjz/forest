// The barrier lives only while requests for this resource are in flight. A
// response captured before an unavailable receipt must not restore its body.
export function controlScope(path) {
  const match = path.match(/^\/bots\/[^/?]+\/(?:components\/[^/?]+|runtime-modules\/[^/?]+|jobs|bindings|delivery)(?=[/?]|$)/);
  return match?.[0] || null;
}

function unavailable(receipt) {
  const message = receipt ? `原操作已记录，回执内容已不可用。操作编号：${receipt.operation_id}。请重新加载并核对当前状态。` :
    '相关内容已不可用，请重新加载并核对当前状态。';
  const error = new Error(message);
  error.code = 'CONTROL_CONTENT_REVOKED';
  error.receipt = receipt;
  return error;
}

export function controlReceipts(invalidate) {
  const active = new Map();
  return async (path, read) => {
    const scope = controlScope(path);
    if (!scope) return read();
    let group = active.get(scope);
    if (!group) { group = {pending:0, revision:0, receipt:null}; active.set(scope, group); }
    group.pending++;
    const revision = group.revision;
    try {
      let data;
      try { data = await read(); }
      catch (error) {
        if (error.code === 'CONTROL_CONTENT_REVOKED') {
          group.revision++;
          invalidate(scope);
        }
        throw error;
      }
      if (data?.result_payload_state === 'UNAVAILABLE' && data.operation_status === 'RECORDED') {
        // Preserve only the receipt facts; never copy an unexpected old body.
        group.receipt = {operation_id:data.operation_id, action:data.action};
        group.revision++;
        invalidate(scope);
        throw unavailable(group.receipt);
      }
      if (revision !== group.revision) throw unavailable(group.receipt);
      return data;
    } finally {
      if (--group.pending === 0) active.delete(scope);
    }
  };
}
