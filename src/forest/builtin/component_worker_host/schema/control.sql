CREATE TABLE selection_heads (
    bot_id text PRIMARY KEY,
    revision bigint NOT NULL DEFAULT 0 CHECK (revision >= 0)
);

CREATE TABLE selection_entries (
    bot_id text NOT NULL,
    component_id text NOT NULL CHECK (component_id ~ '^[a-z0-9][a-z0-9._-]{0,127}$'),
    source_kind text NOT NULL CHECK (source_kind IN ('builtin','installed')),
    package_digest text,
    manifest_digest text,
    approved_capabilities jsonb NOT NULL DEFAULT '[]'::jsonb,
    desired_enabled boolean NOT NULL,
    desired_revision bigint NOT NULL CHECK (desired_revision > 0),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id, component_id),
    CHECK (
        (source_kind='builtin' AND package_digest IS NULL AND manifest_digest IS NULL
            AND approved_capabilities='[]'::jsonb)
        OR (source_kind='installed'
            AND package_digest IS NOT NULL AND manifest_digest IS NOT NULL
            AND package_digest ~ '^sha256:[a-f0-9]{64}$'
            AND manifest_digest ~ '^sha256:[a-f0-9]{64}$'
            AND jsonb_typeof(approved_capabilities)='array'
            AND jsonb_array_length(approved_capabilities)<=64)
    )
);

CREATE TABLE selection_operations (
    bot_id text NOT NULL,
    operation_id text NOT NULL,
    input_digest text NOT NULL,
    input_payload jsonb NOT NULL,
    snapshot_payload jsonb NOT NULL,
    desired_enabled boolean NOT NULL,
    resulting_revision bigint NOT NULL CHECK (resulting_revision > 0),
    expected_graph_revision bigint NOT NULL CHECK (expected_graph_revision > 0),
    requested_by text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id, operation_id),
    UNIQUE (bot_id, resulting_revision)
);

CREATE FUNCTION selection_operation_immutable() RETURNS trigger
LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN
    RAISE EXCEPTION 'Forest component selection operation is immutable'
        USING ERRCODE = '55000';
END;
$$;
CREATE TRIGGER selection_operations_immutable
    BEFORE UPDATE OR DELETE ON selection_operations
    FOR EACH ROW EXECUTE FUNCTION selection_operation_immutable();

CREATE TABLE selection_applications (
    bot_id text NOT NULL,
    operation_id text NOT NULL,
    request_id text NOT NULL,
    state text NOT NULL DEFAULT 'PENDING'
        CHECK (state IN ('PENDING','RUNNING','SUBMITTED','SUPERSEDED','FAILED')),
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    claim_token text,
    lease_until timestamptz,
    available_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    receipt jsonb,
    error_code text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id, operation_id),
    UNIQUE (bot_id, request_id),
    FOREIGN KEY (bot_id, operation_id) REFERENCES selection_operations(bot_id, operation_id)
);
CREATE INDEX selection_applications_pending_idx
    ON selection_applications(bot_id,state,available_at,created_at);

CREATE TABLE selection_settlements (
    claim_token text PRIMARY KEY,
    input_payload jsonb NOT NULL,
    result_payload jsonb NOT NULL
);

CREATE FUNCTION component_selection_snapshot(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE sql STABLE SET search_path=pg_catalog AS $$
SELECT jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',
  jsonb_build_object('protocol','forest.component-selection-snapshot/1',
    'revision',COALESCE((SELECT revision FROM __forest_module_schema__.selection_heads
        WHERE bot_id=context->>'bot_id'),0),
    'entries',COALESCE((SELECT jsonb_agg(jsonb_build_object(
        'component_id',component_id,'source_kind',source_kind,
        'package_digest',package_digest,'manifest_digest',manifest_digest,
        'approved_capabilities',approved_capabilities,'desired_enabled',desired_enabled,
        'desired_revision',desired_revision) ORDER BY component_id)
        FROM __forest_module_schema__.selection_entries
        WHERE bot_id=context->>'bot_id'),'[]'::jsonb)))
$$;

CREATE FUNCTION component_selection_project(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path=pg_catalog AS $$
DECLARE source text:=input->>'source_kind'; target text:=input->>'component_id';
        current jsonb; projected jsonb; new_revision bigint;
BEGIN
  IF context->>'port_kind' IS DISTINCT FROM 'query'
     OR context->>'port_name' IS DISTINCT FROM 'component.selection.project'
     OR target IS NULL OR target !~ '^[a-z0-9][a-z0-9._-]{0,127}$'
     OR source IS NULL OR source NOT IN ('builtin','installed')
     OR jsonb_typeof(input->'desired_enabled') IS DISTINCT FROM 'boolean'
     OR jsonb_typeof(input->'expected_selection_revision') IS DISTINCT FROM 'number'
     OR jsonb_typeof(input->'approved_capabilities') IS DISTINCT FROM 'array'
     OR (source='builtin' AND (input->'package_digest' IS DISTINCT FROM 'null'::jsonb
         OR input->'manifest_digest' IS DISTINCT FROM 'null'::jsonb
         OR input->'approved_capabilities' IS DISTINCT FROM '[]'::jsonb))
     OR (source='installed' AND (input->>'package_digest' IS NULL
         OR input->>'manifest_digest' IS NULL
         OR input->>'package_digest' !~ '^sha256:[a-f0-9]{64}$'
         OR input->>'manifest_digest' !~ '^sha256:[a-f0-9]{64}$'))
  THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
      jsonb_build_object('code','COMPONENT_SELECTION_INVALID',
        'message','Component selection projection input is invalid','status',422));
  END IF;
  current:=__forest_module_schema__.component_selection_snapshot(context,'{}'::jsonb);
  IF current->'value'->'revision' IS DISTINCT FROM input->'expected_selection_revision' THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
      jsonb_build_object('code','COMPONENT_SELECTION_REVISION_CONFLICT',
        'message','Component selection revision changed','status',409));
  END IF;
  new_revision:=(current->'value'->>'revision')::bigint+1;
  SELECT COALESCE(jsonb_agg(entry ORDER BY entry->>'component_id'),'[]'::jsonb)
    INTO projected FROM (
      SELECT value AS entry
        FROM jsonb_array_elements(current->'value'->'entries') AS old(value)
        WHERE value->>'component_id' IS DISTINCT FROM target
      UNION ALL
      SELECT jsonb_build_object(
        'component_id',target,'source_kind',source,
        'package_digest',input->'package_digest',
        'manifest_digest',input->'manifest_digest',
        'approved_capabilities',input->'approved_capabilities',
        'desired_enabled',input->'desired_enabled',
        'desired_revision',new_revision)
    ) AS members;
  IF jsonb_array_length(projected)>128 THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
      jsonb_build_object('code','COMPONENT_SELECTION_LIMIT',
        'message','Bot component selection limit reached','status',422));
  END IF;
  RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',
    jsonb_build_object('protocol','forest.component-selection-snapshot/1',
      'revision',new_revision,'entries',projected));
END;
$$;

CREATE FUNCTION component_selection_operation_probe(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path=pg_catalog AS $$
DECLARE existing __forest_module_schema__.selection_operations%ROWTYPE;
        requested_operation_id text:=input->>'operation_id';
        input_digest text:=input->>'input_digest';
        enabled boolean;
        receipt jsonb;
BEGIN
  IF context->>'port_kind' IS DISTINCT FROM 'query'
     OR context->>'port_name' IS DISTINCT FROM 'component.selection.operation-probe'
     OR requested_operation_id IS NULL
     OR octet_length(requested_operation_id) NOT BETWEEN 1 AND 256
     OR input_digest IS NULL OR input_digest !~ '^sha256:[a-f0-9]{64}$'
     OR jsonb_typeof(input->'desired_enabled') IS DISTINCT FROM 'boolean'
  THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
      jsonb_build_object('code','COMPONENT_SELECTION_INVALID',
        'message','Component selection operation probe is invalid','status',422));
  END IF;
  enabled:=(input->>'desired_enabled')::boolean;
  SELECT * INTO existing FROM __forest_module_schema__.selection_operations AS operations
    WHERE operations.bot_id=context->>'bot_id'
      AND operations.operation_id=requested_operation_id;
  IF NOT FOUND THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',
      jsonb_build_object('protocol','forest.component-selection-operation-probe/1',
        'disposition','ABSENT','receipt',null));
  END IF;
  IF existing.input_digest IS DISTINCT FROM input_digest
     OR existing.desired_enabled IS DISTINCT FROM enabled THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
      jsonb_build_object('code','COMPONENT_SELECTION_OPERATION_CONFLICT',
        'message','Selection operation input differs','status',409));
  END IF;
  receipt:=jsonb_build_object('protocol','forest.component-selection-operation/1',
    'operation_id',requested_operation_id,
    'component_id',existing.input_payload->>'component_id',
    'desired_enabled',enabled,'selection_revision',existing.resulting_revision,
    'expected_graph_revision',existing.expected_graph_revision);
  RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',
    jsonb_build_object('protocol','forest.component-selection-operation-probe/1',
      'disposition','REPLAY','receipt',receipt));
END;
$$;

CREATE FUNCTION component_selection_set(context jsonb, input jsonb, enabled boolean)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog AS $$
DECLARE bot text:=context->>'bot_id'; op text:=context->>'operation_id';
        source text:=input->>'source_kind'; target text:=input->>'component_id';
        head_revision bigint; existing __forest_module_schema__.selection_operations%ROWTYPE;
        new_revision bigint; result jsonb; frozen jsonb;
BEGIN
  IF context->>'port_kind' IS DISTINCT FROM 'management_action'
     OR (enabled AND context->>'port_name' IS DISTINCT FROM 'component.selection.enable')
     OR (NOT enabled AND context->>'port_name' IS DISTINCT FROM 'component.selection.disable')
     OR op IS NULL OR octet_length(op) NOT BETWEEN 1 AND 256
     OR target IS NULL OR target !~ '^[a-z0-9][a-z0-9._-]{0,127}$'
     OR (NOT enabled AND target='component.worker-host')
     OR source IS NULL OR source NOT IN ('builtin','installed')
     OR (source='builtin' AND (input->'package_digest' IS DISTINCT FROM 'null'::jsonb
         OR input->'manifest_digest' IS DISTINCT FROM 'null'::jsonb
         OR input->'approved_capabilities' IS DISTINCT FROM '[]'::jsonb))
     OR (source='installed' AND (input->>'package_digest' IS NULL
         OR input->>'manifest_digest' IS NULL
         OR input->>'package_digest' !~ '^sha256:[a-f0-9]{64}$'
         OR input->>'manifest_digest' !~ '^sha256:[a-f0-9]{64}$'
         OR jsonb_typeof(input->'approved_capabilities') IS DISTINCT FROM 'array'))
     OR (input->>'expected_selection_revision')::bigint < 0
     OR (input->>'expected_graph_revision')::bigint <= 0
     OR octet_length(input->>'requested_by') NOT BETWEEN 1 AND 256
  THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
      jsonb_build_object('code','COMPONENT_SELECTION_INVALID',
        'message','Component selection input is invalid','status',422));
  END IF;
  PERFORM pg_advisory_xact_lock(hashtextextended('forest.component.selection/'||bot,0));
  INSERT INTO __forest_module_schema__.selection_heads(bot_id,revision)
    VALUES(bot,0) ON CONFLICT (bot_id) DO NOTHING;
  SELECT revision INTO head_revision FROM __forest_module_schema__.selection_heads
    WHERE bot_id=bot FOR UPDATE;
  SELECT * INTO existing FROM __forest_module_schema__.selection_operations
    WHERE bot_id=bot AND operation_id=op;
  IF FOUND THEN
    IF existing.input_digest IS DISTINCT FROM context->>'input_digest'
       OR existing.desired_enabled IS DISTINCT FROM enabled THEN
      RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
        jsonb_build_object('code','COMPONENT_SELECTION_OPERATION_CONFLICT',
          'message','Selection operation input differs','status',409));
    END IF;
    result:=jsonb_build_object('protocol','forest.component-selection-operation/1',
      'operation_id',op,'component_id',existing.input_payload->>'component_id',
      'desired_enabled',enabled,'selection_revision',existing.resulting_revision,
      'expected_graph_revision',existing.expected_graph_revision);
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',result);
  END IF;
  IF head_revision IS DISTINCT FROM (input->>'expected_selection_revision')::bigint THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
      jsonb_build_object('code','COMPONENT_SELECTION_REVISION_CONFLICT',
        'message','Component selection revision changed','status',409));
  END IF;
  IF NOT EXISTS (SELECT 1 FROM __forest_module_schema__.selection_entries
      WHERE bot_id=bot AND component_id=target)
     AND (SELECT count(*) FROM __forest_module_schema__.selection_entries
          WHERE bot_id=bot)>=128 THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
      jsonb_build_object('code','COMPONENT_SELECTION_LIMIT',
        'message','Bot component selection limit reached','status',422));
  END IF;
  new_revision:=head_revision+1;
  UPDATE __forest_module_schema__.selection_heads
    SET revision=new_revision WHERE bot_id=bot;
  INSERT INTO __forest_module_schema__.selection_entries(
      bot_id,component_id,source_kind,package_digest,manifest_digest,
      approved_capabilities,desired_enabled,desired_revision)
    VALUES(bot,target,source,input->>'package_digest',input->>'manifest_digest',
      input->'approved_capabilities',enabled,new_revision)
    ON CONFLICT (bot_id,component_id) DO UPDATE SET
      source_kind=EXCLUDED.source_kind,package_digest=EXCLUDED.package_digest,
      manifest_digest=EXCLUDED.manifest_digest,
      approved_capabilities=EXCLUDED.approved_capabilities,
      desired_enabled=EXCLUDED.desired_enabled,desired_revision=EXCLUDED.desired_revision,
      updated_at=clock_timestamp();
  SELECT value->'value' INTO frozen FROM (
    SELECT __forest_module_schema__.component_selection_snapshot(
      jsonb_build_object('bot_id',bot),'{}'::jsonb) AS value
  ) AS snapshot;
  INSERT INTO __forest_module_schema__.selection_operations(
      bot_id,operation_id,input_digest,input_payload,snapshot_payload,desired_enabled,
      resulting_revision,expected_graph_revision,requested_by)
    VALUES(bot,op,context->>'input_digest',input,frozen,enabled,new_revision,
      (input->>'expected_graph_revision')::bigint,input->>'requested_by');
  INSERT INTO __forest_module_schema__.selection_applications(bot_id,operation_id,request_id)
    VALUES(bot,op,'selection:'||md5(bot||chr(31)||op));
  result:=jsonb_build_object('protocol','forest.component-selection-operation/1',
    'operation_id',op,'component_id',target,'desired_enabled',enabled,
    'selection_revision',new_revision,
    'expected_graph_revision',(input->>'expected_graph_revision')::bigint);
  RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',result);
END;
$$;

CREATE FUNCTION component_selection_enable(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE sql VOLATILE SET search_path=pg_catalog AS $$
SELECT __forest_module_schema__.component_selection_set(context,input,true)
$$;

CREATE FUNCTION component_selection_disable(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE sql VOLATILE SET search_path=pg_catalog AS $$
SELECT __forest_module_schema__.component_selection_set(context,input,false)
$$;

CREATE FUNCTION component_selection_application_read(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path=pg_catalog AS $$
DECLARE item __forest_module_schema__.selection_applications%ROWTYPE;
        accepted __forest_module_schema__.selection_operations%ROWTYPE;
BEGIN
  SELECT * INTO item FROM __forest_module_schema__.selection_applications
    WHERE bot_id=context->>'bot_id' AND request_id=input->>'request_id';
  IF NOT FOUND THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
      jsonb_build_object('code','COMPONENT_SELECTION_NOT_FOUND',
        'message','Selection application does not exist','status',404));
  END IF;
  SELECT * INTO accepted FROM __forest_module_schema__.selection_operations
    WHERE bot_id=item.bot_id AND operation_id=item.operation_id;
  RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',
    jsonb_build_object('protocol','forest.component-selection-application/1',
      'request_id',item.request_id,'component_id',accepted.input_payload->>'component_id',
      'desired_enabled',accepted.desired_enabled,
      'selection_revision',accepted.resulting_revision,
      'expected_graph_revision',accepted.expected_graph_revision,
      'state',item.state,'attempt_count',item.attempt_count,
      'receipt',item.receipt,'error_code',item.error_code,
      'created_at',item.created_at,'updated_at',item.updated_at));
END;
$$;

CREATE FUNCTION component_selection_application_claim(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog AS $$
DECLARE item __forest_module_schema__.selection_applications%ROWTYPE;
        accepted __forest_module_schema__.selection_operations%ROWTYPE;
BEGIN
  UPDATE __forest_module_schema__.selection_applications
    SET state='PENDING',claim_token=NULL,lease_until=NULL,
      available_at=clock_timestamp(),updated_at=clock_timestamp()
    WHERE bot_id=context->>'bot_id' AND state='RUNNING'
      AND lease_until<=clock_timestamp();
  SELECT * INTO item FROM __forest_module_schema__.selection_applications
    WHERE bot_id=context->>'bot_id' AND state='PENDING'
      AND available_at<=clock_timestamp()
    ORDER BY available_at,created_at,request_id FOR UPDATE SKIP LOCKED LIMIT 1;
  IF NOT FOUND THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',NULL);
  END IF;
  SELECT * INTO accepted FROM __forest_module_schema__.selection_operations
    WHERE bot_id=item.bot_id AND operation_id=item.operation_id;
  UPDATE __forest_module_schema__.selection_applications
    SET state='RUNNING',attempt_count=attempt_count+1,
      claim_token=input->>'claim_token',
      lease_until=clock_timestamp()+(input->>'lease_seconds')::integer*interval '1 second',
      updated_at=clock_timestamp()
    WHERE bot_id=item.bot_id AND operation_id=item.operation_id RETURNING * INTO item;
  RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',
    jsonb_build_object('protocol','forest.owned-work-claim/1',
      'work_id',item.request_id,'claim_token',item.claim_token,'attempt',item.attempt_count,
      'operation_id','component-selection.run:'||item.request_id||':'||item.attempt_count,
      'causal_root_kind','component.selection','causal_root_id',item.request_id,
      'payload',jsonb_build_object('request_id',item.request_id,
        'component_id',accepted.input_payload->>'component_id',
        'expected_graph_revision',accepted.expected_graph_revision,
        'selection_revision',accepted.resulting_revision,
        'requested_by',accepted.requested_by,
        'snapshot',accepted.snapshot_payload)));
END;
$$;

CREATE FUNCTION component_selection_application_settle(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog AS $$
DECLARE item __forest_module_schema__.selection_applications%ROWTYPE;
        old __forest_module_schema__.selection_settlements%ROWTYPE;
        work jsonb:=input->'outcome'->'result';
        disposition text; result_payload jsonb;
BEGIN
  SELECT * INTO old FROM __forest_module_schema__.selection_settlements
    WHERE claim_token=input->>'claim_token';
  IF FOUND THEN
    IF old.input_payload=input THEN RETURN old.result_payload; END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
      jsonb_build_object('code','COMPONENT_SELECTION_SETTLEMENT_CONFLICT',
        'message','Claim has another settlement','status',409));
  END IF;
  SELECT * INTO item FROM __forest_module_schema__.selection_applications
    WHERE bot_id=context->>'bot_id' AND request_id=input->>'work_id'
      AND state='RUNNING' AND claim_token=input->>'claim_token'
      AND lease_until>clock_timestamp() FOR UPDATE;
  IF NOT FOUND THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
      jsonb_build_object('code','COMPONENT_SELECTION_STALE_CLAIM',
        'message','Selection claim expired','status',409));
  END IF;
  IF input->'outcome'->>'status'='SUCCEEDED' THEN
    disposition:=work->>'disposition';
  ELSIF COALESCE((input->'outcome'->'error'->>'retryable')::boolean,false) THEN
    disposition:='RETRY';
  ELSE disposition:='FAILED';
  END IF;
  UPDATE __forest_module_schema__.selection_applications
    SET state=CASE disposition WHEN 'SUBMITTED' THEN 'SUBMITTED'
      WHEN 'SUPERSEDED' THEN 'SUPERSEDED'
      WHEN 'RETRY' THEN 'PENDING' ELSE 'FAILED' END,
      claim_token=NULL,lease_until=NULL,
      available_at=CASE WHEN disposition='RETRY' THEN clock_timestamp()+
        LEAST(60,2^LEAST(item.attempt_count,5)) * interval '1 second' ELSE available_at END,
      receipt=CASE WHEN disposition='SUBMITTED' THEN work->'receipt' ELSE receipt END,
      error_code=CASE WHEN disposition='SUBMITTED' THEN NULL
        WHEN input->'outcome'->>'status'='SUCCEEDED' THEN work->>'error_code'
        ELSE input->'outcome'->'error'->>'code' END,
      updated_at=clock_timestamp()
    WHERE bot_id=item.bot_id AND operation_id=item.operation_id;
  result_payload:=jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',
    jsonb_build_object('protocol','forest.owned-work-settled/1','work_id',item.request_id));
  INSERT INTO __forest_module_schema__.selection_settlements(
    claim_token,input_payload,result_payload)
    VALUES(input->>'claim_token',input,result_payload);
  RETURN result_payload;
END;
$$;

CREATE TABLE reload_requests (
    bot_id text NOT NULL,
    request_id text NOT NULL,
    enqueue_operation_id text NOT NULL,
    input_payload jsonb NOT NULL,
    state text NOT NULL CHECK (state IN ('QUEUED','RUNNING','SUBMITTED','FAILED')),
    attempt_count integer NOT NULL DEFAULT 0,
    claim_token text,
    lease_until timestamptz,
    available_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    receipt jsonb,
    error_code text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id, request_id),
    UNIQUE (bot_id, enqueue_operation_id)
);
CREATE INDEX reload_requests_pending_idx
    ON reload_requests(bot_id, state, available_at, created_at);

CREATE TABLE reload_settlements (
    claim_token text PRIMARY KEY,
    input_payload jsonb NOT NULL,
    result_payload jsonb NOT NULL
);

CREATE TABLE configuration_rebuild_intents (
    bot_id text NOT NULL,
    operation_id text NOT NULL,
    request_id text NOT NULL,
    input_digest text NOT NULL,
    target_component_id text NOT NULL,
    target_generation_id text NOT NULL,
    plan_id text NOT NULL,
    desired_revision bigint NOT NULL CHECK (desired_revision > 1),
    payload_digest text NOT NULL CHECK (payload_digest ~ '^sha256:[a-f0-9]{64}$'),
    expected_graph_revision bigint NOT NULL CHECK (expected_graph_revision > 0),
    requested_by text NOT NULL,
    state text NOT NULL DEFAULT 'PENDING_REBUILD' CHECK (
        state IN ('PENDING_REBUILD','RUNNING','REBUILD_REQUESTED','EFFECTIVE','REBUILD_FAILED')
    ),
    attempt_count integer NOT NULL DEFAULT 0,
    claim_token text,
    lease_until timestamptz,
    available_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    kernel_receipt jsonb,
    error_code text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (bot_id, operation_id),
    UNIQUE (bot_id, request_id),
    UNIQUE (bot_id, target_component_id, desired_revision)
);
CREATE INDEX configuration_rebuild_pending_idx
    ON configuration_rebuild_intents(bot_id, state, available_at, created_at, request_id);

CREATE TABLE configuration_rebuild_settlements (
    claim_token text PRIMARY KEY,
    input_payload jsonb NOT NULL,
    result_payload jsonb NOT NULL
);

CREATE FUNCTION component_configuration_rebuild_projection(
    item __forest_module_schema__.configuration_rebuild_intents
) RETURNS jsonb LANGUAGE sql STABLE SET search_path=pg_catalog AS $$
SELECT jsonb_build_object('protocol','forest.configuration-rebuild/1',
  'request_id',item.request_id,'state',item.state,
  'target_component_id',item.target_component_id,
  'desired_revision',item.desired_revision,
  'expected_graph_revision',item.expected_graph_revision,
  'requested_by',item.requested_by,
  'attempt_count',item.attempt_count,'error_code',item.error_code,
  'kernel_receipt',item.kernel_receipt,
  'created_at',item.created_at,'updated_at',item.updated_at)
$$;

CREATE FUNCTION component_record_configuration_rebuild(context jsonb, envelope jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog AS $$
DECLARE input jsonb:=envelope->'input'; prior jsonb:=envelope->'prior'->'configuration';
        operation text:=context->>'operation_id'; bot text:=context->>'bot_id';
        saved __forest_module_schema__.configuration_rebuild_intents%ROWTYPE;
        request_identity text;
BEGIN
  IF context->>'port_kind' IS DISTINCT FROM 'participant'
     OR context->>'port_name' IS DISTINCT FROM 'component.configuration-rebuild.record'
     OR context->>'causal_root_kind' IS DISTINCT FROM 'management.configuration'
     OR envelope->>'protocol' IS DISTINCT FROM 'forest.uow-step/1'
     OR envelope->'caller'->>'component_id' IS DISTINCT FROM input->>'target_component_id'
     OR envelope->'caller'->>'generation_id' IS DISTINCT FROM input->>'target_generation_id'
     OR prior->>'component_id' IS DISTINCT FROM input->>'target_component_id'
     OR prior->>'generation_id' IS DISTINCT FROM input->>'target_generation_id'
     OR prior->>'plan_id' IS DISTINCT FROM input->>'plan_id'
     OR (prior->>'revision')::bigint IS DISTINCT FROM (input->>'desired_revision')::bigint
     OR prior->>'payload_digest' IS DISTINCT FROM input->>'payload_digest'
     OR octet_length(operation) NOT BETWEEN 1 AND 256
  THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
      jsonb_build_object('code','COMPONENT_CONFIGURATION_REBUILD_INVALID',
        'message','Configuration rebuild evidence is invalid','status',422));
  END IF;
  PERFORM pg_advisory_xact_lock(hashtextextended(
    'forest.component.configuration-rebuild/'||bot||'/'||operation,0));
  SELECT * INTO saved FROM __forest_module_schema__.configuration_rebuild_intents
   WHERE bot_id=bot AND operation_id=operation;
  IF FOUND THEN
    IF saved.target_component_id IS DISTINCT FROM input->>'target_component_id'
       OR saved.plan_id IS DISTINCT FROM input->>'plan_id'
       OR saved.desired_revision IS DISTINCT FROM (input->>'desired_revision')::bigint
       OR saved.payload_digest IS DISTINCT FROM input->>'payload_digest'
       OR saved.requested_by IS DISTINCT FROM input->>'requested_by' THEN
      RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
        jsonb_build_object('code','COMPONENT_CONFIGURATION_REBUILD_CONFLICT',
          'message','Configuration rebuild operation input differs','status',409));
    END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',
      __forest_module_schema__.component_configuration_rebuild_projection(saved));
  END IF;
  request_identity:='config-rebuild:'||md5(bot||chr(31)||operation);
  INSERT INTO __forest_module_schema__.configuration_rebuild_intents(
    bot_id,operation_id,request_id,input_digest,target_component_id,
    target_generation_id,plan_id,desired_revision,payload_digest,
    expected_graph_revision,requested_by)
  VALUES(bot,operation,request_identity,context->>'input_digest',
    input->>'target_component_id',input->>'target_generation_id',
    input->>'plan_id',(input->>'desired_revision')::bigint,input->>'payload_digest',
    (input->>'expected_graph_revision')::bigint,input->>'requested_by')
  RETURNING * INTO saved;
  RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',
    __forest_module_schema__.component_configuration_rebuild_projection(saved));
END;
$$;

CREATE FUNCTION component_configuration_rebuild_read(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path=pg_catalog AS $$
DECLARE item __forest_module_schema__.configuration_rebuild_intents%ROWTYPE;
BEGIN
  SELECT * INTO item FROM __forest_module_schema__.configuration_rebuild_intents
   WHERE bot_id=context->>'bot_id' AND request_id=input->>'request_id';
  IF NOT FOUND THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
      jsonb_build_object('code','COMPONENT_CONFIGURATION_REBUILD_NOT_FOUND',
        'message','Configuration rebuild does not exist','status',404));
  END IF;
  RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',
    __forest_module_schema__.component_configuration_rebuild_projection(item));
END;
$$;

CREATE FUNCTION component_configuration_rebuild_claim(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog AS $$
DECLARE item __forest_module_schema__.configuration_rebuild_intents%ROWTYPE;
BEGIN
  UPDATE __forest_module_schema__.configuration_rebuild_intents
   SET state='PENDING_REBUILD',claim_token=NULL,lease_until=NULL,
       available_at=clock_timestamp(),updated_at=clock_timestamp()
   WHERE bot_id=context->>'bot_id' AND state='RUNNING' AND lease_until<=clock_timestamp();
  UPDATE __forest_module_schema__.configuration_rebuild_intents
   SET state='REBUILD_FAILED',error_code='COMPONENT_CONFIGURATION_REBUILD_EXPIRED',
       updated_at=clock_timestamp()
   WHERE bot_id=context->>'bot_id' AND state IN ('PENDING_REBUILD','REBUILD_REQUESTED')
     AND created_at+interval '2 hours'<=clock_timestamp();
  SELECT * INTO item FROM __forest_module_schema__.configuration_rebuild_intents
   WHERE bot_id=context->>'bot_id'
     AND state IN ('PENDING_REBUILD','REBUILD_REQUESTED')
     AND available_at<=clock_timestamp()
   ORDER BY available_at,created_at,request_id FOR UPDATE SKIP LOCKED LIMIT 1;
  IF NOT FOUND THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',NULL);
  END IF;
  UPDATE __forest_module_schema__.configuration_rebuild_intents
   SET state='RUNNING',attempt_count=attempt_count+1,
       claim_token=input->>'claim_token',
       lease_until=clock_timestamp()+(input->>'lease_seconds')::integer*interval '1 second',
       updated_at=clock_timestamp()
   WHERE bot_id=item.bot_id AND operation_id=item.operation_id RETURNING * INTO item;
  RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',
    jsonb_build_object('protocol','forest.owned-work-claim/1',
      'work_id',item.request_id,'claim_token',item.claim_token,'attempt',item.attempt_count,
      'operation_id','component.configuration-rebuild.run:'||item.request_id||':'||item.attempt_count,
      'causal_root_kind','component.configuration-rebuild',
      'causal_root_id',item.request_id,
      'payload',jsonb_build_object('request_id',item.request_id,
        'target_component_id',item.target_component_id,
        'desired_revision',item.desired_revision,
        'requested_by',item.requested_by,
        'kernel_receipt',item.kernel_receipt)));
END;
$$;

CREATE FUNCTION component_configuration_rebuild_settle(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog AS $$
DECLARE item __forest_module_schema__.configuration_rebuild_intents%ROWTYPE;
        old __forest_module_schema__.configuration_rebuild_settlements%ROWTYPE;
        work jsonb:=input->'outcome'->'result'; disposition text; result_payload jsonb;
BEGIN
  SELECT * INTO old FROM __forest_module_schema__.configuration_rebuild_settlements
   WHERE claim_token=input->>'claim_token';
  IF FOUND THEN
    IF old.input_payload=input THEN RETURN old.result_payload; END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
      jsonb_build_object('code','COMPONENT_CONFIGURATION_REBUILD_SETTLEMENT_CONFLICT',
        'message','Claim has another settlement','status',409));
  END IF;
  SELECT * INTO item FROM __forest_module_schema__.configuration_rebuild_intents
   WHERE bot_id=context->>'bot_id' AND request_id=input->>'work_id'
     AND state='RUNNING' AND claim_token=input->>'claim_token'
     AND lease_until>clock_timestamp() FOR UPDATE;
  IF NOT FOUND THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
      jsonb_build_object('code','COMPONENT_CONFIGURATION_REBUILD_STALE_CLAIM',
        'message','Configuration rebuild claim expired','status',409));
  END IF;
  IF input->'outcome'->>'status'='SUCCEEDED' THEN
    disposition:=work->>'disposition';
  ELSIF COALESCE((input->'outcome'->'error'->>'retryable')::boolean,false) THEN
    disposition:='RETRY';
  ELSE disposition:='FAILED';
  END IF;
  UPDATE __forest_module_schema__.configuration_rebuild_intents
   SET state=CASE disposition WHEN 'EFFECTIVE' THEN 'EFFECTIVE'
       WHEN 'REQUESTED' THEN 'REBUILD_REQUESTED'
       WHEN 'RETRY' THEN 'PENDING_REBUILD' ELSE 'REBUILD_FAILED' END,
       claim_token=NULL,lease_until=NULL,
       available_at=CASE WHEN disposition='REQUESTED' THEN clock_timestamp()+interval '2 seconds'
         WHEN disposition='RETRY' THEN clock_timestamp()+interval '5 seconds'
         ELSE available_at END,
       kernel_receipt=CASE WHEN disposition='REQUESTED' THEN work->'receipt'
         ELSE kernel_receipt END,
       error_code=CASE WHEN disposition IN ('EFFECTIVE','REQUESTED') THEN NULL
         WHEN input->'outcome'->>'status'='SUCCEEDED' THEN work->>'error_code'
         ELSE input->'outcome'->'error'->>'code' END,
       updated_at=clock_timestamp()
   WHERE bot_id=item.bot_id AND operation_id=item.operation_id;
  result_payload:=jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',
    jsonb_build_object('protocol','forest.owned-work-settled/1','work_id',item.request_id));
  INSERT INTO __forest_module_schema__.configuration_rebuild_settlements(
    claim_token,input_payload,result_payload)
    VALUES(input->>'claim_token',input,result_payload);
  RETURN result_payload;
END;
$$;

CREATE FUNCTION component_reload_projection(item __forest_module_schema__.reload_requests)
RETURNS jsonb LANGUAGE sql STABLE SET search_path=pg_catalog AS $$
SELECT jsonb_build_object(
    'protocol','forest.component-reload/1',
    'request_id',item.request_id,
    'component_id',item.input_payload->>'component_id',
    'expected_graph_revision',(item.input_payload->>'expected_graph_revision')::bigint,
    'requested_by',item.input_payload->>'requested_by',
    'state',item.state,
    'attempt_count',item.attempt_count,
    'receipt',item.receipt,
    'error_code',item.error_code,
    'created_at',item.created_at,
    'updated_at',item.updated_at
)
$$;

CREATE FUNCTION component_reload_enqueue(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog AS $$
DECLARE item __forest_module_schema__.reload_requests%ROWTYPE;
        op text:=context->>'operation_id'; bot text:=context->>'bot_id';
        new_id text;
BEGIN
  IF op IS NULL OR octet_length(op) NOT BETWEEN 1 AND 256 THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
      jsonb_build_object('code','COMPONENT_RELOAD_INPUT_INVALID','message','Reload operation is invalid','status',422));
  END IF;
  PERFORM pg_advisory_xact_lock(hashtextextended('forest.component.reload/'||bot||'/'||op,0));
  SELECT * INTO item FROM __forest_module_schema__.reload_requests
   WHERE bot_id=bot AND enqueue_operation_id=op;
  IF FOUND THEN
    IF item.input_payload=input THEN
      RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',
        __forest_module_schema__.component_reload_projection(item));
    END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
      jsonb_build_object('code','COMPONENT_RELOAD_OPERATION_CONFLICT',
        'message','Reload operation input differs','status',409));
  END IF;
  new_id:='reload:'||md5(bot||chr(31)||op);
  INSERT INTO __forest_module_schema__.reload_requests(
    bot_id,request_id,enqueue_operation_id,input_payload,state)
  VALUES(bot,new_id,op,input,'QUEUED') RETURNING * INTO item;
  RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',
    __forest_module_schema__.component_reload_projection(item));
END;
$$;

CREATE FUNCTION component_reload_read(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql STABLE SET search_path=pg_catalog AS $$
DECLARE item __forest_module_schema__.reload_requests%ROWTYPE;
BEGIN
  SELECT * INTO item FROM __forest_module_schema__.reload_requests
   WHERE bot_id=context->>'bot_id' AND request_id=input->>'request_id';
  IF NOT FOUND THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
      jsonb_build_object('code','COMPONENT_RELOAD_NOT_FOUND','message','Reload request does not exist','status',404));
  END IF;
  RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',
    __forest_module_schema__.component_reload_projection(item));
END;
$$;

CREATE FUNCTION component_reload_claim(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog AS $$
DECLARE item __forest_module_schema__.reload_requests%ROWTYPE;
BEGIN
  UPDATE __forest_module_schema__.reload_requests SET state='QUEUED',claim_token=NULL,
    lease_until=NULL,available_at=clock_timestamp(),updated_at=clock_timestamp()
   WHERE bot_id=context->>'bot_id' AND state='RUNNING' AND lease_until<=clock_timestamp();
  SELECT * INTO item FROM __forest_module_schema__.reload_requests
   WHERE bot_id=context->>'bot_id' AND state='QUEUED' AND available_at<=clock_timestamp()
   ORDER BY available_at,created_at,request_id FOR UPDATE SKIP LOCKED LIMIT 1;
  IF NOT FOUND THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',NULL);
  END IF;
  UPDATE __forest_module_schema__.reload_requests SET state='RUNNING',
    attempt_count=attempt_count+1,claim_token=input->>'claim_token',
    lease_until=clock_timestamp()+(input->>'lease_seconds')::integer*interval '1 second',
    updated_at=clock_timestamp()
   WHERE bot_id=item.bot_id AND request_id=item.request_id RETURNING * INTO item;
  RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',
    jsonb_build_object('protocol','forest.owned-work-claim/1',
      'work_id',item.request_id,'claim_token',item.claim_token,'attempt',item.attempt_count,
      'operation_id','component-reload.run:'||item.request_id||':'||item.attempt_count,
      'causal_root_kind','component.reload','causal_root_id',item.request_id,
      'payload',jsonb_build_object('request_id',item.request_id,
        'component_id',item.input_payload->>'component_id',
        'expected_graph_revision',(item.input_payload->>'expected_graph_revision')::bigint,
        'requested_by',item.input_payload->>'requested_by')));
END;
$$;

CREATE FUNCTION component_reload_settle(context jsonb, input jsonb)
RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog AS $$
DECLARE item __forest_module_schema__.reload_requests%ROWTYPE;
        old __forest_module_schema__.reload_settlements%ROWTYPE;
        work jsonb:=input->'outcome'->'result';
        disposition text; result_payload jsonb;
BEGIN
  SELECT * INTO old FROM __forest_module_schema__.reload_settlements
   WHERE claim_token=input->>'claim_token';
  IF FOUND THEN
    IF old.input_payload=input THEN RETURN old.result_payload; END IF;
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
      jsonb_build_object('code','COMPONENT_RELOAD_SETTLEMENT_CONFLICT',
        'message','Claim has another settlement','status',409));
  END IF;
  SELECT * INTO item FROM __forest_module_schema__.reload_requests
   WHERE bot_id=context->>'bot_id' AND request_id=input->>'work_id'
     AND state='RUNNING' AND claim_token=input->>'claim_token'
     AND lease_until>clock_timestamp() FOR UPDATE;
  IF NOT FOUND THEN
    RETURN jsonb_build_object('protocol','forest.domain-result/1','ok',false,'error',
      jsonb_build_object('code','COMPONENT_RELOAD_STALE_CLAIM','message','Reload claim expired','status',409));
  END IF;
  IF input->'outcome'->>'status'='SUCCEEDED' THEN
    disposition:=work->>'disposition';
  ELSIF COALESCE((input->'outcome'->'error'->>'retryable')::boolean,false) THEN
    disposition:='RETRY';
  ELSE disposition:='FAILED';
  END IF;
  UPDATE __forest_module_schema__.reload_requests SET
    state=CASE disposition WHEN 'SUCCEEDED' THEN 'SUBMITTED'
      WHEN 'RETRY' THEN 'QUEUED' ELSE 'FAILED' END,
    claim_token=NULL,lease_until=NULL,
    available_at=CASE WHEN disposition='RETRY' THEN clock_timestamp()+
      LEAST(60,2^LEAST(item.attempt_count,5)) * interval '1 second' ELSE available_at END,
    receipt=CASE WHEN disposition='SUCCEEDED' THEN work->'receipt' ELSE receipt END,
    error_code=CASE WHEN disposition='SUCCEEDED' THEN NULL
      WHEN input->'outcome'->>'status'='SUCCEEDED' THEN work->>'error_code'
      ELSE input->'outcome'->'error'->>'code' END,
    updated_at=clock_timestamp()
   WHERE bot_id=item.bot_id AND request_id=item.request_id;
  result_payload:=jsonb_build_object('protocol','forest.domain-result/1','ok',true,'value',
    jsonb_build_object('protocol','forest.owned-work-settled/1','work_id',item.request_id));
  INSERT INTO __forest_module_schema__.reload_settlements(claim_token,input_payload,result_payload)
    VALUES(input->>'claim_token',input,result_payload);
  RETURN result_payload;
END;
$$;
