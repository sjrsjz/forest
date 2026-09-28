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
